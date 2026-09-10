import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, inventoryEvents, TOPICS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { env } from '../src/env.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 };

function addressFor(zipCode: string) {
  return {
    street: 'Rua Teste',
    number: '1',
    district: 'Centro',
    city: 'São Paulo',
    state: 'SP',
    zipCode,
    country: 'BR',
  };
}

function makeOrderCreated(orderId: string, zipCode: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [ITEM],
      totalAmountCents: 4990,
      currency: 'BRL',
      shippingAddress: addressFor(zipCode),
    },
  });
}

function makeStockReserved(orderId: string) {
  return createEvent(inventoryEvents.stockReserved, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'inventory-service-test@0.0.0',
    payload: {
      reservationId: randomUUID(),
      orderId,
      items: [{ sku: ITEM.sku, quantity: ITEM.quantity }],
      expiresAt: new Date(Date.now() + 30 * 60_000).toISOString(),
      reservedAt: new Date().toISOString(),
    },
  });
}

async function waitUntil(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error('Timeout esperando condição');
}

describe('Shipping Service — e2e (Kafka + Postgres reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);

    // EventProducer (@ecommerce/kafka), não kafkajs cru — mesma convenção usada
    // pelos testes e2e das Fases 3/4 para publicar envelopes fabricados
    // diretamente nos tópicos, simulando os outros serviços.
    producer = new EventProducer({ brokers: env.KAFKA_BROKERS, clientId: 'shipping-e2e-test-producer' });
    await producer.connect();
  });

  beforeEach(async () => {
    await prisma.client.shipment.deleteMany();
    await prisma.client.knownOrder.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
  });

  afterAll(async () => {
    await producer.disconnect();
    await app.close();
  });

  it('GET /health/live responde sem autenticação', async () => {
    await request(app.getHttpServer()).get('/health/live').expect(200);
  });

  it('cenário feliz: order.created + stock.reserved gera Shipment e publica shipment.created', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, '01000-000'));
    await producer.publish(TOPICS.inventory, makeStockReserved(orderId));

    await waitUntil(async () => {
      const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
      return shipment?.status === 'CREATED';
    }, 20_000);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.created');
  }, 25_000);

  it('CEP 00000-XXX publica shipment.failed, sem criar Shipment', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, '00000-000'));
    await producer.publish(TOPICS.inventory, makeStockReserved(orderId));

    await waitUntil(async () => {
      const row = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
      return row?.eventType === 'shipment.failed';
    }, 20_000);

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment).toBeNull();
  }, 25_000);

  it('TESTE MAIS IMPORTANTE — stock.reserved publicado ANTES de order.created ainda assim gera o envio, via retry real (retry-5s)', async () => {
    const orderId = randomUUID();

    // Publica stock.reserved PRIMEIRO. O ShippingConsumerService vai
    // processá-lo, não encontrar KnownOrder, lançar erro retriável — a
    // mensagem é redirecionada para
    // ecommerce.inventory.v1.shipping-service.retry-5s e volta ~5s depois.
    await producer.publish(TOPICS.inventory, makeStockReserved(orderId));

    // Só publica order.created DEPOIS — reproduz a entrega fora de ordem
    // entre tópicos diferentes que motivou o Shipping a assinar `orders`.
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, '01000-000'));

    await waitUntil(async () => {
      const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
      return shipment?.status === 'CREATED';
    }, 20_000); // > delay do degrau retry-5s + margem para o segundo processamento

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.created');
  }, 30_000);
});
