import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, paymentEvents, TOPICS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { env } from '../src/env.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, items: Array<{ sku: string; quantity: number }>) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: items.map((item) => ({ ...item, name: 'Item de teste', unitPriceCents: 1000 })),
      totalAmountCents: items.reduce((sum, item) => sum + item.quantity * 1000, 0),
      currency: 'BRL',
      shippingAddress: ADDRESS,
    },
  });
}

function makePaymentApproved(orderId: string) {
  return createEvent(paymentEvents.paymentApproved, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'payment-service-test@0.0.0',
    payload: {
      paymentId: randomUUID(),
      orderId,
      amountCents: 2000,
      currency: 'BRL',
      authorizationCode: 'AUTH-TEST-1',
      instrument: { gatewayToken: 'tok_test_1', cardLast4: '4242', brand: 'VISA' },
      approvedAt: new Date().toISOString(),
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

describe('Inventory Service — e2e (Kafka + Postgres reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);

    // EventProducer (@ecommerce/kafka), não kafkajs cru — mesma convenção usada pelos
    // testes de integração da Fase 2 (consumer-runtime.integration.spec.ts) para publicar
    // envelopes fabricados diretamente nos tópicos, simulando o outro serviço.
    producer = new EventProducer({ brokers: env.KAFKA_BROKERS, clientId: 'inventory-e2e-test-producer' });
    await producer.connect();
  });

  beforeEach(async () => {
    await prisma.client.stockReservation.deleteMany();
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

  it('cenário feliz: order.created + payment.approved reserva estoque e publica stock.reserved', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 1 }]));
    await producer.publish(TOPICS.payments, makePaymentApproved(orderId));

    await waitUntil(async () => {
      const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
      return reservation?.status === 'RESERVED';
    }, 20_000);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('stock.reserved');
  }, 25_000);

  it('SKU OUT- publica stock.unavailable, sem reservar nada', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, [{ sku: 'OUT-1', quantity: 1 }]));
    await producer.publish(TOPICS.payments, makePaymentApproved(orderId));

    await waitUntil(async () => {
      const row = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
      return row?.eventType === 'stock.unavailable';
    }, 20_000);

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation).toBeNull();
  }, 25_000);

  it('TESTE MAIS IMPORTANTE — payment.approved publicado ANTES de order.created ainda assim reserva, via retry real (retry-5s)', async () => {
    const orderId = randomUUID();

    // Publica payment.approved PRIMEIRO. O InventoryConsumerService vai processá-lo, não
    // encontrar KnownOrder, lançar erro retriável — a mensagem é redirecionada para
    // ecommerce.payments.v1.inventory-service.retry-5s e volta ~5s depois.
    await producer.publish(TOPICS.payments, makePaymentApproved(orderId));

    // Só publica order.created DEPOIS — reproduz a entrega fora de ordem entre tópicos
    // diferentes que motivou o Inventory a assinar `orders` (ver topics.ts).
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));

    await waitUntil(async () => {
      const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
      return reservation?.status === 'RESERVED';
    }, 20_000); // > delay do degrau retry-5s + margem para o segundo processamento

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation?.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('stock.reserved');
  }, 30_000);
});
