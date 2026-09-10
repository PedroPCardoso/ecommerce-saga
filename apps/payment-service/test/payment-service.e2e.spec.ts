import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { TOPICS, createEvent, orderEvents } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { env } from '../src/env.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function orderCreatedEnvelope(totalAmountCents: number, orderId: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.1.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: totalAmountCents }],
      totalAmountCents,
      currency: 'BRL',
      shippingAddress: ADDRESS,
    },
  });
}

function orderConfirmedEnvelope(orderId: string) {
  return createEvent(orderEvents.orderConfirmed, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.1.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      totalAmountCents: 4990,
      currency: 'BRL',
      confirmedAt: new Date().toISOString(),
    },
  });
}

async function waitForPayment(prisma: PrismaService, orderId: string, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const payment = await prisma.client.payment.findUnique({ where: { orderId } });
    if (payment) return payment;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`Payment para orderId ${orderId} não apareceu em ${timeoutMs}ms`);
}

async function waitForPaymentEvent(
  orderId: string,
  timeoutMs = 15_000,
): Promise<{ eventType: string; key: string | null }> {
  const kafka = new Kafka({
    clientId: 'payment-service-e2e-reader',
    brokers: env.KAFKA_BROKERS,
    logLevel: logLevel.NOTHING,
  });
  const consumer = kafka.consumer({ groupId: `payment-service-e2e-reader-${orderId}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.payments, fromBeginning: true });

  const found = await new Promise<{ eventType: string; key: string | null }>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timeout esperando evento de pagamento')), timeoutMs);
    consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        const value = JSON.parse(message.value!.toString());
        if (value.aggregateId === orderId) {
          clearTimeout(timeout);
          resolve({ eventType: value.eventType, key: message.key?.toString() ?? null });
        }
      },
    });
  });
  await consumer.disconnect();
  return found;
}

describe('Payment Service — e2e (Kafka + Postgres reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);

    producer = new EventProducer({
      brokers: env.KAFKA_BROKERS,
      clientId: 'payment-service-e2e-producer',
    });
    await producer.connect();
  });

  beforeEach(async () => {
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.payment.deleteMany();
  });

  afterAll(async () => {
    await producer.disconnect();
    await app.close();
  });

  it('consome order.created de verdade, autoriza e publica payment.approved', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, orderCreatedEnvelope(4990, orderId));

    const payment = await waitForPayment(prisma, orderId);
    expect(payment.status).toBe('AUTHORIZED');

    const event = await waitForPaymentEvent(orderId);
    expect(event.eventType).toBe('payment.approved');
    expect(event.key).toBe(orderId);
  }, 20_000);

  it('recusa quando totalAmountCents termina em 13 e publica payment.failed', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, orderCreatedEnvelope(5013, orderId));

    const payment = await waitForPayment(prisma, orderId);
    expect(payment.status).toBe('FAILED');

    const event = await waitForPaymentEvent(orderId);
    expect(event.eventType).toBe('payment.failed');
  }, 20_000);

  it('ignora order.confirmed (mesmo tópico, outro eventType) — sem criar Payment nem erro', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, orderConfirmedEnvelope(orderId));

    // Dá tempo do consumer processar (ou melhor: NÃO processar) a mensagem.
    await new Promise((resolve) => setTimeout(resolve, 3_000));

    const payment = await prisma.client.payment.findUnique({ where: { orderId } });
    expect(payment).toBeNull();
  }, 10_000);

  it('reentrega manual do mesmo order.created (mesmo eventId) não duplica Payment', async () => {
    const orderId = randomUUID();
    const envelope = orderCreatedEnvelope(4990, orderId);

    await producer.publish(TOPICS.orders, envelope);
    await waitForPayment(prisma, orderId);

    await producer.publish(TOPICS.orders, envelope); // mesmo eventId — reentrega simulada
    await new Promise((resolve) => setTimeout(resolve, 3_000));

    const count = await prisma.client.payment.count({ where: { orderId } });
    expect(count).toBe(1);
  }, 20_000);
});
