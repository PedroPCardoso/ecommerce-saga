import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createEvent, orderEvents } from '@ecommerce/contracts';
import { env } from '../src/env.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OutboxRelayService } from '../src/infrastructure/outbox-relay.service.js';
import { AuthorizePaymentUseCase } from '../src/application/authorize-payment.use-case.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function orderCreatedEnvelope(totalAmountCents: number, orderId: string = randomUUID()) {
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

describe('AuthorizePaymentUseCase (integração — Postgres + Kafka reais, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const relay = new OutboxRelayService();

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.payment.deleteMany();
    await relay.onModuleInit();
  });

  afterAll(async () => {
    await relay.onModuleDestroy();
    await prisma.onModuleDestroy();
  });

  it('autoriza e grava Payment AUTHORIZED + Outbox payment.approved na mesma transação', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(9980);

    await useCase.execute(envelope);

    const payment = await prisma.client.payment.findUnique({
      where: { orderId: envelope.payload.orderId },
    });
    expect(payment?.status).toBe('AUTHORIZED');
    expect(payment?.authorizationCode).toBeTruthy();
    expect(payment?.failureCode).toBeNull();

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: envelope.payload.orderId },
    });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('payment.approved');
  });

  it('recusa quando totalAmountCents termina em 13 e grava Payment FAILED + Outbox payment.failed', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(5013);

    await useCase.execute(envelope);

    const payment = await prisma.client.payment.findUnique({
      where: { orderId: envelope.payload.orderId },
    });
    expect(payment?.status).toBe('FAILED');
    expect(payment?.failureCode).toBe('CARD_DECLINED');
    expect(payment?.authorizationCode).toBeNull();

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: envelope.payload.orderId },
    });
    expect(outboxRows[0]?.eventType).toBe('payment.failed');
  });

  it('reentrega do mesmo order.created (mesmo eventId) não duplica Payment nem Outbox', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(4990);

    await useCase.execute(envelope);
    await useCase.execute(envelope); // mesmo eventId — simula reentrega do broker

    const paymentCount = await prisma.client.payment.count({
      where: { orderId: envelope.payload.orderId },
    });
    expect(paymentCount).toBe(1);

    const outboxCount = await prisma.client.outbox.count({
      where: { aggregateId: envelope.payload.orderId },
    });
    expect(outboxCount).toBe(1);
  });

  it('acima de R$100 (10_000 centavos) dorme 30s ANTES de decidir — verificado via função substituída, sem esperar de verdade', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const sleep = vi.fn().mockResolvedValue(undefined);
    useCase.sleep = sleep;
    const envelope = orderCreatedEnvelope(15_000);

    await useCase.execute(envelope);

    expect(sleep).toHaveBeenCalledWith(30_000);
    expect(sleep).toHaveBeenCalledTimes(1);

    const payment = await prisma.client.payment.findUnique({
      where: { orderId: envelope.payload.orderId },
    });
    expect(payment?.status).toBe('AUTHORIZED');
  });

  it('o relay publica payment.approved no tópico ecommerce.payments.v1 com a chave = orderId', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(4990);

    await useCase.execute(envelope);

    const kafka = new Kafka({
      clientId: 'payment-service-test-reader',
      brokers: env.KAFKA_BROKERS,
      logLevel: logLevel.NOTHING,
    });
    const consumer = kafka.consumer({
      groupId: `payment-service-test-reader-${envelope.payload.orderId}`,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ecommerce.payments.v1', fromBeginning: true });

    const found = await new Promise<{ key: string | null; eventType: string }>((resolve) => {
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          const value = JSON.parse(message.value!.toString());
          if (value.aggregateId === envelope.payload.orderId) {
            resolve({ key: message.key?.toString() ?? null, eventType: value.eventType });
          }
        },
      });
    });
    await consumer.disconnect();

    expect(found.key).toBe(envelope.payload.orderId);
    expect(found.eventType).toBe('payment.approved');
  }, 15_000);
});
