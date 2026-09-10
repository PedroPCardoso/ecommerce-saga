import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { env } from '../src/env.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OutboxRelayService } from '../src/infrastructure/outbox-relay.service.js';
import { CreateOrderUseCase } from '../src/application/create-order.use-case.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 2, unitPriceCents: 4990 };
const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

describe('CreateOrderUseCase (integração — Postgres + Kafka reais, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const relay = new OutboxRelayService();
  const useCase = new CreateOrderUseCase(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.idempotencyKey.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.order.deleteMany();
    await relay.onModuleInit();
  });

  afterAll(async () => {
    await relay.onModuleDestroy();
    await prisma.onModuleDestroy();
  });

  it('grava Order + Outbox + IdempotencyKey na mesma transação', async () => {
    const customerId = randomUUID();
    const idempotencyKey = randomUUID();

    const { result, replayed } = await useCase.execute({
      customerId,
      idempotencyKey,
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });

    expect(replayed).toBe(false);
    expect(result.status).toBe('PENDING');

    const order = await prisma.client.order.findUnique({ where: { id: result.orderId } });
    expect(order?.totalAmountCents).toBe(9980); // 2 * 4990 — calculado no servidor

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: result.orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('order.created');
  });

  it('Idempotency-Key repetida devolve o MESMO orderId, sem criar segundo pedido', async () => {
    const customerId = randomUUID();
    const idempotencyKey = randomUUID();

    const first = await useCase.execute({
      customerId,
      idempotencyKey,
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });
    const second = await useCase.execute({
      customerId,
      idempotencyKey,
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });

    expect(second.replayed).toBe(true);
    expect(second.result.orderId).toBe(first.result.orderId);

    const count = await prisma.client.order.count({ where: { customerId } });
    expect(count).toBe(1);
  });

  it('o relay publica order.created no tópico ecommerce.orders.v1 com a chave = orderId', async () => {
    const customerId = randomUUID();
    const { result } = await useCase.execute({
      customerId,
      idempotencyKey: randomUUID(),
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });

    const kafka = new Kafka({
      clientId: 'order-service-test-reader',
      brokers: env.KAFKA_BROKERS,
      logLevel: logLevel.NOTHING,
    });
    const consumer = kafka.consumer({ groupId: `order-service-test-reader-${result.orderId}` });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ecommerce.orders.v1', fromBeginning: true });

    const found = await new Promise<{ key: string | null; eventType: string }>((resolve) => {
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          const value = JSON.parse(message.value!.toString());
          if (value.aggregateId === result.orderId) {
            resolve({ key: message.key?.toString() ?? null, eventType: value.eventType });
          }
        },
      });
    });
    await consumer.disconnect();

    expect(found.key).toBe(result.orderId);
    expect(found.eventType).toBe('order.created');
  }, 15_000);
});
