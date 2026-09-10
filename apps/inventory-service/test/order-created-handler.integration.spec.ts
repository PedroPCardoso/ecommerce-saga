import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';

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

describe('OrderCreatedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const handler = new OrderCreatedHandler(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.stockReservation.deleteMany();
    await prisma.client.knownOrder.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('grava KnownOrder só com sku+quantity — nunca publica evento de domínio', async () => {
    const orderId = randomUUID();
    const envelope = makeOrderCreated(orderId, [
      { sku: 'BOOK-001', quantity: 2 },
      { sku: 'BOOK-002', quantity: 1 },
    ]);

    await handler.handle(envelope);

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.items).toEqual([
      { sku: 'BOOK-001', quantity: 2 },
      { sku: 'BOOK-002', quantity: 1 },
    ]);

    const outboxCount = await prisma.client.outbox.count();
    expect(outboxCount).toBe(0);
  });

  it('reentrega do MESMO evento (mesmo eventId) não falha nem duplica — idempotência', async () => {
    const orderId = randomUUID();
    const envelope = makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]);

    await handler.handle(envelope);
    await handler.handle(envelope);

    const count = await prisma.client.knownOrder.count({ where: { orderId } });
    expect(count).toBe(1);

    const processed = await prisma.client.processedMessage.count({
      where: { eventId: envelope.eventId },
    });
    expect(processed).toBe(1);
  });
});
