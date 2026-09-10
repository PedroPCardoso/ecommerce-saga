import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 };
const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, address = ADDRESS) {
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
      shippingAddress: address,
    },
  });
}

describe('OrderCreatedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const handler = new OrderCreatedHandler(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.knownOrder.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('grava KnownOrder com o endereço — nunca publica evento de domínio', async () => {
    const orderId = randomUUID();

    await handler.handle(makeOrderCreated(orderId));

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.shippingAddress).toEqual(ADDRESS);

    const outboxCount = await prisma.client.outbox.count();
    expect(outboxCount).toBe(0);
  });

  it('reentrega do MESMO eventId não falha nem duplica (idempotência)', async () => {
    const orderId = randomUUID();
    const envelope = makeOrderCreated(orderId);

    await handler.handle(envelope);
    await handler.handle(envelope);

    const count = await prisma.client.knownOrder.count({ where: { orderId } });
    expect(count).toBe(1);
  });

  it('order.created mais recente ATUALIZA o endereço aprendido (upsert)', async () => {
    const orderId = randomUUID();
    const novoEndereco = { ...ADDRESS, zipCode: '02000-000' };

    await handler.handle(makeOrderCreated(orderId, ADDRESS));
    await handler.handle(makeOrderCreated(orderId, novoEndereco));

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.shippingAddress).toEqual(novoEndereco);
  });
});
