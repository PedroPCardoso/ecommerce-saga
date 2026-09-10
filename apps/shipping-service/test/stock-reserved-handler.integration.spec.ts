import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, inventoryEvents, orderEvents, shippingEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';
import { StockReservedHandler } from '../src/application/stock-reserved.handler.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 2, unitPriceCents: 4990 };
const ADDRESS_OK = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};
const ADDRESS_FAIL = { ...ADDRESS_OK, zipCode: '00000-000' };

function makeOrderCreated(orderId: string, address: typeof ADDRESS_OK) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [ITEM],
      totalAmountCents: 9980,
      currency: 'BRL',
      shippingAddress: address,
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

describe('StockReservedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const orderCreatedHandler = new OrderCreatedHandler(prisma);
  const handler = new StockReservedHandler(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.shipment.deleteMany();
    await prisma.client.knownOrder.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('CEP normal: cria Shipment e publica shipment.created com o schema correto', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_OK));

    await handler.handle(makeStockReserved(orderId));

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment?.status).toBe('CREATED');
    expect(shipment?.carrier).toBe('CORREIOS');
    expect(shipment?.trackingCode).toMatch(/^BR\d{9}BR$/);
    expect(shipment?.labelUrl).toBe(`http://shipping-service.internal/labels/${shipment?.id}`);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.created');
    // O envelope publicado precisa satisfazer o schema real do contrato —
    // não só "parecer certo".
    expect(() => shippingEvents.shipmentCreated.envelope.parse(outboxRow?.payload)).not.toThrow();
  });

  it('CEP 00000-XXX: publica shipment.failed, SEM criar Shipment', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_FAIL));

    await handler.handle(makeStockReserved(orderId));

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment).toBeNull();

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.failed');
    expect(() => shippingEvents.shipmentFailed.envelope.parse(outboxRow?.payload)).not.toThrow();
    const payload = outboxRow?.payload as { payload: { failureCode: string } };
    expect(payload.payload.failureCode).toBe('ADDRESS_NOT_SERVICEABLE');
  });

  it('RACE CONDITION: stock.reserved chega ANTES do order.created — erro retriável, rollback do markProcessed, e recuperação depois', async () => {
    const orderId = randomUUID();
    const stockReservedEnvelope = makeStockReserved(orderId);

    let caught: (Error & { permanent?: boolean }) | undefined;
    try {
      await handler.handle(stockReservedEnvelope);
    } catch (error) {
      caught = error as Error & { permanent?: boolean };
    }

    expect(caught?.message).toMatch(/KnownOrder/);
    // Erro COMUM — classifyError (@ecommerce/kafka) precisa tratar isto como
    // RETRIÁVEL, nunca permanente.
    expect(caught?.permanent).toBeUndefined();

    // O ROLLBACK desfez o markProcessed junto com a transação — sem isso, o
    // retry encontraria "já processado" e desistiria sem nunca ter enviado nada.
    const processedCount = await prisma.client.processedMessage.count({
      where: { eventId: stockReservedEnvelope.eventId },
    });
    expect(processedCount).toBe(0);

    // order.created chega DEPOIS (fora de ordem, mas chega).
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_OK));

    // Reentrega da MESMA mensagem stock.reserved — simula o degrau retry-5s.
    await handler.handle(stockReservedEnvelope);

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment).not.toBeNull();
    expect(shipment?.status).toBe('CREATED');
  });

  it('reentrega do MESMO eventId (já processado com sucesso) não cria um segundo Shipment', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_OK));
    const envelope = makeStockReserved(orderId);

    await handler.handle(envelope);
    await handler.handle(envelope);

    const outboxCount = await prisma.client.outbox.count({ where: { aggregateId: orderId } });
    expect(outboxCount).toBe(1);
  });
});
