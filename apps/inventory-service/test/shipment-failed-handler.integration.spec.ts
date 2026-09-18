import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, paymentEvents, shippingEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';
import { PaymentApprovedHandler } from '../src/application/payment-approved.handler.js';
import { ShipmentFailedHandler } from '../src/application/shipment-failed.handler.js';

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

function makeShipmentFailed(orderId: string) {
  return createEvent(shippingEvents.shipmentFailed, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'shipping-service-test@0.0.0',
    payload: {
      orderId,
      failureCode: 'ADDRESS_NOT_SERVICEABLE',
      reason: 'CEP fora da área de cobertura',
      failedAt: new Date().toISOString(),
    },
  });
}

describe('ShipmentFailedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const orderCreatedHandler = new OrderCreatedHandler(prisma);
  const paymentApprovedHandler = new PaymentApprovedHandler(prisma);
  const shipmentFailedHandler = new ShipmentFailedHandler(prisma);

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

  it('libera a reserva RESERVED e publica stock.released', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));
    await paymentApprovedHandler.handle(makePaymentApproved(orderId));

    await shipmentFailedHandler.handle(makeShipmentFailed(orderId));

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation?.status).toBe('RELEASED');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'stock.released' },
    });
    expect(outboxRows).toHaveLength(1);
    const payload = outboxRows[0]?.payload as {
      payload: { compensationFor: string; items: unknown };
    };
    expect(payload.payload.compensationFor).toBe('shipment.failed');
    expect(payload.payload.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);
  });

  it('reentrega do MESMO evento não libera duas vezes — idempotência', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 1 }]));
    await paymentApprovedHandler.handle(makePaymentApproved(orderId));
    const envelope = makeShipmentFailed(orderId);

    await shipmentFailedHandler.handle(envelope);
    await shipmentFailedHandler.handle(envelope);

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'stock.released' },
    });
    expect(outboxRows).toHaveLength(1);
  });

  it('reserva inexistente é erro PERMANENTE — shipment.failed só acontece depois de stock.reserved', async () => {
    const orderId = randomUUID(); // nunca reservado
    await expect(shipmentFailedHandler.handle(makeShipmentFailed(orderId))).rejects.toMatchObject({
      permanent: true,
    });
  });
});
