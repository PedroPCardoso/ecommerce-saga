import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, inventoryEvents, orderEvents, shippingEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { AuthorizePaymentUseCase } from '../src/application/authorize-payment.use-case.js';
import { RefundPaymentUseCase } from '../src/application/refund-payment.use-case.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, totalAmountCents: number) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
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

function makeStockUnavailable(orderId: string) {
  return createEvent(inventoryEvents.stockUnavailable, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'inventory-service-test@0.0.0',
    payload: {
      orderId,
      unavailableItems: [{ sku: 'OUT-999', requested: 1, available: 0 }],
      checkedAt: new Date().toISOString(),
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

describe('RefundPaymentUseCase (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const authorizePayment = new AuthorizePaymentUseCase(prisma);
  const refundPayment = new RefundPaymentUseCase(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.payment.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('stock.unavailable: estorna o pagamento AUTHORIZED e publica payment.refunded', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 2000));

    await refundPayment.execute(makeStockUnavailable(orderId));

    const payment = await prisma.client.payment.findUnique({ where: { orderId } });
    expect(payment?.status).toBe('REFUNDED');

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(2); // payment.approved (da autorização) + payment.refunded
    const refunded = outboxRows.find((r) => r.eventType === 'payment.refunded');
    expect(refunded).toBeDefined();
    const payload = refunded?.payload as { payload: { compensationFor: string; amountCents: number } };
    expect(payload.payload.compensationFor).toBe('stock.unavailable');
    expect(payload.payload.amountCents).toBe(2000);
  });

  it('shipment.failed: estorna o pagamento AUTHORIZED e publica payment.refunded com compensationFor correto', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 3000));

    await refundPayment.execute(makeShipmentFailed(orderId));

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'payment.refunded' },
    });
    const payload = outboxRows[0]?.payload as { payload: { compensationFor: string } };
    expect(payload.payload.compensationFor).toBe('shipment.failed');
  });

  it('reentrega do MESMO evento não estorna duas vezes — idempotência', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 2000));
    const envelope = makeStockUnavailable(orderId);

    await refundPayment.execute(envelope);
    await refundPayment.execute(envelope);

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'payment.refunded' },
    });
    expect(outboxRows).toHaveLength(1);
  });

  it('pagamento já REFUNDED (defesa extra contra corrida entre stock.unavailable/shipment.failed) não estorna de novo', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 2000));
    await refundPayment.execute(makeStockUnavailable(orderId));

    // Segundo evento de compensação, eventId DIFERENTE — markProcessed não pega isto.
    await refundPayment.execute(makeShipmentFailed(orderId));

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'payment.refunded' },
    });
    expect(outboxRows).toHaveLength(1); // continua só o primeiro estorno
  });

  it('Payment inexistente para o orderId é erro PERMANENTE — a cadeia causal garante que o Payment já existe', async () => {
    const orderId = randomUUID(); // nunca autorizado
    await expect(refundPayment.execute(makeStockUnavailable(orderId))).rejects.toMatchObject({
      permanent: true,
    });
  });
});
