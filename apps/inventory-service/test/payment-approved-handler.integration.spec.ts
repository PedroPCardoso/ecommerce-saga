import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, paymentEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';
import { PaymentApprovedHandler } from '../src/application/payment-approved.handler.js';

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

describe('PaymentApprovedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const orderCreatedHandler = new OrderCreatedHandler(prisma);
  const paymentApprovedHandler = new PaymentApprovedHandler(prisma);

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

  it('reserva estoque e publica stock.reserved quando todos os SKUs estão disponíveis', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));

    await paymentApprovedHandler.handle(makePaymentApproved(orderId));

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation?.status).toBe('RESERVED');
    expect(reservation?.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);
    expect(reservation?.expiresAt.getTime()).toBeGreaterThan(reservation!.reservedAt.getTime());

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('stock.reserved');
  });

  it('publica stock.unavailable e NÃO reserva nada (tudo ou nada) quando algum SKU começa com OUT-', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(
      makeOrderCreated(orderId, [
        { sku: 'BOOK-001', quantity: 1 },
        { sku: 'OUT-999', quantity: 3 },
      ]),
    );

    await paymentApprovedHandler.handle(makePaymentApproved(orderId));

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation).toBeNull(); // nem o item disponível (BOOK-001) foi reservado

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('stock.unavailable');

    const payload = outboxRows[0]?.payload as { payload: { unavailableItems: unknown } };
    expect(payload.payload.unavailableItems).toEqual([{ sku: 'OUT-999', requested: 3, available: 0 }]);
  });

  it('reentrega do MESMO evento (mesmo eventId) não reserva duas vezes — idempotência', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 1 }]));
    const paymentEnvelope = makePaymentApproved(orderId);

    await paymentApprovedHandler.handle(paymentEnvelope);
    await paymentApprovedHandler.handle(paymentEnvelope);

    const count = await prisma.client.stockReservation.count({ where: { orderId } });
    expect(count).toBe(1);
  });

  it('RACE CONDITION — payment.approved chega ANTES de order.created: falha retriável sem deixar rastro; ao chegar order.created, o retry reserva normalmente', async () => {
    const orderId = randomUUID();
    const paymentEnvelope = makePaymentApproved(orderId);

    // 1) payment.approved processado primeiro — KnownOrder ainda não existe.
    await expect(paymentApprovedHandler.handle(paymentEnvelope)).rejects.toThrow(/KnownOrder/);

    // 2) A transação inteira foi desfeita — INCLUSIVE o markProcessed. Sem este rollback,
    //    a "retentativa" abaixo encontraria o par (eventId, consumerGroup) já marcado e
    //    devolveria silenciosamente sem nunca ter reservado nada.
    const processed = await prisma.client.processedMessage.findUnique({
      where: {
        eventId_consumerGroup: { eventId: paymentEnvelope.eventId, consumerGroup: 'inventory-service' },
      },
    });
    expect(processed).toBeNull();
    const reservationBefore = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservationBefore).toBeNull();

    // 3) order.created finalmente chega (fora de ordem entre tópicos diferentes).
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));

    // 4) "Retentativa": mesmo handler, mesmo evento — é exatamente o que a escada de
    //    retry (5s/1m/10m) do @ecommerce/kafka faria ao redeliverar a mensagem.
    await paymentApprovedHandler.handle(paymentEnvelope);

    const reservationAfter = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservationAfter?.status).toBe('RESERVED');
    expect(reservationAfter?.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);
  });
});
