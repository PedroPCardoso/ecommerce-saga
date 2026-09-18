import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, inventoryEvents, paymentEvents, shippingEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderProjectionHandler } from '../src/application/order-projection.handler.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

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

function makePaymentFailed(orderId: string) {
  return createEvent(paymentEvents.paymentFailed, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'payment-service-test@0.0.0',
    payload: {
      paymentId: randomUUID(),
      orderId,
      amountCents: 2000,
      currency: 'BRL',
      failureCode: 'CARD_DECLINED',
      reason: 'Cartão recusado pelo emissor',
      failedAt: new Date().toISOString(),
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
      items: [{ sku: 'BOOK-001', quantity: 2 }],
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
      reservedAt: new Date().toISOString(),
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
      unavailableItems: [{ sku: 'OUT-999', requested: 3, available: 0 }],
      checkedAt: new Date().toISOString(),
    },
  });
}

function makeShipmentCreated(orderId: string) {
  return createEvent(shippingEvents.shipmentCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'shipping-service-test@0.0.0',
    payload: {
      shipmentId: randomUUID(),
      orderId,
      carrier: 'CORREIOS',
      trackingCode: 'BR123456789',
      labelUrl: 'https://labels.example.com/BR123456789',
      estimatedDeliveryAt: new Date(Date.now() + 86_400_000).toISOString(),
      createdAt: new Date().toISOString(),
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

function makePaymentRefunded(orderId: string) {
  return createEvent(paymentEvents.paymentRefunded, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'payment-service-test@0.0.0',
    payload: {
      paymentId: randomUUID(),
      orderId,
      refundId: randomUUID(),
      amountCents: 2000,
      currency: 'BRL',
      compensationFor: 'stock.unavailable',
      refundedAt: new Date().toISOString(),
    },
  });
}

function makeStockReleased(orderId: string) {
  return createEvent(inventoryEvents.stockReleased, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'inventory-service-test@0.0.0',
    payload: {
      reservationId: randomUUID(),
      orderId,
      items: [{ sku: 'BOOK-001', quantity: 2 }],
      compensationFor: 'shipment.failed',
      releasedAt: new Date().toISOString(),
    },
  });
}

describe('OrderProjectionHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const handler = new OrderProjectionHandler(prisma);

  async function createTestOrder(): Promise<string> {
    const orderId = randomUUID();
    await prisma.client.order.create({
      data: {
        id: orderId,
        customerId: randomUUID(),
        items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 2, unitPriceCents: 1000 }],
        totalAmountCents: 2000,
        currency: 'BRL',
        status: 'PENDING',
        shippingAddress: ADDRESS,
      },
    });
    return orderId;
  }

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.idempotencyKey.deleteMany();
    await prisma.client.order.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('caminho feliz: payment.approved -> stock.reserved -> shipment.created leva o pedido a CONFIRMED e publica order.confirmed', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    let order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('PAYMENT_APPROVED');

    await handler.handle(makeStockReserved(orderId));
    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('STOCK_RESERVED');

    await handler.handle(makeShipmentCreated(orderId));
    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('CONFIRMED');

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('order.confirmed');
    const payload = outboxRows[0]?.payload as { payload: { customerId: string } };
    expect(payload.payload.customerId).toBe(order.customerId);
  });

  it('payment.failed leva o pedido de PENDING direto a CANCELLED e publica order.cancelled com compensationsApplied vazio', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentFailed(orderId));

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('CANCELLED');

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('order.cancelled');
    const payload = outboxRows[0]?.payload as {
      payload: { reason: string; compensationsApplied: string[] };
    };
    expect(payload.payload.reason).toBe('PAYMENT_FAILED');
    expect(payload.payload.compensationsApplied).toEqual([]);
  });

  it('stock.unavailable leva o pedido a COMPENSATING (não fecha sozinho — I4 ainda não implementado) e não publica order.cancelled', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockUnavailable(orderId));

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(0);
  });

  it('shipment.failed leva o pedido a COMPENSATING (não fecha sozinho — I4 ainda não implementado)', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockReserved(orderId));
    await handler.handle(makeShipmentFailed(orderId));

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');
  });

  it('reentrega do MESMO evento (mesmo eventId) não reprocessa — idempotência', async () => {
    const orderId = await createTestOrder();
    const envelope = makePaymentApproved(orderId);

    await handler.handle(envelope);
    await handler.handle(envelope);

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('PAYMENT_APPROVED');
  });

  it('RACE CONDITION — stock.reserved chega ANTES de payment.approved: falha retriável sem deixar rastro; ao chegar payment.approved, o retry aplica normalmente', async () => {
    const orderId = await createTestOrder();
    const stockEnvelope = makeStockReserved(orderId);

    // 1) stock.reserved processado primeiro — o pedido ainda está em PENDING, não em
    //    PAYMENT_APPROVED (estado que este evento exige).
    await expect(handler.handle(stockEnvelope)).rejects.toThrow(/ainda não chegou/);

    // 2) A transação inteira foi desfeita — INCLUSIVE o markProcessed. Sem este rollback,
    //    a "retentativa" abaixo encontraria o par (eventId, consumerGroup) já marcado e
    //    devolveria silenciosamente sem nunca ter aplicado a transição.
    const processed = await prisma.client.processedMessage.findUnique({
      where: {
        eventId_consumerGroup: {
          eventId: stockEnvelope.eventId,
          consumerGroup: 'order-projection',
        },
      },
    });
    expect(processed).toBeNull();
    const orderBefore = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(orderBefore.status).toBe('PENDING');

    // 3) payment.approved finalmente chega (fora de ordem entre tópicos diferentes).
    await handler.handle(makePaymentApproved(orderId));

    // 4) "Retentativa": mesmo handler, mesmo evento — é exatamente o que a escada de
    //    retry (5s/1m/10m) do @ecommerce/kafka faria ao redeliverar a mensagem.
    await handler.handle(stockEnvelope);

    const orderAfter = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(orderAfter.status).toBe('STOCK_RESERVED');
  });

  it('evento OBSOLETO (stale) — payment.approved reentregue com eventId novo depois de o pedido já estar em STOCK_RESERVED — é ignorado com WARN, sem regredir nem lançar', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockReserved(orderId));

    // eventId novo (não é reentrega do mesmo evento — markProcessed não pega isto),
    // mas o pedido já passou de PAYMENT_APPROVED: a REGRA DE OURO precisa rejeitar
    // sem lançar, porque não há nada "anterior" para esperar aqui.
    await handler.handle(makePaymentApproved(orderId));

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('STOCK_RESERVED');
  });

  it('stock.unavailable + payment.refunded fecha o pedido em CANCELLED com compensationsApplied=[PAYMENT_REFUNDED] e publica order.cancelled', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockUnavailable(orderId));

    let order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');
    expect(order.compensationReason).toBe('STOCK_UNAVAILABLE');

    await handler.handle(makePaymentRefunded(orderId));

    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('CANCELLED');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'order.cancelled' },
    });
    expect(outboxRows).toHaveLength(1);
    const payload = outboxRows[0]?.payload as {
      payload: { reason: string; compensationsApplied: string[] };
    };
    expect(payload.payload.reason).toBe('STOCK_UNAVAILABLE');
    expect(payload.payload.compensationsApplied).toEqual(['PAYMENT_REFUNDED']);
  });

  it('shipment.failed exige payment.refunded E stock.released — só fecha quando as duas chegarem, em qualquer ordem', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockReserved(orderId));
    await handler.handle(makeShipmentFailed(orderId));

    let order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');
    expect(order.compensationReason).toBe('SHIPMENT_FAILED');

    // stock.released chega PRIMEIRO — não fecha ainda, falta payment.refunded.
    await handler.handle(makeStockReleased(orderId));
    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');

    // payment.refunded chega DEPOIS — agora sim as duas chegaram, fecha.
    await handler.handle(makePaymentRefunded(orderId));
    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('CANCELLED');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'order.cancelled' },
    });
    const payload = outboxRows[0]?.payload as { payload: { compensationsApplied: string[] } };
    expect(payload.payload.compensationsApplied.sort()).toEqual(['PAYMENT_REFUNDED', 'STOCK_RELEASED']);
  });

  it('RACE CONDITION — payment.refunded chega ANTES de o pedido entrar em COMPENSATING: falha retriável sem deixar rastro', async () => {
    const orderId = await createTestOrder();
    const refundedEnvelope = makePaymentRefunded(orderId);

    // Pedido ainda em PENDING — nem payment.approved chegou.
    await expect(handler.handle(refundedEnvelope)).rejects.toThrow();

    const processed = await prisma.client.processedMessage.findUnique({
      where: {
        eventId_consumerGroup: {
          eventId: refundedEnvelope.eventId,
          consumerGroup: 'order-projection',
        },
      },
    });
    expect(processed).toBeNull();

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('PENDING'); // não regrediu nem avançou
  });

  it('reentrega do MESMO evento de compensação não conta a compensação duas vezes', async () => {
    const orderId = await createTestOrder();
    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockReserved(orderId));
    await handler.handle(makeShipmentFailed(orderId));
    const releasedEnvelope = makeStockReleased(orderId);

    await handler.handle(releasedEnvelope);
    await handler.handle(releasedEnvelope);

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    const received = order.compensationsReceived as string[];
    expect(received).toEqual(['STOCK_RELEASED']); // não duplicou
  });
});
