import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, inventoryEvents, orderEvents, paymentEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { MailerService } from '../src/infrastructure/mailer.service.js';
import { NotificationEventHandler } from '../src/application/notification-event.handler.js';
import { clearMailhogInbox, countMailhogMessagesTo, findMailhogMessage, subjectOf } from './support/mailhog-client.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};
const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 };

function makeOrderCreated(orderId: string, customerId: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId,
      items: [ITEM],
      totalAmountCents: 4990,
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
      amountCents: 4990,
      currency: 'BRL',
      authorizationCode: 'AUTH-TEST-1',
      instrument: { gatewayToken: 'tok_test_1', cardLast4: '4242', brand: 'VISA' },
      approvedAt: new Date().toISOString(),
    },
  });
}

describe('NotificationEventHandler (integração — Postgres + Mailhog reais, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const mailer = new MailerService();
  const handler = new NotificationEventHandler(prisma, mailer);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.knownOrder.deleteMany();
    await clearMailhogInbox();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('order.created envia e-mail de boas-vindas e aprende o customerId', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();

    await handler.handle(makeOrderCreated(orderId, customerId));

    const message = await findMailhogMessage((item) => subjectOf(item).includes(orderId));
    expect(subjectOf(message)).toBe(`Recebemos seu pedido ${orderId}`);
    expect(message.To[0]).toEqual(
      expect.objectContaining({ Mailbox: `cliente-${customerId}`, Domain: 'example.com' }),
    );

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.customerId).toBe(customerId);
  });

  it('reentrega do MESMO eventId não reenvia o e-mail (idempotência)', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();
    const envelope = makeOrderCreated(orderId, customerId);

    await handler.handle(envelope);
    await handler.handle(envelope); // mesmo eventId

    const count = await countMailhogMessagesTo(`cliente-${customerId}@example.com`);
    expect(count).toBe(1);
  });

  it('eventType sem template nesta fase (stock.reserved) não envia e-mail', async () => {
    const orderId = randomUUID();
    const envelope = createEvent(inventoryEvents.stockReserved, {
      aggregateId: orderId,
      correlationId: orderId,
      producer: 'inventory-service-test@0.0.0',
      payload: {
        reservationId: randomUUID(),
        orderId,
        items: [{ sku: 'BOOK-001', quantity: 1 }],
        expiresAt: new Date(Date.now() + 3600_000).toISOString(),
        reservedAt: new Date().toISOString(),
      },
    });

    await handler.handle(envelope);

    const count = await countMailhogMessagesTo('cliente-qualquer-coisa@example.com');
    expect(count).toBe(0);
    // Mesmo sem template, o evento é registrado como processado — evita
    // reavaliar a mesma decisão de "nada a fazer" para sempre.
    const processed = await prisma.client.processedMessage.count({ where: { eventId: envelope.eventId } });
    expect(processed).toBe(1);
  });

  it('RACE CONDITION: payment.approved chega ANTES de order.created — erro retriável, e recuperação depois', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();
    const paymentEnvelope = makePaymentApproved(orderId);

    let caught: (Error & { permanent?: boolean }) | undefined;
    try {
      await handler.handle(paymentEnvelope);
    } catch (error) {
      caught = error as Error & { permanent?: boolean };
    }

    expect(caught?.message).toMatch(/KnownOrder/);
    expect(caught?.permanent).toBeUndefined();

    // markProcessed NUNCA chegou a ser chamado nesta execução — o erro subiu
    // antes disso (diferente de Shipping/Inventory, ver decisão 3 do plano).
    const processedBefore = await prisma.client.processedMessage.count({
      where: { eventId: paymentEnvelope.eventId },
    });
    expect(processedBefore).toBe(0);

    const countBeforeRecovery = await countMailhogMessagesTo(`cliente-${customerId}@example.com`);
    expect(countBeforeRecovery).toBe(0);

    // order.created chega DEPOIS — a escada de retry real reentregaria a
    // MESMA mensagem de payment.approved; aqui simulamos isso chamando o
    // handler de novo diretamente.
    await handler.handle(makeOrderCreated(orderId, customerId));
    await handler.handle(paymentEnvelope);

    const message = await findMailhogMessage((item) => subjectOf(item).includes(`Pagamento do pedido ${orderId}`));
    expect(subjectOf(message)).toBe(`Pagamento do pedido ${orderId} aprovado`);
  });
});
