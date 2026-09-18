import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { SagaTimeoutSweeperService } from '../src/infrastructure/saga-timeout-sweeper.service.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

describe('SagaTimeoutSweeperService (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const sweeper = new SagaTimeoutSweeperService(prisma);

  async function createOrder(status: string, updatedAtMsAgo: number): Promise<string> {
    const orderId = randomUUID();
    await prisma.client.order.create({
      data: {
        id: orderId,
        customerId: randomUUID(),
        items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 2000 }],
        totalAmountCents: 2000,
        currency: 'BRL',
        status,
        shippingAddress: ADDRESS,
      },
    });
    // @updatedAt do Prisma sobrescreve qualquer valor passado num .update() normal —
    // só um UPDATE cru consegue "voltar no tempo" o campo para simular um pedido
    // realmente parado há muito tempo, sem esperar o tempo de verdade passar.
    const backdated = new Date(Date.now() - updatedAtMsAgo);
    // `id` é `String @id` sem `@db.Uuid` no schema (coluna Postgres é `text`,
    // não `uuid` nativo) — cast `::uuid` no parâmetro quebraria a comparação.
    await prisma.client.$executeRawUnsafe(
      `UPDATE orders SET updated_at = $1::timestamp WHERE id = $2`,
      backdated,
      orderId,
    );
    return orderId;
  }

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.outbox.deleteMany();
    await prisma.client.order.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('leva um pedido preso em PAYMENT_APPROVED há mais do que o limite para COMPENSATING e publica saga.timeout', async () => {
    const orderId = await createOrder('PAYMENT_APPROVED', 10 * 60_000); // 10 min atrás

    const swept = await sweeper.sweepOnce();

    expect(swept).toBe(1);
    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');
    expect(order.compensationReason).toBe('SAGA_TIMEOUT');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'saga.timeout' },
    });
    expect(outboxRows).toHaveLength(1);
  });

  it('NÃO varre um pedido em PAYMENT_APPROVED ainda dentro do limite', async () => {
    await createOrder('PAYMENT_APPROVED', 1_000); // 1s atrás — bem abaixo do threshold padrão (5 min)

    const swept = await sweeper.sweepOnce();

    expect(swept).toBe(0);
  });

  it('NÃO varre pedidos em outros estados (PENDING, STOCK_RESERVED, CONFIRMED, CANCELLED) mesmo se antigos', async () => {
    await createOrder('PENDING', 10 * 60_000);
    await createOrder('STOCK_RESERVED', 10 * 60_000);
    await createOrder('CONFIRMED', 10 * 60_000);
    await createOrder('CANCELLED', 10 * 60_000);

    const swept = await sweeper.sweepOnce();

    expect(swept).toBe(0);
  });

  it('rodar sweepOnce duas vezes seguidas não publica saga.timeout duas vezes para o mesmo pedido', async () => {
    await createOrder('PAYMENT_APPROVED', 10 * 60_000);

    await sweeper.sweepOnce();
    const secondPass = await sweeper.sweepOnce(); // já está COMPENSATING, não é mais PAYMENT_APPROVED

    expect(secondPass).toBe(0);
    const outboxRows = await prisma.client.outbox.findMany({ where: { eventType: 'saga.timeout' } });
    expect(outboxRows).toHaveLength(1);
  });
});
