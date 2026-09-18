import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};
const ITEMS = [{ sku: 'BOOK-001', quantity: 1 }];

async function waitForTerminalStatus(prisma: PrismaService, orchestratedOrderId: string, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const order = await prisma.client.orchestratedOrder.findUnique({ where: { id: orchestratedOrderId } });
    if (order && (order.status === 'CONFIRMED' || order.status === 'CANCELLED')) return order;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`OrchestratedOrder ${orchestratedOrderId} não chegou a um status terminal em ${timeoutMs}ms`);
}

describe('Saga Orchestrator Service — e2e (Kafka + Postgres reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let prisma: PrismaService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);

    // Dá tempo dos 4 consumer groups (orquestrador + 3 executores) concluírem o
    // join/rebalance antes do primeiro POST — mesmo cuidado que os demais e2e
    // deste monorepo (ver apps/notification-service/test/notification.e2e.spec.ts).
    await new Promise((resolve) => setTimeout(resolve, 3000));
  });

  afterAll(async () => {
    await app.close();
  });

  it('GET /health/live responde sem autenticação', async () => {
    await request(app.getHttpServer()).get('/health/live').expect(200);
  });

  it('caminho feliz: pedido percorre payment -> stock -> shipment e chega a CONFIRMED', async () => {
    const response = await request(app.getHttpServer())
      .post('/orchestrated-orders')
      .send({ amountCents: 5_000, currency: 'BRL', items: ITEMS, address: ADDRESS })
      .expect(201);

    const orchestratedOrderId = response.body.orchestratedOrderId as string;
    expect(orchestratedOrderId).toBeTruthy();

    const order = await waitForTerminalStatus(prisma, orchestratedOrderId);
    expect(order.status).toBe('CONFIRMED');
  }, 25_000);

  it('valor terminando em .13 é recusado pelo executor de pagamento e o pedido cancela', async () => {
    const response = await request(app.getHttpServer())
      .post('/orchestrated-orders')
      .send({ amountCents: 5_013, currency: 'BRL', items: ITEMS, address: ADDRESS })
      .expect(201);

    const orchestratedOrderId = response.body.orchestratedOrderId as string;

    const order = await waitForTerminalStatus(prisma, orchestratedOrderId);
    expect(order.status).toBe('CANCELLED');
  }, 25_000);

  it('SKU começando com OUT- é recusado pelo executor de estoque e o pedido cancela após passar por AWAITING_STOCK', async () => {
    const response = await request(app.getHttpServer())
      .post('/orchestrated-orders')
      .send({
        amountCents: 5_000,
        currency: 'BRL',
        items: [{ sku: 'OUT-001', quantity: 1 }],
        address: ADDRESS,
      })
      .expect(201);

    const orchestratedOrderId = response.body.orchestratedOrderId as string;

    const order = await waitForTerminalStatus(prisma, orchestratedOrderId);
    expect(order.status).toBe('CANCELLED');
  }, 25_000);

  it('CEP começando com 00000 é recusado pelo executor de envio e o pedido cancela após reservar estoque', async () => {
    const response = await request(app.getHttpServer())
      .post('/orchestrated-orders')
      .send({
        amountCents: 5_000,
        currency: 'BRL',
        items: ITEMS,
        address: { ...ADDRESS, zipCode: '00000-000' },
      })
      .expect(201);

    const orchestratedOrderId = response.body.orchestratedOrderId as string;

    const order = await waitForTerminalStatus(prisma, orchestratedOrderId);
    expect(order.status).toBe('CANCELLED');
  }, 25_000);
});
