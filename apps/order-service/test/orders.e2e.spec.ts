import { randomUUID } from 'node:crypto';
import jwt from 'jsonwebtoken';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { env } from '../src/env.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 5000 };
const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function tokenFor(customerId: string): string {
  return jwt.sign({ sub: customerId }, env.JWT_SECRET, { issuer: env.JWT_ISSUER, expiresIn: '15m' });
}

describe('Order Service — e2e (requer pnpm infra:up)', () => {
  let app: INestApplication;
  let prisma: PrismaService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);
  });

  beforeEach(async () => {
    await prisma.client.idempotencyKey.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.order.deleteMany();
  });

  afterAll(async () => {
    await app.close();
  });

  it('rejeita POST /orders sem Authorization', async () => {
    await request(app.getHttpServer())
      .post('/orders')
      .set('Idempotency-Key', randomUUID())
      .send({ items: [ITEM], currency: 'BRL', shippingAddress: ADDRESS })
      .expect(401);
  });

  it('rejeita POST /orders sem Idempotency-Key', async () => {
    const token = tokenFor(randomUUID());
    await request(app.getHttpServer())
      .post('/orders')
      .set('Authorization', `Bearer ${token}`)
      .send({ items: [ITEM], currency: 'BRL', shippingAddress: ADDRESS })
      .expect(400);
  });

  it('cria o pedido, calcula o total no servidor e ignora customerId do body', async () => {
    const customerId = randomUUID();
    const bodyComCustomerIdFalso = {
      customerId: 'atacante-tentando-passar-outro-id',
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    };

    const res = await request(app.getHttpServer())
      .post('/orders')
      .set('Authorization', `Bearer ${tokenFor(customerId)}`)
      .set('Idempotency-Key', randomUUID())
      .send(bodyComCustomerIdFalso)
      .expect(201);

    expect(res.body.status).toBe('PENDING');
    expect(res.body.orderId).toBeDefined();

    const order = await prisma.client.order.findUnique({ where: { id: res.body.orderId } });
    expect(order?.customerId).toBe(customerId); // veio do JWT, não do body
    expect(order?.totalAmountCents).toBe(5000);
  });

  it('Idempotency-Key repetida devolve o mesmo pedido, com 201', async () => {
    const customerId = randomUUID();
    const idempotencyKey = randomUUID();
    const token = tokenFor(customerId);

    const first = await request(app.getHttpServer())
      .post('/orders')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', idempotencyKey)
      .send({ items: [ITEM], currency: 'BRL', shippingAddress: ADDRESS })
      .expect(201);

    const second = await request(app.getHttpServer())
      .post('/orders')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', idempotencyKey)
      .send({ items: [ITEM], currency: 'BRL', shippingAddress: ADDRESS })
      .expect(201);

    expect(second.body.orderId).toBe(first.body.orderId);
    const count = await prisma.client.order.count({ where: { customerId } });
    expect(count).toBe(1);
  });

  it('GET /orders/:id devolve o pedido para o dono', async () => {
    const customerId = randomUUID();
    const token = tokenFor(customerId);
    const created = await request(app.getHttpServer())
      .post('/orders')
      .set('Authorization', `Bearer ${token}`)
      .set('Idempotency-Key', randomUUID())
      .send({ items: [ITEM], currency: 'BRL', shippingAddress: ADDRESS });

    const res = await request(app.getHttpServer())
      .get(`/orders/${created.body.orderId}`)
      .set('Authorization', `Bearer ${token}`)
      .expect(200);

    expect(res.body.orderId).toBe(created.body.orderId);
  });

  it('GET /orders/:id devolve 404 para quem não é dono — nunca 403 (evita enumeração)', async () => {
    const owner = randomUUID();
    const stranger = randomUUID();
    const created = await request(app.getHttpServer())
      .post('/orders')
      .set('Authorization', `Bearer ${tokenFor(owner)}`)
      .set('Idempotency-Key', randomUUID())
      .send({ items: [ITEM], currency: 'BRL', shippingAddress: ADDRESS });

    await request(app.getHttpServer())
      .get(`/orders/${created.body.orderId}`)
      .set('Authorization', `Bearer ${tokenFor(stranger)}`)
      .expect(404);
  });

  it('GET /health/live responde sem autenticação', async () => {
    await request(app.getHttpServer()).get('/health/live').expect(200);
  });
});
