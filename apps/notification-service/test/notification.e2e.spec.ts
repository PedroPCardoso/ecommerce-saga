import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, TOPICS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { env } from '../src/env.js';
import { clearMailhogInbox, findMailhogMessage, subjectOf } from './support/mailhog-client.js';

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

describe('Notification Service — e2e (Kafka + Postgres + Mailhog reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();

    producer = new EventProducer({ brokers: env.KAFKA_BROKERS, clientId: 'notification-e2e-test-producer' });
    await producer.connect();

    // Dá tempo do consumer group 'notification-service' concluir o
    // join/rebalance antes de publicarmos — senão a mensagem pode ser
    // publicada cedo demais e nunca ser entregue a este consumidor.
    await new Promise((resolve) => setTimeout(resolve, 3000));
  });

  beforeEach(async () => {
    await clearMailhogInbox();
  });

  afterAll(async () => {
    await producer.disconnect();
    await app.close();
  });

  it('GET /health/live responde sem autenticação', async () => {
    await request(app.getHttpServer()).get('/health/live').expect(200);
  });

  it('order.created publicado de verdade em ecommerce.orders.v1 chega ao Mailhog com o assunto esperado', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();

    await producer.publish(
      TOPICS.orders,
      createEvent(orderEvents.orderCreated, {
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
      }),
    );

    const message = await findMailhogMessage((item) => subjectOf(item).includes(orderId), 15_000);
    expect(subjectOf(message)).toBe(`Recebemos seu pedido ${orderId}`);
    expect(message.To[0]).toEqual(
      expect.objectContaining({ Mailbox: `cliente-${customerId}`, Domain: 'example.com' }),
    );
  }, 20_000);
});
