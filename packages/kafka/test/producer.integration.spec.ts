import { randomUUID } from 'node:crypto';
import { Kafka, logLevel, type Admin } from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createEvent, orderEvents } from '@ecommerce/contracts';
import { EventProducer } from '../src/index.js';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
const TOPIC = 'lab.producer.pedidos';

function makeEnvelope(orderId: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: 'corr-1',
    producer: 'test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 1000 }],
      totalAmountCents: 1000,
      currency: 'BRL',
      shippingAddress: {
        street: 'Rua Teste',
        number: '1',
        district: 'Centro',
        city: 'SP',
        state: 'SP',
        zipCode: '01000-000',
        country: 'BR',
      },
    },
  });
}

describe('EventProducer (integração — Kafka real, requer pnpm infra:up)', () => {
  let admin: Admin;
  let producer: EventProducer;

  beforeAll(async () => {
    const kafka = new Kafka({
      clientId: 'producer-test-admin',
      brokers: BROKERS,
      logLevel: logLevel.NOTHING,
    });
    admin = kafka.admin();
    await admin.connect();
    const existing = new Set(await admin.listTopics());
    if (!existing.has(TOPIC)) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }],
      });
    }
    producer = new EventProducer({ brokers: BROKERS, clientId: 'producer-test' });
    await producer.connect();
  });

  afterAll(async () => {
    await producer.disconnect();
    await admin.deleteTopics({ topics: [TOPIC], timeout: 15_000 });
    await admin.disconnect();
  });

  it('publica usando o aggregateId como chave — nunca escolhida pelo chamador', async () => {
    const orderId = randomUUID();
    const envelope = makeEnvelope(orderId);

    await producer.publish(TOPIC, envelope);

    const kafka = new Kafka({
      clientId: 'producer-test-reader',
      brokers: BROKERS,
      logLevel: logLevel.NOTHING,
    });
    const consumer = kafka.consumer({ groupId: `lab-producer-reader-${orderId}` });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

    const message = await new Promise<{ key: string | null; value: string }>((resolve) => {
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ message: m }) => {
          resolve({ key: m.key?.toString() ?? null, value: m.value!.toString() });
        },
      });
    });
    await consumer.disconnect();

    expect(message.key).toBe(orderId);
    expect(JSON.parse(message.value).aggregateId).toBe(orderId);
  }, 15_000);
});
