import { randomUUID } from 'node:crypto';
import { Kafka, logLevel, type Admin } from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createEvent,
  deadLetterTopic,
  MAX_RETRY_ATTEMPTS,
  orderEvents,
  retryTopic,
  type ConsumerGroup,
} from '@ecommerce/contracts';
import { EventProducer } from '../src/producer.js';
import { KafkaConsumerRuntime, type MessageContext } from '../src/consumer-runtime.js';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
const SOURCE_TOPIC = 'lab.kcr.pedidos';
const SOURCE_TOPIC_B = 'lab.kcr.pagamentos';
const GROUP = 'lab-kcr-group' as ConsumerGroup;
const GROUP_MULTI = 'lab-kcr-multi-group' as ConsumerGroup;

async function waitUntil(predicate: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('Timeout esperando condição');
}

function makeEnvelope(orderId: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: `corr-${orderId}`,
    producer: 'kcr-test@0.0.0',
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

/** Tópicos que um runtime precisa para um par (sourceTopic, group): origem + escada de retry + DLT. */
function topicsFor(sourceTopic: string, group: ConsumerGroup): string[] {
  return [
    sourceTopic,
    ...Array.from({ length: MAX_RETRY_ATTEMPTS }, (_, i) => retryTopic(sourceTopic, group, i)),
    deadLetterTopic(sourceTopic, group),
  ];
}

async function ensureTopics(admin: Admin, ...pairs: Array<[string, ConsumerGroup]>): Promise<void> {
  const topics = pairs.flatMap(([sourceTopic, group]) => topicsFor(sourceTopic, group));
  const existing = new Set(await admin.listTopics());
  const missing = [...new Set(topics)].filter((t) => !existing.has(t));
  if (missing.length > 0) {
    await admin.createTopics({
      waitForLeaders: true,
      topics: missing.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 1 })),
    });
  }
}

describe('KafkaConsumerRuntime (integração — Kafka real, requer pnpm infra:up)', () => {
  let admin: Admin;
  let producer: EventProducer;

  beforeAll(async () => {
    const kafka = new Kafka({ clientId: 'kcr-test-admin', brokers: BROKERS, logLevel: logLevel.NOTHING });
    admin = kafka.admin();
    await admin.connect();

    await ensureTopics(
      admin,
      [SOURCE_TOPIC, GROUP],
      [SOURCE_TOPIC, GROUP_MULTI],
      [SOURCE_TOPIC_B, GROUP_MULTI],
    );

    producer = new EventProducer({ brokers: BROKERS, clientId: 'kcr-test-producer' });
    await producer.connect();
  });

  afterAll(async () => {
    await producer.disconnect();
    const leftover = (await admin.listTopics()).filter((t) => t.startsWith('lab.kcr.'));
    if (leftover.length > 0) await admin.deleteTopics({ topics: leftover, timeout: 15_000 });
    await admin.disconnect();
  });

  it('processa com sucesso e não desvia para retry', async () => {
    const orderId = randomUUID();
    const seen: MessageContext[] = [];
    const runtime = new KafkaConsumerRuntime({
      brokers: BROKERS,
      groupId: GROUP,
      sourceTopics: [SOURCE_TOPIC],
      producer,
      handler: async (ctx) => {
        seen.push(ctx);
      },
    });
    await runtime.start();

    await producer.publish(SOURCE_TOPIC, makeEnvelope(orderId));
    await waitUntil(() => seen.some((ctx) => ctx.envelope.aggregateId === orderId));
    await runtime.stop();

    expect(seen.some((ctx) => ctx.envelope.aggregateId === orderId)).toBe(true);
  }, 20_000);

  it('erro retriável sobe a escada e se recupera no 2º degrau (retry-5s)', async () => {
    const orderId = randomUUID();
    let attempts = 0;
    const runtime = new KafkaConsumerRuntime({
      brokers: BROKERS,
      groupId: GROUP,
      sourceTopics: [SOURCE_TOPIC],
      producer,
      handler: async (ctx) => {
        if (ctx.envelope.aggregateId !== orderId) return;
        attempts += 1;
        if (attempts < 2) throw new Error('ETIMEDOUT ao chamar serviço externo');
      },
    });
    await runtime.start();

    await producer.publish(SOURCE_TOPIC, makeEnvelope(orderId));
    await waitUntil(() => attempts >= 2, 15_000);
    await runtime.stop();

    expect(attempts).toBe(2);
  }, 20_000);

  it('com múltiplos sourceTopics, o retry-5s se recupera para AMBOS os tópicos (regressão do bug de groupId compartilhado)', async () => {
    const orderIdA = randomUUID();
    const orderIdB = randomUUID();
    const attempts: Record<string, number> = { [orderIdA]: 0, [orderIdB]: 0 };
    const runtime = new KafkaConsumerRuntime({
      brokers: BROKERS,
      groupId: GROUP_MULTI,
      sourceTopics: [SOURCE_TOPIC, SOURCE_TOPIC_B],
      producer,
      handler: async (ctx) => {
        const id = ctx.envelope.aggregateId;
        if (id !== orderIdA && id !== orderIdB) return;
        attempts[id] += 1;
        if (attempts[id] < 2) throw new Error('ETIMEDOUT ao chamar serviço externo');
      },
    });
    await runtime.start();

    await producer.publish(SOURCE_TOPIC, makeEnvelope(orderIdA));
    await producer.publish(SOURCE_TOPIC_B, makeEnvelope(orderIdB));
    await waitUntil(() => attempts[orderIdA] >= 2 && attempts[orderIdB] >= 2, 15_000);
    await runtime.stop();

    // Antes do fix, os N consumidores de retry por rung compartilhavam o mesmo groupId
    // (`${groupId}-${suffix}`) e competiam pelas partições de tópicos DIFERENTES: só um
    // dos dois tópicos de origem tinha seu retry-5s efetivamente consumido, o outro
    // travava aqui em `attempts < 2` até o timeout.
    expect(attempts[orderIdA]).toBe(2);
    expect(attempts[orderIdB]).toBe(2);
  }, 20_000);

  it('erro permanente vai direto para a DLT, sem passar pela escada', async () => {
    const orderId = randomUUID();
    let handlerCalls = 0;
    const runtime = new KafkaConsumerRuntime({
      brokers: BROKERS,
      groupId: GROUP,
      sourceTopics: [SOURCE_TOPIC],
      producer,
      handler: async (ctx) => {
        if (ctx.envelope.aggregateId !== orderId) return;
        handlerCalls += 1;
        const error = new Error('regra de negócio violada') as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      },
    });
    await runtime.start();
    await producer.publish(SOURCE_TOPIC, makeEnvelope(orderId));

    const kafka = new Kafka({ clientId: 'kcr-test-dlt-reader', brokers: BROKERS, logLevel: logLevel.NOTHING });
    const reader = kafka.consumer({ groupId: `lab-kcr-dlt-reader-${orderId}` });
    await reader.connect();
    await reader.subscribe({ topic: deadLetterTopic(SOURCE_TOPIC, GROUP), fromBeginning: true });

    const found = await new Promise<boolean>((resolve) => {
      const timeout = setTimeout(() => resolve(false), 10_000);
      reader.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          const value = JSON.parse(message.value!.toString());
          if (value.aggregateId === orderId) {
            clearTimeout(timeout);
            resolve(true);
          }
        },
      });
    });
    await reader.disconnect();
    await runtime.stop();

    expect(handlerCalls).toBe(1);
    expect(found).toBe(true);
  }, 20_000);
});
