import { randomUUID } from 'node:crypto';
import { Kafka, logLevel, type Admin } from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';
import { MAX_RETRY_ATTEMPTS, createEvent, orderEvents, retryTopic, type ConsumerGroup } from '@ecommerce/contracts';
import { EventProducer } from '../src/producer.js';
import { KafkaConsumerRuntime, type MessageContext } from '../src/consumer-runtime.js';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
const TOPIC = `lab.replay.pedidos`;
const EPHEMERAL_GROUPS = ['replay-test-live', 'replay-test-replay'] as const;

/**
 * `KafkaConsumerRuntime.start()` assina não só `sourceTopics` como também
 * cada degrau da escada de retry por grupo (`retryTopic`) — e o broker roda
 * com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (deploy/docker/docker-compose.yml),
 * então esses tópicos precisam existir ANTES do `consumer.subscribe`, senão
 * a metadata request falha com "This server does not host this topic-partition".
 * Os grupos efêmeros deste teste não fazem parte de `CONSUMER_GROUPS`/
 * `SUBSCRIPTIONS`, então `pnpm topics:create` nunca criou esses tópicos —
 * o teste precisa criar os seus próprios.
 */
function retryTopicsFor(groupId: string): string[] {
  const topics: string[] = [];
  for (let attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt += 1) {
    topics.push(retryTopic(TOPIC, groupId as ConsumerGroup, attempt));
  }
  return topics;
}

/**
 * `KafkaConsumerRuntime` não expõe `fromBeginning` nas suas opções — ele
 * assina o tópico e deixa o kafkajs decidir o offset inicial quando o grupo
 * não tem nada commitado, e o default do kafkajs para isso é `latest`, não
 * `earliest` (fromBeginning:false é o default do `consumer.subscribe`). Um
 * grupo genuinamente NOVO, portanto, ignoraria silenciosamente tudo que já
 * foi publicado antes dele existir — o oposto de "replay". Setar o offset
 * commitado para 0 ANTES do `start()` (permitido pelo kafkajs só com o grupo
 * sem membros ativos, que é o nosso caso aqui) reproduz `fromBeginning: true`
 * sem precisar mudar a runtime de produção.
 */
async function forceOffsetZero(admin: Admin, groupId: string): Promise<void> {
  await admin.setOffsets({ groupId, topic: TOPIC, partitions: [{ partition: 0, offset: '0' }] });
}

/**
 * Simula o efeito de negócio de um handler real: soma 1 por orderId numa
 * tabela, protegido por uma constraint UNIQUE(order_id, event_id) — o
 * equivalente mínimo de `processed_messages`, sem puxar Postgres real de um
 * serviço específico (este pacote não tem schema de domínio próprio).
 */
async function setupTable(pool: Pool): Promise<void> {
  await pool.query('DROP TABLE IF EXISTS replay_test_effects');
  await pool.query(`
    CREATE TABLE replay_test_effects (
      order_id text NOT NULL,
      event_id uuid NOT NULL,
      PRIMARY KEY (order_id, event_id)
    )
  `);
}

describe('Replay (integração — Kafka + Postgres reais, requer pnpm infra:up)', () => {
  const kafka = new Kafka({ clientId: 'replay-test-setup', brokers: BROKERS, logLevel: logLevel.ERROR });
  const admin = kafka.admin();
  const pool = new Pool({ connectionString: process.env.ORDER_DATABASE_URL });

  const allRetryTopics = EPHEMERAL_GROUPS.flatMap((groupId) => retryTopicsFor(groupId));

  beforeAll(async () => {
    await admin.connect();
    await admin.createTopics({
      topics: [TOPIC, ...allRetryTopics].map((topic) => ({ topic, numPartitions: 1 })),
      waitForLeaders: true,
    });
    await setupTable(pool);
  });

  afterAll(async () => {
    await admin.deleteTopics({ topics: [TOPIC, ...allRetryTopics] }).catch(() => {});
    await admin.disconnect();
    await pool.end();
  });

  it('um consumer group NOVO, lendo do offset zero, aplica o efeito para cada orderId exatamente uma vez — mesmo estado final de um consumer group que processou ao vivo', async () => {
    const producer = new EventProducer({ brokers: BROKERS, clientId: 'replay-test-producer' });
    await producer.connect();

    const orderIds = Array.from({ length: 5 }, () => randomUUID());
    for (const orderId of orderIds) {
      // `KafkaConsumerRuntime.parse()` valida o eventType contra o registro global de
      // eventos de @ecommerce/contracts (parseEvent) — um eventType inventado (ex.:
      // "test.replay") não existe nesse registro e cai direto na DLT em vez de chegar
      // ao handler. Por isso usamos um evento real e já registrado (sagaTimedOut, Fase
      // 6) em vez de um envelope sintético.
      const envelope = createEvent(orderEvents.sagaTimedOut, {
        aggregateId: orderId,
        correlationId: orderId,
        producer: 'replay-test@0.0.0',
        payload: {
          orderId,
          stuckStatus: 'PAYMENT_APPROVED',
          timedOutAt: new Date().toISOString(),
        },
      });
      await producer.publish(TOPIC, envelope);
    }
    await producer.disconnect();

    async function applyEffect(ctx: MessageContext): Promise<void> {
      const { orderId } = ctx.envelope.payload as { orderId: string };
      await pool.query(
        'INSERT INTO replay_test_effects (order_id, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
        [orderId, ctx.envelope.eventId],
      );
    }

    // Consumer group "ao vivo" processa primeiro.
    await forceOffsetZero(admin, 'replay-test-live');
    const liveProducer = new EventProducer({ brokers: BROKERS, clientId: 'replay-test-live-producer' });
    await liveProducer.connect();
    const liveRuntime = new KafkaConsumerRuntime({
      brokers: BROKERS,
      groupId: 'replay-test-live' as never,
      sourceTopics: [TOPIC],
      producer: liveProducer,
      handler: applyEffect,
    });
    await liveRuntime.start();
    await new Promise((resolve) => setTimeout(resolve, 3_000));
    await liveRuntime.stop();
    await liveProducer.disconnect();

    const afterLive = await pool.query('SELECT COUNT(*)::int AS count FROM replay_test_effects');
    expect(afterLive.rows[0].count).toBe(5);

    // Consumer group NOVO reprocessa o MESMO tópico do offset zero.
    await forceOffsetZero(admin, 'replay-test-replay');
    const replayProducer = new EventProducer({ brokers: BROKERS, clientId: 'replay-test-replay-producer' });
    await replayProducer.connect();
    const replayRuntime = new KafkaConsumerRuntime({
      brokers: BROKERS,
      groupId: 'replay-test-replay' as never,
      sourceTopics: [TOPIC],
      producer: replayProducer,
      handler: applyEffect,
    });
    await replayRuntime.start();
    await new Promise((resolve) => setTimeout(resolve, 3_000));
    await replayRuntime.stop();
    await replayProducer.disconnect();

    // Mesmo estado final: 5 orderIds, 1 linha cada — o replay não duplicou nada porque
    // a chave (order_id, event_id) é a MESMA em ambos os grupos (mesmo eventId
    // publicado uma única vez), e ON CONFLICT DO NOTHING faz o papel do
    // processed_messages de um serviço de verdade.
    const afterReplay = await pool.query('SELECT COUNT(*)::int AS count FROM replay_test_effects');
    expect(afterReplay.rows[0].count).toBe(5);

    const distinctOrders = await pool.query('SELECT COUNT(DISTINCT order_id)::int AS count FROM replay_test_effects');
    expect(distinctOrders.rows[0].count).toBe(5);
  }, 30_000);
});
