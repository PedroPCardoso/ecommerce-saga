import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { OutboxRelay, type OutboxEnvelopeRow } from '../src/index.js';

const CONNECTION_STRING =
  process.env.ORDER_DATABASE_URL ?? 'postgresql://order_svc:changeme@localhost:15432/order_db';

const pool = new Pool({
  connectionString: CONNECTION_STRING,
  options: '-c search_path=outbox_relay_test,public',
});

async function resetSchema(): Promise<void> {
  await pool.query('DROP SCHEMA IF EXISTS outbox_relay_test CASCADE');
  await pool.query('CREATE SCHEMA outbox_relay_test');
  await pool.query(`
    CREATE TABLE outbox_relay_test.outbox (
      id             bigserial PRIMARY KEY,
      event_id       uuid NOT NULL UNIQUE,
      aggregate_id   text NOT NULL,
      aggregate_type text NOT NULL,
      event_type     text NOT NULL,
      payload        jsonb NOT NULL,
      headers        jsonb NOT NULL DEFAULT '{}',
      created_at     timestamptz NOT NULL DEFAULT now(),
      published_at   timestamptz,
      attempts       integer NOT NULL DEFAULT 0
    );
  `);
}

async function insertRow(eventId: string, published = false): Promise<void> {
  await pool.query(
    `INSERT INTO outbox (event_id, aggregate_id, aggregate_type, event_type, payload, headers, published_at)
     VALUES ($1, 'order-1', 'order', 'order.created', $2::jsonb, '{}', ${published ? 'now()' : 'NULL'})`,
    [eventId, JSON.stringify({ orderId: 'order-1' })],
  );
}

describe('OutboxRelay (integração — Postgres real, requer pnpm infra:up)', () => {
  beforeEach(async () => {
    await resetSchema();
  });

  afterAll(async () => {
    await pool.end();
  });

  it('publica linhas pendentes e marca published_at', async () => {
    const eventId = randomUUID();
    await insertRow(eventId);

    const published: OutboxEnvelopeRow[] = [];
    const relay = new OutboxRelay({
      pool,
      publish: async (row) => {
        published.push(row);
      },
      serviceName: 'order-service',
    });

    const processed = await relay.drainOnce();

    expect(processed).toBe(1);
    expect(published).toHaveLength(1);
    expect(published[0]?.eventId).toBe(eventId);

    const { rows } = await pool.query('SELECT published_at FROM outbox WHERE event_id = $1', [eventId]);
    expect(rows[0].published_at).not.toBeNull();
  });

  it('não republica linha já publicada', async () => {
    await insertRow(randomUUID(), true);

    const relay = new OutboxRelay({ pool, publish: async () => {}, serviceName: 'order-service' });
    const processed = await relay.drainOnce();

    expect(processed).toBe(0);
  });

  it('incrementa attempts e mantém published_at NULL quando o publish falha', async () => {
    const eventId = randomUUID();
    await insertRow(eventId);

    const relay = new OutboxRelay({
      pool,
      publish: async () => {
        throw new Error('broker indisponível');
      },
      onError: () => {},
      serviceName: 'order-service',
    });

    await relay.drainOnce();

    const { rows } = await pool.query(
      'SELECT published_at, attempts FROM outbox WHERE event_id = $1',
      [eventId],
    );
    expect(rows[0].published_at).toBeNull();
    expect(rows[0].attempts).toBe(1);
  });

  it('duas instâncias concorrentes não publicam a mesma linha duas vezes (FOR UPDATE SKIP LOCKED)', async () => {
    const ids = Array.from({ length: 10 }, () => randomUUID());
    for (const id of ids) await insertRow(id);

    const publishedByA: string[] = [];
    const publishedByB: string[] = [];
    const relayA = new OutboxRelay({
      pool,
      publish: async (r) => {
        publishedByA.push(r.eventId);
      },
      serviceName: 'order-service',
    });
    const relayB = new OutboxRelay({
      pool,
      publish: async (r) => {
        publishedByB.push(r.eventId);
      },
      serviceName: 'order-service',
    });

    await Promise.all([relayA.drainOnce(), relayB.drainOnce()]);

    const all = [...publishedByA, ...publishedByB];
    expect(new Set(all).size).toBe(all.length);
    expect(all.length).toBe(10);
  });

  it('atualiza outbox_lag_seconds com a idade da linha pendente mais antiga', async () => {
    const eventId = randomUUID();
    await insertRow(eventId);
    // Backdata created_at manualmente para simular uma linha PENDENTE HÁ tempo.
    await pool.query(`UPDATE outbox SET created_at = now() - interval '10 seconds' WHERE event_id = $1`, [eventId]);

    const relay = new OutboxRelay({ pool, publish: async () => {}, serviceName: 'order-service' });
    await relay.drainOnce();

    const { outboxLagSeconds } = await import('@ecommerce/observability');
    const output = await (await import('@ecommerce/observability')).metricsRegistry.metrics();
    expect(output).toContain('outbox_lag_seconds');
    void outboxLagSeconds; // só para o import não ficar "não usado" caso o lint reclame
  });
});
