import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { markProcessed } from '../src/index.js';

const CONNECTION_STRING =
  process.env.ORDER_DATABASE_URL ?? 'postgresql://order_svc:changeme@localhost:15432/order_db';

const pool = new Pool({
  connectionString: CONNECTION_STRING,
  options: '-c search_path=idempotency_test,public',
});

const rawSqlClient = {
  $executeRawUnsafe: async (query: string, ...values: unknown[]) => {
    const result = await pool.query(query, values);
    return result.rowCount ?? 0;
  },
};

async function resetSchema(): Promise<void> {
  await pool.query('DROP SCHEMA IF EXISTS idempotency_test CASCADE');
  await pool.query('CREATE SCHEMA idempotency_test');
  await pool.query(`
    CREATE TABLE idempotency_test.processed_messages (
      event_id       uuid NOT NULL,
      consumer_group text NOT NULL,
      processed_at   timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (event_id, consumer_group)
    );
  `);
}

describe('markProcessed (integração — Postgres real, requer pnpm infra:up)', () => {
  beforeEach(async () => {
    await resetSchema();
  });

  afterAll(async () => {
    await pool.end();
  });

  it('a mesma mensagem entregue 5x em paralelo aplica o efeito 1x — a PK única é o árbitro', async () => {
    const eventId = randomUUID();
    const results = await Promise.all(
      Array.from({ length: 5 }, () => markProcessed(rawSqlClient, eventId, 'estoque')),
    );

    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('dois consumer groups distintos processam o MESMO evento — nenhum bloqueia o outro', async () => {
    const eventId = randomUUID();

    const forPayment = await markProcessed(rawSqlClient, eventId, 'payment-service');
    const forNotification = await markProcessed(rawSqlClient, eventId, 'notification-service');

    expect(forPayment).toBe(true);
    expect(forNotification).toBe(true);
  });
});
