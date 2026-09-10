import { Pool, type PoolClient } from 'pg';

/**
 * Os exemplos usam o banco do Order Service, num schema separado (`lab`), pelas
 * portas do compose (154xx — ver deploy/docker/docker-compose.yml).
 */
const URL =
  process.env.LAB_DATABASE_URL ?? 'postgresql://order_svc:changeme@localhost:15432/order_db';

export const pool = new Pool({ connectionString: URL, max: 12 });

/** Deixa o schema `lab` num estado conhecido. Todo exemplo começa por aqui. */
export async function recriarSchema(ddl: string): Promise<void> {
  await pool.query('DROP SCHEMA IF EXISTS lab CASCADE');
  await pool.query('CREATE SCHEMA lab');
  await pool.query(ddl);
}

/**
 * Roda uma função dentro de UMA transação. É o bloco que sustenta outbox e inbox:
 * efeito de negócio e registro de mensagem precisam viver ou morrer juntos.
 */
export async function emTransacao<T>(fn: (c: PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const r = await fn(c);
    await c.query('COMMIT');
    return r;
  } catch (e) {
    await c.query('ROLLBACK');
    throw e;
  } finally {
    c.release();
  }
}

export const contar = async (sql: string, params: unknown[] = []): Promise<number> =>
  Number((await pool.query(sql, params)).rows[0]?.n ?? 0);

export const fechar = () => pool.end();
