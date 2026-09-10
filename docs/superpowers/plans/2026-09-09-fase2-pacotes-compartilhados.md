# Fase 2 — Pacotes Compartilhados (kafka, outbox, idempotency) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar `packages/idempotency`, `packages/outbox` e `packages/kafka`
— a infraestrutura reutilizável de consumo/produção que todos os 5
microserviços vão importar. Nenhum destes pacotes tem HTTP nem framework;
são bibliotecas puras que os apps (NestJS) chamam.

**Architecture:** `packages/idempotency` e `packages/outbox` operam contra
uma interface estrutural mínima (`{ $executeRawUnsafe(query, ...values):
Promise<number> }`) satisfeita por qualquer `PrismaClient`/
`Prisma.TransactionClient`, sem importar `@prisma/client` — cada serviço tem
seu próprio client gerado, e acoplar o tipo aqui prenderia todos numa versão
única. `packages/outbox` também expõe um `OutboxRelay` que roda como poller
à parte com `pg.Pool` puro (não participa da transação de domínio) e recebe
uma função `publish` injetada, para não depender de `packages/kafka`
diretamente. `packages/kafka` embrulha `kafkajs` com commit manual, escada
de retry em tópicos dedicados e desvio para DLT, usando os helpers já
existentes em `@ecommerce/contracts` (`retryTopic`, `deadLetterTopic`,
`RETRY_LADDER`, `RETRY_HEADERS`).

**Tech Stack:** TypeScript strict, `kafkajs` 2.2.4, `pg` 8.13, Vitest,
`@ecommerce/contracts` (já implementado).

## Global Constraints

- Node >=22, pnpm workspace — todo pacote novo segue o padrão de
  `packages/contracts` (`package.json` com `type: module`, `exports` com
  `types`+`default`, scripts `build`/`typecheck`/`lint`/`test`).
- `tsconfig.json` de cada pacote: `{ "extends": "../../tsconfig.base.json",
  "compilerOptions": { "rootDir": "src", "outDir": "dist", "tsBuildInfoFile":
  "dist/.tsbuildinfo" }, "include": ["src/**/*.ts"] }`.
- `vitest.config.ts` de cada pacote: `{ test: { include:
  ["test/**/*.spec.ts"], environment: "node" } }` via `defineConfig` de
  `vitest/config`.
- Testes de integração (que tocam Postgres/Kafka reais) exigem
  `pnpm infra:up` rodando — mesma convenção de `examples/`, prefixo `lab.`
  para tópicos e schema dedicado para tabelas, nunca a topologia real.
- Nenhum destes pacotes importa `@nestjs/*` — são bibliotecas puras.
- Commits diretos em `master`, conventional commits (hook já ativo desde a
  Fase 0).
- Antes de rodar testes que dependem de `@ecommerce/contracts`, rode
  `pnpm --filter @ecommerce/contracts build` (os outros pacotes importam do
  `dist/` publicado, não do `src/`).

---

### Task 1: `packages/idempotency`

**Files:**
- Create: `packages/idempotency/package.json`
- Create: `packages/idempotency/tsconfig.json`
- Create: `packages/idempotency/vitest.config.ts`
- Create: `packages/idempotency/src/schema.sql`
- Create: `packages/idempotency/src/processed-messages.ts`
- Create: `packages/idempotency/src/index.ts`
- Test: `packages/idempotency/test/processed-messages.spec.ts`

**Interfaces:**
- Produces: `RawSqlClient` (interface), `markProcessed(tx: RawSqlClient,
  eventId: string, consumerGroup: string): Promise<boolean>` — usado por
  todo handler de consumo a partir da Fase 3.

- [ ] **Step 1: Criar o `package.json`**

```json
{
  "name": "@ecommerce/idempotency",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "default": "./dist/index.js"
    }
  },
  "files": ["dist"],
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "lint": "eslint src",
    "test": "vitest run"
  },
  "devDependencies": {
    "@types/pg": "^8.11.10",
    "pg": "^8.13.1",
    "typescript": "^5.7.2",
    "vitest": "^2.1.8"
  }
}
```

`pg` é só devDependency: o código de produção (`processed-messages.ts`) não
importa `pg`, só o teste de integração usa para criar a tabela de teste.

- [ ] **Step 2: `tsconfig.json` e `vitest.config.ts`**

`packages/idempotency/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "rootDir": "src",
    "outDir": "dist",
    "tsBuildInfoFile": "dist/.tsbuildinfo"
  },
  "include": ["src/**/*.ts"]
}
```

`packages/idempotency/vitest.config.ts`:
```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.spec.ts'],
    environment: 'node',
  },
});
```

- [ ] **Step 3: DDL de referência**

`packages/idempotency/src/schema.sql`:
```sql
-- Copie esta tabela para a migration Prisma de CADA serviço que consome
-- eventos Kafka. A chave é o par (event_id, consumer_group): dois consumer
-- groups diferentes (ex.: payment-service e notification-service) precisam
-- processar o MESMO evento — só event_id bloquearia o segundo grupo sem
-- nenhum erro visível.
CREATE TABLE processed_messages (
  event_id       uuid NOT NULL,
  consumer_group text NOT NULL,
  processed_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, consumer_group)
);
```

- [ ] **Step 4: Escrever o teste (unitário, cliente fake) antes da implementação**

`packages/idempotency/test/processed-messages.spec.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import { markProcessed, type RawSqlClient } from '../src/index.js';

describe('markProcessed (unitário — cliente fake)', () => {
  it('devolve true e insere quando é a primeira vez', async () => {
    const executeRawUnsafe = vi.fn().mockResolvedValue(1);
    const tx: RawSqlClient = { $executeRawUnsafe: executeRawUnsafe };

    const result = await markProcessed(tx, '018f3f4e-0000-7000-8000-000000000001', 'payment-service');

    expect(result).toBe(true);
    expect(executeRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO processed_messages'),
      '018f3f4e-0000-7000-8000-000000000001',
      'payment-service',
    );
  });

  it('devolve false quando o par (event_id, consumer_group) já existe', async () => {
    const tx: RawSqlClient = { $executeRawUnsafe: vi.fn().mockResolvedValue(0) };

    const result = await markProcessed(tx, 'evt-repetido', 'payment-service');

    expect(result).toBe(false);
  });
});
```

- [ ] **Step 5: Rodar o teste e confirmar que falha (módulo não existe ainda)**

```bash
pnpm --filter @ecommerce/idempotency test
```

Esperado: FALHA — `Cannot find module '../src/index.js'` ou equivalente.

- [ ] **Step 6: Implementar `processed-messages.ts` e `index.ts`**

`packages/idempotency/src/processed-messages.ts`:
```ts
/**
 * Cliente mínimo, estrutural: qualquer PrismaClient ou
 * Prisma.TransactionClient satisfaz isto sem que este pacote precise
 * importar @prisma/client — cada serviço gera o seu, com schema próprio, e
 * acoplar o tipo aqui prenderia todos numa única versão de client gerado.
 */
export interface RawSqlClient {
  $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
}

/**
 * Marca o evento como processado por este consumer group, DENTRO da mesma
 * transação do efeito de negócio. Devolve `true` na primeira vez — o
 * chamador deve aplicar o efeito — e `false` se já processado — o chamador
 * pula o efeito, mas ainda assim deixa a transação commitar (vazia) e o
 * offset ser commitado: reentrega não é erro.
 */
export async function markProcessed(
  tx: RawSqlClient,
  eventId: string,
  consumerGroup: string,
): Promise<boolean> {
  const affected = await tx.$executeRawUnsafe(
    `INSERT INTO processed_messages (event_id, consumer_group) VALUES ($1::uuid, $2)
     ON CONFLICT DO NOTHING`,
    eventId,
    consumerGroup,
  );

  return affected === 1;
}
```

`packages/idempotency/src/index.ts`:
```ts
export * from './processed-messages.js';
```

- [ ] **Step 7: Rodar o teste e confirmar que passa**

```bash
pnpm --filter @ecommerce/idempotency test
```

Esperado: PASS, 2 testes.

- [ ] **Step 8: Escrever o teste de integração (Postgres real)**

`packages/idempotency/test/processed-messages.integration.spec.ts`:
```ts
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
      Array.from({ length: 5 }, () => markProcessed(pool, eventId, 'estoque')),
    );

    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('dois consumer groups distintos processam o MESMO evento — nenhum bloqueia o outro', async () => {
    const eventId = randomUUID();

    const forPayment = await markProcessed(pool, eventId, 'payment-service');
    const forNotification = await markProcessed(pool, eventId, 'notification-service');

    expect(forPayment).toBe(true);
    expect(forNotification).toBe(true);
  });
});
```

Note que `pool` (node-postgres `Pool`) satisfaz `RawSqlClient` porque também
tem um método `$executeRawUnsafe`? **Não tem** — `pg.Pool` não tem esse
método, tem `.query()`. Ajuste: crie um adaptador fino no próprio teste, não
no pacote (o pacote não deve saber nada sobre `pg`):

```ts
const rawSqlClient = {
  $executeRawUnsafe: async (query: string, ...values: unknown[]) => {
    const result = await pool.query(query, values);
    return result.rowCount ?? 0;
  },
};
```

Substitua todo `markProcessed(pool, ...)` acima por
`markProcessed(rawSqlClient, ...)`.

- [ ] **Step 9: Rodar o teste de integração (requer infra no ar)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/idempotency test
```

Esperado: PASS, 4 testes no total (2 unitários + 2 de integração).

- [ ] **Step 10: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/idempotency build
pnpm --filter @ecommerce/idempotency typecheck
pnpm --filter @ecommerce/idempotency lint
```

Esperado: os três sem erro.

- [ ] **Step 11: Commit**

```bash
git add packages/idempotency pnpm-lock.yaml
git commit -m "feat(idempotency): tabela de inbox e markProcessed para dedup por consumer group"
```

---

### Task 2: `packages/outbox`

**Files:**
- Create: `packages/outbox/package.json`
- Create: `packages/outbox/tsconfig.json`
- Create: `packages/outbox/vitest.config.ts`
- Create: `packages/outbox/src/schema.sql`
- Create: `packages/outbox/src/outbox-repository.ts`
- Create: `packages/outbox/src/outbox-relay.ts`
- Create: `packages/outbox/src/index.ts`
- Test: `packages/outbox/test/outbox-repository.spec.ts`
- Test: `packages/outbox/test/outbox-relay.integration.spec.ts`

**Interfaces:**
- Consumes: nenhuma (independente da Task 1).
- Produces: `RawSqlClient`, `insertOutboxRow(tx, row): Promise<void>`,
  `OutboxRelay` (classe com `start()`, `stop()`, `drainOnce()`),
  `OutboxEnvelopeRow`, `PublishFn` — usados pelo Order Service (Fase 1) e
  por todo serviço que publica eventos (Fase 3, 4, 5).

- [ ] **Step 1: `package.json`, `tsconfig.json`, `vitest.config.ts`**

`packages/outbox/package.json`:
```json
{
  "name": "@ecommerce/outbox",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "default": "./dist/index.js"
    }
  },
  "files": ["dist"],
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "lint": "eslint src",
    "test": "vitest run"
  },
  "dependencies": {
    "pg": "^8.13.1"
  },
  "devDependencies": {
    "@types/pg": "^8.11.10",
    "typescript": "^5.7.2",
    "vitest": "^2.1.8"
  }
}
```

Diferente de `idempotency`, aqui `pg` é dependency de produção — o
`OutboxRelay` usa `Pool` de verdade (é um poller à parte, não participa da
transação do Prisma do serviço).

`tsconfig.json` e `vitest.config.ts`: idênticos ao padrão da Task 1 (troque
apenas o necessário — não há nada específico deste pacote).

- [ ] **Step 2: DDL de referência**

`packages/outbox/src/schema.sql`:
```sql
-- Copie esta tabela para a migration Prisma de CADA serviço que publica
-- eventos. `payload` guarda o ENVELOPE completo já validado (não só o
-- payload de negócio) — é o que o relay publica sem reconstruir nada.
CREATE TABLE outbox (
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

-- Índice PARCIAL: o relay só pergunta pelas pendentes, e essa consulta roda
-- a cada 200ms para sempre. Sem o WHERE, o índice cresce com o histórico
-- inteiro e a consulta degrada junto.
CREATE INDEX outbox_pending_idx ON outbox (created_at) WHERE published_at IS NULL;
```

- [ ] **Step 3: Teste do repositório (unitário, cliente fake) antes da implementação**

`packages/outbox/test/outbox-repository.spec.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import { insertOutboxRow, type RawSqlClient } from '../src/index.js';

describe('insertOutboxRow', () => {
  it('monta o INSERT parametrizado com o envelope serializado', async () => {
    const executeRawUnsafe = vi.fn().mockResolvedValue(1);
    const tx: RawSqlClient = { $executeRawUnsafe: executeRawUnsafe };

    await insertOutboxRow(tx, {
      eventId: '018f3f4e-0000-7000-8000-000000000001',
      aggregateId: 'order-1',
      aggregateType: 'order',
      eventType: 'order.created',
      envelope: { eventId: '018f3f4e-0000-7000-8000-000000000001', payload: { orderId: 'order-1' } },
      headers: { 'x-trace': 'abc' },
    });

    expect(executeRawUnsafe).toHaveBeenCalledTimes(1);
    const [sql, ...values] = executeRawUnsafe.mock.calls[0]!;
    expect(sql).toContain('INSERT INTO outbox');
    expect(values[0]).toBe('018f3f4e-0000-7000-8000-000000000001');
    expect(values[1]).toBe('order-1');
    expect(values[2]).toBe('order');
    expect(values[3]).toBe('order.created');
    expect(JSON.parse(values[4] as string)).toEqual({
      eventId: '018f3f4e-0000-7000-8000-000000000001',
      payload: { orderId: 'order-1' },
    });
    expect(JSON.parse(values[5] as string)).toEqual({ 'x-trace': 'abc' });
  });

  it('lança erro se nenhuma linha foi afetada', async () => {
    const tx: RawSqlClient = { $executeRawUnsafe: vi.fn().mockResolvedValue(0) };

    await expect(
      insertOutboxRow(tx, {
        eventId: 'evt-1',
        aggregateId: 'order-1',
        aggregateType: 'order',
        eventType: 'order.created',
        envelope: {},
      }),
    ).rejects.toThrow(/Falha ao inserir/);
  });
});
```

- [ ] **Step 4: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/outbox test
```

Esperado: FALHA (módulo não existe).

- [ ] **Step 5: Implementar `outbox-repository.ts` e `index.ts` (parcial)**

`packages/outbox/src/outbox-repository.ts`:
```ts
/**
 * Cliente mínimo, estrutural: qualquer PrismaClient ou
 * Prisma.TransactionClient satisfaz isto sem que este pacote precise
 * importar @prisma/client.
 */
export interface RawSqlClient {
  $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
}

export interface OutboxRow {
  eventId: string;
  aggregateId: string;
  aggregateType: string;
  eventType: string;
  /**
   * O envelope COMPLETO e já validado (saída de createEvent de
   * @ecommerce/contracts), não só o payload de negócio — é o que o relay
   * publica sem reconstruir nada.
   */
  envelope: unknown;
  headers?: Record<string, string>;
}

/**
 * Insere a linha de outbox. DEVE ser chamado dentro da mesma transação
 * Prisma que grava o efeito de domínio — é isso que torna o par atômico.
 */
export async function insertOutboxRow(tx: RawSqlClient, row: OutboxRow): Promise<void> {
  const affected = await tx.$executeRawUnsafe(
    `INSERT INTO outbox (event_id, aggregate_id, aggregate_type, event_type, payload, headers)
     VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6::jsonb)`,
    row.eventId,
    row.aggregateId,
    row.aggregateType,
    row.eventType,
    JSON.stringify(row.envelope),
    JSON.stringify(row.headers ?? {}),
  );

  if (affected !== 1) {
    throw new Error(`Falha ao inserir linha de outbox para o evento ${row.eventId}`);
  }
}
```

`packages/outbox/src/index.ts`:
```ts
export * from './outbox-repository.js';
export * from './outbox-relay.js';
```

(o `index.ts` já referencia `outbox-relay.js`, que ainda não existe — normal,
o próximo step cria.)

- [ ] **Step 6: Rodar e confirmar que o teste do repositório passa (mesmo com index.ts quebrado por ora)**

Antes de rodar, comente temporariamente a linha `export * from
'./outbox-relay.js';` em `index.ts` (ou pule para o Step 7 e crie o relay
antes de rodar — mais simples: faça o Step 7 primeiro, depois rode os dois
juntos no Step 9).

- [ ] **Step 7: Escrever o teste do relay (integração, Postgres real) antes da implementação**

`packages/outbox/test/outbox-relay.integration.spec.ts`:
```ts
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

    const relay = new OutboxRelay({ pool, publish: async () => {} });
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
    });
    const relayB = new OutboxRelay({
      pool,
      publish: async (r) => {
        publishedByB.push(r.eventId);
      },
    });

    await Promise.all([relayA.drainOnce(), relayB.drainOnce()]);

    const all = [...publishedByA, ...publishedByB];
    expect(new Set(all).size).toBe(all.length);
    expect(all.length).toBe(10);
  });
});
```

- [ ] **Step 8: Implementar `outbox-relay.ts`**

`packages/outbox/src/outbox-relay.ts`:
```ts
import type { Pool } from 'pg';

export interface OutboxEnvelopeRow {
  id: string;
  eventId: string;
  envelope: unknown;
  headers: Record<string, string>;
}

export type PublishFn = (row: OutboxEnvelopeRow) => Promise<void>;

export interface OutboxRelayOptions {
  pool: Pool;
  publish: PublishFn;
  pollIntervalMs?: number;
  batchSize?: number;
  onError?: (error: unknown, row: OutboxEnvelopeRow) => void;
}

/**
 * Relay assíncrono: lê linhas pendentes com FOR UPDATE SKIP LOCKED, publica,
 * marca published_at. At-least-once por construção — pode publicar e morrer
 * antes do UPDATE, o que republica na próxima volta. O consumidor precisa de
 * idempotência (packages/idempotency) por causa disto, não apesar disto.
 */
export class OutboxRelay {
  private readonly pool: Pool;
  private readonly publishFn: PublishFn;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly onError: (error: unknown, row: OutboxEnvelopeRow) => void;
  private running = false;
  private loopPromise: Promise<void> | null = null;

  constructor(opts: OutboxRelayOptions) {
    this.pool = opts.pool;
    this.publishFn = opts.publish;
    this.pollIntervalMs = opts.pollIntervalMs ?? 200;
    this.batchSize = opts.batchSize ?? 100;
    this.onError =
      opts.onError ??
      ((error, row) => console.error(`[outbox-relay] falha ao publicar ${row.eventId}`, error));
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.loop();
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.loopPromise;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      const processed = await this.drainOnce();
      if (processed === 0) {
        await sleep(this.pollIntervalMs);
      }
    }
  }

  /** Uma passada: publica até `batchSize` linhas pendentes. Devolve quantas processou. */
  async drainOnce(): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<{
        id: string;
        event_id: string;
        payload: unknown;
        headers: Record<string, string>;
      }>(
        `SELECT id, event_id, payload, headers
           FROM outbox
          WHERE published_at IS NULL
          ORDER BY created_at
          LIMIT $1
          FOR UPDATE SKIP LOCKED`,
        [this.batchSize],
      );

      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }

      const published: string[] = [];
      for (const row of rows) {
        const outboxRow: OutboxEnvelopeRow = {
          id: row.id,
          eventId: row.event_id,
          envelope: row.payload,
          headers: row.headers ?? {},
        };
        try {
          await this.publishFn(outboxRow);
          published.push(row.id);
        } catch (error) {
          this.onError(error, outboxRow);
          await client.query('UPDATE outbox SET attempts = attempts + 1 WHERE id = $1', [row.id]);
        }
      }

      if (published.length > 0) {
        await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [
          published,
        ]);
      }

      await client.query('COMMIT');
      return rows.length;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
```

- [ ] **Step 9: Rodar os testes (unitário + integração, requer infra no ar)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/outbox test
```

Esperado: PASS, 6 testes no total (2 do repositório + 4 do relay).

- [ ] **Step 10: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/outbox build
pnpm --filter @ecommerce/outbox typecheck
pnpm --filter @ecommerce/outbox lint
```

- [ ] **Step 11: Commit**

```bash
git add packages/outbox pnpm-lock.yaml
git commit -m "feat(outbox): tabela, repositório e relay com FOR UPDATE SKIP LOCKED"
```

---

### Task 3: `packages/kafka` — produtor, classificação de erro, headers de retry

**Files:**
- Create: `packages/kafka/package.json`
- Create: `packages/kafka/tsconfig.json`
- Create: `packages/kafka/vitest.config.ts`
- Create: `packages/kafka/src/producer.ts`
- Create: `packages/kafka/src/error-classification.ts`
- Create: `packages/kafka/src/retry-headers.ts`
- Create: `packages/kafka/src/index.ts`
- Test: `packages/kafka/test/error-classification.spec.ts`
- Test: `packages/kafka/test/retry-headers.spec.ts`
- Test: `packages/kafka/test/producer.integration.spec.ts`

**Interfaces:**
- Consumes: `UnknownEnvelope`, `UnprocessableEventError`, `RETRY_HEADERS`
  de `@ecommerce/contracts` (já implementado).
- Produces: `EventProducer` (classe com `connect()`, `disconnect()`,
  `publish(topic, envelope, headers?)`, `publishRaw(topic, value, headers,
  key?)`), `classifyError(error): 'retriable' | 'permanent'`,
  `buildRedirectHeaders(opts)`, `readRetryCount(headers)` — usados pela
  Task 4 (consumer runtime) e por todos os serviços a partir da Fase 1.

- [ ] **Step 1: `package.json`, `tsconfig.json`, `vitest.config.ts`**

`packages/kafka/package.json`:
```json
{
  "name": "@ecommerce/kafka",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "default": "./dist/index.js"
    }
  },
  "files": ["dist"],
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "lint": "eslint src",
    "test": "vitest run"
  },
  "dependencies": {
    "kafkajs": "^2.2.4",
    "@ecommerce/contracts": "workspace:*"
  },
  "devDependencies": {
    "typescript": "^5.7.2",
    "vitest": "^2.1.8"
  }
}
```

`tsconfig.json`/`vitest.config.ts`: mesmo padrão das Tasks 1 e 2.

- [ ] **Step 2: Testes unitários primeiro — classificação de erro**

`packages/kafka/test/error-classification.spec.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { UnprocessableEventError } from '@ecommerce/contracts';
import { classifyError } from '../src/index.js';

describe('classifyError', () => {
  it('classifica UnprocessableEventError como permanente', () => {
    expect(classifyError(new UnprocessableEventError('schema inválido'))).toBe('permanent');
  });

  it('classifica erro marcado .permanent=true como permanente', () => {
    const error = new Error('regra de negócio violada') as Error & { permanent: boolean };
    error.permanent = true;
    expect(classifyError(error)).toBe('permanent');
  });

  it('classifica erro comum como retriável — fail secure: não desiste cedo demais', () => {
    expect(classifyError(new Error('ETIMEDOUT'))).toBe('retriable');
  });

  it('classifica valor não-Error como retriável', () => {
    expect(classifyError('string genérica')).toBe('retriable');
  });
});
```

- [ ] **Step 3: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/contracts build   # kafka importa de @ecommerce/contracts
pnpm --filter @ecommerce/kafka test
```

Esperado: FALHA (módulo não existe).

- [ ] **Step 4: Implementar `error-classification.ts`**

`packages/kafka/src/error-classification.ts`:
```ts
import { UnprocessableEventError } from '@ecommerce/contracts';

export type ErrorClass = 'retriable' | 'permanent';

export interface ClassifiableError {
  permanent?: boolean;
}

/**
 * Erro desconhecido classifica como RETRIABLE, não PERMANENT — fail secure
 * aqui significa não desistir cedo demais de algo que pode ser transitório.
 * O pior caso é gastar a escada inteira (5s+1m+10m) antes de cair na DLT, o
 * que ainda é seguro: nada se perde, só demora mais para ser investigado.
 * Erros conhecidos como definitivos (schema inválido, versão desconhecida,
 * regra de negócio violada) chegam aqui marcados `.permanent = true`.
 */
export function classifyError(error: unknown): ErrorClass {
  if (error instanceof UnprocessableEventError) return 'permanent';
  if (isClassifiableError(error) && error.permanent === true) return 'permanent';
  return 'retriable';
}

function isClassifiableError(error: unknown): error is ClassifiableError {
  return typeof error === 'object' && error !== null && 'permanent' in error;
}
```

- [ ] **Step 5: Teste unitário de headers de retry**

`packages/kafka/test/retry-headers.spec.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { RETRY_HEADERS } from '@ecommerce/contracts';
import { buildRedirectHeaders, readRetryCount } from '../src/index.js';

describe('readRetryCount', () => {
  it('devolve 0 quando o header não existe', () => {
    expect(readRetryCount({})).toBe(0);
  });

  it('lê o valor do header x-retry-count', () => {
    expect(readRetryCount({ [RETRY_HEADERS.retryCount]: '2' })).toBe(2);
  });
});

describe('buildRedirectHeaders', () => {
  it('monta todos os headers obrigatórios sem vazar o stacktrace inteiro', () => {
    const headers = buildRedirectHeaders({
      originalTopic: 'ecommerce.orders.v1',
      originalPartition: 0,
      originalOffset: '42',
      retryCount: 1,
      firstFailureAt: '2026-09-09T00:00:00.000Z',
      error: new Error('ETIMEDOUT ao chamar gateway'),
      consumerGroup: 'payment-service',
    });

    expect(headers[RETRY_HEADERS.originalTopic]).toBe('ecommerce.orders.v1');
    expect(headers[RETRY_HEADERS.retryCount]).toBe('1');
    expect(headers[RETRY_HEADERS.lastError]).toBe('ETIMEDOUT ao chamar gateway');
    expect(headers[RETRY_HEADERS.stacktraceHash]).toMatch(/^sha256:[0-9a-f]{8}$/);
    expect(headers[RETRY_HEADERS.stacktraceHash]).not.toContain('at ');
  });
});
```

- [ ] **Step 6: Implementar `retry-headers.ts`**

`packages/kafka/src/retry-headers.ts`:
```ts
import { RETRY_HEADERS } from '@ecommerce/contracts';

export function readRetryCount(headers: Record<string, Buffer | string | undefined>): number {
  const raw = headers[RETRY_HEADERS.retryCount];
  if (raw === undefined) return 0;
  const value = Number(raw.toString());
  return Number.isFinite(value) ? value : 0;
}

export function buildRedirectHeaders(opts: {
  originalTopic: string;
  originalPartition: number;
  originalOffset: string;
  retryCount: number;
  firstFailureAt: string;
  error: unknown;
  consumerGroup: string;
}): Record<string, string> {
  return {
    [RETRY_HEADERS.originalTopic]: opts.originalTopic,
    [RETRY_HEADERS.originalPartition]: String(opts.originalPartition),
    [RETRY_HEADERS.originalOffset]: opts.originalOffset,
    [RETRY_HEADERS.retryCount]: String(opts.retryCount),
    [RETRY_HEADERS.firstFailureAt]: opts.firstFailureAt,
    [RETRY_HEADERS.lastError]: errorMessage(opts.error).slice(0, 500),
    [RETRY_HEADERS.stacktraceHash]: `sha256:${hashString(errorStack(opts.error))}`,
    [RETRY_HEADERS.consumerGroup]: opts.consumerGroup,
  };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorStack(error: unknown): string {
  return error instanceof Error ? (error.stack ?? error.message) : String(error);
}

/**
 * FNV-1a — determinístico, sem dependência externa. Só para correlacionar
 * erros na DLT (ex.: "essas 40 mensagens falharam pelo mesmo motivo"),
 * nunca para segurança.
 */
function hashString(input: string): string {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}
```

- [ ] **Step 7: Teste de integração do produtor (Kafka real)**

`packages/kafka/test/producer.integration.spec.ts`:
```ts
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
```

- [ ] **Step 8: Implementar `producer.ts` e `index.ts`**

`packages/kafka/src/producer.ts`:
```ts
import { CompressionTypes, Kafka, logLevel, type Producer } from 'kafkajs';
import type { UnknownEnvelope } from '@ecommerce/contracts';

export interface EventProducerOptions {
  brokers: string[];
  clientId: string;
}

/**
 * Produtor com as garantias que a saga exige: `idempotent: true` faz o
 * broker deduplicar por (producerId, sequence) — protege contra retry
 * interno do cliente, não contra republicação do relay depois de um
 * reinício (isso é o par outbox+idempotency). Chave SEMPRE o aggregateId do
 * envelope — nunca escolhida pelo chamador — porque é isso que garante que
 * todo evento de um pedido cai na mesma partição (ADR-0009).
 *
 * Compressão: GZIP (builtin do kafkajs). docs/PLAN.md pede zstd, mas isso
 * exige um codec nativo adicional (@kafkajs/zstd) — dívida documentada, não
 * necessária para o sistema funcionar.
 */
export class EventProducer {
  private readonly kafka: Kafka;
  private producer: Producer | null = null;

  constructor(opts: EventProducerOptions) {
    this.kafka = new Kafka({
      clientId: opts.clientId,
      brokers: opts.brokers,
      logLevel: logLevel.ERROR,
      retry: { retries: 5, initialRetryTime: 300 },
    });
  }

  async connect(): Promise<void> {
    this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 5 });
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    this.producer = null;
  }

  async publish(
    topic: string,
    envelope: UnknownEnvelope,
    headers: Record<string, string> = {},
  ): Promise<void> {
    this.assertConnected();
    await this.producer!.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{ key: envelope.aggregateId, value: JSON.stringify(envelope), headers }],
    });
  }

  /** Usado pelo consumer runtime (Task 4) para redirecionar bytes originais para retry/DLT sem re-serializar. */
  async publishRaw(
    topic: string,
    value: Buffer | string | null,
    headers: Record<string, string>,
    key?: Buffer | string | null,
  ): Promise<void> {
    this.assertConnected();
    await this.producer!.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{ key: key ?? undefined, value, headers }],
    });
  }

  private assertConnected(): void {
    if (!this.producer) {
      throw new Error('EventProducer usado antes de connect()');
    }
  }
}
```

`packages/kafka/src/index.ts`:
```ts
export * from './producer.js';
export * from './error-classification.js';
export * from './retry-headers.js';
```

(a Task 4 acrescenta `export * from './consumer-runtime.js';` aqui.)

- [ ] **Step 9: Rodar todos os testes (requer infra no ar)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/kafka test
```

Esperado: PASS, 7 testes (4 de classificação + 2 de headers + 1 de produtor).

- [ ] **Step 10: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/kafka build
pnpm --filter @ecommerce/kafka typecheck
pnpm --filter @ecommerce/kafka lint
```

- [ ] **Step 11: Commit**

```bash
git add packages/kafka pnpm-lock.yaml
git commit -m "feat(kafka): produtor idempotente, classificação de erro e headers de retry"
```

---

### Task 4: `packages/kafka` — consumer runtime (escada de retry + DLT)

**Files:**
- Create: `packages/kafka/src/consumer-runtime.ts`
- Modify: `packages/kafka/src/index.ts` (acrescentar export)
- Test: `packages/kafka/test/consumer-runtime.integration.spec.ts`

**Interfaces:**
- Consumes: `EventProducer` e `classifyError`/`buildRedirectHeaders` da
  Task 3; `MAX_RETRY_ATTEMPTS`, `RETRY_LADDER`, `retryTopic`,
  `deadLetterTopic`, `parseEvent`, `ConsumerGroup`, `UnknownEnvelope` de
  `@ecommerce/contracts`.
- Produces: `KafkaConsumerRuntime` (classe com `start()`, `stop()`),
  `MessageContext { envelope: UnknownEnvelope }`, `MessageHandler` — usado
  por todos os serviços consumidores a partir da Fase 3.

- [ ] **Step 1: Escrever o teste de integração antes da implementação**

`packages/kafka/test/consumer-runtime.integration.spec.ts`:
```ts
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
const GROUP = 'lab-kcr-group' as ConsumerGroup;

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

describe('KafkaConsumerRuntime (integração — Kafka real, requer pnpm infra:up)', () => {
  let admin: Admin;
  let producer: EventProducer;

  beforeAll(async () => {
    const kafka = new Kafka({ clientId: 'kcr-test-admin', brokers: BROKERS, logLevel: logLevel.NOTHING });
    admin = kafka.admin();
    await admin.connect();

    const topics = [
      SOURCE_TOPIC,
      ...Array.from({ length: MAX_RETRY_ATTEMPTS }, (_, i) => retryTopic(SOURCE_TOPIC, GROUP, i)),
      deadLetterTopic(SOURCE_TOPIC, GROUP),
    ];
    const existing = new Set(await admin.listTopics());
    const missing = topics.filter((t) => !existing.has(t));
    if (missing.length > 0) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 1 })),
      });
    }

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
```

**Nota de escopo:** este teste NÃO cobre o caminho de esgotamento total da
escada (3 degraus até a DLT) porque os delays de produção são 5s/1m/10m —
esperar ~11 minutos por teste não é razoável aqui. Esse caminho já está
provado em `examples/src/05-retry-e-dlt.ts` com delays curtos (500ms/1s/2s).
A lógica de roteamento (`route()`) é a mesma para "escalar um degrau" e
"esgotar e cair na DLT" — o que muda é só a condição `nextAttempt >=
MAX_RETRY_ATTEMPTS`, e essa condição já é implicitamente exercitada aqui:
o teste "erro permanente" prova o desvio direto à DLT pela classificação, e
o teste de retry prova a escalada de um degrau. Se quiser fechar 100% do
caminho, isso vira um item da Fase 6 (testes de caos com infra dedicada).

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/kafka test
```

Esperado: FALHA (`consumer-runtime.js` não existe).

- [ ] **Step 3: Implementar `consumer-runtime.ts`**

`packages/kafka/src/consumer-runtime.ts`:
```ts
import { Kafka, logLevel, type Consumer, type EachMessagePayload } from 'kafkajs';
import {
  MAX_RETRY_ATTEMPTS,
  RETRY_LADDER,
  deadLetterTopic,
  parseEvent,
  retryTopic,
  type ConsumerGroup,
  type UnknownEnvelope,
} from '@ecommerce/contracts';
import { EventProducer } from './producer.js';
import { classifyError } from './error-classification.js';
import { buildRedirectHeaders } from './retry-headers.js';

export interface MessageContext {
  envelope: UnknownEnvelope;
}

export type MessageHandler = (ctx: MessageContext) => Promise<void>;

export interface KafkaConsumerRuntimeOptions {
  brokers: string[];
  groupId: ConsumerGroup;
  sourceTopics: readonly string[];
  handler: MessageHandler;
  producer: EventProducer;
  clientId?: string;
}

type KafkaMessage = EachMessagePayload['message'];

/**
 * Consumidor gerenciado: commit manual pós-processamento, escada de retry em
 * tópicos dedicados e desvio para DLT. Um consumidor para os tópicos de
 * negócio + um consumidor por degrau da escada (por tópico de origem),
 * todos compartilhando o mesmo handler e producer.
 *
 * Por que kafkajs direto, não @nestjs/microservices: o transport do Nest
 * abstrai o commit de offset e dificulta commit manual pós-transação
 * (docs/PLAN.md, armadilha #1).
 */
export class KafkaConsumerRuntime {
  private readonly kafka: Kafka;
  private readonly groupId: ConsumerGroup;
  private readonly sourceTopics: readonly string[];
  private readonly handler: MessageHandler;
  private readonly producer: EventProducer;
  private consumers: Consumer[] = [];

  constructor(opts: KafkaConsumerRuntimeOptions) {
    this.kafka = new Kafka({
      clientId: opts.clientId ?? `${opts.groupId}-consumer`,
      brokers: opts.brokers,
      logLevel: logLevel.ERROR,
      retry: { retries: 5, initialRetryTime: 300 },
    });
    this.groupId = opts.groupId;
    this.sourceTopics = opts.sourceTopics;
    this.handler = opts.handler;
    this.producer = opts.producer;
  }

  async start(): Promise<void> {
    const main = this.kafka.consumer({ groupId: this.groupId, sessionTimeout: 30_000 });
    await main.connect();
    await main.subscribe({ topics: [...this.sourceTopics] });
    await main.run({
      autoCommit: false,
      eachMessage: (payload) => this.processMainMessage(main, payload),
    });
    this.consumers.push(main);

    for (const sourceTopic of this.sourceTopics) {
      for (let attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt += 1) {
        const topic = retryTopic(sourceTopic, this.groupId, attempt);
        const rungGroupId = `${this.groupId}-${RETRY_LADDER[attempt]!.suffix}`;
        const consumer = this.kafka.consumer({ groupId: rungGroupId, sessionTimeout: 30_000 });
        await consumer.connect();
        await consumer.subscribe({ topics: [topic] });
        await consumer.run({
          autoCommit: false,
          eachMessage: (payload) => this.processRetryMessage(consumer, sourceTopic, attempt, payload),
        });
        this.consumers.push(consumer);
      }
    }
  }

  async stop(): Promise<void> {
    await Promise.all(this.consumers.map((consumer) => consumer.disconnect()));
    this.consumers = [];
  }

  private async processMainMessage(consumer: Consumer, payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    const commit = () =>
      consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);

    let envelope: UnknownEnvelope;
    try {
      envelope = this.parse(message.value);
    } catch (error) {
      await this.sendToDlt(topic, partition, message, error, 0);
      await commit();
      return;
    }

    try {
      await this.handler({ envelope });
      await commit();
    } catch (error) {
      await this.route(topic, partition, message, error, 0);
      await commit();
    }
  }

  /** Processa uma mensagem que chegou a um degrau da escada de retry. Comita no PRÓPRIO tópico de retry (onde a mensagem está), não no tópico de origem. */
  private async processRetryMessage(
    consumer: Consumer,
    sourceTopic: string,
    attempt: number,
    payload: EachMessagePayload,
  ): Promise<void> {
    await sleep(RETRY_LADDER[attempt]!.delayMs);
    const { topic, partition, message } = payload;
    const commit = () =>
      consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);

    let envelope: UnknownEnvelope;
    try {
      envelope = this.parse(message.value);
    } catch (error) {
      await this.sendToDlt(sourceTopic, partition, message, error, attempt + 1);
      await commit();
      return;
    }

    try {
      await this.handler({ envelope });
      await commit();
    } catch (error) {
      await this.route(sourceTopic, partition, message, error, attempt + 1);
      await commit();
    }
  }

  private parse(value: Buffer | null): UnknownEnvelope {
    if (!value) throw new Error('Mensagem sem payload');
    const { event } = parseEvent(JSON.parse(value.toString()));
    return event as UnknownEnvelope;
  }

  /** Decide entre o próximo degrau da escada ou a DLT, e publica lá. */
  private async route(
    sourceTopic: string,
    partition: number,
    message: KafkaMessage,
    error: unknown,
    retryCount: number,
  ): Promise<void> {
    const errorClass = classifyError(error);

    if (errorClass === 'permanent' || retryCount >= MAX_RETRY_ATTEMPTS) {
      await this.sendToDlt(sourceTopic, partition, message, error, retryCount);
      return;
    }

    const headers = buildRedirectHeaders({
      originalTopic: sourceTopic,
      originalPartition: partition,
      originalOffset: message.offset,
      retryCount: retryCount + 1,
      firstFailureAt: firstFailureAt(message, retryCount),
      error,
      consumerGroup: this.groupId,
    });

    await this.producer.publishRaw(
      retryTopic(sourceTopic, this.groupId, retryCount),
      message.value,
      headers,
      message.key,
    );
  }

  private async sendToDlt(
    sourceTopic: string,
    partition: number,
    message: KafkaMessage,
    error: unknown,
    retryCount: number,
  ): Promise<void> {
    const headers = buildRedirectHeaders({
      originalTopic: sourceTopic,
      originalPartition: partition,
      originalOffset: message.offset,
      retryCount,
      firstFailureAt: firstFailureAt(message, retryCount),
      error,
      consumerGroup: this.groupId,
    });

    await this.producer.publishRaw(
      deadLetterTopic(sourceTopic, this.groupId),
      message.value,
      headers,
      message.key,
    );
  }
}

function firstFailureAt(message: KafkaMessage, retryCount: number): string {
  if (retryCount === 0) return new Date().toISOString();
  const existing = message.headers?.['x-first-failure-at'];
  return existing ? existing.toString() : new Date().toISOString();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
```

`processMainMessage` e `processRetryMessage` são parecidos de propósito,
mas cada um comita no seu próprio tópico (o principal vs. o degrau de retry
em que a mensagem está) e só o segundo aplica o atraso da escada — por isso
são dois métodos, não um com `if` no meio.

- [ ] **Step 4: Acrescentar o export no `index.ts`**

`packages/kafka/src/index.ts` (adicionar a última linha):
```ts
export * from './producer.js';
export * from './error-classification.js';
export * from './retry-headers.js';
export * from './consumer-runtime.js';
```

- [ ] **Step 5: Rodar todos os testes do pacote (requer infra no ar)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/kafka test
```

Esperado: PASS, 10 testes no total (7 da Task 3 + 3 do consumer runtime).
O teste de retry demora ~5-6s (aguarda o degrau `retry-5s`); os outros são
rápidos. Timeout total esperado do arquivo: bem dentro de 20s por teste.

- [ ] **Step 6: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/kafka build
pnpm --filter @ecommerce/kafka typecheck
pnpm --filter @ecommerce/kafka lint
```

- [ ] **Step 7: Commit**

```bash
git add packages/kafka
git commit -m "feat(kafka): consumer runtime com escada de retry e desvio para DLT"
```

---

## Verificação final da fase

- [ ] `pnpm --filter @ecommerce/idempotency test && pnpm --filter @ecommerce/outbox test && pnpm --filter @ecommerce/kafka test` — todos passam com `pnpm infra:up` no ar.
- [ ] `pnpm typecheck && pnpm lint` na raiz — sem erros nos 3 pacotes novos.
- [ ] `git log --oneline -5` mostra os 4 commits desta fase, todos em `master`.

Com isso, a Fase 1 (Order Service) já pode importar `@ecommerce/outbox` e
`@ecommerce/kafka`, e a Fase 3 (Payment) já pode importar
`@ecommerce/idempotency` para o inbox de consumo.
