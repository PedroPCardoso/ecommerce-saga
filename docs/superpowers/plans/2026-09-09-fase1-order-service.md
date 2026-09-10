# Fase 1 — Order Service (HTTP + Outbox) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar o Order Service: `POST /orders` (autenticado, idempotente),
`GET /orders/:id` (só o dono vê), Prisma + outbox atômico, relay publicando
`order.created` no Kafka de verdade.

**Architecture:** NestJS (sem `@nestjs/cli`, compilado com `tsc` como o
resto do monorepo) + Prisma 6 contra o Postgres `order_db`. `CreateOrderUseCase`
grava `Order` + linha de outbox + registro de idempotência HTTP na MESMA
transação Prisma, usando `insertOutboxRow` de `@ecommerce/outbox`.
`OutboxRelayService` roda o `OutboxRelay` (também de `@ecommerce/outbox`) como
singleton do processo, publicando via `EventProducer` de `@ecommerce/kafka`.
Autenticação JWT HS256 (dev): `customerId` vem SEMPRE do claim `sub` do
token, nunca do body (OWASP A01).

**Tech Stack:** NestJS 11 (`@nestjs/core`, `@nestjs/common`,
`@nestjs/platform-express`, `@nestjs/throttler`), Prisma 6, `jsonwebtoken`,
`pg`, `@ecommerce/contracts`/`outbox`/`kafka` (Fase 2), Vitest + `supertest`.

## Global Constraints

- Requer a Fase 2 completa e commitada (`@ecommerce/kafka`,
  `@ecommerce/outbox`, `@ecommerce/idempotency` publicados em `dist/`).
- Requer a Fase 0 completa (`pnpm infra:up` funcionando, Husky/commitlint
  ativos).
- `customerId` NUNCA vem do corpo da requisição — sempre do claim `sub` do
  JWT verificado no servidor (OWASP A01/A07, exigência organizacional).
- `totalAmountCents` é SEMPRE calculado no servidor a partir dos `items`
  enviados — nunca aceito do cliente (evita adulteração de preço, A05/A06).
- `GET /orders/:id` devolve 404 tanto para "não existe" quanto para "existe
  mas não é seu" — nunca 403 — para não permitir enumeração de IDs (A01).
- Testes de integração exigem `pnpm infra:up` no ar e a migration do
  Prisma aplicada (`ORDER_DATABASE_URL`, porta `15432`).
- Commits diretos em `master`, conventional commits.

---

### Task 1: Scaffolding, dependências, schema Prisma e migration

**Files:**
- Modify: `package.json` (raiz — acrescenta `dotenv-cli`)
- Create: `apps/order-service/package.json`
- Create: `apps/order-service/tsconfig.json`
- Create: `apps/order-service/vitest.config.ts`
- Create: `apps/order-service/prisma/schema.prisma`
- Create: `apps/order-service/prisma/migrations/<timestamp>_init/migration.sql` (gerado + editado)
- Create: `apps/order-service/src/env.ts`

**Interfaces:**
- Produces: `env` (objeto validado com Zod, usado por todas as tasks
  seguintes: `ORDER_DATABASE_URL`, `ORDER_SERVICE_PORT`, `KAFKA_BROKERS:
  string[]`, `KAFKA_CLIENT_ID_PREFIX`, `JWT_SECRET`, `JWT_ISSUER`).
- Produces: os modelos Prisma `Order`, `Outbox`, `IdempotencyKey` — a Task 2
  os usa via `PrismaClient` gerado.

- [ ] **Step 1: Dependência de tooling na raiz**

```bash
pnpm add -D dotenv-cli -w
```

Usada para carregar o `.env` da raiz nos comandos do Prisma e no `dev`/`start`
do serviço (o `.env` fica só na raiz do monorepo, não duplicado por app).

- [ ] **Step 2: `package.json` do serviço**

`apps/order-service/package.json`:
```json
{
  "name": "@ecommerce/order-service",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "lint": "eslint src",
    "test": "vitest run",
    "dev": "dotenv -e ../../.env -- tsx watch src/main.ts",
    "start": "dotenv -e ../../.env -- node dist/main.js",
    "prisma:generate": "dotenv -e ../../.env -- prisma generate",
    "prisma:migrate": "dotenv -e ../../.env -- prisma migrate dev",
    "prisma:deploy": "dotenv -e ../../.env -- prisma migrate deploy"
  },
  "dependencies": {
    "@ecommerce/contracts": "workspace:*",
    "@ecommerce/kafka": "workspace:*",
    "@ecommerce/outbox": "workspace:*",
    "@nestjs/common": "^11.0.1",
    "@nestjs/core": "^11.0.1",
    "@nestjs/platform-express": "^11.0.1",
    "@nestjs/throttler": "^6.3.1",
    "@prisma/client": "^6.1.0",
    "jsonwebtoken": "^9.0.2",
    "pg": "^8.13.1",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1",
    "zod": "^3.24.1"
  },
  "devDependencies": {
    "@nestjs/testing": "^11.0.1",
    "@types/jsonwebtoken": "^9.0.7",
    "@types/pg": "^8.11.10",
    "@types/supertest": "^6.0.2",
    "dotenv-cli": "^7.4.2",
    "prisma": "^6.1.0",
    "supertest": "^7.0.0",
    "tsx": "^4.19.2",
    "typescript": "^5.7.2",
    "vitest": "^2.1.8"
  }
}
```

- [ ] **Step 3: `tsconfig.json` e `vitest.config.ts`**

`apps/order-service/tsconfig.json`:
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

`apps/order-service/vitest.config.ts`:
```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.spec.ts'],
    environment: 'node',
    testTimeout: 20_000,
  },
});
```

- [ ] **Step 4: Instalar as dependências no workspace**

```bash
pnpm install
```

Esperado: `pnpm-lock.yaml` atualizado, sem erro de resolução (as
`workspace:*` resolvem para os pacotes da Fase 2 já commitados).

- [ ] **Step 5: Schema Prisma**

`apps/order-service/prisma/schema.prisma`:
```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("ORDER_DATABASE_URL")
}

model Order {
  id               String   @id
  customerId       String   @map("customer_id")
  items            Json
  totalAmountCents Int      @map("total_amount_cents")
  currency         String
  status           String   @default("PENDING")
  shippingAddress  Json     @map("shipping_address")
  createdAt        DateTime @default(now()) @map("created_at")
  updatedAt        DateTime @updatedAt @map("updated_at")

  @@map("orders")
}

model Outbox {
  id            BigInt    @id @default(autoincrement())
  eventId       String    @unique @map("event_id")
  aggregateId   String    @map("aggregate_id")
  aggregateType String    @map("aggregate_type")
  eventType     String    @map("event_type")
  payload       Json
  headers       Json      @default("{}")
  createdAt     DateTime  @default(now()) @map("created_at")
  publishedAt   DateTime? @map("published_at")
  attempts      Int       @default(0)

  @@map("outbox")
}

model IdempotencyKey {
  key            String
  customerId     String   @map("customer_id")
  orderId        String   @map("order_id")
  responseStatus Int      @map("response_status")
  responseBody   Json     @map("response_body")
  createdAt      DateTime @default(now()) @map("created_at")

  @@id([key, customerId])
  @@map("idempotency_keys")
}
```

`Order.id` **sem** `@default(uuid())` de propósito: o `CreateOrderUseCase`
(Task 2) gera o `orderId` explicitamente ANTES da transação, porque o mesmo
valor precisa ir para a linha de `Order`, para o `aggregateId` do envelope
de outbox e para o registro de idempotência — os três na mesma transação.

- [ ] **Step 6: Gerar a migration sem aplicar (para editar o índice parcial)**

```bash
cd apps/order-service
pnpm prisma:generate
pnpm exec dotenv -e ../../.env -- prisma migrate dev --create-only --name init
```

Isso cria `prisma/migrations/<timestamp>_init/migration.sql`. Abra o arquivo
gerado e troque o índice simples que o Prisma criou para `outbox` (procure
por `CREATE INDEX ... ON "outbox"("created_at")` — pode não existir ainda,
já que o schema não declara `@@index` nenhum; então ACRESCENTE ao final do
arquivo):

```sql
-- Índice PARCIAL: o relay só pergunta pelas pendentes, e essa consulta roda
-- a cada 200ms para sempre. Sem o WHERE, o índice cresce com o histórico
-- inteiro e a consulta degrada junto. O Prisma DSL não expressa índice
-- parcial diretamente — por isso a edição manual desta migration.
CREATE INDEX "outbox_pending_idx" ON "outbox" ("created_at") WHERE "published_at" IS NULL;
```

- [ ] **Step 7: Aplicar a migration**

```bash
pnpm exec dotenv -e ../../.env -- prisma migrate deploy
cd ../..
```

Esperado: saída confirmando 1 migration aplicada, sem erro. Confirme:

```bash
docker exec -it $(docker ps -qf "name=postgres-order") psql -U order_svc -d order_db -c "\dt"
```

Esperado: tabelas `orders`, `outbox`, `idempotency_keys`,
`_prisma_migrations` listadas.

- [ ] **Step 8: Loader de ambiente validado**

`apps/order-service/src/env.ts`:
```ts
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  ORDER_SERVICE_PORT: z.coerce.number().int().positive().default(3000),
  ORDER_DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) => value.split(',')),
  KAFKA_CLIENT_ID_PREFIX: z.string().min(1).default('ecommerce'),
  JWT_SECRET: z.string().min(1),
  JWT_ISSUER: z.string().min(1),
});

export const env = envSchema.parse(process.env);
```

- [ ] **Step 9: Commit**

```bash
git add package.json pnpm-lock.yaml apps/order-service
git commit -m "chore(order-service): scaffolding, prisma schema e migration inicial"
```

---

### Task 2: Camada de aplicação — `CreateOrderUseCase`, `PrismaService`, `OutboxRelayService`

**Files:**
- Create: `apps/order-service/src/infrastructure/prisma.service.ts`
- Create: `apps/order-service/src/infrastructure/outbox-relay.service.ts`
- Create: `apps/order-service/src/application/create-order.use-case.ts`
- Test: `apps/order-service/test/create-order.integration.spec.ts`

**Interfaces:**
- Consumes: `insertOutboxRow`/`OutboxRelay` de `@ecommerce/outbox`,
  `EventProducer` de `@ecommerce/kafka`, `createEvent`/`orderEvents`/
  `findDefinition` de `@ecommerce/contracts`, `env` da Task 1.
- Produces: `CreateOrderUseCase.execute(input): Promise<{ result:
  CreateOrderResult; replayed: boolean }>` — consumido pelo
  `OrdersController` na Task 3.

- [ ] **Step 1: Escrever o teste de integração antes da implementação**

`apps/order-service/test/create-order.integration.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { env } from '../src/env.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OutboxRelayService } from '../src/infrastructure/outbox-relay.service.js';
import { CreateOrderUseCase } from '../src/application/create-order.use-case.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 2, unitPriceCents: 4990 };
const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

describe('CreateOrderUseCase (integração — Postgres + Kafka reais, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const relay = new OutboxRelayService();
  const useCase = new CreateOrderUseCase(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.idempotencyKey.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.order.deleteMany();
    await relay.onModuleInit();
  });

  afterAll(async () => {
    await relay.onModuleDestroy();
    await prisma.onModuleDestroy();
  });

  it('grava Order + Outbox + IdempotencyKey na mesma transação', async () => {
    const customerId = randomUUID();
    const idempotencyKey = randomUUID();

    const { result, replayed } = await useCase.execute({
      customerId,
      idempotencyKey,
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });

    expect(replayed).toBe(false);
    expect(result.status).toBe('PENDING');

    const order = await prisma.client.order.findUnique({ where: { id: result.orderId } });
    expect(order?.totalAmountCents).toBe(9980); // 2 * 4990 — calculado no servidor

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: result.orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('order.created');
  });

  it('Idempotency-Key repetida devolve o MESMO orderId, sem criar segundo pedido', async () => {
    const customerId = randomUUID();
    const idempotencyKey = randomUUID();

    const first = await useCase.execute({
      customerId,
      idempotencyKey,
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });
    const second = await useCase.execute({
      customerId,
      idempotencyKey,
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });

    expect(second.replayed).toBe(true);
    expect(second.result.orderId).toBe(first.result.orderId);

    const count = await prisma.client.order.count({ where: { customerId } });
    expect(count).toBe(1);
  });

  it('o relay publica order.created no tópico ecommerce.orders.v1 com a chave = orderId', async () => {
    const customerId = randomUUID();
    const { result } = await useCase.execute({
      customerId,
      idempotencyKey: randomUUID(),
      items: [ITEM],
      currency: 'BRL',
      shippingAddress: ADDRESS,
    });

    const kafka = new Kafka({
      clientId: 'order-service-test-reader',
      brokers: env.KAFKA_BROKERS,
      logLevel: logLevel.NOTHING,
    });
    const consumer = kafka.consumer({ groupId: `order-service-test-reader-${result.orderId}` });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ecommerce.orders.v1', fromBeginning: true });

    const found = await new Promise<{ key: string | null; eventType: string }>((resolve) => {
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          const value = JSON.parse(message.value!.toString());
          if (value.aggregateId === result.orderId) {
            resolve({ key: message.key?.toString() ?? null, eventType: value.eventType });
          }
        },
      });
    });
    await consumer.disconnect();

    expect(found.key).toBe(result.orderId);
    expect(found.eventType).toBe('order.created');
  }, 15_000);
});
```

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/order-service test
```

Esperado: FALHA (módulos ainda não existem).

- [ ] **Step 3: Implementar `PrismaService`**

`apps/order-service/src/infrastructure/prisma.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService implements OnModuleInit, OnModuleDestroy {
  readonly client = new PrismaClient();

  async onModuleInit(): Promise<void> {
    await this.client.$connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.client.$disconnect();
  }
}
```

- [ ] **Step 4: Implementar `OutboxRelayService`**

`apps/order-service/src/infrastructure/outbox-relay.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { OutboxRelay } from '@ecommerce/outbox';
import { EventProducer } from '@ecommerce/kafka';
import { findDefinition, type UnknownEnvelope } from '@ecommerce/contracts';
import { env } from '../env.js';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: env.ORDER_DATABASE_URL });
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-order-service-relay`,
  });
  private readonly relay = new OutboxRelay({
    pool: this.pool,
    publish: async (row) => {
      const envelope = row.envelope as UnknownEnvelope;
      const definition = findDefinition(envelope.eventType, envelope.eventVersion);
      if (!definition) {
        throw new Error(
          `Evento ${envelope.eventType}@${envelope.eventVersion} sem tópico declarado em @ecommerce/contracts`,
        );
      }
      await this.producer.publish(definition.topic, envelope, row.headers);
    },
  });

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.relay.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.relay.stop();
    await this.producer.disconnect();
    await this.pool.end();
  }
}
```

- [ ] **Step 5: Implementar `CreateOrderUseCase`**

`apps/order-service/src/application/create-order.use-case.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import type { Prisma } from '@prisma/client';
import { createEvent, orderEvents, type Address, type Currency, type OrderItem } from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { PrismaService } from '../infrastructure/prisma.service.js';

export interface CreateOrderInput {
  customerId: string;
  idempotencyKey: string;
  items: OrderItem[];
  currency: Currency;
  shippingAddress: Address;
}

export interface CreateOrderResult {
  orderId: string;
  status: string;
  createdAt: string;
}

@Injectable()
export class CreateOrderUseCase {
  constructor(private readonly prisma: PrismaService) {}

  async execute(input: CreateOrderInput): Promise<{ result: CreateOrderResult; replayed: boolean }> {
    const existing = await this.prisma.client.idempotencyKey.findUnique({
      where: { key_customerId: { key: input.idempotencyKey, customerId: input.customerId } },
    });
    if (existing) {
      return { result: existing.responseBody as unknown as CreateOrderResult, replayed: true };
    }

    const orderId = randomUUID();
    // SEMPRE calculado no servidor — aceitar totalAmountCents do cliente permitiria
    // adulterar o preço do pedido (A05/A06).
    const totalAmountCents = input.items.reduce(
      (sum, item) => sum + item.unitPriceCents * item.quantity,
      0,
    );
    const createdAt = new Date();

    const envelope = createEvent(orderEvents.orderCreated, {
      aggregateId: orderId,
      correlationId: orderId,
      producer: 'order-service@0.1.0',
      payload: {
        orderId,
        customerId: input.customerId,
        items: input.items,
        totalAmountCents,
        currency: input.currency,
        shippingAddress: input.shippingAddress,
      },
    });

    const result: CreateOrderResult = {
      orderId,
      status: 'PENDING',
      createdAt: createdAt.toISOString(),
    };

    await this.prisma.client.$transaction(async (tx) => {
      await tx.order.create({
        data: {
          id: orderId,
          customerId: input.customerId,
          items: input.items,
          totalAmountCents,
          currency: input.currency,
          status: 'PENDING',
          shippingAddress: input.shippingAddress,
          createdAt,
        },
      });

      await insertOutboxRow(tx, {
        eventId: envelope.eventId,
        aggregateId: orderId,
        aggregateType: 'order',
        eventType: 'order.created',
        envelope,
      });

      await tx.idempotencyKey.create({
        data: {
          key: input.idempotencyKey,
          customerId: input.customerId,
          orderId,
          responseStatus: 201,
          responseBody: result as unknown as Prisma.InputJsonValue,
          createdAt,
        },
      });
    });

    return { result, replayed: false };
  }
}
```

- [ ] **Step 6: Rodar o teste (requer infra no ar e migration aplicada)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/contracts build
pnpm --filter @ecommerce/outbox build
pnpm --filter @ecommerce/kafka build
pnpm --filter @ecommerce/order-service test
```

Esperado: PASS, 3 testes.

- [ ] **Step 7: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/order-service build
pnpm --filter @ecommerce/order-service typecheck
pnpm --filter @ecommerce/order-service lint
```

- [ ] **Step 8: Commit**

```bash
git add apps/order-service
git commit -m "feat(order-service): CreateOrderUseCase com outbox atômico e relay"
```

---

### Task 3: Camada HTTP — autenticação JWT, controller, health, e2e

**Files:**
- Create: `apps/order-service/src/api/auth/jwt-auth.guard.ts`
- Create: `apps/order-service/src/api/auth/current-customer.decorator.ts`
- Create: `apps/order-service/src/api/orders.controller.ts`
- Create: `apps/order-service/src/health/health.controller.ts`
- Create: `apps/order-service/src/app.module.ts`
- Create: `apps/order-service/src/main.ts`
- Test: `apps/order-service/test/orders.e2e.spec.ts`

**Interfaces:**
- Consumes: `CreateOrderUseCase`, `PrismaService`, `OutboxRelayService` da
  Task 2; `env` da Task 1.
- Produces: `AuthenticatedRequest` (tipo), `JwtAuthGuard`, `CurrentCustomer`
  — reutilizáveis por outros endpoints futuros deste serviço.

- [ ] **Step 1: Escrever o teste e2e antes da implementação**

`apps/order-service/test/orders.e2e.spec.ts`:
```ts
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
```

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/order-service test
```

Esperado: FALHA (módulos `app.module.js` etc. não existem).

- [ ] **Step 3: Implementar o guard JWT**

`apps/order-service/src/api/auth/jwt-auth.guard.ts`:
```ts
import { type CanActivate, type ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import jwt from 'jsonwebtoken';
import type { Request } from 'express';
import { z } from 'zod';
import { env } from '../../env.js';

const claimsSchema = z.object({ sub: z.string().uuid() });

export interface AuthenticatedRequest extends Request {
  customerId: string;
}

/**
 * JWT HS256 — suficiente para dev/estudo. Em produção seria RS256 via IdP
 * externo com rotação de chave (dívida documentada, fora de escopo aqui).
 * `customerId` é extraído do claim `sub`, nunca aceito de outro lugar —
 * é o que impede um cliente de ler/criar pedido em nome de outro (A01).
 */
@Injectable()
export class JwtAuthGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const header = request.headers.authorization;
    if (!header?.startsWith('Bearer ')) {
      throw new UnauthorizedException('Authorization header ausente ou malformado');
    }

    const token = header.slice('Bearer '.length);
    let payload: unknown;
    try {
      payload = jwt.verify(token, env.JWT_SECRET, { issuer: env.JWT_ISSUER });
    } catch {
      throw new UnauthorizedException('Token inválido ou expirado');
    }

    const claims = claimsSchema.safeParse(payload);
    if (!claims.success) {
      throw new UnauthorizedException('Token sem claim "sub" válida');
    }

    request.customerId = claims.data.sub;
    return true;
  }
}
```

- [ ] **Step 4: Implementar o decorator `@CurrentCustomer()`**

`apps/order-service/src/api/auth/current-customer.decorator.ts`:
```ts
import { createParamDecorator, type ExecutionContext } from '@nestjs/common';
import type { AuthenticatedRequest } from './jwt-auth.guard.js';

export const CurrentCustomer = createParamDecorator(
  (_: unknown, ctx: ExecutionContext): string => {
    const request = ctx.switchToHttp().getRequest<AuthenticatedRequest>();
    return request.customerId;
  },
);
```

- [ ] **Step 5: Implementar o controller**

`apps/order-service/src/api/orders.controller.ts`:
```ts
import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Post,
  UseGuards,
} from '@nestjs/common';
import { Throttle } from '@nestjs/throttler';
import { z } from 'zod';
import { addressSchema, currencySchema, orderItemSchema } from '@ecommerce/contracts';
import { JwtAuthGuard } from './auth/jwt-auth.guard.js';
import { CurrentCustomer } from './auth/current-customer.decorator.js';
import { CreateOrderUseCase } from '../application/create-order.use-case.js';
import { PrismaService } from '../infrastructure/prisma.service.js';

const createOrderBodySchema = z.object({
  items: z.array(orderItemSchema).min(1).max(100),
  currency: currencySchema,
  shippingAddress: addressSchema,
});

@Controller('orders')
@UseGuards(JwtAuthGuard)
export class OrdersController {
  constructor(
    private readonly createOrder: CreateOrderUseCase,
    private readonly prisma: PrismaService,
  ) {}

  @Post()
  @HttpCode(201)
  @Throttle({ default: { limit: 10, ttl: 60_000 } })
  async create(
    @CurrentCustomer() customerId: string,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
  ) {
    if (!idempotencyKey) {
      throw new BadRequestException('Header Idempotency-Key é obrigatório');
    }

    const parsed = createOrderBodySchema.safeParse(body);
    if (!parsed.success) {
      throw new BadRequestException(
        parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`),
      );
    }

    const { result } = await this.createOrder.execute({
      customerId,
      idempotencyKey,
      items: parsed.data.items,
      currency: parsed.data.currency,
      shippingAddress: parsed.data.shippingAddress,
    });

    return result;
  }

  @Get(':id')
  async findOne(@CurrentCustomer() customerId: string, @Param('id', ParseUUIDPipe) id: string) {
    const order = await this.prisma.client.order.findUnique({ where: { id } });

    // 404 tanto para "não existe" quanto para "não é seu" — nunca 403 —
    // para não permitir enumeração de pedidos alheios (A01).
    if (!order || order.customerId !== customerId) {
      throw new NotFoundException('Pedido não encontrado');
    }

    return {
      orderId: order.id,
      status: order.status,
      totalAmountCents: order.totalAmountCents,
      currency: order.currency,
      createdAt: order.createdAt.toISOString(),
    };
  }
}
```

- [ ] **Step 6: Health controller**

`apps/order-service/src/health/health.controller.ts`:
```ts
import { Controller, Get } from '@nestjs/common';

@Controller('health')
export class HealthController {
  @Get('live')
  live(): { status: string } {
    return { status: 'ok' };
  }
}
```

- [ ] **Step 7: `app.module.ts`**

`apps/order-service/src/app.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import { OrdersController } from './api/orders.controller.js';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { CreateOrderUseCase } from './application/create-order.use-case.js';

@Module({
  imports: [ThrottlerModule.forRoot({ throttlers: [{ ttl: 60_000, limit: 100 }] })],
  controllers: [OrdersController, HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    CreateOrderUseCase,
    { provide: APP_GUARD, useClass: ThrottlerGuard },
  ],
})
export class AppModule {}
```

- [ ] **Step 8: `main.ts`**

`apps/order-service/src/main.ts`:
```ts
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { env } from './env.js';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  await app.listen(env.ORDER_SERVICE_PORT);
  console.log(`[order-service] ouvindo na porta ${env.ORDER_SERVICE_PORT}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[order-service] recebido ${signal}, encerrando graciosamente`);
    await app.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

bootstrap().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
```

- [ ] **Step 9: Rodar a suite e2e completa (requer infra no ar e migration aplicada)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/order-service test
```

Esperado: PASS, todos os testes das Tasks 2 e 3 (10 no total: 3 + 7).

- [ ] **Step 10: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/order-service build
pnpm --filter @ecommerce/order-service typecheck
pnpm --filter @ecommerce/order-service lint
```

- [ ] **Step 11: Verificação manual ponta a ponta**

```bash
pnpm --filter @ecommerce/order-service dev &
sleep 2
TOKEN=$(node -e "console.log(require('jsonwebtoken').sign({sub:'018f3f4e-0000-7000-8000-000000000099'}, 'dev-only-not-a-real-secret-change-me', {issuer:'ecommerce-local', expiresIn:'15m'}))")
curl -s -X POST http://localhost:3000/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $(node -e 'console.log(require("crypto").randomUUID())')" \
  -H "Content-Type: application/json" \
  -d '{"items":[{"sku":"BOOK-001","name":"Livro","quantity":1,"unitPriceCents":4990}],"currency":"BRL","shippingAddress":{"street":"Rua X","number":"1","district":"Centro","city":"SP","state":"SP","zipCode":"01000-000","country":"BR"}}'
kill %1
```

Esperado: resposta `201` com `{ orderId, status: "PENDING", createdAt }`.
Confirme no Kafka UI (http://localhost:8080) que uma mensagem apareceu em
`ecommerce.orders.v1`.

- [ ] **Step 12: Commit**

```bash
git add apps/order-service
git commit -m "feat(order-service): API HTTP com JWT, idempotência e GET /orders/:id"
```

---

## Verificação final da fase

- [ ] `pnpm --filter @ecommerce/order-service test` — todos os testes
      passam com `pnpm infra:up` no ar e a migration aplicada.
- [ ] `pnpm typecheck && pnpm lint` na raiz — sem erros.
- [ ] Critério de pronto do `docs/PLAN.md` Fase 1: um POST cria a linha em
      `orders` e `order.created` aparece no Kafka UI com a chave certa; um
      segundo POST com a mesma `Idempotency-Key` devolve o mesmo `orderId`
      sem criar nada — ambos cobertos pelos testes e pela Step 11.

Com o Order Service publicando `order.created` de verdade, a Fase 3
(Payment Service) já tem o que consumir.
