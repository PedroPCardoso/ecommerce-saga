# Fase 4 — Inventory Service + 1ª compensação (parte Inventory) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar o Inventory Service: consome `order.created` (só para
aprender os itens do pedido) e `payment.approved` (o gatilho real da
reserva), decide `stock.reserved` ou `stock.unavailable` por um gatilho
determinístico de SKU, e publica o resultado via outbox. Cobre em especial a
recuperação por retry quando `payment.approved` chega antes de
`order.created` — a race condition central desta fase.

**Escopo e limite deste documento:** o título da Fase 4 no `docs/PLAN.md`
("Inventory + primeira compensação") inclui também o Payment Service passar
a consumir `stock.unavailable` e emitir `payment.refunded` — isso é
responsabilidade do **Payment Service** (Fase 3), já provisionado no
`packages/contracts` (`SUBSCRIPTIONS[CONSUMER_GROUPS.payment]` já inclui
`TOPICS.inventory`). Este documento cobre **só o Inventory Service**: ele
produz o `stock.unavailable` que dispara aquela compensação, mas não
implementa o lado que reage a ele. Ver "Verificação final da fase" para o
que isso implica no critério de pronto.

**Architecture:** NestJS (sem `@nestjs/cli`, compilado com `tsc`, mesmo
padrão dos demais serviços) + Prisma 6 contra o Postgres `inventory_db`.
Dois handlers de aplicação, injetáveis mas chamáveis diretamente em teste:
`OrderCreatedHandler` (grava `KnownOrder`, nenhum evento de domínio sai
daqui) e `PaymentApprovedHandler` (decide reserva/indisponibilidade e
publica via outbox). Um `InventoryEventRouter` decide qual handler chamar a
partir de `envelope.eventType`; um `InventoryConsumerService` liga isso a um
`KafkaConsumerRuntime` de `@ecommerce/kafka` assinando
`ecommerce.orders.v1` e `ecommerce.payments.v1` com o grupo
`inventory-service`. `OutboxRelayService` publica de verdade, mesmo padrão
do Order Service. Sem endpoint HTTP de negócio — só `/health/live`.

**A race condition central:** `payment.approved` não carrega os itens do
pedido (só dados de pagamento — ver comentário em
`packages/contracts/src/topics.ts`, `SUBSCRIPTIONS[CONSUMER_GROUPS.inventory]`).
O Inventory só sabe o que reservar através de `order.created`. Nada garante
que `order.created` seja processado antes do `payment.approved`
correspondente chegar (tópicos diferentes, sem ordem garantida entre si).
Quando `PaymentApprovedHandler` não encontra o `KnownOrder`, ele lança um
erro comum (sem `.permanent = true`) **dentro** da transação Prisma onde
`markProcessed` já rodou — o Prisma faz rollback de tudo, inclusive do
registro de idempotência, e `classifyError` (`@ecommerce/kafka`) trata esse
erro como retriável por padrão. A escada 5s/1m/10m dá tempo para
`order.created` chegar antes de desistir.

**Tech Stack:** NestJS 11 (`@nestjs/common`, `@nestjs/core`,
`@nestjs/platform-express`), Prisma 6, `pg`, `@ecommerce/contracts`/
`kafka`/`outbox`/`idempotency` (Fase 2), Vitest + `supertest` (só para o
`/health/live`).

## Global Constraints

- Requer a Fase 2 completa (`@ecommerce/kafka`, `@ecommerce/outbox`,
  `@ecommerce/idempotency`) e a Fase 1 (Order Service publicando
  `order.created` de verdade). Os testes deste plano publicam
  `order.created`/`payment.approved` diretamente via Kafka (produtor de
  teste) — não dependem do Payment Service (Fase 3) estar implementado para
  rodar.
- `payment.approved` NUNCA carrega SKUs — o Inventory aprende os itens só
  por `order.created`. Ignore qualquer `eventType` que não seja
  `order.created` no tópico `orders` (ex. `order.confirmed`,
  `order.cancelled`) e qualquer `eventType` que não seja `payment.approved`
  no tópico `payments` (ex. `payment.failed` — matriz de compensação:
  "ninguém compensa, nada foi efetivado"; `payment.refunded`, que a Fase 5
  talvez adicione).
- "Ainda não vi o `order.created` deste pedido" é **sempre** erro
  retriável (nunca `.permanent = true`), e o `markProcessed` correspondente
  **precisa** estar na mesma transação Prisma que lança esse erro — é o
  rollback que garante que o retry não seja descartado como "já
  processado" sem nunca ter reservado nada.
- Gatilho determinístico (PLAN.md 4.5): SKU que começa com `OUT-` está
  indisponível. Tudo ou nada por pedido — se qualquer item for
  indisponível, **nenhum** item do pedido é reservado (nem os disponíveis).
- Reserva tem `expiresAt` = `reservedAt + 30min` — prazo arbitrário
  documentado; o sweeper que efetivamente expira reservas é a Fase 6.
- Sem endpoint HTTP de negócio — só `/health/live`, sem autenticação
  (nenhum cliente externo chama este serviço via HTTP).
- Consumir `shipment.failed` para publicar `stock.released` é Fase 5 — não
  implementar agora. `TOPICS.shipping` já está em
  `SUBSCRIPTIONS[CONSUMER_GROUPS.inventory]` (logo `pnpm topics:create` já
  criou os tópicos de retry/DLT correspondentes), mas o
  `KafkaConsumerRuntime` deste serviço só assina `orders` e `payments`
  nesta fase.
- Segurança (OWASP, exigência organizacional — dado com I/O externo):
  payload de evento é entrada não confiável e chega ao handler já validado
  pelo `parseEvent`/Zod dentro do `KafkaConsumerRuntime` (A05); erro
  desconhecido classifica como retriável, nunca conserta silenciosamente
  travando uma reserva incorreta (A10, fail secure); dado de `items`
  recuperado do próprio banco é revalidado com `reservedItemSchema` antes
  de decidir a reserva — corrupção local vira erro permanente, não
  retriável, porque retry não conserta dado corrompido (A08); nenhum PII
  trafega pelos eventos deste serviço (só `sku`/`quantity`).
- Testes de integração exigem `pnpm infra:up` no ar e a migration do
  Prisma aplicada (`INVENTORY_DATABASE_URL`, porta `15434`).
- `dotenv-cli` já foi adicionado na raiz pela Fase 1 — reaproveite, não
  reinstale.
- Commits diretos em `master`, conventional commits, um por Task.

---

### Task 1: Scaffolding, dependências, schema Prisma e migration

**Files:**
- Create: `apps/inventory-service/package.json`
- Create: `apps/inventory-service/tsconfig.json`
- Create: `apps/inventory-service/vitest.config.ts`
- Create: `apps/inventory-service/prisma/schema.prisma`
- Create: `apps/inventory-service/prisma/migrations/<timestamp>_init/migration.sql` (gerado + editado)
- Create: `apps/inventory-service/src/env.ts`

**Interfaces:**
- Produces: `env` (objeto validado com Zod: `INVENTORY_DATABASE_URL`,
  `INVENTORY_SERVICE_PORT`, `KAFKA_BROKERS: string[]`,
  `KAFKA_CLIENT_ID_PREFIX`) — usado por todas as tasks seguintes.
- Produces: os modelos Prisma `KnownOrder`, `StockReservation`, `Outbox`,
  `ProcessedMessage` — a Task 2 os usa via `PrismaClient` gerado.

- [ ] **Step 1: `package.json` do serviço**

`apps/inventory-service/package.json`:
```json
{
  "name": "@ecommerce/inventory-service",
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
    "@ecommerce/idempotency": "workspace:*",
    "@ecommerce/kafka": "workspace:*",
    "@ecommerce/outbox": "workspace:*",
    "@nestjs/common": "^11.0.1",
    "@nestjs/core": "^11.0.1",
    "@nestjs/platform-express": "^11.0.1",
    "@prisma/client": "^6.1.0",
    "pg": "^8.13.1",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1",
    "zod": "^3.24.1"
  },
  "devDependencies": {
    "@nestjs/testing": "^11.0.1",
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

Sem `jsonwebtoken`/`@nestjs/throttler`: este serviço não expõe HTTP de
negócio, só `/health/live` — nada aqui precisa de autenticação ou rate
limit. `@ecommerce/idempotency` entra pela primeira vez neste serviço (o
Order Service, Fase 1, só produz eventos — não consome).

- [ ] **Step 2: `tsconfig.json` e `vitest.config.ts`**

`apps/inventory-service/tsconfig.json`:
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

`apps/inventory-service/vitest.config.ts`:
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

- [ ] **Step 3: Instalar as dependências no workspace**

```bash
pnpm install
```

Esperado: `pnpm-lock.yaml` atualizado, sem erro de resolução.

- [ ] **Step 4: Schema Prisma**

`apps/inventory-service/prisma/schema.prisma`:
```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("INVENTORY_DATABASE_URL")
}

// Aprendido via order.created — nenhum evento de domínio nasce daqui.
// orderId é a PK direta (vem do evento, não gerado aqui).
model KnownOrder {
  orderId   String   @id @map("order_id")
  items     Json
  createdAt DateTime @default(now()) @map("created_at")

  @@map("known_orders")
}

// Criada quando payment.approved decide que dá para reservar.
// id gerado pela aplicação com randomUUID() — mesmo padrão do Order.id no
// Order Service: gerado ANTES da transação para poder ir também no
// aggregateId do envelope de outbox.
model StockReservation {
  id         String   @id
  orderId    String   @map("order_id")
  items      Json
  status     String   @default("RESERVED")
  expiresAt  DateTime @map("expires_at")
  reservedAt DateTime @default(now()) @map("reserved_at")
  createdAt  DateTime @default(now()) @map("created_at")

  @@map("stock_reservations")
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

// Espelha packages/idempotency/src/schema.sql. A chave é o par
// (event_id, consumer_group) — não só event_id — porque outro consumer
// group (ex. notification-service) processa o MESMO evento sem que um
// bloqueie o outro.
model ProcessedMessage {
  eventId       String   @map("event_id")
  consumerGroup String   @map("consumer_group")
  processedAt   DateTime @default(now()) @map("processed_at")

  @@id([eventId, consumerGroup])
  @@map("processed_messages")
}
```

`eventId`/`orderId` ficam `String` sem `@db.Uuid` — mesma convenção do
`Order.id` no Order Service (Fase 1): a coluna física fica `text`, e é o
Zod (`@ecommerce/contracts`) que garante o formato UUID antes do dado
chegar aqui. O `$1::uuid` usado internamente por `insertOutboxRow`/
`markProcessed` só converte o parâmetro da query, não exige a coluna
tipada `uuid`.

- [ ] **Step 5: Gerar a migration sem aplicar (para editar o índice parcial)**

```bash
cd apps/inventory-service
pnpm prisma:generate
pnpm exec dotenv -e ../../.env -- prisma migrate dev --create-only --name init
```

Abra `prisma/migrations/<timestamp>_init/migration.sql` e acrescente ao
final (mesmo motivo do Order Service: índice parcial que o Prisma DSL não
expressa):

```sql
-- Índice PARCIAL: o relay só pergunta pelas pendentes, e essa consulta roda
-- a cada 200ms para sempre. Sem o WHERE, o índice cresce com o histórico
-- inteiro e a consulta degrada junto.
CREATE INDEX "outbox_pending_idx" ON "outbox" ("created_at") WHERE "published_at" IS NULL;
```

- [ ] **Step 6: Aplicar a migration**

```bash
pnpm exec dotenv -e ../../.env -- prisma migrate deploy
cd ../..
```

Confirme:

```bash
docker exec -it ecommerce-pg-inventory psql -U inventory_svc -d inventory_db -c "\dt"
```

Esperado: tabelas `known_orders`, `stock_reservations`, `outbox`,
`processed_messages`, `_prisma_migrations`.

- [ ] **Step 7: Loader de ambiente validado**

`apps/inventory-service/src/env.ts`:
```ts
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  INVENTORY_SERVICE_PORT: z.coerce.number().int().positive().default(3002),
  INVENTORY_DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) => value.split(',')),
  KAFKA_CLIENT_ID_PREFIX: z.string().min(1).default('ecommerce'),
});

export const env = envSchema.parse(process.env);
```

Sem `JWT_SECRET`/`JWT_ISSUER`: este serviço não tem HTTP autenticado.

- [ ] **Step 8: Commit**

```bash
git add apps/inventory-service pnpm-lock.yaml
git commit -m "chore(inventory-service): scaffolding, prisma schema e migration inicial"
```

---

### Task 2: `PrismaService`, `OutboxRelayService` e o handler de `order.created`

**Files:**
- Create: `apps/inventory-service/src/infrastructure/prisma.service.ts`
- Create: `apps/inventory-service/src/infrastructure/outbox-relay.service.ts`
- Create: `apps/inventory-service/src/application/order-created.handler.ts`
- Test: `apps/inventory-service/test/order-created-handler.integration.spec.ts`

**Interfaces:**
- Consumes: `markProcessed` de `@ecommerce/idempotency`; `CONSUMER_GROUPS`,
  `EventOf`, `orderEvents` de `@ecommerce/contracts`; `env` da Task 1.
- Produces: `OrderCreatedHandler.handle(envelope): Promise<void>` —
  consumido pelo `InventoryEventRouter` na Task 4 e chamado diretamente
  pelos testes de integração desta e da próxima Task.

- [ ] **Step 1: Escrever o teste de integração antes da implementação**

`apps/inventory-service/test/order-created-handler.integration.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, items: Array<{ sku: string; quantity: number }>) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: items.map((item) => ({ ...item, name: 'Item de teste', unitPriceCents: 1000 })),
      totalAmountCents: items.reduce((sum, item) => sum + item.quantity * 1000, 0),
      currency: 'BRL',
      shippingAddress: ADDRESS,
    },
  });
}

describe('OrderCreatedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const handler = new OrderCreatedHandler(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.stockReservation.deleteMany();
    await prisma.client.knownOrder.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('grava KnownOrder só com sku+quantity — nunca publica evento de domínio', async () => {
    const orderId = randomUUID();
    const envelope = makeOrderCreated(orderId, [
      { sku: 'BOOK-001', quantity: 2 },
      { sku: 'BOOK-002', quantity: 1 },
    ]);

    await handler.handle(envelope);

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.items).toEqual([
      { sku: 'BOOK-001', quantity: 2 },
      { sku: 'BOOK-002', quantity: 1 },
    ]);

    const outboxCount = await prisma.client.outbox.count();
    expect(outboxCount).toBe(0);
  });

  it('reentrega do MESMO evento (mesmo eventId) não falha nem duplica — idempotência', async () => {
    const orderId = randomUUID();
    const envelope = makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]);

    await handler.handle(envelope);
    await handler.handle(envelope);

    const count = await prisma.client.knownOrder.count({ where: { orderId } });
    expect(count).toBe(1);

    const processed = await prisma.client.processedMessage.count({
      where: { eventId: envelope.eventId },
    });
    expect(processed).toBe(1);
  });
});
```

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/inventory-service test
```

Esperado: FALHA (módulos ainda não existem).

- [ ] **Step 3: Implementar `PrismaService`**

`apps/inventory-service/src/infrastructure/prisma.service.ts`:
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

`apps/inventory-service/src/infrastructure/outbox-relay.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { OutboxRelay } from '@ecommerce/outbox';
import { EventProducer } from '@ecommerce/kafka';
import { findDefinition, type UnknownEnvelope } from '@ecommerce/contracts';
import { env } from '../env.js';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: env.INVENTORY_DATABASE_URL });
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-inventory-service-relay`,
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

- [ ] **Step 5: Implementar `OrderCreatedHandler`**

`apps/inventory-service/src/application/order-created.handler.ts`:
```ts
import { Injectable } from '@nestjs/common';
import type { Prisma } from '@prisma/client';
import { markProcessed } from '@ecommerce/idempotency';
import { CONSUMER_GROUPS, type EventOf, orderEvents } from '@ecommerce/contracts';
import { PrismaService } from '../infrastructure/prisma.service.js';

export type OrderCreatedEvent = EventOf<typeof orderEvents.orderCreated>;

/**
 * Só aprende os itens do pedido — NENHUM evento de domínio é publicado
 * aqui. O gatilho real da reserva é sempre `payment.approved`
 * (PaymentApprovedHandler). `payment.approved` não carrega SKUs, então é
 * este handler que dá ao Inventory o "o que reservar" — ver o comentário
 * em `packages/contracts/src/topics.ts` (SUBSCRIPTIONS do inventory-service).
 */
@Injectable()
export class OrderCreatedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: OrderCreatedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.inventory);
      if (!isNew) return; // reentrega do mesmo evento — já aprendemos estes itens

      const items = envelope.payload.items.map((item) => ({
        sku: item.sku,
        quantity: item.quantity,
      }));

      await tx.knownOrder.upsert({
        where: { orderId: envelope.payload.orderId },
        create: {
          orderId: envelope.payload.orderId,
          items: items as unknown as Prisma.InputJsonValue,
        },
        update: {
          items: items as unknown as Prisma.InputJsonValue,
        },
      });
    });
  }
}
```

- [ ] **Step 6: Rodar o teste (requer infra no ar e migration aplicada)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/contracts build
pnpm --filter @ecommerce/idempotency build
pnpm --filter @ecommerce/outbox build
pnpm --filter @ecommerce/kafka build
pnpm --filter @ecommerce/inventory-service test
```

Esperado: PASS, 2 testes.

- [ ] **Step 7: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/inventory-service build
pnpm --filter @ecommerce/inventory-service typecheck
pnpm --filter @ecommerce/inventory-service lint
```

- [ ] **Step 8: Commit**

```bash
git add apps/inventory-service
git commit -m "feat(inventory-service): OrderCreatedHandler aprende itens do pedido"
```

---

### Task 3: `PaymentApprovedHandler` — reserva, indisponibilidade e a race condition

**Files:**
- Create: `apps/inventory-service/src/application/payment-approved.handler.ts`
- Test: `apps/inventory-service/test/payment-approved-handler.integration.spec.ts`

**Interfaces:**
- Consumes: `OrderCreatedHandler` (para montar o cenário nos testes),
  `markProcessed`, `insertOutboxRow`, `createEvent`, `inventoryEvents`,
  `paymentEvents`, `reservedItemSchema`, `CONSUMER_GROUPS`, `EventOf`,
  `ReservedItem`.
- Produces: `PaymentApprovedHandler.handle(envelope): Promise<void>` —
  consumido pelo `InventoryEventRouter` na Task 4.

- [ ] **Step 1: Escrever o teste de integração antes da implementação**

`apps/inventory-service/test/payment-approved-handler.integration.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, paymentEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';
import { PaymentApprovedHandler } from '../src/application/payment-approved.handler.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, items: Array<{ sku: string; quantity: number }>) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: items.map((item) => ({ ...item, name: 'Item de teste', unitPriceCents: 1000 })),
      totalAmountCents: items.reduce((sum, item) => sum + item.quantity * 1000, 0),
      currency: 'BRL',
      shippingAddress: ADDRESS,
    },
  });
}

function makePaymentApproved(orderId: string) {
  return createEvent(paymentEvents.paymentApproved, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'payment-service-test@0.0.0',
    payload: {
      paymentId: randomUUID(),
      orderId,
      amountCents: 2000,
      currency: 'BRL',
      authorizationCode: 'AUTH-TEST-1',
      instrument: { gatewayToken: 'tok_test_1', cardLast4: '4242', brand: 'VISA' },
      approvedAt: new Date().toISOString(),
    },
  });
}

describe('PaymentApprovedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const orderCreatedHandler = new OrderCreatedHandler(prisma);
  const paymentApprovedHandler = new PaymentApprovedHandler(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.stockReservation.deleteMany();
    await prisma.client.knownOrder.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('reserva estoque e publica stock.reserved quando todos os SKUs estão disponíveis', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));

    await paymentApprovedHandler.handle(makePaymentApproved(orderId));

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation?.status).toBe('RESERVED');
    expect(reservation?.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);
    expect(reservation?.expiresAt.getTime()).toBeGreaterThan(reservation!.reservedAt.getTime());

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('stock.reserved');
  });

  it('publica stock.unavailable e NÃO reserva nada (tudo ou nada) quando algum SKU começa com OUT-', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(
      makeOrderCreated(orderId, [
        { sku: 'BOOK-001', quantity: 1 },
        { sku: 'OUT-999', quantity: 3 },
      ]),
    );

    await paymentApprovedHandler.handle(makePaymentApproved(orderId));

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation).toBeNull(); // nem o item disponível (BOOK-001) foi reservado

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('stock.unavailable');

    const payload = outboxRows[0]?.payload as { payload: { unavailableItems: unknown } };
    expect(payload.payload.unavailableItems).toEqual([{ sku: 'OUT-999', requested: 3, available: 0 }]);
  });

  it('reentrega do MESMO evento (mesmo eventId) não reserva duas vezes — idempotência', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 1 }]));
    const paymentEnvelope = makePaymentApproved(orderId);

    await paymentApprovedHandler.handle(paymentEnvelope);
    await paymentApprovedHandler.handle(paymentEnvelope);

    const count = await prisma.client.stockReservation.count({ where: { orderId } });
    expect(count).toBe(1);
  });

  it('RACE CONDITION — payment.approved chega ANTES de order.created: falha retriável sem deixar rastro; ao chegar order.created, o retry reserva normalmente', async () => {
    const orderId = randomUUID();
    const paymentEnvelope = makePaymentApproved(orderId);

    // 1) payment.approved processado primeiro — KnownOrder ainda não existe.
    await expect(paymentApprovedHandler.handle(paymentEnvelope)).rejects.toThrow(/KnownOrder/);

    // 2) A transação inteira foi desfeita — INCLUSIVE o markProcessed. Sem este rollback,
    //    a "retentativa" abaixo encontraria o par (eventId, consumerGroup) já marcado e
    //    devolveria silenciosamente sem nunca ter reservado nada.
    const processed = await prisma.client.processedMessage.findUnique({
      where: {
        eventId_consumerGroup: { eventId: paymentEnvelope.eventId, consumerGroup: 'inventory-service' },
      },
    });
    expect(processed).toBeNull();
    const reservationBefore = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservationBefore).toBeNull();

    // 3) order.created finalmente chega (fora de ordem entre tópicos diferentes).
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));

    // 4) "Retentativa": mesmo handler, mesmo evento — é exatamente o que a escada de
    //    retry (5s/1m/10m) do @ecommerce/kafka faria ao redeliverar a mensagem.
    await paymentApprovedHandler.handle(paymentEnvelope);

    const reservationAfter = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservationAfter?.status).toBe('RESERVED');
    expect(reservationAfter?.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);
  });
});
```

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/inventory-service test
```

Esperado: FALHA (`payment-approved.handler.js` não existe).

- [ ] **Step 3: Implementar `PaymentApprovedHandler`**

`apps/inventory-service/src/application/payment-approved.handler.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { z } from 'zod';
import type { Prisma } from '@prisma/client';
import { markProcessed } from '@ecommerce/idempotency';
import { insertOutboxRow } from '@ecommerce/outbox';
import {
  CONSUMER_GROUPS,
  createEvent,
  type EventOf,
  inventoryEvents,
  paymentEvents,
  reservedItemSchema,
} from '@ecommerce/contracts';
import { PrismaService } from '../infrastructure/prisma.service.js';

export type PaymentApprovedEvent = EventOf<typeof paymentEvents.paymentApproved>;

/** Arbitrário e documentado: o sweeper que expira reservas de verdade é a Fase 6. */
const RESERVATION_TTL_MINUTES = 30;

/**
 * Gatilho determinístico de indisponibilidade (docs/PLAN.md 4.5): qualquer
 * SKU que comece com "OUT-" está fora de estoque. Sem Math.random() — teste
 * não determinístico não é teste.
 */
function isUnavailable(sku: string): boolean {
  return sku.startsWith('OUT-');
}

@Injectable()
export class PaymentApprovedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: PaymentApprovedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.inventory);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const { orderId } = envelope.payload;
      const knownOrder = await tx.knownOrder.findUnique({ where: { orderId } });

      if (!knownOrder) {
        /*
         * order.created deste pedido ainda não foi processado por este serviço — nada
         * garante ordem ENTRE tópicos diferentes (orders vs payments). Este throw
         * acontece DENTRO da transação, DEPOIS do markProcessed acima: o Prisma faz
         * ROLLBACK de tudo, inclusive do registro de idempotência. Sem esse rollback, a
         * escada de retry encontraria o (eventId, consumerGroup) já marcado e desistiria
         * silenciosamente, sem nunca ter reservado nada.
         *
         * Erro sem `.permanent = true` -> classifyError (@ecommerce/kafka) classifica
         * como RETRIÁVEL por padrão -> a escada 5s/1m/10m dá tempo para order.created
         * chegar antes de cair na DLT.
         */
        throw new Error(
          `KnownOrder ${orderId} ainda não visto por este serviço — aguardando order.created`,
        );
      }

      // Defesa extra (A08/A10): o dado veio do nosso próprio banco, mas revalidar contra
      // o schema não custa nada e transforma corrupção local em erro PERMANENTE — retry
      // nunca conserta dado corrompido, então não faz sentido gastar a escada nele.
      const parsedItems = z.array(reservedItemSchema).safeParse(knownOrder.items);
      if (!parsedItems.success) {
        const error = new Error(
          `KnownOrder ${orderId} tem "items" corrompidos no banco — não é erro retriável`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }
      const items = parsedItems.data;

      const unavailableItems = items.filter((item) => isUnavailable(item.sku));

      if (unavailableItems.length > 0) {
        const unavailableEnvelope = createEvent(inventoryEvents.stockUnavailable, {
          aggregateId: orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'inventory-service@0.1.0',
          payload: {
            orderId,
            unavailableItems: unavailableItems.map((item) => ({
              sku: item.sku,
              requested: item.quantity,
              available: 0,
            })),
            checkedAt: new Date().toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: unavailableEnvelope.eventId,
          aggregateId: orderId,
          aggregateType: 'stock-reservation',
          eventType: 'stock.unavailable',
          envelope: unavailableEnvelope,
        });
        return; // tudo ou nada: nenhum item deste pedido é reservado, nem os disponíveis
      }

      const reservationId = randomUUID();
      const reservedAt = new Date();
      const expiresAt = new Date(reservedAt.getTime() + RESERVATION_TTL_MINUTES * 60_000);

      await tx.stockReservation.create({
        data: {
          id: reservationId,
          orderId,
          items: items as unknown as Prisma.InputJsonValue,
          status: 'RESERVED',
          expiresAt,
          reservedAt,
        },
      });

      const reservedEnvelope = createEvent(inventoryEvents.stockReserved, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'inventory-service@0.1.0',
        payload: {
          reservationId,
          orderId,
          items,
          expiresAt: expiresAt.toISOString(),
          reservedAt: reservedAt.toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: reservedEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'stock-reservation',
        eventType: 'stock.reserved',
        envelope: reservedEnvelope,
      });
    });
  }
}
```

- [ ] **Step 4: Rodar o teste (requer infra no ar e migration aplicada)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/inventory-service test
```

Esperado: PASS, 4 testes desta Task (6 no total, somando a Task 2). O
teste de race condition é o mais importante desta Task — se ele falhar,
pare e investigue antes de seguir (não pule para a Task 4).

- [ ] **Step 5: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/inventory-service build
pnpm --filter @ecommerce/inventory-service typecheck
pnpm --filter @ecommerce/inventory-service lint
```

- [ ] **Step 6: Commit**

```bash
git add apps/inventory-service
git commit -m "feat(inventory-service): PaymentApprovedHandler reserva estoque com recuperação por retry"
```

---

### Task 4: Consumo Kafka de verdade — `KafkaConsumerRuntime`, roteamento, `app.module.ts`, `main.ts`, health

**Files:**
- Create: `apps/inventory-service/src/application/inventory-event.router.ts`
- Create: `apps/inventory-service/src/infrastructure/inventory-consumer.service.ts`
- Create: `apps/inventory-service/src/health/health.controller.ts`
- Create: `apps/inventory-service/src/app.module.ts`
- Create: `apps/inventory-service/src/main.ts`
- Test: `apps/inventory-service/test/inventory.e2e.spec.ts`

**Interfaces:**
- Consumes: `OrderCreatedHandler`, `PaymentApprovedHandler` (Tasks 2/3);
  `KafkaConsumerRuntime`, `EventProducer`, `MessageContext` de
  `@ecommerce/kafka`; `CONSUMER_GROUPS`, `TOPICS`, `UnknownEnvelope` de
  `@ecommerce/contracts`; `env` da Task 1.
- Produces: serviço completo, executável via `pnpm --filter
  @ecommerce/inventory-service dev`.

- [ ] **Step 1: Implementar o roteador de eventos**

`apps/inventory-service/src/application/inventory-event.router.ts`:
```ts
import { Injectable } from '@nestjs/common';
import type { UnknownEnvelope } from '@ecommerce/contracts';
import { OrderCreatedHandler, type OrderCreatedEvent } from './order-created.handler.js';
import { PaymentApprovedHandler, type PaymentApprovedEvent } from './payment-approved.handler.js';

/**
 * Decide qual handler chamar a partir de `envelope.eventType`. O envelope já
 * chegou validado (Zod, dentro do KafkaConsumerRuntime) contra o schema
 * exato do seu tipo+versão — os casts abaixo só satisfazem o TypeScript.
 */
@Injectable()
export class InventoryEventRouter {
  constructor(
    private readonly orderCreatedHandler: OrderCreatedHandler,
    private readonly paymentApprovedHandler: PaymentApprovedHandler,
  ) {}

  async route(envelope: UnknownEnvelope): Promise<void> {
    switch (envelope.eventType) {
      case 'order.created':
        await this.orderCreatedHandler.handle(envelope as OrderCreatedEvent);
        return;
      case 'payment.approved':
        await this.paymentApprovedHandler.handle(envelope as PaymentApprovedEvent);
        return;
      default:
        // payment.failed (matriz de compensação: nada a fazer aqui), order.confirmed,
        // order.cancelled, e qualquer eventType futuro (ex. payment.refunded na Fase 5)
        // não são assunto do Inventory — ignora e deixa o offset comitar normalmente.
        return;
    }
  }
}
```

- [ ] **Step 2: Implementar o serviço de consumo**

`apps/inventory-service/src/infrastructure/inventory-consumer.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, TOPICS } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
import { InventoryEventRouter } from '../application/inventory-event.router.js';

@Injectable()
export class InventoryConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-inventory-service-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly router: InventoryEventRouter) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.inventory,
      // Escopo desta fase: só orders (aprende itens) + payments (gatilho da reserva).
      // `shipping` já está em SUBSCRIPTIONS[inventory] — os tópicos de retry/DLT dele já
      // foram criados por `pnpm topics:create` — mas consumir `shipment.failed` (para
      // publicar `stock.released`) só é acrescentado na Fase 5.
      sourceTopics: [TOPICS.orders, TOPICS.payments],
      producer: this.producer,
      handler: (ctx) => this.router.route(ctx.envelope),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }
}
```

- [ ] **Step 3: Health controller**

`apps/inventory-service/src/health/health.controller.ts`:
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

- [ ] **Step 4: `app.module.ts`**

`apps/inventory-service/src/app.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { InventoryConsumerService } from './infrastructure/inventory-consumer.service.js';
import { OrderCreatedHandler } from './application/order-created.handler.js';
import { PaymentApprovedHandler } from './application/payment-approved.handler.js';
import { InventoryEventRouter } from './application/inventory-event.router.js';

@Module({
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    OrderCreatedHandler,
    PaymentApprovedHandler,
    InventoryEventRouter,
    InventoryConsumerService,
  ],
})
export class AppModule {}
```

- [ ] **Step 5: `main.ts`**

`apps/inventory-service/src/main.ts`:
```ts
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { env } from './env.js';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  await app.listen(env.INVENTORY_SERVICE_PORT);
  console.log(`[inventory-service] ouvindo na porta ${env.INVENTORY_SERVICE_PORT}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[inventory-service] recebido ${signal}, encerrando graciosamente`);
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

- [ ] **Step 6: Escrever o teste e2e ponta a ponta (Kafka real) — o teste mais importante desta fase**

`apps/inventory-service/test/inventory.e2e.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, paymentEvents, TOPICS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { env } from '../src/env.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, items: Array<{ sku: string; quantity: number }>) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: items.map((item) => ({ ...item, name: 'Item de teste', unitPriceCents: 1000 })),
      totalAmountCents: items.reduce((sum, item) => sum + item.quantity * 1000, 0),
      currency: 'BRL',
      shippingAddress: ADDRESS,
    },
  });
}

function makePaymentApproved(orderId: string) {
  return createEvent(paymentEvents.paymentApproved, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'payment-service-test@0.0.0',
    payload: {
      paymentId: randomUUID(),
      orderId,
      amountCents: 2000,
      currency: 'BRL',
      authorizationCode: 'AUTH-TEST-1',
      instrument: { gatewayToken: 'tok_test_1', cardLast4: '4242', brand: 'VISA' },
      approvedAt: new Date().toISOString(),
    },
  });
}

async function waitUntil(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error('Timeout esperando condição');
}

describe('Inventory Service — e2e (Kafka + Postgres reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);

    // EventProducer (@ecommerce/kafka), não kafkajs cru — mesma convenção usada pelos
    // testes de integração da Fase 2 (consumer-runtime.integration.spec.ts) para publicar
    // envelopes fabricados diretamente nos tópicos, simulando o outro serviço.
    producer = new EventProducer({ brokers: env.KAFKA_BROKERS, clientId: 'inventory-e2e-test-producer' });
    await producer.connect();
  });

  beforeEach(async () => {
    await prisma.client.stockReservation.deleteMany();
    await prisma.client.knownOrder.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
  });

  afterAll(async () => {
    await producer.disconnect();
    await app.close();
  });

  it('GET /health/live responde sem autenticação', async () => {
    await request(app.getHttpServer()).get('/health/live').expect(200);
  });

  it('cenário feliz: order.created + payment.approved reserva estoque e publica stock.reserved', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 1 }]));
    await producer.publish(TOPICS.payments, makePaymentApproved(orderId));

    await waitUntil(async () => {
      const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
      return reservation?.status === 'RESERVED';
    }, 20_000);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('stock.reserved');
  }, 25_000);

  it('SKU OUT- publica stock.unavailable, sem reservar nada', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, [{ sku: 'OUT-1', quantity: 1 }]));
    await producer.publish(TOPICS.payments, makePaymentApproved(orderId));

    await waitUntil(async () => {
      const row = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
      return row?.eventType === 'stock.unavailable';
    }, 20_000);

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation).toBeNull();
  }, 25_000);

  it('TESTE MAIS IMPORTANTE — payment.approved publicado ANTES de order.created ainda assim reserva, via retry real (retry-5s)', async () => {
    const orderId = randomUUID();

    // Publica payment.approved PRIMEIRO. O InventoryConsumerService vai processá-lo, não
    // encontrar KnownOrder, lançar erro retriável — a mensagem é redirecionada para
    // ecommerce.payments.v1.inventory-service.retry-5s e volta ~5s depois.
    await producer.publish(TOPICS.payments, makePaymentApproved(orderId));

    // Só publica order.created DEPOIS — reproduz a entrega fora de ordem entre tópicos
    // diferentes que motivou o Inventory a assinar `orders` (ver topics.ts).
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));

    await waitUntil(async () => {
      const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
      return reservation?.status === 'RESERVED';
    }, 20_000); // > delay do degrau retry-5s + margem para o segundo processamento

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation?.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('stock.reserved');
  }, 30_000);
});
```

- [ ] **Step 7: Rodar a suite completa (requer infra no ar, migration aplicada e tópicos criados)**

```bash
pnpm infra:up          # se ainda não estiver rodando
pnpm topics:create     # garante os tópicos de retry/DLT para inventory-service
pnpm --filter @ecommerce/inventory-service test
```

Esperado: PASS, 10 testes no total (2 da Task 2 + 4 da Task 3 + 4 desta
Task). O teste de retry real demora ~5-6s (aguarda o degrau `retry-5s`);
os demais são rápidos.

- [ ] **Step 8: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/inventory-service build
pnpm --filter @ecommerce/inventory-service typecheck
pnpm --filter @ecommerce/inventory-service lint
```

- [ ] **Step 9: Verificação manual ponta a ponta**

```bash
pnpm --filter @ecommerce/inventory-service dev &
sleep 2
kill %1
```

Esperado: log `[inventory-service] ouvindo na porta 3002` e nenhum erro de
conexão com Kafka/Postgres nos primeiros segundos (confirma que
`PrismaService`, `OutboxRelayService` e `InventoryConsumerService`
conectam ao subir o processo real, não só no teste).

- [ ] **Step 10: Commit**

```bash
git add apps/inventory-service
git commit -m "feat(inventory-service): consumo Kafka real com escada de retry para a race condition orders/payments"
```

---

## Verificação final da fase

- [ ] `pnpm --filter @ecommerce/inventory-service test` — todos os testes
      passam com `pnpm infra:up` e `pnpm topics:create` executados.
- [ ] `pnpm typecheck && pnpm lint` na raiz — sem erros.
- [ ] Critério de pronto do `docs/PLAN.md` Fase 4, na parte que cabe a
      este documento: pedido com SKU `OUT-*` faz o Inventory publicar
      `stock.unavailable` (provado pelos testes); pedido com SKUs normais
      publica `stock.reserved` com a reserva persistida. **O restante do
      critério de pronto do PLAN.md** ("pedido... termina em `CANCELLED`
      com o pagamento estornado") depende do Payment Service (Fase 3)
      também consumir `stock.unavailable` e emitir `payment.refunded`, e do
      Order Service reagir a isso — isso está fora do escopo deste
      documento e é validado quando a Fase 3/Order tiverem essa reação
      implementada.
- [ ] O teste de race condition (`PaymentApprovedHandler`, Task 3, e o
      teste "TESTE MAIS IMPORTANTE" da Task 4) passam — é a prova de que
      `payment.approved` chegando antes de `order.created` se recupera
      sozinho via retry, sem intervenção manual.

Com o Inventory Service publicando `stock.reserved`/`stock.unavailable` de
verdade, a Fase 5 (Shipping + Notification) já tem o que consumir para
gerar a etiqueta de envio, e ganha a responsabilidade de, mais adiante,
disparar a liberação de reserva (`stock.released`) quando `shipment.failed`
acontecer — isso exigirá voltar a este serviço para acrescentar um
terceiro handler e incluir `TOPICS.shipping` em `sourceTopics`.
