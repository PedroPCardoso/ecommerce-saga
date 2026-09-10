# Fase 3 — Payment Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar o Payment Service: consumidor de `order.created` no
tópico `ecommerce.orders.v1` (grupo `payment-service`), autorização MOCK
determinística (nunca `Math.random()`), gravação de `Payment` no Postgres
próprio (`payment_db`), publicação de `payment.approved`/`payment.failed`
via outbox atômico, e idempotência de consumo via `markProcessed` para que
reentrega de `order.created` não autorize duas vezes o mesmo pedido.

**Architecture:** NestJS (sem `@nestjs/cli`, compilado com `tsc`, mesmo
pipeline do resto do monorepo) + Prisma 6 contra o Postgres `payment_db`.
`KafkaConsumerRuntime` (de `@ecommerce/kafka`) consome `ecommerce.orders.v1`
com `groupId: CONSUMER_GROUPS.payment` e repassa cada envelope já validado
para `OrderEventsConsumerService`, que ignora tudo que não for
`order.created` (o tópico também carrega `order.confirmed`/
`order.cancelled`) e delega o resto para `AuthorizePaymentUseCase`.
`AuthorizePaymentUseCase` decide aprovar/recusar por regra determinística
sobre `payload.totalAmountCents`, grava `Payment` + linha de outbox +
`processed_messages` na MESMA transação Prisma (via `insertOutboxRow` e
`markProcessed`), e o `OutboxRelayService` (idêntico ao padrão do Order
Service, só trocando a env var de conexão) publica de fato em
`ecommerce.payments.v1`. Sem HTTP de negócio — só `/health/live`, para
consistência com o liveness/readiness que a Fase 10 (Kubernetes) vai exigir
de todos os serviços.

**Tech Stack:** NestJS 11 (`@nestjs/core`, `@nestjs/common`,
`@nestjs/platform-express`), Prisma 6, `pg`, `@ecommerce/contracts`/
`kafka`/`outbox`/`idempotency` (Fase 2), Vitest + `supertest` (só para o
`/health/live`).

## Global Constraints

- Requer a Fase 2 completa e commitada (`@ecommerce/kafka`,
  `@ecommerce/outbox`, `@ecommerce/idempotency` publicados em `dist/`).
- Requer a Fase 1 completa e commitada (o Order Service é quem, em
  produção, publica `order.created`) — mas os testes deste plano **não**
  precisam do Order Service rodando: eles publicam `order.created`
  diretamente em `ecommerce.orders.v1` com `EventProducer`, exatamente como
  o Payment Service veria de verdade, sem depender de nenhum processo HTTP.
- `totalAmountCents` é sempre um inteiro em centavos (nunca float) — vem do
  payload de `order.created`, que já é entrada não confiável do broker
  (A05): o `KafkaConsumerRuntime` já valida o envelope com Zod antes de o
  handler tocar nele (`parseEvent`/`findDefinition`); nenhuma validação
  adicional é responsabilidade deste serviço além de re-tipar com `parseAs`.
- Gatilhos de simulação são determinísticos sobre o payload, nunca
  `Math.random()` (docs/PLAN.md §4.5): `totalAmountCents % 100 === 13` →
  `payment.failed`; `totalAmountCents > 10_000` → autorização propositalmente
  lenta (30s), gatilho para o sweeper de timeout de saga da Fase 6.
- `instrument.gatewayToken`/`authorizationCode` são MOCK (`randomUUID()`).
  Nunca persista PAN/CVV, nem cifrado — só token opaco + 4 últimos dígitos
  fixos (`4242`/`VISA`), como o schema `paymentInstrumentSchema` de
  `@ecommerce/contracts` já impõe (A04/ADR-0011).
- `markProcessed` roda SEMPRE dentro da mesma transação Prisma que grava o
  efeito (`Payment` + outbox) — é o que faz o rollback desfazer os dois
  juntos se algo falhar no meio, e o que garante que reentrega não duplica
  autorização (A06: duplo processamento de pagamento = cobrança dupla).
- Erro inesperado (ex.: Postgres fora do ar) NUNCA vira aprovação/recusa
  silenciosa: propague a exceção — o `KafkaConsumerRuntime` cuida de rotear
  para a escada de retry ou a DLT (fail secure, A10). Este serviço não
  precisa (e não deve) engolir exceção num `catch` "genérico".
- Payment Service não tem endpoint HTTP de negócio — só `GET /health/live`,
  sem autenticação, mesmo padrão do Order Service.
- Fora de escopo NESTA fase (documentado, não implementado): consumir
  `stock.unavailable`/`shipment.failed` e publicar `payment.refunded` — isso
  é responsabilidade das Fases 4 e 5, que vão MODIFICAR
  `OrderEventsConsumerService`/`AppModule` para assinar `ecommerce.inventory.v1`
  e `ecommerce.shipping.v1` também (a topologia de retry/DLT para esses dois
  já existe desde a Fase 0, porque `payment-service` já está em
  `SUBSCRIPTIONS` para os três tópicos — só o `sourceTopics` deste serviço
  ainda não os inclui).
- Testes de integração exigem `pnpm infra:up` no ar, `pnpm topics:create`
  já executado (cria os tópicos de retry/DLT do grupo `payment-service`) e a
  migration do Prisma aplicada (`PAYMENT_DATABASE_URL`, porta `15433`).
  Publicam de verdade em `ecommerce.orders.v1`/`ecommerce.payments.v1` — os
  tópicos reais da topologia, não `lab.*`, porque o objetivo desta fase é
  provar a integração ponta a ponta.
- Commits diretos em `master`, conventional commits.

---

### Task 1: Scaffolding, dependências, schema Prisma e migration

**Files:**
- Create: `apps/payment-service/package.json`
- Create: `apps/payment-service/tsconfig.json`
- Create: `apps/payment-service/vitest.config.ts`
- Create: `apps/payment-service/prisma/schema.prisma`
- Create: `apps/payment-service/prisma/migrations/<timestamp>_init/migration.sql` (gerado + editado)
- Create: `apps/payment-service/src/env.ts`

**Interfaces:**
- Produces: `env` (objeto validado com Zod: `PAYMENT_DATABASE_URL`,
  `PAYMENT_SERVICE_PORT`, `KAFKA_BROKERS: string[]`,
  `KAFKA_CLIENT_ID_PREFIX`) — usado por todas as tasks seguintes. Sem
  `JWT_SECRET`/`JWT_ISSUER`: este serviço não tem HTTP autenticado.
- Produces: os modelos Prisma `Payment`, `Outbox`, `ProcessedMessage` — a
  Task 2 os usa via `PrismaClient` gerado.

- [ ] **Step 1: Confirmar que `dotenv-cli` já está na raiz**

`dotenv-cli` foi adicionado como devDependency da raiz na Fase 1
(`pnpm add -D dotenv-cli -w`). Não repita o `pnpm add` aqui — os scripts
deste serviço só reaproveitam `dotenv -e ../../.env -- ...`, mesmo padrão
do Order Service.

- [ ] **Step 2: `package.json` do serviço**

`apps/payment-service/package.json`:
```json
{
  "name": "@ecommerce/payment-service",
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

Sem `jsonwebtoken` nem `@nestjs/throttler`: não há endpoint HTTP autenticado
nem rate-limitado neste serviço — a única rota é `/health/live`.

- [ ] **Step 3: `tsconfig.json` e `vitest.config.ts`**

`apps/payment-service/tsconfig.json`:
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

`apps/payment-service/vitest.config.ts`:
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
`workspace:*` resolvem para os pacotes da Fase 2 já commitados, incluindo
`@ecommerce/idempotency`, que o Order Service não usa mas este serviço sim).

- [ ] **Step 5: Schema Prisma**

`apps/payment-service/prisma/schema.prisma`:
```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("PAYMENT_DATABASE_URL")
}

model Payment {
  id                String   @id
  orderId           String   @unique @map("order_id")
  amountCents       Int      @map("amount_cents")
  currency          String
  status            String
  authorizationCode String?  @map("authorization_code")
  failureCode       String?  @map("failure_code")
  createdAt         DateTime @default(now()) @map("created_at")
  updatedAt         DateTime @updatedAt @map("updated_at")

  @@map("payments")
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

model ProcessedMessage {
  eventId       String   @map("event_id")
  consumerGroup String   @map("consumer_group")
  processedAt   DateTime @default(now()) @map("processed_at")

  @@id([eventId, consumerGroup])
  @@map("processed_messages")
}
```

`Payment.id` **sem** `@default(uuid())`, mesma razão do `Order.id` na Fase
1: o `AuthorizePaymentUseCase` (Task 2) gera o `paymentId` explicitamente
ANTES da transação, porque o mesmo valor vai para a linha de `Payment`, para
o `payload.paymentId` do envelope de saída e para o `aggregateId`
(`orderId`, na verdade — ver nota na Task 2) do outbox, todos na mesma
transação.

`Payment.orderId` é `@unique`: nesta fase, no máximo um pagamento por
pedido (não há reautorização nem fluxo de reenvio de outro valor). Isso é
uma camada extra de defesa em profundidade além do `markProcessed` — o par
`(event_id, consumer_group)` já impede duplo processamento da MESMA
mensagem, e o `@unique` em `orderId` impede que dois eventos DIFERENTES
(ex.: um bug publicando dois `order.created` para o mesmo pedido) ainda
assim gerem duas autorizações. Esta constraint não está no texto original
do brief da fase — é um acréscimo deliberado, documentado aqui.

`ProcessedMessage` é o modelo Prisma que espelha exatamente
`packages/idempotency/src/schema.sql` (tabela `processed_messages`,
`PRIMARY KEY (event_id, consumer_group)`), e `Outbox` espelha
`packages/outbox/src/schema.sql`.

- [ ] **Step 6: Gerar a migration sem aplicar (para editar o índice parcial)**

```bash
cd apps/payment-service
pnpm prisma:generate
pnpm exec dotenv -e ../../.env -- prisma migrate dev --create-only --name init
```

Abra `prisma/migrations/<timestamp>_init/migration.sql` e acrescente ao
final (o Prisma DSL não expressa índice parcial diretamente):

```sql
-- Índice PARCIAL: o relay só pergunta pelas pendentes, e essa consulta roda
-- a cada 200ms para sempre. Sem o WHERE, o índice cresce com o histórico
-- inteiro e a consulta degrada junto.
CREATE INDEX "outbox_pending_idx" ON "outbox" ("created_at") WHERE "published_at" IS NULL;
```

- [ ] **Step 7: Aplicar a migration**

```bash
pnpm exec dotenv -e ../../.env -- prisma migrate deploy
cd ../..
```

Confirme:

```bash
docker exec -it $(docker ps -qf "name=postgres-payment") psql -U payment_svc -d payment_db -c "\dt"
```

Esperado: tabelas `payments`, `outbox`, `processed_messages`,
`_prisma_migrations` listadas.

- [ ] **Step 8: Loader de ambiente validado**

`apps/payment-service/src/env.ts`:
```ts
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PAYMENT_SERVICE_PORT: z.coerce.number().int().positive().default(3001),
  PAYMENT_DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) => value.split(',')),
  KAFKA_CLIENT_ID_PREFIX: z.string().min(1).default('ecommerce'),
});

export const env = envSchema.parse(process.env);
```

- [ ] **Step 9: Commit**

```bash
git add pnpm-lock.yaml apps/payment-service
git commit -m "chore(payment-service): scaffolding, prisma schema e migration inicial"
```

---

### Task 2: Camada de aplicação — `AuthorizePaymentUseCase`, `PrismaService`, `OutboxRelayService`

**Files:**
- Create: `apps/payment-service/src/infrastructure/prisma.service.ts`
- Create: `apps/payment-service/src/infrastructure/outbox-relay.service.ts`
- Create: `apps/payment-service/src/application/authorize-payment.use-case.ts`
- Test: `apps/payment-service/test/authorize-payment.integration.spec.ts`

**Interfaces:**
- Consumes: `insertOutboxRow`/`OutboxRelay` de `@ecommerce/outbox`,
  `EventProducer` de `@ecommerce/kafka`, `markProcessed` de
  `@ecommerce/idempotency`, `createEvent`/`orderEvents`/`paymentEvents`/
  `PAYMENT_FAILURE_CODE`/`CONSUMER_GROUPS`/`findDefinition`/`EventOf` de
  `@ecommerce/contracts`, `env` da Task 1.
- Produces: `AuthorizePaymentUseCase.execute(envelope: OrderCreatedEvent):
  Promise<void>` — consumido por `OrderEventsConsumerService` na Task 3.
  `OrderCreatedEvent` (alias de `EventOf<typeof orderEvents.orderCreated>`)
  é o tipo exato que a Task 3 usa depois de validar com `parseAs`.

- [ ] **Step 1: Escrever o teste de integração antes da implementação**

`apps/payment-service/test/authorize-payment.integration.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createEvent, orderEvents } from '@ecommerce/contracts';
import { env } from '../src/env.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OutboxRelayService } from '../src/infrastructure/outbox-relay.service.js';
import { AuthorizePaymentUseCase } from '../src/application/authorize-payment.use-case.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function orderCreatedEnvelope(totalAmountCents: number, orderId: string = randomUUID()) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.1.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: totalAmountCents }],
      totalAmountCents,
      currency: 'BRL',
      shippingAddress: ADDRESS,
    },
  });
}

describe('AuthorizePaymentUseCase (integração — Postgres + Kafka reais, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const relay = new OutboxRelayService();

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.payment.deleteMany();
    await relay.onModuleInit();
  });

  afterAll(async () => {
    await relay.onModuleDestroy();
    await prisma.onModuleDestroy();
  });

  it('autoriza e grava Payment AUTHORIZED + Outbox payment.approved na mesma transação', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(9980);

    await useCase.execute(envelope);

    const payment = await prisma.client.payment.findUnique({
      where: { orderId: envelope.payload.orderId },
    });
    expect(payment?.status).toBe('AUTHORIZED');
    expect(payment?.authorizationCode).toBeTruthy();
    expect(payment?.failureCode).toBeNull();

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: envelope.payload.orderId },
    });
    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.eventType).toBe('payment.approved');
  });

  it('recusa quando totalAmountCents termina em 13 e grava Payment FAILED + Outbox payment.failed', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(5013);

    await useCase.execute(envelope);

    const payment = await prisma.client.payment.findUnique({
      where: { orderId: envelope.payload.orderId },
    });
    expect(payment?.status).toBe('FAILED');
    expect(payment?.failureCode).toBe('CARD_DECLINED');
    expect(payment?.authorizationCode).toBeNull();

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: envelope.payload.orderId },
    });
    expect(outboxRows[0]?.eventType).toBe('payment.failed');
  });

  it('reentrega do mesmo order.created (mesmo eventId) não duplica Payment nem Outbox', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(4990);

    await useCase.execute(envelope);
    await useCase.execute(envelope); // mesmo eventId — simula reentrega do broker

    const paymentCount = await prisma.client.payment.count({
      where: { orderId: envelope.payload.orderId },
    });
    expect(paymentCount).toBe(1);

    const outboxCount = await prisma.client.outbox.count({
      where: { aggregateId: envelope.payload.orderId },
    });
    expect(outboxCount).toBe(1);
  });

  it('acima de R$100 (10_000 centavos) dorme 30s ANTES de decidir — verificado via função substituída, sem esperar de verdade', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const sleep = vi.fn().mockResolvedValue(undefined);
    useCase.sleep = sleep;
    const envelope = orderCreatedEnvelope(15_000);

    await useCase.execute(envelope);

    expect(sleep).toHaveBeenCalledWith(30_000);
    expect(sleep).toHaveBeenCalledTimes(1);

    const payment = await prisma.client.payment.findUnique({
      where: { orderId: envelope.payload.orderId },
    });
    expect(payment?.status).toBe('AUTHORIZED');
  });

  it('o relay publica payment.approved no tópico ecommerce.payments.v1 com a chave = orderId', async () => {
    const useCase = new AuthorizePaymentUseCase(prisma);
    const envelope = orderCreatedEnvelope(4990);

    await useCase.execute(envelope);

    const kafka = new Kafka({
      clientId: 'payment-service-test-reader',
      brokers: env.KAFKA_BROKERS,
      logLevel: logLevel.NOTHING,
    });
    const consumer = kafka.consumer({
      groupId: `payment-service-test-reader-${envelope.payload.orderId}`,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ecommerce.payments.v1', fromBeginning: true });

    const found = await new Promise<{ key: string | null; eventType: string }>((resolve) => {
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          const value = JSON.parse(message.value!.toString());
          if (value.aggregateId === envelope.payload.orderId) {
            resolve({ key: message.key?.toString() ?? null, eventType: value.eventType });
          }
        },
      });
    });
    await consumer.disconnect();

    expect(found.key).toBe(envelope.payload.orderId);
    expect(found.eventType).toBe('payment.approved');
  }, 15_000);
});
```

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/payment-service test
```

Esperado: FALHA (módulos ainda não existem).

- [ ] **Step 3: Implementar `PrismaService`**

`apps/payment-service/src/infrastructure/prisma.service.ts`:
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

`apps/payment-service/src/infrastructure/outbox-relay.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { OutboxRelay } from '@ecommerce/outbox';
import { EventProducer } from '@ecommerce/kafka';
import { findDefinition, type UnknownEnvelope } from '@ecommerce/contracts';
import { env } from '../env.js';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: env.PAYMENT_DATABASE_URL });
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-payment-service-relay`,
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

Idêntico ao do Order Service — só troca `ORDER_DATABASE_URL` por
`PAYMENT_DATABASE_URL` e o sufixo do `clientId`.

- [ ] **Step 5: Implementar `AuthorizePaymentUseCase`**

`apps/payment-service/src/application/authorize-payment.use-case.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import {
  CONSUMER_GROUPS,
  PAYMENT_FAILURE_CODE,
  createEvent,
  orderEvents,
  paymentEvents,
  type EventOf,
} from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { markProcessed } from '@ecommerce/idempotency';
import { PrismaService } from '../infrastructure/prisma.service.js';

export type OrderCreatedEvent = EventOf<typeof orderEvents.orderCreated>;

/**
 * Acima deste valor (em centavos), a autorização é propositalmente lenta —
 * gatilho determinístico (docs/PLAN.md §4.5) para a Fase 6 exercitar o
 * sweeper de timeout de saga. Nada de Math.random(): teste que não é
 * determinístico não é teste.
 */
const SLOW_PAYMENT_THRESHOLD_CENTS = 10_000;
const SLOW_PAYMENT_DELAY_MS = 30_000;

/** Terminação ".13" do valor em reais == totalAmountCents % 100 === 13. */
function isDeclined(amountCents: number): boolean {
  return amountCents % 100 === 13;
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

@Injectable()
export class AuthorizePaymentUseCase {
  /**
   * Campo público, não parâmetro de construtor: esta classe é um provider
   * do Nest (`AppModule` a instancia via DI), e um parâmetro de construtor
   * tipado como função não tem um token de injeção válido — o Nest tentaria
   * resolver uma dependência para o tipo `Function` e o bootstrap quebraria
   * com "Nest can't resolve dependency". Como campo público com valor
   * padrão, o Nest só precisa injetar `PrismaService`, e o teste substitui
   * `sleep` diretamente na instância (`useCase.sleep = vi.fn()...`) sem
   * esperar 30s de verdade a cada suíte.
   */
  sleep: (ms: number) => Promise<void> = defaultSleep;

  constructor(private readonly prisma: PrismaService) {}

  async execute(envelope: OrderCreatedEvent): Promise<void> {
    if (envelope.payload.totalAmountCents > SLOW_PAYMENT_THRESHOLD_CENTS) {
      await this.sleep(SLOW_PAYMENT_DELAY_MS);
    }

    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.payment);
      if (!isNew) return; // reentrega do mesmo order.created — efeito já aplicado, nada a fazer

      const paymentId = randomUUID();
      const now = new Date();
      const declined = isDeclined(envelope.payload.totalAmountCents);

      let authorizationCode: string | null = null;
      let failureCode: string | null = null;
      let outEnvelope;

      if (declined) {
        failureCode = PAYMENT_FAILURE_CODE.CARD_DECLINED;
        outEnvelope = createEvent(paymentEvents.paymentFailed, {
          aggregateId: envelope.payload.orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'payment-service@0.1.0',
          payload: {
            paymentId,
            orderId: envelope.payload.orderId,
            amountCents: envelope.payload.totalAmountCents,
            currency: envelope.payload.currency,
            failureCode: PAYMENT_FAILURE_CODE.CARD_DECLINED,
            reason: 'Cartão recusado pelo emissor (simulação determinística)',
            failedAt: now.toISOString(),
          },
        });
      } else {
        authorizationCode = randomUUID();
        outEnvelope = createEvent(paymentEvents.paymentApproved, {
          aggregateId: envelope.payload.orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'payment-service@0.1.0',
          payload: {
            paymentId,
            orderId: envelope.payload.orderId,
            amountCents: envelope.payload.totalAmountCents,
            currency: envelope.payload.currency,
            authorizationCode,
            // Mock fixo — nunca PAN/CVV real, mesmo em ambiente de estudo (A04/ADR-0011).
            instrument: { gatewayToken: randomUUID(), cardLast4: '4242', brand: 'VISA' },
            approvedAt: now.toISOString(),
          },
        });
      }

      await tx.payment.create({
        data: {
          id: paymentId,
          orderId: envelope.payload.orderId,
          amountCents: envelope.payload.totalAmountCents,
          currency: envelope.payload.currency,
          status: declined ? 'FAILED' : 'AUTHORIZED',
          authorizationCode,
          failureCode,
          createdAt: now,
        },
      });

      await insertOutboxRow(tx, {
        eventId: outEnvelope.eventId,
        aggregateId: envelope.payload.orderId,
        aggregateType: 'payment',
        eventType: outEnvelope.eventType,
        envelope: outEnvelope,
      });
    });
  }
}
```

Nota de tipos: `outEnvelope` é inferido como a união dos dois retornos de
`createEvent` (`payment.failed` | `payment.approved`). Por isso o código
só acessa os campos comuns do envelope (`eventId`, `eventType`) depois do
`if`/`else` — os campos específicos de cada payload (`authorizationCode`,
`failureCode`) são lidos das variáveis locais já computadas, nunca de
`outEnvelope.payload`, o que evitaria um erro de tipo ao acessar um campo
que só existe num dos dois lados da união.

- [ ] **Step 6: Rodar o teste (requer infra no ar e migration aplicada)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/contracts build
pnpm --filter @ecommerce/outbox build
pnpm --filter @ecommerce/kafka build
pnpm --filter @ecommerce/idempotency build
pnpm --filter @ecommerce/payment-service test
```

Esperado: PASS, 5 testes.

- [ ] **Step 7: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/payment-service build
pnpm --filter @ecommerce/payment-service typecheck
pnpm --filter @ecommerce/payment-service lint
```

- [ ] **Step 8: Commit**

```bash
git add apps/payment-service
git commit -m "feat(payment-service): AuthorizePaymentUseCase com outbox atômico e idempotência"
```

---

### Task 3: Consumo Kafka — `OrderEventsConsumerService`, health, e2e ponta a ponta

**Files:**
- Create: `apps/payment-service/src/consumers/order-events-consumer.service.ts`
- Create: `apps/payment-service/src/health/health.controller.ts`
- Create: `apps/payment-service/src/app.module.ts`
- Create: `apps/payment-service/src/main.ts`
- Test: `apps/payment-service/test/payment-service.e2e.spec.ts`

**Interfaces:**
- Consumes: `AuthorizePaymentUseCase` da Task 2; `env` da Task 1;
  `KafkaConsumerRuntime`/`MessageContext` de `@ecommerce/kafka`;
  `CONSUMER_GROUPS`/`TOPICS`/`orderEvents`/`parseAs` de
  `@ecommerce/contracts`.
- Produces: nada consumido por outra task desta fase — este é o ponto de
  entrada do processo (`main.ts` monta tudo).

- [ ] **Step 1: Escrever o teste e2e antes da implementação**

`apps/payment-service/test/payment-service.e2e.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { TOPICS, createEvent, orderEvents } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { env } from '../src/env.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function orderCreatedEnvelope(totalAmountCents: number, orderId: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.1.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: totalAmountCents }],
      totalAmountCents,
      currency: 'BRL',
      shippingAddress: ADDRESS,
    },
  });
}

function orderConfirmedEnvelope(orderId: string) {
  return createEvent(orderEvents.orderConfirmed, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.1.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      totalAmountCents: 4990,
      currency: 'BRL',
      confirmedAt: new Date().toISOString(),
    },
  });
}

async function waitForPayment(prisma: PrismaService, orderId: string, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const payment = await prisma.client.payment.findUnique({ where: { orderId } });
    if (payment) return payment;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`Payment para orderId ${orderId} não apareceu em ${timeoutMs}ms`);
}

async function waitForPaymentEvent(
  orderId: string,
  timeoutMs = 15_000,
): Promise<{ eventType: string; key: string | null }> {
  const kafka = new Kafka({
    clientId: 'payment-service-e2e-reader',
    brokers: env.KAFKA_BROKERS,
    logLevel: logLevel.NOTHING,
  });
  const consumer = kafka.consumer({ groupId: `payment-service-e2e-reader-${orderId}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.payments, fromBeginning: true });

  const found = await new Promise<{ eventType: string; key: string | null }>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timeout esperando evento de pagamento')), timeoutMs);
    consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        const value = JSON.parse(message.value!.toString());
        if (value.aggregateId === orderId) {
          clearTimeout(timeout);
          resolve({ eventType: value.eventType, key: message.key?.toString() ?? null });
        }
      },
    });
  });
  await consumer.disconnect();
  return found;
}

describe('Payment Service — e2e (Kafka + Postgres reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);

    producer = new EventProducer({
      brokers: env.KAFKA_BROKERS,
      clientId: 'payment-service-e2e-producer',
    });
    await producer.connect();
  });

  beforeEach(async () => {
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.payment.deleteMany();
  });

  afterAll(async () => {
    await producer.disconnect();
    await app.close();
  });

  it('consome order.created de verdade, autoriza e publica payment.approved', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, orderCreatedEnvelope(4990, orderId));

    const payment = await waitForPayment(prisma, orderId);
    expect(payment.status).toBe('AUTHORIZED');

    const event = await waitForPaymentEvent(orderId);
    expect(event.eventType).toBe('payment.approved');
    expect(event.key).toBe(orderId);
  }, 20_000);

  it('recusa quando totalAmountCents termina em 13 e publica payment.failed', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, orderCreatedEnvelope(5013, orderId));

    const payment = await waitForPayment(prisma, orderId);
    expect(payment.status).toBe('FAILED');

    const event = await waitForPaymentEvent(orderId);
    expect(event.eventType).toBe('payment.failed');
  }, 20_000);

  it('ignora order.confirmed (mesmo tópico, outro eventType) — sem criar Payment nem erro', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, orderConfirmedEnvelope(orderId));

    // Dá tempo do consumer processar (ou melhor: NÃO processar) a mensagem.
    await new Promise((resolve) => setTimeout(resolve, 3_000));

    const payment = await prisma.client.payment.findUnique({ where: { orderId } });
    expect(payment).toBeNull();
  }, 10_000);

  it('reentrega manual do mesmo order.created (mesmo eventId) não duplica Payment', async () => {
    const orderId = randomUUID();
    const envelope = orderCreatedEnvelope(4990, orderId);

    await producer.publish(TOPICS.orders, envelope);
    await waitForPayment(prisma, orderId);

    await producer.publish(TOPICS.orders, envelope); // mesmo eventId — reentrega simulada
    await new Promise((resolve) => setTimeout(resolve, 3_000));

    const count = await prisma.client.payment.count({ where: { orderId } });
    expect(count).toBe(1);
  }, 20_000);
});
```

**Nota de escopo — gatilho de 30s:** este teste não publica um pedido acima
de R$100 para observar os 30s de atraso em tempo real: isso já está coberto
(de forma rápida, com função injetada) pelo teste de unidade/integração da
Task 2. Esperar 30-35s de verdade num teste e2e deixaria a suíte lenta e
frágil sem provar nada que a Task 2 já não prove — a lógica de decisão é a
mesma nos dois casos, só muda quem chama `AuthorizePaymentUseCase.execute`.

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/payment-service test
```

Esperado: FALHA (`app.module.js` etc. não existem).

- [ ] **Step 3: Implementar `OrderEventsConsumerService`**

`apps/payment-service/src/consumers/order-events-consumer.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { EventProducer, KafkaConsumerRuntime, type MessageContext } from '@ecommerce/kafka';
import { CONSUMER_GROUPS, TOPICS, orderEvents, parseAs } from '@ecommerce/contracts';
import { env } from '../env.js';
import { AuthorizePaymentUseCase } from '../application/authorize-payment.use-case.js';

@Injectable()
export class OrderEventsConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-payment-service-consumer`,
  });
  private readonly runtime = new KafkaConsumerRuntime({
    brokers: env.KAFKA_BROKERS,
    groupId: CONSUMER_GROUPS.payment,
    sourceTopics: [TOPICS.orders],
    producer: this.producer,
    handler: (ctx) => this.handle(ctx),
  });

  constructor(private readonly authorizePayment: AuthorizePaymentUseCase) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime.stop();
    await this.producer.disconnect();
  }

  private async handle(ctx: MessageContext): Promise<void> {
    if (ctx.envelope.eventType !== 'order.created') {
      // ecommerce.orders.v1 também carrega order.confirmed/order.cancelled —
      // não é assunto do Payment. Ignora sem erro, offset comita normal.
      return;
    }

    const orderCreated = parseAs(orderEvents.orderCreated, ctx.envelope);
    await this.authorizePayment.execute(orderCreated);
  }
}
```

`sourceTopics: [TOPICS.orders]` é deliberadamente só `orders` nesta fase —
mesmo que `SUBSCRIPTIONS[CONSUMER_GROUPS.payment]` (em
`@ecommerce/contracts`) já liste `inventory` e `shipping` também (a
topologia de retry/DLT para os três já existe desde a Fase 0). As Fases 4 e
5 são quem vai acrescentar `TOPICS.inventory`/`TOPICS.shipping` aqui e
implementar `payment.refunded` — não implementado agora de propósito.

- [ ] **Step 4: Health controller**

`apps/payment-service/src/health/health.controller.ts`:
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

- [ ] **Step 5: `app.module.ts`**

`apps/payment-service/src/app.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { AuthorizePaymentUseCase } from './application/authorize-payment.use-case.js';
import { OrderEventsConsumerService } from './consumers/order-events-consumer.service.js';

@Module({
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    AuthorizePaymentUseCase,
    OrderEventsConsumerService,
  ],
})
export class AppModule {}
```

Sem `ThrottlerModule`/`APP_GUARD`: diferente do Order Service, não há
endpoint de escrita público aqui para proteger contra abuso.

- [ ] **Step 6: `main.ts`**

`apps/payment-service/src/main.ts`:
```ts
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { env } from './env.js';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  await app.listen(env.PAYMENT_SERVICE_PORT);
  console.log(`[payment-service] ouvindo na porta ${env.PAYMENT_SERVICE_PORT}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[payment-service] recebido ${signal}, encerrando graciosamente`);
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

- [ ] **Step 7: Rodar a suite completa (requer infra no ar, tópicos criados, migration aplicada)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm topics:create
pnpm --filter @ecommerce/payment-service test
```

Esperado: PASS, todos os testes das Tasks 2 e 3 (9 no total: 5 + 4).

- [ ] **Step 8: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/payment-service build
pnpm --filter @ecommerce/payment-service typecheck
pnpm --filter @ecommerce/payment-service lint
```

- [ ] **Step 9: Verificação manual ponta a ponta**

```bash
pnpm --filter @ecommerce/payment-service dev &
sleep 2
node -e "
const { EventProducer } = require('@ecommerce/kafka');
const { createEvent, orderEvents } = require('@ecommerce/contracts');
const { randomUUID } = require('crypto');
(async () => {
  const producer = new EventProducer({ brokers: ['localhost:29092'], clientId: 'manual-check' });
  await producer.connect();
  const orderId = randomUUID();
  await producer.publish('ecommerce.orders.v1', createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'manual-check@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 }],
      totalAmountCents: 4990,
      currency: 'BRL',
      shippingAddress: { street: 'Rua X', number: '1', district: 'Centro', city: 'SP', state: 'SP', zipCode: '01000-000', country: 'BR' },
    },
  }));
  console.log('publicado order.created para', orderId);
  await producer.disconnect();
})();
"
sleep 3
kill %1
```

Esperado: no Kafka UI (http://localhost:8080), uma mensagem `payment.approved`
aparece em `ecommerce.payments.v1` poucos segundos depois, com o mesmo
`orderId` como chave; `docker exec ... psql -d payment_db -c "select * from payments"`
mostra a linha `AUTHORIZED` correspondente.

- [ ] **Step 10: Commit**

```bash
git add apps/payment-service
git commit -m "feat(payment-service): consumo de order.created via KafkaConsumerRuntime"
```

---

## Verificação final da fase

- [ ] `pnpm --filter @ecommerce/payment-service test` — todos os testes
      passam com `pnpm infra:up` + `pnpm topics:create` no ar e a migration
      aplicada.
- [ ] `pnpm typecheck && pnpm lint` na raiz — sem erros.
- [ ] Critério de pronto do `docs/PLAN.md` Fase 3: o cenário feliz e o
      `payment.failed` funcionam ponta a ponta (Task 3, testes 1 e 2), e
      reentregar manualmente o mesmo `order.created` não gera segunda
      autorização (Task 2 teste 3 e Task 3 teste 4).
- [ ] `order.confirmed`/`order.cancelled` no mesmo tópico não quebram nem
      geram efeito indevido (Task 3 teste 3).

Com o Payment Service publicando `payment.approved`/`payment.failed` de
verdade em `ecommerce.payments.v1`, a Fase 4 (Inventory Service) já tem o
que consumir para reservar estoque — e vai voltar a este serviço para
acrescentar o consumo de `stock.unavailable` e a publicação de
`payment.refunded` (primeira compensação da saga).
