# Fase 5 — Shipping Service + Notification Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar os dois últimos serviços do núcleo funcional da saga.
**Shipping Service** consome `stock.reserved` (gatilho) e `order.created`
(aprende o endereço), decide `shipment.created` ou `shipment.failed` por um
gatilho determinístico de CEP, e publica o resultado via outbox — mesmo
padrão de race condition e recuperação por retry já usado por Payment
(Fase 3) e Inventory (Fase 4). **Notification Service** é só-consumidor:
assina os 4 tópicos de negócio, resolve um e-mail fictício determinístico
por pedido e "envia" (via SMTP real contra o Mailhog do
`docker-compose.yml`) uma notificação por `eventType` reconhecido, com
dedup por `@ecommerce/idempotency`.

Este documento cobre os dois serviços em duas partes independentes — Parte
A (Shipping) e Parte B (Notification) — porque Notification só consome e
não depende de nada que Shipping produza para funcionar. As Tasks são
numeradas de forma contínua (1–4 Shipping, 5–7 Notification) para facilitar
execução sequencial por um único agente, mas as duas partes podem ser
delegadas a agentes distintos em paralelo se preferir.

**Escopo e limite deste documento:** o título da Fase 5 no `docs/PLAN.md`
("Shipping + Notification") também menciona a "compensação dupla no
`shipment.failed`: Inventory libera, Payment estorna". Isso é
responsabilidade dos serviços **Inventory** e **Payment** (Fases 3/4), que
precisarão ganhar um handler novo para `shipment.failed` e passar a
consumir `TOPICS.shipping` (já provisionado em
`SUBSCRIPTIONS[CONSUMER_GROUPS.payment]` e
`SUBSCRIPTIONS[CONSUMER_GROUPS.inventory]`, ver `packages/contracts/src/topics.ts`).
Este documento cobre **só Shipping e Notification**: Shipping produz o
`shipment.failed` que dispara aquela compensação dupla, mas não implementa
o lado que reage a ele — isso fica para a Fase 6 (ou uma extensão futura),
que volta a Inventory e Payment para acrescentar esse handler. Ver
"Verificação final da fase" para o que isso implica no critério de pronto.

## Decisões e desvios do brief desta fase (leia antes de implementar)

1. **Notification precisa de uma tabela local `KnownOrder (orderId →
   customerId)`, além de `ProcessedMessage`.** Nenhum evento de
   `payment.*`/`stock.*`/`shipment.*` carrega `customerId` — só `orderId`
   (confirmado lendo `packages/contracts/src/events/payment.ts`,
   `inventory.ts` e `shipping.ts` linha por linha: `payment.approved`,
   `payment.failed`, `stock.unavailable`, `shipment.created` e
   `shipment.failed` só têm `orderId`). Só `order.created`,
   `order.confirmed` e `order.cancelled` carregam `customerId`
   diretamente. Notification precisa aprender o `customerId` via
   `order.created` (mesmo padrão que Shipping usa para aprender o
   endereço, e que Inventory usa para aprender os itens) e resolvê-lo por
   `orderId` para os outros 5 tipos de evento. Isso implica a mesma
   race condition dos outros serviços — ver decisão 3.
2. **A ordem de `markProcessed` em Notification é invertida em relação aos
   outros consumidores da saga.** Em Payment/Inventory/Shipping,
   `markProcessed` roda ANTES do efeito de negócio, na mesma transação —
   porque o efeito (gravar linha, publicar outbox) pode ser desfeito por
   `ROLLBACK`. Em Notification o "efeito" é uma chamada SMTP externa, que
   NENHUM `ROLLBACK` de banco desfaz. Se `markProcessed` rodasse antes do
   envio e o envio falhasse, o retry encontraria "já processado" e NUNCA
   reenviaria o e-mail que falhou. Por isso: primeiro uma checagem de
   LEITURA (`processedMessage.findUnique`, sem inserir nada) decide se já
   foi processado; se não foi, o e-mail é enviado; só se o envio tiver
   sucesso é que `markProcessed` roda, numa transação separada. Efeito
   colateral aceito: se o processo morrer ENTRE o envio ter sucesso e essa
   transação committar, o e-mail pode ser reenviado numa reentrega
   seguinte — documentado como trade-off aceitável (ver Task 6).
3. **Por causa da decisão 2, o "erro de KnownOrder ausente" em Notification
   NÃO precisa do truque de rollback-de-transação que Shipping/Inventory
   usam.** Nos outros serviços, o erro é lançado DENTRO da transação que já
   rodou `markProcessed`, para que o `ROLLBACK` desfaça o registro de
   idempotência junto. Em Notification, quando esse erro é lançado
   (dentro de `resolveCustomerId`), `markProcessed` ainda nem foi chamado
   nesta execução — não há nada para desfazer, o erro simplesmente sobe.
   Mais simples, e correto, só por causa da ordem diferente.
4. **`labelUrl` do `shipment.created` é uma URL mock fixa, host controlado
   pelo próprio Shipping** (`http://shipping-service.internal/labels/{shipmentId}`),
   nunca construída a partir de dado externo — é o que o comentário no
   schema Zod (`packages/contracts/src/events/shipping.ts`) já avisa:
   "quem consumir isto NÃO deve buscar a URL cegamente" (OWASP A01 —
   SSRF). Este serviço só publica a URL; não existe client HTTP nenhum
   aqui, de propósito — ver comentário dedicado no código (Task 3).

## Global Constraints

- Requer a Fase 2 completa (`@ecommerce/kafka`, `@ecommerce/outbox`,
  `@ecommerce/idempotency`) e as Fases 1/3/4 publicando `order.created`,
  `payment.approved`/`payment.failed` e `stock.reserved`/`stock.unavailable`
  de verdade. Os testes deste plano publicam os eventos-gatilho
  diretamente via Kafka (produtor de teste) — não dependem dos outros
  serviços estarem no ar para rodar.
- Sem endpoint HTTP de negócio em nenhum dos dois serviços — só
  `/health/live`, sem autenticação (nenhum cliente externo chama estes
  serviços via HTTP).
- "Ainda não vi o `order.created` deste pedido" é **sempre** erro
  retriável (nunca `.permanent = true`) em ambos os serviços — a escada de
  retry (5s/1m/10m) dá tempo para o evento chegar fora de ordem.
- Gatilho determinístico de falha de envio (PLAN.md 4.5): CEP que começa
  com `00000` está fora da área de cobertura simulada. Sem
  `Math.random()` na decisão de negócio — teste não determinístico não é
  teste.
- Nenhum evento carrega e-mail do cliente — Notification deriva um
  endereço FICTÍCIO determinístico a partir do `customerId`:
  `` `cliente-${customerId}@example.com` `` — sempre `@example.com`, nunca
  domínio real (política de PII do projeto: mascarar/usar fictício).
- Segurança (OWASP, exigência organizacional — dado com I/O externo):
  payload de evento é entrada não confiável e chega ao handler já validado
  pelo `parseEvent`/Zod dentro do `KafkaConsumerRuntime` (A05); erro
  desconhecido classifica como retriável, nunca conserta silenciosamente
  (A10, fail secure); `labelUrl` é mock fixo, nunca buscado — sem isso
  seria SSRF dirigível por evento (A01); PII (endereço, e-mail derivado)
  nunca aparece em log — só em campo de dado, nunca interpolado em
  mensagem de log (A09, dívida de `packages/observability` na Fase 7 para
  mascaramento automático).
- Testes de integração exigem `pnpm infra:up` no ar e as migrations
  aplicadas (`SHIPPING_DATABASE_URL` porta `15435`,
  `NOTIFICATION_DATABASE_URL` porta `15436`). Os testes e2e de Kafka
  também exigem `pnpm topics:create`.
- `dotenv-cli` já foi adicionado na raiz pela Fase 1 — reaproveite, não
  reinstale.
- Commits diretos em `master`, conventional commits, um por Task.

---

# Parte A — Shipping Service

**Architecture:** NestJS (sem `@nestjs/cli`, compilado com `tsc`, mesmo
padrão dos demais serviços) + Prisma 6 contra o Postgres `shipping_db`.
Dois handlers de aplicação, injetáveis mas chamáveis diretamente em teste:
`OrderCreatedHandler` (grava `KnownOrder` com o endereço, nenhum evento de
domínio sai daqui) e `StockReservedHandler` (o gatilho real: decide
`shipment.created`/`shipment.failed` e publica via outbox). Um
`ShippingEventRouter` decide qual handler chamar a partir de
`envelope.eventType`; um `ShippingConsumerService` liga isso a um
`KafkaConsumerRuntime` de `@ecommerce/kafka` assinando exatamente
`SUBSCRIPTIONS[CONSUMER_GROUPS.shipping]` (`ecommerce.orders.v1` +
`ecommerce.inventory.v1`) com o grupo `shipping-service`.
`OutboxRelayService` publica de verdade, mesmo padrão do Order Service.

**A race condition central:** `stock.reserved` (tópico `inventory`) não
carrega endereço de entrega — só itens reservados (ver comentário em
`packages/contracts/src/topics.ts`, `SUBSCRIPTIONS[CONSUMER_GROUPS.shipping]`).
Shipping só sabe para onde enviar através de `order.created` (tópico
`orders`). Nada garante que `order.created` seja processado antes do
`stock.reserved` correspondente chegar (tópicos diferentes, sem ordem
garantida entre si). Quando `StockReservedHandler` não encontra o
`KnownOrder`, ele lança um erro comum (sem `.permanent = true`) **dentro**
da transação Prisma onde `markProcessed` já rodou — o Prisma faz rollback
de tudo, inclusive do registro de idempotência, e `classifyError`
(`@ecommerce/kafka`) trata esse erro como retriável por padrão.

**Tech Stack:** NestJS 11 (`@nestjs/common`, `@nestjs/core`,
`@nestjs/platform-express`), Prisma 6, `pg`, `@ecommerce/contracts`/
`kafka`/`outbox`/`idempotency` (Fase 2), Vitest + `supertest` (só para o
`/health/live`).

---

### Task 1: Scaffolding, dependências, schema Prisma e migration

**Files:**
- Create: `apps/shipping-service/package.json`
- Create: `apps/shipping-service/tsconfig.json`
- Create: `apps/shipping-service/vitest.config.ts`
- Create: `apps/shipping-service/prisma/schema.prisma`
- Create: `apps/shipping-service/prisma/migrations/<timestamp>_init/migration.sql` (gerado + editado)
- Create: `apps/shipping-service/src/env.ts`

**Interfaces:**
- Produces: `env` (objeto validado com Zod: `SHIPPING_DATABASE_URL`,
  `SHIPPING_SERVICE_PORT`, `KAFKA_BROKERS: string[]`,
  `KAFKA_CLIENT_ID_PREFIX`) — usado por todas as tasks seguintes.
- Produces: os modelos Prisma `KnownOrder`, `Shipment`, `Outbox`,
  `ProcessedMessage` — a Task 2 os usa via `PrismaClient` gerado.

- [ ] **Step 1: `package.json` do serviço**

`apps/shipping-service/package.json`:
```json
{
  "name": "@ecommerce/shipping-service",
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
    "@ecommerce/idempotency": "workspace:*",
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

- [ ] **Step 2: `tsconfig.json` e `vitest.config.ts`**

`apps/shipping-service/tsconfig.json`:
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

`apps/shipping-service/vitest.config.ts`:
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

`apps/shipping-service/prisma/schema.prisma`:
```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("SHIPPING_DATABASE_URL")
}

/// Aprendido via `order.created` — só o endereço, que `stock.reserved` não carrega.
model KnownOrder {
  orderId         String   @id @map("order_id")
  shippingAddress Json     @map("shipping_address")
  createdAt       DateTime @default(now()) @map("created_at")

  @@map("known_orders")
}

model Shipment {
  id                  String   @id
  orderId             String   @unique @map("order_id")
  carrier             String
  trackingCode        String   @map("tracking_code")
  labelUrl            String   @map("label_url")
  estimatedDeliveryAt DateTime @map("estimated_delivery_at")
  status              String   @default("CREATED")
  createdAt           DateTime @default(now()) @map("created_at")

  @@map("shipments")
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

`Shipment.orderId` é `@unique` de propósito: este simulador assume no
máximo um envio por pedido (sem reenvio/reemissão de etiqueta nesta fase),
o que permite `findUnique({ where: { orderId } })` nos testes e no futuro
handler que reagir a `shipment.failed` (fora de escopo aqui).

- [ ] **Step 5: Gerar a migration sem aplicar (para editar o índice parcial do outbox)**

```bash
cd apps/shipping-service
pnpm prisma:generate
pnpm exec dotenv -e ../../.env -- prisma migrate dev --create-only --name init
```

Abra o arquivo gerado (`prisma/migrations/<timestamp>_init/migration.sql`)
e acrescente ao final (mesmo motivo documentado na Fase 1 e replicado em
Payment/Inventory: o Prisma DSL não expressa índice parcial diretamente, e
sem o `WHERE` o índice cresce com o histórico inteiro):

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
docker exec ecommerce-pg-shipping psql -U shipping_svc -d shipping_db -c "\dt"
```

Esperado: tabelas `known_orders`, `shipments`, `outbox`,
`processed_messages`, `_prisma_migrations`.

- [ ] **Step 7: Loader de ambiente validado**

`apps/shipping-service/src/env.ts`:
```ts
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  SHIPPING_SERVICE_PORT: z.coerce.number().int().positive().default(3003),
  SHIPPING_DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) => value.split(',')),
  KAFKA_CLIENT_ID_PREFIX: z.string().min(1).default('ecommerce'),
});

export const env = envSchema.parse(process.env);
```

- [ ] **Step 8: Commit**

```bash
git add package.json pnpm-lock.yaml apps/shipping-service
git commit -m "chore(shipping-service): scaffolding, prisma schema e migration inicial"
```

---

### Task 2: `OrderCreatedHandler` — aprende o endereço de entrega

**Files:**
- Create: `apps/shipping-service/src/infrastructure/prisma.service.ts`
- Create: `apps/shipping-service/src/infrastructure/outbox-relay.service.ts`
- Create: `apps/shipping-service/src/application/order-created.handler.ts`
- Test: `apps/shipping-service/test/order-created-handler.integration.spec.ts`

**Interfaces:**
- Consumes: `markProcessed` de `@ecommerce/idempotency`;
  `CONSUMER_GROUPS`, `EventOf`, `orderEvents` de `@ecommerce/contracts`.
- Produces: `OrderCreatedHandler.handle(envelope: OrderCreatedEvent):
  Promise<void>` — consumido pelo `ShippingEventRouter` na Task 4 e
  chamado diretamente pelos testes da Task 3.

- [ ] **Step 1: Escrever o teste de integração antes da implementação**

`apps/shipping-service/test/order-created-handler.integration.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 };
const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, address = ADDRESS) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [ITEM],
      totalAmountCents: 4990,
      currency: 'BRL',
      shippingAddress: address,
    },
  });
}

describe('OrderCreatedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const handler = new OrderCreatedHandler(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.knownOrder.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('grava KnownOrder com o endereço — nunca publica evento de domínio', async () => {
    const orderId = randomUUID();

    await handler.handle(makeOrderCreated(orderId));

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.shippingAddress).toEqual(ADDRESS);

    const outboxCount = await prisma.client.outbox.count();
    expect(outboxCount).toBe(0);
  });

  it('reentrega do MESMO eventId não falha nem duplica (idempotência)', async () => {
    const orderId = randomUUID();
    const envelope = makeOrderCreated(orderId);

    await handler.handle(envelope);
    await handler.handle(envelope);

    const count = await prisma.client.knownOrder.count({ where: { orderId } });
    expect(count).toBe(1);
  });

  it('order.created mais recente ATUALIZA o endereço aprendido (upsert)', async () => {
    const orderId = randomUUID();
    const novoEndereco = { ...ADDRESS, zipCode: '02000-000' };

    await handler.handle(makeOrderCreated(orderId, ADDRESS));
    await handler.handle(makeOrderCreated(orderId, novoEndereco));

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.shippingAddress).toEqual(novoEndereco);
  });
});
```

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/shipping-service test
```

Esperado: FALHA (módulos ainda não existem).

- [ ] **Step 3: Implementar `PrismaService`**

`apps/shipping-service/src/infrastructure/prisma.service.ts`:
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

`apps/shipping-service/src/infrastructure/outbox-relay.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { OutboxRelay } from '@ecommerce/outbox';
import { EventProducer } from '@ecommerce/kafka';
import { findDefinition, type UnknownEnvelope } from '@ecommerce/contracts';
import { env } from '../env.js';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: env.SHIPPING_DATABASE_URL });
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-shipping-service-relay`,
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

`apps/shipping-service/src/application/order-created.handler.ts`:
```ts
import { Injectable } from '@nestjs/common';
import type { Prisma } from '@prisma/client';
import { markProcessed } from '@ecommerce/idempotency';
import { CONSUMER_GROUPS, type EventOf, orderEvents } from '@ecommerce/contracts';
import { PrismaService } from '../infrastructure/prisma.service.js';

export type OrderCreatedEvent = EventOf<typeof orderEvents.orderCreated>;

/**
 * Só aprende o endereço de entrega do pedido — NENHUM evento de domínio é
 * publicado aqui. O gatilho real do envio é sempre `stock.reserved`
 * (StockReservedHandler, Task 3). `stock.reserved` não carrega endereço,
 * então é este handler que dá ao Shipping "para onde enviar" — ver o
 * comentário em `packages/contracts/src/topics.ts`
 * (SUBSCRIPTIONS[CONSUMER_GROUPS.shipping]).
 */
@Injectable()
export class OrderCreatedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: OrderCreatedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.shipping);
      if (!isNew) return; // reentrega do mesmo evento — já aprendemos este endereço

      await tx.knownOrder.upsert({
        where: { orderId: envelope.payload.orderId },
        create: {
          orderId: envelope.payload.orderId,
          shippingAddress: envelope.payload.shippingAddress as unknown as Prisma.InputJsonValue,
        },
        update: {
          shippingAddress: envelope.payload.shippingAddress as unknown as Prisma.InputJsonValue,
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
pnpm --filter @ecommerce/shipping-service test
```

Esperado: PASS, 3 testes.

- [ ] **Step 7: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/shipping-service build
pnpm --filter @ecommerce/shipping-service typecheck
pnpm --filter @ecommerce/shipping-service lint
```

- [ ] **Step 8: Commit**

```bash
git add apps/shipping-service
git commit -m "feat(shipping-service): OrderCreatedHandler aprende o endereço de entrega"
```

---

### Task 3: `StockReservedHandler` — gatilho do envio, falha determinística e a race condition

**Files:**
- Create: `apps/shipping-service/src/application/stock-reserved.handler.ts`
- Test: `apps/shipping-service/test/stock-reserved-handler.integration.spec.ts`

**Interfaces:**
- Consumes: `OrderCreatedHandler` (Task 2, para montar o cenário nos
  testes); `markProcessed`, `insertOutboxRow`, `createEvent`,
  `inventoryEvents`, `shippingEvents`, `CONSUMER_GROUPS`, `EventOf`,
  `Address` de `@ecommerce/contracts`/`@ecommerce/outbox`.
- Produces: `StockReservedHandler.handle(envelope: StockReservedEvent):
  Promise<void>` — consumido pelo `ShippingEventRouter` na Task 4.

- [ ] **Step 1: Escrever o teste de integração antes da implementação**

`apps/shipping-service/test/stock-reserved-handler.integration.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, inventoryEvents, orderEvents, shippingEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';
import { StockReservedHandler } from '../src/application/stock-reserved.handler.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 2, unitPriceCents: 4990 };
const ADDRESS_OK = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};
const ADDRESS_FAIL = { ...ADDRESS_OK, zipCode: '00000-000' };

function makeOrderCreated(orderId: string, address: typeof ADDRESS_OK) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [ITEM],
      totalAmountCents: 9980,
      currency: 'BRL',
      shippingAddress: address,
    },
  });
}

function makeStockReserved(orderId: string) {
  return createEvent(inventoryEvents.stockReserved, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'inventory-service-test@0.0.0',
    payload: {
      reservationId: randomUUID(),
      orderId,
      items: [{ sku: ITEM.sku, quantity: ITEM.quantity }],
      expiresAt: new Date(Date.now() + 30 * 60_000).toISOString(),
      reservedAt: new Date().toISOString(),
    },
  });
}

describe('StockReservedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const orderCreatedHandler = new OrderCreatedHandler(prisma);
  const handler = new StockReservedHandler(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.shipment.deleteMany();
    await prisma.client.knownOrder.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('CEP normal: cria Shipment e publica shipment.created com o schema correto', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_OK));

    await handler.handle(makeStockReserved(orderId));

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment?.status).toBe('CREATED');
    expect(shipment?.carrier).toBe('CORREIOS');
    expect(shipment?.trackingCode).toMatch(/^BR\d{9}BR$/);
    expect(shipment?.labelUrl).toBe(`http://shipping-service.internal/labels/${shipment?.id}`);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.created');
    // O envelope publicado precisa satisfazer o schema real do contrato —
    // não só "parecer certo".
    expect(() => shippingEvents.shipmentCreated.envelope.parse(outboxRow?.payload)).not.toThrow();
  });

  it('CEP 00000-XXX: publica shipment.failed, SEM criar Shipment', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_FAIL));

    await handler.handle(makeStockReserved(orderId));

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment).toBeNull();

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.failed');
    expect(() => shippingEvents.shipmentFailed.envelope.parse(outboxRow?.payload)).not.toThrow();
    const payload = outboxRow?.payload as { payload: { failureCode: string } };
    expect(payload.payload.failureCode).toBe('ADDRESS_NOT_SERVICEABLE');
  });

  it('RACE CONDITION: stock.reserved chega ANTES do order.created — erro retriável, rollback do markProcessed, e recuperação depois', async () => {
    const orderId = randomUUID();
    const stockReservedEnvelope = makeStockReserved(orderId);

    let caught: (Error & { permanent?: boolean }) | undefined;
    try {
      await handler.handle(stockReservedEnvelope);
    } catch (error) {
      caught = error as Error & { permanent?: boolean };
    }

    expect(caught?.message).toMatch(/KnownOrder/);
    // Erro COMUM — classifyError (@ecommerce/kafka) precisa tratar isto como
    // RETRIÁVEL, nunca permanente.
    expect(caught?.permanent).toBeUndefined();

    // O ROLLBACK desfez o markProcessed junto com a transação — sem isso, o
    // retry encontraria "já processado" e desistiria sem nunca ter enviado nada.
    const processedCount = await prisma.client.processedMessage.count({
      where: { eventId: stockReservedEnvelope.eventId },
    });
    expect(processedCount).toBe(0);

    // order.created chega DEPOIS (fora de ordem, mas chega).
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_OK));

    // Reentrega da MESMA mensagem stock.reserved — simula o degrau retry-5s.
    await handler.handle(stockReservedEnvelope);

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment).not.toBeNull();
    expect(shipment?.status).toBe('CREATED');
  });

  it('reentrega do MESMO eventId (já processado com sucesso) não cria um segundo Shipment', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, ADDRESS_OK));
    const envelope = makeStockReserved(orderId);

    await handler.handle(envelope);
    await handler.handle(envelope);

    const outboxCount = await prisma.client.outbox.count({ where: { aggregateId: orderId } });
    expect(outboxCount).toBe(1);
  });
});
```

- [ ] **Step 2: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/shipping-service test
```

Esperado: FALHA (`stock-reserved.handler.js` não existe).

- [ ] **Step 3: Implementar `StockReservedHandler`**

`apps/shipping-service/src/application/stock-reserved.handler.ts`:
```ts
import { randomInt, randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { markProcessed } from '@ecommerce/idempotency';
import { insertOutboxRow } from '@ecommerce/outbox';
import {
  CONSUMER_GROUPS,
  createEvent,
  type Address,
  type EventOf,
  inventoryEvents,
  shippingEvents,
} from '@ecommerce/contracts';
import { PrismaService } from '../infrastructure/prisma.service.js';

export type StockReservedEvent = EventOf<typeof inventoryEvents.stockReserved>;

/**
 * Gatilho determinístico de falha de envio (docs/PLAN.md 4.5): CEP que
 * começa com "00000" está fora da área de cobertura simulada. Sem
 * Math.random() — teste não determinístico não é teste.
 */
function isAddressServiceable(address: Address): boolean {
  return !address.zipCode.startsWith('00000');
}

/**
 * Formato de rastreio dos Correios (mock): 2 letras + 9 dígitos + 2 letras.
 * Não determinístico de propósito — só o CAMINHO da saga (created vs.
 * failed) precisa ser determinístico; o valor exato do código de rastreio
 * não afeta nenhum teste.
 */
function generateTrackingCode(): string {
  const digits = randomInt(0, 1_000_000_000).toString().padStart(9, '0');
  return `BR${digits}BR`;
}

@Injectable()
export class StockReservedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: StockReservedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.shipping);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const { orderId } = envelope.payload;
      const knownOrder = await tx.knownOrder.findUnique({ where: { orderId } });

      if (!knownOrder) {
        /*
         * order.created deste pedido ainda não foi processado por este
         * serviço — nada garante ordem ENTRE tópicos diferentes (orders vs
         * inventory). Este throw acontece DENTRO da transação, DEPOIS do
         * markProcessed acima: o Prisma faz ROLLBACK de tudo, inclusive do
         * registro de idempotência. Sem esse rollback, a escada de retry
         * encontraria o (eventId, consumerGroup) já marcado e desistiria
         * silenciosamente, sem nunca ter tentado o envio.
         *
         * Erro sem `.permanent = true` -> classifyError (@ecommerce/kafka)
         * classifica como RETRIÁVEL por padrão -> a escada 5s/1m/10m dá tempo
         * para order.created chegar antes de cair na DLT.
         */
        throw new Error(
          `KnownOrder ${orderId} ainda não visto por este serviço — aguardando order.created`,
        );
      }

      const address = knownOrder.shippingAddress as unknown as Address;
      const now = new Date();

      if (!isAddressServiceable(address)) {
        const failedEnvelope = createEvent(shippingEvents.shipmentFailed, {
          aggregateId: orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'shipping-service@0.1.0',
          payload: {
            orderId,
            failureCode: shippingEvents.SHIPMENT_FAILURE_CODE.ADDRESS_NOT_SERVICEABLE,
            reason: 'CEP fora da área de cobertura simulada (gatilho determinístico: CEP inicia com 00000)',
            failedAt: now.toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: failedEnvelope.eventId,
          aggregateId: orderId,
          aggregateType: 'shipment',
          eventType: 'shipment.failed',
          envelope: failedEnvelope,
        });

        // Compensação dupla (Inventory libera estoque via stock.released,
        // Payment estorna via payment.refunded) é responsabilidade de outro
        // plano — este serviço só publica o fato; quem reage a ele não é o
        // Shipping (ver "Escopo e limite deste documento" no topo do plano).
        return;
      }

      const shipmentId = randomUUID();
      const trackingCode = generateTrackingCode();
      const estimatedDeliveryAt = new Date(now.getTime() + 5 * 24 * 60 * 60 * 1000);
      /**
       * URL MOCK apontando para um host controlado por ESTE serviço — nunca
       * uma URL vinda de dado externo/evento. O schema
       * `shippingEvents.shipmentCreated.payload.labelUrl` em
       * @ecommerce/contracts já avisa: "quem consumir isto NÃO deve buscar a
       * URL cegamente" (OWASP A01 — SSRF, docs/PLAN.md seção 8). Este handler
       * só PUBLICA a URL — não existe nenhum client HTTP aqui, de propósito.
       */
      const labelUrl = `http://shipping-service.internal/labels/${shipmentId}`;

      await tx.shipment.create({
        data: {
          id: shipmentId,
          orderId,
          carrier: 'CORREIOS',
          trackingCode,
          labelUrl,
          estimatedDeliveryAt,
          status: 'CREATED',
          createdAt: now,
        },
      });

      const createdEnvelope = createEvent(shippingEvents.shipmentCreated, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'shipping-service@0.1.0',
        payload: {
          shipmentId,
          orderId,
          carrier: 'CORREIOS',
          trackingCode,
          labelUrl,
          estimatedDeliveryAt: estimatedDeliveryAt.toISOString(),
          createdAt: now.toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: createdEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'shipment',
        eventType: 'shipment.created',
        envelope: createdEnvelope,
      });
    });
  }
}
```

- [ ] **Step 4: Rodar o teste (requer infra no ar e migration aplicada)**

```bash
pnpm infra:up   # se ainda não estiver rodando
pnpm --filter @ecommerce/shipping-service test
```

Esperado: PASS, 7 testes no total (3 da Task 2 + 4 desta Task).

- [ ] **Step 5: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/shipping-service build
pnpm --filter @ecommerce/shipping-service typecheck
pnpm --filter @ecommerce/shipping-service lint
```

- [ ] **Step 6: Commit**

```bash
git add apps/shipping-service
git commit -m "feat(shipping-service): StockReservedHandler decide envio com recuperação por retry"
```

---

### Task 4: Consumo Kafka de verdade — roteamento, `app.module.ts`, `main.ts`, health, e2e

**Files:**
- Create: `apps/shipping-service/src/application/shipping-event.router.ts`
- Create: `apps/shipping-service/src/infrastructure/shipping-consumer.service.ts`
- Create: `apps/shipping-service/src/health/health.controller.ts`
- Create: `apps/shipping-service/src/app.module.ts`
- Create: `apps/shipping-service/src/main.ts`
- Test: `apps/shipping-service/test/shipping.e2e.spec.ts`

**Interfaces:**
- Consumes: `OrderCreatedHandler`, `StockReservedHandler` (Tasks 2/3);
  `KafkaConsumerRuntime`, `EventProducer` de `@ecommerce/kafka`;
  `CONSUMER_GROUPS`, `SUBSCRIPTIONS`, `TOPICS`, `UnknownEnvelope` de
  `@ecommerce/contracts`; `env` da Task 1.
- Produces: serviço completo, executável via `pnpm --filter
  @ecommerce/shipping-service dev`.

- [ ] **Step 1: Implementar o roteador de eventos**

`apps/shipping-service/src/application/shipping-event.router.ts`:
```ts
import { Injectable } from '@nestjs/common';
import type { UnknownEnvelope } from '@ecommerce/contracts';
import { OrderCreatedHandler, type OrderCreatedEvent } from './order-created.handler.js';
import { StockReservedHandler, type StockReservedEvent } from './stock-reserved.handler.js';

/**
 * Decide qual handler chamar a partir de `envelope.eventType`. O envelope já
 * chegou validado (Zod, dentro do KafkaConsumerRuntime) contra o schema
 * exato do seu tipo+versão — os casts abaixo só satisfazem o TypeScript.
 */
@Injectable()
export class ShippingEventRouter {
  constructor(
    private readonly orderCreatedHandler: OrderCreatedHandler,
    private readonly stockReservedHandler: StockReservedHandler,
  ) {}

  async route(envelope: UnknownEnvelope): Promise<void> {
    switch (envelope.eventType) {
      case 'order.created':
        await this.orderCreatedHandler.handle(envelope as OrderCreatedEvent);
        return;
      case 'stock.reserved':
        await this.stockReservedHandler.handle(envelope as StockReservedEvent);
        return;
      default:
        // order.confirmed, order.cancelled, stock.unavailable, stock.released —
        // não são assunto do Shipping nesta fase: stock.unavailable já terminou
        // a saga em cancelamento antes de chegar aqui, e stock.released é a
        // própria compensação de um shipment.failed anterior (não dispara
        // outro envio). Ignora e deixa o offset comitar normalmente.
        return;
    }
  }
}
```

- [ ] **Step 2: Implementar o serviço de consumo**

`apps/shipping-service/src/infrastructure/shipping-consumer.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
import { ShippingEventRouter } from '../application/shipping-event.router.js';

@Injectable()
export class ShippingConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-shipping-service-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly router: ShippingEventRouter) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.shipping,
      // SUBSCRIPTIONS[CONSUMER_GROUPS.shipping] = [orders, inventory] — já é
      // o escopo completo desta fase, sem nada a deferir (diferente do
      // Inventory na Fase 4, que adiou `shipping` para depois).
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.shipping],
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

`apps/shipping-service/src/health/health.controller.ts`:
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

`apps/shipping-service/src/app.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { ShippingConsumerService } from './infrastructure/shipping-consumer.service.js';
import { OrderCreatedHandler } from './application/order-created.handler.js';
import { StockReservedHandler } from './application/stock-reserved.handler.js';
import { ShippingEventRouter } from './application/shipping-event.router.js';

@Module({
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    OrderCreatedHandler,
    StockReservedHandler,
    ShippingEventRouter,
    ShippingConsumerService,
  ],
})
export class AppModule {}
```

- [ ] **Step 5: `main.ts`**

`apps/shipping-service/src/main.ts`:
```ts
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { env } from './env.js';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  await app.listen(env.SHIPPING_SERVICE_PORT);
  console.log(`[shipping-service] ouvindo na porta ${env.SHIPPING_SERVICE_PORT}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[shipping-service] recebido ${signal}, encerrando graciosamente`);
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

- [ ] **Step 6: Escrever o teste e2e ponta a ponta (Kafka real)**

`apps/shipping-service/test/shipping.e2e.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, inventoryEvents, TOPICS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { env } from '../src/env.js';

const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 };

function addressFor(zipCode: string) {
  return {
    street: 'Rua Teste',
    number: '1',
    district: 'Centro',
    city: 'São Paulo',
    state: 'SP',
    zipCode,
    country: 'BR',
  };
}

function makeOrderCreated(orderId: string, zipCode: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId: randomUUID(),
      items: [ITEM],
      totalAmountCents: 4990,
      currency: 'BRL',
      shippingAddress: addressFor(zipCode),
    },
  });
}

function makeStockReserved(orderId: string) {
  return createEvent(inventoryEvents.stockReserved, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'inventory-service-test@0.0.0',
    payload: {
      reservationId: randomUUID(),
      orderId,
      items: [{ sku: ITEM.sku, quantity: ITEM.quantity }],
      expiresAt: new Date(Date.now() + 30 * 60_000).toISOString(),
      reservedAt: new Date().toISOString(),
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

describe('Shipping Service — e2e (Kafka + Postgres reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);

    // EventProducer (@ecommerce/kafka), não kafkajs cru — mesma convenção usada
    // pelos testes e2e das Fases 3/4 para publicar envelopes fabricados
    // diretamente nos tópicos, simulando os outros serviços.
    producer = new EventProducer({ brokers: env.KAFKA_BROKERS, clientId: 'shipping-e2e-test-producer' });
    await producer.connect();
  });

  beforeEach(async () => {
    await prisma.client.shipment.deleteMany();
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

  it('cenário feliz: order.created + stock.reserved gera Shipment e publica shipment.created', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, '01000-000'));
    await producer.publish(TOPICS.inventory, makeStockReserved(orderId));

    await waitUntil(async () => {
      const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
      return shipment?.status === 'CREATED';
    }, 20_000);

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.created');
  }, 25_000);

  it('CEP 00000-XXX publica shipment.failed, sem criar Shipment', async () => {
    const orderId = randomUUID();
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, '00000-000'));
    await producer.publish(TOPICS.inventory, makeStockReserved(orderId));

    await waitUntil(async () => {
      const row = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
      return row?.eventType === 'shipment.failed';
    }, 20_000);

    const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
    expect(shipment).toBeNull();
  }, 25_000);

  it('TESTE MAIS IMPORTANTE — stock.reserved publicado ANTES de order.created ainda assim gera o envio, via retry real (retry-5s)', async () => {
    const orderId = randomUUID();

    // Publica stock.reserved PRIMEIRO. O ShippingConsumerService vai
    // processá-lo, não encontrar KnownOrder, lançar erro retriável — a
    // mensagem é redirecionada para
    // ecommerce.inventory.v1.shipping-service.retry-5s e volta ~5s depois.
    await producer.publish(TOPICS.inventory, makeStockReserved(orderId));

    // Só publica order.created DEPOIS — reproduz a entrega fora de ordem
    // entre tópicos diferentes que motivou o Shipping a assinar `orders`.
    await producer.publish(TOPICS.orders, makeOrderCreated(orderId, '01000-000'));

    await waitUntil(async () => {
      const shipment = await prisma.client.shipment.findUnique({ where: { orderId } });
      return shipment?.status === 'CREATED';
    }, 20_000); // > delay do degrau retry-5s + margem para o segundo processamento

    const outboxRow = await prisma.client.outbox.findFirst({ where: { aggregateId: orderId } });
    expect(outboxRow?.eventType).toBe('shipment.created');
  }, 30_000);
});
```

- [ ] **Step 7: Rodar a suite completa (requer infra no ar, migration aplicada e tópicos criados)**

```bash
pnpm infra:up          # se ainda não estiver rodando
pnpm topics:create     # garante os tópicos de retry/DLT para shipping-service
pnpm --filter @ecommerce/shipping-service test
```

Esperado: PASS, 11 testes no total (3 da Task 2 + 4 da Task 3 + 4 desta
Task). O teste de retry real demora ~5-6s (aguarda o degrau `retry-5s`);
os demais são rápidos.

- [ ] **Step 8: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/shipping-service build
pnpm --filter @ecommerce/shipping-service typecheck
pnpm --filter @ecommerce/shipping-service lint
```

- [ ] **Step 9: Verificação manual ponta a ponta**

```bash
pnpm --filter @ecommerce/shipping-service dev &
sleep 2
kill %1
```

Esperado: log `[shipping-service] ouvindo na porta 3003` e nenhum erro de
conexão com Kafka/Postgres nos primeiros segundos.

- [ ] **Step 10: Commit**

```bash
git add apps/shipping-service
git commit -m "feat(shipping-service): consumo Kafka real com escada de retry para a race condition orders/inventory"
```

---

# Parte B — Notification Service

**Architecture:** NestJS (sem `@nestjs/cli`, compilado com `tsc`) + Prisma 6
contra o Postgres `notification_db`. Serviço só-consumidor: nenhum evento
de domínio é publicado por aqui, então não há `@ecommerce/outbox` nem
`OutboxRelayService` — só `@ecommerce/idempotency`. Um único
`NotificationEventHandler` cobre os 8 tipos de evento com template, decide
o e-mail (assunto + corpo) e resolve o destinatário fictício; um
`NotificationConsumerService` liga isso a um `KafkaConsumerRuntime`
assinando exatamente `SUBSCRIPTIONS[CONSUMER_GROUPS.notification]` (todos
os 4 tópicos de negócio) com o grupo `notification-service`. Um
`MailerService` fino embrulha `nodemailer`, enviando via SMTP real contra
o Mailhog do `docker-compose.yml`.

**Diferente de Shipping/Inventory/Payment, a ordem de idempotência é
invertida** (ver decisão 2 no topo do documento): aqui `markProcessed` só
roda DEPOIS do e-mail ter sido enviado com sucesso, porque o "efeito" é uma
chamada SMTP externa que nenhum `ROLLBACK` de Postgres desfaz.

**A mesma race condition dos outros serviços, adaptada:** nenhum evento de
`payment.*`/`inventory.*`/`shipping.*` carrega `customerId` — só `orderId`.
Notification aprende o `customerId` via `order.created` (tabela local
`KnownOrder`) e o resolve por `orderId` para os outros 5 tipos de evento
com template. Quando o `KnownOrder` não existe ainda, o erro sobe como
retriável — mas, por causa da ordem invertida de `markProcessed`, isso
acontece ANTES de qualquer escrita, então não há nada para desfazer em
rollback (mais simples que Shipping/Inventory, ver decisão 3).

**Tech Stack:** NestJS 11, Prisma 6, `nodemailer`, `@ecommerce/contracts`/
`kafka`/`idempotency` (Fase 2), Vitest + `supertest` (só para o
`/health/live`).

---

### Task 5: Scaffolding, dependências, schema Prisma e migration

**Files:**
- Create: `apps/notification-service/package.json`
- Create: `apps/notification-service/tsconfig.json`
- Create: `apps/notification-service/vitest.config.ts`
- Create: `apps/notification-service/prisma/schema.prisma`
- Create: `apps/notification-service/prisma/migrations/<timestamp>_init/migration.sql`
- Create: `apps/notification-service/src/env.ts`

**Interfaces:**
- Produces: `env` (objeto validado com Zod: `NOTIFICATION_DATABASE_URL`,
  `NOTIFICATION_SERVICE_PORT`, `KAFKA_BROKERS: string[]`,
  `KAFKA_CLIENT_ID_PREFIX`, `SMTP_HOST`, `SMTP_PORT: number`, `SMTP_FROM`).
- Produces: os modelos Prisma `KnownOrder`, `ProcessedMessage`.

- [ ] **Step 1: `package.json` do serviço**

`apps/notification-service/package.json`:
```json
{
  "name": "@ecommerce/notification-service",
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
    "@ecommerce/idempotency": "workspace:*",
    "@nestjs/common": "^11.0.1",
    "@nestjs/core": "^11.0.1",
    "@nestjs/platform-express": "^11.0.1",
    "@prisma/client": "^6.1.0",
    "nodemailer": "^6.9.16",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1",
    "zod": "^3.24.1"
  },
  "devDependencies": {
    "@nestjs/testing": "^11.0.1",
    "@types/nodemailer": "^6.4.16",
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

Sem `@ecommerce/outbox` nem `pg`: este serviço não publica evento de
domínio, então não há outbox nem `Pool` cru.

- [ ] **Step 2: `tsconfig.json` e `vitest.config.ts`**

`apps/notification-service/tsconfig.json`:
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

`apps/notification-service/vitest.config.ts`:
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

- [ ] **Step 4: Schema Prisma**

`apps/notification-service/prisma/schema.prisma`:
```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("NOTIFICATION_DATABASE_URL")
}

/// Aprendido via `order.created` — nenhum outro evento carrega customerId.
model KnownOrder {
  orderId    String   @id @map("order_id")
  customerId String   @map("customer_id")
  createdAt  DateTime @default(now()) @map("created_at")

  @@map("known_orders")
}

model ProcessedMessage {
  eventId       String   @map("event_id")
  consumerGroup String   @map("consumer_group")
  processedAt   DateTime @default(now()) @map("processed_at")

  @@id([eventId, consumerGroup])
  @@map("processed_messages")
}
```

- [ ] **Step 5: Gerar e aplicar a migration (sem edição manual — não há outbox aqui)**

```bash
cd apps/notification-service
pnpm prisma:generate
pnpm exec dotenv -e ../../.env -- prisma migrate dev --create-only --name init
```

Diferente de Shipping (e dos outros serviços que publicam evento), não há
tabela `outbox` neste schema — não há índice parcial para editar
manualmente. Aplique diretamente:

```bash
pnpm exec dotenv -e ../../.env -- prisma migrate deploy
cd ../..
```

Confirme:

```bash
docker exec ecommerce-pg-notification psql -U notification_svc -d notification_db -c "\dt"
```

Esperado: tabelas `known_orders`, `processed_messages`,
`_prisma_migrations`.

- [ ] **Step 6: Loader de ambiente validado**

`apps/notification-service/src/env.ts`:
```ts
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  NOTIFICATION_SERVICE_PORT: z.coerce.number().int().positive().default(3004),
  NOTIFICATION_DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) => value.split(',')),
  KAFKA_CLIENT_ID_PREFIX: z.string().min(1).default('ecommerce'),
  SMTP_HOST: z.string().min(1),
  SMTP_PORT: z.coerce.number().int().positive(),
  SMTP_FROM: z.string().min(1),
});

export const env = envSchema.parse(process.env);
```

- [ ] **Step 7: Commit**

```bash
git add package.json pnpm-lock.yaml apps/notification-service
git commit -m "chore(notification-service): scaffolding, prisma schema e migration inicial"
```

---

### Task 6: `MailerService` + `NotificationEventHandler` — templates, roteamento e a race condition, testados contra Mailhog

**Files:**
- Create: `apps/notification-service/src/infrastructure/prisma.service.ts`
- Create: `apps/notification-service/src/infrastructure/mailer.service.ts`
- Create: `apps/notification-service/src/application/notification-event.handler.ts`
- Create: `apps/notification-service/test/support/mailhog-client.ts`
- Test: `apps/notification-service/test/notification-event-handler.integration.spec.ts`

**Interfaces:**
- Consumes: `markProcessed` de `@ecommerce/idempotency`; `CONSUMER_GROUPS`,
  `EventOf`, `UnknownEnvelope`, `orderEvents`, `paymentEvents`,
  `inventoryEvents`, `shippingEvents` de `@ecommerce/contracts`; `env` da
  Task 5.
- Produces: `NotificationEventHandler.handle(envelope: UnknownEnvelope):
  Promise<void>` — consumido pelo `NotificationConsumerService` na Task 7.

- [ ] **Step 1: Implementar `PrismaService` (idêntico ao padrão dos demais serviços)**

`apps/notification-service/src/infrastructure/prisma.service.ts`:
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

- [ ] **Step 2: Implementar `MailerService`**

`apps/notification-service/src/infrastructure/mailer.service.ts`:
```ts
import { Injectable } from '@nestjs/common';
import nodemailer, { type Transporter } from 'nodemailer';
import { env } from '../env.js';

export interface SendEmailInput {
  to: string;
  subject: string;
  text: string;
}

/**
 * Wrapper fino de `nodemailer` contra o Mailhog do `docker-compose.yml`
 * (SMTP fake — nunca envia e-mail de verdade). `secure: false` porque
 * Mailhog não faz TLS: infra de estudo local, nunca use isto em produção
 * (A02/A04 — em produção seria um provedor SMTP real com STARTTLS/SSL).
 */
@Injectable()
export class MailerService {
  private readonly transporter: Transporter = nodemailer.createTransport({
    host: env.SMTP_HOST,
    port: env.SMTP_PORT,
    secure: false,
  });

  async send(input: SendEmailInput): Promise<void> {
    await this.transporter.sendMail({
      from: env.SMTP_FROM,
      to: input.to,
      subject: input.subject,
      text: input.text,
    });
  }
}
```

- [ ] **Step 3: Escrever o teste de integração antes da implementação**

`apps/notification-service/test/support/mailhog-client.ts`:
```ts
const MAILHOG_API_BASE = 'http://localhost:18025/api';

export interface MailhogItem {
  To: Array<{ Mailbox: string; Domain: string }>;
  Content: { Headers: Record<string, string[]>; Body: string };
}

/** Limpa a caixa de entrada do Mailhog — usado no beforeEach de cada teste. */
export async function clearMailhogInbox(): Promise<void> {
  await fetch(`${MAILHOG_API_BASE}/v1/messages`, { method: 'DELETE' });
}

export async function findMailhogMessage(
  predicate: (item: MailhogItem) => boolean,
  timeoutMs = 10_000,
): Promise<MailhogItem> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = await fetch(`${MAILHOG_API_BASE}/v2/messages?limit=100`);
    const body = (await res.json()) as { items: MailhogItem[] };
    const found = body.items.find(predicate);
    if (found) return found;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error('Mensagem esperada não apareceu no Mailhog a tempo');
}

export async function countMailhogMessagesTo(email: string): Promise<number> {
  const res = await fetch(`${MAILHOG_API_BASE}/v2/messages?limit=100`);
  const body = (await res.json()) as { items: MailhogItem[] };
  return body.items.filter(
    (item) => item.To.some((to) => `${to.Mailbox}@${to.Domain}`.toLowerCase() === email.toLowerCase()),
  ).length;
}

export function subjectOf(item: MailhogItem): string {
  return item.Content.Headers['Subject']?.[0] ?? '';
}
```

`apps/notification-service/test/notification-event-handler.integration.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, inventoryEvents, orderEvents, paymentEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { MailerService } from '../src/infrastructure/mailer.service.js';
import { NotificationEventHandler } from '../src/application/notification-event.handler.js';
import { clearMailhogInbox, countMailhogMessagesTo, findMailhogMessage, subjectOf } from './support/mailhog-client.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};
const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 };

function makeOrderCreated(orderId: string, customerId: string) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      customerId,
      items: [ITEM],
      totalAmountCents: 4990,
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
      amountCents: 4990,
      currency: 'BRL',
      authorizationCode: 'AUTH-TEST-1',
      instrument: { gatewayToken: 'tok_test_1', cardLast4: '4242', brand: 'VISA' },
      approvedAt: new Date().toISOString(),
    },
  });
}

describe('NotificationEventHandler (integração — Postgres + Mailhog reais, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const mailer = new MailerService();
  const handler = new NotificationEventHandler(prisma, mailer);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.processedMessage.deleteMany();
    await prisma.client.knownOrder.deleteMany();
    await clearMailhogInbox();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('order.created envia e-mail de boas-vindas e aprende o customerId', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();

    await handler.handle(makeOrderCreated(orderId, customerId));

    const message = await findMailhogMessage((item) => subjectOf(item).includes(orderId));
    expect(subjectOf(message)).toBe(`Recebemos seu pedido ${orderId}`);
    expect(message.To[0]).toEqual(
      expect.objectContaining({ Mailbox: `cliente-${customerId}`, Domain: 'example.com' }),
    );

    const known = await prisma.client.knownOrder.findUnique({ where: { orderId } });
    expect(known?.customerId).toBe(customerId);
  });

  it('reentrega do MESMO eventId não reenvia o e-mail (idempotência)', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();
    const envelope = makeOrderCreated(orderId, customerId);

    await handler.handle(envelope);
    await handler.handle(envelope); // mesmo eventId

    const count = await countMailhogMessagesTo(`cliente-${customerId}@example.com`);
    expect(count).toBe(1);
  });

  it('eventType sem template nesta fase (stock.reserved) não envia e-mail', async () => {
    const orderId = randomUUID();
    const envelope = createEvent(inventoryEvents.stockReserved, {
      aggregateId: orderId,
      correlationId: orderId,
      producer: 'inventory-service-test@0.0.0',
      payload: {
        reservationId: randomUUID(),
        orderId,
        items: [{ sku: 'BOOK-001', quantity: 1 }],
        expiresAt: new Date(Date.now() + 3600_000).toISOString(),
        reservedAt: new Date().toISOString(),
      },
    });

    await handler.handle(envelope);

    const count = await countMailhogMessagesTo('cliente-qualquer-coisa@example.com');
    expect(count).toBe(0);
    // Mesmo sem template, o evento é registrado como processado — evita
    // reavaliar a mesma decisão de "nada a fazer" para sempre.
    const processed = await prisma.client.processedMessage.count({ where: { eventId: envelope.eventId } });
    expect(processed).toBe(1);
  });

  it('RACE CONDITION: payment.approved chega ANTES de order.created — erro retriável, e recuperação depois', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();
    const paymentEnvelope = makePaymentApproved(orderId);

    let caught: (Error & { permanent?: boolean }) | undefined;
    try {
      await handler.handle(paymentEnvelope);
    } catch (error) {
      caught = error as Error & { permanent?: boolean };
    }

    expect(caught?.message).toMatch(/KnownOrder/);
    expect(caught?.permanent).toBeUndefined();

    // markProcessed NUNCA chegou a ser chamado nesta execução — o erro subiu
    // antes disso (diferente de Shipping/Inventory, ver decisão 3 do plano).
    const processedBefore = await prisma.client.processedMessage.count({
      where: { eventId: paymentEnvelope.eventId },
    });
    expect(processedBefore).toBe(0);

    const countBeforeRecovery = await countMailhogMessagesTo(`cliente-${customerId}@example.com`);
    expect(countBeforeRecovery).toBe(0);

    // order.created chega DEPOIS — a escada de retry real reentregaria a
    // MESMA mensagem de payment.approved; aqui simulamos isso chamando o
    // handler de novo diretamente.
    await handler.handle(makeOrderCreated(orderId, customerId));
    await handler.handle(paymentEnvelope);

    const message = await findMailhogMessage((item) => subjectOf(item).includes(`Pagamento do pedido ${orderId}`));
    expect(subjectOf(message)).toBe(`Pagamento do pedido ${orderId} aprovado`);
  });
});
```

- [ ] **Step 4: Rodar e confirmar falha**

```bash
pnpm --filter @ecommerce/notification-service test
```

Esperado: FALHA (`notification-event.handler.js` não existe).

- [ ] **Step 5: Implementar `NotificationEventHandler`**

`apps/notification-service/src/application/notification-event.handler.ts`:
```ts
import { Injectable } from '@nestjs/common';
import { markProcessed } from '@ecommerce/idempotency';
import {
  CONSUMER_GROUPS,
  type EventOf,
  type UnknownEnvelope,
  inventoryEvents,
  orderEvents,
  paymentEvents,
  shippingEvents,
} from '@ecommerce/contracts';
import { PrismaService } from '../infrastructure/prisma.service.js';
import { MailerService, type SendEmailInput } from '../infrastructure/mailer.service.js';

type OrderCreatedEvent = EventOf<typeof orderEvents.orderCreated>;
type OrderConfirmedEvent = EventOf<typeof orderEvents.orderConfirmed>;
type OrderCancelledEvent = EventOf<typeof orderEvents.orderCancelled>;
type PaymentApprovedEvent = EventOf<typeof paymentEvents.paymentApproved>;
type PaymentFailedEvent = EventOf<typeof paymentEvents.paymentFailed>;
type StockUnavailableEvent = EventOf<typeof inventoryEvents.stockUnavailable>;
type ShipmentCreatedEvent = EventOf<typeof shippingEvents.shipmentCreated>;
type ShipmentFailedEvent = EventOf<typeof shippingEvents.shipmentFailed>;

/**
 * Serviço só-consumidor: nenhum evento de domínio sai daqui, então não há
 * outbox — só @ecommerce/idempotency, usada de um jeito DIFERENTE dos
 * outros consumidores desta saga. `markProcessed` só roda DEPOIS do e-mail
 * ser enviado com sucesso, não antes — o efeito aqui é uma chamada SMTP
 * externa, não uma escrita de domínio que possa ser desfeita num rollback.
 * Se o processo morrer ENTRE o envio ter sucesso e essa gravação, uma
 * reentrega pode reenviar o mesmo e-mail — trade-off aceito e documentado
 * no plano (Task 6): duplicar uma NOTIFICAÇÃO é inofensivo, diferente de
 * duplicar um pagamento ou uma reserva de estoque.
 */
@Injectable()
export class NotificationEventHandler {
  constructor(
    private readonly prisma: PrismaService,
    private readonly mailer: MailerService,
  ) {}

  async handle(envelope: UnknownEnvelope): Promise<void> {
    const already = await this.prisma.client.processedMessage.findUnique({
      where: {
        eventId_consumerGroup: { eventId: envelope.eventId, consumerGroup: CONSUMER_GROUPS.notification },
      },
    });
    if (already) return; // já processado — não reenvia.

    if (envelope.eventType === 'order.created') {
      // Gravação otimista e idempotente (upsert) — INDEPENDENTE do envio do
      // e-mail ter sucesso. Repetir isto em todo retry é inofensivo, e
      // adiantar o aprendizado do customerId destrava mais cedo qualquer
      // payment.approved/stock.unavailable/shipment.* deste pedido que já
      // esteja esperando no retry por falta dele.
      await this.recordKnownOrder(envelope as OrderCreatedEvent);
    }

    const email = await this.buildEmail(envelope);
    if (!email) {
      // eventType sem template nesta fase (stock.reserved, payment.refunded,
      // stock.released, ...) — nada a enviar.
      await this.markAsProcessed(envelope.eventId);
      return;
    }

    // Envia PRIMEIRO, fora de transação — só registra processed_messages
    // DEPOIS do envio ter sucesso (ver comentário da classe).
    await this.mailer.send(email);
    await this.markAsProcessed(envelope.eventId);
  }

  private async markAsProcessed(eventId: string): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      await markProcessed(tx, eventId, CONSUMER_GROUPS.notification);
    });
  }

  private async recordKnownOrder(event: OrderCreatedEvent): Promise<void> {
    await this.prisma.client.knownOrder.upsert({
      where: { orderId: event.payload.orderId },
      create: { orderId: event.payload.orderId, customerId: event.payload.customerId },
      update: { customerId: event.payload.customerId },
    });
  }

  private async resolveCustomerId(orderId: string): Promise<string> {
    const known = await this.prisma.client.knownOrder.findUnique({ where: { orderId } });
    if (!known) {
      /*
       * Mesmo padrão de Shipping/Inventory (roadmap, decisão técnica #8):
       * nenhum destes eventos carrega customerId — só orderId (confirmado
       * lendo packages/contracts/src/events/{payment,inventory,shipping}.ts).
       * Erro COMUM (sem `.permanent`) — RETRIÁVEL — dá tempo para o
       * order.created deste pedido ser processado.
       *
       * Diferente de Shipping/Inventory, aqui NÃO precisamos do truque de
       * "lançar dentro da mesma transação que já rodou markProcessed": neste
       * ponto ainda não chamamos markProcessed nem enviamos e-mail nenhum —
       * markProcessed só acontece no fim, depois do envio ter sucesso —
       * então não há nada a desfazer, o erro simplesmente sobe.
       */
      throw new Error(
        `KnownOrder ainda não disponível para orderId=${orderId} — order.created não processado ainda`,
      );
    }
    return known.customerId;
  }

  private async buildEmail(envelope: UnknownEnvelope): Promise<SendEmailInput | null> {
    switch (envelope.eventType) {
      case 'order.created': {
        const event = envelope as OrderCreatedEvent;
        return {
          to: emailFor(event.payload.customerId),
          subject: `Recebemos seu pedido ${event.payload.orderId}`,
          text: `Seu pedido ${event.payload.orderId} foi recebido e está sendo processado.`,
        };
      }
      case 'order.confirmed': {
        const event = envelope as OrderConfirmedEvent;
        return {
          to: emailFor(event.payload.customerId),
          subject: `Pedido ${event.payload.orderId} confirmado`,
          text: `Seu pedido ${event.payload.orderId} foi confirmado. Obrigado pela compra!`,
        };
      }
      case 'order.cancelled': {
        const event = envelope as OrderCancelledEvent;
        return {
          to: emailFor(event.payload.customerId),
          subject: `Pedido ${event.payload.orderId} cancelado`,
          text: `Seu pedido ${event.payload.orderId} foi cancelado. Motivo: ${event.payload.reason}.`,
        };
      }
      case 'payment.approved': {
        const event = envelope as PaymentApprovedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Pagamento do pedido ${event.payload.orderId} aprovado`,
          text: `O pagamento do seu pedido ${event.payload.orderId} foi aprovado.`,
        };
      }
      case 'payment.failed': {
        const event = envelope as PaymentFailedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Pagamento do pedido ${event.payload.orderId} recusado`,
          text: `O pagamento do seu pedido ${event.payload.orderId} foi recusado.`,
        };
      }
      case 'stock.unavailable': {
        const event = envelope as StockUnavailableEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Item indisponível no pedido ${event.payload.orderId}`,
          text: `Um ou mais itens do pedido ${event.payload.orderId} ficaram indisponíveis.`,
        };
      }
      case 'shipment.created': {
        const event = envelope as ShipmentCreatedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Pedido ${event.payload.orderId} enviado — rastreio ${event.payload.trackingCode}`,
          text: `Seu pedido ${event.payload.orderId} foi enviado. Código de rastreio: ${event.payload.trackingCode}.`,
        };
      }
      case 'shipment.failed': {
        const event = envelope as ShipmentFailedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Problema no envio do pedido ${event.payload.orderId}`,
          text: `Houve um problema para enviar o pedido ${event.payload.orderId}.`,
        };
      }
      default:
        // stock.reserved, payment.refunded, stock.released e qualquer
        // eventType futuro — sem template nesta fase.
        return null;
    }
  }
}

/**
 * Nenhum evento carrega e-mail do cliente (confirmado lendo todos os
 * schemas em packages/contracts/src/events/*.ts — não existe esse campo).
 * Endereço FICTÍCIO determinístico a partir do customerId — sempre
 * @example.com, nunca domínio real: exigência de política de PII do
 * projeto (mascarar dado real / usar fictício).
 */
function emailFor(customerId: string): string {
  return `cliente-${customerId}@example.com`;
}
```

- [ ] **Step 6: Rodar o teste (requer infra no ar — Postgres e Mailhog)**

```bash
pnpm infra:up   # se ainda não estiver rodando (inclui o Mailhog)
pnpm --filter @ecommerce/contracts build
pnpm --filter @ecommerce/idempotency build
pnpm --filter @ecommerce/kafka build
pnpm --filter @ecommerce/notification-service test
```

Esperado: PASS, 4 testes.

- [ ] **Step 7: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/notification-service build
pnpm --filter @ecommerce/notification-service typecheck
pnpm --filter @ecommerce/notification-service lint
```

- [ ] **Step 8: Commit**

```bash
git add apps/notification-service
git commit -m "feat(notification-service): MailerService e NotificationEventHandler com templates por eventType"
```

---

### Task 7: Consumo Kafka de verdade — `app.module.ts`, `main.ts`, health, e2e via Mailhog

**Files:**
- Create: `apps/notification-service/src/infrastructure/notification-consumer.service.ts`
- Create: `apps/notification-service/src/health/health.controller.ts`
- Create: `apps/notification-service/src/app.module.ts`
- Create: `apps/notification-service/src/main.ts`
- Test: `apps/notification-service/test/notification.e2e.spec.ts`

**Interfaces:**
- Consumes: `NotificationEventHandler` (Task 6); `KafkaConsumerRuntime`,
  `EventProducer` de `@ecommerce/kafka`; `CONSUMER_GROUPS`,
  `SUBSCRIPTIONS`, `TOPICS` de `@ecommerce/contracts`; `env` da Task 5.
- Produces: serviço completo, executável via `pnpm --filter
  @ecommerce/notification-service dev`.

- [ ] **Step 1: Implementar o serviço de consumo**

`apps/notification-service/src/infrastructure/notification-consumer.service.ts`:
```ts
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
import { NotificationEventHandler } from '../application/notification-event.handler.js';

@Injectable()
export class NotificationConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-notification-service-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly handler: NotificationEventHandler) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.notification,
      // SUBSCRIPTIONS[CONSUMER_GROUPS.notification] = os 4 tópicos de
      // negócio — Notification é o único consumidor que já assina tudo
      // desde esta fase, sem nada a deferir para depois.
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.notification],
      producer: this.producer,
      handler: (ctx) => this.handler.handle(ctx.envelope),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }
}
```

- [ ] **Step 2: Health controller**

`apps/notification-service/src/health/health.controller.ts`:
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

- [ ] **Step 3: `app.module.ts`**

`apps/notification-service/src/app.module.ts`:
```ts
import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { MailerService } from './infrastructure/mailer.service.js';
import { NotificationConsumerService } from './infrastructure/notification-consumer.service.js';
import { NotificationEventHandler } from './application/notification-event.handler.js';

@Module({
  controllers: [HealthController],
  providers: [PrismaService, MailerService, NotificationEventHandler, NotificationConsumerService],
})
export class AppModule {}
```

- [ ] **Step 4: `main.ts`**

`apps/notification-service/src/main.ts`:
```ts
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { env } from './env.js';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  await app.listen(env.NOTIFICATION_SERVICE_PORT);
  console.log(`[notification-service] ouvindo na porta ${env.NOTIFICATION_SERVICE_PORT}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[notification-service] recebido ${signal}, encerrando graciosamente`);
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

- [ ] **Step 5: Escrever o teste e2e ponta a ponta (Kafka + Mailhog reais)**

`apps/notification-service/test/notification.e2e.spec.ts`:
```ts
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, TOPICS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { AppModule } from '../src/app.module.js';
import { env } from '../src/env.js';
import { clearMailhogInbox, findMailhogMessage, subjectOf } from './support/mailhog-client.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '1',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};
const ITEM = { sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 4990 };

describe('Notification Service — e2e (Kafka + Postgres + Mailhog reais, requer pnpm infra:up e pnpm topics:create)', () => {
  let app: INestApplication;
  let producer: EventProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.init();

    producer = new EventProducer({ brokers: env.KAFKA_BROKERS, clientId: 'notification-e2e-test-producer' });
    await producer.connect();

    // Dá tempo do consumer group 'notification-service' concluir o
    // join/rebalance antes de publicarmos — senão a mensagem pode ser
    // publicada cedo demais e nunca ser entregue a este consumidor.
    await new Promise((resolve) => setTimeout(resolve, 3000));
  });

  beforeEach(async () => {
    await clearMailhogInbox();
  });

  afterAll(async () => {
    await producer.disconnect();
    await app.close();
  });

  it('GET /health/live responde sem autenticação', async () => {
    await request(app.getHttpServer()).get('/health/live').expect(200);
  });

  it('order.created publicado de verdade em ecommerce.orders.v1 chega ao Mailhog com o assunto esperado', async () => {
    const orderId = randomUUID();
    const customerId = randomUUID();

    await producer.publish(
      TOPICS.orders,
      createEvent(orderEvents.orderCreated, {
        aggregateId: orderId,
        correlationId: orderId,
        producer: 'order-service-test@0.0.0',
        payload: {
          orderId,
          customerId,
          items: [ITEM],
          totalAmountCents: 4990,
          currency: 'BRL',
          shippingAddress: ADDRESS,
        },
      }),
    );

    const message = await findMailhogMessage((item) => subjectOf(item).includes(orderId), 15_000);
    expect(subjectOf(message)).toBe(`Recebemos seu pedido ${orderId}`);
    expect(message.To[0]).toEqual(
      expect.objectContaining({ Mailbox: `cliente-${customerId}`, Domain: 'example.com' }),
    );
  }, 20_000);
});
```

- [ ] **Step 6: Rodar a suite completa (requer infra no ar, migration aplicada e tópicos criados)**

```bash
pnpm infra:up          # se ainda não estiver rodando (inclui Mailhog)
pnpm topics:create     # garante os tópicos de retry/DLT para notification-service
pnpm --filter @ecommerce/notification-service test
```

Esperado: PASS, 6 testes no total (4 da Task 6 + 2 desta Task).

- [ ] **Step 7: Build, typecheck, lint**

```bash
pnpm --filter @ecommerce/notification-service build
pnpm --filter @ecommerce/notification-service typecheck
pnpm --filter @ecommerce/notification-service lint
```

- [ ] **Step 8: Verificação manual ponta a ponta**

```bash
pnpm --filter @ecommerce/notification-service dev &
sleep 2
kill %1
```

Esperado: log `[notification-service] ouvindo na porta 3004` e nenhum erro
de conexão com Kafka/Postgres/SMTP nos primeiros segundos.

- [ ] **Step 9: Commit**

```bash
git add apps/notification-service
git commit -m "feat(notification-service): consumo Kafka real dos 4 tópicos com envio via Mailhog"
```

---

## Verificação final da fase

- [ ] `pnpm --filter @ecommerce/shipping-service test` e `pnpm --filter
      @ecommerce/notification-service test` — todos os testes passam com
      `pnpm infra:up` e `pnpm topics:create` executados.
- [ ] `pnpm typecheck && pnpm lint` na raiz — sem erros nos dois serviços
      novos.
- [ ] Critério de pronto do `docs/PLAN.md` Fase 5, na parte que cabe a
      este documento: pedido com CEP `00000-XXX` faz o Shipping publicar
      `shipment.failed` (provado pelos testes); pedido com CEP normal
      publica `shipment.created` com o `Shipment` persistido; Notification
      envia e-mail (visível no Mailhog, `http://localhost:18025`) para
      cada `eventType` com template, sem duplicar em reentrega do mesmo
      `eventId`. **O restante do critério de pronto do PLAN.md**
      ("compensação dupla no `shipment.failed`: Inventory libera, Payment
      estorna") depende de Inventory e Payment também consumirem
      `shipment.failed` e emitirem `stock.released`/`payment.refunded` —
      isso está fora do escopo deste documento (ver "Escopo e limite deste
      documento" no topo) e é validado quando essa extensão for
      implementada.
- [ ] O teste de race condition do Shipping (`StockReservedHandler`, Task
      3, e o teste "TESTE MAIS IMPORTANTE" da Task 4) passam — prova de
      que `stock.reserved` chegando antes de `order.created` se recupera
      sozinho via retry.
- [ ] O teste de race condition do Notification (Task 6) passa — prova de
      que qualquer evento sem `customerId` direto (`payment.approved`,
      `payment.failed`, `stock.unavailable`, `shipment.created`,
      `shipment.failed`) chegando antes de `order.created` se recupera sem
      intervenção manual.
- [ ] `pnpm --filter @ecommerce/shipping-service dev` e `pnpm --filter
      @ecommerce/notification-service dev` sobem sem erro de conexão.

Com Shipping publicando `shipment.created`/`shipment.failed` de verdade e
Notification enviando e-mail para os 8 tipos de evento com template, o
núcleo funcional da saga coreografada (Fases 0–5) está completo: um
`POST /orders` percorre Order → Payment → Inventory → Shipping, com
Notification observando tudo. A compensação dupla de `shipment.failed`
(Inventory + Payment reagindo) e a resiliência mais ampla (sweeper de
timeout, dlq-inspector, testes de caos) ficam para a Fase 6.
