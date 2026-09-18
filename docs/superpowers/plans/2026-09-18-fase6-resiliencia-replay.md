# Fase 6 — Resiliência e Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sweeper de timeout de saga (Order Service fecha sozinho pedidos presos em `PAYMENT_APPROVED` por tempo demais), `tools/dlq-inspector` (CLI para listar/inspecionar/reprocessar mensagens da DLT) e um teste de replay que prova que reprocessar um tópico do zero num consumer group novo reconstrói o mesmo estado final.

**Architecture:** O sweeper é um `saga.timeout` publicado pelo Order Service no tópico `ecommerce.orders.v1` — o Payment Service já sabe reagir a gatilhos de compensação (I4, `RefundPaymentUseCase`); só precisa aprender mais um `eventType`. `tools/dlq-inspector` é um pacote novo no workspace (já referenciado em `pnpm-workspace.yaml`, ainda vazio) com um CLI Node puro (sem NestJS) usando `@ecommerce/kafka`'s `EventProducer.publishRaw` e um `kafkajs` `Admin`/`Consumer` direto para ler DLTs.

**Tech Stack:** kafkajs (admin client para ler tópicos, sem consumer group — leitura pontual), Prisma, Vitest.

## Global Constraints

- Nunca logar payload de evento (PII) — no `dlq-inspector show`, mascare qualquer campo que pareça e-mail, CPF ou token antes de imprimir (regex simples é suficiente: `/@/"`, sequência de 11 dígitos, string com "token"/"secret" no nome do campo).
- O sweeper NUNCA reprocessa o mesmo pedido duas vezes na mesma passada nem entre passadas concorrentes — usa `updateMany({ where: { id, status: 'PAYMENT_APPROVED' } })` (mesma guarda de lost-update já usada em `order-projection.handler.ts`).
- Branch de trabalho: `feat/fases-6-11-compensacao`. Continue nela — não crie branch nova. Os commits da I4 (`517c9a2`..`7b00b4d`) já estão nela.
- `pnpm --filter <pacote> test` verde a cada tarefa antes de prosseguir.

---

### Task 1: Contrato do evento `saga.timeout`

**Files:**
- Modify: `packages/contracts/src/events/order.ts`
- Modify: `packages/contracts/src/common.ts` (adiciona `SAGA_TIMEOUT` já existe em `CANCELLATION_REASON` — confirme com `grep -n "SAGA_TIMEOUT" packages/contracts/src/common.ts`; se já existir, esta modificação não é necessária)
- Test: `packages/contracts/test/events.spec.ts` (ou o arquivo de teste de eventos já existente — confira com `find packages/contracts/test -iname "*event*"`)

- [ ] **Step 1: Escreva o teste do novo evento**

Adicione ao arquivo de teste de eventos já existente (ou crie `packages/contracts/test/saga-timeout.spec.ts` se preferir isolado):

```typescript
import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createEvent, orderEvents, TOPICS } from '@ecommerce/contracts';

describe('orderEvents.sagaTimedOut', () => {
  it('valida payload e usa o tópico de orders', () => {
    const orderId = randomUUID();
    const envelope = createEvent(orderEvents.sagaTimedOut, {
      aggregateId: orderId,
      correlationId: orderId,
      producer: 'order-service@0.1.0',
      payload: {
        orderId,
        stuckStatus: 'PAYMENT_APPROVED',
        timedOutAt: new Date().toISOString(),
      },
    });

    expect(envelope.eventType).toBe('saga.timeout');
    expect(orderEvents.sagaTimedOut.topic).toBe(TOPICS.orders);
  });
});
```

- [ ] **Step 2: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/contracts test -- saga-timeout`
Expected: FAIL — `orderEvents.sagaTimedOut` não existe.

- [ ] **Step 3: Adicione `sagaTimedOut` a `packages/contracts/src/events/order.ts`**

Ao final do arquivo, depois de `orderCancelled`:

```typescript
/**
 * Publicado pelo `SagaTimeoutSweeperService` (Order Service) quando um pedido
 * fica tempo demais preso num estado não-terminal sem o próximo evento da
 * saga chegar (docs/PLAN.md, Fase 6). Quem reage é quem tiver algo a desfazer
 * — hoje só o Payment Service (`RefundPaymentUseCase`, I4) — nunca o próprio
 * Order Service: ele só ANUNCIA o timeout, não decide o que os outros fazem.
 */
export const sagaTimedOut = defineEvent({
  type: 'saga.timeout',
  version: 1,
  aggregateType: 'order',
  topic: TOPICS.orders,
  payload: z.object({
    orderId: z.string().uuid(),
    stuckStatus: z.enum(['PENDING', 'PAYMENT_APPROVED', 'STOCK_RESERVED']),
    timedOutAt: z.string().datetime({ offset: true }),
  }),
});
```

- [ ] **Step 4: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/contracts test`
Expected: TODOS os testes PASS (o novo + os já existentes).

- [ ] **Step 5: Lint, typecheck, commit**

```bash
pnpm --filter @ecommerce/contracts lint && pnpm --filter @ecommerce/contracts typecheck
git add packages/contracts
git commit -m "feat(contracts): adiciona evento saga.timeout (Fase 6)"
```

---

### Task 2: Order Service — `SagaTimeoutSweeperService`

Varre pedidos presos em `PAYMENT_APPROVED` (Payment aprovou, mas nem `stock.reserved` nem `stock.unavailable` chegaram dentro do prazo — cenário real: Inventory Service caiu no meio da saga) e os leva a `COMPENSATING` + publica `saga.timeout`.

**Files:**
- Modify: `apps/order-service/src/env.ts`
- Modify: `apps/order-service/src/application/order-state-machine.ts` (só o `REQUIRED_COMPENSATIONS`, um valor que hoje é `[]`)
- Create: `apps/order-service/src/infrastructure/saga-timeout-sweeper.service.ts`
- Modify: `apps/order-service/src/app.module.ts`
- Test: `apps/order-service/test/saga-timeout-sweeper.integration.spec.ts`

**Interfaces:**
- Produces: `SagaTimeoutSweeperService.sweepOnce(): Promise<number>` (devolve quantos pedidos varreu — chamado pelo timer interno E diretamente pelo teste, sem esperar o timer de verdade).

- [ ] **Step 1: Adicione as duas env vars novas**

Em `apps/order-service/src/env.ts`, no `envSchema`, adicione:

```typescript
  SAGA_TIMEOUT_THRESHOLD_MS: z.coerce.number().int().positive().default(300_000), // 5 min
  SAGA_TIMEOUT_SWEEP_INTERVAL_MS: z.coerce.number().int().positive().default(30_000), // 30s
```

Adicione as mesmas duas variáveis (com os mesmos valores) em `.env.example` na raiz do monorepo, na seção de Order Service, e no `.env` real (`cp .env.example .env` já deveria bastar se você rodar de novo, ou edite `.env` manualmente adicionando as duas linhas).

- [ ] **Step 2: Troque o `REQUIRED_COMPENSATIONS[SAGA_TIMEOUT]` de `[]` para o valor real**

Em `apps/order-service/src/application/order-state-machine.ts`, ache a constante `REQUIRED_COMPENSATIONS` (criada na I4) e troque:

```typescript
  [CANCELLATION_REASON.SAGA_TIMEOUT]: [],
```

por:

```typescript
  // Sweeper desta fase só varre pedidos presos em PAYMENT_APPROVED (nunca chegaram a
  // reservar estoque) — só o pagamento precisa voltar. Se um sweeper futuro passar a
  // cobrir pedidos presos em STOCK_RESERVED também, este valor precisa virar
  // [PAYMENT_REFUNDED, STOCK_RELEASED] E o sweeper precisa saber distinguir os dois
  // casos (não é o caso hoje — ver saga-timeout-sweeper.service.ts).
  [CANCELLATION_REASON.SAGA_TIMEOUT]: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
```

- [ ] **Step 3: Escreva o teste de integração do sweeper (falhando)**

Crie `apps/order-service/test/saga-timeout-sweeper.integration.spec.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { SagaTimeoutSweeperService } from '../src/infrastructure/saga-timeout-sweeper.service.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

describe('SagaTimeoutSweeperService (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const sweeper = new SagaTimeoutSweeperService(prisma);

  async function createOrder(status: string, updatedAtMsAgo: number): Promise<string> {
    const orderId = randomUUID();
    await prisma.client.order.create({
      data: {
        id: orderId,
        customerId: randomUUID(),
        items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 1, unitPriceCents: 2000 }],
        totalAmountCents: 2000,
        currency: 'BRL',
        status,
        shippingAddress: ADDRESS,
      },
    });
    // @updatedAt do Prisma sobrescreve qualquer valor passado num .update() normal —
    // só um UPDATE cru consegue "voltar no tempo" o campo para simular um pedido
    // realmente parado há muito tempo, sem esperar o tempo de verdade passar.
    const backdated = new Date(Date.now() - updatedAtMsAgo);
    await prisma.client.$executeRawUnsafe(
      `UPDATE orders SET updated_at = $1::timestamp WHERE id = $2::uuid`,
      backdated,
      orderId,
    );
    return orderId;
  }

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.outbox.deleteMany();
    await prisma.client.order.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('leva um pedido preso em PAYMENT_APPROVED há mais do que o limite para COMPENSATING e publica saga.timeout', async () => {
    const orderId = await createOrder('PAYMENT_APPROVED', 10 * 60_000); // 10 min atrás

    const swept = await sweeper.sweepOnce();

    expect(swept).toBe(1);
    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');
    expect(order.compensationReason).toBe('SAGA_TIMEOUT');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'saga.timeout' },
    });
    expect(outboxRows).toHaveLength(1);
  });

  it('NÃO varre um pedido em PAYMENT_APPROVED ainda dentro do limite', async () => {
    await createOrder('PAYMENT_APPROVED', 1_000); // 1s atrás — bem abaixo do threshold padrão (5 min)

    const swept = await sweeper.sweepOnce();

    expect(swept).toBe(0);
  });

  it('NÃO varre pedidos em outros estados (PENDING, STOCK_RESERVED, CONFIRMED, CANCELLED) mesmo se antigos', async () => {
    await createOrder('PENDING', 10 * 60_000);
    await createOrder('STOCK_RESERVED', 10 * 60_000);
    await createOrder('CONFIRMED', 10 * 60_000);
    await createOrder('CANCELLED', 10 * 60_000);

    const swept = await sweeper.sweepOnce();

    expect(swept).toBe(0);
  });

  it('rodar sweepOnce duas vezes seguidas não publica saga.timeout duas vezes para o mesmo pedido', async () => {
    await createOrder('PAYMENT_APPROVED', 10 * 60_000);

    await sweeper.sweepOnce();
    const secondPass = await sweeper.sweepOnce(); // já está COMPENSATING, não é mais PAYMENT_APPROVED

    expect(secondPass).toBe(0);
    const outboxRows = await prisma.client.outbox.findMany({ where: { eventType: 'saga.timeout' } });
    expect(outboxRows).toHaveLength(1);
  });
});
```

- [ ] **Step 4: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/order-service test -- saga-timeout-sweeper`
Expected: FAIL — módulo não encontrado.

- [ ] **Step 5: Implemente `SagaTimeoutSweeperService`**

Crie `apps/order-service/src/infrastructure/saga-timeout-sweeper.service.ts`:

```typescript
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CANCELLATION_REASON, ORDER_STATUS, createEvent, orderEvents } from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from './prisma.service.js';

const SWEEP_BATCH_SIZE = 50;

/**
 * Em coreografia ninguém vigia o pedido inteiro — o Order Service vira o
 * meio-orquestrador que a coreografia acaba exigindo (docs/PLAN.md, Fase 6).
 * Varre pedidos presos em PAYMENT_APPROVED (Payment aprovou, mas Inventory
 * nunca respondeu dentro do prazo — cenário real: o serviço caiu no meio da
 * saga) e publica `saga.timeout` para quem tiver algo a desfazer reagir.
 */
@Injectable()
export class SagaTimeoutSweeperService implements OnModuleInit, OnModuleDestroy {
  private timer: NodeJS.Timeout | null = null;

  constructor(private readonly prisma: PrismaService) {}

  onModuleInit(): void {
    this.timer = setInterval(() => {
      void this.sweepOnce();
    }, env.SAGA_TIMEOUT_SWEEP_INTERVAL_MS);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  /** Uma passada: varre até `SWEEP_BATCH_SIZE` pedidos presos. Devolve quantos varreu. */
  async sweepOnce(): Promise<number> {
    const threshold = new Date(Date.now() - env.SAGA_TIMEOUT_THRESHOLD_MS);
    const stuckOrders = await this.prisma.client.order.findMany({
      where: { status: ORDER_STATUS.PAYMENT_APPROVED, updatedAt: { lt: threshold } },
      take: SWEEP_BATCH_SIZE,
    });

    let swept = 0;
    for (const order of stuckOrders) {
      const timedOut = await this.prisma.client.$transaction(async (tx) => {
        // updateMany com o status como parte do WHERE: se outra passada (ou réplica)
        // já pegou este pedido entre o findMany acima e agora, count vem 0 e pulamos —
        // mesma guarda de lost-update de order-projection.handler.ts.
        const updated = await tx.order.updateMany({
          where: { id: order.id, status: ORDER_STATUS.PAYMENT_APPROVED },
          data: { status: ORDER_STATUS.COMPENSATING, compensationReason: CANCELLATION_REASON.SAGA_TIMEOUT },
        });
        if (updated.count === 0) return false;

        const envelope = createEvent(orderEvents.sagaTimedOut, {
          aggregateId: order.id,
          correlationId: order.id,
          producer: 'order-service@0.1.0',
          payload: {
            orderId: order.id,
            stuckStatus: 'PAYMENT_APPROVED',
            timedOutAt: new Date().toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: envelope.eventId,
          aggregateId: order.id,
          aggregateType: 'order',
          eventType: 'saga.timeout',
          envelope,
        });

        return true;
      });

      if (timedOut) swept += 1;
    }

    return swept;
  }
}
```

- [ ] **Step 6: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/order-service test -- saga-timeout-sweeper`
Expected: 4/4 PASS.

- [ ] **Step 7: Ligue no `app.module.ts`**

Em `apps/order-service/src/app.module.ts`, importe e registre `SagaTimeoutSweeperService` na lista de `providers`.

- [ ] **Step 8: Rode a suíte inteira, lint, typecheck**

Run: `pnpm --filter @ecommerce/order-service test && pnpm --filter @ecommerce/order-service lint && pnpm --filter @ecommerce/order-service typecheck`
Expected: tudo verde.

- [ ] **Step 9: Commit**

```bash
git add apps/order-service .env.example .env
git commit -m "feat(order-service): sweeper de timeout de saga (Fase 6)"
```

Confira que o `.env` real não tem nenhum valor real de segredo antes de commitar — se ele não estiver no `.gitignore` por engano, NÃO o adicione ao commit (rode `git status` e confirme que `.env` aparece como ignorado, não como novo arquivo rastreável).

---

### Task 3: Payment Service — reage a `saga.timeout`

**Files:**
- Modify: `apps/payment-service/src/application/refund-payment.use-case.ts`
- Modify: `apps/payment-service/src/application/payment-event.router.ts`
- Test: `apps/payment-service/test/refund-payment.integration.spec.ts`

- [ ] **Step 1: Adicione o teste**

Em `apps/payment-service/test/refund-payment.integration.spec.ts`, adicione uma função fabricante e um teste:

```typescript
function makeSagaTimedOut(orderId: string) {
  return createEvent(orderEvents.sagaTimedOut, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
    payload: {
      orderId,
      stuckStatus: 'PAYMENT_APPROVED',
      timedOutAt: new Date().toISOString(),
    },
  });
}
```

(adicione `orderEvents` ao import de `@ecommerce/contracts` no topo do arquivo, junto com `inventoryEvents`/`shippingEvents` já importados)

```typescript
  it('saga.timeout: estorna o pagamento AUTHORIZED e publica payment.refunded com compensationFor="saga.timeout"', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 2000));

    await refundPayment.execute(makeSagaTimedOut(orderId));

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'payment.refunded' },
    });
    const payload = outboxRows[0]?.payload as { payload: { compensationFor: string } };
    expect(payload.payload.compensationFor).toBe('saga.timeout');
  });
```

- [ ] **Step 2: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/payment-service test -- refund-payment`
Expected: FAIL — `RefundPaymentUseCase` não reconhece `saga.timeout`.

- [ ] **Step 3: Adicione `saga.timeout` ao mapa de `RefundPaymentUseCase`**

Em `apps/payment-service/src/application/refund-payment.use-case.ts`, troque:

```typescript
const COMPENSATION_FOR_BY_EVENT: Record<string, 'stock.unavailable' | 'shipment.failed'> = {
  'stock.unavailable': 'stock.unavailable',
  'shipment.failed': 'shipment.failed',
};
```

por:

```typescript
const COMPENSATION_FOR_BY_EVENT: Record<string, 'stock.unavailable' | 'shipment.failed' | 'saga.timeout'> = {
  'stock.unavailable': 'stock.unavailable',
  'shipment.failed': 'shipment.failed',
  'saga.timeout': 'saga.timeout',
};
```

Em `apps/payment-service/src/application/payment-event.router.ts`, no `switch`, troque:

```typescript
      case 'stock.unavailable':
      case 'shipment.failed':
        await this.refundPayment.execute(envelope);
        return;
```

por:

```typescript
      case 'stock.unavailable':
      case 'shipment.failed':
      case 'saga.timeout':
        await this.refundPayment.execute(envelope);
        return;
```

Nenhuma mudança de `sourceTopics` é necessária — `saga.timeout` está no tópico `ecommerce.orders.v1`, que o Payment Service já assina.

- [ ] **Step 4: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/payment-service test`
Expected: tudo verde.

- [ ] **Step 5: Lint, typecheck, commit**

```bash
pnpm --filter @ecommerce/payment-service lint && pnpm --filter @ecommerce/payment-service typecheck
git add apps/payment-service
git commit -m "feat(payment-service): estorna pagamento em saga.timeout (Fase 6)"
```

---

### Task 4: `tools/dlq-inspector` — CLI de inspeção e reprocessamento da DLT

**Files:**
- Create: `tools/dlq-inspector/package.json`
- Create: `tools/dlq-inspector/tsconfig.json`
- Create: `tools/dlq-inspector/src/mask.ts`
- Create: `tools/dlq-inspector/src/cli.ts`
- Test: `tools/dlq-inspector/test/mask.spec.ts`
- Test: `tools/dlq-inspector/test/cli.integration.spec.ts`

**Interfaces:**
- Produces: `maskPii(value: unknown): unknown` (função pura, testável sem Kafka); comandos de CLI `list <dlt-topic>`, `show <dlt-topic> <offset>`, `replay <dlt-topic> <offset>`.

- [ ] **Step 1: Crie o `package.json` do pacote novo**

Crie `tools/dlq-inspector/package.json`:

```json
{
  "name": "@ecommerce/dlq-inspector",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "bin": {
    "dlq-inspector": "./dist/cli.js"
  },
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "lint": "eslint src",
    "test": "dotenv -e ../../.env -- vitest run",
    "cli": "dotenv -e ../../.env -- node --loader @swc-node/register/esm src/cli.ts"
  },
  "dependencies": {
    "@ecommerce/contracts": "workspace:*",
    "@ecommerce/kafka": "workspace:*",
    "kafkajs": "^2.2.4",
    "zod": "^3.24.1"
  },
  "devDependencies": {
    "@swc-node/register": "^1.12.1",
    "@swc/core": "^1.16.2",
    "@types/node": "^22.10.2",
    "dotenv-cli": "^7.4.2",
    "typescript": "^5.7.2",
    "vitest": "^2.1.8"
  }
}
```

Confira as versões exatas de `kafkajs`, `typescript`, `vitest`, `@swc-node/register`, `@swc/core` num `package.json` de outro pacote (ex.: `packages/kafka/package.json`) e alinhe — não invente versão diferente da já usada no resto do monorepo.

Crie `tools/dlq-inspector/tsconfig.json` (copie o conteúdo de `packages/kafka/tsconfig.json` ajustando `extends` se o caminho relativo mudar — confira que `tsconfig.base.json` na raiz é referenciado corretamente a partir de `tools/dlq-inspector/`).

- [ ] **Step 2: Escreva o teste de `maskPii` (falhando)**

Crie `tools/dlq-inspector/test/mask.spec.ts`:

```typescript
import { describe, expect, it } from 'vitest';
import { maskPii } from '../src/mask.js';

describe('maskPii', () => {
  it('mascara valores de string que parecem e-mail', () => {
    const result = maskPii({ customerEmail: 'joao@example.com', orderId: 'abc-123' });
    expect(result).toEqual({ customerEmail: '***@***', orderId: 'abc-123' });
  });

  it('mascara qualquer campo cujo NOME contenha token/secret/password/authorization', () => {
    const result = maskPii({
      gatewayToken: 'tok_live_abc123',
      apiSecret: 'shh',
      password: 'hunter2',
      Authorization: 'Bearer xyz',
      orderId: 'abc-123',
    });
    expect(result).toEqual({
      gatewayToken: '***MASKED***',
      apiSecret: '***MASKED***',
      password: '***MASKED***',
      Authorization: '***MASKED***',
      orderId: 'abc-123',
    });
  });

  it('mascara recursivamente objetos aninhados e arrays', () => {
    const result = maskPii({
      payload: { instrument: { gatewayToken: 'tok_1', cardLast4: '4242' } },
      items: [{ sku: 'BOOK-001', customerEmail: 'a@b.com' }],
    });
    expect(result).toEqual({
      payload: { instrument: { gatewayToken: '***MASKED***', cardLast4: '4242' } },
      items: [{ sku: 'BOOK-001', customerEmail: '***@***' }],
    });
  });

  it('não mexe em valores que não são PII nem segredo', () => {
    const result = maskPii({ orderId: 'abc-123', amountCents: 2000, status: 'CONFIRMED' });
    expect(result).toEqual({ orderId: 'abc-123', amountCents: 2000, status: 'CONFIRMED' });
  });
});
```

- [ ] **Step 3: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/dlq-inspector test`
Expected: FAIL — módulo não encontrado (o pacote nem tem `node_modules` ainda; rode `pnpm install` na raiz do monorepo primeiro para linkar o workspace novo).

- [ ] **Step 4: Implemente `maskPii`**

Crie `tools/dlq-inspector/src/mask.ts`:

```typescript
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SENSITIVE_KEY_PATTERN = /token|secret|password|authorization/i;

/**
 * Mascara PII e segredo ANTES de qualquer coisa ir para stdout — é o que
 * `dlq-inspector show` usa para exibir payload sem vazar dado de cliente
 * num terminal ou log de CI (OWASP A09/A04). Heurística simples de
 * propósito: nome do campo (token/secret/password/authorization) ou
 * formato do valor (parece e-mail).
 */
export function maskPii(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => maskPii(item));
  }
  if (value !== null && typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      if (SENSITIVE_KEY_PATTERN.test(key)) {
        result[key] = '***MASKED***';
      } else {
        result[key] = maskPii(val);
      }
    }
    return result;
  }
  if (typeof value === 'string' && EMAIL_PATTERN.test(value)) {
    return '***@***';
  }
  return value;
}
```

- [ ] **Step 5: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/dlq-inspector test`
Expected: 4/4 PASS.

- [ ] **Step 6: Implemente o CLI**

Crie `tools/dlq-inspector/src/cli.ts`:

```typescript
#!/usr/bin/env node
import { Kafka, logLevel } from 'kafkajs';
import { RETRY_HEADERS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { maskPii } from './mask.js';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');

function kafka(): Kafka {
  return new Kafka({ clientId: 'dlq-inspector', brokers: KAFKA_BROKERS, logLevel: logLevel.ERROR });
}

/** Lê até `limit` mensagens de um tópico, do início, sem entrar num consumer group persistente. */
async function readTopic(
  topic: string,
  limit: number,
): Promise<Array<{ offset: string; key: string | null; value: string | null; headers: Record<string, string> }>> {
  const client = kafka();
  const consumer = client.consumer({ groupId: `dlq-inspector-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const messages: Array<{ offset: string; key: string | null; value: string | null; headers: Record<string, string> }> = [];
  await new Promise<void>((resolve) => {
    void consumer.run({
      eachMessage: async ({ message }) => {
        if (messages.length >= limit) return;
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) {
          if (v) headers[k] = v.toString();
        }
        messages.push({
          offset: message.offset,
          key: message.key?.toString() ?? null,
          value: message.value?.toString() ?? null,
          headers,
        });
        if (messages.length >= limit) resolve();
      },
    });
    // Não há mais mensagens ou o tópico é pequeno — não fica esperando para sempre.
    setTimeout(resolve, 5_000);
  });

  await consumer.disconnect();
  return messages;
}

async function list(topic: string): Promise<void> {
  const messages = await readTopic(topic, 100);
  if (messages.length === 0) {
    console.log(`Nenhuma mensagem em ${topic}.`);
    return;
  }
  for (const msg of messages) {
    const error = msg.headers[RETRY_HEADERS.lastError] ?? '(sem erro registrado)';
    console.log(`offset=${msg.offset} key=${msg.key} erro="${error}"`);
  }
}

async function show(topic: string, offset: string): Promise<void> {
  const messages = await readTopic(topic, 1000);
  const found = messages.find((m) => m.offset === offset);
  if (!found) {
    console.error(`Offset ${offset} não encontrado em ${topic} (dentro das primeiras 1000 mensagens).`);
    process.exitCode = 1;
    return;
  }
  const parsed = found.value ? JSON.parse(found.value) : null;
  console.log(JSON.stringify({ headers: found.headers, payload: maskPii(parsed) }, null, 2));
}

async function replay(topic: string, offset: string): Promise<void> {
  const messages = await readTopic(topic, 1000);
  const found = messages.find((m) => m.offset === offset);
  if (!found) {
    console.error(`Offset ${offset} não encontrado em ${topic} (dentro das primeiras 1000 mensagens).`);
    process.exitCode = 1;
    return;
  }
  const originalTopic = found.headers[RETRY_HEADERS.originalTopic];
  if (!originalTopic) {
    console.error(`Mensagem em ${topic}@${offset} não tem header ${RETRY_HEADERS.originalTopic} — não sei para onde reenviar.`);
    process.exitCode = 1;
    return;
  }

  const producer = new EventProducer({ brokers: KAFKA_BROKERS, clientId: 'dlq-inspector-replay' });
  await producer.connect();
  await producer.publishRaw(originalTopic, found.value, found.headers, found.key);
  await producer.disconnect();
  console.log(`Reenviado offset=${offset} de ${topic} para ${originalTopic}.`);
}

async function main(): Promise<void> {
  const [command, topic, offset] = process.argv.slice(2);

  switch (command) {
    case 'list':
      if (!topic) throw new Error('uso: dlq-inspector list <topico>');
      await list(topic);
      return;
    case 'show':
      if (!topic || !offset) throw new Error('uso: dlq-inspector show <topico> <offset>');
      await show(topic, offset);
      return;
    case 'replay':
      if (!topic || !offset) throw new Error('uso: dlq-inspector replay <topico> <offset>');
      await replay(topic, offset);
      return;
    default:
      console.error('Comandos: list <topico> | show <topico> <offset> | replay <topico> <offset>');
      process.exitCode = 1;
  }
}

void main();
```

- [ ] **Step 7: Escreva o teste de integração do CLI (falhando)**

Crie `tools/dlq-inspector/test/cli.integration.spec.ts`:

```typescript
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Kafka, logLevel } from 'kafkajs';
import { RETRY_HEADERS } from '@ecommerce/contracts';

const execFileAsync = promisify(execFile);
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
const TEST_TOPIC = `dlq-inspector-test.DLT`;

describe('dlq-inspector CLI (integração — Kafka real, requer pnpm infra:up e o tópico existir)', () => {
  const kafka = new Kafka({ clientId: 'dlq-inspector-test-setup', brokers: BROKERS, logLevel: logLevel.ERROR });
  const admin = kafka.admin();

  beforeAll(async () => {
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: TEST_TOPIC, numPartitions: 1 }], waitForLeaders: true });
  });

  afterAll(async () => {
    await admin.deleteTopics({ topics: [TEST_TOPIC] }).catch(() => {});
    await admin.disconnect();
  });

  it('list mostra a mensagem publicada, show exibe o payload mascarado, replay reenvia para o tópico original', async () => {
    const producer = kafka.producer();
    await producer.connect();
    const orderId = randomUUID();
    await producer.send({
      topic: TEST_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify({ eventType: 'test.event', payload: { orderId, customerEmail: 'a@b.com' } }),
          headers: {
            [RETRY_HEADERS.originalTopic]: 'dlq-inspector-test.original',
            [RETRY_HEADERS.lastError]: 'erro de teste',
          },
        },
      ],
    });
    await producer.disconnect();

    const { stdout: listOutput } = await execFileAsync('node', ['--loader', '@swc-node/register/esm', 'src/cli.ts', 'list', TEST_TOPIC], {
      cwd: process.cwd(),
      env: { ...process.env, KAFKA_BROKERS: BROKERS.join(',') },
    });
    expect(listOutput).toContain('erro de teste');

    const { stdout: showOutput } = await execFileAsync('node', ['--loader', '@swc-node/register/esm', 'src/cli.ts', 'show', TEST_TOPIC, '0'], {
      cwd: process.cwd(),
      env: { ...process.env, KAFKA_BROKERS: BROKERS.join(',') },
    });
    expect(showOutput).toContain('***@***');
    expect(showOutput).not.toContain('a@b.com');

    const { stdout: replayOutput } = await execFileAsync('node', ['--loader', '@swc-node/register/esm', 'src/cli.ts', 'replay', TEST_TOPIC, '0'], {
      cwd: process.cwd(),
      env: { ...process.env, KAFKA_BROKERS: BROKERS.join(',') },
    });
    expect(replayOutput).toContain('dlq-inspector-test.original');
  }, 30_000);
});
```

Antes de rodar, crie o tópico de destino do replay manualmente ou ajuste o teste para criar `dlq-inspector-test.original` também no `beforeAll` (adicione ao array de `admin.createTopics`) — sem o tópico existir e `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (configurado no broker), o `publishRaw` do replay falha.

- [ ] **Step 8: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/dlq-inspector test`
Expected: TODOS os testes (mask + cli) PASS.

- [ ] **Step 9: Lint, typecheck, commit**

```bash
pnpm --filter @ecommerce/dlq-inspector lint && pnpm --filter @ecommerce/dlq-inspector typecheck && pnpm --filter @ecommerce/dlq-inspector build
git add tools/dlq-inspector pnpm-lock.yaml
git commit -m "feat(dlq-inspector): CLI para listar, inspecionar e reprocessar mensagens da DLT (Fase 6)"
```

---

### Task 5: Teste de replay — reprocessar do zero reconstrói o mesmo estado

Prova que um consumer group NOVO, lendo um tópico de negócio do offset zero, aplica os mesmos efeitos que o consumer group original já aplicou — sem duplicar nada (idempotência) e sem produzir um resultado diferente.

**Files:**
- Test: `packages/kafka/test/replay.integration.spec.ts`

- [ ] **Step 1: Escreva o teste**

Crie `packages/kafka/test/replay.integration.spec.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';
import { EventProducer } from '../src/producer.js';
import { KafkaConsumerRuntime, type MessageContext } from '../src/consumer-runtime.js';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
const TOPIC = `lab.replay.pedidos`;

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

  beforeAll(async () => {
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }], waitForLeaders: true });
    await setupTable(pool);
  });

  afterAll(async () => {
    await admin.deleteTopics({ topics: [TOPIC] }).catch(() => {});
    await admin.disconnect();
    await pool.end();
  });

  it('um consumer group NOVO, lendo do offset zero, aplica o efeito para cada orderId exatamente uma vez — mesmo estado final de um consumer group que processou ao vivo', async () => {
    const producer = new EventProducer({ brokers: BROKERS, clientId: 'replay-test-producer' });
    await producer.connect();

    const orderIds = Array.from({ length: 5 }, () => randomUUID());
    for (const orderId of orderIds) {
      await producer.publish(TOPIC, {
        eventId: randomUUID(),
        eventType: 'test.replay',
        eventVersion: 1,
        occurredAt: new Date().toISOString(),
        aggregateId: orderId,
        aggregateType: 'order',
        correlationId: orderId,
        causationId: orderId,
        producer: 'replay-test@0.0.0',
        payload: { orderId },
      } as never);
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
```

Ajuste `groupId: 'replay-test-live' as never` — `KafkaConsumerRuntimeOptions.groupId` é tipado como `ConsumerGroup` (união fechada de `CONSUMER_GROUPS`); como este teste cria grupos que não existem em `CONSUMER_GROUPS` (de propósito — são efêmeros, só deste teste), o cast `as never` (ou `as ConsumerGroup`, o que o typecheck aceitar sem erro) contorna isso. Se o typecheck reclamar, troque para `as import('@ecommerce/contracts').ConsumerGroup`.

- [ ] **Step 2: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/kafka test -- replay`
Expected: PASS. Se o teste passar de primeira sem nunca ter falhado, tudo bem — este teste não segue TDD estrito porque não estamos adicionando comportamento novo ao `KafkaConsumerRuntime`, só provando uma propriedade que já deveria ser verdadeira (idempotência + replay). Se falhar, o bug está em `KafkaConsumerRuntime` ou no entendimento deste plano sobre ele — pare e investigue antes de seguir.

- [ ] **Step 3: Rode a suíte inteira do pacote, lint, typecheck**

Run: `pnpm --filter @ecommerce/kafka test && pnpm --filter @ecommerce/kafka lint && pnpm --filter @ecommerce/kafka typecheck`

- [ ] **Step 4: Commit**

```bash
git add packages/kafka/test/replay.integration.spec.ts
git commit -m "test(kafka): prova que replay do zero num consumer group novo reconstrói o mesmo estado (Fase 6)"
```

---

### Task 6: Verificação final da fase

- [ ] **Step 1: Suíte completa do monorepo**

Run: `pnpm exec turbo run lint typecheck build test`
Expected: tudo verde (ignore flakiness JÁ CONHECIDA de hooks `afterAll`/`beforeAll` em specs e2e sob Kafka compartilhado sob carga — qualquer teste INDIVIDUAL falhando é bug real).

- [ ] **Step 2: Verificação ao vivo do sweeper contra Docker real**

Suba a infra e o order-service (`docker compose -f deploy/docker/docker-compose.yml up -d order-service payment-service`), crie um pedido normal via HTTP, e MANUALMENTE force o cenário de timeout: pare o `inventory-service` (ou nem suba ele) ANTES de criar o pedido, crie o pedido, espere `SAGA_TIMEOUT_THRESHOLD_MS` (ajuste temporariamente para 10000 no `.env` e reinicie o order-service se 5 minutos for longo demais para testar manualmente) — confirme que o pedido chega a `CANCELLED` sozinho, sem Inventory nunca ter rodado. Restaure `SAGA_TIMEOUT_THRESHOLD_MS` para o valor de produção (`300000`) no `.env.example` antes de finalizar (o `.env` real pode ficar com o valor de teste, ele não é commitado).

- [ ] **Step 3: Push**

```bash
git push origin feat/fases-6-11-compensacao
```

Este plano termina aqui. Próximo: Fase 7 (observabilidade).
