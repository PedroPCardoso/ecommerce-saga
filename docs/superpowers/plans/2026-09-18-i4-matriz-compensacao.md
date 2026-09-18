# I4 — Matriz de Compensação da Saga — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar a matriz de compensação da saga: Payment Service estorna (`payment.refunded`) quando `stock.unavailable`/`shipment.failed` acontece, Inventory Service libera estoque (`stock.released`) quando `shipment.failed` acontece, e o Order Service finalmente fecha pedidos presos em `COMPENSATING` para `CANCELLED` assim que as compensações exigidas chegarem.

**Architecture:** Cada compensação segue o MESMO padrão já usado no resto do sistema: consumidor Kafka (retry ladder + DLT via `@ecommerce/kafka`) → handler dentro de uma transação Prisma → `markProcessed` (idempotência) + efeito de domínio + `insertOutboxRow` (outbox transacional). A novidade é só no Order Service: `COMPENSATING` deixa de ser terminal — o projetor passa a rastrear `compensationsReceived` (quais compensações já chegaram) contra `compensationReason` (qual falha exige quais compensações) e fecha em `CANCELLED` só quando a lista exigida está completa.

**Tech Stack:** NestJS, kafkajs (via `@ecommerce/kafka`), Prisma 6, Zod, Vitest (testes de integração contra Kafka+Postgres reais).

## Global Constraints

- Nunca implemente um handler que aceite reprocessar um evento sem `markProcessed` DENTRO da mesma transação Prisma do efeito de domínio (docs/PLAN.md, padrão obrigatório de idempotência).
- Transição de estado inválida por evento QUE AINDA NÃO CHEGOU NA HORA (corrida legítima entre tópicos) deve **lançar erro retriável DENTRO da transação, depois do `markProcessed`** — nunca `return` silencioso. Um `return` silencioso commitaria o registro de idempotência sem aplicar o efeito, perdendo o evento para sempre (foi exatamente o bug crítico C2 encontrado na revisão final deste projeto — não repita).
- Transição inválida por evento QUE JÁ FOI SUPERADO (reentrega tardia, pedido já fechado) deve logar em WARN e retornar sem lançar — é seguro commitar, não há efeito pendente.
- Todo evento novo precisa existir em `packages/contracts` (schema Zod + `defineEvent`) antes de qualquer serviço publicá-lo ou consumi-lo — os eventos `payment.refunded` e `stock.released` **já existem** em `packages/contracts/src/events/payment.ts` e `packages/contracts/src/events/inventory.ts` respectivamente; não precisam ser criados, só usados.
- Nunca logar o payload de um evento (pode carregar PII) — só metadados (orderId, eventType, motivo) — já é o padrão de todo `console.warn`/`console.error`/`this.logger.*` existente no projeto.
- `pnpm --filter <pacote> test` deve ficar verde a cada tarefa antes de prosseguir para a próxima.
- Branch de trabalho: `feat/fases-6-11-compensacao` (já existe e está com upstream configurado em `origin`). Commits vão direto nesta branch — não crie branch nova.

---

### Task 1: Contratos — nomeia o enum de compensação

Hoje `packages/contracts/src/events/order.ts` declara `compensationsApplied: z.array(z.enum(['PAYMENT_REFUNDED', 'STOCK_RELEASED']))` como enum **inline**, sem nome — toda outra enumeração do projeto (`ORDER_STATUS`, `CANCELLATION_REASON`, `PAYMENT_FAILURE_CODE`, `SHIPMENT_FAILURE_CODE`) é uma constante nomeada + schema Zod derivado. Esta tarefa só extrai o nome; não muda nenhum valor.

**Files:**
- Modify: `packages/contracts/src/common.ts`
- Modify: `packages/contracts/src/events/order.ts`
- Test: `packages/contracts/test/common.spec.ts` (crie se não existir; se já existir um arquivo de teste para `common.ts`, adicione lá)

**Interfaces:**
- Produces: `COMPENSATION_TYPE` (objeto `{ PAYMENT_REFUNDED: 'PAYMENT_REFUNDED', STOCK_RELEASED: 'STOCK_RELEASED' }`), `compensationTypeSchema` (Zod), `CompensationType` (tipo TS) — exportados de `@ecommerce/contracts` via `common.ts`. Usado pelas Tasks 3 e 4.

- [ ] **Step 1: Escreva o teste que prova o novo export**

Crie (ou abra, se já existir) `packages/contracts/test/common.spec.ts`:

```typescript
import { describe, expect, it } from 'vitest';
import { COMPENSATION_TYPE, compensationTypeSchema } from '../src/common.js';

describe('COMPENSATION_TYPE', () => {
  it('aceita PAYMENT_REFUNDED e STOCK_RELEASED, rejeita qualquer outro valor', () => {
    expect(compensationTypeSchema.parse('PAYMENT_REFUNDED')).toBe(COMPENSATION_TYPE.PAYMENT_REFUNDED);
    expect(compensationTypeSchema.parse('STOCK_RELEASED')).toBe(COMPENSATION_TYPE.STOCK_RELEASED);
    expect(() => compensationTypeSchema.parse('REFUND')).toThrow();
  });
});
```

- [ ] **Step 2: Rode o teste e confirme que falha**

Run: `pnpm --filter @ecommerce/contracts test -- common.spec`
Expected: FAIL — `Cannot find name 'COMPENSATION_TYPE'` ou `does not provide an export named 'COMPENSATION_TYPE'`.

- [ ] **Step 3: Adicione o enum nomeado em `common.ts`**

Abra `packages/contracts/src/common.ts`. Depois do bloco `CANCELLATION_REASON`/`cancellationReasonSchema` (já existente no arquivo, perto do fim), adicione:

```typescript
/**
 * O que a saga já desfez ao fechar um pedido em CANCELLED. Nomeado (não enum
 * inline) porque `order-state-machine.ts` do Order Service precisa comparar
 * contra ele para decidir se uma compensação pendente já está completa.
 */
export const COMPENSATION_TYPE = {
  PAYMENT_REFUNDED: 'PAYMENT_REFUNDED',
  STOCK_RELEASED: 'STOCK_RELEASED',
} as const;

export const compensationTypeSchema = z.nativeEnum(COMPENSATION_TYPE);
export type CompensationType = z.infer<typeof compensationTypeSchema>;
```

- [ ] **Step 4: Troque o enum inline em `order.ts` pelo novo nome**

Em `packages/contracts/src/events/order.ts`, veja o topo do arquivo — o import de `common.js` já traz `cancellationReasonSchema`. Adicione `compensationTypeSchema` a esse mesmo import:

```typescript
import {
  addressSchema,
  amountCentsSchema,
  cancellationReasonSchema,
  compensationTypeSchema,
  currencySchema,
  orderItemSchema,
} from '../common.js';
```

Depois, na definição de `orderCancelled`, troque a linha do `compensationsApplied`:

```typescript
    compensationsApplied: z.array(compensationTypeSchema),
```

(era `z.array(z.enum(['PAYMENT_REFUNDED', 'STOCK_RELEASED']))` — mesmos valores, só nomeado agora.)

- [ ] **Step 5: Rode os testes e confirme que passam — inclusive a suíte inteira de contracts, para garantir que o refactor não quebrou nada**

Run: `pnpm --filter @ecommerce/contracts test`
Expected: todos os testes (o novo + os 22 já existentes) PASS.

- [ ] **Step 6: Lint e typecheck**

Run: `pnpm --filter @ecommerce/contracts lint && pnpm --filter @ecommerce/contracts typecheck`
Expected: sem erros.

- [ ] **Step 7: Commit**

```bash
git add packages/contracts/src/common.ts packages/contracts/src/events/order.ts packages/contracts/test/common.spec.ts
git commit -m "feat(contracts): nomeia COMPENSATION_TYPE (antes enum inline em order.cancelled)"
```

---

### Task 2: Payment Service — consome `stock.unavailable`/`shipment.failed`, publica `payment.refunded`

Hoje `apps/payment-service/src/consumers/order-events-consumer.service.ts` só assina `TOPICS.orders`, embora `SUBSCRIPTIONS[CONSUMER_GROUPS.payment]` (em `packages/contracts/src/topics.ts`) já declare `[TOPICS.orders, TOPICS.inventory, TOPICS.shipping]` — os tópicos de retry/DLT desses dois já existem (criados por `pnpm topics:create`), só ninguém consome as mensagens de negócio ainda. Esta tarefa fecha essa lacuna extraindo um roteador (mesmo padrão de `InventoryEventRouter`/`ShippingEventRouter`) e adicionando o caso de uso de estorno.

**Files:**
- Create: `apps/payment-service/src/application/refund-payment.use-case.ts`
- Create: `apps/payment-service/src/application/payment-event.router.ts`
- Rename: `apps/payment-service/src/consumers/order-events-consumer.service.ts` → `apps/payment-service/src/consumers/payment-consumer.service.ts` (o consumidor agora cobre mais que `order.created`; renomeia para casar com o padrão `inventory-consumer.service.ts`/`shipping-consumer.service.ts`)
- Modify: `apps/payment-service/src/app.module.ts`
- Test: `apps/payment-service/test/refund-payment.integration.spec.ts`

**Interfaces:**
- Consumes: `EventOf`, `paymentEvents`, `inventoryEvents`, `shippingEvents`, `CONSUMER_GROUPS`, `SUBSCRIPTIONS`, `createEvent`, `parseAs` de `@ecommerce/contracts`; `insertOutboxRow` de `@ecommerce/outbox`; `markProcessed` de `@ecommerce/idempotency`; `PrismaService` de `../infrastructure/prisma.service.js`.
- Produces: `RefundPaymentUseCase.execute(envelope)` — aceita tanto o envelope de `stock.unavailable` quanto o de `shipment.failed` (ambos só precisam de `payload.orderId`, o resto do use case é idêntico); `PaymentEventRouter.route(envelope)` — chamado pelo `PaymentConsumerService` como handler único.

- [ ] **Step 1: Escreva o teste de integração do estorno (falhando)**

Crie `apps/payment-service/test/refund-payment.integration.spec.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, inventoryEvents, orderEvents, shippingEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { AuthorizePaymentUseCase } from '../src/application/authorize-payment.use-case.js';
import { RefundPaymentUseCase } from '../src/application/refund-payment.use-case.js';

const ADDRESS = {
  street: 'Rua Teste',
  number: '100',
  district: 'Centro',
  city: 'São Paulo',
  state: 'SP',
  zipCode: '01000-000',
  country: 'BR',
};

function makeOrderCreated(orderId: string, totalAmountCents: number) {
  return createEvent(orderEvents.orderCreated, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'order-service-test@0.0.0',
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

function makeStockUnavailable(orderId: string) {
  return createEvent(inventoryEvents.stockUnavailable, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'inventory-service-test@0.0.0',
    payload: {
      orderId,
      unavailableItems: [{ sku: 'OUT-999', requested: 1, available: 0 }],
      checkedAt: new Date().toISOString(),
    },
  });
}

function makeShipmentFailed(orderId: string) {
  return createEvent(shippingEvents.shipmentFailed, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'shipping-service-test@0.0.0',
    payload: {
      orderId,
      failureCode: 'ADDRESS_NOT_SERVICEABLE',
      reason: 'CEP fora da área de cobertura',
      failedAt: new Date().toISOString(),
    },
  });
}

describe('RefundPaymentUseCase (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const authorizePayment = new AuthorizePaymentUseCase(prisma);
  const refundPayment = new RefundPaymentUseCase(prisma);

  beforeEach(async () => {
    await prisma.onModuleInit();
    await prisma.client.payment.deleteMany();
    await prisma.client.outbox.deleteMany();
    await prisma.client.processedMessage.deleteMany();
  });

  afterAll(async () => {
    await prisma.onModuleDestroy();
  });

  it('stock.unavailable: estorna o pagamento AUTHORIZED e publica payment.refunded', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 2000));

    await refundPayment.execute(makeStockUnavailable(orderId));

    const payment = await prisma.client.payment.findUnique({ where: { orderId } });
    expect(payment?.status).toBe('REFUNDED');

    const outboxRows = await prisma.client.outbox.findMany({ where: { aggregateId: orderId } });
    expect(outboxRows).toHaveLength(2); // payment.approved (da autorização) + payment.refunded
    const refunded = outboxRows.find((r) => r.eventType === 'payment.refunded');
    expect(refunded).toBeDefined();
    const payload = refunded?.payload as { payload: { compensationFor: string; amountCents: number } };
    expect(payload.payload.compensationFor).toBe('stock.unavailable');
    expect(payload.payload.amountCents).toBe(2000);
  });

  it('shipment.failed: estorna o pagamento AUTHORIZED e publica payment.refunded com compensationFor correto', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 3000));

    await refundPayment.execute(makeShipmentFailed(orderId));

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'payment.refunded' },
    });
    const payload = outboxRows[0]?.payload as { payload: { compensationFor: string } };
    expect(payload.payload.compensationFor).toBe('shipment.failed');
  });

  it('reentrega do MESMO evento não estorna duas vezes — idempotência', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 2000));
    const envelope = makeStockUnavailable(orderId);

    await refundPayment.execute(envelope);
    await refundPayment.execute(envelope);

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'payment.refunded' },
    });
    expect(outboxRows).toHaveLength(1);
  });

  it('pagamento já REFUNDED (defesa extra contra corrida entre stock.unavailable/shipment.failed) não estorna de novo', async () => {
    const orderId = randomUUID();
    await authorizePayment.execute(makeOrderCreated(orderId, 2000));
    await refundPayment.execute(makeStockUnavailable(orderId));

    // Segundo evento de compensação, eventId DIFERENTE — markProcessed não pega isto.
    await refundPayment.execute(makeShipmentFailed(orderId));

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'payment.refunded' },
    });
    expect(outboxRows).toHaveLength(1); // continua só o primeiro estorno
  });

  it('Payment inexistente para o orderId é erro PERMANENTE — a cadeia causal garante que o Payment já existe', async () => {
    const orderId = randomUUID(); // nunca autorizado
    await expect(refundPayment.execute(makeStockUnavailable(orderId))).rejects.toMatchObject({
      permanent: true,
    });
  });
});
```

- [ ] **Step 2: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/payment-service test -- refund-payment`
Expected: FAIL — `Cannot find module '../src/application/refund-payment.use-case.js'`.

- [ ] **Step 3: Implemente `RefundPaymentUseCase`**

Crie `apps/payment-service/src/application/refund-payment.use-case.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { CONSUMER_GROUPS, createEvent, paymentEvents, type UnknownEnvelope } from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { markProcessed } from '@ecommerce/idempotency';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

const COMPENSATION_FOR_BY_EVENT: Record<string, 'stock.unavailable' | 'shipment.failed'> = {
  'stock.unavailable': 'stock.unavailable',
  'shipment.failed': 'shipment.failed',
};

/**
 * Estorna a autorização que o próprio Payment Service criou, em reação a uma
 * falha de um passo POSTERIOR da saga (docs/PLAN.md, matriz de compensação).
 * `stock.unavailable` e `shipment.failed` levam ao MESMO efeito aqui — a
 * diferença entre eles só importa para o `compensationFor` do evento publicado
 * e para o Order Service decidir se falta liberar estoque também.
 */
@Injectable()
export class RefundPaymentUseCase {
  constructor(private readonly prisma: PrismaService) {}

  async execute(envelope: UnknownEnvelope): Promise<void> {
    const compensationFor = COMPENSATION_FOR_BY_EVENT[envelope.eventType];
    if (!compensationFor) {
      throw new Error(`RefundPaymentUseCase não sabe processar eventType "${envelope.eventType}"`);
    }
    const orderId = (envelope.payload as { orderId: string }).orderId;

    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.payment);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const payment = await tx.payment.findUnique({ where: { orderId } });
      if (!payment) {
        // A cadeia causal da saga GARANTE que o Payment já existe: stock.unavailable e
        // shipment.failed só acontecem depois de payment.approved ter sido commitado
        // (Inventory só reserva DEPOIS de consumir payment.approved; Shipping só envia
        // DEPOIS de stock.reserved). Diferente do KnownOrder do Inventory, não há
        // corrida legítima aqui — Payment ausente é dado inconsistente, não atraso.
        const error = new Error(
          `Payment do pedido ${orderId} não encontrado ao processar ${envelope.eventType} — dado inconsistente`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }

      if (payment.status === 'REFUNDED') {
        // Defesa extra (A08/A10): só UM dos dois gatilhos de compensação pode acontecer
        // por pedido na coreografia atual (stock.unavailable e shipment.failed são
        // mutuamente exclusivos — o segundo só existe se o primeiro NÃO aconteceu), mas
        // isto não devia ser assumido silenciosamente. Evento com eventId diferente do
        // já processado (logo markProcessed não pegou) tentando estornar de novo é
        // ignorado com segurança.
        return;
      }

      const refundId = randomUUID();
      const refundedAt = new Date();

      await tx.payment.update({
        where: { orderId },
        data: { status: 'REFUNDED', updatedAt: refundedAt },
      });

      const refundedEnvelope = createEvent(paymentEvents.paymentRefunded, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'payment-service@0.1.0',
        payload: {
          paymentId: payment.id,
          orderId,
          refundId,
          amountCents: payment.amountCents,
          currency: payment.currency,
          compensationFor,
          refundedAt: refundedAt.toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: refundedEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'payment',
        eventType: 'payment.refunded',
        envelope: refundedEnvelope,
      });
    });
  }
}
```

- [ ] **Step 4: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/payment-service test -- refund-payment`
Expected: 5/5 PASS.

- [ ] **Step 5: Extraia o roteador e amplie o consumidor**

Crie `apps/payment-service/src/application/payment-event.router.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { orderEvents, parseAs, type UnknownEnvelope } from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { AuthorizePaymentUseCase } from './authorize-payment.use-case.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { RefundPaymentUseCase } from './refund-payment.use-case.js';

/**
 * Decide qual caso de uso chamar a partir de `envelope.eventType`. O envelope
 * já chegou validado (Zod, dentro do KafkaConsumerRuntime) contra o schema
 * exato do seu tipo+versão.
 */
@Injectable()
export class PaymentEventRouter {
  constructor(
    private readonly authorizePayment: AuthorizePaymentUseCase,
    private readonly refundPayment: RefundPaymentUseCase,
  ) {}

  async route(envelope: UnknownEnvelope): Promise<void> {
    switch (envelope.eventType) {
      case 'order.created':
        await this.authorizePayment.execute(parseAs(orderEvents.orderCreated, envelope));
        return;
      case 'stock.unavailable':
      case 'shipment.failed':
        await this.refundPayment.execute(envelope);
        return;
      default:
        // order.confirmed, order.cancelled, stock.reserved, stock.released — não são
        // assunto do Payment. Ignora e deixa o offset comitar normalmente.
        return;
    }
  }
}
```

Delete `apps/payment-service/src/consumers/order-events-consumer.service.ts` e crie `apps/payment-service/src/consumers/payment-consumer.service.ts` no lugar:

```typescript
import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { CONSUMER_GROUPS, SUBSCRIPTIONS } from '@ecommerce/contracts';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PaymentEventRouter } from '../application/payment-event.router.js';

@Injectable()
export class PaymentConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-payment-service-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly router: PaymentEventRouter) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.payment,
      // orders (gatilho da autorização) + inventory/shipping (gatilhos de compensação:
      // stock.unavailable e shipment.failed) — os três já declarados em
      // SUBSCRIPTIONS[payment] (packages/contracts/src/topics.ts).
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.payment],
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

- [ ] **Step 6: Atualize `app.module.ts`**

Abra `apps/payment-service/src/app.module.ts` e substitua pelo conteúdo:

```typescript
import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { AuthorizePaymentUseCase } from './application/authorize-payment.use-case.js';
import { RefundPaymentUseCase } from './application/refund-payment.use-case.js';
import { PaymentEventRouter } from './application/payment-event.router.js';
import { PaymentConsumerService } from './consumers/payment-consumer.service.js';

@Module({
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    AuthorizePaymentUseCase,
    RefundPaymentUseCase,
    PaymentEventRouter,
    PaymentConsumerService,
  ],
})
export class AppModule {}
```

- [ ] **Step 7: Rode a suíte inteira do serviço**

Run: `pnpm --filter @ecommerce/payment-service test`
Expected: TODOS os testes (os 9 já existentes + os 5 novos) PASS. Se `payment-service.e2e.spec.ts` referenciar `OrderEventsConsumerService` por nome em algum lugar, ajuste o import para `PaymentConsumerService` — confira com `grep -rn "OrderEventsConsumerService" apps/payment-service` antes de rodar.

- [ ] **Step 8: Lint e typecheck**

Run: `pnpm --filter @ecommerce/payment-service lint && pnpm --filter @ecommerce/payment-service typecheck`
Expected: sem erros.

- [ ] **Step 9: Commit**

```bash
git add apps/payment-service
git commit -m "feat(payment-service): estorna pagamento em stock.unavailable/shipment.failed (I4)"
```

---

### Task 3: Inventory Service — consome `shipment.failed`, publica `stock.released`

**Files:**
- Create: `apps/inventory-service/src/application/shipment-failed.handler.ts`
- Modify: `apps/inventory-service/src/application/inventory-event.router.ts`
- Modify: `apps/inventory-service/src/infrastructure/inventory-consumer.service.ts`
- Modify: `apps/inventory-service/src/app.module.ts`
- Test: `apps/inventory-service/test/shipment-failed-handler.integration.spec.ts`

**Interfaces:**
- Consumes: `EventOf`, `shippingEvents`, `inventoryEvents`, `CONSUMER_GROUPS`, `TOPICS`, `createEvent` de `@ecommerce/contracts`; `insertOutboxRow`; `markProcessed`; `PrismaService`.
- Produces: `ShipmentFailedHandler.handle(envelope)`.

- [ ] **Step 1: Escreva o teste de integração (falhando)**

Crie `apps/inventory-service/test/shipment-failed-handler.integration.spec.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createEvent, orderEvents, paymentEvents, shippingEvents } from '@ecommerce/contracts';
import { PrismaService } from '../src/infrastructure/prisma.service.js';
import { OrderCreatedHandler } from '../src/application/order-created.handler.js';
import { PaymentApprovedHandler } from '../src/application/payment-approved.handler.js';
import { ShipmentFailedHandler } from '../src/application/shipment-failed.handler.js';

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

function makeShipmentFailed(orderId: string) {
  return createEvent(shippingEvents.shipmentFailed, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'shipping-service-test@0.0.0',
    payload: {
      orderId,
      failureCode: 'ADDRESS_NOT_SERVICEABLE',
      reason: 'CEP fora da área de cobertura',
      failedAt: new Date().toISOString(),
    },
  });
}

describe('ShipmentFailedHandler (integração — Postgres real, requer pnpm infra:up)', () => {
  const prisma = new PrismaService();
  const orderCreatedHandler = new OrderCreatedHandler(prisma);
  const paymentApprovedHandler = new PaymentApprovedHandler(prisma);
  const shipmentFailedHandler = new ShipmentFailedHandler(prisma);

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

  it('libera a reserva RESERVED e publica stock.released', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 2 }]));
    await paymentApprovedHandler.handle(makePaymentApproved(orderId));

    await shipmentFailedHandler.handle(makeShipmentFailed(orderId));

    const reservation = await prisma.client.stockReservation.findFirst({ where: { orderId } });
    expect(reservation?.status).toBe('RELEASED');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'stock.released' },
    });
    expect(outboxRows).toHaveLength(1);
    const payload = outboxRows[0]?.payload as {
      payload: { compensationFor: string; items: unknown };
    };
    expect(payload.payload.compensationFor).toBe('shipment.failed');
    expect(payload.payload.items).toEqual([{ sku: 'BOOK-001', quantity: 2 }]);
  });

  it('reentrega do MESMO evento não libera duas vezes — idempotência', async () => {
    const orderId = randomUUID();
    await orderCreatedHandler.handle(makeOrderCreated(orderId, [{ sku: 'BOOK-001', quantity: 1 }]));
    await paymentApprovedHandler.handle(makePaymentApproved(orderId));
    const envelope = makeShipmentFailed(orderId);

    await shipmentFailedHandler.handle(envelope);
    await shipmentFailedHandler.handle(envelope);

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'stock.released' },
    });
    expect(outboxRows).toHaveLength(1);
  });

  it('reserva inexistente é erro PERMANENTE — shipment.failed só acontece depois de stock.reserved', async () => {
    const orderId = randomUUID(); // nunca reservado
    await expect(shipmentFailedHandler.handle(makeShipmentFailed(orderId))).rejects.toMatchObject({
      permanent: true,
    });
  });
});
```

- [ ] **Step 2: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/inventory-service test -- shipment-failed`
Expected: FAIL — módulo não encontrado.

- [ ] **Step 3: Implemente `ShipmentFailedHandler`**

Crie `apps/inventory-service/src/application/shipment-failed.handler.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { CONSUMER_GROUPS, createEvent, inventoryEvents, type EventOf, type shippingEvents } from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { markProcessed } from '@ecommerce/idempotency';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

export type ShipmentFailedEvent = EventOf<typeof shippingEvents.shipmentFailed>;

/**
 * Libera o estoque que a própria Inventory reservou, em reação a uma falha
 * POSTERIOR da saga (shipment.failed). Compensação dupla e paralela junto
 * com RefundPaymentUseCase (Payment Service) — o Order só fecha quando as
 * duas chegarem (docs/PLAN.md).
 */
@Injectable()
export class ShipmentFailedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: ShipmentFailedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.inventory);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const { orderId } = envelope.payload;
      const reservation = await tx.stockReservation.findFirst({ where: { orderId } });

      if (!reservation) {
        // shipment.failed só acontece depois de stock.reserved ter sido publicado (o
        // Shipping só tenta enviar depois de saber que reservou) — cadeia causal
        // garante que a reserva já existe. Ausente é dado inconsistente, não corrida.
        const error = new Error(
          `StockReservation do pedido ${orderId} não encontrada ao processar shipment.failed — dado inconsistente`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }

      if (reservation.status === 'RELEASED') {
        return; // defesa extra: já liberado (mesmo raciocínio do Payment Service)
      }

      await tx.stockReservation.update({
        where: { id: reservation.id },
        data: { status: 'RELEASED' },
      });

      const releasedEnvelope = createEvent(inventoryEvents.stockReleased, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'inventory-service@0.1.0',
        payload: {
          reservationId: reservation.id,
          orderId,
          items: reservation.items as Array<{ sku: string; quantity: number }>,
          compensationFor: 'shipment.failed',
          releasedAt: new Date().toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: releasedEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'stock-reservation',
        eventType: 'stock.released',
        envelope: releasedEnvelope,
      });
    });
  }
}
```

- [ ] **Step 4: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/inventory-service test -- shipment-failed`
Expected: 3/3 PASS.

- [ ] **Step 5: Ligue o handler no roteador e no consumidor**

Em `apps/inventory-service/src/application/inventory-event.router.ts`, adicione o import e o `case`:

```typescript
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { ShipmentFailedHandler, type ShipmentFailedEvent } from './shipment-failed.handler.js';
```

(adicione junto aos outros imports de handler no topo do arquivo) e no construtor:

```typescript
  constructor(
    private readonly orderCreatedHandler: OrderCreatedHandler,
    private readonly paymentApprovedHandler: PaymentApprovedHandler,
    private readonly shipmentFailedHandler: ShipmentFailedHandler,
  ) {}
```

e no `switch`, antes do `default`:

```typescript
      case 'shipment.failed':
        await this.shipmentFailedHandler.handle(envelope as ShipmentFailedEvent);
        return;
```

Atualize também o comentário do `default` (remova a menção a `shipment.failed` de lá, já que agora tem case próprio) para algo como:

```typescript
      default:
        // payment.failed (matriz de compensação: nada a fazer aqui), order.confirmed,
        // order.cancelled, stock.unavailable (o próprio Inventory que publicou),
        // stock.released (o próprio Inventory que publicou) — não são assunto de um
        // handler novo aqui. Ignora e deixa o offset comitar normalmente.
        return;
```

Em `apps/inventory-service/src/infrastructure/inventory-consumer.service.ts`, troque:

```typescript
      sourceTopics: [TOPICS.orders, TOPICS.payments],
```

por:

```typescript
      // orders (aprende itens) + payments (gatilho da reserva) + shipping (gatilho da
      // liberação em shipment.failed) — os três já declarados em
      // SUBSCRIPTIONS[inventory] (packages/contracts/src/topics.ts).
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.inventory],
```

e adicione `SUBSCRIPTIONS` ao import de `@ecommerce/contracts` no topo do arquivo (junto com `CONSUMER_GROUPS` e `TOPICS` — se `TOPICS` não for mais usado em nenhum outro lugar do arquivo depois desta troca, remova-o do import).

- [ ] **Step 6: Ligue o handler no `app.module.ts`**

Em `apps/inventory-service/src/app.module.ts`, importe e registre `ShipmentFailedHandler` junto aos outros providers:

```typescript
import { ShipmentFailedHandler } from './application/shipment-failed.handler.js';
```

e na lista de `providers`, adicione `ShipmentFailedHandler` (em qualquer posição, antes de `InventoryEventRouter`).

- [ ] **Step 7: Rode a suíte inteira do serviço**

Run: `pnpm --filter @ecommerce/inventory-service test`
Expected: TODOS os testes PASS.

- [ ] **Step 8: Lint e typecheck**

Run: `pnpm --filter @ecommerce/inventory-service lint && pnpm --filter @ecommerce/inventory-service typecheck`
Expected: sem erros.

- [ ] **Step 9: Commit**

```bash
git add apps/inventory-service
git commit -m "feat(inventory-service): libera estoque em shipment.failed (I4)"
```

---

### Task 4: Order Service — fecha `COMPENSATING` quando as compensações exigidas chegarem

Esta é a tarefa mais delicada: `COMPENSATING` deixa de ser um estado sem saída. O projetor precisa saber DUAS coisas que não existiam antes: (1) qual foi o motivo de ter entrado em `COMPENSATING` (`stock.unavailable` só exige `payment.refunded`; `shipment.failed` exige `payment.refunded` **e** `stock.released`); (2) quais compensações já chegaram até agora.

**Files:**
- Modify: `apps/order-service/prisma/schema.prisma`
- Create (migração): `apps/order-service/prisma/migrations/<timestamp>_add_compensation_tracking/migration.sql` (gerada pelo Step 3, não escrita à mão)
- Modify: `apps/order-service/src/application/order-state-machine.ts`
- Modify: `apps/order-service/src/application/order-projection.handler.ts`
- Test: `apps/order-service/test/order-state-machine.spec.ts`
- Test: `apps/order-service/test/order-projection-handler.integration.spec.ts`

**Interfaces:**
- Consumes: `COMPENSATION_TYPE`, `CompensationType`, `CANCELLATION_REASON`, `CancellationReason` de `@ecommerce/contracts` (Task 1).
- Produces: `applyCompensationEvent(order, eventType)` em `order-state-machine.ts` — nova função, ao lado da já existente `applyEvent`.

- [ ] **Step 1: Adicione os campos ao schema do Order e gere a migração**

Em `apps/order-service/prisma/schema.prisma`, no `model Order`, adicione dois campos novos depois de `status`:

```prisma
model Order {
  id                   String   @id
  customerId           String   @map("customer_id")
  items                Json
  totalAmountCents     Int      @map("total_amount_cents")
  currency             String
  status               String   @default("PENDING")
  // Preenchidos só quando status vira COMPENSATING — null enquanto o pedido
  // não precisou de compensação. compensationReason diz QUAL falha disparou a
  // compensação (decide quais compensações são exigidas); compensationsReceived
  // acumula os tipos já confirmados ('PAYMENT_REFUNDED', 'STOCK_RELEASED').
  compensationReason      String? @map("compensation_reason")
  compensationsReceived   Json    @default("[]") @map("compensations_received")
  shippingAddress      Json     @map("shipping_address")
  createdAt            DateTime @default(now()) @map("created_at")
  updatedAt            DateTime @updatedAt @map("updated_at")

  @@map("orders")
}
```

Rode:

Run: `cd apps/order-service && pnpm prisma:migrate --name add_compensation_tracking && cd ../..`
Expected: cria `prisma/migrations/<timestamp>_add_compensation_tracking/migration.sql` e aplica no Postgres local (requer `pnpm infra:up` já rodando). Confira que o SQL gerado só adiciona as duas colunas novas (`ALTER TABLE "orders" ADD COLUMN "compensation_reason" TEXT; ALTER TABLE "orders" ADD COLUMN "compensations_received" JSONB NOT NULL DEFAULT '[]';` ou equivalente) — se o Prisma gerar algo destrutivo (`DROP`/renomeação de coluna existente), pare e revise o schema antes de continuar.

- [ ] **Step 2: Escreva os testes da máquina de estados (falhando)**

Em `apps/order-service/test/order-state-machine.spec.ts`, adicione ao final do arquivo (antes do `});` de fechamento do `describe` mais externo, ou como um novo `describe` no mesmo arquivo):

```typescript
describe('applyCompensationEvent', () => {
  it('stock.unavailable + payment.refunded fecha em CANCELLED (só uma compensação exigida)', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.STOCK_UNAVAILABLE,
      compensationsReceived: [] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({
      changed: true,
      next: ORDER_STATUS.CANCELLED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
    });
  });

  it('shipment.failed + só payment.refunded NÃO fecha ainda — falta stock.released', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.SHIPMENT_FAILED,
      compensationsReceived: [] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({
      changed: true,
      next: ORDER_STATUS.COMPENSATING,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
    });
  });

  it('shipment.failed + payment.refunded já recebido + stock.released chegando agora fecha em CANCELLED', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.SHIPMENT_FAILED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'stock.released');

    expect(result).toEqual({
      changed: true,
      next: ORDER_STATUS.CANCELLED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED, COMPENSATION_TYPE.STOCK_RELEASED],
    });
  });

  it('compensação repetida (mesmo tipo já recebido) é stale — ignora sem regredir', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.SHIPMENT_FAILED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({ changed: false, reason: 'stale' });
  });

  it('payment.refunded chegando ANTES de o pedido entrar em COMPENSATING é premature — retriável', () => {
    const order = {
      status: ORDER_STATUS.PAYMENT_APPROVED,
      compensationReason: null,
      compensationsReceived: [] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({ changed: false, reason: 'premature' });
  });

  it('compensação chegando depois de o pedido já ter fechado (CONFIRMED/CANCELLED) é stale', () => {
    const confirmed = {
      status: ORDER_STATUS.CONFIRMED,
      compensationReason: null,
      compensationsReceived: [] as CompensationType[],
    };
    expect(applyCompensationEvent(confirmed, 'payment.refunded')).toEqual({
      changed: false,
      reason: 'stale',
    });

    const cancelled = {
      status: ORDER_STATUS.CANCELLED,
      compensationReason: CANCELLATION_REASON.STOCK_UNAVAILABLE,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED] as CompensationType[],
    };
    expect(applyCompensationEvent(cancelled, 'payment.refunded')).toEqual({
      changed: false,
      reason: 'stale',
    });
  });
});
```

No topo do arquivo, atualize os imports:

```typescript
import { CANCELLATION_REASON, COMPENSATION_TYPE, ORDER_STATUS, type CompensationType } from '@ecommerce/contracts';
import { applyCompensationEvent, applyEvent } from '../src/application/order-state-machine.js';
```

- [ ] **Step 3: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/order-service test -- order-state-machine`
Expected: FAIL — `applyCompensationEvent` não existe.

- [ ] **Step 4: Implemente `applyCompensationEvent` em `order-state-machine.ts`**

Abra `apps/order-service/src/application/order-state-machine.ts`. Adicione ao topo do arquivo, no import de `@ecommerce/contracts`:

```typescript
import {
  CANCELLATION_REASON,
  COMPENSATION_TYPE,
  ORDER_STATUS,
  type CancellationReason,
  type CompensationType,
  type OrderStatus,
} from '@ecommerce/contracts';
```

Ao final do arquivo (depois da função `applyEvent` já existente), adicione:

```typescript
export type CompensationEventType = 'payment.refunded' | 'stock.released';

export interface OrderCompensationState {
  status: OrderStatus;
  compensationReason: CancellationReason | null;
  compensationsReceived: CompensationType[];
}

export type CompensationResult =
  | { changed: true; next: OrderStatus; compensationsReceived: CompensationType[] }
  | { changed: false; reason: 'stale' }
  | { changed: false; reason: 'premature' };

const COMPENSATION_TYPE_BY_EVENT: Record<CompensationEventType, CompensationType> = {
  'payment.refunded': COMPENSATION_TYPE.PAYMENT_REFUNDED,
  'stock.released': COMPENSATION_TYPE.STOCK_RELEASED,
};

/**
 * Quais compensações uma falha exige antes do pedido poder fechar em
 * CANCELLED. `stock.unavailable`: nada foi reservado ainda, só o pagamento
 * precisa voltar. `shipment.failed`: o pagamento JÁ estava autorizado E o
 * estoque JÁ estava reservado — as duas precisam ser desfeitas, em qualquer
 * ordem (docs/PLAN.md: "Order só fecha quando as duas chegarem").
 */
const REQUIRED_COMPENSATIONS: Record<CancellationReason, readonly CompensationType[]> = {
  [CANCELLATION_REASON.STOCK_UNAVAILABLE]: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
  [CANCELLATION_REASON.SHIPMENT_FAILED]: [
    COMPENSATION_TYPE.PAYMENT_REFUNDED,
    COMPENSATION_TYPE.STOCK_RELEASED,
  ],
  // Os dois motivos abaixo não usam COMPENSATING nesta versão do projetor
  // (payment.failed fecha direto em CANCELLED; CUSTOMER_REQUEST não é
  // disparado por este projetor) — mapeados só para o Record ficar total.
  [CANCELLATION_REASON.PAYMENT_FAILED]: [],
  [CANCELLATION_REASON.SAGA_TIMEOUT]: [],
  [CANCELLATION_REASON.CUSTOMER_REQUEST]: [],
};

/**
 * Decide o efeito de um evento de compensação (`payment.refunded`/
 * `stock.released`) sobre um pedido. Só produz `changed: true` quando o
 * pedido JÁ está em COMPENSATING — chegar antes disso é 'premature'
 * (retriável: a mesma corrida entre tópicos que `applyEvent` já trata) e
 * chegar depois de CONFIRMED/CANCELLED é 'stale' (seguro ignorar). Usa
 * `STATUS_RANK` (já existe no arquivo, usado por `applyEvent`) para
 * distinguir os dois casos: qualquer status "antes" de COMPENSATING na
 * linha do tempo é premature; "depois" (CONFIRMED/CANCELLED, mesmo rank de
 * COMPENSATING) é stale.
 */
export function applyCompensationEvent(
  order: OrderCompensationState,
  eventType: CompensationEventType,
): CompensationResult {
  if (order.status !== ORDER_STATUS.COMPENSATING) {
    // TERMINAL_STATUS_RANK e STATUS_RANK já existem no arquivo (usados por applyEvent).
    // Qualquer status "antes" de COMPENSATING é corrida legítima (premature); qualquer
    // status "depois" (CONFIRMED/CANCELLED, ambos rank 3, igual a COMPENSATING) já
    // fechou e um evento de compensação chegando agora é tardio (stale).
    const reason = STATUS_RANK[order.status] < STATUS_RANK[ORDER_STATUS.COMPENSATING] ? 'premature' : 'stale';
    return { changed: false, reason };
  }
  if (!order.compensationReason) {
    // Nunca deveria acontecer na prática (compensationReason é setado no mesmo UPDATE
    // que leva o pedido a COMPENSATING — ver order-projection.handler.ts), mas um
    // schema.prisma sem essa garantia em nível de banco pede a defesa em código.
    return { changed: false, reason: 'stale' };
  }

  const compensationType = COMPENSATION_TYPE_BY_EVENT[eventType];
  if (order.compensationsReceived.includes(compensationType)) {
    return { changed: false, reason: 'stale' };
  }

  const compensationsReceived = [...order.compensationsReceived, compensationType];
  const required = REQUIRED_COMPENSATIONS[order.compensationReason];
  const complete = required.every((type) => compensationsReceived.includes(type));

  return {
    changed: true,
    next: complete ? ORDER_STATUS.CANCELLED : ORDER_STATUS.COMPENSATING,
    compensationsReceived,
  };
}
```

`STATUS_RANK` já é uma constante módulo-privada no arquivo (usada por `applyEvent`) — confirme com `grep -n "STATUS_RANK" apps/order-service/src/application/order-state-machine.ts` que ela já existe e já mapeia `COMPENSATING`, `CONFIRMED`, `CANCELLED` todos para o mesmo rank (3). Se o nome ou os valores forem diferentes do que este plano assume, ajuste a lógica acima para usar o que já existe em vez de duplicar a tabela.

- [ ] **Step 5: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/order-service test -- order-state-machine`
Expected: TODOS os testes (os já existentes + os 6 novos) PASS.

- [ ] **Step 6: Escreva os testes de integração do handler (falhando)**

Em `apps/order-service/test/order-projection-handler.integration.spec.ts`, adicione as funções fabricantes de evento que faltam (perto das outras `makeX` funções já existentes no arquivo):

```typescript
function makePaymentRefunded(orderId: string) {
  return createEvent(paymentEvents.paymentRefunded, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'payment-service-test@0.0.0',
    payload: {
      paymentId: randomUUID(),
      orderId,
      refundId: randomUUID(),
      amountCents: 2000,
      currency: 'BRL',
      compensationFor: 'stock.unavailable',
      refundedAt: new Date().toISOString(),
    },
  });
}

function makeStockReleased(orderId: string) {
  return createEvent(inventoryEvents.stockReleased, {
    aggregateId: orderId,
    correlationId: orderId,
    producer: 'inventory-service-test@0.0.0',
    payload: {
      reservationId: randomUUID(),
      orderId,
      items: [{ sku: 'BOOK-001', quantity: 2 }],
      compensationFor: 'shipment.failed',
      releasedAt: new Date().toISOString(),
    },
  });
}
```

Adicione `paymentEvents` ao import de `@ecommerce/contracts` no topo do arquivo se ainda não estiver lá (confira — as fábricas de `payment.approved`/`payment.failed` já existentes provavelmente já importam).

No final do arquivo, antes do `});` de fechamento do `describe`, adicione:

```typescript
  it('stock.unavailable + payment.refunded fecha o pedido em CANCELLED com compensationsApplied=[PAYMENT_REFUNDED] e publica order.cancelled', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockUnavailable(orderId));

    let order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');
    expect(order.compensationReason).toBe('STOCK_UNAVAILABLE');

    await handler.handle(makePaymentRefunded(orderId));

    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('CANCELLED');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'order.cancelled' },
    });
    expect(outboxRows).toHaveLength(1);
    const payload = outboxRows[0]?.payload as {
      payload: { reason: string; compensationsApplied: string[] };
    };
    expect(payload.payload.reason).toBe('STOCK_UNAVAILABLE');
    expect(payload.payload.compensationsApplied).toEqual(['PAYMENT_REFUNDED']);
  });

  it('shipment.failed exige payment.refunded E stock.released — só fecha quando as duas chegarem, em qualquer ordem', async () => {
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockReserved(orderId));
    await handler.handle(makeShipmentFailed(orderId));

    let order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');
    expect(order.compensationReason).toBe('SHIPMENT_FAILED');

    // stock.released chega PRIMEIRO — não fecha ainda, falta payment.refunded.
    await handler.handle(makeStockReleased(orderId));
    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('COMPENSATING');

    // payment.refunded chega DEPOIS — agora sim as duas chegaram, fecha.
    await handler.handle(makePaymentRefunded(orderId));
    order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('CANCELLED');

    const outboxRows = await prisma.client.outbox.findMany({
      where: { aggregateId: orderId, eventType: 'order.cancelled' },
    });
    const payload = outboxRows[0]?.payload as { payload: { compensationsApplied: string[] } };
    expect(payload.payload.compensationsApplied.sort()).toEqual(['PAYMENT_REFUNDED', 'STOCK_RELEASED']);
  });

  it('RACE CONDITION — payment.refunded chega ANTES de o pedido entrar em COMPENSATING: falha retriável sem deixar rastro', async () => {
    const orderId = await createTestOrder();
    const refundedEnvelope = makePaymentRefunded(orderId);

    // Pedido ainda em PENDING — nem payment.approved chegou.
    await expect(handler.handle(refundedEnvelope)).rejects.toThrow();

    const processed = await prisma.client.processedMessage.findUnique({
      where: {
        eventId_consumerGroup: {
          eventId: refundedEnvelope.eventId,
          consumerGroup: 'order-projection',
        },
      },
    });
    expect(processed).toBeNull();

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    expect(order.status).toBe('PENDING'); // não regrediu nem avançou
  });

  it('reentrega do MESMO evento de compensação não conta a compensação duas vezes', async () => {
    const orderId = await createTestOrder();
    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockReserved(orderId));
    await handler.handle(makeShipmentFailed(orderId));
    const releasedEnvelope = makeStockReleased(orderId);

    await handler.handle(releasedEnvelope);
    await handler.handle(releasedEnvelope);

    const order = await prisma.client.order.findUniqueOrThrow({ where: { id: orderId } });
    const received = order.compensationsReceived as string[];
    expect(received).toEqual(['STOCK_RELEASED']); // não duplicou
  });
```

- [ ] **Step 7: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/order-service test -- order-projection-handler`
Expected: FAIL — o handler ainda ignora `payment.refunded`/`stock.released` (não são `ProjectionEventType`) e o schema do Prisma Client ainda não tem os dois campos novos tipados corretamente para o handler.

- [ ] **Step 8: Atualize `order-projection.handler.ts`**

Abra `apps/order-service/src/application/order-projection.handler.ts`. No import de `../src/application/order-state-machine.js` (topo do arquivo), troque:

```typescript
import { applyEvent, type ProjectionEventType } from './order-state-machine.js';
```

por:

```typescript
import {
  applyCompensationEvent,
  applyEvent,
  type CompensationEventType,
  type ProjectionEventType,
} from './order-state-machine.js';
```

No import de `@ecommerce/contracts`, adicione `COMPENSATION_TYPE`:

```typescript
import {
  CANCELLATION_REASON,
  COMPENSATION_TYPE,
  CONSUMER_GROUPS,
  ORDER_STATUS,
  createEvent,
  orderEvents,
  orderStatusSchema,
  type CancellationReason,
  type CompensationType,
  type Currency,
  type UnknownEnvelope,
} from '@ecommerce/contracts';
```

Adicione, ao lado de `CANCELLATION_REASON_BY_EVENT` já existente, o mapa inverso (evento de falha → motivo de cancelamento, usado quando a transição LEVA a `COMPENSATING`):

```typescript
/** Motivo de compensação por eventType que dispara COMPENSATING. */
const COMPENSATION_REASON_BY_EVENT: Partial<Record<ProjectionEventType, CancellationReason>> = {
  'stock.unavailable': CANCELLATION_REASON.STOCK_UNAVAILABLE,
  'shipment.failed': CANCELLATION_REASON.SHIPMENT_FAILED,
};

const COMPENSATION_EVENT_TYPES: ReadonlySet<string> = new Set<CompensationEventType>([
  'payment.refunded',
  'stock.released',
]);

function isCompensationEventType(eventType: string): eventType is CompensationEventType {
  return COMPENSATION_EVENT_TYPES.has(eventType);
}
```

Troque a guarda inicial do método `handle`:

```typescript
  async handle(envelope: UnknownEnvelope): Promise<void> {
    if (!isProjectionEventType(envelope.eventType)) return;
    const eventType = envelope.eventType;
```

por:

```typescript
  async handle(envelope: UnknownEnvelope): Promise<void> {
    if (isCompensationEventType(envelope.eventType)) {
      await this.handleCompensationEvent(envelope, envelope.eventType);
      return;
    }
    if (!isProjectionEventType(envelope.eventType)) return;
    const eventType = envelope.eventType;
```

No trecho onde `result.next === ORDER_STATUS.COMPENSATING` já loga o erro (adicionado na correção do C2), acrescente o `compensationReason` no MESMO `tx.order.updateMany` que já existe logo acima — troque:

```typescript
      const updated = await tx.order.updateMany({
        where: { id: orderId, status: currentStatus },
        data: { status: result.next },
      });
```

por:

```typescript
      const updated = await tx.order.updateMany({
        where: { id: orderId, status: currentStatus },
        data: {
          status: result.next,
          ...(result.next === ORDER_STATUS.COMPENSATING
            ? { compensationReason: COMPENSATION_REASON_BY_EVENT[eventType] }
            : {}),
        },
      });
```

Por fim, adicione o método novo `handleCompensationEvent` como um método privado da classe (depois do fechamento do método `handle`):

```typescript
  private async handleCompensationEvent(
    envelope: UnknownEnvelope,
    eventType: CompensationEventType,
  ): Promise<void> {
    const orderId = (envelope.payload as { orderId: string }).orderId;

    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.orderProjection);
      if (!isNew) return;

      const order = await tx.order.findUnique({ where: { id: orderId } });
      if (!order) {
        const error = new Error(
          `Order ${orderId} não encontrado ao projetar ${eventType} — dado inconsistente`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }

      const result = applyCompensationEvent(
        {
          status: orderStatusSchema.parse(order.status),
          compensationReason: order.compensationReason as CancellationReason | null,
          compensationsReceived: (order.compensationsReceived as CompensationType[]) ?? [],
        },
        eventType,
      );

      if (!result.changed) {
        if (result.reason === 'premature') {
          // Mesmo raciocínio de handle(): payment.refunded/stock.released chegando antes
          // de o pedido entrar em COMPENSATING é corrida legítima entre tópicos — lança
          // DENTRO da transação para desfazer o markProcessed junto (regra de ouro).
          throw new Error(
            `Pedido ${orderId} ainda não está em COMPENSATING ao processar ${eventType} — aguardando evento anterior da saga`,
          );
        }
        this.logger.warn(`Compensação obsoleta ignorada: pedido ${orderId}, evento ${eventType}`);
        return;
      }

      await tx.order.update({
        where: { id: orderId },
        data: { status: result.next, compensationsReceived: result.compensationsReceived },
      });

      if (result.next === ORDER_STATUS.CANCELLED) {
        const cancelledEnvelope = createEvent(orderEvents.orderCancelled, {
          aggregateId: order.id,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'order-service@0.1.0',
          payload: {
            orderId: order.id,
            customerId: order.customerId,
            reason: order.compensationReason as CancellationReason,
            compensationsApplied: result.compensationsReceived,
            cancelledAt: new Date().toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: cancelledEnvelope.eventId,
          aggregateId: order.id,
          aggregateType: 'order',
          eventType: 'order.cancelled',
          envelope: cancelledEnvelope,
        });
      }
    });
  }
```

`COMPENSATION_TYPE` importado acima não é referenciado diretamente neste arquivo (só o tipo `CompensationType`) — se o lint acusar import não usado, remova `COMPENSATION_TYPE` do import e mantenha só `type CompensationType`.

- [ ] **Step 9: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/order-service test -- order-projection-handler`
Expected: TODOS os testes (os já existentes + os 4 novos) PASS.

- [ ] **Step 10: Rode a suíte inteira do serviço**

Run: `pnpm --filter @ecommerce/order-service test`
Expected: TODOS os testes PASS.

- [ ] **Step 11: Lint e typecheck**

Run: `pnpm --filter @ecommerce/order-service lint && pnpm --filter @ecommerce/order-service typecheck`
Expected: sem erros. Se o lint acusar linha longa ou formatação no schema.prisma (não é lintado por ESLint, ignore) ou no handler, rode `pnpm format` na raiz do monorepo e re-commit.

- [ ] **Step 12: Commit**

```bash
git add apps/order-service
git commit -m "feat(order-service): fecha COMPENSATING quando as compensações exigidas chegam (I4)"
```

---

### Task 5: Verificação de sistema — end-to-end contra Docker real

**Files:** nenhum arquivo novo — só comandos de verificação e, se algo quebrar, correções pontuais nos arquivos das Tasks 2–4.

- [ ] **Step 1: Rode a suíte inteira do monorepo**

Run: `pnpm exec turbo run lint typecheck build test`
Expected: TODAS as tasks PASS (nenhuma task deve falhar; ignore flakiness conhecida de hooks `afterAll`/`beforeAll` em suítes e2e sob Kafka compartilhado — se um teste INDIVIDUAL falhar, é regressão real, não flakiness).

- [ ] **Step 2: Rebuilda as 3 imagens Docker tocadas**

Run: `docker compose -f deploy/docker/docker-compose.yml build order-service payment-service inventory-service`
Expected: build sem erro, 0 HIGH/CRITICAL se rodar Trivy (`docker run --rm -v /var/run/docker.sock:/var/run/docker.sock aquasec/trivy image --severity HIGH,CRITICAL ecommerce-saga-order-service` — repita para os outros dois nomes de imagem).

- [ ] **Step 3: Suba os 5 serviços e rode os DOIS caminhos de falha ao vivo**

```bash
docker compose -f deploy/docker/docker-compose.yml up -d order-service payment-service inventory-service shipping-service notification-service
```

Gere um token (mesmo comando usado nas sessões anteriores, com `dev-only-not-a-real-secret-change-me`/`ecommerce-local`) e crie DOIS pedidos:

1. Um com item de SKU começando com `OUT-` (dispara `stock.unavailable`) — espere ~10s e confira: `GET /orders/:id` deve devolver `status: "CANCELLED"`; `docker exec ecommerce-pg-payment psql -U payment_svc -d payment_db -c "select status from payments where order_id='<id>';"` deve devolver `REFUNDED`.
2. Um com endereço de CEP `00000-XXX` (dispara `shipment.failed`, gatilho determinístico já existente no Shipping) — espere ~10s e confira: `status: "CANCELLED"`; `payments.status = 'REFUNDED'` E `stock_reservations.status = 'RELEASED'` (via `docker exec ecommerce-pg-inventory psql ...`).

Expected: os dois pedidos chegam a `CANCELLED` (não mais presos em `COMPENSATING`), com o Payment estornado e — no segundo caso — o estoque também liberado. Confira em http://localhost:18025 que o e-mail "Pedido ... cancelado" foi enviado nos dois casos.

- [ ] **Step 4: Pare os containers de aplicação (deixe a infra rodando para o CI local, se for rodar de novo depois)**

```bash
docker compose -f deploy/docker/docker-compose.yml stop order-service payment-service inventory-service shipping-service notification-service
```

- [ ] **Step 5: Push da branch e checagem final de diff**

```bash
git push origin feat/fases-6-11-compensacao
git log --oneline main..HEAD 2>/dev/null || git log --oneline master..HEAD
git diff master...HEAD --stat
```

Expected: os 5 commits das Tasks 1–4 aparecem (a Task 5 não gera commit — é só verificação). Confirme que `docker-compose.yml`, `.env`, ou qualquer segredo NÃO aparecem no diff.

Este plano termina aqui. O PR contra `master`, o acompanhamento do CI e o merge final acontecem depois de TODOS os planos desta rodada (I4 + Fases 6/7/7b/8/10/11) estarem implementados nesta mesma branch — não abra o PR ainda ao terminar só este plano.
