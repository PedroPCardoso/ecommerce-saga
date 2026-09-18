# Fase 7b — Saga Observer (a UI real) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Um `POST /orders` real aparece numa página web em menos de 1 segundo, mostrando cada evento da saga chegando ao vivo — a mesma ideia do simulador (`docs/simulator/`), mas ligada ao sistema de verdade.

**Architecture:** Um 6º serviço, `apps/saga-observer`, com um consumer group PRÓPRIO (`saga-observer`) assinando os 4 tópicos de negócio — sem nenhum efeito colateral, só projeta em memória (`Map<orderId, OrderProjection>`) e empurra cada evento por Server-Sent Events (SSE) para um front estático (HTML+JS puro, sem framework, servido pelo próprio NestJS). Nunca toca banco — o estado vive só na memória do processo, e reinicia junto com ele (aceitável: é um observador, não fonte de verdade).

**Tech Stack:** NestJS (`@Sse()`), RxJS `Subject` (já é dependência transitiva do Nest), HTML/JS estático sem build step.

## Global Constraints

- **Escopo reduzido, deliberado, desta fase** em relação ao que `docs/PLAN.md` descreve para a Fase 7b original: SEM autenticação/papel de operador, SEM reprocessamento de DLT pela UI, SEM endpoint `/internal/saga-debug/:orderId` por serviço. Motivo: essas três coisas juntas criam uma superfície administrativa nova (auth + RBAC + audit log de quem reprocessou o quê) que merece revisão de segurança dedicada e desproporcional ao tempo restante desta rodada. Documente isto explicitamente no README ao final — não deixe implícito.
- O que a UI expõe via SSE é só `{ orderId, eventType, status, occurredAt }` — NUNCA `customerId`, endereço, dado de pagamento ou qualquer campo de `payload` além do necessário para o texto do evento (A01/A09: isto é um painel observável por qualquer um que tenha acesso à rede local, sem login).
- Branch de trabalho: `feat/fases-6-11-compensacao`.
- `pnpm --filter @ecommerce/saga-observer test` verde antes de prosseguir.

---

### Task 1: Estrutura do serviço + consumidor projetando em memória

**Files:**
- Create: `apps/saga-observer/package.json` (copie a estrutura de `apps/notification-service/package.json` — é o serviço mais parecido: só consome, não escreve num banco de domínio, não tem outbox)
- Create: `apps/saga-observer/tsconfig.json`, `apps/saga-observer/vitest.config.ts` (copie de `apps/notification-service`, ajustando o nome)
- Create: `apps/saga-observer/src/env.ts`
- Create: `apps/saga-observer/src/main.ts`
- Create: `apps/saga-observer/src/app.module.ts`
- Create: `apps/saga-observer/src/domain/order-projection.store.ts`
- Create: `apps/saga-observer/src/infrastructure/saga-observer-consumer.service.ts`
- Create: `apps/saga-observer/src/health/health.controller.ts`
- Test: `apps/saga-observer/test/order-projection.store.spec.ts`

**Interfaces:**
- Produces: `OrderProjectionStore` — `upsert(orderId, patch): OrderSnapshot`, `getAll(): OrderSnapshot[]`, `events$: Observable<OrderSnapshot>` (emite a cada atualização, é isto que o SSE controller assina).

`apps/saga-observer/package.json` não tem Prisma nem `prisma:*` scripts (não tem banco). Dependências: `@nestjs/common`, `@nestjs/core`, `@nestjs/platform-express`, `@ecommerce/contracts`, `@ecommerce/kafka`, `rxjs`, `reflect-metadata` — as mesmas versões já usadas em `apps/notification-service/package.json`.

- [ ] **Step 1: `OrderProjectionStore` (unidade pura, TDD real)**

Escreva `apps/saga-observer/test/order-projection.store.spec.ts`:

```typescript
import { describe, expect, it } from 'vitest';
import { OrderProjectionStore } from '../src/domain/order-projection.store.js';

describe('OrderProjectionStore', () => {
  it('upsert cria uma projeção nova na primeira chamada', () => {
    const store = new OrderProjectionStore();
    const snapshot = store.upsert('order-1', { eventType: 'order.created', status: 'PENDING' });
    expect(snapshot).toEqual({ orderId: 'order-1', eventType: 'order.created', status: 'PENDING', occurredAt: expect.any(String) });
  });

  it('upsert subsequente atualiza a MESMA entrada, getAll devolve só uma por orderId', () => {
    const store = new OrderProjectionStore();
    store.upsert('order-1', { eventType: 'order.created', status: 'PENDING' });
    store.upsert('order-1', { eventType: 'payment.approved', status: 'PAYMENT_APPROVED' });

    const all = store.getAll();
    expect(all).toHaveLength(1);
    expect(all[0]?.status).toBe('PAYMENT_APPROVED');
  });

  it('events$ emite uma vez por upsert, com o snapshot atualizado', async () => {
    const store = new OrderProjectionStore();
    const received: string[] = [];
    const sub = store.events$.subscribe((snap) => received.push(snap.status));

    store.upsert('order-1', { eventType: 'order.created', status: 'PENDING' });
    store.upsert('order-1', { eventType: 'payment.approved', status: 'PAYMENT_APPROVED' });

    sub.unsubscribe();
    expect(received).toEqual(['PENDING', 'PAYMENT_APPROVED']);
  });
});
```

Run: `pnpm --filter @ecommerce/saga-observer test` → FAIL (módulo não existe).

Implemente `apps/saga-observer/src/domain/order-projection.store.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { Subject } from 'rxjs';

export interface OrderSnapshot {
  orderId: string;
  eventType: string;
  status: string;
  occurredAt: string;
}

/**
 * Estado em memória, de propósito: este serviço NÃO é fonte de verdade — se
 * reiniciar, esquece tudo, e o próximo evento reconstrói a partir daí. Um
 * painel observável não precisa sobreviver a restart mais do que um
 * `top` sobrevive a fechar o terminal.
 */
@Injectable()
export class OrderProjectionStore {
  private readonly projections = new Map<string, OrderSnapshot>();
  private readonly subject = new Subject<OrderSnapshot>();

  readonly events$ = this.subject.asObservable();

  upsert(orderId: string, patch: { eventType: string; status: string }): OrderSnapshot {
    const snapshot: OrderSnapshot = {
      orderId,
      eventType: patch.eventType,
      status: patch.status,
      occurredAt: new Date().toISOString(),
    };
    this.projections.set(orderId, snapshot);
    this.subject.next(snapshot);
    return snapshot;
  }

  getAll(): OrderSnapshot[] {
    return [...this.projections.values()];
  }
}
```

Run: `pnpm --filter @ecommerce/saga-observer test` → 3/3 PASS.

- [ ] **Step 2: Consumidor Kafka que projeta cada evento**

Implemente `apps/saga-observer/src/infrastructure/saga-observer-consumer.service.ts` seguindo EXATAMENTE o padrão de `apps/notification-service/src/infrastructure/*-consumer.service.ts` (leia esse arquivo antes de escrever este — mesma estrutura de `KafkaConsumerRuntime`), com uma diferença: não existe `CONSUMER_GROUPS.sagaObserver` em `packages/contracts` ainda — adicione (veja Step 3 abaixo antes de implementar este arquivo).

O handler passado ao `KafkaConsumerRuntime` deve mapear `envelope.eventType` para um status legível de negócio, ex.:

```typescript
const STATUS_BY_EVENT_TYPE: Record<string, string> = {
  'order.created': 'PENDING',
  'payment.approved': 'PAYMENT_APPROVED',
  'payment.failed': 'CANCELLED',
  'stock.reserved': 'STOCK_RESERVED',
  'stock.unavailable': 'COMPENSATING',
  'shipment.created': 'CONFIRMED',
  'shipment.failed': 'COMPENSATING',
  'payment.refunded': 'COMPENSATING',
  'stock.released': 'COMPENSATING',
  'order.confirmed': 'CONFIRMED',
  'order.cancelled': 'CANCELLED',
};
```

e chamar `this.store.upsert(envelope.aggregateId, { eventType: envelope.eventType, status: STATUS_BY_EVENT_TYPE[envelope.eventType] ?? 'DESCONHECIDO' })` para todo `eventType` reconhecido (ignore silenciosamente qualquer outro, sem erro — este serviço não tem efeito de negócio para errar).

- [ ] **Step 3: Adicione `CONSUMER_GROUPS.sagaObserver` e a entrada em `SUBSCRIPTIONS`**

Em `packages/contracts/src/topics.ts`, adicione ao objeto `CONSUMER_GROUPS`:

```typescript
  sagaObserver: 'saga-observer',
```

e em `SUBSCRIPTIONS`:

```typescript
  [CONSUMER_GROUPS.sagaObserver]: [TOPICS.orders, TOPICS.payments, TOPICS.inventory, TOPICS.shipping],
```

Rode `pnpm --filter @ecommerce/contracts test` — confirme que o teste de topologia (`topics.spec.ts` ou equivalente) ainda passa; se ele tiver uma lista fixa de consumer groups esperados, atualize-a.

- [ ] **Step 4: `env.ts`, `app.module.ts`, `main.ts`, `health.controller.ts`**

Siga o padrão EXATO de `apps/notification-service` para os quatro arquivos (só troque `NOTIFICATION_SERVICE_PORT`/`NOTIFICATION_DATABASE_URL` — este serviço não tem banco, remova a env var de DATABASE_URL inteiramente do `env.ts`). Porta sugerida: `SAGA_OBSERVER_PORT` default `3005`. Adicione `SAGA_OBSERVER_PORT=3005` ao `.env.example`.

- [ ] **Step 5: Rode a suíte, lint, typecheck, commit**

```bash
pnpm install  # workspace novo
pnpm --filter @ecommerce/saga-observer test
pnpm --filter @ecommerce/saga-observer lint
pnpm --filter @ecommerce/saga-observer typecheck
git add apps/saga-observer packages/contracts .env.example pnpm-lock.yaml
git commit -m "feat(saga-observer): serviço novo — consumidor projeta a saga em memória (Fase 7b)"
```

---

### Task 2: Endpoint SSE + snapshot + front estático

**Files:**
- Create: `apps/saga-observer/src/api/orders-stream.controller.ts`
- Create: `apps/saga-observer/public/index.html`
- Modify: `apps/saga-observer/src/main.ts` (serve `public/` como estático)
- Modify: `apps/saga-observer/src/app.module.ts`

- [ ] **Step 1: Controller SSE**

Crie `apps/saga-observer/src/api/orders-stream.controller.ts`:

```typescript
import { Controller, Get, Sse } from '@nestjs/common';
import { map, type Observable } from 'rxjs';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { OrderProjectionStore, type OrderSnapshot } from '../domain/order-projection.store.js';

@Controller('api/orders')
export class OrdersStreamController {
  constructor(private readonly store: OrderProjectionStore) {}

  @Get()
  snapshot(): OrderSnapshot[] {
    return this.store.getAll();
  }

  @Sse('stream')
  stream(): Observable<{ data: OrderSnapshot }> {
    return this.store.events$.pipe(map((snapshot) => ({ data: snapshot })));
  }
}
```

Registre `OrdersStreamController` em `app.module.ts` (`controllers`) e `OrderProjectionStore` em `providers`.

- [ ] **Step 2: Front estático mínimo**

Crie `apps/saga-observer/public/index.html` — uma página só, sem dependência externa, que: (1) faz `fetch('/api/orders')` no load para a foto inicial, (2) abre `new EventSource('/api/orders/stream')` e atualiza a MESMA linha da tabela por `orderId` a cada evento (não acrescenta linha nova — atualiza), (3) colore a célula de status (verde para CONFIRMED, vermelho para CANCELLED, amarelo para COMPENSATING, cinza para os intermediários):

```html
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>Saga Observer</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; background: #0f172a; color: #e2e8f0; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 0.5rem 1rem; text-align: left; border-bottom: 1px solid #334155; }
    .status-CONFIRMED { color: #4ade80; }
    .status-CANCELLED { color: #f87171; }
    .status-COMPENSATING { color: #facc15; }
    .status-PENDING, .status-PAYMENT_APPROVED, .status-STOCK_RESERVED { color: #94a3b8; }
  </style>
</head>
<body>
  <h1>Saga Observer</h1>
  <p id="connection-status">conectando…</p>
  <table>
    <thead><tr><th>Order ID</th><th>Status</th><th>Último evento</th><th>Quando</th></tr></thead>
    <tbody id="orders"></tbody>
  </table>

  <script>
    const rows = new Map();
    const tbody = document.getElementById('orders');

    function render(snapshot) {
      let row = rows.get(snapshot.orderId);
      if (!row) {
        row = document.createElement('tr');
        row.innerHTML = '<td class="order-id"></td><td class="status"></td><td class="event"></td><td class="when"></td>';
        tbody.appendChild(row);
        rows.set(snapshot.orderId, row);
      }
      row.querySelector('.order-id').textContent = snapshot.orderId;
      row.querySelector('.status').textContent = snapshot.status;
      row.querySelector('.status').className = 'status status-' + snapshot.status;
      row.querySelector('.event').textContent = snapshot.eventType;
      row.querySelector('.when').textContent = new Date(snapshot.occurredAt).toLocaleTimeString('pt-BR');
    }

    fetch('/api/orders').then((r) => r.json()).then((snapshots) => snapshots.forEach(render));

    const source = new EventSource('/api/orders/stream');
    source.onopen = () => { document.getElementById('connection-status').textContent = 'conectado'; };
    source.onerror = () => { document.getElementById('connection-status').textContent = 'reconectando…'; };
    source.onmessage = (event) => render(JSON.parse(event.data));
  </script>
</body>
</html>
```

- [ ] **Step 3: Sirva `public/` como estático**

Em `apps/saga-observer/src/main.ts`, use `NestExpressApplication` e `useStaticAssets`:

```typescript
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { AppModule } from './app.module.js';
import { env } from './env.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  app.useStaticAssets(join(__dirname, '..', 'public'));
  await app.listen(env.SAGA_OBSERVER_PORT);
  console.log(`[saga-observer] ouvindo na porta ${env.SAGA_OBSERVER_PORT}`);
  // ... mesmo shutdown gracioso dos outros main.ts
}

bootstrap().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
```

Confirme que `@nestjs/platform-express` já está nas dependências (deveria estar, copiado do `notification-service`).

- [ ] **Step 4: Verificação manual (sem teste automatizado — é UI)**

Suba a infra + o saga-observer + os 5 serviços de negócio, crie um pedido via HTTP, abra `http://localhost:3005/index.html` num navegador e confirme que a linha do pedido aparece e atualiza de status ao vivo, sem dar refresh.

- [ ] **Step 5: Adicione ao `docker-compose.yml`**

Siga o padrão dos outros 5 serviços (bloco de serviço + Dockerfile multi-stage — copie `apps/notification-service/Dockerfile` trocando o nome). Adicione a porta `3005:3005` e `SAGA_OBSERVER_PORT=3005` às variáveis de ambiente do bloco.

- [ ] **Step 6: Commit**

```bash
git add apps/saga-observer deploy/docker
git commit -m "feat(saga-observer): SSE + front estático + Docker (Fase 7b)"
```

Este plano termina aqui. Próximo: Fase 10 (Kubernetes).
