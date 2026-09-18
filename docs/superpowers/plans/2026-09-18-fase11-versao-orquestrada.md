# Fase 11 — Versão Orquestrada (opcional, comparação) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provar, com código de verdade rodando ao lado da coreografia (não em substituição a ela), a diferença estrutural entre coreografia e orquestração — e produzir a comparação que `docs/PLAN.md` pede (linhas de código para uma 5ª etapa, serviços tocados, tempo para responder "por que foi cancelado", latência).

**Architecture:** `docs/PLAN.md` descreve esta fase como "adiciona `saga-orchestrator-service`... os serviços de domínio viram executores burros" — implementado aqui como um harness de comparação **isolado**: um único app novo, `apps/saga-orchestrator-service`, com sua PRÓPRIA state machine persistida, seus PRÓPRIOS tópicos de comando/resposta, e módulos de "executor burro" (Payment/Inventory/Shipping) simulados DENTRO do mesmo processo — não modifica os 5 serviços de produção já testados e revisados. Escopo deliberadamente reduzido: o objetivo é a COMPARAÇÃO pedida, não uma segunda implementação de produção completa (a própria `docs/PLAN.md` marca esta fase como opcional).

**Tech Stack:** NestJS, kafkajs (via `@ecommerce/kafka`), Prisma.

## Global Constraints

- **NÃO toque em `apps/order-service`, `apps/payment-service`, `apps/inventory-service`, `apps/shipping-service`, `apps/notification-service`** — a coreografia já implementada, testada e revisada não deve ser modificada ou arriscada por esta fase de comparação/estudo.
- Tópicos novos, isolados dos de negócio: `ecommerce.commands.payment.v1`, `ecommerce.commands.inventory.v1`, `ecommerce.commands.shipping.v1`, `ecommerce.responses.orchestrator.v1`. Adicione-os a `packages/contracts/src/topics.ts` (`TOPICS`) e um novo `CONSUMER_GROUPS.orchestrator` — sem afetar `SUBSCRIPTIONS` dos grupos já existentes.
- Branch de trabalho: `feat/fases-6-11-compensacao`.

---

### Task 1: Contratos dos comandos e respostas

**Files:**
- Modify: `packages/contracts/src/topics.ts`
- Create: `packages/contracts/src/events/orchestration.ts`
- Modify: `packages/contracts/src/registry.ts` (spread do novo módulo, mesmo padrão de `orderEvents`/`paymentEvents`)
- Test: `packages/contracts/test/orchestration.spec.ts`

- [ ] **Step 1: Adicione os tópicos e o consumer group**

Em `packages/contracts/src/topics.ts`, adicione a `TOPICS`:

```typescript
  commandsPayment: 'ecommerce.commands.payment.v1',
  commandsInventory: 'ecommerce.commands.inventory.v1',
  commandsShipping: 'ecommerce.commands.shipping.v1',
  responsesOrchestrator: 'ecommerce.responses.orchestrator.v1',
```

e a `CONSUMER_GROUPS`:

```typescript
  orchestrator: 'saga-orchestrator',
  paymentExecutor: 'payment-executor',
  inventoryExecutor: 'inventory-executor',
  shippingExecutor: 'shipping-executor',
```

Em `SUBSCRIPTIONS`, adicione as 4 entradas correspondentes (o orquestrador assina `responsesOrchestrator`; cada executor assina seu próprio tópico de comando).

- [ ] **Step 2: Defina os eventos de comando/resposta**

Crie `packages/contracts/src/events/orchestration.ts` com `defineEvent` para: `authorizePaymentCommand` (payload: `orchestratedOrderId`, `amountCents`, `currency`), `reserveStockCommand` (`orchestratedOrderId`, `items`), `createShipmentCommand` (`orchestratedOrderId`, `address`), e UMA resposta genérica `executorResponded` (payload: `orchestratedOrderId`, `step: z.enum(['payment','inventory','shipping'])`, `outcome: z.enum(['success','failure'])`, `reason: z.string().optional()`) — siga exatamente o padrão de `defineEvent` já usado em `packages/contracts/src/events/order.ts` (mesmos campos de envelope, mesma forma de export).

- [ ] **Step 3: Teste + registro + lint/typecheck/commit**

Escreva um teste simples (`createEvent` + assert de `topic`/`type`, mesmo padrão do `saga-timeout.spec.ts` da Fase 6). Adicione `export * from './events/orchestration.js';` em `registry.ts` junto aos outros exports de eventos.

```bash
pnpm --filter @ecommerce/contracts test && pnpm --filter @ecommerce/contracts lint && pnpm --filter @ecommerce/contracts typecheck
git add packages/contracts
git commit -m "feat(contracts): comandos/respostas para o harness de comparação orquestrada (Fase 11)"
```

---

### Task 2: `apps/saga-orchestrator-service` — state machine persistida + comando/resposta

**Files:**
- Create: `apps/saga-orchestrator-service/` (estrutura completa — package.json, tsconfig, prisma/schema.prisma com um único model `OrchestratedOrder { id, status, amountCents, currency, items, address, createdAt, updatedAt }`, seguindo o MESMO padrão de qualquer um dos 5 serviços já existentes)
- Create: `apps/saga-orchestrator-service/src/api/orchestrated-orders.controller.ts` (`POST /orchestrated-orders`)
- Create: `apps/saga-orchestrator-service/src/application/orchestrator-state-machine.ts`
- Create: `apps/saga-orchestrator-service/src/application/orchestrator.service.ts` (envia o PRÓXIMO comando ao consumir uma resposta)
- Create: `apps/saga-orchestrator-service/src/executors/payment-executor.service.ts`
- Create: `apps/saga-orchestrator-service/src/executors/inventory-executor.service.ts`
- Create: `apps/saga-orchestrator-service/src/executors/shipping-executor.service.ts`
- Test: `apps/saga-orchestrator-service/test/orchestrator.e2e.spec.ts`

**Interfaces:**
- `OrchestratorStateMachine`: estados `AWAITING_PAYMENT -> AWAITING_STOCK -> AWAITING_SHIPMENT -> CONFIRMED`, ou `CANCELLED` a partir de qualquer um em caso de `outcome: 'failure'` — bem mais simples que a coreografia porque AQUI existe UM lugar que decide o próximo passo (é exatamente o ponto de comparação).
- Os "executores burros": cada um só consome seu tópico de comando e responde `success`/`failure` — SEM outbox, SEM idempotência própria (não precisam: um único processo decide o fluxo, não há concorrência de quem publica o quê). Reaproveite o MESMO gatilho determinístico já usado na coreografia para o `payment-executor` (valor terminando em `.13` = falha) — consistência de comportamento observável entre as duas versões facilita a comparação.

- [ ] **Step 1: `OrchestratorStateMachine` (TDD puro, sem infra)**

```typescript
export type OrchestratorStatus = 'AWAITING_PAYMENT' | 'AWAITING_STOCK' | 'AWAITING_SHIPMENT' | 'CONFIRMED' | 'CANCELLED';

export function applyExecutorResponse(
  current: OrchestratorStatus,
  step: 'payment' | 'inventory' | 'shipping',
  outcome: 'success' | 'failure',
): OrchestratorStatus {
  if (outcome === 'failure') return 'CANCELLED';
  if (current === 'AWAITING_PAYMENT' && step === 'payment') return 'AWAITING_STOCK';
  if (current === 'AWAITING_STOCK' && step === 'inventory') return 'AWAITING_SHIPMENT';
  if (current === 'AWAITING_SHIPMENT' && step === 'shipping') return 'CONFIRMED';
  return current; // resposta fora de ordem/duplicada — ignora (aqui pode, é single-writer)
}
```

Escreva os testes cobrindo o caminho feliz completo e uma falha em cada etapa (4 testes) — TDD real: teste antes, implementação depois, mesmo processo do resto do projeto.

- [ ] **Step 2: `OrchestratorService` — publica o comando seguinte a cada resposta**

Consome `TOPICS.responsesOrchestrator`. Ao receber uma resposta: carrega o `OrchestratedOrder`, aplica `applyExecutorResponse`, grava o novo status, e PUBLICA o próximo comando (`reserveStockCommand` se virou `AWAITING_STOCK`, `createShipmentCommand` se virou `AWAITING_SHIPMENT`, nada se `CONFIRMED`/`CANCELLED`). Sem outbox aqui é uma escolha válida e PARTE da comparação: um único serviço decidindo o fluxo inteiro pode publicar direto porque não há efeito de domínio concorrente a proteger — anote isto explicitamente num comentário no código, é o ponto pedagógico central desta fase.

- [ ] **Step 3: Os 3 "executores burros"**

Cada um: consome seu tópico de comando, aplica a MESMA regra determinística de falha que a coreografia usa (`.13` para pagamento, SKU `OUT-` para estoque, CEP `00000-XXX` para envio), publica `executorResponded` com `outcome`. Sem banco próprio, sem outbox — são deliberadamente "burros".

- [ ] **Step 4: `POST /orchestrated-orders` + teste e2e**

Endpoint cria o `OrchestratedOrder` (`AWAITING_PAYMENT`) e publica o primeiro comando. Teste e2e: cria um pedido via HTTP, espera, confirma `CONFIRMED`; cria outro com valor `.13`, confirma `CANCELLED`.

- [ ] **Step 5: Rode, lint, typecheck, adicione ao docker-compose, commit**

```bash
pnpm --filter @ecommerce/saga-orchestrator-service test
pnpm --filter @ecommerce/saga-orchestrator-service lint
pnpm --filter @ecommerce/saga-orchestrator-service typecheck
git add apps/saga-orchestrator-service deploy/docker
git commit -m "feat(saga-orchestrator): harness de comparação orquestração vs. coreografia (Fase 11)"
```

---

### Task 3: A comparação, com números reais (não opinião)

**Files:**
- Create: `docs/adr/0012-comparacao-orquestracao-vs-coreografia.md` (formato MADR, como os outros 11 ADRs)

- [ ] **Step 1: Meça as 4 métricas que `docs/PLAN.md` pede**

1. **Linhas de código para adicionar uma 5ª etapa na saga:** conte, de verdade, quantos arquivos e linhas mudariam em CADA versão para adicionar (por exemplo) uma etapa de "verificação de fraude" entre pagamento e estoque. Na coreografia: liste os serviços que precisariam aprender um evento novo (Payment, Inventory, Order — pelo menos 3, coloque um número real de linhas estimado a partir de um handler já existente como referência de tamanho). Na orquestração: só a `OrchestratorStateMachine` e o `OrchestratorService` mudam — 1 arquivo, uma transição nova.
2. **Número de serviços tocados:** coreografia = todos os que reagem à falha da nova etapa (compensação inclusa); orquestração = 1 (a state machine) + 1 executor novo.
3. **Tempo para responder "por que o pedido X foi cancelado?":** na coreografia, reconstrua via `correlationId` nos logs/outbox de quantos serviços (cite os comandos reais usados, ex.: `SELECT * FROM outbox WHERE aggregate_id = X` em 3 bancos diferentes); na orquestração, é UMA linha (`SELECT status, updated_at FROM orchestrated_orders WHERE id = X`) — meça o tempo de relógio de cada abordagem contra o sistema rodando.
4. **Latência ponta a ponta:** rode 10 pedidos em cada versão (script simples com `curl` em loop + timestamp), compare `createdAt` até status terminal.

- [ ] **Step 2: Escreva o ADR com os números medidos**

Formato MADR (copie a estrutura de `docs/adr/0002-saga-coreografada.md`), seção "Consequências" com os 4 números medidos no Step 1 — não invente números, meça de verdade contra o sistema rodando (Task 8 do plano de Fase 10 se o cluster ainda estiver de pé, ou contra o docker-compose).

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0012-comparacao-orquestracao-vs-coreografia.md
git commit -m "docs(adr): compara orquestração vs. coreografia com números medidos (Fase 11)"
```

Este plano termina aqui — é o último das 6 rodadas desta iteração (I4, Fase 6, 7, 7b, 8, 10, 11). Depois dele: revisão final da branch inteira, PR contra `master`, CI, merge.
