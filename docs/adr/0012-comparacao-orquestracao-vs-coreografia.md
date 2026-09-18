# 0012 — Comparação orquestração vs. coreografia (Fase 11)

- **Status:** Aceito
- **Data:** 2026-09-18

## Contexto

ADR-0002 escolheu coreografia para a saga de pedidos e já registrou, nas suas
"Consequências negativas", a suspeita de que orquestração resolveria dois problemas
específicos: acoplamento implícito ao adicionar uma etapa nova, e dificuldade de
responder "por que o pedido X foi cancelado?". `docs/PLAN.md` pede que a Fase 11
prove essa suspeita com números, não com opinião — linhas de código para uma 5ª
etapa, número de serviços tocados, tempo para reconstruir uma causa de
cancelamento, e latência ponta a ponta.

Para medir isso sem arriscar a coreografia já implementada, testada e revisada
(Fases I1–I4, 6, 7, 7b, 10), a Fase 11 implementou um harness **isolado**:
`apps/saga-orchestrator-service`, um app novo com sua própria state machine
persistida (`OrchestratorStateMachine`), seus próprios tópicos de comando/resposta
(`ecommerce.commands.*.v1`, `ecommerce.responses.orchestrator.v1`) e três
"executores burros" (payment/inventory/shipping) simulados dentro do mesmo
processo. Nenhum dos 5 serviços de produção foi modificado — este ADR documenta a
medição, não uma migração.

## Decisão

Mantemos a coreografia (ADR-0002) como a arquitetura de produção. A versão
orquestrada continua existindo só como harness de comparação, ao lado da
coreografia, nunca em substituição a ela — exatamente como `docs/PLAN.md` descreve
esta fase ("opcional"). As métricas abaixo existem para que essa escolha seja
revisitável com dados, não para justificar uma migração agora.

## Consequências

As 4 métricas abaixo foram medidas de verdade contra o sistema rodando localmente
(`pnpm infra:up`, os 5 serviços de produção + `saga-orchestrator-service` todos em
execução, Kafka e os 6 Postgres reais) em 2026-09-18. Nenhum número aqui é estimado
quando marcado como "medido"; as estimativas de LoC de uma mudança **não
implementada de fato** estão marcadas como tal e ancoradas em arquivos reais do
repositório como referência de tamanho.

### 1 — Linhas de código para adicionar uma 5ª etapa ("verificação de fraude", entre pagamento e reserva de estoque)

**Coreografia** (estimado a partir do tamanho real de arquivos análogos já existentes):

| Mudança                                                                  | Arquivo                                                        | Linhas                                                             |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------- |
| Novo módulo de eventos (`fraud.approved`/`fraud.rejected`)               | `packages/contracts/src/events/fraud.ts` (novo)                | ~50 (referência: `events/shipping.ts`, 48 linhas para 2 eventos)      |
| Novo tópico + consumer group + subscriptions                            | `packages/contracts/src/topics.ts`                              | ~15                                                                    |
| Registro do módulo + linha na `COMPENSATION_MATRIX`                     | `packages/contracts/src/registry.ts`                            | ~10                                                                    |
| **Serviço novo `fraud-service`** (handler, router, consumer, outbox-relay, prisma.service, health, env, main, app.module) | `apps/fraud-service/src/**` (novo)                              | ~450–500 (referência: `apps/inventory-service/src`, 513 linhas no total) |
| Schema/migração, Dockerfile, testes de integração/e2e do `fraud-service` | `apps/fraud-service/{prisma,test,Dockerfile}` (novo)            | ~250–350                                                               |
| Compensação: novo `case` reagindo a `fraud.rejected` (reusa `RefundPaymentUseCase` já existente) | `apps/payment-service/src/application/payment-event.router.ts` (40 linhas hoje) | +8                                                                     |
| Retarget do gatilho: reservar estoque passa a reagir a `fraud.approved`, não mais a `payment.approved` diretamente | `apps/inventory-service/src/application/payment-approved.handler.ts` (151 linhas hoje) | arquivo inteiro tocado — diff real estimado ~20–30                    |
| Novo `case` de dispatch                                                  | `apps/inventory-service/src/application/inventory-event.router.ts` (46 linhas hoje) | +5                                                                     |
| Novo status intermediário `FRAUD_CHECK` na state machine do pedido       | `apps/order-service/src/application/order-state-machine.ts` (199 linhas hoje) | +15–25                                                                 |
| Sweeper de timeout aprende a vigiar o novo status preso (hoje só cobre `PAYMENT_APPROVED`, ver linha 41 do arquivo) | `apps/order-service/src/infrastructure/saga-timeout-sweeper.service.ts` (84 linhas hoje) | +15–20                                                                 |

**Total estimado: ~830–1000 linhas, ~10 arquivos existentes tocados + 1 serviço
inteiro novo (~15–18 arquivos) implantável separadamente.**

**Orquestração** (medido nos arquivos reais já existentes desta fase, projetando a
mesma mudança):

| Mudança                                     | Arquivo                                                             | Linhas                                                          |
| ---------------------------------------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------- |
| Novo status + transição na state machine     | `orchestrator-state-machine.ts` (28 linhas hoje)                    | +5                                                                    |
| Novo `case` publicando o comando de fraude   | `orchestrator.service.ts` (137 linhas hoje)                         | +12                                                                   |
| Novo comando + evento                        | `events/orchestration.ts` (68 linhas hoje)                          | +15                                                                   |
| Novo tópico + consumer group + subscription  | `topics.ts`                                                         | +10                                                                   |
| Novo executor "burro" de fraude              | `executors/fraud-executor.service.ts` (novo)                       | ~65–70 (referência real: os 3 executores existentes têm 67–70 linhas cada) |

**Total: ~110–115 linhas, 4 arquivos modificados + 1 arquivo novo — tudo dentro do
MESMO deployable (`saga-orchestrator-service`), nenhum outro processo muda.**

A coreografia precisa de **~8x mais código** e de um serviço de produção inteiro
novo (com banco, outbox, idempotência, testes e deploy próprios) só para inserir um
passo no meio do fluxo. A orquestração precisa de uma transição nova na state
machine e de mais um módulo "burro" no mesmo processo — nenhum deploy novo.

### 2 — Número de serviços tocados

- **Coreografia: 4** — `fraud-service` (novo) + `payment-service` (aprende a
  compensar `fraud.rejected`) + `inventory-service` (muda o gatilho de reserva) +
  `order-service` (novo status intermediário + sweeper). `shipping-service` e
  `notification-service` não mudam porque a fraude é verificada antes da reserva
  de estoque — mas mudariam também se a etapa entrasse depois.
- **Orquestração: 1** — só `saga-orchestrator-service` (a state machine ganha uma
  transição, o processo ganha um módulo "burro" a mais). Nenhum serviço novo é
  implantado, nenhum outro processo é tocado.

### 3 — Tempo para responder "por que o pedido X foi cancelado?" (medido, agora, contra o sistema rodando)

**Coreografia** — pedido real `eb57ad69-88c6-47ee-a9ac-5b64016e1f8f`, cancelado por
recusa de pagamento (valor terminando em `.13`), criado via `POST /orders` real:

```
docker exec ecommerce-pg-order psql -U order_svc -d order_db -c \
  "SELECT event_type, payload->'payload'->>'reason' FROM outbox WHERE aggregate_id = '<id>';"
# order.cancelled | reason: PAYMENT_FAILED  (só o enum genérico do Order)

docker exec ecommerce-pg-payment psql -U payment_svc -d payment_db -c \
  "SELECT event_type, payload->'payload'->>'failureCode', payload->'payload'->>'reason' FROM outbox WHERE aggregate_id = '<id>';"
# payment.failed | CARD_DECLINED | "Cartão recusado pelo emissor (simulação determinística)"
```

2 bancos diferentes (credenciais e hosts diferentes), 2 comandos, campo de negócio
aninhado em `payload->payload->>'reason'` porque o outbox grava o envelope inteiro
(ADR-0006). Tempo de relógio das duas queries (`time` do shell): **0.077s de
execução** — mas esse número esconde o custo real: o operador precisa **saber**
que são bancos diferentes e que o *detalhe* do motivo ("cartão recusado") só existe
no outbox do Payment Service — o outbox do Order Service só tem o código genérico
`PAYMENT_FAILED`.

**Orquestração** — pedido real `04991c4e-47e3-4e6e-88f0-d79b17d79779`, mesmo
gatilho (`.13`), criado via `POST /orchestrated-orders` real:

```
docker exec ecommerce-pg-orchestrator psql -U orchestrator_svc -d orchestrator_db -c \
  "SELECT status, updated_at FROM orchestrated_orders WHERE id = '<id>';"
# CANCELLED | 2026-09-18 06:10:09.88
```

1 banco, 1 comando, **0.049s de execução**.

**Ressalva honesta:** a query orquestrada responde "cancelado, e quando" — não o
motivo detalhado (qual passo falhou e por quê), porque o schema deste harness
(deliberadamente mínimo, ver Task 2 do plano da Fase 11) não persiste a `reason` do
`executorResponded` que causou o cancelamento. Persistir isso custaria 1 coluna a
mais no MESMO arquivo (`schema.prisma`) e ~3 linhas em `OrchestratorService` —
esforço comparável a **uma única** das ~10 mudanças que a coreografia já exige
hoje para a mesma pergunta.

### 4 — Latência ponta a ponta (medida, 10 pedidos por versão, sistema local rodando)

Metodologia: `amountCents = 4990` fixo (sem gatilho de falha) nas duas versões,
cronômetro do lado do cliente entre o `POST` e o primeiro `GET`/`SELECT` que
observa o status terminal, poll a cada 100ms.

**Coreografia** (`POST /orders` → `GET /orders/:id` até `CONFIRMED`):

```
635, 644, 763, 768, 777, 848, 864, 869, 891, 903 ms
média: 796,2ms · mediana: 812,5ms · mín: 635ms · máx: 903ms
```

**Orquestração** (`POST /orchestrated-orders` → `SELECT status` até `CONFIRMED`):

```
34, 37, 37, 38, 39, 39, 39, 40, 42, 213 ms
média: 55,8ms · mediana: 39ms · mín: 34ms · máx: 213ms (1º pedido — warm-up de conexão)
```

A orquestração respondeu **~14x mais rápido na média e ~20x na mediana**. A causa
está no código, não é hipótese: `packages/outbox/src/outbox-relay.ts` faz polling
a cada `pollIntervalMs` (padrão **200ms**, linha 42) para descobrir linhas novas
na tabela outbox. A coreografia atravessa pelo menos 4 hops de outbox+relay
(`order.created` → `payment.approved` → `stock.reserved` → `shipment.created`),
cada um pagando até 200ms de espera de poll **antes** de o Kafka sequer publicar a
mensagem. A orquestração não usa outbox — publica direto após cada consumo (ver
comentário em `OrchestratorService`) — e paga só round-trips de Kafka e Postgres.

Isto **não** é "outbox é ruim": é o preço documentado da garantia que o outbox
compra (ADR-0006 — sobreviver a uma falha entre gravar e publicar, sem duplicar
nem perder o efeito, quando múltiplos escritores concorrentes disputam esse
efeito). A orquestração pode abrir mão dessa garantia porque só ela decide o fluxo
inteiro; a coreografia não pode, porque cada serviço é, por design, um escritor
independente.

## Alternativas consideradas

Nenhuma nova: este ADR não substitui a decisão do ADR-0002, só a instrumenta com
números. A alternativa "adotar orquestração em produção" fica registrada como
possibilidade futura, condicionada a esta mesma tabela sendo revisitada se/quando
a saga ganhar uma etapa nova de verdade — é exatamente o gatilho que a Métrica 1
mede.
