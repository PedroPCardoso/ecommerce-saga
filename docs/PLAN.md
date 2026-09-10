# Sistema de Pedidos de E-commerce — Plano de Execução

> SAGA coreografada sobre Kafka, 5 microserviços NestJS, dockerizado e rodando em Kubernetes (Minikube), documentado em C4.

---

## 0. Decisões arquiteturais (ADR resumido)

| #   | Decisão             | Escolha                                                                    | Por quê                                                                                                    | Alternativa descartada                                                              |
| --- | ------------------- | -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| 1   | Linguagem/framework | Node 22 + NestJS 11 + TypeScript (strict)                                  | DI e módulos maduros, iteração rápida, imagens leves                                                       | Spring Boot (verboso), Go (sem framework de saga)                                   |
| 2   | Padrão SAGA         | **Coreografia**                                                            | Sentir na pele o acoplamento implícito e a ausência de dono do fluxo                                       | Orquestração → Fase 11 (opcional, para comparação)                                  |
| 3   | Broker              | **Kafka** (KRaft, sem Zookeeper)                                           | Particionamento, consumer groups, replay, retenção — o que você quer estudar                               | RabbitMQ (mais simples, menos assunto)                                              |
| 4   | Cliente Kafka       | `kafkajs` embrulhado em módulo próprio                                     | Controle de commit manual de offset, que o transport do `@nestjs/microservices` esconde                    | `@nestjs/microservices` Kafka (ver Armadilha #1)                                    |
| 5   | Persistência        | PostgreSQL, **um banco por serviço**                                       | Sem banco compartilhado = sem transação distribuída = razão de existir da SAGA                             | Banco único (mata o exercício)                                                      |
| 6   | ORM                 | Prisma 6                                                                   | Migrations versionadas, queries parametrizadas por padrão, transação interativa (necessária para o outbox) | TypeORM (migrations frágeis)                                                        |
| 7   | Entrega de mensagem | At-least-once + **Transactional Outbox** + **Inbox de idempotência**       | Exactly-once não existe; o que existe é efeito-uma-vez                                                     | Publicar direto no handler (perde mensagem se o broker cair após o commit do banco) |
| 8   | Contrato de evento  | JSON + envelope versionado, validado com Zod → Schema Registry na Fase 12  | Começa simples, evolui para governança de schema                                                           | Avro desde o dia 1 (fricção alta cedo demais)                                       |
| 9   | Monorepo            | pnpm workspaces + Turborepo                                                | Um `packages/contracts` compartilhado evita drift de evento entre serviços                                 | Polirepo (drift de contrato garantido)                                              |
| 10  | Kubernetes          | Minikube + Strimzi (Kafka) + CloudNativePG (Postgres) + KEDA (autoscaling) | KEDA escalando por **consumer lag** é o autoscaling que faz sentido em event-driven                        | HPA por CPU (métrica errada para consumidor)                                        |
| 11  | Docs                | Structurizr DSL (um modelo → os 4 níveis C4) + ADRs em MADR                | Diagrama que não desatualiza porque nasce de código                                                        | Draw.io (desatualiza na primeira semana)                                            |

Cada decisão dessas vira um arquivo em `docs/adr/NNNN-titulo.md` no formato MADR. Escreva o ADR **antes** de implementar; se você não consegue escrever o "por quê", ainda não entendeu a decisão.

---

## 1. Modelo de domínio

### Agregados (um por serviço, sem tabela compartilhada)

- **Order** (Order Service) — `id`, `customerId`, `items[]`, `totalAmount`, `currency`, `status`, `version`, `createdAt`
- **Payment** (Payment Service) — `id`, `orderId`, `amount`, `status`, `authorizationCode`, `refundedAt`
- **StockReservation** (Inventory Service) — `id`, `orderId`, `sku`, `quantity`, `status`, `expiresAt`
- **Shipment** (Shipping Service) — `id`, `orderId`, `trackingCode`, `carrier`, `labelUrl`, `status`
- **Notification** (Notification Service) — `id`, `orderId`, `channel`, `template`, `sentAt` (serviço só-consumidor, não publica evento de negócio)

### Máquina de estados do pedido

```
                        ┌──────────────── PaymentFailed ─────────────► CANCELLED
                        │
PENDING ──OrderCreated──┤
                        │                 ┌── StockUnavailable ──► COMPENSATING ──PaymentRefunded──► CANCELLED
                        └─PaymentApproved─┤
                                          │                        ┌─ShipmentFailed─► COMPENSATING ─┐
                                          └────StockReserved───────┤                                │
                                                                   └─ShipmentCreated──► CONFIRMED   │
                                                                                                     ▼
                                                        COMPENSATING + StockReleased + PaymentRefunded ► CANCELLED
```

Estados: `PENDING → PAYMENT_APPROVED → STOCK_RESERVED → CONFIRMED` (feliz) e `→ COMPENSATING → CANCELLED` (falha).

**Regra de ouro:** a transição é _monotônica e idempotente_. `applyEvent(state, event)` retorna o mesmo estado se o evento já foi aplicado e **rejeita silenciosamente** (com log em WARN) transições inválidas. Isso é o que te salva quando eventos chegam fora de ordem entre tópicos diferentes — e eles **vão** chegar.

---

## 2. Contratos de evento

### Envelope (idêntico para todo evento, em `packages/contracts`)

```ts
type EventEnvelope<T> = {
  eventId: string; // UUID v7 — chave de idempotência do consumidor
  eventType: string; // "payment.approved"
  eventVersion: number; // 1
  occurredAt: string; // ISO 8601 UTC
  aggregateId: string; // orderId — também a chave da partição Kafka
  aggregateType: string; // "order"
  correlationId: string; // constante em toda a saga: rastreia o pedido inteiro
  causationId: string; // eventId do evento que causou este: monta a árvore causal
  producer: string; // "payment-service@1.4.2"
  payload: T;
};
```

`correlationId` + `causationId` são o que permite reconstruir "por que este refund aconteceu?" três semanas depois. Não pule.

### Catálogo de eventos

| Evento              | Publicado por | Tópico                   | Consumido por                                                         |
| ------------------- | ------------- | ------------------------ | --------------------------------------------------------------------- |
| `order.created`     | Order         | `ecommerce.orders.v1`    | Payment, Notification                                                 |
| `order.cancelled`   | Order         | `ecommerce.orders.v1`    | Notification                                                          |
| `order.confirmed`   | Order         | `ecommerce.orders.v1`    | Notification                                                          |
| `payment.approved`  | Payment       | `ecommerce.payments.v1`  | Order, Inventory, Notification                                        |
| `payment.failed`    | Payment       | `ecommerce.payments.v1`  | Order, Notification                                                   |
| `payment.refunded`  | Payment       | `ecommerce.payments.v1`  | Order, Notification                                                   |
| `stock.reserved`    | Inventory     | `ecommerce.inventory.v1` | Order, Shipping, Notification                                         |
| `stock.unavailable` | Inventory     | `ecommerce.inventory.v1` | Order, **Payment (compensa)**, Notification                           |
| `stock.released`    | Inventory     | `ecommerce.inventory.v1` | Order, Notification                                                   |
| `shipment.created`  | Shipping      | `ecommerce.shipping.v1`  | Order, Notification                                                   |
| `shipment.failed`   | Shipping      | `ecommerce.shipping.v1`  | Order, **Payment (compensa)**, **Inventory (compensa)**, Notification |

**Olhe a coluna da direita.** Payment consome `stock.unavailable` e `shipment.failed` — eventos de domínios que não são dele. Inventory consome `shipment.failed`. Esse é o acoplamento implícito da coreografia: cada nova etapa da saga obriga a mexer em **todos** os serviços anteriores. Documente isso no ADR-002; é a lição principal do exercício.

### Matriz de compensação

| Falha               | Quem compensa                  | Ação compensatória       | Evento emitido                       |
| ------------------- | ------------------------------ | ------------------------ | ------------------------------------ |
| `payment.failed`    | ninguém (nada foi feito ainda) | —                        | Order → `order.cancelled`            |
| `stock.unavailable` | Payment                        | estorna a autorização    | `payment.refunded`                   |
| `shipment.failed`   | Inventory **e** Payment        | libera reserva / estorna | `stock.released`, `payment.refunded` |

Order só emite `order.cancelled` quando **todas** as compensações pendentes chegaram. Isso exige que Order saiba quais compensações esperar por estado — mais um pedaço de conhecimento global vazando para um serviço que teoricamente só reage.

---

## 3. Topologia Kafka

### Tópicos de negócio

| Tópico                   | Partições         | Replicação | Retenção | Chave     |
| ------------------------ | ----------------- | ---------- | -------- | --------- |
| `ecommerce.orders.v1`    | 3 (dev) / 6 (k8s) | 1 / 3      | 7 dias   | `orderId` |
| `ecommerce.payments.v1`  | 3 / 6             | 1 / 3      | 7 dias   | `orderId` |
| `ecommerce.inventory.v1` | 3 / 6             | 1 / 3      | 7 dias   | `orderId` |
| `ecommerce.shipping.v1`  | 3 / 6             | 1 / 3      | 7 dias   | `orderId` |

**Chave = `orderId`** é a decisão mais importante aqui: garante que todos os eventos de um mesmo pedido caem na mesma partição, logo são ordenados entre si. Eventos de pedidos diferentes podem se cruzar — e não importa.

### Retry não-bloqueante + DLT

Retry no mesmo tópico bloqueia a partição inteira (head-of-line blocking): um pedido problemático trava os outros 10 mil da partição. Solução — tópicos de retry escalonados por consumer group:

```
ecommerce.payments.v1
   └─ falha → ecommerce.payments.v1.inventory-service.retry-5s     (consumer com delay 5s)
        └─ falha → ecommerce.payments.v1.inventory-service.retry-1m
             └─ falha → ecommerce.payments.v1.inventory-service.retry-10m
                  └─ falha → ecommerce.payments.v1.inventory-service.DLT
```

**Trade-off que você precisa saber e documentar:** ao mandar uma mensagem para o tópico de retry, você **perde a ordenação** daquele pedido — a mensagem seguinte do mesmo `orderId` pode ser processada antes da que foi para o retry. Mitigações: (a) máquina de estados que rejeita transição inválida, (b) para agregados críticos, aceitar o head-of-line blocking e retryar in-place. Não existe resposta certa; existe escolha consciente.

Headers obrigatórios ao redirecionar: `x-original-topic`, `x-retry-count`, `x-first-failure-at`, `x-last-error`, `x-stacktrace-hash` (nunca o stacktrace inteiro, e nunca payload sensível).

### Erros retriáveis vs. permanentes

Classifique **antes** de retryar. Sem isso o DLQ vira lixeira e o retry vira loop caro.

- **Retriável** (vai para retry): timeout de rede, `503` do gateway, deadlock de banco, broker indisponível.
- **Permanente** (vai direto para DLT, sem passar pelos retries): falha de validação de schema, evento de versão desconhecida, regra de negócio violada, agregado inexistente.

### Configuração de produtor e consumidor

```
# Producer
acks=all
enable.idempotence=true          # dedup no broker por (producerId, seq)
max.in.flight.requests=5         # seguro com idempotence ligado
compression.type=zstd
linger.ms=10

# Consumer
enable.auto.commit=false         # NÃO NEGOCIÁVEL — commit manual após o commit do banco
isolation.level=read_committed
max.poll.records=50
session.timeout.ms=30000
heartbeat.interval.ms=3000
partition.assignment.strategy=roundRobin           # único que o kafkajs 2.x traz;
                                                  # cooperative-sticky é do cliente Java
```

`enable.auto.commit=false` é o que separa at-least-once real de perda silenciosa de mensagem. Se você commitar o offset antes de persistir, um crash come a mensagem.

---

## 4. Padrões obrigatórios

### 4.1 Transactional Outbox (produção de eventos)

Problema: gravar no banco e publicar no Kafka são dois sistemas. Se o processo morre entre os dois, você tem pedido sem evento (ou evento sem pedido).

```
BEGIN;
  INSERT INTO orders (...);
  INSERT INTO outbox (event_id, aggregate_id, event_type, payload, headers, created_at, published_at NULL);
COMMIT;
-- relay assíncrono lê outbox WHERE published_at IS NULL, publica no Kafka, marca published_at
```

Tabela `outbox`: `id`, `event_id`, `aggregate_id`, `aggregate_type`, `event_type`, `payload jsonb`, `headers jsonb`, `created_at`, `published_at`, `attempts`.

Relay: polling a cada 200ms com `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100` (o `SKIP LOCKED` é o que permite rodar N réplicas do relay sem duplicar trabalho). Índice parcial: `CREATE INDEX ON outbox (created_at) WHERE published_at IS NULL`.

O relay é **at-least-once** por construção: pode publicar e morrer antes de marcar. Por isso o consumidor precisa de idempotência — os dois padrões são um par, nunca use só um.

Job de limpeza: `DELETE FROM outbox WHERE published_at < now() - interval '7 days'`.

### 4.2 Inbox / idempotência (consumo de eventos)

```sql
CREATE TABLE processed_messages (
  event_id       uuid NOT NULL,
  consumer_group text NOT NULL,
  processed_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, consumer_group)
);
```

Todo handler segue o mesmo esqueleto — **uma única transação de banco**:

```
BEGIN;
  INSERT INTO processed_messages (event_id, consumer_group) VALUES (...);
  -- conflito de PK → mensagem já processada → ROLLBACK, commita o offset, segue a vida
  <efeito de negócio>
  INSERT INTO outbox (...);   -- o evento seguinte da saga
COMMIT;
commitOffset();               -- só depois do COMMIT
```

A chave é `(event_id, consumer_group)`, não só `event_id`: dois consumer groups diferentes precisam processar o mesmo evento.

Retenção: `processed_messages` cresce para sempre. Job diário apagando > 30 dias, com a retenção **maior** que a retenção do tópico (senão um replay reprocessa tudo).

### 4.3 Idempotência na borda HTTP

`POST /orders` com header `Idempotency-Key`. Guarde `(key, customerId) → orderId + response body + status` por 24h (Redis ou tabela). Mesma chave → devolve a resposta original sem criar pedido novo. Sem isso, um duplo-clique do cliente vira dois pedidos e duas cobranças.

### 4.4 Timeout de saga

Coreografia não tem quem vigie o todo. Um `stock.reserved` que nunca vira `shipment.created` deixa o pedido pendurado para sempre.

Implemente no Order Service um **sweeper**: job que varre pedidos em estado não-terminal com `updated_at < now() - timeout_do_estado` e dispara compensação. Timeouts por estado: `PENDING` 2min, `PAYMENT_APPROVED` 5min, `STOCK_RESERVED` 10min.

Note a ironia e anote no ADR: para a coreografia funcionar de verdade, o Order Service acabou virando um meio-orquestrador. Isso é exatamente o argumento a favor da orquestração.

### 4.5 Simulação determinística de falha

Nada de `Math.random()` — teste que não é determinístico não é teste. Use regras sobre o payload:

| Gatilho                                 | Efeito                                                  |
| --------------------------------------- | ------------------------------------------------------- |
| `totalAmount` termina em `.13`          | `payment.failed`                                        |
| `totalAmount` > 10000                   | Payment demora 30s (testa timeout de saga)              |
| SKU começa com `OUT-`                   | `stock.unavailable`                                     |
| CEP começa com `00000`                  | `shipment.failed`                                       |
| header `x-simulate: duplicate`          | outbox publica o evento duas vezes (testa idempotência) |
| header `x-simulate: crash-after-commit` | mata o processo entre COMMIT e commitOffset             |

---

## 5. Estrutura do monorepo

```
microserviceProjet/
├─ apps/
│  ├─ order-service/
│  │  ├─ src/
│  │  │  ├─ api/               # OrdersController, DTOs, IdempotencyGuard, AuthGuard
│  │  │  ├─ application/       # CreateOrderUseCase, OrderStateMachine, SagaTimeoutSweeper
│  │  │  ├─ domain/            # Order (agregado), erros de domínio
│  │  │  ├─ infrastructure/    # PrismaOrderRepository, OutboxRepository, consumers/
│  │  │  └─ health/
│  │  ├─ prisma/schema.prisma
│  │  ├─ test/                 # integração com Testcontainers
│  │  └─ Dockerfile
│  ├─ payment-service/         # mesma estrutura
│  ├─ inventory-service/
│  ├─ shipping-service/
│  └─ notification-service/
├─ packages/
│  ├─ contracts/               # envelope + schemas Zod + tipos de todos os eventos
│  ├─ kafka/                   # KafkaModule: producer, consumer, retry/DLT, commit manual
│  ├─ outbox/                  # tabela, repositório e relay reutilizáveis
│  ├─ idempotency/             # ProcessedMessages + decorator @Idempotent
│  └─ observability/           # OTel bootstrap, logger pino, métricas
├─ deploy/
│  ├─ docker/docker-compose.yml
│  ├─ k8s/base/                # manifests crus (Fase 10a)
│  └─ helm/                    # charts (Fase 10b)
├─ docs/
│  ├─ architecture/workspace.dsl    # Structurizr — os 4 níveis C4
│  ├─ adr/
│  └─ runbooks/
├─ tools/
│  ├─ dlq-inspector/           # CLI: listar, inspecionar, reprocessar DLT
│  └─ load/k6/
└─ turbo.json, pnpm-workspace.yaml
```

`packages/contracts` é a única fonte de verdade dos eventos. Serviço **nunca** define o schema de um evento que ele consome — importa do contracts. É o que evita drift silencioso.

---

## 6. Fases de execução

Cada fase tem critério de pronto verificável. Não avance sem cumprir.

### Fase 0 — Fundação (½ dia)

- pnpm workspaces + Turborepo, TypeScript strict, ESLint + Prettier, Husky + commitlint
- `packages/contracts` com o envelope e o schema Zod de `order.created`
- `deploy/docker/docker-compose.yml`: Kafka (KRaft, single node), Kafka UI, Postgres, Jaeger, Prometheus, Grafana
- `.env.example` com valores **fictícios**; `.env` no `.gitignore` desde o primeiro commit
- ✅ **Pronto quando:** `docker compose up` sobe tudo e o Kafka UI lista os tópicos criados por script (`auto.create.topics.enable=false`)

### Fase 1 — Order Service, HTTP + outbox (1 dia)

- `POST /orders`, `GET /orders/:id`, validação com Zod, `Idempotency-Key`
- Prisma schema + migration, tabela `outbox`
- Outbox relay com `FOR UPDATE SKIP LOCKED`
- ✅ **Pronto quando:** um POST cria a linha em `orders` e o `order.created` aparece no Kafka UI com a chave certa — e um segundo POST com a mesma Idempotency-Key devolve o mesmo `orderId` sem criar nada

### Fase 2 — Infra de consumo reutilizável (1 dia)

- `packages/kafka`: consumer com commit manual, classificação de erro, retry escalonado, DLT
- `packages/idempotency`: tabela + decorator `@Idempotent()`
- Graceful shutdown: SIGTERM → para de consumir → termina o que está em voo → fecha conexões
- ✅ **Pronto quando:** um handler que lança erro permanente cai na DLT na primeira tentativa, e um que lança erro retriável passa por `retry-5s → retry-1m → retry-10m → DLT` com os headers corretos

### Fase 3 — Payment Service (1 dia)

- Consome `order.created`, autoriza (mock determinístico), publica `payment.approved` / `payment.failed`
- Order consome os dois e projeta o estado
- ✅ **Pronto quando:** o cenário feliz e o `payment.failed` funcionam ponta a ponta, e reentregar o mesmo `order.created` manualmente não gera segunda autorização

### Fase 4 — Inventory + primeira compensação (1 dia)

- Consome `payment.approved`, reserva estoque, publica `stock.reserved` / `stock.unavailable`
- **Payment passa a consumir `stock.unavailable`** e emite `payment.refunded`
- Order só cancela depois de receber o `payment.refunded`
- ✅ **Pronto quando:** pedido com SKU `OUT-*` termina em `CANCELLED` com o pagamento estornado, e o saldo de estoque volta ao valor original

### Fase 5 — Shipping + Notification (1 dia)

- Shipping consome `stock.reserved`, gera etiqueta, publica `shipment.created` / `shipment.failed`
- Compensação dupla no `shipment.failed`: Inventory libera, Payment estorna
- Notification consome tudo e "envia" e-mail (log estruturado + Mailhog)
- ✅ **Pronto quando:** os 4 caminhos (feliz, falha em cada etapa) fecham em estado terminal correto, com compensações completas

### Fase 6 — Resiliência e replay (1–2 dias)

- Sweeper de timeout de saga
- `tools/dlq-inspector`: listar DLT, ver payload (mascarado), reprocessar mensagem para o tópico original
- Replay: reprocessar um tópico do offset zero num consumer group novo e verificar que o estado final é idêntico
- Testes de caos: matar o broker no meio da saga; matar o serviço entre COMMIT e commitOffset; entregar evento duplicado; entregar eventos fora de ordem
- ✅ **Pronto quando:** todos os cenários de caos terminam em estado consistente, e o replay do zero reconstrói o mesmo estado final

### Fase 7 — Observabilidade (1 dia)

- OpenTelemetry: auto-instrumentação Nest + kafkajs + pg, `traceparent` propagado nos headers Kafka
- Jaeger mostrando o trace da saga inteira, atravessando os 5 serviços
- Métricas Prometheus: `saga_duration_seconds`, `saga_compensations_total`, `dlq_messages_total`, `outbox_lag_seconds`, `kafka_consumer_lag`
- Dashboard Grafana + alertas (lag > 1000, DLT > 0, taxa de compensação > 5%)
- Log estruturado (pino) com `correlationId` em toda linha
- ✅ **Pronto quando:** você abre um `orderId` no Jaeger e vê a saga inteira, incluindo as compensações, num único trace

### Fase 7b — Saga Observer: a UI real (1,5 dia)

O simulador em `docs/simulator/` ensina os padrões, mas é uma simulação. Esta fase liga a mesma ideia ao sistema de verdade — e só faz sentido **depois** da Fase 7, porque consome o que a observabilidade já produz.

Um app em `apps/saga-observer` (NestJS + SSE + página estática, sem framework de front):

- **Backend:** consumer group próprio (`saga-observer`) assinando os 4 tópicos de negócio, os de retry e as DLT. Ele **não** tem efeito de negócio: só projeta em memória e empurra por SSE. Grupo separado é o que garante que observar não interfere no fluxo.
- **Leitura direta:** `GET /api/orders/:id/saga` compõe, via API de cada serviço, o estado do pedido + outbox pendente + linhas de `processed_messages`. Cada serviço expõe isso em `/internal/saga-debug/:orderId`, **desligado por padrão** (`SAGA_DEBUG_ENABLED=false`) — é dado de cliente, não dashboard público.
- **Lag e DLT reais:** admin client do kafkajs (`fetchOffsets` / `fetchTopicOffsets`) por grupo, e leitura das DLT sob demanda.
- **Front:** a mesma topologia de barramentos do simulador, alimentada por SSE em vez de fila simulada. Os botões de disparo fazem `POST /orders` de verdade, com os gatilhos determinísticos da seção 4.5.
- **Ação, não só leitura:** reprocessar mensagem da DLT pela UI, chamando a `tools/dlq-inspector` por trás.

**Segurança desta fase** — é uma UI que expõe dado de pedido e permite reprocessar mensagem: `SAGA_DEBUG_ENABLED` desligado por padrão; autenticação obrigatória e papel de operador para reprocessar (A01, deny-by-default); PII mascarada **na resposta da API**, não no front — mascarar no cliente significa que o dado cru já saiu do servidor; sem Ingress público, só port-forward ou Ingress interno autenticado (A02); audit log de todo reprocessamento, com quem pediu (A09).

- ✅ **Pronto quando:** um `POST /orders` real aparece na topologia em menos de 1s; matar o pod do Payment no meio da saga é visível como lag subindo e depois drenando; uma mensagem na DLT é reprocessada pela UI e o pedido chega a estado terminal.

### Fase 8 — Documentação C4 (1 dia)

- `docs/architecture/workspace.dsl` — Structurizr DSL, um modelo, quatro views
- Nível 1 Contexto: Cliente, Operador, Gateway de Pagamento, Transportadora, Provedor de E-mail
- Nível 2 Contêineres: 5 serviços + Kafka + 5 Postgres + Redis + stack de observabilidade
- Nível 3 Componentes do Order Service: Controller, IdempotencyGuard, CreateOrderUseCase, OrderStateMachine, OrderRepository, OutboxRepository, OutboxRelay, EventConsumer, SagaTimeoutSweeper, HealthController
- Nível 4 (opcional): a máquina de estados
- Structurizr Lite no compose; export Mermaid para o README
- ADRs preenchidos
- ✅ **Pronto quando:** alguém que nunca viu o projeto entende o fluxo pelos diagramas, sem abrir código

### Fase 9 — Dockerização de verdade (½ dia)

Dockerfile multi-stage por serviço:

1. `deps` — `pnpm fetch` + `--frozen-lockfile` (cacheável)
2. `build` — compila TS, `prisma generate`
3. `prune` — `pnpm deploy --filter=<svc> --prod`
4. `runtime` — `node:22-alpine`, `USER node` (não-root), `tini` como PID 1, sem shell desnecessário

Regras: imagem base **pinada por digest**, `.dockerignore` completo, `HEALTHCHECK` em `/health/live`, sem segredo em `ARG`/`ENV`, `NODE_ENV=production`, scan com Trivy e SBOM com Syft no build.

- ✅ **Pronto quando:** todas as imagens < 200MB, rodam como não-root, Trivy sem HIGH/CRITICAL, e `docker compose up` sobe a saga completa só com imagens buildadas

### Fase 10 — Kubernetes / Minikube (2–3 dias)

**10a — Manifests crus** (`deploy/k8s/base/`) — escreva YAML na mão antes de partir para Helm; é o que ensina.

```bash
minikube start --memory=8192 --cpus=4 --disk-size=40g
minikube addons enable ingress metrics-server
```

Componentes de plataforma:

- **Strimzi** operator → `Kafka` CR em modo KRaft, `KafkaTopic` CR por tópico (com os de retry e DLT), `KafkaUser` com SCRAM-SHA-512 + ACLs por serviço (deny-by-default: cada serviço só lê e escreve o que precisa)
- **CloudNativePG** operator → um `Cluster` por serviço (se a RAM apertar: um cluster com 5 databases, e anote a dívida)
- **KEDA** → `ScaledObject` por consumidor, escalando por **lag do consumer group** (não por CPU)

Por serviço: `Namespace`, `ServiceAccount`, `ConfigMap` (não-sensível), `Secret` (credenciais — via Sealed Secrets ou SOPS, **nunca** YAML plano no git), `Deployment` (2 réplicas), `Service` ClusterIP, `PodDisruptionBudget` (minAvailable 1), `NetworkPolicy` (deny-all + allow explícito), `Ingress` só para o Order Service.

Probes — cada uma responde a uma pergunta diferente:

- `startupProbe` `/health/startup` — failureThreshold 30, period 2s (dá tempo à migration)
- `readinessProbe` `/health/ready` — checa Postgres **e** conexão Kafka; falhar tira do Service sem matar o pod
- `livenessProbe` `/health/live` — raso, só "o event loop responde". Nunca cheque dependência externa aqui: um Kafka fora do ar reiniciaria todos os pods em loop

`terminationGracePeriodSeconds: 45` + `preStop` sleep 5s + `maxUnavailable: 0` / `maxSurge: 1`, casado com o graceful shutdown da Fase 2. Sem isso, todo deploy perde mensagem em voo.

Migrations: `Job` com `helm.sh/hook: pre-upgrade` rodando `prisma migrate deploy` — **não** em initContainer (5 réplicas rodariam a migration em paralelo).

**10b — Helm** — chart-biblioteca `microservice` + values por serviço + umbrella chart. Depois: `helm template | kubeconform` no CI.

- ✅ **Pronto quando:** `helm install` sobe tudo do zero; a saga funciona via Ingress; matar um pod no meio da saga não perde mensagem; e injetar 5k pedidos faz o KEDA escalar o consumidor mais lento de 2 para 8 réplicas e voltar

### Fase 11 — Versão orquestrada (opcional, 2 dias)

Só se quiser a comparação que o enunciado original pedia. Adiciona `saga-orchestrator-service`: state machine persistida, envia **comandos** (`ReserveStockCommand`) em vez de reagir a eventos, e trata as falhas num lugar só. Os serviços de domínio viram executores burros com tópicos de comando/resposta.

Depois compare por métrica, não por opinião: linhas de código para adicionar uma 5ª etapa na saga; número de serviços tocados; tempo para responder "por que o pedido X foi cancelado?"; latência ponta a ponta.

### Fase 12 — Avançado (opcional)

Schema Registry + evolução de contrato com compatibilidade `BACKWARD`; Debezium substituindo o outbox relay por CDC; tópico compactado com o estado da saga; particionamento e reprocessamento seletivo; multi-região.

---

## 7. Estratégia de testes

| Nível      | O que                                                                                                               | Ferramenta                      |
| ---------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| Unitário   | Máquina de estados, classificação de erro, regras de compensação                                                    | Vitest                          |
| Contrato   | Todo evento publicado valida contra o Zod do `contracts`; snapshot de schema quebra o build em mudança incompatível | Vitest + snapshot               |
| Integração | Um serviço + Kafka + Postgres reais                                                                                 | Testcontainers                  |
| SAGA E2E   | 4 caminhos + duplicata + fora de ordem + crash entre COMMIT e offset                                                | Testcontainers (stack completa) |
| Carga      | 1000 pedidos/min, medir lag e p99                                                                                   | k6                              |
| Caos       | Broker cai, pod morre, partição de rede, disco cheio                                                                | scripts + Minikube              |

O teste que mais ensina: **entregue o mesmo evento 3× e verifique que o efeito de negócio aconteceu 1×**. Se passar, sua idempotência é real.

---

## 8. Segurança (OWASP Top 10:2025)

Este sistema tem I/O externo, autenticação, dado de cliente e infraestrutura — a revisão é obrigatória, não opcional.

| Categoria                         | Risco concreto aqui                                                              | Correção                                                                                                                                | Sev.    |
| --------------------------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| **A01 Broken Access Control**     | `GET /orders/:id` devolvendo pedido de outro cliente; `customerId` vindo do body | Authz no servidor, deny-by-default; `customerId` **sempre** do JWT, nunca do payload; checar dono em toda leitura                       | Alta    |
| **A01 (SSRF)**                    | Shipping chamando URL de transportadora vinda do evento                          | Allowlist de hosts; bloquear IP privado/link-local; sem seguir redirect                                                                 | Média   |
| **A02 Security Misconfiguration** | Kafka sem auth; Postgres com NodePort; Kafka UI exposto                          | SCRAM-SHA-512 + TLS via Strimzi; `auto.create.topics.enable=false`; NetworkPolicy deny-all; nada de banco/UI em Ingress                 | Alta    |
| **A03 Supply Chain**              | Dependência transitiva comprometida; imagem base `:latest`                       | Lockfile commitado, `pnpm audit` + Trivy no CI, imagem pinada por digest, SBOM (Syft) por build                                         | Alta    |
| **A04 Cryptographic Failures**    | Dado de cartão no evento e, pior, na DLT                                         | **Nunca** persistir PAN/CVV: só token do gateway + últimos 4 dígitos; TLS em todo tráfego; disco criptografado                          | Crítica |
| **A05 Injection**                 | Payload de evento é entrada **não confiável** vinda do broker                    | Zod valida **antes** de tocar no handler; Prisma parametriza queries; nunca `$queryRawUnsafe` com dado de evento                        | Alta    |
| **A06 Insecure Design**           | Duplo processamento = cobrança dupla                                             | Idempotência é controle de negócio, não detalhe técnico; limite de retry; timeout de saga; reserva de estoque com expiração             | Alta    |
| **A07 Authentication Failures**   | `POST /orders` sem rate limit vira vetor de abuso                                | JWT de vida curta, rate limit por cliente e por IP, senha com Argon2id se houver login próprio                                          | Alta    |
| **A08 Data Integrity Failures**   | Consumir evento de versão desconhecida e "adivinhar"                             | Rejeitar `eventVersion` desconhecida → DLT; nunca desserializar em tipo arbitrário; validar `producer`                                  | Média   |
| **A09 Logging Failures**          | Log com e-mail, CPF, endereço completo; stacktrace com payload                   | `correlationId` sim, PII não: mascare (`j***@example.com`); DLT com retenção curta e criptografada; audit log de reprocessamento de DLQ | Alta    |
| **A10 Exceptional Conditions**    | `catch` que aprova pagamento ou libera estoque "porque deu erro"                 | **Fail secure**: erro desconhecido → não commita offset → retry → DLT. Estado indeterminado nunca vira estado favorável ao cliente      | Crítica |

**Segredos:** `.env` no `.gitignore` no primeiro commit; `.env.example` só com valores fictícios (`example.com`, `changeme`); credencial em `Secret` do k8s via Sealed Secrets/SOPS, nunca em `ConfigMap` nem em `ARG` de Dockerfile (fica na camada da imagem); rotação documentada em `docs/runbooks/`. Dados de cliente nos seeds: fictícios, domínio `example.com`.

---

## 9. Armadilhas conhecidas

1. **`@nestjs/microservices` Kafka transport** — abstrai o commit de offset e dificulta commit manual pós-transação. Por isso a decisão de usar `kafkajs` direto num módulo próprio. Se insistir no transport, você vai reescrever na Fase 2.
2. **Tópico de retry quebra a ordenação** — já detalhado. Decida conscientemente e escreva o ADR.
3. **Outbox relay sem `SKIP LOCKED`** — com 2 réplicas, ambos pegam as mesmas linhas e publicam duplicado. Idempotência salva, mas você desperdiça throughput.
4. **Commit de offset antes do commit do banco** — perde mensagem em crash. É o bug mais comum e o mais silencioso.
5. **Liveness probe checando Kafka** — Kafka pisca, Kubernetes mata todos os pods, o cluster entra em crashloop e você perde a manhã.
6. **RAM do Minikube** — Kafka + 5 Postgres + 5 serviços + observabilidade não cabe em 4GB. Se a máquina apertar: um Postgres com 5 databases, 1 réplica por serviço, e Prometheus com retenção de 6h.
7. **Rebalance storm no deploy** — atenção: `cooperative-sticky` **não existe no kafkajs 2.x** (só `roundRobin`, protocolo eager, sem static membership). As mitigações que funcionam são `maxUnavailable: 0` + `maxSurge: 1` (um pod por vez) e graceful shutdown que sai do grupo — saída suja custa um `sessionTimeout` inteiro de consumo parado. Ver `docs/aprender/10`.
8. **DLQ sem ferramenta de reprocesso** — vira cemitério que ninguém olha. A `tools/dlq-inspector` da Fase 6 não é enfeite.
9. **`processed_messages` crescendo sem limite** — some com o disco em semanas. Job de limpeza desde a Fase 2, com retenção maior que a do tópico.
10. **Testar com `Math.random()`** — teste intermitente é pior que teste ausente. Gatilhos determinísticos, sempre.

---

## 10. Cronograma

| Fase                        | Esforço | Acumulado |
| --------------------------- | ------- | --------- |
| 0 — Fundação                | 0,5d    | 0,5d      |
| 1 — Order + outbox          | 1d      | 1,5d      |
| 2 — Infra de consumo        | 1d      | 2,5d      |
| 3 — Payment                 | 1d      | 3,5d      |
| 4 — Inventory + compensação | 1d      | 4,5d      |
| 5 — Shipping + Notification | 1d      | 5,5d      |
| 6 — Resiliência e replay    | 1,5d    | 7d        |
| 7 — Observabilidade         | 1d      | 8d        |
| 7b — Saga Observer (UI)     | 1,5d    | 9,5d      |
| 8 — C4                      | 1d      | 10,5d     |
| 9 — Docker                  | 0,5d    | 11d       |
| 10 — Kubernetes             | 2,5d    | **13,5d** |
| 11 — Orquestrada (opcional) | 2d      | 15,5d     |
| 12 — Avançado (opcional)    | 3d+     | —         |

**Núcleo entregável: ~13,5 dias de trabalho focado.**

Marcos de corte, se o tempo apertar: Fase 5 = saga completa funcionando (demo possível); Fase 8 = documentado; Fase 10 = rodando em Kubernetes.

---

## 11. Checklist de conclusão

- [ ] `docker compose up` sobe o sistema inteiro em um comando
- [ ] `helm install` sobe em Minikube em um comando
- [ ] Os 4 caminhos da saga terminam em estado terminal correto
- [ ] Evento entregue 3× produz efeito de negócio 1×
- [ ] Matar pod no meio da saga não perde nem duplica efeito
- [ ] Erro permanente vai direto para DLT; retriável passa pelos 3 níveis
- [ ] DLQ inspecionável e reprocessável por CLI
- [ ] Replay do offset zero reconstrói o mesmo estado final
- [ ] Trace único no Jaeger cobrindo os 5 serviços
- [ ] Saga Observer mostra um `POST /orders` real na topologia em menos de 1s
- [ ] KEDA escala por consumer lag e volta ao repouso
- [ ] 4 níveis C4 gerados do Structurizr DSL
- [ ] ADRs escritos para as 11 decisões
- [ ] Trivy sem HIGH/CRITICAL; imagens não-root
- [ ] Nenhum segredo no git; `.env.example` só com valores fictícios
- [ ] Nenhuma PII em log ou DLT
- [ ] README explica o trade-off coreografia vs. orquestração com evidência do próprio código
