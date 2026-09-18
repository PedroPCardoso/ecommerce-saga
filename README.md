# SAGA coreografada sobre Kafka — repositório de estudos

Cinco microserviços NestJS coordenando um pedido de e-commerce **sem orquestrador
central**, com Docker e Kubernetes, documentado em C4.

O objetivo não é ter o código pronto: é entender por que cada peça existe, o que ela
custa, e como cada uma falha quando você erra. Por isso o repositório tem três coisas que
se complementam de propósito.

|                                                   | Serve para                                                 | Não serve para                              |
| ------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------- |
| **[Módulos de aprendizado](docs/aprender/)**      | entender o _porquê_ de cada decisão                        | ver funcionando                             |
| **[Simulador](docs/simulator/saga-console.html)** | ver o fluxo inteiro e o estado de cada tabela a cada passo | ver falha real de infraestrutura            |
| **[Exemplos executáveis](examples/)**             | provar o comportamento contra Kafka e Postgres reais       | ver a saga completa (isolam um padrão cada) |

O simulador mostra o **todo** sem ser real. Os exemplos mostram o **real** sem ser o todo.
Os módulos explicam por que as duas coisas são assim.

---

## O fluxo

```
POST /orders
    │
    └─► order.created ──► Payment autoriza
                              ├── payment.failed ─────────────────────────► CANCELLED
                              └── payment.approved ──► Inventory reserva
                                                          ├── stock.unavailable ──► Payment ESTORNA* ──► CANCELLED
                                                          └── stock.reserved ──► Shipping etiqueta
                                                                                     ├── shipment.failed ──► Payment ESTORNA*
                                                                                     │                    └► Inventory LIBERA* ──► CANCELLED
                                                                                     └── shipment.created ──► CONFIRMED
```

Ninguém comanda. Cada serviço reage a eventos e publica o que aconteceu no seu domínio.

\* **Estorno/liberação (compensação) ainda não estão implementados** — ver "Estado
atual" mais abaixo. Hoje esses dois ramos param em `COMPENSATING`, não em `CANCELLED`.

## O ponto do exercício

Olhe o Payment Service. Para estornar, ele precisa assinar `ecommerce.inventory.v1` e
`ecommerce.shipping.v1` — tópicos de domínios que **não são dele**. Acrescentar uma 5ª
etapa na saga obriga a mexer em todos os serviços anteriores, e ninguém tem o desenho do
fluxo inteiro.

Esse acoplamento implícito é a lição, não um defeito da implementação. Está em
[ADR-0002](docs/adr/0002-saga-coreografada.md), com a comparação contra orquestração, e
desenhado na topologia do simulador.

---

## Começando

Requer Node 22 (`nvm use`), pnpm 9 e Docker.

```bash
nvm use                 # respeita o .nvmrc
pnpm install
cp .env.example .env    # valores locais fictícios; .env nunca é versionado

pnpm infra:up           # Kafka, 5x Postgres, Jaeger, Prometheus, Grafana, Mailhog, Structurizr
pnpm topics:create      # cria os 56 tópicos declarados em packages/contracts
pnpm test               # contratos de evento e topologia

cd examples && pnpm ex todos    # os 6 exemplos, contra a infra real (~1 min)
```

| Serviço          | URL                    |
| ---------------- | ---------------------- |
| Kafka UI         | http://localhost:8080  |
| Structurizr (C4) | http://localhost:8081  |
| Jaeger           | http://localhost:16686 |
| Prometheus       | http://localhost:9090  |
| Grafana          | http://localhost:3300  |
| Mailhog          | http://localhost:18025 |

Os cinco Postgres ficam em `15432`–`15436` (order, payment, inventory, shipping,
notification) e o SMTP do Mailhog em `11025`. A faixa é deslocada de propósito: `5432` e
`8025` costumam estar ocupadas por outros projetos, e o Compose falha o bind **em
silêncio** — o container sobe, a porta não publica, e você perde meia hora achando que é
rede.

`pnpm infra:down` para. `pnpm infra:nuke` apaga os volumes também.

---

## Por onde estudar

Comece pelo [caminho de aprendizado](docs/aprender/). Dez módulos, em ordem, cada um com
o problema concreto, o mecanismo, onde ele vive no código, o exemplo que o prova, e o modo
de falha específico de quando você erra.

| #   | Módulo                                                                          | Pergunta                                | Exemplo      |
| --- | ------------------------------------------------------------------------------- | --------------------------------------- | ------------ |
| 01  | [Por que SAGA](docs/aprender/01-por-que-saga.md)                                | consistência sem transação distribuída? | —            |
| 02  | [Coreografia vs. orquestração](docs/aprender/02-coreografia-vs-orquestracao.md) | quem coordena?                          | simulador    |
| 03  | [Partições, chave e ordenação](docs/aprender/03-particoes-chave-e-ordenacao.md) | por que `orderId` é a chave?            | `pnpm ex 01` |
| 04  | [Entrega e commit de offset](docs/aprender/04-entrega-e-commit-de-offset.md)    | por que `autoCommit: false`?            | `pnpm ex 02` |
| 05  | [Transactional Outbox](docs/aprender/05-outbox.md)                              | publicar e persistir atomicamente?      | `pnpm ex 03` |
| 06  | [Idempotência](docs/aprender/06-idempotencia.md)                                | não cobrar duas vezes?                  | `pnpm ex 04` |
| 07  | [Retry e DLT](docs/aprender/07-retry-e-dlt.md)                                  | e quando o handler falha?               | `pnpm ex 05` |
| 08  | [Compensação](docs/aprender/08-compensacao.md)                                  | desfazer o que já foi efetivado?        | simulador    |
| 09  | [Replay e evolução de schema](docs/aprender/09-replay-e-evolucao.md)            | reprocessar o passado?                  | `pnpm ex 06` |
| 10  | [Do Docker ao Kubernetes](docs/aprender/10-do-docker-ao-kubernetes.md)          | o que muda num cluster?                 | —            |

E abra o [simulador](docs/simulator/saga-console.html) numa aba enquanto lê — ele executa
a saga inteira no navegador, com nove cenários determinísticos e inspetores para outbox,
`processed_messages`, escada de retry, DLT e offsets. O modo **Passo** avança uma operação
por vez: é assim que se vê que o commit de offset acontece _depois_ do COMMIT do banco.

---

## Estrutura

```
docs/aprender/         os 10 módulos — comece aqui
docs/simulator/        o console interativo da saga (HTML, abre no navegador)
docs/adr/              as 11 decisões, em MADR, com as consequências negativas escritas
docs/architecture/     modelo C4 em Structurizr DSL (Contexto, Contêineres, Componentes do Order Service — nível 4/máquina de estados fica no ASCII do §1 acima, de propósito)
docs/PLAN.md           plano de execução completo, 13 fases

examples/              6 exemplos executáveis contra Kafka e Postgres reais

packages/contracts/    fonte ÚNICA dos contratos de evento e da topologia Kafka
packages/kafka/        consumo com commit manual, retry escalonado, DLT      (Fase 2)
packages/outbox/       Transactional Outbox + relay                          (Fase 2)
packages/idempotency/  tabela de inbox + markProcessed() dentro da transação  (Fase 2)
apps/                  os 5 serviços de negócio (Fases 1 a 5) + saga-observer (Fase 7b)
deploy/docker/         infra local
deploy/k8s|helm/       Kubernetes                                          (Fase 10)
tools/dlq-inspector/   CLI para inspecionar e reprocessar a DLT              (Fase 6)
```

Serviço **nunca** declara o schema de um evento que consome — importa de
`@ecommerce/contracts`. É o que evita divergência silenciosa entre cinco bases de código.

---

## Os padrões, e o que cada um cobra

| Padrão                                                                   | Problema que resolve                                 | O que cobra em troca                                         |
| ------------------------------------------------------------------------ | ---------------------------------------------------- | ------------------------------------------------------------ |
| **[Outbox](docs/aprender/05-outbox.md)**                                 | gravar no banco e publicar não é atômico             | latência do relay + publicação at-least-once                 |
| **[Inbox](docs/aprender/06-idempotencia.md)**                            | at-least-once = risco de cobrar duas vezes           | tabela que cresce, com retenção maior que a do tópico        |
| **[Retry + DLT](docs/aprender/07-retry-e-dlt.md)**                       | Kafka não tem DLQ; retry in-place trava a partição   | a ordenação daquele pedido morre no desvio                   |
| **[Chave de partição](docs/aprender/03-particoes-chave-e-ordenacao.md)** | sem chave não há ordem entre eventos do mesmo pedido | hot partition; repartitionar quebra a ordem                  |
| **[Compensação](docs/aprender/08-compensacao.md)**                       | não existe rollback distribuído                      | é visível ao cliente, pode falhar, e precisa ser idempotente |
| **Sweeper de timeout**                                                   | em coreografia ninguém vigia o todo                  | o Order Service vira um meio-orquestrador                    |

Outbox e idempotência são um **par**. Usar só um dá falsa segurança, o que é pior que não
usar nenhum — o exemplo 03 mostra exatamente por quê.

---

## Estado atual

- [x] **Fase 0** — fundação: monorepo, contratos (11 eventos, 22 testes), infra local, 11 ADRs, C4 níveis 1 e 2
- [x] **Material de estudo** — 10 módulos, simulador interativo, 6 exemplos executáveis
- [x] **Fase 1** — Order Service: HTTP + outbox + **projeção do estado da saga** (`order.status` avança de verdade até `CONFIRMED`/`CANCELLED`)
- [x] **Fase 2** — `packages/kafka` e `packages/idempotency`
- [x] **Fases 3–5** — Payment, Inventory, Shipping, Notification (caminho feliz e `payment.failed` completos, ponta a ponta, contra Kafka/Postgres reais)
- [x] **Fase 9** — Docker de produção (5 imagens multi-stage, non-root, 0 CVE HIGH/CRITICAL, docker-compose integrado)
- [x] **Fase 6** — resiliência, caos, replay, `dlq-inspector`
- [x] **Fase 7** — observabilidade (tracing + métricas nos 5 serviços) · **7b** — `apps/saga-observer`: 6º serviço, só-consumidor, projeta a saga em memória e expõe via SSE (`GET /api/orders`, `GET /api/orders/stream`) um front estático (`public/index.html`) que mostra cada evento chegando ao vivo
- [ ] **Fase 8** — C4 nível 3
- [~] **Fase 10** — Kubernetes / Minikube: manifests crus (10a) escritos e commitados, CloudNativePG provado ao vivo, Strimzi Kafka **bloqueado** nesta rodada — ver [seção dedicada](#kubernetes-fase-10) abaixo
- [ ] **Fase 11** — versão orquestrada, para comparação (opcional)

**Limitação conhecida e deliberada:** a matriz de compensação (`payment.refunded`,
`stock.released` — ver [módulo 08](docs/aprender/08-compensacao.md) e
`COMPENSATION_MATRIX` em `packages/contracts/src/registry.ts`) ainda não está
implementada em nenhum serviço. Um pedido que falha em `stock.unavailable` ou
`shipment.failed` avança para `COMPENSATING` e **fica lá** — de propósito: fechar o
pedido como `CANCELLED` sem a compensação ter de fato acontecido seria mentir no
histórico do pedido. `payment.failed` (nada foi efetivado ainda) fecha normalmente em
`CANCELLED`. Isso significa que hoje não há nada — humano ou automático — vigiando
pedidos presos em `COMPENSATING`; é o próximo trabalho antes de a Fase 6 (DLQ/replay)
fazer sentido.

---

## Segurança

`.env` nunca é versionado; `.env.example` só tem valores fictícios. Nenhum evento carrega
PAN ou CVV — só o token opaco do gateway e os 4 últimos dígitos. Payload vindo do broker é
tratado como entrada não confiável e validado com Zod **antes** de chegar ao handler. Erro
desconhecido nunca libera estoque nem aprova pagamento: sem commit de offset, a mensagem
vai para retry e depois para a DLT. Stacktrace em header de DLT vai como hash, porque
stacktrace carrega payload e payload carrega PII.

A revisão OWASP Top 10:2025 completa está na seção 8 do [plano](docs/PLAN.md), e o módulo
[10](docs/aprender/10-do-docker-ao-kubernetes.md) cobre o que muda ao ir para o cluster.

**Escopo reduzido, deliberado, do `saga-observer` (Fase 7b):** o painel em
`http://localhost:3005` **não tem autenticação, não tem papel de operador, e não permite
reprocessar a DLT nem consultar um endpoint `/internal/saga-debug/:orderId` por serviço** —
qualquer coisa nessa rede local enxerga o painel. Isso é intencional: as três coisas juntas
(auth + RBAC + audit log de quem reprocessou o quê) criam uma superfície administrativa nova
que merece revisão de segurança dedicada, fora do escopo desta fase. Pelo mesmo motivo, o
que o SSE expõe é só `{ orderId, eventType, status, occurredAt }` — nunca `customerId`,
endereço, dado de pagamento ou qualquer outro campo de `payload` (A01/A09). Ver
`docs/superpowers/plans/2026-09-18-fase7b-saga-observer.md` (Global Constraints).

---

## Kubernetes (Fase 10)

Manifests crus (sem Helm) em `deploy/k8s/base/` — `Namespace` único (`ecommerce-saga`),
`Cluster` CloudNativePG com 5 databases lógicos, `Kafka` CR do Strimzi (KRaft, nó único),
80 `KafkaTopic` CRs gerados por script a partir de `packages/contracts` (nunca escritos à
mão), um `Deployment`+`Service`+`ConfigMap`+`Secret`+`NetworkPolicy`+`PodDisruptionBudget`
por serviço de negócio, `Ingress` só para o Order Service, e um `ScaledObject` de exemplo do
KEDA. Ver `docs/superpowers/plans/2026-09-18-fase10-kubernetes.md` para o plano completo,
incluindo os Global Constraints (RAM do Docker local limitada a 7.8GB — por isso um único
`Cluster` Postgres com 5 databases em vez de 5 clusters, e Helm/10b e o teste de carga do
KEDA ficaram deliberadamente fora do escopo).

### Como subir

```bash
brew install minikube helm
docker compose -f deploy/docker/docker-compose.yml down   # as duas infras não cabem juntas na RAM local
minikube start --driver=docker --memory=6144 --cpus=4 --disk-size=30g
minikube addons enable ingress
minikube addons enable metrics-server
kubectl create namespace ecommerce-saga
kubectl config set-context --current --namespace=ecommerce-saga

helm repo add strimzi https://strimzi.io/charts/
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install strimzi-operator strimzi/strimzi-kafka-operator --namespace ecommerce-saga
helm install cnpg-operator cnpg/cloudnative-pg --namespace ecommerce-saga
helm install keda kedacore/keda --namespace ecommerce-saga

kubectl apply -f deploy/k8s/base/postgres/cluster.yaml
kubectl create secret generic saga-postgres-order-svc-credentials -n ecommerce-saga \
  --from-literal=username=order_svc --from-literal=password=changeme   # NUNCA committar isto com valor real
kubectl apply -f deploy/k8s/base/postgres/init-databases-job.yaml
kubectl apply -f deploy/k8s/base/kafka/kafka-cluster.yaml

# imagens: o daemon Docker do minikube é separado do daemon do host —
# builde no host (docker compose já faz isso no dia a dia) e importe com
# `minikube image load ecommerce-saga-<serviço>:latest` para cada um dos 5
# (o `docker build` via `eval $(minikube docker-env)` NÃO funciona neste
# minikube — ver "O que ficou bloqueado" abaixo).

kubectl apply -f deploy/k8s/base/services/<serviço>.yaml   # um de cada vez, com o Job de migration antes
kubectl apply -f deploy/k8s/base/ingress.yaml
kubectl apply -f deploy/k8s/base/keda/
```

### O que foi PROVADO ao vivo nesta rodada

- **Operators via Helm**: Strimzi, CloudNativePG e KEDA sobem `Running` em minutos.
- **CloudNativePG**: `Cluster` de 1 instância fica `Cluster in healthy state` em segundos;
  o Job de init cria os outros 4 databases/roles (`payment_db`, `inventory_db`,
  `shipping_db`, `notification_db`) com sucesso, confirmado via `psql \l`.
- **Build de imagem para o minikube**: `docker build` clássico direto no daemon do
  minikube (`eval $(minikube docker-env)`) gera uma imagem "fantasma" — aparece em
  `docker images` mas `docker run` falha com "Unable to find image locally" (o runtime
  containerd experimental do minikube não populra o content store corretamente por esse
  caminho). O caminho que funciona: build no Docker do host (`docker compose build`,
  como já é feito no dia a dia) + `minikube image load <imagem>:latest` por serviço —
  confirmado com as 5 imagens de serviço.
- **`/health/ready` e `/health/startup`**: implementados e testados (Task 1 da Fase 10)
  nos 5 serviços de negócio — checam Postgres via `SELECT 1`; `saga-observer` devolve
  `{ status: 'ok' }` direto (sem dependência própria).

### O que ficou BLOQUEADO nesta rodada

**Strimzi Kafka (KRaft, nó único) não estabiliza neste minikube.** O broker/controller
combinado entra em crash loop recorrente (~span de 1–6 min de uptime, depois
`java.lang.RuntimeException: Received a fatal error while waiting for the controller to
acknowledge that we are caught up`, causado por `UnknownHostException`/`ECONNREFUSED` ao
tentar se auto-registrar via o próprio nome DNS do headless service
`saga-kafka-saga-pool-0.saga-kafka-kafka-brokers.ecommerce-saga.svc`). Tentativas feitas,
nesta ordem, todas commitadas nos manifests como a versão final usada:

1. `Kafka.spec.kafka.version` do plano original (3.9.0) não é suportado pelo operator
   Strimzi que o Helm instala hoje (1.2.0 — só aceita 4.2.x/4.3.x); corrigido para 4.3.1.
2. `spec.kafka.resources` migrou para `spec.resources` no `KafkaNodePool` nesta versão do
   Strimzi (Kafka CR rejeita o campo); corrigido.
3. Suba de CPU do node pool (1 → 2 CPU) — melhorou mas não eliminou os restarts.
4. Timeouts do quorum KRaft afrouxados (`controller.quorum.request/election/fetch.timeout.ms`)
   — mesmo resultado.
5. `dnsConfig.options.ndots: '2'` no template do pod, mirando a race de resolução DNS
   vista nos logs do CoreDNS (NXDOMAIN nos primeiros domínios de busca antes do FQDN
   correto responder) — mesmo resultado.

Uso real de CPU/memória do pod em todo esse tempo ficou bem abaixo dos limites (161m de
2000m, 401Mi de 2Gi) — ou seja, **não é falta de recursos**; é uma instabilidade de rede/DNS
mais fundamental deste minikube específico (driver Docker sobre o daemon do
[OrbStack](https://orbstack.dev/) desta máquina) com o auto-registro do controller KRaft
combinado em nó único desta versão do Strimzi. Como consequência em cascata, os 5 serviços
de negócio também entram em crash loop ao subir: o `OutboxRelayService`/consumer Kafka de
cada um falha ao conectar durante o `onModuleInit`, o que hoje derruba o bootstrap inteiro
do Nest (não é um problema de Postgres — Postgres respondeu normalmente o tempo todo nos
testes feitos à parte).

**Por isso, a Task 8 do plano (saga real via Ingress + resiliência a pod kill) não pôde
ser executada nesta rodada** — sem um broker Kafka estável não há saga para provar.
Helm (10b) e o teste de carga de 5k pedidos do KEDA já estavam fora de escopo desde o
início (ver Global Constraints do plano) e continuam não tentados.

**Próximo passo sugerido:** tentar uma versão mais antiga e mais amplamente testada do
Strimzi (ex. a família 0.4x, compatível com Kafka 3.x, como o plano original previa) em vez
da 1.2.0 mais recente puxada pelo Helm hoje, ou trocar o driver do minikube (`--driver=hyperkit`
ou testar fora do daemon do OrbStack) para isolar se a causa é o driver Docker específico
desta máquina.
