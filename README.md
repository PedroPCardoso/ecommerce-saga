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
                                                          ├── stock.unavailable ──► Payment ESTORNA ──► CANCELLED
                                                          └── stock.reserved ──► Shipping etiqueta
                                                                                     ├── shipment.failed ──► Payment ESTORNA
                                                                                     │                    └► Inventory LIBERA ──► CANCELLED
                                                                                     └── shipment.created ──► CONFIRMED
```

Ninguém comanda. Cada serviço reage a eventos e publica o que aconteceu no seu domínio.

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
docs/architecture/     modelo C4 em Structurizr DSL (1 modelo → 4 níveis)
docs/PLAN.md           plano de execução completo, 13 fases

examples/              6 exemplos executáveis contra Kafka e Postgres reais

packages/contracts/    fonte ÚNICA dos contratos de evento e da topologia Kafka
packages/kafka/        consumo com commit manual, retry escalonado, DLT      (Fase 2)
packages/outbox/       Transactional Outbox + relay                          (Fase 2)
packages/idempotency/  tabela de inbox + decorator @Idempotent               (Fase 2)
apps/                  os 5 serviços                                     (Fases 1 a 5)
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
- [ ] **Fase 1** — Order Service: HTTP + outbox
- [ ] **Fase 2** — `packages/kafka` e `packages/idempotency`
- [ ] **Fases 3–5** — Payment, Inventory, Shipping, Notification
- [ ] **Fase 6** — resiliência, caos, replay, `dlq-inspector`
- [ ] **Fase 7** — observabilidade · **7b** — Saga Observer (a UI ligada ao sistema real)
- [ ] **Fase 8** — C4 nível 3
- [ ] **Fase 9** — Docker de produção · **Fase 10** — Kubernetes / Minikube
- [ ] **Fase 11** — versão orquestrada, para comparação (opcional)

Os padrões dos módulos 03 a 09 já rodam de verdade nos exemplos. O que falta é montá-los
dentro dos cinco serviços, que é o que as Fases 1 a 5 do [plano](docs/PLAN.md) descrevem.

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
