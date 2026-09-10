# Caminho de aprendizado

Dez módulos, em ordem. Cada um tem a mesma estrutura:

1. **O problema** — a falha concreta que existe sem o mecanismo
2. **O mecanismo** — como ele resolve, com diagrama
3. **No código** — onde isso vive neste repositório, com arquivo e linha
4. **Rode** — o exemplo executável que prova o comportamento
5. **O que quebra se você errar** — o modo de falha específico
6. **Leia também** — o ADR com a decisão e suas consequências

Não pule os exemplos. Eles rodam contra o Kafka e o Postgres de verdade, falham com
exit code diferente de zero se o comportamento não for o esperado, e mostram coisas
que nenhum texto convence: uma mensagem sumindo, um saldo dobrando, um relay
publicando 60 vezes o que deveria publicar 30.

## Antes de começar

```bash
nvm use && pnpm install
cp .env.example .env
pnpm infra:up          # Kafka, 5x Postgres, Jaeger, Prometheus, Grafana, Mailhog, Structurizr
pnpm topics:create
```

E abra o [simulador](../simulator/saga-console.html) numa aba. Ele executa a saga
inteira no navegador e serve de mapa enquanto você lê.

## Os módulos

| #   | Módulo                                                            | Pergunta que ele responde                           | Exemplo      |
| --- | ----------------------------------------------------------------- | --------------------------------------------------- | ------------ |
| 01  | [Por que SAGA](01-por-que-saga.md)                                | Como manter consistência sem transação distribuída? | —            |
| 02  | [Coreografia vs. orquestração](02-coreografia-vs-orquestracao.md) | Quem coordena a saga?                               | simulador    |
| 03  | [Partições, chave e ordenação](03-particoes-chave-e-ordenacao.md) | Por que `orderId` é a chave de toda mensagem?       | `pnpm ex 01` |
| 04  | [Entrega e commit de offset](04-entrega-e-commit-de-offset.md)    | Por que `autoCommit: false` não é negociável?       | `pnpm ex 02` |
| 05  | [Transactional Outbox](05-outbox.md)                              | Como publicar e persistir atomicamente?             | `pnpm ex 03` |
| 06  | [Idempotência](06-idempotencia.md)                                | Como não cobrar o cliente duas vezes?               | `pnpm ex 04` |
| 07  | [Retry e DLT](07-retry-e-dlt.md)                                  | O que fazer quando o handler falha?                 | `pnpm ex 05` |
| 08  | [Compensação](08-compensacao.md)                                  | Como desfazer o que já foi efetivado?               | simulador    |
| 09  | [Replay e evolução de schema](09-replay-e-evolucao.md)            | Como reprocessar o passado com segurança?           | `pnpm ex 06` |
| 10  | [Do Docker ao Kubernetes](10-do-docker-ao-kubernetes.md)          | O que muda quando isto vai para um cluster?         | —            |

## Os três recursos, e para que serve cada um

| Recurso                                         | Serve para                                                 | Não serve para                                   |
| ----------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------ |
| **Estes módulos**                               | entender o _porquê_ de cada decisão                        | ver funcionando                                  |
| **[Simulador](../simulator/saga-console.html)** | ver o fluxo inteiro e o estado de cada tabela a cada passo | ver falha real de infraestrutura                 |
| **[Exemplos](../../examples/)**                 | provar o comportamento contra Kafka e Postgres reais       | ver a saga completa (eles isolam um padrão cada) |

Os três se complementam de propósito: o simulador mostra o **todo** sem ser real, os
exemplos mostram o **real** sem ser o todo, e os módulos explicam por que as duas
coisas são assim.
