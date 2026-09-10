# 10 — Do Docker ao Kubernetes

## O problema

Tudo o que os nove módulos anteriores construíram assume um processo que sobe, consome e
fica de pé. Kubernetes quebra essa premissa de propósito: ele mata pods, reagenda,
escala e faz rolling update. Cada uma dessas ações atinge exatamente os pontos delicados
da saga.

## O que muda, e por quê

### Rolling update reproduz o crash do módulo 04

Todo deploy envia `SIGTERM` para pods que estão no meio de um handler. Sem graceful
shutdown, cada deploy vira o caso B do [módulo 04](04-entrega-e-commit-de-offset.md):
mensagens persistidas e não commitadas, reentregues duplicadas.

```yaml
terminationGracePeriodSeconds: 45
lifecycle:
  preStop:
    exec: { command: ['sleep', '5'] } # deixa o endpoint sair do Service antes
```

Casado, na aplicação, com: parar de buscar → drenar o que está em voo → commitar → fechar.

### As três probes respondem a perguntas diferentes

| Probe            | Pergunta               | Checa                                | Erro comum                                                |
| ---------------- | ---------------------- | ------------------------------------ | --------------------------------------------------------- |
| `startupProbe`   | já terminou de subir?  | rota simples, `failureThreshold: 30` | não ter uma, e a liveness matar o pod durante a migration |
| `readinessProbe` | posso receber tráfego? | Postgres **e** conexão Kafka         | —                                                         |
| `livenessProbe`  | o processo travou?     | só o event loop responde             | **checar Kafka aqui**                                     |

O erro do canto inferior direito é caro: Kafka pisca, a liveness falha em **todos** os
pods, Kubernetes mata todos, o cluster entra em crashloop, e você perde a manhã. Liveness
nunca checa dependência externa.

### Autoscaling por CPU é a métrica errada

Um consumidor com lag de 50 mil mensagens pode estar com 12% de CPU, esperando I/O. HPA
por CPU não escala. A métrica de saturação de um sistema event-driven é o **lag do
consumer group**:

```yaml
# KEDA ScaledObject
triggers:
  - type: kafka
    metadata:
      consumerGroup: inventory-service
      topic: ecommerce.payments.v1
      lagThreshold: '500'
```

Limite natural: escalar além do número de **partições** não adiciona throughput — os
consumidores excedentes ficam sem partição atribuída. Com 6 partições, `maxReplicaCount: 6`.

### Migration em Job, não em initContainer

Com 2+ réplicas, um initContainer roda a migration em paralelo em cada pod. Duas execuções
concorrentes de `prisma migrate deploy` sobre o mesmo banco é como se corrompe um schema.

```yaml
annotations:
  'helm.sh/hook': pre-upgrade,pre-install
  'helm.sh/hook-weight': '-5'
```

### Rebalance storm — e a mitigação que kafkajs NÃO tem

Cada pod que entra ou sai do grupo dispara rebalance. Com o protocolo **eager**, todos os
consumidores param, devolvem suas partições e recebem novas.

A recomendação que circula em toda documentação de Kafka é:

```
partition.assignment.strategy = cooperative-sticky
```

**Ela não se aplica aqui.** kafkajs 2.x implementa apenas o protocolo eager, e o único
assigner que ele traz é `roundRobin`. Não há `cooperative-sticky`, e também não há static
membership (`group.instance.id`) — os dois são do cliente Java e do librdkafka. Você pode
escrever um assigner customizado (kafkajs aceita `partitionAssigners`), mas rebalance
_incremental_ exige suporte de protocolo, que não existe no cliente.

Confira você mesmo:

```bash
node -e "console.log(Object.keys(require('kafkajs').PartitionAssigners))"
# [ 'roundRobin' ]
```

O que **de fato** funciona com kafkajs:

| Mitigação                                      | Por que ajuda                                                                                            |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `maxUnavailable: 0` e `maxSurge: 1`            | um pod por vez → N rebalances curtos em vez de um caos simultâneo                                        |
| **graceful shutdown** que sai do grupo         | saída limpa dispara `LeaveGroup` na hora; saída suja custa um `sessionTimeout` inteiro de consumo parado |
| `sessionTimeout` e `rebalanceTimeout` folgados | handler lento não é expulso do grupo no meio do batch                                                    |
| réplicas ≤ partições                           | consumidor sem partição atribuída só adiciona rebalance                                                  |

A segunda linha é a mais importante e a mais barata: um pod que morre sem sair do grupo
deixa o coordenador esperando o `sessionTimeout` (10–30s) antes de redistribuir as
partições dele. Multiplicado por 6 pods num rolling update, é minutos de lag acumulando.

Se rebalance incremental virar requisito duro, a saída é trocar de cliente
(`@confluentinc/kafka-javascript`, baseado em librdkafka) — e aí
[ADR-0004](../adr/0004-kafkajs-em-vez-de-nestjs-microservices.md) é substituído, não editado.

## O que muda na segurança

Em dev, Kafka em PLAINTEXT e Postgres com porta exposta são convenientes. Em cluster:

| Item               | Dev           | Kubernetes                                                                                                 |
| ------------------ | ------------- | ---------------------------------------------------------------------------------------------------------- |
| Kafka              | PLAINTEXT     | SASL/SCRAM-SHA-512 + TLS via Strimzi, `KafkaUser` com ACL por serviço (deny-by-default)                    |
| Postgres           | porta no host | ClusterIP, `NetworkPolicy` deny-all + allow explícito                                                      |
| Segredos           | `.env` local  | `Secret` via Sealed Secrets/SOPS — nunca `ConfigMap`, nunca `ARG` de Dockerfile (fica na camada da imagem) |
| Kafka UI / Grafana | porta no host | sem Ingress público                                                                                        |
| Imagem             | qualquer tag  | pinada por **digest**, `USER node`, Trivy sem HIGH/CRITICAL, SBOM por build                                |

`auto.create.topics.enable=false` vale nos dois: tópico que nasce de um typo é consumidor
esperando para sempre uma mensagem que está em outro lugar.

## A conta de RAM do Minikube

Kafka (~1GB) + 5 Postgres + 5 serviços + observabilidade não cabe em 4GB.

```bash
minikube start --memory=8192 --cpus=4 --disk-size=40g
minikube addons enable ingress metrics-server
```

Se apertar, o recuo é: 1 Postgres com 5 _databases_ e usuários distintos (nunca schema
compartilhado — ver [ADR-0005](../adr/0005-banco-por-servico.md)), 1 réplica por serviço,
Prometheus com retenção de 6h. Recuo é dívida: anote.

## Manifests crus antes de Helm

A Fase 10 do [plano](../PLAN.md) começa com YAML escrito à mão de propósito. Helm com
`{{ .Values.x }}` esconde justamente o que você precisa entender: qual campo do
`Deployment` controla o quê. Converta para chart **depois** de o YAML cru funcionar.

## Rode

Ainda não — Fases 9 e 10 do [plano](../PLAN.md). O que já roda hoje:

```bash
pnpm infra:up      # a mesma topologia, em Docker Compose
pnpm topics:create
pnpm ex todos
```

## O que quebra se você errar

Copiar `livenessProbe` e `readinessProbe` com o mesmo path e o mesmo check. É o padrão
mais comum em exemplo de tutorial, e transforma qualquer indisponibilidade momentânea de
Kafka ou Postgres num crashloop de todos os pods ao mesmo tempo.

## Leia também

- [Plano de execução](../PLAN.md) — Fases 9, 10 e 7b
- [ADR-0005 — Um banco por serviço](../adr/0005-banco-por-servico.md)
- Voltar ao [índice](README.md)
