# 0003 — Kafka como broker de eventos

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

A saga precisa de entrega confiável entre cinco serviços, com ordenação por pedido e capacidade de reprocessar histórico.

## Decisão

Apache Kafka 3.9 em modo KRaft (sem Zookeeper). Em Kubernetes, operado pelo Strimzi.

## Consequências

**Positivas**

- **Log retido** em vez de fila consumida: reprocessar do offset zero é uma operação normal, não uma cirurgia. É o que permite validar que o estado final é reconstruível.
- Ordenação garantida por partição — combinada com `orderId` como chave (ADR-0009), dá ordenação por pedido.
- Consumer groups dão paralelismo e failover sem lógica própria.
- **Lag de consumer group** é uma métrica de saturação honesta, e serve de sinal para o KEDA escalar (ADR na Fase 10).

**Negativas**

- Operação bem mais pesada que RabbitMQ: KRaft, partições, ISR, retenção, rebalance. Em Kubernetes exige um operator.
- Kafka **não tem retry/DLQ nativo** como o RabbitMQ tem com DLX. Toda a escada de retry é construída à mão (ADR-0008).
- Rebalance mal configurado transforma deploy em tempestade. E a mitigação usual —
  `cooperative-sticky` — **não está disponível** no cliente escolhido: kafkajs 2.x só
  implementa o protocolo eager, com `roundRobin` como único assigner. Ver a consequência
  registrada em [ADR-0004](0004-kafkajs-em-vez-de-nestjs-microservices.md).
- Consome bastante RAM — sensível no Minikube.

## Alternativas consideradas

- **RabbitMQ:** DLX e TTL nativos tornariam retry/DLQ triviais, e o setup é muito mais leve. Descartado porque não oferece particionamento, consumer groups nem replay — exatamente os três assuntos que este projeto existe para estudar.
- **Os dois atrás de uma abstração de porta:** mais elegante no papel, mas esconderia as particularidades de cada broker, que são o conteúdo.
