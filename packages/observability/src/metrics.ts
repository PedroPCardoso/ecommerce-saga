import { Counter, Gauge, Histogram, Registry } from 'prom-client';

/**
 * UM registry compartilhado por processo — cada serviço expõe `GET /metrics`
 * lendo dele (ver `metrics.module.ts`). Nomes e labels aqui são o contrato
 * público consumido pelo `deploy/docker/prometheus/prometheus.yml` e pelo
 * dashboard do Grafana (Task 6) — não renomeie sem atualizar os dois.
 */
export const metricsRegistry = new Registry();

export const sagaDurationSeconds = new Histogram({
  name: 'saga_duration_seconds',
  help: 'Tempo entre a criação do pedido e ele chegar a um estado terminal (CONFIRMED/CANCELLED).',
  labelNames: ['outcome'] as const,
  buckets: [0.5, 1, 2, 5, 10, 30, 60, 120, 300],
  registers: [metricsRegistry],
});

export const sagaCompensationsTotal = new Counter({
  name: 'saga_compensations_total',
  help: 'Quantas compensações (payment.refunded/stock.released) foram aplicadas, por tipo.',
  labelNames: ['compensationType'] as const,
  registers: [metricsRegistry],
});

export const dlqMessagesTotal = new Counter({
  name: 'dlq_messages_total',
  help: 'Mensagens desviadas para a DLT, por tópico de origem e consumer group.',
  labelNames: ['topic', 'consumerGroup'] as const,
  registers: [metricsRegistry],
});

export const outboxLagSeconds = new Gauge({
  name: 'outbox_lag_seconds',
  help: 'Idade (segundos) da linha mais antiga ainda não publicada na tabela outbox, por serviço.',
  labelNames: ['service'] as const,
  registers: [metricsRegistry],
});

export const kafkaConsumerLag = new Gauge({
  name: 'kafka_consumer_lag',
  help: 'high watermark - offset commitado, por grupo/tópico/partição.',
  labelNames: ['group', 'topic', 'partition'] as const,
  registers: [metricsRegistry],
});
