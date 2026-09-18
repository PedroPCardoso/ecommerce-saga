import { describe, expect, it } from 'vitest';
import {
  dlqMessagesTotal,
  kafkaConsumerLag,
  metricsRegistry,
  outboxLagSeconds,
  sagaCompensationsTotal,
  sagaDurationSeconds,
} from '../src/metrics.js';

describe('métricas de negócio', () => {
  it('saga_duration_seconds aparece no output do registry depois de observar um valor', async () => {
    sagaDurationSeconds.observe({ outcome: 'confirmed' }, 12.5);
    const output = await metricsRegistry.metrics();
    expect(output).toContain('saga_duration_seconds');
    expect(output).toContain('outcome="confirmed"');
  });

  it('saga_compensations_total incrementa por tipo de compensação', async () => {
    sagaCompensationsTotal.inc({ compensationType: 'PAYMENT_REFUNDED' });
    const output = await metricsRegistry.metrics();
    expect(output).toContain('saga_compensations_total');
    expect(output).toContain('compensationType="PAYMENT_REFUNDED"');
  });

  it('dlq_messages_total incrementa por tópico e consumer group', async () => {
    dlqMessagesTotal.inc({ topic: 'ecommerce.payments.v1', consumerGroup: 'inventory-service' });
    const output = await metricsRegistry.metrics();
    expect(output).toContain('dlq_messages_total');
  });

  it('outbox_lag_seconds e kafka_consumer_lag aceitam .set()', async () => {
    outboxLagSeconds.set({ service: 'order-service' }, 0.42);
    kafkaConsumerLag.set({ group: 'payment-service', topic: 'ecommerce.orders.v1', partition: '0' }, 3);
    const output = await metricsRegistry.metrics();
    expect(output).toContain('outbox_lag_seconds');
    expect(output).toContain('kafka_consumer_lag');
  });
});
