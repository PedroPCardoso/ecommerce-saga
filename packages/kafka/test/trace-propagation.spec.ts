import { context, trace } from '@opentelemetry/api';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { EventProducer } from '../src/producer.js';

describe('propagação de traceparent', () => {
  let provider: NodeTracerProvider;

  beforeEach(() => {
    provider = new NodeTracerProvider();
    provider.register();
  });

  afterEach(async () => {
    await provider.shutdown();
  });

  it('EventProducer.publish injeta o header traceparent quando há um span ativo', async () => {
    const tracer = trace.getTracer('test');
    const span = tracer.startSpan('test-span');
    const activeContext = trace.setSpan(context.active(), span);

    const producer = new EventProducer({ brokers: ['localhost:1'], clientId: 'test' });
    // @ts-expect-error acessa o campo privado só para o teste poder inspecionar sem
    // precisar de um broker de verdade — publish() chama producer.send internamente.
    producer.producer = { send: vi.fn().mockResolvedValue(undefined) };

    await context.with(activeContext, async () => {
      await producer.publish('test-topic', {
        eventId: 'evt-1',
        eventType: 'test.event',
        eventVersion: 1,
        occurredAt: new Date().toISOString(),
        aggregateId: 'agg-1',
        aggregateType: 'test',
        correlationId: 'agg-1',
        causationId: 'evt-1',
        producer: 'test@0.0.0',
        payload: {},
      } as never);
    });

    span.end();

    // @ts-expect-error mesmo acesso ao mock acima
    const sendCall = producer.producer.send.mock.calls[0][0];
    expect(sendCall.messages[0].headers.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  });
});
