import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createEvent, orderEvents, TOPICS } from '@ecommerce/contracts';

describe('orderEvents.sagaTimedOut', () => {
  it('valida payload e usa o tópico de orders', () => {
    const orderId = randomUUID();
    const envelope = createEvent(orderEvents.sagaTimedOut, {
      aggregateId: orderId,
      correlationId: orderId,
      producer: 'order-service@0.1.0',
      payload: {
        orderId,
        stuckStatus: 'PAYMENT_APPROVED',
        timedOutAt: new Date().toISOString(),
      },
    });

    expect(envelope.eventType).toBe('saga.timeout');
    expect(orderEvents.sagaTimedOut.topic).toBe(TOPICS.orders);
  });
});
