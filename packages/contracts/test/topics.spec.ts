import { describe, expect, it } from 'vitest';
import {
  ALL_BUSINESS_TOPICS,
  CONSUMER_GROUPS,
  MAX_RETRY_ATTEMPTS,
  RETRY_LADDER,
  SUBSCRIPTIONS,
  TOPICS,
  allTopics,
  deadLetterTopic,
  retryTopic,
} from '../src/index.js';

describe('topologia', () => {
  it('nomeia o tópico de retry por (origem, consumer group, degrau)', () => {
    expect(retryTopic(TOPICS.payments, CONSUMER_GROUPS.inventory, 0)).toBe(
      'ecommerce.payments.v1.inventory-service.retry-5s',
    );
    expect(retryTopic(TOPICS.payments, CONSUMER_GROUPS.inventory, 2)).toBe(
      'ecommerce.payments.v1.inventory-service.retry-10m',
    );
  });

  it('estoura ao pedir um degrau além da escada — nesse ponto o destino é a DLT', () => {
    expect(() =>
      retryTopic(TOPICS.payments, CONSUMER_GROUPS.inventory, MAX_RETRY_ATTEMPTS),
    ).toThrow(/DLT/);
  });

  it('nomeia a DLT por (origem, consumer group)', () => {
    expect(deadLetterTopic(TOPICS.orders, CONSUMER_GROUPS.payment)).toBe(
      'ecommerce.orders.v1.payment-service.DLT',
    );
  });

  it('a escada de atrasos é crescente', () => {
    const delays = RETRY_LADDER.map((step) => step.delayMs);
    expect([...delays].sort((a, b) => a - b)).toEqual(delays);
  });

  it('deriva a topologia inteira: negócio + retry + DLT, sem duplicata', () => {
    const topics = allTopics();
    const subscriptionCount = Object.values(SUBSCRIPTIONS).reduce(
      (total, sources) => total + sources.length,
      0,
    );

    expect(new Set(topics).size).toBe(topics.length);
    expect(topics).toHaveLength(
      ALL_BUSINESS_TOPICS.length + subscriptionCount * (MAX_RETRY_ATTEMPTS + 1),
    );
    expect(topics).toContain(TOPICS.orders);
  });
});

describe('acoplamento implícito da coreografia', () => {
  it('payment-service assina inventory e shipping — domínios que não são dele', () => {
    const paymentSubscriptions = SUBSCRIPTIONS[CONSUMER_GROUPS.payment];

    expect(paymentSubscriptions).toContain(TOPICS.inventory);
    expect(paymentSubscriptions).toContain(TOPICS.shipping);
  });

  it('nenhum consumer group assina o próprio tópico de saída (evita laço)', () => {
    const ownTopic: Record<string, string> = {
      [CONSUMER_GROUPS.payment]: TOPICS.payments,
      [CONSUMER_GROUPS.inventory]: TOPICS.inventory,
      [CONSUMER_GROUPS.shipping]: TOPICS.shipping,
    };

    for (const [group, topic] of Object.entries(ownTopic)) {
      expect(SUBSCRIPTIONS[group as keyof typeof SUBSCRIPTIONS]).not.toContain(topic);
    }
  });
});
