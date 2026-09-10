import { describe, expect, it } from 'vitest';
import {
  createEvent,
  orderEvents,
  parseAs,
  parseEvent,
  UnprocessableEventError,
} from '../src/index.js';

const validPayload = {
  orderId: '018f3f4e-0000-7000-8000-000000000001',
  customerId: '018f3f4e-0000-7000-8000-000000000002',
  items: [{ sku: 'BOOK-001', name: 'Livro', quantity: 2, unitPriceCents: 4990 }],
  totalAmountCents: 9980,
  currency: 'BRL' as const,
  shippingAddress: {
    street: 'Rua Exemplo',
    number: '100',
    district: 'Centro',
    city: 'São Paulo',
    state: 'SP',
    zipCode: '01000-000',
    country: 'BR',
  },
};

describe('createEvent', () => {
  it('monta um envelope completo e válido', () => {
    const event = createEvent(orderEvents.orderCreated, {
      aggregateId: validPayload.orderId,
      payload: validPayload,
      correlationId: 'corr-1',
      producer: 'order-service@0.1.0',
    });

    expect(event.eventType).toBe('order.created');
    expect(event.eventVersion).toBe(1);
    expect(event.aggregateId).toBe(validPayload.orderId);
    expect(event.aggregateType).toBe('order');
    expect(() => new Date(event.occurredAt)).not.toThrow();
  });

  it('usa o próprio eventId como causationId quando não há evento anterior (raiz da saga)', () => {
    const event = createEvent(orderEvents.orderCreated, {
      aggregateId: validPayload.orderId,
      payload: validPayload,
      correlationId: 'corr-1',
      producer: 'order-service@0.1.0',
    });

    expect(event.causationId).toBe(event.eventId);
  });

  it('preserva o causationId recebido, montando a árvore causal', () => {
    const event = createEvent(orderEvents.orderCreated, {
      aggregateId: validPayload.orderId,
      payload: validPayload,
      correlationId: 'corr-1',
      causationId: 'evento-anterior',
      producer: 'order-service@0.1.0',
    });

    expect(event.causationId).toBe('evento-anterior');
    expect(event.causationId).not.toBe(event.eventId);
  });

  it('gera eventIds monotônicos (UUID v7 ordena por tempo)', () => {
    const ids = Array.from(
      { length: 5 },
      () =>
        createEvent(orderEvents.orderCreated, {
          aggregateId: validPayload.orderId,
          payload: validPayload,
          correlationId: 'corr-1',
          producer: 'order-service@0.1.0',
        }).eventId,
    );

    expect([...ids].sort()).toEqual(ids);
  });

  it('recusa publicar payload inválido — falha no produtor, não no consumidor', () => {
    expect(() =>
      createEvent(orderEvents.orderCreated, {
        aggregateId: validPayload.orderId,
        payload: { ...validPayload, items: [] },
        correlationId: 'corr-1',
        producer: 'order-service@0.1.0',
      }),
    ).toThrow();
  });

  it('recusa dinheiro em float: centavos são inteiros', () => {
    expect(() =>
      createEvent(orderEvents.orderCreated, {
        aggregateId: validPayload.orderId,
        payload: { ...validPayload, totalAmountCents: 99.8 },
        correlationId: 'corr-1',
        producer: 'order-service@0.1.0',
      }),
    ).toThrow(/inteiro/);
  });
});

describe('parseEvent — mensagem do broker é entrada não confiável', () => {
  const valid = createEvent(orderEvents.orderCreated, {
    aggregateId: validPayload.orderId,
    payload: validPayload,
    correlationId: 'corr-1',
    producer: 'order-service@0.1.0',
  });

  it('aceita evento conhecido e devolve a definição', () => {
    const { definition } = parseEvent(JSON.parse(JSON.stringify(valid)));
    expect(definition.type).toBe('order.created');
    expect(definition.topic).toBe('ecommerce.orders.v1');
  });

  it('rejeita tipo desconhecido como erro PERMANENTE (vai direto para a DLT)', () => {
    const error = (() => {
      try {
        parseEvent({ ...valid, eventType: 'order.teleported' });
      } catch (caught) {
        return caught;
      }
      return undefined;
    })();

    expect(error).toBeInstanceOf(UnprocessableEventError);
    expect((error as UnprocessableEventError).permanent).toBe(true);
  });

  it('rejeita versão desconhecida em vez de adivinhar o formato', () => {
    expect(() => parseEvent({ ...valid, eventVersion: 2 })).toThrow(UnprocessableEventError);
  });

  it('rejeita payload que viola o schema', () => {
    expect(() => parseEvent({ ...valid, payload: { orderId: 'nao-e-uuid' } })).toThrow(
      UnprocessableEventError,
    );
  });

  it('rejeita mensagem sem os campos de rastreio da saga', () => {
    const { correlationId: _dropped, ...semCorrelation } = valid;
    expect(() => parseEvent(semCorrelation)).toThrow(/envelope/);
  });
});

describe('parseAs', () => {
  it('recusa um evento correto mas do tipo errado para aquele handler', () => {
    const event = createEvent(orderEvents.orderCreated, {
      aggregateId: validPayload.orderId,
      payload: validPayload,
      correlationId: 'corr-1',
      producer: 'order-service@0.1.0',
    });

    expect(() => parseAs(orderEvents.orderConfirmed, event)).toThrow(UnprocessableEventError);
  });
});
