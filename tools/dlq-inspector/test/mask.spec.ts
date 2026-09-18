import { describe, expect, it } from 'vitest';
import { maskPii } from '../src/mask.js';

describe('maskPii', () => {
  it('mascara valores de string que parecem e-mail', () => {
    const result = maskPii({ customerEmail: 'joao@example.com', orderId: 'abc-123' });
    expect(result).toEqual({ customerEmail: '***@***', orderId: 'abc-123' });
  });

  it('mascara qualquer campo cujo NOME contenha token/secret/password/authorization', () => {
    const result = maskPii({
      gatewayToken: 'tok_live_abc123',
      apiSecret: 'shh',
      password: 'hunter2',
      Authorization: 'Bearer xyz',
      orderId: 'abc-123',
    });
    expect(result).toEqual({
      gatewayToken: '***MASKED***',
      apiSecret: '***MASKED***',
      password: '***MASKED***',
      Authorization: '***MASKED***',
      orderId: 'abc-123',
    });
  });

  it('mascara recursivamente objetos aninhados e arrays', () => {
    const result = maskPii({
      payload: { instrument: { gatewayToken: 'tok_1', cardLast4: '4242' } },
      items: [{ sku: 'BOOK-001', customerEmail: 'a@b.com' }],
    });
    expect(result).toEqual({
      payload: { instrument: { gatewayToken: '***MASKED***', cardLast4: '4242' } },
      items: [{ sku: 'BOOK-001', customerEmail: '***@***' }],
    });
  });

  it('não mexe em valores que não são PII nem segredo', () => {
    const result = maskPii({ orderId: 'abc-123', amountCents: 2000, status: 'CONFIRMED' });
    expect(result).toEqual({ orderId: 'abc-123', amountCents: 2000, status: 'CONFIRMED' });
  });
});
