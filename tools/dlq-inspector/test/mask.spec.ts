import { describe, expect, it } from 'vitest';
import { maskPii } from '../src/mask.js';

describe('maskPii', () => {
  it('mascara valores de string que parecem e-mail', () => {
    const result = maskPii({ customerEmail: 'joao@example.com', orderId: 'abc-123' });
    expect(result).toEqual({ customerEmail: '***@***', orderId: 'abc-123' });
  });

  it('mascara qualquer campo cujo NOME contenha token/secret/password/authorization', () => {
    const result = maskPii({
      gatewayToken: 'tok_test_abc123',
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

  it('mascara shippingAddress, customerId e items POR INTEIRO — não campo a campo', () => {
    const result = maskPii({
      orderId: 'abc-123',
      customerId: 'cust-456',
      shippingAddress: { street: 'Rua A', number: '100', city: 'São Paulo', zipCode: '01000-000' },
      items: [{ sku: 'BOOK-001', unitPriceCents: 2000 }],
    });
    expect(result).toEqual({
      orderId: 'abc-123',
      customerId: '***MASKED***',
      shippingAddress: '***MASKED***',
      items: '***MASKED***',
    });
  });

  it('mascara recursivamente objetos aninhados', () => {
    const result = maskPii({
      payload: { instrument: { gatewayToken: 'tok_test_1', cardLast4: '4242' } },
    });
    expect(result).toEqual({
      payload: { instrument: { gatewayToken: '***MASKED***', cardLast4: '4242' } },
    });
  });

  it('não mexe em valores que não são PII nem segredo', () => {
    const result = maskPii({ orderId: 'abc-123', amountCents: 2000, status: 'CONFIRMED' });
    expect(result).toEqual({ orderId: 'abc-123', amountCents: 2000, status: 'CONFIRMED' });
  });
});
