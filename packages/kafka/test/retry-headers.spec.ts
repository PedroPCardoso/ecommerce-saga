import { describe, expect, it } from 'vitest';
import { RETRY_HEADERS } from '@ecommerce/contracts';
import { buildRedirectHeaders, readRetryCount } from '../src/index.js';

describe('readRetryCount', () => {
  it('devolve 0 quando o header não existe', () => {
    expect(readRetryCount({})).toBe(0);
  });

  it('lê o valor do header x-retry-count', () => {
    expect(readRetryCount({ [RETRY_HEADERS.retryCount]: '2' })).toBe(2);
  });
});

describe('buildRedirectHeaders', () => {
  it('monta todos os headers obrigatórios sem vazar o stacktrace inteiro', () => {
    const headers = buildRedirectHeaders({
      originalTopic: 'ecommerce.orders.v1',
      originalPartition: 0,
      originalOffset: '42',
      retryCount: 1,
      firstFailureAt: '2026-09-09T00:00:00.000Z',
      error: new Error('ETIMEDOUT ao chamar gateway'),
      consumerGroup: 'payment-service',
    });

    expect(headers[RETRY_HEADERS.originalTopic]).toBe('ecommerce.orders.v1');
    expect(headers[RETRY_HEADERS.retryCount]).toBe('1');
    expect(headers[RETRY_HEADERS.lastError]).toBe('ETIMEDOUT ao chamar gateway');
    expect(headers[RETRY_HEADERS.stacktraceHash]).toMatch(/^sha256:[0-9a-f]{8}$/);
    expect(headers[RETRY_HEADERS.stacktraceHash]).not.toContain('at ');
  });
});
