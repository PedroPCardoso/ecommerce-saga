import { describe, expect, it } from 'vitest';
import { COMPENSATION_TYPE, compensationTypeSchema } from '../src/common.js';

describe('COMPENSATION_TYPE', () => {
  it('aceita PAYMENT_REFUNDED e STOCK_RELEASED, rejeita qualquer outro valor', () => {
    expect(compensationTypeSchema.parse('PAYMENT_REFUNDED')).toBe(COMPENSATION_TYPE.PAYMENT_REFUNDED);
    expect(compensationTypeSchema.parse('STOCK_RELEASED')).toBe(COMPENSATION_TYPE.STOCK_RELEASED);
    expect(() => compensationTypeSchema.parse('REFUND')).toThrow();
  });
});
