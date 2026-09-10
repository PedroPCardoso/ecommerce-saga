import { describe, expect, it } from 'vitest';
import { ORDER_STATUS } from '@ecommerce/contracts';
import { applyEvent } from '../src/application/order-state-machine.js';

describe('OrderStateMachine.applyEvent', () => {
  it('caminho feliz completo: PENDING -> PAYMENT_APPROVED -> STOCK_RESERVED -> CONFIRMED', () => {
    const step1 = applyEvent(ORDER_STATUS.PENDING, 'payment.approved');
    expect(step1).toEqual({ changed: true, next: ORDER_STATUS.PAYMENT_APPROVED });

    const step2 = applyEvent(step1.next, 'stock.reserved');
    expect(step2).toEqual({ changed: true, next: ORDER_STATUS.STOCK_RESERVED });

    const step3 = applyEvent(step2.next, 'shipment.created');
    expect(step3).toEqual({ changed: true, next: ORDER_STATUS.CONFIRMED });
  });

  it('payment.failed a partir de PENDING vai direto para CANCELLED', () => {
    const result = applyEvent(ORDER_STATUS.PENDING, 'payment.failed');
    expect(result).toEqual({ changed: true, next: ORDER_STATUS.CANCELLED });
  });

  it('stock.unavailable a partir de PAYMENT_APPROVED vai para COMPENSATING (não fecha sozinho)', () => {
    const result = applyEvent(ORDER_STATUS.PAYMENT_APPROVED, 'stock.unavailable');
    expect(result).toEqual({ changed: true, next: ORDER_STATUS.COMPENSATING });
  });

  it('shipment.failed a partir de STOCK_RESERVED vai para COMPENSATING (não fecha sozinho)', () => {
    const result = applyEvent(ORDER_STATUS.STOCK_RESERVED, 'shipment.failed');
    expect(result).toEqual({ changed: true, next: ORDER_STATUS.COMPENSATING });
  });

  it('rejeita silenciosamente evento fora de ordem — stock.reserved chegando antes de payment.approved', () => {
    const result = applyEvent(ORDER_STATUS.PENDING, 'stock.reserved');
    expect(result).toEqual({
      changed: false,
      next: ORDER_STATUS.PENDING,
      reason: 'invalid-transition',
    });
  });

  it('rejeita silenciosamente reentrega tardia de payment.approved quando o pedido já avançou', () => {
    const result = applyEvent(ORDER_STATUS.STOCK_RESERVED, 'payment.approved');
    expect(result).toEqual({
      changed: false,
      next: ORDER_STATUS.STOCK_RESERVED,
      reason: 'invalid-transition',
    });
  });

  it('estados terminais (CONFIRMED, CANCELLED) não aceitam nenhuma transição — idempotência final', () => {
    expect(applyEvent(ORDER_STATUS.CONFIRMED, 'shipment.created')).toEqual({
      changed: false,
      next: ORDER_STATUS.CONFIRMED,
      reason: 'invalid-transition',
    });
    expect(applyEvent(ORDER_STATUS.CANCELLED, 'payment.approved')).toEqual({
      changed: false,
      next: ORDER_STATUS.CANCELLED,
      reason: 'invalid-transition',
    });
  });

  it('COMPENSATING não aceita nenhuma transição direta enquanto I4 (compensação) não existir', () => {
    const result = applyEvent(ORDER_STATUS.COMPENSATING, 'shipment.created');
    expect(result).toEqual({
      changed: false,
      next: ORDER_STATUS.COMPENSATING,
      reason: 'invalid-transition',
    });
  });
});
