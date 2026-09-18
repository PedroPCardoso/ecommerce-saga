import { describe, expect, it } from 'vitest';
import { CANCELLATION_REASON, COMPENSATION_TYPE, ORDER_STATUS, type CompensationType } from '@ecommerce/contracts';
import { applyCompensationEvent, applyEvent } from '../src/application/order-state-machine.js';

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

  it('evento PREMATURO — stock.reserved chegando antes de payment.approved — é retriável, não silenciosamente descartado', () => {
    const result = applyEvent(ORDER_STATUS.PENDING, 'stock.reserved');
    expect(result).toEqual({
      changed: false,
      next: ORDER_STATUS.PENDING,
      reason: 'premature',
    });
  });

  it('evento PREMATURO — shipment.created chegando antes de stock.reserved (pedido ainda em PAYMENT_APPROVED)', () => {
    const result = applyEvent(ORDER_STATUS.PAYMENT_APPROVED, 'shipment.created');
    expect(result).toEqual({
      changed: false,
      next: ORDER_STATUS.PAYMENT_APPROVED,
      reason: 'premature',
    });
  });

  it('evento OBSOLETO (stale) — reentrega tardia de payment.approved quando o pedido já avançou — seguro ignorar', () => {
    const result = applyEvent(ORDER_STATUS.STOCK_RESERVED, 'payment.approved');
    expect(result).toEqual({
      changed: false,
      next: ORDER_STATUS.STOCK_RESERVED,
      reason: 'stale',
    });
  });

  it('estados terminais (CONFIRMED, CANCELLED) não aceitam nenhuma transição — sempre stale, nunca retriável', () => {
    expect(applyEvent(ORDER_STATUS.CONFIRMED, 'shipment.created')).toEqual({
      changed: false,
      next: ORDER_STATUS.CONFIRMED,
      reason: 'stale',
    });
    expect(applyEvent(ORDER_STATUS.CANCELLED, 'payment.approved')).toEqual({
      changed: false,
      next: ORDER_STATUS.CANCELLED,
      reason: 'stale',
    });
  });

  it('COMPENSATING não aceita nenhuma transição direta enquanto I4 (compensação) não existir — stale, não retriável', () => {
    const result = applyEvent(ORDER_STATUS.COMPENSATING, 'shipment.created');
    expect(result).toEqual({
      changed: false,
      next: ORDER_STATUS.COMPENSATING,
      reason: 'stale',
    });
  });
});

describe('applyCompensationEvent', () => {
  it('stock.unavailable + payment.refunded fecha em CANCELLED (só uma compensação exigida)', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.STOCK_UNAVAILABLE,
      compensationsReceived: [] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({
      changed: true,
      next: ORDER_STATUS.CANCELLED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
    });
  });

  it('shipment.failed + só payment.refunded NÃO fecha ainda — falta stock.released', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.SHIPMENT_FAILED,
      compensationsReceived: [] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({
      changed: true,
      next: ORDER_STATUS.COMPENSATING,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
    });
  });

  it('shipment.failed + payment.refunded já recebido + stock.released chegando agora fecha em CANCELLED', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.SHIPMENT_FAILED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'stock.released');

    expect(result).toEqual({
      changed: true,
      next: ORDER_STATUS.CANCELLED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED, COMPENSATION_TYPE.STOCK_RELEASED],
    });
  });

  it('compensação repetida (mesmo tipo já recebido) é stale — ignora sem regredir', () => {
    const order = {
      status: ORDER_STATUS.COMPENSATING,
      compensationReason: CANCELLATION_REASON.SHIPMENT_FAILED,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({ changed: false, reason: 'stale' });
  });

  it('payment.refunded chegando ANTES de o pedido entrar em COMPENSATING é premature — retriável', () => {
    const order = {
      status: ORDER_STATUS.PAYMENT_APPROVED,
      compensationReason: null,
      compensationsReceived: [] as CompensationType[],
    };

    const result = applyCompensationEvent(order, 'payment.refunded');

    expect(result).toEqual({ changed: false, reason: 'premature' });
  });

  it('compensação chegando depois de o pedido já ter fechado (CONFIRMED/CANCELLED) é stale', () => {
    const confirmed = {
      status: ORDER_STATUS.CONFIRMED,
      compensationReason: null,
      compensationsReceived: [] as CompensationType[],
    };
    expect(applyCompensationEvent(confirmed, 'payment.refunded')).toEqual({
      changed: false,
      reason: 'stale',
    });

    const cancelled = {
      status: ORDER_STATUS.CANCELLED,
      compensationReason: CANCELLATION_REASON.STOCK_UNAVAILABLE,
      compensationsReceived: [COMPENSATION_TYPE.PAYMENT_REFUNDED] as CompensationType[],
    };
    expect(applyCompensationEvent(cancelled, 'payment.refunded')).toEqual({
      changed: false,
      reason: 'stale',
    });
  });
});
