import { describe, expect, it } from 'vitest';
import { applyExecutorResponse } from '../src/application/orchestrator-state-machine.js';

describe('applyExecutorResponse (Fase 11 — TDD puro, sem infra)', () => {
  it('caminho feliz completo: payment -> stock -> shipment -> confirmado', () => {
    let status = applyExecutorResponse('AWAITING_PAYMENT', 'payment', 'success');
    expect(status).toBe('AWAITING_STOCK');

    status = applyExecutorResponse(status, 'inventory', 'success');
    expect(status).toBe('AWAITING_SHIPMENT');

    status = applyExecutorResponse(status, 'shipping', 'success');
    expect(status).toBe('CONFIRMED');
  });

  it('falha no pagamento cancela direto — nada foi efetivado ainda', () => {
    const status = applyExecutorResponse('AWAITING_PAYMENT', 'payment', 'failure');
    expect(status).toBe('CANCELLED');
  });

  it('falha na reserva de estoque cancela a partir de AWAITING_STOCK', () => {
    const status = applyExecutorResponse('AWAITING_STOCK', 'inventory', 'failure');
    expect(status).toBe('CANCELLED');
  });

  it('falha no envio cancela a partir de AWAITING_SHIPMENT', () => {
    const status = applyExecutorResponse('AWAITING_SHIPMENT', 'shipping', 'failure');
    expect(status).toBe('CANCELLED');
  });

  it('resposta fora de ordem/duplicada é ignorada — devolve o status atual sem mudar', () => {
    // payment já respondeu (current já avançou para AWAITING_STOCK); uma reentrega
    // do sucesso de payment não deve fazer a máquina regredir nem travar.
    const status = applyExecutorResponse('AWAITING_STOCK', 'payment', 'success');
    expect(status).toBe('AWAITING_STOCK');
  });
});
