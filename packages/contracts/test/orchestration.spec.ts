import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createEvent, orchestrationEvents, TOPICS } from '@ecommerce/contracts';

describe('orchestrationEvents (Fase 11 — harness de comparação)', () => {
  it('authorizePaymentCommand valida payload e usa o tópico de comandos de pagamento', () => {
    const orchestratedOrderId = randomUUID();
    const envelope = createEvent(orchestrationEvents.authorizePaymentCommand, {
      aggregateId: orchestratedOrderId,
      correlationId: orchestratedOrderId,
      producer: 'saga-orchestrator-service@0.1.0',
      payload: {
        orchestratedOrderId,
        amountCents: 5_000,
        currency: 'BRL',
      },
    });

    expect(envelope.eventType).toBe('command.payment.authorize');
    expect(orchestrationEvents.authorizePaymentCommand.topic).toBe(TOPICS.commandsPayment);
  });

  it('reserveStockCommand valida payload e usa o tópico de comandos de estoque', () => {
    const orchestratedOrderId = randomUUID();
    const envelope = createEvent(orchestrationEvents.reserveStockCommand, {
      aggregateId: orchestratedOrderId,
      correlationId: orchestratedOrderId,
      producer: 'saga-orchestrator-service@0.1.0',
      payload: {
        orchestratedOrderId,
        items: [{ sku: 'SKU-1', quantity: 2 }],
      },
    });

    expect(envelope.eventType).toBe('command.inventory.reserve-stock');
    expect(orchestrationEvents.reserveStockCommand.topic).toBe(TOPICS.commandsInventory);
  });

  it('createShipmentCommand valida payload e usa o tópico de comandos de envio', () => {
    const orchestratedOrderId = randomUUID();
    const envelope = createEvent(orchestrationEvents.createShipmentCommand, {
      aggregateId: orchestratedOrderId,
      correlationId: orchestratedOrderId,
      producer: 'saga-orchestrator-service@0.1.0',
      payload: {
        orchestratedOrderId,
        address: {
          street: 'Rua Teste',
          number: '100',
          district: 'Centro',
          city: 'São Paulo',
          state: 'SP',
          zipCode: '01000-000',
          country: 'BR',
        },
      },
    });

    expect(envelope.eventType).toBe('command.shipping.create-shipment');
    expect(orchestrationEvents.createShipmentCommand.topic).toBe(TOPICS.commandsShipping);
  });

  it('executorResponded valida payload e usa o tópico de respostas do orquestrador', () => {
    const orchestratedOrderId = randomUUID();
    const envelope = createEvent(orchestrationEvents.executorResponded, {
      aggregateId: orchestratedOrderId,
      correlationId: orchestratedOrderId,
      producer: 'payment-executor@0.1.0',
      payload: {
        orchestratedOrderId,
        step: 'payment',
        outcome: 'failure',
        reason: 'Cartão recusado pelo emissor (simulação determinística)',
      },
    });

    expect(envelope.eventType).toBe('orchestrator.executor-responded');
    expect(orchestrationEvents.executorResponded.topic).toBe(TOPICS.responsesOrchestrator);
  });
});
