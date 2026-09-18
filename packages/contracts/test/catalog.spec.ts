import { describe, expect, it } from 'vitest';
import { COMPENSATION_MATRIX, EVENT_DEFINITIONS, KNOWN_EVENT_TYPES } from '../src/index.js';

/**
 * Teste de contrato: qualquer mudança no catálogo de eventos precisa passar por aqui.
 *
 * Renomear um evento, mudar o tópico de destino ou publicar uma v2 sem atualizar este
 * snapshot QUEBRA O BUILD. É o único jeito de impedir que um serviço mude um contrato
 * que outros quatro consomem sem que ninguém perceba na revisão.
 */
describe('catálogo de eventos (contrato)', () => {
  it('bate com o snapshot — se falhou, foi mudança de contrato: revise e atualize de propósito', () => {
    const catalog = EVENT_DEFINITIONS.map(({ type, version, topic, aggregateType }) => ({
      type,
      version,
      topic,
      aggregateType,
    }));

    expect(catalog).toMatchInlineSnapshot(`
      [
        {
          "aggregateType": "orchestratedOrder",
          "topic": "ecommerce.commands.inventory.v1",
          "type": "command.inventory.reserve-stock",
          "version": 1,
        },
        {
          "aggregateType": "orchestratedOrder",
          "topic": "ecommerce.commands.payment.v1",
          "type": "command.payment.authorize",
          "version": 1,
        },
        {
          "aggregateType": "orchestratedOrder",
          "topic": "ecommerce.commands.shipping.v1",
          "type": "command.shipping.create-shipment",
          "version": 1,
        },
        {
          "aggregateType": "orchestratedOrder",
          "topic": "ecommerce.responses.orchestrator.v1",
          "type": "orchestrator.executor-responded",
          "version": 1,
        },
        {
          "aggregateType": "order",
          "topic": "ecommerce.orders.v1",
          "type": "order.cancelled",
          "version": 1,
        },
        {
          "aggregateType": "order",
          "topic": "ecommerce.orders.v1",
          "type": "order.confirmed",
          "version": 1,
        },
        {
          "aggregateType": "order",
          "topic": "ecommerce.orders.v1",
          "type": "order.created",
          "version": 1,
        },
        {
          "aggregateType": "payment",
          "topic": "ecommerce.payments.v1",
          "type": "payment.approved",
          "version": 1,
        },
        {
          "aggregateType": "payment",
          "topic": "ecommerce.payments.v1",
          "type": "payment.failed",
          "version": 1,
        },
        {
          "aggregateType": "payment",
          "topic": "ecommerce.payments.v1",
          "type": "payment.refunded",
          "version": 1,
        },
        {
          "aggregateType": "order",
          "topic": "ecommerce.orders.v1",
          "type": "saga.timeout",
          "version": 1,
        },
        {
          "aggregateType": "shipment",
          "topic": "ecommerce.shipping.v1",
          "type": "shipment.created",
          "version": 1,
        },
        {
          "aggregateType": "shipment",
          "topic": "ecommerce.shipping.v1",
          "type": "shipment.failed",
          "version": 1,
        },
        {
          "aggregateType": "stock-reservation",
          "topic": "ecommerce.inventory.v1",
          "type": "stock.released",
          "version": 1,
        },
        {
          "aggregateType": "stock-reservation",
          "topic": "ecommerce.inventory.v1",
          "type": "stock.reserved",
          "version": 1,
        },
        {
          "aggregateType": "stock-reservation",
          "topic": "ecommerce.inventory.v1",
          "type": "stock.unavailable",
          "version": 1,
        },
      ]
    `);
  });

  it('todo evento citado na matriz de compensação existe no catálogo', () => {
    const known = new Set([...KNOWN_EVENT_TYPES, 'saga.timeout']);

    for (const row of COMPENSATION_MATRIX) {
      expect(known, `falha "${row.failure}" não está no catálogo`).toContain(row.failure);
      for (const emitted of row.emits) {
        expect(known, `evento emitido "${emitted}" não está no catálogo`).toContain(emitted);
      }
    }
  });

  it('a falha no envio exige compensação de DOIS serviços', () => {
    const shipmentFailure = COMPENSATION_MATRIX.find((row) => row.failure === 'shipment.failed');

    expect(shipmentFailure?.compensatedBy).toEqual(['payment-service', 'inventory-service']);
  });
});
