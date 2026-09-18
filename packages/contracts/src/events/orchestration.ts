import { z } from 'zod';
import { defineEvent } from '../envelope.js';
import { TOPICS } from '../topics.js';
import { addressSchema, amountCentsSchema, currencySchema, reservedItemSchema } from '../common.js';

/**
 * Comandos/respostas do harness de comparação orquestrada (Fase 11, ADR-0012).
 *
 * Diferença estrutural que estes eventos existem para provar: na coreografia
 * (`events/order.ts`, `events/payment.ts`, ...) todo evento é um FATO que qualquer
 * serviço interessado pode assinar — quem decide o que fazer a seguir é quem escuta.
 * Aqui, os três primeiros são COMANDOS: só um executor específico os consome, e quem
 * decide quando emiti-los é sempre o `OrchestratorService` (um único lugar). A
 * resposta é genérica de propósito — os 3 executores respondem na mesma forma para
 * que o orquestrador tenha UM handler, não um por passo.
 */

export const authorizePaymentCommand = defineEvent({
  type: 'command.payment.authorize',
  version: 1,
  aggregateType: 'orchestratedOrder',
  topic: TOPICS.commandsPayment,
  payload: z.object({
    orchestratedOrderId: z.string().uuid(),
    amountCents: amountCentsSchema,
    currency: currencySchema,
  }),
});

export const reserveStockCommand = defineEvent({
  type: 'command.inventory.reserve-stock',
  version: 1,
  aggregateType: 'orchestratedOrder',
  topic: TOPICS.commandsInventory,
  payload: z.object({
    orchestratedOrderId: z.string().uuid(),
    items: z.array(reservedItemSchema).min(1).max(100),
  }),
});

export const createShipmentCommand = defineEvent({
  type: 'command.shipping.create-shipment',
  version: 1,
  aggregateType: 'orchestratedOrder',
  topic: TOPICS.commandsShipping,
  payload: z.object({
    orchestratedOrderId: z.string().uuid(),
    address: addressSchema,
  }),
});

/**
 * Resposta ÚNICA e genérica dos 3 executores — não uma por passo. É o que permite
 * ao `OrchestratorService` ter um único consumidor/handler para o fluxo inteiro,
 * em vez de um router por evento de domínio como a coreografia precisa.
 */
export const executorResponded = defineEvent({
  type: 'orchestrator.executor-responded',
  version: 1,
  aggregateType: 'orchestratedOrder',
  topic: TOPICS.responsesOrchestrator,
  payload: z.object({
    orchestratedOrderId: z.string().uuid(),
    step: z.enum(['payment', 'inventory', 'shipping']),
    outcome: z.enum(['success', 'failure']),
    reason: z.string().optional(),
  }),
});
