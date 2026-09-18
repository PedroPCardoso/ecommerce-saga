import { z } from 'zod';
import { defineEvent } from '../envelope.js';
import { TOPICS } from '../topics.js';
import {
  addressSchema,
  amountCentsSchema,
  cancellationReasonSchema,
  compensationTypeSchema,
  currencySchema,
  orderItemSchema,
} from '../common.js';

export const orderCreated = defineEvent({
  type: 'order.created',
  version: 1,
  aggregateType: 'order',
  topic: TOPICS.orders,
  payload: z.object({
    orderId: z.string().uuid(),
    customerId: z.string().uuid(),
    items: z.array(orderItemSchema).min(1).max(100),
    totalAmountCents: amountCentsSchema,
    currency: currencySchema,
    shippingAddress: addressSchema,
  }),
});

export const orderConfirmed = defineEvent({
  type: 'order.confirmed',
  version: 1,
  aggregateType: 'order',
  topic: TOPICS.orders,
  payload: z.object({
    orderId: z.string().uuid(),
    customerId: z.string().uuid(),
    totalAmountCents: amountCentsSchema,
    currency: currencySchema,
    confirmedAt: z.string().datetime({ offset: true }),
  }),
});

export const orderCancelled = defineEvent({
  type: 'order.cancelled',
  version: 1,
  aggregateType: 'order',
  topic: TOPICS.orders,
  payload: z.object({
    orderId: z.string().uuid(),
    customerId: z.string().uuid(),
    reason: cancellationReasonSchema,
    /**
     * Compensações que o Order Service esperou antes de fechar o cancelamento.
     * É a prova de que a saga desfez o que fez — e o que você vai olhar no post-mortem.
     */
    compensationsApplied: z.array(compensationTypeSchema),
    cancelledAt: z.string().datetime({ offset: true }),
  }),
});

/**
 * Publicado pelo `SagaTimeoutSweeperService` (Order Service) quando um pedido
 * fica tempo demais preso num estado não-terminal sem o próximo evento da
 * saga chegar (docs/PLAN.md, Fase 6). Quem reage é quem tiver algo a desfazer
 * — hoje só o Payment Service (`RefundPaymentUseCase`, I4) — nunca o próprio
 * Order Service: ele só ANUNCIA o timeout, não decide o que os outros fazem.
 */
export const sagaTimedOut = defineEvent({
  type: 'saga.timeout',
  version: 1,
  aggregateType: 'order',
  topic: TOPICS.orders,
  payload: z.object({
    orderId: z.string().uuid(),
    stuckStatus: z.enum(['PENDING', 'PAYMENT_APPROVED', 'STOCK_RESERVED']),
    timedOutAt: z.string().datetime({ offset: true }),
  }),
});
