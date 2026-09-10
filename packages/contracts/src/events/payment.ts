import { z } from 'zod';
import { defineEvent } from '../envelope.js';
import { TOPICS } from '../topics.js';
import { amountCentsSchema, currencySchema } from '../common.js';

/**
 * Nada aqui carrega PAN, CVV ou validade — nem cifrado.
 * O único identificador do instrumento de pagamento é o token opaco do gateway,
 * mais os 4 últimos dígitos, que existem só para o cliente reconhecer o cartão (A04).
 */
const paymentInstrumentSchema = z.object({
  gatewayToken: z.string().min(1).max(128),
  cardLast4: z.string().regex(/^\d{4}$/),
  brand: z.enum(['VISA', 'MASTERCARD', 'ELO', 'AMEX']),
});

export const PAYMENT_FAILURE_CODE = {
  INSUFFICIENT_FUNDS: 'INSUFFICIENT_FUNDS',
  CARD_DECLINED: 'CARD_DECLINED',
  GATEWAY_TIMEOUT: 'GATEWAY_TIMEOUT',
  FRAUD_SUSPECTED: 'FRAUD_SUSPECTED',
} as const;

export const paymentApproved = defineEvent({
  type: 'payment.approved',
  version: 1,
  aggregateType: 'payment',
  topic: TOPICS.payments,
  payload: z.object({
    paymentId: z.string().uuid(),
    orderId: z.string().uuid(),
    amountCents: amountCentsSchema,
    currency: currencySchema,
    authorizationCode: z.string().min(1).max(64),
    instrument: paymentInstrumentSchema,
    approvedAt: z.string().datetime({ offset: true }),
  }),
});

export const paymentFailed = defineEvent({
  type: 'payment.failed',
  version: 1,
  aggregateType: 'payment',
  topic: TOPICS.payments,
  payload: z.object({
    paymentId: z.string().uuid(),
    orderId: z.string().uuid(),
    amountCents: amountCentsSchema,
    currency: currencySchema,
    failureCode: z.nativeEnum(PAYMENT_FAILURE_CODE),
    /** Mensagem para humano. Nunca inclua resposta crua do gateway: ela pode trazer PII. */
    reason: z.string().max(200),
    failedAt: z.string().datetime({ offset: true }),
  }),
});

/**
 * Evento de COMPENSAÇÃO. Publicado quando o Payment Service reage a uma falha
 * de um passo POSTERIOR da saga (`stock.unavailable` ou `shipment.failed`).
 * O `compensationFor` registra qual falha disparou o estorno.
 */
export const paymentRefunded = defineEvent({
  type: 'payment.refunded',
  version: 1,
  aggregateType: 'payment',
  topic: TOPICS.payments,
  payload: z.object({
    paymentId: z.string().uuid(),
    orderId: z.string().uuid(),
    refundId: z.string().uuid(),
    amountCents: amountCentsSchema,
    currency: currencySchema,
    compensationFor: z.enum(['stock.unavailable', 'shipment.failed', 'saga.timeout']),
    refundedAt: z.string().datetime({ offset: true }),
  }),
});
