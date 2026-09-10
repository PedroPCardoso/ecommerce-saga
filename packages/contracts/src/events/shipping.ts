import { z } from 'zod';
import { defineEvent } from '../envelope.js';
import { TOPICS } from '../topics.js';

export const SHIPMENT_FAILURE_CODE = {
  ADDRESS_NOT_SERVICEABLE: 'ADDRESS_NOT_SERVICEABLE',
  CARRIER_UNAVAILABLE: 'CARRIER_UNAVAILABLE',
  INVALID_ADDRESS: 'INVALID_ADDRESS',
} as const;

export const shipmentCreated = defineEvent({
  type: 'shipment.created',
  version: 1,
  aggregateType: 'shipment',
  topic: TOPICS.shipping,
  payload: z.object({
    shipmentId: z.string().uuid(),
    orderId: z.string().uuid(),
    carrier: z.enum(['CORREIOS', 'JADLOG', 'LOGGI']),
    trackingCode: z.string().min(1).max(64),
    /**
     * URL da etiqueta. Quem consumir isto NÃO deve buscar a URL cegamente:
     * allowlist de host e bloqueio de IP privado/link-local, ou você acabou de
     * construir um SSRF dirigível por evento (A01).
     */
    labelUrl: z.string().url(),
    estimatedDeliveryAt: z.string().datetime({ offset: true }),
    createdAt: z.string().datetime({ offset: true }),
  }),
});

/**
 * A falha mais interessante da saga: dispara compensação DUPLA e paralela —
 * Inventory publica `stock.released` e Payment publica `payment.refunded`.
 * O Order só fecha em CANCELLED quando as duas chegarem.
 */
export const shipmentFailed = defineEvent({
  type: 'shipment.failed',
  version: 1,
  aggregateType: 'shipment',
  topic: TOPICS.shipping,
  payload: z.object({
    orderId: z.string().uuid(),
    failureCode: z.nativeEnum(SHIPMENT_FAILURE_CODE),
    reason: z.string().max(200),
    failedAt: z.string().datetime({ offset: true }),
  }),
});
