import { z } from 'zod';
import { defineEvent } from '../envelope.js';
import { TOPICS } from '../topics.js';
import { quantitySchema, reservedItemSchema, skuSchema } from '../common.js';

export const stockReserved = defineEvent({
  type: 'stock.reserved',
  version: 1,
  aggregateType: 'stock-reservation',
  topic: TOPICS.inventory,
  payload: z.object({
    reservationId: z.string().uuid(),
    orderId: z.string().uuid(),
    items: z.array(reservedItemSchema).min(1),
    /**
     * Reserva com prazo. Se a saga morrer sem confirmar nem liberar, a reserva expira
     * e o estoque volta sozinho — rede de segurança para o caso de o evento de
     * compensação se perder de vez (A06: insecure design).
     */
    expiresAt: z.string().datetime({ offset: true }),
    reservedAt: z.string().datetime({ offset: true }),
  }),
});

export const stockUnavailable = defineEvent({
  type: 'stock.unavailable',
  version: 1,
  aggregateType: 'stock-reservation',
  topic: TOPICS.inventory,
  payload: z.object({
    orderId: z.string().uuid(),
    unavailableItems: z
      .array(
        z.object({
          sku: skuSchema,
          requested: quantitySchema,
          available: z.number().int().nonnegative(),
        }),
      )
      .min(1),
    checkedAt: z.string().datetime({ offset: true }),
  }),
});

/** Evento de COMPENSAÇÃO: devolve ao estoque o que havia sido reservado. */
export const stockReleased = defineEvent({
  type: 'stock.released',
  version: 1,
  aggregateType: 'stock-reservation',
  topic: TOPICS.inventory,
  payload: z.object({
    reservationId: z.string().uuid(),
    orderId: z.string().uuid(),
    items: z.array(reservedItemSchema).min(1),
    compensationFor: z.enum(['shipment.failed', 'saga.timeout', 'reservation.expired']),
    releasedAt: z.string().datetime({ offset: true }),
  }),
});
