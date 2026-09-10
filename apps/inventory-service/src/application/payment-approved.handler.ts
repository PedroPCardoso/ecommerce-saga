import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { z } from 'zod';
import type { Prisma } from '../../prisma/generated/index.js';
import { markProcessed } from '@ecommerce/idempotency';
import { insertOutboxRow } from '@ecommerce/outbox';
import {
  CONSUMER_GROUPS,
  createEvent,
  type EventOf,
  inventoryEvents,
  type paymentEvents,
  reservedItemSchema,
} from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

export type PaymentApprovedEvent = EventOf<typeof paymentEvents.paymentApproved>;

/** Arbitrário e documentado: o sweeper que expira reservas de verdade é a Fase 6. */
const RESERVATION_TTL_MINUTES = 30;

/**
 * Gatilho determinístico de indisponibilidade (docs/PLAN.md 4.5): qualquer
 * SKU que comece com "OUT-" está fora de estoque. Sem Math.random() — teste
 * não determinístico não é teste.
 */
function isUnavailable(sku: string): boolean {
  return sku.startsWith('OUT-');
}

/**
 * Gatilho REAL da reserva de estoque (docs/PLAN.md 4.5). `order.created` só
 * ensina os SKUs (OrderCreatedHandler); é `payment.approved` que decide se
 * reserva ou não.
 */
@Injectable()
export class PaymentApprovedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: PaymentApprovedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.inventory);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const { orderId } = envelope.payload;
      const knownOrder = await tx.knownOrder.findUnique({ where: { orderId } });

      if (!knownOrder) {
        /*
         * order.created deste pedido ainda não foi processado por este serviço — nada
         * garante ordem ENTRE tópicos diferentes (orders vs payments). Este throw
         * acontece DENTRO da transação, DEPOIS do markProcessed acima: o Prisma faz
         * ROLLBACK de tudo, inclusive do registro de idempotência. Sem esse rollback, a
         * escada de retry encontraria o (eventId, consumerGroup) já marcado e desistiria
         * silenciosamente, sem nunca ter reservado nada.
         *
         * Erro sem `.permanent = true` -> classifyError (@ecommerce/kafka) classifica
         * como RETRIÁVEL por padrão -> a escada 5s/1m/10m dá tempo para order.created
         * chegar antes de cair na DLT.
         */
        throw new Error(
          `KnownOrder ${orderId} ainda não visto por este serviço — aguardando order.created`,
        );
      }

      // Defesa extra (A08/A10): o dado veio do nosso próprio banco, mas revalidar contra
      // o schema não custa nada e transforma corrupção local em erro PERMANENTE — retry
      // nunca conserta dado corrompido, então não faz sentido gastar a escada nele.
      const parsedItems = z.array(reservedItemSchema).safeParse(knownOrder.items);
      if (!parsedItems.success) {
        const error = new Error(
          `KnownOrder ${orderId} tem "items" corrompidos no banco — não é erro retriável`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }
      const items = parsedItems.data;

      const unavailableItems = items.filter((item) => isUnavailable(item.sku));

      if (unavailableItems.length > 0) {
        const unavailableEnvelope = createEvent(inventoryEvents.stockUnavailable, {
          aggregateId: orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'inventory-service@0.1.0',
          payload: {
            orderId,
            unavailableItems: unavailableItems.map((item) => ({
              sku: item.sku,
              requested: item.quantity,
              available: 0,
            })),
            checkedAt: new Date().toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: unavailableEnvelope.eventId,
          aggregateId: orderId,
          aggregateType: 'stock-reservation',
          eventType: 'stock.unavailable',
          envelope: unavailableEnvelope,
        });
        return; // tudo ou nada: nenhum item deste pedido é reservado, nem os disponíveis
      }

      const reservationId = randomUUID();
      const reservedAt = new Date();
      const expiresAt = new Date(reservedAt.getTime() + RESERVATION_TTL_MINUTES * 60_000);

      await tx.stockReservation.create({
        data: {
          id: reservationId,
          orderId,
          items: items as unknown as Prisma.InputJsonValue,
          status: 'RESERVED',
          expiresAt,
          reservedAt,
        },
      });

      const reservedEnvelope = createEvent(inventoryEvents.stockReserved, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'inventory-service@0.1.0',
        payload: {
          reservationId,
          orderId,
          items,
          expiresAt: expiresAt.toISOString(),
          reservedAt: reservedAt.toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: reservedEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'stock-reservation',
        eventType: 'stock.reserved',
        envelope: reservedEnvelope,
      });
    });
  }
}
