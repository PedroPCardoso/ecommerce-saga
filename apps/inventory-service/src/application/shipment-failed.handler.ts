import { Injectable } from '@nestjs/common';
import { CONSUMER_GROUPS, createEvent, inventoryEvents, type EventOf, type shippingEvents } from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { markProcessed } from '@ecommerce/idempotency';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

export type ShipmentFailedEvent = EventOf<typeof shippingEvents.shipmentFailed>;

/**
 * Libera o estoque que a própria Inventory reservou, em reação a uma falha
 * POSTERIOR da saga (shipment.failed). Compensação dupla e paralela junto
 * com RefundPaymentUseCase (Payment Service) — o Order só fecha quando as
 * duas chegarem (docs/PLAN.md).
 */
@Injectable()
export class ShipmentFailedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: ShipmentFailedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.inventory);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const { orderId } = envelope.payload;
      const reservation = await tx.stockReservation.findFirst({ where: { orderId } });

      if (!reservation) {
        // shipment.failed só acontece depois de stock.reserved ter sido publicado (o
        // Shipping só tenta enviar depois de saber que reservou) — cadeia causal
        // garante que a reserva já existe. Ausente é dado inconsistente, não corrida.
        const error = new Error(
          `StockReservation do pedido ${orderId} não encontrada ao processar shipment.failed — dado inconsistente`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }

      if (reservation.status === 'RELEASED') {
        return; // defesa extra: já liberado (mesmo raciocínio do Payment Service)
      }

      await tx.stockReservation.update({
        where: { id: reservation.id },
        data: { status: 'RELEASED' },
      });

      const releasedEnvelope = createEvent(inventoryEvents.stockReleased, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'inventory-service@0.1.0',
        payload: {
          reservationId: reservation.id,
          orderId,
          items: reservation.items as Array<{ sku: string; quantity: number }>,
          compensationFor: 'shipment.failed',
          releasedAt: new Date().toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: releasedEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'stock-reservation',
        eventType: 'stock.released',
        envelope: releasedEnvelope,
      });
    });
  }
}
