import { Injectable } from '@nestjs/common';
import type { Prisma } from '../../prisma/generated/index.js';
import { markProcessed } from '@ecommerce/idempotency';
import { CONSUMER_GROUPS, type EventOf, type orderEvents } from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

export type OrderCreatedEvent = EventOf<typeof orderEvents.orderCreated>;

/**
 * Só aprende os itens do pedido — NENHUM evento de domínio é publicado
 * aqui. O gatilho real da reserva é sempre `payment.approved`
 * (PaymentApprovedHandler). `payment.approved` não carrega SKUs, então é
 * este handler que dá ao Inventory o "o que reservar" — ver o comentário
 * em `packages/contracts/src/topics.ts` (SUBSCRIPTIONS do inventory-service).
 */
@Injectable()
export class OrderCreatedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: OrderCreatedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.inventory);
      if (!isNew) return; // reentrega do mesmo evento — já aprendemos estes itens

      const items = envelope.payload.items.map((item) => ({
        sku: item.sku,
        quantity: item.quantity,
      }));

      await tx.knownOrder.upsert({
        where: { orderId: envelope.payload.orderId },
        create: {
          orderId: envelope.payload.orderId,
          items: items as unknown as Prisma.InputJsonValue,
        },
        update: {
          items: items as unknown as Prisma.InputJsonValue,
        },
      });
    });
  }
}
