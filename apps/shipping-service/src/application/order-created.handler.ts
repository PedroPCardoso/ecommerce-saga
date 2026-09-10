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
 * Só aprende o endereço de entrega do pedido — NENHUM evento de domínio é
 * publicado aqui. O gatilho real do envio é sempre `stock.reserved`
 * (StockReservedHandler, Task 3). `stock.reserved` não carrega endereço,
 * então é este handler que dá ao Shipping "para onde enviar" — ver o
 * comentário em `packages/contracts/src/topics.ts`
 * (SUBSCRIPTIONS[CONSUMER_GROUPS.shipping]).
 */
@Injectable()
export class OrderCreatedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: OrderCreatedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.shipping);
      if (!isNew) return; // reentrega do mesmo evento — já aprendemos este endereço

      await tx.knownOrder.upsert({
        where: { orderId: envelope.payload.orderId },
        create: {
          orderId: envelope.payload.orderId,
          shippingAddress: envelope.payload.shippingAddress as unknown as Prisma.InputJsonValue,
        },
        update: {
          shippingAddress: envelope.payload.shippingAddress as unknown as Prisma.InputJsonValue,
        },
      });
    });
  }
}
