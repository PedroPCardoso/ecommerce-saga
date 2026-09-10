import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import type { Prisma } from '@prisma/client';
import { createEvent, orderEvents, type Address, type Currency, type OrderItem } from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

export interface CreateOrderInput {
  customerId: string;
  idempotencyKey: string;
  items: OrderItem[];
  currency: Currency;
  shippingAddress: Address;
}

export interface CreateOrderResult {
  orderId: string;
  status: string;
  createdAt: string;
}

@Injectable()
export class CreateOrderUseCase {
  constructor(private readonly prisma: PrismaService) {}

  async execute(input: CreateOrderInput): Promise<{ result: CreateOrderResult; replayed: boolean }> {
    const existing = await this.prisma.client.idempotencyKey.findUnique({
      where: { key_customerId: { key: input.idempotencyKey, customerId: input.customerId } },
    });
    if (existing) {
      return { result: existing.responseBody as unknown as CreateOrderResult, replayed: true };
    }

    const orderId = randomUUID();
    // SEMPRE calculado no servidor — aceitar totalAmountCents do cliente permitiria
    // adulterar o preço do pedido (A05/A06).
    const totalAmountCents = input.items.reduce(
      (sum, item) => sum + item.unitPriceCents * item.quantity,
      0,
    );
    const createdAt = new Date();

    const envelope = createEvent(orderEvents.orderCreated, {
      aggregateId: orderId,
      correlationId: orderId,
      producer: 'order-service@0.1.0',
      payload: {
        orderId,
        customerId: input.customerId,
        items: input.items,
        totalAmountCents,
        currency: input.currency,
        shippingAddress: input.shippingAddress,
      },
    });

    const result: CreateOrderResult = {
      orderId,
      status: 'PENDING',
      createdAt: createdAt.toISOString(),
    };

    await this.prisma.client.$transaction(async (tx) => {
      await tx.order.create({
        data: {
          id: orderId,
          customerId: input.customerId,
          items: input.items,
          totalAmountCents,
          currency: input.currency,
          status: 'PENDING',
          shippingAddress: input.shippingAddress,
          createdAt,
        },
      });

      await insertOutboxRow(tx, {
        eventId: envelope.eventId,
        aggregateId: orderId,
        aggregateType: 'order',
        eventType: 'order.created',
        envelope,
      });

      await tx.idempotencyKey.create({
        data: {
          key: input.idempotencyKey,
          customerId: input.customerId,
          orderId,
          responseStatus: 201,
          responseBody: result as unknown as Prisma.InputJsonValue,
          createdAt,
        },
      });
    });

    return { result, replayed: false };
  }
}
