import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Post,
  UseGuards,
} from '@nestjs/common';
import { Throttle } from '@nestjs/throttler';
import { z } from 'zod';
import { addressSchema, currencySchema, orderItemSchema } from '@ecommerce/contracts';
import { JwtAuthGuard } from './auth/jwt-auth.guard.js';
import { CurrentCustomer } from './auth/current-customer.decorator.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { CreateOrderUseCase } from '../application/create-order.use-case.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

const createOrderBodySchema = z.object({
  items: z.array(orderItemSchema).min(1).max(100),
  currency: currencySchema,
  shippingAddress: addressSchema,
});

@Controller('orders')
@UseGuards(JwtAuthGuard)
export class OrdersController {
  constructor(
    private readonly createOrder: CreateOrderUseCase,
    private readonly prisma: PrismaService,
  ) {}

  @Post()
  @HttpCode(201)
  @Throttle({ default: { limit: 10, ttl: 60_000 } })
  async create(
    @CurrentCustomer() customerId: string,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
  ) {
    if (!idempotencyKey) {
      throw new BadRequestException('Header Idempotency-Key é obrigatório');
    }

    const parsed = createOrderBodySchema.safeParse(body);
    if (!parsed.success) {
      throw new BadRequestException(
        parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`),
      );
    }

    const { result } = await this.createOrder.execute({
      customerId,
      idempotencyKey,
      items: parsed.data.items,
      currency: parsed.data.currency,
      shippingAddress: parsed.data.shippingAddress,
    });

    return result;
  }

  @Get(':id')
  async findOne(@CurrentCustomer() customerId: string, @Param('id', ParseUUIDPipe) id: string) {
    const order = await this.prisma.client.order.findUnique({ where: { id } });

    // 404 tanto para "não existe" quanto para "não é seu" — nunca 403 —
    // para não permitir enumeração de pedidos alheios (A01).
    if (!order || order.customerId !== customerId) {
      throw new NotFoundException('Pedido não encontrado');
    }

    return {
      orderId: order.id,
      status: order.status,
      totalAmountCents: order.totalAmountCents,
      currency: order.currency,
      createdAt: order.createdAt.toISOString(),
    };
  }
}
