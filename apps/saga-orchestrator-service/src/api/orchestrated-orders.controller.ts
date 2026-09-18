import { randomUUID } from 'node:crypto';
import { BadRequestException, Body, Controller, HttpCode, Post } from '@nestjs/common';
import { z } from 'zod';
import {
  addressSchema,
  amountCentsSchema,
  createEvent,
  currencySchema,
  orchestrationEvents,
  reservedItemSchema,
} from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { CommandProducerService } from '../infrastructure/command-producer.service.js';

const createOrchestratedOrderBodySchema = z.object({
  amountCents: amountCentsSchema,
  currency: currencySchema,
  items: z.array(reservedItemSchema).min(1).max(100),
  address: addressSchema,
});

/**
 * Contraparte de `POST /orders` (order-service) no lado orquestrado: cria o
 * agregado já em `AWAITING_PAYMENT` e publica o PRIMEIRO comando. A partir daqui
 * quem decide os próximos passos é sempre `OrchestratorService`, nunca este
 * controller de novo — ver docs/adr/0012.
 *
 * Deliberadamente sem autenticação/Idempotency-Key/throttling (presentes em
 * `order-service`): este endpoint existe só para medir a comparação da Fase 11,
 * não para produção — ver Global Constraints do plano desta fase.
 */
@Controller('orchestrated-orders')
export class OrchestratedOrdersController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly commandProducer: CommandProducerService,
  ) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown) {
    const parsed = createOrchestratedOrderBodySchema.safeParse(body);
    if (!parsed.success) {
      throw new BadRequestException(
        parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`),
      );
    }

    const id = randomUUID();
    const now = new Date();

    await this.prisma.client.orchestratedOrder.create({
      data: {
        id,
        status: 'AWAITING_PAYMENT',
        amountCents: parsed.data.amountCents,
        currency: parsed.data.currency,
        items: parsed.data.items,
        address: parsed.data.address,
        createdAt: now,
      },
    });

    const envelope = createEvent(orchestrationEvents.authorizePaymentCommand, {
      aggregateId: id,
      correlationId: id,
      producer: 'saga-orchestrator-service@0.1.0',
      payload: {
        orchestratedOrderId: id,
        amountCents: parsed.data.amountCents,
        currency: parsed.data.currency,
      },
    });

    await this.commandProducer.producer.publish(orchestrationEvents.authorizePaymentCommand.topic, envelope);

    return { orchestratedOrderId: id, status: 'AWAITING_PAYMENT', createdAt: now.toISOString() };
  }
}
