import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import {
  CONSUMER_GROUPS,
  createEvent,
  paymentEvents,
  type Currency,
  type UnknownEnvelope,
} from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { markProcessed } from '@ecommerce/idempotency';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

const COMPENSATION_FOR_BY_EVENT: Record<string, 'stock.unavailable' | 'shipment.failed' | 'saga.timeout'> = {
  'stock.unavailable': 'stock.unavailable',
  'shipment.failed': 'shipment.failed',
  'saga.timeout': 'saga.timeout',
};

/**
 * Estorna a autorização que o próprio Payment Service criou, em reação a uma
 * falha de um passo POSTERIOR da saga (docs/PLAN.md, matriz de compensação).
 * `stock.unavailable` e `shipment.failed` levam ao MESMO efeito aqui — a
 * diferença entre eles só importa para o `compensationFor` do evento publicado
 * e para o Order Service decidir se falta liberar estoque também.
 */
@Injectable()
export class RefundPaymentUseCase {
  constructor(private readonly prisma: PrismaService) {}

  async execute(envelope: UnknownEnvelope): Promise<void> {
    const compensationFor = COMPENSATION_FOR_BY_EVENT[envelope.eventType];
    if (!compensationFor) {
      throw new Error(`RefundPaymentUseCase não sabe processar eventType "${envelope.eventType}"`);
    }
    const orderId = (envelope.payload as { orderId: string }).orderId;

    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.payment);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const payment = await tx.payment.findUnique({ where: { orderId } });
      if (!payment) {
        // A cadeia causal da saga GARANTE que o Payment já existe: stock.unavailable e
        // shipment.failed só acontecem depois de payment.approved ter sido commitado
        // (Inventory só reserva DEPOIS de consumir payment.approved; Shipping só envia
        // DEPOIS de stock.reserved). Diferente do KnownOrder do Inventory, não há
        // corrida legítima aqui — Payment ausente é dado inconsistente, não atraso.
        const error = new Error(
          `Payment do pedido ${orderId} não encontrado ao processar ${envelope.eventType} — dado inconsistente`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }

      if (payment.status === 'REFUNDED') {
        // Defesa extra (A08/A10): só UM dos dois gatilhos de compensação pode acontecer
        // por pedido na coreografia atual (stock.unavailable e shipment.failed são
        // mutuamente exclusivos — o segundo só existe se o primeiro NÃO aconteceu), mas
        // isto não devia ser assumido silenciosamente. Evento com eventId diferente do
        // já processado (logo markProcessed não pegou) tentando estornar de novo é
        // ignorado com segurança.
        return;
      }

      const refundId = randomUUID();
      const refundedAt = new Date();

      await tx.payment.update({
        where: { orderId },
        data: { status: 'REFUNDED', updatedAt: refundedAt },
      });

      const refundedEnvelope = createEvent(paymentEvents.paymentRefunded, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'payment-service@0.1.0',
        payload: {
          paymentId: payment.id,
          orderId,
          refundId,
          amountCents: payment.amountCents,
          currency: payment.currency as Currency,
          compensationFor,
          refundedAt: refundedAt.toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: refundedEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'payment',
        eventType: 'payment.refunded',
        envelope: refundedEnvelope,
      });
    });
  }
}
