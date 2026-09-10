import { Injectable, Logger } from '@nestjs/common';
import { markProcessed } from '@ecommerce/idempotency';
import { insertOutboxRow } from '@ecommerce/outbox';
import {
  CANCELLATION_REASON,
  CONSUMER_GROUPS,
  ORDER_STATUS,
  createEvent,
  orderEvents,
  orderStatusSchema,
  type CancellationReason,
  type Currency,
  type UnknownEnvelope,
} from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';
import { applyEvent, type ProjectionEventType } from './order-state-machine.js';

/** Motivo de cancelamento por eventType que dispara CANCELLED diretamente. */
const CANCELLATION_REASON_BY_EVENT: Partial<Record<ProjectionEventType, CancellationReason>> = {
  'payment.failed': CANCELLATION_REASON.PAYMENT_FAILED,
};

const PROJECTION_EVENT_TYPES: ReadonlySet<string> = new Set<ProjectionEventType>([
  'payment.approved',
  'payment.failed',
  'stock.reserved',
  'stock.unavailable',
  'shipment.created',
  'shipment.failed',
]);

function isProjectionEventType(eventType: string): eventType is ProjectionEventType {
  return PROJECTION_EVENT_TYPES.has(eventType);
}

/**
 * Projeta o estado da saga sobre o agregado Order (C2 da revisão final):
 * sem isto, o pedido fica `PENDING` para sempre mesmo depois de a saga
 * inteira terminar em outros serviços. Consome `SUBSCRIPTIONS[orderProjection]`
 * (payments, inventory, shipping) e aplica `OrderStateMachine.applyEvent`.
 *
 * Ao chegar a um estado terminal (`CONFIRMED`/`CANCELLED`), publica
 * `order.confirmed`/`order.cancelled` via outbox — é disto que o
 * Notification Service depende para mandar o e-mail final.
 */
@Injectable()
export class OrderProjectionHandler {
  private readonly logger = new Logger(OrderProjectionHandler.name);

  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: UnknownEnvelope): Promise<void> {
    if (!isProjectionEventType(envelope.eventType)) return;
    const eventType = envelope.eventType;
    const orderId = (envelope.payload as { orderId: string }).orderId;

    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.orderProjection);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const order = await tx.order.findUnique({ where: { id: orderId } });
      if (!order) {
        // O Order é criado SINCRONAMENTE na requisição HTTP (CreateOrderUseCase), antes
        // de qualquer evento de saga existir — diferente do KnownOrder do Inventory, não
        // há corrida legítima aqui. Se o pedido não existe, é dado inconsistente (retry
        // nunca conserta), não atraso de propagação.
        const error = new Error(
          `Order ${orderId} não encontrado ao projetar ${eventType} — dado inconsistente`,
        ) as Error & { permanent: boolean };
        error.permanent = true;
        throw error;
      }

      const currentStatus = orderStatusSchema.parse(order.status);
      const result = applyEvent(currentStatus, eventType);

      if (!result.changed) {
        if (result.reason === 'premature') {
          // O pedido AINDA NÃO chegou ao estado que este evento exige — a mesma corrida
          // legítima entre tópicos diferentes que Inventory/Shipping já tratam
          // (payment-approved.handler.ts, stock-reserved.handler.ts): nada garante ordem
          // ENTRE ecommerce.payments.v1/inventory.v1/shipping.v1. Por isso este `throw`
          // acontece DENTRO da transação, DEPOIS do markProcessed acima: o Prisma faz
          // ROLLBACK de tudo, inclusive do registro de idempotência. Sem esse rollback, a
          // escada de retry encontraria o (eventId, consumerGroup) já marcado e desistiria
          // silenciosamente — exatamente o bug que este projetor existe para corrigir (C2),
          // só que pela porta do "invalid-transition" em vez de "nenhum consumidor".
          //
          // Erro sem `.permanent = true` -> classifyError (@ecommerce/kafka) classifica
          // como RETRIÁVEL por padrão -> a escada 5s/1m/10m dá tempo do evento anterior
          // chegar.
          throw new Error(
            `Pedido ${orderId} ainda não chegou ao estado exigido por ${eventType} (está em ${currentStatus}) — aguardando evento anterior da saga`,
          );
        }

        // 'stale': o pedido já passou deste ponto (reentrega tardia, ou evento superado por
        // um mais recente que chegou primeiro) — regra de ouro (docs/PLAN.md §1): rejeita
        // silenciosamente, com WARN. Seguro commitar aqui: não há efeito pendente.
        this.logger.warn(
          `Transição obsoleta ignorada: pedido ${orderId} em ${currentStatus}, evento ${eventType}`,
        );
        return;
      }

      // Guarda contra lost update: duas partições diferentes (ex.: payments e shipping)
      // podem estar sendo processadas ao mesmo tempo por réplicas diferentes deste
      // consumer group. O `where: { status: currentStatus }` faz o UPDATE falhar
      // (count 0) se outra transação já mudou o status entre nosso SELECT e este UPDATE,
      // em vez de sobrescrever silenciosamente uma transição concorrente.
      const updated = await tx.order.updateMany({
        where: { id: orderId, status: currentStatus },
        data: { status: result.next },
      });
      if (updated.count === 0) {
        throw new Error(
          `Pedido ${orderId} mudou de status concorrentemente enquanto projetava ${eventType} — retentando`,
        );
      }

      if (result.next === ORDER_STATUS.COMPENSATING) {
        // Nada observa hoje um pedido preso em COMPENSATING (a matriz de compensação — I4 —
        // ainda não publica payment.refunded/stock.released para destravá-lo). Sem este
        // log, um pagamento capturado e nunca estornado não deixa rastro em lugar nenhum
        // (OWASP A09) até alguém notar "faltando" numa auditoria manual.
        this.logger.error(
          `Pedido ${orderId} entrou em COMPENSATING via ${eventType} e vai FICAR PRESO aqui — ` +
            `compensação (payment.refunded/stock.released) ainda não implementada. Ação manual necessária.`,
        );
      } else if (result.next === ORDER_STATUS.CONFIRMED) {
        const confirmedEnvelope = createEvent(orderEvents.orderConfirmed, {
          aggregateId: order.id,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'order-service@0.1.0',
          payload: {
            orderId: order.id,
            customerId: order.customerId,
            totalAmountCents: order.totalAmountCents,
            currency: order.currency as Currency,
            confirmedAt: new Date().toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: confirmedEnvelope.eventId,
          aggregateId: order.id,
          aggregateType: 'order',
          eventType: 'order.confirmed',
          envelope: confirmedEnvelope,
        });
      } else if (result.next === ORDER_STATUS.CANCELLED) {
        const reason = CANCELLATION_REASON_BY_EVENT[eventType];
        if (reason) {
          const cancelledEnvelope = createEvent(orderEvents.orderCancelled, {
            aggregateId: order.id,
            correlationId: envelope.correlationId,
            causationId: envelope.eventId,
            producer: 'order-service@0.1.0',
            payload: {
              orderId: order.id,
              customerId: order.customerId,
              reason,
              // Nada foi efetivado ainda no caminho payment.failed (COMPENSATION_MATRIX em
              // @ecommerce/contracts) — não há o que desfazer, por isso a lista vem vazia.
              compensationsApplied: [],
              cancelledAt: new Date().toISOString(),
            },
          });

          await insertOutboxRow(tx, {
            eventId: cancelledEnvelope.eventId,
            aggregateId: order.id,
            aggregateType: 'order',
            eventType: 'order.cancelled',
            envelope: cancelledEnvelope,
          });
        }
      }
    });
  }
}
