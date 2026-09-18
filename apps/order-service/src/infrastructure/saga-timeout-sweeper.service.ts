import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CANCELLATION_REASON, ORDER_STATUS, createEvent, orderEvents } from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from './prisma.service.js';

const SWEEP_BATCH_SIZE = 50;

/**
 * Em coreografia ninguém vigia o pedido inteiro — o Order Service vira o
 * meio-orquestrador que a coreografia acaba exigindo (docs/PLAN.md, Fase 6).
 * Varre pedidos presos em PAYMENT_APPROVED (Payment aprovou, mas Inventory
 * nunca respondeu dentro do prazo — cenário real: o serviço caiu no meio da
 * saga) e publica `saga.timeout` para quem tiver algo a desfazer reagir.
 */
@Injectable()
export class SagaTimeoutSweeperService implements OnModuleInit, OnModuleDestroy {
  private timer: NodeJS.Timeout | null = null;

  constructor(private readonly prisma: PrismaService) {}

  onModuleInit(): void {
    this.timer = setInterval(() => {
      void this.sweepOnce();
    }, env.SAGA_TIMEOUT_SWEEP_INTERVAL_MS);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  /** Uma passada: varre até `SWEEP_BATCH_SIZE` pedidos presos. Devolve quantos varreu. */
  async sweepOnce(): Promise<number> {
    const threshold = new Date(Date.now() - env.SAGA_TIMEOUT_THRESHOLD_MS);
    const stuckOrders = await this.prisma.client.order.findMany({
      where: { status: ORDER_STATUS.PAYMENT_APPROVED, updatedAt: { lt: threshold } },
      take: SWEEP_BATCH_SIZE,
    });

    let swept = 0;
    for (const order of stuckOrders) {
      const timedOut = await this.prisma.client.$transaction(async (tx) => {
        // updateMany com o status como parte do WHERE: se outra passada (ou réplica)
        // já pegou este pedido entre o findMany acima e agora, count vem 0 e pulamos —
        // mesma guarda de lost-update de order-projection.handler.ts.
        const updated = await tx.order.updateMany({
          where: { id: order.id, status: ORDER_STATUS.PAYMENT_APPROVED },
          data: { status: ORDER_STATUS.COMPENSATING, compensationReason: CANCELLATION_REASON.SAGA_TIMEOUT },
        });
        if (updated.count === 0) return false;

        const envelope = createEvent(orderEvents.sagaTimedOut, {
          aggregateId: order.id,
          correlationId: order.id,
          producer: 'order-service@0.1.0',
          payload: {
            orderId: order.id,
            stuckStatus: 'PAYMENT_APPROVED',
            timedOutAt: new Date().toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: envelope.eventId,
          aggregateId: order.id,
          aggregateType: 'order',
          eventType: 'saga.timeout',
          envelope,
        });

        return true;
      });

      if (timedOut) swept += 1;
    }

    return swept;
  }
}
