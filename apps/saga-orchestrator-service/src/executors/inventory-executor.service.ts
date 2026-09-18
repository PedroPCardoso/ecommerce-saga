import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS, createEvent, orchestrationEvents, type EventOf } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';

type ReserveStockCommand = EventOf<typeof orchestrationEvents.reserveStockCommand>;

/**
 * SKU que comece com "OUT-" está fora de estoque — MESMO gatilho determinístico
 * que `apps/inventory-service/src/application/payment-approved.handler.ts` usa na
 * coreografia. Sem Math.random(): teste que não é determinístico não é teste.
 */
function hasOutOfStockItem(items: readonly { sku: string }[]): boolean {
  return items.some((item) => item.sku.startsWith('OUT-'));
}

/**
 * Executor "burro": ver comentário em `PaymentExecutorService` — mesma
 * justificativa vale aqui (sem banco, sem outbox, sem idempotência própria).
 */
@Injectable()
export class InventoryExecutorService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-inventory-executor`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.inventoryExecutor,
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.inventoryExecutor],
      producer: this.producer,
      handler: (ctx) => this.handle(ctx.envelope as unknown as ReserveStockCommand),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }

  private async handle(envelope: ReserveStockCommand): Promise<void> {
    const { orchestratedOrderId, items } = envelope.payload;
    const unavailable = hasOutOfStockItem(items);

    const response = createEvent(orchestrationEvents.executorResponded, {
      aggregateId: orchestratedOrderId,
      correlationId: envelope.correlationId,
      causationId: envelope.eventId,
      producer: 'inventory-executor@0.1.0',
      payload: {
        orchestratedOrderId,
        step: 'inventory',
        outcome: unavailable ? 'failure' : 'success',
        ...(unavailable
          ? { reason: 'Item fora de estoque (simulação determinística: SKU inicia com OUT-)' }
          : {}),
      },
    });

    await this.producer.publish(orchestrationEvents.executorResponded.topic, response);
  }
}
