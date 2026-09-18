import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS, createEvent, orchestrationEvents, type EventOf } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';

type CreateShipmentCommand = EventOf<typeof orchestrationEvents.createShipmentCommand>;

/**
 * CEP que comece com "00000" está fora da área de cobertura simulada — MESMO
 * gatilho determinístico que
 * `apps/shipping-service/src/application/stock-reserved.handler.ts` usa na
 * coreografia.
 */
function isOutOfCoverage(zipCode: string): boolean {
  return zipCode.startsWith('00000');
}

/**
 * Executor "burro": ver comentário em `PaymentExecutorService` — mesma
 * justificativa vale aqui (sem banco, sem outbox, sem idempotência própria).
 */
@Injectable()
export class ShippingExecutorService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-shipping-executor`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.shippingExecutor,
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.shippingExecutor],
      producer: this.producer,
      handler: (ctx) => this.handle(ctx.envelope as unknown as CreateShipmentCommand),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }

  private async handle(envelope: CreateShipmentCommand): Promise<void> {
    const { orchestratedOrderId, address } = envelope.payload;
    const outOfCoverage = isOutOfCoverage(address.zipCode);

    const response = createEvent(orchestrationEvents.executorResponded, {
      aggregateId: orchestratedOrderId,
      correlationId: envelope.correlationId,
      causationId: envelope.eventId,
      producer: 'shipping-executor@0.1.0',
      payload: {
        orchestratedOrderId,
        step: 'shipping',
        outcome: outOfCoverage ? 'failure' : 'success',
        ...(outOfCoverage
          ? { reason: 'CEP fora da área de cobertura simulada (gatilho determinístico: CEP inicia com 00000)' }
          : {}),
      },
    });

    await this.producer.publish(orchestrationEvents.executorResponded.topic, response);
  }
}
