import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { NotificationEventHandler } from '../application/notification-event.handler.js';

@Injectable()
export class NotificationConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-notification-service-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly handler: NotificationEventHandler) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.notification,
      // SUBSCRIPTIONS[CONSUMER_GROUPS.notification] = os 4 tópicos de
      // negócio — Notification é o único consumidor que já assina tudo
      // desde esta fase, sem nada a deferir para depois.
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.notification],
      producer: this.producer,
      handler: (ctx) => this.handler.handle(ctx.envelope),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }
}
