import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { OrderProjectionHandler } from '../application/order-projection.handler.js';

@Injectable()
export class OrderProjectionConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-order-service-projection-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly handler: OrderProjectionHandler) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.orderProjection,
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.orderProjection],
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
