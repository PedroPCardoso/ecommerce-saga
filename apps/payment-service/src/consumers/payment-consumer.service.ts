import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { CONSUMER_GROUPS, SUBSCRIPTIONS } from '@ecommerce/contracts';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PaymentEventRouter } from '../application/payment-event.router.js';

@Injectable()
export class PaymentConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-payment-service-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly router: PaymentEventRouter) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.payment,
      // orders (gatilho da autorização) + inventory/shipping (gatilhos de compensação:
      // stock.unavailable e shipment.failed) — os três já declarados em
      // SUBSCRIPTIONS[payment] (packages/contracts/src/topics.ts).
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.payment],
      producer: this.producer,
      handler: (ctx) => this.router.route(ctx.envelope),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }
}
