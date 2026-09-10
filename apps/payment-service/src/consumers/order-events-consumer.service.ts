import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { EventProducer, KafkaConsumerRuntime, type MessageContext } from '@ecommerce/kafka';
import { CONSUMER_GROUPS, TOPICS, orderEvents, parseAs } from '@ecommerce/contracts';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { AuthorizePaymentUseCase } from '../application/authorize-payment.use-case.js';

@Injectable()
export class OrderEventsConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-payment-service-consumer`,
  });
  private readonly runtime = new KafkaConsumerRuntime({
    brokers: env.KAFKA_BROKERS,
    groupId: CONSUMER_GROUPS.payment,
    sourceTopics: [TOPICS.orders],
    producer: this.producer,
    handler: (ctx) => this.handle(ctx),
  });

  constructor(private readonly authorizePayment: AuthorizePaymentUseCase) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime.stop();
    await this.producer.disconnect();
  }

  private async handle(ctx: MessageContext): Promise<void> {
    if (ctx.envelope.eventType !== 'order.created') {
      // ecommerce.orders.v1 também carrega order.confirmed/order.cancelled —
      // não é assunto do Payment. Ignora sem erro, offset comita normal.
      return;
    }

    const orderCreated = parseAs(orderEvents.orderCreated, ctx.envelope);
    await this.authorizePayment.execute(orderCreated);
  }
}
