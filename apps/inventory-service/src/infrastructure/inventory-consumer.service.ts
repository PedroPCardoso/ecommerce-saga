import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, TOPICS } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { InventoryEventRouter } from '../application/inventory-event.router.js';

@Injectable()
export class InventoryConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-inventory-service-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly router: InventoryEventRouter) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.inventory,
      // Escopo desta fase: só orders (aprende itens) + payments (gatilho da reserva).
      // `shipping` já está em SUBSCRIPTIONS[inventory] — os tópicos de retry/DLT dele já
      // foram criados por `pnpm topics:create` — mas consumir `shipment.failed` (para
      // publicar `stock.released`) só é acrescentado na Fase 5.
      sourceTopics: [TOPICS.orders, TOPICS.payments],
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
