import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { OrderProjectionStore } from '../domain/order-projection.store.js';

/**
 * Mapeia `eventType` para um status legível de negócio, o que a UI mostra na
 * coluna "status". Puramente cosmético — este serviço não é fonte de verdade
 * do estado da saga (o Order Service é), só projeta o que já viu passar.
 */
const STATUS_BY_EVENT_TYPE: Record<string, string> = {
  'order.created': 'PENDING',
  'payment.approved': 'PAYMENT_APPROVED',
  'payment.failed': 'CANCELLED',
  'stock.reserved': 'STOCK_RESERVED',
  'stock.unavailable': 'COMPENSATING',
  'shipment.created': 'CONFIRMED',
  'shipment.failed': 'COMPENSATING',
  'payment.refunded': 'COMPENSATING',
  'stock.released': 'COMPENSATING',
  'order.confirmed': 'CONFIRMED',
  'order.cancelled': 'CANCELLED',
};

/**
 * Serviço só-consumidor, sem outbox, sem banco: nunca produz evento nenhum,
 * só projeta em memória via `OrderProjectionStore`. Diferente dos outros
 * consumidores da saga, um `eventType` desconhecido ou sem handler NUNCA é
 * erro — este serviço não tem efeito de negócio para errar, só um painel de
 * observação (ver Global Constraints do plano da Fase 7b).
 */
@Injectable()
export class SagaObserverConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-saga-observer-consumer`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(private readonly store: OrderProjectionStore) {}

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.sagaObserver,
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.sagaObserver],
      producer: this.producer,
      handler: (ctx) => this.handle(ctx.envelope.aggregateId, ctx.envelope.eventType),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }

  private handle(orderId: string, eventType: string): Promise<void> {
    const status = STATUS_BY_EVENT_TYPE[eventType];
    if (status) {
      this.store.upsert(orderId, { eventType, status });
    }
    // eventType sem entrada no mapa: ignora silenciosamente — sem efeito de
    // negócio para errar, e o roteamento de retry/DLT deste consumer group
    // não tem razão de existir para um observador puramente cosmético.
    return Promise.resolve();
  }
}
