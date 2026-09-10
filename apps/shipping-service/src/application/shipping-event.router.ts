import { Injectable } from '@nestjs/common';
import type { UnknownEnvelope } from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { OrderCreatedHandler, type OrderCreatedEvent } from './order-created.handler.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { StockReservedHandler, type StockReservedEvent } from './stock-reserved.handler.js';

/**
 * Decide qual handler chamar a partir de `envelope.eventType`. O envelope já
 * chegou validado (Zod, dentro do KafkaConsumerRuntime) contra o schema
 * exato do seu tipo+versão — os casts abaixo só satisfazem o TypeScript.
 */
@Injectable()
export class ShippingEventRouter {
  constructor(
    private readonly orderCreatedHandler: OrderCreatedHandler,
    private readonly stockReservedHandler: StockReservedHandler,
  ) {}

  async route(envelope: UnknownEnvelope): Promise<void> {
    switch (envelope.eventType) {
      case 'order.created':
        await this.orderCreatedHandler.handle(envelope as OrderCreatedEvent);
        return;
      case 'stock.reserved':
        await this.stockReservedHandler.handle(envelope as StockReservedEvent);
        return;
      default:
        // order.confirmed, order.cancelled, stock.unavailable, stock.released —
        // não são assunto do Shipping nesta fase: stock.unavailable já terminou
        // a saga em cancelamento antes de chegar aqui, e stock.released é a
        // própria compensação de um shipment.failed anterior (não dispara
        // outro envio). Ignora e deixa o offset comitar normalmente.
        return;
    }
  }
}
