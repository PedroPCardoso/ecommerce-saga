import { Injectable } from '@nestjs/common';
import type { UnknownEnvelope } from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { OrderCreatedHandler, type OrderCreatedEvent } from './order-created.handler.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PaymentApprovedHandler, type PaymentApprovedEvent } from './payment-approved.handler.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { ShipmentFailedHandler, type ShipmentFailedEvent } from './shipment-failed.handler.js';

/**
 * Decide qual handler chamar a partir de `envelope.eventType`. O envelope já
 * chegou validado (Zod, dentro do KafkaConsumerRuntime) contra o schema
 * exato do seu tipo+versão — os casts abaixo só satisfazem o TypeScript.
 */
@Injectable()
export class InventoryEventRouter {
  constructor(
    private readonly orderCreatedHandler: OrderCreatedHandler,
    private readonly paymentApprovedHandler: PaymentApprovedHandler,
    private readonly shipmentFailedHandler: ShipmentFailedHandler,
  ) {}

  async route(envelope: UnknownEnvelope): Promise<void> {
    switch (envelope.eventType) {
      case 'order.created':
        await this.orderCreatedHandler.handle(envelope as OrderCreatedEvent);
        return;
      case 'payment.approved':
        await this.paymentApprovedHandler.handle(envelope as PaymentApprovedEvent);
        return;
      case 'shipment.failed':
        await this.shipmentFailedHandler.handle(envelope as ShipmentFailedEvent);
        return;
      default:
        // payment.failed (matriz de compensação: nada a fazer aqui), order.confirmed,
        // order.cancelled, stock.unavailable (o próprio Inventory que publicou),
        // stock.released (o próprio Inventory que publicou) — não são assunto de um
        // handler novo aqui. Ignora e deixa o offset comitar normalmente.
        return;
    }
  }
}
