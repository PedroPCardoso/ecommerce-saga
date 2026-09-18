import { Injectable } from '@nestjs/common';
import { orderEvents, parseAs, type UnknownEnvelope } from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { AuthorizePaymentUseCase } from './authorize-payment.use-case.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { RefundPaymentUseCase } from './refund-payment.use-case.js';

/**
 * Decide qual caso de uso chamar a partir de `envelope.eventType`. O envelope
 * já chegou validado (Zod, dentro do KafkaConsumerRuntime) contra o schema
 * exato do seu tipo+versão.
 */
@Injectable()
export class PaymentEventRouter {
  constructor(
    private readonly authorizePayment: AuthorizePaymentUseCase,
    private readonly refundPayment: RefundPaymentUseCase,
  ) {}

  async route(envelope: UnknownEnvelope): Promise<void> {
    switch (envelope.eventType) {
      case 'order.created':
        await this.authorizePayment.execute(parseAs(orderEvents.orderCreated, envelope));
        return;
      case 'stock.unavailable':
      case 'shipment.failed':
        await this.refundPayment.execute(envelope);
        return;
      default:
        // order.confirmed, order.cancelled, stock.reserved, stock.released — não são
        // assunto do Payment. Ignora e deixa o offset comitar normalmente.
        return;
    }
  }
}
