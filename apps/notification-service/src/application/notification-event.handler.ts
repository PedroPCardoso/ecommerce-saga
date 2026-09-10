import { Injectable } from '@nestjs/common';
import { markProcessed } from '@ecommerce/idempotency';
import {
  CONSUMER_GROUPS,
  type EventOf,
  type UnknownEnvelope,
  type inventoryEvents,
  type orderEvents,
  type paymentEvents,
  type shippingEvents,
} from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { MailerService, type SendEmailInput } from '../infrastructure/mailer.service.js';

type OrderCreatedEvent = EventOf<typeof orderEvents.orderCreated>;
type OrderConfirmedEvent = EventOf<typeof orderEvents.orderConfirmed>;
type OrderCancelledEvent = EventOf<typeof orderEvents.orderCancelled>;
type PaymentApprovedEvent = EventOf<typeof paymentEvents.paymentApproved>;
type PaymentFailedEvent = EventOf<typeof paymentEvents.paymentFailed>;
type StockUnavailableEvent = EventOf<typeof inventoryEvents.stockUnavailable>;
type ShipmentCreatedEvent = EventOf<typeof shippingEvents.shipmentCreated>;
type ShipmentFailedEvent = EventOf<typeof shippingEvents.shipmentFailed>;

/**
 * Serviço só-consumidor: nenhum evento de domínio sai daqui, então não há
 * outbox — só @ecommerce/idempotency, usada de um jeito DIFERENTE dos
 * outros consumidores desta saga. `markProcessed` só roda DEPOIS do e-mail
 * ser enviado com sucesso, não antes — o efeito aqui é uma chamada SMTP
 * externa, não uma escrita de domínio que possa ser desfeita num rollback.
 * Se o processo morrer ENTRE o envio ter sucesso e essa gravação, uma
 * reentrega pode reenviar o mesmo e-mail — trade-off aceito e documentado
 * no plano (Task 6): duplicar uma NOTIFICAÇÃO é inofensivo, diferente de
 * duplicar um pagamento ou uma reserva de estoque.
 */
@Injectable()
export class NotificationEventHandler {
  constructor(
    private readonly prisma: PrismaService,
    private readonly mailer: MailerService,
  ) {}

  async handle(envelope: UnknownEnvelope): Promise<void> {
    const already = await this.prisma.client.processedMessage.findUnique({
      where: {
        eventId_consumerGroup: { eventId: envelope.eventId, consumerGroup: CONSUMER_GROUPS.notification },
      },
    });
    if (already) return; // já processado — não reenvia.

    if (envelope.eventType === 'order.created') {
      // Gravação otimista e idempotente (upsert) — INDEPENDENTE do envio do
      // e-mail ter sucesso. Repetir isto em todo retry é inofensivo, e
      // adiantar o aprendizado do customerId destrava mais cedo qualquer
      // payment.approved/stock.unavailable/shipment.* deste pedido que já
      // esteja esperando no retry por falta dele.
      await this.recordKnownOrder(envelope as OrderCreatedEvent);
    }

    const email = await this.buildEmail(envelope);
    if (!email) {
      // eventType sem template nesta fase (stock.reserved, payment.refunded,
      // stock.released, ...) — nada a enviar.
      await this.markAsProcessed(envelope.eventId);
      return;
    }

    // Envia PRIMEIRO, fora de transação — só registra processed_messages
    // DEPOIS do envio ter sucesso (ver comentário da classe).
    await this.mailer.send(email);
    await this.markAsProcessed(envelope.eventId);
  }

  private async markAsProcessed(eventId: string): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      await markProcessed(tx, eventId, CONSUMER_GROUPS.notification);
    });
  }

  private async recordKnownOrder(event: OrderCreatedEvent): Promise<void> {
    await this.prisma.client.knownOrder.upsert({
      where: { orderId: event.payload.orderId },
      create: { orderId: event.payload.orderId, customerId: event.payload.customerId },
      update: { customerId: event.payload.customerId },
    });
  }

  private async resolveCustomerId(orderId: string): Promise<string> {
    const known = await this.prisma.client.knownOrder.findUnique({ where: { orderId } });
    if (!known) {
      /*
       * Mesmo padrão de Shipping/Inventory (roadmap, decisão técnica #8):
       * nenhum destes eventos carrega customerId — só orderId (confirmado
       * lendo packages/contracts/src/events/{payment,inventory,shipping}.ts).
       * Erro COMUM (sem `.permanent`) — RETRIÁVEL — dá tempo para o
       * order.created deste pedido ser processado.
       *
       * Diferente de Shipping/Inventory, aqui NÃO precisamos do truque de
       * "lançar dentro da mesma transação que já rodou markProcessed": neste
       * ponto ainda não chamamos markProcessed nem enviamos e-mail nenhum —
       * markProcessed só acontece no fim, depois do envio ter sucesso —
       * então não há nada a desfazer, o erro simplesmente sobe.
       */
      throw new Error(
        `KnownOrder ainda não disponível para orderId=${orderId} — order.created não processado ainda`,
      );
    }
    return known.customerId;
  }

  private async buildEmail(envelope: UnknownEnvelope): Promise<SendEmailInput | null> {
    switch (envelope.eventType) {
      case 'order.created': {
        const event = envelope as OrderCreatedEvent;
        return {
          to: emailFor(event.payload.customerId),
          subject: `Recebemos seu pedido ${event.payload.orderId}`,
          text: `Seu pedido ${event.payload.orderId} foi recebido e está sendo processado.`,
        };
      }
      case 'order.confirmed': {
        const event = envelope as OrderConfirmedEvent;
        return {
          to: emailFor(event.payload.customerId),
          subject: `Pedido ${event.payload.orderId} confirmado`,
          text: `Seu pedido ${event.payload.orderId} foi confirmado. Obrigado pela compra!`,
        };
      }
      case 'order.cancelled': {
        const event = envelope as OrderCancelledEvent;
        return {
          to: emailFor(event.payload.customerId),
          subject: `Pedido ${event.payload.orderId} cancelado`,
          text: `Seu pedido ${event.payload.orderId} foi cancelado. Motivo: ${event.payload.reason}.`,
        };
      }
      case 'payment.approved': {
        const event = envelope as PaymentApprovedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Pagamento do pedido ${event.payload.orderId} aprovado`,
          text: `O pagamento do seu pedido ${event.payload.orderId} foi aprovado.`,
        };
      }
      case 'payment.failed': {
        const event = envelope as PaymentFailedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Pagamento do pedido ${event.payload.orderId} recusado`,
          text: `O pagamento do seu pedido ${event.payload.orderId} foi recusado.`,
        };
      }
      case 'stock.unavailable': {
        const event = envelope as StockUnavailableEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Item indisponível no pedido ${event.payload.orderId}`,
          text: `Um ou mais itens do pedido ${event.payload.orderId} ficaram indisponíveis.`,
        };
      }
      case 'shipment.created': {
        const event = envelope as ShipmentCreatedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Pedido ${event.payload.orderId} enviado — rastreio ${event.payload.trackingCode}`,
          text: `Seu pedido ${event.payload.orderId} foi enviado. Código de rastreio: ${event.payload.trackingCode}.`,
        };
      }
      case 'shipment.failed': {
        const event = envelope as ShipmentFailedEvent;
        const customerId = await this.resolveCustomerId(event.payload.orderId);
        return {
          to: emailFor(customerId),
          subject: `Problema no envio do pedido ${event.payload.orderId}`,
          text: `Houve um problema para enviar o pedido ${event.payload.orderId}.`,
        };
      }
      default:
        // stock.reserved, payment.refunded, stock.released e qualquer
        // eventType futuro — sem template nesta fase.
        return null;
    }
  }
}

/**
 * Nenhum evento carrega e-mail do cliente (confirmado lendo todos os
 * schemas em packages/contracts/src/events/*.ts — não existe esse campo).
 * Endereço FICTÍCIO determinístico a partir do customerId — sempre
 * @example.com, nunca domínio real: exigência de política de PII do
 * projeto (mascarar dado real / usar fictício).
 */
function emailFor(customerId: string): string {
  return `cliente-${customerId}@example.com`;
}
