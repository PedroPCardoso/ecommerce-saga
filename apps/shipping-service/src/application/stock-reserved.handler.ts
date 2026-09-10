import { randomInt, randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { markProcessed } from '@ecommerce/idempotency';
import { insertOutboxRow } from '@ecommerce/outbox';
import {
  CONSUMER_GROUPS,
  createEvent,
  type Address,
  type EventOf,
  type inventoryEvents,
  shippingEvents,
} from '@ecommerce/contracts';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

export type StockReservedEvent = EventOf<typeof inventoryEvents.stockReserved>;

/**
 * Gatilho determinístico de falha de envio (docs/PLAN.md 4.5): CEP que
 * começa com "00000" está fora da área de cobertura simulada. Sem
 * Math.random() — teste não determinístico não é teste.
 */
function isAddressServiceable(address: Address): boolean {
  return !address.zipCode.startsWith('00000');
}

/**
 * Formato de rastreio dos Correios (mock): 2 letras + 9 dígitos + 2 letras.
 * Não determinístico de propósito — só o CAMINHO da saga (created vs.
 * failed) precisa ser determinístico; o valor exato do código de rastreio
 * não afeta nenhum teste.
 */
function generateTrackingCode(): string {
  const digits = randomInt(0, 1_000_000_000).toString().padStart(9, '0');
  return `BR${digits}BR`;
}

/**
 * Gatilho REAL do envio (docs/PLAN.md 4.5). `order.created` só ensina o
 * endereço (OrderCreatedHandler); é `stock.reserved` que decide se cria o
 * Shipment ou publica falha determinística.
 */
@Injectable()
export class StockReservedHandler {
  constructor(private readonly prisma: PrismaService) {}

  async handle(envelope: StockReservedEvent): Promise<void> {
    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.shipping);
      if (!isNew) return; // reentrega do mesmo evento — já decidimos isto antes

      const { orderId } = envelope.payload;
      const knownOrder = await tx.knownOrder.findUnique({ where: { orderId } });

      if (!knownOrder) {
        /*
         * order.created deste pedido ainda não foi processado por este
         * serviço — nada garante ordem ENTRE tópicos diferentes (orders vs
         * inventory). Este throw acontece DENTRO da transação, DEPOIS do
         * markProcessed acima: o Prisma faz ROLLBACK de tudo, inclusive do
         * registro de idempotência. Sem esse rollback, a escada de retry
         * encontraria o (eventId, consumerGroup) já marcado e desistiria
         * silenciosamente, sem nunca ter tentado o envio.
         *
         * Erro sem `.permanent = true` -> classifyError (@ecommerce/kafka)
         * classifica como RETRIÁVEL por padrão -> a escada 5s/1m/10m dá tempo
         * para order.created chegar antes de cair na DLT.
         */
        throw new Error(
          `KnownOrder ${orderId} ainda não visto por este serviço — aguardando order.created`,
        );
      }

      const address = knownOrder.shippingAddress as unknown as Address;
      const now = new Date();

      if (!isAddressServiceable(address)) {
        const failedEnvelope = createEvent(shippingEvents.shipmentFailed, {
          aggregateId: orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'shipping-service@0.1.0',
          payload: {
            orderId,
            failureCode: shippingEvents.SHIPMENT_FAILURE_CODE.ADDRESS_NOT_SERVICEABLE,
            reason:
              'CEP fora da área de cobertura simulada (gatilho determinístico: CEP inicia com 00000)',
            failedAt: now.toISOString(),
          },
        });

        await insertOutboxRow(tx, {
          eventId: failedEnvelope.eventId,
          aggregateId: orderId,
          aggregateType: 'shipment',
          eventType: 'shipment.failed',
          envelope: failedEnvelope,
        });

        // Compensação dupla (Inventory libera estoque via stock.released,
        // Payment estorna via payment.refunded) é responsabilidade de outro
        // plano — este serviço só publica o fato; quem reage a ele não é o
        // Shipping (ver "Escopo e limite deste documento" no topo do plano).
        return;
      }

      const shipmentId = randomUUID();
      const trackingCode = generateTrackingCode();
      const estimatedDeliveryAt = new Date(now.getTime() + 5 * 24 * 60 * 60 * 1000);
      /**
       * URL MOCK apontando para um host controlado por ESTE serviço — nunca
       * uma URL vinda de dado externo/evento. O schema
       * `shippingEvents.shipmentCreated.payload.labelUrl` em
       * @ecommerce/contracts já avisa: "quem consumir isto NÃO deve buscar a
       * URL cegamente" (OWASP A01 — SSRF, docs/PLAN.md seção 8). Este handler
       * só PUBLICA a URL — não existe nenhum client HTTP aqui, de propósito.
       */
      const labelUrl = `http://shipping-service.internal/labels/${shipmentId}`;

      await tx.shipment.create({
        data: {
          id: shipmentId,
          orderId,
          carrier: 'CORREIOS',
          trackingCode,
          labelUrl,
          estimatedDeliveryAt,
          status: 'CREATED',
          createdAt: now,
        },
      });

      const createdEnvelope = createEvent(shippingEvents.shipmentCreated, {
        aggregateId: orderId,
        correlationId: envelope.correlationId,
        causationId: envelope.eventId,
        producer: 'shipping-service@0.1.0',
        payload: {
          shipmentId,
          orderId,
          carrier: 'CORREIOS',
          trackingCode,
          labelUrl,
          estimatedDeliveryAt: estimatedDeliveryAt.toISOString(),
          createdAt: now.toISOString(),
        },
      });

      await insertOutboxRow(tx, {
        eventId: createdEnvelope.eventId,
        aggregateId: orderId,
        aggregateType: 'shipment',
        eventType: 'shipment.created',
        envelope: createdEnvelope,
      });
    });
  }
}
