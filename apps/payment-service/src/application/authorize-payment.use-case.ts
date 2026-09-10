import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import {
  CONSUMER_GROUPS,
  PAYMENT_FAILURE_CODE,
  createEvent,
  paymentEvents,
  type EventOf,
  type orderEvents,
} from '@ecommerce/contracts';
import { insertOutboxRow } from '@ecommerce/outbox';
import { markProcessed } from '@ecommerce/idempotency';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

export type OrderCreatedEvent = EventOf<typeof orderEvents.orderCreated>;

/**
 * Acima deste valor (em centavos), a autorização é propositalmente lenta —
 * gatilho determinístico (docs/PLAN.md §4.5) para a Fase 6 exercitar o
 * sweeper de timeout de saga. Nada de Math.random(): teste que não é
 * determinístico não é teste.
 */
const SLOW_PAYMENT_THRESHOLD_CENTS = 10_000;
const SLOW_PAYMENT_DELAY_MS = 30_000;

/** Terminação ".13" do valor em reais == totalAmountCents % 100 === 13. */
function isDeclined(amountCents: number): boolean {
  return amountCents % 100 === 13;
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

@Injectable()
export class AuthorizePaymentUseCase {
  /**
   * Campo público, não parâmetro de construtor: esta classe é um provider
   * do Nest (`AppModule` a instancia via DI), e um parâmetro de construtor
   * tipado como função não tem um token de injeção válido — o Nest tentaria
   * resolver uma dependência para o tipo `Function` e o bootstrap quebraria
   * com "Nest can't resolve dependency". Como campo público com valor
   * padrão, o Nest só precisa injetar `PrismaService`, e o teste substitui
   * `sleep` diretamente na instância (`useCase.sleep = vi.fn()...`) sem
   * esperar 30s de verdade a cada suíte.
   */
  sleep: (ms: number) => Promise<void> = defaultSleep;

  constructor(private readonly prisma: PrismaService) {}

  async execute(envelope: OrderCreatedEvent): Promise<void> {
    if (envelope.payload.totalAmountCents > SLOW_PAYMENT_THRESHOLD_CENTS) {
      await this.sleep(SLOW_PAYMENT_DELAY_MS);
    }

    await this.prisma.client.$transaction(async (tx) => {
      const isNew = await markProcessed(tx, envelope.eventId, CONSUMER_GROUPS.payment);
      if (!isNew) return; // reentrega do mesmo order.created — efeito já aplicado, nada a fazer

      const paymentId = randomUUID();
      const now = new Date();
      const declined = isDeclined(envelope.payload.totalAmountCents);

      let authorizationCode: string | null = null;
      let failureCode: string | null = null;
      let outEnvelope;

      if (declined) {
        failureCode = PAYMENT_FAILURE_CODE.CARD_DECLINED;
        outEnvelope = createEvent(paymentEvents.paymentFailed, {
          aggregateId: envelope.payload.orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'payment-service@0.1.0',
          payload: {
            paymentId,
            orderId: envelope.payload.orderId,
            amountCents: envelope.payload.totalAmountCents,
            currency: envelope.payload.currency,
            failureCode: PAYMENT_FAILURE_CODE.CARD_DECLINED,
            reason: 'Cartão recusado pelo emissor (simulação determinística)',
            failedAt: now.toISOString(),
          },
        });
      } else {
        authorizationCode = randomUUID();
        outEnvelope = createEvent(paymentEvents.paymentApproved, {
          aggregateId: envelope.payload.orderId,
          correlationId: envelope.correlationId,
          causationId: envelope.eventId,
          producer: 'payment-service@0.1.0',
          payload: {
            paymentId,
            orderId: envelope.payload.orderId,
            amountCents: envelope.payload.totalAmountCents,
            currency: envelope.payload.currency,
            authorizationCode,
            // Mock fixo — nunca PAN/CVV real, mesmo em ambiente de estudo (A04/ADR-0011).
            instrument: { gatewayToken: randomUUID(), cardLast4: '4242', brand: 'VISA' },
            approvedAt: now.toISOString(),
          },
        });
      }

      await tx.payment.create({
        data: {
          id: paymentId,
          orderId: envelope.payload.orderId,
          amountCents: envelope.payload.totalAmountCents,
          currency: envelope.payload.currency,
          status: declined ? 'FAILED' : 'AUTHORIZED',
          authorizationCode,
          failureCode,
          createdAt: now,
        },
      });

      await insertOutboxRow(tx, {
        eventId: outEnvelope.eventId,
        aggregateId: envelope.payload.orderId,
        aggregateType: 'payment',
        eventType: outEnvelope.eventType,
        envelope: outEnvelope,
      });
    });
  }
}
