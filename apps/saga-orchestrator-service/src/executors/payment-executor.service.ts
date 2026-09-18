import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { CONSUMER_GROUPS, SUBSCRIPTIONS, createEvent, orchestrationEvents, type EventOf } from '@ecommerce/contracts';
import { EventProducer, KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';

type AuthorizePaymentCommand = EventOf<typeof orchestrationEvents.authorizePaymentCommand>;

/**
 * Terminação ".13" do valor em reais == amountCents % 100 === 13 — MESMO gatilho
 * determinístico que `AuthorizePaymentUseCase` usa na coreografia
 * (apps/payment-service/src/application/authorize-payment.use-case.ts):
 * consistência de comportamento observável entre as duas versões é o que torna a
 * comparação da Fase 11 (ADR-0012) válida.
 */
function isDeclined(amountCents: number): boolean {
  return amountCents % 100 === 13;
}

/**
 * Executor "burro": só consome o comando e responde. SEM banco, SEM outbox, SEM
 * idempotência própria — não precisam: um único processo (`OrchestratorService`)
 * decide o fluxo inteiro, não há concorrência de quem publica o quê. Nenhum efeito
 * de domínio real é praticado aqui: é simulação, ao lado da coreografia real, nunca
 * em substituição a ela (Global Constraints do plano da Fase 11).
 */
@Injectable()
export class PaymentExecutorService implements OnModuleInit, OnModuleDestroy {
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-payment-executor`,
  });
  private runtime: KafkaConsumerRuntime | null = null;

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.paymentExecutor,
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.paymentExecutor],
      producer: this.producer,
      handler: (ctx) => this.handle(ctx.envelope as unknown as AuthorizePaymentCommand),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
    await this.producer.disconnect();
  }

  private async handle(envelope: AuthorizePaymentCommand): Promise<void> {
    const { orchestratedOrderId, amountCents } = envelope.payload;
    const declined = isDeclined(amountCents);

    const response = createEvent(orchestrationEvents.executorResponded, {
      aggregateId: orchestratedOrderId,
      correlationId: envelope.correlationId,
      causationId: envelope.eventId,
      producer: 'payment-executor@0.1.0',
      payload: {
        orchestratedOrderId,
        step: 'payment',
        outcome: declined ? 'failure' : 'success',
        ...(declined ? { reason: 'Cartão recusado pelo emissor (simulação determinística)' } : {}),
      },
    });

    await this.producer.publish(orchestrationEvents.executorResponded.topic, response);
  }
}
