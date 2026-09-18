import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import {
  CONSUMER_GROUPS,
  SUBSCRIPTIONS,
  createEvent,
  orchestrationEvents,
  type Address,
  type EventOf,
  type ReservedItem,
} from '@ecommerce/contracts';
import { KafkaConsumerRuntime } from '@ecommerce/kafka';
import { env } from '../env.js';
import { applyExecutorResponse, type OrchestratorStatus } from './orchestrator-state-machine.js';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { CommandProducerService } from '../infrastructure/command-producer.service.js';

type ExecutorRespondedEvent = EventOf<typeof orchestrationEvents.executorResponded>;

/**
 * O "cérebro" do harness orquestrado (Fase 11, ADR-0012): consome
 * `executorResponded`, aplica `applyExecutorResponse` e publica o PRÓXIMO comando.
 * UM lugar decide o fluxo inteiro — compare com a coreografia, onde cada serviço
 * decide sozinho reagindo a eventos de outros domínios (ver o comentário de
 * `SUBSCRIPTIONS` em packages/contracts/src/topics.ts).
 *
 * Publica direto, SEM outbox: escolha deliberada e PARTE da comparação, não um
 * atalho. Outbox existe para proteger um efeito de domínio contra a falha entre
 * "gravar no meu banco" e "publicar o evento" QUANDO múltiplos escritores
 * concorrentes (outras réplicas do mesmo serviço, outros serviços) poderiam
 * competir por esse mesmo efeito. Aqui não há isso: este é o ÚNICO processo no
 * sistema que decide o próximo passo da saga inteira. Se cair entre o `update` e o
 * `publish`, o pior caso é o pedido ficar visivelmente parado em
 * `orchestrated_orders` (sem o comando seguinte) — nunca um efeito duplicado ou
 * perdido em OUTRO serviço, porque não existe concorrência para o outbox proteger.
 * Esse é o ponto pedagógico central desta fase.
 */
@Injectable()
export class OrchestratorService implements OnModuleInit, OnModuleDestroy {
  private runtime: KafkaConsumerRuntime | null = null;

  constructor(
    private readonly prisma: PrismaService,
    private readonly commandProducer: CommandProducerService,
  ) {}

  async onModuleInit(): Promise<void> {
    this.runtime = new KafkaConsumerRuntime({
      brokers: env.KAFKA_BROKERS,
      groupId: CONSUMER_GROUPS.orchestrator,
      sourceTopics: SUBSCRIPTIONS[CONSUMER_GROUPS.orchestrator],
      producer: this.commandProducer.producer,
      handler: (ctx) => this.handle(ctx.envelope as unknown as ExecutorRespondedEvent),
    });
    await this.runtime.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.runtime?.stop();
  }

  private async handle(envelope: ExecutorRespondedEvent): Promise<void> {
    const { orchestratedOrderId, step, outcome } = envelope.payload;

    const order = await this.prisma.client.orchestratedOrder.findUnique({
      where: { id: orchestratedOrderId },
    });
    if (!order) {
      // Resposta para um pedido que este processo não conhece (replay de tópico
      // antigo ou ambiente cruzado) — nenhuma quantidade de retry resolve isto.
      throw new Error(`OrchestratedOrder ${orchestratedOrderId} não encontrado`);
    }

    const currentStatus = order.status as OrchestratorStatus;
    const nextStatus = applyExecutorResponse(currentStatus, step, outcome);

    if (nextStatus === currentStatus) {
      // Resposta fora de ordem ou reentrega: o comando seguinte já foi publicado da
      // primeira vez. Republicar aqui faria um executor SEM idempotência própria
      // (ver executors/*) executar o mesmo passo duas vezes — este guard É a
      // idempotência deste fluxo, não um detalhe cosmético.
      return;
    }

    await this.prisma.client.orchestratedOrder.update({
      where: { id: orchestratedOrderId },
      data: { status: nextStatus },
    });

    await this.publishNextCommand(
      { id: order.id, items: order.items, address: order.address },
      nextStatus,
      envelope.eventId,
      envelope.correlationId,
    );
  }

  private async publishNextCommand(
    order: { id: string; items: unknown; address: unknown },
    nextStatus: OrchestratorStatus,
    causationId: string,
    correlationId: string,
  ): Promise<void> {
    if (nextStatus === 'AWAITING_STOCK') {
      const envelope = createEvent(orchestrationEvents.reserveStockCommand, {
        aggregateId: order.id,
        correlationId,
        causationId,
        producer: 'saga-orchestrator-service@0.1.0',
        payload: { orchestratedOrderId: order.id, items: order.items as ReservedItem[] },
      });
      await this.commandProducer.producer.publish(orchestrationEvents.reserveStockCommand.topic, envelope);
      return;
    }

    if (nextStatus === 'AWAITING_SHIPMENT') {
      const envelope = createEvent(orchestrationEvents.createShipmentCommand, {
        aggregateId: order.id,
        correlationId,
        causationId,
        producer: 'saga-orchestrator-service@0.1.0',
        payload: { orchestratedOrderId: order.id, address: order.address as Address },
      });
      await this.commandProducer.producer.publish(orchestrationEvents.createShipmentCommand.topic, envelope);
      return;
    }

    // CONFIRMED ou CANCELLED: estado terminal, nada mais a publicar. E é só ISTO
    // que o operador precisa checar para saber por que um pedido parou — ver
    // docs/adr/0012, métrica 3.
  }
}
