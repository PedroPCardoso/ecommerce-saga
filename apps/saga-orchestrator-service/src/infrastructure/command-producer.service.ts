import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { EventProducer } from '@ecommerce/kafka';
import { env } from '../env.js';

/**
 * Único `EventProducer` do serviço: compartilhado pelo endpoint HTTP (primeiro
 * comando, `OrchestratedOrdersController`) e pelo `OrchestratorService` (comandos
 * seguintes) — e reaproveitado por este último também como producer interno do
 * `KafkaConsumerRuntime` (redireciona para retry/DLT em caso de falha).
 *
 * Não há outbox aqui: ver o comentário em `OrchestratorService` para o porquê disso
 * ser uma escolha válida neste harness, não um atalho perigoso copiado sem pensar.
 */
@Injectable()
export class CommandProducerService implements OnModuleInit, OnModuleDestroy {
  readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-saga-orchestrator-producer`,
  });

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.producer.disconnect();
  }
}
