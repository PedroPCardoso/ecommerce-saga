import { CompressionTypes, Kafka, logLevel, type Producer } from 'kafkajs';
import type { UnknownEnvelope } from '@ecommerce/contracts';

export interface EventProducerOptions {
  brokers: string[];
  clientId: string;
}

/**
 * Produtor com as garantias que a saga exige: `idempotent: true` faz o
 * broker deduplicar por (producerId, sequence) — protege contra retry
 * interno do cliente, não contra republicação do relay depois de um
 * reinício (isso é o par outbox+idempotency). Chave SEMPRE o aggregateId do
 * envelope — nunca escolhida pelo chamador — porque é isso que garante que
 * todo evento de um pedido cai na mesma partição (ADR-0009).
 *
 * Compressão: GZIP (builtin do kafkajs). docs/PLAN.md pede zstd, mas isso
 * exige um codec nativo adicional (@kafkajs/zstd) — dívida documentada, não
 * necessária para o sistema funcionar.
 */
export class EventProducer {
  private readonly kafka: Kafka;
  private producer: Producer | null = null;

  constructor(opts: EventProducerOptions) {
    this.kafka = new Kafka({
      clientId: opts.clientId,
      brokers: opts.brokers,
      logLevel: logLevel.ERROR,
      retry: { retries: 5, initialRetryTime: 300 },
    });
  }

  async connect(): Promise<void> {
    this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 5 });
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    this.producer = null;
  }

  async publish(
    topic: string,
    envelope: UnknownEnvelope,
    headers: Record<string, string> = {},
  ): Promise<void> {
    this.assertConnected();
    await this.producer!.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{ key: envelope.aggregateId, value: JSON.stringify(envelope), headers }],
    });
  }

  /** Usado pelo consumer runtime (Task 4) para redirecionar bytes originais para retry/DLT sem re-serializar. */
  async publishRaw(
    topic: string,
    value: Buffer | string | null,
    headers: Record<string, string>,
    key?: Buffer | string | null,
  ): Promise<void> {
    this.assertConnected();
    await this.producer!.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{ key: key ?? null, value, headers }],
    });
  }

  private assertConnected(): void {
    if (!this.producer) {
      throw new Error('EventProducer usado antes de connect()');
    }
  }
}
