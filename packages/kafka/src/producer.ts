import { CompressionTypes, Kafka, logLevel, type Producer } from 'kafkajs';
import type { UnknownEnvelope } from '@ecommerce/contracts';
import { context, propagation, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';

const tracer = trace.getTracer('@ecommerce/kafka');

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
    // Se `headers` já carrega um `traceparent` (ex.: propagado através da tabela outbox —
    // ver insertOutboxRow em @ecommerce/outbox), usa-o como PAI do novo span: é isto que
    // faz o trace sobreviver ao hop por Postgres entre "consumir uma mensagem" e
    // "publicar o efeito dela" — nenhum contexto em memória atravessa esse hop sozinho,
    // porque o outbox relay roda em um timer completamente desligado da call stack
    // original. Sem traceparent nos headers (ex.: order.created, criado pela requisição
    // HTTP), isto não acha nada para extrair e o span abaixo nasce como raiz de um trace
    // novo — o começo natural da saga.
    const parentContext = propagation.extract(context.active(), headers);
    await tracer.startActiveSpan(
      `kafka.publish ${topic}`,
      { kind: SpanKind.PRODUCER },
      parentContext,
      async (span) => {
        try {
          const tracedHeaders = { ...headers };
          propagation.inject(context.active(), tracedHeaders);
          await this.producer!.send({
            topic,
            compression: CompressionTypes.GZIP,
            messages: [{ key: envelope.aggregateId, value: JSON.stringify(envelope), headers: tracedHeaders }],
          });
        } catch (error) {
          span.recordException(error as Error);
          span.setStatus({ code: SpanStatusCode.ERROR, message: (error as Error).message });
          throw error;
        } finally {
          span.end();
        }
      },
    );
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
