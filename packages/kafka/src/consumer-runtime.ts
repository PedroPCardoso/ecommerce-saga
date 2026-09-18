import { Kafka, logLevel, type Consumer, type EachMessagePayload } from 'kafkajs';
import {
  MAX_RETRY_ATTEMPTS,
  RETRY_LADDER,
  deadLetterTopic,
  parseEvent,
  retryTopic,
  type ConsumerGroup,
  type UnknownEnvelope,
} from '@ecommerce/contracts';
import type { EventProducer } from './producer.js';
import { classifyError } from './error-classification.js';
import { buildRedirectHeaders } from './retry-headers.js';
import { context, propagation } from '@opentelemetry/api';
import { dlqMessagesTotal, kafkaConsumerLag } from '@ecommerce/observability';

export interface MessageContext {
  envelope: UnknownEnvelope;
}

export type MessageHandler = (ctx: MessageContext) => Promise<void>;

export interface KafkaConsumerRuntimeOptions {
  brokers: string[];
  groupId: ConsumerGroup;
  sourceTopics: readonly string[];
  handler: MessageHandler;
  producer: EventProducer;
  clientId?: string;
}

type KafkaMessage = EachMessagePayload['message'];

/**
 * Consumidor gerenciado: commit manual pós-processamento, escada de retry em
 * tópicos dedicados e desvio para DLT. Um consumidor para os tópicos de
 * negócio + um consumidor por degrau da escada (por tópico de origem),
 * todos compartilhando o mesmo handler e producer.
 *
 * Por que kafkajs direto, não @nestjs/microservices: o transport do Nest
 * abstrai o commit de offset e dificulta commit manual pós-transação
 * (docs/PLAN.md, armadilha #1).
 */
export class KafkaConsumerRuntime {
  private readonly kafka: Kafka;
  private readonly groupId: ConsumerGroup;
  private readonly sourceTopics: readonly string[];
  private readonly handler: MessageHandler;
  private readonly producer: EventProducer;
  private consumers: Consumer[] = [];
  private lagPollTimer: NodeJS.Timeout | null = null;

  constructor(opts: KafkaConsumerRuntimeOptions) {
    this.kafka = new Kafka({
      clientId: opts.clientId ?? `${opts.groupId}-consumer`,
      brokers: opts.brokers,
      logLevel: logLevel.ERROR,
      retry: { retries: 5, initialRetryTime: 300 },
    });
    this.groupId = opts.groupId;
    this.sourceTopics = opts.sourceTopics;
    this.handler = opts.handler;
    this.producer = opts.producer;
  }

  async start(): Promise<void> {
    const main = this.kafka.consumer({ groupId: this.groupId, sessionTimeout: 30_000 });
    await main.connect();
    await main.subscribe({ topics: [...this.sourceTopics] });
    await main.run({
      autoCommit: false,
      eachMessage: (payload) => this.processMainMessage(main, payload),
    });
    this.consumers.push(main);

    for (const sourceTopic of this.sourceTopics) {
      for (let attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt += 1) {
        const topic = retryTopic(sourceTopic, this.groupId, attempt);
        const rungGroupId = `${this.groupId}-${sourceTopic}-${RETRY_LADDER[attempt]!.suffix}`;
        const consumer = this.kafka.consumer({ groupId: rungGroupId, sessionTimeout: 30_000 });
        await consumer.connect();
        await consumer.subscribe({ topics: [topic] });
        await consumer.run({
          autoCommit: false,
          eachMessage: (payload) => this.processRetryMessage(consumer, sourceTopic, attempt, payload),
        });
        this.consumers.push(consumer);
      }
    }

    this.lagPollTimer = setInterval(() => {
      void this.pollConsumerLag();
    }, 15_000);
  }

  async stop(): Promise<void> {
    if (this.lagPollTimer) clearInterval(this.lagPollTimer);
    await Promise.all(this.consumers.map((consumer) => consumer.disconnect()));
    this.consumers = [];
  }

  /** Atualiza kafka_consumer_lag = high watermark - offset commitado, por partição. */
  private async pollConsumerLag(): Promise<void> {
    const admin = this.kafka.admin();
    try {
      await admin.connect();
      for (const topic of this.sourceTopics) {
        const [committed, watermarks] = await Promise.all([
          admin.fetchOffsets({ groupId: this.groupId, topics: [topic] }),
          admin.fetchTopicOffsets(topic),
        ]);
        const committedByPartition = new Map(committed[0]?.partitions.map((p) => [p.partition, p.offset]) ?? []);
        for (const wm of watermarks) {
          const committedOffset = Number(committedByPartition.get(wm.partition) ?? '0');
          const lag = Math.max(0, Number(wm.high) - committedOffset);
          kafkaConsumerLag.set({ group: this.groupId, topic, partition: String(wm.partition) }, lag);
        }
      }
    } catch {
      // Falha ao medir lag não pode derrubar o consumidor real — é telemetria, não
      // efeito de negócio. Próxima passada (15s) tenta de novo.
    } finally {
      await admin.disconnect();
    }
  }

  private async processMainMessage(consumer: Consumer, payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    const commit = () =>
      consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);

    let envelope: UnknownEnvelope;
    try {
      envelope = this.parse(message.value);
    } catch (error) {
      await this.sendToDlt(topic, partition, message, error, 0);
      await commit();
      return;
    }

    const extractedContext = propagation.extract(context.active(), this.headersToRecord(message.headers));
    try {
      await context.with(extractedContext, () => this.handler({ envelope }));
    } catch (error) {
      await this.route(topic, partition, message, error, 0);
      await commit();
      return;
    }

    // O handler teve SUCESSO: uma falha aqui é problema de infraestrutura do commit em
    // si (conexão caiu, coordinator trocou), não do processamento — não pode ser
    // reclassificada como falha de handler e desviada para retry/DLT, ou uma mensagem
    // já efetivada seria republicada. Deixa propagar: o offset não avança, o kafkajs
    // reentrega esta mesma mensagem, e reprocessar é seguro porque o handler é
    // idempotente (markProcessed).
    await commit();
  }

  /** Processa uma mensagem que chegou a um degrau da escada de retry. Comita no PRÓPRIO tópico de retry (onde a mensagem está), não no tópico de origem. */
  private async processRetryMessage(
    consumer: Consumer,
    sourceTopic: string,
    attempt: number,
    payload: EachMessagePayload,
  ): Promise<void> {
    // Degraus de 1m/10m excedem o sessionTimeout (30s): um único `sleep`
    // bloqueando o `eachMessage` inteiro nunca dá chance ao kafkajs de mandar
    // heartbeat (isso só acontece ENTRE mensagens, não durante uma). Sem
    // heartbeat o coordinator expulsa o membro do grupo antes do delay
    // terminar, o commit seguinte falha, e a mensagem nunca avança —
    // trava naquele degrau para sempre em vez de escalar ou cair na DLT.
    // `payload.heartbeat()` é a única forma de manter o membro vivo durante
    // um processamento longo; chamamos a cada poucos segundos, nunca mais
    // espaçado que `heartbeatInterval` (3s, config do produtor/consumidor).
    await sleepWithHeartbeat(RETRY_LADDER[attempt]!.delayMs, payload.heartbeat);
    const { topic, partition, message } = payload;
    const commit = () =>
      consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);

    let envelope: UnknownEnvelope;
    try {
      envelope = this.parse(message.value);
    } catch (error) {
      await this.sendToDlt(sourceTopic, partition, message, error, attempt + 1);
      await commit();
      return;
    }

    const extractedContext = propagation.extract(context.active(), this.headersToRecord(message.headers));
    try {
      await context.with(extractedContext, () => this.handler({ envelope }));
    } catch (error) {
      await this.route(sourceTopic, partition, message, error, attempt + 1);
      await commit();
      return;
    }

    // Mesmo raciocínio de processMainMessage: sucesso do handler + falha do commit não
    // é falha de processamento — deixa propagar em vez de desviar para o próximo degrau.
    await commit();
  }

  private headersToRecord(headers: EachMessagePayload['message']['headers']): Record<string, string> {
    const record: Record<string, string> = {};
    for (const [key, value] of Object.entries(headers ?? {})) {
      if (value !== undefined) record[key] = value.toString();
    }
    return record;
  }

  private parse(value: Buffer | null): UnknownEnvelope {
    if (!value) throw new Error('Mensagem sem payload');
    const { event } = parseEvent(JSON.parse(value.toString()));
    return event as UnknownEnvelope;
  }

  /** Decide entre o próximo degrau da escada ou a DLT, e publica lá. */
  private async route(
    sourceTopic: string,
    partition: number,
    message: KafkaMessage,
    error: unknown,
    retryCount: number,
  ): Promise<void> {
    const errorClass = classifyError(error);

    if (errorClass === 'permanent' || retryCount >= MAX_RETRY_ATTEMPTS) {
      await this.sendToDlt(sourceTopic, partition, message, error, retryCount);
      return;
    }

    const headers = buildRedirectHeaders({
      originalTopic: sourceTopic,
      originalPartition: partition,
      originalOffset: message.offset,
      retryCount: retryCount + 1,
      firstFailureAt: firstFailureAt(message, retryCount),
      error,
      consumerGroup: this.groupId,
    });

    const nextTopic = retryTopic(sourceTopic, this.groupId, retryCount);
    await this.producer.publishRaw(nextTopic, message.value, headers, message.key);
    // Nunca logar o payload (pode carregar PII, A09) — só metadados de
    // roteamento. Sem isto, uma mensagem desviada não deixa rastro nenhum em
    // lugar algum (docs/PLAN.md armadilha #8: DLQ/retry vira cemitério).
    console.warn(
      `[kafka] ${sourceTopic} → ${nextTopic} (grupo=${this.groupId}, tentativa=${retryCount + 1}, erro=${errorMessageOf(error)})`,
    );
  }

  private async sendToDlt(
    sourceTopic: string,
    partition: number,
    message: KafkaMessage,
    error: unknown,
    retryCount: number,
  ): Promise<void> {
    const headers = buildRedirectHeaders({
      originalTopic: sourceTopic,
      originalPartition: partition,
      originalOffset: message.offset,
      retryCount,
      firstFailureAt: firstFailureAt(message, retryCount),
      error,
      consumerGroup: this.groupId,
    });

    const dlt = deadLetterTopic(sourceTopic, this.groupId);
    await this.producer.publishRaw(dlt, message.value, headers, message.key);
    console.error(
      `[kafka] ${sourceTopic} → DLT ${dlt} (grupo=${this.groupId}, tentativas=${retryCount}, erro=${errorMessageOf(error)})`,
    );
    dlqMessagesTotal.inc({ topic: sourceTopic, consumerGroup: this.groupId });
  }
}

function errorMessageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function firstFailureAt(message: KafkaMessage, retryCount: number): string {
  if (retryCount === 0) return new Date().toISOString();
  const existing = message.headers?.['x-first-failure-at'];
  return existing ? existing.toString() : new Date().toISOString();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Dorme `totalMs`, mas chama `heartbeat()` a cada `intervalMs` (bem abaixo do
 * `sessionTimeout` de 30s) para o kafkajs não expulsar o consumidor do grupo
 * enquanto o degrau de retry espera. Ver comentário em `processRetryMessage`.
 */
export async function sleepWithHeartbeat(
  totalMs: number,
  heartbeat: () => Promise<void>,
  intervalMs = 3_000,
): Promise<void> {
  let remaining = totalMs;
  while (remaining > 0) {
    const step = Math.min(intervalMs, remaining);
    await sleep(step);
    remaining -= step;
    try {
      await heartbeat();
    } catch {
      // Um heartbeat isolado falhando (ex.: rebalance em andamento) não pode
      // abortar o degrau inteiro sem passar pelo try/catch de
      // parse+handler — se o grupo realmente expulsou o membro, o próximo
      // `commitOffsets` vai falhar por conta própria e isso já é tratado
      // (a mensagem é reprocessada na próxima passada do consumer). Deixar
      // esta exceção subir aqui só trocaria uma falha tratável por uma
      // não-tratada no meio do sleep.
    }
  }
}
