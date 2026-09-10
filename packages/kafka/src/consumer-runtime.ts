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
        const rungGroupId = `${this.groupId}-${RETRY_LADDER[attempt]!.suffix}`;
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
  }

  async stop(): Promise<void> {
    await Promise.all(this.consumers.map((consumer) => consumer.disconnect()));
    this.consumers = [];
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

    try {
      await this.handler({ envelope });
      await commit();
    } catch (error) {
      await this.route(topic, partition, message, error, 0);
      await commit();
    }
  }

  /** Processa uma mensagem que chegou a um degrau da escada de retry. Comita no PRÓPRIO tópico de retry (onde a mensagem está), não no tópico de origem. */
  private async processRetryMessage(
    consumer: Consumer,
    sourceTopic: string,
    attempt: number,
    payload: EachMessagePayload,
  ): Promise<void> {
    await sleep(RETRY_LADDER[attempt]!.delayMs);
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

    try {
      await this.handler({ envelope });
      await commit();
    } catch (error) {
      await this.route(sourceTopic, partition, message, error, attempt + 1);
      await commit();
    }
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

    await this.producer.publishRaw(
      retryTopic(sourceTopic, this.groupId, retryCount),
      message.value,
      headers,
      message.key,
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

    await this.producer.publishRaw(
      deadLetterTopic(sourceTopic, this.groupId),
      message.value,
      headers,
      message.key,
    );
  }
}

function firstFailureAt(message: KafkaMessage, retryCount: number): string {
  if (retryCount === 0) return new Date().toISOString();
  const existing = message.headers?.['x-first-failure-at'];
  return existing ? existing.toString() : new Date().toISOString();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
