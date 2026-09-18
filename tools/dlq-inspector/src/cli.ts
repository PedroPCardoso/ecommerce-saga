#!/usr/bin/env node
import { Kafka, logLevel } from 'kafkajs';
import { RETRY_HEADERS } from '@ecommerce/contracts';
import { EventProducer } from '@ecommerce/kafka';
import { maskPii } from './mask.js';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');

function kafka(): Kafka {
  return new Kafka({ clientId: 'dlq-inspector', brokers: KAFKA_BROKERS, logLevel: logLevel.ERROR });
}

/** Lê até `limit` mensagens de um tópico, do início, sem entrar num consumer group persistente. */
async function readTopic(
  topic: string,
  limit: number,
): Promise<Array<{ offset: string; key: string | null; value: string | null; headers: Record<string, string> }>> {
  const client = kafka();
  const consumer = client.consumer({ groupId: `dlq-inspector-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const messages: Array<{ offset: string; key: string | null; value: string | null; headers: Record<string, string> }> = [];
  await new Promise<void>((resolve) => {
    void consumer.run({
      eachMessage: async ({ message }) => {
        if (messages.length >= limit) return;
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) {
          if (v) headers[k] = v.toString();
        }
        messages.push({
          offset: message.offset,
          key: message.key?.toString() ?? null,
          value: message.value?.toString() ?? null,
          headers,
        });
        if (messages.length >= limit) resolve();
      },
    });
    // Não há mais mensagens ou o tópico é pequeno — não fica esperando para sempre.
    setTimeout(resolve, 5_000);
  });

  await consumer.disconnect();
  return messages;
}

async function list(topic: string): Promise<void> {
  const messages = await readTopic(topic, 100);
  if (messages.length === 0) {
    console.log(`Nenhuma mensagem em ${topic}.`);
    return;
  }
  for (const msg of messages) {
    const error = msg.headers[RETRY_HEADERS.lastError] ?? '(sem erro registrado)';
    console.log(`offset=${msg.offset} key=${msg.key} erro="${error}"`);
  }
}

async function show(topic: string, offset: string): Promise<void> {
  const messages = await readTopic(topic, 1000);
  const found = messages.find((m) => m.offset === offset);
  if (!found) {
    console.error(`Offset ${offset} não encontrado em ${topic} (dentro das primeiras 1000 mensagens).`);
    process.exitCode = 1;
    return;
  }
  const parsed = found.value ? JSON.parse(found.value) : null;
  console.log(JSON.stringify({ headers: found.headers, payload: maskPii(parsed) }, null, 2));
}

async function replay(topic: string, offset: string): Promise<void> {
  const messages = await readTopic(topic, 1000);
  const found = messages.find((m) => m.offset === offset);
  if (!found) {
    console.error(`Offset ${offset} não encontrado em ${topic} (dentro das primeiras 1000 mensagens).`);
    process.exitCode = 1;
    return;
  }
  const originalTopic = found.headers[RETRY_HEADERS.originalTopic];
  if (!originalTopic) {
    console.error(`Mensagem em ${topic}@${offset} não tem header ${RETRY_HEADERS.originalTopic} — não sei para onde reenviar.`);
    process.exitCode = 1;
    return;
  }

  const producer = new EventProducer({ brokers: KAFKA_BROKERS, clientId: 'dlq-inspector-replay' });
  await producer.connect();
  await producer.publishRaw(originalTopic, found.value, found.headers, found.key);
  await producer.disconnect();
  console.log(`Reenviado offset=${offset} de ${topic} para ${originalTopic}.`);
}

async function main(): Promise<void> {
  const [command, topic, offset] = process.argv.slice(2);

  switch (command) {
    case 'list':
      if (!topic) throw new Error('uso: dlq-inspector list <topico>');
      await list(topic);
      return;
    case 'show':
      if (!topic || !offset) throw new Error('uso: dlq-inspector show <topico> <offset>');
      await show(topic, offset);
      return;
    case 'replay':
      if (!topic || !offset) throw new Error('uso: dlq-inspector replay <topico> <offset>');
      await replay(topic, offset);
      return;
    default:
      console.error('Comandos: list <topico> | show <topico> <offset> | replay <topico> <offset>');
      process.exitCode = 1;
  }
}

void main();
