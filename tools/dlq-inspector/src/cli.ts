#!/usr/bin/env node
import { Kafka, logLevel } from 'kafkajs';
import { ALL_BUSINESS_TOPICS, RETRY_HEADERS } from '@ecommerce/contracts';
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
    // x-last-error carrega até 500 caracteres de mensagem de erro ARBITRÁRIA
    // (packages/kafka/src/retry-headers.ts) — não confiável o bastante para imprimir
    // crua; passa pelo mesmo maskPii do `show`, não só o payload.
    const maskedHeaders = maskPii(msg.headers) as Record<string, string>;
    const error = maskedHeaders[RETRY_HEADERS.lastError] ?? '(sem erro registrado)';
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
  // headers também passam por maskPii — x-last-error pode carregar até 500
  // caracteres de erro arbitrário (potencialmente com dado de payload embutido).
  console.log(JSON.stringify({ headers: maskPii(found.headers), payload: maskPii(parsed) }, null, 2));
}

/** Tópicos para onde um replay pode legitimamente mandar uma mensagem de volta. */
const REPLAY_ALLOWED_TOPICS = new Set<string>(ALL_BUSINESS_TOPICS);

async function replay(topic: string, offset: string, force: boolean): Promise<void> {
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
  // O destino vem de um header ESCRITO POR QUEM PRODUZIU A MENSAGEM NA DLT — entrada
  // não confiável, como qualquer payload vindo do broker (A05/A01). Sem allowlist, um
  // operador rodando replay numa DLT poderia ser levado a publicar em qualquer tópico
  // arbitrário, inclusive um de retry/DLT (reprocessamento em cascata) ou um tópico
  // novo criado silenciosamente (o broker do compose tem auto-create habilitado).
  if (!REPLAY_ALLOWED_TOPICS.has(originalTopic)) {
    console.error(
      `Destino "${originalTopic}" (do header ${RETRY_HEADERS.originalTopic}) não é um tópico de negócio conhecido — recusando replay. Tópicos permitidos: ${[...REPLAY_ALLOWED_TOPICS].join(', ')}`,
    );
    process.exitCode = 1;
    return;
  }
  if (!force) {
    console.error(
      `Replay reenvia offset=${offset} de ${topic} para ${originalTopic} de forma IRREVERSÍVEL. Rode de novo com --force para confirmar.`,
    );
    process.exitCode = 1;
    return;
  }

  const producer = new EventProducer({ brokers: KAFKA_BROKERS, clientId: 'dlq-inspector-replay' });
  await producer.connect();
  await producer.publishRaw(originalTopic, found.value, found.headers, found.key);
  await producer.disconnect();
  console.log(`Reenviado offset=${offset} de ${topic} para ${originalTopic}.`);
}

function usage(message: string): void {
  console.error(message);
  console.error('Comandos: list <topico> | show <topico> <offset> | replay <topico> <offset> [--force]');
  process.exitCode = 1;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const force = args.includes('--force');
  const [command, topic, offset] = args.filter((arg) => arg !== '--force');

  switch (command) {
    case 'list':
      if (!topic) return usage('uso: dlq-inspector list <topico>');
      await list(topic);
      return;
    case 'show':
      if (!topic || !offset) return usage('uso: dlq-inspector show <topico> <offset>');
      await show(topic, offset);
      return;
    case 'replay':
      if (!topic || !offset) return usage('uso: dlq-inspector replay <topico> <offset> [--force]');
      await replay(topic, offset, force);
      return;
    default:
      usage('Comando desconhecido.');
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
