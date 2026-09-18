import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Kafka, logLevel } from 'kafkajs';
import { RETRY_HEADERS } from '@ecommerce/contracts';

const execFileAsync = promisify(execFile);
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
const TEST_TOPIC = `dlq-inspector-test.DLT`;
const ORIGINAL_TOPIC = `dlq-inspector-test.original`;

describe('dlq-inspector CLI (integração — Kafka real, requer pnpm infra:up e o tópico existir)', () => {
  const kafka = new Kafka({ clientId: 'dlq-inspector-test-setup', brokers: BROKERS, logLevel: logLevel.ERROR });
  const admin = kafka.admin();

  beforeAll(async () => {
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: TEST_TOPIC, numPartitions: 1 },
        { topic: ORIGINAL_TOPIC, numPartitions: 1 },
      ],
      waitForLeaders: true,
    });
  });

  afterAll(async () => {
    await admin.deleteTopics({ topics: [TEST_TOPIC, ORIGINAL_TOPIC] }).catch(() => {});
    await admin.disconnect();
  });

  it('list mostra a mensagem publicada, show exibe o payload mascarado, replay reenvia para o tópico original', async () => {
    const producer = kafka.producer();
    await producer.connect();
    const orderId = randomUUID();
    await producer.send({
      topic: TEST_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify({ eventType: 'test.event', payload: { orderId, customerEmail: 'a@b.com' } }),
          headers: {
            [RETRY_HEADERS.originalTopic]: ORIGINAL_TOPIC,
            [RETRY_HEADERS.lastError]: 'erro de teste',
          },
        },
      ],
    });
    await producer.disconnect();

    const { stdout: listOutput } = await execFileAsync('node', ['--loader', '@swc-node/register/esm', 'src/cli.ts', 'list', TEST_TOPIC], {
      cwd: process.cwd(),
      env: { ...process.env, KAFKA_BROKERS: BROKERS.join(',') },
    });
    expect(listOutput).toContain('erro de teste');

    const { stdout: showOutput } = await execFileAsync('node', ['--loader', '@swc-node/register/esm', 'src/cli.ts', 'show', TEST_TOPIC, '0'], {
      cwd: process.cwd(),
      env: { ...process.env, KAFKA_BROKERS: BROKERS.join(',') },
    });
    expect(showOutput).toContain('***@***');
    expect(showOutput).not.toContain('a@b.com');

    const { stdout: replayOutput } = await execFileAsync('node', ['--loader', '@swc-node/register/esm', 'src/cli.ts', 'replay', TEST_TOPIC, '0'], {
      cwd: process.cwd(),
      env: { ...process.env, KAFKA_BROKERS: BROKERS.join(',') },
    });
    expect(replayOutput).toContain(ORIGINAL_TOPIC);
  }, 30_000);
});
