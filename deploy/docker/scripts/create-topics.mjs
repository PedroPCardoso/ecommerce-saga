#!/usr/bin/env node
/**
 * Cria a topologia Kafka a partir de `@ecommerce/contracts`.
 *
 * Por que um script e não `auto.create.topics.enable=true`: auto-criação transforma um typo
 * em nome de tópico num tópico novo, vazio e silencioso — o consumidor fica esperando para
 * sempre uma mensagem que está em outro lugar. Aqui a topologia é declarada, versionada e
 * derivada do MESMO módulo que os serviços usam para publicar. Sem chance de divergir.
 *
 * Usa o admin client do kafkajs em vez de `docker exec kafka-topics.sh`: uma única chamada
 * para os ~56 tópicos, em vez de uma JVM por tópico, e funciona contra qualquer broker
 * (local, Strimzi no cluster, CI) sem depender de um container com nome conhecido.
 */
import { Kafka, logLevel } from 'kafkajs';
import { ALL_BUSINESS_TOPICS, allTopics } from '../../../packages/contracts/dist/index.js';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
const BUSINESS = new Set(ALL_BUSINESS_TOPICS);

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * Retry e DLT usam UMA partição de propósito: ao desviar a mensagem para lá a ordenação
 * daquele pedido já foi perdida, então paralelismo extra não compra nada e só espalha o
 * problema por mais lugares.
 */
function specFor(topic) {
  if (BUSINESS.has(topic)) {
    return { partitions: 3, retentionMs: 7 * DAY_MS, kind: 'negócio' };
  }
  if (topic.endsWith('.DLT')) {
    // Janela suficiente para investigar e reprocessar, curta o bastante para não acumular
    // PII indefinidamente (A09).
    return { partitions: 1, retentionMs: 7 * DAY_MS, kind: 'DLT' };
  }
  return { partitions: 1, retentionMs: 2 * DAY_MS, kind: 'retry' };
}

const kafka = new Kafka({
  clientId: 'topology-bootstrap',
  brokers: BROKERS,
  logLevel: logLevel.ERROR,
  retry: { retries: 3, initialRetryTime: 300 },
});

const admin = kafka.admin();

try {
  await admin.connect();
} catch (error) {
  console.error(`Não consegui falar com o Kafka em ${BROKERS.join(',')}.`);
  console.error('Suba a infra primeiro: pnpm infra:up (e espere o healthcheck ficar verde).');
  console.error(`  ${error.message}`);
  process.exit(1);
}

try {
  const existing = new Set(await admin.listTopics());
  const desired = allTopics();
  const missing = desired.filter((topic) => !existing.has(topic));

  if (missing.length > 0) {
    await admin.createTopics({
      waitForLeaders: true,
      topics: missing.map((topic) => {
        const { partitions, retentionMs } = specFor(topic);
        return {
          topic,
          numPartitions: partitions,
          replicationFactor: 1,
          configEntries: [
            { name: 'retention.ms', value: String(retentionMs) },
            { name: 'min.insync.replicas', value: '1' },
          ],
        };
      }),
    });

    const byKind = missing.reduce((acc, topic) => {
      const { kind } = specFor(topic);
      acc[kind] = (acc[kind] ?? 0) + 1;
      return acc;
    }, {});
    const breakdown = Object.entries(byKind)
      .map(([kind, count]) => `${count} de ${kind}`)
      .join(' · ');
    console.log(`Criados ${missing.length} tópicos: ${breakdown}`);
  }

  console.log(
    `Topologia pronta: ${desired.length} tópicos declarados ` +
      `(${ALL_BUSINESS_TOPICS.length} de negócio, ${desired.length - ALL_BUSINESS_TOPICS.length} de retry/DLT), ` +
      `${desired.length - missing.length} já existiam.`,
  );

  // Tópico que existe no broker mas não está declarado costuma ser sobra de rename —
  // e sobra de rename é consumidor esperando mensagem que nunca vem.
  const declared = new Set(desired);
  const orphans = [...existing].filter((topic) => !declared.has(topic) && !topic.startsWith('__'));
  if (orphans.length > 0) {
    console.warn(
      `\nAviso: ${orphans.length} tópico(s) no broker não estão declarados nos contratos:`,
    );
    for (const orphan of orphans) console.warn(`  ? ${orphan}`);
    console.warn('Sobra de rename? Apague de propósito — este script nunca deleta nada.');
  }

  console.log('\nKafka UI: http://localhost:8080');
} finally {
  await admin.disconnect();
}
