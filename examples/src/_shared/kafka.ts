import { Kafka, logLevel, type Admin, type Producer } from 'kafkajs';

/** Tudo que os exemplos criam usa este prefixo, para não encostar na topologia real. */
export const LAB = 'lab';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');

/**
 * Nota sobre rebalance, que custou uma verificação: kafkajs 2.x implementa apenas o
 * protocolo EAGER, e o único assigner que ele traz é `roundRobin`. Não existe
 * `cooperative-sticky` nem static membership (`group.instance.id`) — os dois são do
 * cliente Java / librdkafka. Consequência prática: todo pod que entra ou sai do grupo
 * para TODOS os consumidores dele por um instante. Ver docs/aprender/10.
 */
export function cliente(clientId: string): Kafka {
  return new Kafka({
    clientId: `lab-${clientId}`,
    brokers: BROKERS,
    logLevel: logLevel.NOTHING,
    retry: { retries: 5, initialRetryTime: 200 },
  });
}

/**
 * Produtor com as garantias que a saga exige.
 *
 * `acks: -1` (all) espera todas as réplicas em sync. `idempotent: true` faz o broker
 * deduplicar por (producerId, sequence) — o que protege contra retry interno do
 * cliente, e **não** contra republicação do relay depois de um reinício. Essa é
 * outra história, e é o exemplo 04.
 */
export async function produtor(kafka: Kafka): Promise<Producer> {
  const p = kafka.producer({ idempotent: true, maxInFlightRequests: 5 });
  await p.connect();
  return p;
}

export async function admin(kafka: Kafka): Promise<Admin> {
  const a = kafka.admin();
  await a.connect();
  return a;
}

/** Recria os tópicos do laboratório do zero, para o exemplo ser repetível. */
export async function recriarTopicos(
  a: Admin,
  topicos: Array<{ topic: string; numPartitions?: number }>,
): Promise<void> {
  const existentes = new Set(await a.listTopics());
  const paraApagar = topicos.map((t) => t.topic).filter((t) => existentes.has(t));
  if (paraApagar.length) {
    await a.deleteTopics({ topics: paraApagar, timeout: 15_000 });
    // A deleção é assíncrona no broker; recriar imediatamente às vezes colide.
    await new Promise((r) => setTimeout(r, 800));
  }
  await a.createTopics({
    waitForLeaders: true,
    topics: topicos.map((t) => ({
      topic: t.topic,
      numPartitions: t.numPartitions ?? 1,
      replicationFactor: 1,
      configEntries: [{ name: 'retention.ms', value: String(60 * 60 * 1000) }],
    })),
  });
}

/** Apaga todo tópico `lab.*`. Chamado no fim de cada exemplo. */
export async function limparLab(a: Admin): Promise<void> {
  const alvos = (await a.listTopics()).filter((t) => t.startsWith(`${LAB}.`));
  if (alvos.length) await a.deleteTopics({ topics: alvos, timeout: 15_000 });
}

/** Quantas mensagens existem em cada partição de um tópico. */
export async function contarPorParticao(a: Admin, topic: string): Promise<number[]> {
  const offsets = await a.fetchTopicOffsets(topic);
  return offsets
    .sort((x, y) => x.partition - y.partition)
    .map((o) => Number(o.high) - Number(o.low));
}
