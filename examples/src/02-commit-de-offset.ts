/**
 * 02 — A ordem entre COMMIT do banco e commit de offset
 *
 * PERGUNTA: por que `enable.auto.commit=false` não é negociável?
 *
 * O consumidor faz duas coisas: aplica o efeito no banco e avança o offset. A ordem
 * entre as duas decide qual falha você vai ter quando o processo morrer no meio:
 *
 *   commitar ANTES de persistir  → mensagem PERDIDA  (o offset já passou dela)
 *   commitar DEPOIS de persistir → mensagem DUPLICADA (é a que você quer, e o
 *                                  exemplo 04 mostra como tornar a duplicata inofensiva)
 *
 * Aqui os dois casos rodam de verdade, com um "crash" no meio, e o banco mostra o estrago.
 *
 *   pnpm ex 02
 */
import type { Kafka } from 'kafkajs';
import { admin, cliente, limparLab, produtor, recriarTopicos } from './_shared/kafka.js';
import { contar, fechar, pool, recriarSchema } from './_shared/db.js';
import {
  alerta,
  confereIgual,
  detalhe,
  dorme,
  fim,
  info,
  licao,
  passo,
  tabela,
  titulo,
} from './_shared/log.js';

const TOPICO = 'lab.commit.pedidos';
const MENSAGENS = ['m0', 'm1', 'm2', 'm3', 'm4', 'm5'];
const CRASH_EM = 2; // índice da mensagem em que o processo "morre"

const DDL = `
  CREATE TABLE lab.processado (
    id            bigserial PRIMARY KEY,
    grupo         text NOT NULL,
    mensagem      text NOT NULL,
    processado_em timestamptz NOT NULL DEFAULT now()
  );
`;

/**
 * Consome até o handler pedir para parar, ou até o tópico secar.
 *
 * `autoCommit: false` é o ponto do exemplo: sem isso o kafkajs avança o offset num
 * timer próprio, sem a menor ideia de se o seu COMMIT de banco aconteceu.
 */
async function consumir(
  kafka: Kafka,
  grupo: string,
  aoReceber: (m: {
    offset: number;
    valor: string;
    persistir: () => Promise<void>;
    commitar: () => Promise<void>;
  }) => Promise<'segue' | 'crash'>,
): Promise<void> {
  const c = kafka.consumer({ groupId: grupo, sessionTimeout: 10_000 });
  await c.connect();
  await c.subscribe({ topic: TOPICO, fromBeginning: true });

  let pedirParada!: () => void;
  const parada = new Promise<void>((r) => {
    pedirParada = r;
  });

  // Watchdog de ociosidade: sem isso não há como saber que o tópico secou.
  let ocioso: NodeJS.Timeout;
  const rearmar = (r: () => void) => {
    clearTimeout(ocioso);
    ocioso = setTimeout(r, 2500);
  };
  const secou = new Promise<void>((r) => rearmar(r));

  await c.run({
    autoCommit: false,
    eachMessage: async ({ message, partition }) => {
      rearmar(() => pedirParada());
      const offset = Number(message.offset);
      const valor = message.value!.toString();
      const resultado = await aoReceber({
        offset,
        valor,
        persistir: async () => {
          await pool.query('INSERT INTO lab.processado (grupo, mensagem) VALUES ($1, $2)', [
            grupo,
            valor,
          ]);
        },
        commitar: async () => {
          await c.commitOffsets([{ topic: TOPICO, partition, offset: String(offset + 1) }]);
        },
      });
      if (resultado === 'crash') pedirParada();
    },
  });

  await Promise.race([parada, secou]);
  clearTimeout(ocioso!);
  await c.disconnect();
}

async function main() {
  titulo(
    '02 — A ordem entre COMMIT do banco e commit de offset',
    `${MENSAGENS.length} mensagens, um "crash" na de índice ${CRASH_EM}, dois grupos com ordens opostas.`,
  );

  const kafka = cliente('02');
  const a = await admin(kafka);
  const p = await produtor(kafka);

  passo('Preparando tópico (1 partição) e schema');
  await recriarTopicos(a, [{ topic: TOPICO, numPartitions: 1 }]);
  await recriarSchema(DDL);
  await p.send({ topic: TOPICO, messages: MENSAGENS.map((m) => ({ key: 'k', value: m })) });
  info(`publicadas: ${MENSAGENS.join(', ')}`);

  /* ─────────────────────────────────────────────────────────────────────────
     CASO A — commitar antes de persistir
     ───────────────────────────────────────────────────────────────────────── */
  passo('CASO A: commita o offset PRIMEIRO, persiste depois');
  const GRUPO_A = 'lab-commit-antes';
  let crashouA = false;

  await consumir(kafka, GRUPO_A, async (m) => {
    await m.commitar(); // offset avança
    if (!crashouA && m.offset === CRASH_EM) {
      // ...e o processo morre aqui
      crashouA = true;
      detalhe(`offset ${m.offset}: commitado, e o processo morreu antes do INSERT`);
      return 'crash';
    }
    await m.persistir();
    return 'segue';
  });

  detalhe('reiniciando o consumidor no mesmo grupo...');
  await dorme(400);
  await consumir(kafka, GRUPO_A, async (m) => {
    await m.commitar();
    await m.persistir();
    return 'segue';
  });

  /* ─────────────────────────────────────────────────────────────────────────
     CASO B — persistir antes de commitar
     ───────────────────────────────────────────────────────────────────────── */
  passo('CASO B: persiste PRIMEIRO, commita o offset depois');
  const GRUPO_B = 'lab-commit-depois';
  let crashouB = false;

  await consumir(kafka, GRUPO_B, async (m) => {
    await m.persistir(); // efeito de negócio aplicado
    if (!crashouB && m.offset === CRASH_EM) {
      // ...e o processo morre antes do commit
      crashouB = true;
      detalhe(`offset ${m.offset}: INSERT feito, e o processo morreu antes do commit de offset`);
      return 'crash';
    }
    await m.commitar();
    return 'segue';
  });

  detalhe('reiniciando o consumidor no mesmo grupo...');
  await dorme(400);
  await consumir(kafka, GRUPO_B, async (m) => {
    await m.persistir();
    await m.commitar();
    return 'segue';
  });

  /* ─────────────────────────────────────────────────────────────────────── */
  passo('O que ficou no banco');
  const linhas = await pool.query<{ grupo: string; mensagem: string; n: string }>(
    `SELECT grupo, mensagem, count(*)::text AS n
       FROM lab.processado GROUP BY grupo, mensagem ORDER BY grupo, mensagem`,
  );

  const porGrupo = (g: string) =>
    MENSAGENS.map((m) => {
      const r = linhas.rows.find((x) => x.grupo === g && x.mensagem === m);
      const n = r ? Number(r.n) : 0;
      return n === 0 ? '— PERDIDA' : n === 1 ? '1' : `${n} ← DUPLICADA`;
    });

  tabela(
    ['mensagem', 'CASO A (commit antes)', 'CASO B (commit depois)'],
    MENSAGENS.map((m, i) => [m, porGrupo(GRUPO_A)[i]!, porGrupo(GRUPO_B)[i]!]),
  );

  const totalA = await contar('SELECT count(*)::int AS n FROM lab.processado WHERE grupo=$1', [
    GRUPO_A,
  ]);
  const totalB = await contar('SELECT count(*)::int AS n FROM lab.processado WHERE grupo=$1', [
    GRUPO_B,
  ]);
  const distintosA = await contar(
    'SELECT count(DISTINCT mensagem)::int AS n FROM lab.processado WHERE grupo=$1',
    [GRUPO_A],
  );

  passo('Verificação');
  confereIgual(
    totalA,
    MENSAGENS.length - 1,
    'CASO A processou uma mensagem A MENOS que o publicado',
  );
  confereIgual(
    distintosA,
    MENSAGENS.length - 1,
    'CASO A perdeu uma mensagem para sempre — nenhum retry a traz de volta',
  );
  confereIgual(
    totalB,
    MENSAGENS.length + 1,
    'CASO B processou uma mensagem A MAIS — duplicata, nada perdido',
  );

  alerta('O caso A é silencioso: nenhum erro, nenhum log, nenhum alerta. Só falta um pedido.');

  licao(
    'Commitar o offset é dizer "já lidei com isso". Dizer isso antes de ser verdade\n' +
      '  troca uma duplicata (tratável) por uma perda (irreversível). Por isso o esqueleto de\n' +
      '  todo handler é: BEGIN → efeito + outbox → COMMIT → e SÓ ENTÃO commitOffsets.\n' +
      '  A duplicata do caso B é o preço do at-least-once, e o exemplo 04 mostra como pagá-lo.',
  );

  await p.disconnect();
  await limparLab(a);
  await a.disconnect();
  await fechar();
  fim();
}

main().catch(async (e) => {
  console.error(e);
  await fechar().catch(() => {});
  process.exit(1);
});
