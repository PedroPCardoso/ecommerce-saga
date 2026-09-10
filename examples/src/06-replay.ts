/**
 * 06 — Replay: reprocessar o tópico do zero
 *
 * PERGUNTA: por que "retenção do inbox MAIOR que a retenção do tópico" aparece em
 * todo lugar no plano?
 *
 * Porque Kafka é um log retido, não uma fila consumida. Reprocessar do offset zero é
 * operação de rotina: corrigir bug de projeção, construir uma leitura nova, auditar.
 * A idempotência é o que torna isso seguro — e ela tem uma condição escondida.
 *
 * Quatro rodadas:
 *   1. projeção nova consome tudo → estado final X
 *   2. MESMO grupo, offsets resetados, inbox intacto → nada reaplicado, estado X
 *   3. grupo NOVO (inbox vazio para ele) → reconstrói do zero → estado X de novo
 *   4. inbox EXPIRADO antes do tópico → tudo reaplicado → estado ERRADO
 *
 *   pnpm ex 06
 */
import { randomUUID } from 'node:crypto';
import type { Kafka } from 'kafkajs';
import { admin, cliente, limparLab, produtor, recriarTopicos } from './_shared/kafka.js';
import { emTransacao, fechar, pool, recriarSchema } from './_shared/db.js';
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

const TOPICO = 'lab.replay.movimentos';
const CONTA = 'conta-1';

/** Sequência determinística. O saldo final tem que ser 10, sempre. */
const MOVIMENTOS = [
  { valor: +10, motivo: 'depósito inicial' },
  { valor: -3, motivo: 'compra' },
  { valor: +5, motivo: 'estorno' },
  { valor: -2, motivo: 'tarifa' },
];
const SALDO_ESPERADO = MOVIMENTOS.reduce((s, m) => s + m.valor, 0);

const DDL = `
  CREATE TABLE lab.processed_messages (
    event_id       uuid NOT NULL,
    consumer_group text NOT NULL,
    processado_em  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
  );

  CREATE TABLE lab.projecao (
    consumer_group text NOT NULL,
    conta          text NOT NULL,
    saldo          integer NOT NULL DEFAULT 0,
    aplicados      integer NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_group, conta)
  );
`;

async function main() {
  titulo(
    '06 — Replay: reprocessar o tópico do zero',
    `${MOVIMENTOS.length} movimentos, saldo final esperado ${SALDO_ESPERADO}.`,
  );

  const kafka = cliente('06');
  const a = await admin(kafka);
  const p = await produtor(kafka);

  passo('Preparando tópico, schema e publicando os movimentos');
  await recriarTopicos(a, [{ topic: TOPICO, numPartitions: 1 }]);
  await recriarSchema(DDL);
  await p.send({
    topic: TOPICO,
    messages: MOVIMENTOS.map((m) => ({
      key: CONTA,
      value: JSON.stringify({ eventId: randomUUID(), conta: CONTA, ...m }),
    })),
  });
  tabela(
    ['movimento', 'valor'],
    MOVIMENTOS.map((m) => [m.motivo, (m.valor > 0 ? '+' : '') + m.valor]),
  );

  /* ── rodada 1 ────────────────────────────────────────────────────────── */
  passo('RODADA 1 — projeção nova consome o tópico inteiro');
  await consumirTudo(kafka, 'proj-v1');
  const r1 = await projecao('proj-v1');
  tabela(
    ['grupo', 'saldo', 'eventos aplicados'],
    [['proj-v1', String(r1.saldo), String(r1.aplicados)]],
  );
  confereIgual(r1.saldo, SALDO_ESPERADO, 'saldo correto');
  confereIgual(r1.aplicados, MOVIMENTOS.length, 'todos os eventos foram aplicados uma vez');

  /* ── rodada 2 ────────────────────────────────────────────────────────── */
  passo('RODADA 2 — MESMO grupo, offsets resetados para zero, inbox INTACTO');
  detalhe('é o que acontece quando alguém reseta um grupo por engano, ou num rollback de deploy');
  await resetarOffsets(a, 'proj-v1');
  await consumirTudo(kafka, 'proj-v1');
  const r2 = await projecao('proj-v1');
  tabela(
    ['grupo', 'saldo', 'eventos aplicados'],
    [['proj-v1', String(r2.saldo), String(r2.aplicados)]],
  );
  confereIgual(r2.saldo, SALDO_ESPERADO, 'o saldo NÃO mudou — o inbox barrou as 4 reentregas');
  confereIgual(r2.aplicados, MOVIMENTOS.length, 'nenhum evento foi aplicado duas vezes');

  /* ── rodada 3 ────────────────────────────────────────────────────────── */
  passo('RODADA 3 — grupo NOVO, reconstruindo a projeção do zero');
  detalhe('é assim que se corrige um bug de projeção: novo grupo, offset zero, sem migration');
  await consumirTudo(kafka, 'proj-v2');
  const r3 = await projecao('proj-v2');
  tabela(
    ['grupo', 'saldo', 'eventos aplicados'],
    [
      ['proj-v1', String(r2.saldo), String(r2.aplicados)],
      ['proj-v2', String(r3.saldo), String(r3.aplicados)],
    ],
  );
  confereIgual(r3.saldo, SALDO_ESPERADO, 'a projeção nova chegou ao MESMO saldo');
  confereIgual(r3.aplicados, MOVIMENTOS.length, 'e aplicou cada evento uma vez');
  info('as duas projeções coexistem: a antiga serve o tráfego enquanto a nova é conferida');

  /* ── rodada 4 ────────────────────────────────────────────────────────── */
  passo('RODADA 4 — a armadilha: inbox expirou ANTES do tópico');
  detalhe('tópico com retenção de 7 dias, processed_messages limpo em 3: o job de limpeza');
  detalhe('apagou registros de mensagens que ainda existem no log');
  const apagados = await pool.query(
    "DELETE FROM lab.processed_messages WHERE consumer_group = 'proj-v1'",
  );
  info(`${apagados.rowCount} registros de idempotência apagados`);

  await resetarOffsets(a, 'proj-v1');
  await consumirTudo(kafka, 'proj-v1');
  const r4 = await projecao('proj-v1');
  tabela(
    ['grupo', 'saldo', 'esperado', 'eventos aplicados'],
    [['proj-v1', String(r4.saldo), String(SALDO_ESPERADO), String(r4.aplicados)]],
  );
  confereIgual(
    r4.saldo,
    SALDO_ESPERADO * 2,
    'o saldo DOBROU — cada movimento foi aplicado de novo',
  );
  confereIgual(r4.aplicados, MOVIMENTOS.length * 2, 'e o contador confirma a dupla aplicação');
  alerta(
    'Nenhum erro em log. Só um saldo errado, dias depois, sem ninguém ligar ao job de limpeza.',
  );

  licao(
    'Replay é uma capacidade, não um acidente — é o que permite corrigir uma projeção\n' +
      '  errada sem migration, e auditar o passado. Mas ele só é seguro sob uma condição:\n' +
      '\n' +
      '      retenção de processed_messages  >  retenção do tópico\n' +
      '\n' +
      '  A rodada 4 é essa desigualdade invertida. E note o que a rodada 3 mostra: para\n' +
      '  reconstruir uma projeção você não reseta o grupo existente — você cria um grupo\n' +
      '  NOVO, que naturalmente não tem registro de idempotência nenhum, e por isso aplica\n' +
      '  tudo de propósito. Resetar o grupo antigo é o que dá errado.',
  );

  await p.disconnect();
  await limparLab(a);
  await a.disconnect();
  await fechar();
  fim();
}

/** Consome tudo o que houver, com o esqueleto idempotente de sempre. */
async function consumirTudo(kafka: Kafka, grupo: string): Promise<void> {
  const c = kafka.consumer({ groupId: grupo, sessionTimeout: 10_000 });
  await c.connect();
  await c.subscribe({ topic: TOPICO, fromBeginning: true });

  let ocioso: NodeJS.Timeout;
  let terminar!: () => void;
  const fimDaFila = new Promise<void>((r) => {
    terminar = r;
  });
  const rearmar = () => {
    clearTimeout(ocioso);
    ocioso = setTimeout(terminar, 2000);
  };
  rearmar();

  await c.run({
    autoCommit: false,
    eachMessage: async ({ message, partition }) => {
      rearmar();
      const ev = JSON.parse(message.value!.toString()) as {
        eventId: string;
        conta: string;
        valor: number;
      };

      await emTransacao(async (cli) => {
        const marca = await cli.query(
          `INSERT INTO lab.processed_messages (event_id, consumer_group) VALUES ($1, $2)
           ON CONFLICT DO NOTHING`,
          [ev.eventId, grupo],
        );
        if (marca.rowCount === 0) return; // já processado: sai sem tocar na projeção

        await cli.query(
          `INSERT INTO lab.projecao (consumer_group, conta, saldo, aplicados)
           VALUES ($1, $2, $3, 1)
           ON CONFLICT (consumer_group, conta)
           DO UPDATE SET saldo = lab.projecao.saldo + $3, aplicados = lab.projecao.aplicados + 1`,
          [grupo, ev.conta, ev.valor],
        );
      });

      await c.commitOffsets([
        { topic: TOPICO, partition, offset: String(Number(message.offset) + 1) },
      ]);
    },
  });

  await fimDaFila;
  clearTimeout(ocioso!);
  await c.disconnect();
}

/** `setOffsets` exige que o grupo não tenha membro ativo — daí o disconnect antes. */
async function resetarOffsets(a: Awaited<ReturnType<typeof admin>>, grupo: string): Promise<void> {
  await dorme(300);
  await a.setOffsets({
    groupId: grupo,
    topic: TOPICO,
    partitions: [{ partition: 0, offset: '0' }],
  });
  detalhe(`offsets do grupo ${grupo} resetados para 0`);
}

async function projecao(grupo: string): Promise<{ saldo: number; aplicados: number }> {
  const { rows } = await pool.query<{ saldo: number; aplicados: number }>(
    'SELECT saldo, aplicados FROM lab.projecao WHERE consumer_group = $1 AND conta = $2',
    [grupo, CONTA],
  );
  return rows[0] ?? { saldo: 0, aplicados: 0 };
}

main().catch(async (e) => {
  console.error(e);
  await fechar().catch(() => {});
  process.exit(1);
});
