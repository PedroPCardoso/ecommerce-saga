/**
 * 03 — Transactional Outbox
 *
 * PERGUNTA: por que não publicar direto no handler, depois do save()?
 *
 * Porque o banco e o broker são dois sistemas sem transação comum. Morrer entre os
 * dois deixa pedido sem evento (a saga nunca começa) ou evento sem pedido (o Payment
 * autoriza a cobrança de um pedido que não existe).
 *
 * Este exemplo roda três coisas contra o Postgres de verdade:
 *   parte 1 — sem outbox: a inconsistência acontecendo
 *   parte 2 — com outbox: a intenção de publicar sobrevive ao crash
 *   parte 3 — FOR UPDATE SKIP LOCKED: por que ele não é enfeite quando há 2+ réplicas
 *
 *   pnpm ex 03
 */
import { randomUUID } from 'node:crypto';
import {
  admin,
  cliente,
  contarPorParticao,
  limparLab,
  produtor,
  recriarTopicos,
} from './_shared/kafka.js';
import { contar, emTransacao, fechar, pool, recriarSchema } from './_shared/db.js';
import {
  alerta,
  confere,
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

const TOPICO = 'lab.outbox.eventos';

const DDL = `
  CREATE TABLE lab.pedido (
    id          uuid PRIMARY KEY,
    valor_cents integer NOT NULL,
    criado_em   timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE lab.outbox (
    id            bigserial PRIMARY KEY,
    event_id      uuid NOT NULL UNIQUE,
    aggregate_id  uuid NOT NULL,
    event_type    text NOT NULL,
    payload       jsonb NOT NULL,
    criado_em     timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz
  );

  -- Índice PARCIAL: o relay só pergunta pelas pendentes, e essa é a única
  -- consulta que roda a cada 200ms para sempre. Sem o WHERE, o índice cresce
  -- com o histórico inteiro e a consulta degrada junto.
  CREATE INDEX outbox_pendentes ON lab.outbox (id) WHERE published_at IS NULL;
`;

async function main() {
  titulo('03 — Transactional Outbox', 'Postgres e Kafka de verdade, com crash no meio.');

  const kafka = cliente('03');
  const a = await admin(kafka);
  const p = await produtor(kafka);

  passo('Preparando tópico e schema');
  await recriarTopicos(a, [{ topic: TOPICO, numPartitions: 1 }]);
  await recriarSchema(DDL);

  /* ─────────────────────────────────────────────────────────────────────────
     PARTE 1 — sem outbox
     ───────────────────────────────────────────────────────────────────────── */
  passo('PARTE 1 — sem outbox: INSERT, COMMIT, e o processo morre antes do publish');
  const pedido1 = randomUUID();
  try {
    await emTransacao(async (c) => {
      await c.query('INSERT INTO lab.pedido (id, valor_cents) VALUES ($1, $2)', [pedido1, 9980]);
    });
    // COMMIT já aconteceu. Agora o processo morre.
    throw new Error('processo morreu entre o COMMIT e o publish');
  } catch (e) {
    detalhe((e as Error).message);
  }

  const pedidosNoBanco = await contar('SELECT count(*)::int AS n FROM lab.pedido');
  const [noTopico1] = await contarPorParticao(a, TOPICO);
  tabela(
    ['onde', 'quantos'],
    [
      ['pedidos no Postgres', String(pedidosNoBanco)],
      ['eventos no Kafka', String(noTopico1)],
    ],
  );
  confereIgual(pedidosNoBanco, 1, 'o pedido existe no banco');
  confereIgual(noTopico1, 0, 'o evento NÃO existe no Kafka — a saga nunca vai começar');
  alerta('Nada falhou de forma visível. O pedido simplesmente nunca vai ser cobrado nem enviado.');

  /* ─────────────────────────────────────────────────────────────────────────
     PARTE 2 — com outbox
     ───────────────────────────────────────────────────────────────────────── */
  passo('PARTE 2 — com outbox: pedido e evento na MESMA transação');
  const pedido2 = randomUUID();
  const evento2 = randomUUID();

  await emTransacao(async (c) => {
    await c.query('INSERT INTO lab.pedido (id, valor_cents) VALUES ($1, $2)', [pedido2, 15900]);
    await c.query(
      `INSERT INTO lab.outbox (event_id, aggregate_id, event_type, payload)
       VALUES ($1, $2, $3, $4)`,
      [
        evento2,
        pedido2,
        'order.created',
        JSON.stringify({ orderId: pedido2, totalAmountCents: 15900 }),
      ],
    );
  });
  detalhe('COMMIT: ou os dois existem, ou nenhum');

  detalhe('processo morre aqui, antes de o relay rodar');
  const pendentes = await contar(
    'SELECT count(*)::int AS n FROM lab.outbox WHERE published_at IS NULL',
  );
  confereIgual(
    pendentes,
    1,
    'a intenção de publicar sobreviveu ao crash — está na tabela, esperando',
  );

  info('agora o relay sobe e drena a fila');
  await relay('relay-1', true, p, []);
  const [noTopico2] = await contarPorParticao(a, TOPICO);
  const aindaPendentes = await contar(
    'SELECT count(*)::int AS n FROM lab.outbox WHERE published_at IS NULL',
  );
  confereIgual(noTopico2, 1, 'o evento chegou ao Kafka, mesmo tendo o processo morrido antes');
  confereIgual(aindaPendentes, 0, 'a linha do outbox foi liquidada');

  /* ─────────────────────────────────────────────────────────────────────────
     PARTE 3 — SKIP LOCKED
     ───────────────────────────────────────────────────────────────────────── */
  passo('PARTE 3 — três réplicas do relay disputando as mesmas 30 linhas');
  const LINHAS = 30;
  await pool.query('TRUNCATE lab.outbox');
  for (let i = 0; i < LINHAS; i++) {
    await pool.query(
      `INSERT INTO lab.outbox (event_id, aggregate_id, event_type, payload)
       VALUES ($1, $2, 'order.created', '{}')`,
      [randomUUID(), pedido2],
    );
  }

  info('primeiro SEM o SKIP LOCKED (SELECT simples)');
  const semLock: string[] = [];
  await Promise.all([
    relay('r1', false, null, semLock),
    relay('r2', false, null, semLock),
    relay('r3', false, null, semLock),
  ]);
  const distintosSem = new Set(semLock).size;

  await pool.query('UPDATE lab.outbox SET published_at = NULL');

  info('agora COM FOR UPDATE SKIP LOCKED');
  const comLock: string[] = [];
  await Promise.all([
    relay('r1', true, null, comLock),
    relay('r2', true, null, comLock),
    relay('r3', true, null, comLock),
  ]);
  const distintosCom = new Set(comLock).size;

  tabela(
    ['variante', 'publicações', 'eventos distintos', 'duplicatas'],
    [
      [
        'SELECT simples',
        String(semLock.length),
        String(distintosSem),
        String(semLock.length - distintosSem),
      ],
      [
        'FOR UPDATE SKIP LOCKED',
        String(comLock.length),
        String(distintosCom),
        String(comLock.length - distintosCom),
      ],
    ],
  );

  confere(
    semLock.length > LINHAS,
    `sem o lock, as três réplicas leram as mesmas linhas e publicaram ${semLock.length} vezes ` +
      `para ${LINHAS} eventos`,
  );
  confereIgual(
    comLock.length,
    LINHAS,
    'com SKIP LOCKED, cada evento foi publicado exatamente uma vez',
  );
  confereIgual(distintosCom, LINHAS, 'e nenhum evento ficou para trás');

  licao(
    'O outbox resolve a atomicidade, e cobra duas coisas em troca:\n' +
      '  1. LATÊNCIA — o evento sai no próximo ciclo do relay, não no instante do COMMIT.\n' +
      '  2. AT-LEAST-ONCE — o relay pode publicar e morrer antes do UPDATE, republicando na\n' +
      '     volta. Repare que a parte 3 não elimina isso: o SKIP LOCKED evita que réplicas\n' +
      '     concorrentes dupliquem, mas não protege contra o crash entre publish e UPDATE.\n' +
      '  Ou seja: outbox SEM idempotência no consumidor troca um bug por outro. Os dois\n' +
      '  padrões são um par — veja o exemplo 04.',
  );

  await p.disconnect();
  await limparLab(a);
  await a.disconnect();
  await fechar();
  fim();
}

/**
 * O relay. `comSkipLocked` existe só para a parte 3 poder mostrar o estrago sem ele.
 * Em código de produção não há variante: é sempre com.
 */
async function relay(
  nome: string,
  comSkipLocked: boolean,
  produtorKafka: Awaited<ReturnType<typeof produtor>> | null,
  publicados: string[],
): Promise<void> {
  const LOTE = 5;
  for (;;) {
    const enviados = await emTransacao(async (c) => {
      const { rows } = await c.query<{
        id: string;
        event_id: string;
        event_type: string;
        payload: unknown;
      }>(
        `SELECT id, event_id, event_type, payload
           FROM lab.outbox
          WHERE published_at IS NULL
          ORDER BY id
          LIMIT ${LOTE}
          ${comSkipLocked ? 'FOR UPDATE SKIP LOCKED' : ''}`,
      );

      if (rows.length === 0) return 0;

      for (const r of rows) {
        publicados.push(r.event_id);
        if (produtorKafka) {
          await produtorKafka.send({
            topic: TOPICO,
            messages: [{ key: r.event_id, value: JSON.stringify(r.payload) }],
          });
        }
      }

      await c.query('UPDATE lab.outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [
        rows.map((r) => r.id),
      ]);
      return rows.length;
    });

    if (enviados === 0) return;
    // Dá chance de as réplicas se intercalarem — sem isso a primeira drena tudo
    // antes de as outras acordarem, e a parte 3 não demonstra nada.
    await dorme(15);
  }
}

main().catch(async (e) => {
  console.error(e);
  await fechar().catch(() => {});
  process.exit(1);
});
