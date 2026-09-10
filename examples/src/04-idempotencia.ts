/**
 * 04 — Idempotência com tabela de inbox
 *
 * PERGUNTA: o exemplo 02 mostrou que a entrega correta duplica, e o 03 que o relay
 * também republica. Como fazer isso não cobrar o cliente duas vezes?
 *
 * `processed_messages (event_id, consumer_group)` com PK COMPOSTA, escrita na MESMA
 * transação do efeito. Violação de PK significa "já processei": rollback e segue.
 *
 * Aqui rodam quatro coisas contra o Postgres de verdade:
 *   parte 1 — a mesma mensagem entregue 5× em sequência
 *   parte 2 — a mesma mensagem entregue 5× em PARALELO (a corrida de verdade)
 *   parte 3 — dois consumer groups distintos, que PRECISAM processar o mesmo evento
 *   parte 4 — a versão errada, com PK só no event_id, e o bug silencioso que ela cria
 *
 *   pnpm ex 04
 */
import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { contar, emTransacao, fechar, recriarSchema } from './_shared/db.js';
import {
  alerta,
  confereIgual,
  detalhe,
  fim,
  info,
  licao,
  passo,
  tabela,
  titulo,
} from './_shared/log.js';

const SKU = 'BOOK-001';
const SALDO_INICIAL = 100;

const DDL = `
  -- A versão CERTA: a chave é o par (evento, grupo).
  CREATE TABLE lab.processed_messages (
    event_id       uuid NOT NULL,
    consumer_group text NOT NULL,
    processado_em  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
  );

  -- A versão ERRADA, para a parte 4: chave só no evento.
  CREATE TABLE lab.processed_errado (
    event_id       uuid PRIMARY KEY,
    consumer_group text NOT NULL
  );

  CREATE TABLE lab.saldo (
    sku        text PRIMARY KEY,
    quantidade integer NOT NULL
  );

  CREATE TABLE lab.movimento (
    id             bigserial PRIMARY KEY,
    event_id       uuid NOT NULL,
    consumer_group text NOT NULL,
    quantidade     integer NOT NULL
  );

  INSERT INTO lab.saldo (sku, quantidade) VALUES ('${SKU}', ${SALDO_INICIAL});
`;

type Resultado = 'aplicado' | 'ja-processado';

/**
 * O esqueleto de todo handler do sistema. Repare em três coisas:
 *
 * 1. O INSERT no inbox e o efeito de negócio estão na MESMA transação. Se o efeito
 *    falhar, o registro de "já processei" desaparece com ele no ROLLBACK.
 *
 * 2. `ON CONFLICT DO NOTHING` em vez de capturar a exceção de PK. No Postgres, uma
 *    violação de constraint ABORTA a transação: depois dela, todo comando na mesma
 *    transação falha com "current transaction is aborted". Capturar o erro e seguir
 *    não funciona — daria para salvar com SAVEPOINT, mas ON CONFLICT é direto.
 *
 * 3. `rowCount === 0` é a resposta "esse evento já passou por aqui".
 */
async function handler(
  eventId: string,
  grupo: string,
  tabela: 'certa' | 'errada',
): Promise<Resultado> {
  return emTransacao(async (c: PoolClient) => {
    const alvo = tabela === 'certa' ? 'lab.processed_messages' : 'lab.processed_errado';
    const marca = await c.query(
      `INSERT INTO ${alvo} (event_id, consumer_group) VALUES ($1, $2)
       ON CONFLICT DO NOTHING`,
      [eventId, grupo],
    );

    if (marca.rowCount === 0) return 'ja-processado';

    await c.query('UPDATE lab.saldo SET quantidade = quantidade - 1 WHERE sku = $1', [SKU]);
    await c.query(
      'INSERT INTO lab.movimento (event_id, consumer_group, quantidade) VALUES ($1, $2, -1)',
      [eventId, grupo],
    );
    return 'aplicado';
  });
}

const saldo = () => contar(`SELECT quantidade AS n FROM lab.saldo WHERE sku = '${SKU}'`);
const movimentos = (grupo?: string) =>
  grupo
    ? contar('SELECT count(*)::int AS n FROM lab.movimento WHERE consumer_group = $1', [grupo])
    : contar('SELECT count(*)::int AS n FROM lab.movimento');

async function main() {
  titulo(
    '04 — Idempotência com tabela de inbox',
    'A mesma mensagem chegando várias vezes, e o efeito acontecendo uma só.',
  );

  await recriarSchema(DDL);
  info(`saldo inicial de ${SKU}: ${SALDO_INICIAL}`);

  /* ── parte 1 ─────────────────────────────────────────────────────────── */
  passo('PARTE 1 — a mesma mensagem entregue 5× em sequência');
  const ev1 = randomUUID();
  const seq: Resultado[] = [];
  for (let i = 0; i < 5; i++) seq.push(await handler(ev1, 'estoque', 'certa'));

  tabela(
    ['entrega', 'resultado'],
    seq.map((r, i) => [`${i + 1}ª`, r]),
  );
  confereIgual(
    seq.filter((r) => r === 'aplicado').length,
    1,
    'exatamente uma entrega aplicou o efeito',
  );
  confereIgual(await saldo(), SALDO_INICIAL - 1, 'o saldo caiu 1, não 5');

  /* ── parte 2 ─────────────────────────────────────────────────────────── */
  passo('PARTE 2 — a mesma mensagem entregue 5× em PARALELO');
  detalhe('sequencial é fácil; o caso real é rebalance entregando o mesmo record a duas réplicas');
  const ev2 = randomUUID();
  const par = await Promise.all(Array.from({ length: 5 }, () => handler(ev2, 'estoque', 'certa')));

  confereIgual(
    par.filter((r) => r === 'aplicado').length,
    1,
    'mesmo em corrida, só uma aplicou — a PK única é o árbitro, não o código da aplicação',
  );
  confereIgual(await saldo(), SALDO_INICIAL - 2, 'o saldo caiu apenas 1 nesta parte');

  /* ── parte 3 ─────────────────────────────────────────────────────────── */
  passo('PARTE 3 — dois consumer groups, o MESMO evento');
  detalhe('o Payment e o Notification consomem order.created; os dois precisam processar');
  const ev3 = randomUUID();
  const rPagamento = await handler(ev3, 'payment-service', 'certa');
  const rNotificacao = await handler(ev3, 'notification-service', 'certa');

  tabela(
    ['consumer group', 'resultado'],
    [
      ['payment-service', rPagamento],
      ['notification-service', rNotificacao],
    ],
  );
  confereIgual(
    [rPagamento, rNotificacao],
    ['aplicado', 'aplicado'],
    'os dois grupos aplicaram — é para isso que a PK é composta',
  );

  /* ── parte 4 ─────────────────────────────────────────────────────────── */
  passo('PARTE 4 — a versão ERRADA: PK só no event_id');
  const ev4 = randomUUID();
  const eA = await handler(ev4, 'payment-service', 'errada');
  const eB = await handler(ev4, 'notification-service', 'errada');

  tabela(
    ['consumer group', 'resultado'],
    [
      ['payment-service', eA],
      ['notification-service', eB],
    ],
  );
  confereIgual(eB, 'ja-processado', 'o segundo grupo foi BARRADO por um evento que ele nunca viu');
  alerta('Nenhum erro, nenhum log. O cliente só nunca recebe o e-mail — e ninguém liga isso à PK.');

  passo('Contabilidade final');
  const movTotal = await movimentos();
  const movEstoque = await movimentos('estoque');
  tabela(
    ['métrica', 'valor'],
    [
      ['saldo', String(await saldo())],
      ['movimentos totais', String(movTotal)],
      ['movimentos do grupo estoque', String(movEstoque)],
      ['entregas simuladas', '13'],
    ],
  );
  confereIgual(
    movEstoque,
    2,
    'o grupo estoque recebeu 10 entregas de 2 eventos e gravou 2 movimentos',
  );

  licao(
    'Três detalhes que decidem se isto funciona:\n' +
      '  1. A chave é (event_id, consumer_group). Só event_id barra grupos inocentes (parte 4).\n' +
      '  2. O INSERT vive na MESMA transação do efeito. Fora dela, um crash entre os dois\n' +
      '     marca como processado algo que não foi — e aí você tem a perda do exemplo 02.\n' +
      '  3. A tabela cresce para sempre. Precisa de job de limpeza, com retenção MAIOR que a\n' +
      '     do tópico: se o tópico guarda 7 dias e o inbox 3, um replay reprocessa tudo o que\n' +
      '     o inbox já esqueceu. Veja o exemplo 06.\n' +
      '\n' +
      '  E o limite: isto só cobre o que está DENTRO da transação. Chamada ao gateway de\n' +
      '  pagamento e envio de e-mail ficam de fora, e precisam da própria chave de\n' +
      '  idempotência no lado remoto.',
  );

  await fechar();
  fim();
}

main().catch(async (e) => {
  console.error(e);
  await fechar().catch(() => {});
  process.exit(1);
});
