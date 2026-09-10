/**
 * 05 — Escada de retry e DLT
 *
 * PERGUNTA: Kafka não tem DLQ nem retry nativo. Um handler que lança erro tem duas
 * saídas ruins — não commitar (e reler a mesma mensagem para sempre, travando a
 * partição) ou commitar (e perder a mensagem). Como sair dessa?
 *
 * Três coisas juntas:
 *   1. CLASSIFICAR o erro antes de retryar. Transitório sobe a escada; permanente vai
 *      direto para a DLT, sem queimar tentativas no que nunca vai funcionar.
 *   2. Escada em TÓPICOS DEDICADOS, não na partição original — senão uma mensagem
 *      problemática trava todas as outras que caíram naquela partição.
 *   3. DLT com headers de diagnóstico, para a mensagem ser investigável depois.
 *
 * Três mensagens percorrem caminhos diferentes, com tópicos e atrasos reais.
 *
 *   pnpm ex 05
 */
import type { Consumer, Kafka, Producer } from 'kafkajs';
import {
  admin,
  cliente,
  contarPorParticao,
  limparLab,
  produtor,
  recriarTopicos,
} from './_shared/kafka.js';
import {
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

const PRINCIPAL = 'lab.retry.pedidos';
const GRUPO = 'g1';

/** Atrasos curtos para o exemplo rodar em segundos. Em produção: 5s, 1m, 10m. */
const ESCADA = [
  { sufixo: 'retry-500ms', atrasoMs: 500 },
  { sufixo: 'retry-1s', atrasoMs: 1_000 },
  { sufixo: 'retry-2s', atrasoMs: 2_000 },
];

const topicoRetry = (n: number) => `${PRINCIPAL}.${GRUPO}.${ESCADA[n]!.sufixo}`;
const DLT = `${PRINCIPAL}.${GRUPO}.DLT`;

type Msg = { id: string; tipo: 'transitorio' | 'permanente'; falharAte?: number };

const MENSAGENS: Msg[] = [
  { id: 'A', tipo: 'transitorio', falharAte: 2 }, // recupera no 2º degrau
  { id: 'B', tipo: 'permanente' }, // DLT direto
  { id: 'C', tipo: 'transitorio', falharAte: 99 }, // esgota a escada
];

class ErroDeNegocio extends Error {
  constructor(
    msg: string,
    readonly permanente: boolean,
  ) {
    super(msg);
  }
}

type Evento = { msg: string; topico: string; tentativa: number; desfecho: string };
const trilha: Evento[] = [];
const terminais = new Set<string>();

async function main() {
  titulo('05 — Escada de retry e DLT', 'Tópicos e atrasos reais; três mensagens, três destinos.');

  const kafka = cliente('05');
  const a = await admin(kafka);
  const p = await produtor(kafka);

  passo('Criando a topologia da escada');
  await recriarTopicos(a, [
    { topic: PRINCIPAL, numPartitions: 1 },
    ...ESCADA.map((_, i) => ({ topic: topicoRetry(i), numPartitions: 1 })),
    { topic: DLT, numPartitions: 1 },
  ]);
  tabela(
    ['tópico', 'papel'],
    [
      [PRINCIPAL, 'principal'],
      ...ESCADA.map((e, i) => [topicoRetry(i), `degrau ${i + 1} · atraso ${e.atrasoMs}ms`]),
      [DLT, 'fim da linha'],
    ],
  );
  detalhe('a escada é por (tópico de origem, consumer group): o retry do Inventory não');
  detalhe('interfere no do Payment sobre o mesmo tópico de origem');

  passo('Subindo um consumidor por degrau');
  detalhe('cada degrau tem consumidor próprio porque o atraso de um não pode bloquear os');
  detalhe('outros. A alternativa é um consumidor só com pause/resume da partição — mais');
  detalhe('código, mesmo efeito.');

  const consumidores: Consumer[] = [];
  consumidores.push(await subir(kafka, p, PRINCIPAL, `lab-${GRUPO}`, 0));
  for (let i = 0; i < ESCADA.length; i++) {
    consumidores.push(
      await subir(
        kafka,
        p,
        topicoRetry(i),
        `lab-${GRUPO}-${ESCADA[i]!.sufixo}`,
        ESCADA[i]!.atrasoMs,
      ),
    );
  }

  passo(`Publicando ${MENSAGENS.length} mensagens no tópico principal`);
  await p.send({
    topic: PRINCIPAL,
    messages: MENSAGENS.map((m) => ({ key: m.id, value: JSON.stringify(m) })),
  });
  for (const m of MENSAGENS) {
    info(
      `${m.id}: ${m.tipo}${m.falharAte !== undefined ? ` (falha até a tentativa ${m.falharAte})` : ''}`,
    );
  }

  passo('Acompanhando a jornada de cada mensagem');
  detalhe('cada linha é UMA tentativa de processamento, e o desfecho dela');
  const limite = Date.now() + 20_000;
  while (terminais.size < MENSAGENS.length && Date.now() < limite) await dorme(150);

  for (const c of consumidores) await c.disconnect();

  tabela(
    ['msg', 'tópico', 'tentativa', 'desfecho'],
    trilha.map((e) => [
      e.msg,
      e.topico.replace(`${PRINCIPAL}.${GRUPO}.`, '').replace(PRINCIPAL, '(principal)'),
      String(e.tentativa),
      e.desfecho,
    ]),
  );

  passo('Verificação');
  const caminho = (id: string) => trilha.filter((e) => e.msg === id);

  const a1 = caminho('A');
  confereIgual(a1.length, 3, 'A foi processada 3× — principal, degrau 1, degrau 2');
  confereIgual(a1.at(-1)?.desfecho, 'sucesso', 'e recuperou no degrau 2, sem chegar à DLT');
  confereIgual(
    a1.map((e) => e.tentativa),
    [0, 1, 2],
    'o x-retry-count subiu a cada desvio',
  );

  const b1 = caminho('B');
  confereIgual(b1.length, 1, 'B foi processada 1× SÓ — nenhum degrau da escada foi gasto');
  confereIgual(b1.at(-1)?.desfecho, 'DLT', 'erro permanente vai direto para a DLT');
  confereIgual(b1[0]?.topico, PRINCIPAL, 'e saiu do próprio tópico principal');

  const c1 = caminho('C');
  confereIgual(c1.length, ESCADA.length + 1, 'C foi processada 4× — principal + os 3 degraus');
  confereIgual(c1.at(-1)?.desfecho, 'DLT', 'e só então caiu na DLT');
  confereIgual(c1.at(-1)?.tentativa, ESCADA.length, 'chegou na DLT com x-retry-count = 3');

  const [naDlt] = await contarPorParticao(a, DLT);
  confereIgual(naDlt, 2, 'a DLT tem exatamente 2 mensagens: B e C');

  passo('Headers de uma mensagem na DLT');
  const headers = await lerDlt(kafka, DLT);
  tabela(
    ['header', 'valor'],
    Object.entries(headers).map(([k, v]) => [k, v]),
  );
  confere(
    'x-stacktrace-hash' in headers,
    'o stacktrace vai como HASH, não inteiro — stacktrace carrega payload, e payload carrega PII',
  );

  passo('E a partição principal?');
  const lagPrincipal = await a.fetchOffsets({ groupId: `lab-${GRUPO}`, topics: [PRINCIPAL] });
  const commitado = Number(lagPrincipal[0]?.partitions[0]?.offset ?? 0);
  confereIgual(
    commitado,
    MENSAGENS.length,
    'todos os offsets do principal foram commitados — a partição nunca travou',
  );

  licao(
    'Compare as jornadas: B gastou UMA tentativa, C gastou quatro, e as duas terminaram\n' +
      '  no mesmo lugar. A diferença inteira foi a classificação do erro.\n' +
      '\n' +
      '  O que mais se erra aqui:\n' +
      '  1. Não classificar o erro. Schema inválido não melhora em 10 minutos: ele gasta a\n' +
      '     escada inteira e chega na DLT igual, só 11 minutos depois.\n' +
      '  2. Esquecer de commitar o offset do tópico PRINCIPAL ao desviar. É esse commit — e\n' +
      '     não o retry — que devolve a partição ao fluxo. Sem ele você construiu a escada e\n' +
      '     manteve o travamento que ela existia para resolver.\n' +
      '  3. E o preço inevitável: desviada para o retry, a mensagem pode ser processada DEPOIS\n' +
      '     da seguinte do mesmo pedido. A ordenação daquele agregado morre ali. A defesa é a\n' +
      '     máquina de estados monotônica, que rejeita transição inválida em vez de corromper.\n' +
      '     Onde ordem estrita valer mais que throughput, a escolha certa é o oposto: aceitar\n' +
      '     o travamento e retryar in-place.\n' +
      '\n' +
      '  E lembre: DLT sem CLI de reprocessamento é cemitério. Ninguém abre um tópico com\n' +
      '  4 mil mensagens na mão.',
  );

  await p.disconnect();
  await limparLab(a);
  await a.disconnect();
  fim();
}

/** Um consumidor de degrau: aplica o atraso, processa, e roteia em caso de erro. */
async function subir(
  kafka: Kafka,
  p: Producer,
  topico: string,
  grupo: string,
  atrasoMs: number,
): Promise<Consumer> {
  const c = kafka.consumer({ groupId: grupo, sessionTimeout: 10_000 });
  await c.connect();
  await c.subscribe({ topic: topico, fromBeginning: true });

  await c.run({
    autoCommit: false,
    eachMessage: async ({ message, partition }) => {
      if (atrasoMs > 0) await dorme(atrasoMs);

      const msg = JSON.parse(message.value!.toString()) as Msg;
      const tentativa = Number(message.headers?.['x-retry-count']?.toString() ?? '0');
      const commitar = () =>
        c.commitOffsets([{ topic: topico, partition, offset: String(Number(message.offset) + 1) }]);

      try {
        if (msg.tipo === 'permanente') {
          throw new ErroDeNegocio('payload viola o schema da v1', true);
        }
        if (tentativa < (msg.falharAte ?? 0)) {
          throw new ErroDeNegocio('ETIMEDOUT ao chamar o serviço de saldo', false);
        }
        trilha.push({ msg: msg.id, topico, tentativa, desfecho: 'sucesso' });
        terminais.add(msg.id);
        await commitar();
      } catch (e) {
        const erro = e as ErroDeNegocio;
        const proximo = erro.permanente || tentativa >= ESCADA.length ? null : tentativa;

        if (proximo === null) {
          await p.send({
            topic: DLT,
            messages: [
              {
                key: msg.id,
                value: message.value,
                headers: {
                  'x-original-topic': topico,
                  'x-retry-count': String(tentativa),
                  'x-last-error': erro.message,
                  // Hash, nunca o stacktrace inteiro: ele carrega payload, e payload carrega PII.
                  'x-stacktrace-hash': 'sha256:' + hash(erro.message),
                  'x-consumer-group': GRUPO,
                  'x-motivo': erro.permanente ? 'erro permanente' : 'escada esgotada',
                },
              },
            ],
          });
          trilha.push({ msg: msg.id, topico, tentativa, desfecho: 'DLT' });
          terminais.add(msg.id);
        } else {
          await p.send({
            topic: topicoRetry(proximo),
            messages: [
              {
                key: msg.id,
                value: message.value,
                headers: { 'x-retry-count': String(tentativa + 1), 'x-original-topic': PRINCIPAL },
              },
            ],
          });
          trilha.push({ msg: msg.id, topico, tentativa, desfecho: `→ ${ESCADA[proximo]!.sufixo}` });
        }

        // SEMPRE commitar depois de rotear. Este é o commit que libera a partição.
        await commitar();
      }
    },
  });

  return c;
}

async function lerDlt(kafka: Kafka, topico: string): Promise<Record<string, string>> {
  const c = kafka.consumer({ groupId: `lab-leitor-dlt-${Date.now()}` });
  await c.connect();
  await c.subscribe({ topic: topico, fromBeginning: true });
  const headers = await new Promise<Record<string, string>>((resolve) => {
    c.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        resolve(
          Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, v]) => [k, String(v)])),
        );
      },
    });
  });
  await c.disconnect();
  return headers;
}

function hash(s: string): string {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0).toString(16).padStart(8, '0');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
