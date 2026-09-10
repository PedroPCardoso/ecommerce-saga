/**
 * 01 — A chave de partição é o que dá ordenação
 *
 * PERGUNTA: por que `aggregateId: orderId` virou a chave de toda mensagem?
 *
 * Kafka garante ordem DENTRO de uma partição, nunca entre partições. Sem chave,
 * o produtor espalha em round-robin e os eventos de um mesmo pedido caem em
 * partições diferentes — que são consumidas em paralelo. `payment.refunded` pode
 * ser processado antes de `payment.approved`.
 *
 * Este exemplo prova as duas coisas, de forma determinística: conta em quantas
 * partições distintas os eventos de cada pedido caíram.
 *
 *   pnpm ex 01
 */
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
  fim,
  info,
  licao,
  passo,
  tabela,
  titulo,
} from './_shared/log.js';

const SEM_CHAVE = 'lab.ordenacao.sem-chave';
const COM_CHAVE = 'lab.ordenacao.com-chave';
const PARTICOES = 3;

const PEDIDOS = ['pedido-A', 'pedido-B', 'pedido-C', 'pedido-D'];
const ETAPAS = ['order.created', 'payment.approved', 'stock.reserved', 'shipment.created'];

async function main() {
  titulo(
    '01 — A chave de partição é o que dá ordenação',
    'Mesmos eventos, dois tópicos: um sem chave, um com orderId como chave.',
  );

  const kafka = cliente('01');
  const a = await admin(kafka);
  const p = await produtor(kafka);

  passo(`Criando dois tópicos com ${PARTICOES} partições cada`);
  await recriarTopicos(a, [
    { topic: SEM_CHAVE, numPartitions: PARTICOES },
    { topic: COM_CHAVE, numPartitions: PARTICOES },
  ]);
  detalhe(`${SEM_CHAVE} e ${COM_CHAVE}`);

  passo(
    `Publicando ${PEDIDOS.length * ETAPAS.length} eventos — ${ETAPAS.length} por pedido, intercalados`,
  );
  // Intercalar é o ponto: em produção os eventos de pedidos diferentes se misturam.
  for (const etapa of ETAPAS) {
    for (const pedido of PEDIDOS) {
      const valor = JSON.stringify({ orderId: pedido, eventType: etapa });
      await p.send({ topic: SEM_CHAVE, messages: [{ value: valor }] });
      await p.send({ topic: COM_CHAVE, messages: [{ key: pedido, value: valor }] });
    }
  }
  info(`por pedido: ${ETAPAS.join(' → ')}`);
  detalhe('sem chave o kafkajs distribui em round-robin; com chave usa hash(chave) % partições');

  passo('Onde cada evento foi parar');
  const distribuicao = async (topic: string) => {
    const consumidor = kafka.consumer({ groupId: `leitor-${topic}-${Date.now()}` });
    await consumidor.connect();
    await consumidor.subscribe({ topic, fromBeginning: true });

    const porPedido = new Map<string, Set<number>>();
    let lidas = 0;
    const total = PEDIDOS.length * ETAPAS.length;

    await new Promise<void>((resolve, reject) => {
      consumidor
        .run({
          autoCommit: false,
          eachMessage: async ({ message, partition }) => {
            const { orderId } = JSON.parse(message.value!.toString()) as { orderId: string };
            if (!porPedido.has(orderId)) porPedido.set(orderId, new Set());
            porPedido.get(orderId)!.add(partition);
            if (++lidas >= total) resolve();
          },
        })
        .catch(reject);
    });

    await consumidor.disconnect();
    return porPedido;
  };

  const semChave = await distribuicao(SEM_CHAVE);
  const comChave = await distribuicao(COM_CHAVE);

  const linhas = PEDIDOS.map((pedido) => {
    const s = [...(semChave.get(pedido) ?? [])].sort();
    const c = [...(comChave.get(pedido) ?? [])].sort();
    return [
      pedido,
      s.map((n) => `p${n}`).join(' ') + `  (${s.length})`,
      c.map((n) => `p${n}`).join(' ') + `  (${c.length})`,
    ];
  });
  tabela(['pedido', 'sem chave', 'com chave'], linhas);

  passo('O que isso significa');
  const espalhados = PEDIDOS.filter((x) => (semChave.get(x)?.size ?? 0) > 1);
  const concentrados = PEDIDOS.filter((x) => comChave.get(x)?.size === 1);

  confereIgual(
    espalhados.length,
    PEDIDOS.length,
    'sem chave, TODO pedido teve eventos espalhados em mais de uma partição → ordem NÃO garantida',
  );
  confereIgual(
    concentrados.length,
    PEDIDOS.length,
    'com chave, todo pedido ficou numa única partição → ordem garantida por pedido',
  );

  passo('Distribuição entre as partições (o paralelismo não foi sacrificado)');
  const [dSem, dCom] = await Promise.all([
    contarPorParticao(a, SEM_CHAVE),
    contarPorParticao(a, COM_CHAVE),
  ]);
  tabela(
    ['tópico', 'p0', 'p1', 'p2'],
    [
      ['sem chave', ...dSem.map(String)],
      ['com chave', ...dCom.map(String)],
    ],
  );
  confere(
    dCom.filter((n) => n > 0).length > 1,
    'com chave, as mensagens continuam distribuídas em várias partições — ' +
      'ordenar por pedido não serializou o tópico',
  );

  licao(
    'A chave não é metadado decorativo: é a única coisa que amarra eventos do mesmo\n' +
      '  agregado à mesma partição, e portanto à mesma ordem. Cuidado com dois detalhes:\n' +
      '  (a) a garantia vale POR TÓPICO — o Order Service consome três tópicos e ainda\n' +
      '      pode ver stock.reserved antes de payment.approved;\n' +
      '  (b) mudar o número de partições reembaralha hash(chave) % n e quebra a ordem\n' +
      '      dos pedidos em voo. Repartitionar é operação planejada, não ajuste de capacidade.\n' +
      '\n' +
      '  Nota de quem tropeçou: a primeira versão deste exemplo usava 3 pedidos e 3 partições.\n' +
      '  O round-robin sem chave alinhou cada pedido a uma partição fixa por acidente aritmético,\n' +
      '  e a demonstração "passava" pelo motivo errado. Quatro pedidos em três partições torna\n' +
      '  esse alinhamento impossível. Teste que acerta por acaso é pior que teste ausente,\n' +
      '  porque você acredita nele.',
  );

  await p.disconnect();
  await limparLab(a);
  await a.disconnect();
  fim();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
