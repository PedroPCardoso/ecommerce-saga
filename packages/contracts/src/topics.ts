/**
 * Topologia Kafka.
 *
 * Regra que sustenta tudo: a **chave** de toda mensagem de negócio é o `orderId`.
 * Isso joga todos os eventos de um pedido na mesma partição, logo eles são ordenados
 * entre si. Pedidos diferentes podem se cruzar — e não importa.
 */
export const TOPICS = {
  orders: 'ecommerce.orders.v1',
  payments: 'ecommerce.payments.v1',
  inventory: 'ecommerce.inventory.v1',
  shipping: 'ecommerce.shipping.v1',
  /**
   * Tópicos do harness de comparação orquestrada (Fase 11, ADR-0012) — isolados dos
   * tópicos de negócio acima. Nenhum dos 5 serviços de produção assina ou publica
   * nestes tópicos; só `apps/saga-orchestrator-service` (orquestrador + executores
   * "burros" simulados no mesmo processo).
   */
  commandsPayment: 'ecommerce.commands.payment.v1',
  commandsInventory: 'ecommerce.commands.inventory.v1',
  commandsShipping: 'ecommerce.commands.shipping.v1',
  responsesOrchestrator: 'ecommerce.responses.orchestrator.v1',
} as const;

export type BusinessTopic = (typeof TOPICS)[keyof typeof TOPICS];

export const ALL_BUSINESS_TOPICS: readonly BusinessTopic[] = Object.values(TOPICS);

/**
 * Consumer groups. O nome é estável e faz parte da chave de idempotência
 * `(eventId, consumerGroup)`: dois grupos distintos precisam processar o mesmo evento.
 * Renomear um grupo aqui = reprocessar o tópico inteiro. Trate como contrato.
 */
export const CONSUMER_GROUPS = {
  payment: 'payment-service',
  inventory: 'inventory-service',
  shipping: 'shipping-service',
  notification: 'notification-service',
  /** Projeção de estado do pedido dentro do Order Service. */
  orderProjection: 'order-projection',
  /** Painel observável (Fase 7b) — só lê, nunca escreve, nenhum efeito de negócio. */
  sagaObserver: 'saga-observer',
  /**
   * Harness de comparação orquestrada (Fase 11, ADR-0012). Os 4 grupos abaixo vivem
   * dentro do MESMO processo (`apps/saga-orchestrator-service`), mas em consumer
   * groups Kafka distintos porque consomem tópicos distintos.
   */
  orchestrator: 'saga-orchestrator',
  paymentExecutor: 'payment-executor',
  inventoryExecutor: 'inventory-executor',
  shippingExecutor: 'shipping-executor',
} as const;

export type ConsumerGroup = (typeof CONSUMER_GROUPS)[keyof typeof CONSUMER_GROUPS];

/**
 * Retry escalonado, um degrau por atraso.
 *
 * Por que não retryar no próprio tópico: um pedido problemático travaria a partição inteira
 * (head-of-line blocking) e junto com ela todos os outros pedidos que caíram lá.
 *
 * TRADE-OFF a não esquecer: ao desviar para o tópico de retry você **perde a ordenação**
 * daquele pedido — a mensagem seguinte do mesmo orderId pode ser processada antes da que
 * foi desviada. A defesa é a máquina de estados idempotente e monotônica, que rejeita
 * transição inválida em vez de corromper o agregado.
 */
export const RETRY_LADDER = [
  { suffix: 'retry-5s', delayMs: 5_000 },
  { suffix: 'retry-1m', delayMs: 60_000 },
  { suffix: 'retry-10m', delayMs: 600_000 },
] as const;

export const MAX_RETRY_ATTEMPTS = RETRY_LADDER.length;

/** `ecommerce.payments.v1.inventory-service.retry-5s` */
export function retryTopic(
  sourceTopic: string,
  consumerGroup: ConsumerGroup,
  attempt: number,
): string {
  const step = RETRY_LADDER[attempt];
  if (!step) {
    throw new Error(
      `Tentativa ${attempt} fora da escada de retry (máx ${MAX_RETRY_ATTEMPTS}). ` +
        'Quem chamou deveria ter mandado para a DLT.',
    );
  }
  return `${sourceTopic}.${consumerGroup}.${step.suffix}`;
}

/** `ecommerce.payments.v1.inventory-service.DLT` */
export function deadLetterTopic(sourceTopic: string, consumerGroup: ConsumerGroup): string {
  return `${sourceTopic}.${consumerGroup}.DLT`;
}

/**
 * Quem consome o quê.
 *
 * Olhe o `payment-service`: ele assina `inventory` e `shipping` — domínios que não são dele —
 * porque em coreografia **cada serviço precisa conhecer as falhas de todos os passos
 * posteriores** para compensar. Adicionar uma 5ª etapa na saga obriga a mexer aqui e em todos
 * os serviços anteriores. Esse acoplamento implícito é a lição central do exercício (ADR-0002).
 */
export const SUBSCRIPTIONS: Readonly<Record<ConsumerGroup, readonly BusinessTopic[]>> = {
  [CONSUMER_GROUPS.payment]: [TOPICS.orders, TOPICS.inventory, TOPICS.shipping],
  /**
   * `orders` está aqui por um motivo que não é óbvio: `payment.approved` carrega só
   * dados de pagamento (paymentId, amountCents, instrument) — nunca os SKUs do pedido,
   * porque isso não é assunto de Payment. Inventory precisa saber O QUE reservar, e só
   * `order.created` tem essa informação. Ele assina os dois: aprende os itens pelo
   * primeiro, e é disparado a agir pelo segundo. Se `payment.approved` chegar antes de
   * `order.created` ser processado (nada garante ordem ENTRE tópicos diferentes), o
   * handler trata isso como erro RETRIÁVEL — não permanente — e a escada de retry dá
   * tempo para o outro evento chegar. Mais um pedaço do acoplamento implícito da
   * coreografia (ADR-0002): a lista de itens do pedido, que é dado do Order, vaza para
   * o modelo de consumo do Inventory.
   */
  [CONSUMER_GROUPS.inventory]: [TOPICS.orders, TOPICS.payments, TOPICS.shipping],
  /**
   * Mesma razão do Inventory acima: `stock.reserved` não carrega endereço de entrega
   * (não é assunto do Inventory). Shipping só sabe para onde mandar a etiqueta porque
   * também assina `order.created`. E pela mesma lógica, `stock.reserved` chegando antes
   * do `order.created` correspondente é erro RETRIÁVEL, não permanente.
   */
  [CONSUMER_GROUPS.shipping]: [TOPICS.orders, TOPICS.inventory],
  [CONSUMER_GROUPS.notification]: [
    TOPICS.orders,
    TOPICS.payments,
    TOPICS.inventory,
    TOPICS.shipping,
  ],
  [CONSUMER_GROUPS.orderProjection]: [TOPICS.payments, TOPICS.inventory, TOPICS.shipping],
  [CONSUMER_GROUPS.sagaObserver]: [TOPICS.orders, TOPICS.payments, TOPICS.inventory, TOPICS.shipping],
  // Harness de comparação orquestrada (Fase 11): o orquestrador só assina as
  // respostas dos executores; cada executor só assina o próprio comando. Nenhuma
  // dessas 4 linhas toca em `TOPICS.orders/payments/inventory/shipping`.
  [CONSUMER_GROUPS.orchestrator]: [TOPICS.responsesOrchestrator],
  [CONSUMER_GROUPS.paymentExecutor]: [TOPICS.commandsPayment],
  [CONSUMER_GROUPS.inventoryExecutor]: [TOPICS.commandsInventory],
  [CONSUMER_GROUPS.shippingExecutor]: [TOPICS.commandsShipping],
};

/** Toda a topologia derivada: negócio + retry + DLT. Usado pelo script de criação de tópicos. */
export function allTopics(): string[] {
  const topics = new Set<string>(ALL_BUSINESS_TOPICS);

  for (const [group, sources] of Object.entries(SUBSCRIPTIONS)) {
    for (const source of sources) {
      for (let attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt += 1) {
        topics.add(retryTopic(source, group as ConsumerGroup, attempt));
      }
      topics.add(deadLetterTopic(source, group as ConsumerGroup));
    }
  }

  return [...topics].sort();
}

/** Headers de rastreio que acompanham a mensagem quando ela é desviada para retry ou DLT. */
export const RETRY_HEADERS = {
  originalTopic: 'x-original-topic',
  originalPartition: 'x-original-partition',
  originalOffset: 'x-original-offset',
  retryCount: 'x-retry-count',
  firstFailureAt: 'x-first-failure-at',
  lastError: 'x-last-error',
  /** Hash do stacktrace, nunca o stacktrace inteiro: ele carrega payload e vaza PII (A09). */
  stacktraceHash: 'x-stacktrace-hash',
  consumerGroup: 'x-consumer-group',
} as const;

/** Propagação de trace distribuído (W3C) pelos headers Kafka. */
export const TRACE_HEADERS = {
  traceparent: 'traceparent',
  tracestate: 'tracestate',
} as const;
