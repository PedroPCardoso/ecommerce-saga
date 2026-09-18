import {
  CANCELLATION_REASON,
  COMPENSATION_TYPE,
  ORDER_STATUS,
  type CancellationReason,
  type CompensationType,
  type OrderStatus,
} from '@ecommerce/contracts';

/**
 * Eventos de saga que este projetor entende. Cada um mapeia para uma
 * transição de estado válida a partir de um estado de origem específico —
 * não existe transição "de qualquer estado".
 */
export type ProjectionEventType =
  | 'payment.approved'
  | 'payment.failed'
  | 'stock.reserved'
  | 'stock.unavailable'
  | 'shipment.created'
  | 'shipment.failed';

export type ProjectionResult =
  | { changed: true; next: OrderStatus }
  /**
   * O pedido já passou deste ponto (reentrega tardia, ou evento superado por
   * um evento mais recente que chegou primeiro) — seguro ignorar e commitar.
   */
  | { changed: false; next: OrderStatus; reason: 'stale' }
  /**
   * O pedido AINDA NÃO chegou ao estado que este evento exige — a mesma
   * corrida legítima entre tópicos diferentes que Inventory/Shipping já
   * tratam (ex.: payment.approved-handler.ts, stock-reserved.handler.ts):
   * nada garante ordem ENTRE `ecommerce.payments.v1`, `ecommerce.inventory.v1`
   * e `ecommerce.shipping.v1`, e o consumidor único deste projetor os lê
   * concorrentemente. O chamador DEVE tratar isto como retriável (nunca como
   * "ignora e segue"), ou o evento se perde para sempre.
   */
  | { changed: false; next: OrderStatus; reason: 'premature' };

/**
 * Tabela de transição: `[estado-de-origem, evento] -> próximo estado`.
 * `COMPENSATING` como destino de `stock.unavailable`/`shipment.failed` é
 * intencional e NÃO chega a `CANCELLED` sozinho: a matriz de compensação
 * (packages/contracts `COMPENSATION_MATRIX`) exige `payment.refunded` e/ou
 * `stock.released` antes de fechar o pedido, e nenhum dos dois serviços
 * ainda emite esses eventos (compensação real fica para um trabalho futuro
 * — rastreado como I4 na revisão final). Até lá, pedidos que caem em
 * `stock.unavailable`/`shipment.failed` ficam visivelmente presos em
 * `COMPENSATING`, o que é honesto: nada foi de fato compensado ainda.
 */
const TRANSITIONS: Record<OrderStatus, Partial<Record<ProjectionEventType, OrderStatus>>> = {
  [ORDER_STATUS.PENDING]: {
    'payment.approved': ORDER_STATUS.PAYMENT_APPROVED,
    'payment.failed': ORDER_STATUS.CANCELLED,
  },
  [ORDER_STATUS.PAYMENT_APPROVED]: {
    'stock.reserved': ORDER_STATUS.STOCK_RESERVED,
    'stock.unavailable': ORDER_STATUS.COMPENSATING,
  },
  [ORDER_STATUS.STOCK_RESERVED]: {
    'shipment.created': ORDER_STATUS.CONFIRMED,
    'shipment.failed': ORDER_STATUS.COMPENSATING,
  },
  [ORDER_STATUS.COMPENSATING]: {},
  [ORDER_STATUS.CONFIRMED]: {},
  [ORDER_STATUS.CANCELLED]: {},
};

/** Posição de cada evento na linha do tempo da saga — o estado de ORIGEM que ele exige. */
const REQUIRED_SOURCE_RANK: Record<ProjectionEventType, number> = {
  'payment.approved': 0,
  'payment.failed': 0,
  'stock.reserved': 1,
  'stock.unavailable': 1,
  'shipment.created': 2,
  'shipment.failed': 2,
};

/** Posição de cada estado do pedido na mesma linha do tempo. */
const STATUS_RANK: Record<OrderStatus, number> = {
  [ORDER_STATUS.PENDING]: 0,
  [ORDER_STATUS.PAYMENT_APPROVED]: 1,
  [ORDER_STATUS.STOCK_RESERVED]: 2,
  [ORDER_STATUS.COMPENSATING]: 3,
  [ORDER_STATUS.CONFIRMED]: 3,
  [ORDER_STATUS.CANCELLED]: 3,
};

/**
 * Regra de ouro (docs/PLAN.md §1): a transição é monotônica e idempotente.
 * Nunca lança — quem decide o que fazer com uma transição inválida é o
 * chamador, a partir do `reason`. A distinção `stale` vs. `premature` é o
 * que evita repetir, aqui, o próprio bug que este projetor existe para
 * corrigir (C2 da revisão final): tratar TODA transição inválida como
 * "ignora e commita" perderia para sempre um evento que só chegou cedo
 * demais, e o pedido ficaria preso não mais em PENDING, mas num estado
 * intermediário qualquer — mesmo destino, porta diferente.
 */
export function applyEvent(current: OrderStatus, eventType: ProjectionEventType): ProjectionResult {
  const next = TRANSITIONS[current]?.[eventType];

  if (next) {
    return { changed: true, next };
  }

  const reason = STATUS_RANK[current] < REQUIRED_SOURCE_RANK[eventType] ? 'premature' : 'stale';
  return { changed: false, next: current, reason };
}

export type CompensationEventType = 'payment.refunded' | 'stock.released';

export interface OrderCompensationState {
  status: OrderStatus;
  compensationReason: CancellationReason | null;
  compensationsReceived: CompensationType[];
}

export type CompensationResult =
  | { changed: true; next: OrderStatus; compensationsReceived: CompensationType[] }
  | { changed: false; reason: 'stale' }
  | { changed: false; reason: 'premature' };

const COMPENSATION_TYPE_BY_EVENT: Record<CompensationEventType, CompensationType> = {
  'payment.refunded': COMPENSATION_TYPE.PAYMENT_REFUNDED,
  'stock.released': COMPENSATION_TYPE.STOCK_RELEASED,
};

/**
 * Quais compensações uma falha exige antes do pedido poder fechar em
 * CANCELLED. `stock.unavailable`: nada foi reservado ainda, só o pagamento
 * precisa voltar. `shipment.failed`: o pagamento JÁ estava autorizado E o
 * estoque JÁ estava reservado — as duas precisam ser desfeitas, em qualquer
 * ordem (docs/PLAN.md: "Order só fecha quando as duas chegarem").
 */
const REQUIRED_COMPENSATIONS: Record<CancellationReason, readonly CompensationType[]> = {
  [CANCELLATION_REASON.STOCK_UNAVAILABLE]: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
  [CANCELLATION_REASON.SHIPMENT_FAILED]: [
    COMPENSATION_TYPE.PAYMENT_REFUNDED,
    COMPENSATION_TYPE.STOCK_RELEASED,
  ],
  // Os dois motivos abaixo não usam COMPENSATING nesta versão do projetor
  // (payment.failed fecha direto em CANCELLED; CUSTOMER_REQUEST não é
  // disparado por este projetor) — mapeados só para o Record ficar total.
  [CANCELLATION_REASON.PAYMENT_FAILED]: [],
  // Sweeper desta fase só varre pedidos presos em PAYMENT_APPROVED (nunca chegaram a
  // reservar estoque) — só o pagamento precisa voltar. Se um sweeper futuro passar a
  // cobrir pedidos presos em STOCK_RESERVED também, este valor precisa virar
  // [PAYMENT_REFUNDED, STOCK_RELEASED] E o sweeper precisa saber distinguir os dois
  // casos (não é o caso hoje — ver saga-timeout-sweeper.service.ts).
  [CANCELLATION_REASON.SAGA_TIMEOUT]: [COMPENSATION_TYPE.PAYMENT_REFUNDED],
  [CANCELLATION_REASON.CUSTOMER_REQUEST]: [],
};

/**
 * Decide o efeito de um evento de compensação (`payment.refunded`/
 * `stock.released`) sobre um pedido. Só produz `changed: true` quando o
 * pedido JÁ está em COMPENSATING — chegar antes disso é 'premature'
 * (retriável: a mesma corrida entre tópicos que `applyEvent` já trata) e
 * chegar depois de CONFIRMED/CANCELLED é 'stale' (seguro ignorar). Usa
 * `STATUS_RANK` (já existe no arquivo, usado por `applyEvent`) para
 * distinguir os dois casos: qualquer status "antes" de COMPENSATING na
 * linha do tempo é premature; "depois" (CONFIRMED/CANCELLED, mesmo rank de
 * COMPENSATING) é stale.
 */
export function applyCompensationEvent(
  order: OrderCompensationState,
  eventType: CompensationEventType,
): CompensationResult {
  if (order.status !== ORDER_STATUS.COMPENSATING) {
    // TERMINAL_STATUS_RANK e STATUS_RANK já existem no arquivo (usados por applyEvent).
    // Qualquer status "antes" de COMPENSATING é corrida legítima (premature); qualquer
    // status "depois" (CONFIRMED/CANCELLED, ambos rank 3, igual a COMPENSATING) já
    // fechou e um evento de compensação chegando agora é tardio (stale).
    const reason = STATUS_RANK[order.status] < STATUS_RANK[ORDER_STATUS.COMPENSATING] ? 'premature' : 'stale';
    return { changed: false, reason };
  }
  if (!order.compensationReason) {
    // Nunca deveria acontecer na prática (compensationReason é setado no mesmo UPDATE
    // que leva o pedido a COMPENSATING — ver order-projection.handler.ts), mas um
    // schema.prisma sem essa garantia em nível de banco pede a defesa em código.
    return { changed: false, reason: 'stale' };
  }

  const compensationType = COMPENSATION_TYPE_BY_EVENT[eventType];
  if (order.compensationsReceived.includes(compensationType)) {
    return { changed: false, reason: 'stale' };
  }

  const compensationsReceived = [...order.compensationsReceived, compensationType];
  const required = REQUIRED_COMPENSATIONS[order.compensationReason];
  const complete = required.every((type) => compensationsReceived.includes(type));

  return {
    changed: true,
    next: complete ? ORDER_STATUS.CANCELLED : ORDER_STATUS.COMPENSATING,
    compensationsReceived,
  };
}
