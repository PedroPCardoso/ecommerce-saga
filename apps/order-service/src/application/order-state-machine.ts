import { ORDER_STATUS, type OrderStatus } from '@ecommerce/contracts';

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
