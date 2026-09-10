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
  | { changed: false; next: OrderStatus; reason: 'invalid-transition' };

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

/**
 * Regra de ouro (docs/PLAN.md §1): a transição é monotônica e idempotente.
 * Nunca lança — evento fora de ordem ou reentrega é dado do dia a dia da
 * coreografia, não uma exceção. O chamador decide o que fazer com
 * `reason: 'invalid-transition'` (tipicamente: logar em WARN e não tocar
 * no agregado).
 */
export function applyEvent(current: OrderStatus, eventType: ProjectionEventType): ProjectionResult {
  const next = TRANSITIONS[current]?.[eventType];

  if (!next) {
    return { changed: false, next: current, reason: 'invalid-transition' };
  }

  return { changed: true, next };
}
