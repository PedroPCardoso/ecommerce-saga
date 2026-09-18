import { z } from 'zod';

/**
 * Dinheiro é **sempre** inteiro em centavos.
 *
 * Float em dinheiro é bug de produção esperando data: 0.1 + 0.2 !== 0.3, e num sistema com
 * estorno você compara valores o tempo todo. `amountCents` também torna os gatilhos de
 * simulação determinísticos (`amountCents % 100 === 13`), sem depender de formatação.
 */
export const amountCentsSchema = z
  .number()
  .int('Dinheiro em centavos: precisa ser inteiro, não float')
  .nonnegative();

export const currencySchema = z.enum(['BRL', 'USD', 'EUR']);
export type Currency = z.infer<typeof currencySchema>;

export const skuSchema = z
  .string()
  .min(1)
  .max(64)
  .regex(/^[A-Z0-9-]+$/, 'SKU aceita apenas A-Z, 0-9 e hífen');

export const quantitySchema = z.number().int().positive().max(1000);

export const orderItemSchema = z.object({
  sku: skuSchema,
  name: z.string().min(1).max(200),
  quantity: quantitySchema,
  unitPriceCents: amountCentsSchema,
});
export type OrderItem = z.infer<typeof orderItemSchema>;

export const reservedItemSchema = z.object({
  sku: skuSchema,
  quantity: quantitySchema,
});
export type ReservedItem = z.infer<typeof reservedItemSchema>;

/**
 * Endereço é PII. Ele viaja no evento porque o Shipping precisa dele, mas:
 * - nunca vai para log (o logger mascara — ver `packages/observability`);
 * - a DLT que puder conter isto tem retenção curta;
 * - em produção, tráfego cifrado ponta a ponta (A04).
 */
export const addressSchema = z.object({
  street: z.string().min(1).max(200),
  number: z.string().min(1).max(20),
  complement: z.string().max(100).optional(),
  district: z.string().min(1).max(100),
  city: z.string().min(1).max(100),
  state: z.string().length(2),
  zipCode: z.string().regex(/^\d{5}-?\d{3}$/, 'CEP no formato 00000-000'),
  country: z.string().length(2).default('BR'),
});
export type Address = z.infer<typeof addressSchema>;

/**
 * Estados do pedido. `CONFIRMED` e `CANCELLED` são terminais.
 * `COMPENSATING` é o estado em que a saga está desfazendo o que já fez.
 */
export const ORDER_STATUS = {
  PENDING: 'PENDING',
  PAYMENT_APPROVED: 'PAYMENT_APPROVED',
  STOCK_RESERVED: 'STOCK_RESERVED',
  COMPENSATING: 'COMPENSATING',
  CONFIRMED: 'CONFIRMED',
  CANCELLED: 'CANCELLED',
} as const;

export const orderStatusSchema = z.nativeEnum(ORDER_STATUS);
export type OrderStatus = z.infer<typeof orderStatusSchema>;

export const TERMINAL_ORDER_STATUSES: readonly OrderStatus[] = [
  ORDER_STATUS.CONFIRMED,
  ORDER_STATUS.CANCELLED,
];

export function isTerminal(status: OrderStatus): boolean {
  return TERMINAL_ORDER_STATUSES.includes(status);
}

/**
 * Motivo de cancelamento — enum, não string livre.
 * String livre em campo de motivo vira relatório impossível de agregar.
 */
export const CANCELLATION_REASON = {
  PAYMENT_FAILED: 'PAYMENT_FAILED',
  STOCK_UNAVAILABLE: 'STOCK_UNAVAILABLE',
  SHIPMENT_FAILED: 'SHIPMENT_FAILED',
  SAGA_TIMEOUT: 'SAGA_TIMEOUT',
  CUSTOMER_REQUEST: 'CUSTOMER_REQUEST',
} as const;

export const cancellationReasonSchema = z.nativeEnum(CANCELLATION_REASON);
export type CancellationReason = z.infer<typeof cancellationReasonSchema>;

/**
 * O que a saga já desfez ao fechar um pedido em CANCELLED. Nomeado (não enum
 * inline) porque `order-state-machine.ts` do Order Service precisa comparar
 * contra ele para decidir se uma compensação pendente já está completa.
 */
export const COMPENSATION_TYPE = {
  PAYMENT_REFUNDED: 'PAYMENT_REFUNDED',
  STOCK_RELEASED: 'STOCK_RELEASED',
} as const;

export const compensationTypeSchema = z.nativeEnum(COMPENSATION_TYPE);
export type CompensationType = z.infer<typeof compensationTypeSchema>;
