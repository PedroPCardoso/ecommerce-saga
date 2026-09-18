import type { z } from 'zod';
import type { AnyEventDefinition, UnknownEnvelope } from './envelope.js';
import { unknownEnvelopeSchema } from './envelope.js';
import * as orderEvents from './events/order.js';
import * as paymentEvents from './events/payment.js';
import * as inventoryEvents from './events/inventory.js';
import * as shippingEvents from './events/shipping.js';
import * as orchestrationEvents from './events/orchestration.js';

export const EVENTS = {
  ...orderEvents,
  ...paymentEvents,
  ...inventoryEvents,
  ...shippingEvents,
  ...orchestrationEvents,
} as const;

/**
 * Os módulos de evento também exportam enums auxiliares (códigos de falha, por exemplo).
 * Este guard separa o que é definição de evento do que é constante de apoio, para que
 * acrescentar um enum novo num módulo não entre por acidente no registro de eventos.
 */
function isEventDefinition(value: unknown): value is AnyEventDefinition {
  return (
    typeof value === 'object' &&
    value !== null &&
    'envelope' in value &&
    'type' in value &&
    'version' in value &&
    'topic' in value
  );
}

/** Todas as definições de evento do sistema, em ordem estável por tipo. */
export const EVENT_DEFINITIONS: readonly AnyEventDefinition[] = (
  [
    ...Object.values(orderEvents),
    ...Object.values(paymentEvents),
    ...Object.values(inventoryEvents),
    ...Object.values(shippingEvents),
    ...Object.values(orchestrationEvents),
  ] as readonly unknown[]
)
  .filter(isEventDefinition)
  .sort((a, b) => a.type.localeCompare(b.type) || a.version - b.version);

const DEFINITIONS = EVENT_DEFINITIONS;

/**
 * Índice `"tipo@versão" -> definição`.
 *
 * A versão faz parte da chave de propósito: `payment.approved@2` é um evento **diferente**
 * de `payment.approved@1`. Um consumidor que ainda não conhece a v2 precisa rejeitá-la
 * explicitamente, não processá-la com o parser da v1 (A08).
 */
const BY_TYPE_AND_VERSION = new Map<string, AnyEventDefinition>(
  DEFINITIONS.map((definition) => [`${definition.type}@${definition.version}`, definition]),
);

export const KNOWN_EVENT_TYPES: readonly string[] = [
  ...new Set(DEFINITIONS.map((definition) => definition.type)),
].sort();

export function findDefinition(type: string, version: number): AnyEventDefinition | undefined {
  return BY_TYPE_AND_VERSION.get(`${type}@${version}`);
}

/**
 * Erro PERMANENTE: nenhuma quantidade de retry conserta um schema inválido ou um tipo
 * desconhecido. Quem consome deve mandar direto para a DLT, sem passar pela escada de retry.
 */
export class UnprocessableEventError extends Error {
  readonly permanent = true as const;

  constructor(
    message: string,
    readonly detail: { eventType?: string; eventVersion?: number; issues?: string[] } = {},
  ) {
    super(message);
    this.name = 'UnprocessableEventError';
  }
}

/**
 * Valida uma mensagem crua vinda do broker.
 *
 * O payload que chega do Kafka é **entrada não confiável** — tão não confiável quanto um body
 * HTTP de internet (A05). Ele pode vir de um serviço com deploy ruim, de um replay de tópico
 * antigo, ou de alguém que ganhou acesso de produtor. Valide antes de deixar o handler tocar nele.
 */
export function parseEvent(raw: unknown): { definition: AnyEventDefinition; event: unknown } {
  const peek = unknownEnvelopeSchema.safeParse(raw);
  if (!peek.success) {
    throw new UnprocessableEventError('Mensagem não é um envelope de evento válido', {
      issues: peek.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`),
    });
  }

  const { eventType, eventVersion } = peek.data satisfies UnknownEnvelope;
  const definition = findDefinition(eventType, eventVersion);
  if (!definition) {
    throw new UnprocessableEventError(
      `Evento desconhecido "${eventType}@${eventVersion}" — este serviço não sabe interpretá-lo`,
      { eventType, eventVersion },
    );
  }

  const parsed = definition.envelope.safeParse(raw);
  if (!parsed.success) {
    throw new UnprocessableEventError(`Payload de "${eventType}@${eventVersion}" viola o schema`, {
      eventType,
      eventVersion,
      issues: parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`),
    });
  }

  return { definition, event: parsed.data };
}

/** Valida contra UMA definição esperada e devolve o envelope tipado. */
export function parseAs<TDef extends AnyEventDefinition>(
  definition: TDef,
  raw: unknown,
): z.infer<TDef['envelope']> {
  const parsed = definition.envelope.safeParse(raw);
  if (!parsed.success) {
    throw new UnprocessableEventError(
      `Esperava "${definition.type}@${definition.version}" e recebi algo incompatível`,
      { issues: parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`) },
    );
  }
  return parsed.data as z.infer<TDef['envelope']>;
}

/**
 * A matriz de compensação, como dado em vez de comentário.
 *
 * Leia a coluna `compensatedBy`: quando o envio falha, **dois** serviços precisam agir, e
 * nenhum deles é o Shipping. Em coreografia, esse conhecimento fica espalhado — cada serviço
 * carrega um pedaço do fluxo global sem que ninguém tenha o desenho inteiro.
 *
 * Esta tabela existe para documentação, teste e para o sweeper de timeout saber o que esperar.
 * Ela NÃO é um orquestrador: nenhum serviço a consulta para decidir o próximo passo.
 */
export const COMPENSATION_MATRIX = [
  {
    failure: 'payment.failed',
    compensatedBy: [],
    emits: ['order.cancelled'],
    note: 'Nada foi efetivado ainda: não há o que desfazer.',
  },
  {
    failure: 'stock.unavailable',
    compensatedBy: ['payment-service'],
    emits: ['payment.refunded', 'order.cancelled'],
    note: 'Payment estorna a autorização que ele mesmo criou.',
  },
  {
    failure: 'shipment.failed',
    compensatedBy: ['payment-service', 'inventory-service'],
    emits: ['payment.refunded', 'stock.released', 'order.cancelled'],
    note: 'Compensação dupla e paralela. Order só fecha quando as duas chegarem.',
  },
  {
    failure: 'saga.timeout',
    compensatedBy: ['payment-service', 'inventory-service'],
    emits: ['payment.refunded', 'stock.released', 'order.cancelled'],
    note: 'Disparado pelo sweeper do Order Service — o meio-orquestrador que a coreografia acabou exigindo.',
  },
] as const;
