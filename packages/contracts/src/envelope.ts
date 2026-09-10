import { v7 as uuidv7 } from 'uuid';
import { z } from 'zod';

/**
 * Envelope comum a TODO evento do sistema.
 *
 * Campos que costumam ser esquecidos e que você vai agradecer ter:
 * - `correlationId`: constante durante a saga inteira. Responde "o que aconteceu com o pedido X?".
 * - `causationId`: eventId do evento que causou este. Monta a árvore causal e responde
 *   "por que este estorno aconteceu?" três semanas depois.
 * - `producer`: serviço + versão que publicou. Essencial quando um deploy ruim envenena um tópico.
 */
export const envelopeBaseSchema = z.object({
  /** UUID v7 — monotônico no tempo, serve de chave de idempotência no consumidor. */
  eventId: z.string().uuid(),
  eventType: z.string().min(1),
  eventVersion: z.number().int().positive(),
  /** ISO 8601 com offset. Momento em que o fato ocorreu, não em que foi publicado. */
  occurredAt: z.string().datetime({ offset: true }),
  /** Sempre o orderId: é também a chave da partição Kafka, o que garante ordenação por pedido. */
  aggregateId: z.string().min(1),
  aggregateType: z.string().min(1),
  correlationId: z.string().min(1),
  causationId: z.string().min(1),
  /** Formato "payment-service@1.4.2". */
  producer: z.string().min(1),
});

export type EnvelopeBase = z.infer<typeof envelopeBaseSchema>;

/** Envelope frouxo: dá peek na mensagem antes de saber qual schema específico aplicar. */
export const unknownEnvelopeSchema = envelopeBaseSchema.extend({ payload: z.unknown() });
export type UnknownEnvelope = z.infer<typeof unknownEnvelopeSchema>;

/**
 * Declara um evento: tipo, versão, tópico e schema do payload.
 *
 * Devolve também o schema do envelope completo com `eventType`/`eventVersion` travados em
 * literais — é isso que faz a validação **rejeitar** mensagem de versão desconhecida em vez de
 * "adivinhar" o formato (OWASP A08: Software and Data Integrity Failures).
 */
export function defineEvent<
  TType extends string,
  TVersion extends number,
  TPayload extends z.ZodTypeAny,
>(config: {
  type: TType;
  version: TVersion;
  aggregateType: string;
  topic: string;
  payload: TPayload;
}) {
  const envelope = envelopeBaseSchema.extend({
    eventType: z.literal(config.type),
    eventVersion: z.literal(config.version),
    payload: config.payload,
  });

  return { ...config, envelope } as const;
}

/** Forma mínima que qualquer definição de evento satisfaz — use em assinaturas genéricas. */
export type AnyEventDefinition = {
  readonly type: string;
  readonly version: number;
  readonly aggregateType: string;
  readonly topic: string;
  readonly payload: z.ZodTypeAny;
  readonly envelope: z.ZodTypeAny;
};

/** Envelope tipado a partir de uma definição de evento. */
export type EventOf<TDef extends { envelope: z.ZodTypeAny }> = z.infer<TDef['envelope']>;

/** Payload tipado a partir de uma definição de evento. */
export type PayloadOf<TDef extends { payload: z.ZodTypeAny }> = z.infer<TDef['payload']>;

export type CreateEventInput<TPayload> = {
  aggregateId: string;
  payload: TPayload;
  correlationId: string;
  producer: string;
  /**
   * eventId do evento que causou este. Na borda HTTP não existe evento anterior:
   * omita e o próprio eventId é usado, marcando a raiz da árvore causal.
   */
  causationId?: string;
  occurredAt?: string;
};

/**
 * Monta e valida um evento.
 *
 * Validar na saída parece redundante, mas é o que impede um serviço de publicar lixo que só
 * estouraria no consumidor — onde o contexto de negócio já se perdeu e só resta a DLT.
 */
export function createEvent<
  TType extends string,
  TVersion extends number,
  TPayload extends z.ZodTypeAny,
>(
  definition: ReturnType<typeof defineEvent<TType, TVersion, TPayload>>,
  input: CreateEventInput<z.input<TPayload>>,
): z.infer<ReturnType<typeof defineEvent<TType, TVersion, TPayload>>['envelope']> {
  const eventId = uuidv7();

  return definition.envelope.parse({
    eventId,
    eventType: definition.type,
    eventVersion: definition.version,
    occurredAt: input.occurredAt ?? new Date().toISOString(),
    aggregateId: input.aggregateId,
    aggregateType: definition.aggregateType,
    correlationId: input.correlationId,
    causationId: input.causationId ?? eventId,
    producer: input.producer,
    payload: input.payload,
  });
}

export { uuidv7 };
