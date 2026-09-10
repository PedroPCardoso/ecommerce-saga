/**
 * Cliente mínimo, estrutural: qualquer PrismaClient ou
 * Prisma.TransactionClient satisfaz isto sem que este pacote precise
 * importar @prisma/client.
 */
export interface RawSqlClient {
  $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
}

export interface OutboxRow {
  eventId: string;
  aggregateId: string;
  aggregateType: string;
  eventType: string;
  /**
   * O envelope COMPLETO e já validado (saída de createEvent de
   * @ecommerce/contracts), não só o payload de negócio — é o que o relay
   * publica sem reconstruir nada.
   */
  envelope: unknown;
  headers?: Record<string, string>;
}

/**
 * Insere a linha de outbox. DEVE ser chamado dentro da mesma transação
 * Prisma que grava o efeito de domínio — é isso que torna o par atômico.
 */
export async function insertOutboxRow(tx: RawSqlClient, row: OutboxRow): Promise<void> {
  const affected = await tx.$executeRawUnsafe(
    `INSERT INTO outbox (event_id, aggregate_id, aggregate_type, event_type, payload, headers)
     VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6::jsonb)`,
    row.eventId,
    row.aggregateId,
    row.aggregateType,
    row.eventType,
    JSON.stringify(row.envelope),
    JSON.stringify(row.headers ?? {}),
  );

  if (affected !== 1) {
    throw new Error(`Falha ao inserir linha de outbox para o evento ${row.eventId}`);
  }
}
