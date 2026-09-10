/**
 * Cliente mínimo, estrutural: qualquer PrismaClient ou
 * Prisma.TransactionClient satisfaz isto sem que este pacote precise
 * importar @prisma/client — cada serviço gera o seu, com schema próprio, e
 * acoplar o tipo aqui prenderia todos numa única versão de client gerado.
 */
export interface RawSqlClient {
  $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
}

/**
 * Marca o evento como processado por este consumer group, DENTRO da mesma
 * transação do efeito de negócio. Devolve `true` na primeira vez — o
 * chamador deve aplicar o efeito — e `false` se já processado — o chamador
 * pula o efeito, mas ainda assim deixa a transação commitar (vazia) e o
 * offset ser commitado: reentrega não é erro.
 */
export async function markProcessed(
  tx: RawSqlClient,
  eventId: string,
  consumerGroup: string,
): Promise<boolean> {
  const affected = await tx.$executeRawUnsafe(
    `INSERT INTO processed_messages (event_id, consumer_group) VALUES ($1::uuid, $2)
     ON CONFLICT DO NOTHING`,
    eventId,
    consumerGroup,
  );

  return affected === 1;
}
