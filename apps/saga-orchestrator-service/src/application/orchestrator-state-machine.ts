export type OrchestratorStatus =
  | 'AWAITING_PAYMENT'
  | 'AWAITING_STOCK'
  | 'AWAITING_SHIPMENT'
  | 'CONFIRMED'
  | 'CANCELLED';

/**
 * O núcleo do "cérebro" da saga orquestrada — função pura, testada isoladamente,
 * sem infra nenhuma (TDD puro, ver `test/orchestrator-state-machine.spec.ts`).
 *
 * Compare com a coreografia (ADR-0002): lá, "o que fazer a seguir" não existe como
 * função em lugar nenhum — está espalhado pelos handlers dos 5 serviços de produção
 * mais a `COMPENSATION_MATRIX` (packages/contracts/src/registry.ts), e cada serviço
 * só enxerga o pedaço que é assunto dele. Aqui é UMA função, UM arquivo: é
 * exatamente essa diferença estrutural que o ADR-0012 mede.
 */
export function applyExecutorResponse(
  current: OrchestratorStatus,
  step: 'payment' | 'inventory' | 'shipping',
  outcome: 'success' | 'failure',
): OrchestratorStatus {
  if (outcome === 'failure') return 'CANCELLED';
  if (current === 'AWAITING_PAYMENT' && step === 'payment') return 'AWAITING_STOCK';
  if (current === 'AWAITING_STOCK' && step === 'inventory') return 'AWAITING_SHIPMENT';
  if (current === 'AWAITING_SHIPMENT' && step === 'shipping') return 'CONFIRMED';
  return current; // resposta fora de ordem/duplicada — ignora (aqui pode, é single-writer)
}
