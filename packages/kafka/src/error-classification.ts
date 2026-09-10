import { UnprocessableEventError } from '@ecommerce/contracts';

export type ErrorClass = 'retriable' | 'permanent';

export interface ClassifiableError {
  permanent?: boolean;
}

/**
 * Erro desconhecido classifica como RETRIABLE, não PERMANENT — fail secure
 * aqui significa não desistir cedo demais de algo que pode ser transitório.
 * O pior caso é gastar a escada inteira (5s+1m+10m) antes de cair na DLT, o
 * que ainda é seguro: nada se perde, só demora mais para ser investigado.
 * Erros conhecidos como definitivos (schema inválido, versão desconhecida,
 * regra de negócio violada) chegam aqui marcados `.permanent = true`.
 */
export function classifyError(error: unknown): ErrorClass {
  if (error instanceof UnprocessableEventError) return 'permanent';
  if (isClassifiableError(error) && error.permanent === true) return 'permanent';
  return 'retriable';
}

function isClassifiableError(error: unknown): error is ClassifiableError {
  return typeof error === 'object' && error !== null && 'permanent' in error;
}
