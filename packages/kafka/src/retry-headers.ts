import { RETRY_HEADERS } from '@ecommerce/contracts';

export function readRetryCount(headers: Record<string, Buffer | string | undefined>): number {
  const raw = headers[RETRY_HEADERS.retryCount];
  if (raw === undefined) return 0;
  const value = Number(raw.toString());
  return Number.isFinite(value) ? value : 0;
}

export function buildRedirectHeaders(opts: {
  originalTopic: string;
  originalPartition: number;
  originalOffset: string;
  retryCount: number;
  firstFailureAt: string;
  error: unknown;
  consumerGroup: string;
}): Record<string, string> {
  return {
    [RETRY_HEADERS.originalTopic]: opts.originalTopic,
    [RETRY_HEADERS.originalPartition]: String(opts.originalPartition),
    [RETRY_HEADERS.originalOffset]: opts.originalOffset,
    [RETRY_HEADERS.retryCount]: String(opts.retryCount),
    [RETRY_HEADERS.firstFailureAt]: opts.firstFailureAt,
    [RETRY_HEADERS.lastError]: errorMessage(opts.error).slice(0, 500),
    [RETRY_HEADERS.stacktraceHash]: `sha256:${hashString(errorStack(opts.error))}`,
    [RETRY_HEADERS.consumerGroup]: opts.consumerGroup,
  };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorStack(error: unknown): string {
  return error instanceof Error ? (error.stack ?? error.message) : String(error);
}

/**
 * FNV-1a — determinístico, sem dependência externa. Só para correlacionar
 * erros na DLT (ex.: "essas 40 mensagens falharam pelo mesmo motivo"),
 * nunca para segurança.
 */
function hashString(input: string): string {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}
