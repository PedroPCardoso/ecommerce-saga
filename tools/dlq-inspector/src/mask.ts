const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SENSITIVE_KEY_PATTERN = /token|secret|password|authorization/i;

/**
 * Mascara PII e segredo ANTES de qualquer coisa ir para stdout — é o que
 * `dlq-inspector show` usa para exibir payload sem vazar dado de cliente
 * num terminal ou log de CI (OWASP A09/A04). Heurística simples de
 * propósito: nome do campo (token/secret/password/authorization) ou
 * formato do valor (parece e-mail).
 */
export function maskPii(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => maskPii(item));
  }
  if (value !== null && typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      if (SENSITIVE_KEY_PATTERN.test(key)) {
        result[key] = '***MASKED***';
      } else {
        result[key] = maskPii(val);
      }
    }
    return result;
  }
  if (typeof value === 'string' && EMAIL_PATTERN.test(value)) {
    return '***@***';
  }
  return value;
}
