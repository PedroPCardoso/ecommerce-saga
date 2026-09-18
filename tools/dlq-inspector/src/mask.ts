const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SENSITIVE_KEY_PATTERN = /token|secret|password|authorization/i;

/**
 * Nomes de campo que são PII por INTEIRO — a subárvore toda é mascarada de uma vez,
 * não campo a campo. `shippingAddress` cobre `street`/`number`/`complement`/`district`/
 * `city`/`state`/`zipCode`/`country` (ver `addressSchema` em `packages/contracts/src/
 * common.ts`) sem precisar listar cada um (e sem depender de nomes de campo genéricos
 * como `number`, que colidiriam com outros usos legítimos). Igual para `items`
 * (SKU + preço unitário identifica o que o cliente comprou) e `customerId`.
 */
const PII_FIELD_NAME_PATTERN = /^(shippingAddress|customerId|items)$/i;

/**
 * Mascara PII e segredo ANTES de qualquer coisa ir para stdout — é o que
 * `dlq-inspector show`/`list` usam para exibir payload e headers sem vazar dado de
 * cliente num terminal ou log de CI (OWASP A09/A04). Duas heurísticas independentes:
 * nome do campo (segredo por padrão de nome, PII por nome EXATO e conhecido) e
 * formato do valor (string inteira que parece e-mail, como rede de segurança para
 * campos não previstos nesta lista).
 */
export function maskPii(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => maskPii(item));
  }
  if (value !== null && typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      if (SENSITIVE_KEY_PATTERN.test(key) || PII_FIELD_NAME_PATTERN.test(key)) {
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
