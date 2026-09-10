const MAILHOG_API_BASE = 'http://localhost:18025/api';

export interface MailhogItem {
  To: Array<{ Mailbox: string; Domain: string }>;
  Content: { Headers: Record<string, string[]>; Body: string };
}

/** Limpa a caixa de entrada do Mailhog — usado no beforeEach de cada teste. */
export async function clearMailhogInbox(): Promise<void> {
  await fetch(`${MAILHOG_API_BASE}/v1/messages`, { method: 'DELETE' });
}

export async function findMailhogMessage(
  predicate: (item: MailhogItem) => boolean,
  timeoutMs = 10_000,
): Promise<MailhogItem> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = await fetch(`${MAILHOG_API_BASE}/v2/messages?limit=100`);
    const body = (await res.json()) as { items: MailhogItem[] };
    const found = body.items.find(predicate);
    if (found) return found;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error('Mensagem esperada não apareceu no Mailhog a tempo');
}

export async function countMailhogMessagesTo(email: string): Promise<number> {
  const res = await fetch(`${MAILHOG_API_BASE}/v2/messages?limit=100`);
  const body = (await res.json()) as { items: MailhogItem[] };
  return body.items.filter(
    (item) => item.To.some((to) => `${to.Mailbox}@${to.Domain}`.toLowerCase() === email.toLowerCase()),
  ).length;
}

export function subjectOf(item: MailhogItem): string {
  return item.Content.Headers['Subject']?.[0] ?? '';
}
