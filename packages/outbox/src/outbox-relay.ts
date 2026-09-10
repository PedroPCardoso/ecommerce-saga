import type { Pool } from 'pg';

export interface OutboxEnvelopeRow {
  id: string;
  eventId: string;
  envelope: unknown;
  headers: Record<string, string>;
}

export type PublishFn = (row: OutboxEnvelopeRow) => Promise<void>;

export interface OutboxRelayOptions {
  pool: Pool;
  publish: PublishFn;
  pollIntervalMs?: number;
  batchSize?: number;
  onError?: (error: unknown, row: OutboxEnvelopeRow) => void;
}

/**
 * Relay assíncrono: lê linhas pendentes com FOR UPDATE SKIP LOCKED, publica,
 * marca published_at. At-least-once por construção — pode publicar e morrer
 * antes do UPDATE, o que republica na próxima volta. O consumidor precisa de
 * idempotência (packages/idempotency) por causa disto, não apesar disto.
 */
export class OutboxRelay {
  private readonly pool: Pool;
  private readonly publishFn: PublishFn;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly onError: (error: unknown, row: OutboxEnvelopeRow) => void;
  private running = false;
  private loopPromise: Promise<void> | null = null;

  constructor(opts: OutboxRelayOptions) {
    this.pool = opts.pool;
    this.publishFn = opts.publish;
    this.pollIntervalMs = opts.pollIntervalMs ?? 200;
    this.batchSize = opts.batchSize ?? 100;
    this.onError =
      opts.onError ??
      ((error, row) => console.error(`[outbox-relay] falha ao publicar ${row.eventId}`, error));
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.loop();
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.loopPromise;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      try {
        const processed = await this.drainOnce();
        if (processed === 0) {
          await sleep(this.pollIntervalMs);
        }
      } catch (error) {
        // Erro de infra (conexão derrubada, deadlock, Postgres reiniciando)
        // não pode escapar do loop: sem este catch, a promise de `start()`
        // rejeita sem que ninguém a aguarde (ninguém chama `await` nela até
        // `stop()`), vira unhandled rejection, e o Node mata o processo —
        // o relay inteiro para de publicar até o container reiniciar.
        // Aqui, o mesmo `onError` de falha de publish trata isso, e o loop
        // espera um ciclo antes de tentar de novo (evita busy-loop se a
        // conexão estiver mesmo fora do ar).
        this.onError(error, { id: '(loop)', eventId: '(loop)', envelope: null, headers: {} });
        await sleep(this.pollIntervalMs);
      }
    }
  }

  /** Uma passada: publica até `batchSize` linhas pendentes. Devolve quantas processou. */
  async drainOnce(): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<{
        id: string;
        event_id: string;
        payload: unknown;
        headers: Record<string, string>;
      }>(
        `SELECT id, event_id, payload, headers
           FROM outbox
          WHERE published_at IS NULL
          ORDER BY created_at
          LIMIT $1
          FOR UPDATE SKIP LOCKED`,
        [this.batchSize],
      );

      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }

      const published: string[] = [];
      for (const row of rows) {
        const outboxRow: OutboxEnvelopeRow = {
          id: row.id,
          eventId: row.event_id,
          envelope: row.payload,
          headers: row.headers ?? {},
        };
        try {
          await this.publishFn(outboxRow);
          published.push(row.id);
        } catch (error) {
          this.onError(error, outboxRow);
          await client.query('UPDATE outbox SET attempts = attempts + 1 WHERE id = $1', [row.id]);
        }
      }

      if (published.length > 0) {
        await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [
          published,
        ]);
      }

      await client.query('COMMIT');
      // published.length, não rows.length: se TODAS as publicações desta passada
      // falharam (broker fora do ar), devolver rows.length faria loop() enxergar
      // "processei algo" e nunca dormir entre tentativas — busy-loop batendo em
      // Postgres e no broker a cada volta, sem nenhum backoff.
      return published.length;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // Conexão já pode estar morta (é por isso que o BEGIN/SELECT/UPDATE de cima
        // falhou) — o ROLLBACK em si falhar não pode mascarar o erro original abaixo.
      }
      throw error;
    } finally {
      client.release();
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
