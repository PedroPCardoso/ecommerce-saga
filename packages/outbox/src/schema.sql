-- Copie esta tabela para a migration Prisma de CADA serviço que publica
-- eventos. `payload` guarda o ENVELOPE completo já validado (não só o
-- payload de negócio) — é o que o relay publica sem reconstruir nada.
CREATE TABLE outbox (
  id             bigserial PRIMARY KEY,
  event_id       uuid NOT NULL UNIQUE,
  aggregate_id   text NOT NULL,
  aggregate_type text NOT NULL,
  event_type     text NOT NULL,
  payload        jsonb NOT NULL,
  headers        jsonb NOT NULL DEFAULT '{}',
  created_at     timestamptz NOT NULL DEFAULT now(),
  published_at   timestamptz,
  attempts       integer NOT NULL DEFAULT 0
);

-- Índice PARCIAL: o relay só pergunta pelas pendentes, e essa consulta roda
-- a cada 200ms para sempre. Sem o WHERE, o índice cresce com o histórico
-- inteiro e a consulta degrada junto.
CREATE INDEX outbox_pending_idx ON outbox (created_at) WHERE published_at IS NULL;
