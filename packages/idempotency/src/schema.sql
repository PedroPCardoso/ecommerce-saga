-- Copie esta tabela para a migration Prisma de CADA serviço que consome
-- eventos Kafka. A chave é o par (event_id, consumer_group): dois consumer
-- groups diferentes (ex.: payment-service e notification-service) precisam
-- processar o MESMO evento — só event_id bloquearia o segundo grupo sem
-- nenhum erro visível.
CREATE TABLE processed_messages (
  event_id       uuid NOT NULL,
  consumer_group text NOT NULL,
  processed_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, consumer_group)
);
