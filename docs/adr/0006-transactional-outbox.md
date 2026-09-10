# 0006 — Transactional Outbox para publicar eventos

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

Criar um pedido significa gravar no Postgres **e** publicar `order.created` no Kafka. São dois sistemas sem transação comum. Publicar dentro do handler, depois do `save()`, tem duas falhas:

- processo morre depois do COMMIT e antes do publish → pedido existe, saga nunca começa;
- publish acontece e o COMMIT falha no rollback → evento existe, pedido não. Pior das duas: o Payment vai autorizar um pagamento de um pedido inexistente.

## Decisão

Transactional Outbox. O evento é gravado numa tabela `outbox` **na mesma transação** do efeito de negócio. Um relay assíncrono lê `WHERE published_at IS NULL` com `FOR UPDATE SKIP LOCKED`, publica no Kafka e marca a linha.

## Consequências

**Positivas**

- Efeito de negócio e intenção de publicar são atômicos: ou os dois existem, ou nenhum.
- O `SKIP LOCKED` permite N réplicas do relay sem que duas peguem a mesma linha.
- A tabela `outbox` é um log auditável do que o serviço tentou publicar e quando.
- Broker fora do ar deixa de ser erro na borda: os eventos acumulam e saem quando ele volta.

**Negativas**

- **Latência.** O evento não sai no mesmo instante; sai no próximo ciclo do relay (200ms). Aceitável aqui, mortal em fluxo síncrono.
- **At-least-once por construção.** O relay pode publicar e morrer antes de marcar `published_at`, republicando na volta. Isto **exige** idempotência no consumidor (ADR-0007). Os dois padrões são um par; usar só um é pior que usar nenhum, porque dá falsa segurança.
- Uma tabela que cresce e precisa de job de limpeza, mais um índice parcial para o polling não degradar.
- Polling gasta consulta mesmo quando não há nada — o custo de não ter CDC.

## Alternativas consideradas

- **Publicar direto no handler:** o bug descrito acima, garantido.
- **Debezium/CDC lendo o WAL:** elimina o polling e a latência, e é o caminho da Fase 12. Descartado no início por trazer Kafka Connect e configuração de replicação antes de o padrão ter sido entendido.
- **Transação Kafka + banco (2PC):** Kafka não participa de XA. Não é opção.
