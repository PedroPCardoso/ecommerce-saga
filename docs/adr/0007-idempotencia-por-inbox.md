# 0007 — Idempotência de consumo por tabela de inbox

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

A entrega é at-least-once por três motivos somados: o relay do outbox pode republicar (ADR-0006), o consumidor pode processar e morrer antes de commitar o offset, e um rebalance pode reentregar o batch. Num serviço de pagamento, processar duas vezes significa **cobrar duas vezes**.

`enable.idempotence=true` no produtor não resolve isto: ele deduplica no broker por `(producerId, sequence)` dentro de uma sessão. Não sobrevive a reinício do relay nem a replay.

## Decisão

Tabela `processed_messages (event_id, consumer_group)` com chave primária composta. O `INSERT` acontece **na mesma transação** do efeito de negócio; violação de PK significa "já processei" → rollback, commita o offset, segue.

## Consequências

**Positivas**

- Idempotência real: o efeito de negócio acontece uma vez, ainda que a mensagem chegue vinte.
- Chave `(event_id, consumer_group)`, não só `event_id`: consumer groups distintos precisam processar o mesmo evento. Errar isso faz o segundo consumidor ignorar tudo silenciosamente.
- Barato: um `INSERT` e um índice único.
- Torna replay seguro por padrão — e replay é operação de rotina.

**Negativas**

- **Tabela que cresce para sempre.** Sem job de limpeza, some com o disco em semanas. A retenção precisa ser **maior** que a do tópico (30 dias contra 7), senão um replay reencontra mensagens já esquecidas e as reprocessa.
- Um `INSERT` extra em cada mensagem.
- Só protege o que está **dentro** da transação. Efeito colateral externo (chamada ao gateway, envio de e-mail) não é coberto e precisa da própria chave de idempotência no lado remoto.
- Duplicata concorrente vira contenção de lock em vez de erro — comportamento correto, mas confunde em profiling.

## Alternativas consideradas

- **Cache/Redis com TTL:** mais rápido, mas não é atômico com a transação de banco. Uma queda entre o `SET` e o COMMIT reabre exatamente o buraco que o padrão fecha.
- **Handlers naturalmente idempotentes** (`UPDATE ... SET status='PAID'`): funciona para alguns casos e não para `INSERT` de estorno. Cobertura parcial dá falsa segurança.
- **Exactly-once do Kafka (transações + `read_committed`):** só vale ponta a ponta dentro do Kafka. O efeito no Postgres e a chamada ao gateway ficam de fora.
