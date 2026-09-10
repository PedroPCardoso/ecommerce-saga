# 0008 — Retry escalonado em tópicos separados + DLT

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

Kafka não tem DLQ nem retry nativo. Um handler que lança erro tem duas saídas ruins: não commitar o offset (e o consumidor relê a mesma mensagem para sempre, travando a partição) ou commitar e perder a mensagem.

Retryar na própria partição com backoff também trava: enquanto uma mensagem espera, **todas** as outras daquela partição esperam com ela — head-of-line blocking. Um pedido problemático paralisa milhares.

## Decisão

Três coisas juntas:

1. **Classificar o erro antes de retryar.** Retriável (timeout, `503`, deadlock, broker fora) segue para a escada. Permanente (schema inválido, versão desconhecida, regra de negócio violada) vai **direto para a DLT**, sem gastar 3 tentativas no que nunca vai funcionar.
2. **Escada de retry em tópicos dedicados** por `(tópico de origem, consumer group)`: `retry-5s` → `retry-1m` → `retry-10m`, cada um com um consumidor que aplica o atraso.
3. **DLT** ao esgotar a escada, com headers de diagnóstico (`x-original-topic`, `x-retry-count`, `x-first-failure-at`, `x-last-error`, `x-stacktrace-hash`).

## Consequências

**Positivas**

- Sem head-of-line blocking: a mensagem problemática sai da partição principal e o fluxo continua.
- Erro permanente falha rápido, em vez de queimar 11 minutos de retry.
- A DLT é evidência: payload, causa e histórico ficam guardados para investigação e reprocessamento.
- Escada por consumer group: o retry do Inventory não interfere no do Payment sobre o mesmo tópico de origem.

**Negativas**

- **A ordenação daquele pedido é perdida.** Desviada para o retry, a mensagem pode ser processada **depois** da seguinte do mesmo `orderId`. Esta é a consequência mais séria da decisão. A defesa é a máquina de estados idempotente e monotônica, que rejeita transição inválida em vez de corromper o agregado. Onde ordenação estrita for indispensável, a escolha correta é o oposto: aceitar o head-of-line blocking e retryar in-place.
- **Explosão de tópicos.** 13 assinaturas × 4 (3 retries + DLT) = 52 tópicos além dos 4 de negócio. Derivados por código a partir de `packages/contracts`, nunca à mão.
- Um consumidor a mais por degrau da escada.
- **DLT sem ferramenta de reprocessamento vira cemitério.** Ninguém abre um tópico com 4 mil mensagens sem CLI. Daí a `tools/dlq-inspector` da Fase 6 — ela não é enfeite.
- A DLT pode conter PII: retenção curta e acesso auditado (A09).

## Alternativas consideradas

- **Retry in-place com backoff:** simples, preserva ordenação, e trava a partição. Continua sendo a escolha certa para agregados em que ordem estrita vale mais que throughput.
- **RabbitMQ com DLX + TTL:** daria isto quase de graça. Trade-off já resolvido no ADR-0003.
- **Um único tópico de retry com atraso fixo:** menos tópicos, mas sem escalonamento — erro transitório longo esgota as tentativas antes de o serviço voltar.
