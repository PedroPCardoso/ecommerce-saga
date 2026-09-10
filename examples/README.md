# Exemplos executáveis

Seis exemplos, um padrão cada, rodando contra o **Kafka e o Postgres de verdade** que o
`docker compose` sobe. Eles não são demonstração passiva: cada um termina com asserções e
**falha com exit code 1** se o comportamento observado não for o esperado. Servem
simultaneamente de aula e de teste de integração.

```bash
nvm use && pnpm install
pnpm infra:up            # na raiz do repo

cd examples
pnpm ex                  # lista
pnpm ex 03               # roda um
pnpm ex todos            # roda todos em sequência (~1 min)
```

## Os seis

| # | Exemplo | O que ele prova | Módulo |
|---|---------|-----------------|--------|
| 01 | [chave e ordenação](src/01-chave-e-ordenacao.ts) | sem chave, os eventos de um pedido se espalham por 3 partições; com chave, ficam em 1 | [03](../docs/aprender/03-particoes-chave-e-ordenacao.md) |
| 02 | [commit de offset](src/02-commit-de-offset.ts) | commitar antes de persistir **perde** mensagem; depois, **duplica** | [04](../docs/aprender/04-entrega-e-commit-de-offset.md) |
| 03 | [outbox](src/03-outbox.ts) | a inconsistência sem outbox; e 3 relays publicando 60× o que deveriam publicar 30× sem `SKIP LOCKED` | [05](../docs/aprender/05-outbox.md) |
| 04 | [idempotência](src/04-idempotencia.ts) | 13 entregas, 5 efeitos; e o bug silencioso da PK só em `event_id` | [06](../docs/aprender/06-idempotencia.md) |
| 05 | [retry e DLT](src/05-retry-e-dlt.ts) | erro permanente gasta 1 tentativa, transitório gasta 4, ambos terminam na DLT | [07](../docs/aprender/07-retry-e-dlt.md) |
| 06 | [replay](src/06-replay.ts) | replay é seguro com inbox íntegro, e dobra o saldo sem ele | [09](../docs/aprender/09-replay-e-evolucao.md) |

## Como eles se comportam

**Determinísticos.** Nenhum `Math.random()`. Gatilhos de falha vêm do conteúdo da
mensagem (valor terminando em `.13`, SKU com prefixo `OUT-`, CEP `00000`). Teste
intermitente é pior que teste ausente.

**Autolimpantes.** Todo tópico criado usa o prefixo `lab.` e é apagado no fim. O schema
`lab` do `order_db` é recriado a cada execução. Rodar duas vezes dá o mesmo resultado, e
nada encosta na topologia real de 56 tópicos.

**Legíveis de uma vez.** Cada arquivo se explica sozinho, do cabeçalho à lição final. O
único código compartilhado é `src/_shared/` — cliente Kafka, pool de Postgres e saída
formatada.

## Anatomia de um exemplo

```
/**
 * PERGUNTA que o exemplo responde
 */
titulo(...)      → o que vai acontecer
passo(...)       → cada etapa, numerada
tabela(...)      → os dados observados
confere(...)     → a asserção, com ✓ ou ✗
licao(...)       → o que levar embora, incluindo o custo do padrão
fim()            → exit 0 ou 1
```

## Se algo falhar

```bash
docker compose -f ../deploy/docker/docker-compose.yml ps    # infra de pé?
pnpm --filter @ecommerce/examples ex 01                     # um por vez
```

Os exemplos 02, 03, 04 e 06 usam o Postgres do Order Service na porta **15432** (não
5432 — ver a nota sobre portas no [README](../README.md#começando)). Se a conexão falhar,
confirme `LAB_DATABASE_URL` ou a porta publicada do container `ecommerce-pg-order`.

## Um achado que vale contar

A primeira versão do exemplo 01 usava 3 pedidos e 3 partições. O round-robin sem chave
alinhou cada pedido a uma partição fixa por acidente aritmético, e a asserção
"sem chave os eventos se espalham" **falhou** — porque não havia espalhamento nenhum.

Quatro pedidos em três partições torna esse alinhamento impossível. A lição não é sobre
Kafka: é que um cenário de teste pode dar o resultado certo pelo motivo errado, e você
acredita nele. Está registrado na lição final do próprio exemplo.
