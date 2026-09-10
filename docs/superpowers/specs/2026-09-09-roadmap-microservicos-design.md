# Roadmap de implementação dos microserviços — Design

## Contexto

O repositório é um projeto de estudos sobre SAGA coreografada com Kafka (ver
`docs/PLAN.md` e `docs/adr/`). A fundação (`packages/contracts`, `examples/`,
`deploy/docker/docker-compose.yml`, tooling do monorepo) já está implementada.
Os 5 microserviços em `apps/*` são pastas `src/` vazias — nada implementado.
Os pacotes compartilhados `packages/kafka`, `packages/outbox`,
`packages/idempotency`, `packages/observability` também estão vazios.

Objetivo desta rodada: colocar os 5 microserviços funcionando, cobrindo o
roadmap completo do `docs/PLAN.md` (Fases 0–10). Escopo confirmado com o
usuário via brainstorming.

## Decisão de escopo

Alvo: **Fases 0–10** do `docs/PLAN.md` (fundação → saga completa →
resiliência → observabilidade → C4 → Docker → Kubernetes). Fases 11
(orquestração) e 12 (avançado) ficam fora, são opcionais no próprio PLAN.md.

Execução: **um plano de implementação por fase**, cada um autocontido e
delegável a um subagente (`superpowers:subagent-driven-development`), na
ordem de dependência abaixo. Cada fase, ao terminar, deve deixar o sistema
funcional e testável — não há branch/PR: commits vão direto para `master`
conforme cada fase fecha os critérios de pronto.

## Roadmap e dependências

| Fase | Escopo | Depende de |
|---|---|---|
| 0 | Fundação restante (Husky+commitlint, commit inicial, verificação de infra) | — |
| 2 | `packages/kafka`, `packages/outbox`, `packages/idempotency` | Fase 0 |
| 1 | Order Service — HTTP + outbox | Fase 2 (outbox) |
| 3 | Payment Service | Fase 2 (kafka), Fase 1 (consome `order.created`) |
| 4 | Inventory Service + 1ª compensação | Fase 3 (consome `payment.approved`) |
| 5 | Shipping + Notification Service | Fase 4 (consome `stock.reserved`) |
| 9 | Dockerização — todos os 5 serviços rodam via `docker compose`, não só `pnpm dev` | Fases 1–5 |
| 6 | Resiliência: sweeper de timeout, dlq-inspector, testes de caos | Fases 1–5, 9 |
| 7 | Observabilidade: OTel, Jaeger, métricas, `packages/observability` | Fases 1–5, 9 |
| 7b | Saga Observer (UI) | Fase 7 |
| 8 | Documentação C4 completa | Fases 1–5 |
| 10 | Kubernetes/Minikube | Fase 9 |

Fase 2 vem antes da Fase 1 na ordem de execução porque o Order Service
(Fase 1) precisa do `packages/outbox` para publicar `order.created`
atomicamente. A numeração das fases é a do PLAN.md original; a ordem de
execução real é 0 → 2 → 1 → 3 → 4 → 5 → **9** → (6, 7, 8, 10).

**Ajuste de sequência (2026-09-09, feedback do usuário): a Fase 9
(Dockerização) foi antecipada para logo depois da Fase 5**, em vez de
ficar no fim como no `docs/PLAN.md` original. Motivo: "todos os serviços
devem usar docker" é tratado como requisito do núcleo funcional, não como
polimento de produção — assim que os 5 serviços rodam localmente via
`pnpm dev`, eles também precisam subir via `docker compose`, antes de
qualquer trabalho de resiliência/observabilidade/K8s em cima. O ESCOPO da
Fase 9 continua o do `docs/PLAN.md` (multi-stage: deps/build/prune/runtime,
`pnpm deploy --prod`, `USER node`, `tini`, imagem pinada por digest real
— nunca inventado —, `HEALTHCHECK` em `/health/live`, Trivy sem
HIGH/CRITICAL); só a ORDEM mudou. O plano desta fase é escrito depois que
as Fases 3/4/5 estiverem fechadas, porque precisa dos nomes exatos de
variável de ambiente/porta de cada serviço (só o Order Service, Fase 1,
já está travado nesse detalhe).

## Decisões técnicas fixadas nesta rodada

Estas decisões preenchem lacunas que o `docs/PLAN.md` deixa em aberto
("Redis ou tabela", "decorator `@Idempotent()`") ou que são inferidas dos
ADRs já escritos. Cada plano de fase referencia estas decisões em vez de
redecidir.

1. **`packages/outbox` e `packages/idempotency` não dependem do
   `@prisma/client` gerado.** Cada serviço tem seu próprio Prisma Client
   (schemas diferentes). Os pacotes compartilhados operam contra uma
   interface mínima e estrutural — `{ $executeRawUnsafe(query, ...values):
   Promise<number> }` — que qualquer `PrismaClient` ou
   `Prisma.TransactionClient` satisfaz, sem acoplar versão/tipos entre
   serviços.
2. **`OutboxRelay` roda como poller à parte, com `pg.Pool` puro** (não
   Prisma) — não participa da transação de domínio, só lê linhas
   `published_at IS NULL` com `FOR UPDATE SKIP LOCKED` e publica. Recebe uma
   função `publish(envelope, headers)` injetada pelo serviço (que por baixo
   usa `@ecommerce/kafka`), em vez de depender do pacote kafka diretamente —
   mantém `outbox` testável sem broker.
3. **Sem decorator `@Idempotent()`.** Em vez disso, uma função simples
   `markProcessed(tx, eventId, consumerGroup): Promise<boolean>` que o
   handler chama explicitamente dentro da própria transação Prisma. Mais
   simples de testar, mesmo efeito (YAGNI: decorator com metadata/DI é
   complexidade que ninguém pediu ainda).
4. **Idempotência HTTP (`Idempotency-Key`) usa tabela no próprio Postgres do
   Order Service**, não Redis — não há Redis no `docker-compose.yml` e
   adicionar infra nova só para isso viola "banco por serviço, sem infra
   extra sem necessidade real".
5. **Autenticação: JWT HS256 mínimo (dev), `customerId` sempre extraído do
   token, nunca do body** (OWASP A01/A07 — exigência organizacional, não
   opcional). `POST /orders` e `GET /orders/:id` exigem
   `Authorization: Bearer <token>`; token assinado com `JWT_SECRET` do
   `.env`. Em produção seria RS256 via IdP externo — fora de escopo aqui,
   documentado como dívida.
6. **Testes de integração dos pacotes/serviços rodam contra a infra local
   já existente** (`pnpm infra:up`), no mesmo padrão que `examples/` já usa
   (prefixo de schema/tópico isolado, não Testcontainers). Motivo:
   consistência com o que já está provado funcionando neste repo; adicionar
   Testcontainers agora é escopo novo sem necessidade.
7. **NestJS sem `@nestjs/cli`**: os apps compilam com `tsc` (mesmo pipeline
   de build/typecheck/lint/test do Turborepo already usado pelos outros
   pacotes), `main.ts` chama `NestFactory.create` manualmente. Evita duplicar
   sistema de build.
8. **`packages/contracts` corrigido: Inventory e Shipping agora também
   assinam `orders`.** `payment.approved` só carrega dados de pagamento
   (nunca os SKUs) e `stock.reserved` só carrega itens (nunca o endereço de
   entrega) — sem isso nem Inventory (Fase 4) saberia o que reservar, nem
   Shipping (Fase 5) saberia para onde enviar. Já aplicado e testado (22/22
   em `packages/contracts`) antes de escrever os planos das Fases 4 e 5.
   Consequência igual nos dois casos: o handler do evento "gatilho"
   (`payment.approved` no Inventory, `stock.reserved` no Shipping) precisa
   tratar "ainda não vi o `order.created` deste pedido" como erro
   RETRIÁVEL (a escada de retry dá tempo à outra mensagem de chegar), nunca
   permanente — e o `markProcessed` deve acontecer DENTRO da mesma
   transação Prisma que lança esse erro, para que o rollback desfaça o
   registro de idempotência junto (senão o retry seria descartado como
   "já processado" sem nunca ter sido).

## Critério de pronto (todas as fases 0–5, o núcleo funcional)

- `pnpm infra:up && pnpm topics:create` sobe a topologia.
- Os 5 serviços sobem (`pnpm --filter <app> dev` ou equivalente) e um
  `POST /orders` percorre a saga inteira até `CONFIRMED`, com os 3 caminhos
  de falha (`payment.failed`, `stock.unavailable`, `shipment.failed`)
  terminando em `CANCELLED` com as compensações corretas.
- Evento duplicado (mesmo `eventId`) não duplica efeito de negócio.
- `Idempotency-Key` repetida em `POST /orders` não cria segundo pedido.

Fases 6–10 têm critério de pronto próprio, já descrito em `docs/PLAN.md`
seção 6, reaproveitado tal qual nos planos de cada fase.
