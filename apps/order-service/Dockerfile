# syntax=docker/dockerfile:1.7
# Multi-stage build do @ecommerce/order-service.
# Mesmo padrão nos 5 serviços — ver docs/superpowers/plans/2026-09-09-fase9-dockerizacao.md.

# ---- base: só a toolchain, nenhum código ainda --------------------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS base
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate
WORKDIR /repo

# ---- deps: só o lockfile — pnpm fetch não precisa de mais nada, e esta
# camada fica IDÊNTICA (e cacheada) nos 5 Dockerfiles do monorepo ----------
FROM base AS deps
COPY pnpm-lock.yaml ./
RUN pnpm fetch

# ---- build: código completo, compila os pacotes compartilhados + o
# serviço, gera o Prisma Client ---------------------------------------------
FROM deps AS build
COPY . .
RUN pnpm install --frozen-lockfile --offline
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/observability build \
 && pnpm --filter @ecommerce/kafka build \
 && pnpm --filter @ecommerce/outbox build \
 && pnpm --filter @ecommerce/idempotency build
RUN pnpm --filter @ecommerce/order-service prisma:generate
RUN pnpm --filter @ecommerce/order-service build

# ---- prune: bundle autocontido, só dependências de produção --------------
FROM build AS prune
RUN pnpm --filter=@ecommerce/order-service deploy --prod /prod/order-service
# order-service usa o output PADRÃO do Prisma Client (node_modules/@prisma/client,
# sem `output` custom no generator) — verificado empiricamente que `pnpm deploy
# --prod` NÃO preserva esse artefato: ele reconstrói node_modules a partir do
# content-addressable store do pnpm, então o client acaba voltando ao stub
# genérico (sem os models Order/Outbox/IdempotencyKey). Por isso a regeneração
# aqui é OBRIGATÓRIA para este serviço, não redundante (ao contrário do que se
# presumia originalmente — ver task-2-fix-report.md para o teste que provou isso).
COPY apps/order-service/prisma /prod/order-service/prisma
# Usa o binário `prisma` já instalado no estágio `build` (mesma versão travada
# pelo lockfile do @prisma/client, sem o mismatch de versão do antigo
# `npx --yes prisma@6.1.0`) e aponta --schema para o diretório podado — assim
# não precisa baixar nada da rede nem reinstalar o pacote `prisma` dentro de
# /prod/order-service.
RUN pnpm --filter @ecommerce/order-service exec prisma generate --schema=/prod/order-service/prisma/schema.prisma
# Remove artefatos do Prisma comprovadamente mortos em runtime (o serviço só
# usa PostgreSQL, via o binary engine nativo já resolvido em .prisma/client):
# - query/compiler engines wasm de outros providers (mysql/sqlite/sqlserver/
#   cockroachdb) empacotados sempre no @prisma/client, independente de schema;
# - source maps (*.map), nunca lidos em produção;
# - o binário `schema-engine` e a cópia duplicada do `query_engine` dentro de
#   @prisma/engines (usados só por `prisma migrate`/`introspect`, nunca pelo
#   @prisma/client em runtime).
RUN find /prod/order-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f \( -name 'query_engine_bg.*' -o -name 'query_compiler_bg.*' \) ! -iname '*postgresql*' -delete \
 && find /prod/order-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f -name '*.map' -delete \
 && rm -f /prod/order-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/schema-engine-* \
 && rm -f /prod/order-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/libquery_engine-*

# ---- runtime: imagem final, não-root, sem toolchain de build --------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
# Aplica patches de pacotes OS (Alpine) sobre o digest pinado — corrige
# libssl3/libcrypto3 (CVE-2026-14456) sem abrir mão da reprodutibilidade do
# ponto de partida: o digest fixa a base, o upgrade só traz correções de
# segurança já publicadas pelo repositório Alpine desse mesmo release.
RUN apk update && apk upgrade --no-cache
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/order-service ./
# Remove o npm (CLI completo + suas dependências, incluindo tar/pacote/
# sigstore/brace-expansion com CVEs HIGH/CRITICAL) e o corepack embutidos na
# imagem base do Node — nenhum dos dois é invocado em runtime (CMD é só
# `node dist/main.js`). Precisa rodar como root, antes do `USER node`.
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
USER node
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3000/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
