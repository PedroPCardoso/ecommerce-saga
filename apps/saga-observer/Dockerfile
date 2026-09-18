# syntax=docker/dockerfile:1.7
# Multi-stage build do @ecommerce/saga-observer.
# Mesmo padrão nos demais serviços — ver docs/superpowers/plans/2026-09-09-fase9-dockerizacao.md.

# ---- base: só a toolchain, nenhum código ainda --------------------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS base
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate
WORKDIR /repo

# ---- deps: só o lockfile — pnpm fetch não precisa de mais nada, e esta
# camada fica IDÊNTICA (e cacheada) nos Dockerfiles do monorepo ----------
FROM base AS deps
COPY pnpm-lock.yaml ./
RUN pnpm fetch

# ---- build: código completo, compila os pacotes compartilhados + o
# serviço --------------------------------------------------------------
FROM deps AS build
COPY . .
RUN pnpm install --frozen-lockfile --offline
# Saga Observer é só-consumidor e não tem banco (nem Prisma, nem outbox,
# nem idempotency): só compilamos o que o package.json realmente declara
# como dependência direta (contracts, kafka) — @ecommerce/observability
# entra aqui só porque @ecommerce/kafka passou a depender dela na Fase 7
# (métricas de consumer lag/DLQ), não porque saga-observer a use diretamente.
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/observability build \
 && pnpm --filter @ecommerce/kafka build
RUN pnpm --filter @ecommerce/saga-observer build

# ---- prune: bundle autocontido, só dependências de produção --------------
# `pnpm deploy` copia o pacote inteiro (dist/ + public/, incluído mesmo sem
# entrada em "files" — confirmado com um deploy de teste local) mais as
# dependências de produção resolvidas em node_modules.
FROM build AS prune
RUN pnpm --filter=@ecommerce/saga-observer deploy --prod /prod/saga-observer

# ---- runtime: imagem final, não-root, sem toolchain de build --------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
RUN apk update && apk upgrade --no-cache
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/saga-observer ./
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
USER node
EXPOSE 3005
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3005/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
