# syntax=docker/dockerfile:1.7
# Multi-stage build do @ecommerce/saga-orchestrator-service.
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
# serviço, gera o Prisma Client ---------------------------------------------
FROM deps AS build
COPY . .
RUN pnpm install --frozen-lockfile --offline
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/observability build \
 && pnpm --filter @ecommerce/kafka build
RUN pnpm --filter @ecommerce/saga-orchestrator-service prisma:generate
RUN pnpm --filter @ecommerce/saga-orchestrator-service build

# ---- prune: bundle autocontido, só dependências de produção --------------
FROM build AS prune
RUN pnpm --filter=@ecommerce/saga-orchestrator-service deploy --prod /prod/saga-orchestrator-service
# Mesmo raciocínio do payment-service (ver comentário no Dockerfile dele): output
# CUSTOM do Prisma Client sobrevive ao `pnpm deploy` porque fica dentro da árvore
# do próprio pacote, não em node_modules. Remove engine de plataforma errada que
# `COPY . .` pode ter trazido da árvore de trabalho do host, e artefatos mortos em
# runtime (source maps, engines de outros providers).
RUN find /prod/saga-orchestrator-service/prisma/generated -type f -iname 'libquery_engine-darwin*' -delete \
 && find /prod/saga-orchestrator-service/prisma/generated -type f -iname 'libquery_engine-windows*' -delete \
 && find /prod/saga-orchestrator-service/prisma/generated -type f -name '*.map' -delete \
 && rm -f /prod/saga-orchestrator-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/schema-engine-* \
 && rm -f /prod/saga-orchestrator-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/libquery_engine-*
RUN find /prod/saga-orchestrator-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f \( -name 'query_engine_bg.*' -o -name 'query_compiler_bg.*' \) ! -iname '*postgresql*' -delete \
 && find /prod/saga-orchestrator-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f -name '*.map' -delete

# ---- runtime: imagem final, não-root, sem toolchain de build --------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
RUN apk update && apk upgrade --no-cache
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/saga-orchestrator-service ./
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
USER node
EXPOSE 3006
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3006/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
