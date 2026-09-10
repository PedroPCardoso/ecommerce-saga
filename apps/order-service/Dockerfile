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
 && pnpm --filter @ecommerce/kafka build \
 && pnpm --filter @ecommerce/outbox build \
 && pnpm --filter @ecommerce/idempotency build
RUN pnpm --filter @ecommerce/order-service prisma:generate
RUN pnpm --filter @ecommerce/order-service build

# ---- prune: bundle autocontido, só dependências de produção --------------
FROM build AS prune
RUN pnpm --filter=@ecommerce/order-service deploy --prod /prod/order-service
# pnpm deploy não garante preservar o Prisma Client gerado no Step anterior
# (artefato gerado, não rastreado pelo lockfile) — regenera aqui, com a rede
# ainda disponível nesta etapa de build. Copia o schema explicitamente: sem
# `files` no package.json, não há garantia de que `prisma/` sobreviva à poda.
COPY apps/order-service/prisma /prod/order-service/prisma
RUN cd /prod/order-service && npx --yes prisma@6.1.0 generate

# ---- runtime: imagem final, não-root, sem toolchain de build --------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/order-service ./
USER node
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3000/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
