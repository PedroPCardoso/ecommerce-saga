# syntax=docker/dockerfile:1.7
# Multi-stage build do @ecommerce/notification-service.
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
# Notification não depende de @ecommerce/outbox (é só-consumidor, nunca
# publica evento de domínio — sem tabela outbox no seu schema) — diferente
# dos outros 4 serviços, aqui só compilamos o que o package.json realmente
# declara como dependência (contracts, kafka, idempotency).
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/observability build \
 && pnpm --filter @ecommerce/kafka build \
 && pnpm --filter @ecommerce/idempotency build
RUN pnpm --filter @ecommerce/notification-service prisma:generate
RUN pnpm --filter @ecommerce/notification-service build

# ---- prune: bundle autocontido, só dependências de produção --------------
FROM build AS prune
RUN pnpm --filter=@ecommerce/notification-service deploy --prod /prod/notification-service
# notification-service usa output CUSTOM do Prisma Client (`output =
# "./generated"` em apps/notification-service/prisma/schema.prisma), mesmo
# padrão de payment/inventory/shipping-service. Mesma verificação e mesmo
# resultado já confirmados para os três: `pnpm deploy --prod` preserva esse
# artefato corretamente (client com os models KnownOrder/ProcessedMessage,
# engine nativo correto) porque o output fica dentro da árvore do próprio
# pacote, não em node_modules — a regeneração aqui é DESNECESSÁRIA.
#
# O `.dockerignore` da raiz já exclui `**/prisma/generated`, então não há
# contaminação de engine gerado no host (darwin-arm64) a limpar aqui.
#
# `@prisma/client` (o pacote base, não o output custom) também é instalado
# em node_modules/.pnpm por ser uma `dependency` declarada no package.json —
# necessário só para o `prisma generate` funcionar. Confirmado no
# código-fonte (src/infrastructure/prisma.service.ts) que o serviço importa
# o PrismaClient exclusivamente de `../../prisma/generated/index.js`, nunca
# de `@prisma/client` diretamente — mesmo artefato morto que os outros 3
# serviços com output custom já limpam nesse mesmo caminho.
RUN find /prod/notification-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f \( -name 'query_engine_bg.*' -o -name 'query_compiler_bg.*' \) ! -iname '*postgresql*' -delete \
 && find /prod/notification-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f -name '*.map' -delete \
 && rm -f /prod/notification-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/schema-engine-* \
 && rm -f /prod/notification-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/libquery_engine-*

# ---- runtime: imagem final, não-root, sem toolchain de build --------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
RUN apk update && apk upgrade --no-cache
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/notification-service ./
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
USER node
EXPOSE 3004
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3004/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
