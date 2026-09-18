# syntax=docker/dockerfile:1.7
# Multi-stage build do @ecommerce/shipping-service.
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
RUN pnpm --filter @ecommerce/shipping-service prisma:generate
RUN pnpm --filter @ecommerce/shipping-service build

# ---- prune: bundle autocontido, só dependências de produção --------------
FROM build AS prune
RUN pnpm --filter=@ecommerce/shipping-service deploy --prod /prod/shipping-service
# shipping-service usa output CUSTOM do Prisma Client (`output = "./generated"`
# em apps/shipping-service/prisma/schema.prisma), mesmo padrão do
# payment-service e diferente do order-service (output padrão em
# node_modules/@prisma/client). Verificado empiricamente para este serviço
# (build isolado do estágio `prune`, inspecionando
# /prod/shipping-service/prisma/generated/ dentro da imagem resultante) que
# `pnpm deploy --prod` PRESERVA esse artefato corretamente e por completo
# (client com os models KnownOrder/StockReservation/Outbox/ProcessedMessage,
# engine nativo linux-musl-arm64 correto): como o output fica dentro da árvore
# do próprio pacote (não em node_modules, que o deploy reconstrói a partir do
# content-addressable store), o `pnpm deploy` copia o diretório do workspace
# tal como está no estágio `build`, engine nativo e client completo inclusos.
# A regeneração aqui é DESNECESSÁRIA — mesma verificação e mesmo resultado já
# confirmados para payment-service e inventory-service (Dockerfiles idênticos
# neste ponto).
#
# O `.dockerignore` da raiz já exclui `**/prisma/generated` (corrigido antes
# desta tarefa, especificamente por causa da contaminação que o payment-service
# sofreu: o `COPY . .` do estágio `build` trazia junto o client
# darwin-arm64 gerado localmente no host). Com essa entrada no lugar, o
# `COPY . .` não traz mais nenhum `prisma/generated/` pré-existente do host —
# confirmado que a imagem resultante não contém nenhum binário
# `libquery_engine-darwin*`, então o cleanup de contaminação por engine de host
# que o payment-service precisou não é necessário aqui.
#
# `@prisma/client` (o pacote base, não o output custom) também é instalado em
# node_modules/.pnpm por ser uma `dependency` declarada no package.json —
# necessário só para o `prisma generate` funcionar. Confirmado no código-fonte
# (src/infrastructure/prisma.service.ts) que o serviço importa o PrismaClient
# exclusivamente de `../../prisma/generated/index.js`, nunca de `@prisma/client`
# diretamente — então o runtime desse pacote base nunca é executado. Ele
# carrega, por padrão, os query/compiler engines wasm de TODOS os providers
# (mysql/sqlite/sqlserver/cockroachdb/postgresql) mais os source maps — o
# mesmo artefato morto que order-service e payment-service já limpam nesse
# mesmo caminho.
# Mesma limpeza de @prisma/engines que order-service e payment-service já
# fazem: schema-engine e a cópia duplicada do query_engine binário (usados só
# por `prisma migrate`/`introspect`, nunca pelo @prisma/client em runtime) —
# 33.5MB que ficaram para trás nesta primeira versão do Dockerfile.
RUN find /prod/shipping-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f \( -name 'query_engine_bg.*' -o -name 'query_compiler_bg.*' \) ! -iname '*postgresql*' -delete \
 && find /prod/shipping-service/node_modules/.pnpm/@prisma+client*/node_modules/@prisma/client/runtime -type f -name '*.map' -delete \
 && rm -f /prod/shipping-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/schema-engine-* \
 && rm -f /prod/shipping-service/node_modules/.pnpm/@prisma+engines@*/node_modules/@prisma/engines/libquery_engine-*

# ---- runtime: imagem final, não-root, sem toolchain de build --------------
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
RUN apk update && apk upgrade --no-cache
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/shipping-service ./
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
USER node
EXPOSE 3003
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3003/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
