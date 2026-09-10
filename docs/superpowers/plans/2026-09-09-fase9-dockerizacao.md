# Fase 9 — Dockerização dos 5 Microserviços Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cada um dos 5 microserviços (`order`, `payment`, `inventory`,
`shipping`, `notification`) ganha um `Dockerfile` multi-stage não-root,
buildável e rodável via `docker compose`, e entra no
`deploy/docker/docker-compose.yml` — para que `docker compose up --build`
suba a saga inteira em contêiner, não só via `pnpm dev` local.

**Architecture:** Um `Dockerfile` por serviço em `apps/<serviço>/Dockerfile`
(mesmo arquivo em conteúdo, parametrizado só pelo nome do serviço/porta —
por que arquivo-por-serviço em vez de um Dockerfile único com build-arg:
é a convenção que `docs/PLAN.md` já declara — "Dockerfile multi-stage por
serviço" — e o que ferramentas de build/CI esperam encontrar por padrão).
4 estágios: `deps` (`pnpm fetch`, cacheável e IDÊNTICO nos 5 Dockerfiles),
`build` (código completo, compila os 4 pacotes compartilhados + o serviço,
`prisma generate`), `prune` (`pnpm deploy --prod` + regeração do Prisma
Client dentro do bundle podado — ver "Decisão técnica" abaixo), `runtime`
(`node:22-alpine` pinada por digest, `USER node`, `tini`, `HEALTHCHECK` em
`/health/live`). `docker-compose.yml` ganha 5 serviços novos, usando os
hostnames INTERNOS da rede `saga` (`kafka:9092`, `postgres-<serviço>:5432`,
`mailhog:1025`) — nunca `localhost`, que só funciona do host para dentro.

**Decisão técnica — por que regenerar o Prisma Client depois do `pnpm
deploy`:** `pnpm deploy --prod` resolve as dependências do serviço num
diretório autocontido a partir do lockfile, mas o Prisma Client é um
artefato GERADO (não rastreado pelo lockfile) — não há garantia de que ele
sobreviva intacto à poda. Este plano roda `prisma generate` de novo dentro
do diretório podado, ainda no estágio de build (com rede disponível), e a
verificação final (Task 2, Step 9) prova empiricamente que a imagem final
consegue falar com o Postgres — se essa verificação falhar, é o primeiro
lugar a investigar.

**Tech Stack:** `node:22-alpine` pinada por digest real (resolvido nesta
sessão via `docker pull` — nunca inventado), `pnpm@9.15.0` via Corepack,
`tini`, Docker Compose v2, Trivy (scan de vulnerabilidade).

## Global Constraints

- Requer as Fases 0, 2, 1, 3, 4 e 5 completas e commitadas — os 5 serviços
  precisam existir e ter `build`/`prisma:generate` funcionando localmente
  antes de dockerizar (não adianta dockerizar código que não builda).
- Imagem base pinada por **digest real**, resolvido com `docker pull
  node:22-alpine` + `docker inspect` — jamais copie um hash de outro lugar
  sem confirmar contra o registry no momento do build (o digest muda a
  cada rebuild upstream da tag `22-alpine`; se este plano for executado
  muito tempo depois de escrito, re-resolva o digest atual em vez de usar o
  valor fixado aqui às cegas).
- `USER node` (não-root) em todas as imagens finais; `tini` como PID 1 para
  reap de processos zumbi e propagação correta de sinal (SIGTERM do
  `docker stop` precisa chegar ao Node, não travar em PID 1 sem
  encaminhamento).
- Nenhum segredo em `ARG`/`ENV` do Dockerfile — credenciais vêm de variável
  de ambiente injetada pelo `docker-compose.yml` (que por sua vez lê do
  `.env` do host, nunca hardcoded na imagem). `.dockerignore` exclui `.env`
  do contexto de build — sem isso, `.env` do host vazaria para dentro da
  imagem mesmo sem um `COPY .env` explícito (qualquer `COPY . .` copia tudo
  que não estiver no `.dockerignore`).
- `docker-compose.yml` usa hostnames INTERNOS da rede `saga`
  (`kafka:9092`, `postgres-order:5432`, etc.) — nunca `localhost`/portas
  mapeadas do host (`29092`, `154xx`), que só existem do lado de fora do
  Docker.
- `pnpm topics:create` continua sendo passo MANUAL após `docker compose up`
  (mesma convenção que o README já documenta) — este plano não automatiza
  isso num container de inicialização; ver "Escopo e limite" abaixo.
- Trivy sem HIGH/CRITICAL e imagem < 200MB são critérios de pronto do
  `docs/PLAN.md` Fase 9 — verificados explicitamente na Task 2 (e
  replicados nas Tasks 3-6).
- Commits diretos em `master`, conventional commits, um por Task.

## Escopo e limite deste documento

`docs/PLAN.md` Fase 9 também menciona SBOM com Syft e scan Trivy no CI —
este plano cobre o scan Trivy LOCAL (rodado manualmente como parte da
verificação), mas não configura um pipeline de CI (não existe CI configurado
neste repositório ainda — isso seria escopo de uma fase à parte). Também não
automatiza `pnpm topics:create` como container de inicialização do compose
(ver Global Constraints) — é um passo manual documentado, igual já é hoje.

---

### Task 1: `.dockerignore` e verificação do digest da imagem base

**Files:**
- Create: `.dockerignore` (raiz do repo)

**Interfaces:**
- Não produz API — é configuração de build compartilhada por todos os
  Dockerfiles das tasks seguintes.

- [ ] **Step 1: Resolver o digest atual de `node:22-alpine`**

```bash
docker pull node:22-alpine
docker inspect --format='{{index .RepoDigests 0}}' node:22-alpine
```

Guarde o valor exato retornado (formato `node@sha256:...`). **Não** use o
valor deste documento sem reconfirmar — ele foi resolvido em 2026-09-09 e a
tag `22-alpine` é atualizada periodicamente pelo mantenedor da imagem. No
momento em que este plano foi escrito, o valor era:

```
node@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32
```

Use o valor que VOCÊ resolveu no Step acima nas Tasks 2-6, não este.

- [ ] **Step 2: Criar o `.dockerignore`**

`.dockerignore` (raiz):
```
# Segredo — NUNCA deve entrar no contexto de build (COPY . . copia tudo que
# não estiver aqui; sem esta linha, .env do host vazaria para dentro da imagem).
.env
.env.*
!.env.example
*.pem
*.key
*.p12

# Dependências e build — reconstruídos dentro do container, não copiados do host.
node_modules
**/node_modules
dist
**/dist
.turbo
**/.turbo
coverage
**/coverage
*.tsbuildinfo

# Controle de versão e editor.
.git
.gitignore
.vscode
.idea
*.swp
.DS_Store

# Nada disto é necessário dentro da imagem de runtime.
docs
examples
tools
deploy/k8s
deploy/helm
.playwright-mcp
*.log
```

- [ ] **Step 3: Confirmar que o `.env` real (se existir) não aparece no contexto**

```bash
docker build --no-cache -f - -t dockerignore-check . <<'EOF'
FROM busybox
COPY . /ctx
RUN test ! -f /ctx/.env && echo "OK: .env não está no contexto" || (echo "FALHA: .env vazou para o contexto" && exit 1)
EOF
docker rmi dockerignore-check
```

Esperado: `OK: .env não está no contexto`. Se você não tem um `.env` local
ainda (só `.env.example`), rode `cp .env.example .env` antes deste teste
para ele ser significativo, e apague o `.env` de teste depois se não for
usá-lo.

- [ ] **Step 4: Commit**

```bash
git add .dockerignore
git commit -m "chore(docker): .dockerignore compartilhado por todos os Dockerfiles"
```

---

### Task 2: `Dockerfile` do Order Service (referência completa, com toda a explicação)

**Files:**
- Create: `apps/order-service/Dockerfile`

**Interfaces:**
- Não produz API — build artifact (imagem Docker
  `ecommerce/order-service:local`).

- [ ] **Step 1: Escrever o Dockerfile**

`apps/order-service/Dockerfile` (troque o digest pelo que você resolveu na
Task 1, Step 1, se for diferente do fixado aqui):
```dockerfile
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
```

Notas sobre decisões deste arquivo (as mesmas valem para as Tasks 3-6):

- **`pnpm fetch` antes de `COPY . .`**: cacheia a resolução de dependências
  por uma camada que só invalida quando `pnpm-lock.yaml` muda — editar
  código da aplicação não força reinstalar tudo.
- **Build compila os 4 pacotes compartilhados sempre**, mesmo que este
  serviço não use todos (Order não usa `@ecommerce/idempotency`, por
  exemplo) — simplicidade de ter uma receita idêntica nos 5 Dockerfiles
  vale mais que economizar alguns segundos de build.
- **`USER node`**: a imagem base `node:*-alpine` já vem com um usuário
  `node` (uid 1000) pronto — não precisa criar um.
- **`HEALTHCHECK` usa `fetch` nativo do Node 22** (sem `curl`/`wget`
  instalado na imagem final — superfície menor).
- **`tini` como `ENTRYPOINT`**: sem isso, `node` roda como PID 1 e não
  reencaminha `SIGTERM` para processos filhos nem faz reap de zumbis —
  `docker stop` acabaria matando o container à força depois do timeout, em
  vez do shutdown gracioso que o `main.ts` de cada serviço já implementa.

- [ ] **Step 2: Buildar a imagem**

```bash
docker build -f apps/order-service/Dockerfile -t ecommerce/order-service:local .
```

Rode a partir da RAIZ do repo (o `.` no final é o contexto — precisa
enxergar `pnpm-lock.yaml` e `apps/order-service/`).

Esperado: build termina sem erro. Se falhar no estágio `prune` ao rodar
`prisma generate`, confirme que `apps/order-service/prisma/schema.prisma`
existe e que a etapa anterior (`COPY apps/order-service/prisma ...`) rodou.

- [ ] **Step 3: Verificar o tamanho da imagem**

```bash
docker images ecommerce/order-service:local --format '{{.Size}}'
```

Esperado: menor que 200MB (critério do `docs/PLAN.md` Fase 9). Se estourar,
o suspeito mais provável é `@nestjs/*` completo em vez de só os pacotes
usados, ou o `prune` não ter realmente removido devDependencies — confira
com `docker run --rm ecommerce/order-service:local du -sh node_modules`.

- [ ] **Step 4: Rodar como não-root e confirmar**

```bash
docker run --rm ecommerce/order-service:local whoami
```

Esperado: `node` (nunca `root`).

- [ ] **Step 5: Scan de vulnerabilidade com Trivy**

```bash
trivy image --severity HIGH,CRITICAL ecommerce/order-service:local
```

Se o Trivy não estiver instalado localmente: `brew install trivy` (macOS)
ou `docker run --rm -v /var/run/docker.sock:/var/run/docker.sock
aquasec/trivy image --severity HIGH,CRITICAL ecommerce/order-service:local`.

Esperado: 0 vulnerabilidades HIGH/CRITICAL (critério do `docs/PLAN.md` Fase
9). Se aparecer alguma, investigue se é da imagem base (espere a próxima
atualização de `node:22-alpine` e reresolva o digest) ou de uma dependência
npm desatualizada (`pnpm audit` para localizar).

- [ ] **Step 6: Subir a infra e rodar o container conectado a ela**

```bash
pnpm infra:up
pnpm topics:create
docker run --rm --network ecommerce-saga_saga \
  -e ORDER_DATABASE_URL="postgresql://order_svc:changeme@postgres-order:5432/order_db?schema=public" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_CLIENT_ID_PREFIX="ecommerce" \
  -e JWT_SECRET="dev-only-not-a-real-secret-change-me" \
  -e JWT_ISSUER="ecommerce-local" \
  -e ORDER_SERVICE_PORT="3000" \
  -p 3000:3000 \
  ecommerce/order-service:local &
sleep 3
```

O nome da rede (`ecommerce-saga_saga`) vem do `name: ecommerce-saga` +
`networks: [saga]` do `docker-compose.yml` — confirme com `docker network
ls` se o nome real for diferente no seu Docker.

- [ ] **Step 7: Rodar a migration dentro do container (a imagem não roda migration sozinha, de propósito — ver nota)**

A imagem de runtime não inclui o Prisma CLI nem os arquivos de migration
de propósito (imagem de produção enxuta). Para este teste manual, rode a
migration a partir do host, como já feito nas Fases 1/3/4/5:

```bash
cd apps/order-service
pnpm exec dotenv -e ../../.env -- prisma migrate deploy
cd ../..
```

- [ ] **Step 8: Smoke test HTTP contra o container**

```bash
curl -s http://localhost:3000/health/live
```

Esperado: `{"status":"ok"}`.

- [ ] **Step 9: Verificação funcional — POST /orders através do container**

```bash
TOKEN=$(node -e "console.log(require('jsonwebtoken').sign({sub:'018f3f4e-0000-7000-8000-000000000099'}, 'dev-only-not-a-real-secret-change-me', {issuer:'ecommerce-local', expiresIn:'15m'}))")
curl -s -X POST http://localhost:3000/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $(node -e 'console.log(require("crypto").randomUUID())')" \
  -H "Content-Type: application/json" \
  -d '{"items":[{"sku":"BOOK-001","name":"Livro","quantity":1,"unitPriceCents":4990}],"currency":"BRL","shippingAddress":{"street":"Rua X","number":"1","district":"Centro","city":"SP","state":"SP","zipCode":"01000-000","country":"BR"}}'
```

Esperado: `201` com `{ orderId, status: "PENDING", createdAt }` — prova que
a imagem consegue falar com Postgres (via `postgres-order:5432` interno) e
Kafka (via `kafka:9092` interno) de dentro do container, não só do host.
Confirme no Kafka UI (http://localhost:8080) que `order.created` chegou.

- [ ] **Step 10: Encerrar o container de teste**

```bash
kill %1 2>/dev/null || docker stop $(docker ps -qf "ancestor=ecommerce/order-service:local")
```

- [ ] **Step 11: Commit**

```bash
git add apps/order-service/Dockerfile
git commit -m "feat(docker): Dockerfile multi-stage do order-service, não-root, imagem pinada"
```

---

### Task 3: `Dockerfile` do Payment Service

**Files:**
- Create: `apps/payment-service/Dockerfile`

- [ ] **Step 1: Escrever o Dockerfile (mesmo padrão da Task 2, porta 3001)**

`apps/payment-service/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS base
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate
WORKDIR /repo

FROM base AS deps
COPY pnpm-lock.yaml ./
RUN pnpm fetch

FROM deps AS build
COPY . .
RUN pnpm install --frozen-lockfile --offline
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/kafka build \
 && pnpm --filter @ecommerce/outbox build \
 && pnpm --filter @ecommerce/idempotency build
RUN pnpm --filter @ecommerce/payment-service prisma:generate
RUN pnpm --filter @ecommerce/payment-service build

FROM build AS prune
RUN pnpm --filter=@ecommerce/payment-service deploy --prod /prod/payment-service
COPY apps/payment-service/prisma /prod/payment-service/prisma
RUN cd /prod/payment-service && npx --yes prisma@6.1.0 generate

FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/payment-service ./
USER node
EXPOSE 3001
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3001/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
```

- [ ] **Step 2: Build, tamanho, não-root, Trivy (mesmos comandos da Task 2, trocando o nome da imagem)**

```bash
docker build -f apps/payment-service/Dockerfile -t ecommerce/payment-service:local .
docker images ecommerce/payment-service:local --format '{{.Size}}'
docker run --rm ecommerce/payment-service:local whoami
trivy image --severity HIGH,CRITICAL ecommerce/payment-service:local
```

Esperado: build ok, < 200MB, `node`, 0 HIGH/CRITICAL.

- [ ] **Step 3: Smoke test contra a infra real**

```bash
cd apps/payment-service && pnpm exec dotenv -e ../../.env -- prisma migrate deploy && cd ../..
docker run --rm --network ecommerce-saga_saga \
  -e PAYMENT_DATABASE_URL="postgresql://payment_svc:changeme@postgres-payment:5432/payment_db?schema=public" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_CLIENT_ID_PREFIX="ecommerce" \
  -e PAYMENT_SERVICE_PORT="3001" \
  -p 3001:3001 \
  ecommerce/payment-service:local &
sleep 3
curl -s http://localhost:3001/health/live
kill %1 2>/dev/null || docker stop $(docker ps -qf "ancestor=ecommerce/payment-service:local")
```

Esperado: `{"status":"ok"}`.

- [ ] **Step 4: Commit**

```bash
git add apps/payment-service/Dockerfile
git commit -m "feat(docker): Dockerfile multi-stage do payment-service"
```

---

### Task 4: `Dockerfile` do Inventory Service

**Files:**
- Create: `apps/inventory-service/Dockerfile`

- [ ] **Step 1: Escrever o Dockerfile (porta 3002)**

`apps/inventory-service/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS base
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate
WORKDIR /repo

FROM base AS deps
COPY pnpm-lock.yaml ./
RUN pnpm fetch

FROM deps AS build
COPY . .
RUN pnpm install --frozen-lockfile --offline
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/kafka build \
 && pnpm --filter @ecommerce/outbox build \
 && pnpm --filter @ecommerce/idempotency build
RUN pnpm --filter @ecommerce/inventory-service prisma:generate
RUN pnpm --filter @ecommerce/inventory-service build

FROM build AS prune
RUN pnpm --filter=@ecommerce/inventory-service deploy --prod /prod/inventory-service
COPY apps/inventory-service/prisma /prod/inventory-service/prisma
RUN cd /prod/inventory-service && npx --yes prisma@6.1.0 generate

FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/inventory-service ./
USER node
EXPOSE 3002
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3002/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
```

- [ ] **Step 2: Build, tamanho, não-root, Trivy**

```bash
docker build -f apps/inventory-service/Dockerfile -t ecommerce/inventory-service:local .
docker images ecommerce/inventory-service:local --format '{{.Size}}'
docker run --rm ecommerce/inventory-service:local whoami
trivy image --severity HIGH,CRITICAL ecommerce/inventory-service:local
```

- [ ] **Step 3: Smoke test contra a infra real**

```bash
cd apps/inventory-service && pnpm exec dotenv -e ../../.env -- prisma migrate deploy && cd ../..
docker run --rm --network ecommerce-saga_saga \
  -e INVENTORY_DATABASE_URL="postgresql://inventory_svc:changeme@postgres-inventory:5432/inventory_db?schema=public" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_CLIENT_ID_PREFIX="ecommerce" \
  -e INVENTORY_SERVICE_PORT="3002" \
  -p 3002:3002 \
  ecommerce/inventory-service:local &
sleep 3
curl -s http://localhost:3002/health/live
kill %1 2>/dev/null || docker stop $(docker ps -qf "ancestor=ecommerce/inventory-service:local")
```

- [ ] **Step 4: Commit**

```bash
git add apps/inventory-service/Dockerfile
git commit -m "feat(docker): Dockerfile multi-stage do inventory-service"
```

---

### Task 5: `Dockerfile` do Shipping Service

**Files:**
- Create: `apps/shipping-service/Dockerfile`

- [ ] **Step 1: Escrever o Dockerfile (porta 3003)**

`apps/shipping-service/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS base
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate
WORKDIR /repo

FROM base AS deps
COPY pnpm-lock.yaml ./
RUN pnpm fetch

FROM deps AS build
COPY . .
RUN pnpm install --frozen-lockfile --offline
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/kafka build \
 && pnpm --filter @ecommerce/outbox build \
 && pnpm --filter @ecommerce/idempotency build
RUN pnpm --filter @ecommerce/shipping-service prisma:generate
RUN pnpm --filter @ecommerce/shipping-service build

FROM build AS prune
RUN pnpm --filter=@ecommerce/shipping-service deploy --prod /prod/shipping-service
COPY apps/shipping-service/prisma /prod/shipping-service/prisma
RUN cd /prod/shipping-service && npx --yes prisma@6.1.0 generate

FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/shipping-service ./
USER node
EXPOSE 3003
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3003/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
```

- [ ] **Step 2: Build, tamanho, não-root, Trivy**

```bash
docker build -f apps/shipping-service/Dockerfile -t ecommerce/shipping-service:local .
docker images ecommerce/shipping-service:local --format '{{.Size}}'
docker run --rm ecommerce/shipping-service:local whoami
trivy image --severity HIGH,CRITICAL ecommerce/shipping-service:local
```

- [ ] **Step 3: Smoke test contra a infra real**

```bash
cd apps/shipping-service && pnpm exec dotenv -e ../../.env -- prisma migrate deploy && cd ../..
docker run --rm --network ecommerce-saga_saga \
  -e SHIPPING_DATABASE_URL="postgresql://shipping_svc:changeme@postgres-shipping:5432/shipping_db?schema=public" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_CLIENT_ID_PREFIX="ecommerce" \
  -e SHIPPING_SERVICE_PORT="3003" \
  -p 3003:3003 \
  ecommerce/shipping-service:local &
sleep 3
curl -s http://localhost:3003/health/live
kill %1 2>/dev/null || docker stop $(docker ps -qf "ancestor=ecommerce/shipping-service:local")
```

- [ ] **Step 4: Commit**

```bash
git add apps/shipping-service/Dockerfile
git commit -m "feat(docker): Dockerfile multi-stage do shipping-service"
```

---

### Task 6: `Dockerfile` do Notification Service

**Files:**
- Create: `apps/notification-service/Dockerfile`

**Nota:** este serviço não usa `@ecommerce/outbox` nem tem tabela outbox —
o build compila os mesmos 4 pacotes compartilhados por consistência com os
outros Dockerfiles (Notification só realmente precisa de `contracts`,
`kafka` e `idempotency`, mas compilar `outbox` também é inofensivo e mantém
a receita idêntica nos 5 arquivos).

- [ ] **Step 1: Escrever o Dockerfile (porta 3004)**

`apps/notification-service/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS base
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate
WORKDIR /repo

FROM base AS deps
COPY pnpm-lock.yaml ./
RUN pnpm fetch

FROM deps AS build
COPY . .
RUN pnpm install --frozen-lockfile --offline
RUN pnpm --filter @ecommerce/contracts build \
 && pnpm --filter @ecommerce/kafka build \
 && pnpm --filter @ecommerce/outbox build \
 && pnpm --filter @ecommerce/idempotency build
RUN pnpm --filter @ecommerce/notification-service prisma:generate
RUN pnpm --filter @ecommerce/notification-service build

FROM build AS prune
RUN pnpm --filter=@ecommerce/notification-service deploy --prod /prod/notification-service
COPY apps/notification-service/prisma /prod/notification-service/prisma
RUN cd /prod/notification-service && npx --yes prisma@6.1.0 generate

FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS runtime
ENV NODE_ENV=production
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=prune /prod/notification-service ./
USER node
EXPOSE 3004
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3004/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
```

- [ ] **Step 2: Build, tamanho, não-root, Trivy**

```bash
docker build -f apps/notification-service/Dockerfile -t ecommerce/notification-service:local .
docker images ecommerce/notification-service:local --format '{{.Size}}'
docker run --rm ecommerce/notification-service:local whoami
trivy image --severity HIGH,CRITICAL ecommerce/notification-service:local
```

- [ ] **Step 3: Smoke test contra a infra real (inclui o Mailhog interno)**

```bash
cd apps/notification-service && pnpm exec dotenv -e ../../.env -- prisma migrate deploy && cd ../..
docker run --rm --network ecommerce-saga_saga \
  -e NOTIFICATION_DATABASE_URL="postgresql://notification_svc:changeme@postgres-notification:5432/notification_db?schema=public" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_CLIENT_ID_PREFIX="ecommerce" \
  -e NOTIFICATION_SERVICE_PORT="3004" \
  -e SMTP_HOST="mailhog" \
  -e SMTP_PORT="1025" \
  -e SMTP_FROM="no-reply@example.com" \
  -p 3004:3004 \
  ecommerce/notification-service:local &
sleep 3
curl -s http://localhost:3004/health/live
kill %1 2>/dev/null || docker stop $(docker ps -qf "ancestor=ecommerce/notification-service:local")
```

- [ ] **Step 4: Commit**

```bash
git add apps/notification-service/Dockerfile
git commit -m "feat(docker): Dockerfile multi-stage do notification-service"
```

---

### Task 7: Integrar os 5 serviços ao `docker-compose.yml`

**Files:**
- Modify: `deploy/docker/docker-compose.yml`

**Interfaces:**
- Consumes: os 5 Dockerfiles das Tasks 2-6; os serviços `kafka`,
  `postgres-order`, `postgres-payment`, `postgres-inventory`,
  `postgres-shipping`, `postgres-notification`, `mailhog` já existentes no
  mesmo arquivo.
- Produces: `docker compose up --build` sobe a saga inteira (infra + os 5
  serviços), num único comando.

- [ ] **Step 1: Acrescentar os 5 serviços ao `docker-compose.yml`**

Abra `deploy/docker/docker-compose.yml` e acrescente, logo antes da seção
`volumes:` final (ou em qualquer ponto dentro de `services:`, a ordem em
YAML não importa para o Compose):

```yaml
  # ------------------------------------------------------------- serviços ---
  order-service:
    build:
      context: ../..
      dockerfile: apps/order-service/Dockerfile
    container_name: ecommerce-order-service
    restart: unless-stopped
    ports: ['3000:3000']
    environment:
      NODE_ENV: production
      ORDER_SERVICE_PORT: '3000'
      ORDER_DATABASE_URL: postgresql://order_svc:changeme@postgres-order:5432/order_db?schema=public
      KAFKA_BROKERS: kafka:9092
      KAFKA_CLIENT_ID_PREFIX: ecommerce
      JWT_SECRET: dev-only-not-a-real-secret-change-me
      JWT_ISSUER: ecommerce-local
    depends_on:
      kafka:
        condition: service_healthy
      postgres-order:
        condition: service_healthy
    networks: [saga]

  payment-service:
    build:
      context: ../..
      dockerfile: apps/payment-service/Dockerfile
    container_name: ecommerce-payment-service
    restart: unless-stopped
    ports: ['3001:3001']
    environment:
      NODE_ENV: production
      PAYMENT_SERVICE_PORT: '3001'
      PAYMENT_DATABASE_URL: postgresql://payment_svc:changeme@postgres-payment:5432/payment_db?schema=public
      KAFKA_BROKERS: kafka:9092
      KAFKA_CLIENT_ID_PREFIX: ecommerce
    depends_on:
      kafka:
        condition: service_healthy
      postgres-payment:
        condition: service_healthy
    networks: [saga]

  inventory-service:
    build:
      context: ../..
      dockerfile: apps/inventory-service/Dockerfile
    container_name: ecommerce-inventory-service
    restart: unless-stopped
    ports: ['3002:3002']
    environment:
      NODE_ENV: production
      INVENTORY_SERVICE_PORT: '3002'
      INVENTORY_DATABASE_URL: postgresql://inventory_svc:changeme@postgres-inventory:5432/inventory_db?schema=public
      KAFKA_BROKERS: kafka:9092
      KAFKA_CLIENT_ID_PREFIX: ecommerce
    depends_on:
      kafka:
        condition: service_healthy
      postgres-inventory:
        condition: service_healthy
    networks: [saga]

  shipping-service:
    build:
      context: ../..
      dockerfile: apps/shipping-service/Dockerfile
    container_name: ecommerce-shipping-service
    restart: unless-stopped
    ports: ['3003:3003']
    environment:
      NODE_ENV: production
      SHIPPING_SERVICE_PORT: '3003'
      SHIPPING_DATABASE_URL: postgresql://shipping_svc:changeme@postgres-shipping:5432/shipping_db?schema=public
      KAFKA_BROKERS: kafka:9092
      KAFKA_CLIENT_ID_PREFIX: ecommerce
    depends_on:
      kafka:
        condition: service_healthy
      postgres-shipping:
        condition: service_healthy
    networks: [saga]

  notification-service:
    build:
      context: ../..
      dockerfile: apps/notification-service/Dockerfile
    container_name: ecommerce-notification-service
    restart: unless-stopped
    ports: ['3004:3004']
    environment:
      NODE_ENV: production
      NOTIFICATION_SERVICE_PORT: '3004'
      NOTIFICATION_DATABASE_URL: postgresql://notification_svc:changeme@postgres-notification:5432/notification_db?schema=public
      KAFKA_BROKERS: kafka:9092
      KAFKA_CLIENT_ID_PREFIX: ecommerce
      SMTP_HOST: mailhog
      SMTP_PORT: '1025'
      SMTP_FROM: no-reply@example.com
    depends_on:
      kafka:
        condition: service_healthy
      postgres-notification:
        condition: service_healthy
      mailhog:
        condition: service_started
    networks: [saga]
```

`context: ../..` porque o `docker-compose.yml` vive em `deploy/docker/` —
o contexto de build precisa ser a RAIZ do repo (onde estão
`pnpm-lock.yaml` e `apps/`), duas pastas acima.

Nenhum serviço define `healthcheck:` próprio aqui — o `HEALTHCHECK` já está
no `Dockerfile` de cada um (evita duplicar a mesma checagem em dois
lugares).

- [ ] **Step 2: Validar a sintaxe do compose**

```bash
docker compose -f deploy/docker/docker-compose.yml config --quiet
```

Esperado: sem erro (comando silencioso = sintaxe válida).

- [ ] **Step 3: Subir tudo com um único comando**

```bash
pnpm infra:down 2>/dev/null || true
docker compose -f deploy/docker/docker-compose.yml up -d --build
```

Aguarde os healthchecks. Confirme:

```bash
docker compose -f deploy/docker/docker-compose.yml ps
```

Esperado: os 5 serviços novos com status `running` (o `HEALTHCHECK` do
Dockerfile leva alguns segundos para reportar `healthy` — normal).

- [ ] **Step 4: Rodar as migrations (ainda não automatizadas — ver "Escopo e limite")**

```bash
for svc in order payment inventory shipping notification; do
  (cd "apps/${svc}-service" && pnpm exec dotenv -e ../../.env -- prisma migrate deploy)
done
```

- [ ] **Step 5: Criar a topologia de tópicos**

```bash
pnpm topics:create
```

- [ ] **Step 6: Verificação funcional completa — a saga inteira via containers**

```bash
TOKEN=$(node -e "console.log(require('jsonwebtoken').sign({sub:'018f3f4e-0000-7000-8000-000000000099'}, 'dev-only-not-a-real-secret-change-me', {issuer:'ecommerce-local', expiresIn:'15m'}))")
curl -s -X POST http://localhost:3000/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $(node -e 'console.log(require("crypto").randomUUID())')" \
  -H "Content-Type: application/json" \
  -d '{"items":[{"sku":"BOOK-001","name":"Livro","quantity":1,"unitPriceCents":4990}],"currency":"BRL","shippingAddress":{"street":"Rua X","number":"1","district":"Centro","city":"SP","state":"SP","zipCode":"01000-000","country":"BR"}}'
```

Esperado: `201`. Confirme, alguns segundos depois:

```bash
docker exec ecommerce-pg-payment psql -U payment_svc -d payment_db -c "select status from payments order by created_at desc limit 1;"
docker exec ecommerce-pg-inventory psql -U inventory_svc -d inventory_db -c "select status from stock_reservations order by created_at desc limit 1;"
docker exec ecommerce-pg-shipping psql -U shipping_svc -d shipping_db -c "select status from shipments order by created_at desc limit 1;"
curl -s http://localhost:18025/api/v2/messages | node -e "process.stdin.resume();process.stdin.on('data',d=>{const j=JSON.parse(d);console.log(j.items[0]?.Content?.Headers?.Subject)})"
```

Esperado: `AUTHORIZED`, `RESERVED`, `CREATED`, e um assunto de e-mail
"Recebemos seu pedido ..." (ou mais recente, dependendo do que a saga já
processou) — a saga inteira funcionando com os 5 serviços rodando em
container, não mais via `pnpm dev`.

- [ ] **Step 7: Logs — confirmar shutdown gracioso**

```bash
docker compose -f deploy/docker/docker-compose.yml stop order-service
docker compose -f deploy/docker/docker-compose.yml logs order-service --tail 5
```

Esperado: a última linha do log é
`[order-service] recebido SIGTERM, encerrando graciosamente` — prova que o
`tini` está encaminhando o sinal corretamente para o `main.ts`.

```bash
docker compose -f deploy/docker/docker-compose.yml start order-service
```

- [ ] **Step 8: Commit**

```bash
git add deploy/docker/docker-compose.yml
git commit -m "feat(docker): integra os 5 microserviços ao docker-compose.yml"
```

---

## Verificação final da fase

- [ ] `docker compose -f deploy/docker/docker-compose.yml up -d --build`
      sobe infra + os 5 serviços com um único comando (depois de
      `pnpm topics:create` e das migrations, que continuam manuais nesta
      fase).
- [ ] As 5 imagens rodam como `USER node`, cada uma < 200MB, 0
      HIGH/CRITICAL no Trivy.
- [ ] `docker stop` de qualquer serviço loga o shutdown gracioso antes de
      sair (prova que `tini` + `SIGTERM` funcionam).
- [ ] Um `POST /orders` através do Order Service **containerizado** percorre
      a saga inteira (Payment → Inventory → Shipping → Notification), todos
      rodando em container, não em `pnpm dev`.
- [ ] Nenhum segredo real em nenhum `Dockerfile`/`docker-compose.yml` —
      credenciais são as mesmas fictícias que já existem em `.env.example`.

Com os 5 serviços dockerizados, as Fases 6 (resiliência), 7
(observabilidade) e 10 (Kubernetes) já têm imagens reais para trabalhar em
cima — a Fase 10 em particular reaproveita exatamente estes Dockerfiles
como base para os manifests do Minikube.
