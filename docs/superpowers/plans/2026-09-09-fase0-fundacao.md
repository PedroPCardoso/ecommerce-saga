# Fase 0 — Fundação (restante) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Commitar a fundação já existente do monorepo em `master`, adicionar
Husky + commitlint (conventional commits), e confirmar que a infra local sobe
de ponta a ponta antes de qualquer serviço ser implementado.

**Architecture:** Nenhum código novo de aplicação. Três tarefas: (1) primeiro
commit do repositório, (2) hooks de git para mensagens de commit
padronizadas, (3) verificação executável de que `docker compose up` +
criação de tópicos funciona.

**Tech Stack:** Husky 9, `@commitlint/cli` + `@commitlint/config-conventional`,
Docker Compose, kafkajs (script já existente).

## Global Constraints

- Node >=22, pnpm 9 (`packageManager` em `package.json`) — respeitar `.nvmrc`.
- Nenhum segredo real em nenhum arquivo versionado; `.env` nunca sai do
  `.gitignore`.
- Commits diretos em `master` (sem branch/PR) — autorizado pelo usuário para
  este projeto de estudos.
- Mensagens de commit em conventional commits (`feat:`, `fix:`, `chore:`,
  `docs:`, `test:`) a partir do hook desta fase.

---

### Task 1: Commit inicial da fundação existente

**Files:**
- Nenhum arquivo novo — apenas `git add` do que já existe no working tree.

**Interfaces:**
- Não aplicável (tarefa de controle de versão, não de código).

- [ ] **Step 1: Conferir o que será commitado**

Rode:
```bash
git status --porcelain
```

Confirme que **não** aparece nenhum arquivo `.env` (só `.env.example`), nenhum
`node_modules/`, `dist/`, `.turbo/`, `coverage/` — o `.gitignore` já cobre
isso. Se algum desses aparecer, pare e investigue antes de continuar (não
adicione `.gitignore` novo, o existente já está correto — o problema seria
outro).

- [ ] **Step 2: Stage e commit**

```bash
git add -A
git status
```

Revise a lista de arquivos staged uma última vez. Depois:

```bash
git commit -m "$(cat <<'EOF'
chore: fundação do monorepo — workspaces, contracts, infra local, docs

pnpm workspaces + Turborepo, TypeScript strict, ESLint+Prettier;
packages/contracts com envelope, tópicos, escada de retry/DLT e matriz de
compensação; docker-compose com Kafka (KRaft), 5 Postgres, Jaeger,
Prometheus, Grafana, Mailhog; docs/PLAN.md, ADRs e módulos de aprendizado;
6 exemplos executáveis provando outbox, idempotência e retry/DLT contra
infra real.
EOF
)"
```

- [ ] **Step 3: Confirmar o commit**

```bash
git log --oneline -1
git status
```

Esperado: `git status` mostra working tree limpo, e o log mostra o commit
acima como único commit em `master`.

---

### Task 2: Husky + commitlint

**Files:**
- Modify: `package.json` (raiz — devDependencies + script `prepare`)
- Create: `commitlint.config.js`
- Create: `.husky/commit-msg`

**Interfaces:**
- Não produz API para outras tasks consumirem — é tooling de repositório.

- [ ] **Step 1: Instalar as dependências**

```bash
pnpm add -D husky @commitlint/cli @commitlint/config-conventional -w
```

O `-w` instala na raiz do workspace (root `package.json`), não num pacote.

- [ ] **Step 2: Criar o config do commitlint**

Crie `commitlint.config.js`:

```js
export default {
  extends: ['@commitlint/config-conventional'],
};
```

- [ ] **Step 3: Inicializar o Husky e o hook de commit-msg**

```bash
pnpm exec husky init
```

Isso cria `.husky/pre-commit` (apagando o conteúdo padrão depois) e adiciona
`"prepare": "husky"` ao `package.json` raiz. Depois substitua o conteúdo de
`.husky/pre-commit` por um lint dos arquivos staged e crie o hook de
commit-msg:

`.husky/pre-commit`:
```bash
pnpm exec turbo run lint typecheck
```

`.husky/commit-msg`:
```bash
pnpm exec commitlint --edit "$1"
```

Torne os hooks executáveis:
```bash
chmod +x .husky/pre-commit .husky/commit-msg
```

- [ ] **Step 4: Verificar que o hook rejeita mensagem fora do padrão**

```bash
git commit --allow-empty -m "mensagem qualquer sem tipo convencional"
```

Esperado: FALHA, com o commitlint reclamando que a mensagem não segue
`type(scope): subject`.

- [ ] **Step 5: Verificar que o hook aceita mensagem conventional**

```bash
git commit --allow-empty -m "chore: valida hook de commit-msg"
git log --oneline -1
git reset --soft HEAD~1
```

Esperado: o commit vazio passa no hook; depois desfazemos com
`reset --soft` para não deixar um commit de teste na história (o `--soft`
mantém as mudanças da Task 1/2 no working tree, só remove o commit vazio).

- [ ] **Step 6: Commit**

```bash
git add package.json pnpm-lock.yaml commitlint.config.js .husky
git commit -m "chore: adiciona husky + commitlint (conventional commits)"
```

---

### Task 3: Verificar que a infra local sobe de ponta a ponta

**Files:**
- Nenhum arquivo novo — tarefa de verificação executável.

**Interfaces:**
- Confirma que `deploy/docker/docker-compose.yml` e
  `deploy/docker/scripts/create-topics.mjs` (já existentes) funcionam contra
  Docker real. Esta é a pré-condição de todas as fases seguintes: nenhuma
  delas funciona sem Kafka + Postgres no ar.

- [ ] **Step 1: Build do pacote contracts (o script de tópicos importa `dist/`)**

```bash
pnpm --filter @ecommerce/contracts build
```

Esperado: `packages/contracts/dist/index.js` existe e o comando termina sem
erro.

- [ ] **Step 2: Subir a infra**

```bash
pnpm infra:up
```

Aguarde os healthchecks. Confirme com:

```bash
docker compose -f deploy/docker/docker-compose.yml ps
```

Esperado: todos os serviços com status `healthy` ou `running` (Kafka,
kafka-ui, os 5 Postgres, Jaeger, Prometheus, Grafana, Mailhog, Structurizr).
Se algum Postgres falhar o bind de porta silenciosamente (problema conhecido
do README — portas 154xx podem colidir), rode
`docker compose -f deploy/docker/docker-compose.yml logs <serviço>` para
diagnosticar antes de prosseguir.

- [ ] **Step 3: Criar a topologia de tópicos**

```bash
pnpm topics:create
```

Esperado: saída `Topologia pronta: N tópicos declarados (4 de negócio, ...)`
sem erro de conexão. Confirme visualmente em http://localhost:8080
(Kafka UI) que os 4 tópicos de negócio (`ecommerce.orders.v1`,
`ecommerce.payments.v1`, `ecommerce.inventory.v1`, `ecommerce.shipping.v1`)
aparecem, junto com os tópicos de retry/DLT por consumer group.

- [ ] **Step 4: Rodar a suite de testes existente**

```bash
pnpm test
```

Esperado: todos os testes de `packages/contracts` passam (não depende da
infra, é unitário).

- [ ] **Step 5: Rodar os exemplos executáveis contra a infra real**

```bash
cd examples && pnpm ex todos && cd ..
```

Esperado: os 6 exemplos rodam e terminam sem lançar exceção — eles já
provam outbox, idempotência, retry/DLT e replay contra Kafka/Postgres reais.
Se qualquer exemplo falhar, a infra não está pronta para as fases seguintes;
não prossiga para a Fase 2 sem isso passando.

- [ ] **Step 6: Nada para commitar**

Esta task não altera arquivos versionados — é confirmação de ambiente. Se
tudo passou, marque os itens acima como concluídos e prossiga para a Fase 2.
