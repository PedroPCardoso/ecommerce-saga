# Fase 10 — Kubernetes / Minikube Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A saga inteira rodando de verdade num cluster Kubernetes local (Minikube): Strimzi para o Kafka, CloudNativePG para o Postgres, os 6 serviços (5 de negócio + saga-observer) com probes corretas e `NetworkPolicy` deny-by-default, acessível via Ingress, sobrevivendo a matar um pod no meio da saga sem perder mensagem.

**Architecture:** Manifests YAML crus em `deploy/k8s/base/` (10a de `docs/PLAN.md`) — sem Helm ainda. Um `Namespace` só (`ecommerce-saga`), um `Cluster` CloudNativePG com 5 databases lógicos (não 5 clusters — RAM local é escassa, dívida documentada), um `Kafka` CR do Strimzi em modo KRaft de nó único, `KafkaTopic` CRs gerados por SCRIPT a partir de `packages/contracts` (56 tópicos — nunca escritos à mão), e um `Deployment`+`Service`+`ConfigMap`+`NetworkPolicy` por serviço, todos parametrizados pelo MESMO padrão (ver Task 3).

**Tech Stack:** Minikube (driver Docker), `kubectl`, Strimzi Kafka Operator, CloudNativePG Operator, KEDA.

## Global Constraints — LEIA ANTES DE COMEÇAR

- **Escopo e ambiente REDUZIDOS deliberadamente**, por uma restrição real de recursos medida no início desta fase: Docker Desktop nesta máquina está com a VM limitada a **7.8GB de RAM / 8 CPUs**, e o `docker compose` da saga (Kafka, 5 Postgres, Jaeger, Prometheus, Grafana, Mailhog, Structurizr) já usa uma fatia disso quando está de pé. Rodar Minikube com `--memory=8192` (o valor que `docs/PLAN.md` original pede) simultaneamente ao compose NÃO cabe. Por isso:
  1. **Pare o `docker compose` inteiro ANTES de iniciar o Minikube** (`docker compose -f deploy/docker/docker-compose.yml down` — sem `-v`, não apague os volumes). As duas infras não coexistem nesta máquina.
  2. Use `minikube start --driver=docker --memory=6144 --cpus=4 --disk-size=30g` (não 8192/40g).
  3. **Helm (10b do `docs/PLAN.md`) e o teste de carga de 5k pedidos escalando KEDA de 2→8 réplicas ficam FORA do escopo desta rodada** — documentados como próximo passo no README, não implementados. Os manifests crus (10a) são o critério mínimo de "pronto" desta fase: eles PRECISAM subir a saga de ponta a ponta de verdade, com Ingress e resiliência a pod kill provadas ao vivo.
  4. Ao final desta fase, `docker compose -f deploy/docker/docker-compose.yml up -d` volta a ser a forma padrão de rodar o sistema no dia a dia — o cluster Kubernetes fica em pé só durante a verificação (Task 8), depois `minikube stop` (não `delete`, para não perder o trabalho se quiser voltar).
- Instale as ferramentas que faltam ANTES de mais nada: `brew install minikube helm` (nenhum dos dois precisa de sudo).
- Segredo NUNCA em YAML plano commitado — mesmo em dev local, use `kubectl create secret` diretamente (documentado no plano, não commitado) para os valores reais; o que vai pro git é só o `Secret` com `stringData` de valores FICTÍCIOS iguais ao `.env.example` (o mesmo princípio que já rege `.env.example` no resto do repo).
- Branch de trabalho: `feat/fases-6-11-compensacao`.

---

### Task 1: `/health/ready` e `/health/startup` em todos os serviços

K8s probes exigem 3 endpoints distintos (`docs/PLAN.md`: liveness raso, readiness checa dependência, startup dá tempo à migration) — hoje só `/health/live` existe.

**Files (× 5 serviços com banco: order/payment/inventory/shipping/notification):**
- Modify: `apps/<serviço>/src/health/health.controller.ts`
- Test: `apps/<serviço>/test/health.integration.spec.ts` (crie um por serviço, ou um só se preferir testar num serviço representativo e replicar — mas rode em TODOS)

**Escopo consciente:** `/ready` e `/startup` checam só Postgres (`SELECT 1`) — NÃO checam conectividade Kafka (checar Kafka por request HTTP é caro/instável; o `KafkaConsumerRuntime` já se recupera sozinho de desconexão via retry do próprio kafkajs). Documentado, não uma omissão silenciosa.

- [ ] **Step 1: Escreva o teste (num serviço, ex. order-service) — falhando**

```typescript
import { describe, expect, it } from 'vitest';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { AppModule } from '../src/app.module.js';

describe('HealthController /ready e /startup', () => {
  it('GET /health/ready devolve 200 quando o Postgres responde', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    const app: INestApplication = moduleRef.createNestApplication();
    await app.init();

    await request(app.getHttpServer()).get('/health/ready').expect(200, { status: 'ok' });
    await request(app.getHttpServer()).get('/health/startup').expect(200, { status: 'ok' });

    await app.close();
  });
});
```

- [ ] **Step 2: Implemente nos 5 serviços**

Em cada `apps/<serviço>/src/health/health.controller.ts`, troque pelo conteúdo (injetando `PrismaService` — import de valor, mesmo motivo de sempre neste código: DI precisa da classe real, não `import type`):

```typescript
import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../infrastructure/prisma.service.js';

@Controller('health')
export class HealthController {
  constructor(private readonly prisma: PrismaService) {}

  @Get('live')
  live(): { status: string } {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready(): Promise<{ status: string }> {
    try {
      await this.prisma.client.$queryRaw`SELECT 1`;
      return { status: 'ok' };
    } catch {
      throw new ServiceUnavailableException({ status: 'error' });
    }
  }

  @Get('startup')
  async startup(): Promise<{ status: string }> {
    return this.ready();
  }
}
```

Se `saga-observer` (Fase 7b) já existir nesta branch quando você chegar aqui, o dele NÃO tem `PrismaService` — `ready`/`startup` lá só devolvem `{ status: 'ok' }` direto, sem checar nada (não tem dependência própria para checar).

- [ ] **Step 3: Rode a suíte de cada serviço, lint, typecheck**

Run: `pnpm exec turbo run test lint typecheck`
Expected: tudo verde.

- [ ] **Step 4: Commit**

```bash
git add apps/*/src/health apps/*/test/health.integration.spec.ts
git commit -m "feat: adiciona /health/ready e /health/startup (pré-requisito das probes K8s, Fase 10)"
```

---

### Task 2: Ferramentas + Minikube de pé

- [ ] **Step 1: Instale e pare o compose**

```bash
brew install minikube helm
docker compose -f deploy/docker/docker-compose.yml down
```

- [ ] **Step 2: Suba o Minikube**

```bash
minikube start --driver=docker --memory=6144 --cpus=4 --disk-size=30g
minikube addons enable ingress
minikube addons enable metrics-server
kubectl create namespace ecommerce-saga
kubectl config set-context --current --namespace=ecommerce-saga
```

Expected: `kubectl get nodes` mostra 1 nó `Ready`.

- [ ] **Step 3: Instale Strimzi, CloudNativePG e KEDA via Helm**

```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm install strimzi-operator strimzi/strimzi-kafka-operator --namespace ecommerce-saga
helm install cnpg-operator cnpg/cloudnative-pg --namespace ecommerce-saga
helm install keda kedacore/keda --namespace ecommerce-saga
```

Expected: `kubectl get pods` mostra os 3 operators em `Running` dentro de ~2min.

---

### Task 3: Kafka (Strimzi) + Postgres (CloudNativePG)

**Files:**
- Create: `deploy/k8s/base/kafka/kafka-cluster.yaml`
- Create: `deploy/k8s/base/kafka/generate-topics.mjs`
- Create: `deploy/k8s/base/postgres/cluster.yaml`
- Create: `deploy/k8s/base/postgres/init-databases-job.yaml`

- [ ] **Step 1: `Kafka` CR (KRaft, nó único)**

Crie `deploy/k8s/base/kafka/kafka-cluster.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: saga-kafka
  namespace: ecommerce-saga
  annotations:
    strimzi.io/kraft: enabled
    strimzi.io/node-pools: enabled
spec:
  kafka:
    version: 3.9.0
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
      auto.create.topics.enable: 'false'
    resources:
      requests: { memory: 1Gi, cpu: '500m' }
      limits: { memory: 1536Mi, cpu: '1' }
  entityOperator:
    topicOperator: {}
    userOperator: {}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: saga-pool
  namespace: ecommerce-saga
  labels:
    strimzi.io/cluster: saga-kafka
spec:
  replicas: 1
  roles: [controller, broker]
  storage:
    type: persistent-claim
    size: 5Gi
    deleteClaim: false
```

Aplique: `kubectl apply -f deploy/k8s/base/kafka/kafka-cluster.yaml` e espere: `kubectl wait kafka/saga-kafka --for=condition=Ready --timeout=300s`.

- [ ] **Step 2: Gere os 56 `KafkaTopic` CRs a partir de `packages/contracts`, não à mão**

Crie `deploy/k8s/base/kafka/generate-topics.mjs`:

```javascript
#!/usr/bin/env node
import { allTopics } from '@ecommerce/contracts';
import { writeFileSync } from 'node:fs';

const manifests = allTopics()
  .map(
    (topic) => `---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: ${topic.replace(/[^a-z0-9-]/gi, '-').toLowerCase()}
  namespace: ecommerce-saga
  labels:
    strimzi.io/cluster: saga-kafka
spec:
  topicName: ${topic}
  partitions: 3
  replicas: 1
`,
  )
  .join('');

writeFileSync(new URL('./topics-generated.yaml', import.meta.url), manifests);
console.log(`Gerados ${allTopics().length} KafkaTopic CRs em topics-generated.yaml`);
```

(`KafkaTopic.metadata.name` tem restrição de charset do Kubernetes — não pode ter `.`; por isso o `.replace` troca por `-`, mas `spec.topicName` mantém o nome real do tópico, com pontos, que é o que o Kafka de fato usa.)

Rode: `node --experimental-vm-modules deploy/k8s/base/kafka/generate-topics.mjs` (rode de dentro da raiz do monorepo, com `@ecommerce/contracts` já buildado — `pnpm --filter @ecommerce/contracts build` antes se necessário) e depois `kubectl apply -f deploy/k8s/base/kafka/topics-generated.yaml`.

- [ ] **Step 3: `Cluster` CloudNativePG com 5 databases lógicos**

Crie `deploy/k8s/base/postgres/cluster.yaml`:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: saga-postgres
  namespace: ecommerce-saga
spec:
  instances: 1
  imageName: ghcr.io/cloudnative-pg/postgresql:17
  storage:
    size: 5Gi
  resources:
    requests: { memory: 512Mi, cpu: '250m' }
    limits: { memory: 1Gi, cpu: '500m' }
  bootstrap:
    initdb:
      database: order_db
      owner: order_svc
      secret:
        name: saga-postgres-order-svc-credentials
```

**Dívida documentada** (o próprio `docs/PLAN.md` antecipa isto: "se a RAM apertar: um cluster com 5 databases"): este `Cluster` só cria automaticamente `order_db`. Os outros 4 databases (`payment_db`, `inventory_db`, `shipping_db`, `notification_db`) e seus respectivos usuários são criados por um Job de inicialização — Crie `deploy/k8s/base/postgres/init-databases-job.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: init-additional-databases
  namespace: ecommerce-saga
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: init-db
          image: postgres:17-alpine
          env:
            - name: PGHOST
              value: saga-postgres-rw
            - name: PGUSER
              valueFrom:
                secretKeyRef: { name: saga-postgres-order-svc-credentials, key: username }
            - name: PGPASSWORD
              valueFrom:
                secretKeyRef: { name: saga-postgres-order-svc-credentials, key: password }
          command:
            - sh
            - -c
            - |
              for pair in "payment_db:payment_svc" "inventory_db:inventory_svc" "shipping_db:shipping_svc" "notification_db:notification_svc"; do
                db="${pair%%:*}"; user="${pair##*:}";
                psql -d postgres -c "SELECT 1 FROM pg_database WHERE datname = '${db}'" | grep -q 1 || \
                  psql -d postgres -c "CREATE DATABASE ${db}";
                psql -d postgres -c "SELECT 1 FROM pg_roles WHERE rolname = '${user}'" | grep -q 1 || \
                  psql -d postgres -c "CREATE ROLE ${user} LOGIN PASSWORD 'changeme'";
                psql -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE ${db} TO ${user}";
              done
```

Este Job usa a mesma senha fictícia `changeme` do resto do projeto local — troque por um Secret real antes de qualquer uso além de estudo/dev.

Aplique os dois e espere o cluster ficar pronto: `kubectl wait cluster/saga-postgres --for=condition=Ready --timeout=300s`, depois `kubectl apply -f deploy/k8s/base/postgres/init-databases-job.yaml` e `kubectl wait job/init-additional-databases --for=condition=Complete --timeout=120s`.

---

### Task 4: Manifests por serviço (Deployment, Service, ConfigMap, Secret, NetworkPolicy, PDB)

**Files:**
- Create: `deploy/k8s/base/services/order-service.yaml` (worked example completo abaixo)
- Create: `deploy/k8s/base/services/payment-service.yaml`
- Create: `deploy/k8s/base/services/inventory-service.yaml`
- Create: `deploy/k8s/base/services/shipping-service.yaml`
- Create: `deploy/k8s/base/services/notification-service.yaml`
- Create: `deploy/k8s/base/services/migration-job-template.yaml` (comentário — ver Step 3)

- [ ] **Step 1: Worked example completo — `order-service.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata: { name: order-service, namespace: ecommerce-saga }
---
apiVersion: v1
kind: ConfigMap
metadata: { name: order-service-config, namespace: ecommerce-saga }
data:
  NODE_ENV: production
  ORDER_SERVICE_PORT: '3000'
  KAFKA_BROKERS: saga-kafka-kafka-bootstrap:9092
  KAFKA_CLIENT_ID_PREFIX: ecommerce
  JWT_ISSUER: ecommerce-local
  SAGA_TIMEOUT_THRESHOLD_MS: '300000'
  SAGA_TIMEOUT_SWEEP_INTERVAL_MS: '30000'
---
apiVersion: v1
kind: Secret
metadata: { name: order-service-secrets, namespace: ecommerce-saga }
type: Opaque
stringData:
  # Valores FICTÍCIOS, iguais ao .env.example — troque antes de qualquer uso real.
  ORDER_DATABASE_URL: postgresql://order_svc:changeme@saga-postgres-rw:5432/order_db?schema=public
  JWT_SECRET: dev-only-not-a-real-secret-change-me
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: order-service, namespace: ecommerce-saga }
spec:
  replicas: 2
  selector: { matchLabels: { app: order-service } }
  strategy:
    type: RollingUpdate
    rollingUpdate: { maxUnavailable: 0, maxSurge: 1 }
  template:
    metadata: { labels: { app: order-service } }
    spec:
      serviceAccountName: order-service
      terminationGracePeriodSeconds: 45
      containers:
        - name: order-service
          image: ecommerce-saga-order-service:latest
          imagePullPolicy: IfNotPresent
          ports: [{ containerPort: 3000 }]
          envFrom:
            - configMapRef: { name: order-service-config }
            - secretRef: { name: order-service-secrets }
          lifecycle:
            preStop:
              exec: { command: ['sh', '-c', 'sleep 5'] }
          startupProbe:
            httpGet: { path: /health/startup, port: 3000 }
            failureThreshold: 30
            periodSeconds: 2
          readinessProbe:
            httpGet: { path: /health/ready, port: 3000 }
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /health/live, port: 3000 }
            periodSeconds: 10
          resources:
            requests: { memory: 128Mi, cpu: '100m' }
            limits: { memory: 256Mi, cpu: '250m' }
---
apiVersion: v1
kind: Service
metadata: { name: order-service, namespace: ecommerce-saga }
spec:
  selector: { app: order-service }
  ports: [{ port: 3000, targetPort: 3000 }]
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: { name: order-service, namespace: ecommerce-saga }
spec:
  minAvailable: 1
  selector: { matchLabels: { app: order-service } }
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: order-service, namespace: ecommerce-saga }
spec:
  podSelector: { matchLabels: { app: order-service } }
  policyTypes: [Ingress, Egress]
  ingress:
    - from: [] # Ingress controller entra por fora do namespace — refine com um label de namespace do ingress-nginx se quiser trancar mais
      ports: [{ port: 3000 }]
  egress:
    - to: [] # Kafka e Postgres já são outros pods do MESMO namespace — sem seletor, permite tudo dentro; troque por matchLabels do saga-kafka/saga-postgres para trancar mais
```

- [ ] **Step 2: Repita para os outros 4 serviços**

Mesma estrutura EXATA, trocando: nome (`payment-service`, etc.), porta (3001/3002/3003/3004), `*_DATABASE_URL` (usuário/banco correspondente), e — SEM `Ingress` (só o Order Service recebe tráfego externo, os outros só o Kafka aciona). `NetworkPolicy` de cada um só precisa liberar egress (não tem ingress de fora, exceto do próprio Kafka/Postgres — mas Kafka não "entra" no pod, o pod é quem PUXA do Kafka, então `ingress: []` vazio — sem regra de entrada nenhuma — é o correto para os 4 que não são Order Service).

- [ ] **Step 3: `Job` de migration, com hook `pre-upgrade`**

Um `Job` por serviço rodando `prisma migrate deploy` ANTES do Deployment escalar (nunca num initContainer — 2 réplicas rodariam a migration em paralelo, exatamente a armadilha que `docs/PLAN.md` avisa). Crie um Job por serviço (mesma imagem do Deployment, comando diferente):

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: order-service-migrate
  namespace: ecommerce-saga
  annotations: { "helm.sh/hook": pre-upgrade }
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: ecommerce-saga-order-service:latest
          command: ['pnpm', '--filter', '@ecommerce/order-service', 'exec', 'prisma', 'migrate', 'deploy']
          envFrom:
            - configMapRef: { name: order-service-config }
            - secretRef: { name: order-service-secrets }
```

A anotação `helm.sh/hook` só faz sentido de verdade com Helm (Task 10b, fora de escopo) — sem Helm, aplique este Job MANUALMENTE antes do Deployment: `kubectl apply -f <job>.yaml && kubectl wait job/order-service-migrate --for=condition=Complete --timeout=120s && kubectl apply -f order-service.yaml`.

- [ ] **Step 4: `Ingress` só para o Order Service**

Crie `deploy/k8s/base/ingress.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: saga-ingress
  namespace: ecommerce-saga
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /$2
spec:
  ingressClassName: nginx
  rules:
    - http:
        paths:
          - path: /orders(/|$)(.*)
            pathType: ImplementationSpecific
            backend:
              service: { name: order-service, port: { number: 3000 } }
          - path: /health(/|$)(.*)
            pathType: ImplementationSpecific
            backend:
              service: { name: order-service, port: { number: 3000 } }
```

---

### Task 5: `Secret` de imagem — build das 5 imagens DENTRO do Minikube

Minikube com driver Docker tem seu PRÓPRIO daemon Docker, separado do daemon do host — as imagens já buildadas em `docker compose build` NÃO existem lá dentro.

- [ ] **Step 1: Aponte o shell para o Docker do Minikube e rebuilde as 5 imagens**

```bash
eval $(minikube docker-env)
docker compose -f deploy/docker/docker-compose.yml build order-service payment-service inventory-service shipping-service notification-service
eval $(minikube docker-env -u)  # desfaz — volta pro Docker do host depois
```

`imagePullPolicy: IfNotPresent` nos manifests já garante que o kubelet usa a imagem local em vez de tentar puxar de um registry.

---

### Task 6: KEDA — autoscaling por lag do consumidor

**Files:**
- Create: `deploy/k8s/base/keda/order-service-scaledobject.yaml`

- [ ] **Step 1: Um `ScaledObject` de exemplo (Order Service, grupo `order-projection`)**

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: order-service-scaler
  namespace: ecommerce-saga
spec:
  scaleTargetRef: { name: order-service }
  minReplicaCount: 2
  maxReplicaCount: 8
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: saga-kafka-kafka-bootstrap:9092
        consumerGroup: order-projection
        topic: ecommerce.payments.v1
        lagThreshold: '50'
        allowIdleConsumers: 'true'
```

**Fora de escopo desta rodada** (documentado no Global Constraints): o teste de carga de 5k pedidos provando 2→8 réplicas de verdade. Aplicar o `ScaledObject` e confirmar `kubectl get scaledobject` sem erro é o critério mínimo aqui — a prova sob carga real fica para quando houver orçamento de tempo/máquina maior.

---

### Task 7: Aplique tudo, na ordem certa

```bash
kubectl apply -f deploy/k8s/base/services/order-service.yaml
# ... aplique os Jobs de migration ANTES de cada Deployment escalar, um serviço de cada vez
kubectl apply -f deploy/k8s/base/ingress.yaml
kubectl apply -f deploy/k8s/base/keda/
```

Expected: `kubectl get pods` mostra todos os pods `Running` (2/2 para cada Deployment).

---

### Task 8: Verificação — saga real via Ingress + resiliência a pod kill

- [ ] **Step 1: Ache o IP do Minikube e crie um pedido via Ingress**

```bash
minikube ip  # ex.: 192.168.49.2
TOKEN=$(...)  # mesmo processo de sempre
curl -X POST http://$(minikube ip)/orders -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Idempotency-Key: $(uuidgen)" -d '{...}'
```

Expected: `201`, e o pedido chega a `CONFIRMED` (`GET /orders/:id` via o mesmo Ingress) do mesmo jeito que via docker-compose.

- [ ] **Step 2: Mate um pod no meio de uma saga e confirme que nada se perde**

```bash
# crie um pedido, e ANTES dele terminar, mate o pod do inventory-service:
kubectl delete pod -l app=inventory-service --wait=false
```

Expected: o pod é recriado automaticamente (Deployment com 2 réplicas — a outra segue atendendo); o pedido ainda chega a `CONFIRMED` (a mensagem não commitada pelo pod morto é reprocessada pela réplica viva ou pelo pod novo, via o mesmo mecanismo de commit manual + retry que já prova isso no ambiente Docker Compose).

- [ ] **Step 3: Documente o resultado e pare o cluster**

Anote no README (Task final desta fase) os comandos usados e o resultado observado. Depois:

```bash
minikube stop  # NÃO delete — dá pra retomar depois sem reconstruir tudo
docker compose -f deploy/docker/docker-compose.yml up -d  # volta o ambiente padrão do dia a dia
```

- [ ] **Step 4: Commit**

```bash
git add deploy/k8s
git commit -m "feat(k8s): manifests base (Strimzi Kafka, CloudNativePG, 5 serviços, Ingress, KEDA) — Fase 10a"
```

- [ ] **Step 5: Atualize o README**

Adicione uma seção curta "Kubernetes (Fase 10)" ao README com: como subir (`minikube start` + os comandos deste plano), o que foi PROVADO ao vivo (saga via Ingress, pod kill sem perda), e o que ficou de fora deliberadamente (Helm/10b, teste de carga KEDA 5k pedidos) — não deixe isso implícito em nenhum commit message só, tem que estar visível pra quem abrir o repo.

Este plano termina aqui. Próximo: Fase 11 (versão orquestrada, opcional).
