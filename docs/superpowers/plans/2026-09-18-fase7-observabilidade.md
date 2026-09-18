# Fase 7 — Observabilidade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Um `orderId` aberto no Jaeger mostra a saga inteira atravessando os 5 serviços num único trace; Prometheus expõe `saga_duration_seconds`, `saga_compensations_total`, `dlq_messages_total`, `outbox_lag_seconds`, `kafka_consumer_lag`; Grafana tem um dashboard provisionado com esses painéis + alertas (lag > 1000, DLT > 0, taxa de compensação > 5%).

**Architecture:** Rastreamento **manual** (não auto-instrumentação): `packages/observability` registra um `NodeTracerProvider` global exportando para o Jaeger via OTLP HTTP, e o contexto do trace atravessa o Kafka pelo header `traceparent` (W3C, já reservado em `packages/contracts` `TRACE_HEADERS` — nunca usado até agora) via `propagation.inject`/`propagation.extract` do `@opentelemetry/api`, injetado em `EventProducer.publish` e extraído em `KafkaConsumerRuntime` antes de chamar o handler. Decisão deliberada: auto-instrumentação Node sob ESM exige hooks de loader (`--import`) e patching de módulo que são frágeis e dependem de versão exata de cada instrumentação de terceiro — propagação manual do `traceparent` entrega o requisito central ("ver a saga inteira num trace só") com muito menos risco, ao custo de não ganhar spans automáticos de `pg`/`express` de graça (aceito, documentado).

**Tech Stack:** `@opentelemetry/api` + `@opentelemetry/sdk-trace-node` + `@opentelemetry/exporter-trace-otlp-http`, `prom-client`, `pino`.

## Global Constraints

- Nunca coloque `correlationId`/`orderId`/dado de negócio em nome de métrica ou label de alta cardinalidade (Prometheus explode com cardinalidade alta) — labels de métrica são só enums fechados (ex.: `outcome: 'confirmed'|'cancelled'`, `compensationType`, `topic`, `consumerGroup`).
- `initTracing()` precisa rodar ANTES de qualquer span ser criado, mas NÃO precisa rodar antes de outros módulos serem importados (não fazemos auto-instrumentação/monkey-patch) — chame no início da função `bootstrap()` de cada `main.ts`, não via `--import`/loader.
- Escopo explícito, documentado, desta fase: SÓ o startup/shutdown de cada `main.ts` migra para o logger estruturado (`pino`) com `correlationId`. Os `console.warn`/`console.error` já existentes em handlers e no roteamento de retry/DLT (`packages/kafka`) permanecem como estão — migrá-los todos é um projeto à parte, fora do escopo desta fase (registre isto no README ao final, não deixe implícito).
- Branch de trabalho: `feat/fases-6-11-compensacao`. Continue nela.
- `pnpm --filter <pacote> test` verde a cada tarefa antes de prosseguir.

---

### Task 1: `packages/observability` — tracing, métricas e logger compartilhados

**Files:**
- Create: `packages/observability/package.json`
- Create: `packages/observability/tsconfig.json`
- Create: `packages/observability/src/tracing.ts`
- Create: `packages/observability/src/metrics.ts`
- Create: `packages/observability/src/logger.ts`
- Create: `packages/observability/src/metrics.module.ts`
- Create: `packages/observability/src/index.ts`
- Test: `packages/observability/test/metrics.spec.ts`
- Test: `packages/observability/test/logger.spec.ts`

**Interfaces:**
- Produces: `initTracing(serviceName: string): void`; `metricsRegistry: Registry` (prom-client); `sagaDurationSeconds`, `sagaCompensationsTotal`, `dlqMessagesTotal`, `outboxLagSeconds`, `kafkaConsumerLag` (métricas prontas para uso); `createLogger(serviceName: string)` (pino); `ObservabilityModule` (NestJS, expõe `GET /metrics`).

- [ ] **Step 1: `package.json` e `tsconfig.json`**

Crie `packages/observability/package.json` (siga exatamente o padrão de `packages/kafka/package.json` — mesma estrutura de `exports`/`main`/`types`/`files`/`scripts`), com estas dependências:

```json
{
  "name": "@ecommerce/observability",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "default": "./dist/index.js"
    }
  },
  "files": ["dist"],
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "lint": "eslint src",
    "test": "vitest run"
  },
  "dependencies": {
    "@nestjs/common": "^11.0.1",
    "@opentelemetry/api": "^1.9.0",
    "@opentelemetry/exporter-trace-otlp-http": "^0.57.0",
    "@opentelemetry/resources": "^1.30.0",
    "@opentelemetry/sdk-trace-node": "^1.30.0",
    "@opentelemetry/semantic-conventions": "^1.28.0",
    "pino": "^9.6.0",
    "prom-client": "^15.1.3"
  },
  "devDependencies": {
    "typescript": "^5.7.2",
    "vitest": "^2.1.8"
  }
}
```

Confira as versões de `@nestjs/common`/`typescript`/`vitest` num serviço já existente (ex.: `apps/order-service/package.json`) e alinhe se estiverem diferentes das acima. As versões de pacotes `@opentelemetry/*` acima são as mais recentes conhecidas no momento em que este plano foi escrito — se `pnpm install` resolver para uma versão diferente por causa de faixa de compatibilidade, tudo bem, não trave nisso.

Crie `packages/observability/tsconfig.json` copiando `packages/kafka/tsconfig.json`.

- [ ] **Step 2: Escreva os testes (falhando)**

Crie `packages/observability/test/metrics.spec.ts`:

```typescript
import { describe, expect, it } from 'vitest';
import {
  dlqMessagesTotal,
  kafkaConsumerLag,
  metricsRegistry,
  outboxLagSeconds,
  sagaCompensationsTotal,
  sagaDurationSeconds,
} from '../src/metrics.js';

describe('métricas de negócio', () => {
  it('saga_duration_seconds aparece no output do registry depois de observar um valor', async () => {
    sagaDurationSeconds.observe({ outcome: 'confirmed' }, 12.5);
    const output = await metricsRegistry.metrics();
    expect(output).toContain('saga_duration_seconds');
    expect(output).toContain('outcome="confirmed"');
  });

  it('saga_compensations_total incrementa por tipo de compensação', async () => {
    sagaCompensationsTotal.inc({ compensationType: 'PAYMENT_REFUNDED' });
    const output = await metricsRegistry.metrics();
    expect(output).toContain('saga_compensations_total');
    expect(output).toContain('compensationType="PAYMENT_REFUNDED"');
  });

  it('dlq_messages_total incrementa por tópico e consumer group', async () => {
    dlqMessagesTotal.inc({ topic: 'ecommerce.payments.v1', consumerGroup: 'inventory-service' });
    const output = await metricsRegistry.metrics();
    expect(output).toContain('dlq_messages_total');
  });

  it('outbox_lag_seconds e kafka_consumer_lag aceitam .set()', async () => {
    outboxLagSeconds.set({ service: 'order-service' }, 0.42);
    kafkaConsumerLag.set({ group: 'payment-service', topic: 'ecommerce.orders.v1', partition: '0' }, 3);
    const output = await metricsRegistry.metrics();
    expect(output).toContain('outbox_lag_seconds');
    expect(output).toContain('kafka_consumer_lag');
  });
});
```

Crie `packages/observability/test/logger.spec.ts`:

```typescript
import { describe, expect, it } from 'vitest';
import { createLogger } from '../src/logger.js';

describe('createLogger', () => {
  it('cria um logger pino com o nome do serviço no binding base', () => {
    const logger = createLogger('order-service');
    expect(logger.bindings()).toEqual({ service: 'order-service' });
  });

  it('.child({ correlationId }) propaga o campo em toda linha subsequente', () => {
    const logger = createLogger('order-service').child({ correlationId: 'abc-123' });
    expect(logger.bindings()).toMatchObject({ correlationId: 'abc-123', service: 'order-service' });
  });
});
```

- [ ] **Step 2: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/observability test`
Expected: FAIL — pacote nem tem `node_modules` ainda. Rode `pnpm install` na raiz do monorepo primeiro.

- [ ] **Step 3: Implemente `metrics.ts`**

Crie `packages/observability/src/metrics.ts`:

```typescript
import { Counter, Gauge, Histogram, Registry } from 'prom-client';

/**
 * UM registry compartilhado por processo — cada serviço expõe `GET /metrics`
 * lendo dele (ver `metrics.module.ts`). Nomes e labels aqui são o contrato
 * público consumido pelo `deploy/docker/prometheus/prometheus.yml` e pelo
 * dashboard do Grafana (Task 6) — não renomeie sem atualizar os dois.
 */
export const metricsRegistry = new Registry();

export const sagaDurationSeconds = new Histogram({
  name: 'saga_duration_seconds',
  help: 'Tempo entre a criação do pedido e ele chegar a um estado terminal (CONFIRMED/CANCELLED).',
  labelNames: ['outcome'] as const,
  buckets: [0.5, 1, 2, 5, 10, 30, 60, 120, 300],
  registers: [metricsRegistry],
});

export const sagaCompensationsTotal = new Counter({
  name: 'saga_compensations_total',
  help: 'Quantas compensações (payment.refunded/stock.released) foram aplicadas, por tipo.',
  labelNames: ['compensationType'] as const,
  registers: [metricsRegistry],
});

export const dlqMessagesTotal = new Counter({
  name: 'dlq_messages_total',
  help: 'Mensagens desviadas para a DLT, por tópico de origem e consumer group.',
  labelNames: ['topic', 'consumerGroup'] as const,
  registers: [metricsRegistry],
});

export const outboxLagSeconds = new Gauge({
  name: 'outbox_lag_seconds',
  help: 'Idade (segundos) da linha mais antiga ainda não publicada na tabela outbox, por serviço.',
  labelNames: ['service'] as const,
  registers: [metricsRegistry],
});

export const kafkaConsumerLag = new Gauge({
  name: 'kafka_consumer_lag',
  help: 'high watermark - offset commitado, por grupo/tópico/partição.',
  labelNames: ['group', 'topic', 'partition'] as const,
  registers: [metricsRegistry],
});
```

- [ ] **Step 4: Implemente `logger.ts`**

Crie `packages/observability/src/logger.ts`:

```typescript
import pino, { type Logger } from 'pino';

/**
 * Um logger por serviço, com `service` no binding base — é o que permite
 * filtrar/agrupar log agregado (Loki, CloudWatch, etc.) por serviço sem
 * grep manual. `correlationId` entra via `.child({ correlationId })` no
 * ponto de uso (handler de request HTTP ou de mensagem Kafka), nunca aqui —
 * este logger é criado UMA vez no bootstrap do processo.
 */
export function createLogger(serviceName: string): Logger {
  return pino({ level: process.env.LOG_LEVEL ?? 'info' }).child({ service: serviceName });
}
```

- [ ] **Step 5: Implemente `tracing.ts`**

Crie `packages/observability/src/tracing.ts`:

```typescript
import { NodeTracerProvider, BatchSpanProcessor } from '@opentelemetry/sdk-trace-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

/**
 * Rastreamento MANUAL — sem auto-instrumentação (ver cabeçalho do plano
 * desta fase para o porquê). Registra um NodeTracerProvider global; depois
 * disto, qualquer `trace.getTracer(nome)` (de `@opentelemetry/api`, em
 * qualquer pacote) usa este provider. Chame uma vez, no início do
 * `bootstrap()` de cada `main.ts` — não precisa ser antes de outros
 * imports, porque não há módulo de terceiro sendo interceptado.
 */
export function initTracing(serviceName: string): void {
  const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'http://localhost:4318';
  const exporter = new OTLPTraceExporter({ url: `${endpoint}/v1/traces` });

  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: serviceName }),
    spanProcessors: [new BatchSpanProcessor(exporter)],
  });

  provider.register();
}
```

Se a versão instalada de `@opentelemetry/sdk-trace-node` não exportar `resourceFromAttributes` (API mudou entre versões — em algumas é `new Resource({...})` de `@opentelemetry/resources`), ajuste para a API real da versão resolvida pelo `pnpm install`; o objetivo é só anexar `service.name` ao resource, o nome exato da função/classe pode variar.

- [ ] **Step 6: Implemente `metrics.module.ts`**

Crie `packages/observability/src/metrics.module.ts`:

```typescript
import { Controller, Get, Header, Module } from '@nestjs/common';
import { metricsRegistry } from './metrics.js';

@Controller()
export class MetricsController {
  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  async metrics(): Promise<string> {
    return metricsRegistry.metrics();
  }
}

/** Importe em qualquer AppModule para ganhar `GET /metrics` de graça. */
@Module({
  controllers: [MetricsController],
})
export class ObservabilityModule {}
```

- [ ] **Step 7: `index.ts` e rodar os testes**

Crie `packages/observability/src/index.ts`:

```typescript
export * from './tracing.js';
export * from './metrics.js';
export * from './logger.js';
export * from './metrics.module.js';
```

Run: `pnpm --filter @ecommerce/observability test`
Expected: 6/6 PASS.

- [ ] **Step 8: Lint, typecheck, build, commit**

```bash
pnpm --filter @ecommerce/observability lint && pnpm --filter @ecommerce/observability typecheck && pnpm --filter @ecommerce/observability build
git add packages/observability pnpm-lock.yaml
git commit -m "feat(observability): pacote compartilhado de tracing, métricas e logger (Fase 7)"
```

---

### Task 2: `packages/kafka` — propaga `traceparent`, conta DLT, mede lag

**Files:**
- Modify: `packages/kafka/package.json` (adiciona `@ecommerce/observability` e `@opentelemetry/api` como dependências)
- Modify: `packages/kafka/src/producer.ts`
- Modify: `packages/kafka/src/consumer-runtime.ts`
- Test: `packages/kafka/test/trace-propagation.spec.ts`

**Interfaces:**
- Consumes: `dlqMessagesTotal`, `kafkaConsumerLag` de `@ecommerce/observability`.

- [ ] **Step 1: Adicione as dependências**

Em `packages/kafka/package.json`, adicione às `dependencies`:

```json
    "@ecommerce/observability": "workspace:*",
    "@opentelemetry/api": "^1.9.0",
```

Rode `pnpm install` na raiz depois de editar.

- [ ] **Step 2: Escreva o teste de propagação (falhando)**

Crie `packages/kafka/test/trace-propagation.spec.ts`:

```typescript
import { context, trace } from '@opentelemetry/api';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { EventProducer } from '../src/producer.js';

describe('propagação de traceparent', () => {
  let provider: NodeTracerProvider;

  beforeEach(() => {
    provider = new NodeTracerProvider();
    provider.register();
  });

  afterEach(async () => {
    await provider.shutdown();
  });

  it('EventProducer.publish injeta o header traceparent quando há um span ativo', async () => {
    const tracer = trace.getTracer('test');
    const span = tracer.startSpan('test-span');
    const activeContext = trace.setSpan(context.active(), span);

    const producer = new EventProducer({ brokers: ['localhost:1'], clientId: 'test' });
    // @ts-expect-error acessa o campo privado só para o teste poder inspecionar sem
    // precisar de um broker de verdade — publish() chama producer.send internamente.
    producer.producer = { send: vi.fn().mockResolvedValue(undefined) };

    await context.with(activeContext, async () => {
      await producer.publish('test-topic', {
        eventId: 'evt-1',
        eventType: 'test.event',
        eventVersion: 1,
        occurredAt: new Date().toISOString(),
        aggregateId: 'agg-1',
        aggregateType: 'test',
        correlationId: 'agg-1',
        causationId: 'evt-1',
        producer: 'test@0.0.0',
        payload: {},
      } as never);
    });

    span.end();

    // @ts-expect-error mesmo acesso ao mock acima
    const sendCall = producer.producer.send.mock.calls[0][0];
    expect(sendCall.messages[0].headers.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  });
});
```

- [ ] **Step 3: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/kafka test -- trace-propagation`
Expected: FAIL — `publish` ainda não injeta `traceparent`.

- [ ] **Step 4: Injete o contexto em `producer.ts`**

Em `packages/kafka/src/producer.ts`, adicione o import no topo:

```typescript
import { context, propagation } from '@opentelemetry/api';
```

No método `publish`, troque:

```typescript
  async publish(
    topic: string,
    envelope: UnknownEnvelope,
    headers: Record<string, string> = {},
  ): Promise<void> {
    this.assertConnected();
    await this.producer!.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{ key: envelope.aggregateId, value: JSON.stringify(envelope), headers }],
    });
  }
```

por:

```typescript
  async publish(
    topic: string,
    envelope: UnknownEnvelope,
    headers: Record<string, string> = {},
  ): Promise<void> {
    this.assertConnected();
    const tracedHeaders = { ...headers };
    // W3C traceparent (docs/PLAN.md, Fase 7): se houver um span ativo no momento da
    // publicação, o header carrega o trace ID adiante — é isto que faz um `orderId`
    // aberto no Jaeger mostrar a saga inteira como UM trace, não cinco desconexos.
    // Sem span ativo (ex.: fora de uma request HTTP ou de um handler de mensagem já
    // rastreado), injeta nada — não é erro, só não há o que propagar.
    propagation.inject(context.active(), tracedHeaders);
    await this.producer!.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{ key: envelope.aggregateId, value: JSON.stringify(envelope), headers: tracedHeaders }],
    });
  }
```

- [ ] **Step 5: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/kafka test -- trace-propagation`
Expected: PASS.

- [ ] **Step 6: Extraia o contexto em `consumer-runtime.ts` e conte mensagens na DLT**

Em `packages/kafka/src/consumer-runtime.ts`, adicione os imports:

```typescript
import { context, propagation, trace } from '@opentelemetry/api';
import { dlqMessagesTotal } from '@ecommerce/observability';
```

No método `parse` (ou logo depois de chamá-lo, em `processMainMessage`/`processRetryMessage`), o handler precisa rodar DENTRO do contexto extraído dos headers da mensagem. Troque a chamada do handler em AMBOS os métodos — em `processMainMessage`:

```typescript
    try {
      await this.handler({ envelope });
    } catch (error) {
```

por:

```typescript
    const extractedContext = propagation.extract(context.active(), this.headersToRecord(message.headers));
    try {
      await context.with(extractedContext, () => this.handler({ envelope }));
    } catch (error) {
```

Faça o MESMO em `processRetryMessage` (mesma troca, mesmas duas linhas). Adicione o método privado auxiliar `headersToRecord` na classe (headers do kafkajs vêm como `Record<string, Buffer | string | undefined>`, `propagation.extract` espera `Record<string, string>`):

```typescript
  private headersToRecord(headers: EachMessagePayload['message']['headers']): Record<string, string> {
    const record: Record<string, string> = {};
    for (const [key, value] of Object.entries(headers ?? {})) {
      if (value !== undefined) record[key] = value.toString();
    }
    return record;
  }
```

No método `sendToDlt`, adicione a métrica logo depois do `console.error` já existente:

```typescript
    dlqMessagesTotal.inc({ topic: sourceTopic, consumerGroup: this.groupId });
```

- [ ] **Step 7: Rode a suíte inteira do pacote**

Run: `pnpm --filter @ecommerce/kafka test`
Expected: TODOS os testes (os já existentes de retry/DLT + os 2 novos de tracing) PASS — em particular, `consumer-runtime.integration.spec.ts` (já existente) precisa continuar passando com o `context.with()` novo envolvendo o handler.

- [ ] **Step 8: Poller de `kafka_consumer_lag`**

No método `start()` de `KafkaConsumerRuntime`, depois de iniciar o consumidor principal (`main`), adicione um poller periódico usando o admin client do kafkajs. Adicione ao construtor/campos da classe:

```typescript
  private lagPollTimer: NodeJS.Timeout | null = null;
```

Ao final do método `start()` (depois do loop que inicia os consumidores de retry), adicione:

```typescript
    this.lagPollTimer = setInterval(() => {
      void this.pollConsumerLag();
    }, 15_000);
```

E adicione o método privado:

```typescript
  /** Atualiza kafka_consumer_lag = high watermark - offset commitado, por partição. */
  private async pollConsumerLag(): Promise<void> {
    const admin = this.kafka.admin();
    try {
      await admin.connect();
      for (const topic of this.sourceTopics) {
        const [committed, watermarks] = await Promise.all([
          admin.fetchOffsets({ groupId: this.groupId, topics: [topic] }),
          admin.fetchTopicOffsets(topic),
        ]);
        const committedByPartition = new Map(committed[0]?.partitions.map((p) => [p.partition, p.offset]) ?? []);
        for (const wm of watermarks) {
          const committedOffset = Number(committedByPartition.get(wm.partition) ?? '0');
          const lag = Math.max(0, Number(wm.high) - committedOffset);
          kafkaConsumerLag.set({ group: this.groupId, topic, partition: String(wm.partition) }, lag);
        }
      }
    } catch {
      // Falha ao medir lag não pode derrubar o consumidor real — é telemetria, não
      // efeito de negócio. Próxima passada (15s) tenta de novo.
    } finally {
      await admin.disconnect();
    }
  }
```

E em `stop()`, adicione `if (this.lagPollTimer) clearInterval(this.lagPollTimer);` antes do `Promise.all` que desconecta os consumidores.

- [ ] **Step 9: Rode a suíte inteira, lint, typecheck**

Run: `pnpm --filter @ecommerce/kafka test && pnpm --filter @ecommerce/kafka lint && pnpm --filter @ecommerce/kafka typecheck`
Expected: tudo verde.

- [ ] **Step 10: Commit**

```bash
git add packages/kafka pnpm-lock.yaml
git commit -m "feat(kafka): propaga traceparent, conta dlq_messages_total, mede kafka_consumer_lag (Fase 7)"
```

---

### Task 3: `packages/outbox` — mede `outbox_lag_seconds`

**Files:**
- Modify: `packages/outbox/package.json`
- Modify: `packages/outbox/src/outbox-relay.ts`
- Test: `packages/outbox/test/outbox-relay.integration.spec.ts`

- [ ] **Step 1: Adicione a dependência**

Em `packages/outbox/package.json`, adicione `"@ecommerce/observability": "workspace:*"` e `"@opentelemetry/api": "^1.9.0"` (não precisa desta segunda para este pacote — só a primeira; remova se o lint acusar dependência não usada) às `dependencies`. Rode `pnpm install`.

- [ ] **Step 2: Adicione o teste**

Em `packages/outbox/test/outbox-relay.integration.spec.ts`, adicione:

```typescript
  it('atualiza outbox_lag_seconds com a idade da linha pendente mais antiga', async () => {
    const eventId = randomUUID();
    await insertRow(eventId);
    // Backdata created_at manualmente para simular uma linha PENDENTE HÁ tempo.
    await pool.query(`UPDATE outbox SET created_at = now() - interval '10 seconds' WHERE event_id = $1`, [eventId]);

    const relay = new OutboxRelay({ pool, publish: async () => {}, serviceName: 'order-service' });
    await relay.drainOnce();

    const { outboxLagSeconds } = await import('@ecommerce/observability');
    const output = await (await import('@ecommerce/observability')).metricsRegistry.metrics();
    expect(output).toContain('outbox_lag_seconds');
    void outboxLagSeconds; // só para o import não ficar "não usado" caso o lint reclame
  });
```

(o `publish` desta linha some antes do `drainOnce` medir a idade — ajuste se necessário para medir ANTES de publicar: veja o Step 3, a medição precisa acontecer no SELECT, antes do loop de publish.)

- [ ] **Step 3: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/outbox test -- outbox-relay`
Expected: FAIL — `OutboxRelayOptions` não aceita `serviceName`.

- [ ] **Step 4: Implemente a medição em `outbox-relay.ts`**

Adicione o import no topo:

```typescript
import { outboxLagSeconds } from '@ecommerce/observability';
```

Em `OutboxRelayOptions`, adicione o campo:

```typescript
  serviceName: string;
```

No construtor, adicione o campo correspondente e a atribuição (siga o padrão dos outros campos já lidos de `opts` no construtor existente).

Em `drainOnce()`, logo depois do `SELECT` que busca `rows` (antes do `if (rows.length === 0)`), adicione:

```typescript
      if (rows.length > 0) {
        const oldestCreatedAt = rows.reduce(
          (oldest, row) => (row.created_at < oldest ? row.created_at : oldest),
          rows[0]!.created_at,
        );
        outboxLagSeconds.set({ service: this.serviceName }, (Date.now() - new Date(oldestCreatedAt as unknown as string).getTime()) / 1000);
      } else {
        outboxLagSeconds.set({ service: this.serviceName }, 0);
      }
```

Isto exige que a query já SELECIONE `created_at` — confira que o `SELECT` já busca essa coluna (deveria, já é usada no `ORDER BY created_at`); se a query só faz `SELECT id, event_id, payload, headers`, adicione `created_at` à lista de colunas selecionadas.

- [ ] **Step 5: Ajuste todo `new OutboxRelay({...})` existente para passar `serviceName`**

Rode `grep -rln "new OutboxRelay(" apps/*/src` para achar todos os pontos de instanciação (um por serviço, dentro de `outbox-relay.service.ts` de cada app). Em cada um, adicione `serviceName: '<nome-do-serviço>'` (ex.: `'order-service'`, `'payment-service'`, etc. — use o nome exato do diretório em `apps/`) ao objeto de opções passado ao construtor.

- [ ] **Step 6: Rode a suíte inteira, lint, typecheck**

Run: `pnpm --filter @ecommerce/outbox test && pnpm --filter @ecommerce/outbox lint && pnpm --filter @ecommerce/outbox typecheck`

Depois rode o typecheck de TODOS os serviços (o campo `serviceName` agora é obrigatório em `OutboxRelayOptions` — qualquer app que não foi atualizado no Step 5 vai quebrar):

Run: `pnpm exec turbo run typecheck`
Expected: tudo verde.

- [ ] **Step 7: Commit**

```bash
git add packages/outbox apps/*/src/infrastructure/outbox-relay.service.ts pnpm-lock.yaml
git commit -m "feat(outbox): mede outbox_lag_seconds (Fase 7)"
```

---

### Task 4: Order Service — métricas de saga + wiring de observabilidade

**Files:**
- Modify: `apps/order-service/package.json`
- Modify: `apps/order-service/src/main.ts`
- Modify: `apps/order-service/src/app.module.ts`
- Modify: `apps/order-service/src/application/order-projection.handler.ts`
- Test: `apps/order-service/test/order-projection-handler.integration.spec.ts`

- [ ] **Step 1: Dependência + wiring básico**

Adicione `"@ecommerce/observability": "workspace:*"` ao `package.json` do order-service. Rode `pnpm install`.

Em `apps/order-service/src/main.ts`, adicione `initTracing('order-service')` como a PRIMEIRA linha da função `bootstrap()` (antes de `NestFactory.create`):

```typescript
import { initTracing } from '@ecommerce/observability';
// ... resto dos imports já existentes

async function bootstrap(): Promise<void> {
  initTracing('order-service');
  const app = await NestFactory.create(AppModule);
  // ... resto do bootstrap já existente, sem mudança
```

Em `apps/order-service/src/app.module.ts`, importe `ObservabilityModule` de `@ecommerce/observability` e adicione a `imports` do `@Module`.

- [ ] **Step 2: Teste das métricas de saga (falhando)**

Em `apps/order-service/test/order-projection-handler.integration.spec.ts`, adicione:

```typescript
  it('caminho feliz registra saga_duration_seconds com outcome=confirmed', async () => {
    const { metricsRegistry } = await import('@ecommerce/observability');
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockReserved(orderId));
    await handler.handle(makeShipmentCreated(orderId));

    const output = await metricsRegistry.metrics();
    expect(output).toContain('saga_duration_seconds');
    expect(output).toMatch(/saga_duration_seconds_count\{outcome="confirmed"\}/);
  });

  it('stock.unavailable + payment.refunded registra saga_compensations_total{compensationType="PAYMENT_REFUNDED"}', async () => {
    const { metricsRegistry } = await import('@ecommerce/observability');
    const orderId = await createTestOrder();

    await handler.handle(makePaymentApproved(orderId));
    await handler.handle(makeStockUnavailable(orderId));
    await handler.handle(makePaymentRefunded(orderId));

    const output = await metricsRegistry.metrics();
    expect(output).toMatch(/saga_compensations_total\{compensationType="PAYMENT_REFUNDED"\}/);
  });
```

- [ ] **Step 3: Rode e confirme que falha**

Run: `pnpm --filter @ecommerce/order-service test -- order-projection-handler`
Expected: FAIL — as métricas nunca são incrementadas.

- [ ] **Step 4: Instrumente `order-projection.handler.ts`**

Adicione o import:

```typescript
import { sagaCompensationsTotal, sagaDurationSeconds } from '@ecommerce/observability';
```

No método `handle`, no bloco onde `result.next === ORDER_STATUS.CONFIRMED` (dentro do `if/else if` já existente), logo ANTES ou DEPOIS de montar `confirmedEnvelope` (em qualquer ponto dentro daquele ramo, já com `order` disponível no escopo), adicione:

```typescript
        sagaDurationSeconds.observe(
          { outcome: 'confirmed' },
          (Date.now() - order.createdAt.getTime()) / 1000,
        );
```

No ramo `else if (result.next === ORDER_STATUS.CANCELLED)`, dentro do `if (reason)`, adicione de forma análoga:

```typescript
          sagaDurationSeconds.observe(
            { outcome: 'cancelled' },
            (Date.now() - order.createdAt.getTime()) / 1000,
          );
```

No método `handleCompensationEvent` (criado na I4), logo depois de `await tx.order.update({...})` que grava `compensationsReceived`, adicione:

```typescript
      sagaCompensationsTotal.inc({ compensationType: COMPENSATION_TYPE_BY_EVENT_LABEL[eventType] });
```

Onde `COMPENSATION_TYPE_BY_EVENT_LABEL` é simplesmente o valor de compensação correspondente ao `eventType` — reaproveite a MESMA lógica que `order-state-machine.ts` já tem internamente (`COMPENSATION_TYPE_BY_EVENT`, não exportado hoje). Exporte-o de `order-state-machine.ts` (adicione `export` na frente da constante já existente) e importe em `order-projection.handler.ts`:

```typescript
import {
  applyCompensationEvent,
  applyEvent,
  COMPENSATION_TYPE_BY_EVENT,
  type CompensationEventType,
  type ProjectionEventType,
} from './order-state-machine.js';
```

E use `COMPENSATION_TYPE_BY_EVENT[eventType]` diretamente no lugar de `COMPENSATION_TYPE_BY_EVENT_LABEL[eventType]` acima (era o mesmo mapa, só com nome errado no rascunho deste plano — use `COMPENSATION_TYPE_BY_EVENT`).

Ao final do `if (result.next === ORDER_STATUS.CANCELLED)` dentro de `handleCompensationEvent` (quando a compensação FECHA o pedido), registre TAMBÉM `sagaDurationSeconds`:

```typescript
      if (result.next === ORDER_STATUS.CANCELLED) {
        sagaDurationSeconds.observe(
          { outcome: 'cancelled' },
          (Date.now() - order.createdAt.getTime()) / 1000,
        );
        // ... código já existente de createEvent(orderEvents.orderCancelled, ...) e insertOutboxRow
      }
```

- [ ] **Step 5: Rode e confirme que passa**

Run: `pnpm --filter @ecommerce/order-service test -- order-projection-handler`
Expected: TODOS os testes (os já existentes + os 2 novos) PASS.

- [ ] **Step 6: Rode a suíte inteira, lint, typecheck**

Run: `pnpm --filter @ecommerce/order-service test && pnpm --filter @ecommerce/order-service lint && pnpm --filter @ecommerce/order-service typecheck`

- [ ] **Step 7: Commit**

```bash
git add apps/order-service pnpm-lock.yaml
git commit -m "feat(order-service): tracing, /metrics e métricas de saga/compensação (Fase 7)"
```

---

### Task 5: Payment, Inventory, Shipping, Notification Service — wiring de observabilidade

Mesmo padrão exato do Task 4 Step 1 (dependência + `initTracing` + `ObservabilityModule`), repetido para os 4 serviços restantes — SEM as métricas de saga (essas são só do Order Service). Nenhuma das 4 tarefas abaixo tem teste novo (é wiring de infraestrutura, coberto pelo `pnpm exec turbo run test` da Task 7 de verificação final) — mas rode lint+typecheck de cada um antes de commitar.

**Files (× 4, um conjunto por serviço):**
- Modify: `apps/<serviço>/package.json` — adiciona `@ecommerce/observability`
- Modify: `apps/<serviço>/src/main.ts` — `initTracing('<serviço>')` como primeira linha de `bootstrap()`
- Modify: `apps/<serviço>/src/app.module.ts` — importa `ObservabilityModule`

- [ ] **Step 1: payment-service**

`pnpm install` depois de editar `apps/payment-service/package.json`. `initTracing('payment-service')`. `pnpm --filter @ecommerce/payment-service lint typecheck`.

```bash
git add apps/payment-service pnpm-lock.yaml
git commit -m "feat(payment-service): wiring de tracing e /metrics (Fase 7)"
```

- [ ] **Step 2: inventory-service**

Mesmo padrão, `initTracing('inventory-service')`.

```bash
git add apps/inventory-service pnpm-lock.yaml
git commit -m "feat(inventory-service): wiring de tracing e /metrics (Fase 7)"
```

- [ ] **Step 3: shipping-service**

Mesmo padrão, `initTracing('shipping-service')`.

```bash
git add apps/shipping-service pnpm-lock.yaml
git commit -m "feat(shipping-service): wiring de tracing e /metrics (Fase 7)"
```

- [ ] **Step 4: notification-service**

Mesmo padrão, `initTracing('notification-service')`.

```bash
git add apps/notification-service pnpm-lock.yaml
git commit -m "feat(notification-service): wiring de tracing e /metrics (Fase 7)"
```

- [ ] **Step 5: Rode a suíte inteira do monorepo**

Run: `pnpm exec turbo run lint typecheck build test`
Expected: tudo verde (ignore flakiness já conhecida de hooks e2e sob Kafka compartilhado).

---

### Task 6: Grafana — dashboard provisionado + Prometheus — regras de alerta

**Files:**
- Create: `deploy/docker/grafana/provisioning/dashboards/dashboards.yml`
- Create: `deploy/docker/grafana/provisioning/dashboards/saga-overview.json`
- Create: `deploy/docker/prometheus/alerts.yml`
- Modify: `deploy/docker/prometheus/prometheus.yml`
- Modify: `deploy/docker/docker-compose.yml`

- [ ] **Step 1: Provisionamento do dashboard no Grafana**

Crie `deploy/docker/grafana/provisioning/dashboards/dashboards.yml`:

```yaml
apiVersion: 1

providers:
  - name: 'saga'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/provisioning/dashboards
```

Crie `deploy/docker/grafana/provisioning/dashboards/saga-overview.json` com um dashboard mínimo funcional (4 painéis: duração da saga, taxa de compensação, mensagens na DLT, lag do consumidor):

```json
{
  "title": "Saga — visão geral",
  "uid": "saga-overview",
  "schemaVersion": 39,
  "panels": [
    {
      "id": 1,
      "title": "Duração da saga (p50/p95)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        { "expr": "histogram_quantile(0.5, sum(rate(saga_duration_seconds_bucket[5m])) by (le, outcome))", "legendFormat": "p50 {{outcome}}" },
        { "expr": "histogram_quantile(0.95, sum(rate(saga_duration_seconds_bucket[5m])) by (le, outcome))", "legendFormat": "p95 {{outcome}}" }
      ]
    },
    {
      "id": 2,
      "title": "Taxa de compensação",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "targets": [
        { "expr": "sum(rate(saga_compensations_total[5m])) by (compensationType)", "legendFormat": "{{compensationType}}" }
      ]
    },
    {
      "id": 3,
      "title": "Mensagens na DLT",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [
        { "expr": "sum(rate(dlq_messages_total[5m])) by (topic, consumerGroup)", "legendFormat": "{{topic}} / {{consumerGroup}}" }
      ]
    },
    {
      "id": 4,
      "title": "Lag do consumidor",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "targets": [
        { "expr": "kafka_consumer_lag", "legendFormat": "{{group}} / {{topic}} / p{{partition}}" }
      ]
    }
  ]
}
```

- [ ] **Step 2: Regras de alerta no Prometheus**

Crie `deploy/docker/prometheus/alerts.yml`:

```yaml
groups:
  - name: saga
    rules:
      - alert: KafkaConsumerLagAlto
        expr: kafka_consumer_lag > 1000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: 'Lag do consumidor {{ $labels.group }} em {{ $labels.topic }} acima de 1000'

      - alert: MensagensNaDLT
        expr: increase(dlq_messages_total[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: 'Mensagem nova na DLT: {{ $labels.topic }} / {{ $labels.consumerGroup }}'

      - alert: TaxaDeCompensacaoAlta
        expr: |
          sum(rate(saga_compensations_total[15m]))
          /
          sum(rate(saga_duration_seconds_count[15m]))
          > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: 'Mais de 5% das sagas concluídas nos últimos 15min precisaram de compensação'
```

Em `deploy/docker/prometheus/prometheus.yml`, adicione a seção `rule_files` (no mesmo nível de `global`/`scrape_configs`):

```yaml
rule_files:
  - /etc/prometheus/alerts.yml
```

Em `deploy/docker/docker-compose.yml`, no serviço `prometheus`, adicione o volume do arquivo de alertas junto ao volume do `prometheus.yml` já montado:

```yaml
      - ./prometheus/alerts.yml:/etc/prometheus/alerts.yml:ro
```

- [ ] **Step 3: Suba a infra e confira que o Prometheus carregou as regras sem erro**

Run: `docker compose -f deploy/docker/docker-compose.yml up -d prometheus grafana`
Depois: `curl -s http://localhost:9090/api/v1/rules | grep -o '"name":"[A-Za-z]*"'`
Expected: lista os 3 nomes de alerta (`KafkaConsumerLagAlto`, `MensagensNaDLT`, `TaxaDeCompensacaoAlta`). Se o Prometheus não subir ou a API devolver erro, rode `docker compose -f deploy/docker/docker-compose.yml logs prometheus` e corrija a sintaxe do YAML antes de prosseguir.

- [ ] **Step 4: Confira que o dashboard aparece no Grafana**

Abra `http://localhost:3300` (login `admin`/`admin`, já configurado) e confirme que o dashboard "Saga — visão geral" aparece na lista (pode levar até 30s pelo `updateIntervalSeconds` do provisionamento).

- [ ] **Step 5: Commit**

```bash
git add deploy/docker/grafana deploy/docker/prometheus deploy/docker/docker-compose.yml
git commit -m "feat(observability): dashboard Grafana e alertas Prometheus da saga (Fase 7)"
```

---

### Task 7: Verificação final — trace real de ponta a ponta

- [ ] **Step 1: Suba os 5 serviços + infra de observabilidade**

```bash
docker compose -f deploy/docker/docker-compose.yml up -d order-service payment-service inventory-service shipping-service notification-service jaeger prometheus grafana
```

- [ ] **Step 2: Crie um pedido de verdade e confira o trace no Jaeger**

Gere um JWT (mesmo processo de sempre) e faça `POST /orders`. Espere ~10s. Abra `http://localhost:16686`, selecione o serviço `order-service`, procure pelo trace mais recente. Confirme que o MESMO trace ID aparece em spans de `order-service`, `payment-service`, `inventory-service` e `shipping-service` (a saga inteira num trace só — o critério de pronto da Fase 7 em docs/PLAN.md). Se cada serviço aparecer com um trace ID DIFERENTE, o `propagation.inject`/`extract` da Task 2 não está funcionando — volte lá antes de prosseguir.

- [ ] **Step 3: Confira `/metrics` de cada serviço**

```bash
curl -s http://localhost:3000/metrics | grep -E "saga_duration_seconds|saga_compensations_total"
curl -s http://localhost:3001/metrics | grep -E "process_"  # prom-client expõe métricas de processo por padrão também
```

Expected: `saga_duration_seconds` e `saga_compensations_total` aparecem em `order-service` com pelo menos uma observação (do pedido criado no Step 2, se ele chegou a estado terminal).

- [ ] **Step 4: Pare os containers de aplicação**

```bash
docker compose -f deploy/docker/docker-compose.yml stop order-service payment-service inventory-service shipping-service notification-service
```

- [ ] **Step 5: Push**

```bash
git push origin feat/fases-6-11-compensacao
```

Este plano termina aqui. Próximo: Fase 7b (Saga Observer — a UI real).
