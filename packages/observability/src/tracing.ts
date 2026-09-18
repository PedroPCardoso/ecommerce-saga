import { NodeTracerProvider, BatchSpanProcessor } from '@opentelemetry/sdk-trace-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

/**
 * Rastreamento MANUAL — sem auto-instrumentação (ver cabeçalho do plano
 * desta fase para o porquê). Registra um NodeTracerProvider global; depois
 * disto, qualquer `trace.getTracer(nome)` (de `@opentelemetry/api`, em
 * qualquer pacote) usa este provider. Chame uma vez, no início do
 * `bootstrap()` de cada `main.ts` — não precisa ser antes de outros
 * imports, porque não há módulo de terceiro sendo interceptado.
 *
 * Nota de versão: o plano original assumia `resourceFromAttributes` de
 * `@opentelemetry/resources`. A versão resolvida pelo pnpm install
 * (1.30.1) não exporta essa função — usa-se `new Resource({...})`, que é a
 * API real desta versão e cumpre o mesmo objetivo (anexar `service.name`
 * ao resource). `spanProcessors` no construtor do `NodeTracerProvider`
 * (via `TracerConfig`) existe normalmente nesta versão.
 */
export function initTracing(serviceName: string): void {
  const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'http://localhost:4318';
  const exporter = new OTLPTraceExporter({ url: `${endpoint}/v1/traces` });

  const provider = new NodeTracerProvider({
    resource: new Resource({ [ATTR_SERVICE_NAME]: serviceName }),
    spanProcessors: [new BatchSpanProcessor(exporter)],
  });

  provider.register();
}
