import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.spec.ts'],
    environment: 'node',
    testTimeout: 20_000,
    // Os specs de integração/e2e sobem consumidores e produtores Kafka reais
    // contra o mesmo broker; rodar os arquivos em paralelo faz múltiplos
    // processos Node abrirem conexões/produtores/consumidores concorrentes
    // contra o mesmo broker local, gerando contenção de recursos/conexão e
    // timeouts intermitentes no teste do relay (não é rebalanceamento de
    // consumer group — o OutboxRelayService só produz para o Kafka).
    fileParallelism: false,
  },
  plugins: [
    // Vitest transforma TS via esbuild, que não emite `design:paramtypes`
    // (emitDecoratorMetadata). Sem essa metadata a DI do Nest não sabe o
    // que injetar e resolve os parâmetros do construtor como undefined —
    // o swc, com decoratorMetadata habilitado, emite corretamente.
    swc.vite({
      jsc: {
        transform: {
          legacyDecorator: true,
          decoratorMetadata: true,
        },
      },
    }),
  ],
});
