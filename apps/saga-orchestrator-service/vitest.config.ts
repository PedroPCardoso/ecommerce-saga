import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.spec.ts'],
    environment: 'node',
    testTimeout: 20_000,
    // Mesmo motivo dos demais serviços: specs que sobem consumidor/produtor
    // Kafka reais contra o mesmo broker não podem rodar em paralelo entre arquivos.
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
