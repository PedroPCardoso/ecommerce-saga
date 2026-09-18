import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { initTracing } from '@ecommerce/observability';
import { AppModule } from './app.module.js';
import { env } from './env.js';

async function bootstrap(): Promise<void> {
  initTracing('shipping-service');
  const app = await NestFactory.create(AppModule);
  await app.listen(env.SHIPPING_SERVICE_PORT);
  console.log(`[shipping-service] ouvindo na porta ${env.SHIPPING_SERVICE_PORT}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[shipping-service] recebido ${signal}, encerrando graciosamente`);
    await app.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

bootstrap().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
