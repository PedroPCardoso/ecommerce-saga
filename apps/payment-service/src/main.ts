import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { initTracing } from '@ecommerce/observability';
import { AppModule } from './app.module.js';
import { env } from './env.js';

async function bootstrap(): Promise<void> {
  initTracing('payment-service');
  const app = await NestFactory.create(AppModule);
  await app.listen(env.PAYMENT_SERVICE_PORT);
  console.log(`[payment-service] ouvindo na porta ${env.PAYMENT_SERVICE_PORT}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[payment-service] recebido ${signal}, encerrando graciosamente`);
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
