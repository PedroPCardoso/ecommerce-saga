import { describe, it } from 'vitest';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
// Import de valor é obrigatório aqui: o NestJS resolve o token de injeção em runtime a
// partir do design:paramtypes emitido por emitDecoratorMetadata, que só referencia a
// classe real quando o import não é `import type` (senão o metadata cai para `Function`
// e a DI quebra).
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { HealthController } from '../src/health/health.controller.js';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { PrismaService } from '../src/infrastructure/prisma.service.js';

// Testa só HealthController + PrismaService, NUNCA o AppModule inteiro: o AppModule
// sobe até uma dezena de consumidores Kafka no onModuleInit (achado da revisão final,
// M9) — um teste que só quer provar "SELECT 1 funciona" não deveria depender do
// broker nem herdar a variabilidade de tempo de um rebalance de consumer group.
describe('HealthController /ready e /startup (requer pnpm infra:up)', () => {
  it('GET /health/ready e /health/startup devolvem 200 quando o Postgres responde', async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [HealthController],
      providers: [PrismaService],
    }).compile();
    const app: INestApplication = moduleRef.createNestApplication();
    await app.init();

    await request(app.getHttpServer()).get('/health/ready').expect(200, { status: 'ok' });
    await request(app.getHttpServer()).get('/health/startup').expect(200, { status: 'ok' });

    await app.close();
  });
});
