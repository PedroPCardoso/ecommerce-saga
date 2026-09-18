import { describe, expect, it } from 'vitest';
import request from 'supertest';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { AppModule } from '../src/app.module.js';

describe('HealthController /ready e /startup (requer pnpm infra:up)', () => {
  it('GET /health/ready devolve 200 quando o Postgres responde', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    const app: INestApplication = moduleRef.createNestApplication();
    await app.init();

    await request(app.getHttpServer()).get('/health/ready').expect(200, { status: 'ok' });
    await request(app.getHttpServer()).get('/health/startup').expect(200, { status: 'ok' });

    await app.close();
  });
});
