import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import { OrdersController } from './api/orders.controller.js';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { OrderProjectionConsumerService } from './infrastructure/order-projection-consumer.service.js';
import { CreateOrderUseCase } from './application/create-order.use-case.js';
import { OrderProjectionHandler } from './application/order-projection.handler.js';

@Module({
  imports: [ThrottlerModule.forRoot({ throttlers: [{ ttl: 60_000, limit: 100 }] })],
  controllers: [OrdersController, HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    CreateOrderUseCase,
    OrderProjectionHandler,
    OrderProjectionConsumerService,
    { provide: APP_GUARD, useClass: ThrottlerGuard },
  ],
})
export class AppModule {}
