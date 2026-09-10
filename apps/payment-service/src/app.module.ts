import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { AuthorizePaymentUseCase } from './application/authorize-payment.use-case.js';
import { OrderEventsConsumerService } from './consumers/order-events-consumer.service.js';

@Module({
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    AuthorizePaymentUseCase,
    OrderEventsConsumerService,
  ],
})
export class AppModule {}
