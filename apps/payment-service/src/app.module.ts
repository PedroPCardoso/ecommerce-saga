import { Module } from '@nestjs/common';
import { ObservabilityModule } from '@ecommerce/observability';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { AuthorizePaymentUseCase } from './application/authorize-payment.use-case.js';
import { RefundPaymentUseCase } from './application/refund-payment.use-case.js';
import { PaymentEventRouter } from './application/payment-event.router.js';
import { PaymentConsumerService } from './consumers/payment-consumer.service.js';

@Module({
  imports: [ObservabilityModule],
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    AuthorizePaymentUseCase,
    RefundPaymentUseCase,
    PaymentEventRouter,
    PaymentConsumerService,
  ],
})
export class AppModule {}
