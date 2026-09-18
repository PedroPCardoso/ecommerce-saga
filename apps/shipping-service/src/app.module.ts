import { Module } from '@nestjs/common';
import { ObservabilityModule } from '@ecommerce/observability';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { ShippingConsumerService } from './infrastructure/shipping-consumer.service.js';
import { OrderCreatedHandler } from './application/order-created.handler.js';
import { StockReservedHandler } from './application/stock-reserved.handler.js';
import { ShippingEventRouter } from './application/shipping-event.router.js';

@Module({
  imports: [ObservabilityModule],
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    OrderCreatedHandler,
    StockReservedHandler,
    ShippingEventRouter,
    ShippingConsumerService,
  ],
})
export class AppModule {}
