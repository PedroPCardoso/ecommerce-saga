import { Module } from '@nestjs/common';
import { ObservabilityModule } from '@ecommerce/observability';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { OutboxRelayService } from './infrastructure/outbox-relay.service.js';
import { InventoryConsumerService } from './infrastructure/inventory-consumer.service.js';
import { OrderCreatedHandler } from './application/order-created.handler.js';
import { PaymentApprovedHandler } from './application/payment-approved.handler.js';
import { ShipmentFailedHandler } from './application/shipment-failed.handler.js';
import { InventoryEventRouter } from './application/inventory-event.router.js';

@Module({
  imports: [ObservabilityModule],
  controllers: [HealthController],
  providers: [
    PrismaService,
    OutboxRelayService,
    OrderCreatedHandler,
    PaymentApprovedHandler,
    ShipmentFailedHandler,
    InventoryEventRouter,
    InventoryConsumerService,
  ],
})
export class AppModule {}
