import { Module } from '@nestjs/common';
import { ObservabilityModule } from '@ecommerce/observability';
import { HealthController } from './health/health.controller.js';
import { OrchestratedOrdersController } from './api/orchestrated-orders.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { CommandProducerService } from './infrastructure/command-producer.service.js';
import { OrchestratorService } from './application/orchestrator.service.js';
import { PaymentExecutorService } from './executors/payment-executor.service.js';
import { InventoryExecutorService } from './executors/inventory-executor.service.js';
import { ShippingExecutorService } from './executors/shipping-executor.service.js';

@Module({
  imports: [ObservabilityModule],
  controllers: [HealthController, OrchestratedOrdersController],
  providers: [
    PrismaService,
    CommandProducerService,
    OrchestratorService,
    PaymentExecutorService,
    InventoryExecutorService,
    ShippingExecutorService,
  ],
})
export class AppModule {}
