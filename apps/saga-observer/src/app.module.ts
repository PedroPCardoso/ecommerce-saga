import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { OrdersStreamController } from './api/orders-stream.controller.js';
import { OrderProjectionStore } from './domain/order-projection.store.js';
import { SagaObserverConsumerService } from './infrastructure/saga-observer-consumer.service.js';

@Module({
  controllers: [HealthController, OrdersStreamController],
  providers: [OrderProjectionStore, SagaObserverConsumerService],
})
export class AppModule {}
