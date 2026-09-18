import { Controller, Get, Header, Module } from '@nestjs/common';
import { metricsRegistry } from './metrics.js';

@Controller()
export class MetricsController {
  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  async metrics(): Promise<string> {
    return metricsRegistry.metrics();
  }
}

/** Importe em qualquer AppModule para ganhar `GET /metrics` de graça. */
@Module({
  controllers: [MetricsController],
})
export class ObservabilityModule {}
