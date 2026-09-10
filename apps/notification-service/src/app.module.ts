import { Module } from '@nestjs/common';
import { HealthController } from './health/health.controller.js';
import { PrismaService } from './infrastructure/prisma.service.js';
import { MailerService } from './infrastructure/mailer.service.js';
import { NotificationConsumerService } from './infrastructure/notification-consumer.service.js';
import { NotificationEventHandler } from './application/notification-event.handler.js';

@Module({
  controllers: [HealthController],
  providers: [PrismaService, MailerService, NotificationEventHandler, NotificationConsumerService],
})
export class AppModule {}
