import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { OutboxRelay } from '@ecommerce/outbox';
import { EventProducer } from '@ecommerce/kafka';
import { findDefinition, type UnknownEnvelope } from '@ecommerce/contracts';
import { env } from '../env.js';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: env.PAYMENT_DATABASE_URL });
  private readonly producer = new EventProducer({
    brokers: env.KAFKA_BROKERS,
    clientId: `${env.KAFKA_CLIENT_ID_PREFIX}-payment-service-relay`,
  });
  private readonly relay = new OutboxRelay({
    pool: this.pool,
    publish: async (row) => {
      const envelope = row.envelope as UnknownEnvelope;
      const definition = findDefinition(envelope.eventType, envelope.eventVersion);
      if (!definition) {
        throw new Error(
          `Evento ${envelope.eventType}@${envelope.eventVersion} sem tópico declarado em @ecommerce/contracts`,
        );
      }
      await this.producer.publish(definition.topic, envelope, row.headers);
    },
  });

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.relay.start();
  }

  async onModuleDestroy(): Promise<void> {
    await this.relay.stop();
    await this.producer.disconnect();
    await this.pool.end();
  }
}
