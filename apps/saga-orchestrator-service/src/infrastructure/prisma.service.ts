import { Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
// Client gerado em `prisma/generated` (ver `output` no schema.prisma), não em
// `@prisma/client`: o pnpm resolve `@prisma/client` para a mesma entrada física do
// store de conteúdo endereçável em todo pacote do monorepo que dependa da mesma
// versão — inclusive outros serviços com schemas diferentes. Gerar ali faria um
// `prisma generate` sobrescrever o client de outro serviço.
import { PrismaClient } from '../../prisma/generated/index.js';

@Injectable()
export class PrismaService implements OnModuleInit, OnModuleDestroy {
  readonly client = new PrismaClient();

  async onModuleInit(): Promise<void> {
    await this.client.$connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.client.$disconnect();
  }
}
