import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  INVENTORY_SERVICE_PORT: z.coerce.number().int().positive().default(3002),
  INVENTORY_DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) => value.split(',')),
  KAFKA_CLIENT_ID_PREFIX: z.string().min(1).default('ecommerce'),
});

export const env = envSchema.parse(process.env);
