import { describe, expect, it } from 'vitest';
import { createLogger } from '../src/logger.js';

describe('createLogger', () => {
  it('cria um logger pino com o nome do serviço no binding base', () => {
    const logger = createLogger('order-service');
    expect(logger.bindings()).toEqual({ service: 'order-service' });
  });

  it('.child({ correlationId }) propaga o campo em toda linha subsequente', () => {
    const logger = createLogger('order-service').child({ correlationId: 'abc-123' });
    expect(logger.bindings()).toMatchObject({ correlationId: 'abc-123', service: 'order-service' });
  });
});
