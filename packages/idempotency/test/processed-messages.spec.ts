import { describe, expect, it, vi } from 'vitest';
import { markProcessed, type RawSqlClient } from '../src/index.js';

describe('markProcessed (unitário — cliente fake)', () => {
  it('devolve true e insere quando é a primeira vez', async () => {
    const executeRawUnsafe = vi.fn().mockResolvedValue(1);
    const tx: RawSqlClient = { $executeRawUnsafe: executeRawUnsafe };

    const result = await markProcessed(tx, '018f3f4e-0000-7000-8000-000000000001', 'payment-service');

    expect(result).toBe(true);
    expect(executeRawUnsafe).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO processed_messages'),
      '018f3f4e-0000-7000-8000-000000000001',
      'payment-service',
    );
  });

  it('devolve false quando o par (event_id, consumer_group) já existe', async () => {
    const tx: RawSqlClient = { $executeRawUnsafe: vi.fn().mockResolvedValue(0) };

    const result = await markProcessed(tx, 'evt-repetido', 'payment-service');

    expect(result).toBe(false);
  });
});
