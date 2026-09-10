import { describe, expect, it, vi } from 'vitest';
import { insertOutboxRow, type RawSqlClient } from '../src/index.js';

describe('insertOutboxRow', () => {
  it('monta o INSERT parametrizado com o envelope serializado', async () => {
    const executeRawUnsafe = vi.fn().mockResolvedValue(1);
    const tx: RawSqlClient = { $executeRawUnsafe: executeRawUnsafe };

    await insertOutboxRow(tx, {
      eventId: '018f3f4e-0000-7000-8000-000000000001',
      aggregateId: 'order-1',
      aggregateType: 'order',
      eventType: 'order.created',
      envelope: { eventId: '018f3f4e-0000-7000-8000-000000000001', payload: { orderId: 'order-1' } },
      headers: { 'x-trace': 'abc' },
    });

    expect(executeRawUnsafe).toHaveBeenCalledTimes(1);
    const [sql, ...values] = executeRawUnsafe.mock.calls[0]!;
    expect(sql).toContain('INSERT INTO outbox');
    expect(values[0]).toBe('018f3f4e-0000-7000-8000-000000000001');
    expect(values[1]).toBe('order-1');
    expect(values[2]).toBe('order');
    expect(values[3]).toBe('order.created');
    expect(JSON.parse(values[4] as string)).toEqual({
      eventId: '018f3f4e-0000-7000-8000-000000000001',
      payload: { orderId: 'order-1' },
    });
    expect(JSON.parse(values[5] as string)).toEqual({ 'x-trace': 'abc' });
  });

  it('lança erro se nenhuma linha foi afetada', async () => {
    const tx: RawSqlClient = { $executeRawUnsafe: vi.fn().mockResolvedValue(0) };

    await expect(
      insertOutboxRow(tx, {
        eventId: 'evt-1',
        aggregateId: 'order-1',
        aggregateType: 'order',
        eventType: 'order.created',
        envelope: {},
      }),
    ).rejects.toThrow(/Falha ao inserir/);
  });
});
