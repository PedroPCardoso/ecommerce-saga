import { describe, expect, it } from 'vitest';
import { OrderProjectionStore } from '../src/domain/order-projection.store.js';

describe('OrderProjectionStore', () => {
  it('upsert cria uma projeção nova na primeira chamada', () => {
    const store = new OrderProjectionStore();
    const snapshot = store.upsert('order-1', { eventType: 'order.created', status: 'PENDING' });
    expect(snapshot).toEqual({ orderId: 'order-1', eventType: 'order.created', status: 'PENDING', occurredAt: expect.any(String) });
  });

  it('upsert subsequente atualiza a MESMA entrada, getAll devolve só uma por orderId', () => {
    const store = new OrderProjectionStore();
    store.upsert('order-1', { eventType: 'order.created', status: 'PENDING' });
    store.upsert('order-1', { eventType: 'payment.approved', status: 'PAYMENT_APPROVED' });

    const all = store.getAll();
    expect(all).toHaveLength(1);
    expect(all[0]?.status).toBe('PAYMENT_APPROVED');
  });

  it('events$ emite uma vez por upsert, com o snapshot atualizado', async () => {
    const store = new OrderProjectionStore();
    const received: string[] = [];
    const sub = store.events$.subscribe((snap) => received.push(snap.status));

    store.upsert('order-1', { eventType: 'order.created', status: 'PENDING' });
    store.upsert('order-1', { eventType: 'payment.approved', status: 'PAYMENT_APPROVED' });

    sub.unsubscribe();
    expect(received).toEqual(['PENDING', 'PAYMENT_APPROVED']);
  });
});
