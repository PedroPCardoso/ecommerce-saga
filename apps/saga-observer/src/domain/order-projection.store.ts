import { Injectable } from '@nestjs/common';
import { Subject } from 'rxjs';

export interface OrderSnapshot {
  orderId: string;
  eventType: string;
  status: string;
  occurredAt: string;
}

/**
 * Estado em memória, de propósito: este serviço NÃO é fonte de verdade — se
 * reiniciar, esquece tudo, e o próximo evento reconstrói a partir daí. Um
 * painel observável não precisa sobreviver a restart mais do que um
 * `top` sobrevive a fechar o terminal.
 */
@Injectable()
export class OrderProjectionStore {
  private readonly projections = new Map<string, OrderSnapshot>();
  private readonly subject = new Subject<OrderSnapshot>();

  readonly events$ = this.subject.asObservable();

  upsert(orderId: string, patch: { eventType: string; status: string }): OrderSnapshot {
    const snapshot: OrderSnapshot = {
      orderId,
      eventType: patch.eventType,
      status: patch.status,
      occurredAt: new Date().toISOString(),
    };
    this.projections.set(orderId, snapshot);
    this.subject.next(snapshot);
    return snapshot;
  }

  getAll(): OrderSnapshot[] {
    return [...this.projections.values()];
  }
}
