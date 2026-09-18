import { Controller, Get, Sse } from '@nestjs/common';
import { map, type Observable } from 'rxjs';
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
import { OrderProjectionStore, type OrderSnapshot } from '../domain/order-projection.store.js';

@Controller('api/orders')
export class OrdersStreamController {
  constructor(private readonly store: OrderProjectionStore) {}

  @Get()
  snapshot(): OrderSnapshot[] {
    return this.store.getAll();
  }

  @Sse('stream')
  stream(): Observable<{ data: OrderSnapshot }> {
    return this.store.events$.pipe(map((snapshot) => ({ data: snapshot })));
  }
}
