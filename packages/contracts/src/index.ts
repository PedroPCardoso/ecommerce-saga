export * from './common.js';
export * from './envelope.js';
export * from './topics.js';
export * from './registry.js';

// Reexport plano dos enums de código de falha: consumidores (ex.: payment-service,
// shipping-service) precisam deles como valor de uso comum, não só dentro do namespace
// `paymentEvents`/`shippingEvents` — que existe para as *definições* de evento
// (`orderCreated`, `paymentApproved`, ...), não para essas constantes auxiliares.
export { PAYMENT_FAILURE_CODE } from './events/payment.js';
export { SHIPMENT_FAILURE_CODE } from './events/shipping.js';

export * as orderEvents from './events/order.js';
export * as paymentEvents from './events/payment.js';
export * as inventoryEvents from './events/inventory.js';
export * as shippingEvents from './events/shipping.js';
export * as orchestrationEvents from './events/orchestration.js';
