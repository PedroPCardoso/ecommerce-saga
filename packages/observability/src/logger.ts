import pino, { type Logger } from 'pino';

/**
 * Um logger por serviço, com `service` no binding base — é o que permite
 * filtrar/agrupar log agregado (Loki, CloudWatch, etc.) por serviço sem
 * grep manual. `correlationId` entra via `.child({ correlationId })` no
 * ponto de uso (handler de request HTTP ou de mensagem Kafka), nunca aqui —
 * este logger é criado UMA vez no bootstrap do processo.
 */
export function createLogger(serviceName: string): Logger {
  return pino({ level: process.env.LOG_LEVEL ?? 'info' }).child({ service: serviceName });
}
