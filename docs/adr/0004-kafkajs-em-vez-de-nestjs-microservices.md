# 0004 — kafkajs direto em vez do transport do NestJS

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

O NestJS oferece `@nestjs/microservices` com transport Kafka pronto: decorators `@EventPattern`, DI integrada, boilerplate zero. Parece a escolha óbvia.

## Decisão

Usar `kafkajs` diretamente, embrulhado num `packages/kafka` próprio.

## Consequências

**Positivas**

- **Controle do commit de offset.** O requisito central do consumo é: commitar o offset **somente depois** que a transação de banco tiver dado COMMIT. O transport do Nest gerencia o offset por conta própria, e contorná-lo é mais trabalhoso que não usá-lo.
- Classificação de erro (retriável vs. permanente) e a escada de retry ficam explícitas no nosso código, onde o leitor as encontra.
- Graceful shutdown controlado: parar de consumir, drenar o que está em voo, fechar conexões — nessa ordem.
- Headers Kafka acessíveis sem ginástica, o que a propagação de `traceparent` exige.

**Negativas**

- Escrevemos e mantemos ~300 linhas de infraestrutura de consumo que viriam de graça.
- Perdemos a integração automática com o ciclo de vida do Nest; a ligação é manual.
- Um `ConsumerModule` caseiro é uma superfície de bug nova.
- **kafkajs não tem rebalance cooperativo.** A versão 2.x implementa só o protocolo eager
  e traz apenas o assigner `roundRobin`; não há `cooperative-sticky` nem static membership
  (`group.instance.id`). Todo pod que entra ou sai para todos os consumidores do grupo
  por um instante. Se rebalance incremental virar requisito duro, a saída é um cliente
  baseado em librdkafka (`@confluentinc/kafka-javascript`) — e aí este ADR precisa ser
  substituído, não editado.

## Alternativas consideradas

- **`@nestjs/microservices`:** rápido de começar, e a reescrita apareceria exatamente na primeira falha real de consumo — depois de o desenho já ter sido construído em cima dele.
- **`@nestjs/event-emitter` + polling do outbox:** dispensaria o broker, mas descartaria o projeto inteiro.
