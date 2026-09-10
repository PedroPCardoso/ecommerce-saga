# 0001 — NestJS + TypeScript como stack dos serviços

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

Cinco serviços independentes, cada um com API HTTP, consumidores de evento, acesso a banco e ciclo de vida próprio. Precisamos de injeção de dependência (para trocar gateway real por mock nos testes), módulos com fronteira clara e ciclo de vida com hooks de shutdown — este último é requisito duro: um consumidor que morre no meio do processamento perde mensagem.

## Decisão

Node 22 + NestJS 11 + TypeScript em modo `strict`, com `noUncheckedIndexedAccess` e `exactOptionalPropertyTypes` ligados.

## Consequências

**Positivas**

- DI e módulos dão a fronteira que separa domínio de infraestrutura sem disciplina manual.
- `onApplicationShutdown` é o gancho onde o graceful shutdown do consumidor mora.
- Imagens de runtime pequenas e startup rápido — importa quando o KEDA escala de 2 para 8 réplicas.
- Um único ecossistema de tipos entre contrato de evento e código de negócio.

**Negativas**

- Node não tem transação distribuída nem `@Transactional` de framework: o controle transacional do outbox é escrito à mão. Aceito — escrever à mão é justamente o que ensina o padrão.
- Sem biblioteca madura de state machine de saga (o que .NET tem com MassTransit). Também aceito: a máquina de estados explícita é conteúdo, não acidente.
- `strict` + `exactOptionalPropertyTypes` custa fricção nas primeiras horas.

## Alternativas consideradas

- **Spring Boot 3:** melhor ecossistema para este cenário (Spring Modulith, Cloud Stream), mas mais verboso e com imagens maiores.
- **Go:** imagens mínimas e startup instantâneo, ótimo para exercitar Kubernetes, mas tudo da saga sai manual.
- **.NET 9 + MassTransit:** daria a state machine quase de graça — o que, para um projeto cujo objetivo é _entender_ a saga, é justamente o problema.
