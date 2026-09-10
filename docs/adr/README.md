# Architecture Decision Records

Formato [MADR](https://adr.github.io/madr/). Uma decisão por arquivo, numeração sequencial, imutável depois de aceita — decisão que mudou não é editada, é **substituída** por um ADR novo que a marca como `superseded`.

Regra do projeto: **escreva o ADR antes de implementar**. Se você não consegue articular o "por quê" e as consequências negativas, ainda não entendeu a decisão.

| #                                                      | Decisão                                      | Status |
| ------------------------------------------------------ | -------------------------------------------- | ------ |
| [0001](0001-nestjs-typescript.md)                      | NestJS + TypeScript como stack dos serviços  | Aceito |
| [0002](0002-saga-coreografada.md)                      | SAGA coreografada em vez de orquestrada      | Aceito |
| [0003](0003-kafka-como-broker.md)                      | Kafka como broker de eventos                 | Aceito |
| [0004](0004-kafkajs-em-vez-de-nestjs-microservices.md) | kafkajs direto em vez do transport do NestJS | Aceito |
| [0005](0005-banco-por-servico.md)                      | Um banco de dados por serviço                | Aceito |
| [0006](0006-transactional-outbox.md)                   | Transactional Outbox para publicar eventos   | Aceito |
| [0007](0007-idempotencia-por-inbox.md)                 | Idempotência de consumo por tabela de inbox  | Aceito |
| [0008](0008-retry-escalonado-e-dlt.md)                 | Retry escalonado em tópicos separados + DLT  | Aceito |
| [0009](0009-orderid-como-chave-de-particao.md)         | `orderId` como chave de partição             | Aceito |
| [0010](0010-contratos-json-com-zod.md)                 | Contratos JSON validados com Zod             | Aceito |
| [0011](0011-dinheiro-em-centavos.md)                   | Dinheiro como inteiro em centavos            | Aceito |
