# 0009 — `orderId` como chave de partição

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

Kafka garante ordenação **dentro de uma partição**, nunca entre partições. Sem chave, o produtor distribui em round-robin: `payment.approved` e `payment.refunded` do mesmo pedido podem cair em partições diferentes e ser processados na ordem errada — estorno antes da aprovação.

## Decisão

A chave de toda mensagem de negócio é o `orderId`, exposto no envelope como `aggregateId`.

## Consequências

**Positivas**

- Todos os eventos de um pedido caem na mesma partição, logo são ordenados entre si. É a única garantia de ordem que a saga precisa: entre pedidos distintos, ordem é irrelevante.
- O paralelismo continua igual ao número de partições — nada é sacrificado.
- Reprocessar um pedido específico é localizável: partição = `hash(orderId) % n`.

**Negativas**

- **Hot partition.** Um pedido excepcionalmente movimentado concentra tráfego numa partição só. Irrelevante aqui (poucos eventos por pedido), sério num domínio com agregado "quente".
- **Mudar o número de partições reembaralha o mapeamento.** Eventos antigos de um pedido ficam numa partição e os novos em outra, quebrando a ordenação para pedidos em voo. Repartitionar é operação planejada, com o fluxo drenado — não um ajuste de capacidade.
- Ordenação vale **por tópico**. O Order Service consome três tópicos diferentes e pode ver `stock.reserved` antes de `payment.approved`. Não há chave que resolva isso: a defesa é a máquina de estados.
- A escada de retry (ADR-0008) rompe a garantia de propósito.

## Alternativas consideradas

- **Sem chave (round-robin):** melhor distribuição, nenhuma ordenação. Inviável.
- **`customerId` como chave:** ordenaria todos os pedidos de um cliente, garantia mais forte que o necessário, com hot partition muito mais provável.
