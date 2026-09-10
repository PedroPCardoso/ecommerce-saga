# 0005 — Um banco de dados por serviço

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

Cinco serviços com dados relacionados: o pedido referencia pagamento, reserva de estoque e envio. A tentação óbvia é um Postgres com cinco schemas e um `JOIN` quando for conveniente.

## Decisão

Um banco por serviço, com usuário e senha próprios. Em desenvolvimento, cinco containers Postgres separados; em Kubernetes, um `Cluster` CloudNativePG por serviço.

## Consequências

**Positivas**

- Sem banco compartilhado não existe transação distribuída — e é essa ausência que dá razão de existir à SAGA. Compartilhar o banco tornaria todo o projeto uma encenação.
- Cada serviço evolui seu schema sem coordenar migration com os outros.
- Isolamento de falha: banco de pagamento indisponível não derruba a criação de pedidos.
- Isolamento de credencial: comprometer o serviço de notificação não dá acesso aos dados de pagamento (A01/A04).

**Negativas**

- Nenhuma consulta cruzada. Responder "pedidos com envio atrasado" exige compor por API ou manter uma projeção — não há `JOIN`.
- Dado duplicado entre serviços (o Shipping guarda uma cópia do endereço), com consistência apenas eventual.
- Cinco containers Postgres pesam no Minikube. Se a RAM apertar, o recuo é um cluster com cinco databases e usuários distintos — **nunca** um schema compartilhado. Recuo é dívida: anote-a.
- Cinco conjuntos de migration para rodar em ordem no deploy.

## Alternativas consideradas

- **Um Postgres, cinco schemas:** mais leve e, em dev, quase indistinguível — até alguém escrever o `JOIN` que apaga a fronteira. Descartado por razão pedagógica e de segurança.
