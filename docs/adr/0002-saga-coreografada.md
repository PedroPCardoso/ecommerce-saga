# 0002 — SAGA coreografada em vez de orquestrada

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

O fluxo do pedido atravessa quatro serviços (pagamento, estoque, envio, notificação) sem transação distribuída. Falha em qualquer etapa exige desfazer o que as anteriores já efetivaram. Há dois desenhos clássicos: coreografia (cada serviço reage a eventos) e orquestração (um coordenador central manda comandos).

## Decisão

Coreografia. Nenhum serviço coordena os outros; cada um assina os tópicos que lhe interessam e publica o que aconteceu no seu domínio.

## Consequências

**Positivas**

- Acoplamento temporal baixo: publicar não espera ninguém, e um serviço fora do ar não derruba o fluxo — só atrasa.
- Adicionar um consumidor **puramente reativo** (relatório, auditoria, o próprio Notification) não exige mudar nada existente.
- Sem ponto único de falha lógica.

**Negativas — e são o motivo de a decisão estar documentada**

- **Acoplamento implícito.** O Payment Service precisa assinar `ecommerce.inventory.v1` e `ecommerce.shipping.v1` para saber quando estornar. Ele passa a depender de eventos de domínios que não são dele. Acrescentar uma 5ª etapa na saga obriga a mexer em **todos** os serviços anteriores.
- **Ninguém tem o fluxo inteiro.** Responder "por que o pedido X foi cancelado?" exige juntar log de quatro serviços. O `correlationId`/`causationId` do envelope existe para tornar isso possível; não para tornar fácil.
- **Não existe quem vigie o todo.** Uma etapa que simplesmente nunca acontece deixa o pedido pendurado para sempre. Corrigimos com um sweeper de timeout no Order Service — que, ironicamente, transforma o Order num meio-orquestrador. Esta é a evidência mais honesta a favor da orquestração, e está aqui de propósito.
- **Risco de laço de eventos.** Um serviço que publicasse no tópico que ele mesmo assina criaria um loop infinito e caro. Prevenido por teste em `packages/contracts/test/topics.spec.ts`.

## Alternativas consideradas

- **Orquestração** (Fase 11, opcional): coordenador central com state machine persistida enviando comandos. O fluxo fica num arquivo só, o "por quê" fica trivial de responder, e o preço é acoplamento ao orquestrador e um serviço a mais no caminho crítico. A comparação será feita por métrica (LoC para adicionar uma etapa, nº de serviços tocados, latência ponta a ponta), não por preferência.
