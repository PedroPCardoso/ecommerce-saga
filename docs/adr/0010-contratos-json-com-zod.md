# 0010 — Contratos JSON validados com Zod

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

Onze tipos de evento atravessam cinco serviços. Sem contrato compartilhado, cada serviço redeclara o formato do que consome e a divergência aparece em produção, não na revisão. Um evento é entrada não confiável: pode vir de um deploy ruim, de um replay de tópico antigo ou de quem tenha ganhado acesso de produtor (A05).

## Decisão

JSON com envelope versionado, schemas Zod em `packages/contracts` como **fonte única**, validados na publicação e no consumo. `eventType` e `eventVersion` travados em literais por definição, de modo que versão desconhecida seja **rejeitada** em vez de interpretada pelo parser da versão anterior (A08). Schema Registry fica para a Fase 12.

## Consequências

**Positivas**

- Um lugar só define cada evento. Serviço nunca redeclara o schema do que consome.
- Tipos TypeScript derivados do schema: contrato e tipo não podem divergir.
- Validar **na publicação** faz o erro estourar no produtor, com contexto, em vez de no consumidor, onde só resta a DLT.
- Teste de snapshot do catálogo quebra o build em mudança de contrato — a rede que impede alteração acidental.
- JSON é legível no Kafka UI, o que importa muito ao depurar.

**Negativas**

- **`packages/contracts` é acoplamento de build entre os cinco serviços.** Mudança ali obriga rebuild de todos. É acoplamento deliberado: a alternativa é acoplamento implícito, que é pior porque é invisível.
- JSON é verboso e sem tipagem no wire. Sem Schema Registry, nada impede que um produtor fora do monorepo publique lixo — só a validação do consumidor, já na hora do prejuízo.
- Compatibilidade de evolução de schema é responsabilidade humana até a Fase 12.
- Validar toda mensagem custa CPU (irrelevante nesta escala).

## Alternativas consideradas

- **Avro + Schema Registry desde o início:** compatibilidade verificada no registry, payload compacto, evolução governada. Descartado por trazer geração de código e um serviço a mais antes de o fluxo existir. É o destino, não o ponto de partida.
- **Protobuf:** mesma objeção, mais tooling.
- **Sem validação, só tipos TypeScript:** tipo desaparece em runtime; a mensagem chega como `any` e o `any` chega ao banco.
