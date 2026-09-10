# 0011 — Dinheiro como inteiro em centavos

- **Status:** Aceito
- **Data:** 2026-08-25

## Contexto

Valores monetários atravessam a saga inteira e são **comparados**: o estorno precisa bater com a autorização. `0.1 + 0.2 !== 0.3` em IEEE 754, e JSON não tem tipo decimal — um `number` de JavaScript é sempre float.

## Decisão

Todo valor monetário é inteiro em centavos, em campos nomeados `*AmountCents`, com `currency` sempre explícita ao lado. Validado por `z.number().int()`.

## Consequências

**Positivas**

- Comparação e soma exatas: estorno confere com autorização, sem tolerância de epsilon.
- O nome do campo carrega a unidade. `amountCents` não é confundido com reais; `amount` seria.
- Serializa em JSON sem perda (valores muito abaixo de `Number.MAX_SAFE_INTEGER`).
- Torna os gatilhos determinísticos de teste triviais e legíveis: `amountCents % 100 === 13`.

**Negativas**

- Formatação para exibição fica com quem exibe (`/ 100`), e esquecer isso mostra "R$ 999.800,00" no lugar de "R$ 9.998,00".
- Moeda sem duas casas decimais (JPY, KWD) exige tabela de expoente por moeda. Não implementado; se aparecer moeda assim, é ADR novo.
- Divisão continua sendo o ponto delicado: rateio de frete entre itens precisa de política explícita de arredondamento, não de `Math.round` ad hoc.

## Alternativas consideradas

- **Float em unidade monetária:** o bug clássico, garantido.
- **String decimal + decimal.js:** precisão arbitrária e divisão correta, ao custo de conversão em toda fronteira. Vale num sistema financeiro; é peso morto aqui.
- **`NUMERIC` no Postgres com float no wire:** o banco fica correto e o evento errado — pior dos dois mundos.
