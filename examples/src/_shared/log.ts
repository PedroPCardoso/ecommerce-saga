/* Saída de terminal com um pouco de estrutura. Sem dependência: o objetivo destes
   exemplos é que você consiga ler o arquivo inteiro sem abrir mais nada. */

const C = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  bold: '\x1b[1m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
};

let stepN = 0;

export function titulo(texto: string, subtitulo?: string): void {
  const linha = '─'.repeat(Math.min(76, texto.length + 4));
  console.log(`\n${C.bold}${C.cyan}${texto}${C.reset}`);
  if (subtitulo) console.log(`${C.dim}${subtitulo}${C.reset}`);
  console.log(`${C.dim}${linha}${C.reset}`);
  stepN = 0;
}

export function passo(texto: string): void {
  stepN += 1;
  console.log(`\n${C.bold}${stepN}.${C.reset} ${texto}`);
}

export const info = (t: string) => console.log(`   ${t}`);
export const detalhe = (t: string) => console.log(`   ${C.dim}${t}${C.reset}`);
export const bom = (t: string) => console.log(`   ${C.green}✓${C.reset} ${t}`);
export const ruim = (t: string) => console.log(`   ${C.red}✗${C.reset} ${t}`);
export const alerta = (t: string) => console.log(`   ${C.yellow}!${C.reset} ${t}`);

export function licao(texto: string): void {
  console.log(`\n${C.magenta}${C.bold}A lição:${C.reset} ${texto}`);
}

/** Tabela simples, alinhada. Recebe cabeçalhos e linhas já em texto. */
export function tabela(cabecalhos: string[], linhas: string[][]): void {
  const larguras = cabecalhos.map((h, i) =>
    Math.max(h.length, ...linhas.map((l) => (l[i] ?? '').length)),
  );
  const sep = larguras.map((w) => '─'.repeat(w + 2)).join('┼');
  const fmt = (celulas: string[]) =>
    celulas.map((c, i) => ` ${(c ?? '').padEnd(larguras[i]!)} `).join('│');

  console.log(`   ${C.dim}${fmt(cabecalhos)}${C.reset}`);
  console.log(`   ${C.dim}${sep}${C.reset}`);
  for (const l of linhas) console.log(`   ${fmt(l)}`);
}

/* ── asserções ──────────────────────────────────────────────────────────────
   Os exemplos não são só demonstração: eles falham com exit code != 0 se o
   comportamento observado não for o esperado. Servem de teste de integração. */

let falhas = 0;

export function confere(condicao: boolean, descricao: string): void {
  if (condicao) bom(descricao);
  else {
    ruim(descricao);
    falhas += 1;
  }
}

export function confereIgual<T>(atual: T, esperado: T, descricao: string): void {
  const ok = JSON.stringify(atual) === JSON.stringify(esperado);
  if (ok) bom(`${descricao} → ${JSON.stringify(atual)}`);
  else {
    ruim(`${descricao} → esperava ${JSON.stringify(esperado)}, veio ${JSON.stringify(atual)}`);
    falhas += 1;
  }
}

export function fim(): never {
  if (falhas === 0) {
    console.log(`\n${C.green}${C.bold}Tudo conferido.${C.reset}\n`);
    process.exit(0);
  }
  console.log(`\n${C.red}${C.bold}${falhas} verificação(ões) falharam.${C.reset}\n`);
  process.exit(1);
}

export const dorme = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
