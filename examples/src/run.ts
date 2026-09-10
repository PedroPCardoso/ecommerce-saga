/**
 * Atalho para rodar os exemplos: `pnpm ex 03` roda `src/03-*.ts`.
 * Sem argumento, lista o que existe.
 */
import { readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const aqui = dirname(fileURLToPath(import.meta.url));
const arquivos = readdirSync(aqui)
  .filter((f) => /^\d\d-.*\.ts$/.test(f))
  .sort();

const alvo = process.argv[2];
if (!alvo) {
  console.log('\nExemplos disponíveis:\n');
  for (const f of arquivos) {
    console.log(
      `  pnpm ex ${f.slice(0, 2)}   ${f.replace(/^\d\d-|\.ts$/g, '').replace(/-/g, ' ')}`,
    );
  }
  console.log('\n  pnpm ex todos   roda todos em sequência\n');
  process.exit(0);
}

if (alvo === 'todos') {
  const { spawnSync } = await import('node:child_process');
  for (const f of arquivos) {
    const r = spawnSync(process.execPath, ['--import', 'tsx', join(aqui, f)], { stdio: 'inherit' });
    if (r.status !== 0) {
      console.error(`\n${f} falhou. Parando aqui.\n`);
      process.exit(r.status ?? 1);
    }
  }
  console.log('\nTodos os exemplos passaram.\n');
  process.exit(0);
}

const escolhido = arquivos.find((f) => f.startsWith(alvo.padStart(2, '0')));
if (!escolhido) {
  console.error(`Não achei exemplo "${alvo}". Rode sem argumento para ver a lista.`);
  process.exit(1);
}
await import(pathToFileURL(join(aqui, escolhido)).href);
