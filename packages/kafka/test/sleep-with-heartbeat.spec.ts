import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { sleepWithHeartbeat } from '../src/consumer-runtime.js';

/**
 * Prova, com fake timers (sem esperar tempo real), o bug crítico encontrado
 * na revisão final: dormir os 60s/600s dos degraus retry-1m/retry-10m num
 * único `setTimeout` bloqueante nunca dá ao kafkajs a chance de mandar
 * heartbeat — o consumidor é expulso do grupo pelo `sessionTimeout` (30s)
 * antes do delay terminar, e a mensagem trava naquele degrau para sempre.
 * `sleepWithHeartbeat` corrige isso dormindo em passos curtos e chamando
 * `heartbeat()` a cada passo.
 */
describe('sleepWithHeartbeat', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('chama heartbeat() periodicamente durante um degrau de 60s (retry-1m) — nunca fica 30s+ sem heartbeat', async () => {
    const heartbeat = vi.fn().mockResolvedValue(undefined);
    const promise = sleepWithHeartbeat(60_000, heartbeat, 3_000);

    // Avança em passos de 3s, exatamente como o código realmente dorme —
    // depois de CADA passo, heartbeat já deveria ter sido chamado.
    for (let elapsed = 3_000; elapsed <= 60_000; elapsed += 3_000) {
      await vi.advanceTimersByTimeAsync(3_000);
      expect(heartbeat).toHaveBeenCalledTimes(elapsed / 3_000);
    }

    await promise;
    // 60_000 / 3_000 = 20 chamadas — nenhum intervalo entre elas passa dos
    // 3s, bem abaixo do sessionTimeout de 30s configurado no consumer.
    expect(heartbeat).toHaveBeenCalledTimes(20);
  });

  it('chama heartbeat() durante um degrau de 600s (retry-10m) sem nunca deixar 30s se passarem sem chamada', async () => {
    const heartbeat = vi.fn().mockResolvedValue(undefined);
    const promise = sleepWithHeartbeat(600_000, heartbeat, 3_000);

    await vi.advanceTimersByTimeAsync(600_000);
    await promise;

    expect(heartbeat).toHaveBeenCalledTimes(200); // 600_000 / 3_000
  });

  it('com delay menor que o intervalo (ex.: degrau de 5s), ainda dorme o tempo certo e chama heartbeat pelo menos uma vez', async () => {
    const heartbeat = vi.fn().mockResolvedValue(undefined);
    const promise = sleepWithHeartbeat(5_000, heartbeat, 3_000);

    await vi.advanceTimersByTimeAsync(5_000);
    await promise;

    // Passos: 3s (heartbeat 1) + 2s restante (heartbeat 2) = dorme os 5s certinhos.
    expect(heartbeat).toHaveBeenCalledTimes(2);
  });
});
