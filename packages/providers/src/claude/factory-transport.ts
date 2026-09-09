import { spawn } from 'node:child_process';
import type { SpawnOptions, SpawnedProcess } from '@anthropic-ai/claude-agent-sdk';

/** Observe the SDK-owned native process; a result frame is not exit evidence. */
export function createFactoryClaudeTransport(): {
  spawn: (options: SpawnOptions) => SpawnedProcess;
  waitForClosed: () => Promise<void>;
} {
  const exits: Promise<void>[] = [];
  return {
    spawn(options): SpawnedProcess {
      const child = spawn(options.command, options.args, {
        cwd: options.cwd,
        env: options.env,
        signal: options.signal,
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      // close follows exit and closure of the owned stdio streams. Merely
      // having sent kill/abort or seen stdout EOF is insufficient.
      exits.push(
        new Promise<void>(accept => {
          child.once('close', () => {
            accept();
          });
        })
      );
      return child;
    },
    async waitForClosed(): Promise<void> {
      if (exits.length === 0) throw new Error('factory_provider_transport_missing');
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          Promise.all(exits),
          new Promise<never>((_accept, reject) => {
            timer = setTimeout(() => {
              reject(new Error('factory_provider_transport_close_uncertain'));
            }, 10000);
          }),
        ]);
      } finally {
        if (timer) clearTimeout(timer);
      }
    },
  };
}
