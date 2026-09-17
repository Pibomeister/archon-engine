/** Explicit test preload: replaces only the SDK transport, never engine admission. */
import { mock } from 'bun:test';
import { randomUUID } from 'node:crypto';

function trace(phase: string, values: Record<string, unknown> = {}): void {
  process.stderr.write(
    '[factory-counted-sdk] ' + JSON.stringify({ phase, pid: process.pid, ...values }) + '\n'
  );
}

mock.module(import.meta.resolve('@openai/codex-sdk'), () => ({
  Codex: class CountingCodex {
    constructor(options: { config?: Record<string, unknown> } = {}) {
      trace('constructed', { permissionsProfile: options.config?.default_permissions ?? null });
    }
    private thread(
      options: { model?: string; sandboxMode?: string } = {},
      id: string = randomUUID()
    ): object {
      return {
        id,
        async runStreamed(): Promise<{ events: AsyncGenerator<Record<string, unknown>> }> {
          trace('stream-open', {
            model: options.model ?? null,
            sandboxMode: options.sandboxMode ?? null,
          });
          return {
            events: (async function* (): AsyncGenerator<Record<string, unknown>> {
              yield { type: 'thread.started', thread_id: id };
              yield {
                type: 'turn.completed',
                usage: {
                  input_tokens: 1,
                  cached_input_tokens: 0,
                  cache_write_input_tokens: 0,
                  output_tokens: 1,
                  reasoning_output_tokens: 0,
                },
              };
              // Exhausting this explicit SDK fixture is its transport-close proof.
              // The real pinned Codex SDK instead awaits native process exit.
              trace('transport-closed', { sessionId: id });
            })(),
          };
        },
      };
    }
    startThread(options?: { model?: string; sandboxMode?: string }): object {
      return this.thread(options);
    }
    resumeThread(id: string, options?: { model?: string; sandboxMode?: string }): object {
      return this.thread(options, id);
    }
  },
}));
