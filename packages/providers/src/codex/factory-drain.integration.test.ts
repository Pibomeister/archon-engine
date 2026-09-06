import { expect, spyOn, test } from 'bun:test';
import { Codex } from '@openai/codex-sdk';
import { mkdtemp, writeFile, chmod } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { trackTempRoots } from '@archon/paths/test-utils';
import { CodexProvider, resetCodexSingleton } from './provider';
import * as binaryResolver from './binary-resolver';

const temp = trackTempRoots();

test('real SDK terminal event and stdout EOF cannot release before native process exit', async () => {
  const root = await mkdtemp(join(tmpdir(), 'codex-native-drain-'));
  temp(root);
  const executable = join(root, 'fake-codex');
  const gate = join(root, 'exit-allowed');
  await writeFile(
    executable,
    `#!/bin/sh
printf '%s\n' '{"type":"thread.started","thread_id":"00000000-0000-4000-8000-000000000001"}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":1,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0}}'
exec 1>&-
while [ ! -e "$FACTORY_TEST_CODEX_EXIT_GATE" ]; do /bin/sleep 0.01; done
exit 0
`
  );
  await chmod(executable, 0o700);
  const resolver = spyOn(binaryResolver, 'resolveCodexBinaryPath').mockResolvedValue(executable);
  resetCodexSingleton();
  let observed!: () => void;
  const terminalObserved = new Promise<void>(accept => {
    observed = accept;
  });
  const start = Codex.prototype.startThread;
  // Observe, but do not replace, the pinned SDK's real subprocess event stream.
  const observer = spyOn(Codex.prototype, 'startThread').mockImplementation(function (
    this: Codex,
    options
  ) {
    // Guard the actual pinned SDK object BEFORE creating its thread/stream.
    // Source/dev mode deliberately ignores CODEX_BIN_PATH, so never rely on it.
    const native = this as unknown as { exec: { executablePath: string } };
    if (native.exec.executablePath !== executable)
      throw new Error('fixture_refuses_real_provider_binary');
    const thread = start.call(this, options);
    const stream = thread.runStreamed.bind(thread);
    thread.runStreamed = async (...args) => {
      const result = await stream(...args);
      return {
        events: (async function* () {
          for await (const event of result.events) {
            if (event.type === 'turn.completed') observed();
            yield event;
          }
        })(),
      };
    };
    return thread;
  });
  let closed = false;
  let delivered = false;
  const iterator = new CodexProvider({ retryBaseDelayMs: 1 }).sendQuery(
    'No model; native transport fixture',
    root,
    undefined,
    {
      model: 'fixture-model',
      env: {
        HOME: root,
        CODEX_HOME: root,
        FACTORY_TEST_CODEX_EXIT_GATE: gate,
        OPENAI_API_KEY: '',
        CODEX_API_KEY: '',
        OPENAI_BASE_URL: 'http://127.0.0.1:9',
      },
      factoryTransportClosed: () => {
        closed = true;
      },
    }
  );
  const result = iterator.next().then(value => {
    delivered = true;
    return value;
  });
  try {
    await Promise.race([
      terminalObserved,
      result.then(() => {
        throw new Error('fixture_ended_before_observation');
      }),
    ]);
    await new Promise<void>(accept => {
      setImmediate(accept);
    });
    expect(delivered).toBe(false);
    expect(closed).toBe(false);
    await writeFile(gate, 'exit');
    expect((await result).value).toMatchObject({ type: 'result' });
    expect(closed).toBe(true);
  } finally {
    await writeFile(gate, 'exit');
    await result.catch(() => undefined);
    await iterator.return(undefined);
    observer.mockRestore();
    resolver.mockRestore();
    resetCodexSingleton();
  }
}, 15000);
