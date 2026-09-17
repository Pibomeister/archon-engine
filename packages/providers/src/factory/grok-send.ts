import type { MessageChunk, SendQueryOptions } from '../types';
import { resolveGrokBinaryPath } from '../community/grok/binary-resolver';
import { parseGrokConfig } from '../community/grok/config';
import { parseGrokOutput } from '../community/grok/stream';
import { factoryGrokSandboxToml } from '../factory-sandbox';
import { buildGrokFactoryArgv } from './grok-argv';
import { factoryGrokEnv, pathInside, writeFactoryHome } from './grok-home';
import { type GrokCommandRunner } from './grok-transport';

export async function* sendFactoryGrokQuery(
  runner: GrokCommandRunner,
  prompt: string,
  cwd: string,
  resumeSessionId: string | undefined,
  options: SendQueryOptions
): AsyncGenerator<MessageChunk> {
  const scope = options.factoryScope;
  if (!scope) throw new Error('grok_factory_scope_required');
  if (!options.model) throw new Error('factory_provider_invocation_required');
  const config = parseGrokConfig(options.assistantConfig ?? {});
  const command = resolveGrokBinaryPath(config.grokBinaryPath);
  const env = factoryGrokEnv(options.env);
  const grokHome = env.GROK_HOME;
  if (!grokHome) throw new Error('grok_home_required');
  if (scope.writableRoots.some(root => pathInside(root, grokHome))) {
    throw new Error('grok_home_in_write_root');
  }
  writeFactoryHome(grokHome, factoryGrokSandboxToml(scope, cwd));
  const argv = buildGrokFactoryArgv({
    command,
    prompt,
    cwd,
    model: options.model,
    scope,
    ...(resumeSessionId ? { resumeSessionId } : {}),
    grokHome,
  });
  const abort = new AbortController();
  const signal = options.abortSignal
    ? AbortSignal.any([options.abortSignal, abort.signal])
    : abort.signal;
  const lines: string[] = [];
  let notify: (() => void) | undefined;
  let finished = false;
  const running = runner({
    argv,
    cwd,
    env,
    signal,
    onLine(line) {
      lines.push(line);
      notify?.();
    },
  }).finally(() => {
    finished = true;
    notify?.();
  });
  try {
    while (!finished || lines.length > 0) {
      if (lines.length === 0) {
        await new Promise<void>(resolve => {
          notify = resolve;
        });
      }
      const line = lines.shift();
      if (line === undefined) continue;
      for (const chunk of parseGrokOutput(line)) {
        yield chunk;
      }
    }
  } finally {
    abort.abort();
  }
  const result = await running;
  if (result.nativeClosed) options.factoryTransportClosed?.();
  if (result.exitCode !== 0) {
    yield {
      type: 'result',
      isError: true,
      errors: [result.stderr.trim() || `grok_exit_${result.exitCode}`],
    };
  }
}
