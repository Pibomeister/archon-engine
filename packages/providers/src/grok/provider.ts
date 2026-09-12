import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type {
  IAgentProvider,
  MessageChunk,
  ProviderCapabilities,
  SendQueryOptions,
} from '../types';
import { factoryGrokSandboxToml } from '../factory-sandbox';
import { GROK_CAPABILITIES } from './capabilities';
import { parseGrokConfig } from './config';
import { resolveGrokBinaryPath } from './binary-resolver';
import { buildGrokFactoryArgv } from './factory-argv';
import { runGrokCommand, type GrokCommandRunner } from './factory-transport';
import { parseGrokStreamingLine } from './stream';

export class GrokProvider implements IAgentProvider {
  constructor(private readonly runner: GrokCommandRunner = runGrokCommand) {}

  getType(): string {
    return 'grok';
  }

  getCapabilities(): ProviderCapabilities {
    return GROK_CAPABILITIES;
  }

  async *sendQuery(
    prompt: string,
    cwd: string,
    resumeSessionId?: string,
    options?: SendQueryOptions
  ): AsyncGenerator<MessageChunk> {
    if (!options?.factoryScope) throw new Error('grok_factory_scope_required');
    if (!options.model) throw new Error('factory_provider_invocation_required');
    const config = parseGrokConfig(options.assistantConfig ?? {});
    const command = resolveGrokBinaryPath(config.grokBinaryPath);
    const env = factoryGrokEnv(options.env);
    const grokHome = env.GROK_HOME;
    if (!grokHome) throw new Error('grok_home_required');
    if (options.factoryScope.writableRoots.some(root => pathInside(root, grokHome))) {
      throw new Error('grok_home_in_write_root');
    }
    writeFactoryHome(grokHome, factoryGrokSandboxToml(options.factoryScope, cwd));
    const argv = buildGrokFactoryArgv({
      command,
      prompt,
      cwd,
      model: options.model,
      scope: options.factoryScope,
      ...(resumeSessionId ? { resumeSessionId } : {}),
      grokHome,
    });
    const lines: string[] = [];
    let finished = false;
    let notify: (() => void) | undefined;
    const wait = (): Promise<void> =>
      new Promise(resolve => {
        notify = resolve;
      });
    const abort = new AbortController();
    const signal = options.abortSignal
      ? AbortSignal.any([options.abortSignal, abort.signal])
      : abort.signal;
    let nativeClosed = false;
    const running = this.runner({
      argv,
      cwd,
      env,
      signal,
      onLine(line) {
        lines.push(line);
        notify?.();
      },
    })
      .then(result => {
        nativeClosed = result.nativeClosed;
        return result;
      })
      .finally(() => {
        finished = true;
        notify?.();
      });
    const bufferedResults: MessageChunk[] = [];
    try {
      while (!finished || lines.length > 0) {
        if (lines.length === 0) await wait();
        const line = lines.shift();
        if (line === undefined) continue;
        let chunk: MessageChunk | undefined;
        try {
          chunk = parseGrokStreamingLine(line);
        } catch (error) {
          abort.abort();
          bufferedResults.push({
            type: 'result',
            isError: true,
            errors: [error instanceof Error ? error.message : 'grok_stream_json_invalid'],
          });
          break;
        }
        if (!chunk) continue;
        if (chunk.type === 'result') bufferedResults.push(chunk);
        else yield chunk;
      }
    } finally {
      if (!finished) abort.abort();
      const result = await running.catch((error: unknown) => ({
        exitCode: 1,
        stdout: '',
        stderr: error instanceof Error ? error.message : String(error),
        nativeClosed: false,
      }));
      if (nativeClosed) options.factoryTransportClosed?.();
      const terminal = terminalResult(bufferedResults, result.exitCode, result.stderr);
      if (terminal) yield terminal;
    }
  }
}

export function factoryGrokEnv(optionsEnv?: Record<string, string>): NodeJS.ProcessEnv {
  const grokHome = optionsEnv?.GROK_HOME ?? process.env.GROK_HOME;
  const home = optionsEnv?.HOME ?? grokHome;
  const env: NodeJS.ProcessEnv = {
    PATH: optionsEnv?.PATH ?? process.env.PATH ?? '/usr/bin:/bin',
    TMPDIR: optionsEnv?.TMPDIR ?? '/tmp',
    LANG: optionsEnv?.LANG ?? process.env.LANG ?? 'C.UTF-8',
    LC_ALL: optionsEnv?.LC_ALL ?? process.env.LC_ALL ?? 'C.UTF-8',
  };
  if (home) env.HOME = home;
  if (grokHome) env.GROK_HOME = grokHome;
  env.GROK_DISABLE_API_KEY_AUTH = '1';
  env.GROK_CLAUDE_MCPS_ENABLED = '0';
  env.GROK_CURSOR_MCPS_ENABLED = '0';
  return env;
}

function terminalResult(
  buffered: MessageChunk[],
  exitCode: number,
  stderr: string
): MessageChunk | undefined {
  if (exitCode !== 0) {
    return {
      type: 'result',
      isError: true,
      errors: [stderr.trim() || `grok_exit_${exitCode}`],
    };
  }
  const last = [...buffered].reverse().find(chunk => chunk.type === 'result');
  return last ?? { type: 'result', isError: true, errors: ['grok_stream_incomplete'] };
}

function pathInside(parent: string, child: string): boolean {
  return child === parent || child.startsWith(parent.endsWith('/') ? parent : `${parent}/`);
}

function writeFactoryHome(home: string, sandboxBody: string): void {
  mkdirSync(home, { recursive: true, mode: 0o700 });
  writeFileSync(join(home, 'sandbox.toml'), sandboxBody, { encoding: 'utf8', mode: 0o600 });
  writeFileSync(
    join(home, 'config.toml'),
    '[compat.claude]\nmcps = false\n\n[compat.cursor]\nmcps = false\n',
    { encoding: 'utf8', mode: 0o600 }
  );
}
