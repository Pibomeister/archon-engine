/**
 * Claude-in-container spawn hook.
 *
 * Implements the Claude Agent SDK's `spawnClaudeCodeProcess` option so the CLI
 * runs INSIDE a prepared isolation container via `docker exec -i`, rather than
 * on the host. With this hook set, Archon does not resolve or execute a host
 * Claude binary for container runs; the SDK drives the returned
 * {@link SpawnedProcess} over stdin/stdout exactly
 * as it would a local child.
 *
 * `child.kill()` signals only the LOCAL `docker exec` client, which does NOT
 * reliably forward the signal to the process inside the container (docker/cli#2607).
 * Provider-level abort/deadline handling therefore fail-stops the exact owned
 * per-run container and awaits that cleanup. Routine SDK close/kill paths stay
 * local-only so normal successful completion does not stop the run container.
 *
 * v1 is Claude-only. Codex/Pi/community providers latch on here by implementing
 * their own `ExecutionContext`-aware spawn/transport (see the provider support
 * matrix in the plan) — the `containerExec` capability + the engine's
 * pre-dispatch fail-fast are the extension seam.
 */

import { execFile, spawn, type ChildProcess } from 'child_process';
import { isIP } from 'node:net';
import { promisify } from 'util';
import type { SpawnOptions, SpawnedProcess } from '@anthropic-ai/claude-agent-sdk';
import type { ExecutionContext } from '../types';
import { createLogger } from '@archon/paths';

/**
 * Process spawner, injectable so `buildContainerSpawn` can be unit-tested with a
 * fake child (DI, not `mock.module` — keeps this out of a mock-pollution batch).
 * Defaults to `child_process.spawn`.
 */
export type Spawner = (
  command: string,
  args: string[],
  options: { stdio: unknown }
) => ChildProcess;

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.claude.container');
  return cachedLog;
}

/**
 * In-container Claude binary. Resolved via the runner image's PATH
 * (/root/.local/bin), overridable for non-standard images.
 */
const execFileAsync = promisify(execFile);

function containerClaudeBin(): string {
  return process.env.ARCHON_CONTAINER_CLAUDE_BIN ?? 'claude';
}

const DOCKER_EXEC_CLAUDE_ENV_ALLOWLIST: ReadonlySet<string> = new Set([
  'ANTHROPIC_API_KEY',
  'ANTHROPIC_OAUTH_TOKEN',
  'CLAUDE_CODE_DISABLE_1M_CONTEXT',
  'CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS',
  'CLAUDE_CODE_OAUTH_TOKEN',
  'TERM',
]);

const ANTHROPIC_PROVIDER_NAMES: ReadonlySet<string> = new Set(['anthropic', 'claude']);

function isDnsHostname(hostname: string): boolean {
  if (hostname.length === 0 || hostname.length > 253) return false;
  const labels = hostname.split('.');
  return labels.every(label => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(label));
}

function normalizeSealedAnthropicBaseUrl(rawBaseUrl: string): string {
  let url: URL;
  try {
    url = new URL(rawBaseUrl);
  } catch {
    throw new Error('Claude hardened provider origin is malformed.');
  }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
    throw new Error('Claude hardened provider origin must be an exact HTTPS API root.');
  }
  if (url.pathname !== '/') {
    throw new Error('Claude hardened provider origin must be the HTTPS API origin root.');
  }
  const hostname = url.hostname.replace(/^\[|\]$/g, '');
  if (hostname.toLowerCase() === 'localhost' || isIP(hostname) !== 0 || !isDnsHostname(hostname)) {
    throw new Error('Claude hardened provider origin must use a non-local DNS hostname.');
  }
  return url.origin;
}

function requiredSealedAnthropicBaseUrl(
  execContext: Extract<ExecutionContext, { kind: 'container' }>
): string {
  const matches = (execContext.providerOrigins ?? []).filter(origin =>
    ANTHROPIC_PROVIDER_NAMES.has(origin.provider.toLowerCase())
  );
  if (matches.length !== 1) {
    throw new Error(
      'Claude container execution requires exactly one sealed Anthropic provider origin.'
    );
  }
  return normalizeSealedAnthropicBaseUrl(matches[0].baseUrl);
}

function shouldForwardContainerEnv(
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  key: string,
  value: string | undefined
): value is string {
  if (value === undefined) return false;
  if (key === 'ANTHROPIC_BASE_URL') return value === requiredSealedAnthropicBaseUrl(execContext);
  return DOCKER_EXEC_CLAUDE_ENV_ALLOWLIST.has(key);
}

/**
 * Build `docker exec` argv for running Claude inside the container. Exported for
 * unit testing the argument construction without spawning a process.
 *
 * The Claude executable path is passed as a positional argv value and invoked as
 * `exec "$1"` so an overridden in-container path is never shell-interpolated.
 * The SDK args ride `"$@"`, so they are passed as argv too.
 */
export function buildDockerExecArgs(
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  options: SpawnOptions,
  trustedEnv: NodeJS.ProcessEnv
): string[] {
  const args = ['exec', '-i'];
  if (execContext.execUser) args.push('-u', execContext.execUser);
  if (options.cwd) args.push('-w', options.cwd);
  for (const [key, value] of Object.entries(trustedEnv)) {
    if (!shouldForwardContainerEnv(execContext, key, value)) continue;
    args.push('-e', `${key}=${value}`);
  }
  args.push(
    execContext.containerId,
    'sh',
    '-c',
    'claude_bin="$1"; shift; exec "$claude_bin" "$@"',
    'archon-claude-wrapper',
    containerClaudeBin(),
    ...options.args
  );
  return args;
}

/**
 * Awaited provider-level cancellation primitive: stop the exact controller-owned
 * per-run container. This intentionally does not accept a pidfile or process
 * selector from inside the container; untrusted workloads can write those.
 */
export async function stopContainerOnAbort(
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  reason: 'abort' | 'deadline'
): Promise<void> {
  try {
    await execFileAsync('docker', ['stop', '--time', '2', execContext.containerId], {
      timeout: 10_000,
      maxBuffer: 1024 * 1024,
    });
    getLog().warn({ containerId: execContext.containerId, reason }, 'claude.container_stopped');
  } catch (err) {
    getLog().error(
      { containerId: execContext.containerId, reason, err },
      'claude.container_stop_failed'
    );
    throw new Error(
      `Failed to stop Claude container '${execContext.containerId}' after ${reason}: ${
        err instanceof Error ? err.message : String(err)
      }`
    );
  }
}

/**
 * Create the SDK spawn hook for a container execution context.
 *
 * The returned function spawns `docker exec -i` for each SDK-driven Claude run
 * and wraps the resulting child as a {@link SpawnedProcess}: stdio piped
 * (force-pipe is inherent to `-i` with no `-t`), stderr inherited for
 * visibility, and routine `kill()` limited to the local docker-exec client.
 * Provider-level abort/deadline handling owns fail-stopping the per-run
 * container; this hook does not trust in-container pidfiles or selectors.
 */
export function buildContainerSpawn(
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  trustedEnv: NodeJS.ProcessEnv,
  spawnFn: Spawner = spawn as unknown as Spawner
): (options: SpawnOptions) => SpawnedProcess {
  const immutableTrustedEnv = Object.freeze({ ...trustedEnv });
  return (options: SpawnOptions): SpawnedProcess => {
    const dockerArgs = buildDockerExecArgs(execContext, options, immutableTrustedEnv);
    getLog().debug(
      { containerId: execContext.containerId, argc: options.args.length },
      'claude.container_spawn_started'
    );

    // stderr inherited: the SpawnedProcess contract exposes only stdin/stdout, so
    // the SDK can't read the child's stderr — inheriting surfaces container-side
    // Claude errors in Archon's own stderr instead of swallowing them. The
    // 'pipe','pipe','inherit' stdio makes stdin/stdout non-null.
    const child = spawnFn('docker', dockerArgs, {
      stdio: ['pipe', 'pipe', 'inherit'],
    });

    // The 'pipe','pipe' stdio guarantees stdin/stdout are present; guard rather
    // than assert so a misbehaving spawner fails loudly instead of NPE-ing later.
    if (!child.stdin || !child.stdout) {
      throw new Error('docker exec child is missing piped stdin/stdout');
    }

    const wrapped: SpawnedProcess = {
      stdin: child.stdin,
      stdout: child.stdout,
      get killed(): boolean {
        return child.killed;
      },
      get exitCode(): number | null {
        return child.exitCode;
      },
      kill(signal: NodeJS.Signals = 'SIGTERM'): boolean {
        return child.kill(signal);
      },
      on(event: 'exit' | 'error', listener: (...eventArgs: never[]) => void): void {
        child.on(event, listener as never);
      },
      once(event: 'exit' | 'error', listener: (...eventArgs: never[]) => void): void {
        child.once(event, listener as never);
      },
      off(event: 'exit' | 'error', listener: (...eventArgs: never[]) => void): void {
        child.off(event, listener as never);
      },
    };

    return wrapped;
  };
}
