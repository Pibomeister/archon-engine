import { describe, test, expect } from 'bun:test';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';
import type { ChildProcess } from 'child_process';
import type { SpawnOptions } from '@anthropic-ai/claude-agent-sdk';
import { buildDockerExecArgs, buildContainerSpawn, type Spawner } from './container-spawn';

const CTX = {
  kind: 'container' as const,
  profile: 'hardened' as const,
  containerId: 'cid-123',
  providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test' }],
};

function makeSpawnOptions(over: Partial<SpawnOptions> = {}): SpawnOptions {
  return {
    command: '/host/claude',
    args: ['--output-format', 'stream-json', '--verbose'],
    cwd: '/tmp/ops-client',
    env: {
      ANTHROPIC_API_KEY: 'sk-test',
      ANTHROPIC_BASE_URL: 'https://api.anthropic.test',
      CLAUDE_CODE_DISABLE_1M_CONTEXT: '1',
      CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS: '1',
      HTTPS_PROXY: 'http://127.0.0.1:18080',
      HTTP_PROXY: 'http://127.0.0.1:18080',
      NODE_EXTRA_CA_CERTS: '/run/archon/ca.pem',
      SSL_CERT_FILE: '/run/archon/ca.pem',
      NO_PROXY: 'localhost,127.0.0.1,::1',
      ARTIFACTS_DIR: '/a',
      CODEBASE_VAR: 'x',
      GH_TOKEN: 'gh-secret',
      GITHUB_TOKEN: 'github-secret',
      DATABASE_URL: 'postgres://secret',
      PGHOST: 'db.local',
      AWS_SECRET_ACCESS_KEY: 'aws-secret',
      AWS_SESSION_TOKEN: 'aws-session',
      DOCKER_HOST: 'tcp://docker',
      LD_PRELOAD: '/tmp/hook.so',
      NODE_OPTIONS: '--require /tmp/hook.js',
      CLAUDE_API_KEY: 'sk-host-alias',
      PATH: '/host/bin',
      HOME: '/Users/x',
    },
    signal: new AbortController().signal,
    ...over,
  };
}

/** Fake ChildProcess with real streams + a recording kill(). */
function fakeChild(): ChildProcess & { killSignals: string[] } {
  const emitter = new EventEmitter() as ChildProcess & { killSignals: string[] };
  emitter.stdin = new PassThrough() as unknown as ChildProcess['stdin'];
  emitter.stdout = new PassThrough() as unknown as ChildProcess['stdout'];
  emitter.killed = false;
  emitter.exitCode = null;
  emitter.killSignals = [];
  emitter.kill = ((signal?: NodeJS.Signals) => {
    emitter.killSignals.push(signal ?? 'SIGTERM');
    return true;
  }) as ChildProcess['kill'];
  return emitter;
}

/** Recording spawner returning a fresh fake child per call. */
function recordingSpawner(): Spawner & {
  calls: { command: string; args: string[] }[];
  children: ChildProcess[];
} {
  const calls: { command: string; args: string[] }[] = [];
  const children: ChildProcess[] = [];
  const fn = ((command: string, args: string[]) => {
    calls.push({ command, args });
    const child = fakeChild();
    children.push(child);
    return child;
  }) as Spawner & { calls: typeof calls; children: ChildProcess[] };
  fn.calls = calls;
  fn.children = children;
  return fn;
}

describe('buildDockerExecArgs', () => {
  test('builds docker exec -i with cwd, env flags, container id, pid-wrapped claude, and args', () => {
    const args = buildDockerExecArgs(CTX, makeSpawnOptions(), makeSpawnOptions().env);
    expect(args.slice(0, 2)).toEqual(['exec', '-i']);
    expect(args).toContain('-w');
    const wIdx = args.indexOf('-w');
    expect(args[wIdx + 1]).toBe('/tmp/ops-client');
    expect(args).toContain('-e');
    expect(args).toContain('ANTHROPIC_API_KEY=sk-test');
    expect(args).toContain('ANTHROPIC_BASE_URL=https://api.anthropic.test');
    expect(args).toContain('CLAUDE_CODE_DISABLE_1M_CONTEXT=1');
    expect(args).toContain('CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS=1');
    // container id, then the argv-safe shell wrapper, then wrapper $0 + executable + SDK args.
    const cidIdx = args.indexOf('cid-123');
    expect(cidIdx).toBeGreaterThan(-1);
    expect(args[cidIdx + 1]).toBe('sh');
    expect(args[cidIdx + 2]).toBe('-c');
    expect(args[cidIdx + 3]).toBe('claude_bin="$1"; shift; exec "$claude_bin" "$@"');
    expect(args[cidIdx + 4]).toBe('archon-claude-wrapper');
    expect(args[cidIdx + 5]).toBe('claude');
    expect(args.slice(cidIdx + 6)).toEqual(['--output-format', 'stream-json', '--verbose']);
  });

  test('filters ambient and project env at the final docker exec boundary', () => {
    const args = buildDockerExecArgs(CTX, makeSpawnOptions(), makeSpawnOptions().env);
    const joined = args.join(' ');
    expect(joined).not.toContain('PATH=/host/bin');
    expect(joined).not.toContain('HOME=/Users/x');
    expect(joined).not.toContain('HTTPS_PROXY=http://127.0.0.1:18080');
    expect(joined).not.toContain('HTTP_PROXY=http://127.0.0.1:18080');
    expect(joined).not.toContain('NODE_EXTRA_CA_CERTS=/run/archon/ca.pem');
    expect(joined).not.toContain('SSL_CERT_FILE=/run/archon/ca.pem');
    expect(joined).not.toContain('NO_PROXY=localhost,127.0.0.1,::1');
    expect(joined).not.toContain('ARTIFACTS_DIR=/a');
    expect(joined).not.toContain('CODEBASE_VAR=x');
    expect(joined).not.toContain('GH_TOKEN=gh-secret');
    expect(joined).not.toContain('GITHUB_TOKEN=github-secret');
    expect(joined).not.toContain('DATABASE_URL=postgres://secret');
    expect(joined).not.toContain('PGHOST=db.local');
    expect(joined).not.toContain('AWS_SECRET_ACCESS_KEY=aws-secret');
    expect(joined).not.toContain('AWS_SESSION_TOKEN=aws-session');
    expect(joined).not.toContain('DOCKER_HOST=tcp://docker');
    expect(joined).not.toContain('LD_PRELOAD=/tmp/hook.so');
    expect(joined).not.toContain('NODE_OPTIONS=--require /tmp/hook.js');
    expect(joined).not.toContain('CLAUDE_API_KEY=sk-host-alias');
  });

  test('does not forward off-policy Anthropic origins at the final docker exec boundary', () => {
    const args = buildDockerExecArgs(
      CTX,
      makeSpawnOptions({ env: { ANTHROPIC_BASE_URL: 'https://evil.example' } }),
      makeSpawnOptions({ env: { ANTHROPIC_BASE_URL: 'https://evil.example' } }).env
    );
    expect(args.join(' ')).not.toContain('ANTHROPIC_BASE_URL=');
  });

  test('rejects unsealed Anthropic origins at the final docker exec boundary', () => {
    expect(() =>
      buildDockerExecArgs(
        { ...CTX, providerOrigins: undefined },
        makeSpawnOptions({ env: { ANTHROPIC_BASE_URL: 'https://api.anthropic.test' } }),
        makeSpawnOptions({ env: { ANTHROPIC_BASE_URL: 'https://api.anthropic.test' } }).env
      )
    ).toThrow('Claude container execution requires exactly one sealed Anthropic provider origin');
  });

  test('rejects path-scoped sealed Anthropic origins at the final docker exec boundary', () => {
    expect(() =>
      buildDockerExecArgs(
        {
          ...CTX,
          providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test/v1' }],
        },
        makeSpawnOptions({ env: { ANTHROPIC_BASE_URL: 'https://api.anthropic.test/v1' } }),
        makeSpawnOptions({ env: { ANTHROPIC_BASE_URL: 'https://api.anthropic.test/v1' } }).env
      )
    ).toThrow('Claude hardened provider origin must be the HTTPS API origin root');
  });

  test('adds -u when execUser is set', () => {
    const args = buildDockerExecArgs(
      { ...CTX, execUser: '1001' },
      makeSpawnOptions(),
      makeSpawnOptions().env
    );
    const uIdx = args.indexOf('-u');
    expect(uIdx).toBeGreaterThan(-1);
    expect(args[uIdx + 1]).toBe('1001');
  });

  test('passes overridden Claude executable as a literal argv, not shell source', () => {
    const previous = process.env.ARCHON_CONTAINER_CLAUDE_BIN;
    process.env.ARCHON_CONTAINER_CLAUDE_BIN = '/tmp/fake claude; touch /tmp/pwned';
    try {
      const args = buildDockerExecArgs(CTX, makeSpawnOptions(), makeSpawnOptions().env);
      const cidIdx = args.indexOf('cid-123');
      expect(args[cidIdx + 3]).toBe('claude_bin="$1"; shift; exec "$claude_bin" "$@"');
      expect(args[cidIdx + 5]).toBe('/tmp/fake claude; touch /tmp/pwned');
    } finally {
      if (previous === undefined) delete process.env.ARCHON_CONTAINER_CLAUDE_BIN;
      else process.env.ARCHON_CONTAINER_CLAUDE_BIN = previous;
    }
  });
});

describe('buildContainerSpawn — SpawnedProcess contract', () => {
  test('spawns docker exec and exposes the child stdio', () => {
    const spawner = recordingSpawner();
    const proc = buildContainerSpawn(CTX, makeSpawnOptions().env, spawner)(makeSpawnOptions());
    expect(spawner.calls[0]?.command).toBe('docker');
    expect(spawner.calls[0]?.args.slice(0, 2)).toEqual(['exec', '-i']);
    expect(proc.stdin).toBe(spawner.children[0]?.stdin as never);
    expect(proc.stdout).toBe(spawner.children[0]?.stdout as never);
  });

  test('ignores SDK callback env drift at the final docker exec boundary', () => {
    const spawner = recordingSpawner();
    buildContainerSpawn(
      CTX,
      makeSpawnOptions().env,
      spawner
    )(
      makeSpawnOptions({
        env: {
          ANTHROPIC_API_KEY: 'sk-drift',
          ANTHROPIC_BASE_URL: 'https://evil.example',
          HTTPS_PROXY: 'http://evil-proxy',
          GH_TOKEN: 'gh-drift',
        },
      })
    );
    const joined = spawner.calls[0]?.args.join(' ') ?? '';
    expect(joined).toContain('ANTHROPIC_API_KEY=sk-test');
    expect(joined).toContain('ANTHROPIC_BASE_URL=https://api.anthropic.test');
    expect(joined).not.toContain('sk-drift');
    expect(joined).not.toContain('evil.example');
    expect(joined).not.toContain('evil-proxy');
    expect(joined).not.toContain('gh-drift');
  });

  test('kill() tears down only the local docker exec client on routine SDK close', () => {
    const spawner = recordingSpawner();
    const proc = buildContainerSpawn(CTX, makeSpawnOptions().env, spawner)(makeSpawnOptions());
    proc.kill('SIGTERM');
    expect(spawner.calls).toHaveLength(1);
    expect(spawner.calls.some(c => c.args.includes('pkill'))).toBe(false);
    expect(spawner.calls.some(c => c.args.some(arg => arg.includes('kill -TERM')))).toBe(false);
    const mainChild = spawner.children[0] as ChildProcess & { killSignals: string[] };
    expect(mainChild.killSignals).toContain('SIGTERM');
  });

  test('propagates the exit event from the child', () => {
    const spawner = recordingSpawner();
    const proc = buildContainerSpawn(CTX, makeSpawnOptions().env, spawner)(makeSpawnOptions());
    let exitCode: number | null = -999;
    proc.on('exit', code => {
      exitCode = code;
    });
    (spawner.children[0] as EventEmitter).emit('exit', 0, null);
    expect(exitCode).toBe(0);
  });

  test('does not stop the run container directly from the spawn hook on abort', () => {
    const controller = new AbortController();
    const spawner = recordingSpawner();
    buildContainerSpawn(
      CTX,
      makeSpawnOptions().env,
      spawner
    )(makeSpawnOptions({ signal: controller.signal }));
    controller.abort();
    expect(spawner.calls).toHaveLength(1);
  });
});
