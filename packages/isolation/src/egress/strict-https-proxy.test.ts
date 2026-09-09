import { describe, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'fs/promises';
import { join } from 'path';

const NODE_RUNNER_TIMEOUT_MS = 20_000;

describe('strict HTTPS CONNECT proxy', () => {
  test(
    'enforces decrypted HTTPS request grants under real Node TLS',
    async () => {
      const temp = await mkdtemp(join('/tmp', 'as-bundle-'));
      try {
        const bundle = join(temp, 'strict-https-proxy.node-runner.mjs');
        const source = join(import.meta.dir, 'strict-https-proxy.node-runner.ts');
        const build = await runProcess(
          ['bun', 'build', source, '--target=node', '--outfile', bundle],
          temp
        );
        expect(build.exitCode, build.stderr).toBe(0);

        const result = await runProcess(['node', bundle], temp);
        expect(result.exitCode, result.stderr).toBe(0);
        expect(result.stdout).toContain(
          'ok allows approved GET and strips proxy hop-by-hop headers'
        );
        expect(result.stdout).toContain('ok allows approved POST body within grant bound');
        expect(result.stdout).toContain(
          'ok accounts trusted provider requests before upstream and settles terminal usage'
        );
        expect(result.stdout).toContain(
          'ok streams accounted SSE bytes before terminal settlement'
        );
        expect(result.stdout).toContain(
          'ok serializes two accounting lifecycles and rejects a third waiter'
        );
        expect(result.stdout).toContain(
          'ok removes an aborted accounting waiter without reserving tokens'
        );
        expect(result.stdout).toContain(
          'ok preserves fragmented UTF-8 SSE bytes and supports CRLF terminal boundaries'
        );
        expect(result.stdout).toContain(
          'ok bounds terminal SSE hold and incomplete frame buffering'
        );
        expect(result.stdout).toContain(
          'ok holds reservation when downstream cancels before provider terminal event'
        );
        expect(result.stdout).toContain(
          'ok holds reservation when downstream cancels while settlement is pending'
        );
        expect(result.stdout).toContain(
          'ok terminal settlement owns the lifecycle while downstream closes'
        );
        expect(result.stdout).toContain(
          'ok holds reservations on unknown provider completion and blocks the chain'
        );
        expect(result.stdout).toContain(
          'ok accounts Anthropic cache usage and rejects interrupted or contradictory streams'
        );
        expect(result.stdout).toContain('ok preserves HEAD and no-body response framing');
        expect(result.stdout).toContain(
          'ok separates fragmented HTTP header bounds from allowed body bytes'
        );
        expect(result.stdout).toContain(
          'ok releases partial upload readers immediately on client cancellation'
        );
        expect(result.stdout).toContain(
          'ok bounds response buffering while the downstream client is paused'
        );
        expect(result.stdout).toContain(
          'ok rejects mismatched SNI, Host, port, denied path, traversal, and upgrades'
        );
        expect(result.stdout).toContain(
          'ok rejects private DNS, bad upstream certificate, missing config, large headers/body, and frees slots'
        );
        expect(result.stdout).toContain('STRICT_HTTPS_FIXTURE=PASS');
      } finally {
        await rm(temp, { recursive: true, force: true });
      }
    },
    NODE_RUNNER_TIMEOUT_MS + 5_000
  );
});

async function runProcess(
  command: string[],
  home: string
): Promise<{ exitCode: number | null; stdout: string; stderr: string }> {
  const proc = Bun.spawn(command, {
    env: {
      HOME: home,
      NODE_TLS_REJECT_UNAUTHORIZED: '1',
      PATH: process.env.PATH ?? '',
      TMPDIR: '/tmp',
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const timeout = setTimeout(() => proc.kill(), NODE_RUNNER_TIMEOUT_MS);
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  clearTimeout(timeout);
  return { exitCode, stdout, stderr };
}
