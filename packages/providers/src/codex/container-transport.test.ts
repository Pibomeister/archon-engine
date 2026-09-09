import { describe, test, expect } from 'bun:test';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { CodexProvider, resetCodexSingleton } from './provider';

async function withFakeDocker<T>(script: string, run: (logPath: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), 'archon-codex-docker-fixture-'));
  const dockerPath = join(dir, 'docker');
  const logPath = join(dir, 'docker.args');
  const oldPath = process.env.PATH;
  await writeFile(dockerPath, script.replaceAll('__LOG__', logPath), { mode: 0o700 });
  await chmod(dockerPath, 0o700);
  process.env.PATH = `${dir}:${oldPath ?? ''}`;
  try {
    return await run(logPath);
  } finally {
    process.env.PATH = oldPath;
    resetCodexSingleton();
    await rm(dir, { recursive: true, force: true });
  }
}

const SEALED_OPENAI_EXEC_CONTEXT = {
  kind: 'container' as const,
  profile: 'hardened' as const,
  containerId: 'cid-1',
  providerOrigins: [{ provider: 'openai', baseUrl: 'https://api.openai.com/v1' }],
};

const STREAMING_FAKE_DOCKER = `#!/bin/sh
printf '%s\n' "$@" > '__LOG__'
printf '%s\n' '{"type":"thread.started","thread_id":"fixture-thread"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"m1","type":"agent_message","text":"fixture ok"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":1,"cached_input_tokens":0,"output_tokens":2,"reasoning_output_tokens":0}}'
`;

describe('CodexProvider container transport protocol fixture', () => {
  test('spawns the SDK through docker exec and streams JSONL without a real Codex request', async () => {
    await withFakeDocker(STREAMING_FAKE_DOCKER, async logPath => {
      const client = new CodexProvider({ retryBaseDelayMs: 1 });
      const chunks = [];
      for await (const chunk of client.sendQuery('hello', '/repo', undefined, {
        env: { CODEX_API_KEY: 'fixture-key', UNTRUSTED_EXTRA: 'blocked' },
        execContext: SEALED_OPENAI_EXEC_CONTEXT,
      })) {
        chunks.push(chunk);
      }

      expect(chunks).toContainEqual({ type: 'assistant', content: 'fixture ok' });
      expect(chunks[chunks.length - 1]).toMatchObject({
        type: 'result',
        sessionId: 'fixture-thread',
        tokens: { input: 1, output: 2 },
      });
      const argv = await readFile(logPath, 'utf8');
      expect(argv).toContain('exec\n-i\n--user\narchon');
      expect(argv).toContain('cid-1\ncodex\nexec\n--experimental-json');
      expect(argv).toContain('--env\nCODEX_API_KEY');
      expect(argv).not.toContain('UNTRUSTED_EXTRA');
    });
  });

  test('passes resume through the docker-exec protocol fixture', async () => {
    await withFakeDocker(STREAMING_FAKE_DOCKER, async logPath => {
      const client = new CodexProvider({ retryBaseDelayMs: 1 });
      const chunks = [];
      for await (const chunk of client.sendQuery('continue', '/repo', 'thread-1', {
        execContext: SEALED_OPENAI_EXEC_CONTEXT,
      })) {
        chunks.push(chunk);
      }

      expect(chunks.find(chunk => chunk.type === 'result')).toMatchObject({ resumed: true });
      const argv = await readFile(logPath, 'utf8');
      expect(argv).toContain('resume\nthread-1');
    });
  });
});
