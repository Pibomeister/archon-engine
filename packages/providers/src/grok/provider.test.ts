import { describe, expect, test } from 'bun:test';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { removeTempTree } from '@archon/paths/test-utils';
import { GROK_FACTORY_DISALLOWED_TOOLS, GROK_FACTORY_IMPLEMENT_TOOLS } from './capabilities';
import { GrokProvider, factoryGrokEnv } from './provider';
import { buildGrokFactoryArgv, permissionGlob } from './factory-argv';
import { parseGrokStreamingLine } from './stream';
import type { GrokCommandRunner } from './factory-transport';
import type { MessageChunk } from '../types';

const WORKTREE = '/factory/worktree';
const MANUAL = '/factory/manual';
const KNOWLEDGE = '/factory/knowledge';
const RESUME_ID = '11111111-1111-4111-8111-111111111111';

describe('managed Grok factory argv', () => {
  test('requires oauth, no-leader, archon-factory sandbox, and never worktree or yolo', () => {
    const argv = buildGrokFactoryArgv({
      command: '/bin/grok',
      prompt: 'write the marker --yolo --worktree',
      cwd: WORKTREE,
      model: 'grok-4.6',
      grokHome: '/factory/grok-home',
      scope: {
        workspaceRoot: WORKTREE,
        writableRoots: [WORKTREE],
        readableRoots: [KNOWLEDGE],
        deniedRoots: [MANUAL],
      },
    });
    const sandboxAt = argv.indexOf('--sandbox');
    expect(argv[sandboxAt + 1]).toBe('archon-factory');
    expect(argv).toContain('--oauth');
    expect(argv).toContain('--no-leader');
    expect(argv).toContain('dontAsk');
    expect(argv).toContain('streaming-json');
    expect(argv).toContain('--prompt-json');
    expect(argv).toContain('--tools');
    expect(argv).toContain(GROK_FACTORY_IMPLEMENT_TOOLS.join(','));
    expect(argv).toContain('--disallowed-tools');
    expect(argv).toContain(GROK_FACTORY_DISALLOWED_TOOLS.join(','));
    expect(argv).toContain(`Edit(${permissionGlob(WORKTREE)})`);
    expect(argv).toContain(`Read(${permissionGlob(KNOWLEDGE)})`);
    expectDenied(argv, `Edit(${permissionGlob('/factory/grok-home')})`);
    expectDenied(argv, `Write(${permissionGlob('/factory/grok-home')})`);
    expectDenied(argv, `Read(${permissionGlob('/factory/grok-home')})`);
    expectDenied(argv, 'MCPTool(*)');
    expect(argv.join('\0')).not.toContain('--worktree\0');
    expect(argv).not.toContain('--yolo');
    expect(argv).not.toContain('--always-approve');
    expect(argv).not.toContain('bypassPermissions');
  });

  test('resume requires a native UUID and accepts one', () => {
    expect(() =>
      buildGrokFactoryArgv({
        command: '/bin/grok',
        prompt: 'continue',
        cwd: WORKTREE,
        model: 'grok-4.6',
        resumeSessionId: 'latest',
        scope: {
          workspaceRoot: WORKTREE,
          writableRoots: [WORKTREE],
          deniedRoots: [MANUAL],
        },
      })
    ).toThrow('grok_exact_resume_native_uuid_required');
    const argv = buildGrokFactoryArgv({
      command: '/bin/grok',
      prompt: 'continue',
      cwd: WORKTREE,
      model: 'grok-4.6',
      resumeSessionId: RESUME_ID,
      scope: {
        workspaceRoot: WORKTREE,
        writableRoots: [WORKTREE],
        deniedRoots: [MANUAL],
      },
    });
    expect(argv).toEqual(expect.arrayContaining(['--resume', RESUME_ID]));
  });
});

describe('Grok streaming-json mapping', () => {
  test('maps text, tool, error, and end events', () => {
    expect(parseGrokStreamingLine('{"type":"text","data":"hello"}')).toEqual({
      type: 'assistant',
      content: 'hello',
    });
    expect(
      parseGrokStreamingLine(
        '{"type":"tool_call","toolCallId":"c1","toolName":"search_replace","rawInput":{"path":"a"}}'
      )
    ).toMatchObject({ type: 'tool', toolName: 'search_replace', toolCallId: 'c1' });
    expect(parseGrokStreamingLine('{"type":"error","message":"model not supported"}')).toEqual({
      type: 'result',
      isError: true,
      errors: ['model not supported'],
    });
    expect(parseGrokStreamingLine('{"type":"error"}')).toEqual({
      type: 'result',
      isError: true,
      errors: ['grok_stream_error'],
    });
    expect(
      parseGrokStreamingLine(
        '{"type":"end","sessionId":"11111111-1111-4111-8111-111111111111","stopReason":"end_turn"}'
      )
    ).toMatchObject({
      type: 'result',
      sessionId: '11111111-1111-4111-8111-111111111111',
      stopReason: 'end_turn',
    });
  });

  test('rejects invalid JSON', () => {
    expect(() => parseGrokStreamingLine('{')).toThrow('grok_stream_json_invalid');
    expect(() => parseGrokStreamingLine('[]')).toThrow('grok_stream_json_invalid');
    expect(() => parseGrokStreamingLine('null')).toThrow('grok_stream_json_invalid');
  });
});

describe('GrokProvider factory spawn', () => {
  test('writes sandbox.toml, strips API keys, and withholds result until close', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-provider-')));
    const previous = process.env.GROK_BIN_PATH;
    const previousKey = process.env.XAI_API_KEY;
    process.env.XAI_API_KEY = 'should-not-leak';
    try {
      const worktree = join(root, 'worktree');
      const manual = join(root, 'manual');
      const grokHome = join(root, 'grok-home');
      mkdirSync(worktree);
      mkdirSync(manual);
      mkdirSync(grokHome);
      const binary = join(root, 'grok');
      writeFileSync(binary, '#!/bin/sh\nexit 0\n');
      chmodSync(binary, 0o755);
      process.env.GROK_BIN_PATH = binary;
      const recorded: { argv: string[]; env: NodeJS.ProcessEnv }[] = [];
      let resolveRun: ((value: { exitCode: number; stdout: string; stderr: string; nativeClosed: boolean }) => void) | undefined;
      const runner: GrokCommandRunner = async input => {
        recorded.push({ argv: [...input.argv], env: { ...input.env } });
        input.onLine('{"type":"text","data":"wrote marker"}');
        input.onLine(
          `{"type":"end","sessionId":"${RESUME_ID}","stopReason":"end_turn"}`
        );
        return await new Promise(resolve => {
          resolveRun = resolve;
        });
      };
      const provider = new GrokProvider(runner);
      const chunks: MessageChunk[] = [];
      let closed = false;
      const consume = (async () => {
        for await (const chunk of provider.sendQuery('write the marker', worktree, undefined, {
          model: 'grok-4.6',
          env: { GROK_HOME: grokHome, PATH: '/bin' },
          factoryScope: {
            workspaceRoot: worktree,
            writableRoots: [worktree],
            deniedRoots: [manual],
          },
          factoryTransportClosed: () => {
            closed = true;
          },
        })) {
          chunks.push(chunk);
        }
      })();
      await new Promise(resolve => setTimeout(resolve, 20));
      expect(chunks).toEqual([{ type: 'assistant', content: 'wrote marker' }]);
      expect(closed).toBe(false);
      resolveRun?.({ exitCode: 0, stdout: '', stderr: '', nativeClosed: true });
      await consume;
      expect(closed).toBe(true);
      expect(chunks).toEqual([
        { type: 'assistant', content: 'wrote marker' },
        { type: 'result', sessionId: RESUME_ID, stopReason: 'end_turn' },
      ]);
      expect(recorded[0]?.env.XAI_API_KEY).toBeUndefined();
      expect(recorded[0]?.env.GROK_DISABLE_API_KEY_AUTH).toBe('1');
      expect(recorded[0]?.env.GROK_CLAUDE_MCPS_ENABLED).toBe('0');
      expect(recorded[0]?.env.GROK_CURSOR_MCPS_ENABLED).toBe('0');
      expect(recorded[0]?.env.GROK_HOME).toBe(grokHome);
      const toml = readFileSync(join(grokHome, 'sandbox.toml'), 'utf8');
      expect(toml).toContain('[profiles.archon-factory]');
      expect(toml).toContain('extends = "strict"');
      expect(toml).toContain(JSON.stringify(manual));
      expect(readFileSync(join(grokHome, 'config.toml'), 'utf8')).toContain('mcps = false');
    } finally {
      if (previous === undefined) delete process.env.GROK_BIN_PATH;
      else process.env.GROK_BIN_PATH = previous;
      if (previousKey === undefined) delete process.env.XAI_API_KEY;
      else process.env.XAI_API_KEY = previousKey;
      await removeTempTree(root);
    }
  });

  test('non-zero exit yields a single error result after close', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-exit-')));
    const previous = process.env.GROK_BIN_PATH;
    try {
      const worktree = join(root, 'worktree');
      const manual = join(root, 'manual');
      const grokHome = join(root, 'grok-home');
      mkdirSync(worktree);
      mkdirSync(manual);
      mkdirSync(grokHome);
      const binary = join(root, 'grok');
      writeFileSync(binary, '#!/bin/sh\nexit 0\n');
      chmodSync(binary, 0o755);
      process.env.GROK_BIN_PATH = binary;
      const runner: GrokCommandRunner = async input => {
        input.onLine('{"type":"text","data":"partial"}');
        return { exitCode: 1, stdout: '', stderr: 'cli failed', nativeClosed: true };
      };
      const provider = new GrokProvider(runner);
      const chunks: MessageChunk[] = [];
      let closed = false;
      for await (const chunk of provider.sendQuery('write', worktree, undefined, {
        model: 'grok-4.6',
        env: { GROK_HOME: grokHome, PATH: '/bin' },
        factoryScope: {
          workspaceRoot: worktree,
          writableRoots: [worktree],
          deniedRoots: [manual],
        },
        factoryTransportClosed: () => {
          closed = true;
        },
      })) {
        chunks.push(chunk);
      }
      expect(closed).toBe(true);
      expect(chunks).toEqual([
        { type: 'assistant', content: 'partial' },
        { type: 'result', isError: true, errors: ['cli failed'] },
      ]);
    } finally {
      if (previous === undefined) delete process.env.GROK_BIN_PATH;
      else process.env.GROK_BIN_PATH = previous;
      await removeTempTree(root);
    }
  });

  test('invalid stream JSON aborts, closes, then yields one error result', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-json-')));
    const previous = process.env.GROK_BIN_PATH;
    try {
      const worktree = join(root, 'worktree');
      const manual = join(root, 'manual');
      const grokHome = join(root, 'grok-home');
      mkdirSync(worktree);
      mkdirSync(manual);
      mkdirSync(grokHome);
      const binary = join(root, 'grok');
      writeFileSync(binary, '#!/bin/sh\nexit 0\n');
      chmodSync(binary, 0o755);
      process.env.GROK_BIN_PATH = binary;
      let aborted = false;
      const runner: GrokCommandRunner = async input => {
        input.onLine('{');
        await new Promise<void>(resolve => {
          input.signal?.addEventListener('abort', () => {
            aborted = true;
            resolve();
          });
        });
        return { exitCode: 1, stdout: '', stderr: 'aborted', nativeClosed: true };
      };
      const provider = new GrokProvider(runner);
      const chunks: MessageChunk[] = [];
      let closed = false;
      for await (const chunk of provider.sendQuery('write', worktree, undefined, {
        model: 'grok-4.6',
        env: { GROK_HOME: grokHome, PATH: '/bin' },
        factoryScope: {
          workspaceRoot: worktree,
          writableRoots: [worktree],
          deniedRoots: [manual],
        },
        factoryTransportClosed: () => {
          closed = true;
        },
      })) {
        chunks.push(chunk);
      }
      expect(aborted).toBe(true);
      expect(closed).toBe(true);
      expect(chunks).toEqual([{ type: 'result', isError: true, errors: ['aborted'] }]);
    } finally {
      if (previous === undefined) delete process.env.GROK_BIN_PATH;
      else process.env.GROK_BIN_PATH = previous;
      await removeTempTree(root);
    }
  });

  test('refuses unmanaged construction without factory scope', async () => {
    const provider = new GrokProvider();
    await expect(consume(provider.sendQuery('hi', '/tmp', undefined, { model: 'grok-4.6' }))).rejects.toThrow(
      'grok_factory_scope_required'
    );
  });

  test('factory env omits API key variables and disables key auth and vendor MCP scan', () => {
    const env = factoryGrokEnv({ GROK_HOME: '/isolated', PATH: '/bin', XAI_API_KEY: 'nope' });
    expect(env.XAI_API_KEY).toBeUndefined();
    expect(env.GROK_API_KEY).toBeUndefined();
    expect(env.GROK_CODE_XAI_API_KEY).toBeUndefined();
    expect(env.GROK_DISABLE_API_KEY_AUTH).toBe('1');
    expect(env.GROK_CLAUDE_MCPS_ENABLED).toBe('0');
    expect(env.GROK_CURSOR_MCPS_ENABLED).toBe('0');
    expect(env.GROK_HOME).toBe('/isolated');
    expect(env.HOME).toBe('/isolated');
  });

  test('refuses GROK_HOME inside a writable root', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-home-write-')));
    const previous = process.env.GROK_BIN_PATH;
    try {
      const worktree = join(root, 'worktree');
      const manual = join(root, 'manual');
      const grokHome = join(worktree, 'stolen-home');
      mkdirSync(worktree);
      mkdirSync(manual);
      mkdirSync(grokHome);
      const binary = join(root, 'grok');
      writeFileSync(binary, '#!/bin/sh\nexit 0\n');
      chmodSync(binary, 0o755);
      process.env.GROK_BIN_PATH = binary;
      const provider = new GrokProvider(async () => {
        throw new Error('must-not-spawn');
      });
      await expect(
        consume(
          provider.sendQuery('write', worktree, undefined, {
            model: 'grok-4.6',
            env: { GROK_HOME: grokHome, PATH: '/bin' },
            factoryScope: {
              workspaceRoot: worktree,
              writableRoots: [worktree],
              deniedRoots: [manual],
            },
          })
        )
      ).rejects.toThrow('grok_home_in_write_root');
    } finally {
      if (previous === undefined) delete process.env.GROK_BIN_PATH;
      else process.env.GROK_BIN_PATH = previous;
      await removeTempTree(root);
    }
  });
});

function expectDenied(argv: readonly string[], token: string): void {
  const index = argv.indexOf(token);
  expect(index).toBeGreaterThan(0);
  expect(argv[index - 1]).toBe('--deny');
}

async function consume(iter: AsyncGenerator<unknown>): Promise<unknown[]> {
  const items: unknown[] = [];
  for await (const item of iter) items.push(item);
  return items;
}
