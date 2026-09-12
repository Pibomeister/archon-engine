import { describe, expect, test } from 'bun:test';
import { chmodSync, mkdtempSync, realpathSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { removeTempTree } from '@archon/paths/test-utils';
import { runGrokCommand } from './factory-transport';

describe('runGrokCommand never rejects before native close', () => {
  test('missing binary resolves with exit 1 instead of rejecting', async () => {
    const cwd = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-transport-missing-')));
    try {
      const result = await runGrokCommand({
        argv: [join(cwd, 'no-such-grok')],
        cwd,
        env: { PATH: '/usr/bin:/bin' },
        onLine() {},
      });
      expect(result.exitCode).toBe(1);
      expect(result.nativeClosed).toBe(true);
      expect(result.stderr.length).toBeGreaterThan(0);
    } finally {
      await removeTempTree(cwd);
    }
  });

  test('abort of a live child waits for close and resolves, never rejects', async () => {
    const cwd = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-transport-abort-')));
    try {
      const abort = new AbortController();
      const running = runGrokCommand({
        argv: ['/bin/sleep', '30'],
        cwd,
        env: { PATH: '/usr/bin:/bin' },
        signal: abort.signal,
        onLine() {},
      });
      await new Promise(resolve => setTimeout(resolve, 50));
      abort.abort();
      const result = await running;
      expect(result.exitCode).not.toBe(0);
      expect(result.nativeClosed).toBe(true);
    } finally {
      await removeTempTree(cwd);
    }
  });

  test('flushes a final NDJSON line without a trailing newline', async () => {
    const cwd = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-transport-remainder-')));
    try {
      const script = join(cwd, 'emit.sh');
      writeFileSync(script, '#!/bin/sh\nprintf "one\\n"\nprintf "two-without-nl"\n');
      chmodSync(script, 0o755);
      const lines: string[] = [];
      const result = await runGrokCommand({
        argv: [script],
        cwd,
        env: { PATH: '/usr/bin:/bin' },
        onLine(line) {
          lines.push(line);
        },
      });
      expect(result.exitCode).toBe(0);
      expect(result.nativeClosed).toBe(true);
      expect(lines).toEqual(['one', 'two-without-nl']);
    } finally {
      await removeTempTree(cwd);
    }
  });

  test('SIGKILL follows an ignored SIGTERM within the grace window', async () => {
    const cwd = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-transport-kill-')));
    try {
      const script = join(cwd, 'ignore-term.sh');
      writeFileSync(script, '#!/bin/sh\ntrap "" TERM\nsleep 30\n');
      chmodSync(script, 0o755);
      const abort = new AbortController();
      const running = runGrokCommand({
        argv: [script],
        cwd,
        env: { PATH: '/usr/bin:/bin' },
        signal: abort.signal,
        termGraceMs: 50,
        killGraceMs: 50,
        onLine() {},
      });
      await new Promise(resolve => setTimeout(resolve, 30));
      abort.abort();
      const result = await running;
      expect(result.exitCode).not.toBe(0);
      expect(result.nativeClosed).toBe(true);
    } finally {
      await removeTempTree(cwd);
    }
  });

  test('stdout lines flush before close resolves', async () => {
    const cwd = realpathSync(mkdtempSync(join(tmpdir(), 'factory-grok-transport-lines-')));
    try {
      const script = join(cwd, 'emit.sh');
      writeFileSync(script, '#!/bin/sh\nprintf "one\\n"\nprintf "two\\n"\n');
      chmodSync(script, 0o755);
      const lines: string[] = [];
      const result = await runGrokCommand({
        argv: [script],
        cwd,
        env: { PATH: '/usr/bin:/bin' },
        onLine(line) {
          lines.push(line);
        },
      });
      expect(result.exitCode).toBe(0);
      expect(lines).toEqual(['one', 'two']);
    } finally {
      await removeTempTree(cwd);
    }
  });
});
