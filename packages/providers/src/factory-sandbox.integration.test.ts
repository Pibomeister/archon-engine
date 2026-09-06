import { removeTempTree } from '@archon/paths/test-utils';
import { describe, expect, test } from 'bun:test';
import {
  mkdtempSync,
  mkdirSync,
  realpathSync,
  existsSync,
  writeFileSync,
  readFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { factoryCodexConfig } from './factory-sandbox';

const binary = process.env.ARCHON_TEST_CODEX_SANDBOX_BINARY;

describe.skipIf(!binary || process.platform !== 'darwin')(
  'actual pinned Codex native workspace sandbox (no model)',
  () => {
    test('permits a worktree write and prevents a sibling checkout mutation', async () => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-native-sandbox-')));
      try {
        const worktree = join(root, 'worktree');
        const manual = join(root, 'manual-checkout');
        const home = join(root, 'private-home');
        for (const path of [worktree, manual, home]) mkdirSync(path);
        const sentinel = join(manual, 'sentinel');
        writeFileSync(sentinel, 'unchanged');
        const config = factoryCodexConfig(
          { workspaceRoot: worktree, writableRoots: [worktree], deniedRoots: [manual] },
          worktree
        );
        const profile = (
          config.permissions as { 'archon-factory': { filesystem: Record<string, string> } }
        )['archon-factory'];
        writeFileSync(
          join(home, 'config.toml'),
          '[permissions.archon-factory.filesystem]\n' +
            Object.entries(profile.filesystem)
              .map(([key, value]) => JSON.stringify(key) + ' = ' + JSON.stringify(value))
              .join('\n') +
            '\n'
        );
        const run = (target: string) =>
          spawnSync(
            binary!,
            [
              'sandbox',
              '--permission-profile',
              'archon-factory',
              '-C',
              worktree,
              '/usr/bin/touch',
              target,
            ],
            {
              env: { PATH: '/usr/bin:/bin', HOME: home, CODEX_HOME: home },
              encoding: 'utf8',
              timeout: 15000,
            }
          );
        const allowed = run(join(worktree, 'allowed'));
        expect(allowed.error).toBeUndefined();
        expect(allowed.status).toBe(0);
        expect(existsSync(join(worktree, 'allowed'))).toBe(true);
        const blocked = run(join(manual, 'blocked'));
        expect(blocked.error).toBeUndefined();
        expect(blocked.status).not.toBe(0);
        expect(blocked.stderr).toContain('Operation not permitted');
        expect(existsSync(join(manual, 'blocked'))).toBe(false);
        expect(readFileSync(sentinel, 'utf8')).toBe('unchanged');
      } finally {
        await removeTempTree(root);
      }
    });
  }
);
