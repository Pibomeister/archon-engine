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
import { execFileSync, spawnSync } from 'node:child_process';
import { factoryCodexConfig } from './factory-sandbox';

const binary = process.env.ARCHON_TEST_CODEX_SANDBOX_BINARY;

describe.skipIf(!binary || process.platform !== 'darwin')(
  'actual pinned Codex native workspace sandbox (no model)',
  () => {
    test('real linked worktree can read only its shared git metadata while manual files remain protected', async () => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-native-git-')));
      try {
        const source = join(root, 'manual');
        const worktree = join(root, 'worktree');
        const home = join(root, 'home');
        mkdirSync(source);
        mkdirSync(home);
        const env = {
          PATH: '/usr/bin:/bin',
          HOME: home,
          CODEX_HOME: home,
          GIT_CONFIG_NOSYSTEM: '1',
          GIT_OPTIONAL_LOCKS: '0',
          TMPDIR: '/tmp',
        };
        const git = (...args: string[]) =>
          execFileSync('/usr/bin/git', ['-C', source, ...args], { env, encoding: 'utf8' });
        git('init', '-q');
        git('config', 'user.name', 'Fixture');
        git('config', 'user.email', 'fixture@example.invalid');
        writeFileSync(join(source, 'app.txt'), 'original\n');
        git('add', 'app.txt');
        git('commit', '-qm', 'Fixture');
        git('worktree', 'add', '-q', '-b', 'factory-fixture', worktree);
        writeFileSync(join(worktree, 'app.txt'), 'changed\n');
        const config = factoryCodexConfig(
          {
            workspaceRoot: worktree,
            writableRoots: [worktree],
            readableRoots: [join(source, '.git')],
            deniedRoots: [source],
          },
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
        const run = (args: string[]) =>
          spawnSync(binary!, ['sandbox', '-P', 'archon-factory', '-C', worktree, ...args], {
            env,
            encoding: 'utf8',
            timeout: 15000,
          });
        const status = run(['/usr/bin/git', 'status', '--porcelain']);
        expect(status.status, status.stderr).toBe(0);
        expect(status.stdout).toContain('app.txt');
        const diff = run(['/usr/bin/git', 'diff', '--', 'app.txt']);
        expect(diff.status, diff.stderr).toBe(0);
        expect(diff.stdout).toContain('+changed');
        const blocked = run(['/usr/bin/touch', join(source, 'forbidden')]);
        expect(blocked.status).not.toBe(0);
        expect(existsSync(join(source, 'forbidden'))).toBe(false);
        expect(readFileSync(join(source, 'app.txt'), 'utf8')).toBe('original\n');
      } finally {
        await removeTempTree(root);
      }
    }, 30000);
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
