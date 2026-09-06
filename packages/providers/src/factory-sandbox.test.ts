import { removeTempTree } from '@archon/paths/test-utils';
import { describe, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { factoryClaudeScope, factoryCodexScope, factoryCodexConfig } from './factory-sandbox';

describe('trusted factory provider writable scope', () => {
  test.skipIf(process.platform !== 'darwin')(
    'rejects native always-writable temporary protected roots before execution',
    async () => {
      const root = realpathSync(mkdtempSync(join('/tmp', 'factory-protected-tmp-')));
      try {
        const worktree = join(root, 'worktree');
        const manual = join(root, 'manual');
        mkdirSync(worktree);
        mkdirSync(manual);
        expect(() =>
          factoryCodexConfig(
            { workspaceRoot: worktree, writableRoots: [worktree], deniedRoots: [manual] },
            worktree
          )
        ).toThrow('factory_provider_protected_tmp_root_unqualified');
        expect(() =>
          factoryCodexConfig(
            {
              workspaceRoot: worktree,
              writableRoots: [worktree],
              deniedRoots: [manual.replace('/private/tmp/', '/tmp/')],
            },
            worktree
          )
        ).toThrow();
      } finally {
        await removeTempTree(root);
      }
    }
  );
  test('uses native workspace-write and only explicit extra directories for Codex', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-scope-')));
    try {
      const worktree = join(root, 'worktree');
      const artifacts = join(root, 'artifacts');
      const manual = join(root, 'manual');
      for (const path of [worktree, artifacts, manual]) mkdirSync(path);
      const scope = {
        workspaceRoot: worktree,
        writableRoots: [worktree, artifacts],
        deniedRoots: [manual],
      };
      expect(factoryCodexScope(scope, worktree)).toEqual({
        sandboxMode: undefined,
        additionalDirectories: [],
        approvalPolicy: 'never',
      });
      expect(factoryCodexConfig(scope, worktree)).toMatchObject({
        default_permissions: 'archon-factory',
        permissions: {
          'archon-factory': {
            filesystem: { [worktree]: 'write', [artifacts]: 'write', [manual]: 'deny' },
          },
        },
      });
      expect(() => factoryCodexScope(scope, manual)).toThrow('factory_provider_workspace_mismatch');
      expect(() =>
        factoryCodexScope({ ...scope, writableRoots: [worktree, root] }, worktree)
      ).toThrow('factory_provider_scope_overlap');
      const dotted = join(root, '..manual');
      mkdirSync(dotted);
      expect(() =>
        factoryCodexScope(
          { ...scope, writableRoots: [worktree, root], deniedRoots: [dotted] },
          worktree
        )
      ).toThrow('factory_provider_scope_overlap');
    } finally {
      await removeTempTree(root);
    }
  });
  test('Claude fails closed on sandbox absence and cannot load broader user/project settings', async () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-scope-')));
    try {
      const worktree = join(root, 'worktree');
      const manual = join(root, 'manual');
      for (const path of [worktree, manual]) mkdirSync(path);
      const result = factoryClaudeScope(
        { workspaceRoot: worktree, writableRoots: [worktree], deniedRoots: [manual] },
        worktree
      );
      expect(result.permissionMode).toBe('dontAsk');
      expect(result.allowDangerouslySkipPermissions).toBe(false);
      expect(result.settingSources).toEqual([]);
      expect(result.sandbox).toMatchObject({
        enabled: true,
        failIfUnavailable: true,
        allowUnsandboxedCommands: false,
        filesystem: { allowWrite: [worktree], denyWrite: [manual] },
      });
      expect(result.settings).toMatchObject({
        permissions: {
          disableBypassPermissionsMode: 'disable',
          deny: ['Edit(/' + manual + '/**)', 'Read(/' + manual + '/**)'],
        },
      });
    } finally {
      await removeTempTree(root);
    }
  });
});
