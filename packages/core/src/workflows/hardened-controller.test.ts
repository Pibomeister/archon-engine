import { describe, test, expect, beforeEach, afterEach, mock, spyOn } from 'bun:test';
import {
  chmodSync,
  copyFileSync,
  existsSync,
  linkSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { execFileSync } from 'node:child_process';
import { createHash, createHmac } from 'node:crypto';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import type { BrowserObservationRequest, BrowserObservationResult } from '@archon/isolation';
import { makeTestWorkflow } from '@archon/workflows/test-utils';
import {
  computeControllerActionManifestDigest,
  computeControllerWorkflowDigest,
  type ControllerActionGrant,
  type ControllerActionHandlerContext,
} from '@archon/workflows/controller-actions';
import type { WorkflowBudgetGrant } from '@archon/workflows/budget';
import type { WorkflowDefinition, WorkflowRun } from '@archon/workflows/schemas';
import type { IWorkflowStore, WorkflowEventRecord } from '@archon/workflows/store';

const IMAGE_ID = 'sha256:' + 'a'.repeat(64);
const OTHER_IMAGE_ID = 'sha256:' + 'b'.repeat(64);
const STATIC_HELPER_IMAGE_ID = 'sha256:' + 'd'.repeat(64);
const VERIFIER_IMAGE_ID = 'sha256:' + 'e'.repeat(64);
let home: string;
let currentSource: string;
let nextTestEventOrder = 0;

mock.module('@archon/paths', () => ({
  BUNDLED_GIT_COMMIT: 'test',
  BUNDLED_IS_BINARY: false,
  BUNDLED_VERSION: '0.0.0-test',
  BUNDLED_WEB_DIST_SHA256: 'test',
  createLogger: () => ({
    fatal: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    info: () => undefined,
    debug: () => undefined,
    trace: () => undefined,
    child: () => ({
      fatal: () => undefined,
      warn: () => undefined,
      error: () => undefined,
      info: () => undefined,
      debug: () => undefined,
      trace: () => undefined,
    }),
  }),
  captureWorkflowInvoked: () => undefined,
  captureWorkflowCompleted: () => undefined,
  expandTilde: (path: string) => path,
  isDocker: () => false,
  isWSL: () => false,
  getWSLDistroName: () => null,
  getArchonHome: () => home,
  getArchonWorkspacesPath: () => join(home, 'workspaces'),
  ensureArchonWorkspacesPath: () => join(home, 'workspaces'),
  getArchonWorktreesPath: () => join(home, 'worktrees'),
  getProjectWorktreesPath: () => join(home, 'worktrees'),
  getArchonConfigPath: () => join(home, 'config.json'),
  getCredentialKeyPath: () => join(home, 'credential.key'),
  getArchonEnvPath: () => join(home, '.env'),
  getRepoArchonEnvPath: (cwd: string) => join(cwd, '.archon', '.env'),
  getHomeWorkflowsPath: () => join(home, 'workflows'),
  getHomeScriptsPath: () => join(home, 'scripts'),
  getLegacyHomeWorkflowsPath: () => join(home, 'legacy-workflows'),
  getCommandFolderSearchPaths: () => [],
  getHomeCommandsPath: () => join(home, 'commands'),
  getDefaultCommandsPath: () => join(home, 'defaults', 'commands'),
  getWorkflowFolderSearchPaths: () => [],
  getAppArchonBasePath: () => join(home, 'app'),
  getDefaultWorkflowsPath: () => join(home, 'defaults', 'workflows'),
  logArchonPaths: () => undefined,
  validateAppDefaultsPaths: () => undefined,
  parseOwnerRepo: () => null,
  resolveRepoProjectIdentity: () => null,
  getProjectRoot: () => join(home, 'projects', 'mock-project'),
  getProjectSourcePath: () => currentSource,
  getProjectArtifactsPath: () => join(home, 'projects', 'mock-project', 'artifacts'),
  getProjectLogsPath: () => join(home, 'projects', 'mock-project', 'logs'),
  getRunArtifactsPath: (_key: unknown, runId: string) =>
    join(home, 'projects', 'mock-project', 'artifacts', 'runs', runId),
  getRunLogPath: (_key: unknown, runId: string) =>
    join(home, 'projects', 'mock-project', 'logs', `${runId}.log`),
  sanitizeScopeSegment: (value: string) => value.replace(/[^a-zA-Z0-9._-]/g, '-'),
  findMarkdownFilesRecursive: async () => [],
  isInsideArchonHome: (path: string) => path.startsWith(home),
  getStoragePathsForRoot: (root: string) => ({
    root,
    artifactsRoot: join(root, 'artifacts'),
    logsDir: join(root, 'logs'),
    stateRoot: join(root, 'state'),
  }),
  getProjectStoragePaths: () => ({
    root: join(home, 'projects', 'mock-project'),
    artifactsRoot: join(home, 'projects', 'mock-project', 'artifacts'),
    logsDir: join(home, 'projects', 'mock-project', 'logs'),
    stateRoot: join(home, 'projects', 'mock-project', 'state'),
  }),
  getScopeArtifactsPath: (root: string, workflow: string, scope: string) =>
    join(root, 'scopes', workflow, scope),
  getRunArtifactsDirForKey: (_key: unknown, runId: string) =>
    join(home, 'projects', 'mock-project', 'artifacts', 'runs', runId),
  resolveProjectStorageKey: () => ({ kind: 'cwd', cwd: currentSource }),
  resolveProjectStoragePaths: () => ({
    root: join(home, 'projects', 'mock-project'),
    artifactsRoot: join(home, 'projects', 'mock-project', 'artifacts'),
    logsDir: join(home, 'projects', 'mock-project', 'logs'),
    stateRoot: join(home, 'projects', 'mock-project', 'state'),
  }),
  getRunArtifactsDirForRoot: (root: string, runId: string) =>
    join(root, 'artifacts', 'runs', runId),
  slugifyFolderName: (value: string) => value.replace(/[^a-zA-Z0-9._-]/g, '-'),
  getFolderProjectRoot: () => join(home, 'folders', 'mock-project'),
  getFolderProjectArtifactsPath: () => join(home, 'folders', 'mock-project', 'artifacts'),
  getFolderProjectLogsPath: () => join(home, 'folders', 'mock-project', 'logs'),
  getFolderRunArtifactsPath: (_cwd: string, runId: string) =>
    join(home, 'folders', 'mock-project', 'artifacts', 'runs', runId),
  ensureFolderProjectStructure: () => undefined,
  resolveProjectRootFromCwd: () => join(home, 'projects', 'mock-project'),
  ensureProjectStructure: () => undefined,
  createProjectSourceSymlink: () => undefined,
  getWebDistDir: () => join(home, 'web-dist'),
}));

const {
  prepareHardenedControllerSession,
  resumeHardenedControllerSession,
  getHardenedControllerEgressPolicy,
  getHardenedControllerProxyBudgetSeed,
  getHardenedControllerValidatorNodeModules,
  createRefusingHardenedControllerActions,
  createHardenedControllerActions,
  materializeQuarantinedCandidate,
  importQuarantinedCandidateBundle,
} = await import('./hardened-controller');

describe('materializeQuarantinedCandidate', () => {
  let root: string;
  let worktree: string;
  let controlRoot: string;
  let bare: string;
  let destination: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'archon-quarantine-candidate-'));
    home = join(root, 'private-home');
    controlRoot = join(home, 'controller-runs');
    worktree = join(root, 'writer');
    bare = join(controlRoot, 'quarantine.git');
    destination = join(controlRoot, 'candidate-seed');
    mkdirSync(home, { recursive: true, mode: 0o755 });
    mkdirSync(controlRoot, { recursive: true, mode: 0o700 });
    createCandidateRepo(worktree, bare);
  });

  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  test('exports immutable content from a controller-private bare quarantine commit', () => {
    const commit = gitText(worktree, ['rev-parse', 'HEAD']);
    const tree = gitText(worktree, ['rev-parse', 'HEAD^{tree}']);
    writeFileSync(join(worktree, 'app.txt'), 'writer drift\n');

    const result = materializeQuarantinedCandidate({
      quarantineGitDir: bare,
      commit,
      treeOid: tree,
      destination,
      maxFiles: 4,
      maxTotalBytes: 1024,
    });

    expect(result).toMatchObject({
      schema: 'archon.quarantined-candidate-content.v1',
      authority: 'none',
      commit,
      treeOid: tree,
      destination,
      fileCount: 2,
      totalBytes: 'app v1\n'.length + '#!/bin/sh\necho ok\n'.length,
    });
    expect(result.files).toEqual([
      {
        path: 'app.txt',
        gitOid: expect.any(String),
        sha256: expect.stringMatching(/^[0-9a-f]{64}$/),
        size: 'app v1\n'.length,
        executable: false,
      },
      {
        path: 'script.sh',
        gitOid: expect.any(String),
        sha256: expect.stringMatching(/^[0-9a-f]{64}$/),
        size: '#!/bin/sh\necho ok\n'.length,
        executable: true,
      },
    ]);
    expect(readFileSync(join(destination, 'app.txt'), 'utf8')).toBe('app v1\n');
    expect((statSync(join(destination, 'script.sh')).mode & 0o111) !== 0).toBe(true);
  });

  test('shares a single elapsed deadline across candidate Git reads', () => {
    const commit = gitText(worktree, ['rev-parse', 'HEAD']);
    const tree = gitText(worktree, ['rev-parse', 'HEAD^{tree}']);
    const clock = spyOn(Date, 'now')
      .mockImplementationOnce(() => 1000)
      .mockReturnValue(50_000);
    try {
      expect(() =>
        materializeQuarantinedCandidate({
          quarantineGitDir: bare,
          commit,
          treeOid: tree,
          destination,
          deadlineMs: 1000,
        })
      ).toThrow(/deadline/);
      expect(existsSync(destination)).toBe(false);
    } finally {
      clock.mockRestore();
    }
  });

  test('rejects corrupted loose blob contents instead of trusting object filenames', () => {
    const commit = gitText(worktree, ['rev-parse', 'HEAD']);
    const tree = gitText(worktree, ['rev-parse', 'HEAD^{tree}']);
    const expected = gitText(bare, ['rev-parse', `${commit}:app.txt`]);
    const wrong = execFileSync('git', ['-C', bare, 'hash-object', '-w', '--stdin'], {
      input: 'bad v1\n',
      encoding: 'utf8',
    }).trim();
    const expectedPath = join(bare, 'objects', expected.slice(0, 2), expected.slice(2));
    chmodSync(expectedPath, 0o600);
    copyFileSync(join(bare, 'objects', wrong.slice(0, 2), wrong.slice(2)), expectedPath);
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: bare,
        commit,
        treeOid: tree,
        destination,
      })
    ).toThrow(/oid verification/);
    expect(existsSync(join(destination, 'app.txt'))).toBe(false);
  });

  test('rejects grafts, replace refs, and non-bare private checkouts', () => {
    const commit = gitText(worktree, ['rev-parse', 'HEAD']);
    const tree = gitText(worktree, ['rev-parse', 'HEAD^{tree}']);
    const input = { quarantineGitDir: bare, commit, treeOid: tree, destination };
    writeFileSync(join(bare, 'info', 'grafts'), `${commit}\n`);
    expect(() => materializeQuarantinedCandidate(input)).toThrow(/grafts/);
    rmSync(join(bare, 'info', 'grafts'));
    git(bare, ['update-ref', `refs/replace/${commit}`, commit]);
    expect(() => materializeQuarantinedCandidate(input)).toThrow(/replace refs/);
    const checkout = join(controlRoot, 'not-bare');
    execFileSync('git', ['clone', '--no-hardlinks', worktree, checkout], { stdio: 'ignore' });
    chmodSync(join(checkout, '.git'), 0o700);
    expect(() =>
      materializeQuarantinedCandidate({
        ...input,
        quarantineGitDir: join(checkout, '.git'),
      })
    ).toThrow(/must be a bare/);
    expect(existsSync(destination)).toBe(false);
  });

  test('rejects mutable or tampered quarantine pins before writing content', () => {
    const commit = gitText(worktree, ['rev-parse', 'HEAD']);
    const tree = gitText(worktree, ['rev-parse', 'HEAD^{tree}']);
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: bare,
        commit,
        treeOid: '0'.repeat(40),
        destination,
      })
    ).toThrow(/tree/);
    expect(() => readFileSync(join(destination, 'app.txt'), 'utf8')).toThrow();

    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: bare,
        commit: 'HEAD',
        treeOid: tree,
        destination: join(controlRoot, 'candidate-seed-2'),
      })
    ).toThrow(/commit/);
  });

  test('rejects unsafe quarantine inputs and forbidden candidate paths', () => {
    const commit = gitText(worktree, ['rev-parse', 'HEAD']);
    const tree = gitText(worktree, ['rev-parse', 'HEAD^{tree}']);
    mkdirSync(destination, { recursive: true });
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: bare,
        commit,
        treeOid: tree,
        destination,
      })
    ).toThrow(/destination/);

    const publicBare = join(root, 'public.git');
    execFileSync('git', ['clone', '--bare', worktree, publicBare], { stdio: 'ignore' });
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: publicBare,
        commit,
        treeOid: tree,
        destination: join(controlRoot, 'candidate-seed-public'),
      })
    ).toThrow(/private/);

    const forbiddenWorktree = join(root, 'forbidden-writer');
    const forbiddenBare = join(controlRoot, 'forbidden.git');
    createCandidateRepo(forbiddenWorktree, forbiddenBare);
    writeFileSync(join(forbiddenWorktree, '.env'), 'SECRET=1\n');
    git(forbiddenWorktree, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'add',
      '.env',
    ]);
    git(forbiddenWorktree, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'tracked env',
    ]);
    git(forbiddenWorktree, ['push', forbiddenBare, 'HEAD:refs/candidates/sealed']);
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: forbiddenBare,
        commit: gitText(forbiddenWorktree, ['rev-parse', 'HEAD']),
        treeOid: gitText(forbiddenWorktree, ['rev-parse', 'HEAD^{tree}']),
        destination: join(controlRoot, 'candidate-seed-forbidden'),
      })
    ).toThrow(/forbidden path/);
  });

  test('enforces candidate file and byte bounds', () => {
    const commit = gitText(worktree, ['rev-parse', 'HEAD']);
    const tree = gitText(worktree, ['rev-parse', 'HEAD^{tree}']);
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: bare,
        commit,
        treeOid: tree,
        destination,
        maxFiles: 1,
      })
    ).toThrow(/file count/);
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: bare,
        commit,
        treeOid: tree,
        destination: join(controlRoot, 'candidate-seed-bytes'),
        maxTotalBytes: 1,
      })
    ).toThrow(/byte/);
  });

  test('rejects symlink entries and Git alternate/graft configuration', () => {
    const linkedWorktree = join(root, 'linked-writer');
    const linkedBare = join(controlRoot, 'linked.git');
    createCandidateRepo(linkedWorktree, linkedBare);
    symlinkSync('/etc/passwd', join(linkedWorktree, 'passwd-link'));
    git(linkedWorktree, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'add',
      'passwd-link',
    ]);
    git(linkedWorktree, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'tracked symlink',
    ]);
    git(linkedWorktree, ['push', linkedBare, 'HEAD:refs/candidates/sealed']);
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: linkedBare,
        commit: gitText(linkedWorktree, ['rev-parse', 'HEAD']),
        treeOid: gitText(linkedWorktree, ['rev-parse', 'HEAD^{tree}']),
        destination,
      })
    ).toThrow(/regular files/);

    mkdirSync(join(bare, 'objects', 'info'), { recursive: true });
    writeFileSync(join(bare, 'objects', 'info', 'alternates'), '/tmp/elsewhere\n');
    expect(() =>
      materializeQuarantinedCandidate({
        quarantineGitDir: bare,
        commit: gitText(worktree, ['rev-parse', 'HEAD']),
        treeOid: gitText(worktree, ['rev-parse', 'HEAD^{tree}']),
        destination: join(controlRoot, 'candidate-seed-alternates'),
      })
    ).toThrow(/alternate/);
  });
});

describe('importQuarantinedCandidateBundle', () => {
  let root: string;
  let baseline: string;
  let writer: string;
  let controlRoot: string;
  let snapshot: string;
  let bundlePath: string;
  let quarantineGitDir: string;
  let destination: string;
  let baselineCommit: string;
  let baselineTree: string;
  let candidateCommit: string;
  let candidateTree: string;
  let bundleDigest: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'archon-bundle-import-'));
    home = join(root, 'private-home');
    controlRoot = join(home, 'controller-runs');
    snapshot = join(controlRoot, 'snapshot');
    baseline = join(root, 'baseline');
    writer = join(root, 'writer');
    bundlePath = join(snapshot, 'candidate.bundle');
    quarantineGitDir = join(controlRoot, 'candidate.git');
    destination = join(controlRoot, 'candidate-content');
    mkdirSync(snapshot, { recursive: true, mode: 0o700 });
    chmodSync(home, 0o700);
    chmodSync(controlRoot, 0o700);
    chmodSync(snapshot, 0o700);
    createBaselineAndCandidateBundle(baseline, writer, bundlePath, 'changed');
    baselineCommit = gitText(baseline, ['rev-parse', 'HEAD']);
    baselineTree = gitText(baseline, ['rev-parse', 'HEAD^{tree}']);
    candidateCommit = gitText(writer, ['rev-parse', 'HEAD']);
    candidateTree = gitText(writer, ['rev-parse', 'HEAD^{tree}']);
    bundleDigest = fileSha256(bundlePath);
  });

  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  test('imports a changed candidate bundle into a fresh private quarantine and materializes authority-none content', () => {
    const result = importQuarantinedCandidateBundle({
      bundlePath,
      bundleSha256: bundleDigest,
      candidateCommit,
      candidateTreeOid: candidateTree,
      trustedBaselineCommit: baselineCommit,
      trustedBaselineTreeOid: baselineTree,
      quarantineGitDir,
      destination,
      maxFiles: 4,
      maxTotalBytes: 1024,
    });

    expect(result).toMatchObject({
      schema: 'archon.quarantined-candidate-import.v1',
      authority: 'none',
      bundleSha256: bundleDigest,
      candidateCommit,
      candidateTreeOid: candidateTree,
      trustedBaselineCommit: baselineCommit,
      trustedBaselineTreeOid: baselineTree,
      quarantineGitDir: expect.any(String),
      content: {
        authority: 'none',
        commit: candidateCommit,
        treeOid: candidateTree,
        fileCount: 2,
      },
    });
    expect(readFileSync(join(destination, 'app.txt'), 'utf8')).toBe('candidate v2\n');
    expect(gitText(quarantineGitDir, ['rev-parse', 'refs/candidates/sealed'])).toBe(
      candidateCommit
    );
  });

  test('imports a no-change candidate exactly at the trusted baseline commit', () => {
    const noChangeBundle = join(snapshot, 'no-change.bundle');
    git(baseline, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    git(baseline, ['bundle', 'create', noChangeBundle, 'refs/candidates/sealed']);
    chmodSync(noChangeBundle, 0o600);
    const result = importQuarantinedCandidateBundle({
      bundlePath: noChangeBundle,
      bundleSha256: fileSha256(noChangeBundle),
      candidateCommit: baselineCommit,
      candidateTreeOid: baselineTree,
      trustedBaselineCommit: baselineCommit,
      trustedBaselineTreeOid: baselineTree,
      quarantineGitDir,
      destination,
    });

    expect(result.content.commit).toBe(baselineCommit);
    expect(readFileSync(join(destination, 'app.txt'), 'utf8')).toBe('baseline v1\n');
  });

  test('rejects wrong bundle digest, candidate commit, tree, or baseline binding', () => {
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath,
        bundleSha256: '0'.repeat(64),
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir,
        destination,
      })
    ).toThrow(/digest/);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath,
        bundleSha256: bundleDigest,
        candidateCommit: baselineCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'wrong-commit.git'),
        destination: join(controlRoot, 'wrong-commit-content'),
      })
    ).toThrow(/sealed ref|candidate/);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath,
        bundleSha256: bundleDigest,
        candidateCommit,
        candidateTreeOid: baselineTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'wrong-tree.git'),
        destination: join(controlRoot, 'wrong-tree-content'),
      })
    ).toThrow(/tree/);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath,
        bundleSha256: bundleDigest,
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: candidateCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'wrong-baseline.git'),
        destination: join(controlRoot, 'wrong-baseline-content'),
      })
    ).toThrow(/baseline|tree/);
  });

  test('rejects unrelated graph, missing prerequisites, path escapes, malformed bundle, and deadlines', () => {
    const unrelated = createChildRepo(root, 'unrelated', 'other\n');
    const unrelatedBundle = join(snapshot, 'unrelated.bundle');
    git(unrelated, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    git(unrelated, ['bundle', 'create', unrelatedBundle, 'refs/candidates/sealed']);
    chmodSync(unrelatedBundle, 0o600);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: unrelatedBundle,
        bundleSha256: fileSha256(unrelatedBundle),
        candidateCommit: gitText(unrelated, ['rev-parse', 'HEAD']),
        candidateTreeOid: gitText(unrelated, ['rev-parse', 'HEAD^{tree}']),
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'unrelated.git'),
        destination: join(controlRoot, 'unrelated-content'),
      })
    ).toThrow(/baseline|invalid/);

    const prereqBundle = join(snapshot, 'prereq.bundle');
    git(writer, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    git(writer, ['bundle', 'create', prereqBundle, `^${baselineCommit}`, 'refs/candidates/sealed']);
    chmodSync(prereqBundle, 0o600);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: prereqBundle,
        bundleSha256: fileSha256(prereqBundle),
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'prereq.git'),
        destination: join(controlRoot, 'prereq-content'),
      })
    ).toThrow(/prerequisite|invalid|Failed/);

    const outsideBundle = join(root, 'outside.bundle');
    copyFileSync(bundlePath, outsideBundle);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: outsideBundle,
        bundleSha256: fileSha256(outsideBundle),
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'outside.git'),
        destination: join(controlRoot, 'outside-content'),
      })
    ).toThrow(/private controller-runs/);

    const malformed = join(snapshot, 'malformed.bundle');
    writeFileSync(malformed, 'not a git bundle\n', { mode: 0o600 });
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: malformed,
        bundleSha256: fileSha256(malformed),
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'malformed.git'),
        destination: join(controlRoot, 'malformed-content'),
      })
    ).toThrow(/bundle|Failed/);

    const truncated = join(snapshot, 'truncated.bundle');
    writeFileSync(truncated, readFileSync(bundlePath).subarray(0, 32), { mode: 0o600 });
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: truncated,
        bundleSha256: fileSha256(truncated),
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'truncated.git'),
        destination: join(controlRoot, 'truncated-content'),
      })
    ).toThrow(/bundle|Failed/);

    const clock = spyOn(Date, 'now')
      .mockImplementationOnce(() => 1000)
      .mockReturnValue(50_000);
    try {
      expect(() =>
        importQuarantinedCandidateBundle({
          bundlePath,
          bundleSha256: bundleDigest,
          candidateCommit,
          candidateTreeOid: candidateTree,
          trustedBaselineCommit: baselineCommit,
          trustedBaselineTreeOid: baselineTree,
          quarantineGitDir: join(controlRoot, 'deadline.git'),
          destination: join(controlRoot, 'deadline-content'),
          deadlineMs: 1000,
        })
      ).toThrow(/deadline/);
    } finally {
      clock.mockRestore();
    }
  });

  test('imports actual shallow seed bundles when bound to protected baseline objects', () => {
    const rootSource = join(root, 'platform-source');
    createChildRepoWithParent(rootSource, 'api', 'parent v0\n', 'baseline v1\n');
    const workflow = makeTestWorkflow({
      name: 'hardened-shallow-candidate',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const session = prepareHardenedControllerSession({
      runId: 'run-shallow-candidate',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'api' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const baselineInput = session.policyMetadata.repoInputs[0];
    if (!baselineInput) throw new Error('missing test baseline input');
    const baselineGitDir = join(session.seed?.path ?? '', baselineInput.originPath);
    const seedApi = join(session.seed?.path ?? '', 'api');

    git(seedApi, ['update-ref', 'refs/candidates/sealed', baselineInput.commit]);
    const baselineBundle = join(snapshot, 'seed-baseline.bundle');
    git(seedApi, ['bundle', 'create', baselineBundle, 'refs/candidates/sealed']);
    chmodSync(baselineBundle, 0o600);
    const baselineResult = importQuarantinedCandidateBundle({
      bundlePath: baselineBundle,
      bundleSha256: fileSha256(baselineBundle),
      candidateCommit: baselineInput.commit,
      candidateTreeOid: baselineInput.treeOid,
      trustedBaselineCommit: baselineInput.commit,
      trustedBaselineTreeOid: baselineInput.treeOid,
      trustedBaselineGitDir: baselineGitDir,
      trustedBaselineOriginDigest: baselineInput.originDigest,
      quarantineGitDir: join(controlRoot, 'seed-baseline.git'),
      destination: join(controlRoot, 'seed-baseline-content'),
    });
    expect(baselineResult.content.commit).toBe(baselineInput.commit);
    expect(readFileSync(join(baselineResult.content.destination, 'README.md'), 'utf8')).toBe(
      'baseline v1\n'
    );

    writeFileSync(join(seedApi, 'README.md'), 'candidate v2\n');
    git(seedApi, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-am',
      'candidate',
    ]);
    git(seedApi, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    const shallowBundle = join(snapshot, 'seed-shallow.bundle');
    git(seedApi, ['bundle', 'create', shallowBundle, 'refs/candidates/sealed']);
    chmodSync(shallowBundle, 0o600);
    const candidate = gitText(seedApi, ['rev-parse', 'HEAD']);
    const candidateTree = gitText(seedApi, ['rev-parse', 'HEAD^{tree}']);

    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: shallowBundle,
        bundleSha256: fileSha256(shallowBundle),
        candidateCommit: candidate,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineInput.commit,
        trustedBaselineTreeOid: baselineInput.treeOid,
        quarantineGitDir: join(controlRoot, 'seed-shallow-missing.git'),
        destination: join(controlRoot, 'seed-shallow-missing-content'),
      })
    ).toThrow(/unsupported: shallow history omitted baseline parent objects/);

    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: shallowBundle,
        bundleSha256: fileSha256(shallowBundle),
        candidateCommit: candidate,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineInput.commit,
        trustedBaselineTreeOid: baselineInput.treeOid,
        trustedBaselineOriginDigest: baselineInput.originDigest,
        quarantineGitDir: join(controlRoot, 'seed-shallow-digest-only.git'),
        destination: join(controlRoot, 'seed-shallow-digest-only-content'),
      })
    ).toThrow(/supplied together/);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: shallowBundle,
        bundleSha256: fileSha256(shallowBundle),
        candidateCommit: candidate,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineInput.commit,
        trustedBaselineTreeOid: baselineInput.treeOid,
        trustedBaselineGitDir: baselineGitDir,
        quarantineGitDir: join(controlRoot, 'seed-shallow-path-only.git'),
        destination: join(controlRoot, 'seed-shallow-path-only-content'),
      })
    ).toThrow(/supplied together/);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: shallowBundle,
        bundleSha256: fileSha256(shallowBundle),
        candidateCommit: candidate,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineInput.commit,
        trustedBaselineTreeOid: baselineInput.treeOid,
        trustedBaselineGitDir: '',
        trustedBaselineOriginDigest: baselineInput.originDigest,
        quarantineGitDir: join(controlRoot, 'seed-shallow-empty-path.git'),
        destination: join(controlRoot, 'seed-shallow-empty-path-content'),
      })
    ).toThrow(/supplied together/);

    const result = importQuarantinedCandidateBundle({
      bundlePath: shallowBundle,
      bundleSha256: fileSha256(shallowBundle),
      candidateCommit: candidate,
      candidateTreeOid: candidateTree,
      trustedBaselineCommit: baselineInput.commit,
      trustedBaselineTreeOid: baselineInput.treeOid,
      trustedBaselineGitDir: baselineGitDir,
      trustedBaselineOriginDigest: baselineInput.originDigest,
      quarantineGitDir: join(controlRoot, 'seed-shallow.git'),
      destination: join(controlRoot, 'seed-shallow-content'),
    });
    expect(result.content.commit).toBe(candidate);
    expect(readFileSync(join(result.content.destination, 'README.md'), 'utf8')).toBe(
      'candidate v2\n'
    );
    expect(gitText(result.quarantineGitDir, ['rev-parse', '--is-shallow-repository'])).toBe('true');

    let clockReads = 0;
    const copyClock = spyOn(Date, 'now').mockImplementation(() => {
      clockReads += 1;
      return clockReads <= 14 ? 1000 : 50_000;
    });
    try {
      expect(() =>
        importQuarantinedCandidateBundle({
          bundlePath: shallowBundle,
          bundleSha256: fileSha256(shallowBundle),
          candidateCommit: candidate,
          candidateTreeOid: candidateTree,
          trustedBaselineCommit: baselineInput.commit,
          trustedBaselineTreeOid: baselineInput.treeOid,
          trustedBaselineGitDir: baselineGitDir,
          trustedBaselineOriginDigest: baselineInput.originDigest,
          quarantineGitDir: join(controlRoot, 'seed-shallow-copy-deadline.git'),
          destination: join(controlRoot, 'seed-shallow-copy-deadline-content'),
          deadlineMs: 1000,
        })
      ).toThrow(/deadline/);
    } finally {
      copyClock.mockRestore();
    }

    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: shallowBundle,
        bundleSha256: fileSha256(shallowBundle),
        candidateCommit: candidate,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineInput.commit,
        trustedBaselineTreeOid: baselineInput.treeOid,
        trustedBaselineGitDir: baselineGitDir,
        trustedBaselineOriginDigest: '0'.repeat(64),
        quarantineGitDir: join(controlRoot, 'seed-shallow-wrong-digest.git'),
        destination: join(controlRoot, 'seed-shallow-wrong-digest-content'),
      })
    ).toThrow(/baseline source digest/);

    const publicBaseline = join(root, 'public-baseline.git');
    copyPrivateDirectory(baselineGitDir, publicBaseline);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: shallowBundle,
        bundleSha256: fileSha256(shallowBundle),
        candidateCommit: candidate,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineInput.commit,
        trustedBaselineTreeOid: baselineInput.treeOid,
        trustedBaselineGitDir: publicBaseline,
        trustedBaselineOriginDigest: baselineInput.originDigest,
        quarantineGitDir: join(controlRoot, 'seed-shallow-public.git'),
        destination: join(controlRoot, 'seed-shallow-public-content'),
      })
    ).toThrow(/private controller-runs/);

    mkdirSync(join(baselineGitDir, 'objects', 'info'), { recursive: true });
    writeFileSync(join(baselineGitDir, 'objects', 'info', 'alternates'), '/tmp/elsewhere\n');
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: shallowBundle,
        bundleSha256: fileSha256(shallowBundle),
        candidateCommit: candidate,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineInput.commit,
        trustedBaselineTreeOid: baselineInput.treeOid,
        trustedBaselineGitDir: baselineGitDir,
        trustedBaselineOriginDigest: baselineInput.originDigest,
        quarantineGitDir: join(controlRoot, 'seed-shallow-alternate.git'),
        destination: join(controlRoot, 'seed-shallow-alternate-content'),
      })
    ).toThrow(/alternate object stores/);
  }, 15_000);

  test('rejects bundle refs, private-path tricks, symlink entries, hardlinked bundles, and limits', () => {
    const headsBundle = join(snapshot, 'heads.bundle');
    git(writer, ['update-ref', 'refs/heads/main', 'HEAD']);
    git(writer, ['bundle', 'create', headsBundle, 'refs/heads/main']);
    chmodSync(headsBundle, 0o600);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: headsBundle,
        bundleSha256: fileSha256(headsBundle),
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'heads.git'),
        destination: join(controlRoot, 'heads-content'),
      })
    ).toThrow(/refs\/candidates\/sealed/);

    const linkedBundle = join(snapshot, 'linked.bundle');
    symlinkSync(bundlePath, linkedBundle);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: linkedBundle,
        bundleSha256: bundleDigest,
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'linked.git'),
        destination: join(controlRoot, 'linked-content'),
      })
    ).toThrow(/symlink/);

    const hardlinkedBundle = join(snapshot, 'hardlinked.bundle');
    linkSync(bundlePath, hardlinkedBundle);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: hardlinkedBundle,
        bundleSha256: bundleDigest,
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'hardlinked.git'),
        destination: join(controlRoot, 'hardlinked-content'),
      })
    ).toThrow(/hardlinked/);
    rmSync(hardlinkedBundle);

    const linkRepo = createChildRepo(root, 'link-repo', 'safe\n');
    symlinkSync('/etc/passwd', join(linkRepo, 'escape'));
    git(linkRepo, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', 'escape']);
    git(linkRepo, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'link',
    ]);
    const linkBundle = join(snapshot, 'symlink-entry.bundle');
    git(linkRepo, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    git(linkRepo, ['bundle', 'create', linkBundle, 'refs/candidates/sealed']);
    chmodSync(linkBundle, 0o600);
    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath: linkBundle,
        bundleSha256: fileSha256(linkBundle),
        candidateCommit: gitText(linkRepo, ['rev-parse', 'HEAD']),
        candidateTreeOid: gitText(linkRepo, ['rev-parse', 'HEAD^{tree}']),
        trustedBaselineCommit: gitText(linkRepo, ['rev-parse', 'HEAD~1']),
        trustedBaselineTreeOid: gitText(linkRepo, ['rev-parse', 'HEAD~1^{tree}']),
        quarantineGitDir: join(controlRoot, 'symlink-entry.git'),
        destination: join(controlRoot, 'symlink-entry-content'),
      })
    ).toThrow(/regular files/);

    expect(() =>
      importQuarantinedCandidateBundle({
        bundlePath,
        bundleSha256: bundleDigest,
        candidateCommit,
        candidateTreeOid: candidateTree,
        trustedBaselineCommit: baselineCommit,
        trustedBaselineTreeOid: baselineTree,
        quarantineGitDir: join(controlRoot, 'limit.git'),
        destination: join(controlRoot, 'limit-content'),
        maxBundleBytes: 1,
      })
    ).toThrow(/size/);
  });
});

describe('prepareHardenedControllerSession', () => {
  let root: string;
  let source: string;

  beforeEach(() => {
    nextTestEventOrder = 0;
    root = mkdtempSync(join(tmpdir(), 'archon-hardened-controller-'));
    home = join(root, 'private-home');
    source = join(root, 'source');
    currentSource = source;
    mkdirSync(source, { recursive: true });
    writeFileSync(join(source, 'README.md'), 'safe input\n');
    git(source, ['init']);
    git(source, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', 'README.md']);
    git(source, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'seed',
    ]);
  });

  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  test('creates an operator-private seed from committed Git objects only', () => {
    writeFileSync(join(source, '.env'), 'SECRET=1\n');
    writeFileSync(join(source, 'controller.key.backup'), 'untrusted key canary\n');
    symlinkSync('/etc/passwd', join(source, 'passwd-link'));
    const workflow = makeTestWorkflow({
      name: 'hardened-release',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    const session = prepareHardenedControllerSession({
      runId: 'run-123',
      workflow,
      workflowSource: 'project',
      sourceRoot: currentSource,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
      requestedImage: 'archon-runner@sha256:abc',
      budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 1234 } },
    });

    expect(session.seed).toEqual({
      kind: 'directory',
      path: join(session.privateDir, 'seed'),
      allowGitMetadata: true,
    });
    expect(readFileSync(join(session.seed.path, 'README.md'), 'utf8')).toBe('safe input\n');
    expect(() => readFileSync(join(session.seed.path, '.env'), 'utf8')).toThrow();
    expect(() => readFileSync(join(session.seed.path, 'controller.key.backup'), 'utf8')).toThrow();
    expect(() => readFileSync(join(session.seed.path, 'passwd-link'), 'utf8')).toThrow();
    expect(session.workflowBudgetGrants[0]).toMatchObject({
      runId: 'run-123',
      workflowName: 'hardened-release',
      workflowDigest: computeControllerWorkflowDigest(workflow),
      deadlineAt: '2030-01-01T00:00:00.000Z',
      tokens: { total: 1234 },
    });

    const manifest = JSON.parse(
      readFileSync(join(session.privateDir, 'seed-manifest.json'), 'utf8')
    );
    expect(manifest).toMatchObject({
      schema: 'archon.hardened-controller-seed.v1',
      source: {
        kind: 'git-tree',
        sourcePrefix: '',
        commit: expect.any(String),
        treeOid: expect.any(String),
      },
    });
    expect(manifest.files).toEqual([
      expect.objectContaining({ path: 'README.md', size: 'safe input\n'.length }),
    ]);
    const policy = JSON.parse(readFileSync(session.policyPath, 'utf8')) as Record<string, unknown>;
    expect(policy).toMatchObject({
      schema: 'archon.hardened-controller-session.v2',
      runId: 'run-123',
      workflowName: 'hardened-release',
      image: IMAGE_ID,
      requestedImage: 'archon-runner@sha256:abc',
      controllerActionGrants: [],
      hmacKeyPath: expect.any(String),
      sessionBindingPath: expect.any(String),
    });
    expect(policy.seedManifestDigest).toEqual(expect.any(String));
  });

  test('rejects direct calls that pass an unresolved image tag as policy authority', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-tag',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-tag',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: 'archon-runner:latest',
      })
    ).toThrow(/immutable sha256 image id/);
  });

  test('materializes controller-declared repository inputs with clean shallow git metadata', () => {
    const rootSource = join(root, 'platform');
    const api = createChildRepo(rootSource, 'api', 'api v1\n');
    const web = createChildRepo(rootSource, 'web-app', 'web v1\n');
    writeFileSync(join(api, '.env'), 'API_SECRET=1\n');
    writeFileSync(join(web, 'controller.key.backup'), 'control canary\n');
    const apiCommit = gitText(api, ['rev-parse', 'HEAD']);
    const workflow = makeTestWorkflow({
      name: 'hardened-fullstack',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    const session = prepareHardenedControllerSession({
      runId: 'run-fullstack',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'api' }, { targetPath: 'web-app' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    writeFileSync(join(api, 'README.md'), 'api v2\n');
    git(api, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-am', 'v2']);

    const seedPath = session.seed?.path ?? '';
    expect(readFileSync(join(seedPath, 'api', 'README.md'), 'utf8')).toBe('api v1\n');
    expect(readFileSync(join(seedPath, 'web-app', 'README.md'), 'utf8')).toBe('web v1\n');
    expect(existsSync(join(seedPath, 'api', '.git'))).toBe(true);
    expect(existsSync(join(seedPath, 'api', '.git', 'hooks'))).toBe(false);
    expect(() => readFileSync(join(seedPath, 'api', '.env'), 'utf8')).toThrow();
    expect(() =>
      readFileSync(join(seedPath, 'web-app', 'controller.key.backup'), 'utf8')
    ).toThrow();

    const apiOrigin = gitText(join(seedPath, 'api'), ['remote', 'get-url', 'origin']);
    expect(apiOrigin).toMatch(/\.archon\/controller-origins\/api-/);
    expect(session.policyMetadata.repoInputs).toEqual([
      expect.objectContaining({
        targetPath: 'api',
        commit: apiCommit,
        fileCount: 1,
        originKind: 'shallow-git',
      }),
      expect.objectContaining({ targetPath: 'web-app', fileCount: 1 }),
    ]);
  });

  test('rejects repo input sources that escape through an intermediate symlink', () => {
    const rootSource = join(root, 'platform');
    const outside = createChildRepo(root, 'private-repo', 'private\n');
    mkdirSync(rootSource, { recursive: true });
    symlinkSync(outside, join(rootSource, 'linked'));
    const workflow = makeTestWorkflow({
      name: 'hardened-fullstack',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-link',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'api', sourcePath: 'linked/api' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/symlink components|resolve inside/);
  });

  test('does not execute repo-local clean filters while checking tracked bytes', () => {
    const canary = join(root, 'filter-executed');
    writeFileSync(join(source, '.gitattributes'), 'README.md filter=leak\n');
    git(source, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'add',
      '.gitattributes',
    ]);
    git(source, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'attrs',
    ]);
    git(source, ['config', 'filter.leak.clean', `sh -c 'echo pwned > "${canary}"; cat'`]);
    const workflow = makeTestWorkflow({
      name: 'hardened-filter',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    const session = prepareHardenedControllerSession({
      runId: 'run-filter',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });

    expect(existsSync(canary)).toBe(false);
    expect(readFileSync(join(session.seed?.path ?? '', 'README.md'), 'utf8')).toBe('safe input\n');
  });

  test('does not resolve git from an untrusted workspace PATH entry', () => {
    const poisonedBin = join(source, 'node_modules', '.bin');
    const canary = join(root, 'poisoned-git-executed');
    mkdirSync(poisonedBin, { recursive: true });
    writeFileSync(join(poisonedBin, 'git'), `#!/bin/sh\necho poisoned > '${canary}'\nexit 1\n`, {
      mode: 0o755,
    });
    const oldPath = process.env.PATH;
    process.env.PATH = `${poisonedBin}:${oldPath ?? ''}`;
    const workflow = makeTestWorkflow({
      name: 'hardened-path',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    try {
      const session = prepareHardenedControllerSession({
        runId: 'run-path',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      });
      expect(readFileSync(join(session.seed?.path ?? '', 'README.md'), 'utf8')).toBe(
        'safe input\n'
      );
      expect(existsSync(canary)).toBe(false);
    } finally {
      process.env.PATH = oldPath;
    }
  });

  test('rejects tracked symlink seed entries before checkout', () => {
    const linked = join(root, 'linked-target');
    writeFileSync(linked, 'outside\n');
    symlinkSync(linked, join(source, 'tracked-link'));
    git(source, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'add',
      'tracked-link',
    ]);
    git(source, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'tracked symlink',
    ]);
    const workflow = makeTestWorkflow({
      name: 'hardened-symlink-entry',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-tracked-link',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/regular files|symlink components/);
  });

  test('rejects tracked forbidden files in declared repo inputs', () => {
    const rootSource = join(root, 'platform-forbidden');
    const api = createChildRepo(rootSource, 'api', 'api safe\n');
    writeFileSync(join(api, '.env'), 'API_SECRET=1\n');
    git(api, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', '.env']);
    git(api, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'tracked env',
    ]);
    const workflow = makeTestWorkflow({
      name: 'hardened-forbidden',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-forbidden',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'api' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/forbidden path/);
  });

  test('does not expose blobs deleted before the pinned repo head', () => {
    const rootSource = join(root, 'platform-history');
    const api = createChildRepo(rootSource, 'api', 'safe v1\n');
    writeFileSync(join(api, 'secret.txt'), 'deleted secret\n');
    git(api, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', 'secret.txt']);
    git(api, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'secret',
    ]);
    const oldSecretBlob = gitText(api, ['rev-parse', 'HEAD:secret.txt']);
    rmSync(join(api, 'secret.txt'));
    writeFileSync(join(api, 'README.md'), 'safe v2\n');
    git(api, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', '-A']);
    git(api, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'remove secret',
    ]);
    const workflow = makeTestWorkflow({
      name: 'hardened-history',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    const session = prepareHardenedControllerSession({
      runId: 'run-history',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'api' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });

    const seedRepo = join(session.seed?.path ?? '', 'api');
    const origin = join(session.seed?.path ?? '', session.policyMetadata.repoInputs[0].originPath);
    expect(readFileSync(join(seedRepo, 'README.md'), 'utf8')).toBe('safe v2\n');
    expect(() => git(seedRepo, ['cat-file', '-e', oldSecretBlob])).toThrow();
    expect(() => gitBareExpect(origin, ['cat-file', '-e', oldSecretBlob])).toThrow();
    expect(() => git(seedRepo, ['show', `HEAD^:secret.txt`])).toThrow();
  });

  test('resumes only from the frozen persisted controller policy', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-release',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const session = prepareHardenedControllerSession({
      runId: 'run-resume',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });

    const resumed = resumeHardenedControllerSession({
      runId: 'run-resume',
      workflow,
      image: IMAGE_ID,
      policyMetadata: session.policyMetadata,
      budget: resumeBudgetFor(session),
    });

    expect(resumed.seed).toBeUndefined();
    expect(resumed.workflowBudgetGrants[0]).toEqual(session.workflowBudgetGrants[0]);
    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-resume',
        workflow,
        image: OTHER_IMAGE_ID,
        policyMetadata: session.policyMetadata,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/container image changed/);
  });

  test('resume rejects public budget changes, resets, and forged consumption', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-budget-resume',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const session = prepareHardenedControllerSession({
      runId: 'run-budget-resume',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
      budget: {
        deadlineAt: '2030-01-01T00:00:00.000Z',
        tokens: { total: 1234, input: 1000, output: 500 },
      },
    });
    expect(Object.isFrozen(session.workflowBudgetGrants[0])).toBe(true);
    expect(Object.isFrozen(session.workflowBudgetGrants[0].tokens)).toBe(true);
    expect(Object.isFrozen(session.policyMetadata.budgetGrant)).toBe(true);
    expect(Object.isFrozen(session.policyMetadata.budgetGrant.tokens)).toBe(true);

    expect(
      resumeHardenedControllerSession({
        runId: 'run-budget-resume',
        workflow,
        image: IMAGE_ID,
        policyMetadata: session.policyMetadata,
        budget: {
          deadlineAt: '2030-01-01T00:00:00.000Z',
          tokens: { total: 1234, input: 1000, output: 500 },
        },
      }).workflowBudgetGrants[0]
    ).toEqual(session.workflowBudgetGrants[0]);

    for (const budget of [
      {
        deadlineAt: '2031-01-01T00:00:00.000Z',
        tokens: { total: 1234, input: 1000, output: 500 },
      },
      {
        deadlineAt: '2030-01-01T00:00:00.000Z',
        tokens: { total: 1235, input: 1000, output: 500 },
      },
      {
        deadlineAt: '2030-01-01T00:00:00.000Z',
        tokens: { total: 1234, input: 1001, output: 500 },
      },
      {
        deadlineAt: '2030-01-01T00:00:00.000Z',
        tokens: { total: 1234, input: 1000, output: 501 },
      },
      { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 1234 } },
    ]) {
      expect(() =>
        resumeHardenedControllerSession({
          runId: 'run-budget-resume',
          workflow,
          image: IMAGE_ID,
          policyMetadata: session.policyMetadata,
          budget,
        })
      ).toThrow(/budget grant does not match/);
    }

    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-budget-resume',
        workflow,
        image: IMAGE_ID,
        policyMetadata: session.policyMetadata,
        budget: {
          deadlineAt: '2030-01-01T00:00:00.000Z',
          tokens: { total: 1234, input: 1000, output: 500 },
          authoritativeConsumed: { input: 0, output: 0 },
        },
      })
    ).toThrow(/authoritative consumption/);
  });

  test('resume rejects an authenticated budget grant after its deadline expires', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-expired-resume',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const clock = spyOn(Date, 'now').mockReturnValue(Date.parse('2029-01-01T00:00:00.000Z'));
    try {
      const session = prepareHardenedControllerSession({
        runId: 'run-expired-resume',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
        budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 1234 } },
      });
      clock.mockReturnValue(Date.parse('2031-01-01T00:00:00.000Z'));

      expect(() =>
        resumeHardenedControllerSession({
          runId: 'run-expired-resume',
          workflow,
          image: IMAGE_ID,
          policyMetadata: session.policyMetadata,
          budget: resumeBudgetFor(session),
        })
      ).toThrow(/deadline has expired/);
    } finally {
      clock.mockRestore();
    }
  });

  test('requires budget metadata and rejects unsafe budget counts', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-budget-validation',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    for (const [index, budget] of [
      { tokens: { total: 0 } },
      { tokens: { total: -1 } },
      { tokens: { total: 1.5 } },
      { tokens: { total: Number.NaN } },
      { tokens: { total: 10, input: -1 } },
      { tokens: { total: 10, output: 2.5 } },
      { tokens: { total: 10 }, authoritativeConsumed: { input: -1, output: 0 } },
      { tokens: { total: 10 }, authoritativeConsumed: { input: 1, output: Number.NaN } },
      { deadlineAt: '2000-01-01T00:00:00.000Z', tokens: { total: 10 } },
    ].entries()) {
      expect(() =>
        prepareHardenedControllerSession({
          runId: `run-invalid-budget-${index}`,
          workflow,
          sourceRoot: source,
          conversationId: 'cli-conv',
          userMessage: 'ship',
          image: IMAGE_ID,
          budget,
        })
      ).toThrow(/budget|authoritative consumption/);
    }

    const session = prepareHardenedControllerSession({
      runId: 'run-stripped-budget',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
      budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 10 } },
    });
    const stripped = { ...session.policyMetadata };
    delete (stripped as Record<string, unknown>).budgetGrant;

    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-stripped-budget',
        workflow,
        image: IMAGE_ID,
        policyMetadata: stripped,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/budget grant|unsupported keys/);
  });

  test('rejects fake sha256-prefixed image authorities', () => {
    const workflow = makeTestWorkflow({
      name: 'image-authority',
      hardened: { required: true },
      nodes: [{ id: 'ok', bash: 'true' }],
    });
    const invalidImages = [
      'sha256:approval',
      'sha256:' + 'g'.repeat(64),
      'sha256:' + 'a'.repeat(63),
    ];
    for (const [index, image] of invalidImages.entries()) {
      expect(() =>
        prepareHardenedControllerSession({
          runId: `bad-image-${index}`,
          workflow,
          sourceRoot: source,
          conversationId: 'test',
          userMessage: '',
          image,
        })
      ).toThrow(/immutable sha256/);
    }
  });

  test('rejects changed tracked bytes instead of racing the live workspace', () => {
    writeFileSync(join(source, 'README.md'), 'tampered\n');
    const workflow = makeTestWorkflow({
      name: 'hardened-release',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-dirty',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/pinned commit|committed seed/);
  });

  test('refuses controller signing state inside the workspace even without an operator policy', () => {
    home = join(source, 'private-home');
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'inside-source',
        workflow: makeTestWorkflow({ name: 'hardened', nodes: [{ id: 'test', bash: 'echo ok' }] }),
        sourceRoot: source,
        conversationId: 'conv',
        userMessage: 'test',
        image: IMAGE_ID,
      })
    ).toThrow(/outside the workspace/);
  });

  test('rejects a symlinked private controller root', () => {
    mkdirSync(home, { recursive: true, mode: 0o700 });
    chmodSync(home, 0o700);
    symlinkSync(source, join(home, 'controller-runs'));
    const workflow = makeTestWorkflow({
      name: 'hardened-release',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-symlink',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/must be a real directory/);
  });

  test('fails clearly for multi-root or non-git folder inputs without declared pins', () => {
    const nonGitRoot = join(root, 'multi-root');
    mkdirSync(nonGitRoot);
    writeFileSync(join(nonGitRoot, 'api.txt'), 'not a declared repo input\n');
    const workflow = makeTestWorkflow({
      name: 'hardened-release',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-multiroot',
        workflow,
        sourceRoot: nonGitRoot,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/controller-declared pinned repo inputs/);
  });

  test('fails clearly when a folder contains undeclared nested Git repositories', () => {
    mkdirSync(join(source, 'api', '.git'), { recursive: true });
    const workflow = makeTestWorkflow({
      name: 'hardened-release',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-nested',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/cannot infer nested repository inputs/);
  });

  test('imports unsigned candidate evidence through fixed candidate-import controller action', async () => {
    const rootSource = join(root, 'candidate-platform');
    createChildRepoWithParent(rootSource, 'api', 'parent v0\n', 'baseline v1\n');
    const workflow = makeCandidateImportWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'candidate-import',
        action: 'finalize-evidence',
        phase: 'candidate-import',
        repositoryTarget: 'api',
        bundleArtifact: 'run/candidate.bundle',
        candidateArtifact: 'run/candidate.json',
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-candidate-import',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'api' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const seedApi = join(session.seed?.path ?? '', 'api');
    writeFileSync(join(seedApi, 'README.md'), 'candidate v2\n');
    git(seedApi, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-am',
      'candidate',
    ]);
    git(seedApi, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    const bundle = join(root, 'candidate.bundle');
    git(seedApi, ['bundle', 'create', bundle, 'refs/candidates/sealed']);
    const candidate = {
      schema: 'archon.candidate-proposal.v1',
      commit: gitText(seedApi, ['rev-parse', 'HEAD']),
      tree: gitText(seedApi, ['rev-parse', 'HEAD^{tree}']),
    };
    const run = makeRun('run-candidate-import', workflow, { isolation_env_id: 'env-candidate' });
    const store = makeStore(run, []);
    const actions = createHardenedControllerActions({
      session,
      store,
      snapshotArtifacts: async (_envId, destinationDir) => {
        mkdirSync(join(destinationDir, 'run'), { recursive: true });
        copyFileSync(bundle, join(destinationDir, 'run', 'candidate.bundle'));
        chmodSync(join(destinationDir, 'run', 'candidate.bundle'), 0o600);
        writeFileSync(join(destinationDir, 'run', 'candidate.json'), JSON.stringify(candidate), {
          mode: 0o600,
        });
      },
    });

    const output = (await actions['finalize-evidence']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
    )) as Record<string, unknown>;
    const persisted = JSON.parse(JSON.stringify(output)) as Record<string, unknown>;
    expect(persisted.schema).toBe('archon.candidate-import-result.v1');
    expect(persisted.authority).toBe('none');
    expect(persisted.repositoryTarget).toBe('api');
    const imported = persisted.import as Record<string, unknown>;
    expect(imported.authority).toBe('none');
    expect(imported.candidateCommit).toBe(candidate.commit);
    expect(imported.candidateTreeOid).toBe(candidate.tree);
    expect(persisted.importedOnly).toBe(true);
    const contentPath = join(session.privateDir, 'candidate-content/candidate-import/README.md');
    expect(readFileSync(contentPath, 'utf8')).toBe('candidate v2\n');
    const receiptPath = persisted.receipt as string;
    const receipt = readSealedReceiptTest(receiptPath, session.policyMetadata.hmacKeyPath);
    const privateBundlePath = String(receipt.bundlePath);
    expect(structuredClone(receipt)).toMatchObject({
      schema: 'archon.candidate-import-receipt.v1',
      authority: 'controller-private-import-only',
      publicOutputAuthority: 'none',
      runId: 'run-candidate-import',
      workflowDigest: computeControllerWorkflowDigest(workflow),
      controllerActionNodeId: 'candidate-import',
      actionManifestDigest: grantFor(session.controllerActionGrants, 'candidate-import')
        .actionManifest.digest,
      policyDigest: session.policyMetadata.approvalPolicyDigest,
      image: IMAGE_ID,
      requestedImage: IMAGE_ID,
      seedManifestDigest: session.policyMetadata.seedManifestDigest,
      repositoryTarget: 'api',
      trustedBaselineCommit: session.policyMetadata.repoInputs[0]?.commit,
      trustedBaselineTreeOid: session.policyMetadata.repoInputs[0]?.treeOid,
      trustedBaselineOriginDigest: session.policyMetadata.repoInputs[0]?.originDigest,
      candidateCommit: candidate.commit,
      candidateTreeOid: candidate.tree,
      bundlePath: expect.stringContaining(
        '/snapshots/planning_run-candidate-import_candidate-import/run/candidate.bundle'
      ),
    });
    expect(receipt.provenance).toContain('not correctness or release readiness');

    const replayActions = createHardenedControllerActions({
      session,
      store,
      snapshotArtifacts: async () => {
        throw new Error('replay must not snapshot or rematerialize');
      },
    });
    const replay = (await replayActions['finalize-evidence']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
    )) as Record<string, unknown>;
    expect(replay).toMatchObject({
      authority: 'none',
      repositoryTarget: 'api',
      candidate: { commit: candidate.commit, tree: candidate.tree },
      receipt: receiptPath,
      importedOnly: true,
    });

    const content = receipt.content as Record<string, unknown>;
    const destinationRoot = String(content.destination);
    const quarantineRoot = String(content.quarantineGitDir);
    const replayContext = () =>
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'));
    const expectReplayRejects = async (message: RegExp) => {
      writeSealedReceiptTest(receiptPath, receipt, session.policyMetadata.hmacKeyPath);
      await expect(replayActions['finalize-evidence']!(replayContext())).rejects.toThrow(message);
    };

    const linkedDestination = `${destinationRoot}-linked`;
    renameSync(destinationRoot, linkedDestination);
    symlinkSync(linkedDestination, destinationRoot, 'dir');
    await expectReplayRejects(/content root|symlink/);
    rmSync(destinationRoot, { force: true });
    renameSync(linkedDestination, destinationRoot);

    const destinationParent = dirname(destinationRoot);
    const linkedDestinationParent = `${destinationParent}-linked`;
    renameSync(destinationParent, linkedDestinationParent);
    symlinkSync(linkedDestinationParent, destinationParent, 'dir');
    await expectReplayRejects(/content root|symlink path components/);
    rmSync(destinationParent, { force: true });
    renameSync(linkedDestinationParent, destinationParent);

    const linkedQuarantine = `${quarantineRoot}-linked`;
    renameSync(quarantineRoot, linkedQuarantine);
    symlinkSync(linkedQuarantine, quarantineRoot, 'dir');
    await expectReplayRejects(/quarantine|symlink/);
    rmSync(quarantineRoot, { force: true });
    renameSync(linkedQuarantine, quarantineRoot);

    const quarantineParent = dirname(quarantineRoot);
    const linkedQuarantineParent = `${quarantineParent}-linked`;
    renameSync(quarantineParent, linkedQuarantineParent);
    symlinkSync(linkedQuarantineParent, quarantineParent, 'dir');
    await expectReplayRejects(/quarantine|symlink path components/);
    rmSync(quarantineParent, { force: true });
    renameSync(linkedQuarantineParent, quarantineParent);

    const snapshotParent = join(session.privateDir, 'snapshots');
    const linkedSnapshotParent = `${snapshotParent}-linked`;
    renameSync(snapshotParent, linkedSnapshotParent);
    symlinkSync(linkedSnapshotParent, snapshotParent, 'dir');
    await expectReplayRejects(/bundle|symlink path components/);
    rmSync(snapshotParent, { force: true });
    renameSync(linkedSnapshotParent, snapshotParent);

    writeFileSync(join(destinationRoot, 'extra.txt'), 'unlisted\n', { mode: 0o600 });
    await expectReplayRejects(/content manifest/);
    rmSync(join(destinationRoot, 'extra.txt'), { force: true });

    mkdirSync(join(destinationRoot, 'node_modules'), { mode: 0o700 });
    await expectReplayRejects(/directory manifest/);
    rmSync(join(destinationRoot, 'node_modules'), { recursive: true, force: true });

    chmodSync(contentPath, 0o700);
    await expectReplayRejects(/content mode/);
    chmodSync(contentPath, 0o600);

    const hardlinkPath = join(session.privateDir, 'README-hardlink');
    linkSync(contentPath, hardlinkPath);
    await expectReplayRejects(/content hardlink/);
    rmSync(hardlinkPath, { force: true });

    const missingDestination = `${destinationRoot}-missing`;
    renameSync(destinationRoot, missingDestination);
    await expectReplayRejects(/content root|no such file|ENOENT/);
    renameSync(missingDestination, destinationRoot);

    writeSealedReceiptTest(
      receiptPath,
      { ...receipt, runId: 'wrong-run' },
      session.policyMetadata.hmacKeyPath
    );
    await expect(
      replayActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/receipt run changed/);

    writeSealedReceiptTest(
      receiptPath,
      receipt,
      session.policyMetadata.hmacKeyPath,
      '0'.repeat(64)
    );
    await expect(
      replayActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/receipt HMAC/);

    writeSealedReceiptTest(
      receiptPath,
      { ...receipt, candidateTreeOid: candidate.commit },
      session.policyMetadata.hmacKeyPath
    );
    await expect(
      replayActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/candidate tree|tree does not match/);

    writeSealedReceiptTest(
      receiptPath,
      { ...receipt, bundlePath: privateBundlePath },
      session.policyMetadata.hmacKeyPath
    );
    writeFileSync(privateBundlePath, 'mutated bundle bytes\n', { mode: 0o600 });
    await expect(
      replayActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/bundle digest/);
    copyFileSync(bundle, privateBundlePath);
    chmodSync(privateBundlePath, 0o600);
    writeSealedReceiptTest(
      receiptPath,
      { ...receipt, bundlePath: privateBundlePath },
      session.policyMetadata.hmacKeyPath
    );
    writeFileSync(contentPath, 'mutated candidate bytes\n');
    await expect(
      replayActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/content bytes changed/);
  });

  test('candidate-import action rejects unsafe policy, snapshot, and candidate bindings', async () => {
    const rootSource = join(root, 'candidate-negative-platform');
    createChildRepoWithParent(rootSource, 'api', 'parent v0\n', 'baseline v1\n');
    const workflow = makeCandidateImportWorkflow();
    const grant = {
      nodeId: 'candidate-import',
      action: 'finalize-evidence',
      phase: 'candidate-import',
      repositoryTarget: 'api',
      bundleArtifact: 'run/candidate.bundle',
      candidateArtifact: 'run/candidate.json',
    };
    writeApprovalPolicy(workflow, [{ ...grant, repositoryTarget: 'web' }]);
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-candidate-wrong-target',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'api' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/declared input/);
    expectPolicyRejected(
      workflow,
      [{ ...grant, bundleArtifact: 'tmp/candidate.bundle' }],
      /bundleArtifact/
    );
    writeApprovalPolicy(workflow, [grant]);
    const session = prepareHardenedControllerSession({
      runId: 'run-candidate-negative',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'api' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const seedApi = join(session.seed?.path ?? '', 'api');
    writeFileSync(join(seedApi, 'README.md'), 'candidate v2\n');
    git(seedApi, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-am',
      'candidate',
    ]);
    git(seedApi, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    const bundle = join(root, 'negative-candidate.bundle');
    git(seedApi, ['bundle', 'create', bundle, 'refs/candidates/sealed']);
    const candidate = {
      schema: 'archon.candidate-proposal.v1',
      commit: gitText(seedApi, ['rev-parse', 'HEAD']),
      tree: gitText(seedApi, ['rev-parse', 'HEAD^{tree}']),
    };
    const run = makeRun('run-candidate-negative', workflow, { isolation_env_id: 'env-candidate' });
    const context = actionContext(
      run,
      workflow,
      grantFor(session.controllerActionGrants, 'candidate-import')
    );
    const abort = new AbortController();
    context.signal = abort.signal;
    abort.abort();
    let wroteSnapshot = false;
    const store = makeStore(run, []);
    const actions = createHardenedControllerActions({
      session,
      store,
      snapshotArtifacts: async (_envId, destinationDir) => {
        wroteSnapshot = true;
        mkdirSync(join(destinationDir, 'run'), { recursive: true });
        copyFileSync(bundle, join(destinationDir, 'run', 'candidate.bundle'));
        chmodSync(join(destinationDir, 'run', 'candidate.bundle'), 0o600);
        writeFileSync(join(destinationDir, 'run', 'candidate.json'), JSON.stringify(candidate), {
          mode: 0o600,
        });
      },
    });
    await expect(actions['finalize-evidence']!(context)).rejects.toThrow(/cancelled/);
    expect(wroteSnapshot).toBe(false);

    const noSnapshotActions = createHardenedControllerActions({ session, store });
    await expect(
      noSnapshotActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/snapshot support/);

    await expect(
      actions['finalize-evidence']!(
        actionContext(
          run,
          workflow,
          withManifestInput(grantFor(session.controllerActionGrants, 'candidate-import'), {
            sourceBindingDigest: 'forged',
          })
        )
      )
    ).rejects.toThrow(/source binding/);

    const forgedCandidateActions = createHardenedControllerActions({
      session,
      store,
      snapshotArtifacts: async (_envId, destinationDir) => {
        mkdirSync(join(destinationDir, 'run'), { recursive: true });
        copyFileSync(bundle, join(destinationDir, 'run', 'candidate.bundle'));
        chmodSync(join(destinationDir, 'run', 'candidate.bundle'), 0o600);
        writeFileSync(
          join(destinationDir, 'run', 'candidate.json'),
          JSON.stringify({ ...candidate, trustedBaselineCommit: 'agent-forgery' }),
          { mode: 0o600 }
        );
      },
    });
    await expect(
      forgedCandidateActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/unsupported keys/);
    rmSync(join(session.privateDir, 'snapshots'), { recursive: true, force: true });

    const staleActions = createHardenedControllerActions({
      session,
      store,
      snapshotArtifacts: async (_envId, destinationDir) => {
        mkdirSync(join(destinationDir, 'run'), { recursive: true });
        writeFileSync(join(destinationDir, 'run', 'candidate.bundle'), 'stale bundle\n', {
          mode: 0o600,
        });
        writeFileSync(join(destinationDir, 'run', 'candidate.json'), JSON.stringify(candidate), {
          mode: 0o600,
        });
      },
    });
    await expect(
      staleActions['finalize-evidence']!(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
      )
    ).rejects.toThrow(/incomplete snapshot|bundle/);
  });

  test('seals only passed static candidate black-box observations bound to approved imports', async () => {
    const rootSource = join(root, 'candidate-blackbox-platform');
    createChildRepoWithParent(rootSource, 'web-app', 'parent v0\n', 'baseline v1\n');
    const workflow = makeCandidateBlackboxWorkflow();
    const validator = createValidatorNodeModules(home);
    writeApprovalPolicy(
      workflow,
      [
        {
          nodeId: 'freeze',
          action: 'finalize-evidence',
          phase: 'planning-freeze',
          oracleFiles: ['oracle/acceptance.browser.json'],
        },
        {
          nodeId: 'approval-check',
          action: 'verify-approval',
          phase: 'planning-approval',
          approvalNodeId: 'human-approval',
          freezeNodeId: 'freeze',
        },
        {
          nodeId: 'candidate-import',
          action: 'finalize-evidence',
          phase: 'candidate-import',
          repositoryTarget: 'web-app',
          bundleArtifact: 'run/candidate.bundle',
          candidateArtifact: 'run/candidate.json',
        },
        {
          nodeId: 'candidate-blackbox',
          action: 'finalize-evidence',
          phase: 'candidate-blackbox-test',
          repositoryTarget: 'web-app',
          candidateImportNodeId: 'candidate-import',
          freezeNodeId: 'freeze',
          approvalReceiptNodeId: 'approval-check',
          profile: 'static-web-http-v1',
          acceptancePolicyPath: 'oracle/acceptance.browser.json',
          appRoot: 'dist',
          port: 4173,
          staticHelperImage: STATIC_HELPER_IMAGE_ID,
          verifierImage: VERIFIER_IMAGE_ID,
          validatorPackageDigest: validator.playwrightDigest,
          validatorCorePackageDigest: validator.coreDigest,
        },
      ],
      undefined,
      undefined,
      validator.nodeModules
    );
    const session = prepareHardenedControllerSession({
      runId: 'run-candidate-blackbox',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'web-app' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const seedApp = join(session.seed?.path ?? '', 'web-app');
    mkdirSync(join(seedApp, 'dist'), { recursive: true });
    writeFileSync(join(seedApp, 'dist', 'index.html'), '<h1>Hello Archon</h1>\n');
    git(seedApp, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', 'dist']);
    git(seedApp, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'candidate',
    ]);
    git(seedApp, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    const bundle = join(root, 'blackbox-candidate.bundle');
    git(seedApp, ['bundle', 'create', bundle, 'refs/candidates/sealed']);
    const candidate = {
      schema: 'archon.candidate-proposal.v1',
      commit: gitText(seedApp, ['rev-parse', 'HEAD']),
      tree: gitText(seedApp, ['rev-parse', 'HEAD^{tree}']),
    };
    const browserPolicy = {
      required: [
        {
          id: 'home-renders',
          criterion: 'Home page renders approved text',
          path: '/',
          assertions: [{ type: 'text', value: 'Hello Archon' }],
        },
      ],
    };
    const run = makeRun('run-candidate-blackbox', workflow, {
      isolation_env_id: 'env-candidate',
    });
    const events: WorkflowEventRecord[] = [];
    const store = makeStore(run, events);
    const observationCalls: Record<string, unknown>[] = [];
    let observationStatus: 'passed' | 'failed' = 'passed';
    let mutateValidatorSourceDuringObserve = false;
    let mutateObservation: ((result: BrowserObservationResult) => void) | undefined;
    const evidenceDir = join(session.privateDir, 'observed-private');
    mkdirSync(evidenceDir, { recursive: true, mode: 0o700 });
    const screenshotPath = join(evidenceDir, 'home.png');
    const tracePath = join(evidenceDir, 'trace.zip');
    writeFileSync(screenshotPath, 'fake-png-evidence', { mode: 0o600 });
    writeFileSync(tracePath, 'fake-zip-evidence', { mode: 0o600 });
    const snapshotArtifacts = async (_envId: string, destinationDir: string): Promise<void> => {
      if (destinationDir.includes('candidate-import')) {
        mkdirSync(join(destinationDir, 'run'), { recursive: true });
        copyFileSync(bundle, join(destinationDir, 'run', 'candidate.bundle'));
        chmodSync(join(destinationDir, 'run', 'candidate.bundle'), 0o600);
        writeFileSync(join(destinationDir, 'run', 'candidate.json'), JSON.stringify(candidate), {
          mode: 0o600,
        });
        return;
      }
      mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
      writeFileSync(
        join(destinationDir, 'oracle', 'acceptance.browser.json'),
        JSON.stringify(browserPolicy),
        { mode: 0o600 }
      );
    };
    const browserObservationService = {
      observe: async (request: BrowserObservationRequest): Promise<BrowserObservationResult> => {
        observationCalls.push(JSON.parse(JSON.stringify(request)) as Record<string, unknown>);
        if (mutateValidatorSourceDuringObserve) {
          writeFileSync(
            join(validator.nodeModules, 'playwright', 'lib', 'runner.js'),
            'module.exports = "source swapped after check";\n',
            { mode: 0o600 }
          );
        }
        const result: BrowserObservationResult = {
          authority: 'none',
          status: observationStatus,
          runId: request.runId,
          policyDigest: digestStableTest(request.policy),
          observedOrigin: 'http://127.0.0.1:4173',
          app: {
            image: request.app.image,
            imageId: STATIC_HELPER_IMAGE_ID,
            commit: candidate.commit,
            tree: candidate.tree,
          },
          verifier: {
            image: String(request.verifierImage),
            imageId: VERIFIER_IMAGE_ID,
            playwrightVersion: '1.60.0',
          },
          viewport: { width: 1280, height: 720 },
          criteria: [
            {
              id: 'home-renders',
              path: '/',
              status: observationStatus,
              observed_origin: 'http://127.0.0.1:4173',
              assertions: [{ type: 'text', value: 'Hello Archon', status: observationStatus }],
            },
          ],
          security: browserSecurityFixture(),
          evidence: {
            screenshots: [
              {
                criterion: 'home-renders',
                path: 'browser-evidence/home.png',
                controllerPath: screenshotPath,
                sha256: fileSha256(screenshotPath),
              },
            ],
            traces: [
              {
                path: 'browser-evidence/trace.zip',
                controllerPath: tracePath,
                sha256: fileSha256(tracePath),
              },
            ],
          },
        };
        mutateObservation?.(result);
        return result;
      },
    };
    const actions = createHardenedControllerActions({
      session,
      store,
      playwrightNodeModules: validator.nodeModules,
      snapshotArtifacts,
      browserObservationService,
    });

    const freezeOutput = (await actions['finalize-evidence']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    run.metadata.approval = {
      type: 'approval',
      nodeId: 'human-approval',
      resolved: 'approved',
      message: `Approve ${String(freezeOutput.binding_id)} ${String(freezeOutput.oracle_digest)}`,
    };
    events.push(
      freezeCompletedEvent(freezeOutput),
      approvalEvent('approval_requested', 'human-approval', {
        message: `Approve ${String(freezeOutput.binding_id)} ${String(freezeOutput.oracle_digest)}`,
      }),
      approvalEvent('node_completed', 'human-approval', { approval_decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'approved' })
    );
    await actions['verify-approval']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
    );
    await actions['finalize-evidence']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
    );

    const blackboxContext = () =>
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-blackbox'));
    const expectRejectedObservation = async (
      mutate: (result: BrowserObservationResult) => void,
      error: RegExp
    ): Promise<void> => {
      mutateObservation = mutate;
      await expect(actions['finalize-evidence']!(blackboxContext())).rejects.toThrow(error);
      mutateObservation = undefined;
    };

    const badValidator = createValidatorNodeModules(join(home, 'validator-with-symlink'));
    symlinkSync(
      join(badValidator.nodeModules, 'playwright', 'package.json'),
      join(badValidator.nodeModules, 'playwright', 'lib', 'package-link.json')
    );
    const badValidatorActions = createHardenedControllerActions({
      session,
      store,
      playwrightNodeModules: badValidator.nodeModules,
      snapshotArtifacts,
      browserObservationService,
    });
    rmSync(join(session.privateDir, 'validator-node-modules', 'candidate-blackbox'), {
      recursive: true,
      force: true,
    });
    await expect(badValidatorActions['finalize-evidence']!(blackboxContext())).rejects.toThrow(
      /validator package symlink/
    );
    rmSync(join(session.privateDir, 'validator-node-modules', 'candidate-blackbox'), {
      recursive: true,
      force: true,
    });

    await expectRejectedObservation(result => {
      result.runId = 'other-run';
    }, /observation run changed/);
    await expectRejectedObservation(result => {
      result.criteria[0].path = '/admin';
    }, /criterion path changed/);
    await expectRejectedObservation(result => {
      result.criteria[0].assertions = [{ type: 'text', value: 'Hello Archon', status: 'skipped' }];
    }, /assertion result changed/);
    await expectRejectedObservation(result => {
      result.evidence.screenshots = [];
    }, /missed criterion evidence/);
    await expectRejectedObservation(result => {
      result.app.imageId = VERIFIER_IMAGE_ID;
    }, /static helper image id changed/);

    observationStatus = 'failed';
    await expect(actions['finalize-evidence']!(blackboxContext())).rejects.toThrow(
      /did not pass every frozen criterion|criterion result/
    );
    expect(
      existsSync(
        join(
          session.privateDir,
          'controller-receipts',
          'candidate-blackbox-candidate-blackbox.json'
        )
      )
    ).toBe(false);
    observationStatus = 'passed';

    const output = (await actions['finalize-evidence']!(blackboxContext())) as Record<
      string,
      unknown
    >;
    expect(output.schema).toBe('archon.candidate-blackbox-test-result.v1');
    expect(output.authority).toBe('none');
    expect(output.status).toBe('passed');
    expect(output.profile).toBe('static-web-http-v1');
    expect(observationCalls.length).toBe(7);
    const observedRequest = observationCalls.at(-1);
    expect(observedRequest?.verifierImage).toBe(VERIFIER_IMAGE_ID);
    expect(String(observedRequest?.playwrightNodeModules)).toStartWith(session.privateDir);
    expect(observedRequest?.playwrightNodeModules).not.toBe(validator.nodeModules);
    const observedApp = observedRequest?.app as Record<string, unknown>;
    expect(observedApp.image).toBe(STATIC_HELPER_IMAGE_ID);
    expect(observedApp.command).toBeUndefined();
    const source = observedApp.candidateSource as Record<string, unknown>;
    expect(source.profile).toBe('static-web-http-v1');
    expect(source.commit).toBe(candidate.commit);
    expect(source.tree).toBe(candidate.tree);
    expect(source.appRoot).toBe('dist');
    expect(source.contentDigest).toEqual(expect.stringMatching(/^[0-9a-f]{64}$/));
    const receiptPath = String(output.receipt);
    const receipt = readSealedReceiptTest(receiptPath, session.policyMetadata.hmacKeyPath);
    expect(receipt.schema).toBe('archon.candidate-blackbox-test-receipt.v1');
    expect(receipt.authority).toBe('controller-private-browser-blackbox-criteria');
    expect(receipt.publicOutputAuthority).toBe('none');
    expect(receipt.rawObservationAuthority).toBe('none');
    expect(receipt.rawObservationStatus).toBe('passed');
    expect(receipt.candidateCommit).toBe(candidate.commit);
    expect(receipt.candidateTreeOid).toBe(candidate.tree);
    expect(receipt.staticHelperImage).toBe(STATIC_HELPER_IMAGE_ID);
    expect(receipt.verifierImage).toBe(VERIFIER_IMAGE_ID);
    expect(String(receipt.validatorNodeModulesPath)).toStartWith(session.privateDir);
    expect(String(receipt.provenance)).toContain(
      'not release readiness or general API correctness'
    );

    mutateValidatorSourceDuringObserve = true;
    const replay = (await actions['finalize-evidence']!(blackboxContext())) as Record<
      string,
      unknown
    >;
    expect(replay).toMatchObject({
      authority: 'none',
      status: 'passed',
      receipt: receiptPath,
      verifiedOnly: true,
    });
    expect(observationCalls.length).toBe(7);

    writeFileSync(screenshotPath, 'mutated evidence', { mode: 0o600 });
    await expect(actions['finalize-evidence']!(blackboxContext())).rejects.toThrow(
      /evidence bytes changed/
    );
    writeFileSync(screenshotPath, 'fake-png-evidence', { mode: 0o600 });
    writeFileSync(
      join(String(receipt.validatorNodeModulesPath), 'playwright', 'lib', 'runner.js'),
      'module.exports = "tampered private validator";\n',
      { mode: 0o600 }
    );
    await expect(actions['finalize-evidence']!(blackboxContext())).rejects.toThrow(
      /validator package digest changed/
    );
  }, 30_000);

  test('seals node candidate black-box observations with startup binding from the signed manifest', async () => {
    const rootSource = join(root, 'candidate-blackbox-node-platform');
    createChildRepoWithParent(rootSource, 'web-app', 'parent v0\n', 'baseline v1\n');
    const workflow = makeCandidateBlackboxWorkflow();
    const validator = createValidatorNodeModules(home);
    writeApprovalPolicy(
      workflow,
      [
        {
          nodeId: 'freeze',
          action: 'finalize-evidence',
          phase: 'planning-freeze',
          oracleFiles: ['oracle/acceptance.browser.json'],
        },
        {
          nodeId: 'approval-check',
          action: 'verify-approval',
          phase: 'planning-approval',
          approvalNodeId: 'human-approval',
          freezeNodeId: 'freeze',
        },
        {
          nodeId: 'candidate-import',
          action: 'finalize-evidence',
          phase: 'candidate-import',
          repositoryTarget: 'web-app',
          bundleArtifact: 'run/candidate.bundle',
          candidateArtifact: 'run/candidate.json',
        },
        {
          nodeId: 'candidate-blackbox',
          action: 'finalize-evidence',
          phase: 'candidate-blackbox-test',
          repositoryTarget: 'web-app',
          candidateImportNodeId: 'candidate-import',
          freezeNodeId: 'freeze',
          approvalReceiptNodeId: 'approval-check',
          profile: 'node-http-app-v1',
          acceptancePolicyPath: 'oracle/acceptance.browser.json',
          appRoot: 'app',
          startupEntrypoint: 'server.cjs',
          port: 4173,
          staticHelperImage: STATIC_HELPER_IMAGE_ID,
          verifierImage: VERIFIER_IMAGE_ID,
          validatorPackageDigest: validator.playwrightDigest,
          validatorCorePackageDigest: validator.coreDigest,
        },
      ],
      undefined,
      undefined,
      validator.nodeModules
    );
    const session = prepareHardenedControllerSession({
      runId: 'run-candidate-blackbox-node',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'web-app' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const seedApp = join(session.seed?.path ?? '', 'web-app');
    mkdirSync(join(seedApp, 'app'), { recursive: true });
    writeFileSync(
      join(seedApp, 'app', 'server.cjs'),
      "require('http').createServer((_req, res) => res.end('API Ready')).listen(Number(process.env.PORT || process.argv[2]), '127.0.0.1');\n"
    );
    git(seedApp, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', 'app']);
    git(seedApp, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'candidate',
    ]);
    git(seedApp, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
    const bundle = join(root, 'blackbox-node-candidate.bundle');
    git(seedApp, ['bundle', 'create', bundle, 'refs/candidates/sealed']);
    const candidate = {
      schema: 'archon.candidate-proposal.v1',
      commit: gitText(seedApp, ['rev-parse', 'HEAD']),
      tree: gitText(seedApp, ['rev-parse', 'HEAD^{tree}']),
    };
    const browserPolicy = {
      required: [
        {
          id: 'api-ready',
          criterion: 'Node API candidate serves approved text',
          path: '/',
          assertions: [{ type: 'text', value: 'API Ready' }],
        },
      ],
    };
    const run = makeRun('run-candidate-blackbox-node', workflow, {
      isolation_env_id: 'env-candidate-node',
    });
    const events: WorkflowEventRecord[] = [];
    const store = makeStore(run, events);
    const evidenceDir = join(session.privateDir, 'observed-node-private');
    mkdirSync(evidenceDir, { recursive: true, mode: 0o700 });
    const screenshotPath = join(evidenceDir, 'api-ready.png');
    const tracePath = join(evidenceDir, 'trace.zip');
    writeFileSync(screenshotPath, 'fake-node-png-evidence', { mode: 0o600 });
    writeFileSync(tracePath, 'fake-node-zip-evidence', { mode: 0o600 });
    const snapshotArtifacts = async (_envId: string, destinationDir: string): Promise<void> => {
      if (destinationDir.includes('candidate-import')) {
        mkdirSync(join(destinationDir, 'run'), { recursive: true });
        copyFileSync(bundle, join(destinationDir, 'run', 'candidate.bundle'));
        chmodSync(join(destinationDir, 'run', 'candidate.bundle'), 0o600);
        writeFileSync(join(destinationDir, 'run', 'candidate.json'), JSON.stringify(candidate), {
          mode: 0o600,
        });
        return;
      }
      mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
      writeFileSync(
        join(destinationDir, 'oracle', 'acceptance.browser.json'),
        JSON.stringify(browserPolicy),
        { mode: 0o600 }
      );
    };
    let observedCandidateSource: Record<string, unknown> | undefined;
    const browserObservationService = {
      observe: async (request: BrowserObservationRequest): Promise<BrowserObservationResult> => {
        observedCandidateSource = JSON.parse(JSON.stringify(request.app.candidateSource)) as Record<
          string,
          unknown
        >;
        return {
          authority: 'none',
          status: 'passed',
          runId: request.runId,
          policyDigest: digestStableTest(request.policy),
          observedOrigin: 'http://127.0.0.1:4173',
          app: {
            image: request.app.image,
            imageId: STATIC_HELPER_IMAGE_ID,
            commit: candidate.commit,
            tree: candidate.tree,
          },
          verifier: {
            image: String(request.verifierImage),
            imageId: VERIFIER_IMAGE_ID,
            playwrightVersion: '1.60.0',
          },
          viewport: { width: 1280, height: 720 },
          criteria: [
            {
              id: 'api-ready',
              path: '/',
              status: 'passed',
              observed_origin: 'http://127.0.0.1:4173',
              assertions: [{ type: 'text', value: 'API Ready', status: 'passed' }],
            },
          ],
          security: browserSecurityFixture(),
          evidence: {
            screenshots: [
              {
                criterion: 'api-ready',
                path: 'browser-evidence/api-ready.png',
                controllerPath: screenshotPath,
                sha256: fileSha256(screenshotPath),
              },
            ],
            traces: [
              {
                path: 'browser-evidence/trace.zip',
                controllerPath: tracePath,
                sha256: fileSha256(tracePath),
              },
            ],
          },
        };
      },
    };
    const actions = createHardenedControllerActions({
      session,
      store,
      playwrightNodeModules: validator.nodeModules,
      snapshotArtifacts,
      browserObservationService,
    });

    const freezeOutput = (await actions['finalize-evidence']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    run.metadata.approval = {
      type: 'approval',
      nodeId: 'human-approval',
      resolved: 'approved',
      message: `Approve ${String(freezeOutput.binding_id)} ${String(freezeOutput.oracle_digest)}`,
    };
    events.push(
      freezeCompletedEvent(freezeOutput),
      approvalEvent('approval_requested', 'human-approval', {
        message: `Approve ${String(freezeOutput.binding_id)} ${String(freezeOutput.oracle_digest)}`,
      }),
      approvalEvent('node_completed', 'human-approval', { approval_decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'approved' })
    );
    await actions['verify-approval']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
    );
    await actions['finalize-evidence']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-import'))
    );
    const output = (await actions['finalize-evidence']!(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'candidate-blackbox'))
    )) as Record<string, unknown>;

    expect(output.profile).toBe('node-http-app-v1');
    expect(observedCandidateSource?.profile).toBe('node-http-app-v1');
    expect(observedCandidateSource?.startup).toEqual({ entrypoint: 'server.cjs' });
    expect(observedCandidateSource?.contentDigest).toEqual(expect.stringMatching(/^[0-9a-f]{64}$/));
    const receipt = readSealedReceiptTest(
      String(output.receipt),
      session.policyMetadata.hmacKeyPath
    );
    expect(receipt.profile).toBe('node-http-app-v1');
    expect(receipt.startupEntrypoint).toBe('server.cjs');
    expect(receipt.candidateSourceDigest).toBe(observedCandidateSource?.contentDigest);
    expect(receipt.authority).toBe('controller-private-browser-blackbox-criteria');
  }, 10_000);

  test('candidate black-box policy rejects mutable authority and repo-controlled commands', () => {
    const rootSource = join(root, 'candidate-blackbox-policy-platform');
    createChildRepoWithParent(rootSource, 'web-app', 'parent v0\n', 'baseline v1\n');
    const workflow = makeCandidateBlackboxWorkflow();
    const validator = createValidatorNodeModules(home);
    const grant = {
      nodeId: 'candidate-blackbox',
      action: 'finalize-evidence',
      phase: 'candidate-blackbox-test',
      repositoryTarget: 'web-app',
      candidateImportNodeId: 'candidate-import',
      freezeNodeId: 'freeze',
      approvalReceiptNodeId: 'approval-check',
      profile: 'static-web-http-v1',
      acceptancePolicyPath: 'oracle/acceptance.browser.json',
      appRoot: 'dist',
      port: 4173,
      staticHelperImage: STATIC_HELPER_IMAGE_ID,
      verifierImage: VERIFIER_IMAGE_ID,
      validatorPackageDigest: validator.playwrightDigest,
      validatorCorePackageDigest: validator.coreDigest,
    };
    writeApprovalPolicy(
      workflow,
      [{ ...grant, command: ['npm', 'test'] }],
      undefined,
      undefined,
      validator.nodeModules
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-command',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/unsupported keys/);
    writeApprovalPolicy(
      workflow,
      [{ ...grant, staticHelperImage: 'mcr.microsoft.com/playwright:latest' }],
      undefined,
      undefined,
      validator.nodeModules
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-mutable-helper',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/static helper image.*immutable sha256 digest/);
    writeApprovalPolicy(
      workflow,
      [{ ...grant, acceptancePolicyPath: 'oracle/acceptance.browser.json', schema: 'invented' }],
      undefined,
      undefined,
      validator.nodeModules
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-extra-schema',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/unsupported keys/);
    writeApprovalPolicy(
      workflow,
      [{ ...grant, profile: 'static-web-http-v1', startupEntrypoint: 'server.cjs' }],
      undefined,
      undefined,
      validator.nodeModules
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-static-startup',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/unsupported keys/);
    writeApprovalPolicy(
      workflow,
      [{ ...grant, profile: 'node-http-app-v1', startupEntrypoint: 'server.cjs' }],
      undefined,
      undefined,
      validator.nodeModules
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-node-startup',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).not.toThrow();
    writeApprovalPolicy(
      workflow,
      [{ ...grant, profile: 'node-http-app-v1' }],
      undefined,
      undefined,
      validator.nodeModules
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-node-missing-startup',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/startupEntrypoint/);
    writeApprovalPolicy(
      workflow,
      [{ ...grant, profile: 'node-http-app-v1', startupEntrypoint: '--eval.cjs' }],
      undefined,
      undefined,
      validator.nodeModules
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-node-option-startup',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/startup entrypoint/);
    writeApprovalPolicy(workflow, [grant]);
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-missing-validator',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/validator source is missing/);
    writeApprovalPolicy(workflow, [grant], undefined, undefined, rootSource);
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-blackbox-repo-validator',
        workflow,
        sourceRoot: rootSource,
        repoInputs: [{ targetPath: 'web-app' }],
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/must live under the private controller-runs directory/);
  }, 10_000);

  test('authenticates copied validator source policy before exposing the CLI path', () => {
    const rootSource = join(root, 'candidate-blackbox-getter-platform');
    createChildRepoWithParent(rootSource, 'web-app', 'parent v0\n', 'baseline v1\n');
    const workflow = makeCandidateBlackboxWorkflow();
    const validator = createValidatorNodeModules(home);
    const grant = {
      nodeId: 'candidate-blackbox',
      action: 'finalize-evidence',
      phase: 'candidate-blackbox-test',
      repositoryTarget: 'web-app',
      candidateImportNodeId: 'candidate-import',
      freezeNodeId: 'freeze',
      approvalReceiptNodeId: 'approval-check',
      profile: 'static-web-http-v1',
      acceptancePolicyPath: 'oracle/acceptance.browser.json',
      appRoot: 'dist',
      port: 4173,
      staticHelperImage: STATIC_HELPER_IMAGE_ID,
      verifierImage: VERIFIER_IMAGE_ID,
      validatorPackageDigest: validator.playwrightDigest,
      validatorCorePackageDigest: validator.coreDigest,
    };
    writeApprovalPolicy(workflow, [grant], undefined, undefined, validator.nodeModules);
    const session = prepareHardenedControllerSession({
      runId: 'run-blackbox-getter',
      workflow,
      sourceRoot: rootSource,
      repoInputs: [{ targetPath: 'web-app' }],
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });

    expect(getHardenedControllerValidatorNodeModules(session)).toBe(
      realpathSync(validator.nodeModules)
    );

    writeFileSync(
      String(session.policyMetadata.approvalPolicyPath),
      JSON.stringify({
        schema: 'archon.hardened-controller-planning-policy.v1',
        version: 1,
        workflowDigest: computeControllerWorkflowDigest(workflow),
        grants: [grant],
        validatorSource: { nodeModulesPath: join(home, 'attacker-node-modules') },
      }),
      { mode: 0o600 }
    );
    expect(() => getHardenedControllerValidatorNodeModules(session)).toThrow(
      /copied approval policy digest changed/
    );
  });

  test('loads operator-private planning policy and freezes oracle evidence', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
      {
        nodeId: 'approval-check',
        action: 'verify-approval',
        phase: 'planning-approval',
        approvalNodeId: 'human-approval',
        freezeNodeId: 'freeze',
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-approval',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const freezeGrant = grantFor(session.controllerActionGrants, 'freeze');
    const verifyGrant = grantFor(session.controllerActionGrants, 'approval-check');
    const run = makeRun('run-approval', workflow, { isolation_env_id: 'env-1' });
    const events: WorkflowEventRecord[] = [];
    const store = makeStore(run, events);
    const actions = createHardenedControllerActions({
      session,
      store,
      snapshotArtifacts: async (_envId, destinationDir) => {
        mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
        writeFileSync(join(destinationDir, 'oracle', 'plan.md'), 'approved acceptance criteria\n');
      },
    });

    const freezeOutput = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, freezeGrant)
    )) as Record<string, unknown>;
    expect(freezeOutput.binding_id).toEqual(expect.any(String));
    expect(freezeOutput.oracle_digest).toEqual(expect.any(String));
    expect(readFileSync(freezeOutput.receipt as string, 'utf8')).toContain(
      'controller-owned artifact snapshot'
    );

    run.metadata.approval = {
      type: 'approval',
      nodeId: 'human-approval',
      resolved: 'approved',
      message: `Approve ${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
    };
    events.push(
      freezeCompletedEvent(freezeOutput),
      approvalEvent('approval_requested', 'human-approval', {
        message: `Approve ${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
      }),
      approvalEvent('node_completed', 'human-approval', { approval_decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'approved' })
    );

    const approvalOutput = (await actions['verify-approval']?.(
      actionContext(run, workflow, verifyGrant)
    )) as Record<string, unknown>;
    expect(approvalOutput).toMatchObject({
      approved: true,
      binding_id: freezeOutput.binding_id,
      oracle_digest: freezeOutput.oracle_digest,
    });
    expect(readFileSync(approvalOutput.receipt as string, 'utf8')).toContain(
      'protected controller approval event'
    );
  });

  test('does not seal after cancellation or deadline expiry during the final status read', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
    ]);
    for (const mode of ['abort', 'deadline']) {
      const session = prepareHardenedControllerSession({
        runId: `final-status-${mode}`,
        workflow,
        sourceRoot: source,
        conversationId: 'test',
        userMessage: '',
        image: IMAGE_ID,
      });
      const run = makeRun(`final-status-${mode}`, workflow, { isolation_env_id: 'env-1' });
      const context = actionContext(
        run,
        workflow,
        grantFor(session.controllerActionGrants, 'freeze')
      );
      const abort = new AbortController();
      context.signal = abort.signal;
      let reads = 0;
      const store = makeStore(run, []);
      store.getWorkflowRunStatus = async () => {
        reads++;
        if (reads === 3) {
          if (mode === 'abort') abort.abort();
          else context.deadlineAt = Date.now() - 1;
        }
        return 'running';
      };
      const actions = createHardenedControllerActions({
        session,
        store,
        snapshotArtifacts: async (_id, destination) => {
          mkdirSync(join(destination, 'oracle'), { recursive: true });
          writeFileSync(join(destination, 'oracle/plan.md'), 'fixed fixture');
        },
      });
      await expect(actions['finalize-evidence']!(context)).rejects.toThrow(/cancelled|deadline/);
      expect(reads).toBe(3);
      expect(existsSync(join(session.privateDir, 'controller-receipts/freeze-freeze.json'))).toBe(
        false
      );
    }
  });

  test('replays planning freeze idempotently and rejects changed private snapshot bytes', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-freeze-replay',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-freeze-replay', workflow, { isolation_env_id: 'env-1' });
    let snapshots = 0;
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, []),
      snapshotArtifacts: async (_envId, destinationDir) => {
        snapshots += 1;
        mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
        writeFileSync(join(destinationDir, 'oracle', 'plan.md'), 'approved acceptance criteria\n');
      },
    });
    const grant = grantFor(session.controllerActionGrants, 'freeze');

    const first = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grant)
    )) as Record<string, unknown>;
    const second = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grant)
    )) as Record<string, unknown>;
    expect(second.binding_id).toBe(first.binding_id);
    expect(snapshots).toBe(1);

    writeFileSync(
      join(
        session.privateDir,
        'snapshots',
        grant.actionManifest.id.replaceAll(':', '_'),
        'oracle',
        'plan.md'
      ),
      'tampered\n'
    );
    await expect(
      actions['finalize-evidence']?.(actionContext(run, workflow, grant))
    ).rejects.toThrow(/snapshot bytes changed/);
  });

  test('rejects malformed or workspace-owned planning approval policy', () => {
    const workflow = makeApprovalWorkflow();
    mkdirSync(join(home, 'controller-policy'), { recursive: true, mode: 0o700 });
    chmodSync(home, 0o700);
    chmodSync(join(home, 'controller-policy'), 0o700);
    writeFileSync(
      join(home, 'controller-policy', 'planning-approval.json'),
      JSON.stringify({ schema: 'wrong', version: 1, workflowDigest: 'x', grants: [] }),
      { mode: 0o600 }
    );

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-bad-policy',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/Unsupported approval policy schema/);

    rmSync(join(home, 'controller-policy'), { recursive: true, force: true });
    mkdirSync(join(source, 'controller-policy'), { recursive: true });
    writeFileSync(join(source, 'controller-policy', 'planning-approval.json'), '{}');
    mkdirSync(home, { recursive: true, mode: 0o700 });
    symlinkSync(join(source, 'controller-policy'), join(home, 'controller-policy'));

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-linked-policy',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/symlinks|real directory|private/);
  });

  test('rejects approval policy grants that do not match pinned workflow nodes', () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'missing-freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
    ]);

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-wrong-node',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/does not match a controller node/);
  });

  test('rejects approval policies sealed for a different workflow digest', () => {
    const workflow = makeApprovalWorkflow();
    const otherWorkflow = makeTestWorkflow({
      name: 'other-workflow',
      nodes: [{ id: 'freeze', controller_action: 'finalize-evidence', phase: 'planning-freeze' }],
    });
    writeApprovalPolicy(otherWorkflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
    ]);

    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-wrong-workflow',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/does not match workflow digest/);
  });

  test('verify approval replays authenticated approval receipts and refuses corruption', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, approvalPolicyGrants());
    const session = prepareHardenedControllerSession({
      runId: 'run-approval-replay',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-approval-replay', workflow, { isolation_env_id: 'env-1' });
    const events: WorkflowEventRecord[] = [];
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, events),
      snapshotArtifacts: async (_envId, destinationDir) =>
        writeOracle(destinationDir, 'approved acceptance criteria\n'),
    });
    const freezeOutput = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    run.metadata.approval = { type: 'approval', nodeId: 'human-approval', resolved: 'approved' };
    events.push(
      freezeCompletedEvent(freezeOutput),
      approvalEvent('approval_requested', 'human-approval', {
        message: `${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
      }),
      approvalEvent('node_completed', 'human-approval', { approval_decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'approved' })
    );
    const verifyGrant = grantFor(session.controllerActionGrants, 'approval-check');
    const first = (await actions['verify-approval']?.(
      actionContext(run, workflow, verifyGrant)
    )) as Record<string, unknown>;
    const second = (await actions['verify-approval']?.(
      actionContext(run, workflow, verifyGrant)
    )) as Record<string, unknown>;
    expect(second.receipt).toBe(first.receipt);

    events.push(
      orderedFreezeCompletedEvent(4, freezeOutput),
      orderedApprovalEvent(5, 'approval_requested', 'human-approval', {
        message: `${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
      }),
      orderedApprovalEvent(6, 'node_completed', 'human-approval', {
        approval_decision: 'approved',
      }),
      orderedApprovalEvent(7, 'approval_received', 'human-approval', { decision: 'approved' })
    );
    await expect(
      actions['verify-approval']?.(actionContext(run, workflow, verifyGrant))
    ).rejects.toThrow(/no longer matches current approval events/);

    writeFileSync(
      first.receipt as string,
      '{"schema":"archon.planning-approval-receipt.v1","hmac":"00"}'
    );
    await expect(
      actions['verify-approval']?.(actionContext(run, workflow, verifyGrant))
    ).rejects.toThrow(/HMAC is invalid|missing or malformed/);
  });

  test('verify approval rejects freeze snapshot mutation before signing approval receipt', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, approvalPolicyGrants());
    const session = prepareHardenedControllerSession({
      runId: 'run-mutated-snapshot-verify',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-mutated-snapshot-verify', workflow, { isolation_env_id: 'env-1' });
    const events: WorkflowEventRecord[] = [];
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, events),
      snapshotArtifacts: async (_envId, destinationDir) =>
        writeOracle(destinationDir, 'approved acceptance criteria\n'),
    });
    const freezeGrant = grantFor(session.controllerActionGrants, 'freeze');
    const freezeOutput = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, freezeGrant)
    )) as Record<string, unknown>;
    writeFileSync(
      join(
        session.privateDir,
        'snapshots',
        freezeGrant.actionManifest.id.replaceAll(':', '_'),
        'oracle',
        'plan.md'
      ),
      'tampered\n'
    );
    run.metadata.approval = { type: 'approval', nodeId: 'human-approval', resolved: 'approved' };
    events.push(
      freezeCompletedEvent(freezeOutput),
      approvalEvent('approval_requested', 'human-approval', {
        message: `${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
      }),
      approvalEvent('node_completed', 'human-approval', { approval_decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'approved' })
    );

    await expect(
      actions['verify-approval']?.(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
      )
    ).rejects.toThrow(/snapshot bytes changed/);
  });

  test('verify approval rejects stale or newer unmatched approval events', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, approvalPolicyGrants());
    const session = prepareHardenedControllerSession({
      runId: 'run-stale-approval',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-stale-approval', workflow, { isolation_env_id: 'env-1' });
    const events: WorkflowEventRecord[] = [];
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, events),
      snapshotArtifacts: async (_envId, destinationDir) =>
        writeOracle(destinationDir, 'approved acceptance criteria\n'),
    });
    const freezeOutput = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    run.metadata.approval = { type: 'approval', nodeId: 'human-approval', resolved: 'approved' };
    events.push(
      orderedFreezeCompletedEvent(0, freezeOutput),
      orderedApprovalEvent(1, 'approval_received', 'human-approval', { decision: 'approved' }),
      orderedApprovalEvent(2, 'node_completed', 'human-approval', {
        approval_decision: 'approved',
      }),
      orderedApprovalEvent(3, 'approval_requested', 'human-approval', {
        message: `${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
      })
    );

    await expect(
      actions['verify-approval']?.(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
      )
    ).rejects.toThrow(/stale or out of order/);

    events.push(
      orderedApprovalEvent(4, 'approval_requested', 'human-approval', {
        message: 'new mutable packet',
      })
    );
    await expect(
      actions['verify-approval']?.(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
      )
    ).rejects.toThrow(/did not bind/);
  });

  test('verify approval rejects missing, malformed, or duplicate database event orders', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, approvalPolicyGrants());
    const session = prepareHardenedControllerSession({
      runId: 'run-malformed-event-order',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-malformed-event-order', workflow, { isolation_env_id: 'env-1' });
    const events: WorkflowEventRecord[] = [];
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, events),
      snapshotArtifacts: async (_envId, destinationDir) =>
        writeOracle(destinationDir, 'approved acceptance criteria\n'),
    });
    const frozen = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    run.metadata.approval = { type: 'approval', nodeId: 'human-approval', resolved: 'approved' };
    events.push(
      orderedFreezeCompletedEvent(1, frozen),
      orderedApprovalEvent(2, 'approval_requested', 'human-approval', {
        message: `${frozen.binding_id} ${frozen.oracle_digest}`,
      }),
      orderedApprovalEvent(3, 'node_completed', 'human-approval', {
        approval_decision: 'approved',
      }),
      orderedApprovalEvent(4, 'approval_received', 'human-approval', { decision: 'approved' })
    );
    const decision = events[3];
    if (!decision) throw new Error('missing decision fixture');
    decision.created_at = '2026-01-01T00:00:00.000Z';
    for (const invalidOrder of [undefined, null, NaN, Infinity, -1, 3.5, 3]) {
      decision.event_order = invalidOrder;
      await expect(
        actions['verify-approval']?.(
          actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
        )
      ).rejects.toThrow(/database event order/);
    }
    const approvalReceipt = join(
      session.privateDir,
      'controller-receipts',
      'approval-check-approval.json'
    );
    expect(existsSync(approvalReceipt)).toBe(false);
    decision.event_order = 4;
    const completion = events[2];
    if (!completion) throw new Error('missing completion fixture');
    completion.event_order = 5;
    await expect(
      actions['verify-approval']?.(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
      )
    ).rejects.toThrow(/stale or out of order/);
    completion.event_order = 3;
    await actions['verify-approval']?.(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
    );
    expect(existsSync(approvalReceipt)).toBe(true);
  });

  test('verify approval rejects approval requests that predate protected freeze completion', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, approvalPolicyGrants());
    const session = prepareHardenedControllerSession({
      runId: 'run-request-before-freeze',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-request-before-freeze', workflow, { isolation_env_id: 'env-1' });
    const events: WorkflowEventRecord[] = [];
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, events),
      snapshotArtifacts: async (_envId, destinationDir) =>
        writeOracle(destinationDir, 'approved acceptance criteria\n'),
    });
    const freezeOutput = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    run.metadata.approval = { type: 'approval', nodeId: 'human-approval', resolved: 'approved' };
    events.push(
      orderedApprovalEvent(1, 'approval_requested', 'human-approval', {
        message: `${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
      }),
      orderedFreezeCompletedEvent(2, freezeOutput),
      orderedApprovalEvent(3, 'node_completed', 'human-approval', {
        approval_decision: 'approved',
      }),
      orderedApprovalEvent(4, 'approval_received', 'human-approval', { decision: 'approved' })
    );

    await expect(
      actions['verify-approval']?.(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
      )
    ).rejects.toThrow(/stale or out of order/);
  });

  test('rejects approval policies with unknown keys or duplicate oracle/grant entries', () => {
    const workflow = makeApprovalWorkflow();
    expectPolicyRejected(
      workflow,
      [
        {
          nodeId: 'freeze',
          action: 'finalize-evidence',
          phase: 'planning-freeze',
          oracleFiles: ['oracle/plan.md'],
          command: 'echo pwned',
        },
      ],
      /unsupported keys/
    );
    expectPolicyRejected(
      workflow,
      [
        {
          nodeId: 'freeze',
          action: 'finalize-evidence',
          phase: 'planning-freeze',
          oracleFiles: ['oracle/plan.md', './oracle/plan.md'],
        },
      ],
      /entries must be unique/
    );
    expectPolicyRejected(
      workflow,
      [
        {
          nodeId: 'freeze',
          action: 'finalize-evidence',
          phase: 'planning-freeze',
          oracleFiles: ['oracle/plan.md'],
        },
        {
          nodeId: 'freeze',
          action: 'finalize-evidence',
          phase: 'planning-freeze',
          oracleFiles: ['oracle/other.md'],
        },
      ],
      /grants must be unique/
    );
  });

  test('resume rejects missing private session binding or malformed HMAC key', () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-key-resume',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const missingBinding = { ...session.policyMetadata, sessionBindingPath: undefined };
    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-key-resume',
        workflow,
        image: IMAGE_ID,
        policyMetadata: missingBinding,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/private session binding is missing|HMAC key is missing/);

    writeFileSync(session.policyMetadata.hmacKeyPath ?? '', Buffer.alloc(31));
    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-key-resume',
        workflow,
        image: IMAGE_ID,
        policyMetadata: session.policyMetadata,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/HMAC key is malformed/);
  });

  test('rejects approval verification when the human-facing request is not bound to freeze output', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
      {
        nodeId: 'approval-check',
        action: 'verify-approval',
        phase: 'planning-approval',
        approvalNodeId: 'human-approval',
        freezeNodeId: 'freeze',
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-unbound-approval',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-unbound-approval', workflow, { isolation_env_id: 'env-1' });
    const events = [
      approvalEvent('approval_requested', 'human-approval', { message: 'Approve older packet' }),
      approvalEvent('node_completed', 'human-approval', { approval_decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'approved' }),
    ];
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, events),
      snapshotArtifacts: async (_envId, destinationDir) => {
        mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
        writeFileSync(join(destinationDir, 'oracle', 'plan.md'), 'approved acceptance criteria\n');
      },
    });
    const freezeOutput = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    events.unshift(freezeCompletedEvent(freezeOutput));
    run.metadata.approval = { type: 'approval', nodeId: 'human-approval', resolved: 'approved' };

    await expect(
      actions['verify-approval']?.(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
      )
    ).rejects.toThrow(/did not bind/);
  });

  test('rejects mutated controller action manifests before freezing evidence', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-mutated-manifest',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-mutated-manifest', workflow, { isolation_env_id: 'env-1' });
    const grant = withManifestInput(grantFor(session.controllerActionGrants, 'freeze'), {
      nodeId: 'other-freeze',
    });
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, []),
      snapshotArtifacts: async () => undefined,
    });

    await expect(
      actions['finalize-evidence']?.(actionContext(run, workflow, grant))
    ).rejects.toThrow(/node binding/);
  });

  test('refuses rejected latest approval decisions', async () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
      {
        nodeId: 'approval-check',
        action: 'verify-approval',
        phase: 'planning-approval',
        approvalNodeId: 'human-approval',
        freezeNodeId: 'freeze',
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-rejected-approval',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const run = makeRun('run-rejected-approval', workflow, { isolation_env_id: 'env-1' });
    const events: WorkflowEventRecord[] = [];
    const actions = createHardenedControllerActions({
      session,
      store: makeStore(run, events),
      snapshotArtifacts: async (_envId, destinationDir) => {
        mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
        writeFileSync(join(destinationDir, 'oracle', 'plan.md'), 'approved acceptance criteria\n');
      },
    });
    const freezeOutput = (await actions['finalize-evidence']?.(
      actionContext(run, workflow, grantFor(session.controllerActionGrants, 'freeze'))
    )) as Record<string, unknown>;
    run.metadata.approval = { type: 'approval', nodeId: 'human-approval', resolved: 'approved' };
    events.push(
      freezeCompletedEvent(freezeOutput),
      approvalEvent('approval_requested', 'human-approval', {
        message: `${freezeOutput.binding_id} ${freezeOutput.oracle_digest}`,
      }),
      approvalEvent('node_completed', 'human-approval', { approval_decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'approved' }),
      approvalEvent('approval_received', 'human-approval', { decision: 'rejected' })
    );

    await expect(
      actions['verify-approval']?.(
        actionContext(run, workflow, grantFor(session.controllerActionGrants, 'approval-check'))
      )
    ).rejects.toThrow(/Latest planning approval decision is not approved/);
  });

  test('resume uses copied policy and rejects drift inside private run state', () => {
    const workflow = makeApprovalWorkflow();
    writeApprovalPolicy(workflow, [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['oracle/plan.md'],
      },
    ]);
    const session = prepareHardenedControllerSession({
      runId: 'run-policy-resume',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    writeApprovalPolicy(workflow, []);

    expect(
      resumeHardenedControllerSession({
        runId: 'run-policy-resume',
        workflow,
        image: IMAGE_ID,
        policyMetadata: session.policyMetadata,
        budget: resumeBudgetFor(session),
      }).controllerActionGrants
    ).toHaveLength(1);

    writeFileSync(session.policyMetadata.approvalPolicyPath ?? '', 'tampered', { mode: 0o600 });
    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-policy-resume',
        workflow,
        image: IMAGE_ID,
        policyMetadata: session.policyMetadata,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/copied approval policy digest changed/);
  });

  test('resume requires a v2 private binding even when no operator grants exist', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-no-policy',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const session = prepareHardenedControllerSession({
      runId: 'run-v2-baseline',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });

    expect(
      resumeHardenedControllerSession({
        runId: 'run-v2-baseline',
        workflow,
        image: IMAGE_ID,
        policyMetadata: session.policyMetadata,
        budget: resumeBudgetFor(session),
      }).controllerActionGrants
    ).toEqual([]);

    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-v2-baseline',
        workflow,
        image: IMAGE_ID,
        policyMetadata: { ...session.policyMetadata, sessionBindingPath: undefined },
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/private session binding is missing/);

    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-v2-baseline',
        workflow,
        image: IMAGE_ID,
        policyMetadata: {
          ...session.policyMetadata,
          schema: 'archon.hardened-controller-session.v1',
        },
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/schema is unsupported/);
  });

  test('resume rejects forged private keys from nested controller snapshot paths', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-forged-key',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const session = prepareHardenedControllerSession({
      runId: 'run-forged-key',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    const forgedDir = join(session.privateDir, 'snapshots', 'attacker');
    const forgedKey = Buffer.alloc(32, 7);
    mkdirSync(forgedDir, { recursive: true, mode: 0o700 });
    chmodSync(join(session.privateDir, 'snapshots'), 0o700);
    chmodSync(forgedDir, 0o700);
    const forgedMetadata = {
      ...session.policyMetadata,
      hmacKeyPath: join(forgedDir, 'controller-approval.hmac'),
      sessionBindingPath: join(forgedDir, 'controller-session-binding.json'),
    };
    const body = { policyDigest: digestStableTest(forgedMetadata) };
    writeFileSync(forgedMetadata.hmacKeyPath, forgedKey, { mode: 0o600 });
    writeFileSync(
      forgedMetadata.sessionBindingPath,
      JSON.stringify({ ...body, hmac: hmacForBodyTest(body, forgedKey) }, null, 2),
      { mode: 0o600 }
    );

    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-forged-key',
        workflow,
        image: IMAGE_ID,
        policyMetadata: forgedMetadata,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/private .*path is not controller-owned/);
  });

  test('binds operator-private egress to copied policy, image, and session HMAC', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-egress',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const egress = makeEgressPolicy('api.example.com');
    writeApprovalPolicy(
      workflow,
      [],
      { image: IMAGE_ID, policy: egress },
      makeProviderBudgetPolicy('openai', 'api.example.com')
    );

    const session = prepareHardenedControllerSession({
      runId: 'run-egress',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });
    writeApprovalPolicy(workflow, [], {
      image: IMAGE_ID,
      policy: makeEgressPolicy('other.example.com'),
    });

    expect(session.policyMetadata.egressPolicyB64).toEqual(expect.any(String));
    expect(getHardenedControllerEgressPolicy(session)).toEqual({
      targets: [{ host: 'api.example.com', port: 443 }],
      connectTimeoutMs: 10000,
      dnsTimeoutMs: 10000,
      idleTimeoutMs: 30000,
      maxTunnelMs: 300000,
      maxConcurrentConnections: 64,
      httpGrants: [
        {
          host: 'api.example.com',
          port: 443,
          methods: ['GET'],
          paths: [],
          pathPrefixes: ['/v1/'],
          maxBodyBytes: 1048576,
        },
      ],
    });
    expect(getHardenedControllerProxyBudgetSeed(session)).toMatchObject({
      grant: {
        schema: 'archon.proxy-budget-grant.v1',
        rootChainId: 'run-egress',
        runId: 'run-egress',
        workflowDigest: computeControllerWorkflowDigest(workflow),
        deadlineEpochMs: expect.any(Number),
        inputTokenLimit: 8000000,
        outputTokenLimit: 8000000,
        totalTokenLimit: 8000000,
      },
      providerPolicies: [
        {
          provider: 'openai',
          host: 'api.example.com',
          model: 'gpt-5.6-sol',
          maxInputTokens: 1024,
          maxOutputTokens: 256,
        },
      ],
    });

    const stripped = { ...session.policyMetadata, egressPolicyB64: undefined };
    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-egress',
        workflow,
        image: IMAGE_ID,
        policyMetadata: stripped,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/private session binding changed|provider budget seed lacks egress authority/);

    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-egress',
        workflow,
        image: IMAGE_ID,
        policyMetadata: {
          ...session.policyMetadata,
          approvalPolicyDigest: undefined,
          approvalPolicyPath: undefined,
        },
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/lacks operator authority/);

    writeFileSync(session.policyMetadata.approvalPolicyPath, 'tampered', { mode: 0o600 });
    expect(() => getHardenedControllerEgressPolicy(session)).toThrow(
      /copied approval policy digest changed/
    );
  });

  test('rejects malformed operator egress authority instead of minting metadata policy', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-bad-egress',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    writeApprovalPolicy(
      workflow,
      [],
      {
        image: OTHER_IMAGE_ID,
        policy: makeEgressPolicy('api.example.com'),
      },
      makeProviderBudgetPolicy('openai', 'api.example.com')
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-wrong-egress-image',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/egress policy does not match/);

    writeApprovalPolicy(
      workflow,
      [],
      {
        image: IMAGE_ID,
        policy: { ...makeEgressPolicy('api.example.com'), unsupported: true },
      },
      makeProviderBudgetPolicy('openai', 'api.example.com')
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-extra-egress-key',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/unsupported key|unsupported settings/);
  });

  test('binds explicit provider budget policy and rejects drift or stripped seed metadata', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-provider-budget',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    writeApprovalPolicy(
      workflow,
      [],
      { image: IMAGE_ID, policy: makeEgressPolicy('api.openai.com') },
      makeProviderBudgetPolicy('openai', 'api.openai.com', {
        allowedHeaders: { 'openai-beta': 'responses=v1' },
      })
    );
    const session = prepareHardenedControllerSession({
      runId: 'run-provider-budget',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
      budget: {
        deadlineAt: '2030-01-01T00:00:00.000Z',
        tokens: { total: 5000, input: 3000, output: 2000 },
      },
    });

    const seed = getHardenedControllerProxyBudgetSeed(session);
    if (!seed) throw new Error('missing provider budget seed');
    expect(seed).toMatchObject({
      grant: {
        rootChainId: 'run-provider-budget',
        runId: 'run-provider-budget',
        workflowDigest: computeControllerWorkflowDigest(workflow),
        deadlineEpochMs: Date.parse('2030-01-01T00:00:00.000Z'),
        inputTokenLimit: 3000,
        outputTokenLimit: 2000,
        totalTokenLimit: 5000,
      },
    });
    expect(seed.grant.policyDigest).toBe(
      digestStableTest({
        egressPolicyB64: session.policyMetadata.egressPolicyB64,
        image: IMAGE_ID,
        providerPolicies: seed.providerPolicies,
      })
    );
    expect(Object.isFrozen(seed)).toBe(true);
    expect(Object.isFrozen(seed.grant)).toBe(true);
    expect(Object.isFrozen(seed.providerPolicies)).toBe(true);
    expect(Object.isFrozen(seed.providerPolicies[0])).toBe(true);
    expect(Object.isFrozen(seed.providerPolicies[0]?.allowedHeaders)).toBe(true);
    expect(seed.providerPolicies[0]?.allowedHeaders).toEqual({ 'openai-beta': 'responses=v1' });

    const stripped = { ...session.policyMetadata };
    delete (stripped as Record<string, unknown>).proxyBudgetSeedDigest;
    expect(() =>
      resumeHardenedControllerSession({
        runId: 'run-provider-budget',
        workflow,
        image: IMAGE_ID,
        policyMetadata: stripped,
        budget: resumeBudgetFor(session),
      })
    ).toThrow(/private session binding changed|lacks operator authority/);

    const tamperedPolicy = Buffer.from(
      JSON.stringify({
        schema: 'archon.hardened-controller-planning-policy.v1',
        version: 1,
        workflowDigest: computeControllerWorkflowDigest(workflow),
        grants: [],
        egress: { image: IMAGE_ID, policy: makeEgressPolicy('api.openai.com') },
        providerBudget: makeProviderBudgetPolicy('openai', 'api.openai.com', {
          model: 'gpt-5.6-luna',
        }),
      })
    );
    writeFileSync(session.policyMetadata.approvalPolicyPath, tamperedPolicy, { mode: 0o600 });
    const tamperedMetadata = {
      ...session.policyMetadata,
      approvalPolicyDigest: createHash('sha256').update(tamperedPolicy).digest('hex'),
    };
    const body = { policyDigest: digestStableTest(tamperedMetadata) };
    writeFileSync(
      session.policyMetadata.sessionBindingPath,
      JSON.stringify({
        ...body,
        hmac: hmacForBodyTest(body, readFileSync(session.policyMetadata.hmacKeyPath)),
      }),
      { mode: 0o600 }
    );
    expect(() =>
      getHardenedControllerProxyBudgetSeed({ ...session, policyMetadata: tamperedMetadata })
    ).toThrow(/seed digest changed/);
  });

  test('deep-freezes exact provider header arrays and binds mutations to the seed digest', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-provider-header-array-budget',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const betaValues = [
      'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24',
      'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24,structured-outputs-2025-12-15',
    ];
    writeApprovalPolicy(
      workflow,
      [],
      { image: IMAGE_ID, policy: makeEgressPolicy('api.anthropic.com') },
      makeProviderBudgetPolicy('anthropic', 'api.anthropic.com', {
        anthropicBeta: true,
        allowedHeaders: {
          'anthropic-version': '2023-06-01',
          'anthropic-beta': betaValues,
        },
      })
    );
    const session = prepareHardenedControllerSession({
      runId: 'run-provider-header-array-budget',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
      budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 5000 } },
    });
    betaValues.push('mutated-after-policy-copy');

    const seed = getHardenedControllerProxyBudgetSeed(session);
    if (!seed) throw new Error('missing provider budget seed');
    const allowedHeaders = seed.providerPolicies[0]?.allowedHeaders;
    const betaHeader = allowedHeaders?.['anthropic-beta'];
    expect(Object.isFrozen(allowedHeaders)).toBe(true);
    expect(Object.isFrozen(betaHeader)).toBe(true);
    expect(betaHeader).toEqual([
      'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24',
      'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24,structured-outputs-2025-12-15',
    ]);
    expect(() => (betaHeader as string[]).push('runtime-mutation')).toThrow();
    expect(seed.grant.policyDigest).toBe(
      digestStableTest({
        egressPolicyB64: session.policyMetadata.egressPolicyB64,
        image: IMAGE_ID,
        providerPolicies: seed.providerPolicies,
      })
    );

    const resumed = resumeHardenedControllerSession({
      runId: 'run-provider-header-array-budget',
      workflow,
      image: IMAGE_ID,
      policyMetadata: session.policyMetadata,
      budget: resumeBudgetFor(session),
    });
    expect(
      getHardenedControllerProxyBudgetSeed(resumed)?.providerPolicies[0]?.allowedHeaders
    ).toEqual({
      'anthropic-version': '2023-06-01',
      'anthropic-beta': [
        'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24',
        'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24,structured-outputs-2025-12-15',
      ],
    });
  });

  test('rejects unsupported provider header profile names and values', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-provider-header-profile-rejections',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    for (const [index, allowedHeaders] of [
      { 'x-beta': 'context-1m-2025-08-07' },
      { 'anthropic-version': '' },
      { 'anthropic-version': ' 2023-06-01' },
      { 'anthropic-version': '2023-06-01\nnext' },
      { 'anthropic-beta': [] },
      { 'anthropic-beta': ['claude-code-20250219', 'claude-code-20250219'] },
      { 'anthropic-beta': ['claude-code-20250219,context-1m-2025-08-07'] },
      { 'anthropic-beta': ['context-1m-2025-08-07'] },
      { 'anthropic-beta': ['claude-code-20250219,,effort-2025-11-24'] },
      { 'anthropic-beta': ['claude-code-20250219', ''] },
    ].entries()) {
      writeApprovalPolicy(
        workflow,
        [],
        { image: IMAGE_ID, policy: makeEgressPolicy('api.anthropic.com') },
        makeProviderBudgetPolicy('anthropic', 'api.anthropic.com', {
          anthropicBeta: true,
          allowedHeaders,
        })
      );
      expect(() =>
        prepareHardenedControllerSession({
          runId: `run-provider-header-profile-reject-${index}`,
          workflow,
          sourceRoot: source,
          conversationId: 'cli-conv',
          userMessage: 'ship',
          image: IMAGE_ID,
        })
      ).toThrow(
        /unsupported header allowance|header value|unsupported token|unsafe|duplicate header values/
      );
    }
  });

  test('rejects provider egress without explicit usable provider budget caps', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-missing-provider-budget',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });

    writeApprovalPolicy(workflow, [], {
      image: IMAGE_ID,
      policy: makeEgressPolicy('api.example.com'),
    });
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-missing-provider-budget',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/provider budget/);

    writeApprovalPolicy(
      workflow,
      [],
      { image: IMAGE_ID, policy: makeEgressPolicy('api.example.com') },
      makeProviderBudgetPolicy('openai', 'api.example.com', { maxInputTokens: 1001 })
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-provider-budget-over-cap',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
        budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 1000 } },
      })
    ).toThrow(/exceeds workflow budget/);

    writeApprovalPolicy(
      workflow,
      [],
      { image: IMAGE_ID, policy: makeEgressPolicy('api.example.com') },
      makeProviderBudgetPolicy('anthropic', 'api.example.com', {
        allowedHeaders: { 'Anthropic-Version': '2023-06-01' },
      })
    );
    expect(() =>
      prepareHardenedControllerSession({
        runId: 'run-provider-budget-bad-header',
        workflow,
        sourceRoot: source,
        conversationId: 'cli-conv',
        userMessage: 'ship',
        image: IMAGE_ID,
      })
    ).toThrow(/unsupported header allowance/);
  });

  test('keeps deterministic no-egress hardened sessions valid without provider budget seed', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-no-egress-deterministic',
      hardened: { required: true },
      nodes: [{ id: 'test', bash: 'echo ok' }],
    });
    const session = prepareHardenedControllerSession({
      runId: 'run-no-egress',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });

    expect(session.policyMetadata.egressPolicyB64).toBeUndefined();
    expect(session.policyMetadata.proxyBudgetSeedDigest).toBeUndefined();
    expect(getHardenedControllerEgressPolicy(session)).toBeUndefined();
    expect(getHardenedControllerProxyBudgetSeed(session)).toBeUndefined();
  });

  test('does not mint controller-action grants from workflow YAML alone', () => {
    const workflow = makeTestWorkflow({
      name: 'hardened-publish',
      hardened: { required: true },
      nodes: [{ id: 'ship', controller_action: 'publish', phase: 'publication' }],
    });

    const session = prepareHardenedControllerSession({
      runId: 'run-pub',
      workflow,
      sourceRoot: source,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    });

    expect(session.controllerActionGrants).toEqual([]);
  });
});

describe('createRefusingHardenedControllerActions', () => {
  test('fails closed for publication until independent receipts are wired', async () => {
    const actions = createRefusingHardenedControllerActions();
    await expect(actions.publish?.({} as never)).rejects.toThrow(/independently verified receipt/);
  });
});

function makeCandidateImportWorkflow(): WorkflowDefinition {
  return makeTestWorkflow({
    name: 'hardened-candidate-import',
    hardened: { required: true },
    nodes: [
      {
        id: 'candidate-import',
        controller_action: 'finalize-evidence',
        phase: 'candidate-import',
      },
    ],
  });
}

function makeCandidateBlackboxWorkflow(): WorkflowDefinition {
  return makeTestWorkflow({
    name: 'hardened-candidate-blackbox',
    hardened: { required: true },
    nodes: [
      { id: 'freeze', controller_action: 'finalize-evidence', phase: 'planning-freeze' },
      {
        id: 'human-approval',
        depends_on: ['freeze'],
        approval: { message: 'Review $freeze.output.binding_id $freeze.output.oracle_digest' },
      },
      {
        id: 'approval-check',
        depends_on: ['human-approval'],
        controller_action: 'verify-approval',
        phase: 'planning-approval',
      },
      {
        id: 'candidate-import',
        depends_on: ['approval-check'],
        controller_action: 'finalize-evidence',
        phase: 'candidate-import',
      },
      {
        id: 'candidate-blackbox',
        depends_on: ['candidate-import'],
        controller_action: 'finalize-evidence',
        phase: 'candidate-blackbox-test',
      },
    ],
  });
}

function makeApprovalWorkflow(): WorkflowDefinition {
  return makeTestWorkflow({
    name: 'hardened-approval',
    hardened: { required: true },
    nodes: [
      { id: 'freeze', controller_action: 'finalize-evidence', phase: 'planning-freeze' },
      {
        id: 'human-approval',
        depends_on: ['freeze'],
        approval: { message: 'Review $freeze.output.binding_id $freeze.output.oracle_digest' },
      },
      {
        id: 'approval-check',
        depends_on: ['human-approval'],
        controller_action: 'verify-approval',
        phase: 'planning-approval',
      },
    ],
  });
}

function writeApprovalPolicy(
  workflow: WorkflowDefinition,
  grants: unknown[],
  egress?: unknown,
  providerBudget?: unknown,
  validatorNodeModules?: string
): void {
  const policyDir = join(home, 'controller-policy');
  mkdirSync(home, { recursive: true, mode: 0o700 });
  chmodSync(home, 0o700);
  mkdirSync(policyDir, { recursive: true, mode: 0o700 });
  chmodSync(policyDir, 0o700);
  writeFileSync(
    join(policyDir, 'planning-approval.json'),
    JSON.stringify({
      schema: 'archon.hardened-controller-planning-policy.v1',
      version: 1,
      workflowDigest: computeControllerWorkflowDigest(workflow),
      grants,
      ...(egress ? { egress } : {}),
      ...(providerBudget ? { providerBudget } : {}),
      ...(validatorNodeModules
        ? { validatorSource: { nodeModulesPath: validatorNodeModules } }
        : {}),
    }),
    { mode: 0o600 }
  );
  chmodSync(join(policyDir, 'planning-approval.json'), 0o600);
}

function createValidatorNodeModules(root: string): {
  nodeModules: string;
  playwrightDigest: string;
  coreDigest: string;
} {
  const nodeModules = join(root, 'validator-node-modules');
  const playwright = join(nodeModules, 'playwright');
  const core = join(nodeModules, 'playwright-core');
  mkdirSync(playwright, { recursive: true });
  mkdirSync(core, { recursive: true });
  chmodSync(nodeModules, 0o700);
  const playwrightPackage = join(playwright, 'package.json');
  const corePackage = join(core, 'package.json');
  const playwrightRunner = join(playwright, 'lib', 'runner.js');
  const coreClient = join(core, 'lib', 'client.js');
  mkdirSync(dirname(playwrightRunner), { recursive: true });
  mkdirSync(dirname(coreClient), { recursive: true });
  writeFileSync(playwrightPackage, JSON.stringify({ name: 'playwright', version: '1.60.0' }), {
    mode: 0o600,
  });
  writeFileSync(corePackage, JSON.stringify({ name: 'playwright-core', version: '1.60.0' }), {
    mode: 0o600,
  });
  writeFileSync(playwrightRunner, 'module.exports = "playwright-runner";\n', { mode: 0o600 });
  writeFileSync(coreClient, 'module.exports = "playwright-core-client";\n', { mode: 0o600 });
  return {
    nodeModules,
    playwrightDigest: validatorTreeDigestTest(playwright),
    coreDigest: validatorTreeDigestTest(core),
  };
}

function validatorTreeDigestTest(root: string): string {
  const files: Record<string, unknown>[] = [];
  collectValidatorTreeTest(root, root, files);
  return digestStableTest(
    files.sort((left, right) => compareCodepointTest(String(left.path), String(right.path)))
  );
}

function compareCodepointTest(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function collectValidatorTreeTest(
  root: string,
  current: string,
  files: Record<string, unknown>[]
): void {
  for (const entry of readdirSync(current, { withFileTypes: true })) {
    const path = join(current, entry.name);
    if (entry.isDirectory()) {
      collectValidatorTreeTest(root, path, files);
      continue;
    }
    if (!entry.isFile()) continue;
    const bytes = readFileSync(path);
    files.push({
      path: path
        .slice(root.length + 1)
        .split(/[\\/]+/)
        .join('/'),
      sha256: createHash('sha256').update(bytes).digest('hex'),
      size: bytes.byteLength,
      type: 'file',
    });
  }
}

function browserSecurityFixture(): Record<string, unknown> {
  const blocked = { blocked: true, code: 0, signal: null, result: -1, errno: 1 };
  return {
    status: { Seccomp: '2', NoNewPrivs: '1', CapEff: '0', CapBnd: '0' },
    identity: { uid: 1000, gid: 1000 },
    arch: 'x64',
    controls: {
      mount: blocked,
      bpf: blocked,
      init_module: blocked,
      chroot: blocked,
      parent_setns: blocked,
    },
  };
}

function makeEgressPolicy(host: string): Record<string, unknown> {
  return {
    targets: [{ host, port: 443 }],
    httpGrants: [{ host, port: 443, methods: ['GET'], pathPrefixes: ['/v1/'] }],
  };
}

function makeProviderBudgetPolicy(
  provider: 'openai' | 'anthropic',
  host: string,
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  const base = {
    policies: [
      {
        provider,
        host,
        model: provider === 'openai' ? 'gpt-5.6-sol' : 'claude-opus-4-8',
        maxInputTokens: 1024,
        maxOutputTokens: 256,
        ...(provider === 'anthropic' ? { anthropicBeta: false } : {}),
        ...overrides,
      },
    ],
  };
  return base;
}

function hmacForBodyTest(body: Record<string, unknown>, key: Buffer): string {
  return createHmac('sha256', key).update(digestStableTest(body)).digest('hex');
}

function readSealedReceiptTest(path: string, keyPath: string): Record<string, unknown> {
  const sealed = JSON.parse(readFileSync(path, 'utf8')) as Record<string, unknown>;
  const hmac = sealed.hmac;
  const body = { ...sealed };
  delete body.hmac;
  expect(hmac).toBe(hmacForBodyTest(body, readFileSync(keyPath)));
  return body;
}

function writeSealedReceiptTest(
  path: string,
  body: Record<string, unknown>,
  keyPath: string,
  hmac = hmacForBodyTest(body, readFileSync(keyPath))
): void {
  writeFileSync(path, JSON.stringify({ ...body, hmac }, null, 2), { mode: 0o600 });
  chmodSync(path, 0o600);
}

function digestStableTest(value: unknown): string {
  return createHash('sha256').update(stableSerializeTest(value)).digest('hex');
}

function stableSerializeTest(value: unknown): string {
  if (value === undefined) return 'null';
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableSerializeTest).join(',')}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .filter(key => record[key] !== undefined)
    .map(key => `${JSON.stringify(key)}:${stableSerializeTest(record[key])}`)
    .join(',')}}`;
}

function grantFor(grants: readonly ControllerActionGrant[], nodeId: string): ControllerActionGrant {
  const grant = grants.find(candidate => candidate.nodeId === nodeId);
  if (!grant) throw new Error(`missing test grant ${nodeId}`);
  return grant;
}

function withManifestInput(
  grant: ControllerActionGrant,
  input: Record<string, unknown>
): ControllerActionGrant {
  const manifestInput = { ...grant.actionManifest.input, ...input };
  const manifest = {
    id: grant.actionManifest.id,
    input: manifestInput,
    digest: computeControllerActionManifestDigest({
      id: grant.actionManifest.id,
      input: manifestInput,
    }),
  };
  return { ...grant, actionManifest: manifest };
}

function makeRun(
  id: string,
  workflow: WorkflowDefinition,
  metadata: Record<string, unknown>
): WorkflowRun {
  return {
    id,
    workflow_name: workflow.name,
    conversation_id: 'conv',
    parent_conversation_id: null,
    codebase_id: null,
    status: 'running',
    user_message: 'ship',
    metadata,
    started_at: new Date('2026-01-01T00:00:00.000Z'),
    completed_at: null,
    last_activity_at: null,
    working_path: currentSource,
    user_id: null,
    parent_run_id: null,
    output_root: null,
  };
}

function makeStore(run: WorkflowRun, events: WorkflowEventRecord[]): IWorkflowStore {
  return {
    getWorkflowRun: async () => run,
    listWorkflowEvents: async () => events,
    getWorkflowRunStatus: async () => 'running',
  } as unknown as IWorkflowStore;
}

function actionContext(
  run: WorkflowRun,
  workflow: WorkflowDefinition,
  grant: ControllerActionGrant
): ControllerActionHandlerContext {
  const node = workflow.nodes.find(candidate => candidate.id === grant.nodeId);
  if (!node || !('controller_action' in node))
    throw new Error(`missing controller node ${grant.nodeId}`);
  return {
    workflowRun: run,
    workflowName: workflow.name,
    workflowDigest: computeControllerWorkflowDigest(workflow),
    node,
    actionManifest: grant.actionManifest,
    signal: new AbortController().signal,
    deadlineAt: Date.now() + 30_000,
    cwd: currentSource,
    artifactsDir: '',
    stateDir: '',
    logDir: '',
    baseBranch: 'main',
    docsDir: '',
    config: { assistant: 'claude', assistants: { claude: {} }, commands: {} },
    platform: {
      sendMessage: async () => undefined,
      getStreamingMode: () => 'batch',
      getPlatformType: () => 'test',
    },
    conversationId: 'conv',
    execContext: { kind: 'container', profile: 'hardened', containerId: 'cid-1' },
  } as ControllerActionHandlerContext;
}

function approvalEvent(
  eventType: string,
  stepName: string,
  data: Record<string, unknown>
): WorkflowEventRecord {
  return { event_type: eventType, step_name: stepName, event_order: nextTestEventOrder++, data };
}

function freezeCompletedEvent(output: Record<string, unknown>): WorkflowEventRecord {
  return approvalEvent('node_completed', 'freeze', {
    node_output: JSON.stringify(output),
    type: 'controller_action',
    action: 'finalize-evidence',
    phase: 'planning-freeze',
  });
}

function approvalPolicyGrants(): unknown[] {
  return [
    {
      nodeId: 'freeze',
      action: 'finalize-evidence',
      phase: 'planning-freeze',
      oracleFiles: ['oracle/plan.md'],
    },
    {
      nodeId: 'approval-check',
      action: 'verify-approval',
      phase: 'planning-approval',
      approvalNodeId: 'human-approval',
      freezeNodeId: 'freeze',
    },
  ];
}

function writeOracle(destinationDir: string, contents: string): void {
  mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
  writeFileSync(join(destinationDir, 'oracle', 'plan.md'), contents);
}

function expectPolicyRejected(
  workflow: WorkflowDefinition,
  grants: unknown[],
  pattern: RegExp
): void {
  writeApprovalPolicy(workflow, grants);
  expect(() =>
    prepareHardenedControllerSession({
      runId: `run-reject-${Math.random().toString(16).slice(2)}`,
      workflow,
      sourceRoot: currentSource,
      conversationId: 'cli-conv',
      userMessage: 'ship',
      image: IMAGE_ID,
    })
  ).toThrow(pattern);
}

function resumeBudgetFor(session: { workflowBudgetGrants: readonly WorkflowBudgetGrant[] }): {
  deadlineAt: string;
  tokens: WorkflowBudgetGrant['tokens'];
} {
  const grant = session.workflowBudgetGrants[0];
  if (!grant) throw new Error('missing test budget grant');
  return { deadlineAt: grant.deadlineAt, tokens: { ...grant.tokens } };
}

function orderedApprovalEvent(
  order: number,
  eventType: string,
  stepName: string,
  data: Record<string, unknown>
): WorkflowEventRecord {
  return { event_type: eventType, step_name: stepName, event_order: order, data };
}

function orderedFreezeCompletedEvent(
  order: number,
  output: Record<string, unknown>
): WorkflowEventRecord {
  return {
    ...freezeCompletedEvent(output),
    event_order: order,
  };
}

function createBaselineAndCandidateBundle(
  baseline: string,
  writer: string,
  bundlePath: string,
  mode: 'changed' | 'same'
): void {
  mkdirSync(baseline, { recursive: true });
  writeFileSync(join(baseline, 'app.txt'), 'baseline v1\n');
  git(baseline, ['init']);
  git(baseline, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', '.']);
  git(baseline, [
    '-c',
    'user.name=Test',
    '-c',
    'user.email=test@example.com',
    'commit',
    '-m',
    'baseline',
  ]);
  execFileSync('git', ['clone', '--no-hardlinks', baseline, writer], { stdio: 'ignore' });
  if (mode === 'changed') {
    writeFileSync(join(writer, 'app.txt'), 'candidate v2\n');
    writeFileSync(join(writer, 'new.txt'), 'new file\n');
    git(writer, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', '.']);
    git(writer, [
      '-c',
      'user.name=Test',
      '-c',
      'user.email=test@example.com',
      'commit',
      '-m',
      'candidate',
    ]);
  }
  git(writer, ['update-ref', 'refs/candidates/sealed', 'HEAD']);
  git(writer, ['bundle', 'create', bundlePath, 'refs/candidates/sealed']);
  chmodSync(bundlePath, 0o600);
}

function fileSha256(path: string): string {
  return execFileSync('shasum', ['-a', '256', path], { encoding: 'utf8' }).split(' ')[0] ?? '';
}

function createCandidateRepo(worktree: string, bare: string): void {
  mkdirSync(worktree, { recursive: true });
  writeFileSync(join(worktree, 'app.txt'), 'app v1\n');
  writeFileSync(join(worktree, 'script.sh'), '#!/bin/sh\necho ok\n', { mode: 0o755 });
  git(worktree, ['init']);
  git(worktree, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', '.']);
  git(worktree, [
    '-c',
    'user.name=Test',
    '-c',
    'user.email=test@example.com',
    'commit',
    '-m',
    'candidate',
  ]);
  execFileSync('git', ['clone', '--bare', '--no-hardlinks', worktree, bare], { stdio: 'ignore' });
  chmodSync(bare, 0o700);
  git(worktree, ['push', bare, 'HEAD:refs/candidates/sealed']);
}

function createChildRepo(parent: string, name: string, readme: string): string {
  const repo = join(parent, name);
  mkdirSync(repo, { recursive: true });
  writeFileSync(join(repo, 'README.md'), readme);
  git(repo, ['init']);
  git(repo, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', 'README.md']);
  git(repo, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-m', 'seed']);
  return repo;
}

function copyPrivateDirectory(source: string, destination: string): void {
  mkdirSync(destination, { recursive: true, mode: 0o700 });
  for (const entry of readdirSync(source, { withFileTypes: true })) {
    const from = join(source, entry.name);
    const to = join(destination, entry.name);
    if (entry.isDirectory()) {
      copyPrivateDirectory(from, to);
    } else if (entry.isFile()) {
      copyFileSync(from, to);
      chmodSync(to, 0o600);
    }
  }
  chmodSync(destination, 0o700);
}

function createChildRepoWithParent(
  parent: string,
  name: string,
  parentReadme: string,
  baselineReadme: string
): string {
  const repo = createChildRepo(parent, name, parentReadme);
  writeFileSync(join(repo, 'README.md'), baselineReadme);
  git(repo, [
    '-c',
    'user.name=Test',
    '-c',
    'user.email=test@example.com',
    'commit',
    '-am',
    'baseline',
  ]);
  return repo;
}

function git(cwd: string, args: string[]): void {
  execFileSync('git', ['-C', cwd, ...args], { stdio: 'ignore' });
}

function gitBareExpect(gitDir: string, args: string[]): void {
  execFileSync('git', ['--git-dir', gitDir, ...args], { stdio: 'ignore' });
}

function gitText(cwd: string, args: string[]): string {
  return execFileSync('git', ['-C', cwd, ...args], { stdio: ['ignore', 'pipe', 'pipe'] })
    .toString('utf8')
    .trim();
}
