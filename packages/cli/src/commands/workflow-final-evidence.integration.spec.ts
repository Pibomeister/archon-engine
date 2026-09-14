import { expect, test } from 'bun:test';
import { mkdir, mkdtemp, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { trackTempRoots } from '@archon/paths/test-utils';

const temp = trackTempRoots();
const cli = resolve(import.meta.dir, '../cli.ts');
const repoRoot = resolve(import.meta.dir, '../../..');

const factoryExecution = {
  factoryJobId: 'job:final-evidence',
  logicalChainId: 'chain:final-evidence',
  readySnapshotId: 'ready:final-evidence',
  readyDigest: `sha256:${'a'.repeat(64)}`,
  commandId: 'cmd:final-evidence',
  launchId: 'launch:final-evidence',
  launchKey: 'launch-key:final-evidence',
  attemptId: 'attempt:final-evidence',
  runtimeBundleId: 'runtime:final-evidence',
};

const manifest = {
  kind: 'factory-merge-review.v1',
  repository: { provider: 'github', owner: 'GoodwordTeam', name: 'archon' },
  pullRequestNumber: 42,
  headSha: 'b'.repeat(40),
  execution: factoryExecution,
};

async function runProcess(
  args: string[],
  env: Record<string, string>,
  cwd: string
): Promise<{ code: number; stdout: string; stderr: string }> {
  const child = Bun.spawn(args, {
    cwd,
    env: { ...process.env, ...env },
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code, stdout, stderr };
}

async function seedRun(input: {
  env: Record<string, string>;
  cwd: string;
  workflowName?: string;
  files: Record<string, unknown>;
  outputRoot?: string;
}): Promise<{ runId: string; artifactsDir: string; outputRoot: string }> {
  const script = `
    const { randomUUID } = await import('node:crypto');
    const { mkdir, writeFile } = await import('node:fs/promises');
    const paths = await import('@archon/paths');
    const { getDatabase } = await import('@archon/core/db/connection');
    const workflows = await import('@archon/core/db/workflows');
    const db = getDatabase();
    const conversationId = randomUUID();
    await db.query(
      "INSERT INTO remote_agent_conversations (id, platform_type, platform_conversation_id) VALUES ($1, 'cli', $2)",
      [conversationId, conversationId]
    );
    const outputRoot = ${JSON.stringify(input.outputRoot)} ?? paths.getProjectStoragePaths({ kind: 'cwd', cwd: ${JSON.stringify(input.cwd)} }).root;
    const run = await workflows.createWorkflowRun({
      conversation_id: conversationId,
      workflow_name: ${JSON.stringify(input.workflowName ?? 'portable-single-repo-feature')},
      user_message: 'final evidence test',
      working_path: ${JSON.stringify(input.cwd)},
    });
    await workflows.updateWorkflowRun(run.id, { output_root: outputRoot, status: 'running' });
    await workflows.completeWorkflowRun(run.id, { duration_ms: 2000 });
    await db.query("UPDATE remote_agent_workflow_runs SET completed_at = $1, started_at = $2, last_activity_at = $3 WHERE id = $4", [
      '2026-09-05T00:00:02.000Z',
      '2026-09-05T00:00:00.000Z',
      '2026-09-05T00:00:02.000Z',
      run.id,
    ]);
    const artifactsDir = paths.getRunArtifactsDirForRoot(outputRoot, run.id);
    await mkdir(artifactsDir, { recursive: true });
    const files = ${JSON.stringify(input.files)};
    for (const [name, value] of Object.entries(files)) {
      await writeFile(artifactsDir + '/' + name, JSON.stringify(value) + '\\n', { mode: 0o600 });
    }
    console.log(JSON.stringify({ runId: run.id, artifactsDir, outputRoot }));
    await db.close?.();
  `;
  const result = await runProcess([process.execPath, '-e', script], input.env, repoRoot);
  expect(result.code, result.stderr + result.stdout).toBe(0);
  const jsonLine = result.stdout
    .trim()
    .split('\n')
    .reverse()
    .find(line => line.trim().startsWith('{'));
  if (!jsonLine) throw new Error('seed helper did not print JSON: ' + result.stdout);
  return JSON.parse(jsonLine) as { runId: string; artifactsDir: string; outputRoot: string };
}

async function makeFixture(): Promise<{
  root: string;
  project: string;
  home: string;
  env: Record<string, string>;
}> {
  const root = await mkdtemp(join(tmpdir(), 'archon-final-evidence-'));
  temp(root);
  const project = join(root, 'project');
  const home = join(root, 'home');
  await mkdir(join(project, '.archon/workflows'), { recursive: true });
  await mkdir(home, { recursive: true });
  const git = Bun.spawn(['git', 'init'], { cwd: project, stdout: 'ignore', stderr: 'ignore' });
  expect(await git.exited).toBe(0);
  return {
    root,
    project,
    home,
    env: {
      PATH: process.env.PATH ?? '/usr/bin:/bin',
      HOME: home,
      ARCHON_HOME: home,
      DATABASE_URL: '',
      TELEMETRY_DISABLED: '1',
    },
  };
}

async function finalEvidence(
  runId: string,
  fixture: { project: string; env: Record<string, string> }
): Promise<{ code: number; stdout: string; stderr: string }> {
  return runProcess(
    [
      process.execPath,
      cli,
      'workflow',
      'final-evidence',
      runId,
      '--cwd',
      fixture.project,
      '--json',
    ],
    fixture.env,
    fixture.project
  );
}

test('workflow final-evidence exports current Goodword managed PR metadata', async () => {
  const fixture = await makeFixture();
  const runContext = {
    binding: { branch: 'factory/final-evidence' },
    profile: { delivery: { baseBranch: 'main' }, repository: { defaultBranch: 'trunk' } },
  };
  const prEvidence = {
    ready: true,
    draft: true,
    url: 'https://github.com/GoodwordTeam/archon/pull/42',
    head: manifest.headSha,
    baseCommit: 'c'.repeat(40),
    workProduct: { files: [{ path: 'src/index.ts', sha256: 'd'.repeat(64) }] },
    mergeReviewManifest: manifest,
    mergeReviewManifestPath: 'ignored-by-final-evidence-command',
  };
  const seeded = await seedRun({
    env: fixture.env,
    cwd: fixture.project,
    files: {
      'run-context.json': runContext,
      'pr-evidence.json': prEvidence,
      'merge-review-manifest.json': manifest,
    },
  });

  const result = await finalEvidence(seeded.runId, fixture);
  expect(result.code, result.stderr + result.stdout).toBe(0);
  const body = JSON.parse(result.stdout) as { finalEvidence: Record<string, unknown> };
  expect(body.finalEvidence).toMatchObject({
    runId: seeded.runId,
    workflowId: 'portable-single-repo-feature',
    createdAt: '2026-09-05T00:00:02.000Z',
    revision: 1,
    status: 'completed',
    execution: factoryExecution,
    pullRequest: {
      href: 'https://github.com/GoodwordTeam/archon/pull/42',
      repository: { provider: 'github', owner: 'GoodwordTeam', name: 'archon' },
      number: 42,
      headSha: manifest.headSha,
      headBranch: 'factory/final-evidence',
      baseBranch: 'main',
    },
  });
  expect(body.finalEvidence.artifacts).toBeArray();
  expect(JSON.stringify(body)).not.toContain('run-context.json');
});

test('workflow final-evidence reports missing run without inventing evidence', async () => {
  const fixture = await makeFixture();
  const result = await finalEvidence('00000000-0000-0000-0000-000000000000', fixture);
  expect(result.code).toBe(1);
  expect(JSON.parse(result.stdout)).toEqual({ ok: false, code: 'final_evidence_not_found' });
});

test('workflow final-evidence emits terminal metadata for an existing completed run without pr-evidence', async () => {
  const fixture = await makeFixture();
  const seeded = await seedRun({ env: fixture.env, cwd: fixture.project, files: {} });

  const result = await finalEvidence(seeded.runId, fixture);
  expect(result.code, result.stderr + result.stdout).toBe(0);
  const body = JSON.parse(result.stdout) as { finalEvidence: Record<string, unknown> };
  expect(body.finalEvidence).toMatchObject({
    runId: seeded.runId,
    workflowId: 'portable-single-repo-feature',
    createdAt: '2026-09-05T00:00:02.000Z',
    revision: 1,
    status: 'completed',
    artifacts: [],
  });
  expect(body.finalEvidence).not.toHaveProperty('pullRequest');
  expect(body.finalEvidence).not.toHaveProperty('execution');
});

test('workflow final-evidence accepts verified no-change without a pull request', async () => {
  const fixture = await makeFixture();
  const seeded = await seedRun({
    env: fixture.env,
    cwd: fixture.project,
    files: {
      'pr-evidence.json': {
        ready: true,
        draft: false,
        status: 'fulfilled-no-change',
        lifecycleResult: 'fulfilled-no-change',
        workProduct: { files: [] },
      },
      'no-change-closure-status.json': { eligible: true, lifecycleResult: 'fulfilled-no-change' },
      'no-change-closure-intent.json': {
        lifecycleResult: 'fulfilled-no-change',
        workProduct: { files: [] },
      },
    },
  });

  const result = await finalEvidence(seeded.runId, fixture);
  expect(result.code, result.stderr + result.stdout).toBe(0);
  const body = JSON.parse(result.stdout) as { finalEvidence: Record<string, unknown> };
  expect(body.finalEvidence).toMatchObject({
    runId: seeded.runId,
    lifecycleResult: 'fulfilled-no-change',
    status: 'fulfilled-no-change',
  });
  expect(body.finalEvidence).not.toHaveProperty('pullRequest');
});

test('workflow final-evidence accepts ordinary completed terminal output without a pull request', async () => {
  const fixture = await makeFixture();
  const seeded = await seedRun({
    env: fixture.env,
    cwd: fixture.project,
    files: {
      'pr-evidence.json': {
        ready: false,
        draft: { title: 'Prepare PR manually', body: 'Awaiting authorization.' },
        status: 'publication_requires_authorization',
        workProduct: { files: [{ path: 'src/index.ts', sha256: 'd'.repeat(64) }] },
      },
    },
  });

  const result = await finalEvidence(seeded.runId, fixture);
  expect(result.code, result.stderr + result.stdout).toBe(0);
  const body = JSON.parse(result.stdout) as { finalEvidence: Record<string, unknown> };
  expect(body.finalEvidence).toMatchObject({
    runId: seeded.runId,
    status: 'publication_requires_authorization',
  });
  expect(body.finalEvidence).not.toHaveProperty('pullRequest');
});

test('workflow final-evidence rejects managed PR output when the manifest is missing or changed', async () => {
  const missing = await makeFixture();
  const missingRun = await seedRun({
    env: missing.env,
    cwd: missing.project,
    files: {
      'pr-evidence.json': {
        ready: true,
        draft: true,
        url: 'https://github.com/GoodwordTeam/archon/pull/42',
        head: manifest.headSha,
      },
    },
  });
  const missingResult = await finalEvidence(missingRun.runId, missing);
  expect(missingResult.code).toBe(1);
  expect(JSON.parse(missingResult.stdout).error).toContain('missing merge-review-manifest.json');

  const changed = await makeFixture();
  const changedRun = await seedRun({
    env: changed.env,
    cwd: changed.project,
    files: {
      'run-context.json': {
        binding: { branch: 'factory/final-evidence' },
        profile: { delivery: { baseBranch: 'main' }, repository: { defaultBranch: 'main' } },
      },
      'pr-evidence.json': {
        ready: true,
        draft: true,
        url: 'https://github.com/GoodwordTeam/archon/pull/42',
        head: manifest.headSha,
        mergeReviewManifest: { ...manifest, headSha: 'c'.repeat(40) },
      },
      'merge-review-manifest.json': manifest,
    },
  });
  const changedResult = await finalEvidence(changedRun.runId, changed);
  expect(changedResult.code).toBe(1);
  expect(JSON.parse(changedResult.stdout).error).toContain('do not match');
});

test('workflow final-evidence rejects linked final artifacts', async () => {
  const fixture = await makeFixture();
  const seeded = await seedRun({
    env: fixture.env,
    cwd: fixture.project,
    files: {
      'pr-evidence.json': {
        ready: false,
        status: 'publication_requires_authorization',
      },
    },
  });
  await writeFile(join(seeded.artifactsDir, 'outside.json'), '{}\n');
  await symlink(
    join(seeded.artifactsDir, 'outside.json'),
    join(seeded.artifactsDir, 'no-change-closure-status.json')
  );

  const result = await finalEvidence(seeded.runId, fixture);
  expect(result.code).toBe(1);
  expect(JSON.parse(result.stdout).error).toContain('regular file without links');
});
