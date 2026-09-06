import { expect, test } from 'bun:test';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { closeSync, existsSync, realpathSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { trackTempRoots, openAnonymousFixtureFd } from '@archon/paths/test-utils';
import { factoryRequestDigest } from '../../../providers/src/factory-digest';
import golden from '../../../providers/test/fixtures/factory-provider-broker.v1.json';

const temp = trackTempRoots();
const cli = resolve(import.meta.dir, '../cli.ts');
type Config = typeof golden.config & {
  successor?: {
    parentRunId: string;
    parentAttemptId: string;
    parentBindingDigest: string;
    commandId: string;
  };
};
interface Run {
  id: string;
  status: string;
  metadata: Record<string, unknown>;
  adopted_from_run_id?: string;
}

test('native resume rejects changed qualified binding before bash and authorized successor retains it', async () => {
  const root = realpathSync(await mkdtemp(join(tmpdir(), 'archon-factory-binding-')));
  temp(root);
  const project = join(root, 'project');
  const home = join(root, 'home');
  const manual = join(root, 'manual');
  await mkdir(join(project, '.archon/workflows'), { recursive: true });
  await mkdir(home);
  await mkdir(manual);
  await writeFile(
    join(project, '.archon/workflows/parent.yaml'),
    `name: binding-parent
description: Parent with a protected post-gate effect.
interactive: true
nodes:
  - id: prepare
    bash: echo original
  - id: review
    depends_on: [prepare]
    approval:
      message: Review original
      decisions: [{id: approve}, {id: reject}]
  - id: effect
    depends_on: [review]
    bash: |
      echo visited > after-resume.txt
      exit 1
`
  );
  await writeFile(
    join(project, '.archon/workflows/child.yaml'),
    `name: binding-child
description: A bounded successor with a pinned declared model.
provider: codex
model: fixture-model
nodes:
  - id: helper
    workflow: binding-helper
  - id: effect
    depends_on: [helper]
    bash: echo successor > after-successor.txt
`
  );
  await writeFile(
    join(project, '.archon/workflows/helper.yaml'),
    'name: binding-helper\ndescription: Native internal child preserves the managed binding.\nprovider: codex\nmodel: fixture-model\nnodes:\n  - id: effect\n    bash: echo internal > internal-child.txt\n'
  );
  const base: Config = structuredClone(golden.config);
  base.endpoint = 'http://127.0.0.1:9';
  base.managedRun.worktreePath = project;
  base.providerPolicy.allowedWriteRoots = [project];
  base.providerPolicy.allowedReadRoots = [project];
  base.providerPolicy.deniedRoots = [manual];

  async function invoke(
    args: string[],
    config?: Config,
    json = true
  ): Promise<{ code: number; stdout: string; stderr: string }> {
    const descriptor = config ? openAnonymousFixtureFd(root, JSON.stringify(config)) : undefined;
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(
        process.execPath,
        [
          cli,
          'workflow',
          ...args,
          '--cwd',
          project,
          '--folder',
          ...(json ? ['--json'] : []),
          ...(config ? ['--factory-provider-broker-fd', '3'] : []),
        ],
        {
          cwd: project,
          env: {
            PATH: process.env.PATH ?? '/usr/bin:/bin',
            HOME: home,
            ARCHON_HOME: home,
            DATABASE_URL: '',
            TELEMETRY_DISABLED: '1',
            ANTHROPIC_BASE_URL: 'http://127.0.0.1:9',
            OPENAI_BASE_URL: 'http://127.0.0.1:9',
          },
          stdio: ['ignore', 'pipe', 'pipe', descriptor ?? 'ignore'],
        }
      );
    } finally {
      if (descriptor !== undefined) closeSync(descriptor);
    }
    if (!child.stdout || !child.stderr) throw new Error('missing engine output pipes');
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', chunk => {
      stdout += String(chunk);
    });
    child.stderr.on('data', chunk => {
      stderr += String(chunk);
    });
    const code = await new Promise<number | null>((accept, reject) => {
      child.once('error', reject);
      child.once('exit', accept);
    });
    expect(stdout + stderr).not.toContain('"module":"provider.codex"');
    expect(stdout + stderr).not.toContain('"module":"provider.claude"');
    return { code: code ?? 1, stdout, stderr };
  }
  async function get(runId: string): Promise<Run> {
    const response = await invoke(['get', runId]);
    expect(response.code, response.stderr).toBe(0);
    const parsed = JSON.parse(response.stdout) as Run & { run?: Run };
    return parsed.run ?? parsed;
  }
  if (process.platform === 'darwin') {
    const protectedTemp = realpathSync(await mkdtemp(join('/tmp', 'factory-protected-binding-')));
    temp(protectedTemp);
    const rejectedScope = structuredClone(base);
    rejectedScope.providerPolicy.deniedRoots = [protectedTemp];
    const rejected = await invoke(
      ['run', 'binding-child', 'Unsupported protected temp root', '--launch-key', 'tmp-rejected'],
      rejectedScope
    );
    expect(rejected.code).not.toBe(0);
    expect(rejected.stdout + rejected.stderr).toContain(
      'factory_provider_protected_tmp_root_unqualified'
    );
    expect(existsSync(join(project, 'after-successor.txt'))).toBe(false);
  }
  const launched = await invoke(
    ['run', 'binding-parent', 'Parent', '--launch-key', 'binding-parent-one'],
    base
  );
  expect(launched.code, launched.stdout + launched.stderr).toBe(0);
  const status = JSON.parse((await invoke(['launch-status', 'binding-parent-one'])).stdout) as {
    receipt: { runId: string };
  };
  const parentId = status.receipt.runId;
  const parent = await get(parentId);
  const marker = parent.metadata.factory_provider_admission as Record<string, unknown>;
  expect(marker.runtimeBundleId).toBe(base.managedRun.runtimeBundleId);
  expect(marker.runtimeBindingDigest).toBe(base.managedRun.runtimeBindingDigest);
  expect(marker.worktreePath).toBe(project);
  const approval = parent.metadata.approval as { occurrenceId: string; evidenceDigest: string };
  const accepted = await invoke([
    'respond',
    parentId,
    'approve',
    '--command-id',
    'binding-approved',
    '--expected-occurrence',
    approval.occurrenceId,
    '--expected-evidence-digest',
    approval.evidenceDigest,
  ]);
  expect(accepted.code, accepted.stdout + accepted.stderr).toBe(0);
  for (const field of [
    'machineId',
    'hostEpoch',
    'projectId',
    'readySnapshotId',
    'readyDigest',
    'runtimeBundleId',
    'runtimeBindingDigest',
    'attemptId',
  ] as const) {
    const changed = structuredClone(base);
    changed.managedRun[field] = 'changed:' + field;
    const result = await invoke(['resume', parentId], changed, false);
    expect(result.code).not.toBe(0);
    expect(result.stdout + result.stderr).toContain('factory_provider_managed_resume_required');
    expect(existsSync(join(project, 'after-resume.txt'))).toBe(false);
  }
  const policyChanged = structuredClone(base);
  policyChanged.providerPolicy.providers[0].models = ['changed-model'];
  expect((await invoke(['resume', parentId], policyChanged, false)).code).not.toBe(0);
  expect(existsSync(join(project, 'after-resume.txt'))).toBe(false);
  const pathChanged = structuredClone(base);
  pathChanged.managedRun.worktreePath = manual;
  expect((await invoke(['resume', parentId], pathChanged, false)).code).not.toBe(0);
  expect(existsSync(join(project, 'after-resume.txt'))).toBe(false);
  const resumed = await invoke(['resume', parentId], base, false);
  expect(resumed.code).not.toBe(0);
  expect(existsSync(join(project, 'after-resume.txt'))).toBe(true);
  expect((await get(parentId)).status).toBe('failed');

  const successor = structuredClone(base);
  successor.managedRun.attemptId = 'attempt:successor';
  successor.successor = {
    parentRunId: parentId,
    parentAttemptId: base.managedRun.attemptId,
    parentBindingDigest: factoryRequestDigest(marker),
    commandId: 'command:bounded-recovery',
  };
  const changedRuntime = structuredClone(successor);
  changedRuntime.managedRun.runtimeBundleId = 'runtime:new-global';
  const rejected = await invoke(
    ['run', 'binding-child', 'Rejected', '--adopt', parentId, '--launch-key', 'child-rejected'],
    changedRuntime
  );
  expect(rejected.code).not.toBe(0);
  expect(rejected.stdout + rejected.stderr).toContain(
    'factory_provider_successor_binding_mismatch'
  );
  expect(existsSync(join(project, 'after-successor.txt'))).toBe(false);
  const unconfigured = structuredClone(successor);
  delete unconfigured.successor;
  expect(
    (await invoke(['run', 'binding-child', 'No authority', '--adopt', parentId], unconfigured)).code
  ).not.toBe(0);
  expect(existsSync(join(project, 'after-successor.txt'))).toBe(false);
  await writeFile(
    join(project, '.archon/config.yaml'),
    'assistant: codex\nassistants:\n  codex:\n    model: future-global-model\n'
  );
  const continued = await invoke(
    [
      'run',
      'binding-child',
      'Approved bounded recovery',
      '--adopt',
      parentId,
      '--launch-key',
      'child-authorized',
    ],
    successor
  );
  expect(continued.code, continued.stdout + continued.stderr).toBe(0);
  const childStatus = JSON.parse((await invoke(['launch-status', 'child-authorized'])).stdout) as {
    receipt: { runId: string };
  };
  const child = await get(childStatus.receipt.runId);
  expect(child.adopted_from_run_id).toBe(parentId);
  expect(child.metadata.factory_provider_admission).toMatchObject({
    runtimeBundleId: base.managedRun.runtimeBundleId,
    runtimeBindingDigest: base.managedRun.runtimeBindingDigest,
    readyDigest: base.managedRun.readyDigest,
    attemptId: 'attempt:successor',
  });
  expect(child.metadata.factory_provider_successor).toEqual(successor.successor);
  expect(existsSync(join(project, 'after-successor.txt'))).toBe(true);
  expect(existsSync(join(project, 'internal-child.txt'))).toBe(true);
}, 30000);
