import { expect, test } from 'bun:test';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { trackTempRoots } from '@archon/paths/test-utils';

const temp = trackTempRoots();
const cli = resolve(import.meta.dir, '../cli.ts');

test('native offline mode persists across gates and rejects a provider before SDK startup', async () => {
  const root = await mkdtemp(join(tmpdir(), 'archon-factory-offline-'));
  temp(root);
  const project = join(root, 'project');
  const home = join(root, 'home');
  await mkdir(join(project, '.archon/workflows'), { recursive: true });
  await mkdir(home);
  await writeFile(
    join(project, '.archon/workflows/offline.yaml'),
    `name: factory-offline
description: Native offline admission boundary.
interactive: true
nodes:
  - id: prepare
    bash: echo original
  - id: review
    depends_on: [prepare]
    approval:
      message: Review original
      decisions: [{id: approve}, {id: reject}]
  - id: implement
    depends_on: [review]
    provider: codex
    model: gpt-5.4
    prompt: This provider must never be constructed in offline mode.
`
  );
  async function invoke(
    args: string[],
    offline = true,
    json = true
  ): Promise<{ code: number; stdout: string; stderr: string }> {
    const child = Bun.spawn(
      [
        process.execPath,
        cli,
        'workflow',
        ...args,
        '--cwd',
        project,
        '--folder',
        ...(offline ? ['--factory-provider-offline'] : []),
        ...(json ? ['--json'] : []),
      ],
      {
        cwd: project,
        env: {
          PATH: process.env.PATH ?? '/usr/bin:/bin',
          HOME: home,
          ARCHON_HOME: home,
          DATABASE_URL: '',
          TELEMETRY_DISABLED: '1',
          CODEX_HOME: join(home, 'nonexistent-codex'),
        },
        stdout: 'pipe',
        stderr: 'pipe',
      }
    );
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(stdout + stderr).not.toContain('"module":"provider.codex"');
    expect(stdout + stderr).not.toContain('"module":"provider.claude"');
    return { code, stdout, stderr };
  }
  const launched = await invoke([
    'run',
    'factory-offline',
    'Offline gate qualification',
    '--launch-key',
    'offline-one',
  ]);
  expect(launched.code, launched.stdout + launched.stderr).toBe(0);
  const status = JSON.parse((await invoke(['launch-status', 'offline-one'])).stdout);
  expect(status.status).toBe('paused');
  const runId = status.receipt.runId as string;
  const detail = JSON.parse((await invoke(['get', runId])).stdout);
  const run = detail.run ?? detail;
  expect(run.metadata.factory_provider_admission).toEqual({
    version: 1,
    mode: 'offline-deterministic',
  });
  const approval = run.metadata.approval;
  const accepted = await invoke(
    [
      'respond',
      runId,
      'approve',
      '--command-id',
      'offline-approval',
      '--expected-occurrence',
      approval.occurrenceId,
      '--expected-evidence-digest',
      approval.evidenceDigest,
    ],
    false
  );
  expect(accepted.code, accepted.stdout + accepted.stderr).toBe(0);
  const unflagged = await invoke(['resume', runId], false, false);
  expect(unflagged.code).not.toBe(0);
  expect(unflagged.stdout + unflagged.stderr).toContain('factory_provider_managed_resume_required');
  const continued = await invoke(['resume', runId], true, false);
  expect(continued.code).not.toBe(0);
  expect(continued.stdout + continued.stderr).toContain('factory_provider_broker_required');
});

test('native offline deterministic workflow completes with local title generation', async () => {
  const root = await mkdtemp(join(tmpdir(), 'archon-factory-no-model-'));
  temp(root);
  const project = join(root, 'project');
  const home = join(root, 'home');
  await mkdir(join(project, '.archon/workflows'), { recursive: true });
  await mkdir(home);
  await writeFile(
    join(project, '.archon/workflows/plain.yaml'),
    'name: plain\ndescription: Native deterministic completion.\nnodes:\n  - id: script\n    bash: echo deterministic-completion\n'
  );
  const child = Bun.spawn(
    [
      process.execPath,
      cli,
      'workflow',
      'run',
      'plain',
      'No model title required',
      '--cwd',
      project,
      '--folder',
      '--factory-provider-offline',
    ],
    {
      cwd: project,
      env: {
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        HOME: home,
        ARCHON_HOME: home,
        DATABASE_URL: '',
        TELEMETRY_DISABLED: '1',
      },
      stdout: 'pipe',
      stderr: 'pipe',
    }
  );
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  expect(code, stdout + stderr).toBe(0);
  expect(stdout).toContain('deterministic-completion');
  expect(stdout + stderr).not.toContain('query_error');
  expect(stdout + stderr).not.toContain('title_generation_failed');
});
