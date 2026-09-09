import { expect, test } from 'bun:test';
import { mkdir, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { trackTempRoots } from '@archon/paths/test-utils';

const temp = trackTempRoots();
const cli = resolve(import.meta.dir, '../cli.ts');
const repoRoot = resolve(import.meta.dir, '../../..');

async function runProcess(
  args: string[],
  env: Record<string, string>,
  cwd: string,
  stdin?: string
): Promise<{ code: number; stdout: string; stderr: string }> {
  const child = Bun.spawn(args, {
    cwd,
    env: { ...process.env, ...env },
    stdin: stdin === undefined ? 'ignore' : 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (stdin !== undefined) {
    if (!child.stdin) throw new Error('missing process stdin pipe');
    child.stdin.write(stdin);
    child.stdin.end();
  }
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code, stdout, stderr };
}

async function seedPausedRun(
  env: Record<string, string>,
  cwd: string
): Promise<{
  runId: string;
  binding: {
    expectedNodeId: string;
    expectedInvocationId: string;
    expectedRequestDigest: string;
    expectedLeaseId: string;
  };
}> {
  const script = `
    const { randomUUID } = await import('node:crypto');
    const { getDatabase } = await import('@archon/core/db/connection');
    const workflows = await import('@archon/core/db/workflows');
    const db = getDatabase();
    const conversationId = randomUUID();
    await db.query(
      "INSERT INTO remote_agent_conversations (id, platform_type, platform_conversation_id) VALUES ($1, 'cli', $2)",
      [conversationId, conversationId]
    );
    const run = await workflows.createWorkflowRun({
      conversation_id: conversationId,
      workflow_name: 'factory-human-input',
      user_message: 'test',
      working_path: ${JSON.stringify(cwd)},
    });
    await workflows.updateWorkflowRun(run.id, { status: 'running' });
    const context = {
      version: 'archon.factory-human-input.v1',
      runId: run.id,
      nodeId: 'implement',
      invocationId: 'invocation-1',
      requestDigest: 'request-digest-1',
      leaseId: 'lease-1',
      launchId: 'launch-1',
      attemptId: 'attempt-1',
      message: 'Which migration path should I use?',
      reason: 'ambiguous migration',
      sessionId: 'session-before-pause',
      requestedAt: '2026-09-07T12:00:00.000Z',
    };
    await workflows.pauseWorkflowRunForFactoryHumanInput(run.id, context);
    console.log(JSON.stringify({
      runId: run.id,
      binding: {
        expectedNodeId: context.nodeId,
        expectedInvocationId: context.invocationId,
        expectedRequestDigest: context.requestDigest,
        expectedLeaseId: context.leaseId,
      },
    }));
    await db.close?.();
  `;
  const result = await runProcess([process.execPath, '-e', script], env, repoRoot);
  expect(result.code, result.stderr + result.stdout).toBe(0);
  const jsonLine = result.stdout
    .trim()
    .split('\n')
    .reverse()
    .find((line: string) => line.trim().startsWith('{'));
  if (!jsonLine) throw new Error('seed helper did not print JSON: ' + result.stdout);
  const parsed = JSON.parse(jsonLine) as Awaited<ReturnType<typeof seedPausedRun>>;
  return parsed;
}

test('workflow factory-human-input records exact stdin response, replays receipt, and rejects stale identity', async () => {
  const root = await mkdtemp(join(tmpdir(), 'archon-factory-human-input-'));
  temp(root);
  const project = join(root, 'project');
  const home = join(root, 'home');
  await mkdir(join(project, '.archon/workflows'), { recursive: true });
  await mkdir(home);
  const git = Bun.spawn(['git', 'init'], { cwd: project, stdout: 'ignore', stderr: 'ignore' });
  expect(await git.exited).toBe(0);
  const env = {
    PATH: process.env.PATH ?? '/usr/bin:/bin',
    HOME: home,
    ARCHON_HOME: home,
    DATABASE_URL: '',
    TELEMETRY_DISABLED: '1',
  };
  const seeded = await seedPausedRun(env, project);

  async function invoke(
    extra: string[],
    stdin?: string
  ): Promise<{ code: number; stdout: string; stderr: string }> {
    return runProcess(
      [
        process.execPath,
        cli,
        'workflow',
        'factory-human-input',
        seeded.runId,
        '--cwd',
        project,
        '--folder',
        '--json',
        ...extra,
      ],
      env,
      project,
      stdin
    );
  }

  const args = [
    '--command-id',
    'factory-response-1',
    '--expected-node',
    seeded.binding.expectedNodeId,
    '--expected-invocation',
    seeded.binding.expectedInvocationId,
    '--expected-request-digest',
    seeded.binding.expectedRequestDigest,
    '--expected-lease',
    seeded.binding.expectedLeaseId,
  ];
  const accepted = await invoke(args, 'Use the additive migration path.');
  expect(accepted.code, accepted.stderr + accepted.stdout).toBe(0);
  const receipt = JSON.parse(accepted.stdout);
  expect(receipt).toMatchObject({
    ok: true,
    action: 'factory-human-input',
    commandId: 'factory-response-1',
    runId: seeded.runId,
    occurrenceId: seeded.binding.expectedInvocationId,
    evidenceDigest: seeded.binding.expectedRequestDigest,
    resumable: true,
  });

  const replay = await invoke(args, 'Use the additive migration path.');
  expect(replay.code, replay.stderr + replay.stdout).toBe(0);
  expect(JSON.parse(replay.stdout)).toEqual(receipt);

  const stale = await invoke(
    [
      '--command-id',
      'factory-response-stale',
      '--expected-node',
      seeded.binding.expectedNodeId,
      '--expected-invocation',
      seeded.binding.expectedInvocationId,
      '--expected-request-digest',
      seeded.binding.expectedRequestDigest,
      '--expected-lease',
      'different-lease',
    ],
    'Use the risky migration path.'
  );
  expect(stale.code).toBe(1);
  expect(JSON.parse(stale.stdout)).toMatchObject({
    ok: false,
    action: 'factory-human-input',
    commandId: 'factory-response-stale',
    code: 'stale_factory_human_input',
  });
});
