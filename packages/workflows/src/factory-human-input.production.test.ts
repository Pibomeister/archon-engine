import { afterAll, afterEach, describe, expect, mock, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SqliteAdapter } from '@archon/core/db/adapters/sqlite';
import type { FactoryHumanInputContext, WorkflowRun } from './schemas/workflow-run';
import type { DagNode, WorkflowDefinition } from './schemas';
import type { IWorkflowPlatform, WorkflowConfig, WorkflowDeps } from './deps';
import type { MessageChunk, ProviderRegistration, SendQueryOptions } from '@archon/providers/types';
import {
  createAdmittedProvider,
  type AdmissionBroker,
  type AdmissionRequest,
} from '../../providers/src/factory-admission';
import { registerBuiltinProviders, getProviderCapabilities } from '@archon/providers';

// Mechanical integration tier: real DAG, real SQLite workflow store and real
// admission wrapper; only the external model/broker and trusted startup binding
// are fixtures. No SDK model call, OAuth session, or remote database is used.
const previousTelemetry = process.env.ARCHON_TELEMETRY_DISABLED;
process.env.ARCHON_TELEMETRY_DISABLED = '1';
const db = new SqliteAdapter(':memory:');
const roots: string[] = [];
mock.module('@archon/core/db/connection', () => ({
  pool: db,
  getDatabase: () => db,
  getDialect: () => db.sql,
  getDatabaseType: () => 'sqlite',
}));
const nativeFactoryMode = await import('@archon/providers/factory-mode');
mock.module('@archon/providers/factory-mode', () => ({
  ...nativeFactoryMode,
  isFactoryManaged: () => true,
  getFactoryScope: () => undefined,
  getFactoryBinding: () => ({
    machineId: 'machine-fixture',
    hostEpoch: 'epoch-fixture',
    factoryJobId: 'job-fixture',
    logicalChainId: 'chain-fixture',
    attemptId: 'attempt-fixture',
    readySnapshotId: 'ready-fixture',
    readyDigest: 'ready-digest-fixture',
    runtimeBundleId: 'runtime-fixture',
    runtimeBindingDigest: 'runtime-digest-fixture',
    projectId: 'project-fixture',
    launchId: 'launch-fixture',
  }),
}));
registerBuiltinProviders();
const workflows = await import('@archon/core/db/workflows');
const events = await import('@archon/core/db/workflow-events');
const operations = await import('@archon/core/operations/workflow-operations');
const { createWorkflowStore } = await import('@archon/core/workflows/store-adapter');
const { executeDagWorkflow } = await import('./dag-executor');

afterEach(async () => {
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});
afterAll(async () => {
  await db.close();
  if (previousTelemetry === undefined) delete process.env.ARCHON_TELEMETRY_DISABLED;
  else process.env.ARCHON_TELEMETRY_DISABLED = previousTelemetry;
});

const config: WorkflowConfig = {
  assistant: 'codex',
  assistants: { codex: {}, claude: {} },
  commands: {},
  defaults: { loadDefaultCommands: false, loadDefaultWorkflows: false },
};
const platform: IWorkflowPlatform = {
  sendMessage: () => Promise.resolve(),
  getStreamingMode: () => 'batch',
  getPlatformType: () => 'test',
};

describe('factory human input through real DAG and SQLite continuation state', () => {
  test('does not inject an iteration-2 answer or session into loop-group iteration 3', async () => {
    const fixture = await resumedFixture('review.implement', { iteration: 2, reask: 0 });
    const seen: Array<{ prompt: string; resume?: string }> = [];
    const provider = mechanicalProvider(fixture, async function* (prompt, _cwd, resume, options) {
      seen.push({ prompt, ...(resume ? { resume } : {}) });
      yield { type: 'assistant', content: seen.length === 1 ? 'another iteration needed' : 'DONE' };
      options?.factoryTransportClosed?.();
      yield { type: 'result', sessionId: `mechanical-session-${seen.length}` };
    });
    await runDag(
      fixture,
      {
        name: 'group-answer-consumption',
        description: 'Only iteration 2 owns the operator answer.',
        nodes: [
          {
            id: 'review',
            kind: 'loop_group',
            loop_group: {
              fresh_context: true,
              max_iterations: 3,
              until: 'DONE',
              nodes: [
                {
                  id: 'implement',
                  kind: 'agent',
                  source: { kind: 'inline', prompt: 'Review the work.' },
                  provider: 'codex',
                },
              ],
            },
          },
        ],
      },
      provider
    );
    expect(seen).toHaveLength(2);
    expect(seen[0]?.prompt).toContain(fixture.answer);
    expect(seen[0]?.resume).toBe('paused-native-session');
    expect(seen[1]?.prompt).not.toContain(fixture.answer);
    expect(seen[1]?.resume).not.toBe('paused-native-session');
    expect(provider.requests.map(request => request.context.iteration)).toEqual([2, 3]);
  });

  test('resumes an ordinary agent at reask 1 rather than admitting reask 0 again', async () => {
    const fixture = await resumedFixture('implement', { reask: 1 });
    const provider = mechanicalProvider(fixture, async function* (_prompt, _cwd, _resume, options) {
      options?.factoryTransportClosed?.();
      yield {
        type: 'result',
        sessionId: 'schema-answer-session',
        structuredOutput: { done: true },
      };
    });
    await runDag(
      fixture,
      {
        name: 'ordinary-agent-reask-cursor',
        description: 'Restore the exact answered schema reask.',
        nodes: [
          {
            id: 'implement',
            kind: 'agent',
            provider: 'codex',
            source: { kind: 'inline', prompt: 'Produce a valid result.' },
            output_format: {
              type: 'object',
              properties: { done: { type: 'boolean' } },
              required: ['done'],
            },
          },
        ],
      },
      provider
    );
    expect(provider.requests).toHaveLength(1);
    expect(provider.requests[0]?.context).toMatchObject({ nodeId: 'implement', reask: 1 });
    expect((await workflows.getWorkflowRun(fixture.run.id))?.status).toBe('completed');
  });

  test('does not admit a fresh invocation after continuation settlement but before its node checkpoint', async () => {
    const fixture = await resumedFixture('implement', { reask: 0 });
    const counter = join(fixture.root, 'provider-effect-count');
    await writeFile(counter, '0');
    const provider = mechanicalProvider(fixture, async function* (_prompt, _cwd, _resume, options) {
      const prior = Number(await readFile(counter, 'utf8'));
      await writeFile(counter, String(prior + 1));
      yield { type: 'assistant', content: 'Bounded operation finished.' };
      options?.factoryTransportClosed?.();
      yield { type: 'result', sessionId: 'effect-session' };
    });

    // Establish the precise durable crash cut through production APIs: the answer
    // is consumed, node_started persists, the actual admission wrapper has run and
    // settled a provider effect, but node_completed has not been checkpointed.
    await workflows.consumeFactoryHumanInputResponse(fixture.run.id, fixture.context);
    await events.persistWorkflowEvent({
      workflow_run_id: fixture.run.id,
      event_type: 'node_started',
      step_name: 'implement',
      data: { provider: 'codex', node_type: 'agent' },
    });
    for await (const _chunk of provider.agent.sendQuery(
      `Continue with the operator answer: ${fixture.answer}`,
      fixture.root,
      'paused-native-session',
      {
        model: 'qualified-mechanical-model',
        factoryInvocation: {
          runId: fixture.run.id,
          nodeId: 'implement',
          launchId: 'launch-fixture',
          attemptId: 'attempt-fixture',
          reask: 0,
        },
      }
    )) {
      /* Drain the production admission/settlement wrapper. */
    }
    expect(await readFile(counter, 'utf8')).toBe('1');
    expect(provider.settlements).toHaveLength(1);
    expect(provider.settlements[0]?.outcome).toBe('released');
    const snapshot = await events.getDagResumeSnapshot(fixture.run.id);
    expect(snapshot.completedNodeOutputs.has('implement')).toBe(false);
    expect(snapshot.unresolvedNodeStarts.has('implement')).toBe(true);

    // A crashed coordinator is recovered as failed before the ordinary resume CAS.
    await workflows.failWorkflowRun(
      fixture.run.id,
      'Coordinator disappeared before node checkpoint'
    );
    fixture.run = await workflows.resumeWorkflowRun(fixture.run.id);
    await runDag(
      fixture,
      {
        name: 'uncertain-continuation-effects',
        description: 'Reconcile uncertain prior effects before redispatch.',
        nodes: [
          {
            id: 'implement',
            kind: 'agent',
            provider: 'codex',
            source: { kind: 'inline', prompt: 'Perform the bounded operation.' },
          },
        ],
      },
      provider
    );
    expect(await readFile(counter, 'utf8')).toBe('1');
    expect(new Set(provider.requests.map(request => request.invocationId)).size).toBe(1);
  });
});

type Fixture = {
  root: string;
  run: WorkflowRun;
  answer: string;
  context: FactoryHumanInputContext;
};
async function resumedFixture(
  nodeId: string,
  occurrence: { iteration?: number; reask?: number }
): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), 'archon-factory-native-state-'));
  roots.push(root);
  const conversationId = randomUUID();
  await db.query(
    'INSERT INTO remote_agent_conversations (id, platform_type, platform_conversation_id) VALUES ($1, $2, $3)',
    [conversationId, 'cli', conversationId]
  );
  const created = await workflows.createWorkflowRun({
    conversation_id: conversationId,
    workflow_name: 'factory-production-regression',
    user_message: 'mechanical provider only',
  });
  await workflows.updateWorkflowRun(created.id, { status: 'running' });
  const requested: FactoryHumanInputContext = {
    version: 'archon.factory-human-input.v1',
    runId: created.id,
    nodeId,
    ...occurrence,
    invocationId: `paused-invocation-${randomUUID()}`,
    requestDigest: `request-${randomUUID()}`,
    leaseId: `paused-lease-${randomUUID()}`,
    launchId: 'launch-fixture',
    attemptId: 'attempt-fixture',
    message: 'Which migration path?',
    requestedAt: new Date().toISOString(),
    sessionId: 'paused-native-session',
  };
  await workflows.pauseWorkflowRunForFactoryHumanInput(created.id, requested);
  const answer = 'ONLY_THIS_OCCURRENCE_USE_THE_ADDITIVE_MIGRATION';
  const receipt = await operations.respondToFactoryHumanInputConditionally(created.id, answer, {
    commandId: `response-${randomUUID()}`,
    expectedNodeId: nodeId,
    expectedInvocationId: requested.invocationId,
    expectedRequestDigest: requested.requestDigest,
    expectedLeaseId: requested.leaseId,
  });
  expect(receipt.ok).toBe(true);
  const run = await workflows.resumeWorkflowRun(created.id);
  const context = run.metadata.factory_human_input as FactoryHumanInputContext;
  expect(context.response?.text).toBe(answer);
  return { root, run, answer, context };
}

type ProviderBody = (
  prompt: string,
  cwd: string,
  resume: string | undefined,
  options: SendQueryOptions | undefined
) => AsyncGenerator<MessageChunk>;
function mechanicalProvider(fixture: Fixture, body: ProviderBody) {
  const requests: AdmissionRequest[] = [];
  const settlements: Array<{ invocationId: string; outcome: string }> = [];
  const broker: AdmissionBroker = {
    acquire(request) {
      requests.push(request);
      return Promise.resolve({
        invocationId: request.invocationId,
        requestDigest: request.requestDigest,
        leaseId: `lease-${request.invocationId}`,
        leaseExpiresAt: '2099-01-01T00:00:00.000Z',
        version: 'archon.provider-admission.v1',
      });
    },
    async settle(lease, outcome, signals) {
      settlements.push({ invocationId: lease.invocationId, outcome });
      for (const signal of signals ?? [])
        await events.persistWorkflowEvent({
          workflow_run_id: fixture.run.id,
          event_type: 'factory_observation',
          step_name: signal.context.nodeId,
          data: { signal },
        });
    },
  };
  const capabilities = getProviderCapabilities('codex');
  const entry: ProviderRegistration = {
    id: 'codex',
    displayName: 'Mechanical provider fixture',
    builtIn: true,
    capabilities,
    parseRunConfig: raw => raw,
    credentials: { kind: 'static', specs: [] },
    factory: () => ({
      getType: () => 'codex',
      getCapabilities: () => capabilities,
      sendQuery: (prompt, cwd, resume, options) => body(prompt, cwd, resume, options),
    }),
  };
  return { agent: createAdmittedProvider(entry, broker), requests, settlements };
}

async function runDag(
  fixture: Fixture,
  workflow: WorkflowDefinition,
  provider: ReturnType<typeof mechanicalProvider>
): Promise<void> {
  const deps: WorkflowDeps = {
    store: createWorkflowStore(),
    getAgentProvider: () => provider.agent,
    loadConfig: () => Promise.resolve(config),
  };
  await executeDagWorkflow(
    deps,
    platform,
    fixture.run.conversation_id,
    fixture.root,
    { ...workflow, nodes: workflow.nodes as DagNode[] },
    fixture.run,
    'codex',
    'qualified-mechanical-model',
    join(fixture.root, 'artifacts'),
    join(fixture.root, 'state'),
    join(fixture.root, 'logs'),
    'main',
    'docs/',
    config
  );
}
