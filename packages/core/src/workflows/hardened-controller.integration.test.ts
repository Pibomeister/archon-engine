import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { executeWorkflow } from '@archon/workflows/executor';
import type { WorkflowDeps, IWorkflowPlatform, WorkflowConfig } from '@archon/workflows/deps';
import type { IAgentProvider } from '@archon/providers/types';
import type { WorkflowDefinition, WorkflowRun, WorkflowRunStatus } from '@archon/workflows/schemas';
import type {
  IWorkflowStore,
  WorkflowEventRecord,
  WorkflowEventType,
} from '@archon/workflows/store';
import type { ContainerRunContext } from '@archon/workflows/container-context';
import { registerProvider } from '@archon/providers';
import { computeControllerWorkflowDigest } from '@archon/workflows/controller-actions';
import {
  buildWorkflowPinState,
  WORKFLOW_PIN_METADATA_KEY,
} from '@archon/workflows/workflow-pinning';
import {
  createHardenedControllerActions,
  prepareHardenedControllerSession,
} from './hardened-controller';

const priorArchonHome = process.env.ARCHON_HOME;
const priorPath = process.env.PATH;
const IMAGE_ID = `sha256:${'c'.repeat(64)}`;
const PROVIDER_ID = `hardened-controller-e2e-${Date.now().toString(36)}`;

registerProvider({
  id: PROVIDER_ID,
  displayName: 'Hardened Controller E2E',
  builtIn: false,
  credentials: { kind: 'static', specs: [] },
  capabilities: providerCapabilities(),
  factory: () => makeProvider(),
});

describe('hardened controller workflow integration', () => {
  let root: string;
  let source: string;
  let archonHome: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'archon-hardened-controller-e2e-'));
    source = join(root, 'source');
    archonHome = join(root, 'private-home');
    mkdirSync(source, { recursive: true });
    mkdirSync(archonHome, { recursive: true, mode: 0o700 });
    process.env.ARCHON_HOME = archonHome;
    writeFileSync(join(source, 'README.md'), 'safe input\n');
    writeFileSync(join(source, 'controller.key.backup'), 'host-key-canary\n');
    writeFileSync(join(source, '.env'), 'GH_TOKEN=host-gh-canary\n');
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
    if (priorArchonHome === undefined) delete process.env.ARCHON_HOME;
    else process.env.ARCHON_HOME = priorArchonHome;
    if (priorPath === undefined) delete process.env.PATH;
    else process.env.PATH = priorPath;
    rmSync(root, { recursive: true, force: true });
  });

  test('runs bash -> controller freeze -> human approval pause -> resume -> verify approval through the real executor', async () => {
    const workflow = makeWorkflow();
    writeApprovalPolicy(archonHome, workflow);
    const run = makeRun('run-e2e', workflow);
    const store = new MemoryWorkflowStore(run);
    const session = prepareHardenedControllerSession({
      runId: run.id,
      workflow,
      sourceRoot: source,
      conversationId: 'conv-db',
      userMessage: 'ship',
      image: IMAGE_ID,
      requestedImage: 'archon-runner:test',
      budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 100_000 } },
    });
    run.metadata = {
      ...run.metadata,
      hardened_controller_policy: session.policyMetadata,
      hardened_controller_policy_path: session.policyPath,
      hardened_budget: persistedBudget(session),
      [WORKFLOW_PIN_METADATA_KEY]: buildWorkflowPinState(workflow, 'project'),
    };

    const containerRoot = join(root, 'container-root');
    const containerArtifacts = join(containerRoot, 'artifacts');
    const controllerSnapshots = join(root, 'controller-snapshots');
    mkdirSync(containerArtifacts, { recursive: true });
    mkdirSync(controllerSnapshots, { recursive: true });
    installFakeDocker(join(root, 'bin'), containerRoot);

    const backend = makeBackend(containerArtifacts, controllerSnapshots);
    const platform = makePlatform();
    const deps = makeDeps(store, session, backend.snapshotArtifacts);
    const container = makeContainerContext(backend);
    const execContext = {
      kind: 'container' as const,
      profile: 'hardened' as const,
      containerId: 'archon-e2e-container',
      agentArtifactsDir: '/archon-artifacts',
    };

    const first = await executeWorkflow(
      deps,
      platform,
      'conv-platform',
      source,
      workflow,
      'ship',
      'conv-db',
      { preCreatedRun: run, execContext, container, source: 'project' }
    );
    expect(first.success).toBe(true);
    expect(run.status).toBe('paused');
    expect(readFileSync(join(containerArtifacts, 'run', 'oracle', 'plan.md'), 'utf8')).toBe(
      'approved acceptance criteria\n'
    );
    expect(() => readFileSync(join(session.seed.path, 'controller.key.backup'), 'utf8')).toThrow();
    expect(() => readFileSync(join(session.seed.path, '.env'), 'utf8')).toThrow();

    const freezeCompleted = store.event('node_completed', 'freeze');
    const freezeOutput = outputObject(freezeCompleted);
    expect(freezeOutput.binding_id).toEqual(expect.stringMatching(/^[0-9a-f]{64}$/));
    expect(freezeOutput.oracle_digest).toEqual(expect.any(String));
    const approvalMessage = String(
      eventData(store.event('approval_requested', 'human-approval')).message
    );
    expect(approvalMessage).toContain(String(freezeOutput.binding_id));
    expect(approvalMessage).toContain(String(freezeOutput.oracle_digest));

    store.approveGate('human-approval');
    const priorCompleted = store.completedNodeOutputs();
    expect(priorCompleted.get('freeze')).toContain(String(freezeOutput.binding_id));
    const receiptBefore = readFileSync(String(freezeOutput.receipt), 'utf8');

    const second = await executeWorkflow(
      deps,
      platform,
      'conv-platform',
      source,
      workflow,
      'ship',
      'conv-db',
      {
        preCreatedRun: run,
        priorCompletedNodes: priorCompleted,
        execContext,
        container,
        source: 'project',
      }
    );

    expect(second.success).toBe(true);
    expect(run.status).toBe('completed');
    expect(readFileSync(String(freezeOutput.receipt), 'utf8')).toBe(receiptBefore);
    const approvalOutput = outputObject(store.event('node_completed', 'approval-check'));
    expect(approvalOutput).toMatchObject({
      approved: true,
      binding_id: freezeOutput.binding_id,
      oracle_digest: freezeOutput.oracle_digest,
    });
    expect(readFileSync(String(approvalOutput.receipt), 'utf8')).toContain(
      'protected controller approval event'
    );
    expect(backend.snapshotDestinations.length).toBeGreaterThan(0);
    for (const snapshot of backend.snapshotDestinations) {
      expect(() => readFileSync(join(snapshot, 'controller.key.backup'), 'utf8')).toThrow();
      expect(() => readFileSync(join(snapshot, '.env'), 'utf8')).toThrow();
    }
  });
});

function makeWorkflow(): WorkflowDefinition {
  return {
    name: 'hardened-controller-e2e',
    mutates_checkout: false,
    hardened: { required: true },
    container: { enabled: true },
    nodes: [
      {
        id: 'write-oracle',
        bash: 'mkdir -p "$ARTIFACTS_DIR/oracle" && printf "approved acceptance criteria\\n" > "$ARTIFACTS_DIR/oracle/plan.md"',
      },
      {
        id: 'freeze',
        depends_on: ['write-oracle'],
        controller_action: 'finalize-evidence',
        phase: 'planning-freeze',
      },
      {
        id: 'human-approval',
        depends_on: ['freeze'],
        approval: {
          message: 'Approve $freeze.output.binding_id $freeze.output.oracle_digest',
        },
      },
      {
        id: 'approval-check',
        depends_on: ['human-approval'],
        controller_action: 'verify-approval',
        phase: 'planning-approval',
      },
    ],
  } as WorkflowDefinition;
}

function writeApprovalPolicy(home: string, workflow: WorkflowDefinition): void {
  const policyPath = join(home, 'controller-policy', 'planning-approval.json');
  mkdirSync(dirname(policyPath), { recursive: true, mode: 0o700 });
  const policy = {
    schema: 'archon.hardened-controller-planning-policy.v1',
    version: 1,
    workflowDigest: computeControllerWorkflowDigest(workflow),
    grants: [
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
    ],
  };
  writeFileSync(policyPath, JSON.stringify(policy, null, 2), { mode: 0o600 });
  chmodSync(policyPath, 0o600);
}

function makeRun(id: string, workflow: WorkflowDefinition): WorkflowRun {
  return {
    id,
    workflow_name: workflow.name,
    conversation_id: 'conv-db',
    codebase_id: null,
    status: 'running',
    user_message: 'ship',
    started_at: new Date().toISOString(),
    completed_at: null,
    metadata: { isolation: 'container', isolation_env_id: 'env-e2e' },
    last_activity_at: null,
    working_path: null,
    user_id: null,
    parent_run_id: null,
    output_root: null,
  };
}

class MemoryWorkflowStore implements IWorkflowStore {
  events: WorkflowEventRecord[] = [];
  private nextOrder = 1;

  constructor(private run: WorkflowRun) {}

  async createWorkflowRun(): Promise<WorkflowRun> {
    throw new Error('integration fixture uses a pre-created run');
  }

  async getWorkflowRun(): Promise<WorkflowRun> {
    return this.run;
  }

  async getActiveWorkflowRunByPath(): Promise<null> {
    return null;
  }

  async findResumableRun(): Promise<null> {
    return null;
  }

  async failOrphanedRuns(): Promise<{ count: number }> {
    return { count: 0 };
  }

  async resumeWorkflowRun(): Promise<WorkflowRun> {
    this.run.status = 'running';
    return this.run;
  }

  async updateWorkflowRun(
    _id: string,
    updates: Partial<Pick<WorkflowRun, 'status' | 'metadata' | 'output_root'>>
  ): Promise<void> {
    if (updates.status) this.run.status = updates.status;
    if (updates.metadata) this.run.metadata = { ...this.run.metadata, ...updates.metadata };
    if (updates.output_root && !this.run.output_root) this.run.output_root = updates.output_root;
  }

  async updateWorkflowActivity(): Promise<void> {}

  async getWorkflowRunStatus(): Promise<WorkflowRunStatus> {
    return this.run.status;
  }

  async completeWorkflowRun(_id: string, metadata?: Record<string, unknown>): Promise<void> {
    this.run.status = 'completed';
    this.run.metadata = { ...this.run.metadata, ...metadata };
  }

  async failWorkflowRun(_id: string, error: string): Promise<void> {
    this.run.status = 'failed';
    this.run.metadata = { ...this.run.metadata, error };
  }

  async pauseWorkflowRun(
    _id: string,
    approvalContext: Record<string, unknown>,
    extraMetadata?: Record<string, unknown>
  ): Promise<void> {
    this.run.status = 'paused';
    this.run.metadata = {
      ...this.run.metadata,
      ...extraMetadata,
      approval: approvalContext,
    };
  }

  async claimWriteback(): Promise<{ claimed: boolean }> {
    return { claimed: true };
  }

  async releaseWritebackClaim(): Promise<void> {}

  async cancelWorkflowRun(): Promise<{ cancelled: boolean }> {
    this.run.status = 'cancelled';
    return { cancelled: true };
  }

  async createWorkflowEvent(data: WorkflowEventInput): Promise<void> {
    this.recordEvent(data);
  }

  async createWorkflowEventStrict(data: WorkflowEventInput): Promise<void> {
    this.recordEvent(data);
  }

  async createControllerCompletionEvent(
    data: WorkflowEventInput,
    deadlineAt: number
  ): Promise<void> {
    if (this.run.status !== 'running' || Date.now() > deadlineAt)
      throw new Error('Controller completion not live');
    this.recordEvent(data);
  }

  async getDagResumeSnapshot(): Promise<{
    completedNodeOutputs: Map<string, string>;
    tokens: { input: number; output: number };
  }> {
    return { completedNodeOutputs: this.completedNodeOutputs(), tokens: { input: 0, output: 0 } };
  }

  async listWorkflowEvents(): Promise<WorkflowEventRecord[]> {
    return this.events;
  }

  async getCodebaseEnvVars(): Promise<Record<string, string>> {
    return {};
  }

  async getCodebase(): Promise<null> {
    return null;
  }

  async getWorkflowNodeSession(): Promise<null> {
    return null;
  }

  async upsertWorkflowNodeSession(): Promise<void> {}

  async deleteWorkflowNodeSessions(): Promise<{ deleted: number }> {
    return { deleted: 0 };
  }

  async findChildRuns(): Promise<WorkflowRun[]> {
    return [];
  }

  async getRunAncestry(): Promise<WorkflowRun[]> {
    return [];
  }

  private recordEvent(data: WorkflowEventInput): void {
    this.events.push({ ...data, event_order: this.nextOrder++ });
  }

  event(type: string, stepName: string): WorkflowEventRecord {
    const event = this.events.find(item => item.event_type === type && item.step_name === stepName);
    if (!event) throw new Error(`missing event ${type}:${stepName}`);
    return event;
  }

  approveGate(nodeId: string): void {
    const approval = this.run.metadata.approval as Record<string, unknown> | undefined;
    this.events.push({
      event_type: 'node_completed',
      step_name: nodeId,
      event_order: this.nextOrder++,
      data: { node_output: '', approval_decision: 'approved' },
    });
    this.events.push({
      event_type: 'approval_received',
      step_name: nodeId,
      event_order: this.nextOrder++,
      data: { decision: 'approved' },
    });
    this.run.status = 'running';
    this.run.metadata = {
      ...this.run.metadata,
      approval: { ...approval, resolved: 'approved' },
    };
  }

  completedNodeOutputs(): Map<string, string> {
    const outputs = new Map<string, string>();
    for (const event of this.events) {
      if (event.event_type !== 'node_completed' || !event.step_name) continue;
      const data = eventData(event);
      outputs.set(event.step_name, String(data.node_output ?? ''));
    }
    return outputs;
  }
}

type WorkflowEventInput = {
  workflow_run_id: string;
  event_type: WorkflowEventType;
  step_index?: number;
  step_name?: string;
  data?: Record<string, unknown>;
};

function makeDeps(
  store: IWorkflowStore,
  session: ReturnType<typeof prepareHardenedControllerSession>,
  snapshotArtifacts: (envId: string, destinationDir: string) => Promise<unknown>
): WorkflowDeps {
  return {
    store,
    getAgentProvider: () => makeProvider(),
    loadConfig: async (): Promise<WorkflowConfig> => ({
      assistant: PROVIDER_ID,
      baseBranch: 'main',
      assistants: { claude: {}, codex: {}, [PROVIDER_ID]: {} },
      commands: {},
      defaults: { loadDefaultCommands: false, loadDefaultWorkflows: false },
    }),
    controllerActions: createHardenedControllerActions({ session, store, snapshotArtifacts }),
    controllerActionGrants: session.controllerActionGrants,
    workflowBudgetGrants: session.workflowBudgetGrants,
  };
}

function persistedBudget(session: ReturnType<typeof prepareHardenedControllerSession>) {
  const grant = session.workflowBudgetGrants[0];
  return {
    workflowDigest: grant.workflowDigest,
    deadlineAt: grant.deadlineAt,
    tokens: { ...grant.tokens },
    consumed: { input: 0, output: 0 },
    updatedAt: new Date().toISOString(),
  };
}

function makeProvider(): IAgentProvider {
  return {
    getType: () => PROVIDER_ID,
    getCapabilities: () => providerCapabilities(),
    sendQuery: async function* () {},
  } as unknown as IAgentProvider;
}

function providerCapabilities() {
  return {
    sessionResume: false,
    mcp: false,
    hooks: false,
    skills: false,
    agents: false,
    toolRestrictions: false,
    structuredOutput: false,
    envInjection: false,
    costControl: false,
    effortControl: false,
    thinkingControl: false,
    fallbackModel: false,
    sandbox: false,
    settingSources: false,
    nativeTools: false,
    containerExec: true,
  } as const;
}

function makePlatform(): IWorkflowPlatform {
  return {
    sendMessage: mock(() => Promise.resolve()),
    getStreamingMode: () => 'batch',
    getPlatformType: () => 'test',
  };
}

function makeContainerContext(backend: ReturnType<typeof makeBackend>): ContainerRunContext {
  return { envId: 'env-e2e', writeBack: 'auto', backend };
}

function makeBackend(containerArtifacts: string, controllerSnapshots: string) {
  const snapshotDestinations: string[] = [];
  return {
    snapshotDestinations,
    async snapshotArtifacts(_envId: string, destinationDir: string) {
      snapshotDestinations.push(destinationDir);
      copyOracleOnly(join(containerArtifacts, 'run'), destinationDir);
      return { snapshotDir: destinationDir, image: IMAGE_ID, totalBytes: 0, files: [] };
    },
    async suspend() {},
    async finalize() {
      return { requiresApproval: false, changeSummary: { totalCount: 0 } };
    },
    async applyChanges() {
      return { filesApplied: 0, filesDeleted: 0, warnings: [] };
    },
    async discardChanges() {},
    controllerSnapshots,
  };
}

function installFakeDocker(binDir: string, containerRoot: string): void {
  mkdirSync(binDir, { recursive: true });
  const docker = join(binDir, 'docker');
  const script = fakeDockerScript(containerRoot);
  writeFileSync(docker, script, { mode: 0o700 });
  chmodSync(docker, 0o700);
  process.env.PATH = `${binDir}:${priorPath ?? ''}`;
}

function fakeDockerScript(containerRoot: string): string {
  const bunPath = process.execPath;
  return `#!${bunPath}
import { mkdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

const root = ${JSON.stringify(containerRoot)};
const args = process.argv.slice(2);
if (args[0] === 'stop') process.exit(0);
if (args[0] !== 'exec') process.exit(64);
let cursor = 1;
while (cursor < args.length && args[cursor]?.startsWith('-')) {
  const flag = args[cursor];
  cursor += flag === '-w' || flag === '-u' || flag === '-e' ? 2 : 1;
}
const container = args[cursor++];
const command = args[cursor++];
if (container !== 'archon-e2e-container' || command !== 'bash' || args[cursor++] !== '-c') {
  process.exit(64);
}
const artifacts = root + '/artifacts';
const state = root + '/state';
const logs = root + '/logs';
mkdirSync(artifacts, { recursive: true });
mkdirSync(state, { recursive: true });
mkdirSync(logs, { recursive: true });
let commandText = args.slice(cursor).join(' ');
commandText = commandText.split('/archon-artifacts').join(artifacts);
commandText = commandText.split('/archon-state').join(state);
commandText = commandText.split('/archon-logs').join(logs);
const result = spawnSync('bash', ['-c', commandText], {
  stdio: 'inherit',
  env: { ...process.env, ARTIFACTS_DIR: artifacts, STATE_DIR: state, LOG_DIR: logs },
});
process.exit(result.status ?? 1);
`;
}

function copyOracleOnly(containerArtifacts: string, destinationDir: string): void {
  mkdirSync(join(destinationDir, 'oracle'), { recursive: true });
  const contents = readFileSync(join(containerArtifacts, 'oracle', 'plan.md'));
  writeFileSync(join(destinationDir, 'oracle', 'plan.md'), contents, { mode: 0o600 });
}

function outputObject(event: WorkflowEventRecord): Record<string, unknown> {
  const data = eventData(event);
  const raw = data.node_output;
  if (typeof raw !== 'string') throw new Error('node_output missing');
  return JSON.parse(raw) as Record<string, unknown>;
}

function eventData(event: WorkflowEventRecord): Record<string, unknown> {
  if (!event.data || typeof event.data !== 'object' || Array.isArray(event.data)) return {};
  return event.data as Record<string, unknown>;
}

function git(cwd: string, args: string[]): void {
  execFileSync('git', ['-C', cwd, ...args], { stdio: 'ignore' });
}
