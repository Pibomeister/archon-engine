import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import { execFileSync } from 'node:child_process';
import {
  chmodSync,
  copyFileSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import type { IAgentProvider } from '@archon/providers/types';
import type { WorkflowConfig, WorkflowDeps, IWorkflowPlatform } from '@archon/workflows/deps';
import type { WorkflowDefinition } from '@archon/workflows/schemas';
import { registerProvider } from '@archon/providers';
import { ContainerBackend } from '@archon/isolation';
import { computeControllerWorkflowDigest } from '@archon/workflows/controller-actions';
import {
  createHardenedControllerActions,
  getHardenedControllerEgressPolicy,
  getHardenedControllerProxyBudgetSeed,
} from './hardened-controller';
import type { HardenedControllerSession } from './hardened-controller';
import {
  buildWorkflowPinState,
  WORKFLOW_PIN_METADATA_KEY,
} from '@archon/workflows/workflow-pinning';

const PRIOR_ARCHON_HOME = process.env.ARCHON_HOME;
const PRIOR_DATABASE_URL = process.env.DATABASE_URL;
const PRIOR_OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const PRIOR_ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const PRIOR_GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const PRIOR_GH_TOKEN = process.env.GH_TOKEN;
const IMAGE_ID_PATTERN = /^sha256:[0-9a-f]{64}$/;
const PROVIDER_ID = `hardened-controller-docker-${Date.now().toString(36)}`;
const DEFAULT_PLAYWRIGHT_NODE_MODULES = join(process.cwd(), 'node_modules');
const DEFAULT_PLAYWRIGHT_VERIFIER_IMAGE =
  'sha256:48887be1c4f7b4800e4879a96a5f761038b6fb616e6d03961768d8f2cb9d0a8f';
const BROWSER_POLICY = {
  required: [
    {
      id: 'index-renders',
      criterion: 'Static candidate page renders approved text',
      path: '/index.html',
      assertions: [{ type: 'text', value: 'candidate v2' }],
    },
  ],
};

registerProvider({
  id: PROVIDER_ID,
  displayName: 'Hardened Controller Docker Fixture',
  builtIn: false,
  credentials: { kind: 'static', specs: [] },
  capabilities: providerCapabilities(),
  factory: () => makeProvider(),
});

describe('hardened controller Docker integration', () => {
  let root: string;
  let source: string;
  let archonHome: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'archon-hardened-controller-docker-'));
    source = join(root, 'source');
    archonHome = join(root, 'private-home');
    mkdirSync(source, { recursive: true });
    mkdirSync(archonHome, { recursive: true, mode: 0o700 });
    process.env.ARCHON_HOME = archonHome;
    delete process.env.DATABASE_URL;
    delete process.env.OPENAI_API_KEY;
    delete process.env.ANTHROPIC_API_KEY;
    delete process.env.GITHUB_TOKEN;
    delete process.env.GH_TOKEN;
    writeFileSync(join(source, 'controller.key.backup'), 'host-key-canary\n');
    writeFileSync(join(source, '.env'), 'GH_TOKEN=host-gh-canary\n');
    createChildRepoWithParent(source, 'api', 'parent v0\n', 'baseline v1\n');
  });

  afterEach(async () => {
    const { closeDatabase, resetDatabase } = await import('../db/connection');
    await closeDatabase();
    resetDatabase();
    if (PRIOR_ARCHON_HOME === undefined) delete process.env.ARCHON_HOME;
    else process.env.ARCHON_HOME = PRIOR_ARCHON_HOME;
    if (PRIOR_DATABASE_URL === undefined) delete process.env.DATABASE_URL;
    else process.env.DATABASE_URL = PRIOR_DATABASE_URL;
    if (PRIOR_OPENAI_API_KEY === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = PRIOR_OPENAI_API_KEY;
    if (PRIOR_ANTHROPIC_API_KEY === undefined) delete process.env.ANTHROPIC_API_KEY;
    else process.env.ANTHROPIC_API_KEY = PRIOR_ANTHROPIC_API_KEY;
    if (PRIOR_GITHUB_TOKEN === undefined) delete process.env.GITHUB_TOKEN;
    else process.env.GITHUB_TOKEN = PRIOR_GITHUB_TOKEN;
    if (PRIOR_GH_TOKEN === undefined) delete process.env.GH_TOKEN;
    else process.env.GH_TOKEN = PRIOR_GH_TOKEN;
    rmSync(root, { recursive: true, force: true });
  });

  for (const decision of ['approve', 'reject'] as const) {
    test.skipIf(process.env.ARCHON_RUN_DOCKER_CONTROLLER_TEST !== '1')(
      `freezes real artifact volume evidence and enforces the human ${decision} operation`,
      async () => {
        const workflow = makeWorkflow();
        const image = await resolveTestImage();
        const playwrightNodeModules = resolvePlaywrightNodeModules();
        const verifierImage = await resolveVerifierImage();
        writeApprovalPolicy(archonHome, workflow, image, verifierImage, playwrightNodeModules);
        const modules = await loadRuntimeModules();
        await seedParents(modules.db, source);
        const run = await modules.workflowDb.createWorkflowRun({
          workflow_name: workflow.name,
          conversation_id: 'conv-docker',
          codebase_id: 'cb-docker',
          user_message: 'ship',
          working_path: '/workspace',
          metadata: { isolation: 'container' },
        });
        const session = modules.prepareHardenedControllerSession({
          runId: run.id,
          workflow,
          sourceRoot: source,
          repoInputs: [{ targetPath: 'api' }],
          conversationId: 'conv-docker',
          userMessage: 'ship',
          image,
          requestedImage: process.env.ARCHON_CONTAINER_TEST_IMAGE,
          budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 100_000 } },
        });
        const proxyBudget = getHardenedControllerProxyBudgetSeed(session);
        if (!proxyBudget) throw new Error('missing proxy budget seed');
        const backend = new ContainerBackend({
          store: modules.createIsolationStore(),
          config: {
            profile: 'hardened',
            image,
            network: 'none',
            memoryMb: 2048,
            pidsLimit: 256,
            egressPolicy: getHardenedControllerEgressPolicy(session),
            proxyBudget,
          },
        });
        let envId: string | undefined;

        try {
          const prepared = await backend.prepare({
            codebase: {
              id: 'cb-docker',
              defaultCwd: '/workspace',
              name: 'fixture',
              kind: 'folder',
            },
            seed: session.seed,
            ownerRunId: run.id,
          });
          envId = prepared.envId;
          const isolation = await modules.createIsolationStore().getById(envId);
          expect(typeof isolation?.metadata.egressPolicyB64).toBe('string');
          expect(isolation?.metadata.egressPolicyB64).toBe(session.policyMetadata.egressPolicyB64);
          expect(isolation?.metadata.proxyBudgetSeedDigest).toBe(
            session.policyMetadata.proxyBudgetSeedDigest
          );
          const budgetBinding = proxyBudgetBinding(session);
          const budgetStatus = await backend.readProxyBudgetStatus(prepared.envId, budgetBinding);
          expect(budgetStatus).toMatchObject({
            source: 'controller-proxy-ledger',
            envId: prepared.envId,
            pendingReservations: 0,
            unknownReservations: 0,
            acceptingReservations: true,
            consumed: { input: 0, output: 0 },
          });
          expect(budgetStatus.grant).toMatchObject({
            runId: run.id,
            workflowDigest: computeControllerWorkflowDigest(workflow),
            totalTokenLimit: 100_000,
          });
          assertNoCanariesInContainer(prepared.execContext.containerId);
          assertBudgetMaterialInaccessibleToAgent(prepared.execContext.containerId);
          await assertPreparedOwnership(modules.createIsolationStore(), prepared.envId, run.id);
          await modules.workflowDb.updateWorkflowRun(run.id, {
            metadata: {
              isolation: 'container',
              isolation_env_id: prepared.envId,
              hardened_controller_policy: session.policyMetadata,
              hardened_controller_policy_path: session.policyPath,
              hardened_budget: persistedBudget(session),
              [WORKFLOW_PIN_METADATA_KEY]: buildWorkflowPinState(workflow, 'project'),
            },
          });
          const deps = makeDeps(
            modules.createWorkflowStore(),
            session,
            (envId, destinationDir) => backend.snapshotArtifacts(envId, destinationDir),
            playwrightNodeModules
          );
          const container = {
            envId: prepared.envId,
            writeBack: 'auto' as const,
            backend,
            ...budgetBinding,
          };

          const first = await modules.executeWorkflow(
            deps,
            makePlatform(),
            'conv-platform',
            prepared.cwd,
            workflow,
            'ship',
            'conv-docker',
            {
              preCreatedRun: {
                ...run,
                metadata: (await modules.workflowDb.getWorkflowRun(run.id))?.metadata ?? {},
              },
              codebaseId: 'cb-docker',
              execContext: prepared.execContext,
              container,
              source: 'project',
            }
          );
          expect(first.success).toBe(true);
          expect((await modules.workflowDb.getWorkflowRun(run.id))?.status).toBe('paused');
          const freezeOutput = await completedOutput(
            modules.createWorkflowStore(),
            run.id,
            'freeze'
          );
          expect(freezeOutput.binding_id).toEqual(expect.stringMatching(/^[0-9a-f]{64}$/));
          expect(freezeOutput.oracle_digest).toEqual(expect.any(String));
          const frozenReceiptBytes = readFileSync(String(freezeOutput.receipt), 'utf8');
          const freezeReceipt = JSON.parse(frozenReceiptBytes) as Record<string, unknown>;
          const oracleFiles = freezeReceipt.oracleFiles as Record<string, unknown>[];
          expect(oracleFiles.map(file => file.path).sort()).toEqual([
            'run/oracle/acceptance.browser.json',
            'run/oracle/plan.md',
          ]);
          expect(oracleFiles.find(file => file.path === 'run/oracle/plan.md')?.size).toBe(29);
          expect(
            readFileSync(join(String(freezeReceipt.snapshotDir), 'run/oracle/plan.md'), 'utf8')
          ).toBe('approved acceptance criteria\n');
          expect(() =>
            readFileSync(join(String(freezeReceipt.snapshotDir), '.env'), 'utf8')
          ).toThrow();
          const freezeCompleted = await eventRecord(
            modules.createWorkflowStore(),
            run.id,
            'node_completed',
            'freeze'
          );
          const requestedEvent = await eventRecord(
            modules.createWorkflowStore(),
            run.id,
            'approval_requested',
            'human-approval'
          );
          expect(Number(freezeCompleted.event_order)).toBeLessThan(
            Number(requestedEvent.event_order)
          );
          const requested = readEventData(requestedEvent.data);
          expect(String(requested.message)).toContain(String(freezeOutput.binding_id));
          expect(String(requested.message)).toContain(String(freezeOutput.oracle_digest));

          if (decision === 'reject') {
            const result = await modules.rejectWorkflow(run.id, 'acceptance checks need revision');
            expect(result.cancelled).toBe(true);
            expect((await modules.workflowDb.getWorkflowRun(run.id))?.status).toBe('cancelled');
            await expect(modules.resumeWorkflow(run.id)).rejects.toThrow(
              "Cannot resume run with status 'cancelled'"
            );
            const rejected = await eventRecord(
              modules.createWorkflowStore(),
              run.id,
              'approval_received',
              'human-approval'
            );
            expect(readEventData(rejected.data)).toMatchObject({
              decision: 'rejected',
              reason: 'acceptance checks need revision',
              fresh_guarded_run_required: true,
            });
            expect(readFileSync(String(freezeOutput.receipt), 'utf8')).toBe(frozenReceiptBytes);
            return;
          }

          await modules.approveWorkflow(run.id, `approved ${String(freezeOutput.binding_id)}`);
          const approvalCompleted = await eventRecord(
            modules.createWorkflowStore(),
            run.id,
            'node_completed',
            'human-approval'
          );
          const approvalReceived = await eventRecord(
            modules.createWorkflowStore(),
            run.id,
            'approval_received',
            'human-approval'
          );
          expect(Number(requestedEvent.event_order)).toBeLessThan(
            Number(approvalCompleted.event_order)
          );
          expect(Number(approvalCompleted.event_order)).toBeLessThan(
            Number(approvalReceived.event_order)
          );
          const currentRun = await modules.workflowDb.getWorkflowRun(run.id);
          if (!currentRun) throw new Error('missing workflow run after approval');
          expect(currentRun.metadata.hardened_controller_policy).not.toHaveProperty(
            'workflowSource'
          );
          const persistedPolicy = currentRun.metadata.hardened_controller_policy;
          const missingSeedDigest = { ...persistedPolicy };
          delete (missingSeedDigest as Record<string, unknown>).proxyBudgetSeedDigest;
          expect(() =>
            modules.resumeHardenedControllerSession({
              runId: run.id,
              workflow,
              image,
              policyMetadata: missingSeedDigest,
              budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 100_000 } },
            })
          ).toThrow(
            /private session binding changed|provider budget seed lacks egress authority|persisted egress policy lacks operator authority/
          );
          expect(() =>
            modules.resumeHardenedControllerSession({
              runId: run.id,
              workflow,
              image,
              policyMetadata: persistedPolicy,
              budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 100_001 } },
            })
          ).toThrow(/budget grant does not match/);
          const authenticated = modules.resumeHardenedControllerSession({
            runId: run.id,
            workflow,
            image,
            policyMetadata: persistedPolicy,
            budget: { deadlineAt: '2030-01-01T00:00:00.000Z', tokens: { total: 100_000 } },
          });
          expect(getHardenedControllerProxyBudgetSeed(authenticated)?.grant.runId).toBe(run.id);
          const resumeBinding = {
            egressPolicyB64: authenticated.policyMetadata.egressPolicyB64,
            image: authenticated.policyMetadata.image,
            ownerRunId: authenticated.policyMetadata.runId,
            proxyBudgetSeedDigest: authenticated.policyMetadata.proxyBudgetSeedDigest,
          };
          const resumed = await backend.resumeEnv(prepared.envId, resumeBinding);
          await expect(
            backend.readProxyBudgetStatus(prepared.envId, resumeBinding)
          ).resolves.toMatchObject({
            source: 'controller-proxy-ledger',
            envId: prepared.envId,
            pendingReservations: 0,
            unknownReservations: 0,
            acceptingReservations: true,
            consumed: { input: 0, output: 0 },
          });
          const hydrated = await modules.hydrateResumableRun(deps, currentRun);
          if (!hydrated) throw new Error('expected resumable DAG state');
          const second = await modules.executeWorkflow(
            deps,
            makePlatform(),
            'conv-platform',
            resumed.cwd,
            workflow,
            'ship',
            'conv-docker',
            {
              ...hydrated,
              codebaseId: 'cb-docker',
              execContext: resumed.execContext,
              container: {
                envId: prepared.envId,
                writeBack: 'auto' as const,
                backend,
                ...resumeBinding,
              },
              source: 'project',
            }
          );

          expect(second.success).toBe(false);
          const failedEvents =
            (await modules.createWorkflowStore().listWorkflowEvents?.(run.id))?.filter(
              event => event.event_type === 'node_failed'
            ) ?? [];
          expect(`${second.error ?? ''} ${JSON.stringify(failedEvents)}`).toContain(
            'hardened container write-back is not implemented'
          );
          const approvalOutput = await completedOutput(
            modules.createWorkflowStore(),
            run.id,
            'approval-check'
          );
          expect(approvalOutput).toMatchObject({
            approved: true,
            binding_id: freezeOutput.binding_id,
            oracle_digest: freezeOutput.oracle_digest,
          });
          const importOutput = await completedOutput(
            modules.createWorkflowStore(),
            run.id,
            'candidate-import'
          );
          expect(importOutput.schema).toBe('archon.candidate-import-result.v1');
          expect(importOutput.authority).toBe('none');
          expect(importOutput.repositoryTarget).toBe('api');
          const imported = importOutput.import as Record<string, unknown>;
          expect(imported.authority).toBe('none');
          const content = imported.content as Record<string, unknown>;
          const candidate = importOutput.candidate as Record<string, unknown>;
          expect(imported.candidateCommit).toBe(candidate.commit);
          expect(imported.candidateTreeOid).toBe(candidate.tree);
          expect(readFileSync(join(String(content.destination), 'README.md'), 'utf8')).toBe(
            'candidate v2\n'
          );
          expect(Object.keys(candidate).sort()).toEqual(['commit', 'tree']);
          const blackboxOutput = await completedOutput(
            modules.createWorkflowStore(),
            run.id,
            'candidate-blackbox'
          );
          expect(blackboxOutput).toMatchObject({
            schema: 'archon.candidate-blackbox-test-result.v1',
            authority: 'none',
            status: 'passed',
            repositoryTarget: 'api',
            profile: 'static-web-http-v1',
            verifiedOnly: true,
          });
          const blackboxReceipt = JSON.parse(readFileSync(String(blackboxOutput.receipt), 'utf8'));
          expect(blackboxReceipt.schema).toBe('archon.candidate-blackbox-test-receipt.v1');
          expect(blackboxReceipt.rawObservationAuthority).toBe('none');
          expect(blackboxReceipt.validatorNodeModulesPath).toContain('validator-node-modules');
          await modules.workflowDb.updateWorkflowRun(run.id, { status: 'running' });
          const replayOutput = await replayBlackboxControllerAction(
            deps,
            modules,
            run.id,
            workflow,
            prepared.cwd,
            prepared.execContext
          );
          expect(replayOutput).toMatchObject({
            schema: 'archon.candidate-blackbox-test-result.v1',
            authority: 'none',
            status: 'passed',
            verifiedOnly: true,
          });
          expect(replayOutput.receipt).toBe(blackboxOutput.receipt);
          expect(readFileSync(String(freezeOutput.receipt), 'utf8')).toContain(
            String(freezeOutput.binding_id)
          );
        } finally {
          if (envId) await backend.destroy(envId);
          await assertNoOwnedDockerResources(run.id);
        }
      },
      120_000
    );
  }
});

async function loadRuntimeModules() {
  const connection = await import('../db/connection');
  const workflowDb = await import('../db/workflows');
  const { createWorkflowStore } = await import('./store-adapter');
  const { createIsolationStore } = await import('../db/isolation-environments');
  const { executeWorkflow, hydrateResumableRun } = await import('@archon/workflows/executor');
  const { approveWorkflow, rejectWorkflow, resumeWorkflow } =
    await import('../operations/workflow-operations');
  const hardened = await import('./hardened-controller');
  return {
    db: connection.getDatabase(),
    workflowDb,
    createWorkflowStore,
    createIsolationStore,
    executeWorkflow,
    hydrateResumableRun,
    approveWorkflow,
    rejectWorkflow,
    resumeWorkflow,
    prepareHardenedControllerSession: hardened.prepareHardenedControllerSession,
    resumeHardenedControllerSession: hardened.resumeHardenedControllerSession,
  };
}

async function seedParents(
  db: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
  cwd: string
) {
  await db.query(
    `INSERT INTO remote_agent_conversations (id, platform_type, platform_conversation_id)
     VALUES ('conv-docker', 'test', 'conv-platform')`,
    []
  );
  await db.query(
    `INSERT INTO remote_agent_codebases (id, name, default_cwd, kind)
     VALUES ('cb-docker', 'fixture', $1, 'folder')`,
    [cwd]
  );
}

function proxyBudgetBinding(session: HardenedControllerSession): {
  egressPolicyB64: string;
  image: string;
  ownerRunId: string;
  proxyBudgetSeedDigest: string;
} {
  return {
    egressPolicyB64: session.policyMetadata.egressPolicyB64,
    image: session.policyMetadata.image,
    ownerRunId: session.policyMetadata.runId,
    proxyBudgetSeedDigest: session.policyMetadata.proxyBudgetSeedDigest,
  };
}

async function resolveTestImage(): Promise<string> {
  if (process.env.DOCKER_HOST && !process.env.DOCKER_HOST.startsWith('unix://')) {
    throw new Error('Docker controller integration requires a local unix:// DOCKER_HOST.');
  }
  const image = process.env.ARCHON_CONTAINER_TEST_IMAGE;
  if (!image) {
    throw new Error('ARCHON_CONTAINER_TEST_IMAGE must point to the unique budget-enabled image.');
  }
  const output = execFileSync('docker', ['image', 'inspect', image, '--format', '{{.Id}}'], {
    encoding: 'utf8',
  }).trim();
  expect(output).toMatch(IMAGE_ID_PATTERN);
  return output;
}

async function resolveVerifierImage(): Promise<string> {
  const image = process.env.ARCHON_PLAYWRIGHT_VERIFIER_IMAGE ?? DEFAULT_PLAYWRIGHT_VERIFIER_IMAGE;
  const output = execFileSync('docker', ['image', 'inspect', image, '--format', '{{.Id}}'], {
    encoding: 'utf8',
  }).trim();
  expect(output).toMatch(IMAGE_ID_PATTERN);
  return image;
}

function resolvePlaywrightNodeModules(): string {
  const nodeModules = process.env.ARCHON_PLAYWRIGHT_NODE_MODULES ?? DEFAULT_PLAYWRIGHT_NODE_MODULES;
  for (const name of ['playwright', 'playwright-core']) {
    const packageJson = join(nodeModules, name, 'package.json');
    const version = JSON.parse(readFileSync(packageJson, 'utf8')) as { version?: unknown };
    expect(version.version).toBe('1.60.0');
  }
  return nodeModules;
}

function copyValidatorPackageTree(
  sourceNodeModules: string,
  targetNodeModules: string,
  name: string
): void {
  copyValidatorTree(join(sourceNodeModules, name), join(targetNodeModules, name));
}

function copyValidatorTree(source: string, target: string): void {
  const stat = lstatSync(source);
  if (stat.isSymbolicLink()) throw new Error('test validator source must not contain symlinks');
  if (stat.isFile()) {
    mkdirSync(dirname(target), { recursive: true, mode: 0o700 });
    copyFileSync(source, target);
    chmodSync(target, 0o600);
    return;
  }
  if (!stat.isDirectory()) throw new Error('test validator source must contain only files');
  mkdirSync(target, { recursive: true, mode: 0o700 });
  chmodSync(target, 0o700);
  for (const entry of readdirSync(source))
    copyValidatorTree(join(source, entry), join(target, entry));
}

function validatorDigests(nodeModules: string): { playwrightDigest: string; coreDigest: string } {
  return {
    playwrightDigest: validatorTreeDigest(join(nodeModules, 'playwright')),
    coreDigest: validatorTreeDigest(join(nodeModules, 'playwright-core')),
  };
}

function validatorTreeDigest(root: string): string {
  const files: Record<string, unknown>[] = [];
  collectValidatorFiles(root, root, files);
  return stableDigest(
    files.sort((left, right) => compareCodepoint(String(left.path), String(right.path)))
  );
}

function collectValidatorFiles(
  root: string,
  current: string,
  files: Record<string, unknown>[]
): void {
  for (const entry of readdirSync(current, { withFileTypes: true })) {
    const path = join(current, entry.name);
    if (entry.isDirectory()) {
      collectValidatorFiles(root, path, files);
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

function compareCodepoint(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function stableDigest(value: unknown): string {
  return createHash('sha256').update(stableSerialize(value)).digest('hex');
}

function stableSerialize(value: unknown): string {
  if (value === undefined) return 'null';
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableSerialize).join(',')}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .filter(key => record[key] !== undefined)
    .map(key => `${JSON.stringify(key)}:${stableSerialize(record[key])}`)
    .join(',')}}`;
}

async function assertPreparedOwnership(
  store: ReturnType<Awaited<ReturnType<typeof loadRuntimeModules>>['createIsolationStore']>,
  envId: string,
  ownerRunId: string
): Promise<void> {
  const row = await store.getById(envId);
  const metadata = readMetadata(row?.metadata);
  const containerId = stringField(metadata, 'containerId');
  const workspaceVolume = stringField(metadata, 'workspaceVolume');
  const homeVolume = stringField(metadata, 'homeVolume');
  const artifactsVolume = stringField(metadata, 'artifactsVolume');
  expect(stringField(metadata, 'ownerRunId')).toBe(ownerRunId);
  expect(stringField(metadata, 'image')).toMatch(IMAGE_ID_PATTERN);
  expect(await dockerLabel('container', containerId, 'diy.archon.owner-run-id')).toBe(ownerRunId);
  for (const volume of [workspaceVolume, homeVolume, artifactsVolume]) {
    expect(await dockerLabel('volume', volume, 'diy.archon.owner-run-id')).toBe(ownerRunId);
  }
}

async function assertNoOwnedDockerResources(ownerRunId: string): Promise<void> {
  const containers = dockerList([
    'ps',
    '-a',
    '--filter',
    `label=diy.archon.owner-run-id=${ownerRunId}`,
    '--format',
    '{{.Names}}',
  ]);
  const volumes = dockerList([
    'volume',
    'ls',
    '--filter',
    `label=diy.archon.owner-run-id=${ownerRunId}`,
    '--format',
    '{{.Name}}',
  ]);
  expect(containers).toEqual([]);
  expect(volumes).toEqual([]);
}

async function dockerLabel(
  kind: 'container' | 'volume',
  name: string,
  label: string
): Promise<string> {
  const template =
    kind === 'container'
      ? `{{ index .Config.Labels ${JSON.stringify(label)} }}`
      : `{{ index .Labels ${JSON.stringify(label)} }}`;
  const args = kind === 'container' ? ['inspect', '-f'] : ['volume', 'inspect', '-f'];
  return execFileSync('docker', [...args, template, name], { encoding: 'utf8' }).trim();
}

function dockerList(args: string[]): string[] {
  const output = execFileSync('docker', args, { encoding: 'utf8' }).trim();
  return output.length === 0 ? [] : output.split('\n');
}

function readMetadata(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('metadata missing');
  return value as Record<string, unknown>;
}

function stringField(value: Record<string, unknown>, field: string): string {
  const raw = value[field];
  if (typeof raw !== 'string' || raw.length === 0) throw new Error(`metadata ${field} missing`);
  return raw;
}

function assertNoCanariesInContainer(containerId: string): void {
  const output = execFileSync(
    'docker',
    [
      'exec',
      '-u',
      'archon',
      containerId,
      'sh',
      '-c',
      [
        'if test -f /workspace/api/README.md; then echo readme=present; else echo readme=missing; fi',
        'if test -e /workspace/.env; then echo env=present; else echo env=absent; fi',
        'if test -e /workspace/controller.key.backup; then echo key=present; else echo key=absent; fi',
        'stat -c "api=%U:%G:%a root=%U:%G:%a" /workspace/api /workspace || true',
        'find /workspace -maxdepth 3 -print | sort',
      ].join('; '),
    ],
    { encoding: 'utf8' }
  );
  const lines = output.trim().split('\n');
  expect(lines.slice(0, 3), output).toEqual(['readme=present', 'env=absent', 'key=absent']);
}

function assertBudgetMaterialInaccessibleToAgent(containerId: string): void {
  const output = execFileSync(
    'docker',
    [
      'exec',
      '-u',
      'archon',
      containerId,
      'sh',
      '-c',
      [
        'if test -e /archon-budget/ledger.sqlite; then echo ledger=present; else echo ledger=absent; fi',
        'if test -e /archon-proxy-private/budget.json; then echo grant=present; else echo grant=absent; fi',
      ].join('; '),
    ],
    { encoding: 'utf8' }
  );
  expect(output.trim().split('\n'), output).toEqual(['ledger=absent', 'grant=absent']);
}

function makeWorkflow(): WorkflowDefinition {
  return {
    name: 'hardened-controller-docker-e2e',
    mutates_checkout: false,
    hardened: { required: true },
    container: { enabled: true },
    nodes: [
      {
        id: 'write-oracle',
        bash: [
          'mkdir -p "$ARTIFACTS_DIR/oracle"',
          'printf "approved acceptance criteria\\n" > "$ARTIFACTS_DIR/oracle/plan.md"',
          `printf '%s\\n' '${JSON.stringify(BROWSER_POLICY)}' > "$ARTIFACTS_DIR/oracle/acceptance.browser.json"`,
        ].join(' && '),
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
          on_reject: {
            max_attempts: 3,
            prompt: 'Rewrite the frozen acceptance checks in this run',
          },
        },
      },
      {
        id: 'approval-check',
        depends_on: ['human-approval'],
        controller_action: 'verify-approval',
        phase: 'planning-approval',
      },
      {
        id: 'export-candidate',
        depends_on: ['approval-check'],
        bash: [
          'cd /workspace/api',
          'printf "candidate v2\\n" > README.md',
          'printf "<main>candidate v2</main>\\n" > index.html',
          'git add index.html',
          'git -c user.name=Test -c user.email=test@example.com commit -am candidate',
          'git update-ref refs/candidates/sealed HEAD',
          'git bundle create "$ARTIFACTS_DIR/candidate.bundle" refs/candidates/sealed',
          'commit=$(git rev-parse HEAD)',
          'tree=$(git rev-parse HEAD^{tree})',
          `printf '{"schema":"archon.candidate-proposal.v1","commit":"%s","tree":"%s"}\n' "$commit" "$tree" > "$ARTIFACTS_DIR/candidate.json"`,
        ].join(' && '),
      },
      {
        id: 'candidate-import',
        depends_on: ['export-candidate'],
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
  } as WorkflowDefinition;
}

function writeApprovalPolicy(
  home: string,
  workflow: WorkflowDefinition,
  image: string,
  verifierImage: string,
  playwrightNodeModules: string
): void {
  const policyPath = join(home, 'controller-policy', 'planning-approval.json');
  mkdirSync(dirname(policyPath), { recursive: true, mode: 0o700 });
  const privateValidatorNodeModules = join(home, 'controller-policy', 'validator-node-modules');
  copyValidatorPackageTree(playwrightNodeModules, privateValidatorNodeModules, 'playwright');
  copyValidatorPackageTree(playwrightNodeModules, privateValidatorNodeModules, 'playwright-core');
  chmodSync(privateValidatorNodeModules, 0o700);
  const validator = validatorDigests(privateValidatorNodeModules);
  const policy = {
    schema: 'archon.hardened-controller-planning-policy.v1',
    version: 1,
    workflowDigest: computeControllerWorkflowDigest(workflow),
    validatorSource: { nodeModulesPath: privateValidatorNodeModules },
    egress: {
      image,
      policy: {
        targets: [{ host: 'registry.example', port: 443 }],
        httpGrants: [
          { host: 'registry.example', port: 443, methods: ['GET'], paths: ['/package'] },
        ],
      },
    },
    providerBudget: {
      policies: [
        {
          provider: 'openai',
          host: 'registry.example',
          model: 'gpt-5.6-sol',
          maxInputTokens: 4096,
          maxOutputTokens: 1024,
          allowedHeaders: { 'openai-beta': 'responses=v1' },
        },
      ],
    },
    grants: [
      {
        nodeId: 'freeze',
        action: 'finalize-evidence',
        phase: 'planning-freeze',
        oracleFiles: ['run/oracle/plan.md', 'run/oracle/acceptance.browser.json'],
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
        repositoryTarget: 'api',
        bundleArtifact: 'run/candidate.bundle',
        candidateArtifact: 'run/candidate.json',
      },
      {
        nodeId: 'candidate-blackbox',
        action: 'finalize-evidence',
        phase: 'candidate-blackbox-test',
        repositoryTarget: 'api',
        candidateImportNodeId: 'candidate-import',
        freezeNodeId: 'freeze',
        approvalReceiptNodeId: 'approval-check',
        profile: 'static-web-http-v1',
        acceptancePolicyPath: 'run/oracle/acceptance.browser.json',
        appRoot: '.',
        port: 4173,
        staticHelperImage: image,
        verifierImage,
        validatorPackageDigest: validator.playwrightDigest,
        validatorCorePackageDigest: validator.coreDigest,
      },
    ],
  };
  writeFileSync(policyPath, JSON.stringify(policy, null, 2), { mode: 0o600 });
  chmodSync(policyPath, 0o600);
}

function makeDeps(
  store: WorkflowDeps['store'],
  session: Awaited<ReturnType<typeof loadRuntimeModules>> extends {
    prepareHardenedControllerSession: infer F;
  }
    ? F extends (...args: never[]) => infer R
      ? R
      : never
    : never,
  snapshotArtifacts: (envId: string, destinationDir: string) => Promise<unknown>,
  playwrightNodeModules: string
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
    controllerActions: createActions(session, store, snapshotArtifacts, playwrightNodeModules),
    controllerActionGrants: session.controllerActionGrants,
    workflowBudgetGrants: session.workflowBudgetGrants,
  };
}

function createActions(
  session: HardenedControllerSession,
  store: WorkflowDeps['store'],
  snapshotArtifacts: (envId: string, destinationDir: string) => Promise<unknown>,
  playwrightNodeModules: string
) {
  return createHardenedControllerActions({
    session,
    store,
    snapshotArtifacts,
    playwrightNodeModules,
  });
}

async function replayBlackboxControllerAction(
  deps: WorkflowDeps,
  modules: Awaited<ReturnType<typeof loadRuntimeModules>>,
  runId: string,
  workflow: WorkflowDefinition,
  cwd: string,
  execContext: Parameters<IAgentProvider['run']>[0]
): Promise<Record<string, unknown>> {
  const grant = deps.controllerActionGrants?.find(item => item.nodeId === 'candidate-blackbox');
  const node = workflow.nodes.find(item => item.id === 'candidate-blackbox');
  const workflowRun = await modules.workflowDb.getWorkflowRun(runId);
  if (!grant || !node || !workflowRun) throw new Error('missing candidate blackbox replay binding');
  const output = await deps.controllerActions?.['finalize-evidence']?.({
    workflowRun,
    workflowName: workflow.name,
    workflowDigest: computeControllerWorkflowDigest(workflow),
    node,
    actionManifest: grant.actionManifest,
    signal: new AbortController().signal,
    deadlineAt: Date.now() + 120_000,
    cwd,
    artifactsDir: join(cwd, 'run'),
    stateDir: join(cwd, '.state'),
    logDir: join(cwd, '.logs'),
    baseBranch: 'main',
    docsDir: join(cwd, 'docs'),
    config: {
      assistant: PROVIDER_ID,
      baseBranch: 'main',
      assistants: { claude: {}, codex: {}, [PROVIDER_ID]: {} },
      commands: {},
      defaults: { loadDefaultCommands: false, loadDefaultWorkflows: false },
    },
    platform: makePlatform(),
    conversationId: 'conv-docker',
    execContext,
  });
  if (!output || typeof output !== 'object' || Array.isArray(output)) {
    throw new Error('candidate blackbox replay did not return structured output');
  }
  return output;
}

function persistedBudget(session: HardenedControllerSession) {
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

async function completedOutput(
  store: WorkflowDeps['store'],
  runId: string,
  nodeId: string
): Promise<Record<string, unknown>> {
  const events = await store.listWorkflowEvents?.(runId);
  const event = events?.find(
    item => item.event_type === 'node_completed' && item.step_name === nodeId
  );
  if (!event) throw new Error(`missing node_completed event for ${nodeId}`);
  const data = readEventData(event.data);
  const raw = data.node_output;
  if (typeof raw !== 'string') throw new Error(`missing node_output for ${nodeId}`);
  return JSON.parse(raw) as Record<string, unknown>;
}

async function eventRecord(
  store: WorkflowDeps['store'],
  runId: string,
  type: string,
  nodeId: string
) {
  const events = await store.listWorkflowEvents?.(runId);
  const event = events?.find(item => item.event_type === type && item.step_name === nodeId);
  if (!event) throw new Error(`missing ${type} event for ${nodeId}`);
  if (typeof event.event_order !== 'number' || event.event_order < 0) {
    throw new Error(`missing safe event_order for ${type}:${nodeId}`);
  }
  return event;
}

function readEventData(data: unknown): Record<string, unknown> {
  if (typeof data === 'string') return JSON.parse(data) as Record<string, unknown>;
  if (!data || typeof data !== 'object' || Array.isArray(data)) return {};
  return data as Record<string, unknown>;
}

function createChildRepoWithParent(
  parent: string,
  name: string,
  parentReadme: string,
  baselineReadme: string
): string {
  const repo = join(parent, name);
  mkdirSync(repo, { recursive: true });
  writeFileSync(join(repo, 'README.md'), parentReadme);
  git(repo, ['init']);
  git(repo, ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'add', 'README.md']);
  git(repo, [
    '-c',
    'user.name=Test',
    '-c',
    'user.email=test@example.com',
    'commit',
    '-m',
    'parent',
  ]);
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
