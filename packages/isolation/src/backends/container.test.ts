import { describe, test, expect } from 'bun:test';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
import { randomUUID } from 'crypto';
import { ContainerBackend } from './container';
import type { ContainerBackendConfig } from '../types';
import type { IIsolationStore } from '../store';
import type { IsolationEnvironmentRow, CreateEnvironmentParams } from '../types';
import { dockerCli, type DockerRunner, type DockerExecResult } from '../container/docker-exec';
import { encodeStrictEgressPolicy } from '../egress/strict-policy';
import { digestBudgetPolicy, digestProxyBudgetSeed } from '../egress/strict-proxy-launcher';
import type { ProxyBudgetGrant } from '../egress/proxy-budget-ledger';
import type { TrustedProviderBudgetPolicy } from '../egress/provider-budget-contract';

const RESOLVED_IMAGE = 'sha256:' + '2'.repeat(64);
const METADATA_IMAGE = 'sha256:' + '1'.repeat(64);

const CONFIG: ContainerBackendConfig = {
  profile: 'hardened',
  image: 'archon-runner:test',
  network: 'none',
  memoryMb: 4096,
  pidsLimit: 512,
};

const FOLDER = {
  id: 'cb-1',
  defaultCwd: '/tmp/ops-client',
  name: 'ops-client',
  kind: 'folder' as const,
};
const SEED = { kind: 'directory' as const, path: '/tmp/controller-seed/ops-client' };
const STRICT_EGRESS_POLICY = {
  targets: [{ host: 'registry.npmjs.org', port: 443 }],
  httpGrants: [{ host: 'registry.npmjs.org', port: 443, methods: ['GET'], pathPrefixes: ['/-/'] }],
};
const FROZEN_STRICT_POLICY = encodeStrictEgressPolicy(STRICT_EGRESS_POLICY);
const TLS_VALID_UNTIL = '2099-01-01T00:00:00.000Z';
const PROVIDER_POLICIES: TrustedProviderBudgetPolicy[] = [
  {
    provider: 'openai',
    host: 'registry.npmjs.org',
    model: 'gpt-test',
    maxInputTokens: 100,
    maxOutputTokens: 50,
  },
];
const FROZEN_BUDGET_DIGEST = budgetDigestFor(RESOLVED_IMAGE);
const PROXY_BUDGET_GRANT: ProxyBudgetGrant = {
  schema: 'archon.proxy-budget-grant.v1',
  rootChainId: 'chain-test',
  runId: 'run-test',
  workflowDigest: 'sha256:workflow-test',
  policyDigest: FROZEN_BUDGET_DIGEST,
  deadlineEpochMs: Date.now() + 3_600_000,
  inputTokenLimit: 1_000,
  outputTokenLimit: 500,
  totalTokenLimit: 1_200,
};
const PROXY_BUDGET_GRANT_SEED = {
  grant: PROXY_BUDGET_GRANT,
  providerPolicies: PROVIDER_POLICIES,
};
const METADATA_PROXY_BUDGET_GRANT: ProxyBudgetGrant = {
  ...PROXY_BUDGET_GRANT,
  policyDigest: budgetDigestFor(METADATA_IMAGE),
};

function strictBudgetConfig(
  overrides: Partial<ContainerBackendConfig> = {},
  grant?: ProxyBudgetGrant,
  providerPolicies: TrustedProviderBudgetPolicy[] = PROVIDER_POLICIES
): ContainerBackendConfig {
  const image = typeof overrides.image === 'string' ? overrides.image : RESOLVED_IMAGE;
  const effectiveGrant = grant ?? {
    ...PROXY_BUDGET_GRANT,
    policyDigest: budgetDigestFor(image, providerPolicies),
  };
  return {
    ...CONFIG,
    egressPolicy: STRICT_EGRESS_POLICY,
    proxyBudget: { grant: effectiveGrant, providerPolicies },
    ...overrides,
  } as ContainerBackendConfig;
}

function budgetDigestFor(
  image: string,
  providerPolicies: TrustedProviderBudgetPolicy[] = PROVIDER_POLICIES
): string {
  return digestBudgetPolicy({
    egressPolicyB64: FROZEN_STRICT_POLICY,
    image,
    providerPolicies,
  });
}

function egressMetadata(resourceId: string): Record<string, unknown> {
  return {
    proxyContainerName: `archon-${resourceId}-egress-proxy`,
    egressVolume: `archon-${resourceId}-egress`,
    tlsVolume: `archon-${resourceId}-egress-tls`,
    budgetVolume: `archon-${resourceId}-budget`,
    tlsValidUntil: TLS_VALID_UNTIL,
    egressPolicyB64: FROZEN_STRICT_POLICY,
    budgetPolicyDigest: budgetDigestFor(METADATA_IMAGE),
    proxyBudgetSeedDigest: digestProxyBudgetSeed({
      grant: METADATA_PROXY_BUDGET_GRANT,
      providerPolicies: PROVIDER_POLICIES,
    }),
  };
}

function egressBinding(resourceId: string, ownerRunId = 'run-owner') {
  const metadata = egressMetadata(resourceId);
  return {
    egressPolicyB64: metadata.egressPolicyB64 as string,
    image: METADATA_IMAGE,
    ownerRunId,
    proxyBudgetSeedDigest: metadata.proxyBudgetSeedDigest as string,
  };
}

function budgetStatusJson(overrides: Record<string, unknown> = {}): string {
  return (
    JSON.stringify({
      ok: true,
      event: 'status',
      result: {
        grant: METADATA_PROXY_BUDGET_GRANT,
        pendingReservations: 0,
        unknownReservations: 0,
        consumedInputTokens: 20,
        consumedOutputTokens: 10,
        consumedTotalTokens: 30,
        remainingInputTokens: 980,
        remainingOutputTokens: 490,
        remainingTotalTokens: 1170,
        acceptingReservations: true,
        ...overrides,
      },
    }) + '\n'
  );
}

function fakeStore(): IIsolationStore & {
  rows: Map<string, IsolationEnvironmentRow>;
  created?: CreateEnvironmentParams;
} {
  const rows = new Map<string, IsolationEnvironmentRow>();
  let counter = 0;
  return {
    rows,
    created: undefined,
    async getById(id) {
      return rows.get(id) ?? null;
    },
    async findActiveByWorkflow() {
      return null;
    },
    async create(env) {
      this.created = env;
      const row: IsolationEnvironmentRow = {
        id: `env-${++counter}`,
        codebase_id: env.codebase_id,
        workflow_type: env.workflow_type,
        workflow_id: env.workflow_id,
        provider: env.provider ?? 'container',
        working_path: env.working_path,
        branch_name: env.branch_name,
        status: 'active',
        created_at: new Date(),
        created_by_platform: env.created_by_platform ?? null,
        created_by_user_id: env.created_by_user_id ?? null,
        metadata: env.metadata ?? {},
      };
      rows.set(row.id, row);
      return row;
    },
    async updateStatus(id, status) {
      const row = rows.get(id);
      if (row) row.status = status;
    },
    async countActiveByCodebase() {
      return 0;
    },
  };
}

function fakeDocker(
  handler: (args: string[]) => DockerExecResult | Promise<DockerExecResult>
): DockerRunner & { calls: string[][] } {
  const calls: string[][] = [];
  const runner = (async (args: string[]) => {
    calls.push(args);
    return handler(args);
  }) as DockerRunner & { calls: string[][] };
  runner.calls = calls;
  return runner;
}

function okDocker(): DockerRunner & { calls: string[][] } {
  return fakeDocker(args => {
    if (args[0] === 'run') return { stdout: 'agent-container-id\n', stderr: '' };
    if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
      return { stdout: 'true\n', stderr: '' };
    }
    if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
      return {
        stdout: proxyInspectOutput('archon-run-egress-egress-tls', 'archon-run-egress-egress'),
        stderr: '',
      };
    }
    if (args[0] === 'inspect' && args.includes('{{.Id}}'))
      return { stdout: 'resumed-id\n', stderr: '' };
    if (args[0] === 'image' && args[1] === 'inspect')
      return { stdout: `${RESOLVED_IMAGE}\n`, stderr: '' };
    return { stdout: '', stderr: '' };
  });
}

function proxyInspectOutput(
  tlsVolume: string,
  egressVolume: string,
  budgetVolume = 'archon-run-egress-budget',
  image = METADATA_IMAGE,
  budgetDigest = budgetDigestFor(image)
): string {
  return [
    JSON.stringify([
      `ARCHON_EGRESS_POLICY_B64=${FROZEN_STRICT_POLICY}`,
      'ARCHON_EGRESS_SOCKET=/archon-egress/proxy.sock',
      `ARCHON_PROXY_IMAGE_ID=${image}`,
      `ARCHON_PROXY_BUDGET_POLICY_DIGEST=${budgetDigest}`,
    ]),
    image,
    JSON.stringify([
      {
        Type: 'volume',
        Name: tlsVolume,
        Destination: '/archon-proxy-private',
        RW: false,
      },
      { Type: 'volume', Name: egressVolume, Destination: '/archon-egress', RW: true },
      { Type: 'volume', Name: budgetVolume, Destination: '/archon-budget', RW: true },
    ]),
  ].join('\n');
}

function findStrictProxyRun(calls: string[][]): string[] | undefined {
  return calls.find(c => c.includes('/usr/local/lib/archon/egress/strict-https-proxy-cli.mjs'));
}

function findAgentContainerRun(calls: string[][]): string[] | undefined {
  return calls.find(c => c[0] === 'run' && c.includes('-d') && !findStrictProxyRun([c]));
}

function hardenedMetadata(
  resourceId: string,
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  const containerName = `archon-${resourceId}`;
  return {
    containerId: `${resourceId}-container-id`,
    containerName,
    profile: 'hardened',
    volume: `${containerName}-workspace`,
    workspaceVolume: `${containerName}-workspace`,
    homeVolume: `${containerName}-home`,
    artifactsVolume: `${containerName}-artifacts`,
    agentArtifactsDir: '/archon-artifacts',
    workspacePath: FOLDER.defaultCwd,
    image: METADATA_IMAGE,
    resourceId,
    resourceLimits: {
      policyVersion: 1,
      agentCpus: '2',
      controllerHelperCpus: '1',
      memoryMb: 4096,
      pidsLimit: 512,
    },
    isolationMode: 'hardened',
    ...overrides,
  };
}

function proxyContainerInspectOutput(
  metadata: Record<string, unknown>,
  overrides: { env?: string[]; mounts?: Record<string, unknown>[]; image?: string } = {}
): string {
  const env = overrides.env ?? [
    `ARCHON_EGRESS_POLICY_B64=${metadata.egressPolicyB64 as string}`,
    'ARCHON_EGRESS_SOCKET=/archon-egress/proxy.sock',
    `ARCHON_PROXY_IMAGE_ID=${overrides.image ?? METADATA_IMAGE}`,
    `ARCHON_PROXY_BUDGET_POLICY_DIGEST=${metadata.budgetPolicyDigest as string}`,
  ];
  const mounts = overrides.mounts ?? [
    {
      Type: 'volume',
      Name: metadata.tlsVolume as string,
      Destination: '/archon-proxy-private',
      RW: false,
    },
    {
      Type: 'volume',
      Name: metadata.egressVolume as string,
      Destination: '/archon-egress',
      RW: true,
    },
    {
      Type: 'volume',
      Name: metadata.budgetVolume as string,
      Destination: '/archon-budget',
      RW: true,
    },
  ];
  return `${JSON.stringify(env)}
${overrides.image ?? METADATA_IMAGE}
${JSON.stringify(mounts)}
`;
}

async function createContainerRow(
  store: IIsolationStore,
  resourceId: string,
  metadataOverrides: Record<string, unknown> = {}
): Promise<IsolationEnvironmentRow> {
  return store.create({
    codebase_id: FOLDER.id,
    workflow_type: 'task',
    workflow_id: resourceId,
    provider: 'container',
    working_path: FOLDER.defaultCwd,
    branch_name: '' as never,
    metadata: hardenedMetadata(resourceId, metadataOverrides),
  });
}

describe('ContainerBackend.prepare', () => {
  test('seeds named volumes and starts a non-root hardened container without host bind mounts', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });

    expect(prepared.cwd).toBe('/tmp/ops-client');
    expect(prepared.execContext).toEqual({
      kind: 'container',
      profile: 'hardened',
      containerId: 'agent-container-id',
      execUser: 'archon',
      agentArtifactsDir: '/archon-artifacts',
    });
    expect(prepared.envId).toBe('env-1');
    expect(prepared.agentArtifactsDir).toBe('/archon-artifacts');
    expect(prepared.artifactSnapshot).toEqual({
      workspaceVolume: expect.stringMatching(/^archon-.+-artifacts$/),
      image: RESOLVED_IMAGE,
      resourceId: expect.any(String),
    });

    const volumeCreates = docker.calls.filter(c => c[0] === 'volume' && c[1] === 'create');
    expect(volumeCreates.length).toBe(3);
    expect(volumeCreates.every(c => c.join(' ').includes('--label diy.archon.managed=true'))).toBe(
      true
    );

    const seedCreate = docker.calls.find(c => c[0] === 'create');
    expect(seedCreate?.join(' ')).toContain('--network none');
    expect(seedCreate?.join(' ')).toContain('--cpus 1');
    expect(seedCreate?.join(' ')).toContain('--cap-drop ALL');
    expect(seedCreate?.join(' ')).toContain('--cap-add CHOWN');
    expect(seedCreate?.join(' ')).toContain('--security-opt no-new-privileges');
    expect(seedCreate?.join(' ')).toContain(':/seed-workspace');
    expect(seedCreate?.join(' ')).not.toContain('/tmp/ops-client:/');

    const cpCall = docker.calls.find(c => c[0] === 'cp');
    expect(cpCall).toEqual([
      'cp',
      '/tmp/controller-seed/ops-client/.',
      expect.stringMatching(/-seed:\/seed-workspace\//),
    ]);

    const scrubCall = docker.calls.find(
      c => c[0] === 'exec' && c.some(part => part.includes('seed contains forbidden credential'))
    );
    const scrubScript = scrubCall?.at(-1) ?? '';
    expect(scrubScript).toContain("find '/seed-workspace'");
    expect(scrubScript).toContain("-name '.git'");
    expect(scrubScript).toContain("-name '.env*'");
    expect(scrubScript).toContain("-name '.npmrc'");
    expect(scrubScript).toContain("-name '.netrc'");
    expect(scrubScript).toContain("-name '.aws'");
    expect(scrubScript).toContain("-name '.docker'");
    expect(scrubScript).toContain("-path '*/.config/gh'");
    expect(scrubScript).toContain('seed contains forbidden credential, link or VCS path');
    expect(scrubScript).toContain('set -eu');
    expect(scrubScript).toContain('normalize_root_access');
    expect(scrubScript).toContain('bad_owner=');
    expect(scrubScript).not.toContain('| grep');
    expect(scrubScript).toContain('chown -R archon:archon');

    const initArgs = docker.calls.find(
      c =>
        c[0] === 'run' &&
        c.includes(`${prepared.artifactSnapshot?.workspaceVolume ?? ''}:/archon-artifacts`)
    );
    expect(initArgs?.join(' ')).toContain('--cpus 1');
    expect(initArgs?.join(' ')).toContain('--cap-add CHOWN');
    expect(initArgs?.join(' ')).toContain(
      'mkdir -p /archon-artifacts/run /archon-artifacts/state /archon-artifacts/logs'
    );
    expect(initArgs?.join(' ')).not.toContain('chmod');
    expect(initArgs?.join(' ')).toContain('chown -R archon:archon /archon-artifacts');

    const runArgs = docker.calls.find(c => c[0] === 'run' && c.includes('-d'));
    const joined = runArgs?.join(' ') ?? '';
    expect(joined).toContain('--user archon');
    expect(joined).toContain('--read-only');
    expect(joined).toContain('--cap-drop ALL');
    expect(joined).toContain('--security-opt no-new-privileges');
    expect(joined).toContain('--memory 4096m');
    expect(joined).toContain('--cpus 2');
    expect(joined).toContain('--pids-limit 512');
    expect(joined).toContain('--network none');
    expect(joined).not.toContain('--cap-add');
    expect(joined).toContain(':/tmp/ops-client');
    expect(joined).toContain(':/home/archon');
    expect(joined).toContain(':/archon-artifacts');
    expect(joined).toContain(RESOLVED_IMAGE);
    expect(joined).not.toContain('archon-runner:test');
    expect(joined).not.toContain('/tmp/ops-client:/mnt/lower');
    expect(joined).not.toContain('--cap-add SYS_ADMIN');
    expect(joined).not.toContain('/dev/fuse');
    const metadata = store.created?.metadata as {
      artifactsVolume: unknown;
      agentArtifactsDir: unknown;
      image: string;
      isolationMode: string;
      requestedImage: string;
      resourceLimits: unknown;
    };
    expect(metadata.isolationMode).toBe('hardened');
    expect(typeof metadata.artifactsVolume).toBe('string');
    expect(metadata.agentArtifactsDir).toBe('/archon-artifacts');
    expect(metadata.image).toBe(RESOLVED_IMAGE);
    expect(metadata.requestedImage).toBe('archon-runner:test');
    expect(metadata.resourceLimits).toEqual({
      policyVersion: 1,
      agentCpus: '2',
      controllerHelperCpus: '1',
      memoryMb: 4096,
      pidsLimit: 512,
    });
  });

  test.skipIf(process.env.ARCHON_RUN_DOCKER_CONTAINER_TEST !== '1')(
    'real Docker prepare exposes writable artifact volume and snapshots it after stop',
    async () => {
      const root = await mkdtemp(join(tmpdir(), 'archon-container-backend-it-'));
      const seed = join(root, 'seed');
      const snapshotDir = join(root, 'snapshot');
      await mkdir(seed, { recursive: true });
      await writeFile(join(seed, 'README.md'), 'seed-ok');
      const store = fakeStore();
      const ownerRunId = `run-${randomUUID()}`;
      const backend = new ContainerBackend({
        store,
        config: {
          ...CONFIG,
          image: process.env.ARCHON_CONTAINER_TEST_IMAGE ?? 'archon-runner:hardened-test',
        },
      });
      let envId: string | undefined;
      try {
        const prepared = await backend.prepare({
          codebase: { ...FOLDER, defaultCwd: '/workspace' },
          seed: { kind: 'directory', path: seed },
          ownerRunId,
        });
        envId = prepared.envId;
        expect(prepared.execContext.agentArtifactsDir).toBe('/archon-artifacts');
        expect(prepared.artifactSnapshot?.workspaceVolume).toMatch(/^archon-.+-artifacts$/);
        const { stdout: nanoCpus } = await dockerCli([
          'inspect',
          '-f',
          '{{.HostConfig.NanoCpus}}',
          prepared.execContext.containerId,
        ]);
        expect(nanoCpus.trim()).toBe('2000000000');
        const { stdout: ownerContainerLabel } = await dockerCli([
          'inspect',
          '-f',
          '{{ index .Config.Labels "diy.archon.owner-run-id" }}',
          prepared.execContext.containerId,
        ]);
        expect(ownerContainerLabel.trim()).toBe(ownerRunId);
        const metadata = store.created?.metadata as {
          workspaceVolume: string;
          homeVolume: string;
          artifactsVolume: string;
          ownerRunId: string;
        };
        expect(metadata.ownerRunId).toBe(ownerRunId);
        for (const volume of [
          metadata.workspaceVolume,
          metadata.homeVolume,
          metadata.artifactsVolume,
        ]) {
          const { stdout } = await dockerCli([
            'volume',
            'inspect',
            '-f',
            '{{ index .Labels "diy.archon.owner-run-id" }}',
            volume,
          ]);
          expect(stdout.trim()).toBe(ownerRunId);
        }

        await dockerCli([
          'exec',
          '-u',
          'archon',
          prepared.execContext.containerId,
          'sh',
          '-c',
          'printf artifact-canary > /archon-artifacts/run/canary.txt',
        ]);

        const result = await backend.snapshotArtifacts(envId as string, snapshotDir);

        expect(await readFile(join(snapshotDir, 'run/canary.txt'), 'utf8')).toBe('artifact-canary');
        expect(result.files[0]?.path).toBe('run/canary.txt');
      } finally {
        if (envId) await backend.destroy(envId).catch(() => undefined);
        await rm(root, { recursive: true, force: true });
      }
    },
    90_000
  );

  test('fails closed when no controller-prepared seed directory is supplied', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.prepare({ codebase: FOLDER })).rejects.toThrow(
      /requires a controller-prepared committed-input seed directory/
    );
    expect(docker.calls).toHaveLength(0);
  });

  test('rejects strict egress without a controller-seeded proxy budget before Docker side effects', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: { ...CONFIG, egressPolicy: STRICT_EGRESS_POLICY },
      dockerRunner: docker,
    });

    await expect(backend.prepare({ codebase: FOLDER, seed: SEED })).rejects.toThrow(
      /controller-seeded proxy budget/
    );
    expect(docker.calls.filter(c => c[0] === 'volume')).toHaveLength(0);
  });

  test('rejects a proxy budget seed whose digest does not bind the frozen image and egress policy', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const badGrant = { ...PROXY_BUDGET_GRANT, policyDigest: 'sha256:drifted' };
    const backend = new ContainerBackend({
      store,
      config: strictBudgetConfig({}, badGrant),
      dockerRunner: docker,
    });

    await expect(backend.prepare({ codebase: FOLDER, seed: SEED })).rejects.toThrow(
      /budget seed does not match/
    );
    expect(docker.calls.filter(c => c[0] === 'volume')).toHaveLength(0);
  });

  test('labels every created Docker resource with the controller owner run id', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: {
        ...CONFIG,
        ...strictBudgetConfig(),
      },
      dockerRunner: docker,
    });

    await backend.prepare({ codebase: FOLDER, seed: SEED, ownerRunId: 'run-owner-123' });

    const ownerLabel = 'diy.archon.owner-run-id=run-owner-123';
    const volumeCreates = docker.calls.filter(c => c[0] === 'volume' && c[1] === 'create');
    expect(volumeCreates).toHaveLength(6);
    expect(volumeCreates.every(c => c.includes(ownerLabel))).toBe(true);

    const containerCreates = docker.calls.filter(c => c[0] === 'create' || c[0] === 'run');
    expect(containerCreates).toHaveLength(6);
    expect(containerCreates.every(c => c.includes(ownerLabel))).toBe(true);
    expect(store.created?.metadata).toMatchObject({ ownerRunId: 'run-owner-123' });
  });

  test('rejects an unsafe owner run id before Docker side effects', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(
      backend.prepare({ codebase: FOLDER, seed: SEED, ownerRunId: 'run id with spaces' })
    ).rejects.toThrow(/Invalid ownerRunId/);
    expect(docker.calls).toHaveLength(0);
  });

  test('allows controller-generated Git metadata only when the seed contract says so', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await backend.prepare({
      codebase: FOLDER,
      seed: { ...SEED, allowGitMetadata: true },
    });

    const scrubCall = docker.calls.find(
      c => c[0] === 'exec' && c.some(part => part.includes('seed contains forbidden credential'))
    );
    const scrubScript = scrubCall?.at(-1) ?? '';
    expect(scrubScript).not.toContain("-name '.git'");
    expect(scrubScript).toContain("-name '.env*'");
    expect(scrubScript).toContain("-path '*/.config/gh'");
    expect(scrubScript).toContain('chown -R root:root');
    expect(scrubScript).toContain('chmod -R a-w,a+rX');
  });

  test('cleans both volumes when dirty seed validation rejects nested credentials', async () => {
    const store = fakeStore();
    const docker = fakeDocker(args => {
      if (args[0] === 'exec' && args.at(-1)?.includes('seed contains forbidden credential')) {
        throw new Error('seed contains forbidden credential or VCS path');
      }
      if (args[0] === 'image' && args[1] === 'inspect')
        return { stdout: `${RESOLVED_IMAGE}\n`, stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.prepare({ codebase: FOLDER, seed: SEED })).rejects.toThrow(
      /seed contains forbidden credential or VCS path/
    );

    expect(docker.calls.filter(c => c[0] === 'volume' && c[1] === 'rm').length).toBe(3);
    expect(store.created).toBeUndefined();
  });

  test('cleans both volumes if container start fails', async () => {
    const store = fakeStore();
    const docker = fakeDocker(args => {
      if (args[0] === 'run') throw new Error('daemon refused run');
      if (args[0] === 'image' && args[1] === 'inspect')
        return { stdout: `${RESOLVED_IMAGE}\n`, stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.prepare({ codebase: FOLDER, seed: SEED })).rejects.toThrow(
      /daemon refused run/
    );

    expect(docker.calls.filter(c => c[0] === 'volume' && c[1] === 'rm').length).toBe(3);
    expect(store.created).toBeUndefined();
  });
});

describe('ContainerBackend.destroy', () => {
  test('removes container plus workspace and home volumes before marking destroyed', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });
    docker.calls.length = 0;

    await backend.destroy(prepared.envId as string);

    expect(docker.calls.find(c => c[0] === 'rm' && c[1] === '-f')).toBeDefined();
    expect(docker.calls.filter(c => c[0] === 'volume' && c[1] === 'rm').length).toBe(3);
    expect(store.rows.get(prepared.envId as string)?.status).toBe('destroyed');
  });

  test('throws and leaves row active on real docker cleanup failure', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });
    docker.calls.length = 0;
    docker.calls.push = Array.prototype.push;
    const failingDocker = fakeDocker(args => {
      if (args[0] === 'rm') {
        const err = new Error('rm failed') as Error & { stderr?: string };
        err.stderr = 'Cannot connect to the Docker daemon';
        throw err;
      }
      if (args[0] === 'image' && args[1] === 'inspect')
        return { stdout: `${RESOLVED_IMAGE}\n`, stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const failingBackend = new ContainerBackend({
      store,
      config: CONFIG,
      dockerRunner: failingDocker,
    });

    await expect(failingBackend.destroy(prepared.envId as string)).rejects.toThrow(
      /Failed to remove the isolation container/
    );
    expect(store.rows.get(prepared.envId as string)?.status).toBe('active');
  });

  test('rejects tampered metadata before removing resources', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-destroy', {
      containerName: 'not-archon-owned',
    });
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.destroy(row.id)).rejects.toThrow(/hardened metadata is invalid/);
    expect(docker.calls).toHaveLength(0);
    expect(store.rows.get(row.id)?.status).toBe('active');
  });
});

describe('ContainerBackend.resumeEnv', () => {
  test('recreates a missing container from preserved hardened volumes', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-1');
    const docker = fakeDocker(args => {
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        const err = new Error('missing') as Error & { stderr?: string };
        err.stderr = 'No such container';
        throw err;
      }
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'run') return { stdout: 'new-container-id\n', stderr: '' };
      if (args[0] === 'image' && args[1] === 'inspect')
        return { stdout: `${RESOLVED_IMAGE}\n`, stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    const prepared = await backend.resumeEnv(row.id);

    expect(prepared.execContext).toEqual({
      kind: 'container',
      profile: 'hardened',
      containerId: 'new-container-id',
      execUser: 'archon',
      agentArtifactsDir: '/archon-artifacts',
    });
    const runArgs = docker.calls.find(c => c[0] === 'run') ?? [];
    expect(runArgs.join(' ')).toContain('archon-run-1-workspace:/tmp/ops-client');
    expect(runArgs.join(' ')).toContain('archon-run-1-artifacts:/archon-artifacts');
    expect(runArgs.join(' ')).toContain('--memory 4096m');
    expect(runArgs.join(' ')).toContain('--pids-limit 512');
    expect(runArgs.join(' ')).toContain('--cpus 2');
    expect(prepared.agentArtifactsDir).toBe('/archon-artifacts');
  });

  test('recreates with frozen resource limits, not changed backend config', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-limits', {
      resourceLimits: {
        policyVersion: 1,
        agentCpus: '2',
        controllerHelperCpus: '1',
        memoryMb: 2048,
        pidsLimit: 321,
      },
    });
    const docker = fakeDocker(args => {
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        const err = new Error('missing') as Error & { stderr?: string };
        err.stderr = 'No such container';
        throw err;
      }
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'run') return { stdout: 'new-container-id\n', stderr: '' };
      if (args[0] === 'image' && args[1] === 'inspect')
        return { stdout: `${RESOLVED_IMAGE}\n`, stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: { ...CONFIG, memoryMb: 8192, pidsLimit: 999 },
      dockerRunner: docker,
    });

    await backend.resumeEnv(row.id);

    const runArgs = docker.calls.find(c => c[0] === 'run') ?? [];
    const joined = runArgs.join(' ');
    expect(joined).toContain('--memory 2048m');
    expect(joined).toContain('--pids-limit 321');
    expect(joined).toContain('--cpus 2');
    expect(joined).not.toContain('--memory 8192m');
    expect(joined).not.toContain('--pids-limit 999');
  });

  test('rejects missing private authority metadata before Docker resume', async () => {
    for (const field of ['image', 'resourceId', 'profile', 'resourceLimits']) {
      const store = fakeStore();
      const metadata = hardenedMetadata('run-invalid');
      delete metadata[field];
      const row = await store.create({
        codebase_id: FOLDER.id,
        workflow_type: 'task',
        workflow_id: 'run-invalid',
        provider: 'container',
        working_path: FOLDER.defaultCwd,
        branch_name: '' as never,
        metadata,
      });
      const docker = okDocker();
      const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

      await expect(backend.resumeEnv(row.id)).rejects.toThrow(/hardened metadata is invalid/);
      expect(docker.calls).toHaveLength(0);
    }
  });

  test('rejects malformed frozen resource limits before Docker resume', async () => {
    for (const resourceLimits of [
      {
        policyVersion: 2,
        agentCpus: '2',
        controllerHelperCpus: '1',
        memoryMb: 4096,
        pidsLimit: 512,
      },
      {
        policyVersion: 1,
        agentCpus: '3',
        controllerHelperCpus: '1',
        memoryMb: 4096,
        pidsLimit: 512,
      },
      {
        policyVersion: 1,
        agentCpus: '2',
        controllerHelperCpus: '2',
        memoryMb: 4096,
        pidsLimit: 512,
      },
      { policyVersion: 1, agentCpus: '2', controllerHelperCpus: '1', memoryMb: 0, pidsLimit: 512 },
      { policyVersion: 1, agentCpus: '2', controllerHelperCpus: '1', memoryMb: 4096, pidsLimit: 0 },
    ]) {
      const store = fakeStore();
      const row = await createContainerRow(store, 'run-bad-limits', { resourceLimits });
      const docker = okDocker();
      const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

      await expect(backend.resumeEnv(row.id)).rejects.toThrow(/hardened metadata is invalid/);
      expect(docker.calls).toHaveLength(0);
    }
  });
});

describe('ContainerBackend artifact snapshots', () => {
  test('stops owned writers then snapshots the artifact volume to a fresh controller dir', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-artifacts');
    const docker = fakeDocker(args => {
      if (args[0] === 'stop') return { stdout: '', stderr: '' };
      if (
        args[0] === 'run' &&
        args.includes('archon-run-artifacts-artifacts:/snapshot-volume:ro')
      ) {
        const digest = new Bun.CryptoHasher('sha256').update('snapshot-ok').digest('hex');
        return {
          stdout: `${JSON.stringify({
            type: 'file',
            path: 'run/evidence.txt',
            mode: 0o600,
            size: 11,
            sha256: digest,
            contentBase64: Buffer.from('snapshot-ok').toString('base64'),
          })}\n`,
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
    const dest = join(tmpdir(), `archon-container-artifacts-${randomUUID()}`);

    const result = await backend.snapshotArtifacts(row.id, dest);

    expect(docker.calls[0]).toEqual(['stop', 'archon-run-artifacts']);
    const readerRun = docker.calls.find(
      c => c[0] === 'run' && c.includes('archon-run-artifacts-artifacts:/snapshot-volume:ro')
    );
    expect(readerRun?.join(' ')).toContain('--network none');
    expect(readerRun?.join(' ')).toContain('--cap-drop ALL');
    expect(await readFile(join(dest, 'run/evidence.txt'), 'utf8')).toBe('snapshot-ok');
    expect(result.files[0]?.path).toBe('run/evidence.txt');
    await import('fs/promises').then(fs => fs.rm(dest, { recursive: true, force: true }));
  });

  test('refuses to overwrite an existing controller snapshot directory', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-artifacts');
    const dest = join(tmpdir(), `archon-container-artifacts-existing-${randomUUID()}`);
    await mkdir(dest, { recursive: true });
    const docker = fakeDocker(() => ({ stdout: '', stderr: '' }));
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.snapshotArtifacts(row.id, dest)).rejects.toThrow(/already exists/);
    expect(docker.calls).toEqual([['stop', 'archon-run-artifacts']]);
  });

  test('rejects tampered snapshot metadata before stopping writers', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-artifacts', {
      artifactsVolume: 'attacker-volume',
    });
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
    const dest = join(tmpdir(), `archon-container-artifacts-tampered-${randomUUID()}`);

    await expect(backend.snapshotArtifacts(row.id, dest)).rejects.toThrow(
      /hardened metadata is invalid/
    );
    expect(docker.calls).toHaveLength(0);
  });
});

describe('ContainerBackend hardened write-back', () => {
  test('fails closed instead of applying volume contents to the live worktree', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });

    await expect(backend.finalize(prepared.envId as string)).rejects.toThrow(
      /write-back is not implemented/
    );
    await expect(backend.applyChanges(prepared.envId as string)).rejects.toThrow(/Refusing/);
  });
});

function assertStrictProxySidecarLaunched(docker: { calls: string[][] }): void {
  const volumeCreates = docker.calls.filter(c => c[0] === 'volume' && c[1] === 'create');
  expect(volumeCreates.length).toBe(6);
  const proxyRun = findStrictProxyRun(docker.calls);
  const agentRun = findAgentContainerRun(docker.calls);

  expect(proxyRun?.join(' ')).toContain('--network bridge');
  expect(proxyRun?.join(' ')).toContain('--cap-drop ALL');
  expect(proxyRun?.join(' ')).toContain('--cpus 1');
  expect(proxyRun).not.toContain('-p');
  expect(proxyRun).not.toContain('--publish');
  expect(proxyRun?.join(' ')).toContain('ARCHON_EGRESS_POLICY_B64=');
  expect(proxyRun?.join(' ')).toContain('ARCHON_PROXY_IMAGE_ID=');
  expect(proxyRun?.join(' ')).toContain('ARCHON_PROXY_BUDGET_POLICY_DIGEST=');
  expect(proxyRun?.join(' ')).toContain(':/archon-budget');

  expect(agentRun?.join(' ')).toContain('--network none');
  expect(agentRun?.join(' ')).toContain('--cpus 2');
  expect(agentRun?.join(' ')).toContain(':/archon-egress:ro');
  expect(agentRun?.join(' ')).toContain('HTTP_PROXY=http://127.0.0.1:18080');
  expect(agentRun?.join(' ')).not.toContain('--network bridge');
}

function assertStrictEgressMaterialStaged(docker: { calls: string[][] }): void {
  const egressInit = docker.calls.find(
    c =>
      c[0] === 'create' &&
      c.some(part => part.includes(':/archon-egress')) &&
      !c.includes('/usr/local/lib/archon/egress/strict-https-proxy-cli.mjs')
  );
  expect(egressInit?.join(' ')).toContain('--cpus 1');
  expect(egressInit?.join(' ')).toContain('--read-only');
  expect(egressInit?.join(' ')).toContain('--memory 128m');
  expect(egressInit?.join(' ')).toContain(':/archon-budget');
  expect(egressInit?.filter(value => value === '--cap-add').length).toBe(1);
  expect(egressInit).toContain('CHOWN');
  const stagingScript =
    docker.calls
      .find(
        call =>
          call[0] === 'exec' &&
          call.some(value => value.includes('chmod 0400 /archon-proxy-private/policy.json'))
      )
      ?.at(-1) ?? '';
  const rootOwnership = stagingScript.indexOf('chown root:root /archon-proxy-private/policy.json');
  const filePermissions = stagingScript.indexOf('chmod 0400 /archon-proxy-private/policy.json');
  const agentOwnership = stagingScript.indexOf(
    'chown archon:archon /archon-proxy-private/policy.json'
  );
  expect(rootOwnership).toBeGreaterThanOrEqual(0);
  expect(filePermissions).toBeGreaterThan(rootOwnership);
  expect(agentOwnership).toBeGreaterThan(filePermissions);
  expect(stagingScript).toContain('/archon-proxy-private/budget.json');
  expect(stagingScript).toContain('/archon-proxy-private/provider-policies.json');
  const budgetRootOwnership = stagingScript.indexOf('chown root:root /archon-budget');
  const budgetPermissions = stagingScript.indexOf('chmod 0700 /archon-budget');
  const budgetAgentOwnership = stagingScript.indexOf('chown archon:archon /archon-budget');
  expect(budgetRootOwnership).toBeGreaterThanOrEqual(0);
  expect(budgetPermissions).toBeGreaterThan(budgetRootOwnership);
  expect(budgetAgentOwnership).toBeGreaterThan(budgetPermissions);
}

describe('ContainerBackend restricted egress', () => {
  test('projects provider-specific sealed API origins from proxy budget policies', async () => {
    const providerPolicies: TrustedProviderBudgetPolicy[] = [
      {
        provider: 'openai',
        host: 'api.openai.example',
        model: 'gpt-test',
        maxInputTokens: 100,
        maxOutputTokens: 50,
      },
      {
        provider: 'anthropic',
        host: 'api.anthropic.example',
        model: 'claude-test',
        maxInputTokens: 100,
        maxOutputTokens: 50,
      },
    ];
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: {
        ...CONFIG,
        ...strictBudgetConfig({}, undefined, providerPolicies),
      },
      dockerRunner: docker,
    });

    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });

    expect(prepared.execContext).toMatchObject({
      kind: 'container',
      providerOrigins: [
        { provider: 'openai', baseUrl: 'https://api.openai.example/v1' },
        { provider: 'anthropic', baseUrl: 'https://api.anthropic.example' },
      ],
    });
  });

  test('deduplicates same-provider same-host origins without collapsing model policies', async () => {
    const providerPolicies: TrustedProviderBudgetPolicy[] = [
      {
        provider: 'openai',
        host: 'api.openai.example',
        model: 'gpt-test',
        maxInputTokens: 100,
        maxOutputTokens: 50,
      },
      {
        provider: 'openai',
        host: 'api.openai.example',
        model: 'gpt-other',
        maxInputTokens: 200,
        maxOutputTokens: 75,
      },
    ];
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: {
        ...CONFIG,
        ...strictBudgetConfig({}, undefined, providerPolicies),
      },
      dockerRunner: docker,
    });

    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });

    expect(prepared.execContext.providerOrigins).toEqual([
      { provider: 'openai', baseUrl: 'https://api.openai.example/v1' },
    ]);
    const created = store.created;
    if (!created) throw new Error('expected container env row to be created');
    expect((created.metadata as Record<string, unknown>).proxyBudgetSeedDigest).toBe(
      digestProxyBudgetSeed({
        grant: {
          ...PROXY_BUDGET_GRANT,
          policyDigest: budgetDigestFor(RESOLVED_IMAGE, providerPolicies),
        },
        providerPolicies,
      })
    );
  });

  test('rejects same-provider budget policies with conflicting origins', async () => {
    const providerPolicies: TrustedProviderBudgetPolicy[] = [
      {
        provider: 'openai',
        host: 'api.openai.example',
        model: 'gpt-test',
        maxInputTokens: 100,
        maxOutputTokens: 50,
      },
      {
        provider: 'openai',
        host: 'api2.openai.example',
        model: 'gpt-other',
        maxInputTokens: 200,
        maxOutputTokens: 75,
      },
    ];
    const docker = okDocker();
    const backend = new ContainerBackend({
      store: fakeStore(),
      config: {
        ...CONFIG,
        ...strictBudgetConfig({}, undefined, providerPolicies),
      },
      dockerRunner: docker,
    });

    await expect(backend.prepare({ codebase: FOLDER, seed: SEED })).rejects.toThrow(
      /Conflicting provider origins/
    );
    expect(docker.calls.some(call => call[0] === 'run')).toBe(false);
  });

  test('starts a controller-pinned proxy sidecar while the agent remains network none', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: {
        ...CONFIG,
        ...strictBudgetConfig(),
      },
      dockerRunner: docker,
    });

    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });

    expect(prepared.execContext).toMatchObject({
      kind: 'container',
      providerOrigins: [{ provider: 'openai', baseUrl: 'https://registry.npmjs.org/v1' }],
    });

    assertStrictProxySidecarLaunched(docker);
    assertStrictEgressMaterialStaged(docker);

    const metadata = store.created?.metadata as Record<string, unknown>;
    expect(typeof metadata.egressVolume).toBe('string');
    expect(typeof metadata.tlsVolume).toBe('string');
    expect(typeof metadata.tlsValidUntil).toBe('string');
    expect(typeof metadata.proxyContainerName).toBe('string');
    expect(typeof metadata.budgetVolume).toBe('string');
    expect(metadata.budgetPolicyDigest).toBe(FROZEN_BUDGET_DIGEST);
    expect(metadata.proxyBudgetSeedDigest).toBe(digestProxyBudgetSeed(PROXY_BUDGET_GRANT_SEED));
  });

  test('destroys proxy sidecar and egress volume with the owned environment', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: {
        ...CONFIG,
        ...strictBudgetConfig(),
      },
      dockerRunner: docker,
    });
    const prepared = await backend.prepare({ codebase: FOLDER, seed: SEED });
    docker.calls.length = 0;

    await backend.destroy(prepared.envId as string);

    expect(docker.calls.filter(c => c[0] === 'rm' && c[1] === '-f').length).toBe(2);
    expect(docker.calls.filter(c => c[0] === 'volume' && c[1] === 'rm').length).toBe(6);
  });
});

describe('ContainerBackend egress lifecycle', () => {
  test('reads verified proxy budget status from the running proxy after binding checks', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('status-run');
    const row = await createContainerRow(store, 'status-run', {
      ...metadata,
      ownerRunId: 'run-owner',
    });
    const docker = fakeDocker(args => {
      if (args[0] === 'volume' && args[1] === 'inspect' && args.includes('--format')) {
        return { stdout: 'run-owner\n', stderr: '' };
      }
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        return { stdout: 'true\n', stderr: '' };
      }
      if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
        return { stdout: proxyContainerInspectOutput(metadata), stderr: '' };
      }
      if (args[0] === 'inspect' && args.includes('--format')) {
        return { stdout: `run-owner\n${METADATA_IMAGE}\n`, stderr: '' };
      }
      if (args[0] === 'exec' && args.includes('--status')) {
        return { stdout: budgetStatusJson(), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: strictBudgetConfig({ image: METADATA_IMAGE }, METADATA_PROXY_BUDGET_GRANT),
      dockerRunner: docker,
    });

    const status = await backend.readProxyBudgetStatus(row.id, egressBinding('status-run'));

    expect(status).toEqual({
      source: 'controller-proxy-ledger',
      envId: row.id,
      grant: METADATA_PROXY_BUDGET_GRANT,
      consumed: { input: 20, output: 10 },
      pendingReservations: 0,
      unknownReservations: 0,
      acceptingReservations: true,
    });
    expect(docker.calls.some(call => call[0] === 'run' && call.includes('--status'))).toBe(false);
    expect(docker.calls.some(call => call[0] === 'exec' && call.includes('--status'))).toBe(true);
  });

  test('refuses shared-chain proxy budget status before v2 projection is wired', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('status-v2-refusal');
    const row = await createContainerRow(store, 'status-v2-refusal', {
      ...metadata,
      ownerRunId: 'run-owner',
    });
    const v2Grant = {
      schema: 'archon.proxy-budget-grant.v2',
      rootChainId: 'root-run',
      deadlineEpochMs: Date.now() + 3_600_000,
      inputTokenLimit: 1_000,
      outputTokenLimit: 500,
      totalTokenLimit: 1_200,
      workflowBindings: [
        {
          runId: 'root-run',
          workflowDigest: 'sha256:workflow-root',
          policyDigest: METADATA_PROXY_BUDGET_GRANT.policyDigest,
        },
      ],
    } as ProxyBudgetGrant;
    const docker = fakeDocker(args => {
      if (args[0] === 'volume' && args[1] === 'inspect' && args.includes('--format')) {
        return { stdout: 'run-owner\n', stderr: '' };
      }
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        return { stdout: 'true\n', stderr: '' };
      }
      if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
        return { stdout: proxyContainerInspectOutput(metadata), stderr: '' };
      }
      if (args[0] === 'inspect' && args.includes('--format')) {
        return { stdout: `run-owner\n${METADATA_IMAGE}\n`, stderr: '' };
      }
      if (args[0] === 'exec' && args.includes('--status')) {
        return { stdout: budgetStatusJson({ grant: v2Grant }), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: strictBudgetConfig({ image: METADATA_IMAGE }, v2Grant),
      dockerRunner: docker,
    });

    await expect(
      backend.readProxyBudgetStatus(row.id, egressBinding('status-v2-refusal'))
    ).rejects.toThrow('Proxy budget shared-chain status is not wired.');
  });

  test('rejects a running proxy whose container env or mounts drifted before status exec', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('status-running-drift');
    const row = await createContainerRow(store, 'status-running-drift', {
      ...metadata,
      ownerRunId: 'run-owner',
    });
    const docker = fakeDocker(args => {
      if (args[0] === 'volume' && args[1] === 'inspect' && args.includes('--format')) {
        return { stdout: 'run-owner\n', stderr: '' };
      }
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        return { stdout: 'true\n', stderr: '' };
      }
      if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
        return {
          stdout: proxyContainerInspectOutput(metadata, {
            env: ['ARCHON_EGRESS_SOCKET=/archon-egress/proxy.sock'],
          }),
          stderr: '',
        };
      }
      if (args[0] === 'inspect' && args.includes('--format')) {
        return {
          stdout: `run-owner\n${METADATA_IMAGE}\n`,
          stderr: '',
        };
      }
      if (args[0] === 'exec' && args.includes('--status')) {
        throw new Error('status exec must not run after proxy binding drift');
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: strictBudgetConfig({ image: METADATA_IMAGE }, METADATA_PROXY_BUDGET_GRANT),
      dockerRunner: docker,
    });

    await expect(
      backend.readProxyBudgetStatus(row.id, egressBinding('status-running-drift'))
    ).rejects.toThrow(/proxy container binding drifted/);
    expect(docker.calls.some(call => call[0] === 'exec' && call.includes('--status'))).toBe(false);
  });

  test('reads verified proxy budget status with a bounded helper when the proxy is stopped', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('status-paused');
    const row = await createContainerRow(store, 'status-paused', {
      ...metadata,
      ownerRunId: 'run-owner',
    });
    const docker = fakeDocker(args => {
      if (args[0] === 'volume' && args[1] === 'inspect' && args.includes('--format')) {
        return { stdout: 'run-owner\n', stderr: '' };
      }
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        return { stdout: 'false\n', stderr: '' };
      }
      if (args[0] === 'inspect' && args.includes('--format')) {
        return { stdout: `run-owner\n${METADATA_IMAGE}\n`, stderr: '' };
      }
      if (args[0] === 'run' && args.includes('--status')) {
        return { stdout: budgetStatusJson(), stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: strictBudgetConfig({ image: METADATA_IMAGE }, METADATA_PROXY_BUDGET_GRANT),
      dockerRunner: docker,
    });

    await backend.readProxyBudgetStatus(row.id, egressBinding('status-paused'));

    const helperRun = docker.calls.find(call => call[0] === 'run' && call.includes('--status'));
    expect(helperRun?.join(' ')).toContain('--network none');
    expect(helperRun?.join(' ')).toContain('--read-only');
    expect(helperRun?.join(' ')).toContain('--cap-drop ALL');
    expect(helperRun?.join(' ')).toContain('diy.archon.owner-run-id=run-owner');
    expect(helperRun?.join(' ')).toContain(':/archon-proxy-private:ro');
    expect(helperRun?.join(' ')).toContain(':/archon-budget');
  });

  test('rejects wrong proxy budget status binding before Docker effects', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('status-drift');
    const row = await createContainerRow(store, 'status-drift', {
      ...metadata,
      ownerRunId: 'run-owner',
    });
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: strictBudgetConfig({ image: METADATA_IMAGE }, METADATA_PROXY_BUDGET_GRANT),
      dockerRunner: docker,
    });

    await expect(
      backend.readProxyBudgetStatus(row.id, {
        ...egressBinding('status-drift'),
        proxyBudgetSeedDigest: 'b'.repeat(64),
      })
    ).rejects.toThrow(/controller authority/);
    expect(docker.calls).toHaveLength(0);
  });

  test('fails closed when proxy budget status has pending or unknown reservations', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('status-pending');
    const row = await createContainerRow(store, 'status-pending', {
      ...metadata,
      ownerRunId: 'run-owner',
    });
    const docker = fakeDocker(args => {
      if (args[0] === 'volume' && args[1] === 'inspect' && args.includes('--format')) {
        return { stdout: 'run-owner\n', stderr: '' };
      }
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        return { stdout: 'true\n', stderr: '' };
      }
      if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
        return { stdout: proxyContainerInspectOutput(metadata), stderr: '' };
      }
      if (args[0] === 'inspect' && args.includes('--format')) {
        return { stdout: `run-owner\n${METADATA_IMAGE}\n`, stderr: '' };
      }
      if (args[0] === 'exec' && args.includes('--status')) {
        return {
          stdout: budgetStatusJson({ pendingReservations: 1, acceptingReservations: false }),
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: strictBudgetConfig({ image: METADATA_IMAGE }, METADATA_PROXY_BUDGET_GRANT),
      dockerRunner: docker,
    });

    await expect(
      backend.readProxyBudgetStatus(row.id, egressBinding('status-pending'))
    ).rejects.toThrow(/pending or unknown/);
  });

  test('checks controller authority against its own captured metadata before any Docker effect', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'bound-resume', egressMetadata('bound-resume'));
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
    await expect(
      backend.resumeEnv(row.id, {
        egressPolicyB64: undefined,
        image: RESOLVED_IMAGE,
        ownerRunId: 'run-bound',
        proxyBudgetSeedDigest: egressMetadata('bound-resume').proxyBudgetSeedDigest as string,
      })
    ).rejects.toThrow(/controller authority/);
    expect(docker.calls).toHaveLength(0);
  });

  test('rejects proxy budget seed digest drift before any Docker resume effect', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('seed-drift');
    const row = await createContainerRow(store, 'seed-drift', {
      ...metadata,
      ownerRunId: 'run-bound',
      proxyBudgetSeedDigest: 'a'.repeat(64),
    });
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(
      backend.resumeEnv(row.id, {
        egressPolicyB64: metadata.egressPolicyB64 as string,
        image: METADATA_IMAGE,
        ownerRunId: 'run-bound',
        proxyBudgetSeedDigest: metadata.proxyBudgetSeedDigest as string,
      })
    ).rejects.toThrow(/controller authority/);
    expect(docker.calls).toHaveLength(0);
  });
  test('does not trust a substituted image or forged owner field in isolation metadata', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'owner-forgery', { ownerRunId: 'run-bound' });
    const docker = fakeDocker(() => ({ stdout: 'sibling-run\n', stderr: '' }));
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
    await expect(
      backend.resumeEnv(row.id, {
        image: RESOLVED_IMAGE,
        ownerRunId: 'run-bound',
        egressPolicyB64: undefined,
      })
    ).rejects.toThrow(/controller authority/);
    expect(docker.calls).toHaveLength(0);
    await expect(
      backend.resumeEnv(row.id, {
        image: METADATA_IMAGE,
        ownerRunId: 'run-bound',
        egressPolicyB64: undefined,
      })
    ).rejects.toThrow(/volume ownership/);
    expect(docker.calls).toHaveLength(1);
    expect(docker.calls[0]?.slice(0, 2)).toEqual(['volume', 'inspect']);
  });
  test('rejects tampered metadata before stopping writers', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-suspend', { profile: 'legacy' });
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.suspend(row.id)).rejects.toThrow(/hardened metadata is invalid/);
    expect(docker.calls).toHaveLength(0);
  });

  test('suspends both agent and proxy containers', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-egress', egressMetadata('run-egress'));
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await backend.suspend(row.id);

    expect(docker.calls.filter(c => c[0] === 'stop')).toEqual([
      ['stop', 'archon-run-egress'],
      ['stop', 'archon-run-egress-egress-proxy'],
    ]);
  });

  test('still stops the proxy when the agent disappeared or cannot be stopped', async () => {
    for (const failure of ['No such container: archon-run-stop', 'daemon stop refused']) {
      const store = fakeStore();
      const row = await createContainerRow(store, 'run-stop', egressMetadata('run-stop'));
      const docker = fakeDocker(args => {
        if (args[0] === 'stop' && args[1] === 'archon-run-stop') throw new Error(failure);
        return { stdout: '', stderr: '' };
      });
      const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });
      if (failure.startsWith('No such')) await backend.suspend(row.id);
      else await expect(backend.suspend(row.id)).rejects.toThrow(/daemon stop refused/);
      expect(docker.calls).toEqual([
        ['stop', 'archon-run-stop'],
        ['stop', 'archon-run-stop-egress-proxy'],
      ]);
    }
  });

  test('allows cleanup lifecycle to stop expired strict egress containers', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-expired-egress', {
      ...egressMetadata('run-expired-egress'),
      tlsValidUntil: '2000-01-01T00:00:00.000Z',
    });
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await backend.suspend(row.id);

    expect(docker.calls.filter(c => c[0] === 'stop')).toHaveLength(2);
  });

  test('restarts a stopped proxy before resuming a stopped agent', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-egress', egressMetadata('run-egress'));
    const docker = fakeDocker(args => {
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}'))
        return { stdout: 'false\n', stderr: '' };
      if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
        return {
          stdout: proxyInspectOutput('archon-run-egress-egress-tls', 'archon-run-egress-egress'),
          stderr: '',
        };
      }
      if (args[0] === 'inspect' && args.includes('{{.Id}}'))
        return { stdout: 'resumed-id\n', stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: { ...CONFIG, egressPolicy: STRICT_EGRESS_POLICY },
      dockerRunner: docker,
    });

    await backend.resumeEnv(row.id);

    expect(docker.calls.filter(c => c[0] === 'start')).toEqual([
      ['start', 'archon-run-egress-egress-proxy'],
      ['start', 'archon-run-egress'],
    ]);
  });

  test('recreates a missing proxy with the frozen metadata policy, not current config', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-egress', egressMetadata('run-egress'));
    const docker = fakeDocker(args => {
      if (args[0] === 'inspect' && args.includes('archon-run-egress-egress-proxy')) {
        const err = new Error('missing proxy') as Error & { stderr?: string };
        err.stderr = 'No such container';
        throw err;
      }
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}'))
        return { stdout: 'false\n', stderr: '' };
      if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
        return {
          stdout: proxyInspectOutput('archon-run-egress-egress-tls', 'archon-run-egress-egress'),
          stderr: '',
        };
      }
      if (args[0] === 'inspect' && args.includes('{{.Id}}'))
        return { stdout: 'resumed-id\n', stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({
      store,
      config: {
        ...CONFIG,
        egressPolicy: {
          targets: [{ host: 'current-config.example', port: 443 }],
          httpGrants: [
            {
              host: 'current-config.example',
              port: 443,
              methods: ['GET'],
              pathPrefixes: ['/'],
            },
          ],
        },
      },
      dockerRunner: docker,
    });

    await backend.resumeEnv(row.id);

    const proxyRun = findStrictProxyRun(docker.calls);
    expect(proxyRun?.join(' ')).toContain('--cpus 1');
    expect(proxyRun?.join(' ')).toContain('ARCHON_EGRESS_POLICY_B64=');
    expect(proxyRun?.join(' ')).not.toContain('current-config.example');
  });

  test('recreates missing agent and proxy with preserved budget digest and owner label', async () => {
    const store = fakeStore();
    const metadata = egressMetadata('run-egress');
    const row = await createContainerRow(store, 'run-egress', {
      ...metadata,
      ownerRunId: 'run-owner',
    });
    const docker = fakeDocker(args => {
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}')) {
        const err = new Error('missing') as Error & { stderr?: string };
        err.stderr = 'No such container';
        throw err;
      }
      if (args[0] === 'volume' && args[1] === 'inspect') {
        if (args.includes('--format')) return { stdout: 'run-owner\n', stderr: '' };
        return { stdout: '[]', stderr: '' };
      }
      if (args[0] === 'inspect' && args.includes('--format'))
        return { stdout: 'run-owner\n', stderr: '' };
      if (args[0] === 'exec') return { stdout: '', stderr: '' };
      if (
        args[0] === 'run' &&
        args.includes('/usr/local/lib/archon/egress/strict-https-proxy-cli.mjs')
      ) {
        return { stdout: 'proxy-container-id\n', stderr: '' };
      }
      if (args[0] === 'run') return { stdout: 'new-agent-container-id\n', stderr: '' };
      if (args[0] === 'inspect' && args.includes('{{.Id}}')) {
        return { stdout: 'new-agent-container-id\n', stderr: '' };
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    const prepared = await backend.resumeEnv(row.id, {
      egressPolicyB64: metadata.egressPolicyB64 as string,
      image: METADATA_IMAGE,
      ownerRunId: 'run-owner',
      proxyBudgetSeedDigest: metadata.proxyBudgetSeedDigest as string,
    });

    expect(prepared.execContext).toEqual({
      kind: 'container',
      profile: 'hardened',
      containerId: 'new-agent-container-id',
      execUser: 'archon',
      agentArtifactsDir: '/archon-artifacts',
    });

    const proxyRun = findStrictProxyRun(docker.calls) ?? [];
    const proxyRunText = proxyRun.join(' ');
    expect(proxyRunText).toContain('diy.archon.owner-run-id=run-owner');
    expect(proxyRunText).toContain(
      `ARCHON_PROXY_BUDGET_POLICY_DIGEST=${budgetDigestFor(METADATA_IMAGE)}`
    );
    expect(proxyRunText).not.toContain(
      `diy.archon.owner-run-id=${budgetDigestFor(METADATA_IMAGE)}`
    );
  });

  test('refuses resume activation when strict TLS material is expired', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-expired-egress', {
      ...egressMetadata('run-expired-egress'),
      tlsValidUntil: '2000-01-01T00:00:00.000Z',
    });
    const docker = okDocker();
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.resumeEnv(row.id)).rejects.toThrow(/TLS material is expired/);
    expect(docker.calls.some(c => c[0] === 'start')).toBe(false);
  });

  test('refuses resume activation when strict egress volumes are missing', async () => {
    const store = fakeStore();
    const row = await createContainerRow(
      store,
      'run-missing-egress',
      egressMetadata('run-missing-egress')
    );
    const docker = fakeDocker(args => {
      if (
        args[0] === 'volume' &&
        args[1] === 'inspect' &&
        args[2] === 'archon-run-missing-egress-egress-tls'
      ) {
        const err = new Error('missing TLS volume') as Error & { stderr?: string };
        err.stderr = 'No such volume';
        throw err;
      }
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}'))
        return { stdout: 'true\n', stderr: '' };
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.resumeEnv(row.id)).rejects.toThrow(/volume is missing/);
    expect(docker.calls.some(c => c[0] === 'start' || c[0] === 'run')).toBe(false);
  });

  test('refuses to reuse a proxy container whose frozen env binding drifted', async () => {
    const store = fakeStore();
    const row = await createContainerRow(store, 'run-egress', egressMetadata('run-egress'));
    const docker = fakeDocker(args => {
      if (args[0] === 'inspect' && args.includes('{{.State.Running}}'))
        return { stdout: 'true\n', stderr: '' };
      if (args[0] === 'volume' && args[1] === 'inspect') return { stdout: '[]', stderr: '' };
      if (args[0] === 'inspect' && args.some(part => part.includes('{{json .Config.Env}}'))) {
        return {
          stdout: proxyInspectOutput(
            'archon-run-egress-egress-tls',
            'archon-run-egress-egress'
          ).replace(FROZEN_STRICT_POLICY, 'drifted-policy'),
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });
    const backend = new ContainerBackend({ store, config: CONFIG, dockerRunner: docker });

    await expect(backend.resumeEnv(row.id)).rejects.toThrow(/binding drifted/);
    expect(docker.calls.some(c => c[0] === 'start' || c[0] === 'run')).toBe(false);
  });
});

describe('ContainerBackend image freeze', () => {
  test('uses the resolved immutable image id for seed, proxy, and agent containers', async () => {
    const store = fakeStore();
    const docker = okDocker();
    const backend = new ContainerBackend({
      store,
      config: {
        ...CONFIG,
        ...strictBudgetConfig(),
      },
      dockerRunner: docker,
    });

    await backend.prepare({ codebase: FOLDER, seed: SEED });

    const createAndRun = docker.calls.filter(c => c[0] === 'create' || c[0] === 'run');
    expect(createAndRun.length).toBe(6);
    expect(createAndRun.every(c => c.includes(RESOLVED_IMAGE))).toBe(true);
    expect(createAndRun.every(c => !c.includes('archon-runner:test'))).toBe(true);
  });
});
