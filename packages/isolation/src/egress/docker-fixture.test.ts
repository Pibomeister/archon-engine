import { afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { ContainerBackend } from '../backends/container';
import { dockerCli } from '../container/docker-exec';
import type { IIsolationStore } from '../store';
import type { CreateEnvironmentParams, IsolationEnvironmentRow } from '../types';

const IMAGE = process.env.ARCHON_EGRESS_TEST_IMAGE ?? 'archon-runner:hardened-test';
const RUN_DOCKER_FIXTURE = process.env.ARCHON_RUN_DOCKER_FIXTURE === '1';
const docker = dockerCli;
const ownedEnvIds: string[] = [];
const ownedSeeds: string[] = [];
let store: ReturnType<typeof fakeStore>;

describe.skipIf(!RUN_DOCKER_FIXTURE)('restricted egress Docker fixture', () => {
  beforeAll(async () => {
    await docker(['image', 'inspect', IMAGE]);
  });

  afterEach(async () => {
    const failures: unknown[] = [];
    for (const envId of ownedEnvIds.splice(0)) {
      const backend = new ContainerBackend({ store, config: config(), dockerRunner: docker });
      try {
        await backend.destroy(envId);
      } catch (error) {
        failures.push(error);
      }
    }
    for (const seed of ownedSeeds.splice(0)) await rm(seed, { recursive: true, force: true });
    if (failures.length) throw new AggregateError(failures, 'Owned egress fixture cleanup failed');
  });

  test('agent network stays none while strict proxy protects keys and enforces HTTP grants across resume', async () => {
    store = fakeStore();
    const seed = await createSeed();
    const backend = new ContainerBackend({ store, config: config(), dockerRunner: docker });
    const prepared = await backend.prepare({
      codebase: codebase(),
      seed: { kind: 'directory', path: seed },
    });
    ownedEnvIds.push(prepared.envId!);
    const containerId = store.rows.get(prepared.envId!)!.metadata.containerId as string;

    const credentialProbe = await docker([
      'exec',
      containerId,
      'bash',
      '-lc',
      "env | grep -E '^(OPENAI_API_KEY|ANTHROPIC_API_KEY|GH_TOKEN|GITHUB_TOKEN|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|CODEX_AUTH_TOKEN)='",
    ]).then(
      () => 'leaked',
      () => 'absent'
    );
    expect(credentialProbe).toBe('absent');

    const direct = await docker([
      'exec',
      containerId,
      'bash',
      '-lc',
      'HTTPS_PROXY= HTTP_PROXY= curl -fsS --max-time 5 https://registry.npmjs.org/',
    ]).then(
      () => 'unexpected-pass',
      () => 'blocked'
    );
    expect(direct).toBe('blocked');

    const denied = await docker([
      'exec',
      containerId,
      'bash',
      '-lc',
      'curl -fsS --max-time 10 https://example.com/',
    ]).then(
      () => 'unexpected-pass',
      () => 'blocked'
    );
    expect(denied).toBe('blocked');

    const deniedIpLiteral = await docker([
      'exec',
      containerId,
      'bash',
      '-lc',
      'curl -fsS --noproxy "" --proxy http://127.0.0.1:18080 --max-time 10 https://127.0.0.1/',
    ]).then(
      () => 'unexpected-pass',
      () => 'blocked'
    );
    expect(deniedIpLiteral).toBe('blocked');

    const deniedReservedIp = await docker([
      'exec',
      containerId,
      'bash',
      '-lc',
      'curl -fsS --noproxy "" --proxy http://127.0.0.1:18080 --max-time 10 https://192.0.2.1/',
    ]).then(
      () => 'unexpected-pass',
      () => 'blocked'
    );
    expect(deniedReservedIp).toBe('blocked');

    const malformedProxyRequest = await docker([
      'exec',
      containerId,
      'bash',
      '-lc',
      'curl -fsS --noproxy "" --proxy http://127.0.0.1:18080 --max-time 10 http://registry.npmjs.org/',
    ]).then(
      () => 'unexpected-pass',
      () => 'blocked'
    );
    expect(malformedProxyRequest).toBe('blocked');

    const approved = await docker([
      'exec',
      containerId,
      'bash',
      '-lc',
      'curl -fsS --max-time 15 https://registry.npmjs.org/-/ping',
    ]).then(result => result.stdout.trim());
    expect(approved).toBe('{}');

    await docker([
      'exec',
      containerId,
      'sh',
      '-c',
      'test -r /archon-egress/ca.crt && test ! -e /archon-proxy-private/leaf.key',
    ]);
    const forgedHost = await docker([
      'exec',
      containerId,
      'curl',
      '-sS',
      '-o',
      '/dev/null',
      '-w',
      '%{http_code}',
      '--max-time',
      '10',
      '-H',
      'Host: evil.example',
      'https://registry.npmjs.org/-/ping',
    ]);
    expect(forgedHost.stdout).toBe('421');
    const deniedWrite = await docker([
      'exec',
      containerId,
      'curl',
      '-sS',
      '-o',
      '/dev/null',
      '-w',
      '%{http_code}',
      '--max-time',
      '10',
      '-X',
      'POST',
      'https://registry.npmjs.org/-/ping',
    ]);
    expect(deniedWrite.stdout).toBe('403');

    await backend.suspend(prepared.envId!);
    const resumed = await backend.resumeEnv(prepared.envId!);
    if (resumed.execContext?.kind !== 'container') throw new Error('Expected resumed container');
    const afterResume = await docker([
      'exec',
      resumed.execContext.containerId,
      'curl',
      '-fsS',
      '--max-time',
      '15',
      'https://registry.npmjs.org/-/ping',
    ]);
    expect(afterResume.stdout.trim()).toBe('{}');

    await backend.suspend(prepared.envId!);
    const metadata = store.rows.get(prepared.envId!)!.metadata;
    const expiryProof = await probeCertificateExpiry(metadata);
    expect(expiryProof).toContain('archon-strict-https-proxy: ready');
    expect(expiryProof).toContain('EXPIRY_PROBE_CLOSED');
    await docker(['rm', '-f', String(metadata.containerName)]);
    await docker(['start', String(metadata.proxyContainerName)]);
    await backend.suspend(prepared.envId!);
    const proxyState = await docker([
      'inspect',
      '--format',
      '{{.State.Running}}',
      String(metadata.proxyContainerName),
    ]);
    expect(proxyState.stdout.trim()).toBe('false');
  }, 90_000);
});

async function probeCertificateExpiry(metadata: Record<string, unknown>): Promise<string> {
  const name = `${String(metadata.proxyContainerName)}-expiry-probe`;
  const script = [
    "const { createConnection } = await import('node:net');",
    'Date.now = () => Date.parse(process.argv[1]) - 1000;',
    "await import('/usr/local/lib/archon/egress/strict-https-proxy-cli.mjs');",
    "const client = createConnection({path:'/archon-egress/proxy.sock'});",
    "client.on('connect', () => client.write('CONNECT'));",
    "client.on('error', error => { console.error(error.message); process.exitCode=1; });",
    "client.on('close', () => console.log('EXPIRY_PROBE_CLOSED'));",
  ].join('\n');
  try {
    const { stdout } = await docker(
      [
        'run',
        '--rm',
        '--pull=never',
        '--name',
        name,
        '--label',
        `archon.test.gateway-expiry=${name}`,
        '--user',
        '1000:1000',
        '--read-only',
        '--cap-drop',
        'ALL',
        '--security-opt',
        'no-new-privileges',
        '--network',
        'none',
        '--memory',
        '128m',
        '--cpus',
        '1',
        '--pids-limit',
        '32',
        '-v',
        `${String(metadata.tlsVolume)}:/archon-proxy-private:ro`,
        '-v',
        `${String(metadata.egressVolume)}:/archon-egress`,
        '-e',
        'ARCHON_EGRESS_SOCKET=/archon-egress/proxy.sock',
        '-e',
        `ARCHON_EGRESS_POLICY_B64=${String(metadata.egressPolicyB64)}`,
        '--entrypoint',
        'node',
        String(metadata.image),
        '--input-type=module',
        '-e',
        script,
        String(metadata.tlsValidUntil),
      ],
      { timeout: 8000, maxBuffer: 32768 }
    );
    return stdout;
  } finally {
    const existing = await docker([
      'ps',
      '-aq',
      '--filter',
      `name=^/${name}$`,
      '--filter',
      `label=archon.test.gateway-expiry=${name}`,
    ]);
    if (existing.stdout.trim()) await docker(['rm', '-f', name]);
  }
}

function config() {
  return {
    profile: 'hardened' as const,
    image: IMAGE,
    network: 'none' as const,
    memoryMb: 512,
    pidsLimit: 128,
    egressPolicy: {
      targets: [{ host: 'registry.npmjs.org', port: 443 }],
      httpGrants: [{ host: 'registry.npmjs.org', port: 443, methods: ['GET'], paths: ['/-/ping'] }],
    },
  };
}

function codebase() {
  return {
    id: `egress-fixture-${Date.now()}`,
    defaultCwd: '/workspace/fixture',
    name: 'egress-fixture',
    kind: 'folder' as const,
  };
}

async function createSeed(): Promise<string> {
  const seed = await mkdtemp(join(tmpdir(), 'archon-egress-seed-'));
  ownedSeeds.push(seed);
  await writeFile(join(seed, 'README.md'), 'egress fixture\n');
  return seed;
}

function fakeStore(): IIsolationStore & { rows: Map<string, IsolationEnvironmentRow> } {
  const rows = new Map<string, IsolationEnvironmentRow>();
  let counter = 0;
  return {
    rows,
    async getById(id) {
      return rows.get(id) ?? null;
    },
    async findActiveByWorkflow() {
      return null;
    },
    async create(env: CreateEnvironmentParams) {
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
