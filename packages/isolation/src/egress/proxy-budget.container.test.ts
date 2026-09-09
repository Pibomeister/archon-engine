import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { execFileSync, spawnSync } from 'child_process';
import { createHash, randomUUID } from 'crypto';
import { readFileSync, writeFileSync } from 'fs';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { encodeStrictEgressPolicy } from './strict-policy';
import { digestBudgetPolicy } from './strict-proxy-launcher';
import { createEgressTlsMaterial } from './tls-material';
import type { ProxyBudgetGrant, ProxyBudgetWorkflowBinding } from './proxy-budget-ledger';
import type { TrustedProviderBudgetPolicy } from './provider-budget-contract';

const ENABLED = process.env.ARCHON_RUN_PROXY_BUDGET_CONTAINER_TEST === '1';
const OWNER_LABEL = 'archon.test.proxy-budget.owner';
const REQUIRED_BUN_VERSION = '1.3.14';
const RUNNER_DOCKERFILE = 'packages/isolation/docker/runner.Dockerfile';
const RUNNER_CONTEXT = 'packages/isolation';
const BUDGET_CLI = '/usr/local/lib/archon/egress/proxy-budget-ledger-cli.ts';
const STRICT_PROXY_CLI = '/usr/local/lib/archon/egress/strict-https-proxy-cli.mjs';
const CLIENT_SOURCE = join(import.meta.dir, 'proxy-budget-client.ts');

const providerPolicies: TrustedProviderBudgetPolicy[] = [
  {
    provider: 'openai',
    host: 'provider.test',
    model: 'gpt-test',
    maxInputTokens: 100,
    maxOutputTokens: 50,
  },
];
const egressPolicy = {
  targets: [{ host: 'provider.test', port: 443 }],
  httpGrants: [{ host: 'provider.test', port: 443, methods: ['POST'], paths: ['/v1/chat'] }],
};
const egressPolicyB64 = encodeStrictEgressPolicy(egressPolicy);

let image = '';
let builtImage: string | undefined;
const ownedVolumes: string[] = [];
const ownedContainers: string[] = [];
const ownedDirs: string[] = [];

describe.skipIf(!ENABLED)('proxy budget Docker fixture', () => {
  beforeAll(() => {
    image = process.env.ARCHON_PROXY_BUDGET_TEST_IMAGE ?? buildUniqueRunnerImage();
    const inspect = docker(['image', 'inspect', '--format', '{{.Id}}', image]).trim();
    const versions = dockerRun([
      '--entrypoint',
      'sh',
      image,
      '-lc',
      'node --version && bun --version',
    ]);
    expect(versions).toContain('v18.');
    expect(versions).toContain(REQUIRED_BUN_VERSION);
    process.stdout.write(`PROXY_BUDGET_IMAGE=${image}\nPROXY_BUDGET_IMAGE_ID=${inspect}\n`);
    process.stdout.write(`PROXY_BUDGET_SOURCE_HASHES=${JSON.stringify(sourceHashes())}\n`);
  }, 180_000);

  afterEach(() => {
    cleanupOwnedContainers();
    cleanupOwnedVolumes();
    for (const dir of ownedDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });

  afterAll(() => {
    if (builtImage) spawnSync('docker', ['rmi', '-f', builtImage], { encoding: 'utf8' });
  });

  test('fixed Bun CLI and Node client preserve real SQLite reservations across resume', () => {
    const resources = createBudgetResources();
    const grant = createGrant();
    stagePrivateMaterial(resources, grant, []);

    const created = runBudgetCli(resources, ['--create'], 'status');
    expect(created).toContain('"consumedInputTokens":0');
    expect(created).toContain('"pendingReservations":0');

    expect(runClientFixture(resources, 'missing-binding', grant)).toContain(
      'CLIENT_BINDING_REJECTED=PASS Command payload'
    );
    expect(runClientFixture(resources, 'wrong-binding', grant)).toContain(
      'CLIENT_BINDING_REJECTED=PASS Workflow binding is not authorized'
    );
    expect(
      runBudgetCliWithPayload(resources, 'reserve', {
        requestHash: 'extra-binding-field',
        inputCeiling: 1,
        outputCeiling: 1,
        workflowBinding: {
          runId: grant.runId,
          workflowDigest: grant.workflowDigest,
          policyDigest: grant.policyDigest,
          extra: true,
        },
      })
    ).toContain('Command payload contains unsupported settings.');

    const normal = runClientFixture(resources, 'normal', grant);
    expect(normal).toContain('CLIENT_NORMAL=PASS consumed=4/3');

    const pending = runClientFixture(resources, 'reserve-only', grant);
    expect(pending).toContain('CLIENT_PENDING=PASS');

    const resumed = runClientFixture(resources, 'status-pending', grant);
    expect(resumed).toContain('CLIENT_STATUS_PENDING=PASS pending=1');
  }, 90_000);

  test('v2 member clients share one persistent aggregate ledger across containers', () => {
    const resources = createBudgetResources();
    const grant = createV2Grant();
    const [rootBinding, childBinding] = grant.workflowBindings;
    stagePrivateMaterial(resources, grant, []);

    expect(runBudgetCli(resources, ['--create'], 'status')).toContain('"consumedTotalTokens":0');
    expect(runClientFixture(resources, 'settle-root', grant, rootBinding)).toContain(
      'CLIENT_SETTLED=PASS consumed=3/2'
    );
    expect(runClientFixture(resources, 'settle-child', grant, childBinding)).toContain(
      'CLIENT_SETTLED=PASS consumed=7/3'
    );
    expect(runClientFixture(resources, 'status-v2', grant, rootBinding)).toContain(
      'CLIENT_V2_STATUS=PASS consumed=7/3 total=10'
    );
    expect(runClientFixture(resources, 'exhaust-v2', grant, childBinding)).toContain(
      'CLIENT_V2_EXHAUSTED=PASS Total token budget exhausted.'
    );
  }, 90_000);

  test('resume refuses missing database, grant drift, and agent containers cannot read ledger volume', () => {
    const resources = createBudgetResources();
    stagePrivateMaterial(resources, createGrant(), []);
    runBudgetCli(resources, ['--create'], 'status');

    const drifted = createGrant({ workflowDigest: 'sha256:workflow-drifted' });
    const driftPrivateVolume = createVolume('private-drift');
    const driftResources = { ...resources, privateVolume: driftPrivateVolume };
    stagePrivateOnly(driftResources.privateVolume, drifted, []);
    expect(runBudgetCliExpectFailure(driftResources, [], 'status')).toContain(
      'Budget grant binding drift detected.'
    );

    const missing = createBudgetResources();
    stagePrivateMaterial(missing, createGrant(), []);
    expect(runBudgetCliExpectFailure(missing, [], 'status')).toContain('not initialized');

    const agentProbe = dockerRun([
      '--network',
      'none',
      '--user',
      '1000:1000',
      '--cap-drop',
      'ALL',
      '--security-opt',
      'no-new-privileges',
      '-v',
      `${resources.agentVolume}:/workspace`,
      '--entrypoint',
      'sh',
      image,
      '-c',
      'test ! -e /archon-budget/ledger.sqlite && echo AGENT_LEDGER_UNREADABLE=PASS',
    ]);
    expect(agentProbe).toContain('AGENT_LEDGER_UNREADABLE=PASS');
  }, 90_000);

  test('strict launcher starts with matching seed without budget readiness mutation', async () => {
    const resources = createBudgetResources();
    const grant = createGrant();
    const tls = await createEgressTlsMaterial(['provider.test']);
    stagePrivateMaterial(resources, grant, [
      ['policy.json', Buffer.from(egressPolicyB64, 'base64').toString('utf8')],
      ['ca.crt', tls.caCertificate],
      ['leaf.crt', tls.certificate],
      ['leaf.key', tls.privateKey],
    ]);
    runBudgetCli(resources, ['--create'], 'status');

    const name = ownedName('proxy');
    ownedContainers.push(name);
    const launched = spawnSync('docker', strictLauncherArgs(name, resources, grant.policyDigest), {
      encoding: 'utf8',
      timeout: 20_000,
    });
    expect(launched.status, `${launched.stdout}${launched.stderr}`).toBe(0);
    expect(launched.stdout).toContain('archon-strict-https-proxy: ready');
    const status = runBudgetCli(resources, [], 'status');
    expect(status).toContain('"consumedInputTokens":0');
    expect(status).toContain('"unknownReservations":0');
  }, 90_000);
});

interface BudgetResources {
  privateVolume: string;
  budgetVolume: string;
  egressVolume: string;
  agentVolume: string;
}

function createBudgetResources(): BudgetResources {
  return {
    privateVolume: createVolume('private'),
    budgetVolume: createVolume('budget'),
    egressVolume: createVolume('egress'),
    agentVolume: createVolume('agent'),
  };
}

function createGrant(overrides: Partial<ProxyBudgetGrant> = {}): ProxyBudgetGrant {
  const base = {
    schema: 'archon.proxy-budget-grant.v1' as const,
    rootChainId: 'chain-container-test',
    runId: 'run-container-test',
    workflowDigest: 'sha256:workflow-container-test',
    policyDigest: digestBudgetPolicy({
      egressPolicyB64,
      image,
      providerPolicies,
    }),
    deadlineEpochMs: Date.now() + 120_000,
    inputTokenLimit: 1_000,
    outputTokenLimit: 500,
    totalTokenLimit: 1_200,
  };
  return { ...base, ...overrides };
}

function createV2Grant(): Extract<ProxyBudgetGrant, { schema: 'archon.proxy-budget-grant.v2' }> {
  const policyDigest = digestBudgetPolicy({ egressPolicyB64, image, providerPolicies });
  return {
    schema: 'archon.proxy-budget-grant.v2',
    rootChainId: 'a-root-container-test',
    deadlineEpochMs: Date.now() + 120_000,
    inputTokenLimit: 20,
    outputTokenLimit: 10,
    totalTokenLimit: 12,
    workflowBindings: [
      {
        runId: 'a-root-container-test',
        workflowDigest: 'sha256:workflow-root-container-test',
        policyDigest,
      },
      {
        runId: 'b-child-container-test',
        workflowDigest: 'sha256:workflow-child-container-test',
        policyDigest,
      },
    ],
  };
}

function stagePrivateMaterial(
  resources: BudgetResources,
  grant: ProxyBudgetGrant,
  extraFiles: [string, string][]
): void {
  const payload = JSON.stringify({ grant, providerPolicies, extraFiles });
  const program = `
import { writeFileSync, mkdirSync, chownSync, chmodSync } from 'node:fs';
const input = JSON.parse(${JSON.stringify(payload)});
mkdirSync('/archon-proxy-private', { recursive: true });
mkdirSync('/archon-budget', { recursive: true });
mkdirSync('/archon-egress', { recursive: true });
writeFileSync('/archon-proxy-private/budget.json', JSON.stringify(input.grant));
writeFileSync('/archon-proxy-private/provider-policies.json', JSON.stringify(input.providerPolicies));
for (const [name, content] of input.extraFiles) writeFileSync('/archon-proxy-private/' + name, content);
chmodSync('/archon-proxy-private', 0o500);
chmodSync('/archon-budget', 0o700);
chmodSync('/archon-egress', 0o700);
for (const name of ['budget.json', 'provider-policies.json', ...input.extraFiles.map(([name]) => name)]) {
  chmodSync('/archon-proxy-private/' + name, 0o400);
  chownSync('/archon-proxy-private/' + name, 1000, 1000);
}
chownSync('/archon-proxy-private', 1000, 1000);
chownSync('/archon-budget', 1000, 1000);
chownSync('/archon-egress', 1000, 1000);
`;
  dockerRun([
    '--network',
    'none',
    '--user',
    '0:0',
    '--cap-drop',
    'ALL',
    '--cap-add',
    'CHOWN',
    '--security-opt',
    'no-new-privileges',
    '-v',
    `${resources.privateVolume}:/archon-proxy-private`,
    '-v',
    `${resources.budgetVolume}:/archon-budget`,
    '-v',
    `${resources.egressVolume}:/archon-egress`,
    '--entrypoint',
    'node',
    image,
    '--input-type=module',
    '-e',
    program,
  ]);
}

function stagePrivateOnly(
  privateVolume: string,
  grant: ProxyBudgetGrant,
  extraFiles: [string, string][]
): void {
  const payload = JSON.stringify({ grant, providerPolicies, extraFiles });
  const program = `
import { writeFileSync, mkdirSync, chownSync, chmodSync } from 'node:fs';
const input = JSON.parse(${JSON.stringify(payload)});
mkdirSync('/archon-proxy-private', { recursive: true });
writeFileSync('/archon-proxy-private/budget.json', JSON.stringify(input.grant));
writeFileSync('/archon-proxy-private/provider-policies.json', JSON.stringify(input.providerPolicies));
for (const [name, content] of input.extraFiles) writeFileSync('/archon-proxy-private/' + name, content);
chmodSync('/archon-proxy-private', 0o500);
for (const name of ['budget.json', 'provider-policies.json', ...input.extraFiles.map(([name]) => name)]) {
  chmodSync('/archon-proxy-private/' + name, 0o400);
  chownSync('/archon-proxy-private/' + name, 1000, 1000);
}
chownSync('/archon-proxy-private', 1000, 1000);
`;
  dockerRun([
    '--network',
    'none',
    '--user',
    '0:0',
    '--cap-drop',
    'ALL',
    '--cap-add',
    'CHOWN',
    '--security-opt',
    'no-new-privileges',
    '-v',
    `${privateVolume}:/archon-proxy-private`,
    '--entrypoint',
    'node',
    image,
    '--input-type=module',
    '-e',
    program,
  ]);
}

function runBudgetCli(resources: BudgetResources, cliArgs: string[], command: string): string {
  return dockerRunWithInput(
    budgetCliArgs(resources, cliArgs),
    `${JSON.stringify({ id: 'status', command, payload: {} })}\n`
  );
}

function runBudgetCliWithPayload(
  resources: BudgetResources,
  command: string,
  payload: object
): string {
  return dockerRunWithInput(
    budgetCliArgs(resources, []),
    `${JSON.stringify({ id: 'binding-negative', command, payload })}\n`
  );
}

function runBudgetCliExpectFailure(
  resources: BudgetResources,
  cliArgs: string[],
  command: string
): string {
  const result = spawnSync('docker', ['run', '--rm', '-i', ...budgetCliArgs(resources, cliArgs)], {
    input: `${JSON.stringify({ id: 'status', command, payload: {} })}\n`,
    encoding: 'utf8',
    timeout: 20_000,
  });
  expect(result.status).not.toBe(0);
  return `${result.stdout}${result.stderr}`;
}

function budgetCliArgs(resources: BudgetResources, cliArgs: string[]): string[] {
  return [
    '--network',
    'none',
    '--user',
    '1000:1000',
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges',
    '-v',
    `${resources.privateVolume}:/archon-proxy-private:ro`,
    '-v',
    `${resources.budgetVolume}:/archon-budget`,
    '-v',
    `${resources.egressVolume}:/archon-egress`,
    '--entrypoint',
    '/usr/local/bin/bun',
    image,
    BUDGET_CLI,
    ...cliArgs,
  ];
}

function runClientFixture(
  resources: BudgetResources,
  mode: string,
  grant: ProxyBudgetGrant,
  workflowBinding?: ProxyBudgetWorkflowBinding
): string {
  const dir = mkdtempSync(join(tmpdir(), 'archon-proxy-budget-fixture-'));
  ownedDirs.push(dir);
  const entry = join(dir, 'fixture.ts');
  const output = join(dir, 'proxy-budget-client-fixture.mjs');
  writeFileSync(entry, clientFixtureSource(mode, grant, workflowBinding));
  const build = spawnSync('bun', ['build', entry, '--target=node', '--outfile', output], {
    encoding: 'utf8',
    timeout: 30_000,
  });
  expect(build.status, `${build.stdout}${build.stderr}`).toBe(0);
  const name = ownedName('client');
  ownedContainers.push(name);
  execFileSync('docker', [
    'create',
    '--name',
    name,
    '--label',
    `${OWNER_LABEL}=${name}`,
    '--network',
    'none',
    '--user',
    '1000:1000',
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges',
    '-v',
    `${resources.privateVolume}:/archon-proxy-private:ro`,
    '-v',
    `${resources.budgetVolume}:/archon-budget`,
    '-v',
    `${resources.egressVolume}:/archon-egress`,
    '--entrypoint',
    'node',
    image,
    '/usr/local/lib/archon/egress/proxy-budget-client-fixture.mjs',
  ]);
  execFileSync('docker', [
    'cp',
    output,
    `${name}:/usr/local/lib/archon/egress/proxy-budget-client-fixture.mjs`,
  ]);
  return docker(['start', '-a', name]);
}

function clientFixtureSource(
  mode: string,
  grant: ProxyBudgetGrant,
  explicitBinding?: ProxyBudgetWorkflowBinding
): string {
  const workflowBinding = explicitBinding ?? v1FixtureBinding(grant);
  const selectedBinding =
    mode === 'missing-binding'
      ? undefined
      : mode === 'wrong-binding'
        ? { ...workflowBinding, runId: 'wrong-run' }
        : workflowBinding;
  return `
import { createProxyBudgetClient } from ${JSON.stringify(CLIENT_SOURCE)};
const client = createProxyBudgetClient({
  deadlineEpochMs: Date.now() + 60000,
  workflowBinding: ${JSON.stringify(selectedBinding)},
});
await client.ready();
if (${JSON.stringify(mode)} === 'missing-binding' || ${JSON.stringify(mode)} === 'wrong-binding') {
  try {
    await client.reserveBudget({ requestHash: 'binding-negative', inputCeiling: 1, outputCeiling: 1 });
    process.exit(2);
  } catch (error) {
    console.log('CLIENT_BINDING_REJECTED=PASS ' + error.message);
    await client.close();
  }
} else if (${JSON.stringify(mode)} === 'normal') {
  const first = await client.reserveBudget({ requestHash: 'repeat', inputCeiling: 10, outputCeiling: 5 });
  await client.settleBudget({ reservationId: first.reservationId, inputTokens: 3, outputTokens: 2 });
  const second = await client.reserveBudget({ requestHash: 'repeat', inputCeiling: 10, outputCeiling: 5 });
  await client.settleBudget({ reservationId: second.reservationId, inputTokens: 1, outputTokens: 1 });
  const status = await client.getBudgetStatus();
  console.log('CLIENT_NORMAL=PASS consumed=' + status.consumedInputTokens + '/' + status.consumedOutputTokens);
  await client.close();
} else if (${JSON.stringify(mode)} === 'reserve-only') {
  await client.reserveBudget({ requestHash: 'pending', inputCeiling: 10, outputCeiling: 5 });
  console.log('CLIENT_PENDING=PASS');
  process.exit(0);
} else if (${JSON.stringify(mode)} === 'settle-root' || ${JSON.stringify(mode)} === 'settle-child') {
  const usage = ${JSON.stringify(mode)} === 'settle-root' ? { input: 3, output: 2 } : { input: 4, output: 1 };
  const reservation = await client.reserveBudget({ requestHash: ${JSON.stringify(mode)}, inputCeiling: 5, outputCeiling: 2 });
  await client.settleBudget({ reservationId: reservation.reservationId, inputTokens: usage.input, outputTokens: usage.output });
  const status = await client.getBudgetStatus();
  console.log('CLIENT_SETTLED=PASS consumed=' + status.consumedInputTokens + '/' + status.consumedOutputTokens);
  await client.close();
} else if (${JSON.stringify(mode)} === 'status-v2') {
  const status = await client.getBudgetStatus();
  console.log('CLIENT_V2_STATUS=PASS consumed=' + status.consumedInputTokens + '/' + status.consumedOutputTokens + ' total=' + status.consumedTotalTokens);
  await client.close();
} else if (${JSON.stringify(mode)} === 'exhaust-v2') {
  try {
    await client.reserveBudget({ requestHash: 'exhaust-v2', inputCeiling: 2, outputCeiling: 1 });
    process.exit(2);
  } catch (error) {
    console.log('CLIENT_V2_EXHAUSTED=PASS ' + error.message);
    await client.close();
  }
} else {
  const status = await client.getBudgetStatus();
  console.log('CLIENT_STATUS_PENDING=PASS pending=' + status.pendingReservations);
  await client.close();
}
`;
}

function v1FixtureBinding(grant: ProxyBudgetGrant): ProxyBudgetWorkflowBinding {
  if (grant.schema !== 'archon.proxy-budget-grant.v1')
    throw new Error('V2 fixture requires an explicit workflow binding.');
  return {
    runId: grant.runId,
    workflowDigest: grant.workflowDigest,
    policyDigest: grant.policyDigest,
  };
}

function strictLauncherArgs(
  name: string,
  resources: BudgetResources,
  policyDigest: string
): string[] {
  return [
    'run',
    '--rm',
    '--name',
    name,
    '--label',
    `${OWNER_LABEL}=${name}`,
    '--network',
    'none',
    '--user',
    '1000:1000',
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges',
    '-v',
    `${resources.privateVolume}:/archon-proxy-private:ro`,
    '-v',
    `${resources.budgetVolume}:/archon-budget`,
    '-v',
    `${resources.egressVolume}:/archon-egress`,
    '-e',
    'ARCHON_EGRESS_SOCKET=/archon-egress/proxy.sock',
    '-e',
    `ARCHON_EGRESS_POLICY_B64=${egressPolicyB64}`,
    '-e',
    `ARCHON_PROXY_IMAGE_ID=${image}`,
    '-e',
    `ARCHON_PROXY_BUDGET_POLICY_DIGEST=${policyDigest}`,
    '--read-only',
    '--tmpfs',
    '/tmp:rw,nosuid,nodev,size=16m',
    '--entrypoint',
    'sh',
    image,
    '-c',
    strictLauncherProgram(),
  ];
}

function strictLauncherProgram(): string {
  return [
    'set -eu',
    `node ${STRICT_PROXY_CLI} >/tmp/proxy.log 2>&1 &`,
    'pid=$!',
    'for i in $(seq 1 50); do',
    '  if grep -q "archon-strict-https-proxy: ready" /tmp/proxy.log; then break; fi',
    '  if ! kill -0 "$pid" 2>/dev/null; then cat /tmp/proxy.log; exit 1; fi',
    '  sleep 0.1',
    'done',
    'grep -q "archon-strict-https-proxy: ready" /tmp/proxy.log',
    'cat /tmp/proxy.log',
    'kill -TERM "$pid"',
    'wait "$pid" || true',
    'echo LAUNCHER_STOPPED=PASS',
  ].join('\n');
}

function buildUniqueRunnerImage(): string {
  const tag = `archon-runner:proxy-budget-${randomUUID()}`;
  execFileSync('docker', ['build', '-t', tag, '-f', RUNNER_DOCKERFILE, RUNNER_CONTEXT], {
    encoding: 'utf8',
    timeout: 180_000,
    maxBuffer: 8 * 1024 * 1024,
  });
  builtImage = tag;
  return tag;
}

function createVolume(kind: string): string {
  const name = ownedName(kind);
  execFileSync('docker', ['volume', 'create', '--label', `${OWNER_LABEL}=${name}`, name], {
    encoding: 'utf8',
    timeout: 10_000,
  });
  ownedVolumes.push(name);
  return name;
}

function ownedName(kind: string): string {
  return `archon-proxy-budget-${kind}-${randomUUID()}`;
}

function dockerRun(args: string[]): string {
  return docker(['run', '--rm', ...args]);
}

function dockerRunWithInput(args: string[], input: string): string {
  return execFileSync('docker', ['run', '--rm', '-i', ...args], {
    input,
    encoding: 'utf8',
    timeout: 30_000,
    maxBuffer: 4 * 1024 * 1024,
  });
}

function docker(args: string[]): string {
  return execFileSync('docker', args, {
    encoding: 'utf8',
    timeout: 30_000,
    maxBuffer: 4 * 1024 * 1024,
  });
}

function cleanupOwnedContainers(): void {
  for (const name of ownedContainers.splice(0)) {
    const inspected = spawnSync(
      'docker',
      ['inspect', '--format', `{{index .Config.Labels "${OWNER_LABEL}"}}`, name],
      { encoding: 'utf8' }
    );
    if (inspected.status === 0 && inspected.stdout.trim() === name) {
      spawnSync('docker', ['rm', '-f', name], { encoding: 'utf8' });
    }
  }
}

function cleanupOwnedVolumes(): void {
  for (const name of ownedVolumes.splice(0)) {
    const inspected = spawnSync(
      'docker',
      ['volume', 'inspect', '--format', `{{index .Labels "${OWNER_LABEL}"}}`, name],
      { encoding: 'utf8' }
    );
    if (inspected.status === 0 && inspected.stdout.trim() === name) {
      spawnSync('docker', ['volume', 'rm', '-f', name], { encoding: 'utf8' });
    }
  }
}

function sourceHashes(): Record<string, string> {
  const files = [
    RUNNER_DOCKERFILE,
    'packages/isolation/src/egress/proxy-budget-ledger.ts',
    'packages/isolation/src/egress/proxy-budget-ledger-cli.ts',
    'packages/isolation/src/egress/proxy-budget-client.ts',
    'packages/isolation/src/egress/strict-proxy-launcher.ts',
  ];
  return Object.fromEntries(
    files.map(file => [file, createHash('sha256').update(readFileSync(file)).digest('hex')])
  );
}
