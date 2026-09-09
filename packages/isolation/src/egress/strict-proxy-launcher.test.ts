import { afterEach, beforeAll, beforeEach, expect, test } from 'bun:test';
import { chmod, link, mkdtemp, rm, symlink, writeFile } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
import { createEgressTlsMaterial } from './tls-material';
import { encodeStrictEgressPolicy } from './strict-policy';
import { digestBudgetPolicy, loadStrictProxyMaterial } from './strict-proxy-launcher';
import type { ProxyBudgetClient } from './proxy-budget-client';

let material: Awaited<ReturnType<typeof createEgressTlsMaterial>>;
let directory: string;
const encoded = encodeStrictEgressPolicy({
  targets: [{ host: 'registry.example', port: 443 }],
  httpGrants: [{ host: 'registry.example', port: 443, methods: ['GET'], paths: ['/package'] }],
});

beforeAll(async () => {
  material = await createEgressTlsMaterial(['registry.example']);
});
beforeEach(async () => {
  directory = await mkdtemp(join(tmpdir(), 'archon-private-proxy-'));
  for (const [name, contents] of Object.entries({
    'policy.json': Buffer.from(encoded, 'base64'),
    'ca.crt': material.caCertificate,
    'leaf.crt': material.certificate,
    'leaf.key': material.privateKey,
  }))
    await writeFile(join(directory, name), contents, { mode: 0o600 });
});
afterEach(async () => {
  await rm(directory, { recursive: true, force: true });
});

test('loads only matching private TLS material and exact frozen grants', async () => {
  const options = await loadStrictProxyMaterial(directory, encoded);
  expect(options.grants[0]?.paths).toEqual(['/package']);
  expect(options.policy.targets).toEqual([{ host: 'registry.example', port: 443 }]);
  expect(options).not.toHaveProperty('upstreamCa');
  expect(options).not.toHaveProperty('connectTarget');
});

test('rejects substituted policy bytes even if the replacement is valid JSON', async () => {
  await writeFile(join(directory, 'policy.json'), '{}');
  await expect(loadStrictProxyMaterial(directory, encoded)).rejects.toThrow(/frozen/);
});

test('rejects readable secret keys and writable private directories', async () => {
  await chmod(join(directory, 'leaf.key'), 0o644);
  await expect(loadStrictProxyMaterial(directory, encoded)).rejects.toThrow(/permissions/);
  await chmod(join(directory, 'leaf.key'), 0o600);
  await chmod(directory, 0o777);
  await expect(loadStrictProxyMaterial(directory, encoded)).rejects.toThrow(/directory/);
});

test('rejects symlinked or multiply linked private key files', async () => {
  const key = join(directory, 'leaf.key');
  const saved = join(directory, 'saved.key');
  await link(key, saved);
  await expect(loadStrictProxyMaterial(directory, encoded)).rejects.toThrow(/file/);
  await rm(key);
  await symlink(saved, key);
  await expect(loadStrictProxyMaterial(directory, encoded)).rejects.toThrow();
});

test('rejects oversized files before using their content', async () => {
  await writeFile(join(directory, 'policy.json'), 'x'.repeat(65_537));
  await expect(loadStrictProxyMaterial(directory, encoded)).rejects.toThrow(/file/);
});

test('rejects certificate identity outside the frozen targets', async () => {
  const other = encodeStrictEgressPolicy({
    targets: [{ host: 'different.example', port: 443 }],
    httpGrants: [{ host: 'different.example', port: 443, methods: ['GET'], paths: ['/package'] }],
  });
  await writeFile(join(directory, 'policy.json'), Buffer.from(other, 'base64'));
  await expect(loadStrictProxyMaterial(directory, other)).rejects.toThrow();
});

test('loads complete budget material without consuming a readiness reservation', async () => {
  const image = 'sha256:' + '3'.repeat(64);
  const providerPolicies = [
    {
      provider: 'openai' as const,
      host: 'registry.example',
      model: 'gpt-test',
      maxInputTokens: 100,
      maxOutputTokens: 50,
    },
  ];
  const policyDigest = digestBudgetPolicy({
    egressPolicyB64: encoded,
    image,
    providerPolicies,
  });
  const grant = {
    schema: 'archon.proxy-budget-grant.v1' as const,
    rootChainId: 'chain-launcher',
    runId: 'run-launcher',
    workflowDigest: 'sha256:workflow-launcher',
    policyDigest,
    deadlineEpochMs: Date.now() + 60_000,
    inputTokenLimit: 1_000,
    outputTokenLimit: 500,
    totalTokenLimit: 1_200,
  };
  const observedDeadlines: number[] = [];
  const observedBindings: unknown[] = [];
  const fakeClient: ProxyBudgetClient = {
    async ready() {},
    async reserveBudget() {
      throw new Error('readiness must not reserve budget');
    },
    async settleBudget() {
      throw new Error('readiness must not settle budget');
    },
    async markReservationUnknown() {
      throw new Error('readiness must not mark budget unknown');
    },
    async getReservation() {
      throw new Error('not used');
    },
    async getBudgetStatus() {
      return {
        grant,
        pendingReservations: 0,
        unknownReservations: 0,
        consumedInputTokens: 0,
        consumedOutputTokens: 0,
        consumedTotalTokens: 0,
        remainingInputTokens: 1_000,
        remainingOutputTokens: 500,
        remainingTotalTokens: 1_200,
        acceptingReservations: true,
      };
    },
    async close() {},
  };
  await writeFile(join(directory, 'budget.json'), JSON.stringify(grant), { mode: 0o600 });
  await writeFile(join(directory, 'provider-policies.json'), JSON.stringify(providerPolicies), {
    mode: 0o600,
  });

  const loaded = await loadStrictProxyMaterial(directory, encoded, image, options => {
    observedDeadlines.push(options.deadlineEpochMs ?? 0);
    observedBindings.push(options.workflowBinding);
    return fakeClient;
  });

  expect(observedDeadlines).toEqual([grant.deadlineEpochMs]);
  expect(observedBindings).toEqual([
    {
      runId: grant.runId,
      workflowDigest: grant.workflowDigest,
      policyDigest: grant.policyDigest,
    },
  ]);
  expect(loaded.budgetGrant).toEqual(grant);
  expect(await loaded.accounting?.client.getStatus()).toMatchObject({
    consumedInputTokens: 0,
    consumedOutputTokens: 0,
    acceptingReservations: true,
  });
});

test('rejects partial or drifted budget material before starting the ledger client', async () => {
  const image = 'sha256:' + '4'.repeat(64);
  await writeFile(join(directory, 'budget.json'), '{}', { mode: 0o600 });
  await expect(
    loadStrictProxyMaterial(directory, encoded, image, () => {
      throw new Error('client should not start');
    })
  ).rejects.toThrow(/incomplete/);

  const providerPolicies = [
    {
      provider: 'openai' as const,
      host: 'registry.example',
      model: 'gpt-test',
      maxInputTokens: 100,
      maxOutputTokens: 50,
    },
  ];
  const grant = {
    schema: 'archon.proxy-budget-grant.v1' as const,
    rootChainId: 'chain-launcher',
    runId: 'run-launcher',
    workflowDigest: 'sha256:workflow-launcher',
    policyDigest: 'drifted',
    deadlineEpochMs: Date.now() + 60_000,
    inputTokenLimit: 1_000,
    outputTokenLimit: 500,
    totalTokenLimit: 1_200,
  };
  await writeFile(join(directory, 'provider-policies.json'), JSON.stringify(providerPolicies), {
    mode: 0o600,
  });
  await writeFile(join(directory, 'budget.json'), JSON.stringify(grant), { mode: 0o600 });
  await expect(
    loadStrictProxyMaterial(directory, encoded, image, () => {
      throw new Error('client should not start');
    })
  ).rejects.toThrow(/budget policy digest/);
});
