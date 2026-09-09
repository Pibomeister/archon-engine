import { X509Certificate, createHash } from 'crypto';
import { constants } from 'fs';
import { lstat, open } from 'fs/promises';
import { join } from 'path';
import { decodeStrictEgressPolicy } from './strict-policy';
import { validateEgressTlsMaterial } from './tls-material';
import { startStrictHttpsConnectProxy } from './strict-https-proxy';
import { createProxyBudgetClient } from './proxy-budget-client';
import type { ProxyBudgetClient } from './proxy-budget-client';
import type { BudgetStatus, ProxyBudgetGrant } from './proxy-budget-ledger';
import type { TrustedProviderBudgetPolicy } from './provider-budget-contract';
import type {
  StrictHttpsConnectProxyOptions,
  StrictProviderAccountingOptions,
  StrictProviderBudgetClient,
  StrictProviderBudgetReservation,
} from './strict-https-proxy';

const PRIVATE_ROOT = '/archon-proxy-private';
const SOCKET_PATH = '/archon-egress/proxy.sock';
const PROVIDER_POLICIES_PATH = 'provider-policies.json';
const BUDGET_GRANT_PATH = 'budget.json';
const MAX_PRIVATE_FILE_BYTES = 65_536;

interface LauncherBudgetClient extends StrictProviderBudgetClient {
  getStatus(): Promise<BudgetStatus>;
  close(): Promise<void>;
}

type LoadedStrictProxyMaterial = Omit<
  StrictHttpsConnectProxyOptions,
  'socketPath' | 'accounting'
> & {
  validUntil: string;
  budgetGrant?: ProxyBudgetGrant;
  accounting?: StrictProviderAccountingOptions & { client: LauncherBudgetClient };
};

export async function loadStrictProxyMaterial(
  directory: string,
  encodedPolicy: string,
  imageId?: string,
  budgetClientFactory: typeof createProxyBudgetClient = createProxyBudgetClient
): Promise<LoadedStrictProxyMaterial> {
  const dir = await lstat(directory);
  if (
    !dir.isDirectory() ||
    dir.isSymbolicLink() ||
    dir.uid !== process.getuid?.() ||
    dir.mode & 0o022
  ) {
    throw new Error('Invalid private proxy material directory.');
  }
  const policy = decodeStrictEgressPolicy(encodedPolicy);
  const policyBytes = await readPrivateFile(directory, 'policy.json');
  if (policyBytes.toString('base64') !== encodedPolicy) {
    throw new Error('Private proxy policy differs from the frozen controller policy.');
  }
  const caCertificate = (await readPrivateFile(directory, 'ca.crt')).toString('utf8');
  const certificate = (await readPrivateFile(directory, 'leaf.crt')).toString('utf8');
  const privateKey = (await readPrivateFile(directory, 'leaf.key', true)).toString('utf8');
  const validUntil = new Date(
    Math.min(
      Date.parse(new X509Certificate(certificate).validTo),
      Date.parse(new X509Certificate(caCertificate).validTo)
    )
  ).toISOString();
  validateEgressTlsMaterial(
    { caCertificate, certificate, privateKey, validUntil },
    policy.transport.targets.map(target => target.host)
  );
  const budget = await loadBudgetMaterial(directory, encodedPolicy, imageId, budgetClientFactory);
  return {
    policy: policy.transport,
    grants: policy.grants,
    tls: { key: privateKey, cert: certificate },
    validUntil,
    ...(budget ? { budgetGrant: budget.grant, accounting: budget.accounting } : {}),
  };
}

async function loadBudgetMaterial(
  directory: string,
  encodedPolicy: string,
  imageId: string | undefined,
  budgetClientFactory: typeof createProxyBudgetClient
): Promise<
  | {
      grant: ProxyBudgetGrant;
      accounting: StrictProviderAccountingOptions & { client: LauncherBudgetClient };
    }
  | undefined
> {
  const grantBytes = await readOptionalPrivateFile(directory, BUDGET_GRANT_PATH, true);
  const policyBytes = await readOptionalPrivateFile(directory, PROVIDER_POLICIES_PATH);
  if (!grantBytes && !policyBytes) return undefined;
  if (!grantBytes || !policyBytes || !imageId) {
    throw new Error('Strict proxy budget material is incomplete.');
  }
  const grant = JSON.parse(grantBytes.toString('utf8')) as ProxyBudgetGrant;
  const policies = JSON.parse(policyBytes.toString('utf8')) as TrustedProviderBudgetPolicy[];
  if (!Array.isArray(policies))
    throw new Error('Strict proxy provider budget policies are invalid.');
  const digest = digestBudgetPolicy({
    egressPolicyB64: encodedPolicy,
    image: imageId,
    providerPolicies: policies,
  });
  if (grant.policyDigest !== digest) {
    throw new Error('Strict proxy budget policy digest differs from controller authority.');
  }
  return {
    grant,
    accounting: {
      policies,
      client: createBudgetAdapter(
        budgetClientFactory({ deadlineEpochMs: grant.deadlineEpochMs }),
        grant
      ),
    },
  };
}

async function readOptionalPrivateFile(
  directory: string,
  name: string,
  secret = false
): Promise<Buffer | undefined> {
  try {
    return await readPrivateFile(directory, name, secret);
  } catch (error) {
    if (isMissingFileError(error)) return undefined;
    throw error;
  }
}

function createBudgetAdapter(
  client: ProxyBudgetClient,
  grant: ProxyBudgetGrant
): LauncherBudgetClient {
  return {
    async reserve(input): Promise<StrictProviderBudgetReservation> {
      const reservation = await client.reserveBudget({
        requestHash: input.requestHash,
        inputCeiling: input.inputCeiling,
        outputCeiling: input.outputCeiling,
      });
      return { reservationId: reservation.reservationId, deadlineEpochMs: grant.deadlineEpochMs };
    },
    async settleComplete(input): Promise<void> {
      await client.settleBudget(
        {
          reservationId: input.reservationId,
          inputTokens: input.usage.input,
          outputTokens: input.usage.output,
        },
        { signal: input.signal }
      );
    },
    async settleUnknown(input): Promise<void> {
      await client.markReservationUnknown({
        reservationId: input.reservationId,
        reason: input.reason,
      });
    },
    async getStatus(): Promise<BudgetStatus> {
      return await client.getBudgetStatus();
    },
    async close(): Promise<void> {
      await client.close();
    },
  };
}

export function digestBudgetPolicy(input: {
  egressPolicyB64: string;
  image: string;
  providerPolicies: TrustedProviderBudgetPolicy[];
}): string {
  return digestStable(input);
}

export function digestProxyBudgetSeed(seed: {
  grant: ProxyBudgetGrant;
  providerPolicies: TrustedProviderBudgetPolicy[];
}): string {
  return digestStable(seed);
}

function digestStable(value: unknown): string {
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

function grantsMatch(left: ProxyBudgetGrant, right: ProxyBudgetGrant): boolean {
  return stableSerialize(left) === stableSerialize(right);
}

function isMissingFileError(error: unknown): boolean {
  return error instanceof Error && 'code' in error && error.code === 'ENOENT';
}

async function readPrivateFile(directory: string, name: string, secret = false): Promise<Buffer> {
  const handle = await open(join(directory, name), constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = await handle.stat();
    if (
      !info.isFile() ||
      info.nlink !== 1 ||
      info.uid !== process.getuid?.() ||
      info.size > MAX_PRIVATE_FILE_BYTES
    ) {
      throw new Error('Invalid private proxy material file.');
    }
    if (info.mode & 0o022 || (secret && info.mode & 0o077)) {
      throw new Error('Private proxy material permissions are unsafe.');
    }
    const buffer = Buffer.alloc(MAX_PRIVATE_FILE_BYTES + 1);
    let size = 0;
    while (size < buffer.length) {
      const { bytesRead } = await handle.read(buffer, size, buffer.length - size, size);
      if (bytesRead === 0) break;
      size += bytesRead;
    }
    if (size > MAX_PRIVATE_FILE_BYTES) throw new Error('Private proxy material is too large.');
    return buffer.subarray(0, size);
  } finally {
    await handle.close();
  }
}

export async function runStrictHttpsProxyFromEnv(): Promise<void> {
  if (process.env.ARCHON_EGRESS_SOCKET !== SOCKET_PATH) {
    throw new Error('Strict proxy requires the fixed controller socket path.');
  }
  const loaded = await loadStrictProxyMaterial(
    PRIVATE_ROOT,
    process.env.ARCHON_EGRESS_POLICY_B64 ?? '',
    process.env.ARCHON_PROXY_IMAGE_ID
  );
  const accounting = loaded.accounting;
  const budgetGrant = loaded.budgetGrant;
  if (!accounting || !budgetGrant) {
    throw new Error('Strict proxy requires private budget material.');
  }
  if (process.env.ARCHON_PROXY_BUDGET_POLICY_DIGEST !== budgetGrant.policyDigest) {
    throw new Error('Strict proxy budget digest differs from controller launch binding.');
  }
  const status = await accounting.client.getStatus();
  if (!grantsMatch(status.grant, budgetGrant)) {
    throw new Error('Strict proxy budget ledger grant differs from controller seed.');
  }
  if (
    !status.acceptingReservations ||
    status.pendingReservations !== 0 ||
    status.unknownReservations !== 0
  ) {
    throw new Error('Strict proxy budget ledger is not accepting fresh reservations.');
  }
  const server = await startStrictHttpsConnectProxy({ ...loaded, socketPath: SOCKET_PATH });
  let closing = false;
  const remaining =
    Math.min(Date.parse(loaded.validUntil), budgetGrant.deadlineEpochMs) - Date.now();
  const expiry = setTimeout(
    () => {
      close();
    },
    Math.max(0, remaining)
  );
  expiry.unref();
  const close = (): void => {
    if (closing) return;
    closing = true;
    clearTimeout(expiry);
    server.close();
    void accounting.client.close();
  };
  server.once('close', () => {
    clearTimeout(expiry);
  });
  process.once('SIGTERM', close);
  process.once('SIGINT', close);
  process.stdout.write('archon-strict-https-proxy: ready\n');
}
