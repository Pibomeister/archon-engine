/** Trusted CLI-only factory mode. Capture before repo env can alter startup state. */
import { closeSync, readSync, realpathSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
import { isAbsolute } from 'node:path';
import { z } from 'zod';
import type { AdmissionBroker, AdmissionLease, AdmissionRequest } from './factory-admission';
import { validateFactoryProviderScope, type FactoryProviderScope } from './factory-sandbox';
import { factoryRequestDigest } from './factory-digest';

const text = z.string().min(1).max(4096);
export const factoryBindingSchema = z
  .object({
    machineId: text,
    hostEpoch: text,
    factoryJobId: text,
    logicalChainId: text,
    attemptId: text,
    readySnapshotId: text,
    readyDigest: text,
    runtimeBundleId: text,
    runtimeBindingDigest: text,
    projectId: text,
  })
  .strict();
export const configSchema = z
  .object({
    version: z.literal('factory.provider-broker.config.v1'),
    transport: z.enum(['http+unix', 'http+loopback']),
    endpoint: text,
    capability: z.string().min(24).max(4096),
    expiresAt: z.iso.datetime(),
    managedRun: factoryBindingSchema.extend({ worktreePath: text }).strict(),
    successor: z
      .object({
        parentRunId: text,
        parentAttemptId: text,
        parentBindingDigest: text,
        commandId: text,
      })
      .strict()
      .optional(),
    providerPolicy: z
      .object({
        providers: z
          .array(
            z
              .object({
                provider: z.enum(['codex', 'claude', 'grok']),
                models: z.array(text).min(1),
                purpose: z.enum(['implementation', 'auxiliary-call', 'title-generation']),
              })
              .strict()
          )
          .min(1),
        allowedWriteRoots: z.array(text).min(1),
        allowedReadRoots: z.array(text),
        deniedRoots: z.array(text).min(1),
      })
      .strict(),
  })
  .strict();
type BrokerConfig = z.infer<typeof configSchema>;
export type FactoryBinding = z.infer<typeof factoryBindingSchema>;
export const leaseSchema = z
  .object({
    version: z.literal('archon.provider-admission.v1'),
    invocationId: text,
    requestDigest: text,
    leaseId: text,
    leaseExpiresAt: z.iso.datetime(),
  })
  .strict();

function readConfig(fd: number): BrokerConfig {
  if (!Number.isInteger(fd) || fd < 3 || fd > 64) throw new Error('factory_broker_fd_invalid');
  const bytes = Buffer.alloc(32769);
  let used = 0;
  try {
    while (used < bytes.length) {
      const count = readSync(fd, bytes, used, bytes.length - used, null);
      if (count === 0) break;
      used += count;
    }
  } catch {
    throw new Error('factory_broker_fd_unavailable');
  } finally {
    closeSync(fd);
  }
  if (used > 32768) throw new Error('factory_broker_config_too_large');
  try {
    const config = configSchema.parse(JSON.parse(bytes.subarray(0, used).toString('utf8')));
    if (Date.parse(config.expiresAt) <= Date.now()) throw new Error('expired');
    if (config.transport === 'http+unix') {
      if (!isAbsolute(config.endpoint)) throw new Error('socket');
    } else {
      const url = new URL(config.endpoint);
      if (
        url.protocol !== 'http:' ||
        !['127.0.0.1', '[::1]'].includes(url.hostname) ||
        url.username ||
        url.password ||
        url.search ||
        url.hash
      )
        throw new Error('endpoint');
    }
    return config;
  } catch {
    throw new Error('factory_broker_config_invalid');
  }
}

class HttpBroker implements AdmissionBroker {
  constructor(private readonly config: BrokerConfig) {}
  private async post(path: string, body: unknown): Promise<unknown> {
    const bytes = JSON.stringify(body);
    return await new Promise((accept, reject) => {
      const fail = (): void => {
        reject(new Error('factory_broker_unavailable'));
      };
      const endpoint =
        this.config.transport === 'http+loopback' ? new URL(path, this.config.endpoint) : undefined;
      const req = httpRequest(
        {
          ...(endpoint
            ? { hostname: endpoint.hostname, port: endpoint.port, path: endpoint.pathname }
            : { socketPath: this.config.endpoint, path }),
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            authorization: 'Bearer ' + this.config.capability,
            'content-length': Buffer.byteLength(bytes),
          },
        },
        res => {
          let payload = '';
          let size = 0;
          res.on('data', (chunk: Buffer) => {
            size += chunk.length;
            if (size > 32768) {
              req.destroy();
              fail();
            } else payload += chunk.toString();
          });
          res.on('error', fail);
          res.on('end', () => {
            if (res.statusCode !== 200 && res.statusCode !== 201) {
              fail();
              return;
            }
            try {
              accept(JSON.parse(payload) as unknown);
            } catch {
              fail();
            }
          });
        }
      );
      req.setTimeout(5000, () => {
        req.destroy();
        fail();
      });
      req.on('error', fail);
      req.end(bytes);
    });
  }
  async acquire(request: AdmissionRequest): Promise<AdmissionLease> {
    if (Date.parse(this.config.expiresAt) <= Date.now()) throw new Error('factory_broker_expired');
    if (
      !this.config.providerPolicy.providers.some(
        policy =>
          policy.provider === request.provider &&
          policy.purpose === request.purpose &&
          policy.models.includes(request.model)
      )
    )
      throw new Error('factory_provider_policy_denied');
    // One transport retry with the exact same invocation identity can recover a
    // lost acknowledgement. Neither attempt constructs the provider.
    let response: unknown;
    try {
      response = await this.post('/acquire', request);
    } catch {
      response = await this.post('/acquire', request);
    }
    const lease = leaseSchema.parse(response);
    if (Date.parse(lease.leaseExpiresAt) <= Date.now())
      throw new Error('factory_provider_lease_expired');
    return lease;
  }
  async settle(lease: AdmissionLease, outcome: 'released' | 'quarantined'): Promise<void> {
    const body = {
      version: 'archon.provider-admission.v1',
      leaseId: lease.leaseId,
      invocationId: lease.invocationId,
      requestDigest: lease.requestDigest,
      outcome,
      settledAt: new Date().toISOString(),
    };
    const response = await this.post('/settle', body);
    if (
      typeof response !== 'object' ||
      response === null ||
      !('outcome' in response) ||
      response.outcome !== outcome
    )
      throw new Error('factory_provider_settlement_mismatch');
  }
}

const args = process.argv.slice(2);
const offline = args.includes('--factory-provider-offline');
const fdValues = args.flatMap((value, index) =>
  value === '--factory-provider-broker-fd'
    ? [args[index + 1]]
    : value.startsWith('--factory-provider-broker-fd=')
      ? [value.slice('--factory-provider-broker-fd='.length)]
      : []
);
if (fdValues.length > 1 || (offline && fdValues.length))
  throw new Error('factory_provider_mode_conflict');
const config = fdValues.length ? readConfig(Number(fdValues[0])) : undefined;
const broker = config ? new HttpBroker(config) : undefined;
export function isFactoryManaged(): boolean {
  return offline || config !== undefined;
}
export function getFactoryBroker(): AdmissionBroker | undefined {
  return broker;
}
export function getFactoryBinding(): FactoryBinding | undefined {
  if (!config) return undefined;
  const { worktreePath, ...binding } = config.managedRun;
  void worktreePath;
  return { ...binding };
}
export function getFactoryScope(): FactoryProviderScope | undefined {
  return config
    ? {
        workspaceRoot: config.managedRun.worktreePath,
        writableRoots: [...config.providerPolicy.allowedWriteRoots],
        readableRoots: [...config.providerPolicy.allowedReadRoots],
        deniedRoots: [...config.providerPolicy.deniedRoots],
      }
    : undefined;
}
export function factoryLaunchContext(): Record<string, unknown> | undefined {
  const marker = factoryRunMarker();
  if (!marker) return undefined;
  return config
    ? {
        marker,
        factoryBinding: getFactoryBinding(),
        providerPolicy: structuredClone(config.providerPolicy),
        worktreePath: config.managedRun.worktreePath,
        ...(config.successor ? { successor: { ...config.successor } } : {}),
      }
    : { marker };
}
export function factoryRunMarker(): Record<string, unknown> | undefined {
  if (offline) return { version: 1, mode: 'offline-deterministic' };
  return config ? factoryMarkerForConfig(config) : undefined;
}
export function factoryMarkerForConfig(value: BrokerConfig): Record<string, unknown> {
  const { worktreePath, ...binding } = value.managedRun;
  return {
    version: 1,
    mode: 'broker',
    ...binding,
    worktreePath,
    providerPolicyDigest: factoryRequestDigest(value.providerPolicy),
    policyTemplateDigest: factoryRequestDigest(policyTemplate(value.providerPolicy, worktreePath)),
  };
}
export function assertFactoryWorkingPath(cwd: string): void {
  const scope = getFactoryScope();
  if (scope) validateFactoryProviderScope(scope, cwd);
  if (
    config &&
    (realpathSync(cwd) !== config.managedRun.worktreePath ||
      realpathSync(config.managedRun.worktreePath) !== config.managedRun.worktreePath)
  )
    throw new Error('factory_provider_worktree_binding_mismatch');
}

function policyTemplate(
  value: BrokerConfig['providerPolicy'],
  worktreePath: string
): BrokerConfig['providerPolicy'] {
  const path = (item: string): string =>
    item === worktreePath || item.startsWith(worktreePath + '/')
      ? '$WORKTREE' + item.slice(worktreePath.length)
      : item;
  return {
    ...value,
    allowedWriteRoots: value.allowedWriteRoots.map(path),
    allowedReadRoots: value.allowedReadRoots.map(path),
    deniedRoots: value.deniedRoots.map(path),
  };
}

/** A fresh child attempt may vary its worktree instantiation, never its qualified policy. */
export function assertFactorySuccessorMode(
  metadata: Record<string, unknown> | undefined,
  parentRunId: string
): void {
  const prior = metadata?.factory_provider_admission;
  if (!isFactoryManaged() || offline) {
    assertFactoryRunMode(metadata);
    return;
  }
  if (!config?.successor || !prior || typeof prior !== 'object')
    throw new Error('factory_provider_successor_authority_required');
  const parent = prior as Record<string, unknown>;
  const current = factoryRunMarker();
  const authority = config.successor;
  if (
    !current ||
    authority.parentRunId !== parentRunId ||
    authority.parentAttemptId !== parent.attemptId ||
    authority.parentBindingDigest !== factoryRequestDigest(parent) ||
    current.attemptId === parent.attemptId
  )
    throw new Error('factory_provider_successor_lineage_mismatch');
  const mutable = new Set(['attemptId', 'worktreePath', 'providerPolicyDigest']);
  if (
    Object.keys(parent).length !== Object.keys(current).length ||
    Object.entries(current).some(([key, value]) => !mutable.has(key) && parent[key] !== value)
  )
    throw new Error('factory_provider_successor_binding_mismatch');
}

export function factorySuccessorRecord(
  parentRunId: string | undefined
): Record<string, unknown> | undefined {
  if (!config?.successor) return undefined;
  if (parentRunId !== config.successor.parentRunId)
    throw new Error('factory_provider_successor_selection_required');
  return { ...config.successor };
}
export function factorySuccessorParent(): string | undefined {
  return config?.successor?.parentRunId;
}
export function assertFactoryRunMode(
  metadata: Record<string, unknown> | undefined,
  allowUnmarked = false
): void {
  const prior = metadata?.factory_provider_admission;
  if (prior === undefined) {
    if (isFactoryManaged() && !allowUnmarked)
      throw new Error('factory_provider_unmarked_resume_not_qualified');
    return;
  }
  const current = factoryRunMarker();
  if (
    typeof prior !== 'object' ||
    prior === null ||
    !current ||
    Object.keys(prior).length !== Object.keys(current).length ||
    Object.entries(current).some(
      ([key, value]) => (prior as Record<string, unknown>)[key] !== value
    )
  )
    throw new Error('factory_provider_managed_resume_required');
}
export function assertFactoryForeground(): void {
  if (isFactoryManaged()) throw new Error('factory_provider_managed_detach_not_qualified');
}
