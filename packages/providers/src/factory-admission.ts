/** Factory-managed ownership of one sendQuery stream, including SDK retries.
 * This is not per-model-turn metering. Standalone providers do not use it.
 */
import { createHash, randomUUID } from 'node:crypto';
import type { IAgentProvider, MessageChunk, ProviderRegistration, SendQueryOptions } from './types';

export interface FactoryInvocationContext {
  runId: string;
  nodeId: string;
  iteration?: number;
  reask?: number;
}

export interface AdmissionRequest {
  version: 'archon.provider-admission.v1';
  invocationId: string;
  provider: string;
  context: FactoryInvocationContext;
  cwd: string;
  model: string;
  resumeSessionId?: string;
  promptDigest: string;
  requestDigest: string;
}
export interface AdmissionLease {
  invocationId: string;
  requestDigest: string;
  leaseId: string;
}
export interface AdmissionBroker {
  acquire(request: AdmissionRequest): Promise<AdmissionLease>;
  settle(lease: AdmissionLease, outcome: 'released' | 'quarantined'): Promise<void>;
}

function digest(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

/** Copy configuration data; preserve native callbacks and AbortSignal identity. */
function snapshot<T>(value: T): T {
  if (Array.isArray(value)) return value.map((item: unknown) => snapshot(item)) as T;
  if (
    value !== null &&
    typeof value === 'object' &&
    Object.getPrototypeOf(value) === Object.prototype
  ) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, snapshot(item)])
    ) as T;
  }
  return value;
}

async function settleLease(
  broker: AdmissionBroker,
  lease: AdmissionLease,
  closed: boolean
): Promise<void> {
  try {
    await broker.settle(lease, closed ? 'released' : 'quarantined');
  } catch {
    throw new Error('factory_provider_settlement_uncertain');
  }
}

function requestFor(
  provider: string,
  prompt: string,
  cwd: string,
  resumeSessionId: string | undefined,
  options: SendQueryOptions | undefined
): AdmissionRequest {
  const context = options?.factoryInvocation;
  const model = options?.model;
  if (!context?.runId || !context.nodeId || !model) {
    throw new Error('factory_provider_invocation_required');
  }
  const body = {
    version: 'archon.provider-admission.v1' as const,
    invocationId: randomUUID(),
    provider,
    context,
    cwd,
    model,
    ...(resumeSessionId ? { resumeSessionId } : {}),
    promptDigest: digest(prompt),
  };
  return { ...body, requestDigest: digest(JSON.stringify(body)) };
}

/** Lazily construct the SDK provider only after a matching broker receipt. */
export function createAdmittedProvider(
  entry: ProviderRegistration,
  broker: AdmissionBroker | undefined
): IAgentProvider {
  return {
    getType: () => entry.id,
    getCapabilities: () => entry.capabilities,
    async *sendQuery(prompt, cwd, resumeSessionId, options): AsyncGenerator<MessageChunk> {
      if (!broker) throw new Error('factory_provider_broker_required');
      if (entry.id !== 'claude' && entry.id !== 'codex') {
        throw new Error('factory_provider_not_qualified');
      }
      const admittedOptions = snapshot(options);
      const request = requestFor(entry.id, prompt, cwd, resumeSessionId, admittedOptions);
      let lease: AdmissionLease;
      try {
        lease = await broker.acquire(request);
      } catch {
        throw new Error('factory_provider_admission_denied');
      }
      if (
        lease.invocationId !== request.invocationId ||
        lease.requestDigest !== request.requestDigest ||
        !lease.leaseId
      ) {
        // No provider was constructed. The broker owns reconciliation of any
        // reservation behind an invalid/ambiguous acknowledgement.
        throw new Error('factory_provider_lease_mismatch');
      }
      let resultSeen = false;
      let closed = false;
      try {
        if (admittedOptions?.abortSignal?.aborted) throw new Error('factory_provider_aborted');
        const provider = entry.factory();
        for await (const chunk of provider.sendQuery(
          prompt,
          cwd,
          resumeSessionId,
          admittedOptions
        )) {
          if (chunk.type === 'result') resultSeen = true;
          yield chunk;
        }
        closed = resultSeen && !admittedOptions?.abortSignal?.aborted;
        if (!closed) throw new Error('factory_provider_transport_uncertain');
      } finally {
        await settleLease(broker, lease, closed);
      }
    },
  };
}
