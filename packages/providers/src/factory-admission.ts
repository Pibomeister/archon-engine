import { factoryRequestDigest } from './factory-digest';
/** Factory-managed ownership of one sendQuery stream, including SDK retries.
 * This is not per-model-turn metering. Standalone providers do not use it.
 */
import { createHash, randomUUID } from 'node:crypto';
import type {
  FactoryInvocationOutcome,
  FactoryInvocationSignal,
  IAgentProvider,
  MessageChunk,
  ProviderRegistration,
  SendQueryOptions,
} from './types';
import { getFactoryBinding, getFactoryScope } from './factory-mode';
import { validateFactoryProviderScope } from './factory-sandbox';

export interface FactoryInvocationContext {
  runId: string;
  nodeId: string;
  /** Factory command identity that authorized this logical launch. */
  launchId?: string;
  /** Factory attempt identity for this exact execution base. */
  attemptId?: string;
  iteration?: number;
  reask?: number;
}

export interface AdmissionRequest {
  version: 'archon.provider-admission.v1';
  invocationId: string;
  provider: string;
  purpose: 'implementation';
  factoryBinding: Record<string, string>;
  context: FactoryInvocationContext;
  cwd: string;
  model: string;
  resumeSessionId?: string;
  promptDigest: string;
  requestDigest: string;
}
export interface AdmissionLease {
  version: 'archon.provider-admission.v1';
  invocationId: string;
  requestDigest: string;
  leaseId: string;
  leaseExpiresAt: string;
  budgetReservationId?: string;
  admittedActiveExecutionSeconds?: number;
}
export interface AdmissionBroker {
  acquire(request: AdmissionRequest): Promise<AdmissionLease>;
  settle(
    lease: AdmissionLease,
    outcome: 'released' | 'quarantined',
    signals?: readonly FactoryInvocationSignal[]
  ): Promise<void>;
}

function digest(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

export { factoryRequestDigest } from './factory-digest';

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
  closed: boolean,
  signals: readonly FactoryInvocationSignal[]
): Promise<void> {
  try {
    await broker.settle(lease, closed ? 'released' : 'quarantined', signals);
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
  for (const count of [context.iteration, context.reask]) {
    if (count !== undefined && (!Number.isInteger(count) || count < 0))
      throw new Error('factory_provider_invocation_required');
  }
  const body = {
    version: 'archon.provider-admission.v1' as const,
    invocationId: randomUUID(),
    provider,
    purpose: 'implementation' as const,
    factoryBinding: getFactoryBinding() ?? {},
    context,
    cwd,
    model,
    ...(resumeSessionId ? { resumeSessionId } : {}),
    promptDigest: digest(prompt),
  };
  return { ...body, requestDigest: factoryRequestDigest(body) };
}

function classifyResultOutcome(
  result: Extract<MessageChunk, { type: 'result' }>
): FactoryInvocationOutcome {
  const detail = [result.errorSubtype, result.stopReason, ...(result.errors ?? [])]
    .filter((item): item is string => typeof item === 'string')
    .join(' ')
    .toLowerCase();
  if (detail.includes('human') && (detail.includes('input') || detail.includes('review'))) {
    return 'human-input-required';
  }
  if (
    detail.includes('auth') ||
    detail.includes('unauthorized') ||
    detail.includes('forbidden') ||
    detail.includes('credential') ||
    detail.includes('login')
  ) {
    return 'auth-failure';
  }
  return result.isError && result.errorSubtype !== 'success' ? 'provider-error' : 'completed';
}

function signalForResult(
  provider: string,
  request: AdmissionRequest,
  lease: AdmissionLease,
  result: Extract<MessageChunk, { type: 'result' }>
): FactoryInvocationSignal {
  return {
    kind: 'factory-invocation-outcome',
    outcome: classifyResultOutcome(result),
    provider,
    invocationId: lease.invocationId,
    requestDigest: lease.requestDigest,
    leaseId: lease.leaseId,
    context: { ...request.context },
    ...(result.tokens ? { usage: { ...result.tokens } } : {}),
    ...(result.sessionId ? { sessionId: result.sessionId } : {}),
    ...(result.stopReason ? { stopReason: result.stopReason } : {}),
    ...(result.errorSubtype ? { errorSubtype: result.errorSubtype } : {}),
    ...(result.errors?.length ? { errors: [...result.errors] } : {}),
    ...(result.resumed !== undefined ? { resumed: result.resumed } : {}),
    ...(result.resolvedModel ? { resolvedModel: { ...result.resolvedModel } } : {}),
    occurredAt: new Date().toISOString(),
  };
}

function signalForHumanInputRequest(
  provider: string,
  request: AdmissionRequest,
  lease: AdmissionLease,
  chunk: Extract<MessageChunk, { type: 'human_input_request' }>
): FactoryInvocationSignal {
  return {
    kind: 'factory-invocation-outcome',
    outcome: 'human-input-required',
    provider,
    invocationId: lease.invocationId,
    requestDigest: lease.requestDigest,
    leaseId: lease.leaseId,
    context: { ...request.context },
    ...(chunk.sessionId ? { sessionId: chunk.sessionId } : {}),
    ...(chunk.reason ? { stopReason: chunk.reason } : {}),
    humanInput: {
      message: chunk.message,
      ...(chunk.reason ? { reason: chunk.reason } : {}),
      ...(chunk.sessionId ? { sessionId: chunk.sessionId } : {}),
      ...(chunk.choices ? { choices: [...chunk.choices] } : {}),
      ...(Array.isArray(chunk.questions) ? { questions: [...chunk.questions] } : {}),
    },
    occurredAt: new Date().toISOString(),
  };
}

function signalForUncertainTermination(
  provider: string,
  request: AdmissionRequest,
  lease: AdmissionLease,
  reason: string
): FactoryInvocationSignal {
  return {
    kind: 'factory-invocation-outcome',
    outcome: 'uncertain-termination',
    provider,
    invocationId: lease.invocationId,
    requestDigest: lease.requestDigest,
    leaseId: lease.leaseId,
    context: { ...request.context },
    errorSubtype: reason,
    occurredAt: new Date().toISOString(),
  };
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
      if (entry.id !== 'claude' && entry.id !== 'codex' && entry.id !== 'grok') {
        throw new Error('factory_provider_not_qualified');
      }
      const admittedOptions = snapshot(options);
      if (admittedOptions?.execContext?.kind === 'container')
        throw new Error('factory_provider_container_unqualified');
      if (admittedOptions && getFactoryScope()) admittedOptions.factoryScope = getFactoryScope();
      if (admittedOptions?.factoryScope)
        validateFactoryProviderScope(admittedOptions.factoryScope, cwd);
      if (admittedOptions?.factoryScope && process.platform === 'darwin') {
        // Keep native tool caches inside the documented native temporary
        // allowance, never a project-supplied protected checkout path.
        admittedOptions.env = { ...admittedOptions.env, TMPDIR: '/tmp' };
      }
      let transportClosed = false;
      if (admittedOptions)
        admittedOptions.factoryTransportClosed = (): void => {
          transportClosed = true;
        };
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
      let terminalSignal: FactoryInvocationSignal | undefined;
      const backgroundTasks = new Set<string>();
      const expiryController = new AbortController();
      const limitTimers = new Set<ReturnType<typeof setTimeout>>();
      // Arm one abort deadline. The lease expiry and the admitted active
      // execution limit both funnel through here, so whichever elapses first
      // aborts the invocation; a non-finite or already-elapsed budget aborts
      // immediately. Downstream is unchanged: the abort leaves `closed` false,
      // so the existing uncertain-termination signal and quarantined
      // settlement run as they already do for an expired lease.
      const armFactoryLimit = (ms: number): void => {
        if (!Number.isFinite(ms) || ms <= 0) {
          expiryController.abort();
          return;
        }
        limitTimers.add(
          setTimeout(
            () => {
              expiryController.abort();
            },
            Math.min(ms, 2147483647)
          )
        );
      };
      // A lease that had already lapsed when it was acquired is an admission failure, not a
      // mid-flight abort, and the frozen campaign criteria name the two separately. Both funnel
      // through the same abort controller below, so record which cause applies before arming:
      // only an already-elapsed lease expiry is an expired admission. A lease that expires while
      // the invocation is running stays an abort, because by then the provider was admitted.
      const admissionExpired = Boolean(
        lease.leaseExpiresAt && Date.parse(lease.leaseExpiresAt) - Date.now() <= 0
      );
      if (
        admittedOptions &&
        (lease.leaseExpiresAt || lease.admittedActiveExecutionSeconds !== undefined)
      ) {
        admittedOptions.abortSignal = admittedOptions.abortSignal
          ? AbortSignal.any([admittedOptions.abortSignal, expiryController.signal])
          : expiryController.signal;
        if (lease.leaseExpiresAt) armFactoryLimit(Date.parse(lease.leaseExpiresAt) - Date.now());
        if (lease.admittedActiveExecutionSeconds !== undefined) {
          armFactoryLimit(lease.admittedActiveExecutionSeconds * 1000);
        }
      }
      try {
        if (admittedOptions?.abortSignal?.aborted) {
          throw new Error(
            admissionExpired ? 'factory_provider_admission_expired' : 'factory_provider_aborted'
          );
        }
        const provider = entry.factory();
        for await (const chunk of provider.sendQuery(
          prompt,
          cwd,
          resumeSessionId,
          admittedOptions
        )) {
          if (chunk.type === 'result') {
            resultSeen = true;
            terminalSignal = signalForResult(entry.id, request, lease, chunk);
          }
          if (chunk.type === 'human_input_request') {
            terminalSignal = signalForHumanInputRequest(entry.id, request, lease, chunk);
            yield { ...chunk, signal: terminalSignal };
            continue;
          }
          if (chunk.type === 'task_started') backgroundTasks.add(chunk.taskId);
          if (chunk.type === 'task_notification') backgroundTasks.delete(chunk.taskId);
          if (chunk.type === 'background_tasks') {
            backgroundTasks.clear();
            for (const task of chunk.tasks) backgroundTasks.add(task.taskId);
          }
          // Some workflow consumers stop at the result. That is safe only if
          // the provider has already proved native closure before yielding it.
          closed =
            resultSeen &&
            transportClosed &&
            backgroundTasks.size === 0 &&
            !admittedOptions?.abortSignal?.aborted;
          yield chunk;
          if (chunk.type === 'result' && terminalSignal) {
            yield { type: 'factory_observation', signal: terminalSignal };
          }
        }
        closed =
          resultSeen &&
          transportClosed &&
          backgroundTasks.size === 0 &&
          !admittedOptions?.abortSignal?.aborted;
        if (!closed) {
          terminalSignal ??= signalForUncertainTermination(
            entry.id,
            request,
            lease,
            resultSeen ? 'factory_provider_transport_uncertain' : 'factory_provider_result_missing'
          );
          throw new Error('factory_provider_transport_uncertain');
        }
      } finally {
        for (const timer of limitTimers) clearTimeout(timer);
        const signals = [
          terminalSignal ??
            signalForUncertainTermination(entry.id, request, lease, 'factory_provider_exception'),
        ];
        await settleLease(broker, lease, closed, signals);
      }
    },
  };
}
