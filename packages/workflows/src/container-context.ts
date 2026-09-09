/**
 * Container run context shared by the executor and dag-executor (Phase C).
 *
 * Lives in its own module so both `executor.ts` (which threads it in from the
 * caller) and `dag-executor.ts` (which drives suspend + the write-back gate) can
 * import it without an import cycle between those two large files.
 */

import type {
  WriteBackFinalizeResult,
  WriteBackApplySummary,
  ExecutionContext,
} from '@archon/providers/types';
import { execFileAsync } from '@archon/git';
import { randomUUID } from 'crypto';
import { writeFile } from 'fs/promises';
import { join } from 'path';

/**
 * The container-backend methods the engine drives directly for the write-back
 * gate + pause economics. A STRUCTURAL port: the container backend from
 * `@archon/isolation` implements exactly these (plus prepare/resumeEnv/destroy the
 * CALLER drives across process boundaries), so the engine consumes them without
 * importing that package — mirroring the `ExecutionContext` contract split.
 */
export interface ContainerWriteBackBackend {
  /** Stops owned writers and exports diagnostic artifacts, never release authority. */
  snapshotArtifacts?(envId: string, destinationDir: string): Promise<ContainerArtifactSnapshot>;
  /** Controller-verified proxy ledger status for hardened egress budget authority. */
  readProxyBudgetStatus?(
    envId: string,
    binding: VerifiedProxyBudgetBinding
  ): Promise<VerifiedProxyBudgetStatus>;
  /** `docker stop` on pause; the upper volume persists for resume. */
  suspend(envId: string): Promise<void>;
  /** Inspect the overlay diff → whether a write-back gate is warranted + summary. */
  finalize(envId: string): Promise<WriteBackFinalizeResult>;
  /** Apply the overlay diff to the live root (the ONE live-root write). */
  applyChanges(envId: string): Promise<WriteBackApplySummary>;
  /** Discard the overlay diff (live root untouched). */
  discardChanges(envId: string): Promise<void>;
}

/**
 * Container run context threaded from the caller (CLI/orchestrator) into the
 * engine. Present only for folder-project container runs; absent for host runs.
 * `envId` is the prepared `isolation_environments` row the write-back methods act
 * on; the executor also stamps it into the run metadata so a later resume (a
 * separate process) can rediscover the container.
 */
export interface ContainerRunContext {
  envId: string;
  /** `approve` (default) pauses at the write-back gate; `auto` applies without pausing. */
  writeBack: 'approve' | 'auto';
  backend: ContainerWriteBackBackend;
  /** Present only when strict provider egress has a controller-signed proxy budget seed. */
  proxyBudgetSeedDigest?: string;
  /** Base64 frozen strict egress policy bound into the signed controller metadata. */
  egressPolicyB64?: string;
  /** Resolved immutable image id bound into the signed controller metadata. */
  image?: string;
  /** Controller run id bound into Docker ownership labels and signed metadata. */
  ownerRunId?: string;
  /**
   * Overlay mount mode in effect. `native` (CAP_SYS_ADMIN — the common fallback on
   * stock daemons) lets in-container root remount the read-only lower read-write, so
   * an adversarial agent could bypass the write-back gate. The engine emits a loud
   * run-start warning when this is `native` (H4; see SECURITY.md).
   */
  overlayMode?: 'fuse' | 'native';
}

/** Synthetic node id for the engine-level write-back gate (there is no DAG node). */
export const WRITEBACK_GATE_NODE_ID = '__writeback__';

export interface ContainerArtifactSnapshot {
  snapshotDir: string;
  image: string;
  totalBytes: number;
  files: { path: string; size: number; sha256: string; mode: number }[];
}

export interface VerifiedProxyBudgetBinding {
  egressPolicyB64: string;
  image: string;
  ownerRunId: string;
  proxyBudgetSeedDigest: string;
}

export interface VerifiedProxyBudgetStatus {
  source: 'controller-proxy-ledger';
  envId: string;
  grant: {
    schema: 'archon.proxy-budget-grant.v1';
    rootChainId: string;
    runId: string;
    workflowDigest: string;
    policyDigest: string;
    deadlineEpochMs: number;
    inputTokenLimit: number;
    outputTokenLimit: number;
    totalTokenLimit: number;
  };
  consumed: { input: number; output: number };
  pendingReservations: number;
  unknownReservations: number;
  acceptingReservations: boolean;
}

export type ContainerSubprocessStatus = string | null;

export interface GuardedContainerSubprocessOptions {
  timeout: number;
  deadlineAt?: number;
  getRunStatus?: () => Promise<ContainerSubprocessStatus> | ContainerSubprocessStatus;
  statusPollMs?: number;
}

const DEFAULT_STATUS_POLL_MS = 250;
const STOP_TIMEOUT_MS = 30_000;

export async function runGuardedContainerSubprocess(
  execContext: ExecutionContext,
  dockerArgs: string[],
  options: GuardedContainerSubprocessOptions
): Promise<{ stdout: string; stderr: string }> {
  if (execContext.kind !== 'container') {
    throw new Error('Guarded container subprocess requires a container execution context');
  }
  const timeout = clampTimeoutToDeadline(options.timeout, options.deadlineAt);
  if (timeout <= 0) throw new Error('Container subprocess deadline expired before spawn');

  const guard = createSubprocessGuard(execContext.containerId);
  const monitor = monitorGuard(guard, options, timeout);
  const exec = execFileAsync('docker', dockerArgs, { timeout });
  return await settleGuardedExec(exec, monitor, guard);
}

/** Call only after the DAG has drained all active nodes. Snapshots are advisory. */
export async function snapshotDrainedContainerArtifacts(
  context: ContainerRunContext | undefined,
  execution: ExecutionContext,
  controllerArtifactsDir: string
): Promise<void> {
  if (execution.kind !== 'container' || !execution.agentArtifactsDir) return;
  if (!context?.backend.snapshotArtifacts) {
    throw new Error('Container artifact export is unavailable; refusing unsnapshotted completion');
  }
  const destination = join(controllerArtifactsDir, `agent-snapshot-${randomUUID()}`);
  const snapshot = await context.backend.snapshotArtifacts(context.envId, destination);
  if (snapshot.snapshotDir !== destination) {
    throw new Error('Container artifact snapshot destination mismatch');
  }
  await writeFile(
    `${destination}.json`,
    JSON.stringify({
      kind: 'advisory-agent-artifacts',
      authority: 'none',
      terminal: false,
      ...snapshot,
    }) + '\n',
    { flag: 'wx', mode: 0o600 }
  );
}

interface SubprocessGuard {
  containerId: string;
  finished: boolean;
  refusal?: Error;
  timers: NodeJS.Timeout[];
  stopPromise?: Promise<void>;
}

type ExecSettled =
  | { ok: true; result: { stdout: string; stderr: string } }
  | { ok: false; error: Error };

function createSubprocessGuard(containerId: string): SubprocessGuard {
  if (containerId.trim().length === 0) throw new Error('Container subprocess id is required');
  return { containerId, finished: false, timers: [] };
}

async function settleGuardedExec(
  exec: Promise<{ stdout: string; stderr: string }>,
  monitor: Promise<void>,
  guard: SubprocessGuard
): Promise<{ stdout: string; stderr: string }> {
  try {
    const settled = await waitForExecOrRefusal(exec, monitor, guard);
    if (guard.refusal) await throwAfterStop(guard);
    if (!settled.ok) return await handleExecFailure(settled.error, guard);
    return settled.result;
  } finally {
    guard.finished = true;
    clearGuardTimers(guard);
    void monitor.catch(() => undefined);
  }
}

async function waitForExecOrRefusal(
  exec: Promise<{ stdout: string; stderr: string }>,
  monitor: Promise<void>,
  guard: SubprocessGuard
): Promise<ExecSettled> {
  const execSettled = exec.then(toExecSuccess, toExecFailure);
  await Promise.race([execSettled, monitor]);
  if (guard.refusal) await throwAfterStop(guard);
  return await execSettled;
}

function toExecSuccess(result: { stdout: string; stderr: string }): ExecSettled {
  return { ok: true, result };
}

function toExecFailure(error: unknown): ExecSettled {
  return { ok: false, error: toError(error) };
}

async function handleExecFailure(
  error: Error,
  guard: SubprocessGuard
): Promise<{ stdout: string; stderr: string }> {
  if (isLocalTimeoutError(error)) {
    await refuseAndStop(guard, new Error('Container subprocess timed out'));
    await throwAfterStop(guard);
  }
  throw error;
}

async function throwAfterStop(guard: SubprocessGuard): Promise<never> {
  if (guard.stopPromise) await guard.stopPromise;
  throw guard.refusal ?? new Error('Container subprocess stopped');
}

async function monitorGuard(
  guard: SubprocessGuard,
  options: GuardedContainerSubprocessOptions,
  timeout: number
): Promise<void> {
  try {
    await Promise.race([timeoutGuard(guard, timeout), statusGuard(guard, options)]);
  } catch (err) {
    if (guard.finished) return;
    await refuseAndStop(guard, toError(err));
  }
}

function timeoutGuard(guard: SubprocessGuard, timeout: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      removeGuardTimer(guard, timer);
      refuseAndStop(guard, new Error('Container subprocess timed out')).then(resolve, reject);
    }, timeout);
    guard.timers.push(timer);
  });
}

async function statusGuard(
  guard: SubprocessGuard,
  options: GuardedContainerSubprocessOptions
): Promise<void> {
  if (!options.getRunStatus) {
    await neverSettles();
    return;
  }
  const pollMs = normalizePollMs(options.statusPollMs);
  while (!guard.finished) {
    await sleep(guard, pollMs);
    if (guard.finished) return;
    await checkRunStatus(guard, options.getRunStatus);
  }
}

async function checkRunStatus(
  guard: SubprocessGuard,
  getRunStatus: () => Promise<ContainerSubprocessStatus> | ContainerSubprocessStatus
): Promise<void> {
  const status = await getRunStatus();
  if (guard.finished) return;
  if (status === 'running') return;
  if (status === null) throw new Error('Container subprocess run status unavailable');
  throw new Error(`Container subprocess stopped by run status '${status}'`);
}

async function refuseAndStop(guard: SubprocessGuard, error: Error): Promise<void> {
  guard.refusal ??= error;
  await stopOwnedContainer(guard);
}

async function stopOwnedContainer(guard: SubprocessGuard): Promise<void> {
  guard.stopPromise ??= execFileAsync('docker', ['stop', guard.containerId], {
    timeout: STOP_TIMEOUT_MS,
  }).then(() => undefined);
  await guard.stopPromise;
}

function clampTimeoutToDeadline(timeout: number, deadlineAt: number | undefined): number {
  if (!Number.isFinite(timeout) || timeout <= 0) {
    throw new Error('Invalid container subprocess timeout');
  }
  if (deadlineAt === undefined) return timeout;
  if (!Number.isFinite(deadlineAt)) throw new Error('Invalid container subprocess deadline');
  return Math.min(timeout, Math.max(0, deadlineAt - Date.now()));
}

function normalizePollMs(value: number | undefined): number {
  if (value === undefined) return DEFAULT_STATUS_POLL_MS;
  if (!Number.isInteger(value) || value <= 0 || value > 5000) {
    throw new Error('Invalid container subprocess status poll interval');
  }
  return value;
}

function sleep(guard: SubprocessGuard, ms: number): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(() => {
      removeGuardTimer(guard, timer);
      resolve();
    }, ms);
    guard.timers.push(timer);
  });
}

function removeGuardTimer(guard: SubprocessGuard, timer: NodeJS.Timeout): void {
  guard.timers = guard.timers.filter(candidate => candidate !== timer);
}

function clearGuardTimers(guard: SubprocessGuard): void {
  for (const timer of guard.timers.splice(0)) clearTimeout(timer);
}

function neverSettles(): Promise<void> {
  return new Promise(() => undefined);
}

function isLocalTimeoutError(error: Error): boolean {
  const maybeTimeout = error as Error & { killed?: unknown; signal?: unknown };
  return maybeTimeout.killed === true || maybeTimeout.signal === 'SIGTERM';
}

function toError(error: unknown): Error {
  if (error instanceof Error) return error;
  return new Error(String(error));
}
