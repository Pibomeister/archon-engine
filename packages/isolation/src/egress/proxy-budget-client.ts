import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type {
  BudgetReservation,
  BudgetStatus,
  MarkReservationUnknownInput,
  ProxyBudgetGrant,
  ReserveBudgetInput,
  SettleBudgetInput,
} from './proxy-budget-ledger';

export interface ProxyBudgetClient {
  ready(): Promise<void>;
  reserveBudget(
    input: Omit<ReserveBudgetInput, 'nowMs'>,
    options?: ProxyBudgetClientCallOptions
  ): Promise<BudgetReservation>;
  settleBudget(
    input: SettleBudgetInput,
    options?: ProxyBudgetClientCallOptions
  ): Promise<BudgetReservation>;
  markReservationUnknown(
    input: MarkReservationUnknownInput,
    options?: ProxyBudgetClientCallOptions
  ): Promise<BudgetReservation>;
  getReservation(
    reservationId: string,
    options?: ProxyBudgetClientCallOptions
  ): Promise<BudgetReservation>;
  getBudgetStatus(options?: ProxyBudgetClientCallOptions): Promise<BudgetStatus>;
  close(): Promise<void>;
}

export interface ProxyBudgetClientCallOptions {
  signal?: AbortSignal;
}

export interface ProxyBudgetClientTestOptions {
  executable: string;
  args: string[];
  env?: Record<string, string>;
  cwd?: string;
  readyTimeoutMs?: number;
  operationTimeoutMs?: number;
  wallTimeoutMs?: number;
  wallDeadlineEpochMs?: number;
  maxLineBytes?: number;
  maxOutputBacklogBytes?: number;
}

type PrivateClientOptions = ProxyBudgetClientTestOptions;

interface PendingRequest {
  id: string;
  command: string;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  abortListener?: () => void;
}

interface ResponseEnvelope {
  id?: string;
  ok: boolean;
  result?: unknown;
  error?: string;
  event?: string;
  mode?: string;
}

const FIXED_BUN = '/usr/local/bin/bun';
const READY_TIMEOUT_MS = 5_000;
const OPERATION_TIMEOUT_MS = 30_000;
const WALL_TIMEOUT_MS = 300_000;
const MAX_TIMEOUT_MS = 2_147_483_647;
const MAX_LINE_BYTES = 16_384;
const MAX_OUTPUT_BACKLOG_BYTES = 65_536;
const MINIMAL_ENV = Object.freeze({ PATH: '/usr/local/bin:/usr/bin:/bin' });

export interface ProxyBudgetClientOptions {
  deadlineEpochMs?: number;
}

export function createProxyBudgetClient(options: ProxyBudgetClientOptions = {}): ProxyBudgetClient {
  return new StdioProxyBudgetClient({
    executable: FIXED_BUN,
    args: [fixedLedgerCliPath()],
    env: { ...MINIMAL_ENV },
    readyTimeoutMs: timeoutFromDeadline(options.deadlineEpochMs, READY_TIMEOUT_MS),
    operationTimeoutMs: timeoutFromDeadline(options.deadlineEpochMs, OPERATION_TIMEOUT_MS),
    wallDeadlineEpochMs: options.deadlineEpochMs,
    maxLineBytes: MAX_LINE_BYTES,
    maxOutputBacklogBytes: MAX_OUTPUT_BACKLOG_BYTES,
  });
}

export function createProxyBudgetClientForTest(
  options: ProxyBudgetClientTestOptions
): ProxyBudgetClient {
  return new StdioProxyBudgetClient(options);
}

class StdioProxyBudgetClient implements ProxyBudgetClient {
  private readonly child;
  private readonly readyPromise: Promise<void>;
  private readonly operationTimeoutMs: number;
  private readonly maxLineBytes: number;
  private readonly maxOutputBacklogBytes: number;
  private readonly pending = new Map<string, PendingRequest>();
  private buffer = Buffer.alloc(0);
  private closed = false;
  private childExited = false;
  private readyResolved = false;
  private readyReject!: (error: Error) => void;
  private readyResolve!: () => void;
  private wallTimer: ReturnType<typeof setTimeout>;

  constructor(options: PrivateClientOptions) {
    this.operationTimeoutMs = options.operationTimeoutMs ?? OPERATION_TIMEOUT_MS;
    this.maxLineBytes = options.maxLineBytes ?? MAX_LINE_BYTES;
    this.maxOutputBacklogBytes = options.maxOutputBacklogBytes ?? MAX_OUTPUT_BACKLOG_BYTES;
    this.readyPromise = new Promise<void>((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });
    this.child = spawn(options.executable, options.args, {
      cwd: options.cwd,
      env: options.env ?? { ...MINIMAL_ENV },
      shell: false,
      stdio: ['pipe', 'pipe', 'ignore'],
    });
    this.child.stdout.on('data', chunk => {
      this.receiveChunk(chunk);
    });
    this.child.once('error', error => {
      this.failClosed(error);
    });
    this.child.once('exit', (code, signal) => {
      this.handleExit(code, signal);
    });
    this.wallTimer = this.startWallTimer(options);
    this.startReadyTimer(options.readyTimeoutMs ?? READY_TIMEOUT_MS);
  }

  ready(): Promise<void> {
    return this.readyPromise;
  }

  async reserveBudget(
    input: Omit<ReserveBudgetInput, 'nowMs'>,
    options: ProxyBudgetClientCallOptions = {}
  ): Promise<BudgetReservation> {
    const result = await this.call('reserve', input, options);
    return this.readReservationResult(result, {
      requestHash: input.requestHash,
      inputCeiling: input.inputCeiling,
      outputCeiling: input.outputCeiling,
      status: 'pending',
    });
  }

  async settleBudget(
    input: SettleBudgetInput,
    options: ProxyBudgetClientCallOptions = {}
  ): Promise<BudgetReservation> {
    const result = await this.call('settle', input, options);
    return this.readReservationResult(result, {
      reservationId: input.reservationId,
      status: 'settled',
    });
  }

  async markReservationUnknown(
    input: MarkReservationUnknownInput,
    options: ProxyBudgetClientCallOptions = {}
  ): Promise<BudgetReservation> {
    const result = await this.call('markUnknown', input, options);
    return this.readReservationResult(result, {
      reservationId: input.reservationId,
      status: 'unknown',
    });
  }

  async getReservation(
    reservationId: string,
    options: ProxyBudgetClientCallOptions = {}
  ): Promise<BudgetReservation> {
    const result = await this.call('getReservation', { reservationId }, options);
    return this.readReservationResult(result, { reservationId });
  }

  async getBudgetStatus(options: ProxyBudgetClientCallOptions = {}): Promise<BudgetStatus> {
    return this.readStatusResult(await this.call('status', {}, options));
  }

  async close(): Promise<void> {
    this.rejectPending(new Error('Proxy budget client is closed.'));
    this.closed = true;
    clearTimeout(this.wallTimer);
    this.child.stdin.destroy();
    if (this.childExited || !this.child.kill('SIGTERM')) return;
    setTimeout(() => {
      if (!this.childExited) this.child.kill('SIGKILL');
    }, 100).unref();
  }

  private readReservationResult(
    result: unknown,
    expected: Partial<BudgetReservation>
  ): BudgetReservation {
    try {
      const record = readObject(result, 'Budget reservation result');
      const reservation = {
        reservationId: readString(record.reservationId, 'reservationId'),
        requestHash: readString(record.requestHash, 'requestHash'),
        inputCeiling: readPositiveInteger(record.inputCeiling, 'inputCeiling'),
        outputCeiling: readPositiveInteger(record.outputCeiling, 'outputCeiling'),
        status: readReservationStatus(record.status),
        createdAtMs: readNonNegativeInteger(record.createdAtMs, 'createdAtMs'),
      };
      assertExpectedReservation(reservation, expected);
      return reservation;
    } catch (error) {
      const normalized = normalizeProtocolError(error);
      this.failClosed(normalized);
      throw normalized;
    }
  }

  private readStatusResult(result: unknown): BudgetStatus {
    try {
      const record = readObject(result, 'Budget status result');
      const status = {
        grant: readGrantResult(record.grant),
        pendingReservations: readNonNegativeInteger(
          record.pendingReservations,
          'pendingReservations'
        ),
        unknownReservations: readNonNegativeInteger(
          record.unknownReservations,
          'unknownReservations'
        ),
        consumedInputTokens: readNonNegativeInteger(
          record.consumedInputTokens,
          'consumedInputTokens'
        ),
        consumedOutputTokens: readNonNegativeInteger(
          record.consumedOutputTokens,
          'consumedOutputTokens'
        ),
        consumedTotalTokens: readNonNegativeInteger(
          record.consumedTotalTokens,
          'consumedTotalTokens'
        ),
        remainingInputTokens: readNonNegativeInteger(
          record.remainingInputTokens,
          'remainingInputTokens'
        ),
        remainingOutputTokens: readNonNegativeInteger(
          record.remainingOutputTokens,
          'remainingOutputTokens'
        ),
        remainingTotalTokens: readNonNegativeInteger(
          record.remainingTotalTokens,
          'remainingTotalTokens'
        ),
        acceptingReservations: readBoolean(record.acceptingReservations, 'acceptingReservations'),
      };
      return status;
    } catch (error) {
      const normalized = normalizeProtocolError(error);
      this.failClosed(normalized);
      throw normalized;
    }
  }

  private async call(
    command: string,
    payload: object,
    options: ProxyBudgetClientCallOptions
  ): Promise<unknown> {
    await this.ready();
    this.assertCallable();
    if (this.pending.size > 0)
      throw new Error('Proxy budget client already has an outstanding request.');
    const id = randomUUID();
    const line = `${JSON.stringify({ id, command, payload })}\n`;
    return await this.sendRequest(id, command, line, options.signal);
  }

  private sendRequest(
    id: string,
    command: string,
    line: string,
    signal: AbortSignal | undefined
  ): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const timer = this.startOperationTimer(id);
      const pending: PendingRequest = { id, command, resolve, reject, timer, signal };
      pending.abortListener = (): void => {
        this.cancelPending(id, 'Proxy budget operation was cancelled.');
      };
      if (signal?.aborted) {
        clearTimeout(timer);
        const error = new Error('Proxy budget operation was cancelled.');
        reject(error);
        this.failClosed(error);
        return;
      }
      signal?.addEventListener('abort', pending.abortListener, { once: true });
      this.pending.set(id, pending);
      if (!this.child.stdin.write(line)) {
        this.child.stdin.once('drain', () => {
          return undefined;
        });
      }
    });
  }

  private receiveChunk(chunk: Buffer): void {
    if (this.closed) return;
    if (chunk.length > this.maxOutputBacklogBytes) {
      this.failClosed(new Error('Proxy budget child output exceeded backlog limit.'));
      return;
    }
    this.buffer = Buffer.concat([this.buffer, chunk]);
    if (this.buffer.length > this.maxOutputBacklogBytes) {
      this.failClosed(new Error('Proxy budget child output exceeded backlog limit.'));
      return;
    }
    this.processBufferedLines();
  }

  private processBufferedLines(): void {
    let newline = this.buffer.indexOf(0x0a);
    while (newline >= 0) {
      const line = this.buffer.subarray(0, newline);
      this.buffer = this.buffer.subarray(newline + 1);
      if (line.length > this.maxLineBytes) {
        this.failClosed(new Error('Proxy budget child response line is too large.'));
        return;
      }
      this.handleResponseLine(line.toString('utf8'));
      if (this.closed) return;
      newline = this.buffer.indexOf(0x0a);
    }
    if (this.buffer.length > this.maxLineBytes) {
      this.failClosed(new Error('Proxy budget child response line is too large.'));
    }
  }

  private handleResponseLine(line: string): void {
    let response: ResponseEnvelope;
    try {
      response = JSON.parse(line) as ResponseEnvelope;
    } catch {
      this.failClosed(new Error('Proxy budget child returned malformed JSON.'));
      return;
    }
    if (!this.readyResolved) {
      this.handleReady(response);
      return;
    }
    this.handleOperationResponse(response);
  }

  private handleReady(response: ResponseEnvelope): void {
    if (!response.ok || response.event !== 'ready' || typeof response.mode !== 'string') {
      this.failClosed(new Error('Proxy budget child did not send a valid ready event.'));
      return;
    }
    this.readyResolved = true;
    this.readyResolve();
  }

  private handleOperationResponse(response: ResponseEnvelope): void {
    if (typeof response.id !== 'string' || response.id.length === 0) {
      this.failClosed(new Error('Proxy budget child response is missing an operation ID.'));
      return;
    }
    const first = this.pending.values().next().value;
    if (first?.id !== response.id) {
      this.failClosed(new Error('Proxy budget child response ID is not the outstanding request.'));
      return;
    }
    this.pending.delete(response.id);
    this.finishPending(first, response);
  }

  private finishPending(pending: PendingRequest, response: ResponseEnvelope): void {
    this.cleanupPending(pending);
    if (typeof response.ok !== 'boolean') {
      const error = new Error('Proxy budget child returned an invalid response envelope.');
      pending.reject(error);
      this.failClosed(error);
      return;
    }
    if (response.ok) {
      pending.resolve(response.result);
      return;
    }
    if (typeof response.error === 'string') {
      pending.reject(new Error(response.error));
      return;
    }
    const error = new Error('Proxy budget child returned an invalid response envelope.');
    pending.reject(error);
    this.failClosed(error);
  }

  private cancelPending(id: string, message: string): void {
    const pending = this.pending.get(id);
    if (!pending) return;
    this.pending.delete(id);
    this.cleanupPending(pending);
    pending.reject(new Error(message));
    this.failClosed(new Error(message));
  }

  private cleanupPending(pending: PendingRequest): void {
    clearTimeout(pending.timer);
    const listener = pending.abortListener;
    if (listener) pending.signal?.removeEventListener('abort', listener);
  }

  private startReadyTimer(timeoutMs: number): void {
    const timer = setTimeout(() => {
      if (!this.readyResolved)
        this.failClosed(new Error('Proxy budget child readiness timed out.'));
    }, timeoutMs);
    timer.unref();
    this.readyPromise
      .finally(() => {
        clearTimeout(timer);
      })
      .catch(() => {
        return undefined;
      });
  }

  private startOperationTimer(id: string): ReturnType<typeof setTimeout> {
    const timer = setTimeout(() => {
      this.cancelPending(id, 'Proxy budget operation timed out.');
    }, this.operationTimeoutMs);
    timer.unref();
    return timer;
  }

  private startWallTimer(options: PrivateClientOptions): ReturnType<typeof setTimeout> {
    const timeoutMs = nextWallTimeoutMs(options);
    const timer = setTimeout(() => {
      if (options.wallDeadlineEpochMs === undefined || Date.now() >= options.wallDeadlineEpochMs) {
        this.failClosed(new Error('Proxy budget child wall timeout expired.'));
        return;
      }
      this.wallTimer = this.startWallTimer(options);
    }, timeoutMs);
    timer.unref();
    return timer;
  }

  private handleExit(code: number | null, signal: NodeJS.Signals | null): void {
    this.childExited = true;
    const reason = signal ? `signal ${signal}` : `code ${code ?? 'unknown'}`;
    this.failClosed(new Error(`Proxy budget child exited before close (${reason}).`));
  }

  private failClosed(error: Error): void {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.wallTimer);
    this.readyReject(error);
    this.rejectPending(error);
    this.child.stdin.destroy();
    this.child.kill('SIGTERM');
    setTimeout(() => {
      if (!this.childExited) this.child.kill('SIGKILL');
    }, 100).unref();
  }

  private rejectPending(error: Error): void {
    for (const pending of this.pending.values()) {
      this.cleanupPending(pending);
      pending.reject(error);
    }
    this.pending.clear();
  }

  private assertCallable(): void {
    if (this.closed) throw new Error('Proxy budget client is closed.');
  }
}

function nextWallTimeoutMs(options: PrivateClientOptions): number {
  if (options.wallDeadlineEpochMs !== undefined) {
    return Math.min(deadlineRemaining(options.wallDeadlineEpochMs), MAX_TIMEOUT_MS);
  }
  return Math.min(options.wallTimeoutMs ?? WALL_TIMEOUT_MS, MAX_TIMEOUT_MS);
}

function timeoutFromDeadline(deadlineEpochMs: number | undefined, capMs: number): number {
  if (deadlineEpochMs === undefined) return capMs;
  return Math.min(capMs, deadlineRemaining(deadlineEpochMs));
}

function deadlineRemaining(deadlineEpochMs: number): number {
  if (!Number.isSafeInteger(deadlineEpochMs) || deadlineEpochMs <= Date.now()) {
    throw new Error('Proxy budget deadline is invalid or expired.');
  }
  return deadlineEpochMs - Date.now();
}

function readGrantResult(value: unknown): ProxyBudgetGrant {
  const record = readObject(value, 'Budget status grant');
  return {
    schema: readGrantSchema(record.schema),
    rootChainId: readString(record.rootChainId, 'rootChainId'),
    runId: readString(record.runId, 'runId'),
    workflowDigest: readString(record.workflowDigest, 'workflowDigest'),
    policyDigest: readString(record.policyDigest, 'policyDigest'),
    deadlineEpochMs: readPositiveInteger(record.deadlineEpochMs, 'deadlineEpochMs'),
    inputTokenLimit: readNonNegativeInteger(record.inputTokenLimit, 'inputTokenLimit'),
    outputTokenLimit: readNonNegativeInteger(record.outputTokenLimit, 'outputTokenLimit'),
    totalTokenLimit: readNonNegativeInteger(record.totalTokenLimit, 'totalTokenLimit'),
  };
}

function readGrantSchema(value: unknown): ProxyBudgetGrant['schema'] {
  if (value !== 'archon.proxy-budget-grant.v1') throw new Error('Budget status grant is invalid.');
  return value;
}

function readObject(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function readString(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.length === 0)
    throw new Error(`${field} must be a non-empty string.`);
  return value;
}

function readPositiveInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || typeof value !== 'number' || value <= 0) {
    throw new Error(`${field} must be a positive safe integer.`);
  }
  return value;
}

function readNonNegativeInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || typeof value !== 'number' || value < 0) {
    throw new Error(`${field} must be a non-negative safe integer.`);
  }
  return value;
}

function readBoolean(value: unknown, field: string): boolean {
  if (typeof value !== 'boolean') throw new Error(`${field} must be a boolean.`);
  return value;
}

function readReservationStatus(value: unknown): BudgetReservation['status'] {
  if (value === 'pending' || value === 'settled' || value === 'unknown') return value;
  throw new Error('Reservation status is invalid.');
}

function assertExpectedReservation(
  reservation: BudgetReservation,
  expected: Partial<BudgetReservation>
): void {
  for (const [key, value] of Object.entries(expected)) {
    if (reservation[key as keyof BudgetReservation] !== value) {
      throw new Error('Budget reservation result does not match the requested operation.');
    }
  }
}

function normalizeProtocolError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new Error('Proxy budget child returned invalid result data.');
}

function fixedLedgerCliPath(): string {
  return join(dirname(fileURLToPath(import.meta.url)), 'proxy-budget-ledger-cli.ts');
}
