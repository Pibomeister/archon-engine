import { Database } from 'bun:sqlite';
import { existsSync, mkdirSync } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { dirname } from 'node:path';

export interface ProxyBudgetGrant {
  schema: 'archon.proxy-budget-grant.v1';
  rootChainId: string;
  runId: string;
  workflowDigest: string;
  policyDigest: string;
  deadlineEpochMs: number;
  inputTokenLimit: number;
  outputTokenLimit: number;
  totalTokenLimit: number;
}

export interface ProxyBudgetLedgerOptions {
  mode: 'create' | 'resume' | 'status';
  dbPath: string;
  grant: ProxyBudgetGrant;
}

export interface ReserveBudgetInput {
  requestHash: string;
  inputCeiling: number;
  outputCeiling: number;
  nowMs?: number;
}

export interface BudgetReservation {
  reservationId: string;
  requestHash: string;
  inputCeiling: number;
  outputCeiling: number;
  status: 'pending' | 'settled' | 'unknown';
  createdAtMs: number;
}

export interface SettleBudgetInput {
  reservationId: string;
  inputTokens: number;
  outputTokens: number;
}

export interface MarkReservationUnknownInput {
  reservationId: string;
  reason: string;
}

export interface BudgetStatus {
  grant: ProxyBudgetGrant;
  pendingReservations: number;
  unknownReservations: number;
  consumedInputTokens: number;
  consumedOutputTokens: number;
  consumedTotalTokens: number;
  remainingInputTokens: number;
  remainingOutputTokens: number;
  remainingTotalTokens: number;
  acceptingReservations: boolean;
}

interface MetadataRow {
  value: string;
}

interface ReservationRow {
  id: string;
  request_hash: string;
  input_ceiling: number;
  output_ceiling: number;
  status: 'pending' | 'settled' | 'unknown';
  created_at_ms: number;
  unknown_reason: string | null;
}

interface SettlementRow {
  reservation_id: string;
  input_tokens: number;
  output_tokens: number;
}

interface UsageTotals {
  input: number;
  output: number;
  total: number;
}

const SCHEMA_VERSION = '1';
const MAX_REASON_BYTES = 512;

export class ProxyBudgetLedger {
  private readonly db: Database;
  private readonly grant: ProxyBudgetGrant;
  private readonly grantDigest: string;
  private closed = false;

  private constructor(db: Database, grant: ProxyBudgetGrant) {
    this.db = db;
    this.grant = Object.freeze({ ...grant });
    this.grantDigest = digestJson(this.grant);
  }

  static open(options: ProxyBudgetLedgerOptions): ProxyBudgetLedger {
    const grant = normalizeGrant(options.grant);
    if (options.mode !== 'create' && !existsSync(options.dbPath)) {
      throw new Error('Budget ledger is not initialized.');
    }
    mkdirSync(dirname(options.dbPath), { recursive: true });
    const db = new Database(options.dbPath, { create: options.mode === 'create', readwrite: true });
    configureDatabase(db, options.mode);
    const ledger = new ProxyBudgetLedger(db, grant);
    try {
      ledger.initialize(options.mode);
      return ledger;
    } catch (error) {
      db.close();
      throw error;
    }
  }

  reserveBudget(input: ReserveBudgetInput): BudgetReservation {
    this.assertOpen();
    const requestHash = normalizeBoundedString(input.requestHash, 'Request hash', 256);
    const inputCeiling = assertPositiveSafeInteger(input.inputCeiling, 'Input reservation ceiling');
    const outputCeiling = assertPositiveSafeInteger(
      input.outputCeiling,
      'Output reservation ceiling'
    );
    const nowMs = assertNow(input.nowMs ?? Date.now());
    return this.writeTransaction(() => {
      this.assertMetadataBinding();
      this.assertDeadline(nowMs);
      this.assertNoUnsafeOpenReservations();
      const totals = this.calculateUsageTotals();
      assertCanAdd(totals.input, inputCeiling, 'Input reservation total');
      assertCanAdd(totals.output, outputCeiling, 'Output reservation total');
      const reserveTotal = safeAdd(inputCeiling, outputCeiling, 'Reservation total');
      assertCanAdd(totals.total, reserveTotal, 'Overall reservation total');
      if (totals.input + inputCeiling > this.grant.inputTokenLimit) {
        throw new Error('Input token budget exhausted.');
      }
      if (totals.output + outputCeiling > this.grant.outputTokenLimit) {
        throw new Error('Output token budget exhausted.');
      }
      if (totals.total + reserveTotal > this.grant.totalTokenLimit) {
        throw new Error('Total token budget exhausted.');
      }
      const reservationId = randomUUID();
      this.db
        .query(
          `INSERT INTO proxy_budget_reservations
           (id, request_hash, input_ceiling, output_ceiling, status, created_at_ms)
           VALUES (?, ?, ?, ?, 'pending', ?)`
        )
        .run(reservationId, requestHash, inputCeiling, outputCeiling, nowMs);
      return {
        reservationId,
        requestHash,
        inputCeiling,
        outputCeiling,
        status: 'pending',
        createdAtMs: nowMs,
      };
    });
  }

  settleBudget(input: SettleBudgetInput): BudgetReservation {
    this.assertOpen();
    const reservationId = normalizeBoundedString(input.reservationId, 'Reservation ID', 128);
    const inputTokens = assertNonNegativeSafeInteger(input.inputTokens, 'Settled input tokens');
    const outputTokens = assertNonNegativeSafeInteger(input.outputTokens, 'Settled output tokens');
    return this.writeTransaction(() => {
      this.assertMetadataBinding();
      const reservation = this.requireReservation(reservationId);
      if (reservation.status === 'unknown')
        throw new Error('Unknown reservation cannot be settled.');
      assertWithinReservation(inputTokens, outputTokens, reservation);
      const existing = this.findSettlement(reservationId);
      if (existing) {
        assertSameSettlement(existing, inputTokens, outputTokens);
        return rowToReservation({ ...reservation, status: 'settled' });
      }
      if (reservation.status !== 'pending')
        throw new Error('Reservation has invalid settlement state.');
      this.db
        .query(
          'INSERT INTO proxy_budget_settlements (reservation_id, input_tokens, output_tokens) VALUES (?, ?, ?)'
        )
        .run(reservationId, inputTokens, outputTokens);
      this.db
        .query("UPDATE proxy_budget_reservations SET status = 'settled' WHERE id = ?")
        .run(reservationId);
      return rowToReservation({ ...reservation, status: 'settled' });
    });
  }

  markReservationUnknown(input: MarkReservationUnknownInput): BudgetReservation {
    this.assertOpen();
    const reservationId = normalizeBoundedString(input.reservationId, 'Reservation ID', 128);
    const reason = normalizeBoundedString(input.reason, 'Unknown reason', MAX_REASON_BYTES);
    return this.writeTransaction(() => {
      this.assertMetadataBinding();
      const reservation = this.requireReservation(reservationId);
      if (reservation.status === 'settled')
        throw new Error('Settled reservation cannot be marked unknown.');
      this.db
        .query(
          "UPDATE proxy_budget_reservations SET status = 'unknown', unknown_reason = ? WHERE id = ?"
        )
        .run(reason, reservationId);
      return rowToReservation({ ...reservation, status: 'unknown', unknown_reason: reason });
    });
  }

  getReservation(reservationId: string): BudgetReservation {
    this.assertOpen();
    this.assertMetadataBinding();
    const reservation = this.requireReservation(
      normalizeBoundedString(reservationId, 'Reservation ID', 128)
    );
    this.assertCompleteReservationIntegrity(reservation);
    return rowToReservation(reservation);
  }

  getBudgetStatus(): BudgetStatus {
    this.assertOpen();
    return this.readTransaction(() => {
      this.assertMetadataBinding();
      const totals = this.calculateUsageTotals();
      const pendingReservations = this.countReservations('pending');
      const unknownReservations = this.countReservations('unknown');
      return {
        grant: { ...this.grant },
        pendingReservations,
        unknownReservations,
        consumedInputTokens: totals.input,
        consumedOutputTokens: totals.output,
        consumedTotalTokens: totals.total,
        remainingInputTokens: this.grant.inputTokenLimit - totals.input,
        remainingOutputTokens: this.grant.outputTokenLimit - totals.output,
        remainingTotalTokens: this.grant.totalTokenLimit - totals.total,
        acceptingReservations: pendingReservations === 0 && unknownReservations === 0,
      };
    });
  }

  close(): void {
    if (!this.closed) {
      this.db.close();
      this.closed = true;
    }
  }

  private initialize(mode: 'create' | 'resume' | 'status'): void {
    if (mode === 'status') {
      this.readTransaction(() => {
        if (!this.tableExists('proxy_budget_metadata'))
          throw new Error('Budget ledger is not initialized.');
        this.assertMetadataBinding();
      });
      return;
    }
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const exists = this.tableExists('proxy_budget_metadata');
      if (mode === 'create') {
        if (exists) throw new Error('Budget ledger already initialized.');
        if (this.userTableCount() > 0) throw new Error('Budget ledger create target is not empty.');
        this.createSchema();
        this.writeMetadata('schema_version', SCHEMA_VERSION);
        this.writeMetadata('grant_digest', this.grantDigest);
        this.writeMetadata('grant_json', JSON.stringify(this.grant));
      } else {
        if (!exists) throw new Error('Budget ledger is not initialized.');
        this.assertMetadataBinding();
      }
      this.db.exec('COMMIT');
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  private createSchema(): void {
    this.db.exec(`
      CREATE TABLE proxy_budget_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE proxy_budget_reservations (
        id TEXT PRIMARY KEY,
        request_hash TEXT NOT NULL,
        input_ceiling INTEGER NOT NULL CHECK (input_ceiling > 0),
        output_ceiling INTEGER NOT NULL CHECK (output_ceiling > 0),
        status TEXT NOT NULL CHECK (status IN ('pending', 'settled', 'unknown')),
        created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
        unknown_reason TEXT
      );
      CREATE TABLE proxy_budget_settlements (
        reservation_id TEXT PRIMARY KEY REFERENCES proxy_budget_reservations(id),
        input_tokens INTEGER NOT NULL CHECK (input_tokens >= 0),
        output_tokens INTEGER NOT NULL CHECK (output_tokens >= 0)
      );
    `);
  }

  private assertMetadataBinding(): void {
    const version = this.readMetadata('schema_version');
    const digest = this.readMetadata('grant_digest');
    const grantJson = this.readMetadata('grant_json');
    if (version !== SCHEMA_VERSION) throw new Error('Unsupported budget ledger schema version.');
    if (digest !== this.grantDigest) throw new Error('Budget grant binding drift detected.');
    try {
      if (digestJson(JSON.parse(grantJson)) !== this.grantDigest) {
        throw new Error('Budget grant private state is malformed.');
      }
    } catch (error) {
      if (error instanceof SyntaxError) throw new Error('Budget grant private state is malformed.');
      throw error;
    }
  }

  private calculateUsageTotals(): UsageTotals {
    const rows = this.db
      .query(
        `SELECT r.input_ceiling, r.output_ceiling, r.status, s.input_tokens, s.output_tokens
         FROM proxy_budget_reservations r
         LEFT JOIN proxy_budget_settlements s ON s.reservation_id = r.id`
      )
      .all() as {
      input_ceiling: number;
      output_ceiling: number;
      status: string;
      input_tokens: number | null;
      output_tokens: number | null;
    }[];
    let input = 0;
    let output = 0;
    for (const row of rows) {
      const rowInput = usageInput(row);
      const rowOutput = usageOutput(row);
      input = safeAdd(input, rowInput, 'Ledger input usage');
      output = safeAdd(output, rowOutput, 'Ledger output usage');
    }
    const total = safeAdd(input, output, 'Ledger total usage');
    this.assertTotalsWithinGrant({ input, output, total });
    return { input, output, total };
  }

  private assertCompleteReservationIntegrity(reservation: ReservationRow): void {
    if (reservation.status !== 'settled') return;
    const settlement = this.findSettlement(reservation.id);
    if (!settlement) throw new Error('Budget ledger private state is malformed.');
    assertSettlementRow(settlement, reservation);
  }

  private assertTotalsWithinGrant(totals: UsageTotals): void {
    if (totals.input > this.grant.inputTokenLimit)
      throw new Error('Budget ledger private state is malformed.');
    if (totals.output > this.grant.outputTokenLimit)
      throw new Error('Budget ledger private state is malformed.');
    if (totals.total > this.grant.totalTokenLimit)
      throw new Error('Budget ledger private state is malformed.');
  }

  private assertNoUnsafeOpenReservations(): void {
    if (this.countReservations('pending') > 0) {
      throw new Error('Pending budget reservation blocks additional requests.');
    }
    if (this.countReservations('unknown') > 0) {
      throw new Error('Unknown budget reservation blocks additional requests.');
    }
  }

  private countReservations(status: 'pending' | 'unknown'): number {
    const row = this.db
      .query('SELECT COUNT(*) AS count FROM proxy_budget_reservations WHERE status = ?')
      .get(status) as { count: number };
    return assertNonNegativeSafeInteger(row.count, 'Reservation count');
  }

  private requireReservation(id: string): ReservationRow {
    const row = this.db
      .query(
        `SELECT id, request_hash, input_ceiling, output_ceiling, status, created_at_ms, unknown_reason
         FROM proxy_budget_reservations WHERE id = ?`
      )
      .get(id) as ReservationRow | null;
    if (!row) throw new Error('Budget reservation not found.');
    assertReservationRow(row);
    return row;
  }

  private findSettlement(reservationId: string): SettlementRow | null {
    return this.db
      .query(
        'SELECT reservation_id, input_tokens, output_tokens FROM proxy_budget_settlements WHERE reservation_id = ?'
      )
      .get(reservationId) as SettlementRow | null;
  }

  private userTableCount(): number {
    const row = this.db
      .query(
        "SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
      )
      .get() as { count: number };
    return assertNonNegativeSafeInteger(row.count, 'Ledger table count');
  }

  private tableExists(name: string): boolean {
    const row = this.db
      .query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
      .get(name) as { name: string } | null;
    return row !== null;
  }

  private writeMetadata(key: string, value: string): void {
    this.db.query('INSERT INTO proxy_budget_metadata (key, value) VALUES (?, ?)').run(key, value);
  }

  private readMetadata(key: string): string {
    const row = this.db
      .query('SELECT value FROM proxy_budget_metadata WHERE key = ?')
      .get(key) as MetadataRow | null;
    if (!row) throw new Error('Budget ledger private state is malformed.');
    return row.value;
  }

  private writeTransaction<T>(callback: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      this.assertMetadataBinding();
      this.calculateUsageTotals();
      const result = callback();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  private readTransaction<T>(callback: () => T): T {
    this.db.exec('BEGIN DEFERRED');
    try {
      const result = callback();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  private assertDeadline(nowMs: number): void {
    if (nowMs >= this.grant.deadlineEpochMs) throw new Error('Budget grant deadline has expired.');
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('Budget ledger is closed.');
  }
}

export function createProxyBudgetLedger(options: ProxyBudgetLedgerOptions): ProxyBudgetLedger {
  return ProxyBudgetLedger.open(options);
}

function configureDatabase(db: Database, mode: 'create' | 'resume' | 'status'): void {
  if (mode !== 'status') db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA synchronous = FULL');
  db.exec('PRAGMA foreign_keys = ON');
  db.exec('PRAGMA busy_timeout = 250');
}

function normalizeGrant(input: ProxyBudgetGrant): ProxyBudgetGrant {
  if (!input || typeof input !== 'object') throw new Error('Budget grant must be an object.');
  const allowedKeys = [
    'schema',
    'rootChainId',
    'runId',
    'workflowDigest',
    'policyDigest',
    'deadlineEpochMs',
    'inputTokenLimit',
    'outputTokenLimit',
    'totalTokenLimit',
  ];
  if (Object.keys(input).some(key => !allowedKeys.includes(key))) {
    throw new Error('Budget grant contains unsupported settings.');
  }
  if (input.schema !== 'archon.proxy-budget-grant.v1')
    throw new Error('Unsupported budget grant schema.');
  return {
    schema: input.schema,
    rootChainId: normalizeBoundedString(input.rootChainId, 'Root chain ID', 256),
    runId: normalizeBoundedString(input.runId, 'Run ID', 256),
    workflowDigest: normalizeBoundedString(input.workflowDigest, 'Workflow digest', 256),
    policyDigest: normalizeBoundedString(input.policyDigest, 'Policy digest', 256),
    deadlineEpochMs: assertPositiveSafeInteger(input.deadlineEpochMs, 'Grant deadline'),
    inputTokenLimit: assertNonNegativeSafeInteger(input.inputTokenLimit, 'Input token limit'),
    outputTokenLimit: assertNonNegativeSafeInteger(input.outputTokenLimit, 'Output token limit'),
    totalTokenLimit: assertNonNegativeSafeInteger(input.totalTokenLimit, 'Total token limit'),
  };
}

function normalizeBoundedString(input: string, label: string, maxBytes: number): string {
  if (typeof input !== 'string' || input.length === 0)
    throw new Error(`${label} must be a non-empty string.`);
  if (Buffer.byteLength(input, 'utf8') > maxBytes) throw new Error(`${label} is too large.`);
  return input;
}

function assertNow(value: number): number {
  return assertNonNegativeSafeInteger(value, 'Current time');
}

function assertPositiveSafeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0)
    throw new Error(`${label} must be a positive safe integer.`);
  return value;
}

function assertNonNegativeSafeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new Error(`${label} must be a non-negative safe integer.`);
  return value;
}

function safeAdd(left: number, right: number, label: string): number {
  const sum = left + right;
  if (!Number.isSafeInteger(sum)) throw new Error(`${label} exceeds safe integer range.`);
  return sum;
}

function assertCanAdd(left: number, right: number, label: string): void {
  safeAdd(left, right, label);
}

function assertWithinReservation(
  inputTokens: number,
  outputTokens: number,
  reservation: ReservationRow
): void {
  if (inputTokens > reservation.input_ceiling)
    throw new Error('Settled input tokens exceed reservation.');
  if (outputTokens > reservation.output_ceiling)
    throw new Error('Settled output tokens exceed reservation.');
}

function assertSameSettlement(
  existing: SettlementRow,
  inputTokens: number,
  outputTokens: number
): void {
  if (existing.input_tokens !== inputTokens || existing.output_tokens !== outputTokens) {
    throw new Error('Duplicate budget settlement does not match committed usage.');
  }
}

function normalizeDbCount(value: number | null): number {
  if (value === null) throw new Error('Settled reservation is missing usage.');
  return assertNonNegativeSafeInteger(value, 'Ledger token count');
}

function usageInput(row: {
  input_ceiling: number;
  output_ceiling: number;
  status: string;
  input_tokens: number | null;
  output_tokens: number | null;
}): number {
  assertUsageRow(row);
  return row.status === 'settled' ? normalizeDbCount(row.input_tokens) : row.input_ceiling;
}

function usageOutput(row: {
  input_ceiling: number;
  output_ceiling: number;
  status: string;
  input_tokens: number | null;
  output_tokens: number | null;
}): number {
  assertUsageRow(row);
  return row.status === 'settled' ? normalizeDbCount(row.output_tokens) : row.output_ceiling;
}

function assertUsageRow(row: {
  input_ceiling: number;
  output_ceiling: number;
  status: string;
  input_tokens: number | null;
  output_tokens: number | null;
}): void {
  const reservation = {
    id: 'aggregate-row',
    request_hash: 'aggregate-row',
    input_ceiling: row.input_ceiling,
    output_ceiling: row.output_ceiling,
    status: row.status,
    created_at_ms: 0,
    unknown_reason: null,
  } as ReservationRow;
  assertReservationRow(reservation);
  if (row.status === 'settled') {
    assertSettlementRow(
      {
        reservation_id: reservation.id,
        input_tokens: normalizeDbCount(row.input_tokens),
        output_tokens: normalizeDbCount(row.output_tokens),
      },
      reservation
    );
    return;
  }
  if (row.input_tokens !== null || row.output_tokens !== null) {
    throw new Error('Budget ledger private state is malformed.');
  }
}

function assertReservationRow(row: ReservationRow): void {
  if (!['pending', 'settled', 'unknown'].includes(row.status)) {
    throw new Error('Budget ledger private state is malformed.');
  }
  assertPositiveSafeInteger(row.input_ceiling, 'Ledger input reservation ceiling');
  assertPositiveSafeInteger(row.output_ceiling, 'Ledger output reservation ceiling');
  assertNonNegativeSafeInteger(row.created_at_ms, 'Ledger reservation timestamp');
}

function assertSettlementRow(settlement: SettlementRow, reservation: ReservationRow): void {
  const inputTokens = assertNonNegativeSafeInteger(
    settlement.input_tokens,
    'Ledger settled input tokens'
  );
  const outputTokens = assertNonNegativeSafeInteger(
    settlement.output_tokens,
    'Ledger settled output tokens'
  );
  if (settlement.reservation_id !== reservation.id)
    throw new Error('Budget ledger private state is malformed.');
  assertWithinReservation(inputTokens, outputTokens, reservation);
}

function rowToReservation(row: ReservationRow): BudgetReservation {
  return {
    reservationId: row.id,
    requestHash: row.request_hash,
    inputCeiling: row.input_ceiling,
    outputCeiling: row.output_ceiling,
    status: row.status,
    createdAtMs: row.created_at_ms,
  };
}

function digestJson(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
