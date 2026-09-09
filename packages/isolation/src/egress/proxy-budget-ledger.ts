import { Database } from 'bun:sqlite';
import { existsSync, mkdirSync } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { dirname } from 'node:path';

export interface ProxyBudgetWorkflowBinding {
  runId: string;
  workflowDigest: string;
  policyDigest: string;
}

export interface ProxyBudgetGrantV1 extends ProxyBudgetWorkflowBinding {
  schema: 'archon.proxy-budget-grant.v1';
  rootChainId: string;
  deadlineEpochMs: number;
  inputTokenLimit: number;
  outputTokenLimit: number;
  totalTokenLimit: number;
}

export interface ProxyBudgetGrantV2 {
  schema: 'archon.proxy-budget-grant.v2';
  rootChainId: string;
  deadlineEpochMs: number;
  inputTokenLimit: number;
  outputTokenLimit: number;
  totalTokenLimit: number;
  workflowBindings: readonly ProxyBudgetWorkflowBinding[];
}

export type ProxyBudgetGrant = ProxyBudgetGrantV1 | ProxyBudgetGrantV2;

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
  workflowBinding?: ProxyBudgetWorkflowBinding;
}

export interface BudgetReservation {
  reservationId: string;
  requestHash: string;
  inputCeiling: number;
  outputCeiling: number;
  status: 'pending' | 'settled' | 'unknown';
  createdAtMs: number;
  workflowBinding: ProxyBudgetWorkflowBinding;
}

export interface SettleBudgetInput {
  reservationId: string;
  inputTokens: number;
  outputTokens: number;
  workflowBinding?: ProxyBudgetWorkflowBinding;
}

export interface MarkReservationUnknownInput {
  reservationId: string;
  reason: string;
  workflowBinding?: ProxyBudgetWorkflowBinding;
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
  run_id?: string;
  workflow_digest?: string;
  policy_digest?: string;
  binding_digest?: string;
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

const SCHEMA_VERSION_V1 = '1';
const SCHEMA_VERSION_V2 = '2';
const MAX_REASON_BYTES = 512;
const BINDING_KEYS = ['runId', 'workflowDigest', 'policyDigest'] as const;

export class ProxyBudgetLedger {
  private readonly db: Database;
  private readonly grant: ProxyBudgetGrant;
  private readonly grantDigest: string;
  private closed = false;

  private constructor(db: Database, grant: ProxyBudgetGrant) {
    this.db = db;
    this.grant = freezeGrant(grant);
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
    const binding = this.authorizeBinding(input.workflowBinding);
    const requestHash = normalizeBoundedString(input.requestHash, 'Request hash', 256);
    const inputCeiling = assertPositiveSafeInteger(input.inputCeiling, 'Input reservation ceiling');
    const outputCeiling = assertPositiveSafeInteger(
      input.outputCeiling,
      'Output reservation ceiling'
    );
    const nowMs = assertNow(input.nowMs ?? Date.now());
    return this.writeTransaction(() => {
      this.assertDeadline(nowMs);
      this.assertNoUnsafeOpenReservations();
      const totals = this.calculateUsageTotals();
      const reserveTotal = safeAdd(inputCeiling, outputCeiling, 'Reservation total');
      assertCanAdd(totals.input, inputCeiling, 'Input reservation total');
      assertCanAdd(totals.output, outputCeiling, 'Output reservation total');
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
      if (isV2Grant(this.grant)) {
        const bindingDigest = this.bindingDigest(reservationId, binding);
        this.db
          .query(
            `INSERT INTO proxy_budget_reservations
             (id, request_hash, input_ceiling, output_ceiling, status, created_at_ms, run_id, workflow_digest, policy_digest, binding_digest)
             VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)`
          )
          .run(
            reservationId,
            requestHash,
            inputCeiling,
            outputCeiling,
            nowMs,
            binding.runId,
            binding.workflowDigest,
            binding.policyDigest,
            bindingDigest
          );
      } else {
        this.db
          .query(
            `INSERT INTO proxy_budget_reservations
             (id, request_hash, input_ceiling, output_ceiling, status, created_at_ms)
             VALUES (?, ?, ?, ?, 'pending', ?)`
          )
          .run(reservationId, requestHash, inputCeiling, outputCeiling, nowMs);
      }
      return rowToReservation(
        {
          id: reservationId,
          request_hash: requestHash,
          input_ceiling: inputCeiling,
          output_ceiling: outputCeiling,
          status: 'pending',
          created_at_ms: nowMs,
          unknown_reason: null,
        },
        binding
      );
    });
  }

  settleBudget(input: SettleBudgetInput): BudgetReservation {
    this.assertOpen();
    const binding = this.authorizeBinding(input.workflowBinding);
    const reservationId = normalizeBoundedString(input.reservationId, 'Reservation ID', 128);
    const inputTokens = assertNonNegativeSafeInteger(input.inputTokens, 'Settled input tokens');
    const outputTokens = assertNonNegativeSafeInteger(input.outputTokens, 'Settled output tokens');
    return this.writeTransaction(() => {
      const reservation = this.requireReservation(reservationId, binding);
      if (reservation.status === 'unknown')
        throw new Error('Unknown reservation cannot be settled.');
      assertWithinReservation(inputTokens, outputTokens, reservation);
      const existing = this.findSettlement(reservationId);
      if (existing) {
        assertSameSettlement(existing, inputTokens, outputTokens);
        return rowToReservation({ ...reservation, status: 'settled' }, binding);
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
      return rowToReservation({ ...reservation, status: 'settled' }, binding);
    });
  }

  markReservationUnknown(input: MarkReservationUnknownInput): BudgetReservation {
    this.assertOpen();
    const binding = this.authorizeBinding(input.workflowBinding);
    const reservationId = normalizeBoundedString(input.reservationId, 'Reservation ID', 128);
    const reason = normalizeBoundedString(input.reason, 'Unknown reason', MAX_REASON_BYTES);
    return this.writeTransaction(() => {
      const reservation = this.requireReservation(reservationId, binding);
      if (reservation.status === 'settled')
        throw new Error('Settled reservation cannot be marked unknown.');
      this.db
        .query(
          "UPDATE proxy_budget_reservations SET status = 'unknown', unknown_reason = ? WHERE id = ?"
        )
        .run(reason, reservationId);
      return rowToReservation(
        { ...reservation, status: 'unknown', unknown_reason: reason },
        binding
      );
    });
  }

  getReservation(
    reservationId: string,
    workflowBinding?: ProxyBudgetWorkflowBinding
  ): BudgetReservation {
    this.assertOpen();
    const binding = this.authorizeBinding(workflowBinding);
    this.assertMetadataBinding();
    const reservation = this.requireReservation(
      normalizeBoundedString(reservationId, 'Reservation ID', 128),
      binding
    );
    this.assertCompleteReservationIntegrity(reservation);
    return rowToReservation(reservation, binding);
  }

  getBudgetStatus(): BudgetStatus {
    this.assertOpen();
    return this.readTransaction(() => {
      this.assertMetadataBinding();
      const totals = this.calculateUsageTotals();
      const pendingReservations = this.countReservations('pending');
      const unknownReservations = this.countReservations('unknown');
      return {
        grant: cloneGrant(this.grant),
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
        this.writeMetadata('schema_version', schemaVersionForGrant(this.grant));
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
    const bindingColumns = isV2Grant(this.grant)
      ? `,
        run_id TEXT NOT NULL,
        workflow_digest TEXT NOT NULL,
        policy_digest TEXT NOT NULL,
        binding_digest TEXT NOT NULL`
      : '';
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
        unknown_reason TEXT${bindingColumns}
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
    if (version !== schemaVersionForGrant(this.grant))
      throw new Error('Unsupported budget ledger schema version.');
    if (digest !== this.grantDigest) throw new Error('Budget grant binding drift detected.');
    try {
      const storedGrant = normalizeGrant(JSON.parse(grantJson) as ProxyBudgetGrant);
      if (digestJson(storedGrant) !== this.grantDigest)
        throw new Error('Budget grant private state is malformed.');
    } catch (error) {
      if (error instanceof SyntaxError) throw new Error('Budget grant private state is malformed.');
      throw error;
    }
  }

  private authorizeBinding(
    input: ProxyBudgetWorkflowBinding | undefined
  ): ProxyBudgetWorkflowBinding {
    if (!isV2Grant(this.grant) && input === undefined) return rootBinding(this.grant);
    const binding = normalizeWorkflowBinding(input, 'Workflow binding');
    if (!grantContainsBinding(this.grant, binding))
      throw new Error('Workflow binding is not authorized by the budget grant.');
    return binding;
  }

  private calculateUsageTotals(): UsageTotals {
    const bindingFields = isV2Grant(this.grant)
      ? 'r.run_id, r.workflow_digest, r.policy_digest, r.binding_digest'
      : 'NULL AS run_id, NULL AS workflow_digest, NULL AS policy_digest, NULL AS binding_digest';
    const rows = this.db
      .query(
        `SELECT r.id, r.request_hash, r.input_ceiling, r.output_ceiling, r.status,
                r.created_at_ms, r.unknown_reason, ${bindingFields},
                s.input_tokens, s.output_tokens
         FROM proxy_budget_reservations r
         LEFT JOIN proxy_budget_settlements s ON s.reservation_id = r.id`
      )
      .all() as (ReservationRow & {
      input_ceiling: number;
      output_ceiling: number;
      status: string;
      input_tokens: number | null;
      output_tokens: number | null;
    })[];
    let input = 0;
    let output = 0;
    for (const row of rows) {
      assertReservationRow(row);
      this.assertV2BindingIntegrity(row);
      input = safeAdd(input, usageInput(row), 'Ledger input usage');
      output = safeAdd(output, usageOutput(row), 'Ledger output usage');
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
    if (this.countReservations('pending') > 0)
      throw new Error('Pending budget reservation blocks additional requests.');
    if (this.countReservations('unknown') > 0)
      throw new Error('Unknown budget reservation blocks additional requests.');
  }

  private countReservations(status: 'pending' | 'unknown'): number {
    const row = this.db
      .query('SELECT COUNT(*) AS count FROM proxy_budget_reservations WHERE status = ?')
      .get(status) as { count: number };
    return assertNonNegativeSafeInteger(row.count, 'Reservation count');
  }

  private requireReservation(id: string, binding: ProxyBudgetWorkflowBinding): ReservationRow {
    const row = this.readReservationRow(id);
    if (!row) throw new Error('Budget reservation not found.');
    assertReservationRow(row);
    this.assertV2BindingIntegrity(row);
    if (isV2Grant(this.grant)) assertReservationBinding(row, binding);
    return row;
  }

  private readReservationRow(id: string): ReservationRow | null {
    const fields = isV2Grant(this.grant)
      ? 'id, request_hash, input_ceiling, output_ceiling, status, created_at_ms, unknown_reason, run_id, workflow_digest, policy_digest, binding_digest'
      : 'id, request_hash, input_ceiling, output_ceiling, status, created_at_ms, unknown_reason';
    return this.db
      .query(`SELECT ${fields} FROM proxy_budget_reservations WHERE id = ?`)
      .get(id) as ReservationRow | null;
  }

  private findSettlement(reservationId: string): SettlementRow | null {
    return this.db
      .query(
        'SELECT reservation_id, input_tokens, output_tokens FROM proxy_budget_settlements WHERE reservation_id = ?'
      )
      .get(reservationId) as SettlementRow | null;
  }

  private bindingDigest(id: string, binding: ProxyBudgetWorkflowBinding): string {
    return digestJson({ grantDigest: this.grantDigest, reservationId: id, binding });
  }

  private assertV2BindingIntegrity(row: ReservationRow): void {
    if (!isV2Grant(this.grant)) return;
    const binding = normalizeRowBinding(row);
    if (row.binding_digest !== this.bindingDigest(row.id, binding)) {
      throw new Error('Budget ledger private state is malformed.');
    }
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

export function proxyBudgetRootBinding(grant: ProxyBudgetGrant): ProxyBudgetWorkflowBinding {
  return rootBinding(normalizeGrant(grant));
}

export function isProxyBudgetGrantV2(grant: ProxyBudgetGrant): grant is ProxyBudgetGrantV2 {
  return grant.schema === 'archon.proxy-budget-grant.v2';
}

function configureDatabase(db: Database, mode: 'create' | 'resume' | 'status'): void {
  if (mode !== 'status') db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA synchronous = FULL');
  db.exec('PRAGMA foreign_keys = ON');
  db.exec('PRAGMA busy_timeout = 250');
}

function normalizeGrant(input: ProxyBudgetGrant): ProxyBudgetGrant {
  if (!input || typeof input !== 'object' || Array.isArray(input))
    throw new Error('Budget grant must be an object.');
  if (input.schema === 'archon.proxy-budget-grant.v1') return normalizeV1Grant(input);
  if (input.schema === 'archon.proxy-budget-grant.v2') return normalizeV2Grant(input);
  throw new Error('Unsupported budget grant schema.');
}

function normalizeV1Grant(input: ProxyBudgetGrantV1): ProxyBudgetGrantV1 {
  assertExactKeys(
    input as unknown as Record<string, unknown>,
    [
      'schema',
      'rootChainId',
      'runId',
      'workflowDigest',
      'policyDigest',
      'deadlineEpochMs',
      'inputTokenLimit',
      'outputTokenLimit',
      'totalTokenLimit',
    ],
    'Budget grant'
  );
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

function normalizeV2Grant(input: ProxyBudgetGrantV2): ProxyBudgetGrantV2 {
  assertExactKeys(
    input as unknown as Record<string, unknown>,
    [
      'schema',
      'rootChainId',
      'deadlineEpochMs',
      'inputTokenLimit',
      'outputTokenLimit',
      'totalTokenLimit',
      'workflowBindings',
    ],
    'Budget grant'
  );
  const bindings = normalizeWorkflowBindings(input.workflowBindings);
  const grant = {
    schema: input.schema,
    rootChainId: normalizeBoundedString(input.rootChainId, 'Root chain ID', 256),
    deadlineEpochMs: assertPositiveSafeInteger(input.deadlineEpochMs, 'Grant deadline'),
    inputTokenLimit: assertNonNegativeSafeInteger(input.inputTokenLimit, 'Input token limit'),
    outputTokenLimit: assertNonNegativeSafeInteger(input.outputTokenLimit, 'Output token limit'),
    totalTokenLimit: assertNonNegativeSafeInteger(input.totalTokenLimit, 'Total token limit'),
    workflowBindings: bindings,
  };
  assertWorkflowBindingsContainRoot(grant);
  return grant;
}

function normalizeWorkflowBindings(
  input: readonly ProxyBudgetWorkflowBinding[]
): ProxyBudgetWorkflowBinding[] {
  if (!Array.isArray(input) || input.length === 0)
    throw new Error('Workflow bindings must be a non-empty array.');
  const seen = new Set<string>();
  let previous = '';
  return input.map((member, index) => {
    const binding = normalizeWorkflowBinding(member, 'Workflow binding');
    const orderKey = bindingOrderKey(binding);
    if (seen.has(binding.runId)) throw new Error('Workflow bindings must be unique by run ID.');
    if (index > 0 && previous >= orderKey)
      throw new Error('Workflow bindings must be sorted and unique.');
    previous = orderKey;
    seen.add(binding.runId);
    return binding;
  });
}

function normalizeWorkflowBinding(
  input: ProxyBudgetWorkflowBinding | undefined,
  label: string
): ProxyBudgetWorkflowBinding {
  if (!input || typeof input !== 'object' || Array.isArray(input))
    throw new Error(`${label} must be an object.`);
  assertExactKeys(input as unknown as Record<string, unknown>, [...BINDING_KEYS], label);
  return {
    runId: normalizeBoundedString(input.runId, 'Run ID', 256),
    workflowDigest: normalizeBoundedString(input.workflowDigest, 'Workflow digest', 256),
    policyDigest: normalizeBoundedString(input.policyDigest, 'Policy digest', 256),
  };
}

function assertWorkflowBindingsContainRoot(grant: ProxyBudgetGrantV2): void {
  if (!grant.workflowBindings.some(binding => binding.runId === grant.rootChainId)) {
    throw new Error('Workflow bindings must contain the root chain ID.');
  }
}

function grantContainsBinding(
  grant: ProxyBudgetGrant,
  binding: ProxyBudgetWorkflowBinding
): boolean {
  const bindings = isV2Grant(grant) ? grant.workflowBindings : [rootBinding(grant)];
  return bindings.some(member => bindingsEqual(member, binding));
}

function rootBinding(grant: ProxyBudgetGrant): ProxyBudgetWorkflowBinding {
  if (isV2Grant(grant)) {
    const binding = grant.workflowBindings.find(member => member.runId === grant.rootChainId);
    if (!binding) throw new Error('Budget grant private state is malformed.');
    return binding;
  }
  return {
    runId: grant.runId,
    workflowDigest: grant.workflowDigest,
    policyDigest: grant.policyDigest,
  };
}

function bindingOrderKey(binding: ProxyBudgetWorkflowBinding): string {
  return `${binding.runId}\u0000${binding.workflowDigest}\u0000${binding.policyDigest}`;
}

function bindingsEqual(
  left: ProxyBudgetWorkflowBinding,
  right: ProxyBudgetWorkflowBinding
): boolean {
  return (
    left.runId === right.runId &&
    left.workflowDigest === right.workflowDigest &&
    left.policyDigest === right.policyDigest
  );
}

function assertReservationBinding(row: ReservationRow, binding: ProxyBudgetWorkflowBinding): void {
  const rowBinding = normalizeRowBinding(row);
  if (!bindingsEqual(rowBinding, binding))
    throw new Error('Budget reservation is not scoped to this workflow binding.');
}

function normalizeRowBinding(row: ReservationRow): ProxyBudgetWorkflowBinding {
  return {
    runId: normalizeBoundedString(row.run_id ?? '', 'Reservation run ID', 256),
    workflowDigest: normalizeBoundedString(
      row.workflow_digest ?? '',
      'Reservation workflow digest',
      256
    ),
    policyDigest: normalizeBoundedString(row.policy_digest ?? '', 'Reservation policy digest', 256),
  };
}

function schemaVersionForGrant(grant: ProxyBudgetGrant): string {
  return isV2Grant(grant) ? SCHEMA_VERSION_V2 : SCHEMA_VERSION_V1;
}

function isV2Grant(grant: ProxyBudgetGrant): grant is ProxyBudgetGrantV2 {
  return grant.schema === 'archon.proxy-budget-grant.v2';
}

function cloneGrant(grant: ProxyBudgetGrant): ProxyBudgetGrant {
  return isV2Grant(grant)
    ? { ...grant, workflowBindings: grant.workflowBindings.map(binding => ({ ...binding })) }
    : { ...grant };
}

function freezeGrant(grant: ProxyBudgetGrant): ProxyBudgetGrant {
  if (!isV2Grant(grant)) return Object.freeze({ ...grant });
  const workflowBindings = grant.workflowBindings.map(binding => Object.freeze({ ...binding }));
  return Object.freeze({ ...grant, workflowBindings: Object.freeze(workflowBindings) });
}

function assertExactKeys(
  record: Record<string, unknown>,
  allowedKeys: readonly string[],
  label: string
): void {
  if (Object.keys(record).some(key => !allowedKeys.includes(key)))
    throw new Error(`${label} contains unsupported settings.`);
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
  if (row.input_tokens !== null || row.output_tokens !== null)
    throw new Error('Budget ledger private state is malformed.');
}

function assertReservationRow(row: ReservationRow): void {
  normalizeBoundedString(row.id, 'Reservation ID', 128);
  normalizeBoundedString(row.request_hash, 'Request hash', 256);
  assertPositiveSafeInteger(row.input_ceiling, 'Ledger input reservation ceiling');
  assertPositiveSafeInteger(row.output_ceiling, 'Ledger output reservation ceiling');
  if (row.status !== 'pending' && row.status !== 'settled' && row.status !== 'unknown') {
    throw new Error('Budget ledger private state is malformed.');
  }
  assertNonNegativeSafeInteger(row.created_at_ms, 'Reservation creation time');
  if (row.unknown_reason !== null)
    normalizeBoundedString(row.unknown_reason, 'Unknown reason', MAX_REASON_BYTES);
}

function assertSettlementRow(settlement: SettlementRow, reservation: ReservationRow): void {
  if (settlement.reservation_id !== reservation.id)
    throw new Error('Budget ledger private state is malformed.');
  const inputTokens = assertNonNegativeSafeInteger(settlement.input_tokens, 'Settled input tokens');
  const outputTokens = assertNonNegativeSafeInteger(
    settlement.output_tokens,
    'Settled output tokens'
  );
  assertWithinReservation(inputTokens, outputTokens, reservation);
}

function rowToReservation(
  row: ReservationRow,
  binding: ProxyBudgetWorkflowBinding
): BudgetReservation {
  return {
    reservationId: row.id,
    requestHash: row.request_hash,
    inputCeiling: row.input_ceiling,
    outputCeiling: row.output_ceiling,
    status: row.status,
    createdAtMs: row.created_at_ms,
    workflowBinding: { ...binding },
  };
}

function digestJson(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
