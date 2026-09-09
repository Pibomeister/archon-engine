import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { createProxyBudgetLedger } from './proxy-budget-ledger';
import type { ProxyBudgetGrant, ProxyBudgetLedger } from './proxy-budget-ledger';

const tempRoots: string[] = [];

const grant: ProxyBudgetGrant = {
  schema: 'archon.proxy-budget-grant.v1',
  rootChainId: 'chain-root-1',
  runId: 'run-1',
  workflowDigest: 'sha256:workflow',
  policyDigest: 'sha256:policy',
  deadlineEpochMs: 2_000_000,
  inputTokenLimit: 100,
  outputTokenLimit: 80,
  totalTokenLimit: 150,
};

afterEach(() => {
  for (const root of tempRoots.splice(0)) rmSync(root, { recursive: true, force: true });
});

describe('proxy budget ledger', () => {
  test('accepts reserve then settles with trusted known usage', () => {
    const ledger = createLedger('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:req-a',
      inputCeiling: 20,
      outputCeiling: 10,
      nowMs: 1_000,
    });

    expect(reservation.status).toBe('pending');
    expect(ledger.getBudgetStatus()).toMatchObject({
      pendingReservations: 1,
      acceptingReservations: false,
      consumedInputTokens: 20,
      consumedOutputTokens: 10,
      consumedTotalTokens: 30,
    });

    const settled = ledger.settleBudget({
      reservationId: reservation.reservationId,
      inputTokens: 12,
      outputTokens: 5,
    });

    expect(settled.status).toBe('settled');
    expect(ledger.getBudgetStatus()).toMatchObject({
      pendingReservations: 0,
      acceptingReservations: true,
      consumedInputTokens: 12,
      consumedOutputTokens: 5,
      consumedTotalTokens: 17,
      remainingInputTokens: 88,
      remainingOutputTokens: 75,
      remainingTotalTokens: 133,
    });
    ledger.close();
  });

  test('rejects exhausted budgets and per-channel sublimit breaches', () => {
    const ledger = createLedger('create', {
      inputTokenLimit: 10,
      outputTokenLimit: 10,
      totalTokenLimit: 15,
    });

    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:input',
        inputCeiling: 11,
        outputCeiling: 1,
        nowMs: 1_000,
      })
    ).toThrow('Input token budget exhausted.');
    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:output',
        inputCeiling: 1,
        outputCeiling: 11,
        nowMs: 1_000,
      })
    ).toThrow('Output token budget exhausted.');
    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:total',
        inputCeiling: 8,
        outputCeiling: 8,
        nowMs: 1_000,
      })
    ).toThrow('Total token budget exhausted.');
    ledger.close();
  });

  test('resume rejects a changed immutable grant binding', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    ledger.close();

    expect(() =>
      createProxyBudgetLedger({
        mode: 'resume',
        dbPath,
        grant: { ...grant, workflowDigest: 'sha256:changed' },
      })
    ).toThrow('Budget grant binding drift detected.');
  });

  test('identical request hashes are charged as separate actual requests', () => {
    const ledger = createLedger('create');
    const first = ledger.reserveBudget({
      requestHash: 'sha256:same',
      inputCeiling: 10,
      outputCeiling: 5,
      nowMs: 1_000,
    });
    ledger.settleBudget({ reservationId: first.reservationId, inputTokens: 10, outputTokens: 5 });

    const second = ledger.reserveBudget({
      requestHash: 'sha256:same',
      inputCeiling: 10,
      outputCeiling: 5,
      nowMs: 1_001,
    });
    ledger.settleBudget({ reservationId: second.reservationId, inputTokens: 10, outputTokens: 5 });

    expect(first.reservationId).not.toBe(second.reservationId);
    expect(ledger.getBudgetStatus()).toMatchObject({
      consumedInputTokens: 20,
      consumedOutputTokens: 10,
      consumedTotalTokens: 30,
    });
    ledger.close();
  });

  test('missing usage keeps the full reservation and blocks further requests', () => {
    const ledger = createLedger('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:missing',
      inputCeiling: 30,
      outputCeiling: 20,
      nowMs: 1_000,
    });
    ledger.markReservationUnknown({
      reservationId: reservation.reservationId,
      reason: 'upstream closed before usage',
    });

    expect(ledger.getBudgetStatus()).toMatchObject({
      unknownReservations: 1,
      acceptingReservations: false,
      consumedInputTokens: 30,
      consumedOutputTokens: 20,
    });
    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:blocked',
        inputCeiling: 1,
        outputCeiling: 1,
        nowMs: 1_001,
      })
    ).toThrow('Unknown budget reservation blocks additional requests.');
    ledger.close();
  });

  test('reopen preserves pending reservations and committed ack-loss state', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    const pending = ledger.reserveBudget({
      requestHash: 'sha256:pending',
      inputCeiling: 7,
      outputCeiling: 3,
      nowMs: 1_000,
    });
    ledger.close();

    const reopenedPending = createProxyBudgetLedger({ mode: 'resume', dbPath, grant });
    expect(reopenedPending.getReservation(pending.reservationId)).toMatchObject({
      status: 'pending',
      inputCeiling: 7,
    });
    expect(() =>
      reopenedPending.reserveBudget({
        requestHash: 'sha256:blocked',
        inputCeiling: 1,
        outputCeiling: 1,
        nowMs: 1_001,
      })
    ).toThrow('Pending budget reservation blocks additional requests.');
    reopenedPending.settleBudget({
      reservationId: pending.reservationId,
      inputTokens: 6,
      outputTokens: 2,
    });
    reopenedPending.close();

    const reopenedSettled = createProxyBudgetLedger({ mode: 'resume', dbPath, grant });
    expect(reopenedSettled.getReservation(pending.reservationId)).toMatchObject({
      status: 'settled',
    });
    expect(reopenedSettled.getBudgetStatus()).toMatchObject({
      consumedInputTokens: 6,
      consumedOutputTokens: 2,
    });
    reopenedSettled.close();
  });

  test('duplicate settlement is idempotent only for exact matching usage', () => {
    const ledger = createLedger('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:settle',
      inputCeiling: 10,
      outputCeiling: 10,
      nowMs: 1_000,
    });

    ledger.settleBudget({
      reservationId: reservation.reservationId,
      inputTokens: 8,
      outputTokens: 4,
    });
    expect(
      ledger.settleBudget({
        reservationId: reservation.reservationId,
        inputTokens: 8,
        outputTokens: 4,
      })
    ).toMatchObject({
      status: 'settled',
    });
    expect(() =>
      ledger.settleBudget({
        reservationId: reservation.reservationId,
        inputTokens: 8,
        outputTokens: 5,
      })
    ).toThrow('Duplicate budget settlement does not match committed usage.');
    ledger.close();
  });

  test('concurrent SQLite connections cannot both reserve while one request is pending', () => {
    const { ledger: firstConnection, dbPath } = createLedgerWithPath('create');
    const secondConnection = createProxyBudgetLedger({ mode: 'resume', dbPath, grant });

    firstConnection.reserveBudget({
      requestHash: 'sha256:open',
      inputCeiling: 5,
      outputCeiling: 5,
      nowMs: 1_000,
    });

    expect(() =>
      secondConnection.reserveBudget({
        requestHash: 'sha256:concurrent',
        inputCeiling: 5,
        outputCeiling: 5,
        nowMs: 1_001,
      })
    ).toThrow('Pending budget reservation blocks additional requests.');
    firstConnection.close();
    secondConnection.close();
  });

  test('rejects unsafe counts, expired deadlines, and over-ceiling settlements', () => {
    const ledger = createLedger('create', { deadlineEpochMs: 1_500 });

    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:nan',
        inputCeiling: Number.NaN,
        outputCeiling: 1,
        nowMs: 1_000,
      })
    ).toThrow('Input reservation ceiling must be a positive safe integer.');
    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:fraction',
        inputCeiling: 1.5,
        outputCeiling: 1,
        nowMs: 1_000,
      })
    ).toThrow('Input reservation ceiling must be a positive safe integer.');
    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:expired',
        inputCeiling: 1,
        outputCeiling: 1,
        nowMs: 1_501,
      })
    ).toThrow('Budget grant deadline has expired.');

    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:within',
      inputCeiling: 3,
      outputCeiling: 3,
      nowMs: 1_000,
    });
    expect(() =>
      ledger.settleBudget({
        reservationId: reservation.reservationId,
        inputTokens: 4,
        outputTokens: 3,
      })
    ).toThrow('Settled input tokens exceed reservation.');
    ledger.close();
  });

  test('resume fails closed on malformed private ledger state', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    ledger.close();
    const db = new Database(dbPath);
    db.query("UPDATE proxy_budget_metadata SET value = ? WHERE key = 'grant_json'").run(
      '{not-json'
    );
    db.close();

    expect(() => createProxyBudgetLedger({ mode: 'resume', dbPath, grant })).toThrow();
  });

  test('open handles reject metadata tampering before later operations', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    const db = new Database(dbPath);
    db.query("UPDATE proxy_budget_metadata SET value = ? WHERE key = 'grant_digest'").run(
      'sha256:tampered'
    );
    db.close();

    expect(() => ledger.getBudgetStatus()).toThrow('Budget grant binding drift detected.');
    expect(() =>
      ledger.reserveBudget({
        requestHash: 'sha256:after-tamper',
        inputCeiling: 1,
        outputCeiling: 1,
        nowMs: 1_000,
      })
    ).toThrow('Budget grant binding drift detected.');
    ledger.close();
  });

  test('malformed reservation rows fail closed instead of producing negative accounting', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:tamper-row',
      inputCeiling: 5,
      outputCeiling: 5,
      nowMs: 1_000,
    });
    const db = new Database(dbPath);
    db.exec('PRAGMA ignore_check_constraints = ON');
    db.query(
      'UPDATE proxy_budget_reservations SET input_ceiling = ?, output_ceiling = ? WHERE id = ?'
    ).run(-900, -900, reservation.reservationId);
    db.close();

    expect(() => ledger.getBudgetStatus()).toThrow(
      'Ledger input reservation ceiling must be a positive safe integer.'
    );
    ledger.close();
  });

  test('write operations fail closed when any unrelated settled row is corrupt', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    const settled = ledger.reserveBudget({
      requestHash: 'sha256:settled-before-corrupt',
      inputCeiling: 5,
      outputCeiling: 5,
      nowMs: 1_000,
    });
    ledger.settleBudget({ reservationId: settled.reservationId, inputTokens: 3, outputTokens: 3 });
    const pending = ledger.reserveBudget({
      requestHash: 'sha256:pending-after-corrupt',
      inputCeiling: 5,
      outputCeiling: 5,
      nowMs: 1_001,
    });
    const db = new Database(dbPath);
    db.exec('PRAGMA ignore_check_constraints = ON');
    db.query('UPDATE proxy_budget_settlements SET input_tokens = ? WHERE reservation_id = ?').run(
      6,
      settled.reservationId
    );
    db.close();

    expect(() =>
      ledger.markReservationUnknown({ reservationId: pending.reservationId, reason: 'close' })
    ).toThrow('Settled input tokens exceed reservation.');
    expect(ledger.getReservation(pending.reservationId).status).toBe('pending');
    ledger.close();
  });

  test('settled rows with usage above reservation fail closed during status reconciliation', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:tamper-settle',
      inputCeiling: 5,
      outputCeiling: 5,
      nowMs: 1_000,
    });
    ledger.settleBudget({
      reservationId: reservation.reservationId,
      inputTokens: 3,
      outputTokens: 3,
    });
    const db = new Database(dbPath);
    db.exec('PRAGMA ignore_check_constraints = ON');
    db.query('UPDATE proxy_budget_settlements SET input_tokens = ? WHERE reservation_id = ?').run(
      6,
      reservation.reservationId
    );
    db.close();

    expect(() => ledger.getBudgetStatus()).toThrow('Settled input tokens exceed reservation.');
    ledger.close();
  });

  test('create mode fails closed when the target database is not empty', () => {
    const dbPath = freshDbPath();
    const db = new Database(dbPath);
    db.exec('CREATE TABLE attacker_state (id TEXT PRIMARY KEY)');
    db.close();

    expect(() => createProxyBudgetLedger({ mode: 'create', dbPath, grant })).toThrow(
      'Budget ledger create target is not empty.'
    );
  });

  test('status mode reads an existing clean ledger without creating a missing database', () => {
    const missingPath = freshDbPath();
    expect(() => createProxyBudgetLedger({ mode: 'status', dbPath: missingPath, grant })).toThrow(
      'Budget ledger is not initialized.'
    );

    const { ledger, dbPath } = createLedgerWithPath('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:status-clean',
      inputCeiling: 12,
      outputCeiling: 8,
      nowMs: 1_000,
    });
    ledger.settleBudget({
      reservationId: reservation.reservationId,
      inputTokens: 7,
      outputTokens: 3,
    });
    ledger.close();

    const statusLedger = createProxyBudgetLedger({ mode: 'status', dbPath, grant });
    expect(statusLedger.getBudgetStatus()).toMatchObject({
      pendingReservations: 0,
      unknownReservations: 0,
      consumedInputTokens: 7,
      consumedOutputTokens: 3,
      consumedTotalTokens: 10,
      acceptingReservations: true,
    });
    statusLedger.close();
  });

  test('status mode uses deferred reads while another writer holds the ledger', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:status-writer-lock',
      inputCeiling: 9,
      outputCeiling: 6,
      nowMs: 1_000,
    });
    ledger.settleBudget({
      reservationId: reservation.reservationId,
      inputTokens: 4,
      outputTokens: 2,
    });

    const writer = new Database(dbPath);
    writer.exec('BEGIN IMMEDIATE');
    try {
      const statusLedger = createProxyBudgetLedger({ mode: 'status', dbPath, grant });
      expect(statusLedger.getBudgetStatus()).toMatchObject({
        pendingReservations: 0,
        unknownReservations: 0,
        consumedInputTokens: 4,
        consumedOutputTokens: 2,
        consumedTotalTokens: 6,
        acceptingReservations: true,
      });
      statusLedger.close();
    } finally {
      writer.exec('ROLLBACK');
      writer.close();
      ledger.close();
    }
  });

  test('status mode reports live WAL pending reservations from one snapshot', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    ledger.reserveBudget({
      requestHash: 'sha256:status-live-wal',
      inputCeiling: 11,
      outputCeiling: 4,
      nowMs: 1_000,
    });

    const statusLedger = createProxyBudgetLedger({ mode: 'status', dbPath, grant });
    expect(statusLedger.getBudgetStatus()).toMatchObject({
      pendingReservations: 1,
      unknownReservations: 0,
      consumedInputTokens: 11,
      consumedOutputTokens: 4,
      consumedTotalTokens: 15,
      acceptingReservations: false,
    });
    statusLedger.close();
    ledger.close();
  });

  test('status mode fails closed on corrupted settled rows', () => {
    const { ledger, dbPath } = createLedgerWithPath('create');
    const reservation = ledger.reserveBudget({
      requestHash: 'sha256:status-corrupt',
      inputCeiling: 5,
      outputCeiling: 5,
      nowMs: 1_000,
    });
    ledger.settleBudget({
      reservationId: reservation.reservationId,
      inputTokens: 3,
      outputTokens: 3,
    });
    const db = new Database(dbPath);
    db.exec('PRAGMA ignore_check_constraints = ON');
    db.query('UPDATE proxy_budget_settlements SET output_tokens = ? WHERE reservation_id = ?').run(
      6,
      reservation.reservationId
    );
    db.close();

    const statusLedger = createProxyBudgetLedger({ mode: 'status', dbPath, grant });
    expect(() => statusLedger.getBudgetStatus()).toThrow(
      'Settled output tokens exceed reservation.'
    );
    statusLedger.close();
    ledger.close();
  });

  test('requires explicit first-time creation and rejects malicious oversized input', () => {
    const dbPath = freshDbPath();
    expect(() => createProxyBudgetLedger({ mode: 'resume', dbPath, grant })).toThrow(
      'Budget ledger is not initialized.'
    );
    expect(() => createLedger('create', { rootChainId: '' })).toThrow(
      'Root chain ID must be a non-empty string.'
    );

    const ledger = createLedger('create');
    expect(() =>
      ledger.reserveBudget({
        requestHash: 'x'.repeat(300),
        inputCeiling: 1,
        outputCeiling: 1,
        nowMs: 1_000,
      })
    ).toThrow('Request hash is too large.');
    ledger.close();
  });
});

function createLedger(
  mode: 'create' | 'resume' | 'status',
  override: Partial<ProxyBudgetGrant> = {}
): ProxyBudgetLedger {
  return createLedgerWithPath(mode, override).ledger;
}

function createLedgerWithPath(
  mode: 'create' | 'resume' | 'status',
  override: Partial<ProxyBudgetGrant> = {}
): { ledger: ProxyBudgetLedger; dbPath: string } {
  const dbPath = freshDbPath();
  const ledger = createProxyBudgetLedger({ mode, dbPath, grant: { ...grant, ...override } });
  return { ledger, dbPath };
}

function freshDbPath(): string {
  const root = mkdtempSync(join(tmpdir(), 'archon-proxy-budget-'));
  tempRoots.push(root);
  return join(root, 'ledger.sqlite');
}

const rootBinding = Object.freeze({
  runId: 'root-run',
  workflowDigest: 'sha256:workflow-root',
  policyDigest: 'sha256:policy-chain',
});
const childBinding = Object.freeze({
  runId: 'run-child',
  workflowDigest: 'sha256:workflow-child',
  policyDigest: 'sha256:policy-chain',
});
const chainGrant = Object.freeze({
  schema: 'archon.proxy-budget-grant.v2',
  rootChainId: 'root-run',
  deadlineEpochMs: 2_000_000,
  inputTokenLimit: 100,
  outputTokenLimit: 80,
  totalTokenLimit: 150,
  workflowBindings: Object.freeze([rootBinding, childBinding]),
} satisfies ProxyBudgetGrant);

describe('proxy budget ledger v2 shared-chain authority', () => {
  test('aggregates settled usage across two fixed member bindings', () => {
    const ledger = createV2Ledger('create');
    const root = ledger.reserveBudget({
      workflowBinding: rootBinding,
      requestHash: 'sha256:root-request',
      inputCeiling: 30,
      outputCeiling: 20,
      nowMs: 1_000,
    });
    ledger.settleBudget({
      workflowBinding: rootBinding,
      reservationId: root.reservationId,
      inputTokens: 12,
      outputTokens: 5,
    });
    const child = ledger.reserveBudget({
      workflowBinding: childBinding,
      requestHash: 'sha256:child-request',
      inputCeiling: 40,
      outputCeiling: 30,
      nowMs: 1_001,
    });
    ledger.settleBudget({
      workflowBinding: childBinding,
      reservationId: child.reservationId,
      inputTokens: 20,
      outputTokens: 10,
    });

    expect(ledger.getBudgetStatus()).toMatchObject({
      consumedInputTokens: 32,
      consumedOutputTokens: 15,
      consumedTotalTokens: 47,
      remainingTotalTokens: 103,
    });
    expect(ledger.getReservation(child.reservationId, childBinding).workflowBinding).toEqual(
      childBinding
    );
    ledger.close();
  });

  test('cross-member reservations exhaust one shared total budget', () => {
    const ledger = createV2Ledger('create', { totalTokenLimit: 55 });
    const root = ledger.reserveBudget({
      workflowBinding: rootBinding,
      requestHash: 'sha256:root-most',
      inputCeiling: 25,
      outputCeiling: 20,
      nowMs: 1_000,
    });
    ledger.settleBudget({
      workflowBinding: rootBinding,
      reservationId: root.reservationId,
      inputTokens: 25,
      outputTokens: 20,
    });

    expect(() =>
      ledger.reserveBudget({
        workflowBinding: childBinding,
        requestHash: 'sha256:child-too-much',
        inputCeiling: 6,
        outputCeiling: 5,
        nowMs: 1_001,
      })
    ).toThrow('Total token budget exhausted.');
    ledger.close();
  });

  test('rejects unauthorized or altered workflow identity before insertion', () => {
    const ledger = createV2Ledger('create');
    for (const workflowBinding of [
      { ...rootBinding, runId: 'unknown-run' },
      { ...rootBinding, workflowDigest: 'sha256:workflow-tampered' },
      { ...rootBinding, policyDigest: 'sha256:policy-tampered' },
    ]) {
      expect(() =>
        ledger.reserveBudget({
          workflowBinding,
          requestHash: `sha256:${workflowBinding.runId}`,
          inputCeiling: 1,
          outputCeiling: 1,
          nowMs: 1_000,
        })
      ).toThrow('Workflow binding is not authorized by the budget grant.');
    }
    expect(ledger.getBudgetStatus()).toMatchObject({ consumedTotalTokens: 0 });
    ledger.close();
  });

  test('member-scoped mutations refuse cross-settlement and cross-read by UUID', () => {
    const ledger = createV2Ledger('create');
    const child = ledger.reserveBudget({
      workflowBinding: childBinding,
      requestHash: 'sha256:child-owned',
      inputCeiling: 10,
      outputCeiling: 5,
      nowMs: 1_000,
    });

    expect(() => ledger.getReservation(child.reservationId, rootBinding)).toThrow(
      'Budget reservation is not scoped to this workflow binding.'
    );
    expect(() =>
      ledger.settleBudget({
        workflowBinding: rootBinding,
        reservationId: child.reservationId,
        inputTokens: 3,
        outputTokens: 2,
      })
    ).toThrow('Budget reservation is not scoped to this workflow binding.');
    expect(() =>
      ledger.markReservationUnknown({
        workflowBinding: rootBinding,
        reservationId: child.reservationId,
        reason: 'cross-member poison',
      })
    ).toThrow('Budget reservation is not scoped to this workflow binding.');
    expect(
      ledger.settleBudget({
        workflowBinding: childBinding,
        reservationId: child.reservationId,
        inputTokens: 3,
        outputTokens: 2,
      })
    ).toMatchObject({ status: 'settled' });
    ledger.close();
  });

  test('resume rejects grant mutation, reordered bindings, and extended deadline', () => {
    const { ledger, dbPath } = createV2LedgerWithPath('create');
    ledger.close();
    expect(() =>
      createProxyBudgetLedger({
        mode: 'resume',
        dbPath,
        grant: { ...chainGrant, deadlineEpochMs: 3_000_000 },
      })
    ).toThrow('Budget grant binding drift detected.');
    expect(() =>
      createProxyBudgetLedger({
        mode: 'resume',
        dbPath,
        grant: { ...chainGrant, workflowBindings: [childBinding, rootBinding] },
      })
    ).toThrow('Workflow bindings must be sorted and unique.');
    expect(() =>
      createProxyBudgetLedger({
        mode: 'resume',
        dbPath,
        grant: {
          ...chainGrant,
          workflowBindings: [
            rootBinding,
            childBinding,
            { runId: 'z-run', workflowDigest: 'sha256:z', policyDigest: 'sha256:policy-chain' },
          ],
        },
      })
    ).toThrow('Budget grant binding drift detected.');
  });

  test('ambiguous manifests fail closed at create', () => {
    expect(() => createV2Ledger('create', { workflowBindings: [] })).toThrow(
      'Workflow bindings must be a non-empty array.'
    );
    expect(() => createV2Ledger('create', { rootChainId: 'missing-root' })).toThrow(
      'Workflow bindings must contain the root chain ID.'
    );
    expect(() =>
      createV2Ledger('create', { workflowBindings: [childBinding, rootBinding] })
    ).toThrow('Workflow bindings must be sorted and unique.');
    expect(() =>
      createV2Ledger('create', {
        workflowBindings: [rootBinding, { ...rootBinding, workflowDigest: 'sha256:other' }],
      })
    ).toThrow('Workflow bindings must be unique by run ID.');
  });

  test('global scans reject corruption of every persisted workflow identity field', () => {
    for (const [column, replacement] of [
      ['run_id', childBinding.runId],
      ['workflow_digest', childBinding.workflowDigest],
      ['policy_digest', 'sha256:other-policy'],
    ] as const) {
      const { ledger, dbPath } = createV2LedgerWithPath('create');
      const reservation = ledger.reserveBudget({
        workflowBinding: rootBinding,
        requestHash: `sha256:tamper-${column}`,
        inputCeiling: 10,
        outputCeiling: 5,
        nowMs: 1_000,
      });
      const db = new Database(dbPath);
      db.query(`UPDATE proxy_budget_reservations SET ${column} = ? WHERE id = ?`).run(
        replacement,
        reservation.reservationId
      );
      db.close();

      expect(() => ledger.getBudgetStatus()).toThrow('Budget ledger private state is malformed.');
      expect(() =>
        ledger.reserveBudget({
          workflowBinding: childBinding,
          requestHash: `sha256:after-${column}`,
          inputCeiling: 1,
          outputCeiling: 1,
          nowMs: 1_001,
        })
      ).toThrow('Budget ledger private state is malformed.');
      ledger.close();
    }
  });

  test('complete reassignment to another valid member cannot authorize read or mutation', () => {
    for (const operation of ['get', 'settle', 'unknown'] as const) {
      const { ledger, dbPath } = createV2LedgerWithPath('create');
      const reservation = ledger.reserveBudget({
        workflowBinding: rootBinding,
        requestHash: `sha256:reassign-${operation}`,
        inputCeiling: 10,
        outputCeiling: 5,
        nowMs: 1_000,
      });
      const db = new Database(dbPath);
      db.query(
        'UPDATE proxy_budget_reservations SET run_id = ?, workflow_digest = ?, policy_digest = ? WHERE id = ?'
      ).run(
        childBinding.runId,
        childBinding.workflowDigest,
        childBinding.policyDigest,
        reservation.reservationId
      );
      db.close();

      const invoke = (): unknown => {
        if (operation === 'get')
          return ledger.getReservation(reservation.reservationId, childBinding);
        if (operation === 'settle') {
          return ledger.settleBudget({
            workflowBinding: childBinding,
            reservationId: reservation.reservationId,
            inputTokens: 1,
            outputTokens: 1,
          });
        }
        return ledger.markReservationUnknown({
          workflowBinding: childBinding,
          reservationId: reservation.reservationId,
          reason: 'reassigned',
        });
      };
      expect(invoke).toThrow('Budget ledger private state is malformed.');
      ledger.close();
    }
  });

  test('same request hash from different members is charged independently', () => {
    const ledger = createV2Ledger('create');
    for (const workflowBinding of [rootBinding, childBinding]) {
      const reservation = ledger.reserveBudget({
        workflowBinding,
        requestHash: 'sha256:same-across-members',
        inputCeiling: 10,
        outputCeiling: 5,
        nowMs: 1_000,
      });
      ledger.settleBudget({
        workflowBinding,
        reservationId: reservation.reservationId,
        inputTokens: 10,
        outputTokens: 5,
      });
    }
    expect(ledger.getBudgetStatus()).toMatchObject({ consumedTotalTokens: 30 });
    ledger.close();
  });
});

function createV2Ledger(
  mode: 'create' | 'resume' | 'status',
  override: Partial<ProxyBudgetGrant> = {}
): ProxyBudgetLedger {
  return createV2LedgerWithPath(mode, override).ledger;
}

function createV2LedgerWithPath(
  mode: 'create' | 'resume' | 'status',
  override: Partial<ProxyBudgetGrant> = {}
): { ledger: ProxyBudgetLedger; dbPath: string } {
  const dbPath = freshDbPath();
  const ledger = createProxyBudgetLedger({ mode, dbPath, grant: { ...chainGrant, ...override } });
  return { ledger, dbPath };
}
