import { describe, expect, test } from 'bun:test';
import {
  WORKFLOW_BUDGET_METADATA_KEY,
  assertWorkflowBudgetCanContinue,
  nextWorkflowBudgetState,
  resolveWorkflowBudget,
  unknownWorkflowBudgetState,
  type ActiveWorkflowBudget,
  type WorkflowBudgetGrant,
  type WorkflowBudgetState,
} from './budget';
import type { WorkflowRun } from './schemas';

const NOW_MS = Date.parse('2026-01-01T00:00:00.000Z');
const FUTURE_DEADLINE = '2026-01-01T01:00:00.000Z';
const UPDATED_AT = '2026-01-01T00:00:00.000Z';

function makeRun(metadata: Record<string, unknown> = {}): WorkflowRun {
  return {
    id: 'run-1',
    workflow_name: 'budgeted-workflow',
    conversation_id: 'conv-1',
    parent_conversation_id: null,
    codebase_id: null,
    status: 'running',
    user_message: 'run budgeted workflow',
    metadata,
    started_at: new Date(NOW_MS),
    completed_at: null,
    last_activity_at: null,
    working_path: null,
    user_id: null,
    parent_run_id: null,
    output_root: null,
  };
}

function makeGrant(overrides: Partial<WorkflowBudgetGrant> = {}): WorkflowBudgetGrant {
  return {
    runId: 'run-1',
    workflowName: 'budgeted-workflow',
    workflowDigest: 'digest-1',
    deadlineAt: FUTURE_DEADLINE,
    tokens: { total: 100, input: 80, output: 60 },
    ...overrides,
  };
}

function makeState(overrides: Partial<WorkflowBudgetState> = {}): WorkflowBudgetState {
  return {
    workflowDigest: 'digest-1',
    deadlineAt: FUTURE_DEADLINE,
    tokens: { total: 100, input: 80, output: 60 },
    consumed: { input: 0, output: 0 },
    updatedAt: UPDATED_AT,
    ...overrides,
  };
}

function makeActive(state: WorkflowBudgetState = makeState()): ActiveWorkflowBudget {
  return { state, deadlineAtMs: Date.parse(state.deadlineAt) };
}

describe('hardened workflow budget state invariants', () => {
  test('rejects non-safe-integer token limits instead of accepting fractional limits', () => {
    expect(() =>
      resolveWorkflowBudget(
        { name: 'budgeted-workflow', hardened: { required: true } },
        makeRun(),
        'digest-1',
        [makeGrant({ tokens: { total: 10.5 } })],
        undefined,
        false,
        NOW_MS
      )
    ).toThrow(/positive safe integer/);
  });

  test('rejects non-safe-integer consumed counts from prior usage and persisted state', () => {
    expect(() =>
      resolveWorkflowBudget(
        { name: 'budgeted-workflow', hardened: { required: true } },
        makeRun(),
        'digest-1',
        [makeGrant()],
        { input: 1.25, output: 0 },
        false,
        NOW_MS
      )
    ).toThrow(/prior token usage is malformed/);

    expect(() =>
      resolveWorkflowBudget(
        { name: 'budgeted-workflow', hardened: { required: true } },
        makeRun({
          [WORKFLOW_BUDGET_METADATA_KEY]: makeState({ consumed: { input: 1, output: NaN } }),
        }),
        'digest-1',
        [makeGrant()],
        undefined,
        false,
        NOW_MS
      )
    ).toThrow(/persisted budget state is malformed/);
  });

  test('guards total consumption comparisons when input plus output would overflow safe integers', () => {
    const active = makeActive(
      makeState({
        tokens: { total: Number.MAX_SAFE_INTEGER },
        consumed: { input: 0, output: 0 },
      })
    );

    expect(() =>
      assertWorkflowBudgetCanContinue(
        active,
        'overflowing node',
        {
          input: Number.MAX_SAFE_INTEGER,
          output: 1,
        },
        NOW_MS
      )
    ).toThrow(/total token budget/);
  });

  test('next state cannot reduce already-known consumption', () => {
    const active = makeActive(makeState({ consumed: { input: 10, output: 5 } }));

    expect(nextWorkflowBudgetState(active, { input: 3, output: 8 }, NOW_MS).consumed).toEqual({
      input: 10,
      output: 8,
    });
  });

  test('unknown state cannot reduce known consumption or clear the unknown marker', () => {
    const active = makeActive(
      makeState({ consumed: { input: 9, output: 4 }, unknownConsumption: true })
    );

    const knownUpdate = nextWorkflowBudgetState(active, { input: 9, output: 10 }, NOW_MS);
    const unknownUpdate = unknownWorkflowBudgetState(active, { input: 2, output: 1 }, NOW_MS);

    expect(knownUpdate.unknownConsumption).toBe(true);
    expect(unknownUpdate.consumed).toEqual({ input: 9, output: 4 });
    expect(unknownUpdate.unknownConsumption).toBe(true);
  });

  test('rejects caller-supplied authoritative consumption on workflow budget grants', () => {
    expect(() =>
      resolveWorkflowBudget(
        { name: 'budgeted-workflow', hardened: { required: true } },
        makeRun(),
        'digest-1',
        [makeGrant({ authoritativeConsumed: { input: 1, output: 0 } })],
        undefined,
        false,
        NOW_MS
      )
    ).toThrow(/caller-supplied authoritative consumption/);
  });

  test('verified reconciliation clears unknown consumption without lowering persisted floors', () => {
    const persisted = makeState({ consumed: { input: 30, output: 7 }, unknownConsumption: true });

    const active = resolveWorkflowBudget(
      { name: 'budgeted-workflow', hardened: { required: true } },
      makeRun({ [WORKFLOW_BUDGET_METADATA_KEY]: persisted }),
      'digest-1',
      [makeGrant()],
      { input: 5, output: 50 },
      true,
      NOW_MS
    );

    expect(active?.state.consumed).toEqual({ input: 30, output: 50 });
    expect(active?.state.unknownConsumption).toBeUndefined();
  });

  test('healthy accumulation preserves valid safe-integer totals', () => {
    const active = makeActive(makeState({ consumed: { input: 1, output: 2 } }));

    const state = nextWorkflowBudgetState(active, { input: 12, output: 9 }, NOW_MS);

    expect(state).toEqual({
      workflowDigest: 'digest-1',
      deadlineAt: FUTURE_DEADLINE,
      tokens: { total: 100, input: 80, output: 60 },
      consumed: { input: 12, output: 9 },
      updatedAt: UPDATED_AT,
    });
  });
});
