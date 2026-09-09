import type { WorkflowHardenedPolicy, WorkflowRun } from './schemas';

export const WORKFLOW_BUDGET_METADATA_KEY = 'hardened_budget';

export interface WorkflowTokenBudget {
  input?: number;
  output?: number;
  total: number;
}

export interface WorkflowBudgetGrant {
  runId: string;
  workflowName: string;
  workflowDigest: string;
  deadlineAt: string;
  tokens: WorkflowTokenBudget;
  authoritativeConsumed?: { input: number; output: number };
}

export interface WorkflowBudgetState {
  workflowDigest: string;
  deadlineAt: string;
  tokens: WorkflowTokenBudget;
  consumed: { input: number; output: number };
  unknownConsumption?: boolean;
  updatedAt: string;
}

export interface ActiveWorkflowBudget {
  state: WorkflowBudgetState;
  deadlineAtMs: number;
}

export function resolveWorkflowBudget(
  workflow: { name: string; hardened?: WorkflowHardenedPolicy },
  workflowRun: WorkflowRun,
  workflowDigest: string,
  grants: readonly WorkflowBudgetGrant[] | undefined,
  priorTokenUsage: { input: number; output: number } | undefined,
  isResume: boolean,
  nowMs = Date.now(),
  reconcileFromPriorUsage = false
): ActiveWorkflowBudget | undefined {
  if (workflow.hardened?.required !== true) return undefined;

  const grant = grants?.find(
    candidate =>
      candidate.runId === workflowRun.id &&
      candidate.workflowName === workflow.name &&
      candidate.workflowDigest === workflowDigest
  );
  if (!grant) {
    throw new Error(
      `Hardened workflow '${workflow.name}' requires a controller-private budget grant for run '${workflowRun.id}'.`
    );
  }

  const deadlineAtMs = parseDeadline(grant.deadlineAt, 'grant');
  if (deadlineAtMs <= nowMs) {
    throw new Error(`Hardened workflow '${workflow.name}' budget deadline has already expired.`);
  }
  validateTokenBudget(grant.tokens, 'grant');
  if (grant.authoritativeConsumed !== undefined) {
    throw new Error(
      `Hardened workflow '${workflow.name}' budget grant cannot carry caller-supplied authoritative consumption.`
    );
  }
  validateOptionalUsage(priorTokenUsage, 'prior token usage');

  const persisted = readPersistedBudget(workflowRun.metadata?.[WORKFLOW_BUDGET_METADATA_KEY]);
  if (persisted) {
    assertPersistedMatchesGrant(workflow.name, persisted, grant);
    if (persisted.unknownConsumption === true && priorTokenUsage === undefined) {
      throw new Error(
        `Hardened workflow '${workflow.name}' has unknown prior budget consumption; controller-verified budget ledger consumption is required before resuming or continuing.`
      );
    }
  } else if (isResume) {
    throw new Error(
      `Hardened workflow '${workflow.name}' resume is missing persisted budget state.`
    );
  }

  const consumed = resolveInitialBudgetConsumption(
    workflow.name,
    persisted,
    priorTokenUsage,
    reconcileFromPriorUsage
  );
  assertWithinBudget(workflow.name, grant.tokens, consumed);

  return {
    deadlineAtMs,
    state: {
      workflowDigest,
      deadlineAt: grant.deadlineAt,
      tokens: { ...grant.tokens },
      consumed,
      ...(shouldPreserveUnknownConsumption(persisted, priorTokenUsage, reconcileFromPriorUsage)
        ? { unknownConsumption: true }
        : {}),
      updatedAt: new Date(nowMs).toISOString(),
    },
  };
}

function resolveInitialBudgetConsumption(
  workflowName: string,
  persisted: WorkflowBudgetState | undefined,
  priorTokenUsage: { input: number; output: number } | undefined,
  reconcileFromPriorUsage: boolean
): { input: number; output: number } {
  if (!reconcileFromPriorUsage) return maxUsage(persisted?.consumed, priorTokenUsage);
  if (priorTokenUsage !== undefined) return priorTokenUsage;
  throw new Error(
    `Hardened workflow '${workflowName}' verified budget reconciliation requires controller usage.`
  );
}

function shouldPreserveUnknownConsumption(
  persisted: WorkflowBudgetState | undefined,
  priorTokenUsage: { input: number; output: number } | undefined,
  reconcileFromPriorUsage: boolean
): boolean {
  return (
    persisted?.unknownConsumption === true &&
    priorTokenUsage === undefined &&
    !reconcileFromPriorUsage
  );
}

export function nextWorkflowBudgetState(
  active: ActiveWorkflowBudget,
  consumed: { input: number; output: number },
  nowMs = Date.now()
): WorkflowBudgetState {
  assertValidConsumedUsage(consumed);
  const monotonicConsumed = maxUsage(active.state.consumed, consumed);
  return {
    ...active.state,
    consumed: monotonicConsumed,
    updatedAt: new Date(nowMs).toISOString(),
  };
}

export function unknownWorkflowBudgetState(
  active: ActiveWorkflowBudget,
  consumed: { input: number; output: number },
  nowMs = Date.now()
): WorkflowBudgetState {
  assertValidConsumedUsage(consumed);
  const monotonicConsumed = maxUsage(active.state.consumed, consumed);
  return {
    ...active.state,
    consumed: monotonicConsumed,
    unknownConsumption: true,
    updatedAt: new Date(nowMs).toISOString(),
  };
}

export function assertWorkflowBudgetCanContinue(
  budget: ActiveWorkflowBudget | undefined,
  label: string,
  consumed?: { input: number; output: number },
  nowMs = Date.now()
): void {
  if (!budget) return;
  if (nowMs >= budget.deadlineAtMs) {
    throw new Error(`${label} exceeded hardened workflow deadline.`);
  }
  if (consumed) assertWithinBudget(label, budget.state.tokens, consumed);
}

function parseDeadline(value: string, label: string): number {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`Hardened workflow budget ${label} deadline is missing.`);
  }
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) {
    throw new Error(`Hardened workflow budget ${label} deadline is malformed.`);
  }
  return ms;
}

function validateTokenBudget(tokens: WorkflowTokenBudget | undefined, label: string): void {
  if (!tokens || !isPositiveSafeInteger(tokens.total)) {
    throw new Error(
      `Hardened workflow budget ${label} must include a positive safe integer total token limit.`
    );
  }
  if (tokens.input !== undefined && !isPositiveSafeInteger(tokens.input)) {
    throw new Error(
      `Hardened workflow budget ${label} input token limit must be a positive safe integer.`
    );
  }
  if (tokens.output !== undefined && !isPositiveSafeInteger(tokens.output)) {
    throw new Error(
      `Hardened workflow budget ${label} output token limit must be a positive safe integer.`
    );
  }
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function readPersistedBudget(value: unknown): WorkflowBudgetState | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('Hardened workflow persisted budget state is malformed.');
  }
  const raw = value as Record<string, unknown>;
  const tokens = raw.tokens as WorkflowTokenBudget | undefined;
  const consumed = raw.consumed as { input?: unknown; output?: unknown } | undefined;
  if (
    typeof raw.workflowDigest !== 'string' ||
    typeof raw.deadlineAt !== 'string' ||
    !tokens ||
    !consumed ||
    typeof raw.updatedAt !== 'string' ||
    !Number.isFinite(Date.parse(raw.updatedAt)) ||
    !isNonnegativeSafeInteger(consumed.input) ||
    !isNonnegativeSafeInteger(consumed.output) ||
    (raw.unknownConsumption !== undefined && typeof raw.unknownConsumption !== 'boolean')
  ) {
    throw new Error('Hardened workflow persisted budget state is malformed.');
  }
  parseDeadline(raw.deadlineAt, 'persisted');
  validateTokenBudget(tokens, 'persisted');
  return {
    workflowDigest: raw.workflowDigest,
    deadlineAt: raw.deadlineAt,
    tokens: { ...tokens },
    consumed: { input: consumed.input, output: consumed.output },
    ...(raw.unknownConsumption === true ? { unknownConsumption: true } : {}),
    updatedAt: raw.updatedAt,
  };
}

function assertPersistedMatchesGrant(
  workflowName: string,
  persisted: WorkflowBudgetState,
  grant: WorkflowBudgetGrant
): void {
  if (
    persisted.workflowDigest !== grant.workflowDigest ||
    persisted.deadlineAt !== grant.deadlineAt ||
    persisted.tokens.total !== grant.tokens.total ||
    persisted.tokens.input !== grant.tokens.input ||
    persisted.tokens.output !== grant.tokens.output
  ) {
    throw new Error(
      `Hardened workflow '${workflowName}' budget grant does not match persisted budget state.`
    );
  }
}

function maxUsage(...usages: ({ input: number; output: number } | undefined)[]): {
  input: number;
  output: number;
} {
  let input = 0;
  let output = 0;
  for (const usage of usages) {
    input = Math.max(input, usage?.input ?? 0);
    output = Math.max(output, usage?.output ?? 0);
  }
  return { input, output };
}

function validateOptionalUsage(
  usage: { input: number; output: number } | undefined,
  label: string
): void {
  if (usage === undefined) return;
  if (!isNonnegativeSafeInteger(usage.input) || !isNonnegativeSafeInteger(usage.output)) {
    throw new Error(`Hardened workflow ${label} is malformed.`);
  }
}

function assertValidConsumedUsage(consumed: { input: number; output: number }): void {
  if (!isNonnegativeSafeInteger(consumed.input) || !isNonnegativeSafeInteger(consumed.output)) {
    throw new Error('Hardened workflow consumed budget state is malformed.');
  }
}

function assertWithinBudget(
  label: string,
  tokens: WorkflowTokenBudget,
  consumed: { input: number; output: number }
): void {
  assertValidConsumedUsage(consumed);
  if (consumed.input > tokens.total - consumed.output) {
    throw new Error(`${label} exceeded hardened workflow total token budget.`);
  }
  if (tokens.input !== undefined && consumed.input > tokens.input) {
    throw new Error(`${label} exceeded hardened workflow input token budget.`);
  }
  if (tokens.output !== undefined && consumed.output > tokens.output) {
    throw new Error(`${label} exceeded hardened workflow output token budget.`);
  }
}
