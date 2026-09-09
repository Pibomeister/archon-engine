import type { WorkflowRun, WorkflowSource } from './schemas';
import { isApprovalContext } from './schemas/workflow-run';
import { computeControllerWorkflowDigest } from './controller-actions';

export const WORKFLOW_PIN_METADATA_KEY = 'hardened_workflow_pin';

export interface WorkflowPinSourceIdentity {
  source: WorkflowSource;
}

export interface WorkflowPinState {
  workflowName: string;
  workflowDigest: string;
  hardenedRequired: boolean;
  containerRequired: boolean;
  sourceIdentity?: WorkflowPinSourceIdentity;
}

export function buildWorkflowPinState(
  workflow: { name: string; hardened?: { required?: boolean }; container?: { enabled?: boolean } },
  source: WorkflowSource | undefined
): WorkflowPinState {
  return {
    workflowName: workflow.name,
    workflowDigest: computeControllerWorkflowDigest(workflow),
    hardenedRequired: workflow.hardened?.required === true,
    containerRequired: workflow.container?.enabled === true,
    ...(source !== undefined ? { sourceIdentity: { source } } : {}),
  };
}

export function workflowRunRequiresPin(
  workflow: { hardened?: { required?: boolean } },
  workflowRun: WorkflowRun | undefined,
  execContext: { kind: string }
): boolean {
  return (
    workflow.hardened?.required === true ||
    execContext.kind === 'container' ||
    workflowRun?.metadata?.isolation === 'container'
  );
}

export function assertWorkflowPinMatchesRun(
  workflowRun: WorkflowRun,
  expected: WorkflowPinState
): void {
  const persisted = readWorkflowPinState(workflowRun.metadata?.[WORKFLOW_PIN_METADATA_KEY]);
  if (persisted.workflowName !== expected.workflowName) {
    throw new Error(
      `Hardened workflow pin mismatch: expected workflow '${expected.workflowName}'.`
    );
  }
  if (persisted.workflowDigest !== expected.workflowDigest) {
    throw new Error(
      `Hardened workflow '${expected.workflowName}' pin does not match the loaded workflow definition.`
    );
  }
  if (persisted.hardenedRequired !== expected.hardenedRequired) {
    throw new Error(
      `Hardened workflow '${expected.workflowName}' pin does not match hardened policy.`
    );
  }
  if (persisted.containerRequired !== expected.containerRequired) {
    throw new Error(
      `Hardened workflow '${expected.workflowName}' pin does not match container policy.`
    );
  }
  assertSourceIdentityMatches(expected, persisted);
}

function assertSourceIdentityMatches(
  expected: WorkflowPinState,
  persisted: WorkflowPinState
): void {
  if (expected.sourceIdentity === undefined) return;
  if (persisted.sourceIdentity?.source === expected.sourceIdentity.source) return;
  throw new Error(
    `Hardened workflow '${expected.workflowName}' pin does not match workflow source identity.`
  );
}

function readWorkflowPinState(value: unknown): WorkflowPinState {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('Hardened workflow pin state is missing or malformed.');
  }
  const raw = value as Record<string, unknown>;
  if (
    typeof raw.workflowName !== 'string' ||
    typeof raw.workflowDigest !== 'string' ||
    typeof raw.hardenedRequired !== 'boolean' ||
    typeof raw.containerRequired !== 'boolean'
  ) {
    throw new Error('Hardened workflow pin state is malformed.');
  }
  return {
    workflowName: raw.workflowName,
    workflowDigest: raw.workflowDigest,
    hardenedRequired: raw.hardenedRequired,
    containerRequired: raw.containerRequired,
    ...(raw.sourceIdentity !== undefined
      ? { sourceIdentity: readSourceIdentity(raw.sourceIdentity) }
      : {}),
  };
}

function readSourceIdentity(value: unknown): WorkflowPinSourceIdentity {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('Hardened workflow pin source identity is malformed.');
  }
  const raw = value as Record<string, unknown>;
  if (raw.source !== 'bundled' && raw.source !== 'global' && raw.source !== 'project') {
    throw new Error('Hardened workflow pin source identity is malformed.');
  }
  return { source: raw.source };
}

export function isGuardedWorkflowRun(run: Pick<WorkflowRun, 'metadata'>): boolean {
  return (
    run.metadata.isolation === 'container' ||
    'hardened_controller_policy' in run.metadata ||
    WORKFLOW_PIN_METADATA_KEY in run.metadata
  );
}

export function assertNoGuardedApprovalRework(
  run: WorkflowRun,
  execContext: { kind: string }
): void {
  if (execContext.kind !== 'container' && !isGuardedWorkflowRun(run)) return;
  const rejection = run.metadata.rejection_reason;
  if (rejection !== undefined && typeof rejection !== 'string') {
    throw new Error('Guarded approval rejection state is malformed.');
  }
  if (rejection) {
    throw new Error(
      'Guarded approval rejection requires a fresh guarded run; same-run rework is forbidden.'
    );
  }
  const approval = run.metadata.approval;
  if (approval === undefined) return;
  if (
    !isApprovalContext(approval) ||
    ![undefined, 'approval', 'interactive_loop', 'writeback', 'child_workflow'].includes(
      approval.type
    ) ||
    ![undefined, null, 'approved', 'rejected'].includes(approval.resolved)
  ) {
    throw new Error('Guarded approval state is malformed.');
  }
  if (approval.type !== 'writeback' && approval.resolved === 'rejected') {
    throw new Error(
      'Guarded approval rejection requires a fresh guarded run; same-run rework is forbidden.'
    );
  }
}
