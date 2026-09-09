/**
 * Run event stream primitives. Six variants of events that render in the Run
 * detail page: text, tool_call, artifact, node_transition, approval, error.
 *
 * These are the client-side model — normalized from server workflow_events
 * rows AND from SSE events. The shape is deliberately flatter than the raw
 * event schema so EventStream rendering can switch on `kind` only.
 */

export type RunEventKind =
  | 'text'
  | 'tool_call'
  | 'artifact'
  | 'node_transition'
  | 'approval'
  | 'error'
  | 'system';

interface RunEventBase {
  id: string;
  runId: string;
  kind: RunEventKind;
  timestamp: string;
  nodeId: string | null;
}

export interface TextEvent extends RunEventBase {
  kind: 'text';
  content: string;
}

export interface ToolCallEvent extends RunEventBase {
  kind: 'tool_call';
  tool: string;
  argsSummary: string;
  args: unknown;
  result: { ok: true; durationMs: number } | { ok: false; message: string } | null;
}

export interface ArtifactEvent extends RunEventBase {
  kind: 'artifact';
  artifactType: string;
  label: string;
  url: string | null;
  path: string | null;
}

export interface NodeTransitionEvent extends RunEventBase {
  kind: 'node_transition';
  nodeName: string;
  transition: 'started' | 'completed' | 'failed' | 'skipped';
  durationMs: number | null;
  /** Only populated for `skipped` — the server's skip reason (e.g. `when_condition`, `trigger_rule`, `prior_success`). */
  skipReason: string | null;
  /** Only populated for `skipped` — the evaluated expression that gated it. */
  skipExpr: string | null;
  /**
   * `node_completed` enrichment, read straight from the persisted event payload.
   * Populated only on the `completed` transition; null on every other transition
   * (and when a provider doesn't report a given field). Not consumed by any current
   * renderer — carried so the eventual per-node detail view needn't re-touch this.
   */
  outputPreview: string | null;
  costUsd: number | null;
  stopReason: string | null;
  numTurns: number | null;
}

export interface ApprovalEvent extends RunEventBase {
  kind: 'approval';
  prompt: string;
  resolution:
    | { kind: 'approved'; at: string; comment: string | null }
    | { kind: 'rejected'; at: string; reason: string }
    | null;
}

export interface ErrorEvent extends RunEventBase {
  kind: 'error';
  message: string;
  recoverable: boolean;
}

/**
 * Workflow-lifecycle events: `workflow_started`, `workflow_completed`,
 * `workflow_failed`, and any other framework-level signals worth surfacing
 * behind the "System" toggle. These don't belong in the user/agent thread
 * but are useful when diagnosing a run.
 */
export interface SystemEvent extends RunEventBase {
  kind: 'system';
  label: string;
  detail: string;
}

export type RunEvent =
  | TextEvent
  | ToolCallEvent
  | ArtifactEvent
  | NodeTransitionEvent
  | ApprovalEvent
  | ErrorEvent
  | SystemEvent;

// Server row shape (workflow_events table).
interface RawWorkflowEvent {
  id: string;
  workflow_run_id: string;
  event_type: string;
  step_index: number | null;
  step_name: string | null;
  data: Record<string, unknown>;
  created_at: string;
}

function readString(obj: Record<string, unknown>, key: string): string {
  const v = obj[key];
  return typeof v === 'string' ? v : '';
}

function readStringOrNull(obj: Record<string, unknown>, key: string): string | null {
  const v = obj[key];
  return typeof v === 'string' ? v : null;
}

function readNumberOrNull(obj: Record<string, unknown>, key: string): number | null {
  const v = obj[key];
  return typeof v === 'number' ? v : null;
}

/**
 * DB node-event `event_type` → UI transition. Listed explicitly (rather than
 * string-slicing `node_<x>`) because `node_skipped_prior_success` — emitted on
 * resume for already-completed nodes — doesn't fit that shape, and both skip
 * variants collapse to `skipped`.
 */
const NODE_TRANSITION_BY_EVENT: Record<string, NodeTransitionEvent['transition']> = {
  node_started: 'started',
  node_completed: 'completed',
  node_failed: 'failed',
  node_skipped: 'skipped',
  node_skipped_prior_success: 'skipped',
};

const WORKFLOW_EVENT_LABELS: Record<string, string> = {
  workflow_started: 'Workflow started',
  workflow_completed: 'Workflow completed',
  workflow_failed: 'Workflow failed',
  workflow_resumed: 'Workflow resumed (prior error cleared)',
};

const CONTAINER_EVENT_LABELS: Record<string, string> = {
  container_created: 'Container created',
  container_stopped: 'Container stopped (paused)',
  container_resumed: 'Container resumed',
  container_destroyed: 'Container removed',
  writeback_requested: 'Write-back requested',
  writeback_applied: 'Changes applied to live folder',
  writeback_discarded: 'Changes discarded',
};

type RunEventBaseInput = Omit<RunEventBase, 'kind'>;

function toNodeTransitionEvent(
  base: RunEventBaseInput,
  raw: RawWorkflowEvent
): NodeTransitionEvent | null {
  const transition = NODE_TRANSITION_BY_EVENT[raw.event_type];
  if (transition === undefined) return null;
  const output = readStringOrNull(raw.data, 'node_output');
  return {
    ...base,
    kind: 'node_transition',
    nodeName: readString(raw.data, 'name') || (raw.step_name ?? ''),
    transition,
    durationMs: readNumberOrNull(raw.data, 'duration_ms'),
    skipReason: transition === 'skipped' ? readStringOrNull(raw.data, 'reason') : null,
    skipExpr: transition === 'skipped' ? readStringOrNull(raw.data, 'expr') : null,
    outputPreview: output === null ? null : output.slice(0, 300),
    costUsd: readNumberOrNull(raw.data, 'cost_usd'),
    stopReason: readStringOrNull(raw.data, 'stop_reason'),
    numTurns: readNumberOrNull(raw.data, 'num_turns'),
  };
}

function toToolCallEvent(base: RunEventBaseInput, raw: RawWorkflowEvent): ToolCallEvent | null {
  if (raw.event_type !== 'tool_called' && raw.event_type !== 'tool_completed') return null;
  return {
    ...base,
    kind: 'tool_call',
    tool: readString(raw.data, 'tool_name'),
    argsSummary: readString(raw.data, 'argsSummary'),
    args: raw.data.tool_input,
    result:
      raw.event_type === 'tool_called'
        ? null
        : { ok: true, durationMs: readNumberOrNull(raw.data, 'duration_ms') ?? 0 },
  };
}

function toArtifactEvent(base: RunEventBaseInput, raw: RawWorkflowEvent): ArtifactEvent | null {
  if (raw.event_type !== 'workflow_artifact') return null;
  return {
    ...base,
    kind: 'artifact',
    artifactType: readString(raw.data, 'artifactType'),
    label: readString(raw.data, 'label'),
    url: readStringOrNull(raw.data, 'url'),
    path: readStringOrNull(raw.data, 'path'),
  };
}

function toApprovalEvent(base: RunEventBaseInput, raw: RawWorkflowEvent): ApprovalEvent | null {
  if (raw.event_type === 'approval_requested') {
    return { ...base, kind: 'approval', prompt: readString(raw.data, 'message'), resolution: null };
  }
  if (raw.event_type !== 'approval_received') return null;

  return {
    ...base,
    kind: 'approval',
    prompt: '',
    resolution: approvalResolution(raw),
  };
}

function approvalResolution(raw: RawWorkflowEvent): ApprovalEvent['resolution'] {
  const decision = readString(raw.data, 'decision');
  if (decision === 'approved') {
    return { kind: 'approved', at: raw.created_at, comment: readStringOrNull(raw.data, 'comment') };
  }
  if (decision === 'rejected') {
    return { kind: 'rejected', at: raw.created_at, reason: readString(raw.data, 'reason') };
  }
  return null;
}

function toErrorEvent(base: RunEventBaseInput, raw: RawWorkflowEvent): ErrorEvent | null {
  if (raw.event_type !== 'error') return null;
  return {
    ...base,
    kind: 'error',
    message: readString(raw.data, 'error') || readString(raw.data, 'message'),
    recoverable: Boolean(raw.data.recoverable),
  };
}

function toWorkflowSystemEvent(base: RunEventBaseInput, raw: RawWorkflowEvent): SystemEvent | null {
  const label = WORKFLOW_EVENT_LABELS[raw.event_type];
  if (label === undefined) return null;
  return {
    ...base,
    kind: 'system',
    label,
    detail:
      readString(raw.data, 'name') ||
      readString(raw.data, 'workflow') ||
      readString(raw.data, 'message') ||
      readString(raw.data, 'error'),
  };
}

function toContainerSystemEvent(
  base: RunEventBaseInput,
  raw: RawWorkflowEvent
): SystemEvent | null {
  const label = CONTAINER_EVENT_LABELS[raw.event_type];
  if (label === undefined) return null;
  return { ...base, kind: 'system', label, detail: containerEventDetail(raw) };
}

function containerEventDetail(raw: RawWorkflowEvent): string {
  if (raw.event_type === 'writeback_applied') {
    const filesApplied = readNumberOrNull(raw.data, 'files_applied') ?? 0;
    const filesDeleted = readNumberOrNull(raw.data, 'files_deleted') ?? 0;
    return `${filesApplied} written, ${filesDeleted} deleted`;
  }
  if (raw.event_type === 'writeback_requested') {
    const totalCount = readNumberOrNull(raw.data, 'total_count');
    return totalCount !== null ? `${totalCount} file(s) changed` : '';
  }
  const containerId = readString(raw.data, 'containerId');
  return containerId ? containerId.slice(0, 12) : '';
}

function toParseWarningsEvent(base: RunEventBaseInput, raw: RawWorkflowEvent): TextEvent | null {
  if (raw.event_type !== 'workflow_parse_warnings') return null;
  const warnings = Array.isArray(raw.data.warnings)
    ? raw.data.warnings.filter((w): w is string => typeof w === 'string')
    : [];
  return {
    ...base,
    kind: 'text',
    content:
      warnings.length > 0
        ? `⚠️ This workflow declares keys the engine ignores:\n${warnings.map(w => `- ${w}`).join('\n')}`
        : '⚠️ This workflow declares keys the engine ignores.',
  };
}

function toFallbackTextEvent(base: RunEventBaseInput, raw: RawWorkflowEvent): TextEvent {
  return {
    ...base,
    kind: 'text',
    content:
      readString(raw.data, 'text') ||
      readString(raw.data, 'message') ||
      `${raw.event_type} — ${JSON.stringify(raw.data).slice(0, 200)}`,
  };
}

/**
 * Best-effort normalizer from a raw workflow_events row to a typed RunEvent.
 * Unknown event types fall through as text events with the raw payload —
 * the spike surfaces them rather than silently dropping.
 */
export function toRunEvent(raw: RawWorkflowEvent): RunEvent {
  const base = {
    id: raw.id,
    runId: raw.workflow_run_id,
    timestamp: raw.created_at,
    nodeId: raw.step_name,
  };

  return (
    toNodeTransitionEvent(base, raw) ??
    toToolCallEvent(base, raw) ??
    toArtifactEvent(base, raw) ??
    toApprovalEvent(base, raw) ??
    toErrorEvent(base, raw) ??
    toWorkflowSystemEvent(base, raw) ??
    toContainerSystemEvent(base, raw) ??
    toParseWarningsEvent(base, raw) ??
    toFallbackTextEvent(base, raw)
  );
}

/**
 * One node's whole lifecycle, folded from its 2–3 `node_transition` events into a
 * single record. A node emits `node_started` + a terminal (`node_completed` /
 * `node_failed` / `node_skipped`), and a resumed run reuses one run id so the same
 * node can ALSO carry a later `node_skipped_prior_success`. The run stream renders
 * one `NodeRun` per node instead of one divider per raw transition.
 */
export interface NodeRun {
  /** `step_name`. Null-id transitions can't be keyed and are excluded from the fold. */
  nodeId: string;
  nodeName: string;
  /** `running` = only a `started` transition seen so far (in-flight). */
  status: 'running' | 'completed' | 'failed' | 'skipped';
  /** Earliest transition timestamp — positions the single divider in the stream. */
  startedAt: string;
  /** Terminal transition timestamp; null while still running. */
  endedAt: string | null;
  durationMs: number | null;
  /** Written by the engine only on `node_completed`; null for non-AI nodes and any non-completed terminal. */
  costUsd: number | null;
  numTurns: number | null;
  stopReason: string | null;
  skipReason: string | null;
  skipExpr: string | null;
}

/**
 * Folds a run's `node_transition` events into one `NodeRun` per node, keyed by
 * `nodeId`. Status precedence is `completed > failed > skipped > running` —
 * "ever completed wins" (a completed-then-resume-skipped node stays `completed`),
 * matching the dedup `countTerminalNodes` relies on. Null-`nodeId` transitions are
 * skipped (can't be keyed). Returned sorted by `startedAt`.
 */
function transitionBuckets(transitions: NodeTransitionEvent[]): {
  completed: NodeTransitionEvent | null;
  failed: NodeTransitionEvent | null;
  skipped: NodeTransitionEvent | null;
  nodeName: string;
  startedAt: string;
} {
  let completed: NodeTransitionEvent | null = null;
  let failed: NodeTransitionEvent | null = null;
  let skipped: NodeTransitionEvent | null = null;
  let nodeName = '';
  let startedAt = transitions[0]?.timestamp ?? '';

  for (const transition of transitions) {
    if (new Date(transition.timestamp).getTime() < new Date(startedAt).getTime()) {
      startedAt = transition.timestamp;
    }
    if (nodeName === '' && transition.nodeName !== '') nodeName = transition.nodeName;
    if (transition.transition === 'completed') completed = transition;
    else if (transition.transition === 'failed') failed = transition;
    else if (transition.transition === 'skipped') skipped = transition;
  }

  return { completed, failed, skipped, nodeName, startedAt };
}

function nodeRunStatus(
  completed: NodeTransitionEvent | null,
  failed: NodeTransitionEvent | null,
  skipped: NodeTransitionEvent | null
): NodeRun['status'] {
  if (completed !== null) return 'completed';
  if (failed !== null) return 'failed';
  if (skipped !== null) return 'skipped';
  return 'running';
}

function toNodeRun(nodeId: string, transitions: NodeTransitionEvent[]): NodeRun {
  const { completed, failed, skipped, nodeName, startedAt } = transitionBuckets(transitions);
  const terminal = completed ?? failed ?? skipped;
  return {
    nodeId,
    nodeName: nodeName !== '' ? nodeName : nodeId,
    status: nodeRunStatus(completed, failed, skipped),
    startedAt,
    endedAt: terminal?.timestamp ?? null,
    durationMs: terminal?.durationMs ?? null,
    costUsd: completed?.costUsd ?? null,
    numTurns: completed?.numTurns ?? null,
    stopReason: completed?.stopReason ?? null,
    skipReason: skipped?.skipReason ?? null,
    skipExpr: skipped?.skipExpr ?? null,
  };
}

export function foldNodeRuns(events: RunEvent[]): NodeRun[] {
  const byNode = new Map<string, NodeTransitionEvent[]>();
  for (const event of events) {
    if (event.kind !== 'node_transition' || event.nodeId === null) continue;
    const bucket = byNode.get(event.nodeId) ?? [];
    bucket.push(event);
    byNode.set(event.nodeId, bucket);
  }

  return [...byNode.entries()]
    .map(([nodeId, transitions]) => toNodeRun(nodeId, transitions))
    .sort((a, b) => new Date(a.startedAt).getTime() - new Date(b.startedAt).getTime());
}

/**
 * Per-node terminal tally for a run's node-count readout (e.g. `7/8 nodes`).
 * Derived from {@link foldNodeRuns} so the dedup is single-sourced: `total` =
 * distinct nodes that reached a terminal (non-`running`) state; `completed` =
 * distinct nodes that ever completed (a completed-then-resume-skipped node stays
 * counted). Nodes with a null `nodeId` are excluded by the fold.
 */
export function countTerminalNodes(events: RunEvent[]): { completed: number; total: number } {
  let completed = 0;
  let total = 0;
  for (const r of foldNodeRuns(events)) {
    if (r.status === 'running') continue;
    total += 1;
    if (r.status === 'completed') completed += 1;
  }
  return { completed, total };
}
