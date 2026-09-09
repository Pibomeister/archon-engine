/**
 * DAG Workflow Executor
 *
 * Executes a `nodes:`-based workflow in topological order.
 * Independent nodes within the same layer run concurrently via Promise.allSettled.
 * Captures all assistant output regardless of streaming mode for $node_id.output substitution.
 */
import { existsSync, writeFileSync } from 'fs';
import { readFile } from 'fs/promises';
import { isAbsolute, join as joinPath, resolve as resolvePath } from 'path';
import { execFileAsync, resolveBashPath } from '@archon/git';
import { assertNoGuardedApprovalRework } from './workflow-pinning';
import { discoverScriptsForCwd } from './script-discovery';
import { discoverWorkflowsWithConfig } from './workflow-discovery';
import { resolveWorkflowName } from './router';
import type {
  IWorkflowPlatform,
  WorkflowMessageMetadata,
  WorkflowConfig,
  WorkflowDeps,
} from './deps';
import type {
  SendQueryOptions,
  NodeConfig,
  ProviderCapabilities,
  TokenUsage,
  ResolvedModel,
  MessageChunk,
  ExecutionContext,
  OverlayChangeSummary,
} from '@archon/providers/types';
import { CONTAINER_ENV_DENYLIST } from '@archon/providers/types';
import type { ContainerRunContext, VerifiedProxyBudgetStatus } from './container-context';
import {
  WRITEBACK_GATE_NODE_ID,
  snapshotDrainedContainerArtifacts,
  runGuardedContainerSubprocess,
} from './container-context';
import {
  getProviderCapabilities,
  getRegisteredProviders,
  isRegisteredProvider,
  validateStructuredOutput,
} from '@archon/providers';
import type {
  DagNode,
  ApprovalNode,
  BashNode,
  CommandNode,
  PromptNode,
  LoopNode,
  LoopGroupNode,
  ScriptNode,
  WorkflowNode,
  FanOutConfig,
  NodeOutput,
  TriggerRule,
  WorkflowRun,
  EffortLevel,
  ThinkingConfig,
  SandboxSettings,
  WorkflowSource,
  WorkflowDefinition,
  LoopGateRunMetadata,
  ApprovalContext,
  WorkflowEvidencePolicy,
  WorkflowHardenedPolicy,
  ControllerActionNode,
} from './schemas';
import {
  isBashNode,
  isLoopNode,
  isLoopGroupNode,
  isApprovalNode,
  isCancelNode,
  isControllerActionNode,
  isScriptNode,
  isIncludeNode,
  isWorkflowNode,
  isPersistableNode,
  readSubrunMetadata,
  isApprovalContext,
} from './schemas';
import {
  computeControllerWorkflowDigest,
  isControllerActionManifestSealed,
  normalizeControllerActionOutput,
  sealControllerActionManifest,
} from './controller-actions';
import type { ControllerActionGrant } from './controller-actions';
import { formatToolCall } from './utils/tool-formatter';
import { createLogger, captureWorkflowCompleted } from '@archon/paths';
import type { WorkflowErrorClass, WorkflowNodeType } from '@archon/paths';
import { getWorkflowEventEmitter } from './event-emitter';
import { evaluateCondition } from './condition-evaluator';
import {
  declaredFieldsFromSchema,
  resolveNodeOutputField,
  OutputRefError,
  similarNodeIds,
} from './output-ref';
import { buildTruncationMarker } from './utils/output-truncation';
import { writeNodeArtifact, readNodeArtifacts } from './artifacts-index';
import {
  logNodeStart,
  logNodeComplete,
  logNodeSkip,
  logNodeError,
  logAssistant,
  logTool,
  logWorkflowComplete,
  logWorkflowError,
} from './logger';
import { withIdleTimeout, STEP_IDLE_TIMEOUT_MS } from './utils/idle-timeout';
import { mapWithLimit } from './utils/map-with-limit';
import {
  classifyError,
  toTelemetryErrorClass,
  detectCreditExhaustion,
  loadCommandPrompt,
  substituteWorkflowVariables,
  resolveAgentOutputPaths,
  buildPromptWithContext,
  detectCompletionSignal,
  stripCompletionTags,
  isInlineScript,
  formatSubprocessFailure,
  safeSendMessage,
  type SendMessageContext,
} from './executor-shared';
import {
  isLiteralSpec,
  isTierName,
  resolveModelSpec,
  routePresetEffort,
  type ModelAliasPreset,
  type ResolvedAiProfile,
  type TierName,
} from './model-validation';
import {
  WORKFLOW_BUDGET_METADATA_KEY,
  assertWorkflowBudgetCanContinue,
  nextWorkflowBudgetState,
  resolveWorkflowBudget,
  unknownWorkflowBudgetState,
  type ActiveWorkflowBudget,
  type WorkflowBudgetState,
} from './budget';

/**
 * Closed-set node type for telemetry — mirrors the DagNode discriminators.
 * The final `'prompt'` arm is the fallthrough: a future node type added to
 * the schema without a guard here would be reported as `'prompt'` (a metrics
 * misclassification, not a privacy issue) — extend this when adding node types.
 */
function dagNodeTelemetryType(node: DagNode): WorkflowNodeType {
  if (isBashNode(node)) return 'bash';
  if (isScriptNode(node)) return 'script';
  if (isLoopNode(node)) return 'loop';
  if (isLoopGroupNode(node)) return 'loop_group';
  if (isApprovalNode(node)) return 'approval';
  if (isCancelNode(node)) return 'cancel';
  if (isControllerActionNode(node)) return 'controller_action';
  if ('command' in node) return 'command';
  return 'prompt';
}

interface RunningTool {
  toolName: string;
  startedAt: number;
}

function findRunningTool(
  runningTools: Map<string, RunningTool>,
  toolName: string,
  toolCallId: string | undefined
): [string, RunningTool] | undefined {
  if (toolCallId) {
    const tool = runningTools.get(toolCallId);
    return tool ? [toolCallId, tool] : undefined;
  }

  return Array.from(runningTools.entries())
    .reverse()
    .find(([, tool]) => tool.toolName === toolName);
}

/**
 * Usage totals for the terminal telemetry event. Fields are omitted (not sent
 * as zero) when nothing was reported, so absence in PostHog means "providers
 * reported no usage", never "zero spend".
 */
function buildRunUsageProps(totals: {
  costUsd: number;
  tokensIn: number;
  tokensOut: number;
  loopIterations: number;
}): { costUsd?: number; tokensIn?: number; tokensOut?: number; loopIterations?: number } {
  return {
    ...(totals.costUsd > 0 ? { costUsd: totals.costUsd } : {}),
    ...(totals.tokensIn > 0 || totals.tokensOut > 0
      ? { tokensIn: totals.tokensIn, tokensOut: totals.tokensOut }
      : {}),
    ...(totals.loopIterations > 0 ? { loopIterations: totals.loopIterations } : {}),
  };
}

/**
 * Failure taxonomy for the terminal telemetry event: the first failed node's
 * type and a fixed-enum error class derived from its stored error message.
 * Returns {} when nothing failed. Categorical only — the error text itself
 * is classified locally and never transmitted.
 */
function firstFailedNodeTaxonomy(
  nodeOutputs: Map<string, NodeOutput>,
  nodes: readonly DagNode[]
): { errorClass?: WorkflowErrorClass; failedNodeType?: WorkflowNodeType } {
  for (const [nodeId, output] of nodeOutputs) {
    if (output.state !== 'failed') continue;
    const node = nodes.find(n => n.id === nodeId);
    const taxonomy: { errorClass: WorkflowErrorClass; failedNodeType?: WorkflowNodeType } = {
      errorClass: toTelemetryErrorClass(classifyError(new Error(output.error))),
    };
    if (node) {
      taxonomy.failedNodeType = dagNodeTelemetryType(node);
    }
    return taxonomy;
  }
  return {};
}

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('workflow.dag-executor');
  return cachedLog;
}

const MCP_FAILURE_PREFIX = 'MCP server connection failed: ';

/** A failed MCP server entry parsed from the SDK message. `segment` is the
 *  original substring (e.g. `"telegram (disconnected)"`) so callers can
 *  reconstruct a filtered message without losing the status detail. */
export interface McpFailureEntry {
  name: string;
  segment: string;
}

function applyPresetOptions(
  provider: string,
  preset: ModelAliasPreset | undefined,
  node: DagNode,
  workflowLevelOptions: WorkflowLevelOptions,
  nodeConfig: NodeConfig,
  assistantConfig: Record<string, unknown>
): void {
  if (!preset) return;

  if (
    preset.thinking !== undefined &&
    node.thinking === undefined &&
    workflowLevelOptions.thinking === undefined
  ) {
    nodeConfig.thinking = preset.thinking;
  }

  if (
    preset.effort === undefined ||
    node.effort !== undefined ||
    workflowLevelOptions.effort !== undefined
  ) {
    return;
  }

  const routed = routePresetEffort(provider, preset.effort);
  if (!routed) {
    // Cross-provider effort mismatch (e.g. a `tiers:` entry sets `effort: max`
    // on a Codex tier). Warn rather than silently drop it — fail-loud per the
    // project's fail-fast guideline.
    getLog().warn(
      { provider, effort: preset.effort, nodeId: node.id },
      'dag.preset_effort_unsupported'
    );
    return;
  }
  if (routed.field === 'effort') {
    nodeConfig.effort = routed.value;
  } else {
    assistantConfig.modelReasoningEffort = routed.value;
  }
}

/**
 * Parse the SDK's "MCP server connection failed: a (status), b (status)"
 * message. Best-effort — malformed or prefix-free messages return `[]`.
 * Entries are ordered and deduped by name; the segment of the first
 * occurrence wins.
 */
export function parseMcpFailureServerNames(message: string): McpFailureEntry[] {
  if (!message.startsWith(MCP_FAILURE_PREFIX)) return [];
  const seen = new Set<string>();
  const entries: McpFailureEntry[] = [];
  for (const raw of message.slice(MCP_FAILURE_PREFIX.length).split(', ')) {
    const segment = raw.trim();
    const name = segment.split(' (')[0]?.trim();
    if (name && !seen.has(name)) {
      seen.add(name);
      entries.push({ name, segment });
    }
  }
  return entries;
}

/**
 * Load the set of MCP server names that a node's `mcp:` config file declares.
 *
 * Returns an empty set when no `mcp:` is configured or when the file can't be
 * read/parsed. Used to distinguish workflow-configured failures (surface to
 * user) from user-plugin failures (silent debug log). We intentionally do not
 * validate or env-expand here — the provider owns full loading and will
 * surface its own parse errors via the warning channel if the file is broken.
 *
 * Read failures are debug-logged so a transient I/O error (EMFILE/EBUSY) that
 * leaves us with an empty set — and silently reclassifies a real workflow-MCP
 * failure as plugin noise — is at least observable.
 */
export async function loadConfiguredMcpServerNames(
  nodeMcpPath: string | undefined,
  cwd: string
): Promise<Set<string>> {
  if (!nodeMcpPath) return new Set();
  const fullPath = isAbsolute(nodeMcpPath) ? nodeMcpPath : resolvePath(cwd, nodeMcpPath);
  try {
    const raw = await readFile(fullPath, 'utf-8');
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      return new Set();
    }
    return new Set(Object.keys(parsed as Record<string, unknown>));
  } catch (err) {
    getLog().debug({ err, nodeMcpPath, fullPath }, 'dag.mcp_filter_config_read_failed');
    return new Set();
  }
}

/** Workflow-level Claude SDK options — per-node overrides take precedence via ?? */
interface WorkflowLevelOptions {
  effort?: EffortLevel;
  thinking?: ThinkingConfig;
  fallbackModel?: string;
  betas?: string[];
  sandbox?: SandboxSettings;
  /** Workflow-level tier keyword (when `workflow.model` is small/medium/large), so
   *  nodes that inherit the workflow model can still surface the `← tier` annotation. */
  workflowTier?: 'small' | 'medium' | 'large';
}

/** Internal node execution result — extends NodeOutput with cost data for aggregation. */
type NodeExecutionResult = NodeOutput & {
  costUsd?: number;
  /** Provider-reported token usage for the node (loop nodes: summed across iterations). */
  tokens?: TokenUsage;
  /** Loop nodes only: number of iterations executed. */
  loopIterations?: number;
};

type BudgetPassCheckpoint = (
  accumulatedPassOutput: NodeExecutionResult,
  rawPassOutput: NodeExecutionResult
) => Promise<void>;

interface PriorAttemptUsage {
  tokens?: TokenUsage;
  tokensUnknown: boolean;
  costUsd?: number;
}

// ---------------------------------------------------------------------------
// workflow: (sub-run) node — cross-run composition (#2121 Phase 2)
// ---------------------------------------------------------------------------

/** Terminal (or paused) outcome of a child sub-run, as consumed by a `workflow:` node. */
export interface ChildWorkflowOutcome {
  childRunId: string;
  status: 'completed' | 'paused' | 'failed' | 'cancelled';
  /** Child's terminal output (its first sink node's output), threaded as `$<id>.output`. */
  output?: string;
  /** Child run's total cost, rolled up into the parent node's costUsd (D8). */
  costUsd?: number;
  tokens?: TokenUsage;
  error?: string;
}

/** Arguments for starting (or resuming a failed) child sub-run. */
export interface RunChildWorkflowArgs {
  parentRun: WorkflowRun;
  nodeId: string;
  childWorkflowName: string;
  /** Data string forwarded as the child's user_message (substituted upstream). */
  input: string;
  cwd: string;
  /** Platform conversation id (shared with the parent). */
  conversationId: string;
  /** DB conversation UUID (shared with the parent — satisfies the child's NOT-NULL FK). */
  conversationDbId: string;
  userId?: string;
  /** Codebase id inherited from the parent (env vars + attribution). */
  codebaseId?: string;
  /**
   * Per-child isolation mode (#2121 slice 2, PR-A). `'worktree'` runs the child in
   * its own git worktree via the injected child-isolation resolver; `'inherit'`
   * (or undefined) shares the parent's checkout. Threaded from `node.isolation`.
   */
  isolation?: WorkflowNode['isolation'];
  /**
   * Fan-out instance index (#2121 slice 2, PR-C). Set when this child is one of N
   * spawned by a `fan_out:` node; stamped into the child's `metadata.child_index` so
   * parent resume can re-key the ordered instance set by index. Undefined for a
   * single (non-fan-out) `workflow:` child. Also seeds the per-child worktree branch
   * identifier so N fan-out children get distinct worktrees.
   */
  childIndex?: number;
  /**
   * Content hash of a fan-out child's input (#2121 slice 2, PR-C). Stamped into
   * `metadata.fan_out_item_hash` at spawn so parent resume can WARN when a
   * non-deterministic items producer changed the item at a given index (never re-keys).
   */
  itemHash?: string;
  /** Present only when re-driving a FAILED child on parent resume (D5 recovery path). */
  resumeFailedChild?: WorkflowRun;
}

/**
 * Injected closure that starts a child workflow run in-process (#2121 Phase 2).
 * Defined in executor.ts — it captures `executeWorkflow` from the SAME module, so
 * there is no static import cycle — and threaded through executeDagWorkflow →
 * RunLayersContext so a `workflow:` node can spawn its child without dag-executor
 * importing executor.
 */
export type RunChildWorkflowFn = (args: RunChildWorkflowArgs) => Promise<ChildWorkflowOutcome>;

/**
 * Derive a child's node-facing outcome from its persisted run row. Cost, tokens,
 * and the terminal `summary` are written into the child run's metadata at
 * completion (see executeDagWorkflow completion + Task 12), so both the
 * synchronous path (runChildWorkflow reads the row back) and the re-entry path
 * (executeWorkflowNode finds an already-terminal child) read the same source.
 */
export function childOutcomeFromRun(run: WorkflowRun): ChildWorkflowOutcome {
  if (run.status === 'running' || run.status === 'pending') {
    // Fail fast instead of a blind narrowing cast: every caller must hand this a
    // settled (terminal or paused) run. A non-settled status slipping through
    // would fall out of interpret()'s switch and corrupt the node result with
    // `undefined` — throwing turns that into a loud, attributable node failure.
    throw new Error(
      `Sub-run ${run.id} is still '${run.status}' — cannot derive a node outcome from an unsettled run.`
    );
  }
  const md: Record<string, unknown> = run.metadata ?? {};
  const input = typeof md.total_tokens_in === 'number' ? md.total_tokens_in : undefined;
  const output = typeof md.total_tokens_out === 'number' ? md.total_tokens_out : undefined;
  const tokens =
    input !== undefined || output !== undefined
      ? { input: input ?? 0, output: output ?? 0 }
      : undefined;
  return {
    childRunId: run.id,
    status: run.status,
    output: typeof md.summary === 'string' ? md.summary : undefined,
    costUsd: typeof md.total_cost_usd === 'number' ? md.total_cost_usd : undefined,
    tokens,
    error: typeof md.error === 'string' ? md.error : undefined,
  };
}

/**
 * Sequential-session threading cursor. Tagged with the resolved provider that produced
 * the session so a downstream sequential node on a DIFFERENT provider starts fresh
 * instead of attempting an impossible cross-provider resume (#1992) — a foreign session
 * id hard-fails Claude ("No conversation found with session ID") and cold-falls-back
 * on Codex.
 */
interface SequentialSessionCursor {
  sessionId: string;
  provider: string;
}

/** Per-node result surfaced by a runLayers layer closure. `sessionProvider` tags which
 *  resolved provider created `output.sessionId` (session-producing paths only). */
interface LayerNodeResult {
  nodeId: string;
  output: NodeExecutionResult;
  sessionProvider?: string;
}

/** Throttle state for cancel checks (reads — no write contention in WAL mode) */
const lastNodeCancelCheck = new Map<string, number>();
const CANCEL_CHECK_INTERVAL_MS = 10_000;

/**
 * Policy for the during-streaming cancel check: should the currently-streaming
 * node be allowed to continue for a given observed run status?
 *
 * - `running`: the normal case → continue.
 * - `paused`: a concurrent approval node in the same topological layer has
 *   transitioned the run to paused. The streaming node should finish its own
 *   output; workflow progression is gated by the approval node, not by tearing
 *   down unrelated in-flight streams.
 * - `null` (run deleted), `cancelled`, `failed`, `completed`, or any other
 *   state → abort the stream.
 *
 * Exported for unit testing; the full streaming-cancel branch in
 * `executeNodeInternal` only fires once per 10s (CANCEL_CHECK_INTERVAL_MS), so
 * integration-level coverage of the policy is timing-sensitive and flaky.
 */
export function shouldContinueStreamingForStatus(status: string | null): boolean {
  return status === 'running' || status === 'paused';
}

/** Throttle state for activity heartbeat writes (only used for stale/zombie detection) */
const lastNodeActivityUpdate = new Map<string, number>();
const ACTIVITY_HEARTBEAT_INTERVAL_MS = 60_000;

/** Default DAG node retry for TRANSIENT errors */
const DEFAULT_NODE_MAX_RETRIES = 2;
const DEFAULT_NODE_RETRY_DELAY_MS = 3000;

/**
 * Max validate-and-reask attempts for a `best-effort` provider whose structured
 * output fails schema validation (separate from transient-error retries above).
 * Enforced providers don't reask — a validation failure there is a genuine edge
 * (refusal / max_tokens truncation) and fails fast.
 */
const STRUCTURED_OUTPUT_MAX_REASKS = 3;

/**
 * Tracks live background Agent tasks within one provider stream pass (#2083).
 *
 * Since Claude SDK 0.3.193 the model can delegate work to asynchronous
 * background agents, so a `result` chunk only means "top-level turn done" —
 * NOT "all work done". Breaking out of the stream loop at a result while
 * background tasks are live calls `.return()` on the generator chain, which
 * tears down the SDK subprocess (SIGTERM) and kills the tasks — the artifacts
 * they were producing silently never appear.
 *
 * Fed by the provider's `background_tasks` chunk (SDK `background_tasks_changed`,
 * v0.3.209+): a level signal carrying the FULL live set, REPLACE semantics.
 * Both dag-executor stream loops (AI node + loop iteration) instantiate one
 * tracker per stream pass and gate their break-on-result on it: when the set
 * is non-empty, keep consuming — the SDK keeps the subprocess alive until the
 * tasks drain, gives the agent a follow-up turn to integrate their output, and
 * emits a final `result` (verified empirically against SDK 0.3.209). The wait
 * is bounded by the existing idle-timeout machinery: `task_progress` chunks
 * (~30s cadence while subagents run) reset the idle timer, and a genuinely
 * hung task hits the normal idle-timeout path.
 *
 * Providers that never emit the chunk (Codex/Pi/OpenCode/Copilot, older Claude
 * CLIs) leave the set empty → break-on-first-result behavior is unchanged.
 */
function createBackgroundTaskTracker(): {
  update(tasks: { taskId: string; description: string }[]): void;
  shouldBreakOnResult(): boolean;
  count(): number;
  ids(): string[];
  /** True exactly once — lets the caller announce the wait a single time per pass. */
  shouldAnnounceWait(): boolean;
} {
  const live = new Map<string, string>(); // taskId → description
  let announced = false;
  return {
    update(tasks): void {
      live.clear();
      for (const t of tasks) live.set(t.taskId, t.description);
    },
    shouldBreakOnResult(): boolean {
      return live.size === 0;
    },
    count(): number {
      return live.size;
    },
    ids(): string[] {
      return [...live.keys()];
    },
    shouldAnnounceWait(): boolean {
      if (announced) return false;
      announced = true;
      return true;
    },
  };
}

/**
 * Get effective retry config for a DAG node.
 */
function getEffectiveNodeRetryConfig(node: DagNode): {
  maxRetries: number;
  delayMs: number;
  onError: 'transient' | 'all';
} {
  if ('retry' in node && node.retry) {
    return {
      maxRetries: node.retry.max_attempts,
      delayMs: node.retry.delay_ms ?? DEFAULT_NODE_RETRY_DELAY_MS,
      onError: node.retry.on_error ?? 'transient',
    };
  }
  return {
    maxRetries: DEFAULT_NODE_MAX_RETRIES,
    delayMs: DEFAULT_NODE_RETRY_DELAY_MS,
    onError: 'transient',
  };
}

/**
 * Retry config for a deterministic (bash/script) node.
 *
 * Same field mapping as {@link getEffectiveNodeRetryConfig}, but deterministic
 * nodes get NO default: an absent `retry:` block returns `undefined` (single
 * attempt) rather than the AI-node default of {@link DEFAULT_NODE_MAX_RETRIES}
 * transient retries. Retry is strictly opt-in so side-effectful scripts (deploys,
 * `gh` mutations, external CLIs) are never silently re-run on a transient-looking
 * failure. Delegates so the two configs can't derive the retry block differently.
 */
function getExplicitNodeRetryConfig(
  node: DagNode
): ReturnType<typeof getEffectiveNodeRetryConfig> | undefined {
  return 'retry' in node && node.retry ? getEffectiveNodeRetryConfig(node) : undefined;
}

/**
 * Decide whether a failed node output warrants another retry attempt.
 *
 * Shared by {@link runNodeRetryLoop} for every node type so the retry decision
 * cannot drift. Decisive FATAL errors (credentials, authorization, quota/limit
 * windows) are never retried, even when `on_error: all`; generic "auth error"
 * text is fatal only when no transient signal matches. Also returns `isTransient`
 * so callers can label the notification.
 */
function shouldRetryNodeFailure(
  output: NodeOutput,
  onError: 'transient' | 'all'
): { shouldRetry: boolean; isTransient: boolean } {
  // Only failed outputs carry `error` (discriminated union); a non-failed output
  // is never retried. Callers already guard on `state === 'failed'`, but narrow
  // here too so `output.error` type-checks and the helper is safe standalone.
  if (output.state !== 'failed') {
    return { shouldRetry: false, isTransient: false };
  }
  const errorType = output.error ? classifyError(new Error(output.error)) : undefined;
  const isFatal = errorType === 'FATAL';
  const isTransient = errorType === 'TRANSIENT';
  const shouldRetry = !isFatal && (onError === 'all' || (onError === 'transient' && isTransient));
  return { shouldRetry, isTransient };
}

function nodeTotalTimeoutMs(node: DagNode): number | undefined {
  const timeout = (node as { timeout?: unknown }).timeout;
  return typeof timeout === 'number' && Number.isFinite(timeout) && timeout > 0
    ? timeout
    : undefined;
}

function remainingDeadlineMs(deadlineAt: number | undefined): number | undefined {
  if (deadlineAt === undefined) return undefined;
  return Math.max(0, deadlineAt - Date.now());
}

function earlierDeadline(a: number | undefined, b: number | undefined): number | undefined {
  if (a === undefined) return b;
  if (b === undefined) return a;
  return Math.min(a, b);
}

const workflowBudgetPersistQueues = new Map<string, Promise<void>>();

async function persistWorkflowBudgetState(
  deps: WorkflowDeps,
  workflowRunId: string,
  budget: ActiveWorkflowBudget | undefined,
  consumed: { input: number; output: number },
  options: { unknownConsumption?: boolean } = {}
): Promise<void> {
  if (!budget) return;
  const prior = workflowBudgetPersistQueues.get(workflowRunId) ?? Promise.resolve();
  const write = prior
    .catch(() => undefined)
    .then(async () => {
      const monotonicConsumed = {
        input: Math.max(budget.state.consumed.input, consumed.input),
        output: Math.max(budget.state.consumed.output, consumed.output),
      };
      const state =
        options.unknownConsumption === true
          ? unknownWorkflowBudgetState(budget, monotonicConsumed)
          : nextWorkflowBudgetState(budget, monotonicConsumed);
      await deps.store.updateWorkflowRun(workflowRunId, {
        metadata: { [WORKFLOW_BUDGET_METADATA_KEY]: state },
      });
      budget.state = state;
    });
  workflowBudgetPersistQueues.set(workflowRunId, write);
  try {
    await write;
  } finally {
    if (workflowBudgetPersistQueues.get(workflowRunId) === write) {
      workflowBudgetPersistQueues.delete(workflowRunId);
    }
  }
}

async function checkpointHardenedAiAttempt(
  deps: WorkflowDeps,
  workflowRunId: string,
  budget: ActiveWorkflowBudget | undefined,
  containerCtx: ContainerRunContext | undefined,
  label: string,
  _baseUsage: { input: number; output: number },
  _completedUsage: { input: number; output: number },
  _accumulatedAttempt: { tokens?: TokenUsage },
  _rawAttempt: { tokens?: TokenUsage }
): Promise<void> {
  if (!budget) return;
  const ledgerStatus = await readRequiredVerifiedHardenedBudgetStatus(containerCtx);
  assertLedgerStatusMatchesBudget(ledgerStatus, budget);
  await persistWorkflowBudgetState(deps, workflowRunId, budget, ledgerStatus.consumed);
  assertWorkflowBudgetCanContinue(budget, label, ledgerStatus.consumed);
}

function hasUnknownPersistedBudgetState(workflowRun: WorkflowRun): boolean {
  const value = workflowRun.metadata?.[WORKFLOW_BUDGET_METADATA_KEY];
  return (
    !!value &&
    typeof value === 'object' &&
    (value as WorkflowBudgetState).unknownConsumption === true
  );
}

async function readVerifiedHardenedBudgetStatus(
  containerCtx: ContainerRunContext | undefined
): Promise<VerifiedProxyBudgetStatus | undefined> {
  if (!containerCtx?.proxyBudgetSeedDigest) return undefined;
  return readRequiredVerifiedHardenedBudgetStatus(containerCtx);
}

async function readRequiredVerifiedHardenedBudgetStatus(
  containerCtx: ContainerRunContext | undefined
): Promise<VerifiedProxyBudgetStatus> {
  if (!containerCtx?.proxyBudgetSeedDigest) {
    throw new Error('Hardened workflow budget proxy ledger binding is incomplete.');
  }
  const binding = readProxyBudgetBinding(containerCtx);
  if (!containerCtx.backend.readProxyBudgetStatus) {
    throw new Error('Hardened workflow budget requires controller proxy ledger status.');
  }
  const status = await containerCtx.backend.readProxyBudgetStatus(containerCtx.envId, binding);
  assertVerifiedProxyBudgetStatus(status, containerCtx.envId, binding.proxyBudgetSeedDigest);
  return status;
}

function readProxyBudgetBinding(containerCtx: ContainerRunContext): {
  egressPolicyB64: string;
  image: string;
  ownerRunId: string;
  proxyBudgetSeedDigest: string;
} {
  if (
    !containerCtx.egressPolicyB64 ||
    !containerCtx.image ||
    !containerCtx.ownerRunId ||
    !containerCtx.proxyBudgetSeedDigest
  ) {
    throw new Error('Hardened workflow budget proxy ledger binding is incomplete.');
  }
  return {
    egressPolicyB64: containerCtx.egressPolicyB64,
    image: containerCtx.image,
    ownerRunId: containerCtx.ownerRunId,
    proxyBudgetSeedDigest: containerCtx.proxyBudgetSeedDigest,
  };
}

function assertVerifiedProxyBudgetStatus(
  status: VerifiedProxyBudgetStatus,
  envId: string,
  proxyBudgetSeedDigest: string
): void {
  if (status.source !== 'controller-proxy-ledger' || status.envId !== envId) {
    throw new Error('Hardened workflow budget ledger status binding is malformed.');
  }
  if (status.pendingReservations > 0 || status.unknownReservations > 0) {
    throw new Error('Hardened workflow budget ledger has pending or unknown reservations.');
  }
  if (!status.acceptingReservations) {
    throw new Error('Hardened workflow budget ledger is not accepting exact reservations.');
  }
  if (status.grant.runId.length === 0 || proxyBudgetSeedDigest.length !== 64) {
    throw new Error('Hardened workflow budget ledger grant binding is malformed.');
  }
  if (!isNonnegativeSafeTokenUsage(status.consumed)) {
    throw new Error('Hardened workflow budget ledger consumption is malformed.');
  }
}

function assertLedgerStatusMatchesBudget(
  status: VerifiedProxyBudgetStatus,
  budget: ActiveWorkflowBudget
): void {
  if (
    status.grant.workflowDigest !== budget.state.workflowDigest ||
    new Date(status.grant.deadlineEpochMs).toISOString() !== budget.state.deadlineAt ||
    status.grant.totalTokenLimit !== budget.state.tokens.total ||
    status.grant.inputTokenLimit !== (budget.state.tokens.input ?? budget.state.tokens.total) ||
    status.grant.outputTokenLimit !== (budget.state.tokens.output ?? budget.state.tokens.total)
  ) {
    throw new Error('Hardened workflow budget ledger grant does not match controller grant.');
  }
}

function isHardenedBudgetError(error: Error): boolean {
  return /hardened .*budget/i.test(error.message);
}

function isNonnegativeSafeTokenUsage(tokens: TokenUsage): boolean {
  return (
    Number.isSafeInteger(tokens.input) &&
    tokens.input >= 0 &&
    Number.isSafeInteger(tokens.output) &&
    tokens.output >= 0
  );
}

function mergeTokenUsage(left: TokenUsage | undefined, right: TokenUsage): TokenUsage | undefined {
  const input = (left?.input ?? 0) + right.input;
  const output = (left?.output ?? 0) + right.output;
  if (!Number.isSafeInteger(input) || !Number.isSafeInteger(output)) return undefined;
  return { input, output };
}

function resolveFailedNodeTokens(
  accumulated: TokenUsage | undefined,
  current: TokenUsage | undefined,
  currentIsAccumulated: boolean,
  usageUnknown: boolean
): TokenUsage | undefined {
  if (usageUnknown) return undefined;
  if (current === undefined) return undefined;
  if (currentIsAccumulated) return current;
  return mergeTokenUsage(accumulated, current);
}

function withPriorAttemptUsage<T extends NodeExecutionResult>(
  prior: PriorAttemptUsage | undefined,
  output: T
): T {
  if (!prior) return output;
  const rest = { ...output };
  delete rest.tokens;
  delete rest.costUsd;
  const mergedTokens =
    prior.tokensUnknown || output.tokens === undefined
      ? undefined
      : mergeTokenUsage(prior.tokens, output.tokens);
  return {
    ...rest,
    ...(mergedTokens !== undefined ? { tokens: mergedTokens } : {}),
    ...(prior.costUsd !== undefined || output.costUsd !== undefined
      ? { costUsd: (prior.costUsd ?? 0) + (output.costUsd ?? 0) }
      : {}),
  } as T;
}

/**
 * Run a node executor with the shared retry loop: exponential backoff, FATAL
 * never retried, and a platform notification before each retry. Used by both the
 * AI-node path in {@link runLayers} and {@link runDeterministicNodeWithRetry} so
 * the backoff math and user-facing wording are defined once and can't drift.
 * `initialOutput` seeds `output` for the (unreachable) zero-iteration case and is
 * generic in `T` so callers keep their richer result type (e.g. NodeExecutionResult).
 */
async function runNodeRetryLoop<T extends NodeOutput>(
  node: DagNode,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRun: WorkflowRun,
  retryConfig: { maxRetries: number; delayMs: number; onError: 'transient' | 'all' },
  run: (deadlineAt?: number, priorAttemptUsage?: PriorAttemptUsage) => Promise<T>,
  initialOutput: T,
  totalTimeoutMs?: number,
  afterAttempt?: (output: T, attempt: number, rawOutput: T) => Promise<void>
): Promise<T> {
  let output = initialOutput;
  let accumulatedTokens: TokenUsage | undefined;
  let accumulatedTokensUnknown = false;
  let accumulatedCostUsd: number | undefined;
  const addAttemptUsage = (attemptOutput: T): void => {
    const usage = attemptOutput as T & { tokens?: TokenUsage; costUsd?: number };
    if (usage.tokens === undefined) {
      accumulatedTokensUnknown = true;
    } else if (!accumulatedTokensUnknown) {
      const mergedTokens = mergeTokenUsage(accumulatedTokens, usage.tokens);
      if (mergedTokens === undefined) {
        accumulatedTokensUnknown = true;
        accumulatedTokens = undefined;
      } else {
        accumulatedTokens = mergedTokens;
      }
    }
    if (usage.costUsd !== undefined) {
      accumulatedCostUsd = (accumulatedCostUsd ?? 0) + usage.costUsd;
    }
  };
  const withAccumulatedUsage = (attemptOutput: T): T => ({
    ...attemptOutput,
    ...(!accumulatedTokensUnknown && accumulatedTokens !== undefined
      ? { tokens: accumulatedTokens }
      : {}),
    ...(accumulatedCostUsd !== undefined ? { costUsd: accumulatedCostUsd } : {}),
  });
  const priorAttemptUsage = (): PriorAttemptUsage => ({
    tokensUnknown: accumulatedTokensUnknown,
    ...(accumulatedTokens !== undefined ? { tokens: accumulatedTokens } : {}),
    ...(accumulatedCostUsd !== undefined ? { costUsd: accumulatedCostUsd } : {}),
  });
  const deadlineAt = totalTimeoutMs !== undefined ? Date.now() + totalTimeoutMs : undefined;
  for (let attempt = 0; attempt <= retryConfig.maxRetries; attempt++) {
    if (remainingDeadlineMs(deadlineAt) === 0) {
      const timeoutOutput = {
        ...initialOutput,
        state: 'failed',
        output: '',
        error: `Node '${node.id}' exceeded total timeout of ${String(totalTimeoutMs)}ms`,
      };
      await afterAttempt?.(withAccumulatedUsage(timeoutOutput), attempt, timeoutOutput);
      return withAccumulatedUsage(timeoutOutput);
    }
    output = await run(deadlineAt, priorAttemptUsage());
    addAttemptUsage(output);
    const accumulatedOutput = withAccumulatedUsage(output);
    await afterAttempt?.(accumulatedOutput, attempt, output);
    if (output.state !== 'failed') break;

    const { shouldRetry, isTransient } = shouldRetryNodeFailure(output, retryConfig.onError);
    if (!shouldRetry || attempt >= retryConfig.maxRetries) break;

    const delayMs = retryConfig.delayMs * Math.pow(2, attempt);
    getLog().warn(
      {
        nodeId: node.id,
        attempt: attempt + 1,
        maxRetries: retryConfig.maxRetries,
        delayMs,
        error: output.error,
      },
      'dag_node_transient_retry'
    );

    const errorKind = isTransient ? 'transient error' : 'error';
    await safeSendMessage(
      platform,
      conversationId,
      `⚠️ Node \`${node.id}\` failed with ${errorKind} (attempt ${String(attempt + 1)}/${String(retryConfig.maxRetries + 1)}). Retrying in ${String(Math.round(delayMs / 1000))}s...`,
      { workflowId: workflowRun.id, nodeName: node.id }
    );

    const remaining = remainingDeadlineMs(deadlineAt);
    if (remaining === 0) break;
    await new Promise(resolve =>
      setTimeout(resolve, remaining === undefined ? delayMs : Math.min(delayMs, remaining))
    );
  }
  return withAccumulatedUsage(output);
}

/**
 * Run a deterministic (bash/script) node with opt-in retry.
 *
 * Deterministic nodes get exactly one attempt unless they declare an explicit
 * `retry:` block. When they do, transient/all failures are retried via the shared
 * {@link runNodeRetryLoop} (same exponential-backoff + FATAL-never-retried
 * semantics as AI nodes). The single-attempt default is preserved so scripts with
 * side effects aren't silently re-executed (#2088).
 */
async function runDeterministicNodeWithRetry(
  node: DagNode,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRun: WorkflowRun,
  run: () => Promise<NodeOutput>
): Promise<NodeOutput> {
  const retryConfig = getExplicitNodeRetryConfig(node);
  // No explicit retry: preserve the single-attempt deterministic-node default.
  if (!retryConfig) {
    return run();
  }
  return runNodeRetryLoop(node, platform, conversationId, workflowRun, retryConfig, run, {
    state: 'failed',
    output: '',
    error: 'Node did not execute',
  });
}

/**
 * Single-quote a string for safe inline shell use.
 * Replaces each ' with '\'' (end quote, literal single-quote, re-open quote).
 */
function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

/**
 * Shell-quote a value for bash, or write it to a file and return a $(cat ...) reference
 * when the value exceeds the inline size threshold.
 */
function shellQuoteOrFile(
  value: string,
  nodeId: string,
  field: string | undefined,
  outputFileDir: string | undefined
): string {
  if (outputFileDir && value.length > NODE_OUTPUT_FILE_THRESHOLD) {
    const filename = field ? `${nodeId}.${field}.nodeoutput` : `${nodeId}.nodeoutput`;
    const filePath = joinPath(outputFileDir, filename);
    try {
      writeFileSync(filePath, value);
      return `$(cat ${shellQuote(filePath)})`;
    } catch (fileErr) {
      const err = fileErr as Error;
      getLog().error(
        { err, nodeId, field, valueSize: value.length, filePath },
        'dag.large_output_file_write_failed'
      );
      return shellQuote(value); // fallback: inline (pre-file-spill behavior)
    }
  }
  return shellQuote(value);
}

/**
 * Substitute $node_id.output and $node_id.output.field references in a prompt.
 * Called AFTER the standard substituteWorkflowVariables pass.
 *
 * KEEP IN SYNC (three ref-surface enumerations must agree): the fields this is called on
 * (search call sites below), the loader's validateDagStructure scan (which validates the
 * same refs), and rewriteNodeOutputRefs in include-expander.ts (which renames them on
 * inline). Adding a substituted field to one means updating all three.
 *
 * @param escapedForBash - When true, wraps substituted values in single quotes so
 *   they are safe to embed in bash scripts passed to `bash -c`. Set true only for
 *   bash node script substitution; AI/command prompt substitution should use false.
 */
export function substituteNodeOutputRefs(
  prompt: string,
  nodeOutputs: Map<string, NodeOutput>,
  escapedForBash = false,
  outputFileDir?: string
): string {
  return prompt.replace(
    /\$([a-zA-Z_][a-zA-Z0-9_-]*)\.output(?:\.([a-zA-Z_][a-zA-Z0-9_]*))?/g,
    (match, nodeId: string, field: string | undefined) => {
      const nodeOutput = nodeOutputs.get(nodeId);
      if (!nodeOutput) {
        // A `.field` ref that resolves to no output (a typo the load-time validator
        // can't always see — refs in bash/script/approval/cancel fields and inside
        // command-file content aren't scanned — or a real node that hasn't run before
        // this reference) fails the consuming node loudly, matching the strict
        // no-silent-drop posture for known-producer field access below. The whole-text
        // `$id.output` form stays lenient ('') as a long-documented surface (changing
        // it is a bigger compatibility break).
        if (field) {
          throw new OutputRefError(
            nodeId,
            field,
            'unknown-node',
            similarNodeIds(nodeId, nodeOutputs.keys())
          );
        }
        getLog().warn({ nodeId, match }, 'dag_node_output_ref_unknown_node');
        return escapedForBash ? "''" : '';
      }
      if (!field) {
        return escapedForBash
          ? shellQuoteOrFile(nodeOutput.output, nodeId, undefined, outputFileDir)
          : nodeOutput.output;
      }
      // No-silent-drop field access (resolveNodeOutputField): prefers the parsed
      // structuredOutput payload, falls back to parsing `output`, and THROWS an
      // OutputRefError for an unresolvable reference (field not in the producer's
      // declared schema, or a schemaless node whose output isn't JSON / lacks the
      // key). The throw propagates to the dag-executor's per-node catch → the
      // consuming node fails visibly instead of receiving a poisoned ''. The only
      // value that resolves to empty is an author-declared-optional field.
      const resolution = resolveNodeOutputField(nodeOutput, nodeId, field);
      if (resolution.kind === 'empty') return escapedForBash ? "''" : '';
      const value = resolution.value;
      if (typeof value === 'string')
        return escapedForBash ? shellQuoteOrFile(value, nodeId, field, outputFileDir) : value;
      // numbers and booleans are shell-safe without quoting: JSON disallows
      // NaN/Infinity so String(number) is digits/sign/'.', and String(boolean) is
      // 'true'/'false' — no shell metacharacters.
      if (typeof value === 'number' || typeof value === 'boolean') return String(value);
      // arrays and objects: JSON-stringify so downstream tools (jq, etc.) get a
      // single JSON literal argument.
      const json = JSON.stringify(value);
      return escapedForBash ? shellQuoteOrFile(json, nodeId, field, outputFileDir) : json;
    }
  );
}

/**
 * Collect the static ids of every node in a loop_group body, recursing into nested
 * loop_group bodies. This is the *typo detector* for `$LOOP_PREV.<id>.output.<field>`
 * refs: an id that matches no node anywhere in the (possibly nested) body is a genuine
 * typo, distinct from a real body id that merely has no prior-iteration output yet.
 *
 * The transitive set (not just the outer group's direct ids) is deliberate: nested
 * loop_group bodies reuse the OUTER loop's prior-iteration snapshot, so a ref inside a
 * nested body may legitimately name an inner-group node id (which resolves to '' at the
 * outer granularity — see {@link applyLoopPrevToBodyNode}). Including descendants keeps
 * such real-but-empty refs lenient while still catching ids that exist nowhere.
 */
function collectLoopBodyNodeIds(
  nodes: readonly DagNode[],
  into: Set<string> = new Set<string>()
): Set<string> {
  for (const n of nodes) {
    into.add(n.id);
    if (isLoopGroupNode(n)) collectLoopBodyNodeIds(n.loop_group.nodes, into);
  }
  return into;
}

/**
 * Resolve `$LOOP_PREV.<nodeId>.output` and `$LOOP_PREV.<nodeId>.output.<field>` references
 * against a loop_group body's *prior-iteration* node outputs.
 *
 * Cross-iteration analog of {@link substituteNodeOutputRefs}: where `$nodeId.output` reads
 * a node's output from the *current* iteration's scope, `$LOOP_PREV.<nodeId>.output` reads
 * the same node's output from the *previous* iteration — letting a body node reference what
 * a sibling (or itself) produced one iteration ago. On iteration 1 (no prior iteration)
 * `loopPrevOutputs` is empty/undefined and every `$LOOP_PREV.*` ref resolves to '' (matching
 * the empty-on-first semantics of the single-node `$LOOP_PREV_OUTPUT`).
 *
 * Field access reuses {@link resolveNodeOutputField} for the same strict no-silent-drop
 * semantics (declared-schema typo / schemaless non-JSON / missing key → throws
 * `OutputRefError`, propagating to the consuming node's failure). The only value that
 * resolves to empty is an author-declared-optional field — or any ref on iteration 1.
 *
 * Two static id sets from the enclosing loop_group (both via {@link collectLoopBodyNodeIds}
 * / its immediate-ids counterpart) drive the absent-output branch. `knownBodyIds` is the
 * TRANSITIVE set (this group's body plus every nested descendant); `directBodyIds` is only
 * THIS group's immediate body ids. When output is absent, the id is classified:
 *   - not in `knownBodyIds` → a typo that matches no body node anywhere. A `.field` ref
 *     throws `OutputRefError('unknown-node')` (loud, with a did-you-mean) — the loop_group
 *     analog of the same fix at the `$node.output.field` seam (#2135/#2142); a whole-text
 *     `$LOOP_PREV.<id>.output` ref stays lenient ('').
 *   - in `knownBodyIds` but not in `directBodyIds` → the id belongs to a NESTED loop_group,
 *     not this group's own body. The literal token is left INTACT (`return match`) so the
 *     inner loop_group resolves it against its OWN prior-iteration snapshot when it runs
 *     (nested body nodes get a second substituteLoopPrevRefs pass — the outer pass must not
 *     consume their tokens, or the inner loop could never see its own prior iteration).
 *   - in `directBodyIds` with no prior output → legitimate iteration-1 / skipped absence → ''.
 *
 * When `knownBodyIds` is undefined (raw callers with no static set) the seam stays fully
 * lenient — every absent ref resolves to '', preserving the pre-#2142 behavior.
 */
export function substituteLoopPrevRefs(
  prompt: string,
  loopPrevOutputs: Map<string, NodeOutput> | undefined,
  escapedForBash = false,
  outputFileDir?: string,
  knownBodyIds?: ReadonlySet<string>,
  directBodyIds?: ReadonlySet<string>
): string {
  // Fast path: no refs to resolve. When refs ARE present but the map is empty/undefined
  // (iteration 1 — no prior iteration), we still run the replace so each ref resolves to
  // '' via the `!nodeOutput` branch below, rather than leaving a literal `$LOOP_PREV.…`.
  if (!prompt.includes('$LOOP_PREV.')) {
    return prompt;
  }
  return prompt.replace(
    /\$LOOP_PREV\.([a-zA-Z_][a-zA-Z0-9_-]*)\.output(?:\.([a-zA-Z_][a-zA-Z0-9_]*))?/g,
    (match, nodeId: string, field: string | undefined) => {
      const nodeOutput = loopPrevOutputs?.get(nodeId);
      if (!nodeOutput || nodeOutput.state === 'skipped' || nodeOutput.state === 'pending') {
        if (knownBodyIds) {
          if (!knownBodyIds.has(nodeId)) {
            // Typo: id matches NO body node anywhere in the enclosing loop_group (a typo
            // the loader can't see — it never scans `$LOOP_PREV.*` refs). A `.field` ref
            // fails the consuming node loudly, mirroring substituteNodeOutputRefs /
            // resolveOutputRef; a whole-text ref stays lenient ('' below). The static set
            // is required: the runtime `loopPrevOutputs` map is empty on iteration 1, so it
            // alone cannot tell a typo from a legitimate first-pass absence.
            if (field) {
              throw new OutputRefError(
                nodeId,
                field,
                'unknown-node',
                similarNodeIds(nodeId, knownBodyIds)
              );
            }
          } else if (directBodyIds && !directBodyIds.has(nodeId)) {
            // Known id owned by a NESTED loop_group, not this group's own body. Leave the
            // literal token intact so the inner loop_group resolves it against its OWN
            // prior-iteration snapshot when it executes — the outer pass must not consume
            // it, or the inner loop could never reference its own previous iteration.
            return match;
          }
          // else: known + direct id with no prior output → legitimate iteration-1 / skipped
          // absence → lenient '' below.
        }
        // No prior-iteration output for this body node (iteration 1, or the node was
        // skipped / hasn't settled last iteration). Resolve to empty rather than
        // throwing — the author opted into a cross-iteration ref, and absence on the
        // first pass (or after a skipped node) is expected.
        getLog().debug({ nodeId, match }, 'loop_group_prev_ref_no_prior_output');
        return escapedForBash ? "''" : '';
      }
      if (!field) {
        return escapedForBash
          ? shellQuoteOrFile(nodeOutput.output, nodeId, undefined, outputFileDir)
          : nodeOutput.output;
      }
      const resolution = resolveNodeOutputField(nodeOutput, nodeId, field);
      if (resolution.kind === 'empty') return escapedForBash ? "''" : '';
      const value = resolution.value;
      if (typeof value === 'string')
        return escapedForBash ? shellQuoteOrFile(value, nodeId, field, outputFileDir) : value;
      if (typeof value === 'number' || typeof value === 'boolean') return String(value);
      const json = JSON.stringify(value);
      return escapedForBash ? shellQuoteOrFile(json, nodeId, field, outputFileDir) : json;
    }
  );
}

// buildSDKHooksFromYAML moved to @archon/providers/src/claude/provider.ts
// loadMcpConfig moved to @archon/providers/src/mcp/config.ts

/**
 * Resolve per-node provider and model.
 * Node-level overrides take precedence over workflow defaults.
 *
 * Provider-agnostic: builds universal base options + raw nodeConfig.
 * The provider internally translates nodeConfig to SDK-specific options.
 * Capability warnings inform users when features are unsupported.
 */
interface ResolvedNodeModelSelection {
  provider: string;
  model: string | undefined;
  preset: ModelAliasPreset | undefined;
}

async function warnModelProviderConflict(
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRunId: string,
  node: DagNode,
  provider: string
): Promise<void> {
  if (!node.provider || node.provider === provider) return;
  getLog().warn(
    {
      nodeId: node.id,
      configuredProvider: node.provider,
      resolvedProvider: provider,
      modelRef: node.model,
    },
    'dag.model_provider_conflict'
  );
  const delivered = await safeSendMessage(
    platform,
    conversationId,
    `Warning: Node '${node.id}' sets provider '${node.provider}' but model '${node.model}' resolves to provider '${provider}' — using '${provider}'.`,
    { workflowId: workflowRunId, nodeName: node.id }
  );
  if (!delivered) {
    getLog().error(
      { nodeId: node.id, workflowRunId },
      'dag.model_provider_conflict_warning_delivery_failed'
    );
  }
}

async function resolveNodeModelSelection(
  node: DagNode,
  workflowProvider: string,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRunId: string,
  aiProfile?: ResolvedAiProfile
): Promise<ResolvedNodeModelSelection> {
  const provider = node.provider ?? workflowProvider;
  if (!node.model) return { provider, model: undefined, preset: undefined };
  if (!aiProfile) return { provider, model: node.model, preset: undefined };
  const modelSpec = resolveModelSpec(aiProfile, node.model);
  if (isLiteralSpec(modelSpec)) return { provider, model: modelSpec.literal, preset: undefined };
  const preset = modelSpec;
  const model = modelSpec.model;
  await warnModelProviderConflict(
    platform,
    conversationId,
    workflowRunId,
    node,
    modelSpec.provider
  );
  return { provider: modelSpec.provider, model, preset };
}

function isUnknownArray(value: unknown): value is unknown[] {
  return Array.isArray(value);
}

function assertRegisteredNodeProvider(node: DagNode, provider: string): void {
  if (isRegisteredProvider(provider)) return;
  throw new Error(
    `Node '${node.id}': unknown provider '${provider}'. ` +
      `Registered: ${getRegisteredProviders()
        .map(p => p.id)
        .join(', ')}`
  );
}

function assertContainerProvider(
  provider: string,
  caps: ProviderCapabilities,
  execContext: ExecutionContext
): void {
  if (execContext.kind !== 'container' || caps.containerExec) return;
  throw new Error(
    `Provider '${provider}' cannot run inside a container yet (containerExec ` +
      'capability). Use provider claude, or run without --container.'
  );
}

function getNodeCapabilityChecks(
  node: DagNode,
  workflowLevelOptions: WorkflowLevelOptions,
  config: WorkflowConfig
): [string, keyof ProviderCapabilities, boolean][] {
  return [
    [
      'allowed_tools/denied_tools',
      'toolRestrictions',
      node.allowed_tools !== undefined || node.denied_tools !== undefined,
    ],
    ['hooks', 'hooks', node.hooks !== undefined],
    ['mcp', 'mcp', node.mcp !== undefined],
    ['skills', 'skills', node.skills !== undefined && node.skills.length > 0],
    ['agents', 'agents', node.agents !== undefined],
    ['effort', 'effortControl', (node.effort ?? workflowLevelOptions.effort) !== undefined],
    ['thinking', 'thinkingControl', (node.thinking ?? workflowLevelOptions.thinking) !== undefined],
    ['maxBudgetUsd', 'costControl', node.maxBudgetUsd !== undefined],
    [
      'fallbackModel',
      'fallbackModel',
      (node.fallbackModel ?? workflowLevelOptions.fallbackModel) !== undefined,
    ],
    ['sandbox', 'sandbox', (node.sandbox ?? workflowLevelOptions.sandbox) !== undefined],
    ['settingSources', 'settingSources', node.settingSources !== undefined],
    ['env', 'envInjection', (config.envVars && Object.keys(config.envVars).length > 0) === true],
  ];
}

async function warnUnsupportedNodeCapabilities(
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRunId: string,
  node: DagNode,
  provider: string,
  caps: ProviderCapabilities,
  workflowLevelOptions: WorkflowLevelOptions,
  config: WorkflowConfig
): Promise<void> {
  const unsupported = getNodeCapabilityChecks(node, workflowLevelOptions, config)
    .filter(([, cap, isSet]) => isSet && !caps[cap])
    .map(([field]) => field);
  if (unsupported.length === 0) return;
  getLog().warn({ nodeId: node.id, provider, unsupported }, 'dag.unsupported_capabilities');
  const delivered = await safeSendMessage(
    platform,
    conversationId,
    `Warning: Node '${node.id}' uses ${unsupported.join(', ')} but ${provider} doesn't support ${unsupported.length === 1 ? 'it' : 'them'} — ${unsupported.length === 1 ? 'this will be' : 'these will be'} ignored.`,
    { workflowId: workflowRunId, nodeName: node.id }
  );
  if (!delivered)
    getLog().error({ nodeId: node.id, workflowRunId }, 'dag.capability_warning_delivery_failed');
}

async function warnAgentsSkillsCollision(
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRunId: string,
  node: DagNode
): Promise<void> {
  if (
    node.agents?.['dag-node-skills'] === undefined ||
    node.skills === undefined ||
    node.skills.length === 0
  )
    return;
  getLog().warn({ nodeId: node.id }, 'dag.agents_skills_id_collision');
  await safeSendMessage(
    platform,
    conversationId,
    `Warning: Node '${node.id}' defines an agent with reserved ID 'dag-node-skills' AND uses 'skills:'. Your inline agent overrides Archon's automatic skills wrapper — the 'skills:' field will NOT take effect. Rename the agent or remove 'skills:' to fix.`,
    { workflowId: workflowRunId, nodeName: node.id }
  );
}

function buildNodeBaseOptions(
  node: DagNode,
  model: string | undefined,
  config: WorkflowConfig,
  workflowLevelOptions: WorkflowLevelOptions,
  execContext: ExecutionContext
): { baseOptions: SendQueryOptions; fallbackModel: string | undefined } {
  const baseOptions: SendQueryOptions = {};
  if (model) baseOptions.model = model;
  if (execContext.kind === 'container') baseOptions.execContext = execContext;
  if (config.envVars && Object.keys(config.envVars).length > 0) baseOptions.env = config.envVars;
  if (node.systemPrompt !== undefined) baseOptions.systemPrompt = node.systemPrompt;
  if (node.maxBudgetUsd !== undefined) baseOptions.maxBudgetUsd = node.maxBudgetUsd;
  const fallbackModel = node.fallbackModel ?? workflowLevelOptions.fallbackModel;
  if (fallbackModel) baseOptions.fallbackModel = fallbackModel;
  if (node.output_format)
    baseOptions.outputFormat = { type: 'json_schema', schema: node.output_format };
  return { baseOptions, fallbackModel };
}

function buildNodeConfig(
  node: DagNode,
  workflowLevelOptions: WorkflowLevelOptions,
  fallbackModel: string | undefined
): NodeConfig {
  return {
    nodeId: node.id,
    mcp: node.mcp,
    hooks: node.hooks,
    skills: node.skills,
    agents: node.agents,
    pi: node.pi,
    allowed_tools: node.allowed_tools,
    denied_tools: node.denied_tools,
    effort: node.effort ?? workflowLevelOptions.effort,
    thinking: node.thinking ?? workflowLevelOptions.thinking,
    sandbox: node.sandbox ?? workflowLevelOptions.sandbox,
    betas: node.betas ?? workflowLevelOptions.betas,
    output_format: node.output_format,
    maxBudgetUsd: node.maxBudgetUsd,
    systemPrompt: node.systemPrompt,
    fallbackModel,
    settingSources: node.settingSources,
  };
}

function resolveNodeTier(
  node: DagNode,
  provider: string,
  workflowProvider: string,
  workflowLevelOptions: WorkflowLevelOptions
): TierName | undefined {
  if (node.model && isTierName(node.model)) return node.model;
  if (!node.model && provider === workflowProvider) return workflowLevelOptions.workflowTier;
  return undefined;
}

/**
 * Resolve per-node provider and model.
 * Node-level overrides take precedence over workflow defaults.
 *
 * Provider-agnostic: builds universal base options + raw nodeConfig.
 * The provider internally translates nodeConfig to SDK-specific options.
 * Capability warnings inform users when features are unsupported.
 */
async function resolveNodeProviderAndModel(
  node: DagNode,
  workflowProvider: string,
  workflowModel: string | undefined,
  config: WorkflowConfig,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRunId: string,
  _cwd: string,
  workflowLevelOptions: WorkflowLevelOptions,
  aiProfile?: ResolvedAiProfile,
  workflowPreset?: ModelAliasPreset,
  execContext: ExecutionContext = { kind: 'host' }
): Promise<{
  provider: string;
  model: string | undefined;
  options: SendQueryOptions | undefined;
  tier?: TierName;
  effort?: string;
}> {
  const selection = await resolveNodeModelSelection(
    node,
    workflowProvider,
    platform,
    conversationId,
    workflowRunId,
    aiProfile
  );
  const provider = selection.provider;
  let model = selection.model;
  assertRegisteredNodeProvider(node, provider);
  const providerAssistantConfig = config.assistants[provider];
  model ??=
    provider === workflowProvider
      ? workflowModel
      : (providerAssistantConfig?.model as string | undefined);
  const effectivePreset =
    selection.preset ?? (!node.model && provider === workflowProvider ? workflowPreset : undefined);
  const caps = getProviderCapabilities(provider);
  assertContainerProvider(provider, caps, execContext);
  await warnUnsupportedNodeCapabilities(
    platform,
    conversationId,
    workflowRunId,
    node,
    provider,
    caps,
    workflowLevelOptions,
    config
  );
  await warnAgentsSkillsCollision(platform, conversationId, workflowRunId, node);

  const { baseOptions, fallbackModel } = buildNodeBaseOptions(
    node,
    model,
    config,
    workflowLevelOptions,
    execContext
  );
  const nodeConfig = buildNodeConfig(node, workflowLevelOptions, fallbackModel);
  const assistantConfig: Record<string, unknown> = { ...(config.assistants[provider] ?? {}) };
  applyPresetOptions(
    provider,
    effectivePreset,
    node,
    workflowLevelOptions,
    nodeConfig,
    assistantConfig
  );
  const assistantEffort = assistantConfig.modelReasoningEffort;
  const resolvedEffort =
    nodeConfig.effort ?? (typeof assistantEffort === 'string' ? assistantEffort : undefined);
  const options: SendQueryOptions = { ...baseOptions, nodeConfig, assistantConfig };
  return {
    provider,
    model,
    options,
    tier: resolveNodeTier(node, provider, workflowProvider, workflowLevelOptions),
    effort: resolvedEffort,
  };
}

/** Evaluate trigger rule for a node given its upstream states */
export function checkTriggerRule(
  node: DagNode,
  nodeOutputs: Map<string, NodeOutput>
): 'run' | 'skip' {
  const nodeDeps = node.depends_on ?? [];
  if (nodeDeps.length === 0) return 'run';

  const upstreams = nodeDeps.map(
    id =>
      nodeOutputs.get(id) ??
      ({
        state: 'failed',
        output: '',
        error: `upstream '${id}' missing from outputs`,
      } as NodeOutput)
  );
  const rule: TriggerRule = node.trigger_rule ?? 'all_success';

  switch (rule) {
    case 'all_success':
      return upstreams.every(u => u.state === 'completed') ? 'run' : 'skip';
    case 'one_success':
      return upstreams.some(u => u.state === 'completed') ? 'run' : 'skip';
    case 'none_failed_min_one_success': {
      const anyFailed = upstreams.some(u => u.state === 'failed');
      const anySucceeded = upstreams.some(u => u.state === 'completed');
      return !anyFailed && anySucceeded ? 'run' : 'skip';
    }
    case 'all_done':
      return upstreams.every(u => u.state !== 'pending' && u.state !== 'running') ? 'run' : 'skip';
  }
}

/**
 * Build topological layers from DAG nodes using Kahn's algorithm.
 * Layer 0: nodes with no dependencies.
 * Layer N: nodes whose dependencies are all in layers 0..N-1.
 *
 * Cycle detection: if the sum of all layer sizes < nodes.length, a cycle exists.
 * (Cycle detection at load time is the primary guard; this is a runtime safety check.)
 */
export function buildTopologicalLayers(nodes: readonly DagNode[]): DagNode[][] {
  const inDegree = new Map<string, number>();
  const dependents = new Map<string, string[]>();

  for (const node of nodes) {
    inDegree.set(node.id, node.depends_on?.length ?? 0);
    for (const dep of node.depends_on ?? []) {
      const existing = dependents.get(dep) ?? [];
      existing.push(node.id);
      dependents.set(dep, existing);
    }
  }

  const layers: DagNode[][] = [];
  let ready = [...nodes].filter(n => (inDegree.get(n.id) ?? 0) === 0);

  while (ready.length > 0) {
    layers.push(ready);
    const nextIds: string[] = [];
    for (const node of ready) {
      for (const depId of dependents.get(node.id) ?? []) {
        const newDegree = (inDegree.get(depId) ?? 0) - 1;
        inDegree.set(depId, newDegree);
        if (newDegree === 0) nextIds.push(depId);
      }
    }
    ready = nextIds
      .map(id => nodes.find(n => n.id === id))
      .filter((n): n is DagNode => n !== undefined);
  }

  const totalPlaced = layers.reduce((sum, l) => sum + l.length, 0);
  if (totalPlaced < nodes.length) {
    // Should never happen — cycle detection runs at load time
    throw new Error(
      '[DagExecutor] Cycle detected at runtime — was cycle detection skipped at load?'
    );
  }

  return layers;
}

/**
 * Execute a single DAG node. Returns NodeExecutionResult regardless of success/failure.
 * Always accumulates assistant text output (for $node_id.output substitution).
 * Parallel nodes and context: 'fresh' nodes always receive fresh sessions (caller ensures resumeSessionId is undefined).
 */
interface AiNodeExecutionContext {
  deps: WorkflowDeps;
  platform: IWorkflowPlatform;
  conversationId: string;
  cwd: string;
  workflowRun: WorkflowRun;
  node: CommandNode | PromptNode;
  provider: string;
  nodeOptions: SendQueryOptions | undefined;
  artifactsDir: string;
  stateDir: string;
  logDir: string;
  baseBranch: string;
  docsDir: string;
  nodeOutputs: Map<string, NodeOutput>;
  resumeSessionId: string | undefined;
  configuredCommandFolder?: string;
  issueContext?: string;
  resolvedModel?: string;
  resolvedTier?: TierName;
  resolvedEffort?: string;
  stepNamePrefix: string;
  iteration?: number;
  totalDeadlineAt?: number;
  budgetPassCheckpoint?: BudgetPassCheckpoint;
  nodeStartTime: number;
  nodeContext: SendMessageContext;
  stepName: string;
  iterationData: Record<string, number>;
  configuredMcpNames: Set<string>;
  aiClient: ReturnType<WorkflowDeps['getAgentProvider']>;
  streamingMode: ReturnType<IWorkflowPlatform['getStreamingMode']>;
  nodeOptionsWithAbort: SendQueryOptions | undefined;
}

interface AiNodeExecutionState {
  nodeOutputText: string;
  structuredOutput: unknown;
  newSessionId: string | undefined;
  nodeResumed: boolean | undefined;
  nodeTokens: TokenUsage | undefined;
  nodeCostUsd: number | undefined;
  nodeStopReason: string | undefined;
  nodeNumTurns: number | undefined;
  nodeResolvedModel: ResolvedModel | undefined;
  batchMessages: string[];
  nodeAbortController: AbortController;
  nodeIdleTimedOut: boolean;
  nodeTotalTimedOut: boolean;
  effectiveIdleTimeout: number;
  runningTools: Map<string, RunningTool>;
  anonymousToolSequence: number;
  lastAnonymousToolCallId: string | undefined;
  backgroundTasksIncomplete: string[];
  maxReasks: number;
  accumulatedCostUsd: number | undefined;
  accumulatedNodeTokens: TokenUsage | undefined;
  passTokenUsageUnknown: boolean;
  nodeTokensAreAccumulated: boolean;
}

async function emitAiNodeStarted(ctx: AiNodeExecutionContext): Promise<void> {
  getLog().info({ nodeId: ctx.node.id, provider: ctx.provider }, 'dag_node_started');
  await logNodeStart(ctx.logDir, ctx.workflowRun.id, ctx.node.id, ctx.node.command ?? '<inline>');
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_started',
      step_name: ctx.stepName,
      data: {
        command: ctx.node.command ?? null,
        provider: ctx.provider,
        model: ctx.resolvedModel,
        tier: ctx.resolvedTier,
        ...(ctx.resolvedEffort !== undefined ? { effort: ctx.resolvedEffort } : {}),
        ...ctx.iterationData,
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_started' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_started',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.command ?? ctx.node.id,
    provider: ctx.provider,
    model: ctx.resolvedModel,
    tier: ctx.resolvedTier,
    ...(ctx.resolvedEffort !== undefined ? { effort: ctx.resolvedEffort } : {}),
  });
}

async function failAiNodeBeforeStream(
  ctx: AiNodeExecutionContext,
  error: string,
  nodeName: string,
  notifyUser = false
): Promise<NodeExecutionResult> {
  await logNodeError(ctx.logDir, ctx.workflowRun.id, ctx.node.id, error);
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_failed',
      step_name: ctx.stepName,
      data: { error },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_failed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName,
    error,
  });
  if (notifyUser)
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      `Node '${ctx.node.id}' failed: ${error}`,
      ctx.nodeContext
    );
  return { state: 'failed', output: '', error };
}

async function resolveAiNodeRawPrompt(
  ctx: AiNodeExecutionContext
): Promise<string | NodeExecutionResult> {
  if (ctx.node.command === undefined) return ctx.node.prompt;
  const promptResult = await loadCommandPrompt(
    ctx.deps,
    ctx.cwd,
    ctx.node.command,
    ctx.configuredCommandFolder
  );
  if (promptResult.success) return promptResult.content;
  getLog().error(
    { nodeId: ctx.node.id, error: promptResult.message },
    'dag_node_command_load_failed'
  );
  return failAiNodeBeforeStream(ctx, promptResult.message, ctx.node.command);
}

async function buildAiNodeFinalPrompt(
  ctx: AiNodeExecutionContext,
  rawPrompt: string
): Promise<string | NodeExecutionResult> {
  try {
    const substitutedPrompt = buildPromptWithContext(
      rawPrompt,
      ctx.workflowRun.id,
      ctx.workflowRun.user_message,
      ctx.artifactsDir,
      ctx.baseBranch,
      ctx.docsDir,
      ctx.issueContext,
      `dag node '${ctx.node.id}' prompt`,
      { stateDir: ctx.stateDir, execContext: ctx.nodeOptions?.execContext }
    );
    return substituteNodeOutputRefs(substitutedPrompt, ctx.nodeOutputs);
  } catch (error) {
    const err = error as Error;
    getLog().error(
      { nodeId: ctx.node.id, error: err.message },
      'dag.node_prompt_substitution_failed'
    );
    return failAiNodeBeforeStream(ctx, err.message, ctx.node.command ?? ctx.node.id, true);
  }
}

function createAiNodeExecutionState(
  ctx: AiNodeExecutionContext,
  nodeAbortController: AbortController
): AiNodeExecutionState {
  const maxReasks =
    getProviderCapabilities(ctx.provider).structuredOutput === 'best-effort' &&
    ctx.nodeOptions?.outputFormat
      ? STRUCTURED_OUTPUT_MAX_REASKS
      : 0;
  return {
    nodeOutputText: '',
    structuredOutput: undefined,
    newSessionId: undefined,
    nodeResumed: undefined,
    nodeTokens: undefined,
    nodeCostUsd: undefined,
    nodeStopReason: undefined,
    nodeNumTurns: undefined,
    nodeResolvedModel: undefined,
    batchMessages: [],
    nodeAbortController,
    nodeIdleTimedOut: false,
    nodeTotalTimedOut: false,
    effectiveIdleTimeout: ctx.node.idle_timeout ?? STEP_IDLE_TIMEOUT_MS,
    runningTools: new Map<string, RunningTool>(),
    anonymousToolSequence: 0,
    lastAnonymousToolCallId: undefined,
    backgroundTasksIncomplete: [],
    maxReasks,
    accumulatedCostUsd: undefined,
    accumulatedNodeTokens: undefined,
    passTokenUsageUnknown: false,
    nodeTokensAreAccumulated: false,
  };
}

async function shouldContinueAiNodeStream(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  tickNow: number,
  nodeKey: string
): Promise<boolean> {
  if (tickNow - (lastNodeCancelCheck.get(nodeKey) ?? 0) <= CANCEL_CHECK_INTERVAL_MS) return true;
  lastNodeCancelCheck.set(nodeKey, tickNow);
  try {
    const streamStatus = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
    if (shouldContinueStreamingForStatus(streamStatus)) return true;
    getLog().info(
      { workflowRunId: ctx.workflowRun.id, nodeId: ctx.node.id, status: streamStatus ?? 'deleted' },
      'dag.stop_detected_during_streaming'
    );
    state.nodeAbortController.abort();
    return false;
  } catch (cancelCheckErr) {
    getLog().warn(
      { err: cancelCheckErr as Error, workflowRunId: ctx.workflowRun.id, nodeId: ctx.node.id },
      'dag.status_check_failed'
    );
    return true;
  }
}

async function updateAiNodeStreamActivity(
  ctx: AiNodeExecutionContext,
  tickNow: number,
  nodeKey: string
): Promise<void> {
  if (tickNow - (lastNodeActivityUpdate.get(nodeKey) ?? 0) <= ACTIVITY_HEARTBEAT_INTERVAL_MS)
    return;
  lastNodeActivityUpdate.set(nodeKey, tickNow);
  try {
    await ctx.deps.store.updateWorkflowActivity(ctx.workflowRun.id);
  } catch (e) {
    getLog().warn(
      { err: e as Error, workflowRunId: ctx.workflowRun.id },
      'dag.activity_update_failed'
    );
  }
}

function closeAiNodeTool(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  toolCallId: string,
  tool: RunningTool,
  outcome: 'unknown' | 'success' | 'error' | 'interrupted' = 'unknown',
  exitCode?: number
): void {
  const now = Date.now();
  getWorkflowEventEmitter().emit({
    type: 'tool_completed',
    runId: ctx.workflowRun.id,
    toolName: tool.toolName,
    stepName: ctx.node.id,
    durationMs: now - tool.startedAt,
    toolCallId,
    toolOutcome: outcome,
    ...(exitCode !== undefined ? { exitCode } : {}),
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'tool_completed',
      step_name: ctx.stepName,
      data: {
        tool_name: tool.toolName,
        duration_ms: now - tool.startedAt,
        tool_call_id: toolCallId,
        tool_outcome: outcome,
        ...(exitCode !== undefined ? { exit_code: exitCode } : {}),
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'tool_completed' },
        'workflow_event_persist_failed'
      );
    });
  state.runningTools.delete(toolCallId);
  if (toolCallId === state.lastAnonymousToolCallId) state.lastAnonymousToolCallId = undefined;
}

function closeAllAiNodeTools(ctx: AiNodeExecutionContext, state: AiNodeExecutionState): void {
  for (const [toolCallId, prevTool] of state.runningTools)
    closeAiNodeTool(ctx, state, toolCallId, prevTool);
}

async function handleAiNodeAssistantChunk(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  msg: Extract<MessageChunk, { type: 'assistant' }>
): Promise<void> {
  state.nodeOutputText += msg.content;
  if (ctx.streamingMode === 'stream' || msg.flush) {
    if (ctx.streamingMode === 'batch' && state.batchMessages.length > 0) {
      await safeSendMessage(
        ctx.platform,
        ctx.conversationId,
        state.batchMessages.join('\n\n'),
        ctx.nodeContext
      );
      state.batchMessages.length = 0;
    }
    await safeSendMessage(ctx.platform, ctx.conversationId, msg.content, ctx.nodeContext);
  } else {
    state.batchMessages.push(msg.content);
  }
  await logAssistant(ctx.logDir, ctx.workflowRun.id, msg.content);
}

async function handleAiNodeToolChunk(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  msg: Extract<MessageChunk, { type: 'tool' }>
): Promise<void> {
  const now = Date.now();
  const toolCallId = msg.toolCallId ?? `anonymous-${String(++state.anonymousToolSequence)}`;
  const previousTool = state.lastAnonymousToolCallId
    ? state.runningTools.get(state.lastAnonymousToolCallId)
    : undefined;
  if (previousTool && state.lastAnonymousToolCallId !== undefined)
    closeAiNodeTool(ctx, state, state.lastAnonymousToolCallId, previousTool);
  state.runningTools.set(toolCallId, { toolName: msg.toolName, startedAt: now });
  if (!msg.toolCallId) state.lastAnonymousToolCallId = toolCallId;
  getWorkflowEventEmitter().emit({
    type: 'tool_started',
    runId: ctx.workflowRun.id,
    toolName: msg.toolName,
    stepName: ctx.node.id,
    toolCallId,
  });
  if (ctx.streamingMode === 'stream') {
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      formatToolCall(msg.toolName, msg.toolInput),
      ctx.nodeContext,
      { category: 'tool_call_formatted' } as WorkflowMessageMetadata
    );
    if (ctx.platform.sendStructuredEvent)
      await ctx.platform.sendStructuredEvent(ctx.conversationId, msg);
  }
  await logTool(ctx.logDir, ctx.workflowRun.id, msg.toolName, msg.toolInput ?? {});
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'tool_called',
      step_name: ctx.stepName,
      data: { tool_name: msg.toolName, tool_input: msg.toolInput ?? {}, tool_call_id: toolCallId },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'tool_called' },
        'workflow_event_persist_failed'
      );
    });
}

async function handleAiNodeToolResultChunk(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  msg: Extract<MessageChunk, { type: 'tool_result' }>
): Promise<void> {
  const completedTool = findRunningTool(state.runningTools, msg.toolName, msg.toolCallId);
  if (completedTool)
    closeAiNodeTool(ctx, state, completedTool[0], completedTool[1], msg.toolOutcome, msg.exitCode);
  if (ctx.streamingMode === 'stream' && ctx.platform.sendStructuredEvent)
    await ctx.platform.sendStructuredEvent(ctx.conversationId, msg);
}

function recordAiNodeResultUsage(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  msg: Extract<MessageChunk, { type: 'result' }>
): void {
  if (msg.sessionId) state.newSessionId = msg.sessionId;
  if (msg.resumed !== undefined) state.nodeResumed = msg.resumed;
  state.nodeTokens = undefined;
  if (msg.tokens !== undefined && isNonnegativeSafeTokenUsage(msg.tokens))
    state.nodeTokens = { input: msg.tokens.input, output: msg.tokens.output };
  else if (msg.tokens !== undefined) {
    state.passTokenUsageUnknown = true;
    getLog().warn(
      { nodeId: ctx.node.id, tokens: msg.tokens },
      'dag_node.usage_tokens_non_finite_ignored'
    );
  }
  if (msg.cost !== undefined) state.nodeCostUsd = msg.cost;
  if (msg.stopReason !== undefined) state.nodeStopReason = msg.stopReason;
  if (msg.numTurns !== undefined) state.nodeNumTurns = msg.numTurns;
  state.nodeResolvedModel = msg.resolvedModel;
  if (msg.structuredOutput !== undefined) state.structuredOutput = msg.structuredOutput;
}

function throwForAiNodeResultError(
  ctx: AiNodeExecutionContext,
  msg: Extract<MessageChunk, { type: 'result' }>
): void {
  if (msg.isError && msg.errorSubtype === 'error_max_budget_usd')
    throw new Error(
      `Node '${ctx.node.id}' exceeded cost cap${ctx.nodeOptions?.maxBudgetUsd !== undefined ? ` of $${ctx.nodeOptions.maxBudgetUsd.toFixed(2)}` : ''}.`
    );
  if (!msg.isError || msg.errorSubtype === 'success') return;
  const subtype = msg.errorSubtype ?? 'unknown';
  const errorsDetail = msg.errors?.length ? ` — ${msg.errors.join('; ')}` : '';
  getLog().error(
    {
      nodeId: ctx.node.id,
      errorSubtype: subtype,
      errors: msg.errors,
      sessionId: msg.sessionId,
      stopReason: msg.stopReason,
      durationMs: Date.now() - ctx.nodeStartTime,
    },
    'dag.node_sdk_error_result'
  );
  throw new Error(`Node '${ctx.node.id}' failed: SDK returned ${subtype}${errorsDetail}`);
}

async function handleAiNodeResultChunk(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  msg: Extract<MessageChunk, { type: 'result' }>,
  backgroundTasks: ReturnType<typeof createBackgroundTaskTracker>
): Promise<boolean> {
  closeAllAiNodeTools(ctx, state);
  recordAiNodeResultUsage(ctx, state, msg);
  throwForAiNodeResultError(ctx, msg);
  if (backgroundTasks.shouldBreakOnResult()) return false;
  getLog().warn(
    { nodeId: ctx.node.id, taskCount: backgroundTasks.count(), taskIds: backgroundTasks.ids() },
    'dag.node_result_with_live_background_tasks'
  );
  if (backgroundTasks.shouldAnnounceWait())
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      `⏳ Node \`${ctx.node.id}\`: turn ended with ${String(backgroundTasks.count())} background agent task(s) still running — waiting for them to finish before completing the node.`,
      ctx.nodeContext
    );
  return true;
}

async function forwardAiNodeProviderWarning(
  ctx: AiNodeExecutionContext,
  content: string
): Promise<void> {
  getLog().warn({ nodeId: ctx.node.id, systemContent: content }, 'dag.provider_warning_forwarded');
  const delivered = await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    content,
    ctx.nodeContext
  );
  if (!delivered)
    getLog().error(
      { nodeId: ctx.node.id, workflowRunId: ctx.workflowRun.id },
      'dag.provider_warning_delivery_failed'
    );
}

async function handleAiNodeSystemChunk(
  ctx: AiNodeExecutionContext,
  msg: Extract<MessageChunk, { type: 'system' }>
): Promise<void> {
  if (msg.content.startsWith(MCP_FAILURE_PREFIX)) {
    const failedEntries = parseMcpFailureServerNames(msg.content);
    const workflowFailures = failedEntries.filter(e => ctx.configuredMcpNames.has(e.name));
    const pluginFailures = failedEntries.filter(e => !ctx.configuredMcpNames.has(e.name));
    if (workflowFailures.length > 0)
      await forwardAiNodeProviderWarning(
        ctx,
        `${MCP_FAILURE_PREFIX}${workflowFailures.map(e => e.segment).join(', ')}`
      );
    if (pluginFailures.length > 0)
      getLog().debug(
        { nodeId: ctx.node.id, pluginFailures: pluginFailures.map(e => e.name) },
        'dag.mcp_plugin_connection_suppressed'
      );
  } else if (msg.content.startsWith('⚠️')) {
    await forwardAiNodeProviderWarning(ctx, msg.content);
  } else {
    getLog().debug(
      { nodeId: ctx.node.id, systemContent: msg.content },
      'dag.system_message_unhandled'
    );
  }
}

function persistAiNodeTaskActivity(
  ctx: AiNodeExecutionContext,
  taskId: string,
  activity: string,
  data: Record<string, unknown>
): void {
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'task_activity',
      step_name: ctx.stepName,
      data: { task_id: taskId, activity, ...data },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'task_activity' },
        'workflow_event_persist_failed'
      );
    });
}

function persistAiNodeHookActivity(
  ctx: AiNodeExecutionContext,
  data: Record<string, unknown>
): void {
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'hook_activity',
      step_name: ctx.stepName,
      data,
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'hook_activity' },
        'workflow_event_persist_failed'
      );
    });
}

function handleAiNodeTaskStarted(
  ctx: AiNodeExecutionContext,
  msg: Extract<MessageChunk, { type: 'task_started' }>
): void {
  getWorkflowEventEmitter().emit({
    type: 'task_activity',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    taskId: msg.taskId,
    activity: 'started',
    ...(msg.description !== undefined ? { description: msg.description } : {}),
    ...(msg.taskType !== undefined ? { taskType: msg.taskType } : {}),
  });
  persistAiNodeTaskActivity(ctx, msg.taskId, 'started', {
    ...(msg.description !== undefined ? { description: msg.description } : {}),
    ...(msg.taskType !== undefined ? { task_type: msg.taskType } : {}),
  });
}

function handleAiNodeTaskProgress(
  ctx: AiNodeExecutionContext,
  msg: Extract<MessageChunk, { type: 'task_progress' }>
): void {
  getWorkflowEventEmitter().emit({
    type: 'task_activity',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    taskId: msg.taskId,
    activity: 'progress',
    ...(msg.description !== undefined ? { description: msg.description } : {}),
    ...(msg.summary !== undefined ? { summary: msg.summary } : {}),
    ...(msg.usage !== undefined ? { usage: msg.usage } : {}),
    ...(msg.lastToolName !== undefined ? { lastToolName: msg.lastToolName } : {}),
  });
  persistAiNodeTaskActivity(ctx, msg.taskId, 'progress', {
    ...(msg.description !== undefined ? { description: msg.description } : {}),
    ...(msg.summary !== undefined ? { summary: msg.summary } : {}),
    ...(msg.usage !== undefined ? { usage: msg.usage } : {}),
    ...(msg.lastToolName !== undefined ? { last_tool_name: msg.lastToolName } : {}),
  });
}

function handleAiNodeTaskNotification(
  ctx: AiNodeExecutionContext,
  msg: Extract<MessageChunk, { type: 'task_notification' }>
): void {
  getWorkflowEventEmitter().emit({
    type: 'task_activity',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    taskId: msg.taskId,
    activity: msg.status,
    ...(msg.summary !== undefined ? { summary: msg.summary } : {}),
    ...(msg.usage !== undefined ? { usage: msg.usage } : {}),
    ...(msg.outputFile ? { outputFile: msg.outputFile } : {}),
  });
  persistAiNodeTaskActivity(ctx, msg.taskId, msg.status, {
    ...(msg.summary !== undefined ? { summary: msg.summary } : {}),
    ...(msg.usage !== undefined ? { usage: msg.usage } : {}),
    ...(msg.outputFile ? { output_file: msg.outputFile } : {}),
  });
}

function handleAiNodeHookStarted(
  ctx: AiNodeExecutionContext,
  msg: Extract<MessageChunk, { type: 'hook_started' }>
): void {
  getWorkflowEventEmitter().emit({
    type: 'hook_activity',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    hookId: msg.hookId,
    hookName: msg.hookName,
    hookEvent: msg.hookEvent,
    activity: 'started',
  });
  persistAiNodeHookActivity(ctx, {
    hook_id: msg.hookId,
    hook_name: msg.hookName,
    hook_event: msg.hookEvent,
    activity: 'started',
  });
}

function handleAiNodeHookResponse(
  ctx: AiNodeExecutionContext,
  msg: Extract<MessageChunk, { type: 'hook_response' }>
): void {
  getWorkflowEventEmitter().emit({
    type: 'hook_activity',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    hookId: msg.hookId,
    hookName: msg.hookName,
    hookEvent: msg.hookEvent,
    activity: 'response',
    outcome: msg.outcome,
    ...(msg.exitCode !== undefined ? { exitCode: msg.exitCode } : {}),
  });
  persistAiNodeHookActivity(ctx, {
    hook_id: msg.hookId,
    hook_name: msg.hookName,
    hook_event: msg.hookEvent,
    activity: 'response',
    outcome: msg.outcome,
    ...(msg.exitCode !== undefined ? { exit_code: msg.exitCode } : {}),
  });
}

function handleAiNodeLifecycleChunk(ctx: AiNodeExecutionContext, msg: MessageChunk): void {
  switch (msg.type) {
    case 'task_started':
      handleAiNodeTaskStarted(ctx, msg);
      break;
    case 'task_progress':
      handleAiNodeTaskProgress(ctx, msg);
      break;
    case 'task_notification':
      handleAiNodeTaskNotification(ctx, msg);
      break;
    case 'hook_started':
      handleAiNodeHookStarted(ctx, msg);
      break;
    case 'hook_response':
      handleAiNodeHookResponse(ctx, msg);
      break;
  }
}

async function handleAiNodeStreamChunk(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  msg: MessageChunk,
  backgroundTasks: ReturnType<typeof createBackgroundTaskTracker>
): Promise<boolean> {
  switch (msg.type) {
    case 'assistant':
      await handleAiNodeAssistantChunk(ctx, state, msg);
      return true;
    case 'tool':
      await handleAiNodeToolChunk(ctx, state, msg);
      return true;
    case 'tool_result':
      await handleAiNodeToolResultChunk(ctx, state, msg);
      return true;
    case 'result':
      return handleAiNodeResultChunk(ctx, state, msg, backgroundTasks);
    case 'background_tasks':
      backgroundTasks.update(msg.tasks);
      return true;
    case 'system':
      await handleAiNodeSystemChunk(ctx, msg);
      return true;
    default:
      handleAiNodeLifecycleChunk(ctx, msg);
      return true;
  }
}

function resetAiNodePassState(state: AiNodeExecutionState): void {
  state.nodeOutputText = '';
  state.structuredOutput = undefined;
  state.batchMessages.length = 0;
  state.nodeTokens = undefined;
  state.nodeTokensAreAccumulated = false;
  state.nodeCostUsd = undefined;
  state.nodeIdleTimedOut = false;
  state.nodeTotalTimedOut = false;
  state.backgroundTasksIncomplete = [];
}

function startAiNodeTotalDeadlineTimer(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): ReturnType<typeof setTimeout> | undefined {
  const remaining = remainingDeadlineMs(ctx.totalDeadlineAt);
  if (remaining === 0) {
    state.nodeAbortController.abort();
    throw new Error(`Node '${ctx.node.id}' exceeded total timeout`);
  }
  return remaining === undefined
    ? undefined
    : setTimeout(() => {
        state.nodeTotalTimedOut = true;
        getLog().warn(
          { nodeId: ctx.node.id, timeoutMs: remaining },
          'dag_node_total_timeout_reached'
        );
        state.nodeAbortController.abort();
      }, remaining);
}

async function streamAiNodePass(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  attemptPrompt: string,
  attemptResumeId: string | undefined,
  backgroundTasks: ReturnType<typeof createBackgroundTaskTracker>
): Promise<void> {
  for await (const msg of withIdleTimeout(
    ctx.aiClient.sendQuery(attemptPrompt, ctx.cwd, attemptResumeId, ctx.nodeOptionsWithAbort),
    state.effectiveIdleTimeout,
    () => {
      state.nodeIdleTimedOut = true;
      getLog().warn(
        { nodeId: ctx.node.id, timeoutMs: state.effectiveIdleTimeout },
        'dag_node_idle_timeout_reached'
      );
      state.nodeAbortController.abort();
    }
  )) {
    const tickNow = Date.now();
    const nodeKey = `${ctx.workflowRun.id}:${ctx.node.id}`;
    if (!(await shouldContinueAiNodeStream(ctx, state, tickNow, nodeKey))) break;
    await updateAiNodeStreamActivity(ctx, tickNow, nodeKey);
    if (state.nodeTotalTimedOut) break;
    if (!(await handleAiNodeStreamChunk(ctx, state, msg, backgroundTasks))) break;
  }
}

async function warnAiNodeIncompleteBackgroundTasks(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  backgroundTasks: ReturnType<typeof createBackgroundTaskTracker>
): Promise<void> {
  state.backgroundTasksIncomplete = backgroundTasks.ids();
  const cancelled = state.nodeAbortController.signal.aborted && !state.nodeIdleTimedOut;
  getLog().warn(
    {
      nodeId: ctx.node.id,
      taskIds: state.backgroundTasksIncomplete,
      idleTimedOut: state.nodeIdleTimedOut,
      cancelled,
    },
    'dag.node_stream_ended_with_live_background_tasks'
  );
  if (cancelled) return;
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `⚠️ Node \`${ctx.node.id}\`: the provider stream ended with ${String(state.backgroundTasksIncomplete.length)} background agent task(s) still running (${state.backgroundTasksIncomplete.join(', ')}). Their output may be missing — treat this node's artifacts as potentially incomplete.`,
    ctx.nodeContext
  );
}

async function runAiNodeStreamPass(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  attemptPrompt: string,
  attemptResumeId: string | undefined
): Promise<void> {
  resetAiNodePassState(state);
  const backgroundTasks = createBackgroundTaskTracker();
  const totalDeadlineTimer = startAiNodeTotalDeadlineTimer(ctx, state);
  try {
    await streamAiNodePass(ctx, state, attemptPrompt, attemptResumeId, backgroundTasks);
  } finally {
    if (totalDeadlineTimer !== undefined) clearTimeout(totalDeadlineTimer);
  }
  if (state.nodeTotalTimedOut)
    throw new Error(
      `Node '${ctx.node.id}' exceeded total timeout of ${String(nodeTotalTimeoutMs(ctx.node))}ms`
    );
  if (!backgroundTasks.shouldBreakOnResult())
    await warnAiNodeIncompleteBackgroundTasks(ctx, state, backgroundTasks);
}

function buildAiNodeReaskPrompt(finalPrompt: string, errors: string[]): string {
  return (
    `${finalPrompt}\n\n--- CORRECTION ---\n` +
    `Your previous response did not satisfy the required JSON schema: ${errors.join('; ')}. ` +
    'Respond again with ONLY a JSON object matching the schema — no prose, no code fences.'
  );
}

async function emitAiNodeReask(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  attempt: number
): Promise<void> {
  getLog().warn(
    { nodeId: ctx.node.id, workflowRunId: ctx.workflowRun.id, attempt, maxReasks: state.maxReasks },
    'dag.structured_output_reask'
  );
  if (attempt !== 1) return;
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `⚠️ Node \`${ctx.node.id}\`: structured output didn't match the schema — asking the model to correct it (up to ${state.maxReasks} attempt(s)).`,
    ctx.nodeContext
  );
}

function accumulateAiNodePassUsage(state: AiNodeExecutionState): TokenUsage | undefined {
  if (state.nodeCostUsd !== undefined)
    state.accumulatedCostUsd = (state.accumulatedCostUsd ?? 0) + state.nodeCostUsd;
  state.nodeCostUsd = state.accumulatedCostUsd;
  if (state.nodeTokens === undefined) state.passTokenUsageUnknown = true;
  else if (!state.passTokenUsageUnknown) {
    const mergedTokens = mergeTokenUsage(state.accumulatedNodeTokens, state.nodeTokens);
    if (mergedTokens === undefined) state.passTokenUsageUnknown = true;
    else state.accumulatedNodeTokens = mergedTokens;
  }
  const rawPassTokens = state.passTokenUsageUnknown ? undefined : state.nodeTokens;
  state.nodeTokens = state.passTokenUsageUnknown ? undefined : state.accumulatedNodeTokens;
  state.nodeTokensAreAccumulated = true;
  return rawPassTokens;
}

async function checkpointAiNodePass(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  rawPassTokens: TokenUsage | undefined
): Promise<void> {
  await ctx.budgetPassCheckpoint?.(
    {
      state: 'completed',
      output: state.nodeOutputText,
      ...(state.accumulatedNodeTokens !== undefined ? { tokens: state.accumulatedNodeTokens } : {}),
    },
    {
      state: 'completed',
      output: state.nodeOutputText,
      ...(rawPassTokens !== undefined ? { tokens: rawPassTokens } : {}),
    }
  );
}

async function handleAiNodeValidStructuredOutput(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<void> {
  try {
    state.nodeOutputText =
      typeof state.structuredOutput === 'string'
        ? state.structuredOutput
        : JSON.stringify(state.structuredOutput);
  } catch (serializeErr) {
    const err = serializeErr as Error;
    throw new Error(
      `Node '${ctx.node.id}': failed to serialize structured_output to JSON: ${err.message}`
    );
  }
  getLog().debug(
    { nodeId: ctx.node.id, streamingMode: ctx.streamingMode },
    'dag.structured_output_override'
  );
}

async function validateAiNodeStructuredOutput(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  canReask: boolean,
  scheduleReask: (errors: string[]) => Promise<void>
): Promise<boolean> {
  if (!ctx.nodeOptions?.outputFormat) return true;
  if (state.structuredOutput === undefined)
    return handleAiNodeMissingStructuredOutput(ctx, state, canReask, scheduleReask);
  let schemaCompileError: string | undefined;
  const validation = validateStructuredOutput(
    state.structuredOutput,
    ctx.node.output_format ?? {},
    compileMsg => {
      schemaCompileError = compileMsg;
    }
  );
  if (schemaCompileError !== undefined) await warnAiNodeSchemaCompileError(ctx, schemaCompileError);
  if (validation.valid) {
    await handleAiNodeValidStructuredOutput(ctx, state);
    return true;
  }
  getLog().warn(
    { nodeId: ctx.node.id, workflowRunId: ctx.workflowRun.id, errors: validation.errors },
    'dag.structured_output_invalid'
  );
  if (canReask) {
    await scheduleReask(validation.errors);
    return false;
  }
  throw new Error(
    `Node '${ctx.node.id}': output_format declared but the provider's structured output failed schema validation: ${validation.errors.join('; ')}`
  );
}

async function warnAiNodeSchemaCompileError(
  ctx: AiNodeExecutionContext,
  schemaCompileError: string
): Promise<void> {
  getLog().warn(
    { nodeId: ctx.node.id, workflowRunId: ctx.workflowRun.id, compileMsg: schemaCompileError },
    'dag.structured_output_schema_uncompilable'
  );
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `⚠️ Node '${ctx.node.id}': its \`output_format\` schema could not be compiled (${schemaCompileError}), so the structured output was NOT validated against it. Fix the schema to enforce it.`,
    ctx.nodeContext
  );
}

async function handleAiNodeMissingStructuredOutput(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  canReask: boolean,
  scheduleReask: (errors: string[]) => Promise<void>
): Promise<boolean> {
  getLog().warn(
    { nodeId: ctx.node.id, workflowRunId: ctx.workflowRun.id },
    'dag.structured_output_missing'
  );
  if (canReask) {
    await scheduleReask(['no JSON object was found in the response']);
    return false;
  }
  if (state.nodeIdleTimedOut && state.nodeTotalTimedOut)
    throw new Error(
      `Node '${ctx.node.id}' exceeded total timeout of ${String(nodeTotalTimeoutMs(ctx.node))}ms before producing the required structured output.`
    );
  if (state.nodeIdleTimedOut)
    throw new Error(
      `Node '${ctx.node.id}': timed out (no output for ${String(state.effectiveIdleTimeout / 60000)} min) before producing the required structured output.`
    );
  throw new Error(
    `Node '${ctx.node.id}': output_format declared but the provider returned no schema-valid structured output. The model likely replied with prose, refused, or emitted unparseable JSON.`
  );
}

async function runAiNodeReaskLoop(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  finalPrompt: string
): Promise<void> {
  let reaskAttempt = 0;
  let reaskPrompt = finalPrompt;
  const scheduleReask = async (errors: string[]): Promise<void> => {
    reaskAttempt++;
    reaskPrompt = buildAiNodeReaskPrompt(finalPrompt, errors);
    await emitAiNodeReask(ctx, state, reaskAttempt);
  };
  while (true) {
    await ctx.budgetPassCheckpoint?.(
      { state: 'completed', output: '', tokens: { input: 0, output: 0 } },
      { state: 'completed', output: '', tokens: { input: 0, output: 0 } }
    );
    await runAiNodeStreamPass(
      ctx,
      state,
      reaskPrompt,
      reaskAttempt === 0 ? ctx.resumeSessionId : undefined
    );
    const rawPassTokens = accumulateAiNodePassUsage(state);
    await checkpointAiNodePass(ctx, state, rawPassTokens);
    const canReask =
      reaskAttempt < state.maxReasks &&
      !state.nodeIdleTimedOut &&
      !state.nodeAbortController.signal.aborted;
    if (await validateAiNodeStructuredOutput(ctx, state, canReask, scheduleReask)) break;
  }
}

async function maybeWarnAiNodeIdleCompletion(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<void> {
  if (
    !state.nodeIdleTimedOut ||
    (state.nodeOutputText.trim() === '' && state.structuredOutput === undefined)
  )
    return;
  getLog().warn(
    { nodeId: ctx.node.id, timeoutMs: state.effectiveIdleTimeout },
    'dag_node_completed_via_idle_timeout'
  );
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `⚠️ Node \`${ctx.node.id}\` completed via idle timeout (no output for ${String(state.effectiveIdleTimeout / 60000)} min). The AI likely finished but the subprocess didn't exit cleanly.`,
    ctx.nodeContext
  );
}

function cleanupAiNodeThrottle(ctx: AiNodeExecutionContext): void {
  lastNodeCancelCheck.delete(`${ctx.workflowRun.id}:${ctx.node.id}`);
  lastNodeActivityUpdate.delete(`${ctx.workflowRun.id}:${ctx.node.id}`);
}

async function maybeReturnAiNodeCancelled(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<NodeExecutionResult | undefined> {
  if (
    !state.nodeAbortController.signal.aborted ||
    state.nodeIdleTimedOut ||
    state.nodeTotalTimedOut
  )
    return undefined;
  const duration = Date.now() - ctx.nodeStartTime;
  getLog().info(
    { nodeId: ctx.node.id, durationMs: duration },
    'dag_node_cancelled_during_streaming'
  );
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_failed',
      step_name: ctx.stepName,
      data: { error: 'Cancelled by user', duration_ms: duration },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_failed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.command ?? ctx.node.id,
    error: 'Cancelled by user',
  });
  cleanupAiNodeThrottle(ctx);
  return { state: 'failed', output: state.nodeOutputText, error: 'Cancelled by user' };
}

async function flushAiNodeBatchMessages(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<void> {
  if (ctx.streamingMode !== 'batch' || state.batchMessages.length === 0) return;
  const batchContent =
    state.structuredOutput !== undefined && ctx.nodeOptions?.outputFormat
      ? state.nodeOutputText
      : state.batchMessages.join('\n\n');
  await safeSendMessage(ctx.platform, ctx.conversationId, batchContent, ctx.nodeContext);
}

async function maybeReturnAiNodeCreditFailure(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<NodeExecutionResult | undefined> {
  const creditError = detectCreditExhaustion(state.nodeOutputText);
  if (!creditError) return undefined;
  const duration = Date.now() - ctx.nodeStartTime;
  getLog().warn({ nodeId: ctx.node.id, durationMs: duration }, 'dag.node_credit_exhausted');
  await logNodeError(ctx.logDir, ctx.workflowRun.id, ctx.node.id, creditError);
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_failed',
      step_name: ctx.stepName,
      data: { error: creditError },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_failed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.command ?? ctx.node.id,
    error: creditError,
  });
  cleanupAiNodeThrottle(ctx);
  return { state: 'failed', output: state.nodeOutputText, error: creditError };
}

async function maybeReturnAiNodeEmptyFailure(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<NodeExecutionResult | undefined> {
  if (state.nodeOutputText.trim() !== '' || state.structuredOutput !== undefined) return undefined;
  const duration = Date.now() - ctx.nodeStartTime;
  const emptyError = state.nodeIdleTimedOut
    ? `Node '${ctx.node.id}' timed out with no output (idle for ${String(state.effectiveIdleTimeout / 60000)} min). The provider did not emit any content before the watchdog fired — likely time-to-first-token exceeded the timeout. Consider increasing idle_timeout or reducing prompt size.`
    : `Node '${ctx.node.id}' produced no assistant output. The provider stream closed without yielding content — likely a silent provider rejection or stream interruption.`;
  getLog().error({ nodeId: ctx.node.id, durationMs: duration }, 'dag.node_empty_output');
  await logNodeError(ctx.logDir, ctx.workflowRun.id, ctx.node.id, emptyError);
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_failed',
      step_name: ctx.stepName,
      data: { error: emptyError, duration_ms: duration },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_failed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.command ?? ctx.node.id,
    error: emptyError,
  });
  cleanupAiNodeThrottle(ctx);
  return { state: 'failed', output: '', error: emptyError };
}

async function completeAiNode(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<NodeExecutionResult> {
  state.nodeTokens = state.passTokenUsageUnknown ? undefined : state.accumulatedNodeTokens;
  const duration = Date.now() - ctx.nodeStartTime;
  getLog().info({ nodeId: ctx.node.id, durationMs: duration }, 'dag_node_completed');
  await logNodeComplete(
    ctx.logDir,
    ctx.workflowRun.id,
    ctx.node.id,
    ctx.node.command ?? '<inline>',
    { durationMs: duration, tokens: state.nodeTokens }
  );
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_completed',
      step_name: ctx.stepName,
      data: {
        duration_ms: duration,
        node_output: state.nodeOutputText,
        ...(state.nodeTokens !== undefined ? { tokens: state.nodeTokens } : {}),
        ...(state.nodeCostUsd !== undefined ? { cost_usd: state.nodeCostUsd } : {}),
        ...(state.nodeStopReason ? { stop_reason: state.nodeStopReason } : {}),
        ...(state.nodeNumTurns !== undefined ? { num_turns: state.nodeNumTurns } : {}),
        ...(state.nodeResolvedModel
          ? { model_usage: { requested: ctx.resolvedModel, resolved: state.nodeResolvedModel.id } }
          : {}),
        ...(state.backgroundTasksIncomplete.length > 0
          ? { background_tasks_incomplete: state.backgroundTasksIncomplete }
          : {}),
        ...ctx.iterationData,
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_completed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_completed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.command ?? ctx.node.id,
    duration,
    ...(state.nodeCostUsd !== undefined ? { costUsd: state.nodeCostUsd } : {}),
    ...(state.nodeStopReason ? { stopReason: state.nodeStopReason } : {}),
    ...(state.nodeNumTurns !== undefined ? { numTurns: state.nodeNumTurns } : {}),
  });
  cleanupAiNodeThrottle(ctx);
  const declaredFields = declaredFieldsFromSchema(ctx.node.output_format);
  return {
    state: 'completed',
    output: state.nodeOutputText,
    sessionId: state.newSessionId,
    costUsd: state.nodeCostUsd,
    ...(state.nodeTokens !== undefined ? { tokens: state.nodeTokens } : {}),
    ...(state.structuredOutput !== undefined ? { structuredOutput: state.structuredOutput } : {}),
    ...(declaredFields !== undefined ? { declaredFields } : {}),
    ...(state.nodeResumed !== undefined ? { resumed: state.nodeResumed } : {}),
  };
}

async function finishAiNodeSuccess(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState
): Promise<NodeExecutionResult> {
  if (state.nodeTotalTimedOut)
    throw new Error(
      `Node '${ctx.node.id}' exceeded total timeout of ${String(nodeTotalTimeoutMs(ctx.node))}ms`
    );
  await maybeWarnAiNodeIdleCompletion(ctx, state);
  const cancelled = await maybeReturnAiNodeCancelled(ctx, state);
  if (cancelled) return cancelled;
  await flushAiNodeBatchMessages(ctx, state);
  return (
    (await maybeReturnAiNodeCreditFailure(ctx, state)) ??
    (await maybeReturnAiNodeEmptyFailure(ctx, state)) ??
    (await completeAiNode(ctx, state))
  );
}

async function failAiNodeAfterStream(
  ctx: AiNodeExecutionContext,
  state: AiNodeExecutionState,
  error: Error
): Promise<NodeExecutionResult> {
  const failedNodeTokens = resolveFailedNodeTokens(
    state.accumulatedNodeTokens,
    state.nodeTokens,
    state.nodeTokensAreAccumulated,
    state.passTokenUsageUnknown
  );
  cleanupAiNodeThrottle(ctx);
  if (
    state.nodeAbortController.signal.aborted &&
    !state.nodeIdleTimedOut &&
    !state.nodeTotalTimedOut
  ) {
    getLog().info({ nodeId: ctx.node.id }, 'dag_node_cancelled_via_abort');
    return {
      state: 'failed',
      output: state.nodeOutputText,
      error: 'Cancelled by user',
      costUsd: state.nodeCostUsd,
      ...(failedNodeTokens !== undefined ? { tokens: failedNodeTokens } : {}),
    };
  }
  getLog().error({ err: error, nodeId: ctx.node.id }, 'dag_node_failed');
  await logNodeError(ctx.logDir, ctx.workflowRun.id, ctx.node.id, error.message);
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_failed',
      step_name: ctx.stepName,
      data: { error: error.message },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_failed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.command ?? ctx.node.id,
    error: error.message,
  });
  return {
    state: 'failed',
    output: '',
    error: error.message,
    costUsd: state.nodeCostUsd,
    ...(failedNodeTokens !== undefined ? { tokens: failedNodeTokens } : {}),
  };
}

/**
 * Execute a single DAG node. Returns NodeExecutionResult regardless of success/failure.
 * Always accumulates assistant text output (for $node_id.output substitution).
 * Parallel nodes and context: 'fresh' nodes always receive fresh sessions (caller ensures resumeSessionId is undefined).
 */
async function executeNodeInternal(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflowRun: WorkflowRun,
  node: CommandNode | PromptNode,
  provider: string,
  nodeOptions: SendQueryOptions | undefined,
  artifactsDir: string,
  stateDir: string,
  logDir: string,
  baseBranch: string,
  docsDir: string,
  nodeOutputs: Map<string, NodeOutput>,
  resumeSessionId: string | undefined,
  configuredCommandFolder?: string,
  issueContext?: string,
  resolvedModel?: string,
  resolvedTier?: TierName,
  resolvedEffort?: string,
  stepNamePrefix = '',
  iteration?: number,
  totalDeadlineAt?: number,
  budgetPassCheckpoint?: BudgetPassCheckpoint
): Promise<NodeExecutionResult> {
  const nodeStartTime = Date.now();
  const nodeAbortController = new AbortController();
  const shouldForkSession = resumeSessionId !== undefined;
  const ctx: AiNodeExecutionContext = {
    deps,
    platform,
    conversationId,
    cwd,
    workflowRun,
    node,
    provider,
    nodeOptions,
    artifactsDir,
    stateDir,
    logDir,
    baseBranch,
    docsDir,
    nodeOutputs,
    resumeSessionId,
    configuredCommandFolder,
    issueContext,
    resolvedModel,
    resolvedTier,
    resolvedEffort,
    stepNamePrefix,
    iteration,
    totalDeadlineAt,
    budgetPassCheckpoint,
    nodeStartTime,
    nodeContext: { workflowId: workflowRun.id, nodeName: node.id },
    stepName: stepNamePrefix + node.id,
    iterationData: iteration !== undefined ? { iteration } : {},
    configuredMcpNames: await loadConfiguredMcpServerNames(node.mcp, cwd),
    aiClient: deps.getAgentProvider(provider),
    streamingMode: platform.getStreamingMode(),
    nodeOptionsWithAbort: {
      ...nodeOptions,
      abortSignal: nodeAbortController.signal,
      ...(shouldForkSession ? { forkSession: true } : {}),
    },
  };
  await emitAiNodeStarted(ctx);
  const rawPrompt = await resolveAiNodeRawPrompt(ctx);
  if (typeof rawPrompt !== 'string') return rawPrompt;
  const finalPrompt = await buildAiNodeFinalPrompt(ctx, rawPrompt);
  if (typeof finalPrompt !== 'string') return finalPrompt;
  const state = createAiNodeExecutionState(ctx, nodeAbortController);
  try {
    await runAiNodeReaskLoop(ctx, state, finalPrompt);
    return await finishAiNodeSuccess(ctx, state);
  } catch (error) {
    return failAiNodeAfterStream(ctx, state, error as Error);
  }
}

/** Default timeout for subprocess nodes (bash, script): 2 minutes */
const SUBPROCESS_DEFAULT_TIMEOUT = 120_000;

/**
 * Reduce a host-resolved command to the name the container image exposes on
 * PATH: strip any directory (a host absolute path like the Windows Git-Bash
 * `bash.exe` doesn't exist in the Linux runner) and a trailing `.exe`. `bash`,
 * `bun`, and `uv` all live on the runner image's PATH.
 */
export function containerCommandName(cmd: string): string {
  const base = cmd.replace(/\\/g, '/').split('/').pop() ?? cmd;
  return base.replace(/\.exe$/i, '');
}

/**
 * Run a deterministic subprocess (bash/script node body, loop `until_bash`) under
 * the given execution context.
 *
 * `options.env` is the ARCHON-MANAGED env only (node vars + codebase env + creds)
 * — NEVER pre-merged with `process.env`. The host path layers it over the
 * (already-cleaned) host `process.env`, byte-identical to before. The container
 * path delivers ONLY that managed env via `docker exec -e` (host `process.env`
 * never crosses the boundary — the isolation invariant) and runs the command
 * in-container at the same absolute cwd, so `bash:`/`script:` nodes have no
 * host-escape hole.
 */
/**
 * Build the `docker exec` argv for a deterministic subprocess (bash/script) in a
 * container. Env is delivered ONLY via `-e` flags (never merged with the docker
 * CLI's own env / host process.env — the isolation invariant); the command name
 * is normalized to the in-container binary. Exported for the env-isolation
 * enforcement test.
 */
export function buildSubprocessDockerArgs(
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  cmd: string,
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv }
): string[] {
  const dockerArgs = ['exec', '-w', options.cwd];
  if (execContext.execUser) dockerArgs.push('-u', execContext.execUser);
  for (const [key, value] of Object.entries(options.env)) {
    // Skip the denylist (PATH/HOME/…): a project env var must not clobber the
    // in-container binary/home resolution — same policy as the Claude spawn path.
    if (value === undefined || CONTAINER_ENV_DENYLIST.has(key)) continue;
    dockerArgs.push('-e', `${key}=${value}`);
  }
  dockerArgs.push(execContext.containerId, containerCommandName(cmd), ...args);
  return dockerArgs;
}

async function runSubprocess(
  execContext: ExecutionContext,
  cmd: string,
  args: string[],
  options: {
    cwd: string;
    timeout: number;
    env: NodeJS.ProcessEnv;
    deadlineAt?: number;
    getRunStatus?: () => Promise<string | null>;
  }
): Promise<{ stdout: string; stderr: string }> {
  if (execContext.kind === 'container') {
    const dockerArgs = buildSubprocessDockerArgs(execContext, cmd, args, {
      cwd: options.cwd,
      env: options.env,
    });
    return runGuardedContainerSubprocess(execContext, dockerArgs, options);
  }
  return execFileAsync(cmd, args, {
    cwd: options.cwd,
    timeout: options.timeout,
    env: { ...process.env, ...options.env },
  });
}

/** Threshold (bytes) above which $nodeId.output values are written to a temp file
 *  instead of inlined as bash -c arguments, to avoid silent data corruption. */
const NODE_OUTPUT_FILE_THRESHOLD = 32_768;

/** Maximum UTF-8 bytes retained for successful bash stdout in workflow events. */
const PERSISTED_BASH_OUTPUT_MAX_BYTES = 32 * 1024;

function utf8SequenceLength(leadByte: number): number {
  if (leadByte < 0x80) return 1;
  if (leadByte < 0xe0) return 2;
  if (leadByte < 0xf0) return 3;
  return 4;
}

function formatPersistedBashOutput(output: string): {
  nodeOutput: string;
  truncated: boolean;
  originalBytes?: number;
} {
  const outputBytes = Buffer.from(output, 'utf8');
  if (outputBytes.byteLength <= PERSISTED_BASH_OUTPUT_MAX_BYTES) {
    return { nodeOutput: output, truncated: false };
  }

  const marker = buildTruncationMarker(outputBytes.byteLength);
  const markerBytes = Buffer.byteLength(marker, 'utf8');
  let headEnd = PERSISTED_BASH_OUTPUT_MAX_BYTES - markerBytes;

  // The byte cap can land inside a multi-byte code point. Inspect the final
  // sequence in the prefix and drop it when it is incomplete before decoding.
  let sequenceStart = headEnd - 1;
  while (sequenceStart >= 0 && (outputBytes[sequenceStart] & 0xc0) === 0x80) {
    sequenceStart--;
  }
  if (sequenceStart >= 0) {
    const leadByte = outputBytes[sequenceStart];
    const expectedLength = utf8SequenceLength(leadByte);
    if (headEnd - sequenceStart < expectedLength) headEnd = sequenceStart;
  }

  return {
    nodeOutput: outputBytes.subarray(0, headEnd).toString('utf8') + marker,
    truncated: true,
    originalBytes: outputBytes.byteLength,
  };
}

/**
 * Execute a bash (shell script) DAG node.
 * Runs the script via `bash -c`, captures stdout as node output.
 * No AI session is created — bash nodes are free/deterministic.
 */
async function executeBashNode(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflowRun: WorkflowRun,
  node: BashNode,
  artifactsDir: string,
  stateDir: string,
  logDir: string,
  baseBranch: string,
  docsDir: string,
  nodeOutputs: Map<string, NodeOutput>,
  issueContext?: string,
  envVars?: Record<string, string>,
  stepNamePrefix = '',
  iteration?: number,
  execContext: ExecutionContext = { kind: 'host' },
  deadlineAt?: number
): Promise<NodeOutput> {
  const nodeStartTime = Date.now();
  const nodeContext: SendMessageContext = { workflowId: workflowRun.id, nodeName: node.id };
  // Namespaced persisted step_name for loop_group bodies ('' → node.id at top level, #2090).
  const stepName = stepNamePrefix + node.id;
  const iterationData = iteration !== undefined ? { iteration } : {};

  getLog().info({ nodeId: node.id, type: 'bash' }, 'dag_node_started');
  await logNodeStart(logDir, workflowRun.id, node.id, '<bash>');

  deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'node_started',
      step_name: stepName,
      data: { type: 'bash', ...iterationData },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: workflowRun.id, eventType: 'node_started' },
        'workflow_event_persist_failed'
      );
    });

  const emitter = getWorkflowEventEmitter();
  emitter.emit({
    type: 'node_started',
    runId: workflowRun.id,
    nodeId: node.id,
    nodeName: node.id,
  });

  // Variable substitution on script
  const { prompt: substitutedScript } = substituteWorkflowVariables(
    node.bash,
    workflowRun.id,
    workflowRun.user_message,
    artifactsDir,
    baseBranch,
    docsDir,
    issueContext,
    undefined,
    undefined,
    undefined,
    { shellSafe: true, stateDir, execContext: execContext }
  );
  const finalScript = substituteNodeOutputRefs(substitutedScript, nodeOutputs, true, logDir);

  const timeout = node.timeout ?? SUBPROCESS_DEFAULT_TIMEOUT;
  // Archon-managed env only — runSubprocess adds the host env for host runs and
  // delivers ONLY this bag into the container (host process.env never crosses).
  // Configured project env (envVars) spreads FIRST so the engine-reserved keys below
  // always win — a codebase env var named ARGUMENTS/CONTEXT/… must never shadow the
  // values this node delivers (that IS the injection-safe delivery channel, #2115).
  // The GitHub-token scrub keys (GH_TOKEN/GITHUB_TOKEN/COPILOT_GITHUB_TOKEN) are
  // disjoint from the reserved set and stay in the bag, still overriding the ambient
  // host token via runSubprocess's process.env layering — the scrub is unaffected.
  const agentPaths = resolveAgentOutputPaths(artifactsDir, stateDir, logDir, execContext);
  const subprocessEnv: NodeJS.ProcessEnv = {
    ...(envVars ?? {}),
    ARTIFACTS_DIR: agentPaths.artifactsDir,
    STATE_DIR: agentPaths.stateDir,
    LOG_DIR: agentPaths.logDir,
    BASE_BRANCH: baseBranch,
    USER_MESSAGE: workflowRun.user_message,
    ARGUMENTS: workflowRun.user_message,
    LOOP_USER_INPUT: '',
    LOOP_PREV_OUTPUT: '',
    REJECTION_REASON: '',
    CONTEXT: issueContext ?? '',
    EXTERNAL_CONTEXT: issueContext ?? '',
    ISSUE_CONTEXT: issueContext ?? '',
  };

  const bashPath = resolveBashPath();
  try {
    const { stdout, stderr } = await runSubprocess(execContext, bashPath, ['-c', finalScript], {
      cwd,
      timeout,
      deadlineAt,
      getRunStatus: () => deps.store.getWorkflowRunStatus(workflowRun.id),
      env: subprocessEnv,
    });

    // Trim trailing newline from stdout (common shell behavior)
    const output = stdout.replace(/\n$/, '');

    if (stderr.trim()) {
      getLog().warn({ nodeId: node.id, stderr: stderr.trim() }, 'bash_node_stderr');
      await safeSendMessage(
        platform,
        conversationId,
        `Bash node '${node.id}' stderr:\n\`\`\`\n${stderr.trim()}\n\`\`\``,
        nodeContext
      );
    }

    const duration = Date.now() - nodeStartTime;
    getLog().info({ nodeId: node.id, durationMs: duration }, 'dag_node_completed');
    await logNodeComplete(logDir, workflowRun.id, node.id, '<bash>', { durationMs: duration });

    const persistedOutput = formatPersistedBashOutput(output);

    deps.store
      .createWorkflowEvent({
        workflow_run_id: workflowRun.id,
        event_type: 'node_completed',
        step_name: stepName,
        data: {
          duration_ms: duration,
          type: 'bash',
          node_output: persistedOutput.nodeOutput,
          ...(persistedOutput.truncated
            ? {
                node_output_truncated: true,
                node_output_original_bytes: persistedOutput.originalBytes,
              }
            : {}),
          ...iterationData,
        },
      })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: workflowRun.id, eventType: 'node_completed' },
          'workflow_event_persist_failed'
        );
      });

    emitter.emit({
      type: 'node_completed',
      runId: workflowRun.id,
      nodeId: node.id,
      nodeName: node.id,
      duration,
    });

    return { state: 'completed', output };
  } catch (error) {
    const err = error as Error & { killed?: boolean; code?: number | string; stderr?: string };
    const isTimeout = err.killed === true || (err.message ?? '').includes('timed out');
    const label = `Bash node '${node.id}'`;
    // Always run the formatter so logs get sanitized fields regardless of which
    // user-facing branch we end up in — the timeout message also contains the
    // full `Command failed: bash -c <body>` line and would otherwise leak.
    const formatted = formatSubprocessFailure(err, label);
    let errorMsg: string;
    if (isTimeout) {
      errorMsg = `${label} timed out after ${String(timeout)}ms`;
    } else if (err.code === 'ENOENT' || err.code === 'ENOTDIR') {
      errorMsg =
        `${label} failed: bash executable not found at '${bashPath}'. ` +
        'Set ARCHON_BASH_PATH if Git Bash is installed elsewhere ' +
        '(e.g. user-scope installer at %LOCALAPPDATA%\\Programs\\Git\\bin\\bash.exe).';
    } else if (err.code === 'EACCES') {
      errorMsg = `${label} failed: permission denied (check cwd permissions)`;
    } else {
      errorMsg = formatted.userMessage;
    }

    getLog().error(
      { ...formatted.logFields, nodeId: node.id, nodeType: 'bash', isTimeout },
      'dag_node_failed'
    );
    await logNodeError(logDir, workflowRun.id, node.id, errorMsg);

    deps.store
      .createWorkflowEvent({
        workflow_run_id: workflowRun.id,
        event_type: 'node_failed',
        step_name: stepName,
        data: { error: errorMsg, type: 'bash' },
      })
      .catch((dbErr: Error) => {
        getLog().error(
          { err: dbErr, workflowRunId: workflowRun.id, eventType: 'node_failed' },
          'workflow_event_persist_failed'
        );
      });

    emitter.emit({
      type: 'node_failed',
      runId: workflowRun.id,
      nodeId: node.id,
      nodeName: node.id,
      error: errorMsg,
    });

    return { state: 'failed', output: '', error: errorMsg };
  }
}

/**
 * User-controlled workflow variables that {@link executeScriptNode} delivers via
 * subprocess env vars instead of splicing into the script source. Matches the
 * literal `$VAR` form only (word-boundary lookahead) so `$LOOP_PREV.<id>.output`
 * refs and `process.env.ARGUMENTS`-style accessors never false-positive (#2115).
 */
const SCRIPT_USER_VAR_PATTERN =
  /\$(?:USER_MESSAGE|ARGUMENTS|LOOP_USER_INPUT|LOOP_PREV_OUTPUT|REJECTION_REASON|CONTEXT|EXTERNAL_CONTEXT|ISSUE_CONTEXT)(?![A-Za-z0-9_])/g;

/**
 * Migration aid (#2115): script bodies used to raw-splice user-controlled text
 * ($ARGUMENTS/$CONTEXT family/…) directly into TS/Python source — an injection
 * channel. Those refs are now delivered as env vars and no longer substituted, so
 * a literal `$VAR` left in the body silently stops resolving. Warn the author (log
 * + one concise platform line) with the language-appropriate accessor for one
 * release before the refs are removed. `script` is the post-workflow-var,
 * pre-node-output string so an expanded `$nodeId.output` value can't false-positive.
 */
async function warnOnLiteralUserVars(
  node: ScriptNode,
  script: string,
  platform: IWorkflowPlatform,
  conversationId: string,
  nodeContext: SendMessageContext
): Promise<void> {
  const matches = script.match(SCRIPT_USER_VAR_PATTERN);
  if (!matches) return;
  const unique = [...new Set(matches)];
  const accessor = unique
    .map(v => (node.runtime === 'uv' ? `os.environ['${v.slice(1)}']` : `process.env.${v.slice(1)}`))
    .join(', ');
  getLog().warn(
    { nodeId: node.id, runtime: node.runtime, vars: unique },
    'script_node_literal_user_var'
  );
  await safeSendMessage(
    platform,
    conversationId,
    `Script node '${node.id}': ${unique.join(', ')} ${unique.length > 1 ? 'are' : 'is'} no longer ` +
      'substituted into script source (security hardening, #2115). ' +
      `Read from the environment instead: ${accessor}.`,
    nodeContext
  );
}

interface ScriptNodeExecutionMeta {
  workflowRun: WorkflowRun;
  node: ScriptNode;
  stepName: string;
  logDir: string;
  nodeContext: SendMessageContext;
}

type ScriptCommandResolution =
  | { ok: true; cmd: string; args: string[] }
  | { ok: false; output: NodeOutput };

async function emitScriptNodeFailure(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  meta: ScriptNodeExecutionMeta,
  errorMsg: string,
  logMeta: Record<string, unknown> = {}
): Promise<NodeOutput> {
  getLog().error({ nodeId: meta.node.id, ...logMeta }, 'script_node_resolution_failed');
  await safeSendMessage(platform, conversationId, errorMsg, meta.nodeContext);
  await logNodeError(meta.logDir, meta.workflowRun.id, meta.node.id, errorMsg);
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: meta.workflowRun.id,
    nodeId: meta.node.id,
    nodeName: meta.node.id,
    error: errorMsg,
  });
  deps.store
    .createWorkflowEvent({
      workflow_run_id: meta.workflowRun.id,
      event_type: 'node_failed',
      step_name: meta.stepName,
      data: { error: errorMsg, type: 'script' },
    })
    .catch((dbErr: Error) => {
      getLog().error(
        { err: dbErr, workflowRunId: meta.workflowRun.id, eventType: 'node_failed' },
        'workflow_event_persist_failed'
      );
    });
  return { state: 'failed', output: '', error: errorMsg };
}

function inlineScriptCommand(
  node: ScriptNode,
  finalScript: string
): { cmd: string; args: string[] } {
  if (node.runtime === 'bun') return { cmd: 'bun', args: ['--no-env-file', '-e', finalScript] };
  const withFlags = (node.deps ?? []).flatMap(dep => ['--with', dep]);
  return { cmd: 'uv', args: ['run', ...withFlags, 'python', '-c', finalScript] };
}

function discoveredScriptCommand(
  node: ScriptNode,
  scriptDef: NonNullable<
    Awaited<ReturnType<typeof discoverScriptsForCwd>> extends Map<string, infer T> ? T : never
  >
): { cmd: string; args: string[] } {
  if (scriptDef.runtime === 'uv') {
    const withFlags = (node.deps ?? []).flatMap(dep => ['--with', dep]);
    return { cmd: 'uv', args: ['run', ...withFlags, scriptDef.path] };
  }
  return { cmd: 'bun', args: ['--no-env-file', 'run', scriptDef.path] };
}

async function resolveScriptCommand(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  meta: ScriptNodeExecutionMeta,
  finalScript: string
): Promise<ScriptCommandResolution> {
  void deps;
  if (isInlineScript(finalScript))
    return { ok: true, ...inlineScriptCommand(meta.node, finalScript) };
  let scripts: Awaited<ReturnType<typeof discoverScriptsForCwd>>;
  try {
    scripts = await discoverScriptsForCwd(cwd);
  } catch (discoveryErr) {
    const err = discoveryErr as Error;
    const errorMsg = `Script node '${meta.node.id}': failed to discover scripts — ${err.message}`;
    const output = await emitScriptNodeFailure(deps, platform, conversationId, meta, errorMsg, {
      err,
      cwd,
    });
    return { ok: false, output };
  }
  const scriptDef = scripts.get(finalScript);
  if (!scriptDef) {
    const errorMsg = `Script node '${meta.node.id}': named script '${finalScript}' not found in .archon/scripts/ or ~/.archon/scripts/`;
    const output = await emitScriptNodeFailure(deps, platform, conversationId, meta, errorMsg, {
      scriptName: finalScript,
    });
    return { ok: false, output };
  }
  return { ok: true, ...discoveredScriptCommand(meta.node, scriptDef) };
}

function formatScriptExecutionError(
  err: Error & { killed?: boolean; code?: number | string; stderr?: string },
  label: string,
  cmd: string,
  timeout: number
): { errorMsg: string; isTimeout: boolean; formatted: ReturnType<typeof formatSubprocessFailure> } {
  const isTimeout = err.killed === true || (err.message ?? '').includes('timed out');
  const formatted = formatSubprocessFailure(err, label);
  if (isTimeout)
    return { errorMsg: `${label} timed out after ${String(timeout)}ms`, isTimeout, formatted };
  if (err.message?.includes('ENOENT'))
    return {
      errorMsg: `${label} failed: '${cmd}' executable not found in PATH`,
      isTimeout,
      formatted,
    };
  if (err.message?.includes('EACCES'))
    return {
      errorMsg: `${label} failed: permission denied (check cwd permissions)`,
      isTimeout,
      formatted,
    };
  return { errorMsg: formatted.userMessage, isTimeout, formatted };
}

/**
 * Execute a script (TypeScript via bun or Python via uv) DAG node.
 * Supports both inline code snippets and named scripts discovered from .archon/scripts/.
 * stdout is captured and trimmed as the node output; stderr is logged as a warning.
 */
async function executeScriptNode(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflowRun: WorkflowRun,
  node: ScriptNode,
  artifactsDir: string,
  stateDir: string,
  logDir: string,
  baseBranch: string,
  docsDir: string,
  nodeOutputs: Map<string, NodeOutput>,
  issueContext?: string,
  envVars?: Record<string, string>,
  stepNamePrefix = '',
  iteration?: number,
  // Per-iteration $LOOP_USER_INPUT free-text for loop_group body scripts, delivered via
  // env (never spliced into source — #2115). '' for top-level scripts and non-first
  // iterations (mirrors executeBashNode, which delivers loop input via quoted splice).
  loopUserInput = '',
  execContext: ExecutionContext = { kind: 'host' },
  deadlineAt?: number
): Promise<NodeOutput> {
  const nodeStartTime = Date.now();
  const nodeContext: SendMessageContext = { workflowId: workflowRun.id, nodeName: node.id };
  // Namespaced persisted step_name for loop_group bodies ('' → node.id at top level, #2090).
  const stepName = stepNamePrefix + node.id;
  const iterationData = iteration !== undefined ? { iteration } : {};

  getLog().info({ nodeId: node.id, type: 'script', runtime: node.runtime }, 'dag_node_started');
  await logNodeStart(logDir, workflowRun.id, node.id, '<script>');

  deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'node_started',
      step_name: stepName,
      data: { type: 'script', runtime: node.runtime, ...iterationData },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: workflowRun.id, eventType: 'node_started' },
        'workflow_event_persist_failed'
      );
    });

  const emitter = getWorkflowEventEmitter();
  emitter.emit({
    type: 'node_started',
    runId: workflowRun.id,
    nodeId: node.id,
    nodeName: node.id,
  });

  // Variable substitution on script field.
  // shellSafe: true skips literal substitution of the user-controlled variables
  // ($ARGUMENTS/$USER_MESSAGE/$CONTEXT family/$LOOP_*/$REJECTION_REASON) so
  // attacker-influenced text is never spliced into the TS/Python source that
  // `bun -e` / `uv run python -c` executes. Those values ride subprocess env vars
  // below instead (read via process.env.X / os.environ['X']), mirroring the
  // executeBashNode hardening. $nodeId.output refs keep raw substitution — the
  // strict producer contract bounds those values (#2115).
  const { prompt: substitutedScript } = substituteWorkflowVariables(
    node.script,
    workflowRun.id,
    workflowRun.user_message,
    artifactsDir,
    baseBranch,
    docsDir,
    issueContext,
    undefined,
    undefined,
    undefined,
    { shellSafe: true, stateDir, execContext: execContext }
  );
  const finalScript = substituteNodeOutputRefs(substitutedScript, nodeOutputs, false);

  // One-release migration warn for any literal user-controlled var ref that no
  // longer substitutes now that delivery moved to env vars (#2115).
  await warnOnLiteralUserVars(node, substitutedScript, platform, conversationId, nodeContext);

  const timeout = node.timeout ?? SUBPROCESS_DEFAULT_TIMEOUT;
  // Archon-managed env only — runSubprocess adds the host env for host runs and
  // delivers ONLY this bag into the container (host process.env never crosses).
  // User-controlled values ride env vars (never spliced into source) — the
  // sanctioned injection-safe channel, matching executeBashNode (#2115).
  // Configured project env (envVars) spreads FIRST so the engine-reserved keys below
  // always win — a codebase env var named ARGUMENTS/CONTEXT/… must never shadow this
  // delivery channel. The GitHub-token scrub keys are disjoint from the reserved set
  // and still override the ambient host token via runSubprocess (scrub unaffected).
  const agentPaths = resolveAgentOutputPaths(artifactsDir, stateDir, logDir, execContext);
  const subprocessEnv: NodeJS.ProcessEnv = {
    ...(envVars ?? {}),
    ARTIFACTS_DIR: agentPaths.artifactsDir,
    STATE_DIR: agentPaths.stateDir,
    LOG_DIR: agentPaths.logDir,
    BASE_BRANCH: baseBranch,
    USER_MESSAGE: workflowRun.user_message,
    ARGUMENTS: workflowRun.user_message,
    LOOP_USER_INPUT: loopUserInput,
    LOOP_PREV_OUTPUT: '',
    REJECTION_REASON: '',
    CONTEXT: issueContext ?? '',
    EXTERNAL_CONTEXT: issueContext ?? '',
    ISSUE_CONTEXT: issueContext ?? '',
  };

  const commandResolution = await resolveScriptCommand(
    deps,
    platform,
    conversationId,
    cwd,
    { workflowRun, node, stepName, logDir, nodeContext },
    finalScript
  );
  if (!commandResolution.ok) return commandResolution.output;
  const { cmd, args } = commandResolution;

  try {
    const { stdout, stderr } = await runSubprocess(execContext, cmd, args, {
      cwd,
      timeout,
      deadlineAt,
      getRunStatus: () => deps.store.getWorkflowRunStatus(workflowRun.id),
      env: subprocessEnv,
    });

    // Trim trailing newline from stdout (common shell behavior)
    const output = stdout.replace(/\n$/, '');

    if (stderr.trim()) {
      getLog().warn({ nodeId: node.id, stderr: stderr.trim() }, 'script_node_stderr');
      await safeSendMessage(
        platform,
        conversationId,
        `Script node '${node.id}' stderr:\n\`\`\`\n${stderr.trim()}\n\`\`\``,
        nodeContext
      );
    }

    const duration = Date.now() - nodeStartTime;
    getLog().info({ nodeId: node.id, durationMs: duration }, 'dag_node_completed');
    await logNodeComplete(logDir, workflowRun.id, node.id, '<script>', { durationMs: duration });

    deps.store
      .createWorkflowEvent({
        workflow_run_id: workflowRun.id,
        event_type: 'node_completed',
        step_name: stepName,
        data: { duration_ms: duration, type: 'script', node_output: output, ...iterationData },
      })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: workflowRun.id, eventType: 'node_completed' },
          'workflow_event_persist_failed'
        );
      });

    emitter.emit({
      type: 'node_completed',
      runId: workflowRun.id,
      nodeId: node.id,
      nodeName: node.id,
      duration,
    });

    return { state: 'completed', output };
  } catch (error) {
    const err = error as Error & { killed?: boolean; code?: number | string; stderr?: string };
    const label = `Script node '${node.id}'`;
    const { errorMsg, isTimeout, formatted } = formatScriptExecutionError(err, label, cmd, timeout);

    getLog().error(
      { ...formatted.logFields, nodeId: node.id, nodeType: 'script', isTimeout },
      'dag_node_failed'
    );
    await logNodeError(logDir, workflowRun.id, node.id, errorMsg);

    deps.store
      .createWorkflowEvent({
        workflow_run_id: workflowRun.id,
        event_type: 'node_failed',
        step_name: stepName,
        data: { error: errorMsg, type: 'script' },
      })
      .catch((dbErr: Error) => {
        getLog().error(
          { err: dbErr, workflowRunId: workflowRun.id, eventType: 'node_failed' },
          'workflow_event_persist_failed'
        );
      });

    emitter.emit({
      type: 'node_failed',
      runId: workflowRun.id,
      nodeId: node.id,
      nodeName: node.id,
      error: errorMsg,
    });

    return { state: 'failed', output: '', error: errorMsg };
  }
}

/** Cap for the iteration-output excerpt embedded in gate messages — keeps the
 *  persisted `metadata.approval.message` and SSE payloads bounded (mirrors the
 *  tool-input truncation used for progress events). */
const GATE_EXCERPT_MAX = 500;

/**
 * Build the honest interactive-gate message (#2074, change D): an engine-generated
 * status line (was the completion signal detected?) plus a bounded excerpt of the
 * final iteration output, prepended to the author's static `gate_message`. Shared
 * by executeLoopNode and executeLoopGroupNode so both gates tell the truth about
 * the iteration they paused on.
 */
function buildHonestGateMessage(
  completionDetected: boolean,
  untilSignal: string,
  lastIterationOutput: string,
  gateMessage: string
): string {
  const trimmed = lastIterationOutput.trim();
  const excerpt = trimmed.slice(0, GATE_EXCERPT_MAX);
  const statusLine = completionDetected
    ? `✅ Completion signal detected (\`${untilSignal}\`).`
    : `⚠️ No completion signal (\`${untilSignal}\`) in this iteration.`;
  const excerptBlock = excerpt
    ? `\n\n> ${excerpt}${trimmed.length > GATE_EXCERPT_MAX ? '…' : ''}`
    : '';
  return `${statusLine}${excerptBlock}\n\n${gateMessage}`;
}

/**
 * Narrow the token usage a loop gate persisted in its approval context (#2333).
 *
 * `metadata.approval` is free-form JSON read back from the DB and `isApprovalContext`
 * only vouches for nodeId/message, so the declared type carries no runtime authority
 * here: a run paused by a build that predates the field has none, and a malformed or
 * non-finite value must be dropped rather than persisted onward as a number a
 * consumer would believe.
 */
function readSignaledTokens(
  raw: unknown,
  context: { workflowRunId: string; nodeId: string }
): TokenUsage | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw === 'object') {
    const { input, output } = raw as { input?: unknown; output?: unknown };
    if (
      typeof input === 'number' &&
      typeof output === 'number' &&
      Number.isFinite(input) &&
      Number.isFinite(output)
    ) {
      return { input, output };
    }
  }
  getLog().warn({ ...context, tokens: raw }, 'dag_loop.signaled_tokens_invalid_ignored');
  return undefined;
}

/**
 * Finalize-on-approve (#2074), shared by executeLoopNode and executeLoopGroupNode:
 * a gate that paused on a signal-bearing iteration, resumed WITHOUT feedback,
 * completes the node from the persisted `signaledOutput` instead of re-running
 * the (expensive) iteration. Sends the user notice and writes/emits the
 * node_completed pair; the caller builds its own return value (the single-node
 * loop also threads the restored sessionId).
 *
 * `finalizeTokens` is the usage the pausing invocation actually consumed, carried
 * across the gate in the approval context (#2333) — without it this path persists a
 * node_completed reporting no usage for iterations that really ran. Passed by the
 * single-node loop ONLY: its per-iteration rows carry no tokens, so this row is the
 * only record. A loop_group omits it — its body nodes persisted their own namespaced
 * rows (with tokens) before the pause, and those rows survive it, so repeating the
 * total here would double-count in the one event stream. `cost_usd` and the resolved
 * model are lost across the same gate; both are part of the single "preserve terminal
 * provider stats across a gate" fix in #2345.
 */
async function finalizeLoopFromSignal(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRun: WorkflowRun,
  nodeId: string,
  stepName: string,
  nodeLabel: string,
  finalizeOutput: string,
  finalizeTokens?: TokenUsage
): Promise<void> {
  // Impossible by construction today (the gate writes signaledOutput whenever
  // completionSignaled is true) — this warn guards a future decoupling so a
  // finalize that silently loses the iteration output is diagnosable.
  if (finalizeOutput === '') {
    getLog().warn(
      { workflowRunId: workflowRun.id, nodeId },
      'loop_node.finalize_missing_signaled_output'
    );
  }
  await safeSendMessage(
    platform,
    conversationId,
    `${nodeLabel} '${nodeId}' accepted at the completion signal (no re-run)`,
    { workflowId: workflowRun.id, nodeName: nodeId }
  );
  await deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'node_completed',
      step_name: stepName,
      data: {
        duration_ms: 0,
        node_output: finalizeOutput,
        ...(finalizeTokens !== undefined ? { tokens: finalizeTokens } : {}),
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: workflowRun.id, eventType: 'node_completed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_completed',
    runId: workflowRun.id,
    nodeId,
    nodeName: nodeId,
    duration: 0,
  });
}

interface LoopGroupExecutionContext {
  deps: WorkflowDeps;
  platform: IWorkflowPlatform;
  conversationId: string;
  cwd: string;
  workflowRun: WorkflowRun;
  node: LoopGroupNode;
  workflowProvider: string;
  workflowModel: string | undefined;
  workflowLevelOptions: WorkflowLevelOptions;
  aiProfile: ResolvedAiProfile | undefined;
  workflowPreset: ModelAliasPreset | undefined;
  artifactsDir: string;
  stateDir: string;
  logDir: string;
  baseBranch: string;
  docsDir: string;
  outerNodeOutputs: Map<string, NodeOutput>;
  config: WorkflowConfig;
  issueContext?: string;
  stepNamePrefix: string;
  execContext: ExecutionContext;
  runChildWorkflow?: RunChildWorkflowFn;
  workflowDigest: string;
  budget?: ActiveWorkflowBudget;
  budgetBaseUsage: { input: number; output: number };
  stepName: string;
  bodyStepNamePrefix: string;
  msgContext: SendMessageContext;
  knownBodyIds: Set<string>;
  directBodyIds: Set<string>;
  isLoopResume: boolean;
  startIteration: number;
  loopUserInput: string;
}

interface LoopGroupState {
  loopPrevOutputs: Map<string, NodeOutput> | undefined;
  lastIterationOutput: string;
  loopTotalCostUsd: number | undefined;
  loopTotalTokens: TokenUsage | undefined;
  loopLastSequentialSession: SequentialSessionCursor | undefined;
}

async function finalizeLoopGroupResumeSignal(
  ctx: LoopGroupExecutionContext,
  loopGateMeta: ApprovalContext | undefined,
  feedbackGiven: boolean
): Promise<NodeExecutionResult | undefined> {
  if (!ctx.isLoopResume || loopGateMeta?.completionSignaled !== true || feedbackGiven)
    return undefined;
  const finalizeOutput = loopGateMeta.signaledOutput ?? '';
  await finalizeLoopFromSignal(
    ctx.deps,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun,
    ctx.node.id,
    ctx.stepName,
    'Loop-group node',
    finalizeOutput
  );
  return { state: 'completed', output: finalizeOutput };
}

function createLoopGroupState(
  ctx: LoopGroupExecutionContext,
  loopGateMeta: ApprovalContext | undefined
): LoopGroupState {
  const cursor =
    ctx.isLoopResume &&
    typeof loopGateMeta?.sessionId === 'string' &&
    typeof loopGateMeta.sessionProvider === 'string'
      ? { sessionId: loopGateMeta.sessionId, provider: loopGateMeta.sessionProvider }
      : undefined;
  return {
    loopPrevOutputs: undefined,
    lastIterationOutput: '',
    loopTotalCostUsd: undefined,
    loopTotalTokens: undefined,
    loopLastSequentialSession: cursor,
  };
}

async function guardLoopGroupIterationStatus(
  ctx: LoopGroupExecutionContext,
  iteration: number
): Promise<NodeExecutionResult | undefined> {
  const runStatus = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
  if (shouldContinueStreamingForStatus(runStatus)) return undefined;
  const effectiveStatus = runStatus ?? 'deleted';
  getLog().info(
    { workflowRunId: ctx.workflowRun.id, nodeId: ctx.node.id, iteration, status: effectiveStatus },
    'loop_group_node.stop_detected'
  );
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `Loop-group node '${ctx.node.id}' stopped at iteration ${String(iteration)} (${effectiveStatus})`,
    ctx.msgContext
  );
  return { state: 'failed', output: '', error: `Workflow ${effectiveStatus}` };
}

function emitLoopGroupIterationStarted(ctx: LoopGroupExecutionContext, iteration: number): void {
  getWorkflowEventEmitter().emit({
    type: 'loop_iteration_started',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    iteration,
    maxIterations: ctx.node.loop_group.max_iterations,
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'loop_iteration_started',
      step_name: ctx.stepName,
      data: { iteration, maxIterations: ctx.node.loop_group.max_iterations, nodeId: ctx.node.id },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, nodeId: ctx.node.id, iteration },
        'loop_group_node.iteration_event_failed'
      );
    });
}

function buildLoopGroupIterationNodes(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iteration: number
): { iterBodyNodes: DagNode[]; userInputForIter: string } {
  const userInputForIter =
    ctx.isLoopResume && iteration === ctx.startIteration ? ctx.loopUserInput : '';
  const iterBodyNodes = ctx.node.loop_group.nodes.map(n =>
    applyLoopPrevToBodyNode(
      n,
      state.loopPrevOutputs,
      userInputForIter,
      ctx.logDir,
      ctx.knownBodyIds,
      ctx.directBodyIds
    )
  );
  return { iterBodyNodes, userInputForIter };
}

function buildLoopGroupRunContext(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iterBodyLayers: DagNode[][],
  scopedNodeOutputs: Map<string, NodeOutput>,
  iteration: number,
  userInputForIter: string
): RunLayersContext {
  return {
    deps: ctx.deps,
    platform: ctx.platform,
    conversationId: ctx.conversationId,
    cwd: ctx.cwd,
    runChildWorkflow: ctx.runChildWorkflow,
    workflowRun: ctx.workflowRun,
    workflowName: ctx.node.id,
    workflowDigest: ctx.workflowDigest,
    config: ctx.config,
    workflowProvider: ctx.workflowProvider,
    workflowModel: ctx.workflowModel,
    workflowLevelOptions: ctx.workflowLevelOptions,
    aiProfile: ctx.aiProfile,
    workflowPreset: ctx.workflowPreset,
    artifactsDir: ctx.artifactsDir,
    stateDir: ctx.stateDir,
    logDir: ctx.logDir,
    baseBranch: ctx.baseBranch,
    docsDir: ctx.docsDir,
    configuredCommandFolder: undefined,
    issueContext: ctx.issueContext,
    execContext: ctx.execContext,
    persistScopeKey: undefined,
    workflowPersistSessions: false,
    scopeArtifactsDir: undefined,
    layers: iterBodyLayers,
    nodeOutputs: scopedNodeOutputs,
    priorCompletedNodes: undefined,
    lastSequentialSession:
      ctx.node.loop_group.fresh_context || iteration === 1
        ? undefined
        : state.loopLastSequentialSession,
    totalCostUsd: 0,
    totalTokensIn: 0,
    totalTokensOut: 0,
    totalLoopIterations: 0,
    budget: ctx.budget,
    budgetBaseUsage: {
      input: ctx.budgetBaseUsage.input + (state.loopTotalTokens?.input ?? 0),
      output: ctx.budgetBaseUsage.output + (state.loopTotalTokens?.output ?? 0),
    },
    stepNamePrefix: ctx.bodyStepNamePrefix,
    iteration,
    bodyLoopUserInput: userInputForIter,
  };
}

async function guardLoopGroupPostBodyStatus(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iteration: number
): Promise<NodeExecutionResult | undefined> {
  const postBodyStatus = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
  if (shouldContinueStreamingForStatus(postBodyStatus)) return undefined;
  const effectiveStatus = postBodyStatus ?? 'deleted';
  getLog().info(
    { workflowRunId: ctx.workflowRun.id, nodeId: ctx.node.id, iteration, status: effectiveStatus },
    'loop_group_node.post_body_stop'
  );
  return {
    state: 'failed',
    output: state.lastIterationOutput,
    error: `Workflow ${effectiveStatus}`,
  };
}

function mergeLoopGroupIterationUsage(state: LoopGroupState, iterCtx: RunLayersContext): void {
  state.loopTotalCostUsd = (state.loopTotalCostUsd ?? 0) + iterCtx.totalCostUsd;
  if (iterCtx.totalTokensIn > 0 || iterCtx.totalTokensOut > 0) {
    state.loopTotalTokens = {
      input: (state.loopTotalTokens?.input ?? 0) + iterCtx.totalTokensIn,
      output: (state.loopTotalTokens?.output ?? 0) + iterCtx.totalTokensOut,
    };
  }
}

function failedLoopGroupBodyNodes(
  iterBodyNodes: DagNode[],
  scopedNodeOutputs: Map<string, NodeOutput>
): string[] {
  return iterBodyNodes.flatMap(n => {
    const output = scopedNodeOutputs.get(n.id);
    return output?.state === 'failed' ? [`'${n.id}': ${output.error}`] : [];
  });
}

async function maybeFailLoopGroupForBody(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iterBodyNodes: DagNode[],
  scopedNodeOutputs: Map<string, NodeOutput>,
  iteration: number
): Promise<NodeExecutionResult | undefined> {
  const failedBodyNodes = failedLoopGroupBodyNodes(iterBodyNodes, scopedNodeOutputs);
  if (failedBodyNodes.length === 0) return undefined;
  const errorMsg = `Loop-group node '${ctx.node.id}' failed at iteration ${String(iteration)}: ${failedBodyNodes.join('; ')}`;
  getLog().warn(
    { nodeId: ctx.node.id, iteration, failedCount: failedBodyNodes.length },
    'loop_group_node.body_node_failed'
  );
  await safeSendMessage(ctx.platform, ctx.conversationId, errorMsg, ctx.msgContext);
  return {
    state: 'failed',
    output: state.lastIterationOutput,
    error: errorMsg,
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iteration,
  };
}

function updateLoopGroupIterationOutput(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iterBodyNodes: DagNode[],
  scopedNodeOutputs: Map<string, NodeOutput>
): { iterationOutput: string; prevIterationOutput: string; signalDetected: boolean } {
  const allDeps = new Set(iterBodyNodes.flatMap(n => n.depends_on ?? []));
  const terminalOutput = iterBodyNodes
    .filter(n => !allDeps.has(n.id))
    .map(n => scopedNodeOutputs.get(n.id))
    .find(o => o?.state === 'completed' && o.output.trim().length > 0)?.output;
  const iterationOutput = terminalOutput ?? '';
  const prevIterationOutput = state.lastIterationOutput;
  state.lastIterationOutput = stripCompletionTags(iterationOutput, ctx.node.loop_group.until);
  return {
    iterationOutput,
    prevIterationOutput,
    signalDetected: detectCompletionSignal(iterationOutput, ctx.node.loop_group.until),
  };
}

async function runLoopGroupUntilBash(
  ctx: LoopGroupExecutionContext,
  scopedNodeOutputs: Map<string, NodeOutput>,
  iteration: number,
  prevIterationOutput: string,
  signalDetected: boolean
): Promise<boolean> {
  const group = ctx.node.loop_group;
  if (!group.until_bash || signalDetected) return false;
  const groupBashPath = resolveBashPath();
  try {
    const { prompt: bashPrompt } = substituteWorkflowVariables(
      group.until_bash,
      ctx.workflowRun.id,
      ctx.workflowRun.user_message,
      ctx.artifactsDir,
      ctx.baseBranch,
      ctx.docsDir,
      ctx.issueContext,
      iteration === ctx.startIteration ? ctx.loopUserInput : undefined,
      undefined,
      undefined,
      { shellSafe: true, stateDir: ctx.stateDir, execContext: ctx.execContext }
    );
    const substitutedBash = substituteNodeOutputRefs(
      bashPrompt,
      scopedNodeOutputs,
      true,
      ctx.logDir
    );
    await runSubprocess(ctx.execContext, groupBashPath, ['-c', substitutedBash], {
      cwd: ctx.cwd,
      timeout: SUBPROCESS_DEFAULT_TIMEOUT,
      deadlineAt: ctx.budget?.deadlineAtMs,
      getRunStatus: () => ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id),
      env: {
        ...(ctx.config.envVars ?? {}),
        USER_MESSAGE: ctx.workflowRun.user_message,
        ARGUMENTS: ctx.workflowRun.user_message,
        LOOP_USER_INPUT: iteration === ctx.startIteration ? (ctx.loopUserInput ?? '') : '',
        LOOP_PREV_OUTPUT: prevIterationOutput,
        REJECTION_REASON: '',
        CONTEXT: ctx.issueContext ?? '',
        EXTERNAL_CONTEXT: ctx.issueContext ?? '',
        ISSUE_CONTEXT: ctx.issueContext ?? '',
      },
    });
    return true;
  } catch (e) {
    const bashErr = e as NodeJS.ErrnoException;
    if (bashErr.code === 'ENOENT' || bashErr.code === 'EACCES' || bashErr.code === 'ENOTDIR') {
      getLog().error(
        { err: bashErr, nodeId: ctx.node.id, iteration },
        'loop_group.until_bash_failed'
      );
      throw new Error(
        `Loop group '${ctx.node.id}' until_bash failed: cannot execute bash at '${groupBashPath}' (${bashErr.code}). Set ARCHON_BASH_PATH if Git Bash is installed elsewhere.`
      );
    }
    if (typeof bashErr.code !== 'number') {
      getLog().error(
        { err: bashErr, nodeId: ctx.node.id, iteration },
        'loop_group.until_bash_unexpected_error'
      );
      throw bashErr;
    }
    return false;
  }
}

function emitLoopGroupIterationCompleted(
  ctx: LoopGroupExecutionContext,
  iteration: number,
  duration: number,
  completionDetected: boolean
): void {
  getWorkflowEventEmitter().emit({
    type: 'loop_iteration_completed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    iteration,
    duration,
    completionDetected,
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'loop_iteration_completed',
      step_name: ctx.stepName,
      data: { iteration, duration, completionDetected, nodeId: ctx.node.id },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, nodeId: ctx.node.id, iteration },
        'loop_group_node.iteration_event_failed'
      );
    });
}

async function maybeCompleteLoopGroup(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iteration: number,
  duration: number,
  completionDetected: boolean
): Promise<NodeExecutionResult | undefined> {
  const group = ctx.node.loop_group;
  const interactiveFirstRun = group.interactive && !ctx.isLoopResume;
  if (!completionDetected || (interactiveFirstRun && group.signal_completes !== true))
    return undefined;
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `Loop-group node '${ctx.node.id}' completed after ${String(iteration)} iteration${iteration > 1 ? 's' : ''}`,
    ctx.msgContext
  );
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_completed',
      step_name: ctx.stepName,
      data: {
        duration_ms: duration,
        node_output: state.lastIterationOutput,
        ...(state.loopTotalCostUsd !== undefined ? { cost_usd: state.loopTotalCostUsd } : {}),
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_completed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_completed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.id,
    duration,
    ...(state.loopTotalCostUsd !== undefined ? { costUsd: state.loopTotalCostUsd } : {}),
  });
  return {
    state: 'completed',
    output: state.lastIterationOutput,
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iteration,
  };
}

async function maybePauseLoopGroupGate(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iteration: number,
  completionDetected: boolean
): Promise<NodeExecutionResult | undefined> {
  const group = ctx.node.loop_group;
  if (!group.interactive || !group.gate_message) return undefined;
  const honestMessage = buildHonestGateMessage(
    completionDetected,
    group.until,
    state.lastIterationOutput,
    group.gate_message
  );
  const gateMsg = `⏸ **Input required** (loop_group \`${ctx.node.id}\`, iteration ${String(iteration)}): ${honestMessage}\n\nRun ID: \`${ctx.workflowRun.id}\`\nRespond: \`/workflow approve ${ctx.workflowRun.id} <your feedback>\` | Cancel: \`/workflow reject ${ctx.workflowRun.id}\``;
  const gateSent = await safeSendMessage(ctx.platform, ctx.conversationId, gateMsg, {
    workflowId: ctx.workflowRun.id,
    nodeName: ctx.node.id,
  });
  if (!gateSent)
    return {
      state: 'failed',
      output: state.lastIterationOutput,
      error: `Loop-group gate message failed to deliver for node '${ctx.node.id}' — cannot pause safely`,
    };
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'approval_requested',
      step_name: ctx.stepName,
      data: { message: honestMessage, iteration, completionSignaled: completionDetected },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, nodeId: ctx.node.id, iteration },
        'loop_group_node.iteration_event_failed'
      );
    });
  await pauseGateRespectingExternalTransition(ctx.deps, ctx.workflowRun.id, {
    nodeId: ctx.node.id,
    message: honestMessage,
    type: 'interactive_loop',
    iteration,
    sessionId: state.loopLastSequentialSession?.sessionId ?? null,
    sessionProvider: state.loopLastSequentialSession?.provider ?? null,
    completionSignaled: completionDetected,
    signaledOutput: completionDetected ? state.lastIterationOutput : null,
  });
  return {
    state: 'completed',
    output: state.lastIterationOutput,
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iteration,
  };
}

async function runLoopGroupIteration(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState,
  iteration: number
): Promise<NodeExecutionResult | undefined> {
  const iterationStart = Date.now();
  const stopped = await guardLoopGroupIterationStatus(ctx, iteration);
  if (stopped) return stopped;
  emitLoopGroupIterationStarted(ctx, iteration);
  const { iterBodyNodes, userInputForIter } = buildLoopGroupIterationNodes(ctx, state, iteration);
  const scopedNodeOutputs = new Map<string, NodeOutput>(ctx.outerNodeOutputs);
  const iterCtx = buildLoopGroupRunContext(
    ctx,
    state,
    buildTopologicalLayers(iterBodyNodes),
    scopedNodeOutputs,
    iteration,
    userInputForIter
  );
  await runLayers(iterCtx);
  const postBodyStop = await guardLoopGroupPostBodyStatus(ctx, state, iteration);
  if (postBodyStop) return postBodyStop;
  mergeLoopGroupIterationUsage(state, iterCtx);
  const bodyFailure = await maybeFailLoopGroupForBody(
    ctx,
    state,
    iterBodyNodes,
    scopedNodeOutputs,
    iteration
  );
  if (bodyFailure) return bodyFailure;
  state.loopLastSequentialSession = iterCtx.lastSequentialSession;
  state.loopPrevOutputs = new Map(scopedNodeOutputs);
  const { prevIterationOutput, signalDetected } = updateLoopGroupIterationOutput(
    ctx,
    state,
    iterBodyNodes,
    scopedNodeOutputs
  );
  const bashComplete = await runLoopGroupUntilBash(
    ctx,
    scopedNodeOutputs,
    iteration,
    prevIterationOutput,
    signalDetected
  );
  const duration = Date.now() - iterationStart;
  const completionDetected = signalDetected || bashComplete;
  emitLoopGroupIterationCompleted(ctx, iteration, duration, completionDetected);
  return (
    (await maybeCompleteLoopGroup(ctx, state, iteration, duration, completionDetected)) ??
    (await maybePauseLoopGroupGate(ctx, state, iteration, completionDetected))
  );
}

async function loopGroupMaxIterationsResult(
  ctx: LoopGroupExecutionContext,
  state: LoopGroupState
): Promise<NodeExecutionResult> {
  const group = ctx.node.loop_group;
  const errorMsg = `Loop-group node '${ctx.node.id}' exceeded max iterations (${String(group.max_iterations)}) without completion signal '${group.until}'`;
  getLog().warn(
    { nodeId: ctx.node.id, maxIterations: group.max_iterations, signal: group.until },
    'loop_group_node.max_iterations_reached'
  );
  await safeSendMessage(ctx.platform, ctx.conversationId, errorMsg, ctx.msgContext);
  return {
    state: 'failed',
    output: state.lastIterationOutput,
    error: errorMsg,
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: group.max_iterations,
  };
}

/**
 * Execute a loop-group node — runs a multi-node sub-DAG body repeatedly until a
 * completion condition (`until` signal in the body's terminal-node output, and/or
 * `until_bash` exit code) or `max_iterations`.
 *
 * Mirrors {@link executeLoopNode} at subgraph granularity: each iteration runs the body's
 * topological layers via {@link runLayers} against a fresh scoped `nodeOutputs` map. The
 * body is a sealed sub-DAG. Every persisted body event — both runLayers' own control
 * events (skip/trigger_rule/when) AND the node executors' lifecycle events
 * (node_started/node_completed/node_failed, and tool/task/hook activity) — is namespaced
 * `{groupId}.{nodeId}` via `stepNamePrefix`, composing across nested groups; body node
 * lifecycle rows also carry the current `iteration` in their `data` (#2090). The in-process
 * emitter payloads stay raw (unprefixed nodeId) so live SSE/CLI consumers are unaffected.
 * `$LOOP_PREV.<id>.output` refs in body prompts resolve against a snapshot of the
 * *previous* iteration's body outputs (empty on iteration 1).
 *
 * `$groupId.output` (visible to the outer DAG) = the final iteration's terminal-node output
 * (mirrors the top-level run's terminal-output selection).
 *
 * Key behaviors:
 * - Returns NodeExecutionResult (not void) — the outer DAG executor owns run lifecycle
 * - Loop is encapsulated inside this one node; the outer DAG stays acyclic
 * - Usage (cost/tokens) is summed across iterations and returned on the final result,
 *   so the outer `runLayers` aggregates the group as one node's worth of usage
 */
async function executeLoopGroupNode(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflowRun: WorkflowRun,
  node: LoopGroupNode,
  workflowProvider: string,
  workflowModel: string | undefined,
  workflowLevelOptions: WorkflowLevelOptions,
  aiProfile: ResolvedAiProfile | undefined,
  workflowPreset: ModelAliasPreset | undefined,
  artifactsDir: string,
  stateDir: string,
  logDir: string,
  baseBranch: string,
  docsDir: string,
  outerNodeOutputs: Map<string, NodeOutput>,
  config: WorkflowConfig,
  issueContext?: string,
  stepNamePrefix = '',
  execContext: ExecutionContext = { kind: 'host' },
  runChildWorkflow?: RunChildWorkflowFn,
  workflowDigest = '',
  budget?: ActiveWorkflowBudget,
  budgetBaseUsage: { input: number; output: number } = { input: 0, output: 0 }
): Promise<NodeExecutionResult> {
  const group = node.loop_group;
  const stepName = stepNamePrefix + node.id;
  const rawApproval = workflowRun.metadata?.approval;
  const loopGateMeta = isApprovalContext(rawApproval) ? rawApproval : undefined;
  const isLoopResume = loopGateMeta?.type === 'interactive_loop' && loopGateMeta.nodeId === node.id;
  const loopGateRunMeta = (workflowRun.metadata ?? {}) as LoopGateRunMetadata;
  const ctx: LoopGroupExecutionContext = {
    deps,
    platform,
    conversationId,
    cwd,
    workflowRun,
    node,
    workflowProvider,
    workflowModel,
    workflowLevelOptions,
    aiProfile,
    workflowPreset,
    artifactsDir,
    stateDir,
    logDir,
    baseBranch,
    docsDir,
    outerNodeOutputs,
    config,
    issueContext,
    stepNamePrefix,
    execContext,
    runChildWorkflow,
    workflowDigest,
    budget,
    budgetBaseUsage,
    stepName,
    bodyStepNamePrefix: `${stepName}.`,
    msgContext: { workflowId: workflowRun.id, nodeName: node.id },
    knownBodyIds: collectLoopBodyNodeIds(group.nodes),
    directBodyIds: new Set(group.nodes.map(n => n.id)),
    isLoopResume,
    startIteration: isLoopResume ? (loopGateMeta.iteration ?? 0) + 1 : 1,
    loopUserInput: isLoopResume ? (loopGateRunMeta.loop_user_input ?? '') : '',
  };
  const finalized = await finalizeLoopGroupResumeSignal(
    ctx,
    loopGateMeta,
    loopGateRunMeta.loop_feedback_given === true
  );
  if (finalized) return finalized;
  const state = createLoopGroupState(ctx, loopGateMeta);

  for (let i = ctx.startIteration; i <= group.max_iterations; i++) {
    const result = await runLoopGroupIteration(ctx, state, i);
    if (result) return result;
  }
  return loopGroupMaxIterationsResult(ctx, state);
}

/**
 * Clone a body node with `$LOOP_PREV.<id>.output[.field]` refs and `$LOOP_USER_INPUT`
 * pre-substituted into every text field a body executor reads prompts from. Used by
 * {@link executeLoopGroupNode} so the sealed body sub-DAG's executors stay unaware of the
 * enclosing loop iteration (the body's own executors call substituteWorkflowVariables, but
 * that uses the run's user_message — not the loop's per-iteration user input — so
 * $LOOP_USER_INPUT must be resolved here, at the loop-group level).
 *
 * Only prompt-bearing fields are substituted in v1; `when:` conditions are NOT (they use
 * evaluateCondition, which does not call substituteLoopPrevRefs). Body authors who need
 * cross-iteration gating should branch on prompt content, not `when:`.
 *
 * `knownBodyIds` (transitive body-id set) and `directBodyIds` (this group's immediate body
 * ids) are threaded UNCHANGED into every substituteLoopPrevRefs call AND into the
 * nested-loop_group recursion — deliberately not recomputed for the inner group. This keeps
 * `$LOOP_PREV.*` refs validated against the OUTER loop's snapshot (whose body they resolve
 * against): a ref to an outer-direct id resolves now, a ref owned by a nested group is left
 * intact for that inner group's own pass, and a ref to nothing is a typo. Both omitted by
 * raw callers, which then skip the typo/nested classification entirely (fully lenient).
 */
export function applyLoopPrevToBodyNode(
  node: DagNode,
  loopPrevOutputs: Map<string, NodeOutput> | undefined,
  loopUserInput: string,
  outputFileDir?: string,
  knownBodyIds?: ReadonlySet<string>,
  directBodyIds?: ReadonlySet<string>
): DagNode {
  // Substitute $LOOP_USER_INPUT (user free-text) and $LOOP_PREV.* refs.
  // Resolve $LOOP_PREV FIRST, then splice $LOOP_USER_INPUT — so user input containing a
  // literal "$LOOP_PREV." is not itself reprocessed as a workflow-ref. `escapedForBash`
  // is true for shell-bound fields (bash/until_bash): $LOOP_PREV values are shell-quoted
  // (spilling to a file over the size threshold, same as substituteNodeOutputRefs), and
  // $LOOP_USER_INPUT is shell-quoted before splicing (user input is free-text; unquoted
  // it could break or inject into the bash command). Non-shell display/prompt fields
  // (prompt/approval.message/command, and cancel reasons) use the raw values.
  // `skipUserInput` is set ONLY for `script:` bodies: $LOOP_USER_INPUT is free-text that
  // cannot be safely quoted into TS/Python source, so it is left as a literal token here
  // and delivered to the script as a subprocess env var instead (#2115) — matching how
  // executeScriptNode delivers every other user-controlled variable. $LOOP_PREV.* refs
  // stay raw-spliced (bounded producer contract), routed through the knownBodyIds/
  // directBodyIds typo-vs-nested-vs-absent decision table (#2165).
  const sub = (s: string, escapedForBash = false, skipUserInput = false): string => {
    const prevResolved = substituteLoopPrevRefs(
      s,
      loopPrevOutputs,
      escapedForBash,
      outputFileDir,
      knownBodyIds,
      directBodyIds
    );
    if (skipUserInput) return prevResolved;
    const userInputForField = escapedForBash ? shellQuote(loopUserInput) : loopUserInput;
    return prevResolved.replace(/\$LOOP_USER_INPUT/g, userInputForField);
  };
  if (isLoopNode(node)) {
    // until_bash is shell-bound: an unresolved $LOOP_PREV would silently degrade to an
    // (empty) shell variable expansion inside bash -c.
    return {
      ...node,
      loop: {
        ...node.loop,
        // A command-backed loop has no inline prompt to substitute — its prompt text
        // is loaded from the command file inside executeLoopNode. Group-level
        // $LOOP_PREV.<bodyId>.output refs are resolved only in YAML fields (this
        // pass); they are not scanned inside command-file bodies.
        ...(node.loop.prompt !== undefined ? { prompt: sub(node.loop.prompt) } : {}),
        ...(node.loop.until_bash !== undefined
          ? { until_bash: sub(node.loop.until_bash, true) }
          : {}),
      },
    };
  }
  if (isLoopGroupNode(node)) {
    // Nested loop_group: recurse into the body. `knownBodyIds`/`directBodyIds` are the OUTER
    // group's sets, threaded UNCHANGED — so during this OUTER pass a ref to an inner-owned id
    // (in knownBodyIds but not directBodyIds) is left intact (return match) for the inner
    // group's own pass, while a ref to an OUTER-direct id resolves here at the outer
    // granularity and a true typo still throws. The inner group's own executeLoopGroupNode
    // computes fresh sets when it runs, so inner-owned refs resolve at the inner iteration
    // granularity. The inner group's until_bash is shell-bound and only ever substituted
    // here for OUTER-loop refs (a nested group's own until_bash cannot reference its own body
    // via $LOOP_PREV — executeLoopGroupNode does not re-run this pass on the group's until_bash).
    return {
      ...node,
      loop_group: {
        ...node.loop_group,
        ...(node.loop_group.until_bash !== undefined
          ? { until_bash: sub(node.loop_group.until_bash, true) }
          : {}),
        nodes: node.loop_group.nodes.map(n =>
          applyLoopPrevToBodyNode(
            n,
            loopPrevOutputs,
            loopUserInput,
            outputFileDir,
            knownBodyIds,
            directBodyIds
          )
        ),
      },
    };
  }
  if (isApprovalNode(node)) {
    return { ...node, approval: { ...node.approval, message: sub(node.approval.message) } };
  }
  if (isBashNode(node)) return { ...node, bash: sub(node.bash, true) };
  // Scripts never pass through a shell (execFile argv) — bash-quoting would inject
  // literal quote artifacts into TS/Python source. $LOOP_PREV.* refs are spliced raw
  // (mirroring executeScriptNode's substituteNodeOutputRefs(..., false)); $LOOP_USER_INPUT
  // is skipped here (skipUserInput) and delivered via env by executeScriptNode (#2115).
  if (isScriptNode(node)) return { ...node, script: sub(node.script, false, true) };
  // Cancel reason is display text, never executed — mirrors the normal-path default.
  if (isCancelNode(node)) return { ...node, cancel: sub(node.cancel) };
  if (isControllerActionNode(node)) return node;
  if ('command' in node && typeof node.command === 'string')
    return { ...node, command: sub(node.command) };
  if ('prompt' in node && typeof node.prompt === 'string')
    return { ...node, prompt: sub(node.prompt) };
  return node;
}

/**
 * Execute a loop node — runs prompt repeatedly until completion signal or max iterations.
 *
 * Key behaviors:
 * - Returns NodeExecutionResult (not void) — DAG executor owns workflow lifecycle
 * - Receives upstream node outputs for $nodeId.output substitution
 * - Does not write current_step_index (DAG tracks per-node completion)
 */
interface LoopNodeExecutionContext {
  deps: WorkflowDeps;
  platform: IWorkflowPlatform;
  conversationId: string;
  cwd: string;
  workflowRun: WorkflowRun;
  node: LoopNode;
  workflowProvider: string;
  resolvedOptions: SendQueryOptions | undefined;
  artifactsDir: string;
  stateDir: string;
  logDir: string;
  baseBranch: string;
  docsDir: string;
  nodeOutputs: Map<string, NodeOutput>;
  config: WorkflowConfig;
  issueContext?: string;
  configuredCommandFolder?: string;
  stepNamePrefix: string;
  execContext: ExecutionContext;
  resolvedModel?: string;
  resolvedTier?: TierName;
  resolvedEffort?: string;
  budget?: ActiveWorkflowBudget;
  budgetBaseUsage: { input: number; output: number };
  containerCtx?: ContainerRunContext;
  msgContext: SendMessageContext;
  stepName: string;
  loopGateMeta: ApprovalContext | undefined;
  isLoopResume: boolean;
  startIteration: number;
  loopUserInput: string;
  hardenedLoopRequiresLedger: boolean;
}

interface LoopNodeState {
  currentSessionId: string | undefined;
  promptTemplate: string;
  aiClient: ReturnType<WorkflowDeps['getAgentProvider']>;
  lastIterationOutput: string;
  lastIterationStructuredOutput: unknown;
  loopTotalCostUsd: number | undefined;
  loopFinalStopReason: string | undefined;
  loopTotalNumTurns: number | undefined;
  loopTotalTokens: TokenUsage | undefined;
  loopResolvedModel: ResolvedModel | undefined;
  loopBackgroundTasksIncomplete: Set<string>;
}

interface LoopNodeFailureExtras {
  output?: string;
  costUsd?: number;
  tokens?: TokenUsage;
  loopIterations?: number;
  data?: Record<string, unknown>;
}

interface LoopIterationState {
  iteration: number;
  startedAt: number;
  fullOutput: string;
  cleanOutput: string;
  idleTimedOut: boolean;
  abortController: AbortController;
  lastStreamStatusCheckAt: number;
  streamStopStatus: string | undefined;
  backgroundTasks: ReturnType<typeof createBackgroundTaskTracker>;
  iterationCost: number | undefined;
  iterationTokens: TokenUsage | undefined;
  iterationNumTurns: number | undefined;
  usageFolded: boolean;
  budgetTimer: ReturnType<typeof setTimeout> | undefined;
  runningTools: Map<string, RunningTool>;
  anonymousToolSequence: number;
  lastAnonymousToolCallId: string | undefined;
}

async function failLoopNode(
  ctx: LoopNodeExecutionContext,
  error: string,
  extras: LoopNodeFailureExtras = {}
): Promise<NodeExecutionResult> {
  getLog().error({ nodeId: ctx.node.id, error, ...(extras.data ?? {}) }, 'loop_node.failed');
  await logNodeError(ctx.logDir, ctx.workflowRun.id, ctx.node.id, error);
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_failed',
      step_name: ctx.stepName,
      data: { error, ...(extras.data ?? {}) },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_failed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.id,
    error,
  });
  return {
    state: 'failed',
    output: extras.output ?? '',
    error,
    ...(extras.costUsd !== undefined ? { costUsd: extras.costUsd } : {}),
    ...(extras.tokens !== undefined ? { tokens: extras.tokens } : {}),
    ...(extras.loopIterations !== undefined ? { loopIterations: extras.loopIterations } : {}),
  };
}

function emitLoopNodeStarted(ctx: LoopNodeExecutionContext): void {
  const loop = ctx.node.loop;
  getLog().info({ nodeId: ctx.node.id, type: 'loop' }, 'loop_node.started');
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_started',
      step_name: ctx.stepName,
      data: {
        type: 'loop',
        command: loop.command ?? null,
        provider: ctx.workflowProvider,
        model: ctx.resolvedModel,
        tier: ctx.resolvedTier,
        ...(ctx.resolvedEffort !== undefined ? { effort: ctx.resolvedEffort } : {}),
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_started' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_started',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.id,
    provider: ctx.workflowProvider,
    model: ctx.resolvedModel,
    tier: ctx.resolvedTier,
    ...(ctx.resolvedEffort !== undefined ? { effort: ctx.resolvedEffort } : {}),
  });
}

async function maybeFinalizeLoopNodeResume(
  ctx: LoopNodeExecutionContext
): Promise<NodeExecutionResult | undefined> {
  const loopGateRunMeta = (ctx.workflowRun.metadata ?? {}) as LoopGateRunMetadata;
  if (
    !ctx.isLoopResume ||
    ctx.loopGateMeta?.completionSignaled !== true ||
    loopGateRunMeta.loop_feedback_given === true
  )
    return undefined;
  const finalizeOutput = ctx.loopGateMeta.signaledOutput ?? '';
  await finalizeLoopFromSignal(
    ctx.deps,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun,
    ctx.node.id,
    ctx.stepName,
    'Loop node',
    finalizeOutput,
    readSignaledTokens(ctx.loopGateMeta.signaledTokens, {
      workflowRunId: ctx.workflowRun.id,
      nodeId: ctx.node.id,
    })
  );
  return {
    state: 'completed',
    output: finalizeOutput,
    sessionId: ctx.loopGateMeta.sessionId ?? undefined,
  };
}

async function resolveLoopPromptTemplate(
  ctx: LoopNodeExecutionContext
): Promise<string | NodeExecutionResult> {
  const loop = ctx.node.loop;
  if (typeof loop.prompt === 'string') return loop.prompt;
  if (typeof loop.command !== 'string')
    throw new Error(
      `Loop node '${ctx.node.id}' has neither 'loop.prompt' nor 'loop.command' — schema invariant violated`
    );
  if (ctx.isLoopResume && typeof ctx.loopGateMeta?.commandSnapshot === 'string')
    return ctx.loopGateMeta.commandSnapshot;
  const promptResult = await loadCommandPrompt(
    ctx.deps,
    ctx.cwd,
    loop.command,
    ctx.configuredCommandFolder
  );
  if (promptResult.success) return promptResult.content;
  getLog().error(
    { nodeId: ctx.node.id, command: loop.command, error: promptResult.message },
    'loop_node.command_load_failed'
  );
  return failLoopNode(ctx, promptResult.message, { data: { command: loop.command } });
}

function resolveLoopAiClient(
  ctx: LoopNodeExecutionContext
): ReturnType<WorkflowDeps['getAgentProvider']> | Error {
  try {
    return ctx.deps.getAgentProvider(ctx.workflowProvider);
  } catch (error) {
    const err = error as Error;
    const errorMsg = `Invalid provider '${ctx.workflowProvider}' for loop node '${ctx.node.id}'. Check workflow YAML or .archon/config.yaml. Original: ${err.message}`;
    getLog().error(
      { err, nodeId: ctx.node.id, provider: ctx.workflowProvider },
      'loop_node.provider_failed'
    );
    return new Error(errorMsg);
  }
}

function createLoopNodeState(
  ctx: LoopNodeExecutionContext,
  promptTemplate: string,
  aiClient: ReturnType<WorkflowDeps['getAgentProvider']>
): LoopNodeState {
  return {
    currentSessionId: ctx.isLoopResume ? (ctx.loopGateMeta?.sessionId ?? undefined) : undefined,
    promptTemplate,
    aiClient,
    lastIterationOutput: '',
    lastIterationStructuredOutput: undefined,
    loopTotalCostUsd: undefined,
    loopFinalStopReason: undefined,
    loopTotalNumTurns: undefined,
    loopTotalTokens: undefined,
    loopResolvedModel: undefined,
    loopBackgroundTasksIncomplete: new Set<string>(),
  };
}

function logLoopEventStoreError(
  ctx: LoopNodeExecutionContext,
  err: Error,
  iteration: number
): void {
  getLog().error({ err, nodeId: ctx.node.id, iteration }, 'loop_node.iteration_event_failed');
}

async function guardLoopNodeIterationStatus(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iteration: number
): Promise<NodeExecutionResult | undefined> {
  assertWorkflowBudgetCanContinue(
    ctx.budget,
    `Loop '${ctx.node.id}' iteration ${String(iteration)}`,
    {
      input: ctx.budgetBaseUsage.input + (state.loopTotalTokens?.input ?? 0),
      output: ctx.budgetBaseUsage.output + (state.loopTotalTokens?.output ?? 0),
    }
  );
  const runStatus = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
  if (shouldContinueStreamingForStatus(runStatus)) return undefined;
  const effectiveStatus = runStatus ?? 'deleted';
  getLog().info(
    { workflowRunId: ctx.workflowRun.id, nodeId: ctx.node.id, iteration, status: effectiveStatus },
    'loop_node.stop_detected'
  );
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `Loop node '${ctx.node.id}' stopped at iteration ${String(iteration)} (${effectiveStatus})`,
    ctx.msgContext
  );
  return failLoopNode(ctx, `Workflow ${effectiveStatus}`, {
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iteration - 1,
    data: { status: effectiveStatus, iteration },
  });
}

function emitLoopNodeIterationStarted(ctx: LoopNodeExecutionContext, iteration: number): void {
  getWorkflowEventEmitter().emit({
    type: 'loop_iteration_started',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    iteration,
    maxIterations: ctx.node.loop.max_iterations,
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'loop_iteration_started',
      step_name: ctx.stepName,
      data: { iteration, maxIterations: ctx.node.loop.max_iterations, nodeId: ctx.node.id },
    })
    .catch((err: Error) => {
      logLoopEventStoreError(ctx, err, iteration);
    });
}

function createLoopIterationState(iteration: number): LoopIterationState {
  const startedAt = Date.now();
  return {
    iteration,
    startedAt,
    fullOutput: '',
    cleanOutput: '',
    idleTimedOut: false,
    abortController: new AbortController(),
    lastStreamStatusCheckAt: startedAt,
    streamStopStatus: undefined,
    backgroundTasks: createBackgroundTaskTracker(),
    iterationCost: undefined,
    iterationTokens: undefined,
    iterationNumTurns: undefined,
    usageFolded: false,
    budgetTimer: undefined,
    runningTools: new Map<string, RunningTool>(),
    anonymousToolSequence: 0,
    lastAnonymousToolCallId: undefined,
  };
}

function buildLoopIterationPrompt(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState
): string {
  const { prompt: substitutedPrompt } = substituteWorkflowVariables(
    state.promptTemplate,
    ctx.workflowRun.id,
    ctx.workflowRun.user_message,
    ctx.artifactsDir,
    ctx.baseBranch,
    ctx.docsDir,
    ctx.issueContext,
    iter.iteration === ctx.startIteration ? ctx.loopUserInput : '',
    undefined,
    iter.iteration === ctx.startIteration ? '' : state.lastIterationOutput,
    { stateDir: ctx.stateDir, execContext: ctx.execContext }
  );
  return substituteNodeOutputRefs(substitutedPrompt, ctx.nodeOutputs);
}

async function syncLoopIterationBudget(ctx: LoopNodeExecutionContext): Promise<void> {
  if (!ctx.budget && !ctx.hardenedLoopRequiresLedger) return;
  const ledgerStatus = await readRequiredVerifiedHardenedBudgetStatus(ctx.containerCtx);
  if (!ctx.budget) return;
  assertLedgerStatusMatchesBudget(ledgerStatus, ctx.budget);
  await persistWorkflowBudgetState(ctx.deps, ctx.workflowRun.id, ctx.budget, ledgerStatus.consumed);
  assertWorkflowBudgetCanContinue(
    ctx.budget,
    `Loop '${ctx.node.id}' iteration`,
    ledgerStatus.consumed
  );
}

function startLoopBudgetTimer(ctx: LoopNodeExecutionContext, iter: LoopIterationState): void {
  const budgetRemaining = remainingDeadlineMs(ctx.budget?.deadlineAtMs);
  if (budgetRemaining === 0) {
    iter.abortController.abort();
    throw new Error(
      `Loop '${ctx.node.id}' iteration ${String(iter.iteration)} exceeded hardened workflow deadline`
    );
  }
  iter.budgetTimer =
    budgetRemaining === undefined
      ? undefined
      : setTimeout(() => {
          iter.abortController.abort();
        }, budgetRemaining);
}

function foldLoopIterationUsage(state: LoopNodeState, iter: LoopIterationState): void {
  if (iter.usageFolded) return;
  iter.usageFolded = true;
  if (iter.iterationCost !== undefined)
    state.loopTotalCostUsd = (state.loopTotalCostUsd ?? 0) + iter.iterationCost;
  if (iter.iterationTokens !== undefined) {
    state.loopTotalTokens = {
      input: (state.loopTotalTokens?.input ?? 0) + iter.iterationTokens.input,
      output: (state.loopTotalTokens?.output ?? 0) + iter.iterationTokens.output,
    };
  }
  if (iter.iterationNumTurns !== undefined)
    state.loopTotalNumTurns = (state.loopTotalNumTurns ?? 0) + iter.iterationNumTurns;
}

async function maybeAbortLoopStreamForStatus(
  ctx: LoopNodeExecutionContext,
  iter: LoopIterationState
): Promise<boolean> {
  const tickNow = Date.now();
  if (tickNow - iter.lastStreamStatusCheckAt <= CANCEL_CHECK_INTERVAL_MS) return false;
  iter.lastStreamStatusCheckAt = tickNow;
  try {
    const streamStatus = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
    if (shouldContinueStreamingForStatus(streamStatus)) return false;
    iter.streamStopStatus = streamStatus ?? 'deleted';
    getLog().info(
      {
        workflowRunId: ctx.workflowRun.id,
        nodeId: ctx.node.id,
        iteration: iter.iteration,
        status: iter.streamStopStatus,
      },
      'loop_node.stop_detected_during_streaming'
    );
    iter.abortController.abort();
    return true;
  } catch (statusErr) {
    getLog().warn(
      { err: statusErr as Error, workflowRunId: ctx.workflowRun.id, nodeId: ctx.node.id },
      'loop_node.status_check_failed'
    );
    return false;
  }
}

function closeLoopTool(
  ctx: LoopNodeExecutionContext,
  iter: LoopIterationState,
  toolCallId: string,
  tool: RunningTool,
  outcome: 'unknown' | 'success' | 'error' | 'interrupted' = 'unknown',
  exitCode?: number
): void {
  const now = Date.now();
  getWorkflowEventEmitter().emit({
    type: 'tool_completed',
    runId: ctx.workflowRun.id,
    toolName: tool.toolName,
    stepName: ctx.node.id,
    durationMs: now - tool.startedAt,
    toolCallId,
    toolOutcome: outcome,
    ...(exitCode !== undefined ? { exitCode } : {}),
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'tool_completed',
      step_name: ctx.stepName,
      data: {
        tool_name: tool.toolName,
        duration_ms: now - tool.startedAt,
        tool_call_id: toolCallId,
        tool_outcome: outcome,
        ...(exitCode !== undefined ? { exit_code: exitCode } : {}),
      },
    })
    .catch((err: Error) => {
      logLoopEventStoreError(ctx, err, iter.iteration);
    });
  iter.runningTools.delete(toolCallId);
  if (toolCallId === iter.lastAnonymousToolCallId) iter.lastAnonymousToolCallId = undefined;
}

function closeAllLoopTools(ctx: LoopNodeExecutionContext, iter: LoopIterationState): void {
  for (const [toolCallId, tool] of iter.runningTools) closeLoopTool(ctx, iter, toolCallId, tool);
}

async function handleLoopAssistantChunk(
  ctx: LoopNodeExecutionContext,
  iter: LoopIterationState,
  msg: Extract<MessageChunk, { type: 'assistant' }>
): Promise<void> {
  iter.fullOutput += msg.content;
  const cleaned = stripCompletionTags(msg.content, ctx.node.loop.until);
  iter.cleanOutput += cleaned;
  if (ctx.platform.getStreamingMode() === 'stream' && cleaned)
    await safeSendMessage(ctx.platform, ctx.conversationId, cleaned, ctx.msgContext);
  await logAssistant(ctx.logDir, ctx.workflowRun.id, msg.content);
}

function handleLoopResultUsage(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState,
  msg: Extract<MessageChunk, { type: 'result' }>
): void {
  if (msg.sessionId) state.currentSessionId = msg.sessionId;
  if (msg.cost !== undefined) iter.iterationCost = msg.cost;
  if (msg.tokens !== undefined) {
    if (Number.isFinite(msg.tokens.input) && Number.isFinite(msg.tokens.output))
      iter.iterationTokens = { input: msg.tokens.input, output: msg.tokens.output };
    else
      getLog().warn(
        { nodeId: ctx.node.id, iteration: iter.iteration, tokens: msg.tokens },
        'loop_node.usage_tokens_non_finite_ignored'
      );
  }
  if (msg.stopReason !== undefined) state.loopFinalStopReason = msg.stopReason;
  if (msg.numTurns !== undefined) iter.iterationNumTurns = msg.numTurns;
  state.loopResolvedModel = msg.resolvedModel;
  if (msg.structuredOutput !== undefined)
    state.lastIterationStructuredOutput = msg.structuredOutput;
}

async function handleLoopResultChunk(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState,
  msg: Extract<MessageChunk, { type: 'result' }>
): Promise<boolean> {
  closeAllLoopTools(ctx, iter);
  handleLoopResultUsage(ctx, state, iter, msg);
  if (msg.isError && msg.errorSubtype === 'error_max_budget_usd')
    throw new Error(
      `Loop node '${ctx.node.id}' iteration ${String(iter.iteration)} exceeded cost cap${ctx.resolvedOptions?.maxBudgetUsd !== undefined ? ` of $${ctx.resolvedOptions.maxBudgetUsd.toFixed(2)}` : ''}.`
    );
  if (msg.isError && msg.errorSubtype !== 'success') {
    const subtype = msg.errorSubtype ?? 'unknown';
    const errorsDetail = msg.errors?.length ? ` — ${msg.errors.join('; ')}` : '';
    getLog().error(
      {
        nodeId: ctx.node.id,
        iteration: iter.iteration,
        errorSubtype: subtype,
        errors: msg.errors,
        sessionId: msg.sessionId,
        stopReason: msg.stopReason,
        durationMs: Date.now() - iter.startedAt,
      },
      'loop_node.sdk_error_result'
    );
    throw new Error(
      `Loop node '${ctx.node.id}' iteration ${String(iter.iteration)} failed: SDK returned ${subtype}${errorsDetail}`
    );
  }
  if (iter.backgroundTasks.shouldBreakOnResult()) return false;
  getLog().warn(
    {
      nodeId: ctx.node.id,
      iteration: iter.iteration,
      taskCount: iter.backgroundTasks.count(),
      taskIds: iter.backgroundTasks.ids(),
    },
    'loop_node.result_with_live_background_tasks'
  );
  if (iter.backgroundTasks.shouldAnnounceWait())
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      `⏳ Loop \`${ctx.node.id}\` iteration ${String(iter.iteration)}: turn ended with ${String(iter.backgroundTasks.count())} background agent task(s) still running — waiting for them to finish.`,
      ctx.msgContext
    );
  return true;
}

async function handleLoopToolChunk(
  ctx: LoopNodeExecutionContext,
  iter: LoopIterationState,
  msg: Extract<MessageChunk, { type: 'tool' }>
): Promise<void> {
  const now = Date.now();
  const toolCallId = msg.toolCallId ?? `anonymous-${String(++iter.anonymousToolSequence)}`;
  const previousTool = iter.lastAnonymousToolCallId
    ? iter.runningTools.get(iter.lastAnonymousToolCallId)
    : undefined;
  if (previousTool && iter.lastAnonymousToolCallId !== undefined)
    closeLoopTool(ctx, iter, iter.lastAnonymousToolCallId, previousTool);
  iter.runningTools.set(toolCallId, { toolName: msg.toolName, startedAt: now });
  if (!msg.toolCallId) iter.lastAnonymousToolCallId = toolCallId;
  getWorkflowEventEmitter().emit({
    type: 'tool_started',
    runId: ctx.workflowRun.id,
    toolName: msg.toolName,
    stepName: ctx.node.id,
    toolCallId,
  });
  if (ctx.platform.getStreamingMode() === 'stream') {
    const toolMsg = formatToolCall(msg.toolName, msg.toolInput);
    if (toolMsg)
      await safeSendMessage(ctx.platform, ctx.conversationId, toolMsg, ctx.msgContext, {
        category: 'tool_call_formatted',
      } as WorkflowMessageMetadata);
    if (ctx.platform.sendStructuredEvent)
      await ctx.platform.sendStructuredEvent(ctx.conversationId, msg);
  }
  const toolInput = truncateToolInput(msg.toolInput);
  await logTool(ctx.logDir, ctx.workflowRun.id, msg.toolName, toolInput);
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'tool_called',
      step_name: ctx.stepName,
      data: { tool_name: msg.toolName, tool_input: toolInput, tool_call_id: toolCallId },
    })
    .catch((err: Error) => {
      logLoopEventStoreError(ctx, err, iter.iteration);
    });
}

function truncateToolInput(
  toolInput: Record<string, unknown> | undefined
): Record<string, unknown> {
  if (!toolInput) return {};
  return Object.fromEntries(
    Object.entries(toolInput).map(([k, v]) =>
      typeof v === 'string' && v.length > 500 ? [k, v.slice(0, 500) + '...'] : [k, v]
    )
  );
}

async function handleLoopToolResultChunk(
  ctx: LoopNodeExecutionContext,
  iter: LoopIterationState,
  msg: Extract<MessageChunk, { type: 'tool_result' }>
): Promise<void> {
  const completedTool = findRunningTool(iter.runningTools, msg.toolName, msg.toolCallId);
  if (completedTool)
    closeLoopTool(ctx, iter, completedTool[0], completedTool[1], msg.toolOutcome, msg.exitCode);
  if (ctx.platform.sendStructuredEvent)
    await ctx.platform.sendStructuredEvent(ctx.conversationId, msg);
}

async function handleLoopStreamChunk(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState,
  msg: MessageChunk
): Promise<boolean> {
  switch (msg.type) {
    case 'assistant':
      await handleLoopAssistantChunk(ctx, iter, msg);
      return true;
    case 'result':
      return handleLoopResultChunk(ctx, state, iter, msg);
    case 'background_tasks':
      iter.backgroundTasks.update(msg.tasks);
      return true;
    case 'tool':
      await handleLoopToolChunk(ctx, iter, msg);
      return true;
    case 'tool_result':
      await handleLoopToolResultChunk(ctx, iter, msg);
      return true;
    default:
      return true;
  }
}

async function streamLoopIteration(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState
): Promise<void> {
  const finalPrompt = buildLoopIterationPrompt(ctx, state, iter);
  await syncLoopIterationBudget(ctx);
  startLoopBudgetTimer(ctx, iter);
  const resumeSessionId =
    ctx.node.loop.fresh_context || iter.iteration === 1 ? undefined : state.currentSessionId;
  const iterationOptions: SendQueryOptions | undefined = {
    ...ctx.resolvedOptions,
    abortSignal: iter.abortController.signal,
  };
  const generator = state.aiClient.sendQuery(
    finalPrompt,
    ctx.cwd,
    resumeSessionId,
    iterationOptions
  );
  const effectiveIdleTimeout = ctx.node.idle_timeout ?? STEP_IDLE_TIMEOUT_MS;
  for await (const msg of withIdleTimeout(generator, effectiveIdleTimeout, () => {
    iter.idleTimedOut = true;
    getLog().warn(
      { nodeId: ctx.node.id, iteration: iter.iteration, timeoutMs: effectiveIdleTimeout },
      'loop_node.idle_timeout_reached'
    );
    iter.abortController.abort();
  })) {
    if (await maybeAbortLoopStreamForStatus(ctx, iter)) break;
    if (!(await handleLoopStreamChunk(ctx, state, iter, msg))) break;
  }
}

async function finalizeLoopIterationStream(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState
): Promise<NodeExecutionResult | undefined> {
  if (iter.budgetTimer !== undefined) clearTimeout(iter.budgetTimer);
  foldLoopIterationUsage(state, iter);
  await syncLoopIterationBudget(ctx);
  if (!iter.backgroundTasks.shouldBreakOnResult()) await warnLoopBackgroundTasks(ctx, state, iter);
  if (!iter.abortController.signal.aborted || iter.idleTimedOut) return undefined;
  const effectiveStatus = iter.streamStopStatus ?? 'cancelled';
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `Loop node '${ctx.node.id}' stopped during iteration ${String(iter.iteration)} (${effectiveStatus})`,
    ctx.msgContext
  );
  return failLoopNode(ctx, `Workflow ${effectiveStatus}`, {
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iter.iteration,
    data: { status: effectiveStatus, iteration: iter.iteration },
  });
}

async function warnLoopBackgroundTasks(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState
): Promise<void> {
  const danglingTaskIds = iter.backgroundTasks.ids();
  for (const id of danglingTaskIds) state.loopBackgroundTasksIncomplete.add(id);
  const cancelled = iter.abortController.signal.aborted && !iter.idleTimedOut;
  getLog().warn(
    {
      nodeId: ctx.node.id,
      iteration: iter.iteration,
      taskIds: danglingTaskIds,
      idleTimedOut: iter.idleTimedOut,
      cancelled,
    },
    'loop_node.iteration_stream_ended_with_live_background_tasks'
  );
  if (cancelled) return;
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `⚠️ Loop \`${ctx.node.id}\` iteration ${String(iter.iteration)}: the provider stream ended with ${String(iter.backgroundTasks.count())} background agent task(s) still running (${danglingTaskIds.join(', ')}). Their output may be missing.`,
    ctx.msgContext
  );
}

async function failLoopIterationError(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState,
  error: Error
): Promise<NodeExecutionResult> {
  if (iter.budgetTimer !== undefined) clearTimeout(iter.budgetTimer);
  foldLoopIterationUsage(state, iter);
  const duration = Date.now() - iter.startedAt;
  getLog().error(
    { err: error, nodeId: ctx.node.id, iteration: iter.iteration },
    'loop_node.iteration_failed'
  );
  getWorkflowEventEmitter().emit({
    type: 'loop_iteration_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    iteration: iter.iteration,
    error: error.message,
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'loop_iteration_failed',
      step_name: ctx.stepName,
      data: { iteration: iter.iteration, error: error.message, duration, nodeId: ctx.node.id },
    })
    .catch((evtErr: Error) => {
      logLoopEventStoreError(ctx, evtErr, iter.iteration);
    });
  return failLoopNode(ctx, `Loop iteration ${iter.iteration} failed: ${error.message}`, {
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iter.iteration,
    data: { iteration: iter.iteration },
  });
}

async function maybeFailEmptyLoopOutput(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState
): Promise<NodeExecutionResult | undefined> {
  if (iter.idleTimedOut || iter.fullOutput.trim() !== '') return undefined;
  const emptyError =
    'Loop iteration produced no assistant output. The provider stream closed without yielding content — likely a silent provider rejection or stream interruption.';
  const iterationDuration = Date.now() - iter.startedAt;
  getLog().error(
    { nodeId: ctx.node.id, iteration: iter.iteration, durationMs: iterationDuration },
    'loop_node.iteration_empty_output'
  );
  getWorkflowEventEmitter().emit({
    type: 'loop_iteration_failed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    iteration: iter.iteration,
    error: emptyError,
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'loop_iteration_failed',
      step_name: ctx.stepName,
      data: {
        iteration: iter.iteration,
        error: emptyError,
        duration: iterationDuration,
        nodeId: ctx.node.id,
      },
    })
    .catch((evtErr: Error) => {
      logLoopEventStoreError(ctx, evtErr, iter.iteration);
    });
  return failLoopNode(ctx, `Loop iteration ${iter.iteration} failed: ${emptyError}`, {
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iter.iteration,
    data: { iteration: iter.iteration },
  });
}

async function finishLoopOutputDelivery(
  ctx: LoopNodeExecutionContext,
  iter: LoopIterationState
): Promise<void> {
  if (iter.idleTimedOut)
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      `Loop node '${ctx.node.id}' iteration ${String(iter.iteration)} completed via idle timeout (no output for ${String((ctx.node.idle_timeout ?? STEP_IDLE_TIMEOUT_MS) / 60000)} min)`,
      ctx.msgContext
    );
  if (ctx.platform.getStreamingMode() === 'batch' && iter.cleanOutput)
    await safeSendMessage(ctx.platform, ctx.conversationId, iter.cleanOutput, ctx.msgContext);
}

async function runLoopUntilBash(
  ctx: LoopNodeExecutionContext,
  prevIterationOutput: string,
  signalDetected: boolean,
  iteration: number
): Promise<boolean> {
  const loop = ctx.node.loop;
  if (!loop.until_bash) return false;
  const loopBashPath = resolveBashPath();
  try {
    const { prompt: bashPrompt } = substituteWorkflowVariables(
      loop.until_bash,
      ctx.workflowRun.id,
      ctx.workflowRun.user_message,
      ctx.artifactsDir,
      ctx.baseBranch,
      ctx.docsDir,
      ctx.issueContext,
      undefined,
      undefined,
      undefined,
      { shellSafe: true, stateDir: ctx.stateDir, execContext: ctx.execContext }
    );
    const substitutedBash = substituteNodeOutputRefs(bashPrompt, ctx.nodeOutputs, true, ctx.logDir);
    await runSubprocess(ctx.execContext, loopBashPath, ['-c', substitutedBash], {
      cwd: ctx.cwd,
      timeout: SUBPROCESS_DEFAULT_TIMEOUT,
      deadlineAt: ctx.budget?.deadlineAtMs,
      getRunStatus: () => ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id),
      env: {
        ...(ctx.config.envVars ?? {}),
        USER_MESSAGE: ctx.workflowRun.user_message,
        ARGUMENTS: ctx.workflowRun.user_message,
        LOOP_USER_INPUT: iteration === ctx.startIteration ? (ctx.loopUserInput ?? '') : '',
        LOOP_PREV_OUTPUT: prevIterationOutput,
        REJECTION_REASON: '',
        CONTEXT: ctx.issueContext ?? '',
        EXTERNAL_CONTEXT: ctx.issueContext ?? '',
        ISSUE_CONTEXT: ctx.issueContext ?? '',
      },
    });
    return true;
  } catch (e) {
    const bashErr = e as NodeJS.ErrnoException;
    if (bashErr.code === 'ENOENT' || bashErr.code === 'EACCES' || bashErr.code === 'ENOTDIR') {
      getLog().error({ err: bashErr, nodeId: ctx.node.id, iteration }, 'loop.until_bash_failed');
      throw new Error(
        `Loop node '${ctx.node.id}' until_bash failed: cannot execute bash at '${loopBashPath}' (${bashErr.code}). Set ARCHON_BASH_PATH if Git Bash is installed elsewhere.`
      );
    }
    if (typeof bashErr.code !== 'number') {
      getLog().error(
        { err: bashErr, nodeId: ctx.node.id, iteration },
        'loop.until_bash_unexpected_error'
      );
      throw bashErr;
    }
    return signalDetected && false;
  }
}

function emitLoopNodeIterationCompleted(
  ctx: LoopNodeExecutionContext,
  iter: LoopIterationState,
  completionDetected: boolean
): number {
  const duration = Date.now() - iter.startedAt;
  getWorkflowEventEmitter().emit({
    type: 'loop_iteration_completed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    iteration: iter.iteration,
    duration,
    completionDetected,
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'loop_iteration_completed',
      step_name: ctx.stepName,
      data: { iteration: iter.iteration, duration, completionDetected, nodeId: ctx.node.id },
    })
    .catch((err: Error) => {
      logLoopEventStoreError(ctx, err, iter.iteration);
    });
  return duration;
}

async function maybeCompleteLoopNode(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState,
  completionDetected: boolean
): Promise<NodeExecutionResult | undefined> {
  const loop = ctx.node.loop;
  const interactiveFirstRun = loop.interactive && !ctx.isLoopResume;
  if (!completionDetected || (interactiveFirstRun && loop.signal_completes !== true))
    return undefined;
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `Loop node '${ctx.node.id}' completed after ${String(iter.iteration)} iteration${iter.iteration > 1 ? 's' : ''}`,
    ctx.msgContext
  );
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_completed',
      step_name: ctx.stepName,
      data: {
        duration_ms: Date.now() - iter.startedAt,
        node_output: state.lastIterationOutput,
        ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
        ...(state.loopTotalCostUsd !== undefined ? { cost_usd: state.loopTotalCostUsd } : {}),
        ...(state.loopFinalStopReason ? { stop_reason: state.loopFinalStopReason } : {}),
        ...(state.loopTotalNumTurns !== undefined ? { num_turns: state.loopTotalNumTurns } : {}),
        ...(state.loopResolvedModel
          ? { model_usage: { requested: ctx.resolvedModel, resolved: state.loopResolvedModel.id } }
          : {}),
        ...(state.loopBackgroundTasksIncomplete.size > 0
          ? { background_tasks_incomplete: [...state.loopBackgroundTasksIncomplete] }
          : {}),
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_completed' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_completed',
    runId: ctx.workflowRun.id,
    nodeId: ctx.node.id,
    nodeName: ctx.node.id,
    duration: Date.now() - iter.startedAt,
    ...(state.loopTotalCostUsd !== undefined ? { costUsd: state.loopTotalCostUsd } : {}),
    ...(state.loopFinalStopReason ? { stopReason: state.loopFinalStopReason } : {}),
    ...(state.loopTotalNumTurns !== undefined ? { numTurns: state.loopTotalNumTurns } : {}),
  });
  return {
    state: 'completed',
    output: state.lastIterationOutput,
    sessionId: state.currentSessionId,
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iter.iteration,
    ...(state.lastIterationStructuredOutput !== undefined
      ? { structuredOutput: state.lastIterationStructuredOutput }
      : {}),
  };
}

async function maybePauseLoopNodeGate(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iter: LoopIterationState,
  completionDetected: boolean
): Promise<NodeExecutionResult | undefined> {
  const loop = ctx.node.loop;
  if (!loop.interactive || !loop.gate_message) return undefined;
  const honestMessage = buildHonestGateMessage(
    completionDetected,
    loop.until,
    state.lastIterationOutput,
    loop.gate_message
  );
  const gateMsg = `⏸ **Input required** (loop \`${ctx.node.id}\`, iteration ${String(iter.iteration)}): ${honestMessage}\n\nRun ID: \`${ctx.workflowRun.id}\`\nRespond: \`/workflow approve ${ctx.workflowRun.id} <your feedback>\` | Cancel: \`/workflow reject ${ctx.workflowRun.id}\``;
  const gateSent = await safeSendMessage(ctx.platform, ctx.conversationId, gateMsg, {
    workflowId: ctx.workflowRun.id,
    nodeName: ctx.node.id,
  });
  if (!gateSent) {
    getLog().error(
      { nodeId: ctx.node.id, workflowRunId: ctx.workflowRun.id, iteration: iter.iteration },
      'loop_node.gate_message_send_failed'
    );
    return failLoopNode(
      ctx,
      `Loop gate message failed to deliver for node '${ctx.node.id}' — cannot pause safely`,
      {
        output: state.lastIterationOutput,
        costUsd: state.loopTotalCostUsd,
        ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
        loopIterations: iter.iteration,
        data: { iteration: iter.iteration },
      }
    );
  }
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'approval_requested',
      step_name: ctx.stepName,
      data: {
        message: honestMessage,
        iteration: iter.iteration,
        completionSignaled: completionDetected,
      },
    })
    .catch((err: Error) => {
      logLoopEventStoreError(ctx, err, iter.iteration);
    });
  await pauseGateRespectingExternalTransition(ctx.deps, ctx.workflowRun.id, {
    nodeId: ctx.node.id,
    message: honestMessage,
    type: 'interactive_loop',
    iteration: iter.iteration,
    sessionId: state.currentSessionId ?? null,
    completionSignaled: completionDetected,
    signaledOutput: completionDetected ? state.lastIterationOutput : null,
    signaledTokens: completionDetected ? (state.loopTotalTokens ?? null) : null,
    commandSnapshot: typeof loop.command === 'string' ? state.promptTemplate : null,
  });
  return {
    state: 'completed',
    output: state.lastIterationOutput,
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: iter.iteration,
  };
}

async function runLoopNodeIteration(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState,
  iteration: number
): Promise<NodeExecutionResult | undefined> {
  const stopped = await guardLoopNodeIterationStatus(ctx, state, iteration);
  if (stopped) return stopped;
  emitLoopNodeIterationStarted(ctx, iteration);
  const iter = createLoopIterationState(iteration);
  try {
    await streamLoopIteration(ctx, state, iter);
    const streamStop = await finalizeLoopIterationStream(ctx, state, iter);
    if (streamStop) return streamStop;
  } catch (error) {
    return failLoopIterationError(ctx, state, iter, error as Error);
  }
  await finishLoopOutputDelivery(ctx, iter);
  const emptyFailure = await maybeFailEmptyLoopOutput(ctx, state, iter);
  if (emptyFailure) return emptyFailure;
  const prevIterationOutput = state.lastIterationOutput;
  state.lastIterationOutput = iter.cleanOutput || iter.fullOutput;
  const signalDetected = detectCompletionSignal(iter.fullOutput, ctx.node.loop.until);
  const bashComplete = await runLoopUntilBash(ctx, prevIterationOutput, signalDetected, iteration);
  const completionDetected = signalDetected || bashComplete;
  const duration = emitLoopNodeIterationCompleted(ctx, iter, completionDetected);
  await logNodeComplete(
    ctx.logDir,
    ctx.workflowRun.id,
    `${ctx.node.id}-iteration-${String(iteration)}`,
    ctx.node.id,
    { durationMs: duration }
  );
  return (
    (await maybeCompleteLoopNode(ctx, state, iter, completionDetected)) ??
    (await maybePauseLoopNodeGate(ctx, state, iter, completionDetected))
  );
}

async function loopMaxIterationsResult(
  ctx: LoopNodeExecutionContext,
  state: LoopNodeState
): Promise<NodeExecutionResult> {
  const loop = ctx.node.loop;
  const errorMsg = `Loop node '${ctx.node.id}' exceeded max iterations (${String(loop.max_iterations)}) without completion signal '${loop.until}'`;
  getLog().warn(
    { nodeId: ctx.node.id, maxIterations: loop.max_iterations, signal: loop.until },
    'loop_node.max_iterations_reached'
  );
  await safeSendMessage(ctx.platform, ctx.conversationId, errorMsg, ctx.msgContext);
  return failLoopNode(ctx, errorMsg, {
    output: state.lastIterationOutput,
    costUsd: state.loopTotalCostUsd,
    ...(state.loopTotalTokens !== undefined ? { tokens: state.loopTotalTokens } : {}),
    loopIterations: loop.max_iterations,
    data: { maxIterations: loop.max_iterations },
  });
}

async function executeLoopNode(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflowRun: WorkflowRun,
  node: LoopNode,
  workflowProvider: string,
  resolvedOptions: SendQueryOptions | undefined,
  artifactsDir: string,
  stateDir: string,
  logDir: string,
  baseBranch: string,
  docsDir: string,
  nodeOutputs: Map<string, NodeOutput>,
  config: WorkflowConfig,
  issueContext?: string,
  configuredCommandFolder?: string,
  stepNamePrefix = '',
  execContext: ExecutionContext = { kind: 'host' },
  resolvedModel?: string,
  resolvedTier?: TierName,
  resolvedEffort?: string,
  budget?: ActiveWorkflowBudget,
  budgetBaseUsage: { input: number; output: number } = { input: 0, output: 0 },
  containerCtx?: ContainerRunContext
): Promise<NodeExecutionResult> {
  const rawApproval = workflowRun.metadata?.approval;
  const loopGateMeta = isApprovalContext(rawApproval) ? rawApproval : undefined;
  const isLoopResume = loopGateMeta?.type === 'interactive_loop' && loopGateMeta.nodeId === node.id;
  const loopGateRunMeta = (workflowRun.metadata ?? {}) as LoopGateRunMetadata;
  const ctx: LoopNodeExecutionContext = {
    deps,
    platform,
    conversationId,
    cwd,
    workflowRun,
    node,
    workflowProvider,
    resolvedOptions,
    artifactsDir,
    stateDir,
    logDir,
    baseBranch,
    docsDir,
    nodeOutputs,
    config,
    issueContext,
    configuredCommandFolder,
    stepNamePrefix,
    execContext,
    resolvedModel,
    resolvedTier,
    resolvedEffort,
    budget,
    budgetBaseUsage,
    containerCtx,
    msgContext: { workflowId: workflowRun.id, nodeName: node.id },
    stepName: stepNamePrefix + node.id,
    loopGateMeta,
    isLoopResume,
    startIteration: isLoopResume ? (loopGateMeta.iteration ?? 0) + 1 : 1,
    loopUserInput: isLoopResume ? (loopGateRunMeta.loop_user_input ?? '') : '',
    hardenedLoopRequiresLedger: isHardenedContainerContext(execContext),
  };
  await logNodeStart(logDir, workflowRun.id, node.id, '<loop>');
  emitLoopNodeStarted(ctx);
  const finalized = await maybeFinalizeLoopNodeResume(ctx);
  if (finalized) return finalized;
  const promptTemplate = await resolveLoopPromptTemplate(ctx);
  if (typeof promptTemplate !== 'string') return promptTemplate;
  const aiClient = resolveLoopAiClient(ctx);
  if (aiClient instanceof Error)
    return failLoopNode(ctx, aiClient.message, { data: { provider: workflowProvider } });
  const state = createLoopNodeState(ctx, promptTemplate, aiClient);
  for (let i = ctx.startIteration; i <= node.loop.max_iterations; i++) {
    const result = await runLoopNodeIteration(ctx, state, i);
    if (result) return result;
  }
  return loopMaxIterationsResult(ctx, state);
}

/**
 * Pause the run for a human gate, tolerating a lost CAS when the run was
 * externally transitioned while the gate was being raised — e.g. a killed CLI's
 * signal cleanup marked the run failed mid-pause (#1123), or an operator
 * cancelled it from another surface. `pauseWorkflowRun`'s UPDATE only matches
 * status='running'; when it misses, re-read the status: any non-running status
 * means the pause lost a legitimate external race — log, skip the
 * approval_pending emit, and return so the caller's normal completed-shaped
 * output lets the between-layer status check halt the DAG cleanly (the same
 * path a successful pause takes). On a successful pause, the approval_pending
 * live signal is emitted HERE (from the ApprovalContext's own nodeId/message)
 * so no call site can accidentally emit it after a lost CAS. A store error
 * while the run is still 'running' is a genuine pause failure and rethrows.
 *
 * Deliberately NOT used by the container write-back gate (raiseWriteBackGate),
 * which must stay fail-closed: a lost pause there may never fall through
 * toward the apply/teardown path — throwing is the safe behavior, and the H2
 * teardown-preserve logic keeps the overlay volume for a retry.
 */
async function pauseGateRespectingExternalTransition(
  deps: WorkflowDeps,
  runId: string,
  approvalContext: ApprovalContext
): Promise<void> {
  try {
    await deps.store.pauseWorkflowRun(runId, approvalContext);
  } catch (pauseErr) {
    let status: string | null;
    try {
      status = await deps.store.getWorkflowRunStatus(runId);
    } catch {
      // Status unknowable — surface the original pause failure.
      throw pauseErr;
    }
    if (status === 'running') throw pauseErr;
    getLog().warn(
      { workflowRunId: runId, status, err: pauseErr as Error },
      'dag.gate_pause_skipped_external_transition'
    );
    return;
  }
  getWorkflowEventEmitter().emit({
    type: 'approval_pending',
    runId,
    nodeId: approvalContext.nodeId,
    message: approvalContext.message,
  });
}

async function persistAuthorityEvent(
  deps: WorkflowDeps,
  data: Parameters<WorkflowDeps['store']['createWorkflowEvent']>[0]
): Promise<void> {
  if (!deps.store.createWorkflowEventStrict) {
    throw new Error('Controller authority requires durable event persistence support');
  }
  await deps.store.createWorkflowEventStrict(data);
}

interface ApprovalExecutionContext {
  deps: WorkflowDeps;
  platform: IWorkflowPlatform;
  conversationId: string;
  workflowRun: WorkflowRun;
  workflowProvider: string;
  workflowModel: string | undefined;
  cwd: string;
  artifactsDir: string;
  stateDir: string;
  logDir: string;
  baseBranch: string;
  docsDir: string;
  nodeOutputs: Map<string, NodeOutput>;
  config: WorkflowConfig;
  workflowLevelOptions: WorkflowLevelOptions;
  configuredCommandFolder?: string;
  issueContext?: string;
  aiProfile?: ResolvedAiProfile;
  workflowPreset?: ModelAliasPreset;
  stepNamePrefix: string;
  iteration?: number;
  execContext: ExecutionContext;
  budget?: ActiveWorkflowBudget;
  budgetBaseUsage: { input: number; output: number };
  rejectionOutput?: NodeExecutionResult;
}

function approvalRejectionReason(node: ApprovalNode, workflowRun: WorkflowRun): string {
  const rawApproval = workflowRun.metadata?.approval;
  const approvalMeta = isApprovalContext(rawApproval) ? rawApproval : undefined;
  const rawRejection = workflowRun.metadata?.rejection_reason;
  if (approvalMeta?.type !== 'approval' || approvalMeta.nodeId !== node.id) return '';
  return typeof rawRejection === 'string' && rawRejection !== '' ? rawRejection : '';
}

async function cancelApprovalAfterMaxAttempts(
  node: ApprovalNode,
  workflowRun: WorkflowRun,
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  stepName: string,
  maxAttempts: number,
  msgContext: SendMessageContext
): Promise<NodeExecutionResult> {
  await deps.store.cancelWorkflowRun(workflowRun.id);
  deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'workflow_cancelled',
      step_name: stepName,
      data: { reason: `max_attempts (${String(maxAttempts)}) exhausted` },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: workflowRun.id, eventType: 'workflow_cancelled' },
        'workflow.event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'workflow_cancelled',
    runId: workflowRun.id,
    nodeId: node.id,
    reason: `max_attempts (${String(maxAttempts)}) exhausted`,
  });
  await safeSendMessage(
    platform,
    conversationId,
    `❌ Approval node \`${node.id}\` cancelled after ${String(maxAttempts)} rejections.`,
    msgContext
  );
  return { state: 'completed' as const, output: '' };
}

async function runApprovalRejectionPrompt(
  node: ApprovalNode,
  rejectionReason: string,
  ctx: ApprovalExecutionContext
): Promise<NodeExecutionResult> {
  const {
    deps,
    platform,
    conversationId,
    workflowRun,
    artifactsDir,
    baseBranch,
    docsDir,
    issueContext,
    stateDir,
    execContext,
    nodeOutputs,
    workflowProvider,
    workflowModel,
    config,
    workflowLevelOptions,
    aiProfile,
    workflowPreset,
    cwd,
    logDir,
    configuredCommandFolder,
    stepNamePrefix,
    iteration,
    budget,
    budgetBaseUsage,
  } = ctx;
  const { prompt: substitutedPrompt } = substituteWorkflowVariables(
    node.approval.on_reject?.prompt ?? '',
    workflowRun.id,
    workflowRun.user_message ?? '',
    artifactsDir,
    baseBranch,
    docsDir,
    issueContext,
    undefined,
    rejectionReason,
    undefined,
    { stateDir, execContext }
  );
  const syntheticNode: PromptNode = {
    id: `${node.id}:on_reject`,
    prompt: substituteNodeOutputRefs(substitutedPrompt, nodeOutputs),
    ...(node.depends_on ? { depends_on: node.depends_on } : {}),
    ...(node.idle_timeout ? { idle_timeout: node.idle_timeout } : {}),
  };
  const resolved = await resolveNodeProviderAndModel(
    syntheticNode,
    workflowProvider,
    workflowModel,
    config,
    platform,
    conversationId,
    workflowRun.id,
    cwd,
    workflowLevelOptions,
    aiProfile,
    workflowPreset,
    execContext
  );
  const output = await executeNodeInternal(
    deps,
    platform,
    conversationId,
    cwd,
    workflowRun,
    syntheticNode,
    resolved.provider,
    resolved.options,
    artifactsDir,
    stateDir,
    logDir,
    baseBranch,
    docsDir,
    nodeOutputs,
    undefined,
    configuredCommandFolder,
    issueContext,
    resolved.model,
    resolved.tier,
    resolved.effort,
    stepNamePrefix,
    iteration,
    budget?.deadlineAtMs
  );
  await checkpointHardenedAiAttempt(
    deps,
    workflowRun.id,
    budget,
    undefined,
    `Approval ${node.id} rejection`,
    budgetBaseUsage,
    { input: 0, output: 0 },
    output,
    output
  );
  return output;
}

async function maybeHandleApprovalRejection(
  node: ApprovalNode,
  ctx: ApprovalExecutionContext,
  stepName: string,
  msgContext: SendMessageContext
): Promise<NodeExecutionResult | undefined> {
  const rejectionReason = approvalRejectionReason(node, ctx.workflowRun);
  if (rejectionReason === '' || !node.approval.on_reject) return undefined;
  const maxAttempts = node.approval.on_reject.max_attempts ?? 3;
  const rejectionCount = (ctx.workflowRun.metadata?.rejection_count as number | undefined) ?? 0;
  if (rejectionCount >= maxAttempts) {
    return cancelApprovalAfterMaxAttempts(
      node,
      ctx.workflowRun,
      ctx.deps,
      ctx.platform,
      ctx.conversationId,
      stepName,
      maxAttempts,
      msgContext
    );
  }
  const output = await runApprovalRejectionPrompt(node, rejectionReason, ctx);
  if (output.state === 'failed') return output;
  ctx.rejectionOutput = output;
  return undefined;
}

async function requestApprovalPause(
  node: ApprovalNode,
  workflowRun: WorkflowRun,
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  nodeOutputs: Map<string, NodeOutput>,
  stepName: string,
  msgContext: SendMessageContext,
  execContext: ExecutionContext
): Promise<void> {
  const renderedMessage = substituteNodeOutputRefs(node.approval.message, nodeOutputs);
  const approvalMsg =
    `⏸ **Approval required**: ${renderedMessage}\n\n` +
    `Run ID: \`${workflowRun.id}\`\n` +
    `Approve: \`/workflow approve ${workflowRun.id}\` | Reject: \`/workflow reject ${workflowRun.id}\``;
  await safeSendMessage(platform, conversationId, approvalMsg, msgContext);
  const approvalEvent = {
    workflow_run_id: workflowRun.id,
    event_type: 'approval_requested' as const,
    step_name: stepName,
    data: { message: renderedMessage },
  };
  if (execContext.kind === 'container') await persistAuthorityEvent(deps, approvalEvent);
  else
    void deps.store.createWorkflowEvent(approvalEvent).catch((err: Error) => {
      getLog().error({ err, workflowRunId: workflowRun.id }, 'workflow.event_persist_failed');
    });
  await pauseGateRespectingExternalTransition(deps, workflowRun.id, {
    message: renderedMessage,
    nodeId: node.id,
    type: 'approval',
    captureResponse: node.approval.capture_response,
    onRejectPrompt: node.approval.on_reject?.prompt,
    onRejectMaxAttempts: node.approval.on_reject?.max_attempts,
  });
}

/**
 * Execute an approval node — pauses workflow for human review.
 * On rejection resume (when on_reject is configured): runs the on_reject prompt via AI,
 * then re-pauses at the approval gate. After max_attempts rejections, cancels normally.
 */
async function executeApprovalNode(
  node: ApprovalNode,
  workflowRun: WorkflowRun,
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowProvider: string,
  workflowModel: string | undefined,
  cwd: string,
  artifactsDir: string,
  stateDir: string,
  logDir: string,
  baseBranch: string,
  docsDir: string,
  nodeOutputs: Map<string, NodeOutput>,
  config: WorkflowConfig,
  workflowLevelOptions: WorkflowLevelOptions,
  configuredCommandFolder?: string,
  issueContext?: string,
  aiProfile?: ResolvedAiProfile,
  workflowPreset?: ModelAliasPreset,
  stepNamePrefix = '',
  iteration?: number,
  execContext: ExecutionContext = { kind: 'host' },
  budget?: ActiveWorkflowBudget,
  budgetBaseUsage: { input: number; output: number } = { input: 0, output: 0 }
): Promise<NodeExecutionResult> {
  const msgContext = { workflowId: workflowRun.id, nodeName: node.id };
  const stepName = stepNamePrefix + node.id;
  const approvalCtx: ApprovalExecutionContext = {
    deps,
    platform,
    conversationId,
    workflowRun,
    workflowProvider,
    workflowModel,
    cwd,
    artifactsDir,
    stateDir,
    logDir,
    baseBranch,
    docsDir,
    nodeOutputs,
    config,
    workflowLevelOptions,
    configuredCommandFolder,
    issueContext,
    aiProfile,
    workflowPreset,
    stepNamePrefix,
    iteration,
    execContext,
    budget,
    budgetBaseUsage,
  };
  const rejectionResult = await maybeHandleApprovalRejection(
    node,
    approvalCtx,
    stepName,
    msgContext
  );
  if (rejectionResult !== undefined) return rejectionResult;
  await requestApprovalPause(
    node,
    workflowRun,
    deps,
    platform,
    conversationId,
    nodeOutputs,
    stepName,
    msgContext,
    execContext
  );
  return {
    state: 'completed' as const,
    output: '',
    ...(approvalCtx.rejectionOutput?.tokens !== undefined
      ? { tokens: approvalCtx.rejectionOutput.tokens }
      : {}),
    ...(approvalCtx.rejectionOutput?.costUsd !== undefined
      ? { costUsd: approvalCtx.rejectionOutput.costUsd }
      : {}),
  };
}

/**
 * Execute a `workflow:` (sub-run) node (#2121 Phase 2). Starts — or, on parent
 * resume, re-inspects — a CHILD workflow run and threads its terminal output back
 * as this node's output. The re-entry table (D5) makes this idempotent and
 * cross-process-safe:
 *  - no child yet        → start one in-process, interpret the outcome.
 *  - child completed     → thread its summary/cost (runLayers writes node_completed).
 *  - child failed        → resume-through-parent ONCE, then re-interpret.
 *  - child cancelled     → fail the node.
 *  - child paused/running → pause the PARENT "blocked on child" WITHOUT writing
 *    node_completed (mirrors executeApprovalNode), so the node re-runs when the
 *    parent auto-resumes after the child terminates.
 */
async function executeWorkflowNode(
  node: WorkflowNode,
  ctx: RunLayersContext
): Promise<NodeExecutionResult> {
  const { deps, platform, conversationId, cwd, workflowRun: parentRun } = ctx;
  const msgContext = { workflowId: parentRun.id, nodeName: node.id };

  // Build the failed result AND persist a node_failed event with the reason. Unlike
  // command/prompt/bash/script nodes (which write their own node_failed inside their
  // executor), the workflow node returns a failed NodeExecutionResult that runLayers
  // does NOT turn into an event — so without this the sub-run failure reason (cycle,
  // unknown target, cancelled child, …) would be swallowed into the run-level DAG
  // summary and never auditable per-node. Fire-and-forget like every other event.
  const failResult = (error: string): NodeExecutionResult => {
    deps.store
      .createWorkflowEvent({
        workflow_run_id: parentRun.id,
        event_type: 'node_failed',
        step_name: ctx.stepNamePrefix + node.id,
        data: { error, type: 'workflow' },
      })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: parentRun.id, eventType: 'node_failed' },
          'workflow.event_persist_failed'
        );
      });
    getWorkflowEventEmitter().emit({
      type: 'node_failed',
      runId: parentRun.id,
      nodeId: node.id,
      nodeName: node.id,
      error,
    });
    return { state: 'failed', output: '', error };
  };

  if (!ctx.runChildWorkflow) {
    // Fail fast: executor.ts MUST inject the closure. A missing one means a caller
    // wired executeDagWorkflow without sub-run support — never silently no-op.
    return failResult(
      "Internal error: 'workflow:' node cannot run — runChildWorkflow closure was not injected."
    );
  }

  // Dynamic fan-out (slice 2, PR-C): a `fan_out:` node expands into N governed child
  // runs over a data-driven item list, joined into one node outcome. This is a
  // distinct execution path from the slice-1 single-child node below — branch here so
  // the 1:1 pause/resume machinery stays untouched for non-fan-out nodes.
  if (node.fan_out) {
    return executeFanOutWorkflowNode(node, ctx, node.fan_out, ctx.runChildWorkflow);
  }

  // Resolve the input data string (workflow vars + $node.output refs), exactly as
  // prompt/bash nodes resolve their text surface.
  const rawInput = node.input ?? '';
  const { prompt: substitutedInput } = substituteWorkflowVariables(
    rawInput,
    parentRun.id,
    parentRun.user_message ?? '',
    ctx.artifactsDir,
    ctx.baseBranch,
    ctx.docsDir,
    ctx.issueContext,
    undefined, // loopUserInput
    undefined, // rejectionReason
    undefined, // loopPrevOutput
    { stateDir: ctx.stateDir }
  );
  const input = substituteNodeOutputRefs(substitutedInput, ctx.nodeOutputs);

  // Producer's declared field set (only when output_format declares object
  // properties) so a downstream `$node.output.field` on a JSON-emitting child
  // resolves declared-optional-absent → '' vs a typo → throw.
  const declaredFields = declaredFieldsFromSchema(node.output_format);
  // Build the completed result AND write the node_completed event. Unlike
  // command/prompt/bash/script nodes (which write their own inside their executor)
  // and unlike approval nodes (written by the approve handler), the workflow node
  // writes node_completed HERE — and ONLY on true completion, never on the paused
  // branch — so the resume snapshot skips a truly-finished sub-run on resume
  // but re-runs one still blocked on its child.
  const asCompleted = (outcome: ChildWorkflowOutcome): NodeExecutionResult => {
    if (outcome.output === undefined) {
      // A completed child with no non-blank terminal output threads '' into
      // $<node>.output — legal, but indistinguishable downstream from an
      // intentional empty result, so leave a trace for the author.
      getLog().warn(
        { parentRunId: parentRun.id, nodeId: node.id, childRunId: outcome.childRunId },
        'workflow.subrun_completed_without_output'
      );
    }
    const output = outcome.output ?? '';
    // Fire-and-forget (matches every other event write in this file): the run
    // lifecycle must not hinge on the observability event. Awaiting it unguarded
    // would let a transient event-store failure report a successfully-completed
    // child as a FAILED parent node (it self-heals on resume, but reads wrong). A
    // lost write just means the node re-runs on resume and re-threads the same
    // completed child — idempotent.
    deps.store
      .createWorkflowEvent({
        workflow_run_id: parentRun.id,
        event_type: 'node_completed',
        step_name: ctx.stepNamePrefix + node.id,
        data: {
          node_output: output,
          type: 'workflow',
          child_run_id: outcome.childRunId,
          ...(outcome.costUsd !== undefined ? { cost_usd: outcome.costUsd } : {}),
          // Rolled up from the child run's persisted totals, exactly like cost_usd —
          // tokens are the axis every provider reports (Codex reports no cost at all),
          // so dropping them here while keeping cost would hide the one comparable
          // number. Does not double count WITHIN this run: the child's own per-node
          // rows are filed under `child_run_id`, a different workflow_run_id.
          ...(outcome.tokens !== undefined ? { tokens: outcome.tokens } : {}),
        },
      })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: parentRun.id, eventType: 'node_completed' },
          'workflow.event_persist_failed'
        );
      });
    getWorkflowEventEmitter().emit({
      type: 'node_completed',
      runId: parentRun.id,
      nodeId: node.id,
      nodeName: node.id,
      // The wrapper node has no meaningful duration of its own — the child run's
      // own events carry real timing. Emitted as 0 to satisfy NodeCompletedEvent.
      duration: 0,
      ...(outcome.costUsd !== undefined ? { costUsd: outcome.costUsd } : {}),
    });
    return {
      state: 'completed',
      output,
      ...(outcome.costUsd !== undefined ? { costUsd: outcome.costUsd } : {}),
      ...(outcome.tokens !== undefined ? { tokens: outcome.tokens } : {}),
      ...(declaredFields !== undefined ? { declaredFields } : {}),
    };
  };

  // Pause the PARENT "blocked on child" — mirrors executeApprovalNode's PAUSE
  // primitives: pause, emit, return {completed, ''} WITHOUT node_completed so the
  // node re-runs on the parent's resume (the resume snapshot reads only
  // node_completed). The RESUME side deliberately differs: an approval gate is
  // resolved externally by the approve handler, while this node re-runs and
  // re-inspects its child. Also unlike the approval node, no approval_requested
  // workflow_event row is persisted here — the block reason lives on the run
  // itself (metadata.approval), and there is no human decision to audit for a
  // gate that resolves automatically on child completion.
  const pauseParentOnChild = async (childRunId: string): Promise<NodeExecutionResult> => {
    // KNOWN LIMITATION (#2180): the run has a SINGLE approval-gate slot. If two
    // gate-pausing nodes (two `workflow:` children, or a `workflow:` + an `approval:`)
    // land in the SAME topological layer, the second pauseWorkflowRun matches 0 rows
    // (the first already flipped running→paused) and throws — swallowed into a node
    // failure the paused run then short-circuits past. The loser's child is real but
    // unmentioned until a later resume re-pauses on it. A retry can't fix this (there
    // is nowhere to record a second simultaneous block); the real fix is a gate queue
    // or a load-time reject of multiple gate-pausing nodes per layer — tracked in #2180.
    const message =
      `Sub-run \`${node.workflow}\` (run \`${childRunId.slice(0, 8)}\`) is paused awaiting review. ` +
      `Approve it by run id: \`/workflow approve ${childRunId}\``;
    await deps.store.pauseWorkflowRun(parentRun.id, {
      message,
      nodeId: node.id,
      type: 'child_workflow',
      childRunId,
    });
    getWorkflowEventEmitter().emit({
      type: 'approval_pending',
      runId: parentRun.id,
      nodeId: node.id,
      message,
    });
    await safeSendMessage(
      platform,
      conversationId,
      `⏸ **Blocked on sub-run** \`${node.workflow}\`: ${message}`,
      msgContext
    );
    return { state: 'completed', output: '' };
  };

  const interpret = async (outcome: ChildWorkflowOutcome): Promise<NodeExecutionResult> => {
    switch (outcome.status) {
      case 'completed':
        return asCompleted(outcome);
      case 'paused':
        return pauseParentOnChild(outcome.childRunId);
      case 'failed':
        return failResult(outcome.error ?? `Sub-run '${node.workflow}' failed`);
      case 'cancelled':
        return failResult(`Sub-run '${node.workflow}' was cancelled`);
      default: {
        // Compile-time exhaustiveness + runtime fail-loud: without this, a status
        // outside the union would silently return `undefined` into runLayers.
        const unreachable: never = outcome.status;
        return failResult(
          `Sub-run '${node.workflow}' returned unexpected status '${String(unreachable)}'`
        );
      }
    }
  };

  // Re-entry: find THIS node's child (a parent may run several workflow: nodes, so
  // filter by parent_node_id). At most one child per node in slice 1; if somehow
  // several, the most recent wins.
  let existing: WorkflowRun | undefined;
  try {
    const children = (await deps.store.findChildRuns(parentRun.id)).filter(
      c =>
        readSubrunMetadata(c.metadata as Record<string, unknown> | undefined).parentNodeId ===
        node.id
    );
    existing = children.length > 0 ? children[children.length - 1] : undefined;
  } catch (err) {
    return failResult(
      `Failed to look up child runs for node '${node.id}': ${(err as Error).message}`
    );
  }

  const childArgs = {
    parentRun,
    nodeId: node.id,
    childWorkflowName: node.workflow,
    input,
    cwd,
    conversationId,
    conversationDbId: parentRun.conversation_id,
    userId: parentRun.user_id ?? undefined,
    codebaseId: parentRun.codebase_id ?? undefined,
    isolation: node.isolation,
  };

  try {
    if (existing === undefined) {
      return await interpret(await ctx.runChildWorkflow(childArgs));
    }
    if (existing.status === 'failed') {
      // Resume-through-parent recovery (D5/#1764): re-drive the failed child once.
      return await interpret(
        await ctx.runChildWorkflow({ ...childArgs, resumeFailedChild: existing })
      );
    }
    if (
      existing.status === 'paused' ||
      existing.status === 'running' ||
      existing.status === 'pending'
    ) {
      // Still in progress (awaiting a human or a concurrent run). Re-pause the
      // parent; NEVER resume a paused child.
      return await pauseParentOnChild(existing.id);
    }
    // completed / cancelled — thread the outcome through the same state table a
    // freshly-run child uses (interpret handles both).
    return await interpret(childOutcomeFromRun(existing));
  } catch (err) {
    return failResult(`Sub-run '${node.workflow}' errored: ${(err as Error).message}`);
  }
}

function controllerActionGrantFor(
  node: ControllerActionNode,
  ctx: RunLayersContext
): ControllerActionGrant | undefined {
  return (ctx.deps.controllerActionGrants ?? []).find(
    grant =>
      grant.runId === ctx.workflowRun.id &&
      grant.workflowName === ctx.workflowName &&
      grant.workflowDigest === ctx.workflowDigest &&
      grant.nodeId === node.id &&
      grant.action === node.controller_action &&
      grant.phase === node.phase
  );
}

function controllerActionTimeoutMs(node: ControllerActionNode): number {
  return nodeTotalTimeoutMs(node) ?? 30_000;
}

const CONTROLLER_ACTION_STATUS_POLL_MS = 25;

async function awaitControllerAction<T>(
  actionPromise: Promise<T>,
  abortController: AbortController,
  timeoutMs: number
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let timedOut = false;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      timedOut = true;
      abortController.abort();
      reject(new Error(`Controller action exceeded timeout of ${String(timeoutMs)}ms`));
    }, timeoutMs);
  });

  try {
    return await Promise.race([
      actionPromise,
      timeoutPromise,
      new Promise<never>((_, reject) => {
        abortController.signal.addEventListener(
          'abort',
          () => {
            reject(
              new Error(
                timedOut
                  ? `Controller action exceeded timeout of ${String(timeoutMs)}ms`
                  : 'Controller action cancelled'
              )
            );
          },
          { once: true }
        );
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

function isHardenedContainerContext(execContext: ExecutionContext): boolean {
  if (execContext.kind !== 'container') return false;
  return (execContext as { profile?: unknown }).profile === 'hardened';
}

async function assertControllerCompletionLive(
  ctx: RunLayersContext,
  signal: AbortSignal,
  deadlineAt: number
): Promise<void> {
  if (signal.aborted || Date.now() > deadlineAt)
    throw new Error('Controller action cancelled or deadline exceeded');
  const status = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
  if (signal.aborted || Date.now() > deadlineAt || status !== 'running') {
    throw new Error('Controller action is no longer live');
  }
}

async function executeControllerActionNode(
  node: ControllerActionNode,
  ctx: RunLayersContext
): Promise<NodeExecutionResult> {
  const action = node.controller_action;
  const handler = ctx.deps.controllerActions?.[action];
  const stepName = ctx.stepNamePrefix + node.id;
  const fail = (error: string): NodeExecutionResult => {
    ctx.deps.store
      .createWorkflowEvent({
        workflow_run_id: ctx.workflowRun.id,
        event_type: 'node_failed',
        step_name: stepName,
        data: { error, type: 'controller_action', action, phase: node.phase },
      })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_failed' },
          'workflow_event_persist_failed'
        );
      });
    return { state: 'failed', output: '', error };
  };

  if (!handler) {
    return fail(
      `Controller action '${action}' is not configured for node '${node.id}'. ` +
        'Privileged workflow actions require a controller-private handler binding; no bash/script fallback is allowed.'
    );
  }

  const grant = controllerActionGrantFor(node, ctx);
  if (!grant) {
    return fail(
      `Controller action '${action}' is not authorized for node '${node.id}' in workflow '${ctx.workflowName}'. ` +
        'Grant must match run id, workflow digest, node id, action, phase, and sealed action manifest.'
    );
  }

  if (!isControllerActionManifestSealed(grant.actionManifest)) {
    return fail(
      `Controller action '${action}' grant for node '${node.id}' has an invalid action manifest digest.`
    );
  }

  const persistCompletion = ctx.deps.store.createControllerCompletionEvent?.bind(ctx.deps.store);
  if (!persistCompletion)
    return fail('Controller completion requires atomic running-state persistence support');

  try {
    const actionManifest = sealControllerActionManifest(grant.actionManifest);
    const runStatus = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
    if (runStatus !== 'running') {
      return fail(
        `Controller action '${action}' refused because workflow run '${ctx.workflowRun.id}' is ${runStatus ?? 'missing'}.`
      );
    }

    const actionAbortController = new AbortController();
    const deadlineAt = Math.min(
      Date.now() + controllerActionTimeoutMs(node),
      ctx.budget?.deadlineAtMs ?? Infinity
    );
    const timeoutMs = deadlineAt - Date.now();
    if (timeoutMs <= 0) return fail(`Controller action '${action}' exceeded the hardened deadline`);
    await persistAuthorityEvent(ctx.deps, {
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'node_started',
      step_name: stepName,
      data: {
        type: 'controller_action',
        action,
        phase: node.phase,
        action_manifest_id: actionManifest.id,
        action_manifest_digest: actionManifest.digest,
      },
    });

    const statusPoller = setInterval(() => {
      void ctx.deps.store
        .getWorkflowRunStatus(ctx.workflowRun.id)
        .then(status => {
          if (status !== 'running') actionAbortController.abort();
        })
        .catch((err: Error) => {
          getLog().error(
            { err, workflowRunId: ctx.workflowRun.id },
            'controller_action_status_poll_failed'
          );
          actionAbortController.abort();
        });
    }, CONTROLLER_ACTION_STATUS_POLL_MS);

    const startedAt = Date.now();
    const actionPromise = Promise.resolve().then(() =>
      handler({
        workflowRun: ctx.workflowRun,
        workflowName: ctx.workflowName,
        workflowDigest: ctx.workflowDigest,
        node,
        actionManifest,
        signal: actionAbortController.signal,
        deadlineAt,
        cwd: ctx.cwd,
        artifactsDir: ctx.artifactsDir,
        stateDir: ctx.stateDir,
        logDir: ctx.logDir,
        baseBranch: ctx.baseBranch,
        docsDir: ctx.docsDir,
        config: ctx.config,
        platform: ctx.platform,
        conversationId: ctx.conversationId,
        execContext: ctx.execContext,
      })
    );
    const rawOutput = await awaitControllerAction(
      actionPromise,
      actionAbortController,
      timeoutMs
    ).finally(() => {
      if (statusPoller !== undefined) clearInterval(statusPoller);
    });
    await assertControllerCompletionLive(ctx, actionAbortController.signal, deadlineAt);
    const output = normalizeControllerActionOutput(rawOutput);
    const duration = Date.now() - startedAt;
    await persistCompletion(
      {
        workflow_run_id: ctx.workflowRun.id,
        event_type: 'node_completed',
        step_name: stepName,
        data: {
          duration_ms: duration,
          type: 'controller_action',
          action,
          phase: node.phase,
          node_output: output,
        },
      },
      deadlineAt
    );
    return { state: 'completed', output };
  } catch (error) {
    return fail(`Controller action '${action}' failed: ${(error as Error).message}`);
  }
}

/**
 * `metadata.cancelled_reason` values the fan-out path stamps on children it cancels
 * ITSELF (so the cancel is attributable and — unlike a user's out-of-band cancel —
 * recoverable on resume). `fan_out_gate`: a child paused at a gate (#2180).
 * `fan_out_orphan`: a child whose `child_index` fell out of range when the item list shrank.
 *
 * `fan_out_sibling` is READ-ONLY legacy. It marked an in-flight sibling cancelled once an
 * earlier revision's fail-fast sealed the node's fate; nothing writes it any more, because a
 * fan-out no longer ends one child's run on account of another's. It stays in the type and
 * in the recoverable set on purpose: a run that was in flight across the upgrade has rows
 * carrying it, and dropping it would make those children read as user-cancelled — terminal,
 * never re-driven, so the parent would fail every resume with no way back. Delete it only
 * once no resumable run can predate the change.
 */
type FanOutCancelReason = 'fan_out_gate' | 'fan_out_sibling' | 'fan_out_orphan';
const FAN_OUT_RECOVERABLE_CANCEL_REASONS: ReadonlySet<string> = new Set<FanOutCancelReason>([
  'fan_out_gate',
  'fan_out_sibling',
  // Every reason above is engine-owned, so every one belongs here — `fan_out_orphan` was
  // missing, which read an orphan the engine cancelled as a USER cancel. Items shrinking
  // and then growing back left those slots permanently cancelled: dead under all_done, and
  // an unrecoverable node failure on every resume under all_success.
  'fan_out_orphan',
]);

/**
 * A `running`/`pending` child found on re-entry is ambiguous: a crash-orphan of a prior
 * pass, or a live execution in another process. Past this idle window (no
 * `last_activity_at` heartbeat — written ≤ every 60s while a child runs) it reads as an
 * orphan; within it, as possibly still live. Only the MESSAGE differs — per CLAUDE.md's
 * "No Autonomous Lifecycle Mutation Across Process Boundaries", NEITHER branch cancels.
 */
const FAN_OUT_CHILD_STALE_MS = 5 * 60_000;

/** The fan-out cancel reason stamped on a child, if any. */
function fanOutCancelReason(run: WorkflowRun): string | undefined {
  const reason = (run.metadata as Record<string, unknown> | undefined)?.cancelled_reason;
  return typeof reason === 'string' ? reason : undefined;
}

/** True when a cancelled child was cancelled BY the fan-out path → recoverable on resume. */
function isFanOutRecoverableCancel(run: WorkflowRun): boolean {
  const reason = fanOutCancelReason(run);
  return reason !== undefined && FAN_OUT_RECOVERABLE_CANCEL_REASONS.has(reason);
}

/** True when a `running`/`pending` child has had no activity within the idle window. */
function isFanOutChildStale(run: WorkflowRun, now = Date.now()): boolean {
  const last = run.last_activity_at ?? run.started_at;
  return last === null || now - last.getTime() > FAN_OUT_CHILD_STALE_MS;
}

/**
 * Cheap, dependency-free content hash (djb2) of a fan-out child's input string, stamped
 * at spawn so resume can detect a non-deterministic items producer (same index, changed
 * content) and WARN (never re-key). Collision-tolerant: a warn-only signal, not identity.
 */
function hashFanOutItem(input: string): string {
  let h = 5381;
  for (let i = 0; i < input.length; i++) h = (h * 33) ^ input.charCodeAt(i);
  return (h >>> 0).toString(16);
}

/**
 * #2180 pointer: a fan-out child paused at an approval gate. Fan-out children must be
 * autonomous — the parent run has a SINGLE approval-gate slot, so N concurrently-paused
 * children cannot be represented. Names the offending child + index + run id (I4) and
 * points the author at the two supported fixes. Removing the gate then resuming re-drives
 * exactly this child (its cancel is tagged `fan_out_gate` → recoverable).
 */
function fanOutAutonomousGateMessage(
  node: WorkflowNode,
  childRunId: string,
  index: number
): string {
  return (
    `fan_out node '${node.id}': child ${String(index)} (run ${childRunId.slice(0, 8)}) of ` +
    `'${node.workflow}' paused at an approval gate. Fan-out children must run autonomously — ` +
    'the parent run has a single gate slot, so N concurrently-paused children cannot be ' +
    `represented (#2180). Remove the gate from '${node.workflow}' and resume (this child ` +
    "re-drives), or invoke it as a single (non-fan-out) 'workflow:' node."
  );
}

/**
 * A `running`/`pending` fan-out child found on re-entry — ambiguous ownership, so NOT
 * auto-cancelled (CLAUDE.md lifecycle rule). Surfaces the state + a one-click action, with
 * wording keyed to `last_activity_at` staleness (fresh → likely live; stale → likely orphaned).
 */
function fanOutAmbiguousChildMessage(
  node: WorkflowNode,
  child: WorkflowRun,
  index: number,
  stale: boolean
): string {
  const ref = `child ${String(index)} (run ${child.id.slice(0, 8)})`;
  return stale
    ? `fan_out node '${node.id}': ${ref} of '${node.workflow}' is still '${child.status}' with no ` +
        'recent activity — it appears orphaned by an interrupted run. Abandon it (`archon workflow ' +
        `abandon ${child.id}\`) and resume the parent to re-drive it.`
    : `fan_out node '${node.id}': ${ref} of '${node.workflow}' may still be running (recent activity) — ` +
        `wait for it to finish and resume, or abandon it (\`archon workflow abandon ${child.id}\`) if it is stuck.`;
}

/**
 * Concurrent fan-out children sharing the parent checkout collide on the path-exclusive
 * lock (`executor.ts`, guarded by `mutates_checkout !== false`): siblings are deliberately
 * NOT excluded from it, so all but one self-cancel — and a lock-cancelled child is threaded
 * as terminal on re-entry, which makes the failure permanent (#2180 Defect A). The engine
 * cannot infer which way out the author wants, so it names all three and refuses to spend
 * the money finding out.
 */
function fanOutSharedCheckoutMessage(node: WorkflowNode, concurrency: number): string {
  return (
    `fan_out node '${node.id}': up to ${String(concurrency)} children of '${node.workflow}' ` +
    'would run at once in the parent checkout, and that workflow does not declare ' +
    '`mutates_checkout: false`. Concurrent runs on one checkout take a path-exclusive lock, ' +
    'so all but the first would cancel themselves — and a lock-cancelled child is not ' +
    'recoverable by resume (#2180). Choose one: add `mutates_checkout: false` to ' +
    `'${node.workflow}' if it only reads the repo; set \`isolation: worktree\` on '${node.id}' ` +
    'if the children write to it; or set `fan_out.max_parallel: 1` to run them one at a time.'
  );
}

/**
 * Resolve the fan-out target's definition for the shared-checkout preflight, using the
 * same discovery + name resolution `runChildWorkflow` performs at spawn — sub-run targets
 * resolve at spawn time by design (#2200), so this reads the definition the children will
 * actually get rather than one captured at load.
 *
 * Reports WHY it could not resolve rather than collapsing every cause to `undefined`. The
 * preflight it feeds is the only thing standing between a shared-checkout fan-out and a
 * path-lock cascade the engine cannot recover from, so "we could not check" must not read
 * the same as "we checked and it is fine" — that is the silent fallback the engineering
 * principles forbid. An unknown or ambiguous name reaches here without any exception being
 * thrown, so this is not a rare path.
 *
 * The caller still must not report a COLLISION on this branch: the author's actual problem
 * is the unresolvable target, and pointing them at `mutates_checkout` would send them to
 * the wrong file. It fails closed with a message about the resolution instead.
 */
async function resolveFanOutChildDefinition(
  deps: WorkflowDeps,
  cwd: string,
  targetName: string
): Promise<{ definition: WorkflowDefinition } | { unresolved: string }> {
  try {
    const { workflows } = await discoverWorkflowsWithConfig(cwd, deps.loadConfig);
    const definition = resolveWorkflowName(
      targetName,
      workflows.map(w => w.workflow)
    );
    // resolveWorkflowName returns undefined for an unknown name and THROWS only on
    // ambiguity, so the undefined branch is ordinary rather than exceptional.
    return definition
      ? { definition }
      : { unresolved: `no workflow named '${targetName}' was found` };
  } catch (err) {
    return { unresolved: (err as Error).message };
  }
}

/**
 * Σ of defined child `costUsd`. Returns undefined when NO child reported cost so the
 * node's own `costUsd` stays absent (a misleading `0` would look like a free run) —
 * matching the run-level aggregation's "only write when > 0" posture.
 *
 * UNDER-REPORTS: this is Σ of *completed* children, not Σ of children. Usage metadata is
 * persisted in exactly one place — inside `completeWorkflowRun` — so a child that burned
 * tokens and then failed or was cancelled records no spend, and `childOutcomeFromRun`
 * returns undefined for it. A 10-item fan-out where 3 children burn tokens and fail reports
 * the spend of 7. Inherited from the 1:1 sub-run path, but fan-out is what makes it
 * material, and `all_done` being the default makes a partly-failed run the ordinary case
 * rather than the exceptional one. The real fix is upstream: `failWorkflowRun` would have
 * to persist usage the way `completeWorkflowRun` does. Tracked with the run-tree budget
 * work (#1961).
 */
function sumFanOutCost(outcomes: readonly ChildWorkflowOutcome[]): number | undefined {
  let sum = 0;
  let any = false;
  for (const o of outcomes) {
    if (o.costUsd !== undefined && Number.isFinite(o.costUsd)) {
      sum += o.costUsd;
      any = true;
    }
  }
  return any ? sum : undefined;
}

/**
 * Σ of defined child token usage; undefined when no child reported tokens. Carries the same
 * completed-only caveat as {@link sumFanOutCost} — a child that burned tokens and then
 * failed contributes nothing.
 */
function sumFanOutTokens(outcomes: readonly ChildWorkflowOutcome[]): TokenUsage | undefined {
  let input = 0;
  let output = 0;
  let any = false;
  for (const o of outcomes) {
    if (o.tokens !== undefined) {
      if (Number.isFinite(o.tokens.input)) input += o.tokens.input;
      if (Number.isFinite(o.tokens.output)) output += o.tokens.output;
      any = true;
    }
  }
  return any ? { input, output } : undefined;
}

interface FanOutNodeContext {
  deps: WorkflowDeps;
  platform: IWorkflowPlatform;
  conversationId: string;
  cwd: string;
  parentRun: WorkflowRun;
  stepName: string;
  msgContext: SendMessageContext;
}

function fanOutFailResult(
  ctx: FanOutNodeContext,
  node: WorkflowNode,
  error: string,
  costUsd?: number,
  tokens?: TokenUsage
): NodeExecutionResult {
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.parentRun.id,
      event_type: 'node_failed',
      step_name: ctx.stepName,
      data: { error, type: 'workflow' },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.parentRun.id, eventType: 'node_failed' },
        'workflow.event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_failed',
    runId: ctx.parentRun.id,
    nodeId: node.id,
    nodeName: node.id,
    error,
  });
  return {
    state: 'failed',
    output: '',
    error,
    ...(costUsd !== undefined ? { costUsd } : {}),
    ...(tokens !== undefined ? { tokens } : {}),
  };
}

function writeFanOutCompleted(
  ctx: FanOutNodeContext,
  node: WorkflowNode,
  output: string,
  costUsd?: number,
  tokens?: TokenUsage
): void {
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.parentRun.id,
      event_type: 'node_completed',
      step_name: ctx.stepName,
      data: {
        node_output: output,
        type: 'workflow',
        fan_out: true,
        ...(costUsd !== undefined ? { cost_usd: costUsd } : {}),
        ...(tokens !== undefined ? { tokens } : {}),
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.parentRun.id, eventType: 'node_completed' },
        'workflow.event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_completed',
    runId: ctx.parentRun.id,
    nodeId: node.id,
    nodeName: node.id,
    duration: 0,
    ...(costUsd !== undefined ? { costUsd } : {}),
  });
}

async function notifyFanOut(ctx: FanOutNodeContext, text: string): Promise<void> {
  await safeSendMessage(ctx.platform, ctx.conversationId, text, ctx.msgContext);
}

async function cancelFanOutChild(
  ctx: FanOutNodeContext,
  childId: string,
  reason: FanOutCancelReason
): Promise<void> {
  if (!childId) return;
  await ctx.deps.store
    .updateWorkflowRun(childId, { metadata: { cancelled_reason: reason } })
    .catch((err: unknown) => {
      getLog().error(
        { err: err as Error, childRunId: childId, reason },
        'workflow.fan_out_cancel_reason_write_failed'
      );
    });
  await ctx.deps.store.cancelWorkflowRun(childId).catch((err: unknown) => {
    getLog().error({ err: err as Error, childRunId: childId }, 'workflow.fan_out_cancel_failed');
  });
}

function fanOutItemToInput(item: unknown): string {
  return typeof item === 'string' ? item : JSON.stringify(item);
}

async function resolveFanOutItems(
  node: WorkflowNode,
  ctx: RunLayersContext,
  fanOut: FanOutConfig,
  fctx: FanOutNodeContext
): Promise<unknown[] | NodeExecutionResult> {
  try {
    const { prompt: itemsVarsResolved } = substituteWorkflowVariables(
      fanOut.items,
      fctx.parentRun.id,
      fctx.parentRun.user_message ?? '',
      ctx.artifactsDir,
      ctx.baseBranch,
      ctx.docsDir,
      ctx.issueContext
    );
    const parsed: unknown = JSON.parse(
      substituteNodeOutputRefs(itemsVarsResolved, ctx.nodeOutputs)
    );
    if (isUnknownArray(parsed)) return parsed;
    const msg = `fan_out.items on '${node.id}' resolved to ${typeof parsed}, not a JSON array. '${fanOut.items}' must reference a node output that produces a JSON array.`;
    await notifyFanOut(fctx, `❌ **Fan-out failed** (node \`${node.id}\`): ${msg}`);
    return fanOutFailResult(fctx, node, msg);
  } catch (err) {
    const msg = `fan_out.items on '${node.id}' could not be resolved to a JSON array: ${(err as Error).message}`;
    await notifyFanOut(fctx, `❌ **Fan-out failed** (node \`${node.id}\`): ${msg}`);
    return fanOutFailResult(fctx, node, msg);
  }
}

function isNodeExecutionResult(value: unknown): value is NodeExecutionResult {
  return typeof value === 'object' && value !== null && 'state' in value;
}

async function collectExistingFanOutChildren(
  node: WorkflowNode,
  items: unknown[],
  fctx: FanOutNodeContext
): Promise<Map<number, WorkflowRun> | NodeExecutionResult> {
  const existingByIndex = new Map<number, WorkflowRun>();
  try {
    const children = (await fctx.deps.store.findChildRuns(fctx.parentRun.id)).filter(
      c =>
        readSubrunMetadata(c.metadata as Record<string, unknown> | undefined).parentNodeId ===
        node.id
    );
    for (const child of children) await indexFanOutChild(node, items, fctx, existingByIndex, child);
    return existingByIndex;
  } catch (err) {
    const msg = `Failed to look up fan-out child runs for node '${node.id}': ${(err as Error).message}`;
    await notifyFanOut(fctx, `❌ **Fan-out failed** (node \`${node.id}\`): ${msg}`);
    return fanOutFailResult(fctx, node, msg);
  }
}

async function indexFanOutChild(
  node: WorkflowNode,
  items: unknown[],
  fctx: FanOutNodeContext,
  existingByIndex: Map<number, WorkflowRun>,
  child: WorkflowRun
): Promise<void> {
  const meta = readSubrunMetadata(child.metadata as Record<string, unknown> | undefined);
  const idx = meta.childIndex;
  if (idx === undefined) {
    getLog().warn(
      {
        parentRunId: fctx.parentRun.id,
        nodeId: node.id,
        childRunId: child.id,
        status: child.status,
      },
      'workflow.fan_out_child_missing_index'
    );
    if (child.status === 'running' || child.status === 'pending' || child.status === 'paused')
      await cancelFanOutChild(fctx, child.id, 'fan_out_orphan');
    return;
  }
  if (idx < 0 || idx >= items.length) {
    getLog().warn(
      {
        parentRunId: fctx.parentRun.id,
        nodeId: node.id,
        childRunId: child.id,
        childIndex: idx,
        itemCount: items.length,
      },
      'workflow.fan_out_child_index_out_of_range'
    );
    if (child.status === 'running' || child.status === 'pending' || child.status === 'paused')
      await cancelFanOutChild(fctx, child.id, 'fan_out_orphan');
    return;
  }
  if (existingByIndex.has(idx))
    getLog().debug(
      { parentRunId: fctx.parentRun.id, nodeId: node.id, childIndex: idx, childRunId: child.id },
      'workflow.fan_out_duplicate_child_index'
    );
  const priorHash = meta.fanOutItemHash;
  if (priorHash !== undefined && priorHash !== hashFanOutItem(fanOutItemToInput(items[idx]))) {
    getLog().warn(
      { parentRunId: fctx.parentRun.id, nodeId: node.id, childIndex: idx, childRunId: child.id },
      'workflow.fan_out_item_content_changed'
    );
  }
  existingByIndex.set(idx, child);
}

async function rejectNonterminalFanOutChildren(
  node: WorkflowNode,
  existingByIndex: Map<number, WorkflowRun>,
  fctx: FanOutNodeContext
): Promise<NodeExecutionResult | undefined> {
  const pausedExisting = [...existingByIndex.entries()].filter(([, c]) => c.status === 'paused');
  if (pausedExisting.length > 0) {
    const [index, child] = pausedExisting[0];
    for (const [, c] of pausedExisting) await cancelFanOutChild(fctx, c.id, 'fan_out_gate');
    const msg = fanOutAutonomousGateMessage(node, child.id, index);
    await notifyFanOut(fctx, `⏸→❌ **Fan-out gate rejected** (node \`${node.id}\`): ${msg}`);
    return fanOutFailResult(fctx, node, msg);
  }
  const ambiguous = [...existingByIndex.entries()].filter(
    ([, c]) => c.status === 'running' || c.status === 'pending'
  );
  if (ambiguous.length === 0) return undefined;
  const [index, child] = ambiguous[0];
  const stale = isFanOutChildStale(child);
  getLog().warn(
    {
      parentRunId: fctx.parentRun.id,
      nodeId: node.id,
      childRunId: child.id,
      childIndex: index,
      status: child.status,
      stale,
    },
    'workflow.fan_out_child_nonterminal_on_resume'
  );
  const msg = fanOutAmbiguousChildMessage(node, child, index, stale);
  await notifyFanOut(fctx, `⚠️ **Fan-out blocked** (node \`${node.id}\`): ${msg}`);
  return fanOutFailResult(fctx, node, msg);
}

async function guardFanOutSharedCheckout(
  node: WorkflowNode,
  ctx: RunLayersContext,
  fanOut: FanOutConfig,
  items: unknown[],
  existingByIndex: Map<number, WorkflowRun>,
  fctx: FanOutNodeContext
): Promise<NodeExecutionResult | undefined> {
  const pendingCount = items.reduce<number>((n, _item, i) => {
    const existing = existingByIndex.get(i);
    if (existing?.status === 'completed') return n;
    if (existing?.status === 'cancelled' && !isFanOutRecoverableCancel(existing)) return n;
    return n + 1;
  }, 0);
  const plannedConcurrency = Math.min(fanOut.max_parallel, pendingCount);
  if (node.isolation === 'worktree' || plannedConcurrency <= 1) return undefined;
  const resolved = await resolveFanOutChildDefinition(ctx.deps, ctx.cwd, node.workflow);
  if ('unresolved' in resolved)
    return fanOutUnresolvedPreflight(node, plannedConcurrency, resolved.unresolved, fctx);
  if (resolved.definition.mutates_checkout === false) return undefined;
  const msg = fanOutSharedCheckoutMessage(node, plannedConcurrency);
  getLog().warn(
    {
      parentRunId: fctx.parentRun.id,
      nodeId: node.id,
      childWorkflow: node.workflow,
      plannedConcurrency,
    },
    'workflow.fan_out_shared_checkout_collision'
  );
  await notifyFanOut(fctx, `❌ **Fan-out blocked** (node \`${node.id}\`): ${msg}`);
  return fanOutFailResult(fctx, node, msg);
}

async function fanOutUnresolvedPreflight(
  node: WorkflowNode,
  plannedConcurrency: number,
  reason: string,
  fctx: FanOutNodeContext
): Promise<NodeExecutionResult> {
  const msg =
    `fan_out node '${node.id}': cannot verify that ${String(plannedConcurrency)} concurrent ` +
    `children are safe to share the parent checkout, because '${node.workflow}' could not ` +
    `be resolved — ${reason}. Fix the target name; if the children really do ` +
    'run side by side in one checkout, the workflow must also declare `mutates_checkout: false`.';
  getLog().warn(
    {
      parentRunId: fctx.parentRun.id,
      nodeId: node.id,
      childWorkflow: node.workflow,
      plannedConcurrency,
      reason,
    },
    'workflow.fan_out_preflight_unresolved'
  );
  await notifyFanOut(fctx, `❌ **Fan-out blocked** (node \`${node.id}\`): ${msg}`);
  return fanOutFailResult(fctx, node, msg);
}

/**
 * Execute a fan-out `workflow:` node (#2121 slice 2, PR-C): expand the node into N
 * governed child runs over a data-driven item list, bound by a `max_parallel` sliding
 * window, and reduce the N child outcomes into one node outcome via the declared
 * `join`. This is the slice-1 1:1 sub-run re-entry table generalized to 1:N, keyed by
 * `metadata.child_index`:
 *   - resolve `fan_out.items` → a JSON array (fail closed on non-array/malformed);
 *   - re-inspect existing children (findChildRuns by parent_node_id) by child_index, so
 *     parent resume skips completed instances and re-drives failed ones for free;
 *   - spawn/re-drive the incomplete indices through mapWithLimit(max_parallel); a
 *     fan-out-cancelled (gate/sibling) child is recoverable → re-driven, a user-cancelled
 *     one stays terminal;
 *   - #2180 (Defect A): before ANY child is created, refuse a shared-checkout expansion
 *     that would run >1 child at once over a target not declaring `mutates_checkout: false`
 *     — those siblings would self-cancel on the path lock, unrecoverably;
 *   - #2180 (D5): a fan-out child that PAUSES at a gate FAILS the node (autonomous fan-out
 *     — the single parent gate slot can't hold N children) and is cancelled tagged
 *     `fan_out_gate` (removing the gate + resuming re-drives it). A `running`/`pending`
 *     child found on resume is ambiguous → the node fails WITHOUT auto-cancel (CLAUDE.md
 *     lifecycle rule), surfacing a staleness-keyed wait/abandon action;
 *   - EVERY index is spawned and every child runs to its own terminal state — no child's
 *     outcome ends another's — and only then does the join reduce: `all_success` (any
 *     failed/cancelled child fails the node) / `all_done` (aggregate all terminal;
 *     failed/cancelled entries represented);
 *   - aggregate `$<id>.output` = JSON array in item order; cost/tokens = Σ children.
 *
 * Never throws — every failure returns a failed NodeExecutionResult so a child-store
 * error can't unwind the whole DAG. `node_completed` is written ONLY when the join is
 * satisfied, so a failed fan-out node re-runs and re-inspects its children on resume
 * (resume correctness is sourced from child-run status, not the node's own events).
 */
async function executeFanOutWorkflowNode(
  node: WorkflowNode,
  ctx: RunLayersContext,
  fanOut: FanOutConfig,
  runChild: RunChildWorkflowFn
): Promise<NodeExecutionResult> {
  const { deps, platform, conversationId, cwd, workflowRun: parentRun } = ctx;
  const msgContext = { workflowId: parentRun.id, nodeName: node.id };
  const stepName = ctx.stepNamePrefix + node.id;
  const fctx: FanOutNodeContext = {
    deps,
    platform,
    conversationId,
    cwd,
    parentRun,
    stepName,
    msgContext,
  };

  // 1. Resolve `fan_out.items` → a JSON array.
  const resolvedItems = await resolveFanOutItems(node, ctx, fanOut, fctx);
  if (isNodeExecutionResult(resolvedItems)) return resolvedItems;
  const items = resolvedItems;

  // 2. Empty array → a valid zero-width expansion (#977 acceptance): complete with '[]'.
  if (items.length === 0) {
    getLog().info({ parentRunId: parentRun.id, nodeId: node.id }, 'workflow.fan_out_empty');
    writeFanOutCompleted(fctx, node, '[]', undefined);
    return { state: 'completed', output: '[]' };
  }

  // 3. Re-entry: find THIS node's existing children and index them by metadata.child_index.
  const existingChildren = await collectExistingFanOutChildren(node, items, fctx);
  if (isNodeExecutionResult(existingChildren)) return existingChildren;
  const existingByIndex = existingChildren;

  // 4. Existing non-terminal fan-out children block autonomous resume.
  const nonterminalFailure = await rejectNonterminalFanOutChildren(node, existingByIndex, fctx);
  if (nonterminalFailure) return nonterminalFailure;

  // 5. Shared-checkout preflight (#2180 Defect A).
  const sharedCheckoutFailure = await guardFanOutSharedCheckout(
    node,
    ctx,
    fanOut,
    items,
    existingByIndex,
    fctx
  );
  if (sharedCheckoutFailure) return sharedCheckoutFailure;

  // 6. Execute EVERY index through a bounded sliding window. Classification per index: an
  //    existing completed child threads its recorded outcome (resume skip); an existing
  //    failed OR fan-out-cancelled (recoverable) child is re-driven; a user-cancelled child
  //    stays terminal; a missing index spawns fresh.
  //
  //    No child's outcome terminates another's. Every index is spawned and every child runs
  //    to its OWN terminal state before the join reduces — a fan-out is N independent
  //    governed runs that happen to be siblings, not a competition. An earlier revision
  //    fail-fasted here: the first failure under all_success skipped the remaining spawns
  //    and cancelled in-flight siblings. That saved spend by deciding one child's fate from
  //    another's, which is not the engine's call to make, and it made the outcome of an
  //    interrupted sibling depend on which child happened to finish first.
  //
  //    The cost is real and belongs to the author: a wide fan-out whose first child fails
  //    now runs every remaining child, so worst-case spend is items.length rather than
  //    "until the first failure". `max_parallel` bounds concurrency, not total spend —
  //    a run-tree budget ceiling is #1961.
  const settled = await mapWithLimit(
    items,
    fanOut.max_parallel,
    async (item, i): Promise<ChildWorkflowOutcome> => {
      const existing = existingByIndex.get(i);
      // Existing completed child → thread its outcome without re-spawning (resume skip).
      if (existing?.status === 'completed') return childOutcomeFromRun(existing);
      // A user-cancelled child (no fan-out tag) is terminal — thread it as-is (fails
      // all_success; represented in all_done). A fan-out-tagged cancel is recoverable and
      // falls through to re-drive.
      if (existing?.status === 'cancelled' && !isFanOutRecoverableCancel(existing)) {
        return childOutcomeFromRun(existing);
      }
      const input = fanOutItemToInput(item);
      // A fan-out-recoverable-cancelled child (gate/sibling) can't be resumed while
      // 'cancelled' (resumeWorkflowRun rejects that status) — clear it to 'failed' first,
      // then re-drive through the failed path. Our own tagged cancel is terminal state we
      // own, so this recovery heuristic is appropriate (CLAUDE.md).
      let resumeChild = existing?.status === 'failed' ? existing : undefined;
      if (existing?.status === 'cancelled' && isFanOutRecoverableCancel(existing)) {
        await deps.store
          .updateWorkflowRun(existing.id, { status: 'failed' })
          .catch((err: unknown) => {
            getLog().error(
              { err: err as Error, childRunId: existing.id },
              'workflow.fan_out_recover_cancel_failed'
            );
          });
        resumeChild = { ...existing, status: 'failed' };
      }
      // Whatever this child returns — completed, failed, cancelled, or paused — it is this
      // child's outcome alone. The join reads them all once every one has settled.
      const outcome = await runChild({
        parentRun,
        nodeId: node.id,
        childWorkflowName: node.workflow,
        input,
        cwd,
        conversationId,
        conversationDbId: parentRun.conversation_id,
        userId: parentRun.user_id ?? undefined,
        codebaseId: parentRun.codebase_id ?? undefined,
        isolation: node.isolation,
        childIndex: i,
        itemHash: hashFanOutItem(input),
        ...(resumeChild ? { resumeFailedChild: resumeChild } : {}),
      });
      // A paused child is cancelled HERE rather than at the join, and the timing is
      // load-bearing rather than tidiness. A pause is not terminal, and a non-terminal run
      // keeps holding its working path: `getActiveWorkflowRunByPath` counts `paused` as
      // active. On a shared checkout the very next sibling then loses the path lock and
      // self-cancels — with NO reason tag, so it reads as a user cancel, is never re-driven,
      // and the parent fails identically on every resume. Removing the fail-fast is what
      // exposed this: before, a pause sealed the node and no later sibling ever spawned.
      //
      // This is not one child's outcome ending another's — the cancel is decided by the
      // paused child's own state, it is the same cancel the gate path (#2180/#2438) applies
      // at the join a moment later, and every sibling still runs to its own terminal state.
      // All it changes is that the lock is released before the next child starts.
      if (outcome.status === 'paused')
        await cancelFanOutChild(fctx, outcome.childRunId, 'fan_out_gate');
      return outcome;
    }
  );

  // mapWithLimit never rejects here (runChild honors the never-throws contract), but be
  // defensive: a rejected slot becomes a synthetic failed outcome.
  const outcomes: ChildWorkflowOutcome[] = settled.map((r, i) =>
    r.status === 'fulfilled'
      ? r.value
      : {
          childRunId: '',
          status: 'failed',
          error: `fan-out child ${String(i)} threw: ${String(r.reason)}`,
        }
  );

  const totalCostUsd = sumFanOutCost(outcomes);
  const totalTokens = sumFanOutTokens(outcomes);

  // I3: parity with the 1:1 asCompleted path — a completed child with no terminal output
  // threads '' but is indistinguishable downstream from an intentional empty result, so
  // leave a trace. Used by both join reducers.
  const childOutput = (o: ChildWorkflowOutcome, index: number): string => {
    if (o.status === 'completed' && o.output === undefined) {
      getLog().warn(
        { parentRunId: parentRun.id, nodeId: node.id, childRunId: o.childRunId, childIndex: index },
        'workflow.subrun_completed_without_output'
      );
    }
    return o.output ?? '';
  };

  // 7. #2180 (first-run path): a freshly-spawned child that paused at a gate fails the
  //    node. Cancel the paused child(ren) tagged `fan_out_gate` (recoverable once the gate
  //    is removed) and name the offending child (I4).
  //
  //    This is the ONE place a fan-out still cancels a child it did not have to, and it
  //    survives the no-mutual-termination rule deliberately. A pause is not a terminal
  //    state, so "every child runs to its own terminal state" has no answer for it: the
  //    parent has a single approval slot and cannot hand it to N children, so the child
  //    would wait forever for a gate it can never be given. This is an error path (#2438),
  //    not a race: the node is failing either way, and the cancel just stops the run
  //    dangling. A paused child never stops a sibling — everyone still runs to their own
  //    terminal state.
  //
  //    The cancel that actually frees the path lock already fired mid-flight, the moment
  //    the pause was observed. This pass is the idempotent backstop: it covers a paused
  //    outcome that did not come from this attempt's spawn loop (a synthetic outcome from a
  //    rejected slot), and re-cancelling an already-cancelled row is a no-op.
  const pausedIdx = outcomes.findIndex(o => o.status === 'paused');
  if (pausedIdx !== -1) {
    for (const o of outcomes)
      if (o.status === 'paused') await cancelFanOutChild(fctx, o.childRunId, 'fan_out_gate');
    const msg = fanOutAutonomousGateMessage(node, outcomes[pausedIdx].childRunId, pausedIdx);
    await notifyFanOut(fctx, `⏸→❌ **Fan-out gate rejected** (node \`${node.id}\`): ${msg}`);
    return fanOutFailResult(fctx, node, msg, totalCostUsd, totalTokens);
  }

  // 8. Join.
  if (fanOut.join === 'all_success') {
    // Every child ran to its own terminal state, so the lowest-index non-completed outcome
    // IS the causal one — nothing here is a casualty of another child's failure.
    const firstBad = outcomes.findIndex(o => o.status !== 'completed');
    if (firstBad !== -1) {
      const bad = outcomes[firstBad];
      const ref = bad.childRunId ? ` (run ${bad.childRunId.slice(0, 8)})` : '';
      await notifyFanOut(
        fctx,
        `❌ **Fan-out failed** (node \`${node.id}\`): child ${String(firstBad)}${ref} ${bad.status}` +
          (bad.error ? ` — ${bad.error}` : '')
      );
      return fanOutFailResult(
        fctx,
        node,
        `fan_out node '${node.id}' (join: all_success): child ${String(firstBad)}${ref} ${bad.status}` +
          (bad.error ? `: ${bad.error}` : ''),
        totalCostUsd,
        totalTokens
      );
    }
    // All completed → aggregate the child outputs in item order (JSON array string).
    const aggregate = JSON.stringify(outcomes.map((o, i) => childOutput(o, i)));
    writeFanOutCompleted(fctx, node, aggregate, totalCostUsd, totalTokens);
    return {
      state: 'completed',
      output: aggregate,
      ...(totalCostUsd !== undefined ? { costUsd: totalCostUsd } : {}),
      ...(totalTokens !== undefined ? { tokens: totalTokens } : {}),
    };
  }

  // join: all_done — node succeeds once all children are terminal; a failed/cancelled
  // entry is represented as a { error, status } object in the aggregate array (so a
  // collector can reconcile partial results). Never fails the node on a partial failure.
  const aggregate = JSON.stringify(
    outcomes.map((o, i) =>
      o.status === 'completed'
        ? childOutput(o, i)
        : { error: o.error ?? `child ${o.status}`, status: o.status }
    )
  );
  writeFanOutCompleted(fctx, node, aggregate, totalCostUsd, totalTokens);
  return {
    state: 'completed',
    output: aggregate,
    ...(totalCostUsd !== undefined ? { costUsd: totalCostUsd } : {}),
    ...(totalTokens !== undefined ? { tokens: totalTokens } : {}),
  };
}

/**
 * True when a node participates in cross-run session persistence: a command/prompt
 * node (see {@link isPersistableNode}) that hasn't opted out via `context: 'fresh'`,
 * with `persist_session: true` set directly or inherited from the workflow-level
 * `persist_sessions` default. Single source of truth for both the session
 * lookup/persist gates and the #1846 scope-artifact mirror.
 */
function nodeUsesPersistedScope(node: DagNode, workflowPersistSessions: boolean): boolean {
  if (!isPersistableNode(node)) return false;
  if (node.context === 'fresh') return false;
  const nodePersist = 'persist_session' in node ? node.persist_session : undefined;
  return nodePersist ?? workflowPersistSessions;
}

/**
 * Build the by-reference recovery suffix for a cold-resume warning (#1846): list
 * the typed artifacts that PRIOR invocations of this workflow+scope left in the
 * stable scope dir, as absolute file paths — never pasted content. Entries
 * produced by the current run are excluded (they can't recover anything the
 * fresh session doesn't already have). Returns `''` when there is nothing to
 * point at, or when the scope dir can't be read — recovery is best-effort and
 * must never turn a successful (if cold) node into a failure.
 */
async function buildColdResumeRecoveryPointer(
  scopeArtifactsDir: string,
  currentRunId: string,
  nodeId: string
): Promise<string> {
  try {
    const priorArtifacts = (await readNodeArtifacts(scopeArtifactsDir))
      .filter(entry => entry.runId !== currentRunId)
      .sort((a, b) => b.producedAt.localeCompare(a.producedAt));
    if (priorArtifacts.length === 0) return '';
    const lines = priorArtifacts.map(
      entry =>
        `- ${entry.outputType} (\`${entry.nodeId}\`): ${joinPath(scopeArtifactsDir, entry.path)}`
    );
    return `\nArtifacts from the previous invocation are available for recovery (read on demand):\n${lines.join('\n')}`;
  } catch (err) {
    getLog().warn(
      { err: err as Error, scopeArtifactsDir, nodeId },
      'dag.cold_resume_artifacts_read_failed'
    );
    return '';
  }
}

/**
 * Shared context for {@link runLayers}. Bundles the run-level invariants (deps, platform,
 * run record, resolved provider/model/options, paths, config) together with the per-subgraph
 * mutable state (the node set + its pre-computed topological layers, the shared output map,
 * session threading, usage accumulators, and resume cache).
 *
 * The top-level DAG and each `loop_group` body iteration construct their own context: the
 * top-level call uses `workflow.nodes` / a fresh `nodeOutputs`; a loop-group body uses the
 * group's `nodes` / a per-iteration scoped `nodeOutputs` (reset each iteration) and a
 * `stepNamePrefix` of `'{groupId}.'` that namespaces the persisted `step_name` of EVERY
 * body event — runLayers' own control events (skip/trigger_rule/when) AND the lifecycle
 * events emitted inside executeNodeInternal / executeBashNode / executeScriptNode /
 * executeLoopNode / executeApprovalNode. Body lifecycle rows additionally carry `iteration`
 * in `data`. The in-process emitter payloads stay raw (unprefixed) — see #2090.
 */
interface RunLayersContext {
  // --- run-level invariants (shared by top-level DAG and loop_group body) ---
  deps: WorkflowDeps;
  platform: IWorkflowPlatform;
  conversationId: string;
  cwd: string;
  /**
   * Injected closure that starts a child sub-run for a `workflow:` node (#2121
   * Phase 2). Undefined when the caller (e.g. a unit test) doesn't wire it — a
   * `workflow:` node then fails fast rather than silently no-op'ing. Forwarded
   * into loop_group body contexts too, though a `workflow:` node inside a
   * loop_group body is rejected at load time.
   */
  runChildWorkflow?: RunChildWorkflowFn;
  /** Where nodes in these layers execute (host, or the container in Phase B). Threaded
   *  into every AI turn's SendQueryOptions and every deterministic subprocess. */
  execContext: ExecutionContext;
  containerCtx?: ContainerRunContext;
  workflowRun: WorkflowRun;
  /** Workflow name — used for persist_session keying + telemetry. */
  workflowName: string;
  /** Stable digest of the loaded workflow definition used for controller grant binding. */
  workflowDigest: string;
  config: WorkflowConfig;
  workflowProvider: string;
  workflowModel: string | undefined;
  workflowLevelOptions: WorkflowLevelOptions;
  aiProfile?: ResolvedAiProfile;
  workflowPreset?: ModelAliasPreset;
  artifactsDir: string;
  /**
   * `$STATE_DIR` — the per-PROJECT cross-run state directory (#2200), shared by
   * every workflow in the project and pre-created by the executor. A run-level
   * invariant like `artifactsDir`; forwarded unchanged into loop_group bodies.
   */
  stateDir: string;
  logDir: string;
  baseBranch: string;
  docsDir: string;
  configuredCommandFolder?: string;
  issueContext?: string;
  /** Cross-run session-persistence scope key (DB conversation UUID), or undefined to skip. */
  persistScopeKey: string | undefined;
  /** Workflow-level default for per-node `persist_session` (opt-in). */
  workflowPersistSessions: boolean;
  /**
   * Stable cross-invocation artifact scope dir (`scopes/<workflow>/<scope>/`), or
   * undefined when the workflow doesn't use session persistence. When set,
   * persistence-participating nodes with `output_type` mirror their typed sidecars
   * here, and a cold session resume points the user at the prior invocation's
   * artifacts by reference (#1846). Always undefined for loop_group bodies
   * (which also run with `persistScopeKey: undefined`).
   */
  scopeArtifactsDir: string | undefined;

  // --- per-subgraph mutable state (varies between top-level DAG and loop_group body) ---
  /** Pre-computed topological layers (caller builds once — body shape is static). runLayers walks ONLY these; there is deliberately no flat node list here. */
  layers: DagNode[][];
  /** Shared node-output map (caller owns; runLayers writes node results here). */
  nodeOutputs: Map<string, NodeOutput>;
  /** Resume cache: node ids that completed in a prior run (top-level only; undefined for body). */
  priorCompletedNodes?: Map<string, string>;
  /** Sequential-session threading cursor (mutated by runLayers). Provider-tagged so the
   *  session is only threaded into nodes that resolve to the SAME provider (#1992). */
  lastSequentialSession: SequentialSessionCursor | undefined;
  /** Run-level usage accumulators (mutated by runLayers; caller reads after). */
  totalCostUsd: number;
  totalTokensIn: number;
  totalTokensOut: number;
  totalLoopIterations: number;
  budget?: ActiveWorkflowBudget;
  budgetBaseUsage: { input: number; output: number };
  /** Prefix prepended to every persisted `step_name` ('' for top-level, '{groupId}.' for a loop_group body). */
  stepNamePrefix: string;
  /**
   * The enclosing loop_group iteration (1-based) when these layers are a group body,
   * else undefined for the top-level DAG. Tagged into body node lifecycle event `data`
   * so multi-iteration runs are disaggregatable in the persisted event log (#2090).
   */
  iteration?: number;
  /**
   * Per-iteration `$LOOP_USER_INPUT` free-text for loop_group body `script:` nodes,
   * delivered into the subprocess as an env var (never spliced into TS/Python source —
   * #2115). Only non-empty on the first resumed iteration of an interactive group;
   * undefined for the top-level DAG (top-level scripts have no loop user input).
   */
  bodyLoopUserInput?: string;
}

async function persistNodeSkip(
  ctx: RunLayersContext,
  node: DagNode,
  reason: 'trigger_rule' | 'when_condition' | 'when_condition_parse_error' | 'prior_success',
  data: Record<string, unknown> = {}
): Promise<LayerNodeResult> {
  getLog().info({ nodeId: node.id, reason }, 'dag_node_skipped');
  await logNodeSkip(ctx.logDir, ctx.workflowRun.id, node.id, reason).catch((err: Error) => {
    getLog().warn({ err, nodeId: node.id }, 'dag.node_skip_log_write_failed');
  });
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: reason === 'prior_success' ? 'node_skipped_prior_success' : 'node_skipped',
      step_name: ctx.stepNamePrefix + node.id,
      data: { reason, ...data },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_skipped' },
        'workflow_event_persist_failed'
      );
    });
  getWorkflowEventEmitter().emit({
    type: 'node_skipped',
    runId: ctx.workflowRun.id,
    nodeId: node.id,
    nodeName: node.command ?? node.id,
    reason,
  });
  return { nodeId: node.id, output: { state: 'skipped' as const, output: '' } };
}

async function maybeSkipPriorCompletedNode(
  ctx: RunLayersContext,
  node: DagNode
): Promise<LayerNodeResult | undefined> {
  if (!ctx.priorCompletedNodes?.has(node.id)) return undefined;
  if (node.always_run) {
    getLog().info({ nodeId: node.id }, 'dag.node_always_run_resume_forced');
    ctx.deps.store
      .createWorkflowEvent({
        workflow_run_id: ctx.workflowRun.id,
        event_type: 'node_always_run_reset',
        step_name: ctx.stepNamePrefix + node.id,
        data: { prior_output: ctx.priorCompletedNodes.get(node.id) ?? '' },
      })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: ctx.workflowRun.id, eventType: 'node_always_run_reset' },
          'workflow_event_persist_failed'
        );
      });
    return undefined;
  }
  await persistNodeSkip(ctx, node, 'prior_success', {
    node_output: ctx.priorCompletedNodes.get(node.id) ?? '',
  });
  return {
    nodeId: node.id,
    output: ctx.nodeOutputs.get(node.id) ?? { state: 'skipped' as const, output: '' },
  };
}

async function maybeSkipByTriggerRule(
  ctx: RunLayersContext,
  node: DagNode
): Promise<LayerNodeResult | undefined> {
  if (checkTriggerRule(node, ctx.nodeOutputs) !== 'skip') return undefined;
  return persistNodeSkip(ctx, node, 'trigger_rule');
}

async function maybeSkipByWhen(
  ctx: RunLayersContext,
  node: DagNode
): Promise<LayerNodeResult | undefined> {
  if (node.when === undefined) return undefined;
  const { result: conditionPasses, parsed: conditionParsed } = evaluateCondition(
    node.when,
    ctx.nodeOutputs
  );
  if (!conditionParsed) {
    const parseErrMsg = `⚠️ Node '${node.id}': unparseable \`when:\` expression "${node.when}" — node skipped (fail-closed). Check syntax: \`$nodeId.output == 'VALUE'\`, \`$nodeId.output > '5'\`, or compound \`$a.output == 'X' && $b.output != 'Y'\`.`;
    await safeSendMessage(ctx.platform, ctx.conversationId, parseErrMsg, {
      workflowId: ctx.workflowRun.id,
      nodeName: node.id,
    });
    getLog().error({ nodeId: node.id, when: node.when }, 'dag_node_skipped_condition_parse_error');
    return persistNodeSkip(ctx, node, 'when_condition_parse_error', { expr: node.when });
  }
  if (conditionPasses) return undefined;
  getLog().info({ nodeId: node.id, when: node.when }, 'dag_node_skipped_condition');
  return persistNodeSkip(ctx, node, 'when_condition', { expr: node.when });
}

async function maybeSkipLayerNode(
  ctx: RunLayersContext,
  node: DagNode
): Promise<LayerNodeResult | undefined> {
  if (isIncludeNode(node)) {
    throw new Error(
      `Internal error: include node '${node.id}' reached the executor unexpanded. Include nodes must be resolved by expandWorkflowIncludes() during discovery.`
    );
  }
  return (
    (await maybeSkipPriorCompletedNode(ctx, node)) ??
    (await maybeSkipByTriggerRule(ctx, node)) ??
    (await maybeSkipByWhen(ctx, node))
  );
}

function currentBudgetBase(ctx: RunLayersContext): { input: number; output: number } {
  return {
    input: ctx.budgetBaseUsage.input + ctx.totalTokensIn,
    output: ctx.budgetBaseUsage.output + ctx.totalTokensOut,
  };
}

async function executeBashLayerNode(
  ctx: RunLayersContext,
  node: BashNode
): Promise<LayerNodeResult> {
  const output = await runDeterministicNodeWithRetry(
    node,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun,
    () =>
      executeBashNode(
        ctx.deps,
        ctx.platform,
        ctx.conversationId,
        ctx.cwd,
        ctx.workflowRun,
        node,
        ctx.artifactsDir,
        ctx.stateDir,
        ctx.logDir,
        ctx.baseBranch,
        ctx.docsDir,
        ctx.nodeOutputs,
        ctx.issueContext,
        ctx.config.envVars,
        ctx.stepNamePrefix,
        ctx.iteration,
        ctx.execContext,
        ctx.budget?.deadlineAtMs
      )
  );
  return { nodeId: node.id, output };
}

async function executeLoopLayerNode(
  ctx: RunLayersContext,
  node: LoopNode
): Promise<LayerNodeResult> {
  const resolved = await resolveNodeProviderAndModel(
    node,
    ctx.workflowProvider,
    ctx.workflowModel,
    ctx.config,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun.id,
    ctx.cwd,
    ctx.workflowLevelOptions,
    ctx.aiProfile,
    ctx.workflowPreset,
    ctx.execContext
  );
  const output = await executeLoopNode(
    ctx.deps,
    ctx.platform,
    ctx.conversationId,
    ctx.cwd,
    ctx.workflowRun,
    node,
    resolved.provider,
    resolved.options,
    ctx.artifactsDir,
    ctx.stateDir,
    ctx.logDir,
    ctx.baseBranch,
    ctx.docsDir,
    ctx.nodeOutputs,
    ctx.config,
    ctx.issueContext,
    ctx.configuredCommandFolder,
    ctx.stepNamePrefix,
    ctx.execContext,
    resolved.model,
    resolved.tier,
    resolved.effort,
    ctx.budget,
    currentBudgetBase(ctx),
    ctx.containerCtx
  );
  return { nodeId: node.id, output, sessionProvider: resolved.provider };
}

async function executeLoopGroupLayerNode(
  ctx: RunLayersContext,
  node: LoopGroupNode
): Promise<LayerNodeResult> {
  const { provider } = await resolveNodeProviderAndModel(
    node,
    ctx.workflowProvider,
    ctx.workflowModel,
    ctx.config,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun.id,
    ctx.cwd,
    ctx.workflowLevelOptions,
    ctx.aiProfile,
    ctx.workflowPreset,
    ctx.execContext
  );
  const output = await executeLoopGroupNode(
    ctx.deps,
    ctx.platform,
    ctx.conversationId,
    ctx.cwd,
    ctx.workflowRun,
    node,
    provider,
    ctx.workflowModel,
    ctx.workflowLevelOptions,
    ctx.aiProfile,
    ctx.workflowPreset,
    ctx.artifactsDir,
    ctx.stateDir,
    ctx.logDir,
    ctx.baseBranch,
    ctx.docsDir,
    ctx.nodeOutputs,
    ctx.config,
    ctx.issueContext,
    ctx.stepNamePrefix,
    ctx.execContext,
    ctx.runChildWorkflow,
    ctx.workflowDigest,
    ctx.budget,
    currentBudgetBase(ctx)
  );
  return { nodeId: node.id, output };
}

async function executeApprovalLayerNode(
  ctx: RunLayersContext,
  node: ApprovalNode
): Promise<LayerNodeResult> {
  const output = await executeApprovalNode(
    node,
    ctx.workflowRun,
    ctx.deps,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowProvider,
    ctx.workflowModel,
    ctx.cwd,
    ctx.artifactsDir,
    ctx.stateDir,
    ctx.logDir,
    ctx.baseBranch,
    ctx.docsDir,
    ctx.nodeOutputs,
    ctx.config,
    ctx.workflowLevelOptions,
    ctx.configuredCommandFolder,
    ctx.issueContext,
    ctx.aiProfile,
    ctx.workflowPreset,
    ctx.stepNamePrefix,
    ctx.iteration,
    ctx.execContext,
    ctx.budget,
    currentBudgetBase(ctx)
  );
  return { nodeId: node.id, output };
}

async function executeCancelLayerNode(
  ctx: RunLayersContext,
  node: Extract<DagNode, { cancel: string }>
): Promise<LayerNodeResult> {
  const reason = substituteNodeOutputRefs(node.cancel, ctx.nodeOutputs);
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `❌ **Workflow cancelled** (node \`${node.id}\`): ${reason}`,
    { workflowId: ctx.workflowRun.id, nodeName: node.id }
  );
  ctx.deps.store
    .createWorkflowEvent({
      workflow_run_id: ctx.workflowRun.id,
      event_type: 'workflow_cancelled',
      step_name: ctx.stepNamePrefix + node.id,
      data: { reason },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: ctx.workflowRun.id, eventType: 'workflow_cancelled' },
        'workflow.event_persist_failed'
      );
    });
  await ctx.deps.store.cancelWorkflowRun(ctx.workflowRun.id);
  getWorkflowEventEmitter().emit({
    type: 'workflow_cancelled',
    runId: ctx.workflowRun.id,
    nodeId: node.id,
    reason,
  });
  return { nodeId: node.id, output: { state: 'completed' as const, output: reason } };
}

async function executeScriptLayerNode(
  ctx: RunLayersContext,
  node: ScriptNode
): Promise<LayerNodeResult> {
  const output = await runDeterministicNodeWithRetry(
    node,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun,
    () =>
      executeScriptNode(
        ctx.deps,
        ctx.platform,
        ctx.conversationId,
        ctx.cwd,
        ctx.workflowRun,
        node,
        ctx.artifactsDir,
        ctx.stateDir,
        ctx.logDir,
        ctx.baseBranch,
        ctx.docsDir,
        ctx.nodeOutputs,
        ctx.issueContext,
        ctx.config.envVars,
        ctx.stepNamePrefix,
        ctx.iteration,
        ctx.bodyLoopUserInput ?? '',
        ctx.execContext,
        ctx.budget?.deadlineAtMs
      )
  );
  return { nodeId: node.id, output };
}

function resolveLayerResumeSession(
  ctx: RunLayersContext,
  node: CommandNode | PromptNode,
  provider: string,
  isParallelLayer: boolean
): string | undefined {
  if (isParallelLayer || node.context === 'fresh' || ctx.lastSequentialSession === undefined)
    return undefined;
  if (ctx.lastSequentialSession.provider === provider) return ctx.lastSequentialSession.sessionId;
  getLog().info(
    { nodeId: node.id, provider, cursorProvider: ctx.lastSequentialSession.provider },
    'dag.session_provider_boundary_fresh'
  );
  return undefined;
}

async function lookupPersistedLayerSession(
  ctx: RunLayersContext,
  node: CommandNode | PromptNode,
  provider: string,
  resumeSessionId: string | undefined
): Promise<string | undefined> {
  const usesPersistedScope = nodeUsesPersistedScope(node, ctx.workflowPersistSessions);
  if (!usesPersistedScope) return resumeSessionId;
  const caps = ctx.deps.getAgentProvider(provider).getCapabilities();
  if (!caps.sessionResume)
    throw new Error(
      `Node '${node.id}' has persist_session: true but resolved provider '${provider}' does not support sessionResume. Remove persist_session, or use a provider with sessionResume capability.`
    );
  if (!ctx.persistScopeKey) return resumeSessionId;
  try {
    const persisted = await ctx.deps.store.getWorkflowNodeSession({
      workflow_name: ctx.workflowName,
      node_id: node.id,
      scope_key: ctx.persistScopeKey,
      provider,
    });
    if (!persisted) return resumeSessionId;
    const sessionIdPreview = `${persisted.provider_session_id.slice(0, 8)}…`;
    ctx.deps.store
      .createWorkflowEvent({
        workflow_run_id: ctx.workflowRun.id,
        event_type: 'node_session_resumed',
        step_name: ctx.stepNamePrefix + node.id,
        data: {
          provider,
          scope_key: ctx.persistScopeKey,
          provider_session_id_preview: sessionIdPreview,
        },
      })
      .catch((err: Error) => {
        getLog().warn({ err, nodeId: node.id }, 'persist_session_resumed_event_persist_failed');
      });
    return persisted.provider_session_id;
  } catch (err) {
    getLog().warn(
      {
        err: err as Error,
        nodeId: node.id,
        workflow: ctx.workflowName,
        scopeKey: ctx.persistScopeKey,
        provider,
      },
      'persist_session_lookup_failed'
    );
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      `⚠️ Could not load the persisted session for node \`${node.id}\` — it will run without prior context. Session continuity may be broken; if this recurs, check server logs or run \`/workflow reset-sessions ${ctx.workflowName}\`.`,
      { workflowId: ctx.workflowRun.id, nodeName: node.id }
    );
    return resumeSessionId;
  }
}

async function executeAiLayerNode(
  ctx: RunLayersContext,
  node: CommandNode | PromptNode,
  isParallelLayer: boolean
): Promise<LayerNodeResult> {
  const resolved = await resolveNodeProviderAndModel(
    node,
    ctx.workflowProvider,
    ctx.workflowModel,
    ctx.config,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun.id,
    ctx.cwd,
    ctx.workflowLevelOptions,
    ctx.aiProfile,
    ctx.workflowPreset,
    ctx.execContext
  );
  const resumeSessionId = await lookupPersistedLayerSession(
    ctx,
    node,
    resolved.provider,
    resolveLayerResumeSession(ctx, node, resolved.provider, isParallelLayer)
  );
  const output = await runNodeRetryLoop(
    node,
    ctx.platform,
    ctx.conversationId,
    ctx.workflowRun,
    getEffectiveNodeRetryConfig(node),
    (deadlineAt, priorAttemptUsage) =>
      executeNodeInternal(
        ctx.deps,
        ctx.platform,
        ctx.conversationId,
        ctx.cwd,
        ctx.workflowRun,
        node,
        resolved.provider,
        resolved.options,
        ctx.artifactsDir,
        ctx.stateDir,
        ctx.logDir,
        ctx.baseBranch,
        ctx.docsDir,
        ctx.nodeOutputs,
        resumeSessionId,
        ctx.configuredCommandFolder,
        ctx.issueContext,
        resolved.model,
        resolved.tier,
        resolved.effort,
        ctx.stepNamePrefix,
        ctx.iteration,
        earlierDeadline(deadlineAt, ctx.budget?.deadlineAtMs),
        (accumulatedPassOutput, rawPassOutput) =>
          checkpointHardenedAiAttempt(
            ctx.deps,
            ctx.workflowRun.id,
            ctx.budget,
            ctx.containerCtx,
            `Node '${node.id}' structured output pass`,
            ctx.budgetBaseUsage,
            { input: ctx.totalTokensIn, output: ctx.totalTokensOut },
            withPriorAttemptUsage(priorAttemptUsage, accumulatedPassOutput),
            rawPassOutput
          )
      ),
    { state: 'failed', output: '', error: 'Node did not execute' } as NodeExecutionResult,
    nodeTotalTimeoutMs(node),
    (attemptOutput, attempt, rawAttempt) =>
      checkpointHardenedAiAttempt(
        ctx.deps,
        ctx.workflowRun.id,
        ctx.budget,
        ctx.containerCtx,
        `Node '${node.id}' attempt ${String(attempt + 1)}`,
        ctx.budgetBaseUsage,
        { input: ctx.totalTokensIn, output: ctx.totalTokensOut },
        attemptOutput,
        rawAttempt
      )
  );
  await handleColdResumeWarning(ctx, node, resolved.provider, resumeSessionId, output);
  await persistLayerNodeSession(ctx, node, resolved.provider, output);
  return { nodeId: node.id, output, sessionProvider: resolved.provider };
}

async function handleColdResumeWarning(
  ctx: RunLayersContext,
  node: CommandNode | PromptNode,
  provider: string,
  resumeSessionId: string | undefined,
  output: NodeExecutionResult
): Promise<void> {
  if (resumeSessionId === undefined || output.state !== 'completed' || output.resumed !== false)
    return;
  const recoveryPointer = ctx.scopeArtifactsDir
    ? await buildColdResumeRecoveryPointer(ctx.scopeArtifactsDir, ctx.workflowRun.id, node.id)
    : '';
  getLog().warn(
    {
      nodeId: node.id,
      provider,
      workflowRunId: ctx.workflowRun.id,
      resumeSessionId: `${resumeSessionId.slice(0, 8)}…`,
      priorArtifactsFound: recoveryPointer !== '',
    },
    'dag.session_resume_failed'
  );
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `⚠️ Node \`${node.id}\`: could not resume the prior session — continued with a fresh session, so the earlier context was not restored.${recoveryPointer}`,
    { workflowId: ctx.workflowRun.id, nodeName: node.id }
  );
}

async function persistLayerNodeSession(
  ctx: RunLayersContext,
  node: CommandNode | PromptNode,
  provider: string,
  output: NodeExecutionResult
): Promise<void> {
  if (
    !nodeUsesPersistedScope(node, ctx.workflowPersistSessions) ||
    !ctx.persistScopeKey ||
    output.state !== 'completed'
  )
    return;
  try {
    if (output.sessionId !== undefined) {
      await ctx.deps.store.upsertWorkflowNodeSession({
        workflow_name: ctx.workflowName,
        node_id: node.id,
        scope_key: ctx.persistScopeKey,
        provider,
        provider_session_id: output.sessionId,
        last_run_id: ctx.workflowRun.id,
      });
    } else {
      await ctx.deps.store.deleteWorkflowNodeSessions({
        workflow_name: ctx.workflowName,
        scope_key: ctx.persistScopeKey,
        node_id: node.id,
        provider,
      });
    }
  } catch (err) {
    getLog().warn(
      {
        err: err as Error,
        nodeId: node.id,
        workflow: ctx.workflowName,
        scopeKey: ctx.persistScopeKey,
        provider,
      },
      'persist_session_upsert_failed'
    );
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      `⚠️ Could not persist the session for node \`${node.id}\` (${provider}). The next run will start this node fresh.`,
      { workflowId: ctx.workflowRun.id, nodeName: node.id }
    );
  }
}

async function dispatchLayerNode(
  ctx: RunLayersContext,
  node: DagNode,
  isParallelLayer: boolean
): Promise<LayerNodeResult> {
  if (isBashNode(node)) return executeBashLayerNode(ctx, node);
  if (isLoopNode(node)) return executeLoopLayerNode(ctx, node);
  if (isLoopGroupNode(node)) return executeLoopGroupLayerNode(ctx, node);
  if (isApprovalNode(node)) return executeApprovalLayerNode(ctx, node);
  if (isCancelNode(node)) return executeCancelLayerNode(ctx, node);
  if (isControllerActionNode(node))
    return { nodeId: node.id, output: await executeControllerActionNode(node, ctx) };
  if (isScriptNode(node)) return executeScriptLayerNode(ctx, node);
  if (isWorkflowNode(node))
    return { nodeId: node.id, output: await executeWorkflowNode(node, ctx) };
  return executeAiLayerNode(ctx, node as CommandNode | PromptNode, isParallelLayer);
}

async function executeLayerNode(
  ctx: RunLayersContext,
  node: DagNode,
  isParallelLayer: boolean
): Promise<LayerNodeResult> {
  try {
    const skipped = await maybeSkipLayerNode(ctx, node);
    return skipped ?? (await dispatchLayerNode(ctx, node, isParallelLayer));
  } catch (error) {
    const err = error as Error;
    if (ctx.budget && isHardenedBudgetError(err)) throw err;
    getLog().error({ err, nodeId: node.id }, 'dag_node_pre_execution_failed');
    ctx.deps.store
      .createWorkflowEvent({
        workflow_run_id: ctx.workflowRun.id,
        event_type: 'node_failed',
        step_name: ctx.stepNamePrefix + node.id,
        data: { error: err.message },
      })
      .catch((dbErr: Error) => {
        getLog().error({ err: dbErr, nodeId: node.id }, 'workflow_event_persist_failed');
      });
    getWorkflowEventEmitter().emit({
      type: 'node_failed',
      runId: ctx.workflowRun.id,
      nodeId: node.id,
      nodeName: node.command ?? node.id,
      error: err.message,
    });
    await safeSendMessage(
      ctx.platform,
      ctx.conversationId,
      `Node '${node.id}' failed before execution: ${err.message}`,
      { workflowId: ctx.workflowRun.id, nodeName: node.id }
    );
    return {
      nodeId: node.id,
      output: { state: 'failed' as const, output: '', error: err.message },
    };
  }
}

function accumulateLayerUsage(
  ctx: RunLayersContext,
  nodeId: string,
  output: NodeExecutionResult
): void {
  if (output.costUsd !== undefined) ctx.totalCostUsd += output.costUsd;
  if (
    output.tokens !== undefined &&
    Number.isFinite(output.tokens.input) &&
    Number.isFinite(output.tokens.output)
  ) {
    ctx.totalTokensIn += output.tokens.input;
    ctx.totalTokensOut += output.tokens.output;
  } else if (output.tokens !== undefined) {
    getLog().warn({ nodeId, tokens: output.tokens }, 'dag.usage_tokens_non_finite_ignored');
  }
  if (output.loopIterations !== undefined) ctx.totalLoopIterations += output.loopIterations;
}

async function writeCompletedNodeArtifacts(
  ctx: RunLayersContext,
  nodeId: string,
  output: NodeOutput,
  completedNode: DagNode | undefined
): Promise<void> {
  if (output.state !== 'completed' || !completedNode?.output_type) return;
  const meta = {
    nodeId,
    outputType: completedNode.output_type,
    runId: ctx.workflowRun.id,
    producedAt: new Date().toISOString(),
    sessionId: output.sessionId,
  };
  try {
    await writeNodeArtifact(ctx.artifactsDir, meta, output.output);
  } catch (err) {
    getLog().warn(
      { err: err as Error, nodeId, workflowRunId: ctx.workflowRun.id },
      'artifacts.write_failed'
    );
  }
  if (!ctx.scopeArtifactsDir || !nodeUsesPersistedScope(completedNode, ctx.workflowPersistSessions))
    return;
  try {
    await writeNodeArtifact(ctx.scopeArtifactsDir, meta, output.output);
  } catch (err) {
    getLog().warn(
      {
        err: err as Error,
        nodeId,
        workflowRunId: ctx.workflowRun.id,
        scopeArtifactsDir: ctx.scopeArtifactsDir,
      },
      'artifacts.scope_write_failed'
    );
  }
}

async function applyFulfilledLayerResult(
  ctx: RunLayersContext,
  result: LayerNodeResult,
  nodeById: Map<string, DagNode>,
  isParallelLayer: boolean
): Promise<boolean> {
  const { nodeId, output, sessionProvider } = result;
  accumulateLayerUsage(ctx, nodeId, output);
  const ledgerStatus = await readVerifiedHardenedBudgetStatus(ctx.containerCtx);
  const consumedBudget = ledgerStatus?.consumed ?? {
    input: ctx.budgetBaseUsage.input + ctx.totalTokensIn,
    output: ctx.budgetBaseUsage.output + ctx.totalTokensOut,
  };
  await persistWorkflowBudgetState(ctx.deps, ctx.workflowRun.id, ctx.budget, consumedBudget);
  assertWorkflowBudgetCanContinue(ctx.budget, `Node '${nodeId}'`, consumedBudget);
  ctx.nodeOutputs.set(nodeId, output);
  await writeCompletedNodeArtifacts(ctx, nodeId, output, nodeById.get(nodeId));
  if (output.state === 'completed' && !isParallelLayer && output.sessionId !== undefined) {
    ctx.lastSequentialSession =
      sessionProvider !== undefined
        ? { sessionId: output.sessionId, provider: sessionProvider }
        : undefined;
  }
  return output.state === 'failed';
}

async function handleRejectedLayerResult(
  ctx: RunLayersContext,
  result: PromiseRejectedResult,
  layerIdx: number
): Promise<boolean> {
  const err = result.reason instanceof Error ? result.reason : new Error(String(result.reason));
  if (ctx.budget && isHardenedBudgetError(err)) throw err;
  getLog().error({ err, layerIdx }, 'dag_node_unexpected_rejection');
  await safeSendMessage(
    ctx.platform,
    ctx.conversationId,
    `An unexpected error occurred executing a node in layer ${String(layerIdx)}. Check server logs.`,
    { workflowId: ctx.workflowRun.id }
  );
  return true;
}

async function processLayerResults(
  ctx: RunLayersContext,
  layer: DagNode[],
  layerIdx: number,
  layerResults: PromiseSettledResult<LayerNodeResult>[],
  isParallelLayer: boolean
): Promise<void> {
  const nodeById = new Map(layer.map(n => [n.id, n]));
  let layerHadFailure = false;
  for (const result of layerResults) {
    const failed =
      result.status === 'fulfilled'
        ? await applyFulfilledLayerResult(ctx, result.value, nodeById, isParallelLayer)
        : await handleRejectedLayerResult(ctx, result, layerIdx);
    layerHadFailure ||= failed;
  }
  if (layerHadFailure)
    getLog().warn({ layerIdx, nodeCount: layer.length }, 'dag_layer_had_failures');
}

async function shouldStopAfterLayer(ctx: RunLayersContext, layerIdx: number): Promise<boolean> {
  try {
    const dagStatus = await ctx.deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
    if (dagStatus === 'running') return false;
    const effectiveStatus = dagStatus ?? 'deleted';
    getLog().info(
      {
        workflowRunId: ctx.workflowRun.id,
        layerIdx,
        totalLayers: ctx.layers.length,
        status: effectiveStatus,
      },
      'dag.stop_detected_between_layers'
    );
    if (effectiveStatus !== 'paused') {
      await safeSendMessage(
        ctx.platform,
        ctx.conversationId,
        `⚠️ **Workflow stopped** (${effectiveStatus}): DAG execution stopped after layer ${String(layerIdx + 1)}/${String(ctx.layers.length)}`,
        { workflowId: ctx.workflowRun.id }
      );
    }
    return true;
  } catch (statusErr) {
    getLog().warn(
      { err: statusErr as Error, workflowRunId: ctx.workflowRun.id },
      'dag.status_check_failed'
    );
    return false;
  }
}

/**
 * Walk the topological `layers` of a DAG (or subgraph), executing each layer's nodes
 * concurrently, aggregating results into `ctx.nodeOutputs`, and accumulating usage into
 * `ctx`. Stops early (returns) when a between-layer status check sees a non-running run
 * state (paused/cancelled/deleted) — the caller always proceeds to its own terminal tally.
 *
 * Extracted verbatim from the former `executeDagWorkflow` layer loop; the only behavioral
 * addition is `ctx.stepNamePrefix` (empty for the top-level DAG → identical `step_name`s).
 * Shared by the top-level DAG and `executeLoopGroupNode`'s per-iteration body execution.
 */
async function runLayers(ctx: RunLayersContext): Promise<void> {
  const { layers } = ctx;
  // nodeOutputs + accumulators + lastSequentialSession are mutated in place on `ctx`.

  for (let layerIdx = 0; layerIdx < layers.length; layerIdx++) {
    const layer = layers[layerIdx];
    const isParallelLayer = layer.length > 1;

    if (isParallelLayer) {
      ctx.lastSequentialSession = undefined; // reset — parallel nodes can't share sessions
    }

    // Execute all nodes in the layer concurrently. `sessionProvider` is the resolved
    // provider that produced `output.sessionId` — set only by the session-producing
    // dispatch paths (AI command/prompt nodes and loop nodes) so the cursor write
    // below can tag the session with its owner (#1992).
    const layerResults = await Promise.allSettled(
      layer.map(node => executeLayerNode(ctx, node, isParallelLayer))
    );

    await processLayerResults(ctx, layer, layerIdx, layerResults, isParallelLayer);

    if (await shouldStopAfterLayer(ctx, layerIdx)) break;
  }
}

/**
 * Resolve the AI provider a node would use, WITHOUT the messaging/side effects
 * of `resolveNodeProviderAndModel` — just enough for the container capability
 * pre-flight. Mirrors the provider half of that resolver: `node.provider ??
 * workflowProvider`, then a model tier/alias ref may override the provider.
 */
function resolveNodeProviderForPreflight(
  node: DagNode,
  workflowProvider: string,
  aiProfile?: ResolvedAiProfile
): string {
  let provider: string = node.provider ?? workflowProvider;
  if (node.model && aiProfile) {
    const spec = resolveModelSpec(aiProfile, node.model);
    if (!isLiteralSpec(spec)) provider = spec.provider;
  }
  return provider;
}

/**
 * Collect providers used by AI nodes that CANNOT run inside a container
 * (`capabilities.containerExec === false`), recursing loop_group bodies. bash/
 * script/cancel nodes are deterministic (they exec via `docker exec` directly,
 * no provider) and are skipped; an approval node counts only when it has an
 * `on_reject` reprompt (the one AI turn it can spawn). Unknown providers are
 * skipped here — they fail later with a clearer "unknown provider" error.
 */
function assertContainerWorkflowSurfaces(
  nodes: readonly DagNode[],
  evidencePolicy?: WorkflowEvidencePolicy
): void {
  if (evidencePolicy?.required) {
    throw new Error(
      'Legacy presence-only evidence_policy is unsupported in container runs; controller finalization is required'
    );
  }
  for (const node of nodes) {
    if (node.mcp !== undefined) {
      throw new Error('Unpinned MCP configuration is unsupported in container runs');
    }
    if (isWorkflowNode(node)) {
      throw new Error(
        'Child workflows are unsupported in container runs until isolated child contexts and shared budgets are implemented'
      );
    }
    if (('command' in node && node.command) || (isLoopNode(node) && node.loop.command)) {
      throw new Error(
        'Mutable host command discovery is unsupported in container runs; use a pinned inline prompt'
      );
    }
    if (isScriptNode(node) && !isInlineScript(node.script)) {
      throw new Error(
        'Mutable host named-script discovery is unsupported in container runs; use a pinned inline script'
      );
    }
    if (isLoopGroupNode(node)) assertContainerWorkflowSurfaces(node.loop_group.nodes);
  }
}

export function collectContainerIncompatibleProviders(
  nodes: readonly DagNode[],
  workflowProvider: string,
  aiProfile?: ResolvedAiProfile
): Set<string> {
  const incompatible = new Set<string>();
  const check = (provider: string): void => {
    if (!isRegisteredProvider(provider)) return;
    if (!getProviderCapabilities(provider).containerExec) incompatible.add(provider);
  };
  const visit = (ns: readonly DagNode[]): void => {
    for (const node of ns) {
      if (
        isBashNode(node) ||
        isScriptNode(node) ||
        isCancelNode(node) ||
        isControllerActionNode(node) ||
        isWorkflowNode(node)
      ) {
        continue;
      }
      if (isLoopGroupNode(node)) {
        check(resolveNodeProviderForPreflight(node, workflowProvider, aiProfile));
        visit(node.loop_group.nodes);
        continue;
      }
      if (isApprovalNode(node)) {
        if (node.approval.on_reject) {
          check(resolveNodeProviderForPreflight(node, workflowProvider, aiProfile));
        }
        continue;
      }
      // command / prompt / loop → AI node
      check(resolveNodeProviderForPreflight(node, workflowProvider, aiProfile));
    }
  };
  visit(nodes);
  return incompatible;
}

/**
 * Emit + persist a container-lifecycle event (fire-and-forget DB write). Mirrors
 * the `container_created`/`container_destroyed` pattern already in this file so
 * the stop/resume/write-back phases surface in all three logging layers.
 */
function emitContainerLifecycleEvent(
  deps: WorkflowDeps,
  runId: string,
  phase: ContainerLifecyclePhase,
  eventType: ContainerLifecycleDbEvent,
  containerId?: string,
  data: Record<string, unknown> = {}
): void {
  getWorkflowEventEmitter().emit({
    type: 'container_lifecycle',
    runId,
    phase,
    ...(containerId ? { containerId } : {}),
  });
  deps.store
    .createWorkflowEvent({
      workflow_run_id: runId,
      event_type: eventType,
      step_name: 'container',
      data,
    })
    .catch((err: Error) => {
      getLog().error({ err, workflowRunId: runId, eventType }, 'workflow_event_persist_failed');
    });
}

/** Container-lifecycle phases carried by the emitter event (superset of the DB rows). */
type ContainerLifecyclePhase =
  | 'created'
  | 'stopped'
  | 'resumed'
  | 'destroyed'
  | 'writeback_requested'
  | 'writeback_applied'
  | 'writeback_discarded';

/** DB `workflow_events.event_type` values for container lifecycle. */
type ContainerLifecycleDbEvent =
  | 'container_created'
  | 'container_stopped'
  | 'container_resumed'
  | 'container_destroyed'
  | 'writeback_requested'
  | 'writeback_applied'
  | 'writeback_discarded';

/**
 * Suspend the container on pause (`docker stop`) so a multi-day wait costs ~0
 * resources. Best-effort: a suspend failure leaves the container running (a
 * resource leak the resume/teardown reclaims) but must NOT throw — throwing here
 * would mask the pause and flip the run to failed. Surfaced loud (error log +
 * platform note); the `container_stopped` event only fires on success.
 */
async function suspendContainerForPause(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  containerCtx: ContainerRunContext,
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  runId: string
): Promise<void> {
  try {
    await containerCtx.backend.suspend(containerCtx.envId);
    emitContainerLifecycleEvent(
      deps,
      runId,
      'stopped',
      'container_stopped',
      execContext.containerId
    );
    getLog().info({ runId, envId: containerCtx.envId }, 'dag.container_suspended_on_pause');
  } catch (err) {
    getLog().error(
      { err: err as Error, runId, envId: containerCtx.envId },
      'dag.container_suspend_on_pause_failed'
    );
    await safeSendMessage(
      platform,
      conversationId,
      `⚠️ Run paused, but its isolation container could not be stopped: ${
        (err as Error).message
      }. It keeps running until resume/teardown reclaims it.`,
      { workflowId: runId }
    );
  }
}

/** Render the write-back change summary + approve/reject instructions for the gate message. */
/**
 * Sanitize an AGENT-CONTROLLED string (a file path or symlink target) before it is
 * interpolated into the approval-gate message (R2-F3). The container agent chooses
 * these, so a raw newline could forge extra lines in the approver's view and Markdown
 * could forge formatting/links. We (1) replace every control char (C0/C1, incl.
 * newline/CR/tab) with a visible `?`, then (2) wrap the result in inline code with
 * backticks escaped, so the whole token renders literally and inertly regardless of
 * its content. Truncated to keep one entry from dominating the message.
 */
function sanitizeGateText(value: string): string {
  // eslint-disable-next-line no-control-regex -- deliberately matching control chars to neutralize them
  const noControl = value.replace(/[\u0000-\u001f\u007f-\u009f]/g, '?');
  const capped = noControl.length > 300 ? `${noControl.slice(0, 300)}…` : noControl;
  return `\`${capped.replace(/`/g, "'")}\``;
}

function renderWriteBackSummary(summary: OverlayChangeSummary): string {
  const { added, modified, deleted, symlinks, skipped, totalCount, truncated } = summary;
  // Faithfully represent what apply will do (M1): files by kind, symlinks as
  // `path -> target` with escaping ones flagged (apply REFUSES them), and the
  // entries apply will skip. The approver sees exactly what lands and what won't.
  // Every agent-controlled path/target is sanitized (R2-F3) so it can't forge lines
  // or Markdown in the approver's view.
  const preview = [
    ...added.map(p => `+ ${sanitizeGateText(p)}`),
    ...modified.map(p => `~ ${sanitizeGateText(p)}`),
    ...deleted.map(p => `- ${sanitizeGateText(p)}`),
    ...symlinks.map(
      s =>
        `${s.escapes ? '⚠ ' : ''}@ ${sanitizeGateText(s.path)} -> ${sanitizeGateText(s.target)}${s.escapes ? '  (ESCAPES — will be refused)' : ''}`
    ),
  ].slice(0, 25);
  const lines = [
    '**Container run finished — review the changes before they touch the live folder.**',
    '',
    `${totalCount} change(s): ${added.length} added, ${modified.length} modified, ${deleted.length} deleted, ${symlinks.length} symlink(s):`,
    ...preview.map(p => `  ${p}`),
  ];
  if (truncated || totalCount > preview.length) {
    lines.push(`  … and ${totalCount - preview.length} more`);
  }
  if (skipped.length > 0) {
    lines.push(
      '',
      `${skipped.length} entr${skipped.length === 1 ? 'y' : 'ies'} will be SKIPPED (special files / unsafe / escaping):`
    );
    for (const s of skipped.slice(0, 10)) {
      lines.push(`  ! ${sanitizeGateText(s.path)} (${sanitizeGateText(s.reason)})`);
    }
    if (skipped.length > 10) lines.push(`  … and ${skipped.length - 10} more`);
  }
  lines.push('', 'Approve to APPLY these changes to the live folder, or reject to discard them.');
  return lines.join('\n');
}

/**
 * The engine-level container write-back gate (Phase C). Runs after the last node
 * succeeds, and again on each resume (the DAG re-runs with every node skipped and
 * lands here). Returns:
 *  - `paused`    — pending an approval decision; the container was suspended.
 *  - `applied`   — the overlay diff landed on the live root (auto policy, or
 *                  resume-after-approve). Fall through to complete the run.
 *  - `discarded` — the overlay was discarded (resume-after-reject). Complete.
 *  - `skipped`   — empty diff; nothing to apply. Complete normally.
 */
async function runContainerWriteBackGate(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  runId: string,
  containerCtx: ContainerRunContext,
  execContext: Extract<ExecutionContext, { kind: 'container' }>
): Promise<'paused' | 'applied' | 'discarded' | 'skipped'> {
  const run = await deps.store.getWorkflowRun(runId);
  const meta = run?.metadata ?? {};
  const pending = meta.pending_writeback as
    | { envId: string; summary?: OverlayChangeSummary }
    | undefined;
  // Idempotent re-entry: a resume after the decision already applied/discarded on a
  // prior invocation just completes (L2 — never re-pause a resolved gate). Return the
  // HONEST outcome (`writeback_outcome`) so a re-entered DISCARDED run isn't mislabeled
  // as applied.
  if (meta.writeback_resolved === true) {
    return meta.writeback_outcome === 'discarded' ? 'discarded' : 'applied';
  }

  const rawApproval = meta.approval;
  const approval = isApprovalContext(rawApproval) ? rawApproval : undefined;
  const isWriteBackGate = approval?.type === 'writeback';

  // RESUME after the gate was raised. Decide STRICTLY on THIS gate's own resolution
  // (`metadata.approval.resolved` for the `type:'writeback'` context) — NEVER the
  // run-wide `approval_response` (a stale value from an earlier mid-DAG approval node
  // would auto-apply) and NEVER "anything but rejected" (a plain `/workflow resume`
  // carries no decision → must not apply). Unresolved ⇒ FAIL CLOSED (re-pause).
  if (pending && isWriteBackGate) {
    if (approval.resolved === 'approved') {
      // Retry-safe apply (R2-F4). CLAIM the apply atomically BEFORE mutating the live
      // root. Semantics:
      //  - `claimed` false ⇒ a concurrent/prior resume already claimed the apply (or a
      //    crash left it claimed AFTER a successful apply). Do NOT re-apply — no path
      //    applies twice — and complete as applied (the resume CAS already serializes
      //    resumes; the only skipped-apply window is a sub-ms crash between claim and
      //    the apply call, which leaves the volume preserved by H2 for manual recovery).
      //  - `claimed` true ⇒ we own the apply. On SUCCESS record `writeback_resolved`;
      //    on FAILURE release the claim so `workflow resume` can retry (H2), keep the
      //    volume, and rethrow so the run fails with the reconcile teardown message.
      const { claimed } = await deps.store.claimWriteback(runId);
      if (!claimed) {
        getLog().warn({ runId }, 'dag.writeback_apply_already_claimed');
        await deps.store
          .updateWorkflowRun(runId, {
            metadata: { writeback_resolved: true, writeback_outcome: 'applied' },
          })
          .catch(() => undefined);
        return 'applied';
      }
      let applied;
      try {
        applied = await containerCtx.backend.applyChanges(containerCtx.envId);
      } catch (applyErr) {
        await deps.store.releaseWritebackClaim(runId).catch((relErr: unknown) => {
          getLog().error({ err: relErr as Error, runId }, 'dag.writeback_release_claim_failed');
        });
        throw applyErr;
      }
      await deps.store.updateWorkflowRun(runId, {
        metadata: { writeback_resolved: true, writeback_outcome: 'applied' },
      });
      emitContainerLifecycleEvent(
        deps,
        runId,
        'writeback_applied',
        'writeback_applied',
        undefined,
        {
          files_applied: applied.filesApplied,
          files_deleted: applied.filesDeleted,
        }
      );
      await safeSendMessage(
        platform,
        conversationId,
        `✅ Applied to the live folder: ${applied.filesApplied} file(s) written, ${applied.filesDeleted} deleted.` +
          (applied.warnings.length > 0 ? `\n⚠️ ${applied.warnings.join('; ')}` : ''),
        { workflowId: runId }
      );
      return 'applied';
    }
    if (approval.resolved === 'rejected') {
      await containerCtx.backend.discardChanges(containerCtx.envId);
      await deps.store.updateWorkflowRun(runId, {
        metadata: { writeback_resolved: true, writeback_outcome: 'discarded' },
      });
      emitContainerLifecycleEvent(deps, runId, 'writeback_discarded', 'writeback_discarded');
      await safeSendMessage(
        platform,
        conversationId,
        '🗑️ Changes discarded — the live folder was left untouched. (The run itself succeeded; artifacts remain.)',
        { workflowId: runId }
      );
      return 'discarded';
    }
    // FAIL CLOSED: a resume reached the still-open gate with no decision (e.g. a bare
    // `/workflow resume`). Re-raise the gate rather than touching the live root.
    getLog().warn({ runId }, 'dag.writeback_resume_unresolved_repause');
    const summary =
      pending.summary ?? (await containerCtx.backend.finalize(containerCtx.envId)).changeSummary;
    await raiseWriteBackGate(
      deps,
      platform,
      conversationId,
      runId,
      containerCtx,
      execContext,
      summary
    );
    return 'paused';
  }

  // FIRST arrival: inspect the overlay diff.
  const finalize = await containerCtx.backend.finalize(containerCtx.envId);
  const summary = finalize.changeSummary;
  if (!finalize.requiresApproval || !summary || summary.totalCount === 0) {
    getLog().info({ runId }, 'dag.writeback_empty_diff_skipped');
    return 'skipped';
  }

  // `auto` policy: apply without pausing (logged). For unattended workflows.
  if (containerCtx.writeBack === 'auto') {
    // N1 — set the `pending_writeback` preserve marker BEFORE mutating the live root,
    // even in auto mode (which never pauses). If applyChanges throws partway, the run
    // fails with the marker set + unresolved, so the teardown PRESERVES the volume
    // (the un-applied remainder is recoverable) instead of destroying it. Cleared to
    // resolved on success so normal teardown cleanup proceeds. (No claim CAS here:
    // auto runs in one process; a resume of a failed auto run re-enters this first-
    // arrival path and re-applies idempotently.)
    await deps.store.updateWorkflowRun(runId, {
      metadata: { pending_writeback: { envId: containerCtx.envId } },
    });
    const applied = await containerCtx.backend.applyChanges(containerCtx.envId);
    await deps.store.updateWorkflowRun(runId, {
      metadata: { writeback_resolved: true, writeback_outcome: 'applied' },
    });
    emitContainerLifecycleEvent(deps, runId, 'writeback_applied', 'writeback_applied', undefined, {
      files_applied: applied.filesApplied,
      files_deleted: applied.filesDeleted,
      auto: true,
    });
    await safeSendMessage(
      platform,
      conversationId,
      `✅ Auto-applied ${applied.filesApplied} file(s) to the live folder (${applied.filesDeleted} deleted). ` +
        '(`container.write_back: auto` — no approval gate.)',
      { workflowId: runId }
    );
    getLog().info({ runId, filesApplied: applied.filesApplied }, 'dag.writeback_auto_applied');
    return 'applied';
  }

  // `approve` policy (default): raise the write-back gate (pause + suspend).
  await raiseWriteBackGate(
    deps,
    platform,
    conversationId,
    runId,
    containerCtx,
    execContext,
    summary
  );
  return 'paused';
}

/**
 * Raise (or re-raise) the write-back approval gate: pause the run with a synthetic
 * `type:'writeback'` ApprovalContext, persist `pending_writeback`, emit the events +
 * live pause signal, suspend the container, and message the user. Reused by the
 * first-arrival approve path AND the fail-closed re-pause on an unresolved resume.
 */
async function raiseWriteBackGate(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  runId: string,
  containerCtx: ContainerRunContext,
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  summary: OverlayChangeSummary | undefined
): Promise<void> {
  const message = summary
    ? renderWriteBackSummary(summary)
    : 'Container run finished — review before applying to the live folder.';
  // Fold `pending_writeback` into the SAME pause write so there is no window where the
  // run is paused-for-writeback without the resume marker (M3): pass it as extra
  // metadata alongside the approval context, both in one merged write.
  await deps.store.pauseWorkflowRun(
    runId,
    { nodeId: WRITEBACK_GATE_NODE_ID, message, type: 'writeback' },
    { pending_writeback: { envId: containerCtx.envId, ...(summary ? { summary } : {}) } }
  );
  emitContainerLifecycleEvent(
    deps,
    runId,
    'writeback_requested',
    'writeback_requested',
    undefined,
    {
      total_count: summary?.totalCount ?? 0,
    }
  );
  // Live pause signal for the CLI progress renderer + console dock (same event the
  // approval node emits, so the existing pause UI shows approve/reject).
  getWorkflowEventEmitter().emit({
    type: 'approval_pending',
    runId,
    nodeId: WRITEBACK_GATE_NODE_ID,
    message,
  });
  await suspendContainerForPause(deps, platform, conversationId, containerCtx, execContext, runId);
  await safeSendMessage(platform, conversationId, message, { workflowId: runId });
}

/**
 * Execute a complete DAG workflow.
 * Called from executeWorkflow() in executor.ts.
 */
interface ExecutableDagWorkflow extends WorkflowLevelOptions {
  name: string;
  nodes: readonly DagNode[];
  persist_sessions?: boolean;
  model?: string;
  evidence_policy?: WorkflowEvidencePolicy;
  hardened?: WorkflowHardenedPolicy;
}

function buildDagWorkflowLevelOptions(workflow: ExecutableDagWorkflow): WorkflowLevelOptions {
  const workflowTier = workflow.model && isTierName(workflow.model) ? workflow.model : undefined;
  return {
    effort: workflow.effort,
    thinking: workflow.thinking,
    fallbackModel: workflow.fallbackModel,
    betas: workflow.betas,
    sandbox: workflow.sandbox,
    workflowTier,
  };
}

function assertDagExecutionSurface(
  workflow: ExecutableDagWorkflow,
  workflowProvider: string,
  aiProfile: ResolvedAiProfile | undefined,
  execContext: ExecutionContext
): void {
  if (workflow.hardened?.required === true && !isHardenedContainerContext(execContext)) {
    throw new Error(
      `Workflow '${workflow.name}' requires hardened container execution with profile='hardened'.`
    );
  }
  if (execContext.kind !== 'container') return;
  assertContainerWorkflowSurfaces(workflow.nodes, workflow.evidence_policy);
  const incompatible = collectContainerIncompatibleProviders(
    workflow.nodes,
    workflowProvider,
    aiProfile
  );
  if (incompatible.size === 0) return;
  const list = [...incompatible].sort().join(', ');
  throw new Error(
    `Provider${incompatible.size === 1 ? '' : 's'} '${list}' cannot run inside a container yet (containerExec capability). Use provider claude, or run without --container.`
  );
}

async function emitDagContainerStart(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRun: WorkflowRun,
  execContext: ExecutionContext,
  containerCtx: ContainerRunContext | undefined,
  isResume: boolean
): Promise<void> {
  if (execContext.kind !== 'container') return;
  emitContainerLifecycleEvent(
    deps,
    workflowRun.id,
    isResume ? 'resumed' : 'created',
    isResume ? 'container_resumed' : 'container_created',
    execContext.containerId,
    { containerId: execContext.containerId }
  );
  if (containerCtx?.overlayMode !== 'native') return;
  await safeSendMessage(
    platform,
    conversationId,
    '⚠️ Container is running in NATIVE overlay mode (CAP_SYS_ADMIN). An adversarial agent could bypass the write-back review by remounting the project root — treat this run as accident-protection, not a sandbox against hostile code. (See SECURITY.md.)',
    { workflowId: workflowRun.id }
  );
  deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'container_created',
      step_name: 'container',
      data: { overlayMode: 'native', gateBypassable: true },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: workflowRun.id, eventType: 'container_created' },
        'workflow_event_persist_failed'
      );
    });
  getLog().warn({ workflowRunId: workflowRun.id }, 'dag.container_native_mode_gate_bypassable');
}

async function initializeWorkflowBudget(
  deps: WorkflowDeps,
  workflow: ExecutableDagWorkflow,
  workflowRun: WorkflowRun,
  workflowDigest: string,
  containerCtx: ContainerRunContext | undefined,
  priorCompletedNodes: Map<string, string> | undefined,
  priorTokenUsage: { input: number; output: number } | undefined
): Promise<ActiveWorkflowBudget | undefined> {
  const initialLedgerStatus = await readVerifiedHardenedBudgetStatus(containerCtx);
  const budgetPriorUsage =
    initialLedgerStatus?.consumed ??
    (hasUnknownPersistedBudgetState(workflowRun) ? undefined : priorTokenUsage);
  const budget = resolveWorkflowBudget(
    workflow,
    workflowRun,
    workflowDigest,
    deps.workflowBudgetGrants,
    budgetPriorUsage,
    priorCompletedNodes !== undefined || priorTokenUsage !== undefined,
    undefined,
    initialLedgerStatus !== undefined
  );
  if (initialLedgerStatus && budget) assertLedgerStatusMatchesBudget(initialLedgerStatus, budget);
  await persistWorkflowBudgetState(
    deps,
    workflowRun.id,
    budget,
    budget?.state.consumed ?? { input: 0, output: 0 }
  );
  return budget;
}

function prepopulateDagNodeOutputs(
  workflow: ExecutableDagWorkflow,
  workflowRun: WorkflowRun,
  priorCompletedNodes: Map<string, string> | undefined
): Map<string, NodeOutput> {
  const nodeOutputs = new Map<string, NodeOutput>();
  if (!priorCompletedNodes || priorCompletedNodes.size === 0) return nodeOutputs;
  const nodesById = new Map(workflow.nodes.map(n => [n.id, n]));
  let prepopulatedCount = 0;
  for (const [nodeId, output] of priorCompletedNodes) {
    const node = nodesById.get(nodeId);
    if (node?.always_run) continue;
    const declaredFields = declaredFieldsFromSchema(node?.output_format);
    nodeOutputs.set(nodeId, {
      state: 'completed',
      output,
      ...(declaredFields !== undefined ? { declaredFields } : {}),
    });
    prepopulatedCount++;
  }
  getLog().info(
    {
      workflowRunId: workflowRun.id,
      priorCompletedCount: priorCompletedNodes.size,
      prepopulatedCount,
      alwaysRunResumedCount: priorCompletedNodes.size - prepopulatedCount,
    },
    'dag.workflow_resume_prepopulated'
  );
  return nodeOutputs;
}

async function refreshPostLayerBudget(
  deps: WorkflowDeps,
  workflowRunId: string,
  containerCtx: ContainerRunContext | undefined,
  budget: ActiveWorkflowBudget | undefined
): Promise<void> {
  const postLayerLedgerStatus = await readVerifiedHardenedBudgetStatus(containerCtx);
  if (!postLayerLedgerStatus || !budget) return;
  assertLedgerStatusMatchesBudget(postLayerLedgerStatus, budget);
  await persistWorkflowBudgetState(deps, workflowRunId, budget, postLayerLedgerStatus.consumed);
}

async function suspendDagContainerIfPaused(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRun: WorkflowRun,
  execContext: ExecutionContext,
  containerCtx: ContainerRunContext | undefined
): Promise<boolean> {
  if (execContext.kind !== 'container' || !containerCtx) return false;
  const pausedStatus = await deps.store.getWorkflowRunStatus(workflowRun.id);
  if (pausedStatus !== 'paused') return false;
  await suspendContainerForPause(
    deps,
    platform,
    conversationId,
    containerCtx,
    execContext,
    workflowRun.id
  );
  return true;
}

async function skipIfWorkflowStatusChanged(
  deps: WorkflowDeps,
  workflowRun: WorkflowRun,
  logEvent: string
): Promise<boolean> {
  const status = await deps.store.getWorkflowRunStatus(workflowRun.id);
  if (status === 'running') return false;
  getLog().info({ workflowRunId: workflowRun.id, status: status ?? 'deleted' }, logEvent);
  if (status !== 'paused') getWorkflowEventEmitter().unregisterRun(workflowRun.id);
  return true;
}

function countDagNodeOutputs(
  nodeOutputs: Map<string, NodeOutput>,
  total: number
): { completed: number; failed: number; skipped: number; total: number } {
  const nodeCounts = { completed: 0, failed: 0, skipped: 0, total };
  for (const output of nodeOutputs.values()) {
    if (output.state === 'completed') nodeCounts.completed++;
    else if (output.state === 'failed') nodeCounts.failed++;
    else if (output.state === 'skipped') nodeCounts.skipped++;
  }
  return nodeCounts;
}

function dagRunUsageProps(runCtx: RunLayersContext): ReturnType<typeof buildRunUsageProps> {
  return buildRunUsageProps({
    costUsd: runCtx.totalCostUsd,
    tokensIn: runCtx.totalTokensIn,
    tokensOut: runCtx.totalTokensOut,
    loopIterations: runCtx.totalLoopIterations,
  });
}

async function failDagWorkflowRun(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  logDir: string,
  workflowRun: WorkflowRun,
  workflowName: string,
  failMsg: string
): Promise<void> {
  await deps.store.failWorkflowRun(workflowRun.id, failMsg).catch((dbErr: Error) => {
    getLog().error({ err: dbErr, workflowRunId: workflowRun.id }, 'dag_db_fail_failed');
  });
  await logWorkflowError(logDir, workflowRun.id, failMsg).catch((logErr: Error) => {
    getLog().error(
      { err: logErr, workflowRunId: workflowRun.id },
      'dag.workflow_error_log_write_failed'
    );
  });
  const emitter = getWorkflowEventEmitter();
  emitter.emit({ type: 'workflow_failed', runId: workflowRun.id, workflowName, error: failMsg });
  emitter.unregisterRun(workflowRun.id);
  await safeSendMessage(platform, conversationId, `❌ ${failMsg}`, { workflowId: workflowRun.id });
}

function failedDagNodeList(nodeOutputs: Map<string, NodeOutput>): string[] {
  return [...nodeOutputs.entries()].filter(([, o]) => o.state === 'failed').map(([id]) => id);
}

async function maybeFinalizeDagFailure(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  logDir: string,
  workflow: ExecutableDagWorkflow,
  workflowRun: WorkflowRun,
  source: WorkflowSource | undefined,
  workflowProvider: string,
  dagStartTime: number,
  nodeOutputs: Map<string, NodeOutput>,
  nodeCounts: { completed: number; failed: number; skipped: number; total: number },
  runUsageProps: ReturnType<typeof buildRunUsageProps>
): Promise<boolean> {
  const anyCompleted = nodeCounts.completed > 0;
  const anyFailed = nodeCounts.failed > 0;
  if (anyCompleted && !anyFailed) return false;
  if (await skipIfWorkflowStatusChanged(deps, workflowRun, 'dag.skip_fail_status_changed'))
    return true;
  const failureTaxonomy = firstFailedNodeTaxonomy(nodeOutputs, workflow.nodes);
  const exitReason = anyCompleted ? 'node_error' : 'no_nodes_completed';
  const failMsg = anyCompleted
    ? buildPartialDagFailureMessage(workflow.name, nodeOutputs)
    : buildNoSuccessDagFailureMessage(workflow.name, nodeOutputs, nodeCounts.skipped);
  captureWorkflowCompleted({
    outcome: 'failed',
    workflowName: workflow.name,
    workflowSource: source,
    provider: workflowProvider,
    durationMs: Date.now() - dagStartTime,
    nodesCompleted: nodeCounts.completed,
    nodesFailed: nodeCounts.failed,
    nodesSkipped: nodeCounts.skipped,
    nodesTotal: nodeCounts.total,
    exitReason,
    ...failureTaxonomy,
    ...runUsageProps,
  });
  await failDagWorkflowRun(
    deps,
    platform,
    conversationId,
    logDir,
    workflowRun,
    workflow.name,
    failMsg
  );
  return true;
}

function buildNoSuccessDagFailureMessage(
  workflowName: string,
  nodeOutputs: Map<string, NodeOutput>,
  skipped: number
): string {
  const failedNodes = failedDagNodeList(nodeOutputs);
  if (failedNodes.length === 0)
    return `DAG workflow '${workflowName}' completed with no successful nodes. Check node conditions, trigger rules, and upstream failures.`;
  return `DAG workflow '${workflowName}' failed: node${failedNodes.length > 1 ? 's' : ''} ${failedNodes.join(', ')} failed. ${skipped} downstream node${skipped !== 1 ? 's were' : ' was'} skipped.`;
}

function buildPartialDagFailureMessage(
  workflowName: string,
  nodeOutputs: Map<string, NodeOutput>
): string {
  const failedNodes = [...nodeOutputs.entries()]
    .filter(([, o]) => o.state === 'failed')
    .map(([id, o]) => `'${id}': ${o.state === 'failed' ? o.error : 'unknown'}`)
    .join('; ');
  return `DAG workflow '${workflowName}' completed with failures: ${failedNodes}`;
}

async function enforceDagEvidenceGate(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  logDir: string,
  artifactsDir: string,
  workflow: ExecutableDagWorkflow,
  workflowRun: WorkflowRun,
  source: WorkflowSource | undefined,
  workflowProvider: string,
  dagStartTime: number,
  nodeCounts: { completed: number; failed: number; skipped: number; total: number },
  runUsageProps: ReturnType<typeof buildRunUsageProps>
): Promise<boolean> {
  if (workflow.evidence_policy?.required !== true) return false;
  const evidencePath = joinPath(artifactsDir, 'evidence.json');
  if (existsSync(evidencePath)) {
    getLog().info({ workflowRunId: workflowRun.id, evidencePath }, 'dag.evidence_gate_passed');
    return false;
  }
  const failMsg = `DAG workflow '${workflow.name}' failed the evidence gate: evidence_policy.required is true but no evidence file exists at ${evidencePath}. All nodes succeeded — produce evidence.json from a bash/script node, then resume the run once the file exists.`;
  getLog().error({ workflowRunId: workflowRun.id, evidencePath }, 'dag.evidence_gate_failed');
  captureWorkflowCompleted({
    outcome: 'failed',
    workflowName: workflow.name,
    workflowSource: source,
    provider: workflowProvider,
    durationMs: Date.now() - dagStartTime,
    nodesCompleted: nodeCounts.completed,
    nodesFailed: nodeCounts.failed,
    nodesSkipped: nodeCounts.skipped,
    nodesTotal: nodeCounts.total,
    exitReason: 'evidence_missing',
    ...runUsageProps,
  });
  await deps.store
    .updateWorkflowRun(workflowRun.id, {
      metadata: {
        evidence_validation: {
          status: 'missing',
          policy: 'evidence_policy.required',
          expected_path: evidencePath,
          checked_at: new Date().toISOString(),
        },
      },
    })
    .catch((dbErr: Error) => {
      getLog().error(
        { err: dbErr, workflowRunId: workflowRun.id },
        'dag.evidence_metadata_write_failed'
      );
    });
  await deps.store.createWorkflowEvent({
    workflow_run_id: workflowRun.id,
    event_type: 'evidence_validation_failed',
    data: { policy: 'evidence_policy.required', expected_path: evidencePath },
  });
  await failDagWorkflowRun(
    deps,
    platform,
    conversationId,
    logDir,
    workflowRun,
    workflow.name,
    failMsg
  );
  return true;
}

function getTerminalDagOutput(
  workflow: ExecutableDagWorkflow,
  nodeOutputs: Map<string, NodeOutput>
): string | undefined {
  const allDependencies = new Set(workflow.nodes.flatMap(n => n.depends_on ?? []));
  return workflow.nodes
    .filter(n => !allDependencies.has(n.id))
    .map(n => nodeOutputs.get(n.id))
    .find(o => o?.state === 'completed' && o.output.trim().length > 0)?.output;
}

async function completeDagWorkflowRun(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  logDir: string,
  workflow: ExecutableDagWorkflow,
  workflowRun: WorkflowRun,
  source: WorkflowSource | undefined,
  workflowProvider: string,
  dagStartTime: number,
  nodeCounts: { completed: number; failed: number; skipped: number; total: number },
  runCtx: RunLayersContext,
  terminalOutput: string | undefined
): Promise<void> {
  try {
    await deps.store.completeWorkflowRun(workflowRun.id, {
      node_counts: nodeCounts,
      ...(runCtx.totalCostUsd > 0 ? { total_cost_usd: runCtx.totalCostUsd } : {}),
      ...(runCtx.totalTokensIn > 0 ? { total_tokens_in: runCtx.totalTokensIn } : {}),
      ...(runCtx.totalTokensOut > 0 ? { total_tokens_out: runCtx.totalTokensOut } : {}),
      ...(workflowRun.parent_run_id && terminalOutput ? { summary: terminalOutput } : {}),
    });
  } catch (dbErr) {
    getLog().error(
      { err: dbErr as Error, workflowRunId: workflowRun.id },
      'dag_db_complete_failed'
    );
    await safeSendMessage(
      platform,
      conversationId,
      'Warning: workflow completed but the run status could not be saved. The workflow result may appear inconsistent.',
      { workflowId: workflowRun.id }
    );
  }
  await logWorkflowComplete(logDir, workflowRun.id);
  const duration = Date.now() - dagStartTime;
  const emitter = getWorkflowEventEmitter();
  emitter.emit({
    type: 'workflow_completed',
    runId: workflowRun.id,
    workflowName: workflow.name,
    duration,
  });
  captureWorkflowCompleted({
    outcome: 'completed',
    workflowName: workflow.name,
    workflowSource: source,
    provider: workflowProvider,
    durationMs: duration,
    nodesCompleted: nodeCounts.completed,
    nodesFailed: nodeCounts.failed,
    nodesSkipped: nodeCounts.skipped,
    nodesTotal: nodeCounts.total,
    ...dagRunUsageProps(runCtx),
  });
  deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'workflow_completed',
      data: { duration_ms: duration },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: workflowRun.id, eventType: 'workflow_completed' },
        'workflow_event_persist_failed'
      );
    });
  emitter.unregisterRun(workflowRun.id);
}

async function runDagWriteBackGateIfNeeded(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflowRun: WorkflowRun,
  execContext: ExecutionContext,
  containerCtx: ContainerRunContext | undefined
): Promise<boolean> {
  if (execContext.kind !== 'container' || !containerCtx) return false;
  const gate = await runContainerWriteBackGate(
    deps,
    platform,
    conversationId,
    workflowRun.id,
    containerCtx,
    execContext
  );
  return gate === 'paused';
}

export async function executeDagWorkflow(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflow: ExecutableDagWorkflow,
  workflowRun: WorkflowRun,
  workflowProvider: string,
  workflowModel: string | undefined,
  artifactsDir: string,
  stateDir: string,
  logDir: string,
  baseBranch: string,
  docsDir: string,
  config: WorkflowConfig,
  configuredCommandFolder?: string,
  issueContext?: string,
  priorCompletedNodes?: Map<string, string>,
  /** Discovery source — telemetry only (custom-vs-default + name redaction). */
  source?: WorkflowSource,
  aiProfile?: ResolvedAiProfile,
  workflowPreset?: ModelAliasPreset,
  /**
   * Stable cross-invocation artifact scope dir (`scopes/<workflow>/<scope>/`),
   * resolved by executor.ts when the workflow uses session persistence (#1846).
   * Undefined otherwise — no mirroring, no cold-resume pointer.
   */
  scopeArtifactsDir?: string,
  /**
   * Execution context for this run (host by default; the container backend
   * threads a container context in Phase B). Threaded onto every node's
   * `RunLayersContext` so provider turns and subprocesses exec in the right place.
   */
  execContext: ExecutionContext = { kind: 'host' },
  /**
   * Container run context (Phase C): the write-back backend port + env id + policy.
   * Present only for container runs. Drives suspend-on-pause and the engine-level
   * write-back gate that runs after the last node before the run completes.
   */
  containerCtx?: ContainerRunContext,
  /**
   * Injected closure that starts a child sub-run for a `workflow:` node (#2121
   * Phase 2). executor.ts is the sole caller and passes it; other callers (unit
   * tests) may omit it, in which case a `workflow:` node fails fast.
   */
  runChildWorkflow?: RunChildWorkflowFn,
  /** Cumulative usage restored from prior node_completed events on resume. */
  priorTokenUsage?: { input: number; output: number }
): Promise<string | undefined> {
  const dagStartTime = Date.now();
  assertNoGuardedApprovalRework(workflowRun, execContext);
  assertDagExecutionSurface(workflow, workflowProvider, aiProfile, execContext);
  await emitDagContainerStart(
    deps,
    platform,
    conversationId,
    workflowRun,
    execContext,
    containerCtx,
    priorCompletedNodes !== undefined && priorCompletedNodes.size > 0
  );

  const workflowLevelOptions = buildDagWorkflowLevelOptions(workflow);
  const workflowDigest = computeControllerWorkflowDigest(workflow);
  const budget = await initializeWorkflowBudget(
    deps,
    workflow,
    workflowRun,
    workflowDigest,
    containerCtx,
    priorCompletedNodes,
    priorTokenUsage
  );
  const layers = buildTopologicalLayers(workflow.nodes);
  const nodeOutputs = prepopulateDagNodeOutputs(workflow, workflowRun, priorCompletedNodes);
  getLog().info(
    {
      workflowName: workflow.name,
      nodeCount: workflow.nodes.length,
      layerCount: layers.length,
      hasIssueContext: !!issueContext,
      issueContextLength: issueContext?.length ?? 0,
    },
    'dag_workflow_starting'
  );

  const persistScopeKey = workflowRun.conversation_id ?? undefined;
  const runCtx: RunLayersContext = {
    deps,
    platform,
    conversationId,
    cwd,
    execContext,
    containerCtx,
    runChildWorkflow,
    workflowRun,
    workflowName: workflow.name,
    workflowDigest,
    config,
    workflowProvider,
    workflowModel,
    workflowLevelOptions,
    aiProfile,
    workflowPreset,
    artifactsDir,
    stateDir,
    logDir,
    baseBranch,
    docsDir,
    configuredCommandFolder,
    issueContext,
    persistScopeKey,
    workflowPersistSessions: workflow.persist_sessions === true,
    scopeArtifactsDir: persistScopeKey !== undefined ? scopeArtifactsDir : undefined,
    layers,
    nodeOutputs,
    priorCompletedNodes,
    lastSequentialSession: undefined,
    totalCostUsd: 0,
    totalTokensIn: budget?.state.consumed.input ?? priorTokenUsage?.input ?? 0,
    totalTokensOut: budget?.state.consumed.output ?? priorTokenUsage?.output ?? 0,
    totalLoopIterations: 0,
    budget,
    budgetBaseUsage: { input: 0, output: 0 },
    stepNamePrefix: '',
  };

  await runLayers(runCtx);
  await refreshPostLayerBudget(deps, workflowRun.id, containerCtx, runCtx.budget);
  await snapshotDrainedContainerArtifacts(containerCtx, execContext, artifactsDir);
  if (
    await suspendDagContainerIfPaused(
      deps,
      platform,
      conversationId,
      workflowRun,
      execContext,
      containerCtx
    )
  )
    return;

  const nodeCounts = countDagNodeOutputs(nodeOutputs, workflow.nodes.length);
  const runUsageProps = dagRunUsageProps(runCtx);
  getLog().info(
    {
      nodeCount: workflow.nodes.length,
      anyCompleted: nodeCounts.completed > 0,
      anyFailed: nodeCounts.failed > 0,
    },
    'dag_workflow_finished'
  );
  if (
    await maybeFinalizeDagFailure(
      deps,
      platform,
      conversationId,
      logDir,
      workflow,
      workflowRun,
      source,
      workflowProvider,
      dagStartTime,
      nodeOutputs,
      nodeCounts,
      runUsageProps
    )
  )
    return;
  if (await skipIfWorkflowStatusChanged(deps, workflowRun, 'dag.skip_complete_status_changed'))
    return;
  if (
    await enforceDagEvidenceGate(
      deps,
      platform,
      conversationId,
      logDir,
      artifactsDir,
      workflow,
      workflowRun,
      source,
      workflowProvider,
      dagStartTime,
      nodeCounts,
      runUsageProps
    )
  )
    return;
  if (
    await runDagWriteBackGateIfNeeded(
      deps,
      platform,
      conversationId,
      workflowRun,
      execContext,
      containerCtx
    )
  )
    return;

  const terminalOutput = getTerminalDagOutput(workflow, nodeOutputs);
  await completeDagWorkflowRun(
    deps,
    platform,
    conversationId,
    logDir,
    workflow,
    workflowRun,
    source,
    workflowProvider,
    dagStartTime,
    nodeCounts,
    runCtx,
    terminalOutput
  );
  return terminalOutput;
}
