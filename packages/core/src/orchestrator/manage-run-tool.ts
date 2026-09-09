import type { NativeTool } from '@archon/providers/types';
import { createLogger } from '@archon/paths';
import { isApprovalContext } from '@archon/workflows/schemas/workflow-run';
import type { WorkflowRun } from '@archon/workflows/schemas/workflow-run';
import { listDashboardRuns, findWorkflowRunsByIdPrefix } from '../db/workflows';
import { abandonWorkflow, resumeWorkflow } from '../operations/workflow-operations';

const log = createLogger('orchestrator.manage_run');

export interface ManageRunContext {
  /** The project (codebase) this chat is scoped to. */
  codebaseId: string;
  /**
   * Launch a workflow in the background and return a user-facing result line
   * (including a friendly error for an unknown name). Omitted when the dispatch
   * context isn't available — `start` is then rejected.
   */
  startWorkflow?: (workflowName: string, message: string) => Promise<string>;
}

const DESTRUCTIVE_ACTIONS = new Set(['cancel', 'abandon']);
const HUMAN_ONLY_GATE =
  'manage_run: approve/reject are human-only decisions. A human must use direct operator controls; model confirmation cannot authorize a human gate. Do not invoke approval commands through agent tools.';

/** Every action the tool understands, in catalog order. */
const ACTIONS = ['help', 'list', 'get', 'start', 'resume', 'cancel', 'abandon'] as const;
type Action = (typeof ACTIONS)[number];

const INPUT_SCHEMA: Record<string, unknown> = {
  type: 'object',
  properties: {
    action: {
      type: 'string',
      enum: [...ACTIONS],
      description:
        "What to do. Call action='help' (optionally with subtool=<action>) to see exactly what each action needs before using it.",
    },
    subtool: {
      type: 'string',
      description: "For action=help: the action to describe (e.g. 'resume'). Omit for an overview.",
    },
    runId: {
      type: 'string',
      description:
        'Run id — required for get/resume/cancel/abandon. Accepts the short (8-char) or full id.',
    },
    workflow: {
      type: 'string',
      description: 'Workflow name to launch — required for action=start.',
    },
    message: {
      type: 'string',
      description: 'Prompt/instructions for action=start.',
    },
    confirm: {
      type: 'boolean',
      description:
        'Required (true) to actually perform a destructive action (cancel/abandon). Omit first to get a preview.',
    },
  },
  required: ['action'],
};

// ─── Progressive-disclosure help text ───────────────────────────────────────

const HELP_OVERVIEW = [
  'manage_run — inspect and operate this project’s workflow runs.',
  '',
  'Actions (call action=help subtool=<name> for details):',
  '  list     — recent runs in this project (id, workflow, status, step). No params.',
  '  get      — one run’s detail. Params: runId.',
  '  start    — launch a workflow in the background. Params: workflow, message.',
  '  resume   — check a failed/paused run can resume from completed nodes. Params: runId.',
  '  cancel   — mark a running run cancelled. Params: runId, confirm=true.',
  '  abandon  — discard a paused/failed run. Params: runId, confirm=true.',
  '',
  'Destructive actions (cancel/abandon) need confirm=true; call once',
  'without it to preview, confirm with the user, then call again with confirm=true.',
].join('\n');

const HELP_BY_ACTION: Record<Exclude<Action, 'help'>, string> = {
  list: 'list — recent runs for this project, most recent first. No parameters. Returns id · workflow · status · current step.',
  get: 'get — full detail for one run. Required: runId (short or full). Returns status, start/finish times, and error if any. Scoped to this project.',
  start:
    'start — launch a workflow in the background. Required: workflow (name). Recommended: message (what it should do). It appears in the runs list and the workflow dock.',
  resume:
    'resume — validate that a failed/paused run can resume from its completed nodes. Required: runId. Does NOT re-run it — it stays in its current status; continue it from the run’s controls or by re-invoking the workflow.',
  cancel:
    'cancel — mark a running (non-terminal) run cancelled. Required: runId, confirm=true. Irreversible. A process already executing may finish its current step before it stops.',
  abandon:
    'abandon — discard a paused/failed (non-terminal) run. Required: runId, confirm=true. Irreversible: the run becomes cancelled.',
};

/**
 * The `manage_run` native tool. Lets a project-scoped chat agent inspect and
 * operate this project’s workflow runs — list/get (read), start (launch), and
 * the lifecycle writes resume/cancel/abandon — without the user
 * typing slash commands.
 *
 * Design:
 *  - One tool, an `action` discriminator, and a `help` action for progressive
 *    disclosure (the model learns each action’s params on demand, keeping the
 *    tool surface small).
 *  - Writes mutate state through the same core `workflow-operations` functions
 *    the CLI and command-handler use — identical, proven semantics.
 *  - Every by-id action is project-scoped via `getScopedRun`, so an agent in
 *    one project cannot read or mutate another project’s run.
 *  - Destructive actions are gated on `confirm: true` (see DESTRUCTIVE_ACTIONS).
 *
 * The handler closes over the live `codebaseId`, so `@archon/providers` never
 * imports core — the tool crosses the boundary as data on SendQueryOptions.
 * Errors are caught and returned as text; nothing throws into the agent loop.
 */
export function buildManageRunTool(ctx: ManageRunContext): NativeTool {
  return {
    name: 'manage_run',
    description:
      "Inspect and operate this project's workflow runs (list, get, start, resume, cancel, abandon). Call action='help' first to see what each action needs. Destructive actions require confirm=true.",
    inputSchema: INPUT_SCHEMA,
    handler: async (input): Promise<string> => {
      // Switch on the raw string; unknown values fall through to `default`. No
      // assertion to `Action` — the switch's case labels narrow it for us.
      const action = typeof input.action === 'string' ? input.action : '';
      try {
        switch (action) {
          case 'help':
            return handleHelp(typeof input.subtool === 'string' ? input.subtool.trim() : '');
          case 'list':
            return await handleList(ctx);
          case 'get': {
            const runId = typeof input.runId === 'string' ? input.runId.trim() : '';
            if (runId === '') return 'manage_run: action=get requires a runId.';
            const run = await getScopedRun(runId, ctx);
            return typeof run === 'string' ? run : formatRunDetail(run);
          }
          case 'start':
            return await handleStart(ctx, input);
          case 'approve':
          case 'reject':
            return HUMAN_ONLY_GATE;
          case 'resume':
          case 'cancel':
          case 'abandon':
            return await handleWrite(ctx, action, input);
          default:
            return `manage_run: unknown action '${action}'. Call action=help for the list.`;
        }
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        const runId = typeof input.runId === 'string' ? input.runId : undefined;
        log.error({ err: e, action, runId, codebaseId: ctx.codebaseId }, 'manage_run.failed');
        return `manage_run error: ${msg}`;
      }
    },
  };
}

// ─── Read handlers ──────────────────────────────────────────────────────────

function handleHelp(subtool: string): string {
  if (subtool === '') return HELP_OVERVIEW;
  if (subtool === 'approve' || subtool === 'reject') return HUMAN_ONLY_GATE;
  const detail = HELP_BY_ACTION[subtool as Exclude<Action, 'help'>];
  if (detail === undefined) {
    return `manage_run: no help for '${subtool}'. Known actions: ${Object.keys(HELP_BY_ACTION).join(', ')}.`;
  }
  return detail;
}

async function handleList(ctx: ManageRunContext): Promise<string> {
  const { runs } = await listDashboardRuns({ codebaseId: ctx.codebaseId, limit: 20 });
  log.info({ codebaseId: ctx.codebaseId, count: runs.length }, 'manage_run.list_completed');
  if (runs.length === 0) return 'No workflow runs for this project yet.';

  const lines = runs.map(r => {
    const step =
      r.current_step_name !== null
        ? ` · ${r.current_step_name}${r.total_steps !== null ? `/${r.total_steps.toString()}` : ''}`
        : '';
    return `- ${r.id.slice(0, 8)} · ${r.workflow_name} · ${r.status}${step}`;
  });
  return `${runs.length.toString()} run(s) (most recent first):\n${lines.join('\n')}`;
}

/**
 * Runtime-safe timestamp formatter. The run schema declares these fields as
 * Date, but rows are cast, never Zod-parsed: Postgres hydrates TIMESTAMPTZ
 * into Date objects while SQLite returns TEXT ('YYYY-MM-DD HH:MM:SS', UTC)
 * as-is (#2078). Mirrors the API serializer pattern (routes/api.ts
 * toISOString): pass strings through verbatim — re-parsing with new Date()
 * would misread the UTC wall-clock string as local time — and format Dates.
 */
function formatTimestamp(val: Date | string): string {
  return typeof val === 'string' ? val : val.toISOString();
}

function formatRunDetail(run: WorkflowRun): string {
  const parts = [
    `Run ${run.id.slice(0, 8)} · ${run.workflow_name}`,
    `status: ${run.status}`,
    `started: ${formatTimestamp(run.started_at)}`,
  ];
  if (run.completed_at !== null) parts.push(`finished: ${formatTimestamp(run.completed_at)}`);
  const error = run.metadata.error;
  if (typeof error === 'string' && error.length > 0) parts.push(`error: ${error.slice(0, 300)}`);
  const rawApproval = run.metadata.approval;
  if (
    run.status === 'paused' &&
    isApprovalContext(rawApproval) &&
    rawApproval.type === 'interactive_loop'
  ) {
    parts.push(
      `gate: awaiting approval (node ${rawApproval.nodeId}, iteration ${String(rawApproval.iteration ?? '?')})`
    );
    parts.push(`completionSignaled: ${rawApproval.completionSignaled === true ? 'true' : 'false'}`);
    if (rawApproval.completionSignaled === true) {
      parts.push('A human must resolve this gate through the operator UI or CLI.');
    }
    const excerpt = (rawApproval.signaledOutput ?? '').trim().slice(0, 300);
    if (excerpt) parts.push(`output: ${excerpt}`);
  }
  log.info({ runId: run.id, status: run.status }, 'manage_run.get_completed');
  return parts.join('\n');
}

// ─── Write handlers ─────────────────────────────────────────────────────────

async function handleStart(ctx: ManageRunContext, input: Record<string, unknown>): Promise<string> {
  if (ctx.startWorkflow === undefined) {
    return 'manage_run: launching workflows is not available in this context.';
  }
  const workflow = typeof input.workflow === 'string' ? input.workflow.trim() : '';
  if (workflow === '') return 'manage_run: action=start requires a workflow name.';
  const message = typeof input.message === 'string' ? input.message.trim() : '';
  log.info({ codebaseId: ctx.codebaseId, workflow }, 'manage_run.start_requested');
  return await ctx.startWorkflow(workflow, message);
}

async function handleWrite(
  ctx: ManageRunContext,
  action: 'resume' | 'cancel' | 'abandon',
  input: Record<string, unknown>
): Promise<string> {
  const runId = typeof input.runId === 'string' ? input.runId.trim() : '';
  if (runId === '') return `manage_run: action=${action} requires a runId.`;
  const run = await getScopedRun(runId, ctx);
  if (typeof run === 'string') return run;
  if (DESTRUCTIVE_ACTIONS.has(action) && input.confirm !== true) {
    log.info({ runId: run.id, action }, 'manage_run.confirm_preview');
    return `⚠️ This will ${action} run ${run.id.slice(0, 8)} (${run.workflow_name}), currently '${run.status}' — irreversible. Confirm with the user, then call manage_run again with confirm: true to proceed.`;
  }
  log.info({ runId: run.id, action }, 'manage_run.write_requested');
  const id = run.id;
  if (action === 'resume') {
    const resumed = await resumeWorkflow(id);
    return `Run ${resumed.id.slice(0, 8)} (${resumed.workflow_name}) can resume from its completed nodes. It does not restart automatically — continue it from the run’s controls or by re-invoking the workflow.`;
  }
  const { run: cancelled, cascadeFailures, blockedParentRunId } = await abandonWorkflow(id);
  let msg = `Cancelled run ${cancelled.id.slice(0, 8)} (${cancelled.workflow_name}).`;
  if (cascadeFailures > 0) {
    msg += ` Warning: ${String(cascadeFailures)} sub-run(s) could not be cancelled and may still be running.`;
  }
  if (blockedParentRunId) {
    msg += ` Parent run ${blockedParentRunId.slice(0, 8)} was blocked on this sub-run and stays paused — resume it to fail the node cleanly, or abandon it too.`;
  }
  return msg;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/**
 * Resolve a run id — the short prefix shown in listings OR a full id — to a run
 * in THIS chat's project. The lookup is scoped to `codebaseId` in the query, so
 * an agent in project A can never read or mutate project B's runs: a foreign id
 * simply resolves to nothing. Returns the run, or a user-facing string on miss
 * or ambiguous prefix.
 */
async function getScopedRun(runId: string, ctx: ManageRunContext): Promise<WorkflowRun | string> {
  const matches = await findWorkflowRunsByIdPrefix(runId, ctx.codebaseId);
  if (matches.length > 1) {
    return `manage_run: id '${runId}' matches more than one run — use more characters or the full id.`;
  }
  const [run] = matches;
  if (run === undefined) {
    return `manage_run: no run found for id '${runId}' in this project.`;
  }
  return run;
}
