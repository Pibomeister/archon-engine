/**
 * Workflow Executor - runs DAG-based workflows
 */
import { mkdir, writeFile } from 'fs/promises';
import { existsSync } from 'fs';
import { dirname } from 'path';
import type { IWorkflowPlatform, WorkflowMessageMetadata } from './deps';
import type { WorkflowDeps, WorkflowConfig } from './deps';
import * as archonPaths from '@archon/paths';
import { createLogger, captureWorkflowInvoked, captureWorkflowCompleted } from '@archon/paths';
import { getDefaultBranch, toRepoPath } from '@archon/git';
import type {
  DagNode,
  WorkflowDefinition,
  WorkflowRun,
  WorkflowExecutionResult,
  WorkflowSource,
} from './schemas';
import {
  isLoopNode,
  isLoopGroupNode,
  isApprovalNode,
  isScriptNode,
  isBashNode,
  isApprovalContext,
  isRunBlockedOnChild,
  SUBRUN_METADATA_KEYS,
} from './schemas';
import { executeDagWorkflow, childOutcomeFromRun } from './dag-executor';
import type { RunChildWorkflowArgs, ChildWorkflowOutcome } from './dag-executor';
import { discoverWorkflowsWithConfig } from './workflow-discovery';
import { maybeWarnLegacyStatePath, maybeWarnLegacyArtifactsPath } from './state-migration';
import { resolveWorkflowName } from './router';
import { logWorkflowStart, logWorkflowError } from './logger';
import { formatDuration, parseDbTimestamp } from './utils/duration';
import { keepAwake } from './utils/keep-awake';
import { getWorkflowEventEmitter } from './event-emitter';
import { isRegisteredProvider, getRegisteredProviders } from '@archon/providers';
import type { ExecutionContext } from '@archon/providers/types';
import type { ContainerRunContext } from './container-context';
export type { ContainerRunContext, ContainerWriteBackBackend } from './container-context';
import type { ChildIsolationResolver, ChildIsolationResult } from './child-isolation';
export type {
  ChildIsolationResolver,
  ChildIsolationRequest,
  ChildIsolationResult,
} from './child-isolation';
import {
  classifyError,
  toTelemetryErrorClass,
  safeSendMessage,
  type SendMessageContext,
} from './executor-shared';
import { resolveGithubTokenOverrides } from './utils/github-token-policy';
import { buildAiProfile, isLiteralSpec, resolveModelSpec } from './model-validation';
import type { ModelAliasPreset, ResolvedAiProfile } from './model-validation';
import {
  WORKFLOW_PIN_METADATA_KEY,
  assertWorkflowPinMatchesRun,
  buildWorkflowPinState,
  workflowRunRequiresPin,
} from './workflow-pinning';

/** The per-user prefs layer as returned by `WorkflowDeps.getUserAiPrefs`. */
type UserAiPrefsLayer = Awaited<ReturnType<NonNullable<WorkflowDeps['getUserAiPrefs']>>>;

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('workflow.executor');
  return cachedLog;
}

/**
 * Delay execution for specified milliseconds
 */
function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Send a critical message with retry logic.
 * Used for failure/completion notifications that the user must receive.
 */
async function sendCriticalMessage(
  platform: IWorkflowPlatform,
  conversationId: string,
  message: string,
  context?: SendMessageContext,
  maxRetries = 3,
  metadata?: WorkflowMessageMetadata
): Promise<boolean> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await platform.sendMessage(conversationId, message, metadata);
      return true;
    } catch (error) {
      const err = error as Error;
      const errorType = classifyError(err);

      getLog().error(
        {
          err,
          conversationId,
          messageLength: message.length,
          errorType,
          platformType: platform.getPlatformType(),
          ...context,
          attempt,
          maxRetries,
        },
        'platform.critical_message_send_failed'
      );

      // Don't retry fatal errors
      if (errorType === 'FATAL') {
        break;
      }

      // Wait before retry (exponential backoff: 1s, 2s, 3s...)
      if (attempt < maxRetries) {
        await delay(1000 * attempt);
      }
    }
  }

  // Log prominently so operators can manually notify user
  getLog().error(
    { conversationId, messagePreview: message.slice(0, 100), ...context },
    'critical_message_delivery_failed'
  );

  return false;
}

/**
 * Parse `owner/repo` from a github.com URL. Returns null for non-GitHub URLs
 * so the caller can fall through to env-inheritance.
 *
 *   https://github.com/owner/repo.git   → { owner, repo }
 *   https://github.com/owner/repo       → { owner, repo }
 *   git@github.com:owner/repo.git       → { owner, repo }
 *   <anything else>                     → null
 */
function parseGithubRepoUrl(url: string): { owner: string; repo: string } | null {
  // HTTPS form
  const https = /^https?:\/\/(?:www\.)?github\.com\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/i.exec(url);
  if (https) return { owner: https[1], repo: https[2] };
  // SSH form (git@github.com:owner/repo[.git])
  const ssh = /^git@github\.com:([^/]+)\/([^/]+?)(?:\.git)?$/i.exec(url);
  if (ssh) return { owner: ssh[1], repo: ssh[2] };
  return null;
}

/**
 * Resolve a fresh GH_TOKEN/GITHUB_TOKEN pair from the registered bot-token
 * provider, if any. Used at the top of executeWorkflow to inject the token
 * into the workflow's envVars so bash/script subprocesses pick it up.
 *
 * Contract: NEVER THROWS. On any failure (no codebase, non-GitHub URL,
 * provider rejected, network blip) returns {} — the workflow continues with
 * whatever env inheritance was already in place. This matches the
 * resolveBotGitHubToken? contract in deps.ts.
 */
async function resolveBotGitHubEnvForWorkflow(
  deps: WorkflowDeps,
  codebaseId: string | undefined
): Promise<Record<string, string>> {
  if (!codebaseId || !deps.resolveBotGitHubToken) return {};
  try {
    const codebase = await deps.store.getCodebase(codebaseId);
    if (!codebase?.repository_url) return {};
    const parsed = parseGithubRepoUrl(codebase.repository_url);
    if (!parsed) return {};
    const token = await deps.resolveBotGitHubToken(parsed.owner, parsed.repo);
    if (!token) return {};
    getLog().debug(
      { owner: parsed.owner, repo: parsed.repo },
      'workflow.bot_github_token_injected'
    );
    return { GH_TOKEN: token, GITHUB_TOKEN: token };
  } catch (err) {
    // Resolution failure must not block the workflow — log and fall back.
    getLog().warn({ err: err as Error, codebaseId }, 'workflow.bot_github_token_resolve_failed');
    return {};
  }
}

/**
 * Resolve per-user GitHub token overrides for a run. When per-user mode is on
 * and the run has an originating user, this routes `gh`/`git push` through the
 * user's personal token — or scrubs the org/bot token when they haven't
 * connected (see {@link resolveGithubTokenOverrides}). Returns {} (no opinion)
 * for server-initiated runs and solo installs, leaving the bot env untouched.
 */
async function resolveUserGithubEnvForWorkflow(
  deps: WorkflowDeps,
  userId: string | undefined
): Promise<Record<string, string>> {
  const perUserEnabled = deps.isPerUserGitHubEnabled?.() ?? false;
  if (!perUserEnabled) return {};
  let userToken: string | undefined;
  if (userId && deps.getUserGithubToken) {
    try {
      userToken = await deps.getUserGithubToken(userId);
    } catch (err) {
      getLog().warn({ err: err as Error, userId }, 'workflow.user_github_token_resolve_failed');
    }
  }
  return resolveGithubTokenOverrides(perUserEnabled, userId, userToken);
}

/**
 * Resolve per-user AI-provider credential env (Phase 2) for a run, and write
 * any file-based deliveries (e.g. Codex `CODEX_HOME/auth.json`) under the
 * run's artifacts directory. Returns the env bag to merge LAST into
 * `config.envVars` so a connected user's keys win over file/db/bot-github
 * env. Returns `{}` when per-user provider keys are disabled, no userId is
 * present, or the deps adapter is absent.
 *
 * Contract: NEVER THROWS. Adapter failures are logged and yield `{}` so the
 * workflow continues with whatever env inheritance was already in place.
 */
async function resolveUserProviderEnvForWorkflow(
  deps: WorkflowDeps,
  userId: string | undefined,
  artifactsDir: string
): Promise<Record<string, string>> {
  const perUserEnabled = deps.isPerUserProviderKeysEnabled?.() ?? false;
  if (!perUserEnabled || !userId || !deps.getUserProviderEnv) return {};
  try {
    // TODO(#1891 PR-3): when Codex OAuth delivery is enabled, file-write failures
    // must drop only the affected provider's env keys, not all of them. Move file
    // writes into getUserProviderEnv per-delivery so env + write are atomic
    // per-provider, or wrap each write in a per-file try-catch that strips the
    // matching env keys on failure. Currently safe: no OAuth rows can be created
    // in PR-1 so `files` is always empty and this loop never executes.
    const { env, files } = await deps.getUserProviderEnv(userId, artifactsDir);
    for (const f of files) {
      await mkdir(dirname(f.path), { recursive: true });
      await writeFile(f.path, f.contents, { encoding: 'utf8', mode: 0o600 });
    }
    const envKeys = Object.keys(env);
    if (envKeys.length > 0) {
      getLog().debug({ userId, keys: envKeys }, 'workflow.user_provider_env_injected');
    }
    return env;
  } catch (err) {
    getLog().warn({ err: err as Error, userId }, 'workflow.user_provider_env_resolve_failed');
    return {};
  }
}

/**
 * Whether the run's codebase is a folder project (non-git). Folder projects run
 * in place on a non-git root, so git base-branch auto-detection can only fail
 * (`fatal: not a git repository`) — skip it to avoid the ERROR/WARN log-spam
 * pair on every folder run (#2159). `$BASE_BRANCH` keeps its
 * referenced-but-unresolvable failure semantics (empty string when skipped).
 *
 * Contract: NEVER THROWS. A lookup failure returns `false` so the normal
 * auto-detection path still runs, preserving prior behavior on any DB hiccup.
 */
async function isFolderCodebase(
  deps: WorkflowDeps,
  codebaseId: string | undefined
): Promise<boolean> {
  if (!codebaseId) return false;
  try {
    const codebase = await deps.store.getCodebase(codebaseId);
    return codebase?.kind === 'folder';
  } catch (err) {
    getLog().warn({ err: err as Error, codebaseId }, 'workflow.folder_kind_resolve_failed');
    return false;
  }
}

/** The four run-scoped output directories plus the project root they hang off. */
export interface ResolvedProjectPaths {
  artifactsDir: string;
  logDir: string;
  artifactsRoot: string;
  /** `$STATE_DIR` — per-PROJECT cross-run state, shared by every workflow. */
  stateDir: string;
  /** The project root persisted to `workflow_runs.output_root`. */
  outputRoot: string;
}

/**
 * Resolve the output directories for a workflow run.
 *
 * Resolution order:
 *  1. A persisted `output_root` (from the run row) wins outright — a run that
 *     already recorded where its output lives must never re-derive it, or a
 *     renamed codebase (#1192) would orphan its artifacts mid-run.
 *  2. Otherwise look the codebase up once and delegate to the single shared
 *     identity→paths resolver in `@archon/paths`, which handles repo,
 *     `_local`, and folder projects.
 *  3. With no codebase (or a lookup failure, or an unresolvable identity) the
 *     run falls back to the `_cwd/<basename>` pseudo-project — still UNDER
 *     `ARCHON_HOME`. This used to write into `<cwd>/.archon/`, i.e. the user's
 *     repository; relocating it is the breaking change accepted in #2200 so
 *     that every run's output survives worktree teardown and is retrievable.
 *
 * `artifactsRoot` is the parent of the `runs/` layout (`.../artifacts`) — the base
 * that run-scoped (`runs/<id>/`) and scope-scoped (`scopes/<workflow>/<scope>/`,
 * #1846) storage both hang off, whichever branch resolved it.
 *
 * Exported for unit testing of the kind-based branch selection.
 */
export async function resolveProjectPaths(
  deps: WorkflowDeps,
  cwd: string,
  workflowRunId: string,
  codebaseId?: string,
  opts?: { persistedOutputRoot?: string | null }
): Promise<ResolvedProjectPaths> {
  if (opts?.persistedOutputRoot) {
    // The engine only ever persists an in-tree root, so an out-of-tree value is
    // corruption or a hand edit. Acting on it would let a relative or
    // whitespace root scatter this run's artifacts AND its shared state under
    // whatever the server's cwd happens to be. Ignore it and re-derive: the run
    // still lands somewhere correct, and the write-once guard means we never
    // overwrite the bad value silently. Readers apply the same boundary.
    if (archonPaths.isInsideArchonHome(opts.persistedOutputRoot)) {
      return composeRunPaths(
        archonPaths.getStoragePathsForRoot(opts.persistedOutputRoot),
        workflowRunId
      );
    }
    getLog().error(
      { workflowRunId, persistedOutputRoot: opts.persistedOutputRoot },
      'workflow.output_root_outside_archon_home'
    );
  }

  let key: archonPaths.ProjectStorageKey | undefined;
  if (codebaseId) {
    // Retried once (#2304). A failing lookup drops the run onto the `_cwd/<basename>`
    // pseudo-project, and because `output_root` is write-once that location is then
    // pinned for the run's whole life — including its `$STATE_DIR`, so a stateful
    // workflow silently reads an empty state directory. Failing the run instead was
    // considered and rejected: the fallback exists precisely because a registry blip
    // must not kill a run.
    //
    // What the retry is worth, honestly, differs by dialect:
    //   • Postgres — it earns its place. A stale or broken pooled connection is exactly
    //     the fault an immediate retry clears by drawing a fresh one, and this is the
    //     only app-level DB retry in the tree. Zero delay is CORRECT here; backoff would
    //     add latency for nothing.
    //   • SQLite (the default install) — weak. `PRAGMA busy_timeout = 5000` means
    //     SQLITE_BUSY cannot surface as a throw until five seconds of sustained
    //     contention have already elapsed, so what reaches us is by construction not
    //     transient, and retrying at that instant retries the moment least likely to
    //     have cleared. Kept because it costs one attempt and cannot make things worse.
    //
    // The deeper question — whether an unresolved identity should be recorded on the
    // row so "unregistered" and "we could not tell" are distinguishable — stays open
    // in #2304.
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const codebase = await deps.store.getCodebase(codebaseId);
        if (codebase) {
          key = archonPaths.resolveProjectStorageKey(codebase, cwd);
          if (key.kind === 'cwd') {
            // The codebase exists but neither an owner/repo nor a _local identity
            // could be derived from it — the run still gets external storage, but
            // keyed on the working directory rather than the project.
            getLog().warn(
              { codebaseName: codebase.name, cwd: codebase.default_cwd },
              'codebase_project_identity_unresolved'
            );
          }
        }
        break;
      } catch (error) {
        if (attempt === 0) {
          getLog().warn(
            { err: error as Error, codebaseId, cwd },
            'workflow.project_paths_lookup_retrying'
          );
          continue;
        }
        getLog().error(
          { err: error as Error, codebaseId, cwd },
          'project_paths_resolve_failed_using_fallback'
        );
      }
    }
  }

  return composeRunPaths(
    archonPaths.getProjectStoragePaths(key ?? { kind: 'cwd', cwd }),
    workflowRunId
  );
}

/** Project-level roots → the run-scoped view the executor threads downstream. */
function composeRunPaths(
  storage: archonPaths.ProjectStoragePaths,
  workflowRunId: string
): ResolvedProjectPaths {
  return {
    artifactsDir: archonPaths.getRunArtifactsDirForRoot(storage.root, workflowRunId),
    logDir: storage.logsDir,
    artifactsRoot: storage.artifactsRoot,
    stateDir: storage.stateRoot,
    outputRoot: storage.root,
  };
}

/**
 * Resolve the stable cross-invocation artifact scope dir for a run (#1846), or
 * undefined when the feature doesn't apply. Applies only when the workflow uses
 * cross-run session persistence (workflow-level `persist_sessions` or any node
 * `persist_session: true`) AND the run has a conversation scope — the same
 * opt-in + scope key the session store uses. No persistence → no new dirs,
 * default behavior unchanged.
 */
export function resolveScopeArtifactsDir(
  workflow: { name: string; nodes: readonly DagNode[]; persist_sessions?: boolean },
  conversationId: string | null | undefined,
  artifactsRoot: string
): string | undefined {
  if (!conversationId) return undefined;
  const usesPersistence =
    workflow.persist_sessions === true ||
    workflow.nodes.some(n => 'persist_session' in n && n.persist_session === true);
  if (!usesPersistence) return undefined;
  return archonPaths.getScopeArtifactsPath(artifactsRoot, workflow.name, conversationId);
}

/**
 * Resume state may only appear together with `preCreatedRun` — passing prior
 * outputs or usage without the resumed row would silently inject state into a
 * freshly-created run. Lock-token rows (used by `dispatchBackgroundWorkflow`)
 * supply `preCreatedRun` alone.
 */
type ResumePayload =
  | {
      preCreatedRun: WorkflowRun;
      priorCompletedNodes?: Map<string, string>;
      priorTokenUsage?: { input: number; output: number };
    }
  | {
      preCreatedRun?: undefined;
      priorCompletedNodes?: undefined;
      priorTokenUsage?: undefined;
    };

/**
 * Optional parameters for {@link executeWorkflow}. All trailing args live here
 * so call sites stay readable as new options accrue.
 *
 * To resume a prior run, obtain the run, prior outputs, and prior usage from
 * {@link hydrateResumableRun} (or look up via `findResumableRun` and hydrate)
 * and spread them in. The executor never queries the store for a prior run on
 * its own; that decision belongs at the call site.
 */
export type ExecuteWorkflowOptions = ResumePayload & {
  /** Codebase ID for env vars + isolation context. */
  codebaseId?: string;
  /**
   * Caller-provided base branch fallback for `$BASE_BRANCH`, normally the
   * codebase's stored `default_branch`. Repo config still wins when
   * `worktree.baseBranch` is set, and `baseOverride` wins over both; git
   * auto-detection remains the last resort.
   */
  baseBranch?: string;
  /**
   * Per-dispatch base-branch override (CLI `--base <branch>`), the top
   * precedence level for `$BASE_BRANCH` — above repo config and the codebase
   * default. Mirrors `IsolationRequest.baseOverride`, which does the same for
   * the worktree cut-from, so one flag drives both halves of "base". Passing
   * the override through `baseBranch` instead would rank it BELOW
   * `worktree.baseBranch`, so a repo with that config set would cut its
   * worktree from the override while reporting the configured branch here.
   */
  baseOverride?: string;
  /**
   * GitHub issue/PR context. When provided:
   * - Stored in `WorkflowRun.metadata` as `{ github_context }`
   * - Substituted into `$CONTEXT` / `$EXTERNAL_CONTEXT` / `$ISSUE_CONTEXT` variables
   * - Appended to prompts that reference none of those variables
   * Expected format: Markdown with title, author, labels, and body.
   */
  issueContext?: string;
  /** Worktree / branch metadata for isolation-aware nodes. */
  isolationContext?: {
    branchName?: string;
    isPrReview?: boolean;
    prSha?: string;
    prBranch?: string;
  };
  /**
   * Discovery source of the workflow (bundled / global / project). Used only
   * for anonymous telemetry — bundled workflows report their real name, custom
   * ones report `"custom"`. Optional: defaults to the `"custom"`/project
   * treatment when a caller doesn't thread it through.
   */
  source?: WorkflowSource;
  /**
   * Keys the engine dropped from this workflow's YAML (#2213), as produced by
   * discovery. Recorded on the run as a `workflow_parse_warnings` event at
   * start, so the finding survives independently of whether the chat/console
   * notification could be delivered — and so it exists for CLI- and REST-started
   * runs, which have no conversation to post into. Optional: a caller that
   * doesn't thread it through simply records nothing.
   */
  parseWarnings?: readonly string[];
  /** Parent conversation ID — enables approve/reject auto-resume from chat. */
  parentConversationId?: string;
  /**
   * Archon user UUID for attribution on the workflow_run row. Resolved by
   * chat/forge adapters via findOrCreateUserByPlatformIdentity. Web/CLI paths
   * pass undefined until their own auth surfaces are wired.
   * Ignored when `preCreatedRun` is set — the original creator's attribution
   * is preserved on resume.
   */
  userId?: string;
  /**
   * Execution context resolved by the isolation seam: `{ kind: 'host' }` (default)
   * runs on the Archon host; `{ kind: 'container', … }` (folder-project container
   * backend, Phase B) runs provider turns and subprocesses inside the prepared
   * container. Threaded verbatim into `executeDagWorkflow`. Defaults to host when
   * absent, so every existing caller is unchanged.
   */
  execContext?: ExecutionContext;
  /**
   * Container run context (folder-project container backend, Phase C). Present
   * only when `execContext.kind === 'container'`; carries the prepared env id, the
   * write-back policy, and the backend port the engine drives for suspend +
   * write-back. Absent for host runs.
   */
  container?: ContainerRunContext;
  /**
   * Per-child isolation resolver (#2121 slice 2, PR-A). A structural port the
   * engine calls once per `workflow:` child whose node declares
   * `isolation: 'worktree'`, to obtain a per-child worktree cwd + branch. Built by
   * the caller (CLI/orchestrator via `@archon/core`) over `WorktreeProvider` so
   * `@archon/workflows` never imports `@archon/isolation`. Absent → a
   * `isolation: 'worktree'` node fails fast (never a silent shared-checkout
   * fallback). Threaded into the child-spawn closure.
   */
  resolveChildIsolation?: ChildIsolationResolver;
};

/**
 * Hydrate an already-located resumable `WorkflowRun` candidate into the form
 * {@link executeWorkflow} expects. Returns `null` when the candidate has no
 * completed nodes and no interactive-loop gate state — nothing worth resuming.
 *
 * The return shape is spread-compatible with {@link ExecuteWorkflowOptions}
 * so callers can write `executeWorkflow(..., { ...hydrated, codebaseId })`.
 *
 * Throws on database errors; callers decide whether to surface or fall
 * through. The executor itself never performs this lookup — silent fallback
 * inside the executor was the cross-invocation auto-resume bug, so it stays
 * at the call site.
 */
export async function hydrateResumableRun(
  deps: WorkflowDeps,
  candidate: WorkflowRun
): Promise<{
  preCreatedRun: WorkflowRun;
  priorCompletedNodes: Map<string, string>;
  priorTokenUsage: { input: number; output: number };
} | null> {
  const snapshot = await deps.store.getDagResumeSnapshot(candidate.id);
  const priorCompletedNodes = snapshot.completedNodeOutputs;
  // A gate whose node deliberately writes NO node_completed on pause must still be
  // resumable with zero completed nodes: interactive loops, and a `workflow:` node
  // blocked on a child (#2121 Phase 2) whose child is the very first node.
  const approvalType =
    candidate.metadata?.approval !== undefined
      ? (candidate.metadata.approval as Record<string, unknown>).type
      : undefined;
  const hasReRunGateState =
    approvalType === 'interactive_loop' || approvalType === 'child_workflow';
  if (priorCompletedNodes.size === 0 && !hasReRunGateState) {
    getLog().info(
      { resumableRunId: candidate.id },
      'workflow.dag_resume_skipped_no_completed_nodes'
    );
    return null;
  }
  const preCreatedRun = await deps.store.resumeWorkflowRun(candidate.id);
  getLog().info(
    { workflowRunId: preCreatedRun.id, priorCompletedCount: priorCompletedNodes.size },
    'workflow.dag_resuming'
  );
  return { preCreatedRun, priorCompletedNodes, priorTokenUsage: snapshot.tokens };
}

/** Depth cap on the `workflow:` sub-run tree (D9). A node nested deeper fails fast. */
const CHILD_WORKFLOW_DEPTH_CAP = 5;

/** Safety bound on the descendant walk (guards a corrupted run tree). */
const MAX_DESCENDANT_RUNS = 64;

/**
 * Collect the transitive descendant run ids of `rootId` via a bounded downward walk
 * of `parent_run_id` (#2121 Phase 2). Used to exclude a run's own sub-run children
 * from its path-lock: they share the checkout by design, so a parent resumed while
 * still blocked on a paused child must not self-cancel against that child. Throws
 * are the caller's to handle (it fails closed).
 */
async function gatherDescendantRunIds(deps: WorkflowDeps, rootId: string): Promise<string[]> {
  const out: string[] = [];
  const queue: string[] = [rootId];
  const seen = new Set<string>([rootId]);
  let processed = 0;
  while (queue.length > 0 && processed < MAX_DESCENDANT_RUNS) {
    const id = queue.shift();
    if (id === undefined) break;
    processed++;
    const children = await deps.store.findChildRuns(id);
    for (const c of children) {
      if (seen.has(c.id)) continue;
      seen.add(c.id);
      out.push(c.id);
      queue.push(c.id);
    }
  }
  return out;
}

function childFailOutcome(error: string, childRunId = ''): ChildWorkflowOutcome {
  return { childRunId, status: 'failed', error };
}

async function resolveChildWorkflowForNode(
  deps: WorkflowDeps,
  cwd: string,
  childWorkflowName: string
): Promise<WorkflowDefinition | ChildWorkflowOutcome> {
  try {
    const { workflows } = await discoverWorkflowsWithConfig(cwd, deps.loadConfig);
    const childWorkflow = resolveWorkflowName(
      childWorkflowName,
      workflows.map(w => w.workflow)
    );
    return childWorkflow ?? childFailOutcome(`Unknown sub-run workflow '${childWorkflowName}'.`);
  } catch (err) {
    return childFailOutcome(
      `Failed to resolve sub-run '${childWorkflowName}': ${(err as Error).message}`
    );
  }
}

async function validateChildWorkflowAncestry(
  deps: WorkflowDeps,
  parentRun: WorkflowRun,
  childWorkflow: WorkflowDefinition
): Promise<ChildWorkflowOutcome | null> {
  let ancestry: WorkflowRun[];
  try {
    ancestry = [parentRun, ...(await deps.store.getRunAncestry(parentRun.id))];
  } catch (err) {
    return childFailOutcome(
      `Failed to resolve run ancestry for sub-run guard: ${(err as Error).message}`
    );
  }
  if (ancestry.some(a => a.workflow_name === childWorkflow.name)) {
    return childFailOutcome(
      `Sub-run cycle detected: '${childWorkflow.name}' is already an ancestor of this run.`
    );
  }
  if (ancestry.length >= CHILD_WORKFLOW_DEPTH_CAP) {
    return childFailOutcome(
      `Sub-run depth cap (${String(CHILD_WORKFLOW_DEPTH_CAP)}) exceeded nesting '${childWorkflow.name}'.`
    );
  }
  return null;
}

interface ChildCwdResolution {
  childCwd: string;
  childIsolationEnv?: ChildIsolationResult;
}

async function resolveChildWorkflowCwd(
  args: RunChildWorkflowArgs,
  resolveChildIsolation?: ChildIsolationResolver
): Promise<ChildCwdResolution | ChildWorkflowOutcome> {
  const {
    childWorkflowName,
    cwd,
    parentRun,
    nodeId,
    childIndex,
    codebaseId,
    isolation,
    resumeFailedChild,
  } = args;
  if (resumeFailedChild) {
    const priorPath = resumeFailedChild.working_path;
    if (priorPath && !existsSync(priorPath)) {
      return childFailOutcome(
        `Cannot resume sub-run '${childWorkflowName}': its working path no longer exists ` +
          `(${priorPath}). The worktree may have been cleaned up — start a fresh run.`,
        resumeFailedChild.id
      );
    }
    if (!priorPath) {
      return childFailOutcome(
        `Cannot resume sub-run '${childWorkflowName}': its run row has no recorded working ` +
          'path, so the checkout it ran in is unknown — start a fresh run.',
        resumeFailedChild.id
      );
    }
    return { childCwd: priorPath };
  }
  if (isolation !== 'worktree') return { childCwd: cwd };
  if (!resolveChildIsolation) {
    return childFailOutcome(
      `isolation: 'worktree' on sub-run '${childWorkflowName}' requires an injected ` +
        'child-isolation resolver (available for git-repo codebases run via the CLI or ' +
        "orchestrator). Remove the isolation or use 'inherit' (shared checkout)."
    );
  }
  try {
    const childIsolationEnv = await resolveChildIsolation.resolve({
      parentRun,
      nodeId,
      childIndex,
      codebaseId,
    });
    return { childCwd: childIsolationEnv.cwd, childIsolationEnv };
  } catch (err) {
    return childFailOutcome(
      `Failed to create isolated worktree for sub-run '${childWorkflowName}': ${(err as Error).message}`
    );
  }
}

function isChildWorkflowOutcome(
  value: ChildCwdResolution | WorkflowDefinition | ChildWorkflowOutcome
): value is ChildWorkflowOutcome {
  return 'status' in value && 'childRunId' in value;
}

/**
 * Start (or resume a failed) child workflow run in-process for a `workflow:` node
 * (#2121 Phase 2). Reuses the FULL executeWorkflow lifecycle for the child —
 * run-record creation, path-lock, artifacts, credential/model resolution, resume,
 * terminal output — and returns the child's node-facing outcome.
 *
 * The runtime cycle guard + depth cap live here (include:'s guard is load-time and
 * does not cover runtime targets). The child shares the parent's checkout;
 * executeWorkflow derives the ancestor chain from the child's own parent_run_id
 * to exclude it from the path-lock.
 * Never throws — every failure is returned as a `{ status: 'failed' }` outcome so
 * the calling node fails cleanly rather than the whole DAG throwing. (Every step,
 * including the recursive executeWorkflow call, is guarded — keep it that way.)
 */
async function runChildWorkflow(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  args: RunChildWorkflowArgs,
  resolveChildIsolation?: ChildIsolationResolver
): Promise<ChildWorkflowOutcome> {
  const {
    parentRun,
    nodeId,
    childWorkflowName,
    input,
    conversationId,
    conversationDbId,
    userId,
    codebaseId,
    childIndex,
    itemHash,
    resumeFailedChild,
  } = args;

  const childWorkflowResult = await resolveChildWorkflowForNode(deps, args.cwd, childWorkflowName);
  if (isChildWorkflowOutcome(childWorkflowResult)) return childWorkflowResult;
  const childWorkflow = childWorkflowResult;

  const ancestryError = await validateChildWorkflowAncestry(deps, parentRun, childWorkflow);
  if (ancestryError) return ancestryError;

  const cwdResult = await resolveChildWorkflowCwd(args, resolveChildIsolation);
  if (isChildWorkflowOutcome(cwdResult)) return cwdResult;
  const { childCwd, childIsolationEnv } = cwdResult;

  // 4. Create the child run row (fresh) or hydrate the failed one (resume path).
  let childOpts: ExecuteWorkflowOptions;
  let childRunId: string;
  // Thread the resolver into every child so a NESTED grandchild `workflow:` node can
  // also request its own worktree (nesting is first-class up to the depth cap) — the
  // recursive executeWorkflow otherwise has no resolver and would fail-fast. (The
  // sibling `container:` context has the same non-propagation gap today; out of scope
  // for this PR, but noted so it isn't mistaken for intentional.)
  try {
    if (resumeFailedChild) {
      const hydrated = await hydrateResumableRun(deps, resumeFailedChild);
      if (hydrated) {
        childOpts = { ...hydrated, codebaseId, resolveChildIsolation };
        childRunId = hydrated.preCreatedRun.id;
      } else {
        // Failed child with no completed nodes — flip it back to running and re-run
        // from the top (nothing to skip).
        const preCreatedRun = await deps.store.resumeWorkflowRun(resumeFailedChild.id);
        childOpts = { preCreatedRun, codebaseId, resolveChildIsolation };
        childRunId = preCreatedRun.id;
      }
    } else {
      const childRun = await deps.store.createWorkflowRun({
        workflow_name: childWorkflow.name,
        conversation_id: conversationDbId,
        codebase_id: codebaseId,
        user_message: input,
        working_path: childCwd,
        parent_run_id: parentRun.id,
        // Share the parent's parent_conversation_id back-link so approve/reject
        // auto-resume scoping keeps working for the child on chat platforms.
        parent_conversation_id: parentRun.parent_conversation_id ?? undefined,
        user_id: userId,
        metadata: {
          [SUBRUN_METADATA_KEYS.parentNodeId]: nodeId,
          // Fan-out instance index (slice 2, PR-C) — stamped only for a fan-out child so
          // parent resume can re-key the ordered instance set by index (findChildRuns is
          // started_at-ordered, which ≠ items order under max_parallel concurrency). A
          // single (non-fan-out) child carries no child_index. The item-content hash rides
          // alongside so resume can WARN on a non-deterministic producer (same index, new item).
          ...(childIndex !== undefined ? { [SUBRUN_METADATA_KEYS.childIndex]: childIndex } : {}),
          ...(itemHash !== undefined ? { [SUBRUN_METADATA_KEYS.fanOutItemHash]: itemHash } : {}),
          // Record the child's own worktree env + branch (mirrors the container path's
          // isolation_env_id) so `isolation list` correlation + PR-E console grouping
          // can find it. Absent for `inherit`/shared-checkout children.
          ...(childIsolationEnv
            ? {
                isolation_env_id: childIsolationEnv.envId,
                branch_name: childIsolationEnv.branchName,
              }
            : {}),
        },
      });
      childOpts = { preCreatedRun: childRun, codebaseId, resolveChildIsolation };
      childRunId = childRun.id;
    }
  } catch (err) {
    return childFailOutcome(
      `Failed to create sub-run '${childWorkflowName}': ${(err as Error).message}`
    );
  }

  // 5. Run the child in-process (reuses the whole lifecycle) in its resolved cwd
  //    (its own worktree when isolated, else the parent's checkout). Its terminal
  //    output + cost + tokens land in the child run metadata on completion.
  try {
    await executeWorkflow(
      deps,
      platform,
      conversationId,
      childCwd,
      childWorkflow,
      input,
      conversationDbId,
      childOpts
    );

    // 6. Read the child back for the node-facing outcome (status + summary + cost +
    //    tokens). Works for synchronous completion AND a child paused at its gate.
    const finalChild = await deps.store.getWorkflowRun(childRunId);
    if (!finalChild) {
      return childFailOutcome('Child run row disappeared after execution.', childRunId);
    }
    return childOutcomeFromRun(finalChild);
  } catch (err) {
    // Honor the never-throws contract: executeWorkflow can throw from its early
    // setup (before its own failWorkflowRun catch-all), and the read-back can
    // throw on a DB error — both must surface as a failed node outcome, not an
    // exception unwinding the parent's DAG.
    //
    // Wedge guard (symmetric to maybeResumeParentRun's post-CAS handler): a throw in
    // executeWorkflow's EARLY setup (config load, getCodebaseEnvVars, token
    // resolution) fires BEFORE the status→running flip and BEFORE its own catch-all,
    // stranding the pre-created child at 'pending' (or 'running' on a later window) —
    // a non-terminal row that holds the working-path lock. `cancelWorkflowRun` (NOT
    // failWorkflowRun, whose `WHERE status='running'` would miss the 'pending' case)
    // flips any non-terminal child to 'cancelled' and no-ops on a child that reached
    // completed/cancelled on its own. childRunId is always assigned once step 3 ran.
    await deps.store.cancelWorkflowRun(childRunId).catch((cancelErr: unknown) => {
      getLog().error({ err: cancelErr as Error, childRunId }, 'workflow.child_setup_cancel_failed');
    });
    return childFailOutcome(
      `Sub-run '${childWorkflowName}' errored: ${(err as Error).message}`,
      childRunId
    );
  }
}

function logMalformedChildWorkflowGate(parent: WorkflowRun, childRunId: string): void {
  const approval = isApprovalContext(parent.metadata?.approval)
    ? parent.metadata.approval
    : undefined;
  if (approval?.type === 'child_workflow' && !approval.childRunId) {
    getLog().error(
      { parentRunId: parent.id, childRunId },
      'workflow.parent_resume_malformed_gate_missing_child_run_id'
    );
  }
}

/**
 * After a `workflow:` sub-run reaches a terminal state, re-enter its PARENT run if
 * the parent is paused blocked on THIS child (#2121 Phase 2). This is the D5 hook
 * that turns a child's completion into a parent resume — the cross-run analogue of
 * a human approve, driven in the same process.
 *
 * No-op (guarded) when:
 *  - the parent is not 'paused' (synchronous first-run path: the parent is still
 *    'running' on the call stack — output threads directly from the returned outcome),
 *  - the parent's gate isn't a `child_workflow` gate, or
 *  - a DIFFERENT child of the same parent terminated (childRunId mismatch).
 *
 * Never throws — the child's own result must not be corrupted by a parent-resume
 * failure. Every await is guarded here (a parent-side failure is logged, and a
 * post-CAS failure marks the parent 'failed' so it stays resumable); the caller's
 * `.catch` is a belt-and-braces backstop, not the contract.
 *
 * `resolveChildIsolation` is a plain parameter rather than part of the resume state:
 * {@link ResumePayload} carries what was RECORDED about the prior run, and a resolver
 * is a live capability of the surface driving this process — it cannot be rehydrated
 * from a run row. It has to be forwarded because the parent picks up here *mid-DAG*:
 * a parent whose gated child just finished may still have `isolation: 'worktree'`
 * nodes ahead of it, and re-entering without the resolver fails them with
 * "requires an injected child-isolation resolver" even though the surface wired one.
 * The child's resolver is the right one to pass: a child inherits the parent's
 * `codebase_id`, and the resolver is codebase-bound and rejects a mismatch loudly.
 */
async function maybeResumeParentRun(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  conversationDbId: string,
  childRun: WorkflowRun,
  resolveChildIsolation?: ChildIsolationResolver
): Promise<void> {
  const parentRunId = childRun.parent_run_id;
  if (!parentRunId) return;

  // Surface a reconciliation failure to the user with a manual-recovery pointer
  // (per the repo's surface-ambiguous-state principle): the child terminated but the
  // parent stayed paused, so a log-only return leaves a stale "blocked on sub-run"
  // gate with no signal. Guarded (safeSendMessage never throws) so it honors the
  // never-throws contract. Only called once we've confirmed the parent IS blocked on
  // THIS child — never for the synchronous no-op or a different-child terminal.
  const notifyStuck = async (reason: string): Promise<void> => {
    await safeSendMessage(
      platform,
      conversationId,
      `⚠️ Sub-run \`${childRun.id.slice(0, 8)}\` finished, but its parent run ` +
        `\`${parentRunId.slice(0, 8)}\` couldn't auto-resume (${reason}). ` +
        `Resume it manually: \`/workflow resume ${parentRunId}\``
    );
  };

  let parent: WorkflowRun | null;
  try {
    parent = await deps.store.getWorkflowRun(parentRunId);
  } catch (err) {
    getLog().error(
      { err: err as Error, parentRunId, childRunId: childRun.id },
      'workflow.parent_resume_lookup_failed'
    );
    await notifyStuck('the parent run could not be looked up');
    return;
  }
  if (parent?.status !== 'paused') return; // synchronous no-op, or already resumed

  // The core "parent blocked on THIS child" invariant lives in one shared predicate
  // (isRunBlockedOnChild) so this hook and the abandon-strand detector can't drift.
  if (!isRunBlockedOnChild(parent, childRun.id)) {
    // Paused but not blocked on this child. Distinguish a MALFORMED child_workflow
    // gate (missing childRunId — an invariant violation that would wedge the parent
    // forever; make it loud) from a normal different-child / non-child gate (silent).
    logMalformedChildWorkflowGate(parent, childRun.id);
    return;
  }

  const parentCwd = parent.working_path;
  if (!parentCwd) {
    getLog().warn(
      { parentRunId, childRunId: childRun.id },
      'workflow.parent_resume_no_working_path'
    );
    await notifyStuck('the parent has no recorded working path');
    return;
  }

  let parentWorkflow: WorkflowDefinition | undefined;
  try {
    const { workflows } = await discoverWorkflowsWithConfig(parentCwd, deps.loadConfig);
    parentWorkflow = resolveWorkflowName(
      parent.workflow_name,
      workflows.map(w => w.workflow)
    );
  } catch (err) {
    getLog().error({ err: err as Error, parentRunId }, 'workflow.parent_resume_discovery_failed');
    await notifyStuck('workflow discovery failed');
    return;
  }
  if (!parentWorkflow) {
    getLog().warn(
      { parentRunId, workflowName: parent.workflow_name },
      'workflow.parent_resume_workflow_not_found'
    );
    await notifyStuck(`the parent workflow '${parent.workflow_name}' could not be found`);
    return;
  }

  let hydrated: Awaited<ReturnType<typeof hydrateResumableRun>>;
  try {
    hydrated = await hydrateResumableRun(deps, parent);
  } catch (err) {
    // Nothing has been mutated yet on a pre-CAS throw (resumeWorkflowRun's CAS is
    // hydrate's last step; a lost CAS throws WorkflowNotResumableError instead) —
    // the parent stays 'paused' and manually resumable, so log and stand down.
    if (err instanceof Error && err.name === 'WorkflowNotResumableError') {
      // Benign race: a concurrent (manual or duplicate) resume won the CAS and
      // now owns the parent. Not an error — no user-facing message (it IS resuming).
      getLog().info(
        { parentRunId, childRunId: childRun.id },
        'workflow.parent_auto_resume_lost_race'
      );
    } else {
      getLog().error({ err: err as Error, parentRunId }, 'workflow.parent_resume_hydrate_failed');
      await notifyStuck('preparing the parent for resume failed');
    }
    return;
  }
  if (!hydrated) {
    // A parent paused on a child_workflow gate is always resumable (see the
    // child_workflow branch in hydrateResumableRun), so null here is unexpected.
    getLog().warn({ parentRunId }, 'workflow.parent_resume_nothing_to_resume');
    await notifyStuck('the parent had no resumable state');
    return;
  }

  getLog().info(
    { parentRunId, childRunId: childRun.id, childStatus: childRun.status },
    'workflow.parent_auto_resume_started'
  );
  try {
    await executeWorkflow(
      deps,
      platform,
      conversationId,
      parentCwd,
      parentWorkflow,
      parent.user_message ?? '',
      conversationDbId,
      {
        ...hydrated,
        codebaseId: parent.codebase_id ?? undefined,
        resolveChildIsolation,
      }
    );
  } catch (err) {
    // The hydrate CAS above already flipped the parent paused→running, and
    // executeWorkflow's own failWorkflowRun catch-all doesn't cover its early
    // setup (config load, env/credential resolution). Without this handler a
    // throw there would strand the parent at 'running' — a non-terminal status
    // resumeWorkflow refuses, leaving destructive abandon as the only exit.
    // Land it in 'failed' instead so it stays resumable.
    getLog().error(
      { err: err as Error, parentRunId, childRunId: childRun.id },
      'workflow.parent_auto_resume_execute_failed'
    );
    await deps.store
      .failWorkflowRun(parentRunId, `Auto-resume after sub-run failed: ${(err as Error).message}`)
      .catch((failErr: unknown) => {
        getLog().error(
          { err: failErr as Error, parentRunId },
          'workflow.parent_auto_resume_fail_mark_failed'
        );
      });
  }
}

interface WorkflowProviderResolution {
  resolvedProvider: string;
  resolvedModel?: string;
  workflowPreset?: ModelAliasPreset;
  aiProfile: ResolvedAiProfile;
  userAiPrefs: UserAiPrefsLayer;
}

type WorkflowPathBundle = Awaited<ReturnType<typeof resolveProjectPaths>> & {
  scopeArtifactsDir?: string;
};

async function guardPinnedRun(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflow: WorkflowDefinition,
  source: WorkflowSource | undefined,
  preCreatedRun: WorkflowRun | undefined,
  execContext: ExecutionContext
): Promise<WorkflowExecutionResult | null> {
  const workflowPin = buildWorkflowPinState(workflow, source);
  if (!preCreatedRun || !workflowRunRequiresPin(workflow, preCreatedRun, execContext)) return null;
  try {
    assertWorkflowPinMatchesRun(preCreatedRun, workflowPin);
    return null;
  } catch (error) {
    const msg = (error as Error).message;
    getLog().warn(
      { workflowRunId: preCreatedRun.id, workflowName: workflow.name, error: msg },
      'workflow.pin_validation_failed'
    );
    await deps.store.failWorkflowRun(preCreatedRun.id, msg).catch((err: unknown) => {
      getLog().error(
        { err, workflowRunId: preCreatedRun.id },
        'workflow.pin_validation_fail_record_failed'
      );
    });
    await safeSendMessage(platform, conversationId, `⚠️ ${msg}`);
    return { success: false, workflowRunId: preCreatedRun.id, error: msg };
  }
}

async function guardContainerResume(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  preCreatedRun: WorkflowRun | undefined,
  containerCtx: ContainerRunContext | undefined
): Promise<WorkflowExecutionResult | null> {
  if (preCreatedRun?.metadata?.isolation !== 'container' || containerCtx) return null;
  const msg =
    `Run '${preCreatedRun.id}' executed inside an isolation container. Resume it from the ` +
    'CLI in the same project (`archon workflow approve/reject/resume <id>`), where the ' +
    'container is rediscovered — chat/web resume cannot rewire it.';
  getLog().warn({ workflowRunId: preCreatedRun.id }, 'workflow.container_resume_without_backend');
  await deps.store.failWorkflowRun(preCreatedRun.id, msg).catch((err: unknown) => {
    getLog().error(
      { err, workflowRunId: preCreatedRun.id },
      'workflow.container_resume_guard_fail_failed'
    );
  });
  await safeSendMessage(platform, conversationId, `⚠️ ${msg}`);
  return { success: false, workflowRunId: preCreatedRun.id, error: msg };
}

async function buildExecutionConfig(
  deps: WorkflowDeps,
  cwd: string,
  codebaseId: string | undefined,
  userId: string | undefined,
  isolatedExecution: boolean
): Promise<WorkflowConfig> {
  const fileConfig = await deps.loadConfig(cwd);
  const dbEnvVars =
    !isolatedExecution && codebaseId ? await deps.store.getCodebaseEnvVars(codebaseId) : {};
  const botGitHubEnv = isolatedExecution
    ? {}
    : await resolveBotGitHubEnvForWorkflow(deps, codebaseId);
  const userGitHubEnv = isolatedExecution
    ? {}
    : await resolveUserGithubEnvForWorkflow(deps, userId);
  return {
    ...fileConfig,
    envVars: isolatedExecution
      ? {}
      : { ...fileConfig.envVars, ...dbEnvVars, ...botGitHubEnv, ...userGitHubEnv },
  };
}

async function resolveBaseBranchForWorkflow(
  deps: WorkflowDeps,
  cwd: string,
  codebaseId: string | undefined,
  config: WorkflowConfig,
  callerBaseBranch: string | undefined,
  callerBaseOverride: string | undefined
): Promise<string> {
  const overrideBaseBranch = callerBaseOverride?.trim();
  const fallbackBaseBranch = callerBaseBranch?.trim();
  if (overrideBaseBranch) return overrideBaseBranch;
  if (config.baseBranch) return config.baseBranch;
  if (fallbackBaseBranch) return fallbackBaseBranch;
  if (await isFolderCodebase(deps, codebaseId)) return '';
  try {
    return await getDefaultBranch(toRepoPath(cwd));
  } catch (error) {
    getLog().warn(
      { err: error as Error, errorType: (error as Error).constructor.name, cwd },
      'workflow.base_branch_auto_detect_failed'
    );
    return '';
  }
}

async function resolveUserAiPrefsLayer(
  deps: WorkflowDeps,
  userId: string | undefined
): Promise<UserAiPrefsLayer> {
  let userAiPrefs: UserAiPrefsLayer = {};
  if (userId && deps.getUserAiPrefs) {
    try {
      userAiPrefs = await deps.getUserAiPrefs(userId);
    } catch (error) {
      getLog().warn({ err: error as Error, userId }, 'workflow.user_ai_prefs_resolve_failed');
    }
  }
  if (userAiPrefs.tiers || userAiPrefs.aliases || userAiPrefs.defaultProvider) {
    getLog().debug(
      {
        userId,
        tierKeys: Object.keys(userAiPrefs.tiers ?? {}),
        aliasKeys: Object.keys(userAiPrefs.aliases ?? {}),
        defaultProvider: userAiPrefs.defaultProvider,
      },
      'workflow.user_ai_prefs_applied'
    );
  }
  return userAiPrefs;
}

function buildExecutionAiProfile(
  config: WorkflowConfig,
  userAiPrefs: UserAiPrefsLayer,
  userId: string | undefined
): ResolvedAiProfile {
  try {
    return buildAiProfile(userAiPrefs.defaultProvider ?? config.assistant, {
      repoTiers: config.tiers,
      repoAliases: config.aliases,
      userTiers: userAiPrefs.tiers,
      userAliases: userAiPrefs.aliases,
    });
  } catch (error) {
    getLog().error({ err: error as Error, userId }, 'workflow.user_ai_prefs_profile_invalid');
    return buildAiProfile(config.assistant, {
      repoTiers: config.tiers,
      repoAliases: config.aliases,
    });
  }
}

async function resolveWorkflowProviderAndModel(
  platform: IWorkflowPlatform,
  conversationId: string,
  workflow: WorkflowDefinition,
  config: WorkflowConfig,
  aiProfile: ResolvedAiProfile
): Promise<Omit<WorkflowProviderResolution, 'aiProfile' | 'userAiPrefs'>> {
  let resolvedProvider: string = workflow.provider ?? config.assistant;
  let resolvedModel: string | undefined;
  let workflowPreset: ModelAliasPreset | undefined;
  let providerSource = workflow.provider ? 'workflow definition' : 'config';
  if (workflow.model) {
    const workflowModelSpec = resolveModelSpec(aiProfile, workflow.model);
    if (isLiteralSpec(workflowModelSpec)) {
      resolvedModel = workflowModelSpec.literal;
    } else {
      workflowPreset = workflowModelSpec;
      if (workflow.provider && workflow.provider !== workflowModelSpec.provider) {
        getLog().warn(
          {
            workflowName: workflow.name,
            configuredProvider: workflow.provider,
            resolvedProvider: workflowModelSpec.provider,
            modelRef: workflow.model,
          },
          'workflow.model_provider_conflict'
        );
        const delivered = await safeSendMessage(
          platform,
          conversationId,
          `Warning: Workflow '${workflow.name}' sets provider '${workflow.provider}' but model '${workflow.model}' resolves to provider '${workflowModelSpec.provider}' — using '${workflowModelSpec.provider}'.`
        );
        if (!delivered)
          getLog().error(
            { workflowName: workflow.name, conversationId },
            'workflow.model_provider_conflict_warning_delivery_failed'
          );
      }
      resolvedProvider = workflowModelSpec.provider;
      resolvedModel = workflowModelSpec.model;
      providerSource = `model preset '${workflow.model}'`;
    }
  }
  if (!isRegisteredProvider(resolvedProvider)) {
    throw new Error(
      `Workflow '${workflow.name}': unknown provider '${resolvedProvider}'. ` +
        `Registered: ${getRegisteredProviders()
          .map(p => p.id)
          .join(', ')}`
    );
  }
  resolvedModel ??= config.assistants[resolvedProvider]?.model as string | undefined;
  getLog().info(
    {
      workflowName: workflow.name,
      provider: resolvedProvider,
      providerSource,
      model: resolvedModel,
    },
    'workflow_provider_resolved'
  );
  return { resolvedProvider, resolvedModel, workflowPreset };
}

async function resolveWorkflowRunRecord(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  conversationDbId: string,
  workflow: WorkflowDefinition,
  userMessage: string,
  cwd: string,
  opts: ExecuteWorkflowOptions,
  execContext: ExecutionContext,
  workflowPin: ReturnType<typeof buildWorkflowPinState>
): Promise<WorkflowRun | WorkflowExecutionResult> {
  if (opts.preCreatedRun) {
    if (opts.priorCompletedNodes !== undefined) {
      const resumeMsg =
        opts.priorCompletedNodes.size > 0
          ? `▶️ **Resuming** workflow \`${workflow.name}\` — skipping ${String(opts.priorCompletedNodes.size)} already-completed node(s).\n\nNote: AI session context from prior nodes is not restored. Nodes that depend on prior context may need to re-read artifacts.`
          : `▶️ **Resuming** workflow \`${workflow.name}\` — continuing interactive loop.`;
      await safeSendMessage(platform, conversationId, resumeMsg);
    }
    return opts.preCreatedRun;
  }
  try {
    const workflowRun = await deps.store.createWorkflowRun({
      workflow_name: workflow.name,
      conversation_id: conversationDbId,
      codebase_id: opts.codebaseId,
      user_message: userMessage,
      working_path: cwd,
      metadata: {
        ...(opts.issueContext ? { github_context: opts.issueContext } : {}),
        ...(execContext.kind === 'container' ? { isolation: 'container' } : {}),
        ...(opts.container ? { isolation_env_id: opts.container.envId } : {}),
        ...(workflowRunRequiresPin(workflow, undefined, execContext)
          ? { [WORKFLOW_PIN_METADATA_KEY]: workflowPin }
          : {}),
      },
      parent_conversation_id: opts.parentConversationId,
      user_id: opts.userId,
    });
    return {
      ...workflowRun,
      metadata: {
        ...(workflowRun.metadata ?? {}),
        ...(workflowRunRequiresPin(workflow, undefined, execContext)
          ? { [WORKFLOW_PIN_METADATA_KEY]: workflowPin }
          : {}),
      },
    };
  } catch (error) {
    getLog().error(
      { err: error as Error, workflowName: workflow.name, conversationId },
      'db_create_workflow_run_failed'
    );
    await sendCriticalMessage(
      platform,
      conversationId,
      '❌ **Workflow failed**: Unable to start workflow (database error). Please try again later.'
    );
    return { success: false, error: 'Database error creating workflow run' };
  }
}

function isExecutionResult(value: unknown): value is WorkflowExecutionResult {
  return typeof value === 'object' && value !== null && 'success' in value;
}

async function collectPathLockExclusions(
  deps: WorkflowDeps,
  workflowRun: WorkflowRun,
  cwd: string
): Promise<{ skipPathLock: boolean; pathLockExclude: string[] }> {
  const pathLockExclude: string[] = [];
  let skipPathLock = false;
  if (workflowRun.parent_run_id) {
    try {
      const ancestry = await deps.store.getRunAncestry(workflowRun.id);
      pathLockExclude.push(...ancestry.map(a => a.id));
    } catch (err) {
      getLog().error(
        { err: err as Error, workflowRunId: workflowRun.id, cwd },
        'workflow.path_lock_ancestry_lookup_failed'
      );
      skipPathLock = true;
    }
  }
  if (!skipPathLock) {
    try {
      pathLockExclude.push(...(await gatherDescendantRunIds(deps, workflowRun.id)));
    } catch (err) {
      getLog().warn(
        { err: err as Error, workflowRunId: workflowRun.id, cwd },
        'workflow.path_lock_descendant_lookup_failed'
      );
    }
  }
  return { skipPathLock, pathLockExclude };
}

function formatActiveWorkflowMessage(activeWorkflow: WorkflowRun): {
  message: string;
  error: string;
} {
  const elapsedMs = Date.now() - parseDbTimestamp(activeWorkflow.started_at);
  const duration = formatDuration(elapsedMs);
  const shortId = activeWorkflow.id.slice(0, 8);
  const paused = activeWorkflow.status === 'paused';
  const stateLine = paused
    ? `paused waiting for user input (${duration} since started, run \`${shortId}\`)`
    : `${activeWorkflow.status === 'pending' ? 'starting' : 'running'} ${duration}, run \`${shortId}\``;
  const actionLines = paused
    ? `• Approve it: \`/workflow approve ${shortId}\`\n` +
      `• Reject it: \`/workflow reject ${shortId}\`\n` +
      `• Cancel it: \`/workflow cancel ${shortId}\`\n` +
      '• Use a different branch: `--branch <other>`'
    : '• Wait for it to finish: `/workflow status`\n' +
      `• Cancel it: \`/workflow cancel ${shortId}\`\n` +
      '• Use a different branch: `--branch <other>`';
  return {
    message: `❌ **This worktree is in use** by \`${activeWorkflow.workflow_name}\` (${stateLine}).\n${actionLines}`,
    error: `Workflow already active on this path (${activeWorkflow.status}): ${activeWorkflow.workflow_name}`,
  };
}

async function enforceWorkflowPathLock(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflow: WorkflowDefinition,
  workflowRun: WorkflowRun
): Promise<WorkflowExecutionResult | null> {
  if (workflow.mutates_checkout === false) return null;
  try {
    const { skipPathLock, pathLockExclude } = await collectPathLockExclusions(
      deps,
      workflowRun,
      cwd
    );
    const activeWorkflow = skipPathLock
      ? null
      : await deps.store.getActiveWorkflowRunByPath(cwd, {
          id: workflowRun.id,
          startedAt: new Date(parseDbTimestamp(workflowRun.started_at)),
          ...(pathLockExclude.length > 0 ? { excludeRunIds: pathLockExclude } : {}),
        });
    if (!activeWorkflow) return null;
    await deps.store
      .updateWorkflowRun(workflowRun.id, { status: 'cancelled' })
      .catch((cleanupErr: Error) => {
        getLog().warn(
          { err: cleanupErr, workflowRunId: workflowRun.id, cwd },
          'workflow.guard_self_cancel_failed'
        );
      });
    const { message, error } = formatActiveWorkflowMessage(activeWorkflow);
    await sendCriticalMessage(platform, conversationId, message);
    return { success: false, error };
  } catch (error) {
    getLog().error(
      { err: error as Error, conversationId, cwd, pendingRunId: workflowRun.id },
      'db_active_workflow_check_failed'
    );
    await deps.store
      .updateWorkflowRun(workflowRun.id, { status: 'cancelled' })
      .catch((cleanupErr: Error) => {
        getLog().warn(
          { err: cleanupErr, workflowRunId: workflowRun.id },
          'workflow.guard_query_failure_cleanup_failed'
        );
      });
    await sendCriticalMessage(
      platform,
      conversationId,
      '❌ **Workflow blocked**: Unable to verify if another workflow is running (database error). Please try again in a moment.'
    );
    return { success: false, error: 'Database error checking for active workflow' };
  }
}

async function prepareWorkflowPaths(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflow: WorkflowDefinition,
  workflowRun: WorkflowRun,
  codebaseId: string | undefined
): Promise<WorkflowPathBundle | WorkflowExecutionResult> {
  const paths = await resolveProjectPaths(deps, cwd, workflowRun.id, codebaseId, {
    persistedOutputRoot: workflowRun.output_root,
  });
  if (!workflowRun.output_root) {
    await deps.store
      .updateWorkflowRun(workflowRun.id, { output_root: paths.outputRoot })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: workflowRun.id, outputRoot: paths.outputRoot },
          'workflow.output_root_persist_failed'
        );
      });
  }
  const isolated = archonPaths.isInsideArchonHome(cwd);
  await maybeWarnLegacyStatePath(cwd, paths.stateDir, isolated);
  await maybeWarnLegacyArtifactsPath(cwd, paths.artifactsRoot, isolated);
  const scopeArtifactsDir = resolveScopeArtifactsDir(
    workflow,
    workflowRun.conversation_id,
    paths.artifactsRoot
  );
  try {
    await mkdir(paths.artifactsDir, { recursive: true });
    await mkdir(paths.stateDir, { recursive: true });
    if (scopeArtifactsDir) await mkdir(scopeArtifactsDir, { recursive: true });
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    getLog().error(
      {
        err,
        artifactsDir: paths.artifactsDir,
        stateDir: paths.stateDir,
        workflowRunId: workflowRun.id,
      },
      'workflow.artifacts_dir_create_failed'
    );
    await deps.store
      .failWorkflowRun(workflowRun.id, `Artifacts directory creation failed: ${err.message}`)
      .catch((dbErr: Error) => {
        getLog().error(
          { err: dbErr, workflowRunId: workflowRun.id },
          'workflow.artifacts_dir_fail_db_record_failed'
        );
      });
    await sendCriticalMessage(
      platform,
      conversationId,
      `❌ **Workflow failed**: Could not create artifacts directory \`${paths.artifactsDir}\`: ${err.message}`
    );
    return {
      success: false,
      workflowRunId: workflowRun.id,
      error: `Artifacts directory creation failed: ${err.message}`,
    };
  }
  getLog().debug(paths, 'workflow_paths_resolved');
  return { ...paths, ...(scopeArtifactsDir ? { scopeArtifactsDir } : {}) };
}

function getIsolationMode(
  execContext: ExecutionContext,
  isolationContext: ExecuteWorkflowOptions['isolationContext']
): 'container' | 'worktree' | 'in-place' {
  if (execContext.kind === 'container') return 'container';
  if (isolationContext) return 'worktree';
  return 'in-place';
}

function emitWorkflowStartTelemetry(
  platform: IWorkflowPlatform,
  workflow: WorkflowDefinition,
  source: WorkflowSource | undefined,
  resolvedProvider: string,
  resolvedModel: string | undefined,
  isolationContext: ExecuteWorkflowOptions['isolationContext'],
  isResume: boolean
): void {
  captureWorkflowInvoked({
    workflowName: workflow.name,
    workflowSource: source,
    platform: platform.getPlatformType(),
    provider: resolvedProvider,
    model: resolvedModel,
    nodeCount: workflow.nodes.length,
    usesLoop: workflow.nodes.some(isLoopNode),
    usesLoopGroup: workflow.nodes.some(isLoopGroupNode),
    usesApproval: workflow.nodes.some(isApprovalNode),
    usesScript: workflow.nodes.some(isScriptNode),
    usesBash: workflow.nodes.some(isBashNode),
    usesOutputFormat: workflow.nodes.some(n => n.output_format !== undefined),
    usesOutputType: workflow.nodes.some(n => n.output_type !== undefined),
    usesPersistSession:
      workflow.persist_sessions === true || workflow.nodes.some(n => n.persist_session === true),
    usesMcp: workflow.nodes.some(n => n.mcp !== undefined),
    usesSkills: workflow.nodes.some(n => n.skills !== undefined),
    usesFreshContext: workflow.nodes.some(n => isLoopNode(n) && n.loop.fresh_context),
    interactive: workflow.interactive ?? false,
    usedIsolation: isolationContext !== undefined,
    isResume,
  });
}

async function beginWorkflowRun(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  conversationDbId: string,
  workflow: WorkflowDefinition,
  userMessage: string,
  workflowRun: WorkflowRun,
  config: WorkflowConfig,
  baseBranch: string,
  opts: ExecuteWorkflowOptions,
  execContext: ExecutionContext,
  provider: WorkflowProviderResolution,
  logDir: string
): Promise<WorkflowExecutionResult | null> {
  getLog().info(
    {
      workflowName: workflow.name,
      workflowRunId: workflowRun.id,
      hasIssueContext: !!opts.issueContext,
      issueContextLength: opts.issueContext?.length ?? 0,
    },
    'workflow_starting'
  );
  await logWorkflowStart(logDir, workflowRun.id, workflow.name, userMessage);
  const emitter = getWorkflowEventEmitter();
  emitter.registerRun(workflowRun.id, conversationId);
  emitter.emit({
    type: 'workflow_started',
    runId: workflowRun.id,
    workflowName: workflow.name,
    conversationId: conversationDbId,
  });
  emitWorkflowStartTelemetry(
    platform,
    workflow,
    opts.source,
    provider.resolvedProvider,
    provider.resolvedModel,
    opts.isolationContext,
    opts.priorCompletedNodes !== undefined
  );
  deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'workflow_started',
      data: {
        workflowName: workflow.name,
        defaultAssistant: provider.userAiPrefs.defaultProvider ?? config.assistant,
        provider: provider.resolvedProvider,
        model: provider.resolvedModel ?? null,
        isolationMode: getIsolationMode(execContext, opts.isolationContext),
        baseBranch,
        userId: workflowRun.user_id ?? null,
        userMessage: workflowRun.user_message,
        origin: workflowRun.parent_run_id ? 'workflow' : platform.getPlatformType(),
      },
    })
    .catch((err: Error) => {
      getLog().error(
        { err, workflowRunId: workflowRun.id, eventType: 'workflow_started' },
        'workflow_event_persist_failed'
      );
    });
  if (opts.parseWarnings && opts.parseWarnings.length > 0) {
    deps.store
      .createWorkflowEvent({
        workflow_run_id: workflowRun.id,
        event_type: 'workflow_parse_warnings',
        data: { workflowName: workflow.name, warnings: [...opts.parseWarnings] },
      })
      .catch((err: Error) => {
        getLog().error(
          { err, workflowRunId: workflowRun.id, eventType: 'workflow_parse_warnings' },
          'workflow_event_persist_failed'
        );
      });
  }
  if (opts.priorCompletedNodes) return null;
  try {
    await deps.store.updateWorkflowRun(workflowRun.id, { status: 'running' });
    return null;
  } catch (dbError) {
    getLog().error(
      { err: dbError as Error, workflowRunId: workflowRun.id },
      'db_workflow_status_update_failed'
    );
    await sendCriticalMessage(
      platform,
      conversationId,
      'Workflow blocked: Unable to update status. Please try again.'
    );
    return { success: false, error: 'Database error setting workflow to running' };
  }
}

async function sendWorkflowStartupMessage(
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflow: WorkflowDefinition,
  workflowRun: WorkflowRun,
  isolationContext: ExecuteWorkflowOptions['isolationContext']
): Promise<void> {
  const workflowContext: SendMessageContext = { workflowId: workflowRun.id };
  let startupMessage = '';
  if (isolationContext) {
    const { isPrReview, prSha, prBranch, branchName } = isolationContext;
    if (isPrReview && prSha && prBranch) {
      startupMessage += `Reviewing PR at commit \`${prSha.substring(0, 7)}\` (branch: \`${prBranch}\`)\n\n`;
    } else if (branchName) {
      const repoName = cwd.split(/[/\\]/).pop() || 'repository';
      await sendCriticalMessage(
        platform,
        conversationId,
        `📍 ${repoName} @ \`${branchName}\``,
        workflowContext,
        2,
        { category: 'isolation_context', segment: 'new' }
      );
    } else {
      getLog().warn(
        {
          workflowId: workflowRun.id,
          hasFields: {
            isPrReview: !!isPrReview,
            prSha: !!prSha,
            prBranch: !!prBranch,
            branchName: !!branchName,
          },
        },
        'isolation_context_incomplete'
      );
    }
  }
  const cleanDescription = (workflow.description ?? '')
    .split('\n')
    .filter(
      line => !/^\s*(Use when|Handles|NOT for|Capability|Triggers)[:\s]/i.test(line) && line.trim()
    )
    .join('\n')
    .trim();
  startupMessage += `🚀 **Starting workflow**: \`${workflow.name}\`\n\n> ${cleanDescription || workflow.name}`;
  const startupSent = await sendCriticalMessage(
    platform,
    conversationId,
    startupMessage,
    workflowContext,
    2,
    { category: 'workflow_status', segment: 'new' }
  );
  if (!startupSent)
    getLog().error(
      { workflowId: workflowRun.id, conversationId },
      'startup_message_delivery_failed'
    );
}

async function resumeParentAfterChildTerminal(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  conversationDbId: string,
  workflowRun: WorkflowRun,
  finalStatus: WorkflowRun | null,
  resolveChildIsolation?: ChildIsolationResolver
): Promise<void> {
  if (!finalStatus?.parent_run_id) return;
  if (!['completed', 'failed', 'cancelled'].includes(finalStatus.status)) return;
  await maybeResumeParentRun(
    deps,
    platform,
    conversationId,
    conversationDbId,
    finalStatus,
    resolveChildIsolation
  ).catch((err: unknown) => {
    getLog().error(
      { err: err as Error, childRunId: workflowRun.id, parentRunId: finalStatus.parent_run_id },
      'workflow.parent_auto_resume_failed'
    );
  });
}

function resultFromFinalStatus(
  workflowRun: WorkflowRun,
  finalStatus: WorkflowRun | null,
  dagSummary: string | undefined
): WorkflowExecutionResult {
  if (finalStatus?.status === 'completed')
    return { success: true, workflowRunId: workflowRun.id, summary: dagSummary };
  if (finalStatus?.status === 'paused')
    return { success: true, paused: true, workflowRunId: workflowRun.id };
  return {
    success: false,
    workflowRunId: workflowRun.id,
    error: 'Workflow did not complete successfully',
  };
}

async function handleWorkflowUnhandledError(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  workflow: WorkflowDefinition,
  workflowRun: WorkflowRun,
  logDir: string,
  source: WorkflowSource | undefined,
  resolvedProvider: string,
  error: unknown
): Promise<WorkflowExecutionResult> {
  const err = error as Error;
  getLog().error(
    { err, workflowName: workflow.name, workflowId: workflowRun.id },
    'workflow_execution_unhandled_error'
  );
  try {
    await deps.store.failWorkflowRun(workflowRun.id, err.message);
  } catch (dbError) {
    getLog().error(
      { err: dbError as Error, workflowId: workflowRun.id, originalError: err.message },
      'db_record_failure_failed'
    );
  }
  try {
    await logWorkflowError(logDir, workflowRun.id, err.message);
  } catch (logError) {
    getLog().error(
      { err: logError as Error, workflowId: workflowRun.id },
      'workflow_error_log_write_failed'
    );
  }
  const emitter = getWorkflowEventEmitter();
  emitter.emit({
    type: 'workflow_failed',
    runId: workflowRun.id,
    workflowName: workflow.name,
    error: err.message,
  });
  captureWorkflowCompleted({
    outcome: 'failed',
    workflowName: workflow.name,
    workflowSource: source,
    provider: resolvedProvider,
    exitReason: 'unhandled_error',
    errorClass: toTelemetryErrorClass(classifyError(err)),
  });
  deps.store
    .createWorkflowEvent({
      workflow_run_id: workflowRun.id,
      event_type: 'workflow_failed',
      data: { error: err.message },
    })
    .catch((eventErr: Error) => {
      getLog().error(
        { err: eventErr, workflowRunId: workflowRun.id, eventType: 'workflow_failed' },
        'workflow_event_persist_failed'
      );
    });
  emitter.unregisterRun(workflowRun.id);
  const delivered = await sendCriticalMessage(
    platform,
    conversationId,
    `❌ **Workflow failed**: ${err.message}`
  );
  if (!delivered)
    getLog().error(
      { workflowId: workflowRun.id, originalError: err.message },
      'user_failure_notification_failed'
    );
  return { success: false, workflowRunId: workflowRun.id, error: err.message };
}

async function runWorkflowFinallyBackstop(
  deps: WorkflowDeps,
  workflowRun: WorkflowRun | undefined
): Promise<void> {
  keepAwake.release();
  if (!workflowRun) return;
  const runId = workflowRun.id;
  const backstopStatus = await deps.store.getWorkflowRunStatus(runId).catch(() => null);
  if (backstopStatus !== 'running') return;
  getLog().warn({ workflowRunId: runId }, 'executor.backstop_triggered');
  await deps.store
    .failWorkflowRun(runId, 'Workflow exited without finalizing — see logs')
    .catch((err: unknown) => {
      getLog().error({ err, workflowRunId: runId }, 'executor.backstop_fail_failed');
    });
}

/**
 * Execute a complete DAG-based workflow.
 *
 * Required positional args carry identity and dependencies. Everything else
 * lives in `opts` ({@link ExecuteWorkflowOptions}). To resume a prior run,
 * call {@link hydrateResumableRun} first and spread its result into `opts` —
 * the executor does not perform resume detection on its own.
 */
export async function executeWorkflow(
  deps: WorkflowDeps,
  platform: IWorkflowPlatform,
  conversationId: string,
  cwd: string,
  workflow: WorkflowDefinition,
  userMessage: string,
  conversationDbId: string,
  opts: ExecuteWorkflowOptions = {}
): Promise<WorkflowExecutionResult> {
  const execContext = opts.execContext ?? { kind: 'host' };
  const workflowPin = buildWorkflowPinState(workflow, opts.source);
  const pinFailure = await guardPinnedRun(
    deps,
    platform,
    conversationId,
    workflow,
    opts.source,
    opts.preCreatedRun,
    execContext
  );
  if (pinFailure) return pinFailure;
  const containerFailure = await guardContainerResume(
    deps,
    platform,
    conversationId,
    opts.preCreatedRun,
    opts.container
  );
  if (containerFailure) return containerFailure;

  const isolatedExecution =
    execContext.kind === 'container' || workflow.hardened?.required === true;
  const config = await buildExecutionConfig(
    deps,
    cwd,
    opts.codebaseId,
    opts.userId,
    isolatedExecution
  );
  const baseBranch = await resolveBaseBranchForWorkflow(
    deps,
    cwd,
    opts.codebaseId,
    config,
    opts.baseBranch,
    opts.baseOverride
  );
  const docsDir = config.docsPath ?? 'docs/';
  const userAiPrefs = await resolveUserAiPrefsLayer(deps, opts.userId);
  const aiProfile = buildExecutionAiProfile(config, userAiPrefs, opts.userId);
  const providerBits = await resolveWorkflowProviderAndModel(
    platform,
    conversationId,
    workflow,
    config,
    aiProfile
  );
  const provider: WorkflowProviderResolution = { ...providerBits, aiProfile, userAiPrefs };
  if (config.commands.folder)
    getLog().debug(
      { configuredCommandFolder: config.commands.folder },
      'command_folder_configured'
    );

  const runRecord = await resolveWorkflowRunRecord(
    deps,
    platform,
    conversationId,
    conversationDbId,
    workflow,
    userMessage,
    cwd,
    opts,
    execContext,
    workflowPin
  );
  if (isExecutionResult(runRecord)) return runRecord;
  const workflowRun: WorkflowRun | undefined = runRecord;

  const lockFailure = await enforceWorkflowPathLock(
    deps,
    platform,
    conversationId,
    cwd,
    workflow,
    workflowRun
  );
  if (lockFailure) return lockFailure;
  const paths = await prepareWorkflowPaths(
    deps,
    platform,
    conversationId,
    cwd,
    workflow,
    workflowRun,
    opts.codebaseId
  );
  if (isExecutionResult(paths)) return paths;
  if (!isolatedExecution) {
    const userProviderEnv = await resolveUserProviderEnvForWorkflow(
      deps,
      opts.userId,
      paths.artifactsDir
    );
    config.envVars = { ...config.envVars, ...userProviderEnv };
  }

  keepAwake.acquire();
  try {
    const beginFailure = await beginWorkflowRun(
      deps,
      platform,
      conversationId,
      conversationDbId,
      workflow,
      userMessage,
      workflowRun,
      config,
      baseBranch,
      opts,
      execContext,
      provider,
      paths.logDir
    );
    if (beginFailure) return beginFailure;
    await sendWorkflowStartupMessage(
      platform,
      conversationId,
      cwd,
      workflow,
      workflowRun,
      opts.isolationContext
    );
    const dagSummary = await executeDagWorkflow(
      deps,
      platform,
      conversationId,
      cwd,
      workflow,
      workflowRun,
      provider.resolvedProvider,
      provider.resolvedModel,
      paths.artifactsDir,
      paths.stateDir,
      paths.logDir,
      baseBranch,
      docsDir,
      config,
      config.commands.folder,
      opts.issueContext,
      opts.priorCompletedNodes,
      opts.source,
      provider.aiProfile,
      provider.workflowPreset,
      paths.scopeArtifactsDir,
      execContext,
      opts.container,
      (childArgs: RunChildWorkflowArgs): Promise<ChildWorkflowOutcome> =>
        runChildWorkflow(deps, platform, childArgs, opts.resolveChildIsolation),
      opts.priorTokenUsage
    );
    const finalStatus = await deps.store.getWorkflowRun(workflowRun.id);
    await resumeParentAfterChildTerminal(
      deps,
      platform,
      conversationId,
      conversationDbId,
      workflowRun,
      finalStatus,
      opts.resolveChildIsolation
    );
    return resultFromFinalStatus(workflowRun, finalStatus, dagSummary);
  } catch (error) {
    return await handleWorkflowUnhandledError(
      deps,
      platform,
      conversationId,
      workflow,
      workflowRun,
      paths.logDir,
      opts.source,
      provider.resolvedProvider,
      error
    );
  } finally {
    await runWorkflowFinallyBackstop(deps, workflowRun);
  }
}
