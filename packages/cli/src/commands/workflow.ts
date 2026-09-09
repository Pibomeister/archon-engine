/**
 * Workflow command - list and run workflows
 */
import {
  registerRepository,
  registerFolder,
  loadConfig,
  loadRepoConfig,
  generateAndSetTitle,
  createWorkflowStore,
  getUserAiPrefs,
  isPerUserGitHubEnabled,
  getDecryptedAccessToken,
} from '@archon/core';
import { WORKFLOW_EVENT_TYPES, type WorkflowEventType } from '@archon/workflows/store';
import {
  isTierName,
  buildAiProfile,
  TIER_NAMES,
  type TierName,
  type RawTiersConfig,
} from '@archon/workflows/model-validation';
import {
  configureIsolation,
  getIsolationProvider,
  resolveFolderBackend,
  classifyIsolationError,
  encodeStrictEgressPolicy,
} from '@archon/isolation';
import type {
  ExecutionContext,
  ContainerBackend,
  ContainerBackendConfig,
  PreparedEnv,
} from '@archon/isolation';
import {
  createLogger,
  getArchonHome,
  BUNDLED_IS_BINARY,
  BUNDLED_VERSION,
  readTierNoticeState,
  markTierNoticeShown,
} from '@archon/paths';
import { join } from 'node:path';
import { mkdirSync, openSync, closeSync, readFileSync, writeSync } from 'node:fs';
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createWorkflowDeps } from '@archon/core/workflows/store-adapter';
import {
  createHardenedControllerActions,
  prepareHardenedControllerSession,
  resumeHardenedControllerSession,
  getHardenedControllerEgressPolicy,
  getHardenedControllerProxyBudgetSeed,
  getHardenedControllerValidatorNodeModules,
  assertHardenedControllerResumeRoute,
  type HardenedControllerRepoInput,
  type HardenedControllerSession,
} from '@archon/core/workflows/hardened-controller';
import { createChildWorktreeResolver } from '@archon/core/workflows/child-isolation-resolver';
import { discoverWorkflowsWithConfig } from '@archon/workflows/workflow-discovery';
import { resolveWorkflowName } from '@archon/workflows/router';
import {
  executeWorkflow,
  hydrateResumableRun,
  type ExecuteWorkflowOptions,
} from '@archon/workflows/executor';
import {
  buildWorkflowPinState,
  WORKFLOW_PIN_METADATA_KEY,
} from '@archon/workflows/workflow-pinning';
import { WORKFLOW_BUDGET_METADATA_KEY, type WorkflowBudgetState } from '@archon/workflows/budget';
import { assertWorkflowRequirementsMet } from '@archon/workflows/utils/workflow-requirements';
import {
  getWorkflowEventEmitter,
  type WorkflowEmitterEvent,
} from '@archon/workflows/event-emitter';
import type {
  WorkflowDefinition,
  WorkflowLoadResult,
  WorkflowSource,
  WorkflowWithSource,
} from '@archon/workflows/schemas/workflow';
import { workflowRunStatusSchema, isApprovalContext } from '@archon/workflows/schemas/workflow-run';
import type { WorkflowRun, WorkflowRunStatus } from '@archon/workflows/schemas/workflow-run';
import {
  approveWorkflow,
  rejectWorkflow,
  resumeWorkflow as resumeWorkflowOp,
  abandonWorkflow,
  getWorkflowStatus,
  resetWorkflowNodeSessions,
} from '@archon/core/operations/workflow-operations';
import * as conversationDb from '@archon/core/db/conversations';
import * as codebaseDb from '@archon/core/db/codebases';
import * as isolationDb from '@archon/core/db/isolation-environments';
import * as messageDb from '@archon/core/db/messages';
import * as workflowDb from '@archon/core/db/workflows';
import * as workflowEventsDb from '@archon/core/db/workflow-events';
import type { WorkflowEventRow } from '@archon/core/db/workflow-events';
import * as userDb from '@archon/core/db/users';
import * as git from '@archon/git';
import { CLIAdapter } from '../adapters/cli-adapter';
import { writeJsonLine, writeStdout } from '../utils/stdout';
import { resolveCliUserId } from './auth';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('cli.workflow');
  return cachedLog;
}

const DETACHED_STARTUP_WINDOW_MS = 500;
const DETACHED_LOG_TAIL_MAX_CHARS = 4_000;
const DETACHED_LOG_TAIL_MAX_LINES = 40;

function readDetachedLogTail(path: string): string | null {
  try {
    const content = readFileSync(path, 'utf8');
    const lines = content.slice(-DETACHED_LOG_TAIL_MAX_CHARS).split('\n');
    const tail = lines.slice(-DETACHED_LOG_TAIL_MAX_LINES).join('\n').trim();
    return tail.length > 0 ? tail : null;
  } catch {
    return null;
  }
}

function detachedStartupExitError(
  code: number | null,
  signal: NodeJS.Signals | null,
  logPath: string | null
): Error {
  const reason = code === null ? `signal ${signal ?? 'unknown'}` : `exit code ${String(code)}`;
  const tail = logPath ? readDetachedLogTail(logPath) : null;
  const diagnostic = tail ? `\n\nChild output (${logPath}):\n${tail}` : '';
  return new Error(`Detached workflow child exited during startup with ${reason}.${diagnostic}`);
}

async function waitForDetachedStartup(
  child: ChildProcess,
  logPath: string | null,
  execPath: string,
  conversationId: string
): Promise<void> {
  const outcome = await new Promise<'completed' | 'survived'>((resolve, reject) => {
    const cleanup = (): void => {
      clearTimeout(timer);
      child.off('exit', onExit);
      child.off('error', onStartupError);
    };
    const settle = (result: 'completed' | 'survived' | Error): void => {
      cleanup();
      if (result instanceof Error) reject(result);
      else resolve(result);
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null): void => {
      if (code === 0) settle('completed');
      else settle(detachedStartupExitError(code, signal, logPath));
    };
    const onStartupError = (error: Error): void => {
      settle(error);
    };
    const timer = setTimeout(() => {
      settle('survived');
    }, DETACHED_STARTUP_WINDOW_MS);

    child.once('exit', onExit);
    child.once('error', onStartupError);
  });

  if (outcome === 'survived') child.unref();
  getLog().debug(
    { execPath, conversationId, outcome, startupWindowMs: DETACHED_STARTUP_WINDOW_MS },
    'cli.detached_run_startup_acknowledged'
  );
}

/**
 * Options for workflow run command
 *
 * Default: creates worktree with auto-generated branch name (isolation by default).
 * --branch: explicit branch name for the worktree.
 * --no-worktree: opt out of isolation, run in live checkout.
 * --resume: reuse worktree from last failed run.
 * --from: override base branch (start-point for worktree).
 * --base: per-dispatch PR base + worktree cut-from override (wins over config).
 *
 * Mutually exclusive: --branch + --no-worktree, --resume + --branch,
 * --base + --no-worktree.
 */
export interface WorkflowRunOptions {
  branchName?: string;
  fromBranch?: string;
  /**
   * Per-dispatch base-branch override (`--base <branch>`). Wins over repo config
   * and the codebase default for BOTH the worktree cut-from and the PR target
   * (`$BASE_BRANCH`). Mutually exclusive with `--no-worktree`.
   */
  baseBranch?: string;
  noWorktree?: boolean;
  /**
   * Register the current non-git cwd as a folder project on first use and run
   * in place (no worktree isolation). No-op when the cwd is already a registered
   * project or a git repository.
   */
  folder?: boolean;
  /**
   * Run a FOLDER project inside the container isolation backend instead of
   * in-place. Flag beats workflow `container.enabled`, which beats config
   * `container.enabled` (default off). A repo-kind project + `--container` is a
   * hard error (container isolation is folder-only in v1).
   */
  container?: boolean;
  resume?: boolean;
  codebaseId?: string; // Skips path-based codebase lookup when resume/approve/reject already resolved it
  /**
   * Override the directory used for workflow YAML discovery.
   * Pass `codebase.default_cwd` here so the source repo is searched even when
   * `working_path` is a worktree or workspace clone that lacks the file.
   */
  discoveryCwd?: string;
  quiet?: boolean;
  verbose?: boolean;
  /** Platform conversation ID (e.g. `cli-{ts}-{rand}`), NOT a DB UUID. */
  conversationId?: string;
  /**
   * Run the workflow in a detached background child and return immediately.
   * The parent pins a stable branch + conversation id on the child's argv so
   * exactly one worktree/conversation is created. The child does all the work.
   */
  detach?: boolean;
  /**
   * Emit a machine-readable JSON ack for the spawned child instead of human
   * text. Only meaningful together with `detach`: without `detach` a foreground
   * `workflow run` streams human output and has no JSON ack to emit (passing
   * `--json` alone still suppresses CLI logs but does not change the output).
   */
  json?: boolean;
}

/**
 * Default runner image when `.archon/config.yaml > container.image` is unset.
 * The build script (`bun run build:runner-image`) tags both
 * `archon-runner:<version>` and `archon-runner:latest`; defaulting to `latest`
 * always matches the most recently built image without coupling to the
 * dev-vs-binary version string. Operators pin `container.image` for reproducibility.
 */
const DEFAULT_RUNNER_IMAGE = 'archon-runner:latest';

/**
 * Resolve the container backend config from the merged `container` config,
 * applying Phase B defaults (no network egress, 4 GiB memory, 512 pids).
 *
 * `container.*` comes from hand-parsed YAML (not Zod), so the values are
 * untrusted at runtime despite their static types — validate them here. In
 * particular `network` must be `none` until restricted controller-proxy egress
 * exists: `bridge`/`host` would otherwise expose Docker NAT or host networking.
 */
export function resolveContainerBackendConfig(
  cfg:
    | { profile?: string; image?: string; network?: string; memoryMb?: number; pidsLimit?: number }
    | undefined
): ContainerBackendConfig {
  assertSupportedContainerSettings(cfg);
  const profile = cfg?.profile;
  if (profile !== undefined && profile !== 'hardened') {
    throw new Error(
      `Invalid container.profile '${profile}' in .archon/config.yaml — only 'hardened' is supported. ` +
        'Legacy overlay/native profiles are not allowed.'
    );
  }
  const network = cfg?.network;
  if (network !== undefined && network !== 'none') {
    throw new Error(
      `Invalid container.network '${network}' in .archon/config.yaml — must be 'none'. ` +
        'Provider egress requires a restricted controller proxy, which is not implemented yet.'
    );
  }
  // Positive INTEGERS — `docker run --memory`/`--pids-limit` reject fractions,
  // and Number.isFinite alone would let `512.5` through to a runtime docker error.
  const memoryMb = cfg?.memoryMb;
  assertPositiveContainerInteger(memoryMb, 'memoryMb', ' (MiB)');
  const pidsLimit = cfg?.pidsLimit;
  assertPositiveContainerInteger(pidsLimit, 'pidsLimit');
  return {
    profile: 'hardened',
    image: cfg?.image?.trim() || DEFAULT_RUNNER_IMAGE,
    network: network ?? 'none',
    memoryMb: memoryMb ?? 4096,
    pidsLimit: pidsLimit ?? 512,
  };
}

function assertPositiveContainerInteger(
  value: number | undefined,
  key: 'memoryMb' | 'pidsLimit',
  unit = ''
): void {
  if (value === undefined || (Number.isInteger(value) && value > 0)) return;
  throw new Error(
    `Invalid container.${key} '${String(value)}' — must be a positive integer${unit}.`
  );
}

export function bindHardenedContainerConfig(
  base: ContainerBackendConfig,
  session: HardenedControllerSession
): ContainerBackendConfig {
  if (base.egressPolicy !== undefined) {
    throw new Error('Repository container configuration cannot authorize egress.');
  }
  if (base.proxyBudget !== undefined) {
    throw new Error('Repository container configuration cannot authorize provider budget.');
  }
  const egressPolicy = getHardenedControllerEgressPolicy(session);
  const proxyBudget = getHardenedControllerProxyBudgetSeed(session);
  return {
    ...base,
    image: session.policyMetadata.image,
    ...(egressPolicy ? { egressPolicy } : {}),
    ...(proxyBudget ? { proxyBudget } : {}),
  };
}

export function assertHardenedEgressEnvironment(
  config: ContainerBackendConfig,
  metadata: unknown,
  ownerRunId: string
): void {
  if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
    throw new Error('Cannot resume hardened run: isolation metadata is missing.');
  }
  const expected = config.egressPolicy ? encodeStrictEgressPolicy(config.egressPolicy) : undefined;
  if ((metadata as Record<string, unknown>).egressPolicyB64 !== expected) {
    throw new Error(
      'Cannot resume hardened run: egress differs from authenticated controller policy.'
    );
  }
  const record = metadata as Record<string, unknown>;
  if (config.egressPolicy && !config.proxyBudget) {
    throw new Error('Cannot resume hardened run: provider budget authority is missing.');
  }
  const proxyBudgetSeedDigest = config.proxyBudget
    ? sessionProxyBudgetDigest(config.proxyBudget)
    : undefined;
  if (record.proxyBudgetSeedDigest !== proxyBudgetSeedDigest) {
    throw new Error(
      'Cannot resume hardened run: provider budget differs from authenticated controller policy.'
    );
  }
  if (record.image !== config.image || record.ownerRunId !== ownerRunId) {
    throw new Error(
      'Cannot resume hardened run: isolation image or run ownership differs from controller authority.'
    );
  }
}

function assertSupportedContainerSettings(config: unknown): void {
  if (config === undefined) return;
  if (!config || typeof config !== 'object' || Array.isArray(config)) {
    throw new Error('Invalid container configuration: expected an object.');
  }
  const allowed = new Set([
    'profile',
    'image',
    'network',
    'memoryMb',
    'pidsLimit',
    'enabled',
    'repoInputs',
    'repo_inputs',
  ]);
  for (const key of Object.keys(config)) {
    if (!allowed.has(key)) {
      throw new Error(
        `Unsupported container setting '${key}'; additional access requires controller-owned admission.`
      );
    }
  }
}

function sessionProxyBudgetDigest(
  seed: NonNullable<ContainerBackendConfig['proxyBudget']>
): string {
  return createHash('sha256').update(stableSerializeForWorkflow(seed)).digest('hex');
}

function stableSerializeForWorkflow(value: unknown): string {
  if (value === undefined) return 'null';
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableSerializeForWorkflow).join(',')}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .filter(key => record[key] !== undefined)
    .map(key => `${JSON.stringify(key)}:${stableSerializeForWorkflow(record[key])}`)
    .join(',')}}`;
}

/**
 * H2 — a container run has an UNRESOLVED write-back when isolated changes were
 * raised for review (`pending_writeback` set) but never applied or discarded
 * (`writeback_resolved !== true`). This happens on a failed/partial apply. The CLI
 * teardown must PRESERVE the container+volumes in this state (they are the only
 * copy of the changes) rather than destroy them. Pure so the decision is unit-testable.
 */
export function hasUnresolvedWriteback(metadata: Record<string, unknown> | undefined): boolean {
  if (!metadata) return false;
  return metadata.pending_writeback !== undefined && metadata.writeback_resolved !== true;
}

export function shouldPreserveHardenedContainer(
  metadata: Record<string, unknown> | undefined,
  status: WorkflowRunStatus | undefined
): boolean {
  if (metadata?.isolation !== 'container') return false;
  if (hasUnresolvedWriteback(metadata)) return true;
  return status === 'failed' || status === 'cancelled';
}

/**
 * Generate a unique conversation ID for CLI usage
 */
function generateConversationId(): string {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 8);
  return `cli-${String(timestamp)}-${random}`;
}

/**
 * Build the argv for the detached re-invoke. Pure (no spawn / no process reads)
 * so both the dev (bun + entry script) and compiled-binary (execPath only)
 * branches are unit-testable — the binary branch is otherwise unreachable in
 * tests because `BUNDLED_IS_BINARY` is a module-level const. Drops `--detach`
 * and `--json` and appends `--cwd <cwd>` (last-wins) plus any extra flags.
 */
export function buildDetachedRunCmd(
  isBinary: boolean,
  execPath: string,
  argv: string[],
  cwd: string,
  extraArgs: string[]
): string[] {
  // Only the command prefix differs between modes: in a compiled binary
  // execPath IS the archon binary and re-invoking it needs no entry script; in
  // dev, execPath is bun and argv[1] is the cli entry that bun must be handed.
  const baseCmd = isBinary ? [execPath] : [execPath, argv[1]];
  // User args always start at argv[2] in BOTH modes. A Bun single-file
  // executable does have an argv[1] — the virtual entry path
  // (`/$bunfs/root/<name>`, `B:/~BUN/root/<name>.exe` on Windows) — so slicing
  // from 1 in binary mode leaked that path in as the child's first token and
  // the child died with `Unknown command: B:/~BUN/root/archon-...exe` (#2248).
  // cli.ts's own parser reads `process.argv.slice(2)` unconditionally, which is
  // the contract this must match.
  const userArgs = argv.slice(2).filter(arg => arg !== '--detach' && arg !== '--json');
  // --cwd is appended last (parseArgs last-wins) so the child resolves the same
  // absolute working dir regardless of any relative --cwd the caller passed.
  return [...baseCmd, ...userArgs, '--cwd', cwd, ...extraArgs];
}

async function spawnDetachedWorkflowRun(
  cwd: string,
  conversationId: string,
  extraArgs: string[]
): Promise<string | null> {
  const cmd = buildDetachedRunCmd(
    BUNDLED_IS_BINARY,
    process.execPath,
    process.argv,
    cwd,
    extraArgs
  );

  let logPath: string | null = null;
  let logFd: number | undefined;
  try {
    const logDir = join(getArchonHome(), 'logs');
    mkdirSync(logDir, { recursive: true });
    logPath = join(logDir, `detached-run-${conversationId}.log`);
    logFd = openSync(logPath, 'a');
    writeSync(
      logFd,
      `\n--- detached workflow invocation: ${conversationId} at ${new Date().toISOString()} ---\n`
    );
  } catch (error) {
    if (logFd !== undefined) {
      try {
        closeSync(logFd);
      } catch {
        /* fd already closed/invalid — nothing to clean up */
      }
    }
    getLog().warn({ err: error as Error }, 'cli.detached_run_log_open_failed');
    logPath = null;
    logFd = undefined;
  }

  try {
    // Node's spawn with `detached: true` puts the child in its own process
    // group so it survives the parent's exit. Bun.spawn + unref() does NOT
    // detach on Windows — the child was killed ~1s in (at worktree_creating)
    // when the launching shell/console tore down. `detached: true` is the
    // standard fix, also used by setup.ts's trySpawn(); if a kill-on-close Job
    // Object wrapper ever defeats it, a `start /b` breakaway fallback is the
    // next step. `windowsHide` keeps the child headless.
    const child = spawn(cmd[0], cmd.slice(1), {
      cwd,
      env: process.env,
      stdio: ['ignore', logFd ?? 'ignore', logFd ?? 'ignore'],
      detached: true,
      windowsHide: true,
    });
    // Unlike Bun.spawn, Node's spawn does NOT throw synchronously on a bad
    // executable or cwd — the failure arrives as an async 'error' event, which
    // would crash the CLI as an uncaught exception without this listener.
    child.on('error', (error: Error) => {
      getLog().error(
        { err: error, execPath: cmd[0], conversationId },
        'cli.detached_run_spawn_failed'
      );
    });
    // pid is set synchronously iff the OS-level spawn succeeded (same check as
    // setup.ts's trySpawn) — fail fast instead of acking a run that never started.
    if (child.pid === undefined) {
      throw new Error(`Failed to start detached workflow child (executable: ${cmd[0]})`);
    }
    await waitForDetachedStartup(child, logPath, cmd[0], conversationId);
  } finally {
    // The child inherits its own dup of the log fd; close the parent's copy so a
    // synchronous spawn failure (bad execPath, invalid cwd) doesn't leak it.
    if (logFd !== undefined) {
      try {
        closeSync(logFd);
      } catch {
        /* fd already closed/invalid — nothing to clean up */
      }
    }
  }
  return logPath;
}

/**
 * Parses the "Source symlink at X already points to Y, expected Z" error
 * thrown by `createProjectSourceSymlink` in @archon/paths. Cross-package
 * string contract — if that throw site changes wording, this parser silently
 * stops matching. Returns the workspace dir (parent of the `source` link) so
 * the caller can emit an exact cleanup path, or null if unrecognized.
 */
export function extractStaleWorkspaceEntry(message: string): string | null {
  const prefix = 'Source symlink at ';
  const delimiter = ' already points to ';
  if (!message.startsWith(prefix)) return null;

  const remainder = message.slice(prefix.length);
  const delimiterIndex = remainder.indexOf(delimiter);
  if (delimiterIndex === -1) return null;

  const sourcePath = remainder.slice(0, delimiterIndex).trim();
  const lastSeparator = Math.max(sourcePath.lastIndexOf('/'), sourcePath.lastIndexOf('\\'));
  return lastSeparator === -1 ? null : sourcePath.slice(0, lastSeparator);
}

/**
 * Wraps a codebase auto-registration failure for either the worktree-create or
 * resume path. Preserves the original error message and delegates hint detail
 * to `extractStaleWorkspaceEntry`; falls back to a workspace-root pointer when
 * the error shape is unrecognized.
 */
function buildRegistrationFailureError(action: string, error: Error): Error {
  const staleWorkspaceEntry = extractStaleWorkspaceEntry(error.message);
  let hint: string;
  if (staleWorkspaceEntry) {
    hint = `Hint: Remove the stale workspace entry at ${staleWorkspaceEntry} and retry, or use --no-worktree to skip isolation.`;
  } else {
    // Guard against a throwing getArchonHome() (misconfigured env vars, etc.):
    // the registration error we're wrapping is the load-bearing one — we'd
    // rather lose the exact path in the hint than replace it with a secondary
    // home-resolution error that masks the root cause.
    try {
      const workspacesPath = join(getArchonHome(), 'workspaces');
      hint = `Hint: Check your Archon workspace registration under ${workspacesPath} and retry, or use --no-worktree to skip isolation.`;
    } catch {
      hint =
        'Hint: Check your Archon workspace registration and retry, or use --no-worktree to skip isolation.';
    }
  }

  return new Error(
    `Cannot ${action}: repository registration failed.\nError: ${error.message}\n${hint}`
  );
}

/** Error for --branch/--from/--base used against a folder project (no worktree). */
function folderWorktreeOptionError(): Error {
  return new Error(
    'Worktree options require a git-repo project.\n' +
      '  --branch/--from/--base act on an isolated git worktree, which folder projects do not use.\n' +
      '  Drop --branch/--from/--base — folder projects always run in place.'
  );
}

/**
 * Warn that `--base` is only HALF applied when an existing worktree is adopted
 * (`--branch` reuse or `--resume`): its cut-from is already fixed, but the
 * override still reaches `$BASE_BRANCH` and retargets the PR.
 *
 * Deliberately not `--from`'s "was not applied" wording — that is accurate for
 * `--from`, which is wholly inert on reuse, and would understate this case.
 */
function warnBaseOverrideOnReuse(workingPath: string, flagBase: string): void {
  getLog().warn(
    { path: workingPath, baseBranch: flagBase },
    'worktree.reuse_base_override_partial'
  );
  console.warn(
    `Warning: Reusing existing worktree at ${workingPath}. ` +
      `--base ${flagBase} did not change the cut-from (worktree already exists); ` +
      'it still applies to the PR target.'
  );
}

/** Error for a worktree-pinned workflow run against a folder project. */
function folderWorktreePolicyError(workflowName: string): Error {
  return new Error(
    `Workflow '${workflowName}' requires a worktree (worktree.enabled: true), ` +
      'which is not available for folder projects (no git repo to isolate).\n' +
      '  Run this workflow against a git-repo project, or change its worktree policy.'
  );
}

/**
 * Error for a failed `--folder` project registration. Distinct from
 * {@link buildRegistrationFailureError} (which mentions worktrees / `--no-worktree`)
 * because no worktree is ever created for a folder project — that hint would be
 * misleading here.
 */
function buildFolderRegistrationFailureError(error: Error): Error {
  return new Error(
    'Cannot register folder project.\n' +
      `Error: ${error.message}\n` +
      'Hint: Check that the directory is readable and your Archon home ' +
      '(~/.archon) is writable, then retry.'
  );
}

/**
 * Fail fast if `--branch`/`--from`/`--base` (git-worktree-only options) are used
 * against a folder project. Called at three sites — flag-declared (pre-detach), the
 * detach fast-path, and post-lookup (authoritative) — so the check lives in one place.
 *
 * `--base` belongs here even though a folder run creates no worktree for it to
 * redirect: it would still reach `$BASE_BRANCH`, giving the run a PR target with
 * no worktree behind it.
 */
function assertNoWorktreeOptionsForFolder(
  isFolderProject: boolean,
  options: WorkflowRunOptions
): void {
  if (
    isFolderProject &&
    (options.branchName !== undefined ||
      options.fromBranch !== undefined ||
      options.baseBranch !== undefined)
  ) {
    throw folderWorktreeOptionError();
  }
}

/** Fail fast if a `worktree.enabled: true` workflow is run against a folder project. */
function assertWorkflowNotWorktreePinnedForFolder(
  isFolderProject: boolean,
  pinnedEnabled: boolean | undefined,
  workflowName: string
): void {
  if (isFolderProject && pinnedEnabled === true) {
    throw folderWorktreePolicyError(workflowName);
  }
}

/**
 * Capability gate for the CLI run path.
 *
 * Mirrors the orchestrator's `requires: [github]` enforcement
 * (orchestrator-agent.ts `dispatchOrchestratorWorkflow`) so a workflow that
 * declares `requires: [github]` is hard-blocked BEFORE any worktree/clone/AI
 * cost — and before the `--detach` fork — when the acting CLI user hasn't
 * connected their GitHub identity. Throws WorkflowRequirementError, surfaced by
 * the CLI top-level handler (cli.ts) as a clean, actionable `Error: ...` line.
 *
 * No-op on solo PAT installs: `isPerUserGitHubEnabled()` is false unless the
 * GitHub App + TOKEN_ENCRYPTION_KEY are both configured — identical semantics
 * to the orchestrator gate.
 */
async function assertCliWorkflowRequirementsMet(workflow: WorkflowDefinition): Promise<void> {
  if (!isPerUserGitHubEnabled() || !workflow.requires?.length) return;

  // Resolve the acting CLI user (ARCHON_USER_ID, else $USER/$USERNAME) → Archon
  // user id, then check for a stored GitHub connection. An unresolvable user or
  // a lookup failure means "not connected" — fail closed, never silently allow.
  const cliId = resolveCliUserId();
  let githubConnected = false;
  if (cliId) {
    try {
      const cliUser = await userDb.findOrCreateUserByPlatformIdentity('cli', cliId, cliId);
      githubConnected = Boolean(await getDecryptedAccessToken(cliUser.id));
    } catch (error) {
      getLog().warn({ err: error as Error, cliId }, 'cli.requirement_gate_user_resolve_failed');
    }
  }

  assertWorkflowRequirementsMet(workflow, { githubConnected });
}

/**
 * Resolve the provider used for CLI conversation titles from the workflow itself.
 * This keeps auxiliary title generation aligned with workflow execution instead
 * of falling back to a stale conversation default.
 */
function resolveTitleAssistantType(
  workflow: WorkflowDefinition,
  defaultAssistant: string | undefined,
  conversationAssistant: string | undefined
): string {
  // Per CLAUDE.md, provider is resolved via an explicit chain:
  // node.provider ?? workflow.provider ?? config.assistant. Model never
  // influences provider selection — vendor SDKs add new model names faster
  // than we can keep a mapping in sync.
  const fallbackAssistant = defaultAssistant ?? conversationAssistant ?? 'claude';
  if (workflow.provider) return workflow.provider;
  return fallbackAssistant;
}

/**
 * Print a one-time per-version tier notice to stderr when the workflow uses
 * unconfigured tier-keyword nodes (small/medium/large resolving via built-in
 * defaults). Suppressed under --quiet. Uses the same 7-char tier column as
 * `archon ai tier list`.
 */
export async function maybePrintTierNotice(
  workflow: WorkflowDefinition,
  cwd: string,
  cliUserId: string | undefined,
  quiet: boolean | undefined
): Promise<void> {
  if (quiet) return;

  const usedTiers = collectWorkflowTiers(workflow);
  if (usedTiers.size === 0) return;

  const tierInputs = await loadTierNoticeInputs(cwd, cliUserId);
  if (!tierInputs) return;
  if (!hasUnconfiguredTier(usedTiers, tierInputs.configuredTiers, tierInputs.userTiers)) return;

  const version = BUNDLED_VERSION;
  if (readTierNoticeState()?.shownForVersion === version) return;

  const aliases = buildTierNoticeAliases(
    tierInputs.effectiveAssistant,
    tierInputs.configuredTiers,
    tierInputs.userTiers
  );
  if (!aliases) return;

  process.stderr.write(
    buildTierNoticeLines(tierInputs.effectiveAssistant, aliases).join('\n') + '\n'
  );
  markTierNoticeShown(version);
}

function collectWorkflowTiers(workflow: WorkflowDefinition): Set<TierName> {
  const usedTiers = new Set<TierName>();
  if (typeof workflow.model === 'string' && isTierName(workflow.model)) {
    usedTiers.add(workflow.model);
  }
  for (const node of workflow.nodes) {
    if ('model' in node && typeof node.model === 'string' && isTierName(node.model)) {
      usedTiers.add(node.model);
    }
  }
  return usedTiers;
}

interface TierNoticeInputs {
  configuredTiers: RawTiersConfig;
  userTiers: RawTiersConfig;
  effectiveAssistant: string;
}

async function loadTierNoticeInputs(
  cwd: string,
  cliUserId: string | undefined
): Promise<TierNoticeInputs | null> {
  let config: Awaited<ReturnType<typeof loadConfig>>;
  try {
    config = await loadConfig(cwd);
  } catch (err) {
    getLog().debug({ err }, 'tier_notice.config_load_failed');
    return null;
  }

  const configuredTiers: RawTiersConfig = config.tiers ?? {};
  const prefs = await loadUserTierNoticePrefs(cliUserId);
  return {
    configuredTiers,
    userTiers: prefs.userTiers,
    effectiveAssistant: prefs.userDefaultProvider ?? config.assistant,
  };
}

async function loadUserTierNoticePrefs(
  cliUserId: string | undefined
): Promise<{ userTiers: RawTiersConfig; userDefaultProvider: string | undefined }> {
  if (!cliUserId) return { userTiers: {}, userDefaultProvider: undefined };

  try {
    const prefs = await getUserAiPrefs(cliUserId);
    return { userTiers: prefs.tiers ?? {}, userDefaultProvider: prefs.defaultProvider };
  } catch {
    return { userTiers: {}, userDefaultProvider: undefined };
  }
}

function hasUnconfiguredTier(
  usedTiers: Set<TierName>,
  configuredTiers: RawTiersConfig,
  userTiers: RawTiersConfig
): boolean {
  return [...usedTiers].some(t => !configuredTiers[t] && !userTiers[t]);
}

function buildTierNoticeAliases(
  effectiveAssistant: string,
  configuredTiers: RawTiersConfig,
  userTiers: RawTiersConfig
): ReturnType<typeof buildAiProfile>['aliases'] | null {
  try {
    return buildAiProfile(effectiveAssistant, { globalTiers: configuredTiers, userTiers }).aliases;
  } catch (err) {
    getLog().debug({ err }, 'tier_notice.build_profile_failed');
    return null;
  }
}

function buildTierNoticeLines(
  effectiveAssistant: string,
  aliases: ReturnType<typeof buildAiProfile>['aliases']
): string[] {
  const lines: string[] = [
    "ℹ️  This workflow uses model tiers (small/medium/large). You haven't configured them —",
    `   using built-in defaults for '${effectiveAssistant}':`,
  ];
  for (const t of TIER_NAMES) {
    const preset = aliases[t];
    if (preset) lines.push(`     ${t.padEnd(7)} → ${preset.provider}/${preset.model}`);
  }
  appendLargeTierContextNote(lines, aliases.large);
  lines.push(
    '   Customize: `archon ai tier set <tier> <provider> <model>`',
    '              or `tiers:` in .archon/config.yaml',
    '   See anytime: `archon ai tier list`           (shown once per version)',
    ''
  );
  return lines;
}

function appendLargeTierContextNote(
  lines: string[],
  largePreset: ReturnType<typeof buildAiProfile>['aliases']['large']
): void {
  if (largePreset?.provider !== 'claude' || largePreset.model !== 'opus') return;
  lines.push(
    '   (Opus runs a 1M context window on API keys and Max/Team/Enterprise;',
    "    on Pro it's 200K unless you set the `large` tier to `opus[1m]`.)"
  );
}

/** Render a workflow event to stderr as a progress line. Called only when --quiet is not set. */
function renderWorkflowEvent(event: WorkflowEmitterEvent, verbose: boolean): void {
  switch (event.type) {
    case 'node_started': {
      let suffix = '';
      if (event.provider !== undefined && event.model !== undefined) {
        const tierPart = event.tier !== undefined ? ` ← ${event.tier}` : '';
        suffix = `  (${event.provider}/${event.model}${tierPart})`;
      }
      process.stderr.write(`[${event.nodeName}] Started${suffix}\n`);
      break;
    }
    case 'node_completed':
      process.stderr.write(`[${event.nodeName}] Completed (${formatDuration(event.duration)})\n`);
      break;
    case 'node_failed':
      process.stderr.write(`[${event.nodeName}] Failed: ${event.error}\n`);
      break;
    case 'node_skipped':
      process.stderr.write(`[${event.nodeName}] Skipped (${event.reason})\n`);
      break;
    case 'approval_pending':
      process.stderr.write(`[${event.nodeId}] Waiting for approval: ${event.message}\n`);
      break;
    case 'container_lifecycle': {
      const idPart = event.containerId ? ` ${event.containerId.slice(0, 12)}` : '';
      process.stderr.write(`[container] ${event.phase}${idPart}\n`);
      break;
    }
    case 'tool_started':
      if (verbose) {
        process.stderr.write(
          `[${event.stepName}] tool: ${event.toolName} (started, ${event.toolCallId})\n`
        );
      }
      break;
    case 'tool_completed':
      if (verbose) {
        const outcome = event.toolOutcome ? `, ${event.toolOutcome}` : '';
        const exitCode = event.exitCode !== undefined ? `, exit ${String(event.exitCode)}` : '';
        process.stderr.write(
          `[${event.stepName}] tool: ${event.toolName} (${String(event.durationMs)}ms, ${event.toolCallId}${outcome}${exitCode})\n`
        );
      }
      break;
    default:
      // Workflow-level, loop, artifact, and cancelled events are intentionally not rendered.
      break;
  }
}

/**
 * Load workflows from cwd with standardized error handling.
 * Returns the WorkflowLoadResult with both workflows and errors.
 */
async function loadWorkflows(cwd: string): Promise<WorkflowLoadResult> {
  try {
    // Home-scoped workflows at ~/.archon/workflows/ are discovered automatically —
    // no option needed since the discovery helper reads them unconditionally.
    return await discoverWorkflowsWithConfig(cwd, loadConfig);
  } catch (error) {
    const err = error as Error;
    throw new Error(
      `Error loading workflows: ${err.message}\nHint: Check permissions on .archon/workflows/ directory.`
    );
  }
}

/**
 * Print a workflow's parse warnings (keys the engine silently drops) to stderr.
 *
 * stderr rather than stdout so `--json` callers keep a parseable payload while
 * still being told; `console.warn` rather than the logger because `--json` sets
 * the log level to silent, which is exactly the case this has to survive.
 */
export function emitParseWarnings(
  parseWarnings: readonly string[] | undefined,
  workflowName: string
): void {
  if (!parseWarnings || parseWarnings.length === 0) return;
  console.warn(`Warning: '${workflowName}' declares keys the engine ignores:`);
  for (const warning of parseWarnings) {
    console.warn(`  - ${warning}`);
  }
}

function countWorkflowSources(
  workflows: readonly WorkflowWithSource[]
): Record<WorkflowSource, number> {
  return workflows.reduce<Record<WorkflowSource, number>>(
    (counts, entry) => {
      counts[entry.source] += 1;
      return counts;
    },
    { bundled: 0, global: 0, project: 0 }
  );
}

interface WorkflowJsonEntry {
  name: string;
  description: string;
  provider?: string;
  model?: string;
  modelReasoningEffort?: string;
  webSearchMode?: string;
  /** Keys the workflow's YAML declares that the engine drops (#2213). */
  parseWarnings?: string[];
}

/**
 * List available workflows in the current directory
 */
export async function workflowListCommand(cwd: string, json?: boolean): Promise<void> {
  const { workflows: workflowEntries, errors } = await loadWorkflows(cwd);

  if (json) {
    const output = {
      workflows: workflowEntries.map(({ workflow: w, parseWarnings }) => {
        const entry: WorkflowJsonEntry = {
          name: w.name,
          description: w.description,
        };
        if (w.provider !== undefined) entry.provider = w.provider;
        if (w.model !== undefined) entry.model = w.model;
        if (w.modelReasoningEffort !== undefined)
          entry.modelReasoningEffort = w.modelReasoningEffort;
        if (w.webSearchMode !== undefined) entry.webSearchMode = w.webSearchMode;
        if (parseWarnings && parseWarnings.length > 0) entry.parseWarnings = [...parseWarnings];
        return entry;
      }),
      errors: errors.map(e => ({
        filename: e.filename,
        error: e.error,
        errorType: e.errorType,
      })),
    };
    await writeJsonLine(output);
    return;
  }

  console.log(`Discovering workflows in: ${cwd}`);

  if (workflowEntries.length === 0 && errors.length === 0) {
    console.log('\nNo workflows found.');
    console.log('Workflows should be in .archon/workflows/ directory.');
    return;
  }

  if (workflowEntries.length > 0) {
    console.log(`\nFound ${workflowEntries.length} workflow(s):\n`);

    for (const { workflow, parseWarnings } of workflowEntries) {
      console.log(`  ${workflow.name}`);
      console.log(`    ${workflow.description}`);
      if (workflow.provider) {
        console.log(`    Provider: ${workflow.provider}`);
      }
      for (const warning of parseWarnings ?? []) {
        console.log(`    Warning: ${warning}`);
      }
      console.log('');
    }
  }

  if (errors.length > 0) {
    console.log(`\n${errors.length} workflow(s) failed to load:\n`);
    for (const e of errors) {
      console.log(`  ${e.filename}: ${e.error}`);
    }
    console.log('');
  }
}

/**
 * Run a specific workflow
 */

async function resolveCliArchonUserId(): Promise<string | undefined> {
  const cliId = resolveCliUserId();
  if (!cliId) return undefined;
  try {
    const cliUser = await userDb.findOrCreateUserByPlatformIdentity('cli', cliId, cliId);
    return cliUser.id;
  } catch (error) {
    getLog().warn({ err: error as Error, cliId }, 'cli.user_identity_resolve_failed');
    return undefined;
  }
}

function metadataRecord(metadata: Record<string, unknown> | undefined): Record<string, unknown> {
  return metadata ?? {};
}

function resumeBudgetInput(
  metadata: Record<string, unknown> | undefined
): HardenedControllerSession['workflowBudgetGrants'][number]['tokens'] | undefined {
  const raw = metadata?.[WORKFLOW_BUDGET_METADATA_KEY];
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return undefined;
  const tokens = (raw as Record<string, unknown>).tokens;
  if (typeof tokens !== 'object' || tokens === null || Array.isArray(tokens)) return undefined;
  const record = tokens as Record<string, unknown>;
  if (typeof record.total !== 'number') return undefined;
  return {
    total: record.total,
    ...(typeof record.input === 'number' ? { input: record.input } : {}),
    ...(typeof record.output === 'number' ? { output: record.output } : {}),
  };
}

function resumeDeadlineInput(metadata: Record<string, unknown> | undefined): string | undefined {
  const raw = metadata?.[WORKFLOW_BUDGET_METADATA_KEY];
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return undefined;
  const deadlineAt = (raw as Record<string, unknown>).deadlineAt;
  return typeof deadlineAt === 'string' ? deadlineAt : undefined;
}

function createHardenedControllerSessionForRun(input: {
  run: WorkflowRun;
  workflow: WorkflowDefinition;
  workflowSource: WorkflowSource | undefined;
  sourceRoot: string;
  repoInputs?: readonly HardenedControllerRepoInput[];
  conversationId: string;
  userMessage: string;
  image: string;
  requestedImage?: string;
}): HardenedControllerSession {
  const tokens = resumeBudgetInput(input.run.metadata);
  const deadlineAt = resumeDeadlineInput(input.run.metadata);
  return prepareHardenedControllerSession({
    runId: input.run.id,
    workflow: input.workflow,
    workflowSource: input.workflowSource,
    sourceRoot: input.sourceRoot,
    repoInputs: input.repoInputs,
    conversationId: input.conversationId,
    userMessage: input.userMessage,
    image: input.image,
    requestedImage: input.requestedImage,
    ...(tokens || deadlineAt
      ? { budget: { ...(tokens ? { tokens } : {}), ...(deadlineAt ? { deadlineAt } : {}) } }
      : {}),
  });
}

function resumeHardenedControllerSessionForRun(input: {
  run: WorkflowRun;
  workflow: WorkflowDefinition;
  image: string;
}): HardenedControllerSession {
  const tokens = resumeBudgetInput(input.run.metadata);
  const deadlineAt = resumeDeadlineInput(input.run.metadata);
  if (!tokens || !deadlineAt) {
    throw new Error(
      `Cannot resume hardened run '${input.run.id}': persisted budget state is missing or malformed.`
    );
  }
  return resumeHardenedControllerSession({
    runId: input.run.id,
    workflow: input.workflow,
    image: input.image,
    policyMetadata: metadataRecord(input.run.metadata).hardened_controller_policy,
    budget: { tokens, deadlineAt },
  });
}

function resolveDeclaredRepoInputs(
  cfg: Record<string, unknown> | undefined
): readonly HardenedControllerRepoInput[] | undefined {
  const raw = cfg?.repoInputs ?? cfg?.repo_inputs;
  if (raw === undefined) return undefined;
  if (!Array.isArray(raw)) throw new Error('container.repo_inputs must be an array.');
  return raw.map(resolveDeclaredRepoInput);
}

function resolveDeclaredRepoInput(entry: unknown, index: number): HardenedControllerRepoInput {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
    throw new Error(`container.repo_inputs[${String(index)}] must be an object.`);
  }
  const record = entry as Record<string, unknown>;
  if (typeof record.path !== 'string' || record.path.trim().length === 0) {
    throw new Error(`container.repo_inputs[${String(index)}].path must be a non-empty string.`);
  }
  if (record.source !== undefined && typeof record.source !== 'string') {
    throw new Error(`container.repo_inputs[${String(index)}].source must be a string when set.`);
  }
  return {
    targetPath: record.path,
    sourcePath: record.source ?? record.path,
  };
}

function initialBudgetMetadata(session: HardenedControllerSession): WorkflowBudgetState {
  const grant = session.workflowBudgetGrants[0];
  return {
    workflowDigest: grant.workflowDigest,
    deadlineAt: grant.deadlineAt,
    tokens: { ...grant.tokens },
    consumed: { input: 0, output: 0 },
    updatedAt: new Date().toISOString(),
  };
}

export async function workflowRunCommand(
  cwd: string,
  workflowName: string,
  userMessage: string,
  options: WorkflowRunOptions = {}
): Promise<void> {
  const resolved = await resolveWorkflowRunTarget(cwd, workflowName, options);
  emitParseWarnings(resolved.workflowEntry?.parseWarnings, resolved.workflow.name);

  const flagBase = options.baseBranch?.trim() || undefined;
  const pinnedEnabled = resolved.workflow.worktree?.enabled;
  validateWorkflowRunOptions(options, resolved.workflow.name, pinnedEnabled);
  const wantsIsolation = resolveWantsIsolation(options, pinnedEnabled);

  assertNoWorktreeOptionsForFolder(options.folder === true, options);
  assertWorkflowNotWorktreePinnedForFolder(
    options.folder === true,
    pinnedEnabled,
    resolved.workflow.name
  );
  await assertCliWorkflowRequirementsMet(resolved.workflow);

  if (options.detach) {
    await runDetachedWorkflow(cwd, workflowName, resolved.workflow, options, wantsIsolation);
    return;
  }

  const run = await prepareForegroundWorkflowRun(cwd, workflowName, userMessage, options, {
    ...resolved,
    flagBase,
    pinnedEnabled,
    wantsIsolation,
  });
  await executePreparedWorkflowRun(run, userMessage, options);
}

interface ResolvedWorkflowRunTarget {
  workflow: WorkflowDefinition;
  workflowEntry: WorkflowWithSource | undefined;
  workflowSource: WorkflowSource | undefined;
}

async function resolveWorkflowRunTarget(
  cwd: string,
  workflowName: string,
  options: WorkflowRunOptions
): Promise<ResolvedWorkflowRunTarget> {
  const effectiveDiscoveryCwd = options.discoveryCwd ?? cwd;
  const { workflows: workflowEntries, errors } = await loadWorkflows(effectiveDiscoveryCwd);
  maybePrintDiscoverySummary(effectiveDiscoveryCwd, workflowEntries, options);
  if (workflowEntries.length === 0 && errors.length === 0) {
    throw new Error('No workflows found in .archon/workflows/');
  }

  const workflows = workflowEntries.map(ws => ws.workflow);
  const workflow = resolveWorkflowName(workflowName, workflows);
  if (!workflow) throw unresolvedWorkflowError(workflowName, workflows, errors);

  const workflowEntry = workflowEntries.find(ws => ws.workflow === workflow);
  return { workflow, workflowEntry, workflowSource: workflowEntry?.source };
}

function maybePrintDiscoverySummary(
  cwd: string,
  workflowEntries: readonly WorkflowWithSource[],
  options: WorkflowRunOptions
): void {
  if (options.json || options.quiet) return;
  const sourceCounts = countWorkflowSources(workflowEntries);
  console.log(
    `Discovery: root=${cwd} workflows=${String(workflowEntries.length)} ` +
      `bundled=${String(sourceCounts.bundled)} global=${String(sourceCounts.global)} ` +
      `project=${String(sourceCounts.project)}`
  );
}

function unresolvedWorkflowError(
  workflowName: string,
  workflows: WorkflowDefinition[],
  errors: WorkflowLoadResult['errors']
): Error {
  const loadError = errors.find(
    e =>
      e.filename.replace(/\.ya?ml$/, '') === workflowName ||
      e.filename === `${workflowName}.yaml` ||
      e.filename === `${workflowName}.yml`
  );
  if (loadError) {
    return new Error(
      `Workflow '${workflowName}' failed to load: ${loadError.error}\n\nFix the YAML file and try again.`
    );
  }
  const availableWorkflows = workflows.map(w => `  - ${w.name}`).join('\n');
  return new Error(
    `Workflow '${workflowName}' not found.\n\nAvailable workflows:\n${availableWorkflows}`
  );
}

function validateWorkflowRunOptions(
  options: WorkflowRunOptions,
  workflowName: string,
  pinnedEnabled: boolean | undefined
): void {
  validateNoWorktreeOptions(options);
  validateResumeOptions(options);
  validatePinnedWorktreePolicy(options, workflowName, pinnedEnabled);
}

function validateNoWorktreeOptions(options: WorkflowRunOptions): void {
  if (options.branchName !== undefined && options.noWorktree) {
    throw new Error(
      '--branch and --no-worktree are mutually exclusive.\n' +
        '  --branch creates an isolated worktree (safe).\n' +
        '  --no-worktree runs directly in your repo (no isolation).\n' +
        'Use one or the other.'
    );
  }
  if (options.noWorktree && options.fromBranch !== undefined) {
    throw new Error(
      '--from/--from-branch has no effect with --no-worktree.\n' +
        'Remove --from or drop --no-worktree.'
    );
  }
  if (options.noWorktree && options.baseBranch !== undefined) {
    throw new Error(
      '--base has no effect with --no-worktree.\nRemove --base or drop --no-worktree.'
    );
  }
}

function validateResumeOptions(options: WorkflowRunOptions): void {
  if (options.resume && options.branchName !== undefined) {
    throw new Error(
      '--resume and --branch are mutually exclusive.\n' +
        '  --resume reuses the existing worktree from the failed run.\n' +
        '  Remove --branch when using --resume.'
    );
  }
}

function validatePinnedWorktreePolicy(
  options: WorkflowRunOptions,
  workflowName: string,
  pinnedEnabled: boolean | undefined
): void {
  if (pinnedEnabled === false) validateDisabledWorktreePolicy(options, workflowName);
  if (pinnedEnabled === true && options.noWorktree) {
    throw new Error(
      `Workflow '${workflowName}' sets worktree.enabled: true (requires a worktree).\n` +
        '  --no-worktree conflicts with the workflow policy.\n' +
        "  Drop --no-worktree or change the workflow's worktree.enabled."
    );
  }
}

function validateDisabledWorktreePolicy(options: WorkflowRunOptions, workflowName: string): void {
  if (options.branchName !== undefined) {
    throw new Error(
      `Workflow '${workflowName}' sets worktree.enabled: false (runs in live checkout).\n` +
        '  --branch requires an isolated worktree.\n' +
        "  Drop --branch or change the workflow's worktree.enabled."
    );
  }
  if (options.fromBranch !== undefined) {
    throw new Error(
      `Workflow '${workflowName}' sets worktree.enabled: false (runs in live checkout).\n` +
        '  --from/--from-branch only applies when a worktree is created.\n' +
        "  Drop --from or change the workflow's worktree.enabled."
    );
  }
  if (options.baseBranch !== undefined) {
    throw new Error(
      `Workflow '${workflowName}' sets worktree.enabled: false (runs in live checkout).\n` +
        '  --base only applies when a worktree is created.\n' +
        "  Drop --base or change the workflow's worktree.enabled."
    );
  }
}

function resolveWantsIsolation(
  options: WorkflowRunOptions,
  pinnedEnabled: boolean | undefined
): boolean {
  const flagWantsIsolation = !options.resume && !options.noWorktree;
  return !options.resume && pinnedEnabled !== undefined ? pinnedEnabled : flagWantsIsolation;
}

async function runDetachedWorkflow(
  cwd: string,
  workflowName: string,
  workflow: WorkflowDefinition,
  options: WorkflowRunOptions,
  wantsIsolation: boolean
): Promise<void> {
  const childConversationId = options.conversationId ?? generateConversationId();
  const extraArgs: string[] = [];
  const detachIsFolder = await detectDetachedFolderProject(cwd, options.folder === true);
  assertNoWorktreeOptionsForFolder(detachIsFolder, options);

  const pinnedBranch = maybePinDetachedBranch(
    workflowName,
    options,
    wantsIsolation,
    detachIsFolder,
    extraArgs
  );
  if (options.conversationId === undefined)
    extraArgs.push('--conversation-id', childConversationId);

  const logPath = await spawnDetachedWorkflowRun(cwd, childConversationId, extraArgs);
  if (options.json)
    await emitDetachedWorkflowJson(workflow, options, childConversationId, pinnedBranch, logPath);
  else emitDetachedWorkflowText(workflow, logPath);
}

async function detectDetachedFolderProject(cwd: string, declaredFolder: boolean): Promise<boolean> {
  if (declaredFolder) return true;
  try {
    const existing =
      (await codebaseDb.findCodebaseByDefaultCwd(cwd)) ??
      (await codebaseDb.findCodebaseByPathPrefix(cwd));
    return existing?.kind === 'folder';
  } catch (err) {
    getLog().debug({ err: err as Error, cwd }, 'cli.folder_detect_probe_failed');
    return false;
  }
}

function maybePinDetachedBranch(
  workflowName: string,
  options: WorkflowRunOptions,
  wantsIsolation: boolean,
  detachIsFolder: boolean,
  extraArgs: string[]
): string | undefined {
  if (!wantsIsolation || detachIsFolder || options.branchName !== undefined) return undefined;
  const pinnedBranch = `${workflowName}-${String(Date.now())}`;
  extraArgs.push('--branch', pinnedBranch);
  return pinnedBranch;
}

async function emitDetachedWorkflowJson(
  workflow: WorkflowDefinition,
  options: WorkflowRunOptions,
  conversationId: string,
  pinnedBranch: string | undefined,
  logPath: string | null
): Promise<void> {
  await writeJsonLine({
    ok: true,
    action: 'run',
    detached: true,
    workflow: workflow.name,
    branch: pinnedBranch ?? options.branchName ?? null,
    conversationId,
    logPath,
  });
}

function emitDetachedWorkflowText(workflow: WorkflowDefinition, logPath: string | null): void {
  console.log(`Started '${workflow.name}' in the background.`);
  console.log('Track it with: archon workflow runs');
  if (logPath) console.log(`Child output: ${logPath}`);
  else console.warn('Warning: could not open a log file — child output will not be captured.');
}

type CodebaseRow = NonNullable<Awaited<ReturnType<typeof codebaseDb.findCodebaseByDefaultCwd>>>;
type ConversationRow = Awaited<ReturnType<typeof conversationDb.getOrCreateConversation>>;
type WorkflowDeps = ReturnType<typeof createWorkflowDeps>;
type WorkflowRunResult = Awaited<ReturnType<typeof executeWorkflow>>;

type RuntimeWorkflowTarget = ResolvedWorkflowRunTarget & {
  flagBase: string | undefined;
  pinnedEnabled: boolean | undefined;
  wantsIsolation: boolean;
};

interface CodebaseResolution {
  codebase: CodebaseRow | null;
  lookupError: Error | null;
  registrationError: Error | null;
}

interface PreparedWorkflowRun {
  cwd: string;
  workflowName: string;
  workflow: WorkflowDefinition;
  workflowEntry: WorkflowWithSource | undefined;
  workflowSource: WorkflowSource | undefined;
  flagBase: string | undefined;
  codebase: CodebaseRow | null;
  codebaseDefaultBranch: string | undefined;
  adapter: CLIAdapter;
  conversationId: string;
  conversation: ConversationRow;
  cliUserId: string | undefined;
  workingCwd: string;
  isolationEnvId: string | undefined;
  execContext: ExecutionContext;
  containerBackend: ContainerBackend | undefined;
  containerEnvId: string | undefined;
  hardenedControllerSession: HardenedControllerSession | undefined;
  preCreatedHardenedRun: WorkflowRun | undefined;
  resumable: WorkflowRun | null;
}

async function prepareForegroundWorkflowRun(
  cwd: string,
  workflowName: string,
  userMessage: string,
  options: WorkflowRunOptions,
  target: RuntimeWorkflowTarget
): Promise<PreparedWorkflowRun> {
  console.log(`Running workflow: ${workflowName}`);
  console.log(`Working directory: ${cwd}`);
  console.log('');

  const adapter = new CLIAdapter();
  const conversationId = options.conversationId ?? generateConversationId();
  const conversation = await getOrCreateCliConversation(conversationId);
  const cliUserId = await resolveCliArchonUserId();
  const codebaseState = await resolveWorkflowRunCodebase(cwd, options);
  validateFolderRegistration(options, codebaseState);

  const isolation = await prepareWorkflowIsolation(
    cwd,
    workflowName,
    userMessage,
    options,
    target,
    {
      ...codebaseState,
      conversation,
      conversationId,
      cliUserId,
    }
  );

  await finalizeConversationState(
    conversation,
    codebaseState.codebase,
    isolation.workingCwd,
    isolation.isolationEnvId
  );
  adapter.setConversationDbId(conversationId, conversation.id);
  await persistCliUserMessage(conversation.id, userMessage, cliUserId);
  startWorkflowTitleGeneration(
    target.workflow,
    workflowName,
    userMessage,
    cwd,
    isolation.workingCwd,
    conversation
  );

  return {
    cwd,
    workflowName,
    workflow: target.workflow,
    workflowEntry: target.workflowEntry,
    workflowSource: target.workflowSource,
    flagBase: target.flagBase,
    codebase: codebaseState.codebase,
    adapter,
    conversationId,
    conversation,
    cliUserId,
    ...isolation,
  };
}

async function getOrCreateCliConversation(conversationId: string): Promise<ConversationRow> {
  try {
    return await conversationDb.getOrCreateConversation('cli', conversationId);
  } catch (error) {
    const err = error as Error;
    throw new Error(
      `Failed to access database: ${err.message}\nHint: Check that DATABASE_URL is set and the database is running.`
    );
  }
}

async function resolveWorkflowRunCodebase(
  cwd: string,
  options: WorkflowRunOptions
): Promise<CodebaseResolution> {
  const state: CodebaseResolution = { codebase: null, lookupError: null, registrationError: null };
  await lookupCodebaseByPath(cwd, state);
  await lookupCodebaseById(options, state);
  await maybeRegisterCodebase(cwd, options, state);
  return state;
}

async function lookupCodebaseByPath(cwd: string, state: CodebaseResolution): Promise<void> {
  try {
    state.codebase =
      (await codebaseDb.findCodebaseByDefaultCwd(cwd)) ??
      (await codebaseDb.findCodebaseByPathPrefix(cwd));
  } catch (error) {
    const err = error as Error;
    state.lookupError = err;
    getLog().warn({ err, cwd }, 'cli.codebase_lookup_failed');
    maybeLogDbConnectionHint(err);
  }
}

function maybeLogDbConnectionHint(err: Error): void {
  if (!isConnectionErrorMessage(err.message)) return;
  getLog().warn(
    { hint: 'Check DATABASE_URL and that the database is running.' },
    'cli.db_connection_hint'
  );
}

function isConnectionErrorMessage(message: string): boolean {
  return (
    message.includes('connect') || message.includes('ECONNREFUSED') || message.includes('ETIMEDOUT')
  );
}

async function lookupCodebaseById(
  options: WorkflowRunOptions,
  state: CodebaseResolution
): Promise<void> {
  if (state.codebase || state.lookupError || !options.codebaseId) return;
  try {
    state.codebase = await codebaseDb.getCodebase(options.codebaseId);
  } catch (error) {
    const err = error as Error;
    getLog().warn(
      { err, errorType: err.constructor.name, codebaseId: options.codebaseId },
      'cli.codebase_id_lookup_failed'
    );
  }
}

async function maybeRegisterCodebase(
  cwd: string,
  options: WorkflowRunOptions,
  state: CodebaseResolution
): Promise<void> {
  if (state.codebase || state.lookupError) return;
  const repoRoot = await git.findRepoRoot(cwd);
  if (repoRoot) await registerGitCodebase(repoRoot, state);
  else if (options.folder) await registerFolderCodebase(cwd, state);
}

async function registerGitCodebase(repoRoot: string, state: CodebaseResolution): Promise<void> {
  try {
    const result = await registerRepository(repoRoot);
    state.codebase = await codebaseDb.getCodebase(result.codebaseId);
    if (!result.alreadyExisted)
      getLog().info({ name: result.name }, 'cli.codebase_auto_registered');
  } catch (error) {
    const err = error as Error;
    state.registrationError = err;
    getLog().warn(
      { err, errorType: err.constructor.name, repoRoot },
      'cli.codebase_auto_registration_failed'
    );
  }
}

async function registerFolderCodebase(cwd: string, state: CodebaseResolution): Promise<void> {
  try {
    const result = await registerFolder(cwd);
    state.codebase = await codebaseDb.getCodebase(result.codebaseId);
    if (!result.alreadyExisted) {
      console.log(`Registered folder project "${result.name}" (${result.defaultCwd})`);
      getLog().info({ name: result.name }, 'cli.folder_project_auto_registered');
    }
  } catch (error) {
    const err = error as Error;
    state.registrationError = err;
    getLog().warn(
      { err, errorType: err.constructor.name, cwd },
      'cli.folder_project_auto_registration_failed'
    );
  }
}

function validateFolderRegistration(options: WorkflowRunOptions, state: CodebaseResolution): void {
  if (options.folder && !state.codebase && state.registrationError) {
    throw buildFolderRegistrationFailureError(state.registrationError);
  }
}

interface IsolationInputs extends CodebaseResolution {
  conversation: ConversationRow;
  conversationId: string;
  cliUserId: string | undefined;
}

function requireCodebase(input: IsolationInputs): CodebaseRow {
  if (!input.codebase) throw new Error('Codebase was not resolved for workflow isolation.');
  return input.codebase;
}

interface WorkflowIsolationState {
  workingCwd: string;
  isolationEnvId: string | undefined;
  execContext: ExecutionContext;
  containerBackend: ContainerBackend | undefined;
  containerEnvId: string | undefined;
  hardenedControllerSession: HardenedControllerSession | undefined;
  preCreatedHardenedRun: WorkflowRun | undefined;
  resumable: WorkflowRun | null;
  codebaseDefaultBranch: string | undefined;
}

async function prepareWorkflowIsolation(
  cwd: string,
  workflowName: string,
  userMessage: string,
  options: WorkflowRunOptions,
  target: RuntimeWorkflowTarget,
  input: IsolationInputs
): Promise<WorkflowIsolationState> {
  const state: WorkflowIsolationState = newWorkflowIsolationState(cwd, input.codebase);
  if (options.resume)
    await applyResumeIsolation(workflowName, cwd, options, target.workflow, input, state);

  const isFolderCodebase = input.codebase?.kind === 'folder';
  validateContainerRoute(options, isFolderCodebase);
  assertNoWorktreeOptionsForFolder(isFolderCodebase, options);
  assertWorkflowNotWorktreePinnedForFolder(
    isFolderCodebase,
    target.pinnedEnabled,
    target.workflow.name
  );

  if (isFolderCodebase && input.codebase) {
    await prepareFolderProjectIsolation(userMessage, options, target, input, state);
  } else if (target.wantsIsolation && input.codebase) {
    await prepareGitWorktreeIsolation(
      workflowName,
      options,
      input.codebase,
      state,
      target.flagBase
    );
  } else if (options.noWorktree) {
    getLog().info({ cwd }, 'workflow.running_without_isolation');
  } else if (target.wantsIsolation) {
    throwMissingIsolationCodebase(input);
  }
  return state;
}

function newWorkflowIsolationState(
  cwd: string,
  codebase: CodebaseRow | null
): WorkflowIsolationState {
  return {
    workingCwd: cwd,
    isolationEnvId: undefined,
    execContext: { kind: 'host' },
    containerBackend: undefined,
    containerEnvId: undefined,
    hardenedControllerSession: undefined,
    preCreatedHardenedRun: undefined,
    resumable: null,
    codebaseDefaultBranch: codebase?.default_branch?.trim() || undefined,
  };
}

function validateContainerRoute(options: WorkflowRunOptions, isFolderCodebase: boolean): void {
  if (!options.container || isFolderCodebase) return;
  throw new Error(
    'Container isolation is folder-project-only for now. Run --container against a ' +
      'registered folder project (or add --folder to register this directory as one). ' +
      'Repo projects use worktree isolation.'
  );
}

async function applyResumeIsolation(
  workflowName: string,
  cwd: string,
  options: WorkflowRunOptions,
  workflow: WorkflowDefinition,
  input: IsolationInputs,
  state: WorkflowIsolationState
): Promise<void> {
  assertCanResume(input);
  const codebase = input.codebase;
  const resumable = await workflowDb.findResumableRun(workflowName, cwd);
  if (!resumable)
    throw new Error(`No resumable run found for workflow '${workflowName}' at path '${cwd}'.`);

  assertResumeRoute(resumable, codebase, workflow);
  logResumableRun(workflowName, resumable);
  state.resumable = resumable;
  state.workingCwd = await resolveResumeWorkingCwd(resumable, state.workingCwd);
  state.isolationEnvId = await findResumeIsolationEnvId(codebase.id, state.workingCwd);
  printResumeInfo(resumable.id, state.workingCwd);
  if (options.baseBranch?.trim())
    warnBaseOverrideOnReuse(state.workingCwd, options.baseBranch.trim());
}

function assertCanResume(
  input: IsolationInputs
): asserts input is IsolationInputs & { codebase: CodebaseRow } {
  if (input.codebase) return;
  if (input.lookupError) {
    throw new Error(
      'Cannot resume: Database lookup failed.\n' +
        `Error: ${input.lookupError.message}\n` +
        'Hint: Check your database connection before using --resume.'
    );
  }
  if (input.registrationError)
    throw buildRegistrationFailureError('resume', input.registrationError);
  throw new Error(
    'Cannot resume: Not in a git repository.\nEither run from a git repo or use /clone first.'
  );
}

function assertResumeRoute(
  resumable: WorkflowRun,
  codebase: CodebaseRow,
  workflow: WorkflowDefinition
): void {
  assertHardenedControllerResumeRoute(
    resumable.id,
    resumable.metadata?.isolation,
    workflow.hardened?.required === true
  );
  if (resumable.metadata?.isolation === 'container' && codebase.kind !== 'folder') {
    throw new Error('Cannot resume hardened container run through a non-folder isolation route.');
  }
}

function logResumableRun(workflowName: string, resumable: WorkflowRun): void {
  getLog().info(
    { workflowRunId: resumable.id, workflowName, workingPath: resumable.working_path },
    'workflow.resume_found_resumable'
  );
}

async function resolveResumeWorkingCwd(
  resumable: WorkflowRun,
  currentCwd: string
): Promise<string> {
  if (!resumable.working_path) return currentCwd;
  const { existsSync } = await import('fs');
  if (!existsSync(resumable.working_path)) {
    throw new Error(
      `Cannot resume: the working path from the run no longer exists: ${resumable.working_path}\n` +
        'The worktree may have been cleaned up. Start a fresh run with --branch instead.'
    );
  }
  return resumable.working_path;
}

async function findResumeIsolationEnvId(
  codebaseId: string,
  workingCwd: string
): Promise<string | undefined> {
  const allEnvs = await isolationDb.listByCodebase(codebaseId);
  const matchingEnv = allEnvs.find(e => e.working_path === workingCwd);
  if (!matchingEnv) return undefined;
  getLog().info({ envId: matchingEnv.id, workingPath: workingCwd }, 'workflow.resume_env_found');
  return matchingEnv.id;
}

function printResumeInfo(runId: string, workingCwd: string): void {
  console.log(`Resuming workflow run: ${runId}`);
  console.log(`Working path: ${workingCwd}`);
  console.log('');
}

function throwMissingIsolationCodebase(input: IsolationInputs): never {
  if (input.lookupError) {
    throw new Error(
      'Cannot create worktree: database lookup failed.\n' +
        `Error: ${input.lookupError.message}\n` +
        'Hint: Check your database connection, or use --no-worktree to skip isolation.'
    );
  }
  if (input.registrationError)
    throw buildRegistrationFailureError('create worktree', input.registrationError);
  throw new Error(
    'Cannot create worktree: not in a git repository.\n' +
      'Run from within a git repo, or use --no-worktree to skip isolation.'
  );
}

async function prepareFolderProjectIsolation(
  userMessage: string,
  options: WorkflowRunOptions,
  target: RuntimeWorkflowTarget,
  input: IsolationInputs,
  state: WorkflowIsolationState
): Promise<void> {
  const codebase = requireCodebase(input);
  const folderCodebase = {
    id: codebase.id,
    defaultCwd: codebase.default_cwd,
    name: codebase.name,
    kind: 'folder' as const,
  };
  const folderConfig = await loadConfig(codebase.default_cwd);
  const wantsContainer = options.resume
    ? state.resumable?.metadata?.isolation === 'container'
    : (options.container ??
      target.workflow.container?.enabled ??
      folderConfig?.container?.enabled ??
      false);

  if (wantsContainer)
    await prepareFolderContainer(
      userMessage,
      options,
      target,
      input,
      state,
      folderCodebase,
      folderConfig
    );
  else await prepareFolderInPlace(folderCodebase, state);
}

async function prepareFolderInPlace(
  folderCodebase: { id: string; defaultCwd: string; name: string; kind: 'folder' },
  state: WorkflowIsolationState
): Promise<void> {
  console.log('Folder project — running in place (no worktree isolation).');
  getLog().info({ cwd: state.workingCwd }, 'workflow.running_without_isolation');
  const backend = resolveFolderBackend(folderCodebase, { container: false });
  const prepared = await backend.prepare({ codebase: folderCodebase });
  state.execContext = prepared.execContext;
}

async function prepareFolderContainer(
  userMessage: string,
  options: WorkflowRunOptions,
  target: RuntimeWorkflowTarget,
  input: IsolationInputs,
  state: WorkflowIsolationState,
  folderCodebase: { id: string; defaultCwd: string; name: string; kind: 'folder' },
  folderConfig: Awaited<ReturnType<typeof loadConfig>>
): Promise<void> {
  const containerConfig = resolveContainerBackendConfig(folderConfig?.container);
  const isolationStore = isolationDb.createIsolationStore();
  let backend = resolveFolderBackend(folderCodebase, {
    container: true,
    store: isolationStore,
    containerConfig,
  });
  const resolvedImageId = await backend.resolveImage();
  const prepared = options.resume
    ? await resumeFolderContainerRun(
        target.workflow,
        state,
        isolationStore,
        containerConfig,
        folderCodebase,
        resolvedImageId
      )
    : await prepareFreshFolderContainerRun(
        userMessage,
        target,
        input,
        state,
        isolationStore,
        containerConfig,
        folderCodebase,
        folderConfig,
        resolvedImageId
      );
  backend = prepared.backend;
  state.workingCwd = prepared.env.cwd;
  state.execContext = prepared.env.execContext;
  state.containerBackend = backend;
  state.containerEnvId = prepared.env.envId;
  state.isolationEnvId = prepared.env.envId;
}

async function resumeFolderContainerRun(
  workflow: WorkflowDefinition,
  state: WorkflowIsolationState,
  isolationStore: ReturnType<typeof isolationDb.createIsolationStore>,
  containerConfig: ContainerBackendConfig,
  folderCodebase: { id: string; defaultCwd: string; name: string; kind: 'folder' },
  resolvedImageId: string
): Promise<{ backend: ContainerBackend; env: PreparedEnv }> {
  const resumable = state.resumable;
  if (!resumable) {
    throw new Error(
      "Cannot resume container run '?': its isolation env id is " +
        'missing from the run metadata. Start a fresh --container run instead.'
    );
  }
  const resumeEnvId =
    typeof resumable?.metadata?.isolation_env_id === 'string'
      ? resumable.metadata.isolation_env_id
      : undefined;
  if (!resumeEnvId) {
    throw new Error(
      `Cannot resume container run '${resumable?.id ?? '?'}': its isolation env id is ` +
        'missing from the run metadata. Start a fresh --container run instead.'
    );
  }
  console.log(`Folder project — resuming container run (image ${containerConfig.image}).`);
  getLog().info(
    { envId: resumeEnvId, image: containerConfig.image, resolvedImageId },
    'workflow.resuming_in_container'
  );
  try {
    const session = resumeHardenedControllerSessionForRun({
      run: resumable,
      workflow,
      image: resolvedImageId,
    });
    state.hardenedControllerSession = session;
    const boundConfig = bindHardenedContainerConfig(containerConfig, session);
    await assertResumeContainerMetadata(isolationStore, resumeEnvId, boundConfig, session);
    const backend = resolveFolderBackend(folderCodebase, {
      container: true,
      store: isolationStore,
      containerConfig: boundConfig,
    });
    const env = await backend.resumeEnv(
      resumeEnvId,
      buildContainerResumeBinding(boundConfig, session)
    );
    return { backend, env };
  } catch (resumeErr) {
    const err = resumeErr as Error;
    getLog().error({ err, envId: resumeEnvId }, 'workflow.container_resume_failed');
    throw new Error(classifyIsolationError(err));
  }
}

async function assertResumeContainerMetadata(
  isolationStore: ReturnType<typeof isolationDb.createIsolationStore>,
  resumeEnvId: string,
  boundConfig: ContainerBackendConfig,
  session: HardenedControllerSession
): Promise<void> {
  const isolationRow = await isolationStore.getById(resumeEnvId);
  assertHardenedEgressEnvironment(
    boundConfig,
    isolationRow?.metadata,
    session.policyMetadata.runId
  );
}

function buildContainerResumeBinding(
  boundConfig: ContainerBackendConfig,
  session: HardenedControllerSession
): Parameters<ContainerBackend['resumeEnv']>[1] {
  return {
    image: boundConfig.image,
    ownerRunId: session.policyMetadata.runId,
    egressPolicyB64: boundConfig.egressPolicy
      ? encodeStrictEgressPolicy(boundConfig.egressPolicy)
      : undefined,
    proxyBudgetSeedDigest: boundConfig.proxyBudget
      ? sessionProxyBudgetDigest(boundConfig.proxyBudget)
      : undefined,
  };
}

async function prepareFreshFolderContainerRun(
  userMessage: string,
  target: RuntimeWorkflowTarget,
  input: IsolationInputs,
  state: WorkflowIsolationState,
  isolationStore: ReturnType<typeof isolationDb.createIsolationStore>,
  containerConfig: ContainerBackendConfig,
  folderCodebase: { id: string; defaultCwd: string; name: string; kind: 'folder' },
  folderConfig: Awaited<ReturnType<typeof loadConfig>>,
  resolvedImageId: string
): Promise<{ backend: ContainerBackend; env: PreparedEnv }> {
  console.log(`Folder project — running in container (image ${containerConfig.image}).`);
  getLog().info(
    { cwd: folderCodebase.defaultCwd, image: containerConfig.image, resolvedImageId },
    'workflow.running_in_container'
  );
  const codebase = requireCodebase(input);
  const controllerDeps = createWorkflowDeps();
  let backend = resolveFolderBackend(folderCodebase, {
    container: true,
    store: isolationStore,
    containerConfig,
  });
  let prepared: PreparedEnv | undefined;
  try {
    const workflowPin = buildWorkflowPinState(target.workflow, target.workflowSource);
    state.preCreatedHardenedRun = await createPendingHardenedRun(
      controllerDeps,
      target.workflow,
      input,
      userMessage,
      workflowPin
    );
    state.hardenedControllerSession = createFreshHardenedSession(
      state.preCreatedHardenedRun,
      target,
      input,
      userMessage,
      folderCodebase.defaultCwd,
      resolveDeclaredRepoInputs(folderConfig?.container as Record<string, unknown> | undefined),
      resolvedImageId,
      containerConfig.image
    );
    backend = resolveFolderBackend(folderCodebase, {
      container: true,
      store: isolationStore,
      containerConfig: bindHardenedContainerConfig(
        containerConfig,
        state.hardenedControllerSession
      ),
    });
    prepared = await backend.prepare({
      codebase: folderCodebase,
      ownerRunId: state.preCreatedHardenedRun.id,
      seed: requireHardenedSeed(state.hardenedControllerSession),
    });
    await stampPreparedHardenedRun(controllerDeps, state, prepared, workflowPin);
    return { backend, env: prepared };
  } catch (prepErr) {
    return cleanupFailedContainerPrepare(
      backend,
      prepared,
      state.preCreatedHardenedRun,
      prepErr,
      codebase
    );
  }
}

async function createPendingHardenedRun(
  controllerDeps: WorkflowDeps,
  workflow: WorkflowDefinition,
  input: IsolationInputs,
  userMessage: string,
  workflowPin: ReturnType<typeof buildWorkflowPinState>
): Promise<WorkflowRun> {
  const codebase = requireCodebase(input);
  return controllerDeps.store.createWorkflowRun({
    workflow_name: workflow.name,
    conversation_id: input.conversation.id,
    codebase_id: codebase.id,
    user_message: userMessage,
    working_path: codebase.default_cwd,
    metadata: { isolation: 'container', [WORKFLOW_PIN_METADATA_KEY]: workflowPin },
    user_id: input.cliUserId,
  });
}

function createFreshHardenedSession(
  run: WorkflowRun,
  target: RuntimeWorkflowTarget,
  input: IsolationInputs,
  userMessage: string,
  sourceRoot: string,
  repoInputs: readonly HardenedControllerRepoInput[] | undefined,
  image: string,
  requestedImage: string | undefined
): HardenedControllerSession {
  return createHardenedControllerSessionForRun({
    run,
    workflow: target.workflow,
    workflowSource: target.workflowSource,
    sourceRoot,
    repoInputs,
    conversationId: input.conversationId,
    userMessage,
    image,
    requestedImage,
  });
}

function requireHardenedSeed(
  session: HardenedControllerSession
): NonNullable<HardenedControllerSession['seed']> {
  if (!session.seed) throw new Error('Hardened controller did not produce a prepare seed.');
  return session.seed;
}

function requirePreCreatedHardenedRun(state: WorkflowIsolationState): WorkflowRun {
  if (!state.preCreatedHardenedRun) throw new Error('Hardened workflow run was not pre-created.');
  return state.preCreatedHardenedRun;
}

function requireHardenedControllerSession(
  session: HardenedControllerSession | undefined
): HardenedControllerSession {
  if (!session) throw new Error('Hardened controller session was not prepared.');
  return session;
}

function requireContainerBackend(backend: ContainerBackend | undefined): ContainerBackend {
  if (!backend) throw new Error('Container backend was not prepared.');
  return backend;
}

function requireContainerEnvId(envId: string | undefined): string {
  if (!envId) throw new Error('Container environment id was not prepared.');
  return envId;
}

async function stampPreparedHardenedRun(
  controllerDeps: WorkflowDeps,
  state: WorkflowIsolationState,
  prepared: PreparedEnv,
  workflowPin: ReturnType<typeof buildWorkflowPinState>
): Promise<void> {
  const run = requirePreCreatedHardenedRun(state);
  const session = requireHardenedControllerSession(state.hardenedControllerSession);
  const preparedMetadata = {
    ...metadataRecord(run.metadata),
    isolation: 'container',
    isolation_env_id: prepared.envId,
    hardened_controller_policy: session.policyMetadata,
    hardened_controller_policy_path: session.policyPath,
    [WORKFLOW_BUDGET_METADATA_KEY]: initialBudgetMetadata(session),
    [WORKFLOW_PIN_METADATA_KEY]: workflowPin,
  };
  await controllerDeps.store.updateWorkflowRun(run.id, { metadata: preparedMetadata });
  state.preCreatedHardenedRun = { ...run, metadata: preparedMetadata };
}

async function cleanupFailedContainerPrepare(
  backend: ContainerBackend,
  prepared: PreparedEnv | undefined,
  preCreatedRun: WorkflowRun | undefined,
  prepErr: unknown,
  codebase: CodebaseRow
): Promise<never> {
  if (prepared?.envId) {
    await backend.destroy(prepared.envId).catch(destroyErr => {
      getLog().error(
        { err: destroyErr as Error, envId: prepared?.envId },
        'workflow.container_prepare_cleanup_failed'
      );
    });
  }
  if (preCreatedRun) {
    await createWorkflowDeps()
      .store.failWorkflowRun(preCreatedRun.id, (prepErr as Error).message)
      .catch(() => undefined);
  }
  const err = prepErr as Error;
  getLog().error({ err, codebaseId: codebase.id }, 'workflow.container_prepare_failed');
  throw new Error(classifyIsolationError(err));
}

async function prepareGitWorktreeIsolation(
  workflowName: string,
  options: WorkflowRunOptions,
  codebase: CodebaseRow,
  state: WorkflowIsolationState,
  flagBase: string | undefined
): Promise<void> {
  configureIsolation(
    async (repoPath: string) => (await loadRepoConfig(repoPath))?.worktree ?? null
  );
  const provider = getIsolationProvider();
  const branchIdentifier = options.branchName ?? `${workflowName}-${Date.now()}`;
  const existingEnv = options.branchName
    ? await isolationDb.findActiveByWorkflow(codebase.id, 'task', options.branchName)
    : undefined;

  if (existingEnv && (await provider.healthCheck(existingEnv.working_path))) {
    await reuseExistingWorktree(existingEnv, options, codebase, state, flagBase);
    return;
  }
  await createNewWorktree(
    provider,
    workflowName,
    branchIdentifier,
    options,
    codebase,
    state,
    flagBase
  );
}

async function reuseExistingWorktree(
  existingEnv: NonNullable<Awaited<ReturnType<typeof isolationDb.findActiveByWorkflow>>>,
  options: WorkflowRunOptions,
  codebase: CodebaseRow,
  state: WorkflowIsolationState,
  flagBase: string | undefined
): Promise<void> {
  if (options.fromBranch) warnFromBranchIgnored(existingEnv.working_path, options.fromBranch);
  if (flagBase) warnBaseOverrideOnReuse(existingEnv.working_path, flagBase);
  await warnIfReuseBaseMismatches(existingEnv, codebase, state.codebaseDefaultBranch, flagBase);
  getLog().info({ path: existingEnv.working_path }, 'worktree_reused');
  state.workingCwd = existingEnv.working_path;
  state.isolationEnvId = existingEnv.id;
}

function warnFromBranchIgnored(workingPath: string, fromBranch: string): void {
  getLog().warn({ path: workingPath, fromBranch }, 'worktree.reuse_from_branch_ignored');
  console.warn(
    `Warning: Reusing existing worktree at ${workingPath}. ` +
      `--from ${fromBranch} was not applied (worktree already exists).`
  );
}

async function warnIfReuseBaseMismatches(
  existingEnv: NonNullable<Awaited<ReturnType<typeof isolationDb.findActiveByWorkflow>>>,
  codebase: CodebaseRow,
  codebaseDefaultBranch: string | undefined,
  flagBase: string | undefined
): Promise<void> {
  try {
    const configuredBase = await resolveReuseConfiguredBase(
      codebase,
      codebaseDefaultBranch,
      flagBase
    );
    const isValidBase = await git.isAncestorOf(
      git.toWorktreePath(existingEnv.working_path),
      `origin/${configuredBase}`
    );
    if (!isValidBase) warnReuseBaseMismatch(existingEnv, configuredBase);
  } catch (e) {
    getLog().debug({ err: e }, 'worktree.reuse_base_branch_check_skipped');
  }
}

async function resolveReuseConfiguredBase(
  codebase: CodebaseRow,
  codebaseDefaultBranch: string | undefined,
  flagBase: string | undefined
): Promise<git.BranchName> {
  const repoConfig = await loadRepoConfig(codebase.default_cwd);
  const rawBase = repoConfig?.worktree?.baseBranch?.trim();
  if (flagBase) return git.toBranchName(flagBase);
  if (rawBase) return git.toBranchName(rawBase);
  if (codebaseDefaultBranch) return git.toBranchName(codebaseDefaultBranch);
  return git.getDefaultBranch(git.toRepoPath(codebase.default_cwd));
}

function warnReuseBaseMismatch(
  existingEnv: NonNullable<Awaited<ReturnType<typeof isolationDb.findActiveByWorkflow>>>,
  configuredBase: git.BranchName
): void {
  getLog().warn(
    { path: existingEnv.working_path, configuredBase, branch: existingEnv.branch_name },
    'worktree.reuse_base_branch_mismatch'
  );
  console.warn(
    `Warning: Worktree '${existingEnv.branch_name}' is not based on '${configuredBase}'. ` +
      `Recreate with: bun run cli complete ${existingEnv.branch_name} --force`
  );
}

async function createNewWorktree(
  provider: ReturnType<typeof getIsolationProvider>,
  workflowName: string,
  branchIdentifier: string,
  options: WorkflowRunOptions,
  codebase: CodebaseRow,
  state: WorkflowIsolationState,
  flagBase: string | undefined
): Promise<void> {
  getLog().info({ branch: branchIdentifier, fromBranch: options.fromBranch }, 'worktree_creating');
  const isolatedEnv = await provider.create({
    workflowType: 'task',
    identifier: branchIdentifier,
    fromBranch: options.fromBranch?.trim()
      ? git.toBranchName(options.fromBranch.trim())
      : undefined,
    baseBranch: state.codebaseDefaultBranch
      ? git.toBranchName(state.codebaseDefaultBranch)
      : undefined,
    baseOverride: flagBase ? git.toBranchName(flagBase) : undefined,
    codebaseId: codebase.id,
    codebaseName: codebase.name,
    canonicalRepoPath: git.toRepoPath(codebase.default_cwd),
    description: `CLI workflow: ${workflowName}`,
  });
  const envRecord = await isolationDb.create({
    codebase_id: codebase.id,
    workflow_type: 'task',
    workflow_id: branchIdentifier,
    provider: 'worktree',
    working_path: isolatedEnv.workingPath,
    branch_name: isolatedEnv.branchName,
    created_by_platform: 'cli',
    metadata: {},
  });
  state.workingCwd = isolatedEnv.workingPath;
  state.isolationEnvId = envRecord.id;
  getLog().info({ path: state.workingCwd }, 'worktree_created');
}

async function finalizeConversationState(
  conversation: ConversationRow,
  codebase: CodebaseRow | null,
  workingCwd: string,
  isolationEnvId: string | undefined
): Promise<void> {
  try {
    await conversationDb.updateConversation(conversation.id, {
      cwd: workingCwd,
      codebase_id: codebase?.id ?? null,
      isolation_env_id: isolationEnvId ?? null,
    });
  } catch (error) {
    const err = error as Error;
    throw new Error(`Failed to update conversation: ${err.message}`);
  }
}

async function persistCliUserMessage(
  conversationDbId: string,
  userMessage: string,
  cliUserId: string | undefined
): Promise<void> {
  try {
    await messageDb.addMessage(conversationDbId, 'user', userMessage, undefined, cliUserId);
  } catch (error) {
    getLog().warn(
      { err: error as Error, conversationId: conversationDbId },
      'cli_user_message_persist_failed'
    );
  }
}

function startWorkflowTitleGeneration(
  workflow: WorkflowDefinition,
  workflowName: string,
  userMessage: string,
  cwd: string,
  workingCwd: string,
  conversation: ConversationRow
): void {
  void generateWorkflowTitle(workflow, workflowName, userMessage, cwd, workingCwd, conversation);
}

async function generateWorkflowTitle(
  workflow: WorkflowDefinition,
  workflowName: string,
  userMessage: string,
  cwd: string,
  workingCwd: string,
  conversation: ConversationRow
): Promise<void> {
  const workflowConfig = await loadTitleConfig(cwd);
  try {
    const titleAssistantType = resolveTitleAssistantType(
      workflow,
      workflowConfig?.assistant,
      conversation.ai_assistant_type
    );
    const titleAssistantConfig = workflowConfig?.assistants?.[titleAssistantType] ?? {};
    await generateAndSetTitle(
      conversation.id,
      userMessage,
      titleAssistantType,
      workingCwd,
      workflowName,
      titleAssistantConfig
    );
  } catch (error) {
    getLog().warn(
      { err: error as Error, conversationId: conversation.id },
      'workflow.title_generation_failed'
    );
  }
}

async function loadTitleConfig(
  cwd: string
): Promise<Awaited<ReturnType<typeof loadConfig>> | undefined> {
  try {
    return await loadConfig(cwd);
  } catch (error) {
    getLog().warn({ err: error as Error, cwd }, 'workflow.title_config_load_failed');
    return undefined;
  }
}

interface SignalRunHandlers {
  setOwnedRunId(runId: string): void;
  dispose(): void;
}

function registerWorkflowSignalHandlers(run: PreparedWorkflowRun): SignalRunHandlers {
  let ownedRunId: string | undefined = run.resumable?.id;
  let terminating = false;
  const cleanup = (signal: string): void => {
    if (terminating) return;
    terminating = true;
    getLog().info({ conversationId: run.conversation.id, signal }, 'workflow.process_terminating');
    cleanupOwnedWorkflowOnSignal(signal, ownedRunId, run).finally(() => process.exit(1));
  };
  const sigtermHandler = (): void => {
    cleanup('SIGTERM');
  };
  const sigintHandler = (): void => {
    cleanup('SIGINT');
  };
  process.once('SIGTERM', sigtermHandler);
  process.once('SIGINT', sigintHandler);
  return {
    setOwnedRunId(runId: string): void {
      if (ownedRunId === undefined) ownedRunId = runId;
    },
    dispose(): void {
      process.off('SIGTERM', sigtermHandler);
      process.off('SIGINT', sigintHandler);
    },
  };
}

async function cleanupOwnedWorkflowOnSignal(
  signal: string,
  ownedRunId: string | undefined,
  run: PreparedWorkflowRun
): Promise<void> {
  await failOwnedRunOnSignal(signal, ownedRunId, run.conversation.id).catch((err: unknown) => {
    const e = err as Error;
    getLog().error(
      { err: e, errorType: e.constructor.name },
      'workflow.termination_cleanup_failed'
    );
  });
  await destroySignalContainer(signal, run.containerBackend, run.containerEnvId).catch(
    () => undefined
  );
}

async function failOwnedRunOnSignal(
  signal: string,
  ownedRunId: string | undefined,
  conversationId: string
): Promise<void> {
  if (!ownedRunId) {
    getLog().info({ conversationId, signal }, 'workflow.termination_no_owned_run');
    return;
  }
  const status = await workflowDb.getWorkflowRunStatus(ownedRunId);
  if (status !== 'running') {
    getLog().info({ runId: ownedRunId, status, signal }, 'workflow.termination_skip_not_running');
    return;
  }
  await workflowDb.failWorkflowRun(ownedRunId, `Process terminated (${signal})`);
}

async function destroySignalContainer(
  signal: string,
  backend: ContainerBackend | undefined,
  envId: string | undefined
): Promise<void> {
  if (!backend || !envId) return;
  try {
    await backend.destroy(envId);
  } catch (destroyErr) {
    console.error(
      `\nWARNING: could not remove the isolation container on ${signal}: ` +
        `${(destroyErr as Error).message}. Remove it manually: ` +
        'docker ps -a --filter label=diy.archon.managed=true'
    );
  }
}

async function executePreparedWorkflowRun(
  run: PreparedWorkflowRun,
  userMessage: string,
  options: WorkflowRunOptions
): Promise<void> {
  const signalHandlers = registerWorkflowSignalHandlers(run);
  await maybePrintTierNotice(run.workflow, run.workingCwd, run.cliUserId, options.quiet);
  const unsubscribe = subscribeToWorkflowRunEvents(run.conversationId, signalHandlers, options);
  await surfaceWorkflowDispatch(run).catch(dispatchError => {
    getLog().warn(
      { err: dispatchError as Error, conversationId: run.conversationId },
      'cli.workflow_dispatch_surface_failed'
    );
  });

  const deps = createExecutionWorkflowDeps(run);
  const prepared = await hydrateRunIfNeeded(run, deps);
  const result = await executeWorkflowWithCleanup(
    run,
    deps,
    prepared,
    unsubscribe,
    signalHandlers,
    userMessage
  );
  await handleWorkflowRunResult(run, result);
}

function subscribeToWorkflowRunEvents(
  conversationId: string,
  signalHandlers: SignalRunHandlers,
  options: WorkflowRunOptions
): () => void {
  const { quiet, verbose } = options;
  return getWorkflowEventEmitter().subscribeForConversation(conversationId, event => {
    if (event.type === 'workflow_started') signalHandlers.setOwnedRunId(event.runId);
    if (!quiet) renderWorkflowEvent(event, verbose ?? false);
  });
}

async function surfaceWorkflowDispatch(run: PreparedWorkflowRun): Promise<void> {
  await run.adapter.sendMessage(
    run.conversationId,
    `Dispatching workflow: **${run.workflow.name}**`,
    {
      category: 'workflow_dispatch_status',
      segment: 'new',
      workflowDispatch: {
        workerConversationId: run.conversationId,
        workflowName: run.workflow.name,
      },
    }
  );
}

function createExecutionWorkflowDeps(run: PreparedWorkflowRun): WorkflowDeps {
  const controllerHandlerStore = run.hardenedControllerSession
    ? createWorkflowDeps().store
    : undefined;
  if (!run.hardenedControllerSession || !controllerHandlerStore) return createWorkflowDeps({});
  return createWorkflowDeps({
    controllerActions: createHardenedControllerActions({
      session: run.hardenedControllerSession,
      store: controllerHandlerStore,
      snapshotArtifacts: run.containerBackend?.snapshotArtifacts?.bind(run.containerBackend),
      playwrightNodeModules: () =>
        getHardenedControllerValidatorNodeModules(
          requireHardenedControllerSession(run.hardenedControllerSession)
        ),
    }),
    controllerActionGrants: run.hardenedControllerSession.controllerActionGrants,
    workflowBudgetGrants: run.hardenedControllerSession.workflowBudgetGrants,
  });
}

async function hydrateRunIfNeeded(
  run: PreparedWorkflowRun,
  deps: WorkflowDeps
): Promise<Awaited<ReturnType<typeof hydrateResumableRun>>> {
  if (!run.resumable) return null;
  try {
    const prepared = await hydrateResumableRun(deps, run.resumable);
    if (!prepared) {
      throw new Error(
        `Cannot resume: the prior run for '${run.workflowName}' has no completed nodes and no interactive-loop state.`
      );
    }
    return prepared;
  } catch (error) {
    const err = error as Error;
    getLog().error(
      { err, workflowName: run.workflowName, runId: run.resumable.id },
      'cli.workflow_hydrate_resume_failed'
    );
    if (err.message.startsWith('Cannot resume:')) throw err;
    throw new Error(
      `Cannot resume workflow '${run.workflowName}': failed to load prior run state — ${err.message}`
    );
  }
}

function buildContainerRunContext(run: PreparedWorkflowRun): ExecuteWorkflowOptions['container'] {
  if (!run.containerBackend || !run.containerEnvId) return undefined;
  const policy = run.hardenedControllerSession?.policyMetadata;
  return {
    envId: run.containerEnvId,
    writeBack: run.workflow.container?.write_back ?? ('approve' as const),
    backend: run.containerBackend,
    ...(policy?.proxyBudgetSeedDigest
      ? {
          proxyBudgetSeedDigest: policy.proxyBudgetSeedDigest,
          egressPolicyB64: policy.egressPolicyB64,
          image: policy.image,
          ownerRunId: policy.runId,
        }
      : {}),
  };
}

function buildChildIsolationResolver(
  run: PreparedWorkflowRun
): ExecuteWorkflowOptions['resolveChildIsolation'] {
  if (!run.codebase || run.codebase.kind === 'folder') return undefined;
  return createChildWorktreeResolver({
    codebaseId: run.codebase.id,
    codebaseName: run.codebase.name,
    canonicalRepoPath: run.codebase.default_cwd,
    baseBranch: run.codebaseDefaultBranch,
    createdByPlatform: 'cli',
    createdByUserId: run.cliUserId,
  });
}

async function executeWorkflowWithCleanup(
  run: PreparedWorkflowRun,
  deps: WorkflowDeps,
  prepared: Awaited<ReturnType<typeof hydrateResumableRun>>,
  unsubscribe: () => void,
  signalHandlers: SignalRunHandlers,
  userMessage: string
): Promise<WorkflowRunResult> {
  let result: WorkflowRunResult | undefined;
  let containerTeardownError: Error | undefined;
  try {
    result = await executeWorkflow(
      deps,
      run.adapter,
      run.conversationId,
      run.workingCwd,
      run.workflow,
      userMessage,
      run.conversation.id,
      buildExecuteWorkflowOptions(run, prepared)
    );
  } finally {
    unsubscribe();
    signalHandlers.dispose();
    containerTeardownError = await cleanupContainerAfterWorkflow(run, deps, result);
  }
  if (containerTeardownError && result?.success) throw containerTeardownError;
  if (!result) throw new Error('Workflow did not produce a result.');
  return result;
}

function buildExecuteWorkflowOptions(
  run: PreparedWorkflowRun,
  prepared: Awaited<ReturnType<typeof hydrateResumableRun>>
): ExecuteWorkflowOptions {
  const base = {
    codebaseId: run.codebase?.id,
    source: run.workflowSource,
    parseWarnings: run.workflowEntry?.parseWarnings,
    userId: run.cliUserId,
    baseBranch: run.codebaseDefaultBranch,
    baseOverride: run.flagBase,
    execContext: run.execContext,
    container: buildContainerRunContext(run),
    resolveChildIsolation: buildChildIsolationResolver(run),
  };
  return prepared
    ? { ...base, ...prepared }
    : {
        ...base,
        ...(run.preCreatedHardenedRun ? { preCreatedRun: run.preCreatedHardenedRun } : {}),
      };
}

async function cleanupContainerAfterWorkflow(
  run: PreparedWorkflowRun,
  deps: WorkflowDeps,
  result: WorkflowRunResult | undefined
): Promise<Error | undefined> {
  const runPaused = Boolean(result?.success && 'paused' in result && result.paused);
  const preserveContainer = await shouldPreserveContainerAfterRun(run, deps, result, runPaused);
  if (run.containerBackend && run.containerEnvId && preserveContainer)
    warnPreservedContainer(run.containerEnvId, result?.workflowRunId);
  if (!run.containerBackend || !run.containerEnvId || runPaused || preserveContainer)
    return undefined;
  return destroyCompletedContainer(run, deps, result?.workflowRunId);
}

async function shouldPreserveContainerAfterRun(
  run: PreparedWorkflowRun,
  deps: WorkflowDeps,
  result: WorkflowRunResult | undefined,
  runPaused: boolean
): Promise<boolean> {
  if (!run.containerBackend || !run.containerEnvId || runPaused || !result?.workflowRunId)
    return false;
  try {
    const finalRun = await deps.store.getWorkflowRun(result.workflowRunId);
    return shouldPreserveHardenedContainer(finalRun?.metadata, finalRun?.status ?? undefined);
  } catch (lookupErr) {
    getLog().error(
      { err: lookupErr as Error, envId: run.containerEnvId, runId: result.workflowRunId },
      'workflow.teardown_run_lookup_failed'
    );
    return true;
  }
}

function warnPreservedContainer(envId: string, runId: string | undefined): void {
  console.error(
    '\nWARNING: the hardened container state is not fully exported — the container + named volumes are ' +
      'PRESERVED so candidate artifacts are not lost. Retry with ' +
      `\`bun run cli workflow resume ${runId ?? '<run-id>'}\` (re-enters the ` +
      'controller write-back path), or reclaim manually via `docker ps -a --filter label=diy.archon.managed=true`.'
  );
  getLog().warn({ envId, runId }, 'workflow.container_preserved_hardened_state');
}

async function destroyCompletedContainer(
  run: PreparedWorkflowRun,
  deps: WorkflowDeps,
  runId: string | undefined
): Promise<Error | undefined> {
  try {
    await requireContainerBackend(run.containerBackend).destroy(
      requireContainerEnvId(run.containerEnvId)
    );
    await persistContainerDestroyedEvent(deps, runId);
    console.log('Container and named volumes removed.');
    return undefined;
  } catch (destroyErr) {
    console.error(
      `\nWARNING: failed to remove the isolation container/volume: ${(destroyErr as Error).message}\n` +
        'Remove it manually: docker ps -a --filter label=diy.archon.managed=true ' +
        '(then `docker rm -f <name>` and `docker volume rm <name>-upper`).'
    );
    getLog().error(
      { err: destroyErr as Error, envId: run.containerEnvId },
      'workflow.container_destroy_failed'
    );
    return destroyErr as Error;
  }
}

async function persistContainerDestroyedEvent(
  deps: WorkflowDeps,
  runId: string | undefined
): Promise<void> {
  if (!runId) return;
  getWorkflowEventEmitter().emit({ type: 'container_lifecycle', runId, phase: 'destroyed' });
  await deps.store
    .createWorkflowEvent({
      workflow_run_id: runId,
      event_type: 'container_destroyed',
      step_name: 'container',
      data: {},
    })
    .catch((eventErr: Error) => {
      getLog().warn({ err: eventErr, runId }, 'workflow.container_destroyed_event_persist_failed');
    });
}

async function handleWorkflowRunResult(
  run: PreparedWorkflowRun,
  result: WorkflowRunResult
): Promise<void> {
  if (result.success && 'paused' in result && result.paused) {
    console.log('\nWorkflow paused — waiting for approval.');
    return;
  }
  if (!result.success) throw new Error(`Workflow failed: ${result.error}`);
  await maybeSurfaceWorkflowResult(run, result);
  console.log('\nWorkflow completed successfully.');
}

async function maybeSurfaceWorkflowResult(
  run: PreparedWorkflowRun,
  result: WorkflowRunResult
): Promise<void> {
  if (!('summary' in result) || !result.summary) return;
  try {
    await run.adapter.sendMessage(run.conversationId, result.summary, {
      category: 'workflow_result',
      segment: 'new',
      workflowResult: { workflowName: run.workflow.name, runId: result.workflowRunId },
    });
  } catch (surfaceError) {
    getLog().warn(
      { err: surfaceError as Error, conversationId: run.conversationId },
      'cli.workflow_result_surface_failed'
    );
  }
}

/**
 * Format age of a run from started_at to now.
 */
function formatAge(startedAt: Date | string): string {
  // SQLite returns UTC strings without Z suffix — append it so Date parses as UTC
  const date =
    startedAt instanceof Date
      ? startedAt
      : new Date(startedAt.endsWith('Z') ? startedAt : startedAt + 'Z');
  if (Number.isNaN(date.getTime())) return 'unknown';
  const ms = Date.now() - date.getTime();
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ${mins % 60}m`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

/**
 * Format a duration in milliseconds as a compact string.
 */
function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const secs = Math.round(ms / 100) / 10;
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = Math.round(secs % 60);
  return `${mins}m${remSecs}s`;
}

export interface NodeSummary {
  nodeId: string;
  state: 'running' | 'completed' | 'failed' | 'skipped';
  startedAt?: string;
  durationMs?: number;
  outputPreview?: string;
  error?: string;
}

/**
 * Derive per-node summaries from a run's workflow events.
 * Processes node_started / node_completed / node_failed / node_skipped* events.
 */
export function buildNodeSummaries(events: WorkflowEventRow[]): NodeSummary[] {
  const startTimes = new Map<string, number>();
  const summaries = new Map<string, NodeSummary>();

  for (const event of events) {
    const nodeId = event.step_name;
    if (!nodeId) continue;

    switch (event.event_type) {
      case 'node_started': {
        startTimes.set(nodeId, new Date(event.created_at).getTime());
        // A retry is a new active attempt, so stale terminal details must not
        // leak into the compact current-state summary.
        summaries.set(nodeId, { nodeId, state: 'running', startedAt: event.created_at });
        break;
      }
      case 'node_completed': {
        const started = startTimes.get(nodeId);
        const endTime = new Date(event.created_at).getTime();
        const rawOutput = event.data.node_output;
        const output = typeof rawOutput === 'string' ? rawOutput : undefined;
        summaries.set(nodeId, {
          nodeId,
          state: 'completed',
          startedAt: summaries.get(nodeId)?.startedAt,
          durationMs: started !== undefined ? endTime - started : undefined,
          outputPreview:
            output !== undefined
              ? output.slice(0, 200) + (output.length > 200 ? '...' : '')
              : undefined,
        });
        break;
      }
      case 'node_failed': {
        const started = startTimes.get(nodeId);
        const endTime = new Date(event.created_at).getTime();
        summaries.set(nodeId, {
          nodeId,
          state: 'failed',
          startedAt: summaries.get(nodeId)?.startedAt,
          durationMs: started !== undefined ? endTime - started : undefined,
          error: typeof event.data.error === 'string' ? event.data.error : 'Unknown error',
        });
        break;
      }
      case 'node_skipped':
      case 'node_skipped_prior_success': {
        summaries.set(nodeId, { nodeId, state: 'skipped' });
        break;
      }
    }
  }

  return [...summaries.values()];
}

/**
 * Fetch a run's events for `--verbose` rendering. A failed event query must not
 * abort the command (the run summary itself is still useful), but it must NOT be
 * indistinguishable from "this run has no events" — so log a warn and flag the
 * failure to the caller, which prints a visible note. (In `--json` mode logs are
 * silenced; an empty derived/raw payload is the documented signal there.)
 */
async function fetchVerboseEvents(
  runId: string
): Promise<{ events: WorkflowEventRow[]; failed: boolean }> {
  try {
    return { events: await workflowEventsDb.listWorkflowEvents(runId), failed: false };
  } catch (error) {
    getLog().warn({ err: error as Error, runId }, 'cli.workflow_events_fetch_failed');
    return { events: [], failed: true };
  }
}

/**
 * Render per-node summaries for a run's events as an indented "Nodes:" block.
 * Shared by `workflow status --verbose` and `workflow get --verbose`.
 * Prints nothing when the run has no node events.
 */
function printVerboseNodes(events: WorkflowEventRow[]): void {
  const nodes = buildNodeSummaries(events);
  if (nodes.length === 0) return;
  console.log('  Nodes:');
  for (const node of nodes) {
    const iconMap: Record<string, string> = {
      completed: '✓',
      failed: '✗',
      skipped: '-',
      running: '◌',
    };
    const icon = iconMap[node.state] ?? '◌';
    const duration = node.durationMs !== undefined ? ` (${formatDuration(node.durationMs)})` : '';
    const stateLabel = node.state === 'running' ? ' (running)' : '';
    console.log(`    ${icon} ${node.nodeId}${duration}${stateLabel}`);
    if (node.outputPreview !== undefined) {
      console.log(`        Output: ${node.outputPreview}`);
    }
    if (node.error !== undefined) {
      console.log(`        Error:  ${node.error}`);
    }
  }
}

/**
 * Show status of all running workflow runs.
 */
export async function workflowStatusCommand(
  json?: boolean,
  verbose?: boolean,
  rawEvents?: boolean
): Promise<void> {
  let runs: WorkflowRun[];
  try {
    const result = await getWorkflowStatus();
    runs = result.runs;
  } catch (error) {
    const err = error as Error;
    getLog().error({ err }, 'cli.workflow_status_failed');
    throw new Error(`Failed to list workflow runs: ${err.message}`);
  }

  if (json) {
    if (!verbose) {
      await writeJsonLine({ runs });
      return;
    }

    const fetchedPerRun = await Promise.all(runs.map(run => fetchVerboseEvents(run.id)));
    const runsOutput = runs.map((run, i) => {
      const runEvents = fetchedPerRun[i]?.events ?? [];
      return rawEvents
        ? { ...run, events: runEvents }
        : { ...run, nodes: buildNodeSummaries(runEvents) };
    });
    await writeJsonLine({ runs: runsOutput });
    return;
  }

  if (runs.length === 0) {
    console.log('No active workflows.');
    return;
  }

  console.log(`\nActive workflows (${runs.length}):\n`);
  for (const run of runs) {
    const age = formatAge(run.started_at);
    console.log(`  ID:     ${run.id}`);
    console.log(`  Name:   ${run.workflow_name}`);
    console.log(`  Path:   ${run.working_path ?? '(none)'}`);
    console.log(`  Status: ${run.status}`);
    console.log(`  Age:    ${age}`);

    if (verbose) {
      const { events, failed } = await fetchVerboseEvents(run.id);
      if (failed) {
        console.log('  (node events unavailable — see logs)');
      }
      printVerboseNodes(events);
    }

    console.log('');
  }
}

/**
 * Show detail for a single workflow run by ID (any status).
 *
 * Unlike `status` (active runs only), this resolves one run regardless of
 * status — so an agent can answer "did the review pass?" for a completed/failed
 * run. `--verbose` adds the per-node summary; `--json` emits the raw run plus a
 * `nodes` array when verbose (`--events` selects raw event rows instead).
 *
 * `runId` may be the short id printed by `workflow runs` (see resolveRunIdArg).
 */
export async function workflowGetCommand(
  runId: string,
  json?: boolean,
  verbose?: boolean,
  cwd?: string,
  rawEvents?: boolean
): Promise<number> {
  const run = await loadWorkflowRunForGet(runId, cwd, json);
  if (run === 'failed') return 1;
  if (!run) return emitWorkflowGetNotFound(runId, json);

  const detail = verbose ? await loadWorkflowGetDetail(run.id) : undefined;
  if (json) return emitWorkflowGetJson(run, detail, Boolean(verbose), Boolean(rawEvents));
  emitWorkflowGetText(run, detail);
  return 0;
}

async function loadWorkflowRunForGet(
  runId: string,
  cwd: string | undefined,
  json: boolean | undefined
): Promise<WorkflowRun | null | 'failed'> {
  try {
    const resolvedId = await resolveRunIdArg(runId, cwd);
    return await workflowDb.getWorkflowRun(resolvedId);
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, runId }, 'cli.workflow_get_failed');
    if (json) {
      await writeJsonLine({ ok: false, runId, error: err.message });
      return 'failed';
    }
    throw new Error(`Failed to get workflow run: ${err.message}`);
  }
}

async function emitWorkflowGetNotFound(runId: string, json: boolean | undefined): Promise<number> {
  if (json) await writeJsonLine({ ok: false, runId, error: 'not_found' });
  else console.log(`Workflow run not found: ${runId}`);
  return 1;
}

interface WorkflowGetDetail {
  events: WorkflowEventRow[];
  eventsFailed: boolean;
}

async function loadWorkflowGetDetail(runId: string): Promise<WorkflowGetDetail> {
  const fetched = await fetchVerboseEvents(runId);
  return { events: fetched.events, eventsFailed: fetched.failed };
}

async function emitWorkflowGetJson(
  run: WorkflowRun,
  detail: WorkflowGetDetail | undefined,
  verbose: boolean,
  rawEvents: boolean
): Promise<number> {
  if (!verbose) {
    await writeJsonLine(run);
    return 0;
  }

  const verboseEvents = detail?.events ?? [];
  const parseWarnings = readParseWarningEvents(verboseEvents);
  const output = rawEvents
    ? { ...run, events: verboseEvents }
    : {
        ...run,
        nodes: buildNodeSummaries(verboseEvents),
        ...(parseWarnings.length > 0 ? { parseWarnings } : {}),
      };
  await writeJsonLine(output);
  return 0;
}

function emitWorkflowGetText(run: WorkflowRun, detail: WorkflowGetDetail | undefined): void {
  console.log(`  ID:     ${run.id}`);
  console.log(`  Name:   ${run.workflow_name}`);
  console.log(`  Path:   ${run.working_path ?? '(none)'}`);
  console.log(`  Status: ${run.status}`);
  console.log(`  Age:    ${formatAge(run.started_at)}`);
  printWorkflowGetApprovalGate(run);
  const runError = typeof run.metadata.error === 'string' ? run.metadata.error : undefined;
  if (runError) console.log(`  Error:  ${runError}`);
  if (detail) printWorkflowGetEvents(detail.events, detail.eventsFailed);
}

function printWorkflowGetApprovalGate(run: WorkflowRun): void {
  const gateMeta = run.metadata.approval;
  if (
    run.status !== 'paused' ||
    !isApprovalContext(gateMeta) ||
    gateMeta.type !== 'interactive_loop'
  ) {
    return;
  }
  const signal = gateMeta.completionSignaled === true ? 'yes' : 'no';
  console.log(
    `  Gate:   awaiting approval — signal detected: ${signal} (iteration ${String(gateMeta.iteration ?? '?')})`
  );
}

function printWorkflowGetEvents(events: WorkflowEventRow[], eventsFailed: boolean): void {
  if (eventsFailed) console.log('  (node events unavailable — see logs)');
  const parseWarnings = readParseWarningEvents(events);
  if (parseWarnings.length > 0) {
    console.log(`  Ignored keys (${String(parseWarnings.length)}):`);
    for (const w of parseWarnings) console.log(`    - ${w}`);
  }
  printVerboseNodes(events);
}

/**
 * Pull the dropped-key warnings out of a run's event log (#2213).
 *
 * The engine records them once at run start as `workflow_parse_warnings`,
 * whatever surface started the run — so this is the read path for a run that
 * had no conversation to post into (CLI, REST) or whose chat delivery failed.
 */
function readParseWarningEvents(events: readonly WorkflowEventRow[]): string[] {
  const out: string[] = [];
  for (const event of events) {
    if (event.event_type !== 'workflow_parse_warnings') continue;
    const raw: unknown = (event.data as Record<string, unknown> | null)?.warnings;
    if (Array.isArray(raw)) out.push(...raw.filter((w): w is string => typeof w === 'string'));
  }
  return out;
}

/**
 * List recent workflow runs for the current project (all statuses, cwd-scoped).
 *
 * Complements `status` (active-only): resolves the codebase from `cwd` the same
 * way `workflow run` does, then lists that project's recent runs of every
 * status. `--all` drops the project scope (lists across all projects);
 * `--status` filters to one status; `--limit` caps the count (default 20).
 */
export async function workflowRunsCommand(
  cwd: string,
  opts: { json?: boolean; all?: boolean; status?: string; limit?: number } = {}
): Promise<void> {
  let statusFilter: WorkflowRunStatus | undefined;
  if (opts.status) {
    const parsed = workflowRunStatusSchema.safeParse(opts.status);
    if (!parsed.success) {
      const msg = `Invalid --status '${opts.status}'. Valid: ${workflowRunStatusSchema.options.join(', ')}.`;
      // --json never throws — emit one parseable {ok:false} line (write-command contract).
      if (opts.json) {
        await writeJsonLine({ ok: false, error: msg });
        return;
      }
      throw new Error(msg);
    }
    statusFilter = parsed.data;
  }

  // Scope to this project by resolving the codebase from cwd (mirror
  // workflowRunCommand). --all opts out of scoping. A lookup failure or an
  // unregistered cwd both fall back to the global list — never a silent
  // wrong-scope (the human path prints an explicit note below).
  let codebase = null;
  if (!opts.all) {
    try {
      codebase = await codebaseDb.findCodebaseByDefaultCwd(cwd);
    } catch (error) {
      getLog().warn({ err: error as Error, cwd }, 'cli.workflow_runs_codebase_lookup_failed');
    }
  }
  // listDashboardRuns ignores undefined filters (truthy-guarded WHERE clauses),
  // so pass codebaseId/status straight through — no conditional spread needed.
  const codebaseId = opts.all ? undefined : codebase?.id;

  let result;
  try {
    result = await workflowDb.listDashboardRuns({
      codebaseId,
      status: statusFilter,
      limit: opts.limit ?? 20,
    });
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, cwd }, 'cli.workflow_runs_failed');
    if (opts.json) {
      await writeJsonLine({ ok: false, error: err.message });
      return;
    }
    throw new Error(`Failed to list workflow runs: ${err.message}`);
  }

  // True when project scoping was requested but fell back to the global list
  // (unregistered cwd or a lookup failure). The human path prints a note below;
  // surface the same signal in --json so an agent isn't handed a global result
  // it silently mistakes for a project-scoped one.
  const scopeFallback = !opts.all && !codebase;

  if (opts.json) {
    await writeJsonLine({ ...result, scopeFallback });
    return;
  }

  if (scopeFallback) {
    console.log('(not a registered project — showing all runs)');
  }

  if (result.runs.length === 0) {
    console.log('No workflow runs found.');
    return;
  }

  console.log(`\nRecent runs (${result.runs.length} of ${result.total}):\n`);
  for (const run of result.runs) {
    const step =
      run.current_step_name !== null
        ? ` · ${run.current_step_name}${run.total_steps !== null ? `/${String(run.total_steps)}` : ''}`
        : '';
    console.log(
      `  ${run.id.slice(0, 8)}  ${run.status.padEnd(9)}  ${run.workflow_name}${step}  (${formatAge(run.started_at)})`
    );
  }
  console.log('');
}

/**
 * Emit the standard `{ ok: false }` error line for a `--json` write command
 * (approve/reject/abandon/resume). Centralizes the envelope so all four stay in
 * lockstep; never throws — in --json mode the JSON line IS the error surface.
 */
function printJsonWriteError(runId: string, action: string, error: unknown): Promise<void> {
  return writeJsonLine({ ok: false, runId, action, error: (error as Error).message });
}

/**
 * Matches a full run id: a dashed UUID (Postgres `gen_random_uuid()`) or 32
 * undashed hex chars (SQLite `hex(randomblob(16))`). Anything shorter is
 * treated as a prefix.
 */
const FULL_RUN_ID_RE =
  /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{32})$/i;

/**
 * Resolve a run-id argument that may be the 8-char short id printed by
 * `workflow runs` into the full run id. Mirrors the chat `manage_run` tool's
 * prefix resolution (getScopedRun): the lookup is scoped to the cwd's
 * codebase, a unique match resolves, and an ambiguous prefix errors.
 *
 * Full UUIDs skip resolution entirely — exact lookup is global, so full ids
 * keep working from any directory. When `cwd` is omitted, the cwd is not a
 * registered project, or the prefix matches nothing in this project, the
 * argument passes through unchanged so the downstream exact lookup keeps its
 * existing error surface (intentional fallback: it preserves behavior for
 * runs of other projects and non-UUID ids rather than guessing).
 */
async function resolveRunIdArg(runId: string, cwd?: string): Promise<string> {
  if (cwd === undefined || FULL_RUN_ID_RE.test(runId)) return runId;
  const codebase = await codebaseDb.findCodebaseByDefaultCwd(cwd);
  if (!codebase) return runId;
  const matches = await workflowDb.findWorkflowRunsByIdPrefix(runId, codebase.id);
  if (matches.length > 1) {
    throw new Error(
      `Run id '${runId}' matches more than one run in this project — use more characters or the full id (from 'archon workflow runs --json').`
    );
  }
  return matches[0]?.id ?? runId;
}

async function resolveDiscoveryCwdForCodebase(
  runId: string,
  codebaseId: string,
  action: 'resume' | 'approve' | 'reject'
): Promise<string> {
  try {
    const codebase = await codebaseDb.getCodebase(codebaseId);
    if (!codebase) {
      throw new Error(
        `Workflow run '${runId}' references codebase '${codebaseId}', but that codebase no longer exists.\n` +
          'Cannot safely discover workflows from the run worktree because project workflow files may be missing.\n' +
          'Re-register the project or restore the codebase row, then retry.'
      );
    }
    return codebase.default_cwd;
  } catch (error) {
    const err = error as Error;
    if (err.message.includes('references codebase')) {
      throw err;
    }
    getLog().error(
      { err, errorType: err.constructor.name, runId, codebaseId },
      `cli.workflow_${action}_codebase_lookup_failed`
    );
    throw new Error(
      `Failed to load codebase '${codebaseId}' for workflow run '${runId}': ${err.message}\n` +
        'Cannot safely discover workflows from the run worktree because project workflow files may be missing.\n' +
        'Fix the codebase lookup problem, then retry.'
    );
  }
}

/**
 * Resume a failed workflow run by ID.
 *
 * Re-executes the workflow with --resume semantics: `workflowRunCommand` locates
 * the prior failed run via findResumableRun and hands it to the executor, which
 * skips already-completed nodes (the executor no longer auto-detects on its own).
 *
 * `runId` may be the short id printed by `workflow runs` (see resolveRunIdArg).
 */
export async function workflowResumeCommand(
  runId: string,
  json?: boolean,
  cwd?: string
): Promise<void> {
  // JSON mode is a non-blocking control-plane ack: validate the run is resumable
  // and report its state, but do NOT re-execute the workflow inline (execution
  // streams workflow output to stdout, which would corrupt the JSON contract).
  // To actually execute a resumable run, use the blocking `resume` (no --json,
  // run as a background task) or `run <name> --resume --detach`.
  if (json) {
    try {
      const resolvedId = await resolveRunIdArg(runId, cwd);
      const run = await resumeWorkflowOp(resolvedId);
      await writeJsonLine({
        ok: true,
        runId: resolvedId,
        action: 'resume',
        executed: false,
        status: run.status,
        workflowName: run.workflow_name,
        workingPath: run.working_path,
      });
    } catch (error) {
      await printJsonWriteError(runId, 'resume', error);
    }
    return;
  }

  const resolvedId = await resolveRunIdArg(runId, cwd);
  const run = await resumeWorkflowOp(resolvedId);
  if (!run.working_path) {
    throw new Error(
      `Workflow run '${resolvedId}' has no working path recorded.\n` +
        'Cannot determine where to resume. The run may be too old.'
    );
  }
  console.log(`Resuming workflow: ${run.workflow_name}`);
  console.log(`Path: ${run.working_path}`);
  console.log('');

  // Use the codebase's source path for workflow YAML discovery so the file is
  // found even when working_path is a worktree or workspace clone that does
  // not contain the user's local (often untracked) workflow YAML.
  const discoveryCwd = run.codebase_id
    ? await resolveDiscoveryCwdForCodebase(resolvedId, run.codebase_id, 'resume')
    : undefined;

  // Re-execute via workflowRunCommand with --resume: it locates the prior failed
  // run via findResumableRun and skips already-completed nodes (the executor
  // itself no longer auto-detects resumable runs).
  try {
    await workflowRunCommand(run.working_path, run.workflow_name, run.user_message ?? '', {
      resume: true,
      codebaseId: run.codebase_id ?? undefined,
      discoveryCwd,
    });
  } catch (error) {
    const err = error as Error;
    getLog().error(
      { err, runId: resolvedId, workflowName: run.workflow_name },
      'cli.workflow_resume_run_failed'
    );
    throw new Error(`Failed to resume workflow '${run.workflow_name}': ${err.message}`);
  }
}

/**
 * Abandon a workflow run by ID (marks it as cancelled).
 *
 * `--json` emits a structured result instead of human text. In JSON mode the
 * command never throws — lookup/state errors are reported as `{ ok: false }` so
 * a parsing agent always gets one clean JSON line.
 *
 * `runId` may be the short id printed by `workflow runs` (see resolveRunIdArg).
 */
export async function workflowAbandonCommand(
  runId: string,
  json?: boolean,
  cwd?: string
): Promise<void> {
  // The container reclaim (M2) now lives in the shared `abandonWorkflow` op, so EVERY
  // surface reclaims — the CLI just reports the cancellation. Keeps `--json` a clean
  // one-line contract (no reclaim text before the payload).
  if (json) {
    try {
      const resolvedId = await resolveRunIdArg(runId, cwd);
      const { run, cascadeFailures, blockedParentRunId } = await abandonWorkflow(resolvedId);
      await writeJsonLine({
        ok: true,
        runId: resolvedId,
        action: 'abandon',
        status: 'cancelled',
        workflowName: run.workflow_name,
        ...(cascadeFailures > 0 ? { cascadeFailures } : {}),
        ...(blockedParentRunId ? { blockedParentRunId } : {}),
      });
    } catch (error) {
      await printJsonWriteError(runId, 'abandon', error);
    }
    return;
  }

  const resolvedId = await resolveRunIdArg(runId, cwd);
  const { run, cascadeFailures, blockedParentRunId } = await abandonWorkflow(resolvedId);
  console.log(`Abandoned workflow run: ${resolvedId}`);
  console.log(`Workflow: ${run.workflow_name}`);
  if (cascadeFailures > 0) {
    console.log(
      `Warning: ${String(cascadeFailures)} sub-run(s) could not be cancelled and may still be running — check \`archon workflow status\`.`
    );
  }
  if (blockedParentRunId) {
    console.log(
      `Warning: parent run ${blockedParentRunId} was blocked on this sub-run and stays paused.`
    );
    console.log(
      `  Resume it to fail the node cleanly (archon workflow resume ${blockedParentRunId}) or abandon it too.`
    );
  }
}

/**
 * Approve a paused workflow run by ID.
 *
 * Human mode records the approval on the still-'paused' run (the resolution
 * lives in metadata.approval.resolved, #2075) and then auto-resumes the run
 * inline. `--json` mode records the approval and returns a structured ack
 * WITHOUT resuming — the run stays paused-and-staged, resumable by a
 * backgrounded `resume`/`run --resume` (inline resume would stream output and
 * break the JSON).
 *
 * `runId` may be the short id printed by `workflow runs` (see resolveRunIdArg).
 */
export async function workflowApproveCommand(
  runId: string,
  comment?: string,
  json?: boolean,
  cwd?: string
): Promise<void> {
  // JSON mode records the approval and returns a structured ack WITHOUT the
  // inline auto-resume (resuming executes the workflow and streams output to
  // stdout, which would corrupt the JSON contract). The run becomes resumable
  // — drive it to completion with a backgrounded `resume`/`run --resume`.
  if (json) {
    try {
      const resolvedId = await resolveRunIdArg(runId, cwd);
      const result = await approveWorkflow(resolvedId, comment);
      await writeJsonLine({
        ok: true,
        runId: resolvedId,
        action: 'approve',
        type: result.type,
        workflowName: result.workflowName,
        resumable: true,
      });
    } catch (error) {
      await printJsonWriteError(runId, 'approve', error);
    }
    return;
  }

  const resolvedId = await resolveRunIdArg(runId, cwd);
  const result = await approveWorkflow(resolvedId, comment);

  // CLI auto-resumes after approval (unlike chat, which defers to next user message)
  if (!result.workingPath) {
    throw new Error(
      `Workflow run '${resolvedId}' has no working path recorded.\n` +
        'Cannot determine where to resume.'
    );
  }
  console.log(`Approved workflow: ${result.workflowName}`);
  console.log(`Path: ${result.workingPath}`);
  console.log('');
  console.log('Resuming workflow...');

  // Look up the original platform conversation ID to keep all messages in one thread
  let platformConversationId: string | undefined;
  try {
    const originalConversation = await conversationDb.getConversationById(result.conversationId);
    platformConversationId = originalConversation?.platform_conversation_id ?? undefined;
    if (!originalConversation) {
      getLog().info(
        { runId: resolvedId, conversationId: result.conversationId },
        'cli.workflow_approve_conversation_not_found'
      );
    }
  } catch (error) {
    const err = error as Error;
    getLog().warn(
      { err, runId: resolvedId, conversationId: result.conversationId },
      'cli.workflow_approve_conversation_lookup_failed'
    );
  }

  try {
    // Use the codebase's source path for workflow YAML discovery so the file is
    // found even when working_path is a worktree or workspace clone that does
    // not contain the user's local (often untracked) workflow YAML.
    const discoveryCwd = result.codebaseId
      ? await resolveDiscoveryCwdForCodebase(resolvedId, result.codebaseId, 'approve')
      : undefined;

    await workflowRunCommand(result.workingPath, result.workflowName, result.userMessage ?? '', {
      resume: true,
      codebaseId: result.codebaseId ?? undefined,
      conversationId: platformConversationId,
      discoveryCwd,
    });
  } catch (error) {
    const err = error as Error;
    getLog().error(
      { err, runId: resolvedId, workflowName: result.workflowName },
      'cli.workflow_approve_resume_failed'
    );
    throw new Error(
      `Approved but failed to resume workflow '${result.workflowName}': ${err.message}\n` +
        `The approval was recorded. Run 'bun run cli workflow resume ${resolvedId}' to retry.`
    );
  }
}

/**
 * Reject a paused workflow run by ID.
 * Legacy non-guarded workflows with on_reject auto-resume with rejection feedback;
 * guarded normal gates cancel and require a fresh run.
 *
 * `runId` may be the short id printed by `workflow runs` (see resolveRunIdArg).
 */
export async function workflowRejectCommand(
  runId: string,
  reason?: string,
  json?: boolean,
  cwd?: string
): Promise<void> {
  // JSON mode records the rejection and returns a structured ack WITHOUT the
  // inline auto-resume (an on_reject rework executes the workflow and streams
  // to stdout, corrupting the JSON contract). When `cancelled` is false the run
  // is resumable for the rework pass — drive it with a backgrounded `resume`.
  if (json) {
    try {
      const resolvedId = await resolveRunIdArg(runId, cwd);
      const result = await rejectWorkflow(resolvedId, reason);
      await writeJsonLine({
        ok: true,
        runId: resolvedId,
        action: 'reject',
        cancelled: result.cancelled,
        maxAttemptsReached: result.maxAttemptsReached,
        workflowName: result.workflowName,
        resumable: !result.cancelled,
      });
    } catch (error) {
      await printJsonWriteError(runId, 'reject', error);
    }
    return;
  }

  const resolvedId = await resolveRunIdArg(runId, cwd);
  const result = await rejectWorkflow(resolvedId, reason);

  if (result.cancelled) {
    const suffix = result.maxAttemptsReached ? ' (max attempts reached)' : '';
    console.log(`Rejected and cancelled${suffix}: ${result.workflowName}`);
    return;
  }

  // Not cancelled = either an on_reject rework (DAG approval gate) or a container
  // write-back reject (discard isolated changes). Both auto-resume; the resume drives
  // the rework / the discard + completion.
  if (!result.workingPath) {
    throw new Error(
      `Workflow run '${resolvedId}' has no working path recorded.\n` +
        'Cannot determine where to resume.'
    );
  }
  console.log(`Rejected workflow: ${result.workflowName}`);
  console.log(
    result.writeBack
      ? 'Discarding container changes (live folder left untouched)...'
      : 'Resuming with on_reject prompt...'
  );

  // Look up the original platform conversation ID to keep all messages in one thread
  let platformConversationId: string | undefined;
  try {
    const originalConversation = await conversationDb.getConversationById(result.conversationId);
    platformConversationId = originalConversation?.platform_conversation_id ?? undefined;
    if (!originalConversation) {
      getLog().info(
        { runId: resolvedId, conversationId: result.conversationId },
        'cli.workflow_reject_conversation_not_found'
      );
    }
  } catch (error) {
    const err = error as Error;
    getLog().warn(
      { err, runId: resolvedId, conversationId: result.conversationId },
      'cli.workflow_reject_conversation_lookup_failed'
    );
  }

  try {
    // Use the codebase's source path for workflow YAML discovery so the file is
    // found even when working_path is a worktree or workspace clone that does
    // not contain the user's local (often untracked) workflow YAML.
    const discoveryCwd = result.codebaseId
      ? await resolveDiscoveryCwdForCodebase(resolvedId, result.codebaseId, 'reject')
      : undefined;

    await workflowRunCommand(result.workingPath, result.workflowName, result.userMessage ?? '', {
      resume: true,
      codebaseId: result.codebaseId ?? undefined,
      conversationId: platformConversationId,
      discoveryCwd,
    });
  } catch (error) {
    const err = error as Error;
    getLog().error(
      { err, runId: resolvedId, workflowName: result.workflowName },
      'cli.workflow_reject_resume_failed'
    );
    throw new Error(
      `Rejected but failed to resume workflow '${result.workflowName}': ${err.message}\n` +
        `The rejection was recorded. Run 'bun run cli workflow resume ${resolvedId}' to retry.`
    );
  }
}

/**
 * Reset persisted per-node provider sessions for a workflow.
 *
 * Filter rules:
 *   - workflow-name required (positional)
 *   - --scope <key>: restrict to one scope (e.g. a conversation UUID); when
 *     omitted, deletes across ALL scopes (use --yes to skip the confirmation)
 *   - --node <id>: restrict to one node within the scope
 *   - --json: machine-readable output
 */
export async function workflowResetSessionsCommand(
  workflowName: string,
  options: { scope?: string; node?: string; yes?: boolean; json?: boolean }
): Promise<void> {
  if (!options.scope && !options.yes) {
    throw new Error(
      `Refusing to delete every persisted session for workflow '${workflowName}' across all scopes without confirmation.\n` +
        'Pass --scope <key> to narrow, or --yes to confirm cross-scope reset.'
    );
  }
  try {
    const { deleted } = await resetWorkflowNodeSessions({
      workflow_name: workflowName,
      scope_key: options.scope,
      node_id: options.node,
    });
    if (options.json) {
      await writeStdout(
        `${JSON.stringify({
          workflow: workflowName,
          deleted,
          scope: options.scope ?? null,
          node: options.node ?? null,
        })}\n`
      );
    } else if (deleted === 0) {
      console.log(`No persisted sessions matched for workflow '${workflowName}'.`);
    } else {
      const scope = options.scope ? ` in scope '${options.scope}'` : ' across all scopes';
      const node = options.node ? ` for node '${options.node}'` : '';
      console.log(
        `Deleted ${deleted} persisted session(s) for workflow '${workflowName}'${node}${scope}.`
      );
    }
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, workflowName, ...options }, 'cli.workflow_reset_sessions_failed');
    throw new Error(`Failed to reset workflow sessions: ${err.message}`);
  }
}

/**
 * Delete terminal workflow runs older than the given number of days.
 */
export async function workflowCleanupCommand(days: number): Promise<void> {
  try {
    const { count } = await workflowDb.deleteOldWorkflowRuns(days);
    if (count === 0) {
      console.log(`No workflow runs older than ${days} days to clean up.`);
    } else {
      console.log(`Deleted ${count} workflow run(s) older than ${days} days.`);
    }
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, days }, 'cli.workflow_cleanup_failed');
    throw new Error(`Failed to clean up workflow runs: ${err.message}`);
  }
}

/**
 * Emit a workflow event directly to the database.
 * Non-throwing: mirrors the fire-and-forget contract of createWorkflowEvent.
 */
export function isValidEventType(value: string): value is WorkflowEventType {
  return (WORKFLOW_EVENT_TYPES as readonly string[]).includes(value);
}

export async function workflowEventEmitCommand(
  runId: string,
  eventType: WorkflowEventType,
  data?: Record<string, unknown>
): Promise<void> {
  const store = createWorkflowStore();
  await store.createWorkflowEvent({
    workflow_run_id: runId,
    event_type: eventType,
    data,
  });
  // createWorkflowEvent is non-throwing (fire-and-forget) — the event may not
  // have been persisted if the DB was unavailable. Check server logs if missing.
  console.log(`Event submitted (best-effort): ${eventType} for run ${runId}`);
}

// ─── Marketplace commands ────────────────────────────────────────────────────

interface MarketplaceEntryJson {
  slug: string;
  name: string;
  author: string;
  description: string;
  sourceUrl: string;
  sha: string;
  tags: string[];
  archonVersionCompat: string;
  featured?: boolean;
}

const DEFAULT_MARKETPLACE_URL = 'https://archon.diy/workflows.json';

async function fetchMarketplace(): Promise<MarketplaceEntryJson[]> {
  const url = process.env.ARCHON_MARKETPLACE_URL ?? DEFAULT_MARKETPLACE_URL;
  let res: Response;
  try {
    res = await fetch(url);
  } catch (error) {
    const err = error as Error;
    throw new Error(`Cannot reach marketplace at ${url}: ${err.message}`);
  }
  if (!res.ok) {
    throw new Error(`Marketplace fetch failed: HTTP ${String(res.status)} from ${url}`);
  }
  const raw: unknown = await res.json();
  if (!Array.isArray(raw)) {
    throw new Error('Unexpected marketplace response format (expected array)');
  }
  for (const item of raw) {
    if (
      typeof item !== 'object' ||
      item === null ||
      typeof (item as Record<string, unknown>).slug !== 'string' ||
      typeof (item as Record<string, unknown>).sourceUrl !== 'string' ||
      !Array.isArray((item as Record<string, unknown>).tags)
    ) {
      throw new Error('Marketplace response contains invalid entries');
    }
  }
  return raw as MarketplaceEntryJson[];
}

export async function workflowSearchCommand(query?: string, json?: boolean): Promise<void> {
  const entries = await fetchMarketplace();

  const results = query
    ? entries.filter(e => {
        const q = query.toLowerCase();
        return (
          e.name.toLowerCase().includes(q) ||
          e.author.toLowerCase().includes(q) ||
          e.description.toLowerCase().includes(q) ||
          e.tags.some(t => t.toLowerCase().includes(q))
        );
      })
    : entries;

  if (json) {
    await writeJsonLine(results);
    return;
  }

  if (results.length === 0) {
    console.log(query ? `No workflows matching "${query}".` : 'Marketplace is empty.');
    console.log('Browse at https://archon.diy/workflows/');
    return;
  }

  console.log(
    `\nWorkflow Marketplace${query ? ` — results for "${query}"` : ''} (${String(results.length)})\n`
  );
  for (const e of results) {
    const tags = e.tags.join(', ');
    const desc = e.description.length > 80 ? e.description.slice(0, 77) + '...' : e.description;
    console.log(`  ${e.slug}`);
    console.log(`    Name:   ${e.name}`);
    console.log(`    Author: @${e.author}`);
    console.log(`    Tags:   ${tags}`);
    console.log(`    ${desc}`);
    console.log('');
  }
  console.log('Install: archon workflow install <slug>');
}

/** Detect whether a sourceUrl points to a directory (tree URL) or a single file (blob URL). */
function isDirectoryUrl(sourceUrl: string): boolean {
  return sourceUrl.includes('/tree/');
}

/**
 * Validate that a path component from an external source is safe to use in a filesystem path.
 * Rejects names containing path separators, traversal sequences, or non-portable characters.
 */
function isSafePathComponent(name: string): boolean {
  return name !== '.' && name !== '..' && /^[a-zA-Z0-9._-]+$/.test(name);
}

/** Parse owner/repo and path from a GitHub blob or tree URL. */
function parseGitHubUrl(sourceUrl: string): { owner: string; repo: string; path: string } {
  // https://github.com/owner/repo/blob/ref/path or https://github.com/owner/repo/tree/ref/path
  const match = /^https:\/\/github\.com\/([^/]+)\/([^/]+)\/(blob|tree)\/[^/]+\/(.+)$/.exec(
    sourceUrl
  );
  if (!match) {
    throw new Error(`Cannot parse GitHub URL: ${sourceUrl}`);
  }
  return { owner: match[1], repo: match[2], path: match[4] };
}

interface GitHubContentItem {
  name: string;
  type: 'file' | 'dir';
  download_url: string | null;
  path: string;
}

/** Fetch directory listing from GitHub Contents API at a pinned SHA. */
async function fetchGitHubDirectory(
  owner: string,
  repo: string,
  path: string,
  sha: string
): Promise<GitHubContentItem[]> {
  const url = `https://api.github.com/repos/${owner}/${repo}/contents/${path}?ref=${sha}`;
  let res: Response;
  try {
    res = await fetch(url, { headers: { Accept: 'application/vnd.github.v3+json' } });
  } catch (error) {
    const err = error as Error;
    throw new Error(`Cannot reach GitHub API: ${err.message}`);
  }
  if (!res.ok) {
    throw new Error(`GitHub API error: HTTP ${String(res.status)} from ${url}`);
  }
  const data: unknown = await res.json();
  if (!Array.isArray(data)) {
    throw new Error(`Expected directory listing from ${url}, got a single file`);
  }
  return data as GitHubContentItem[];
}

/** Download a file from raw.githubusercontent.com at a pinned SHA. */
async function downloadRawFile(
  owner: string,
  repo: string,
  filePath: string,
  sha: string
): Promise<string> {
  const rawUrl = `https://raw.githubusercontent.com/${owner}/${repo}/${sha}/${filePath}`;
  let res: Response;
  try {
    res = await fetch(rawUrl);
  } catch (error) {
    const err = error as Error;
    throw new Error(`Cannot fetch ${rawUrl}: ${err.message}`);
  }
  if (!res.ok) {
    throw new Error(`Source fetch failed: HTTP ${String(res.status)} from ${rawUrl}`);
  }
  return res.text();
}

export async function workflowInstallCommand(
  slug: string,
  cwd: string,
  force?: boolean
): Promise<void> {
  const entries = await fetchMarketplace();
  const entry = entries.find(e => e.slug === slug);

  if (!entry) {
    console.error(`Error: Workflow '${slug}' not found in marketplace.`);
    console.error("Run 'archon workflow search' to browse available workflows.");
    throw new Error(`Workflow '${slug}' not found`);
  }

  if (!entry.sourceUrl.startsWith('https://github.com/')) {
    throw new Error(
      `Untrusted source URL for '${slug}': ${entry.sourceUrl}\nOnly github.com sources are permitted.`
    );
  }

  if (!/^[a-z0-9-]+$/.test(slug)) {
    throw new Error(`Invalid slug '${slug}': must be lowercase alphanumeric with hyphens only.`);
  }

  const { findRepoRoot } = await import('@archon/git');
  const repoRoot = await findRepoRoot(cwd);
  if (!repoRoot) {
    throw new Error('Not in a git repository. Run archon workflow install from within a git repo.');
  }

  const { existsSync, mkdirSync, writeFileSync } = await import('node:fs');
  const archonDir = join(repoRoot, '.archon');

  if (isDirectoryUrl(entry.sourceUrl)) {
    await installDirectory(entry, slug, archonDir, force, existsSync, mkdirSync, writeFileSync);
  } else {
    await installSingleFile(entry, slug, archonDir, force, existsSync, mkdirSync, writeFileSync);
  }

  console.log(`Run with: archon workflow run ${slug} "<message>"`);
}

async function installSingleFile(
  entry: MarketplaceEntryJson,
  slug: string,
  archonDir: string,
  force: boolean | undefined,
  existsSync: (p: string) => boolean,
  mkdirSync: (p: string, opts: { recursive: boolean }) => void,
  writeFileSync: (p: string, data: string) => void
): Promise<void> {
  const { owner, repo, path } = parseGitHubUrl(entry.sourceUrl);
  const content = await downloadRawFile(owner, repo, path, entry.sha);

  if (!content.trim()) {
    throw new Error(`Downloaded YAML is empty for '${slug}'`);
  }

  const workflowsDir = join(archonDir, 'workflows');
  const destPath = join(workflowsDir, `${slug}.yaml`);

  if (existsSync(destPath) && !force) {
    throw new Error(`Workflow '${slug}' already exists at ${destPath}.\nUse --force to overwrite.`);
  }

  mkdirSync(workflowsDir, { recursive: true });
  writeFileSync(destPath, content);
  console.log(`Installed '${entry.name}' to ${destPath}`);
}

async function installDirectory(
  entry: MarketplaceEntryJson,
  slug: string,
  archonDir: string,
  force: boolean | undefined,
  existsSync: (p: string) => boolean,
  mkdirSync: (p: string, opts: { recursive: boolean }) => void,
  writeFileSync: (p: string, data: string) => void
): Promise<void> {
  const { owner, repo, path } = parseGitHubUrl(entry.sourceUrl);
  const items = await fetchGitHubDirectory(owner, repo, path, entry.sha);

  // Identify the main workflow YAML (named <slug>.yaml or the only .yaml in root)
  const yamlFiles = items.filter(f => f.type === 'file' && f.name.endsWith('.yaml'));
  const mainYaml =
    yamlFiles.find(f => f.name === `${slug}.yaml`) ??
    (yamlFiles.length === 1 ? yamlFiles[0] : undefined);

  if (!mainYaml) {
    throw new Error(
      `Cannot identify main workflow YAML in directory. Expected '${slug}.yaml' or a single .yaml file.`
    );
  }

  const workflowsDir = join(archonDir, 'workflows');
  const destWorkflow = join(workflowsDir, `${slug}.yaml`);

  if (existsSync(destWorkflow) && !force) {
    throw new Error(
      `Workflow '${slug}' already exists at ${destWorkflow}.\nUse --force to overwrite.`
    );
  }

  // Install the main workflow YAML
  const mainContent = await downloadRawFile(owner, repo, mainYaml.path, entry.sha);
  mkdirSync(workflowsDir, { recursive: true });
  writeFileSync(destWorkflow, mainContent);
  console.log(`  Workflow: ${destWorkflow}`);

  // Install supporting files by convention
  const subdirs = items.filter(f => f.type === 'dir');
  let installedCount = 1;

  for (const subdir of subdirs) {
    if (!isSafePathComponent(subdir.name)) {
      console.log(`  Skipped (unsafe directory name): ${subdir.name}`);
      continue;
    }

    const subItems = await fetchGitHubDirectory(owner, repo, subdir.path, entry.sha);
    const files = subItems.filter(f => f.type === 'file');

    let targetDir: string;
    if (subdir.name === 'commands') {
      targetDir = join(archonDir, 'commands');
    } else if (subdir.name === 'scripts') {
      targetDir = join(archonDir, 'scripts');
    } else {
      // Other subdirs (e.g. skills) go under .archon/<dirname>
      targetDir = join(archonDir, subdir.name);
    }

    mkdirSync(targetDir, { recursive: true });

    for (const file of files) {
      if (!isSafePathComponent(file.name)) {
        console.log(`  Skipped (unsafe filename): ${file.name}`);
        continue;
      }
      const destFile = join(targetDir, file.name);
      if (existsSync(destFile) && !force) {
        console.log(`  Skipped (exists): ${destFile}`);
        continue;
      }
      const content = await downloadRawFile(owner, repo, file.path, entry.sha);
      writeFileSync(destFile, content);
      console.log(`  Installed: ${destFile}`);
      installedCount++;
    }
  }

  // Also install any other root-level non-YAML files (e.g. README)
  const otherRootFiles = items.filter(f => f.type === 'file' && !f.name.endsWith('.yaml'));
  for (const file of otherRootFiles) {
    if (!isSafePathComponent(file.name)) {
      console.log(`  Skipped (unsafe filename): ${file.name}`);
      continue;
    }
    const destFile = join(workflowsDir, file.name);
    if (existsSync(destFile) && !force) continue;
    const content = await downloadRawFile(owner, repo, file.path, entry.sha);
    writeFileSync(destFile, content);
    installedCount++;
  }

  console.log(`Installed '${entry.name}' (${String(installedCount)} files)`);
}
