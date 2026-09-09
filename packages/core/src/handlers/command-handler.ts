/**
 * Command handler for slash commands
 * Handles deterministic operations without AI
 */
import { writeFile, access } from 'fs/promises';
import { join, relative } from 'path';
import { type Conversation, type CommandResult, ConversationNotFoundError } from '../types';
import * as db from '../db/conversations';
import * as codebaseDb from '../db/codebases';
import * as sessionDb from '../db/sessions';
import { listWorktrees, execFileAsync, listChildRepos, toRepoPath } from '@archon/git';
import { getIsolationProvider } from '@archon/isolation';
import * as isolationEnvDb from '../db/isolation-environments';
import {
  cleanupMergedWorktrees,
  cleanupStaleWorktrees,
  getWorktreeStatusBreakdown,
} from '../services/cleanup-service';
import { getArchonWorkspacesPath } from '@archon/paths';
import { loadConfig } from '../config/config-loader';
import { discoverWorkflowsWithConfig } from '@archon/workflows/workflow-discovery';
import { resolveWorkflowName } from '@archon/workflows/router';
import type {
  WorkflowWithSource,
  WorkflowLoadError,
  WorkflowDefinition,
} from '@archon/workflows/schemas/workflow';
import * as workflowDb from '../db/workflows';
import {
  approveWorkflow,
  rejectWorkflow,
  getWorkflowStatus,
  resumeWorkflow,
  abandonWorkflow,
  resetWorkflowNodeSessions,
} from '../operations/workflow-operations';
import { safeDeactivateSession } from '../state/session-transitions';
import { createLogger } from '@archon/paths';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('command-handler');
  return cachedLog;
}

/**
 * Workflow timing information calculated from database values
 */
interface WorkflowTimingInfo {
  startedAt: Date;
  lastActivity: Date;
  durationMs: number;
  durationMin: number;
  durationSec: number;
  lastActivityMs: number;
  lastActivityMin: number;
  lastActivitySec: number;
  isValid: boolean;
}

/**
 * Calculate timing information for a workflow run
 * Handles invalid dates gracefully and prevents negative durations
 */
function calculateWorkflowTiming(workflow: {
  started_at: Date | string;
  last_activity_at: Date | string | null;
}): WorkflowTimingInfo {
  const startedAt = new Date(workflow.started_at);
  const lastActivity = workflow.last_activity_at ? new Date(workflow.last_activity_at) : startedAt;

  // Validate dates - check for Invalid Date
  const isValid = !isNaN(startedAt.getTime()) && !isNaN(lastActivity.getTime());

  // Use Math.max(0, ...) to prevent negative durations from clock skew or data corruption
  const durationMs = Math.max(0, Date.now() - startedAt.getTime());
  const lastActivityMs = Math.max(0, Date.now() - lastActivity.getTime());

  return {
    startedAt,
    lastActivity,
    durationMs,
    durationMin: Math.floor(durationMs / 60000),
    durationSec: Math.floor((durationMs % 60000) / 1000),
    lastActivityMs,
    lastActivityMin: Math.floor(lastActivityMs / 60000),
    lastActivitySec: Math.floor((lastActivityMs % 60000) / 1000),
    isValid,
  };
}

/**
 * Convert an absolute path to a relative path from the repository root
 * Falls back to showing relative to workspace if not in a git repo
 */
function shortenPath(absolutePath: string, repoRoot?: string): string {
  // If we have a repo root, show path relative to it
  if (repoRoot) {
    const relPath = relative(repoRoot, absolutePath);
    // Only use relative path if it doesn't start with '..' (i.e., it's within the repo)
    if (!relPath.startsWith('..')) {
      return relPath;
    }
  }

  // Fallback: show relative to workspace
  const workspacePath = getArchonWorkspacesPath();
  const relPath = relative(workspacePath, absolutePath);
  if (!relPath.startsWith('..')) {
    return relPath;
  }

  // If all else fails, return the original path
  return absolutePath;
}

/**
 * Get the current git branch name for a repository.
 * Returns 'unknown' if git command fails, with error logged for debugging.
 *
 * @returns Branch name, 'detached HEAD', or 'unknown'. Never throws.
 */
async function getCurrentBranch(repoPath: string): Promise<string> {
  try {
    const { stdout } = await execFileAsync(
      'git',
      ['-C', repoPath, 'rev-parse', '--abbrev-ref', 'HEAD'],
      { timeout: 3000 }
    );
    const branch = stdout.trim();
    // Handle detached HEAD state - git returns literal "HEAD"
    return branch === 'HEAD' ? 'detached HEAD' : branch;
  } catch (error) {
    getLog().debug({ err: error, repoPath }, 'get_branch_failed');
    return 'unknown';
  }
}

/**
 * Format a folder project's contained git repos for status display.
 * Truncates the visible list at 10 and appends a "(+N more)" count.
 */
function formatChildRepos(childRepos: string[]): string {
  const MAX_SHOWN = 10;
  const shown = childRepos.slice(0, MAX_SHOWN);
  const remaining = childRepos.length - shown.length;
  const suffix = remaining > 0 ? `, … (+${String(remaining)} more)` : '';
  return `Contains ${String(childRepos.length)} git repo${childRepos.length === 1 ? '' : 's'}: ${shown.join(', ')}${suffix}`;
}

/**
 * Format repository context for user-facing display.
 * Shows "owner/repo @ branch" instead of filesystem paths.
 *
 * @returns Formatted context string. Never throws - falls back gracefully on errors.
 */
async function formatRepoContext(
  codebase: { name: string; default_cwd: string; kind?: 'repo' | 'folder' } | null,
  isolationEnvId: string | null
): Promise<string> {
  if (!codebase) {
    return 'No codebase configured';
  }

  // Folder projects have no git — show an honest "no git" label instead of a
  // branch (a folder root may not be a repo at all).
  if (codebase.kind === 'folder') {
    return `${codebase.name} (folder — no git)`;
  }

  // If in a worktree, use the worktree's branch name from database
  if (isolationEnvId) {
    try {
      const env = await isolationEnvDb.getById(isolationEnvId);
      if (env?.branch_name) {
        return `${codebase.name} @ ${env.branch_name} (worktree)`;
      }
      // Log data integrity issue - isolation_env_id exists but record missing or incomplete
      getLog().warn(
        { isolationEnvId, found: !!env, hasBranchName: !!env?.branch_name },
        'isolation_env_incomplete'
      );
      // Fallthrough to git branch detection
    } catch (error) {
      getLog().error({ err: error, isolationEnvId }, 'isolation_env_lookup_failed');
      // Fallthrough to git branch detection on DB error
    }
  }

  // Not in worktree or worktree lookup failed - get branch from git
  const branchName = await getCurrentBranch(codebase.default_cwd);
  return `${codebase.name} @ ${branchName}`;
}

export function parseCommand(text: string): { command: string; args: string[] } {
  const matches: string[] = [];
  let current = '';
  let quote: '"' | "'" | null = null;
  let escaping = false;
  let hasToken = false;

  for (const char of text.trim()) {
    if (quote) {
      hasToken = true;
      if (escaping) {
        current += char;
        escaping = false;
      } else if (char === '\\') {
        escaping = true;
      } else if (char === quote) {
        quote = null;
      } else {
        current += char;
      }
      continue;
    }

    if (/\s/.test(char)) {
      if (hasToken) {
        matches.push(current);
        current = '';
        hasToken = false;
      }
      continue;
    }

    if (char === '"' || char === "'") {
      quote = char;
      hasToken = true;
      continue;
    }

    current += char;
    hasToken = true;
  }

  if (escaping) {
    current += '\\';
  }
  if (hasToken) {
    matches.push(current);
  }

  if (matches.length === 0 || !matches[0]) {
    return { command: '', args: [] };
  }

  if (!matches[0].startsWith('/')) {
    return { command: '', args: [] };
  }

  const command = matches[0].substring(1); // Remove leading '/'
  const args = matches.slice(1);

  return { command, args };
}

function findWorkflowLoadError(
  loadErrors: readonly WorkflowLoadError[],
  workflowName: string
): WorkflowLoadError | undefined {
  // Stripping the .yaml/.yml extension already covers the exact-filename cases.
  return loadErrors.find(error => error.filename.replace(/\.ya?ml$/, '') === workflowName);
}

type CommandCodebase = NonNullable<Awaited<ReturnType<typeof codebaseDb.getCodebase>>>;

interface WorktreeCommandContext {
  conversation: Conversation;
  codebaseId: string;
  codebase: CommandCodebase;
  mainPath: string;
  args: string[];
}

async function handleWorktreeCommand(
  conversation: Conversation,
  args: string[]
): Promise<CommandResult> {
  const context = await resolveWorktreeCommandContext(conversation, args);
  if ('success' in context) return context;

  switch (args[0]) {
    case 'create':
      return handleWorktreeCreate(context);
    case 'list':
      return handleWorktreeList(context);
    case 'remove':
      return handleWorktreeRemove(context);
    case 'orphans':
      return handleWorktreeOrphans(context);
    case 'cleanup':
      return handleWorktreeCleanup(context);
    default:
      return worktreeUsageResult();
  }
}

async function resolveWorktreeCommandContext(
  conversation: Conversation,
  args: string[]
): Promise<WorktreeCommandContext | CommandResult> {
  if (!conversation.codebase_id) {
    return {
      success: false,
      message: 'No codebase configured. Register a project first with /register-project.',
    };
  }

  const codebase = await codebaseDb.getCodebase(conversation.codebase_id);
  if (!codebase) return { success: false, message: 'Codebase not found.' };

  // Worktrees are a git-repo concept — folder projects run in place and have no
  // worktree lifecycle. Reject clearly rather than failing deep in git.
  if (codebase.kind === 'folder') {
    return {
      success: false,
      message: `/worktree is not applicable to folder projects. "${codebase.name}" runs in place (no git worktree).`,
    };
  }

  return {
    conversation,
    codebaseId: conversation.codebase_id,
    codebase,
    mainPath: codebase.default_cwd,
    args,
  };
}

async function handleWorktreeCreate(context: WorktreeCommandContext): Promise<CommandResult> {
  const { conversation, codebaseId, mainPath, args } = context;
  const branchName = args[1];
  if (!branchName) return { success: false, message: 'Usage: /worktree create <branch-name>' };

  const validation = await validateWorktreeCreateRequest(conversation, mainPath, branchName);
  if (validation) return validation;

  try {
    return await createManualWorktree(conversation, codebaseId, mainPath, branchName);
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, branch: branchName }, 'cmd.worktree_create_failed');
    return formatWorktreeCreateError(error, err, branchName);
  }
}

async function validateWorktreeCreateRequest(
  conversation: Conversation,
  mainPath: string,
  branchName: string
): Promise<CommandResult | null> {
  if (conversation.isolation_env_id) {
    const existingEnv = await isolationEnvDb.getById(conversation.isolation_env_id);
    const worktreeLabel = existingEnv
      ? shortenPath(existingEnv.working_path, mainPath)
      : conversation.isolation_env_id;
    return {
      success: false,
      message: `Already using worktree: ${worktreeLabel}\n\nRun /worktree remove first.`,
    };
  }

  if (/^[a-zA-Z0-9_-]+$/.test(branchName)) return null;

  return {
    success: false,
    message: 'Branch name must contain only letters, numbers, dashes, and underscores.',
  };
}

async function createManualWorktree(
  conversation: Conversation,
  codebaseId: string,
  mainPath: string,
  branchName: string
): Promise<CommandResult> {
  const provider = getIsolationProvider();
  const env = await provider.create({
    codebaseId,
    canonicalRepoPath: toRepoPath(mainPath),
    workflowType: 'task',
    identifier: branchName,
    description: `Manual worktree: ${branchName}`,
  });

  await execFileAsync('git', ['config', '--global', '--add', 'safe.directory', env.workingPath]);

  const dbEnv = await isolationEnvDb.create({
    codebase_id: codebaseId,
    workflow_type: 'task',
    workflow_id: `task-${branchName}`,
    provider: 'worktree',
    working_path: env.workingPath,
    branch_name: env.branchName ?? branchName,
    created_by_platform: conversation.platform_type,
  });

  await db.updateConversation(conversation.id, {
    isolation_env_id: dbEnv.id,
    cwd: env.workingPath,
  });

  const shortPath = shortenPath(env.workingPath, mainPath);
  return {
    success: true,
    message: `Worktree created!\n\nBranch: ${env.branchName ?? branchName}\nPath: ${shortPath}\n\nThis conversation now works in isolation.\nRun dependency install if needed (e.g., bun install).`,
    modified: true,
  };
}

function formatWorktreeCreateError(error: unknown, err: Error, branchName: string): CommandResult {
  if (error instanceof ConversationNotFoundError) {
    return {
      success: false,
      message: 'Failed to create worktree: conversation state changed. Please try again.',
    };
  }
  if (err.message.includes('already exists')) {
    return {
      success: false,
      message: `Branch '${branchName}' already exists. Use a different name.`,
    };
  }
  return { success: false, message: `Failed to create worktree: ${err.message}` };
}

async function handleWorktreeList(context: WorktreeCommandContext): Promise<CommandResult> {
  const { conversation, mainPath } = context;
  try {
    const { stdout } = await execFileAsync('git', ['-C', mainPath, 'worktree', 'list']);
    const currentWorktreePath = await getCurrentWorktreePath(conversation);
    return { success: true, message: formatGitWorktreeList(stdout, mainPath, currentWorktreePath) };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, mainPath }, 'cmd.worktree_list_failed');
    return { success: false, message: `Failed to list worktrees: ${err.message}` };
  }
}

async function getCurrentWorktreePath(conversation: Conversation): Promise<string | null> {
  if (!conversation.isolation_env_id) return null;
  const currentEnv = await isolationEnvDb.getById(conversation.isolation_env_id);
  return currentEnv?.working_path ?? null;
}

function formatGitWorktreeList(
  stdout: string,
  mainPath: string,
  currentWorktreePath: string | null
): string {
  let msg = 'Worktrees:\n\n';
  for (const line of stdout.trim().split('\n')) {
    const parts = line.split(/\s+/);
    const fullPath = parts[0];
    const shortPath = shortenPath(fullPath, mainPath);
    const restOfLine = parts.slice(1).join(' ');
    const shortenedLine = restOfLine ? `${shortPath} ${restOfLine}` : shortPath;
    const marker = currentWorktreePath && fullPath === currentWorktreePath ? ' <- active' : '';
    msg += `${shortenedLine}${marker}\n`;
  }
  return msg;
}

async function handleWorktreeRemove(context: WorktreeCommandContext): Promise<CommandResult> {
  const { conversation, mainPath, args } = context;
  const isolationEnvId = conversation.isolation_env_id;
  if (!isolationEnvId)
    return { success: false, message: 'This conversation is not using a worktree.' };

  const isolationEnv = await isolationEnvDb.getById(isolationEnvId);
  if (!isolationEnv)
    return { success: false, message: 'Isolation environment not found in database.' };

  try {
    return await removeManualWorktree(
      conversation,
      isolationEnvId,
      isolationEnv.working_path,
      mainPath,
      args[1] === '--force'
    );
  } catch (error) {
    const err = error as Error;
    getLog().error(
      { err, isolationEnvId, workingPath: isolationEnv.working_path },
      'cmd.worktree_remove_failed'
    );
    return formatWorktreeRemoveError(error, err);
  }
}

async function removeManualWorktree(
  conversation: Conversation,
  isolationEnvId: string,
  workingPath: string,
  mainPath: string,
  force: boolean
): Promise<CommandResult> {
  const provider = getIsolationProvider();
  await provider.destroy(workingPath, { force });
  await isolationEnvDb.updateStatus(isolationEnvId, 'destroyed');
  await db.updateConversation(conversation.id, { isolation_env_id: null, cwd: mainPath });

  const session = await sessionDb.getActiveSession(conversation.id);
  if (session) await safeDeactivateSession(session.id, 'worktree-remove');

  return {
    success: true,
    message: `Worktree removed: ${shortenPath(workingPath, mainPath)}\n\nSwitched back to main repo.`,
    modified: true,
  };
}

function formatWorktreeRemoveError(error: unknown, err: Error): CommandResult {
  if (error instanceof ConversationNotFoundError) {
    return {
      success: false,
      message: 'Failed to remove worktree: conversation state changed. Please try again.',
    };
  }
  if (err.message.includes('untracked files') || err.message.includes('modified')) {
    return {
      success: false,
      message:
        'Worktree has uncommitted changes.\n\nCommit your work first, or use `/worktree remove --force` to discard.',
    };
  }
  return { success: false, message: `Failed to remove worktree: ${err.message}` };
}

async function handleWorktreeOrphans(context: WorktreeCommandContext): Promise<CommandResult> {
  const { conversation, mainPath } = context;
  try {
    const gitWorktrees = await listWorktrees(toRepoPath(mainPath));
    if (gitWorktrees.length <= 1) {
      return {
        success: true,
        message:
          'No worktrees found (only main repo).\n\nUse `/worktree create <branch>` to create one.',
      };
    }
    const currentWorktreePath = await getCurrentWorktreePath(conversation);
    return {
      success: true,
      message: formatOrphanWorktrees(gitWorktrees, mainPath, currentWorktreePath),
    };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, mainPath }, 'cmd.worktree_orphans_failed');
    return { success: false, message: `Failed to list worktrees: ${err.message}` };
  }
}

function formatOrphanWorktrees(
  gitWorktrees: Awaited<ReturnType<typeof listWorktrees>>,
  mainPath: string,
  currentWorktreePath: string | null
): string {
  let msg = 'All worktrees (from git):\n\n';
  for (const wt of gitWorktrees) {
    if (wt.path === mainPath) continue;
    const marker = currentWorktreePath && wt.path === currentWorktreePath ? ' ← current' : '';
    msg += `  ${wt.branch} → ${shortenPath(wt.path, mainPath)}${marker}\n`;
  }
  msg += '\nNote: This shows ALL worktrees including those created by external tools.\n';
  msg += 'Git (`git worktree list`) is the source of truth.';
  return msg;
}

async function handleWorktreeCleanup(context: WorktreeCommandContext): Promise<CommandResult> {
  const { conversation, codebaseId, mainPath, args } = context;
  const cleanupType = args[1];
  if (!cleanupType || !['merged', 'stale'].includes(cleanupType)) {
    return {
      success: false,
      message:
        'Usage:\n  /worktree cleanup merged - Remove worktrees with merged branches\n  /worktree cleanup stale - Remove inactive worktrees (14+ days)',
    };
  }

  try {
    const result =
      cleanupType === 'merged'
        ? await cleanupMergedWorktrees(codebaseId, mainPath)
        : await cleanupStaleWorktrees(codebaseId, mainPath);
    const count = await isolationEnvDb.countActiveByCodebase(codebaseId);
    return { success: true, message: formatWorktreeCleanupResult(result, cleanupType, count) };
  } catch (error) {
    const err = error as Error;
    getLog().error(
      { err, cleanupType, codebaseId: conversation.codebase_id },
      'cmd.worktree_cleanup_failed'
    );
    return { success: false, message: `Failed to cleanup: ${err.message}` };
  }
}

function formatWorktreeCleanupResult(
  result: Awaited<ReturnType<typeof cleanupMergedWorktrees>>,
  cleanupType: string,
  count: number
): string {
  let msg = '';
  if (result.removed.length > 0) {
    msg += `Cleaned up ${String(result.removed.length)} ${cleanupType} worktree(s):\n`;
    for (const branch of result.removed) msg += `  • ${branch}\n`;
  } else {
    msg += `No ${cleanupType} worktrees to clean up.\n`;
  }

  if (result.skipped.length > 0) {
    msg += `\nSkipped ${String(result.skipped.length)} (protected):\n`;
    for (const { branchName, reason } of result.skipped) msg += `  • ${branchName} (${reason})\n`;
  }

  msg += `\nActive worktrees: ${String(count)}`;
  return msg.trim();
}

function worktreeUsageResult(): CommandResult {
  return {
    success: false,
    message:
      'Usage:\n  /worktree create <branch>\n  /worktree list\n  /worktree remove [--force]\n  /worktree cleanup merged|stale\n  /worktree orphans',
  };
}

interface WorkflowCommandContext {
  conversation: Conversation;
  args: string[];
  workflowCwd: string;
}

async function handleWorkflowCommand(
  conversation: Conversation,
  args: string[]
): Promise<CommandResult> {
  const codebase = conversation.codebase_id
    ? await codebaseDb.getCodebase(conversation.codebase_id)
    : null;
  const workflowCwd = codebase
    ? (conversation.cwd ?? codebase.default_cwd)
    : getArchonWorkspacesPath();
  const context = { conversation, args, workflowCwd };

  switch (args[0]) {
    case 'list':
    case 'ls':
      return handleWorkflowList(context);
    case 'reload':
      return handleWorkflowReload(context);
    case 'cancel':
      return handleWorkflowCancel(context);
    case 'status':
      return handleWorkflowStatus(context);
    case 'resume':
      return handleWorkflowResume(context);
    case 'abandon':
      return handleWorkflowAbandon(context);
    case 'reset-sessions':
      return handleWorkflowResetSessions(context);
    case 'approve':
      return handleWorkflowApprove(context);
    case 'reject':
      return handleWorkflowReject(context);
    case 'run':
      return handleWorkflowRun(context);
    default:
      return workflowUsageResult();
  }
}

async function discoverWorkflowEntries(
  workflowCwd: string,
  logEvent: string,
  extraLogContext: Record<string, unknown> = {}
): Promise<
  | { workflowEntries: readonly WorkflowWithSource[]; loadErrors: readonly WorkflowLoadError[] }
  | CommandResult
> {
  try {
    const result = await discoverWorkflowsWithConfig(workflowCwd, loadConfig);
    return { workflowEntries: result.workflows, loadErrors: result.errors };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, cwd: workflowCwd, ...extraLogContext }, logEvent);
    return {
      success: false,
      message: `Failed to load workflows: ${err.message}\n\nCheck .archon/workflows/ for YAML syntax issues.`,
    };
  }
}

async function handleWorkflowList(context: WorkflowCommandContext): Promise<CommandResult> {
  const discovered = await discoverWorkflowEntries(context.workflowCwd, 'cmd.workflow_list_failed');
  if ('success' in discovered) return discovered;
  const { workflowEntries, loadErrors } = discovered;

  if (workflowEntries.length === 0 && loadErrors.length === 0) {
    return {
      success: true,
      message: 'No workflows found.\n\nCreate workflows in `.archon/workflows/` as YAML files.',
    };
  }

  return { success: true, message: formatWorkflowList(workflowEntries, loadErrors) };
}

function formatWorkflowList(
  workflowEntries: readonly WorkflowWithSource[],
  errors: readonly WorkflowLoadError[]
): string {
  let msg = workflowEntries.length > 0 ? formatAvailableWorkflows(workflowEntries) : '';
  if (errors.length > 0) msg += formatWorkflowLoadErrors(errors);
  return msg;
}

function formatAvailableWorkflows(workflowEntries: readonly WorkflowWithSource[]): string {
  let msg = 'Available Workflows:\n\n';
  for (const { workflow: w, parseWarnings } of workflowEntries) {
    msg += `**\`${w.name}\`**\n  ${w.description}\n  DAG: ${String(w.nodes.length)} nodes\n`;
    for (const warning of parseWarnings ?? []) msg += `  ⚠️ ${warning}\n`;
    msg += '\n';
  }
  return msg;
}

function formatWorkflowLoadErrors(errors: readonly WorkflowLoadError[]): string {
  let msg = `\n---\n\n**${String(errors.length)} workflow(s) failed to load:**\n\n`;
  for (const e of errors.slice(0, 10)) msg += `- \`${e.filename}\`: ${e.error}\n`;
  if (errors.length > 10) msg += `\n...and ${String(errors.length - 10)} more\n`;
  return msg;
}

async function handleWorkflowReload(context: WorkflowCommandContext): Promise<CommandResult> {
  try {
    const { workflows: reloadedWorkflows, errors: reloadErrors } =
      await discoverWorkflowsWithConfig(context.workflowCwd, loadConfig);
    let msg = `Discovered ${String(reloadedWorkflows.length)} workflow(s).`;
    if (reloadErrors.length > 0) {
      msg += `\n\n**${String(reloadErrors.length)} failed to load:**\n`;
      for (const e of reloadErrors) msg += `- \`${e.filename}\`: ${e.error}\n`;
    }
    return { success: true, message: msg };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, cwd: context.workflowCwd }, 'cmd.workflow_reload_failed');
    return {
      success: false,
      message: `Failed to reload workflows: ${err.message}\n\nCheck .archon/workflows/ for YAML syntax issues.`,
    };
  }
}

async function handleWorkflowCancel(context: WorkflowCommandContext): Promise<CommandResult> {
  try {
    const activeWorkflow = await workflowDb.getActiveWorkflowRun(context.conversation.id);
    if (!activeWorkflow) return { success: true, message: 'No active workflow to cancel.' };
    await workflowDb.cancelWorkflowRun(activeWorkflow.id);
    return { success: true, message: `Cancelled workflow: \`${activeWorkflow.workflow_name}\`` };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, conversationId: context.conversation.id }, 'cmd.workflow_cancel_failed');
    return { success: false, message: 'Failed to cancel workflow. Please try again.' };
  }
}

async function handleWorkflowStatus(context: WorkflowCommandContext): Promise<CommandResult> {
  try {
    const { runs: activeRuns } = await getWorkflowStatus();
    if (activeRuns.length === 0) return { success: true, message: 'No active workflows.' };

    let msg = `**Active Workflows (${String(activeRuns.length)})**\n\n`;
    for (const run of activeRuns) {
      msg += `**\`${run.workflow_name}\`** (${run.status})\n`;
      msg += `  ID: ${run.id}\n`;
      msg += `  Path: ${run.working_path ?? '(unknown)'}\n`;
      msg += `  Started: ${new Date(run.started_at).toISOString()}\n\n`;
    }
    return { success: true, message: appendWorkflowActionHints(msg, activeRuns).trim() };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, conversationId: context.conversation.id }, 'cmd.workflow_status_failed');
    return { success: false, message: 'Failed to retrieve workflow status. Please try again.' };
  }
}

function appendWorkflowActionHints(
  msg: string,
  activeRuns: Awaited<ReturnType<typeof getWorkflowStatus>>['runs']
): string {
  const hasRunning = activeRuns.some(r => r.status === 'running');
  const hasPaused = activeRuns.some(r => r.status === 'paused');
  if (hasRunning) msg += 'Use `/workflow cancel` to stop a running workflow.';
  if (hasPaused) {
    msg += '\nUse `/workflow approve <id>` or `/workflow reject <id> <reason>` for paused runs.';
  }
  return msg;
}

async function handleWorkflowResume(context: WorkflowCommandContext): Promise<CommandResult> {
  const runId = context.args[1];
  if (!runId) {
    return {
      success: false,
      message: 'Usage: /workflow resume <id>\n\nResumes a failed workflow from completed nodes.',
    };
  }
  try {
    const run = await resumeWorkflow(runId);
    return await buildResumeWorkflowResult(run, runId, context.workflowCwd);
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, runId }, 'cmd.workflow_resume_failed');
    return { success: false, message: `Failed to resume workflow run: ${err.message}` };
  }
}

async function buildResumeWorkflowResult(
  run: Awaited<ReturnType<typeof resumeWorkflow>>,
  runId: string,
  workflowCwd: string
): Promise<CommandResult> {
  const discovered = await discoverWorkflowEntries(
    workflowCwd,
    'cmd.workflow_resume_discovery_failed',
    { runId }
  );
  if ('success' in discovered) return discovered;

  const workflows = discovered.workflowEntries.map(ws => ws.workflow);
  const workflow = resolveWorkflowName(run.workflow_name, workflows);
  if (!workflow) return workflowNotFoundForResume(run.workflow_name, runId, discovered.loadErrors);

  return {
    success: true,
    message: `Resuming workflow: \`${workflow.name}\``,
    workflow: { definition: workflow, args: run.user_message, resumeRunId: run.id, resumeRun: run },
  };
}

function workflowNotFoundForResume(
  workflowName: string,
  runId: string,
  loadErrors: readonly WorkflowLoadError[]
): CommandResult {
  const loadError = findWorkflowLoadError(loadErrors, workflowName);
  if (loadError) {
    return {
      success: false,
      message: `Workflow \`${workflowName}\` failed to load: ${loadError.error}\n\nFix the YAML file and try again.`,
    };
  }
  return {
    success: false,
    message: `Workflow \`${workflowName}\` for run ${runId} was not found.\n\nUse /workflow list to check available workflows.`,
  };
}

async function handleWorkflowAbandon(context: WorkflowCommandContext): Promise<CommandResult> {
  const runId = context.args[1];
  if (!runId)
    return {
      success: false,
      message: 'Usage: /workflow abandon <id>\n\nUse /workflow status to see active runs.',
    };
  try {
    const result = await abandonWorkflow(runId);
    return { success: true, message: formatAbandonWorkflowResult(result, runId) };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, runId }, 'cmd.workflow_abandon_failed');
    return { success: false, message: `Failed to abandon workflow run: ${err.message}` };
  }
}

function formatAbandonWorkflowResult(
  result: Awaited<ReturnType<typeof abandonWorkflow>>,
  runId: string
): string {
  let message = `Abandoned workflow run \`${result.run.workflow_name}\` (${runId})`;
  if (result.cascadeFailures > 0) {
    message += `\n⚠️ ${String(result.cascadeFailures)} sub-run(s) could not be cancelled and may still be running — check /workflow status.`;
  }
  if (result.blockedParentRunId) {
    message += `\n⚠️ Parent run ${result.blockedParentRunId} was blocked on this sub-run and stays paused. Resume it to fail the node cleanly, or abandon it too.`;
  }
  return message;
}

async function handleWorkflowResetSessions(
  context: WorkflowCommandContext
): Promise<CommandResult> {
  const workflowName = context.args[1];
  const nodeId = context.args[2];
  if (!workflowName) {
    return {
      success: false,
      message:
        'Usage: /workflow reset-sessions <workflow-name> [<node-id>]\n\nClears persisted AI session memory for this workflow in this conversation.',
    };
  }
  try {
    const { deleted } = await resetWorkflowNodeSessions({
      workflow_name: workflowName,
      scope_key: context.conversation.id,
      node_id: nodeId,
    });
    const nodeSuffix = nodeId ? ` node \`${nodeId}\` of` : '';
    return {
      success: true,
      message: `Cleared ${deleted} persisted session(s) for${nodeSuffix} workflow \`${workflowName}\` in this conversation.`,
    };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, workflowName, nodeId }, 'cmd.workflow_reset_sessions_failed');
    return { success: false, message: `Failed to reset workflow sessions: ${err.message}` };
  }
}

async function handleWorkflowApprove(context: WorkflowCommandContext): Promise<CommandResult> {
  const runId = context.args[1];
  if (!runId)
    return {
      success: false,
      message: 'Usage: /workflow approve <id> [comment]\n\nApproves a paused workflow run.',
    };
  const rawComment = context.args.slice(2).join(' ');
  const comment = rawComment.length > 0 ? rawComment : undefined;
  try {
    const result = await approveWorkflow(runId, comment);
    const pathInfo = result.workingPath ? `\nPath: \`${result.workingPath}\`` : '';
    const msg =
      result.type === 'interactive_loop'
        ? `Workflow \`${result.workflowName}\` loop input received.${pathInfo}\nType your next message in this conversation to resume the workflow.`
        : `Workflow \`${result.workflowName}\` approved.${pathInfo}\nType your response in this conversation to resume the workflow.`;
    return { success: true, message: msg };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, runId }, 'cmd.workflow_approve_failed');
    return { success: false, message: `Failed to approve workflow run: ${err.message}` };
  }
}

async function handleWorkflowReject(context: WorkflowCommandContext): Promise<CommandResult> {
  const runId = context.args[1];
  if (!runId)
    return {
      success: false,
      message: 'Usage: /workflow reject <id> [reason]\n\nRejects a paused workflow run.',
    };
  const reason = context.args.slice(2).join(' ') || 'Rejected';
  try {
    const result = await rejectWorkflow(runId, reason);
    if (result.cancelled) {
      const suffix = result.maxAttemptsReached ? ' (max attempts reached)' : '';
      return {
        success: true,
        message: `Workflow \`${result.workflowName}\` rejected and cancelled${suffix}.`,
      };
    }
    return {
      success: true,
      message:
        `Workflow \`${result.workflowName}\` rejected. Reworking with your feedback...\n` +
        'Type your next message in this conversation to resume the workflow.',
    };
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, runId }, 'cmd.workflow_reject_failed');
    return { success: false, message: `Failed to reject workflow run: ${err.message}` };
  }
}

async function handleWorkflowRun(context: WorkflowCommandContext): Promise<CommandResult> {
  const workflowName = context.args[1];
  const restArgs = context.args.slice(2);
  const force = restArgs.includes('--force');
  const workflowArgs = restArgs.filter(arg => arg !== '--force').join(' ');

  if (!workflowName) {
    return {
      success: false,
      message:
        'Usage: /workflow run <name> [args]\n\nUse /workflow list to see available workflows.',
    };
  }

  getLog().debug(
    { workflowName, args: workflowArgs, cwd: context.workflowCwd },
    'cmd.workflow_run_invoked'
  );
  const discovered = await discoverWorkflowEntries(
    context.workflowCwd,
    'cmd.workflow_discovery_failed'
  );
  if ('success' in discovered) return discovered;

  return resolveWorkflowRunResult(workflowName, workflowArgs, force, discovered);
}

function resolveWorkflowRunResult(
  workflowName: string,
  workflowArgs: string,
  force: boolean,
  discovered: {
    workflowEntries: readonly WorkflowWithSource[];
    loadErrors: readonly WorkflowLoadError[];
  }
): CommandResult {
  const workflows = discovered.workflowEntries.map(ws => ws.workflow);
  getLog().debug(
    { count: workflows.length, names: workflows.map(w => w.name), searchingFor: workflowName },
    'cmd.workflows_discovered'
  );

  let workflow: WorkflowDefinition | undefined;
  try {
    workflow = resolveWorkflowName(workflowName, workflows);
  } catch (err) {
    getLog().warn(
      { requested: workflowName, error: (err as Error).message },
      'cmd.workflow_resolve_ambiguous'
    );
    return { success: false, message: (err as Error).message };
  }

  if (!workflow) return workflowNotFoundForRun(workflowName, workflows, discovered.loadErrors);

  getLog().info({ workflow: workflow.name, args: workflowArgs }, 'cmd.workflow_starting');
  const resolvedEntry = discovered.workflowEntries.find(ws => ws.workflow === workflow);
  return {
    success: true,
    message: `Starting workflow: \`${workflow.name}\``,
    workflow: {
      definition: workflow,
      args: workflowArgs,
      force: force ? true : undefined,
      ...(resolvedEntry?.parseWarnings && resolvedEntry.parseWarnings.length > 0
        ? { parseWarnings: resolvedEntry.parseWarnings }
        : {}),
    },
  };
}

function workflowNotFoundForRun(
  workflowName: string,
  workflows: readonly WorkflowDefinition[],
  loadErrors: readonly WorkflowLoadError[]
): CommandResult {
  const loadError = findWorkflowLoadError(loadErrors, workflowName);
  if (loadError) {
    return {
      success: false,
      message: `Workflow \`${workflowName}\` failed to load: ${loadError.error}\n\nFix the YAML file and try again.`,
    };
  }
  getLog().warn(
    { requested: workflowName, available: workflows.map(w => w.name) },
    'cmd.workflow_not_found'
  );
  return {
    success: false,
    message: `Workflow \`${workflowName}\` not found.\n\nUse /workflow list to see available workflows.`,
  };
}

function workflowUsageResult(): CommandResult {
  return {
    success: false,
    message:
      'Usage:\n  /workflow list - Show available workflows\n  /workflow reload - Reload workflow definitions\n  /workflow status - Show all active workflows\n  /workflow cancel - Cancel running workflow\n  /workflow resume <id> - Resume a failed run\n  /workflow abandon <id> - Discard a failed run\n  /workflow approve <id> [comment] - Approve a paused run\n  /workflow reject <id> [reason] - Reject a paused run\n  /workflow reset-sessions <name> [<node-id>] - Clear persisted AI session memory for this conversation\n  /workflow run <name> [args] - Run a workflow directly',
  };
}

function helpResult(): CommandResult {
  return {
    success: true,
    message: `## Archon Orchestrator

Talk naturally — the orchestrator routes your requests to the right workflow and project automatically.

### Commands

**Chat**
- Just type your message — the orchestrator handles routing
- Mention a project by name and the orchestrator will use it
- Ask to "run [workflow] on [project]" for explicit invocation

**Workflows**
- \`/workflow list\` — List available workflows
- \`/workflow run <name> [message]\` — Run a workflow explicitly
- \`/workflow status\` — Show all active workflows
- \`/workflow cancel\` — Cancel the active workflow
- \`/workflow resume <id>\` — Resume a failed run
- \`/workflow abandon <id>\` — Discard a failed run
- \`/workflow approve <id>\` — Approve a paused run
- \`/workflow reject <id>\` — Reject a paused run
- \`/workflow reset-sessions <name> [<node-id>]\` — Clear persisted AI session memory for this conversation

**Projects**
- \`/register-project <name> <path>\` — Register a local project
- \`/update-project <name> <new-path>\` — Update a project's path
- \`/remove-project <name>\` — Remove a registered project
- \`/setproject <name>\` — Bind this conversation to a registered project

**Session**
- \`/status\` — Show current session and project info
- \`/reset\` — Clear conversation and start fresh
- \`/help\` — Show this help message

### Tips
- You don't need to select a project first — just describe what you want
- The orchestrator knows all your registered projects and available workflows
- For project setup, ask the orchestrator: "How do I add a new project?"`,
  };
}

async function handleStatusCommand(conversation: Conversation): Promise<CommandResult> {
  let msg = `## Orchestrator Status\n\n**Platform**: ${conversation.platform_type}\n**AI Assistant**: ${conversation.ai_assistant_type}`;
  msg += await formatRegisteredProjects();

  const codebase = conversation.codebase_id
    ? await codebaseDb.getCodebase(conversation.codebase_id)
    : null;
  msg += await formatConversationContext(conversation, codebase);
  msg += await formatActiveSession(conversation);
  msg += await formatActiveWorkflow(conversation);
  msg += await formatWorktreeBreakdown(codebase);

  return { success: true, message: msg };
}

async function formatRegisteredProjects(): Promise<string> {
  const allCodebases = await codebaseDb.listCodebases();
  if (allCodebases.length === 0) {
    return '\n\n## Registered Projects\nNone — ask the orchestrator to add a project.';
  }

  let msg = `\n\n## Registered Projects (${String(allCodebases.length)})\n`;
  for (const cb of allCodebases) {
    const urlSuffix = cb.repository_url
      ? ` (${cb.repository_url.replace(/.*github\.com\//, '')})`
      : '';
    msg += `- ${cb.name}${urlSuffix}\n`;
  }
  return msg;
}

async function formatConversationContext(
  conversation: Conversation,
  codebase: CommandCodebase | null
): Promise<string> {
  if (!codebase?.name) {
    return '\n\n## Conversation Context\n- Project: None — orchestrator will route as needed';
  }

  const repoContext = await formatRepoContext(codebase, conversation.isolation_env_id);
  const effectiveCwd = conversation.cwd ?? codebase.default_cwd;
  let msg = `\n\n## Conversation Context\n- Project: ${repoContext}`;
  msg += `\n- Working Directory: ${effectiveCwd}`;

  if (codebase.kind === 'folder') {
    const childRepos = await listChildRepos(codebase.default_cwd);
    if (childRepos.length > 0) msg += `\n- ${formatChildRepos(childRepos)}`;
  }
  return msg;
}

async function formatActiveSession(conversation: Conversation): Promise<string> {
  const session = await sessionDb.getActiveSession(conversation.id);
  return session?.id ? `\nActive Session: ${session.id.slice(0, 8)}...` : '';
}

async function formatActiveWorkflow(conversation: Conversation): Promise<string> {
  try {
    const activeWorkflow = await workflowDb.getActiveWorkflowRun(conversation.id);
    if (!activeWorkflow) return '';
    const timing = calculateWorkflowTiming(activeWorkflow);
    if (!timing.isValid) {
      return `\n\nActive Workflow: \`${activeWorkflow.workflow_name}\` (timing unavailable)\n  Cancel: \`/workflow cancel\``;
    }
    let msg = `\n\nActive Workflow: \`${activeWorkflow.workflow_name}\``;
    msg += `\n  ID: ${activeWorkflow.id.slice(0, 8)}`;
    msg += `\n  Duration: ${timing.durationMin}m ${timing.durationSec}s`;
    msg += `\n  Last activity: ${timing.lastActivitySec}s ago`;
    msg += '\n  Cancel: `/workflow cancel`';
    return msg;
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, conversationId: conversation.id }, 'cmd.workflow_status_query_failed');
    return '';
  }
}

async function formatWorktreeBreakdown(codebase: CommandCodebase | null): Promise<string> {
  if (!codebase || codebase.kind === 'folder') return '';

  try {
    const breakdown = await getWorktreeStatusBreakdown(codebase.id, codebase.default_cwd);
    let msg = `\n\nWorktrees: ${String(breakdown.total)} active`;
    if (breakdown.merged > 0 || breakdown.stale > 0) {
      if (breakdown.merged > 0) msg += `\n  • ${String(breakdown.merged)} merged (can auto-remove)`;
      if (breakdown.stale > 0) msg += `\n  • ${String(breakdown.stale)} stale (14+ days inactive)`;
      msg += `\n  • ${String(breakdown.active)} active`;
    }
    return msg;
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, codebaseId: codebase.id }, 'cmd.worktree_breakdown_failed');
    return '';
  }
}

async function handleInitCommand(conversation: Conversation): Promise<CommandResult> {
  const initCodebase = conversation.codebase_id
    ? await codebaseDb.getCodebase(conversation.codebase_id)
    : null;
  const initCwd = conversation.cwd ?? initCodebase?.default_cwd;
  if (!initCwd) {
    return {
      success: false,
      message:
        'No project selected. Pick one with /setproject <name> (register it first with /register-project if needed).',
    };
  }

  try {
    return await createArchonScaffold(initCwd);
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, command: 'init' }, 'cmd.init_failed');
    return { success: false, message: `Failed to initialize: ${err.message}` };
  }
}

async function createArchonScaffold(initCwd: string): Promise<CommandResult> {
  const archonDir = join(initCwd, '.archon');
  const commandsDir = join(archonDir, 'commands');
  const configPath = join(archonDir, 'config.yaml');

  try {
    await access(archonDir);
    return { success: false, message: '.archon directory already exists. Nothing to do.' };
  } catch {
    // Directory doesn't exist, we can create it.
  }

  await import('fs/promises').then(fs => fs.mkdir(commandsDir, { recursive: true }));
  await writeFile(configPath, DEFAULT_ARCHON_CONFIG);
  await writeFile(join(commandsDir, 'example.md'), DEFAULT_EXAMPLE_COMMAND);

  return {
    success: true,
    message: `Created .archon structure:
  .archon/
  ├── config.yaml
  └── commands/
      └── example.md

Commands are auto-discovered from .archon/commands/ — no registration needed.`,
  };
}

const DEFAULT_ARCHON_CONFIG = `# Archon repository configuration
# See: https://github.com/coleam00/Archon

# AI assistant preference (optional - overrides global default)
# assistant: claude

# Commands configuration (optional)
# commands:
#   folder: .archon/commands
#   autoLoad: true
`;

const DEFAULT_EXAMPLE_COMMAND = `---
description: Example command
---
# Example Command

This is an example command.

Arguments:
- $ARGUMENTS - The full trigger message

Task: $ARGUMENTS
`;

export async function handleCommand(
  conversation: Conversation,
  message: string
): Promise<CommandResult> {
  const { command, args } = parseCommand(message);

  switch (command) {
    case 'help':
      return helpResult();

    case 'status':
      return handleStatusCommand(conversation);

    case 'commands': {
      if (!conversation.codebase_id) {
        return { success: false, message: 'No codebase configured.' };
      }

      const commands = await codebaseDb.getCodebaseCommands(conversation.codebase_id);

      if (!Object.keys(commands).length) {
        return {
          success: true,
          message: 'No commands registered.\n\nAdd .md files to .archon/commands/ in your project.',
        };
      }

      let msg = 'Registered Commands:\n\n';
      for (const [name, def] of Object.entries(commands)) {
        msg += `${name} - ${def.path}\n`;
      }
      return { success: true, message: msg };
    }

    case 'reset': {
      const session = await sessionDb.getActiveSession(conversation.id);
      if (session) {
        await safeDeactivateSession(session.id, 'reset');
        return {
          success: true,
          message:
            'Session cleared. Starting fresh on next message.\n\nCodebase configuration preserved.',
        };
      }
      return {
        success: true,
        message: 'No active session to reset.',
      };
    }

    case 'worktree':
      return handleWorktreeCommand(conversation, args);

    case 'workflow':
      return handleWorkflowCommand(conversation, args);

    case 'init':
      return handleInitCommand(conversation);

    default:
      return {
        success: false,
        message: `Unknown command: /${command}\n\nType /help to see available commands.`,
      };
  }
}
