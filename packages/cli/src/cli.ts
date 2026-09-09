#!/usr/bin/env bun
/**
 * Archon CLI - Run AI workflows from the command line
 *
 * Usage:
 *   archon workflow list              List available workflows
 *   archon workflow run <name> [msg]  Run a workflow
 *   archon version                    Show version info
 */
// Must be the very first import — strips Bun-auto-loaded CWD .env keys before
// any module reads process.env at init time (e.g. @archon/paths/logger reads LOG_LEVEL).
import '@archon/paths/strip-cwd-env-boot';
// Then load archon-owned env from ~/.archon/.env (user scope) and
// <cwd>/.archon/.env (repo scope, wins over user). Both with override: true.
// See packages/paths/src/env-loader.ts and the three-path model (#1302 / #1303).
import { loadArchonEnv } from '@archon/paths/env-loader';
loadArchonEnv(process.cwd());

import { parseArgs } from 'util';
import { resolve } from 'path';
import { existsSync, realpathSync } from 'fs';

// Smart defaults for Claude auth
// If no explicit tokens, default to global auth from `claude /login`
if (!process.env.CLAUDE_API_KEY && !process.env.CLAUDE_CODE_OAUTH_TOKEN) {
  if (process.env.CLAUDE_USE_GLOBAL_AUTH === undefined) {
    process.env.CLAUDE_USE_GLOBAL_AUTH = 'true';
  }
}

// DATABASE_URL is no longer required - SQLite will be used as default

// Bootstrap provider registry before any provider lookups
import { registerBuiltinProviders, registerCommunityProviders } from '@archon/providers';
registerBuiltinProviders();
registerCommunityProviders();

// Import commands after dotenv is loaded
import { versionCommand } from './commands/version';
import {
  workflowListCommand,
  workflowRunCommand,
  workflowStatusCommand,
  workflowGetCommand,
  workflowRunsCommand,
  workflowResumeCommand,
  workflowAbandonCommand,
  workflowApproveCommand,
  workflowRejectCommand,
  workflowCleanupCommand,
  workflowResetSessionsCommand,
  workflowEventEmitCommand,
  workflowSearchCommand,
  workflowInstallCommand,
  isValidEventType,
} from './commands/workflow';
import { WORKFLOW_EVENT_TYPES } from '@archon/workflows/store';
import {
  isolationListCommand,
  isolationCleanupCommand,
  isolationCleanupMergedCommand,
  isolationCompleteCommand,
} from './commands/isolation';
import { continueCommand } from './commands/continue';
import { chatCommand } from './commands/chat';
import { setupCommand } from './commands/setup';
import { skillInstallCommand } from './commands/skill';
import { validateWorkflowsCommand, validateCommandsCommand } from './commands/validate';
import { serveCommand } from './commands/serve';
import { doctorCommand } from './commands/doctor';
import { authGithubCommand } from './commands/auth';
import {
  aiKeySetCommand,
  aiListCommand,
  aiLogoutCommand,
  aiLoginCommand,
  aiTierSetCommand,
  aiTierListCommand,
  aiTierUnsetCommand,
  aiAliasSetCommand,
  aiAliasListCommand,
  aiAliasUnsetCommand,
  aiDefaultCommand,
} from './commands/ai';
import { telemetryStatusCommand, telemetryResetCommand } from './commands/telemetry';
import { closeDatabase } from '@archon/core';
import {
  setLogLevel,
  createLogger,
  checkForUpdate,
  BUNDLED_IS_BINARY,
  BUNDLED_VERSION,
  shutdownTelemetry,
  captureArchonStarted,
  isVerboseBoot,
} from '@archon/paths';
import * as git from '@archon/git';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('cli');
  return cachedLog;
}

/**
 * Print usage information
 */
function printUsage(): void {
  console.log(`
Archon CLI - Run AI workflows from the command line

Usage:
  archon <command> [subcommand] [options] [arguments]

Commands:
  chat <message>             Send a message to the orchestrator
  setup                      Interactive setup wizard for credentials and config
  workflow list              List available workflows in current directory
  workflow run <name> [msg]  Run a workflow with optional message
  workflow status            Show status of running/paused workflows
  workflow runs              List recent runs (all statuses) for this project
  workflow get <run-id>      Show detail for a single run (any status)
  workflow resume <run-id>   Resume a failed or paused run from completed nodes
  workflow search [query]    Search the workflow marketplace
  workflow install <slug>    Install a workflow from the marketplace
  isolation list             List all active worktrees/environments
  isolation cleanup [days]   Remove stale environments (default: 7 days)
  isolation cleanup --merged Remove environments with branches merged into main
  continue <branch> [msg]    Continue work on an existing worktree with prior context
  complete <branch> [...]    Complete branch lifecycle (remove worktree + branches)
  serve                      Start the web UI server (downloads web UI on first run)
  skill install [path]       Install the bundled Archon skill into .claude/skills/archon
  doctor [--full]            Verify your Archon setup (Claude/Codex binaries, gh auth, DB, adapters; --full also probes the OpenCode runtime SDK)
  auth github                Connect your GitHub identity via device flow (multi-user installs)
  ai key set <provider>      Connect an AI provider API key (multi-user installs; key read from prompt/stdin)
  ai login <provider>        Connect a subscription (claude/copilot) via OAuth — codex is API-key only
  ai list                    List your connected AI provider keys
  ai logout <provider>       Disconnect an AI provider key
  ai tier set <t> <p> <m>    Set a model tier (small/medium/large) → provider/model [--effort <e>] [--scope user|install]
  ai tier list [--json]      Show configured tiers (install + yours) vs built-in defaults
  ai tier unset <tier>       Reset a tier to its built-in default [--scope user|install]
  ai alias set <@n> <p> <m>  Set a @custom model alias [--effort <e>] [--scope user|install]
  ai alias list [--json]     Show configured @custom aliases (install + yours)
  ai alias unset <@name>     Remove a @custom alias [--scope user|install]
  ai default <p> [<model>]   Set the default assistant (+ chat model) [--scope user|install]
  telemetry status           Show anonymous telemetry state (enabled, reason, ID, host)
  telemetry reset            Rotate the anonymous install UUID
  validate workflows [name]  Validate workflow definitions and their references
  validate commands [name]   Validate command files
  version, --version, -V     Show version info (also -v when used alone)
  help                       Show this help message

Options:
  --cwd <path>               Override working directory (default: current directory)
  --branch, -b <name>        Create worktree for branch (or reuse existing)
  --from, --from-branch <name> Create new branch from specific start point
  --base <branch>            Per-dispatch base override for epic slices (worktree cut-from + PR target)
  --no-worktree              Run on branch directly without worktree isolation
  --folder                   Register the current non-git directory as a folder project and run in place
  --resume                   Resume the most recent failed or paused run of the workflow (mutually exclusive with --branch)
  --spawn                    Open setup wizard in a new terminal window (for setup command)
  --quiet, -q                Reduce log verbosity to warnings and errors only
  --verbose, -v              Show debug-level output
  --json                     Output machine-readable JSON (list/status/get/runs/approve/reject/abandon/resume)
  --events                   For verbose JSON status/get: output raw event rows instead of node summaries
  --detach                   Run 'workflow run' in a detached background child (returns immediately)
  --all                      For 'workflow runs': list across all projects (ignore cwd scope)
  --status <status>          For 'workflow runs': filter to one status (running, completed, failed, ...)
  --limit <n>                For 'workflow runs': max rows (default 20)
  --workflow <name>          Workflow to run for 'continue' (default: archon-assist)
  --no-context               Skip context injection for 'continue'
  --conversation-id <id>     Reuse a stable conversation scope across runs (enables
                             persist_session resume between separate CLI invocations)
  --port <port>              Override server port for 'serve' (default: 3090)
  --download-only            Download web UI without starting the server
  --force                    Overwrite existing file (for workflow install)

Examples:
  archon chat "What does the orchestrator do?"
  archon workflow list
  archon workflow run investigate-issue "Fix the login bug"
  archon workflow run plan --cwd /path/to/repo "Add dark mode"
  archon workflow run implement --branch feature-auth "Implement auth"
  archon workflow run quick-fix --no-worktree "Fix typo"
  archon workflow run assist --folder "List every repo under this multi-repo root"
  archon workflow run archon-assist --detach "Investigate the flaky test"
  archon workflow runs --json
  archon workflow get <run-id> --json
  archon workflow resume <run-id>
  archon continue fix/issue-42 --workflow archon-smart-pr-review "Review the changes"
  archon skill install
  archon skill install /path/to/project
  archon workflow search "pr review"
  archon workflow install archon-piv-loop
`);
}

/**
 * Safely close the database connection
 */
async function closeDb(): Promise<void> {
  try {
    await closeDatabase();
  } catch (error) {
    const err = error as Error;
    // Log with details but don't throw - we want the original error to be visible
    getLog().warn({ err }, 'db_close_failed');
  }
}

async function printUpdateNotice(quiet: boolean | undefined): Promise<void> {
  if (quiet || !BUNDLED_IS_BINARY) return;
  try {
    const result = await checkForUpdate(BUNDLED_VERSION);
    if (result?.updateAvailable) {
      process.stderr.write(
        `Update available: v${result.currentVersion} → v${result.latestVersion} — ${result.releaseUrl}\n`
      );
    }
  } catch (err) {
    getLog().debug({ err }, 'update_check.notice_failed');
  }
}

/**
 * Main CLI entry point
 * Returns exit code (0 = success, non-zero = failure)
 */
/**
 * Detect a request for version output. Treats `--version`, `-V`, and the
 * single-dash typo `-version` as version flags anywhere in argv. `-v` keeps
 * its role as the short alias for `--verbose`, except when used alone — then
 * it falls back to version output to match the convention used by node, npm,
 * bun, and most other CLIs.
 */
function isVersionRequest(args: string[]): boolean {
  if (args.length === 1 && args[0] === '-v') return true;
  return args.some(arg => arg === '--version' || arg === '-V' || arg === '-version');
}

interface ParsedCliArgs {
  values: Record<string, unknown>;
  positionals: string[];
}

interface CliContext extends ParsedCliArgs {
  args: string[];
  cwd: string;
  command: string | undefined;
  subcommand: string | undefined;
  effectiveCwd: string;
}

const NO_GIT_COMMANDS = [
  'version',
  'help',
  'setup',
  'chat',
  'continue',
  'serve',
  'skill',
  'doctor',
  'telemetry',
  'auth',
  'ai',
];

async function main(): Promise<number> {
  const args = process.argv.slice(2);
  captureArchonStarted({ surface: 'cli' });

  if (args.length === 0) return shutdownAfterUsage();
  if (isVersionRequest(args)) return runVersionAndShutdown();

  const parsedArgs = await parseCliArgsSafely(args);
  if (!parsedArgs) return 1;
  if (parsedArgs.values.help) return shutdownAfterUsage();

  const context = buildCliContext(args, parsedArgs);
  try {
    configureCliLogging(context);
    const marketplaceResult = await maybeRunWorkflowSearch(context);
    if (marketplaceResult !== undefined) return marketplaceResult;
    context.effectiveCwd = await resolveEffectiveCwd(context);
    const result = await dispatchCliCommand(context);
    if (shouldPrintUpdateNotice(context, result)) {
      await printUpdateNotice(context.values.quiet as boolean | undefined);
    }
    return result;
  } catch (error) {
    printCliError(error as Error);
    return 1;
  } finally {
    await shutdownTelemetry();
    await closeDb();
  }
}

async function shutdownAfterUsage(): Promise<number> {
  printUsage();
  await shutdownTelemetry();
  return 0;
}

async function runVersionAndShutdown(): Promise<number> {
  try {
    await versionCommand();
    return 0;
  } finally {
    await shutdownTelemetry();
    await closeDb();
  }
}

async function parseCliArgsSafely(args: string[]): Promise<ParsedCliArgs | null> {
  try {
    return parseArgs({
      args,
      options: cliParseOptions(),
      allowPositionals: true,
      strict: false,
    });
  } catch (error) {
    const err = error as Error;
    console.error(`Error parsing arguments: ${err.message}`);
    printUsage();
    await shutdownTelemetry();
    return null;
  }
}

function cliParseOptions(): NonNullable<Parameters<typeof parseArgs>[0]>['options'] {
  return {
    cwd: { type: 'string', default: process.cwd() },
    help: { type: 'boolean', short: 'h' },
    branch: { type: 'string', short: 'b' },
    from: { type: 'string' },
    'from-branch': { type: 'string' },
    base: { type: 'string' },
    'no-worktree': { type: 'boolean' },
    folder: { type: 'boolean' },
    container: { type: 'boolean' },
    resume: { type: 'boolean' },
    spawn: { type: 'boolean' },
    quiet: { type: 'boolean', short: 'q' },
    verbose: { type: 'boolean', short: 'v' },
    json: { type: 'boolean' },
    events: { type: 'boolean' },
    'run-id': { type: 'string' },
    type: { type: 'string' },
    data: { type: 'string' },
    comment: { type: 'string' },
    reason: { type: 'string' },
    workflow: { type: 'string' },
    'no-context': { type: 'boolean' },
    port: { type: 'string' },
    'download-only': { type: 'boolean' },
    scope: { type: 'string' },
    node: { type: 'string' },
    yes: { type: 'boolean' },
    force: { type: 'boolean' },
    'conversation-id': { type: 'string' },
    detach: { type: 'boolean' },
    all: { type: 'boolean' },
    status: { type: 'string' },
    limit: { type: 'string' },
    effort: { type: 'string' },
    full: { type: 'boolean' },
  };
}

function buildCliContext(args: string[], parsedArgs: ParsedCliArgs): CliContext {
  const cwdValue = parsedArgs.values.cwd;
  const cwd = resolve(typeof cwdValue === 'string' ? cwdValue : process.cwd());
  return {
    args,
    values: parsedArgs.values,
    positionals: parsedArgs.positionals,
    cwd,
    command: parsedArgs.positionals[0],
    subcommand: parsedArgs.positionals[1],
    effectiveCwd: cwd,
  };
}

function configureCliLogging(context: CliContext): void {
  const { values, command } = context;
  const jsonFlag = values.json as boolean | undefined;
  const isInteractiveCommand =
    command === 'setup' || command === 'doctor' || command === 'telemetry';
  const suppressByDefault = isInteractiveCommand && !values.verbose && !isVerboseBoot();
  if (jsonFlag) setLogLevel('silent');
  else if (values.quiet || suppressByDefault) setLogLevel('warn');
  else if (values.verbose) setLogLevel('debug');
}

async function maybeRunWorkflowSearch(context: CliContext): Promise<number | undefined> {
  if (context.command !== 'workflow' || context.subcommand !== 'search') return undefined;
  try {
    await workflowSearchCommand(context.positionals[2], context.values.json as boolean | undefined);
    return 0;
  } catch (error) {
    const err = error as Error;
    console.error(`Error: ${err.message}`);
    return 1;
  }
}

async function resolveEffectiveCwd(context: CliContext): Promise<string> {
  if (!requiresGitRepo(context.command)) return context.cwd;
  if (!existsSync(context.cwd)) {
    throw new CliUsageError(`Error: Directory does not exist: ${context.cwd}`);
  }

  const repoRoot = await git.findRepoRoot(context.cwd);
  if (repoRoot) return repoRoot;
  return resolveNonGitCwd(context);
}

function requiresGitRepo(command: string | undefined): boolean {
  return !NO_GIT_COMMANDS.includes(command ?? '');
}

class CliUsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CliUsageError';
  }
}

async function resolveNonGitCwd(context: CliContext): Promise<string> {
  const realCwd = realpathIfPossible(context.cwd);
  const { folderCodebase, gateLookupError } = await lookupFolderCodebaseForGate(realCwd);
  if (folderCodebase?.kind === 'folder') return folderCodebase.default_cwd;
  if (context.values.folder && context.command === 'workflow' && context.subcommand === 'run')
    return realCwd;
  if (gateLookupError && looksLikeConnectionError(gateLookupError)) {
    throw new CliUsageError(
      [
        'Error: Could not verify project registration — the database is unavailable.',
        `  ${gateLookupError.message}`,
        '  Check that your database is running (or DATABASE_URL is set), then retry.',
      ].join('\n')
    );
  }
  throw new CliUsageError(
    [
      'Error: Not in a git repository.',
      'The Archon CLI must be run from within a git repository.',
      'Either navigate to a git repo or use --cwd to specify one.',
      'Or register this folder as a project: run with --folder, or use /register-project in chat.',
    ].join('\n')
  );
}

function realpathIfPossible(path: string): string {
  try {
    return realpathSync(path);
  } catch {
    return path;
  }
}

async function lookupFolderCodebaseForGate(cwd: string): Promise<{
  folderCodebase: { default_cwd: string; kind: 'repo' | 'folder' } | null;
  gateLookupError: Error | null;
}> {
  try {
    const codebaseDb = await import('@archon/core/db/codebases');
    const folderCodebase =
      (await codebaseDb.findCodebaseByDefaultCwd(cwd)) ??
      (await codebaseDb.findCodebaseByPathPrefix(cwd));
    return { folderCodebase, gateLookupError: null };
  } catch (dbError) {
    const gateLookupError = dbError as Error;
    getLog().warn({ err: gateLookupError, cwd }, 'cli.folder_project_gate_lookup_failed');
    return { folderCodebase: null, gateLookupError };
  }
}

function looksLikeConnectionError(e: Error): boolean {
  const m = e.message.toLowerCase();
  return m.includes('econnrefused') || m.includes('etimedout') || m.includes('connect');
}

function shouldPrintUpdateNotice(context: CliContext, result: number): boolean {
  if (result !== 0) return false;
  switch (context.command) {
    case 'version':
    case 'help':
    case 'chat':
    case 'setup':
    case 'isolation':
    case 'complete':
    case 'continue':
      return true;
    case 'workflow':
      return context.subcommand !== 'get';
    default:
      return false;
  }
}

async function dispatchCliCommand(context: CliContext): Promise<number> {
  switch (context.command) {
    case 'version':
      await versionCommand();
      return 0;
    case 'help':
      printUsage();
      return 0;
    case 'chat':
      return runChatCommand(context);
    case 'setup':
      return runSetupCommand(context);
    case 'workflow':
      return runWorkflowCommand(context);
    case 'isolation':
      return runIsolationCommand(context);
    case 'validate':
      return runValidateCommand(context);
    case 'complete':
      return runCompleteCommand(context);
    case 'continue':
      return runContinueCommand(context);
    case 'serve':
      return serveCommand({
        port: context.values.port !== undefined ? Number(context.values.port) : undefined,
        downloadOnly: Boolean(context.values['download-only']),
      });
    case 'doctor':
      return doctorCommand(undefined, Boolean(context.values.full));
    case 'auth':
      return runAuthCommand(context);
    case 'ai':
      return runAiCommand(context);
    case 'telemetry':
      return runTelemetryCommand(context);
    case 'skill':
      return runSkillCommand(context);
    default:
      printUnknownCommand(context.command);
      return 1;
  }
}

async function runChatCommand(context: CliContext): Promise<number> {
  const chatMessage = context.positionals.slice(1).join(' ');
  if (!chatMessage) return printUsageError('Usage: archon chat <message>');
  await chatCommand(chatMessage);
  return 0;
}

async function runSetupCommand(context: CliContext): Promise<number> {
  const rawScope = context.values.scope as string | undefined;
  if (rawScope !== undefined && rawScope !== 'home' && rawScope !== 'project') {
    return printUsageError(`Error: Invalid --scope: "${rawScope}". Must be "home" or "project".`);
  }
  const scope: 'home' | 'project' = rawScope ?? 'home';
  const repoPath = await resolveSetupRepoPath(scope, context.cwd);
  await setupCommand({
    spawn: context.values.spawn as boolean | undefined,
    repoPath,
    scope,
    force: (context.values.force as boolean | undefined) ?? false,
  });
  return 0;
}

async function resolveSetupRepoPath(scope: 'home' | 'project', cwd: string): Promise<string> {
  if (scope === 'home') return cwd;
  const repoRoot = await git.findRepoRoot(cwd);
  if (repoRoot) return repoRoot;
  throw new CliUsageError(
    [
      'Error: --scope project requires running from inside a git repository.',
      'Run from the repo root, pass --cwd <repo>, or use --scope home.',
    ].join('\n')
  );
}

async function runWorkflowCommand(context: CliContext): Promise<number> {
  switch (context.subcommand) {
    case 'list':
      await workflowListCommand(context.effectiveCwd, context.values.json as boolean | undefined);
      return 0;
    case 'run':
      return runWorkflowRunSubcommand(context);
    case 'status':
      await workflowStatusCommand(
        jsonFlag(context),
        verboseFlag(context),
        context.values.events as boolean | undefined
      );
      return 0;
    case 'get':
      return runWorkflowGetSubcommand(context);
    case 'runs':
      return runWorkflowRunsSubcommand(context);
    case 'resume':
      return runWorkflowIdCommand(context, 'resume');
    case 'abandon':
      return runWorkflowIdCommand(context, 'abandon');
    case 'approve':
      return runWorkflowApproveSubcommand(context);
    case 'reject':
      return runWorkflowRejectSubcommand(context);
    case 'cleanup':
      return runWorkflowCleanupSubcommand(context);
    case 'reset-sessions':
      return runWorkflowResetSessionsSubcommand(context);
    case 'event':
      return runWorkflowEventSubcommand(context);
    case 'install':
      return runWorkflowInstallSubcommand(context);
    default:
      printUnknownWorkflowSubcommand(context.subcommand);
      return 1;
  }
}

async function runWorkflowRunSubcommand(context: CliContext): Promise<number> {
  const workflowName = context.positionals[2];
  if (!workflowName) return printUsageError('Usage: archon workflow run <name> [message]');
  const invalid = validateWorkflowRunCliFlags(context);
  if (invalid !== undefined) return invalid;
  await workflowRunCommand(
    context.effectiveCwd,
    workflowName,
    context.positionals.slice(3).join(' ') || '',
    {
      branchName: context.values.branch as string | undefined,
      fromBranch:
        (context.values.from as string | undefined) ??
        (context.values['from-branch'] as string | undefined),
      baseBranch: context.values.base as string | undefined,
      noWorktree: context.values['no-worktree'] as boolean | undefined,
      folder: context.values.folder as boolean | undefined,
      container: context.values.container as boolean | undefined,
      resume: context.values.resume as boolean | undefined,
      quiet: context.values.quiet as boolean | undefined,
      verbose: verboseFlag(context),
      conversationId: context.values['conversation-id'] as string | undefined,
      detach: context.values.detach as boolean | undefined,
      json: jsonFlag(context),
    }
  );
  return 0;
}

function validateWorkflowRunCliFlags(context: CliContext): number | undefined {
  const branchName = context.values.branch as string | undefined;
  const fromBranch =
    (context.values.from as string | undefined) ??
    (context.values['from-branch'] as string | undefined);
  const baseBranch = context.values.base as string | undefined;
  const noWorktree = context.values['no-worktree'] as boolean | undefined;
  if (branchName !== undefined && noWorktree) {
    return printUsageError(
      'Error: --branch and --no-worktree are mutually exclusive.\n' +
        '  --branch creates an isolated worktree (safe).\n' +
        '  --no-worktree runs directly in your repo (no isolation).\n' +
        'Use one or the other.'
    );
  }
  if (noWorktree && fromBranch !== undefined) {
    return printUsageError(
      'Error: --from/--from-branch has no effect with --no-worktree.\nRemove --from or drop --no-worktree.'
    );
  }
  if (noWorktree && baseBranch !== undefined) {
    return printUsageError(
      'Error: --base has no effect with --no-worktree.\nRemove --base or drop --no-worktree.'
    );
  }
  if (context.values.resume && branchName !== undefined) {
    return printUsageError(
      'Error: --resume and --branch are mutually exclusive.\n' +
        '  --resume reuses the existing worktree from the failed run.\n' +
        '  Remove --branch when using --resume.'
    );
  }
  return undefined;
}

async function runWorkflowGetSubcommand(context: CliContext): Promise<number> {
  const runId = context.positionals[2];
  if (!runId)
    return printUsageError('Usage: archon workflow get <run-id> [--json] [--verbose] [--events]');
  return workflowGetCommand(
    runId,
    jsonFlag(context),
    verboseFlag(context),
    context.effectiveCwd,
    context.values.events as boolean | undefined
  );
}

async function runWorkflowRunsSubcommand(context: CliContext): Promise<number> {
  const limit = parseWorkflowRunsLimit(context.values.limit as string | undefined);
  if (limit === 'invalid') return 1;
  await workflowRunsCommand(context.effectiveCwd, {
    json: jsonFlag(context),
    all: context.values.all as boolean | undefined,
    status: context.values.status as string | undefined,
    limit,
  });
  return 0;
}

function parseWorkflowRunsLimit(rawLimit: string | undefined): number | undefined | 'invalid' {
  if (rawLimit === undefined) return undefined;
  const limit = Number(rawLimit);
  if (Number.isInteger(limit) && limit >= 1) return limit;
  console.error(`Error: --limit must be a positive integer, got '${rawLimit}'.`);
  return 'invalid';
}

async function runWorkflowIdCommand(
  context: CliContext,
  action: 'resume' | 'abandon'
): Promise<number> {
  const runId = context.positionals[2];
  if (!runId) return printUsageError(`Usage: archon workflow ${action} <run-id>`);
  if (action === 'resume')
    await workflowResumeCommand(runId, jsonFlag(context), context.effectiveCwd);
  else await workflowAbandonCommand(runId, jsonFlag(context), context.effectiveCwd);
  return 0;
}

async function runWorkflowApproveSubcommand(context: CliContext): Promise<number> {
  const runId = context.positionals[2];
  if (!runId) return printUsageError('Usage: archon workflow approve <run-id> [comment]');
  const rawComment =
    (context.values.comment as string | undefined) || context.positionals.slice(3).join(' ');
  await workflowApproveCommand(
    runId,
    rawComment.length > 0 ? rawComment : undefined,
    jsonFlag(context),
    context.effectiveCwd
  );
  return 0;
}

async function runWorkflowRejectSubcommand(context: CliContext): Promise<number> {
  const runId = context.positionals[2];
  if (!runId) return printUsageError('Usage: archon workflow reject <run-id> [reason]');
  const rawReason =
    (context.values.reason as string | undefined) || context.positionals.slice(3).join(' ');
  await workflowRejectCommand(
    runId,
    rawReason.length > 0 ? rawReason : undefined,
    jsonFlag(context),
    context.effectiveCwd
  );
  return 0;
}

async function runWorkflowCleanupSubcommand(context: CliContext): Promise<number> {
  const days = context.positionals[2] ? Number(context.positionals[2]) : 7;
  if (Number.isNaN(days) || days < 0) {
    console.error('Usage: archon workflow cleanup [days]');
    console.error('  days: delete terminal runs older than N days (default: 7)');
    return 1;
  }
  await workflowCleanupCommand(days);
  return 0;
}

async function runWorkflowResetSessionsSubcommand(context: CliContext): Promise<number> {
  const workflowName = context.positionals[2];
  const extras = context.positionals.slice(3);
  if (!workflowName) return printWorkflowResetSessionsUsage();
  if (extras.length > 0) return printWorkflowResetSessionsExtraArgs(extras);
  await workflowResetSessionsCommand(workflowName, {
    scope: context.values.scope as string | undefined,
    node: context.values.node as string | undefined,
    yes: context.values.yes as boolean | undefined,
    json: jsonFlag(context),
  });
  return 0;
}

function printWorkflowResetSessionsUsage(): number {
  console.error(
    'Usage: archon workflow reset-sessions <workflow-name> [--scope <key>] [--node <id>] [--yes] [--json]'
  );
  console.error(
    '  Without --scope: deletes persisted sessions across ALL scopes (requires --yes).'
  );
  return 1;
}

function printWorkflowResetSessionsExtraArgs(extras: string[]): number {
  console.error(
    'Usage: archon workflow reset-sessions <workflow-name> [--scope <key>] [--node <id>] [--yes] [--json]'
  );
  console.error(
    `Error: unexpected positional argument(s): ${extras.join(' ')}. Use --node <id> to filter by node.`
  );
  return 1;
}

async function runWorkflowEventSubcommand(context: CliContext): Promise<number> {
  const action = context.positionals[2];
  if (action !== 'emit') return printWorkflowEventActionError(action);
  const runId = context.values['run-id'] as string | undefined;
  const eventType = context.values.type as string | undefined;
  if (!runId) return printWorkflowEventRequiredError('--run-id');
  if (!eventType) return printWorkflowEventRequiredError('--type');
  if (!isValidEventType(eventType)) {
    console.error(`Error: unknown event type: ${eventType}`);
    console.error(`Valid types: ${WORKFLOW_EVENT_TYPES.join(', ')}`);
    return 1;
  }
  await workflowEventEmitCommand(
    runId,
    eventType,
    parseWorkflowEventData(context.values.data as string | undefined)
  );
  return 0;
}

function printWorkflowEventActionError(action: string | undefined): number {
  console.error(
    action === undefined
      ? 'Missing workflow event subcommand'
      : `Unknown workflow event subcommand: ${action}`
  );
  console.error('Available: emit');
  return 1;
}

function printWorkflowEventRequiredError(flag: '--run-id' | '--type'): number {
  console.error('Usage: archon workflow event emit --run-id <uuid> --type <event-type>');
  console.error(`Error: ${flag} is required`);
  return 1;
}

function parseWorkflowEventData(rawData: string | undefined): Record<string, unknown> | undefined {
  if (!rawData) return undefined;
  try {
    return JSON.parse(rawData) as Record<string, unknown>;
  } catch {
    console.warn(
      `Warning: --data is not valid JSON — event will be emitted without data payload: ${rawData}`
    );
    return undefined;
  }
}

async function runWorkflowInstallSubcommand(context: CliContext): Promise<number> {
  const slug = context.positionals[2];
  if (!slug) return printUsageError('Usage: archon workflow install <slug> [--force]');
  await workflowInstallCommand(
    slug,
    context.effectiveCwd,
    context.values.force as boolean | undefined
  );
  return 0;
}

async function runIsolationCommand(context: CliContext): Promise<number> {
  switch (context.subcommand) {
    case 'list':
      await isolationListCommand();
      return 0;
    case 'cleanup':
      await runIsolationCleanup(context);
      return 0;
    default:
      console.error(
        context.subcommand === undefined
          ? 'Missing isolation subcommand'
          : `Unknown isolation subcommand: ${context.subcommand}`
      );
      console.error('Available: list, cleanup');
      return 1;
  }
}

async function runIsolationCleanup(context: CliContext): Promise<void> {
  const mergedFlag = context.args.includes('--merged') || context.positionals.includes('--merged');
  if (mergedFlag)
    await isolationCleanupMergedCommand({
      includeClosed: context.args.includes('--include-closed'),
    });
  else await isolationCleanupCommand(parseInt(context.positionals[2] ?? '7', 10));
}

async function runValidateCommand(context: CliContext): Promise<number> {
  switch (context.subcommand) {
    case 'workflows':
      return validateWorkflowsCommand(
        context.effectiveCwd,
        context.positionals[2],
        jsonFlag(context)
      );
    case 'commands':
      return validateCommandsCommand(
        context.effectiveCwd,
        context.positionals[2],
        jsonFlag(context)
      );
    default:
      console.error(
        context.subcommand === undefined
          ? 'Missing validate target'
          : `Unknown validate target: ${context.subcommand}`
      );
      console.error('Available: workflows, commands');
      return 1;
  }
}

async function runCompleteCommand(context: CliContext): Promise<number> {
  const branches = context.positionals.slice(1);
  if (branches.length === 0)
    return printUsageError('Usage: archon complete <branch-name> [branch2 ...]');
  await isolationCompleteCommand(branches, {
    force: context.args.includes('--force'),
    deleteRemote: true,
  });
  return 0;
}

async function runContinueCommand(context: CliContext): Promise<number> {
  const branch = context.positionals[1];
  if (!branch)
    return printUsageError('Usage: archon continue <branch> [--workflow <name>] "instruction"');
  await continueCommand(branch, context.positionals.slice(2).join(' ') || '', {
    workflow: context.values.workflow as string | undefined,
    noContext: context.values['no-context'] as boolean | undefined,
  });
  return 0;
}

async function runAuthCommand(context: CliContext): Promise<number> {
  if (context.subcommand === 'github') return authGithubCommand();
  console.error(
    context.subcommand === undefined
      ? 'Missing auth subcommand'
      : `Unknown auth subcommand: ${context.subcommand}`
  );
  console.error('Available: github');
  return 1;
}

async function runAiCommand(context: CliContext): Promise<number> {
  switch (context.subcommand) {
    case 'key':
      return runAiKeyCommand(context);
    case 'list':
      return aiListCommand();
    case 'logout':
      return aiLogoutCommand(context.positionals[2]);
    case 'login':
      return aiLoginCommand(context.positionals[2]);
    case 'tier':
      return runAiTierCommand(context);
    case 'alias':
      return runAiAliasCommand(context);
    case 'default':
      return aiDefaultCommand(
        context.positionals[2],
        context.positionals[3],
        context.values.scope as string | undefined
      );
    default:
      console.error(
        context.subcommand === undefined
          ? 'Missing ai subcommand'
          : `Unknown ai subcommand: ${context.subcommand}`
      );
      console.error(
        'Available: key set <provider>, login <provider>, list, logout <provider>, tier set|list|unset, alias set|list|unset, default <provider> [<model>]'
      );
      return 1;
  }
}

async function runAiKeyCommand(context: CliContext): Promise<number> {
  if (context.positionals[2] !== 'set')
    return printUsageError('Usage: archon ai key set <provider>');
  return aiKeySetCommand(context.positionals[3]);
}

async function runAiTierCommand(context: CliContext): Promise<number> {
  const scopeFlag = context.values.scope as string | undefined;
  switch (context.positionals[2]) {
    case 'set':
      return aiTierSetCommand(
        context.positionals[3],
        context.positionals[4],
        context.positionals[5],
        context.values.effort as string | undefined,
        scopeFlag
      );
    case 'list':
      return aiTierListCommand(jsonFlag(context));
    case 'unset':
      return aiTierUnsetCommand(context.positionals[3], scopeFlag);
    default:
      return printUsageError(
        'Usage: archon ai tier set <small|medium|large> <provider> <model> [--effort <e>] [--scope user|install] | tier list [--json] | tier unset <tier> [--scope user|install]'
      );
  }
}

async function runAiAliasCommand(context: CliContext): Promise<number> {
  const scopeFlag = context.values.scope as string | undefined;
  switch (context.positionals[2]) {
    case 'set':
      return aiAliasSetCommand(
        context.positionals[3],
        context.positionals[4],
        context.positionals[5],
        context.values.effort as string | undefined,
        scopeFlag
      );
    case 'list':
      return aiAliasListCommand(jsonFlag(context));
    case 'unset':
      return aiAliasUnsetCommand(context.positionals[3], scopeFlag);
    default:
      return printUsageError(
        'Usage: archon ai alias set <@name> <provider> <model> [--effort <e>] [--scope user|install] | alias list [--json] | alias unset <@name> [--scope user|install]'
      );
  }
}

async function runTelemetryCommand(context: CliContext): Promise<number> {
  switch (context.subcommand) {
    case 'status':
      return telemetryStatusCommand();
    case 'reset':
      return telemetryResetCommand();
    default:
      console.error(
        context.subcommand === undefined
          ? 'Missing telemetry subcommand'
          : `Unknown telemetry subcommand: ${context.subcommand}`
      );
      console.error('Available: status, reset');
      return 1;
  }
}

async function runSkillCommand(context: CliContext): Promise<number> {
  if (context.subcommand !== 'install') {
    console.error(
      context.subcommand === undefined
        ? 'Missing skill subcommand'
        : `Unknown skill subcommand: ${context.subcommand}`
    );
    console.error('Available: install');
    return 1;
  }
  const targetArg = context.positionals[2];
  return skillInstallCommand(targetArg ? resolve(targetArg) : context.cwd);
}

function jsonFlag(context: CliContext): boolean | undefined {
  return context.values.json as boolean | undefined;
}

function verboseFlag(context: CliContext): boolean | undefined {
  return context.values.verbose as boolean | undefined;
}

function printUsageError(message: string): number {
  console.error(message);
  return 1;
}

function printUnknownWorkflowSubcommand(subcommand: string | undefined): void {
  console.error(
    subcommand === undefined
      ? 'Missing workflow subcommand'
      : `Unknown workflow subcommand: ${subcommand}`
  );
  console.error(
    'Available: list, run, status, get, runs, resume, abandon, approve, reject, cleanup, event, search, install'
  );
}

function printUnknownCommand(command: string | undefined): void {
  if (command === undefined) console.error('Missing command');
  else console.error(`Unknown command: ${command}`);
  printUsage();
}

function printCliError(err: Error): void {
  if (err instanceof CliUsageError) console.error(err.message);
  else console.error(`Error: ${err.message}`);
  if (process.env.DEBUG) console.error(err.stack);
}

// Exit explicitly so a lingering handle (DB pool, spawned child, timer) can
// never leave the CLI hanging after its work is done.
//
// This is safe for piped output because every machine-readable payload is
// emitted through `writeStdout()`/`writeJsonLine()` (src/utils/stdout.ts), which
// resolves only once the bytes have reached the OS. The #2384 truncation
// happened inside `console.log` at call time — not at exit — so deferring the
// exit would not have recovered it.
main()
  .then(exitCode => {
    process.exit(exitCode);
  })
  .catch((error: unknown) => {
    const err = error as Error;
    console.error('Fatal error:', err.message);
    process.exit(1);
  });
