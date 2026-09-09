/**
 * Orchestrator Agent - Main entry point for AI-powered message routing
 *
 * Single entry point for all platforms:
 * - Knows all registered projects and workflows upfront
 * - Can answer directly or invoke workflows
 * - Does NOT require a project to be selected before starting a conversation
 */
import { existsSync, realpathSync } from 'fs';
import { createLogger, captureChatTurn } from '@archon/paths';
import type {
  IPlatformAdapter,
  HandleMessageContext,
  Conversation,
  Codebase,
  AttachedFile,
} from '../types';
import type { MessageChunk, SendQueryOptions, TokenUsage } from '@archon/providers/types';
import { ConversationNotFoundError, isWebAdapter } from '../types';
import * as db from '../db/conversations';
import * as codebaseDb from '../db/codebases';
import * as sessionDb from '../db/sessions';
import * as commandHandler from '../handlers/command-handler';
import { formatToolCall } from '@archon/workflows/utils/tool-formatter';
import { classifyAndFormatError } from '../utils/error-formatter';
import { toError } from '../utils/error';
import { safeDeactivateSession } from '../state/session-transitions';
import { getAgentProvider, getProviderCapabilities } from '@archon/providers';
import { buildManageRunTool } from './manage-run-tool';
import { getArchonWorkspacesPath, ensureArchonWorkspacesPath } from '@archon/paths';
import { syncArchonToWorktree } from '../utils/worktree-sync';
import {
  execFileAsync,
  findRepoRoot,
  getDefaultRemote,
  syncWorkspace,
  toBranchName,
  toRepoPath,
} from '@archon/git';
import type { WorkspaceSyncResult } from '@archon/git';
import { discoverWorkflowsWithConfig } from '@archon/workflows/workflow-discovery';
import { findWorkflow, resolveWorkflowName } from '@archon/workflows/router';
import { executeWorkflow, hydrateResumableRun } from '@archon/workflows/executor';
import type { ExecuteWorkflowOptions } from '@archon/workflows/executor';
import {
  assertWorkflowRequirementsMet,
  WorkflowRequirementError,
} from '@archon/workflows/utils/workflow-requirements';
import type {
  WorkflowDefinition,
  WorkflowWithSource,
  WorkflowLoadError,
  WorkflowSource,
} from '@archon/workflows/schemas/workflow';
import type { WorkflowRun } from '@archon/workflows/schemas/workflow-run';
import { isGuardedWorkflowRun } from '@archon/workflows/workflow-pinning';
import { isPerUserGitHubEnabled } from '../github-auth/config';
import { getDecryptedAccessToken } from '../db/user-github-token-store';
import { isPerUserProviderKeysEnabled } from '../credentials/config';
import { deliverCredential } from '../credentials/delivery';
import { listDecryptedUserProviderCredentials } from '../db/user-provider-key-store';
import { getUserAiPrefs, type UserAiPrefs } from '../db/user-ai-prefs-store';
import { createWorkflowDeps } from '../workflows/store-adapter';
import { createChildWorktreeResolver } from '../workflows/child-isolation-resolver';
import { loadConfig, loadRepoConfig } from '../config/config-loader';
import type { MergedConfig } from '../config/config-types';
import { generateAndSetTitle } from '../services/title-generator';
import { validateAndResolveIsolation, dispatchBackgroundWorkflow } from './orchestrator';
import { IsolationBlockedError } from '@archon/isolation';
import {
  buildOrchestratorSystemAppend,
  buildRunManagementSection,
  formatWorkflowContextSection,
} from './prompt-builder';
import type { WorkflowResultContext } from './prompt-builder';
import { reportUnpushedWorkInSource } from './post-message-reminder';
import * as messageDb from '../db/messages';
import * as workflowDb from '../db/workflows';
import { getCodebaseEnvVars } from '../db/env-vars';
import { approveWorkflow } from '../operations/workflow-operations';
import { isApprovalContext, isGateResolved } from '@archon/workflows/schemas/workflow-run';
import type { ApprovalContext } from '@archon/workflows/schemas/workflow-run';
import {
  buildAiProfile,
  isLiteralSpec,
  isTierName,
  resolveModelSpec,
  resolveTierWithFallback,
  routePresetEffort,
  type ModelAliasPreset,
  type TierName,
} from '@archon/workflows/model-validation';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('orchestrator-agent');
  return cachedLog;
}

// ─── Constants ───────────────────────────────────────────────────────────────

/** Max assistant text chunks to keep in batch mode (oldest are dropped) */
const MAX_BATCH_ASSISTANT_CHUNKS = 20;
/** Max total chunks (assistant + tool) to keep in batch mode */
const MAX_BATCH_TOTAL_CHUNKS = 200;
function applyPresetToRequestOptions(
  provider: string,
  preset: ModelAliasPreset,
  options: SendQueryOptions
): void {
  if (preset.thinking !== undefined) {
    options.nodeConfig = { ...(options.nodeConfig ?? {}), thinking: preset.thinking };
  }

  if (preset.effort === undefined) return;

  const routed = routePresetEffort(provider, preset.effort);
  if (!routed) {
    // Cross-provider effort mismatch — warn instead of silently dropping.
    getLog().warn({ provider, effort: preset.effort }, 'orchestrator.preset_effort_unsupported');
    return;
  }
  if (routed.field === 'effort') {
    options.nodeConfig = { ...(options.nodeConfig ?? {}), effort: routed.value };
  } else {
    options.assistantConfig = {
      ...(options.assistantConfig ?? {}),
      modelReasoningEffort: routed.value,
    };
  }
}

interface ResolvedModelRequest {
  provider: string;
  model: string | undefined;
  preset?: ModelAliasPreset;
  /** When `modelRef` was a tier: which tier in the fallback chain matched. */
  matchedTier?: TierName;
}

function resolveModelRequest(
  aiProfile: ReturnType<typeof buildAiProfile>,
  modelRef: string,
  fallbackProvider: string
): ResolvedModelRequest {
  if (isTierName(modelRef)) {
    const { preset, matchedTier } = resolveTierWithFallback(aiProfile, modelRef);
    return { provider: preset.provider, model: preset.model, preset, matchedTier };
  }
  const spec = resolveModelSpec(aiProfile, modelRef);
  if (isLiteralSpec(spec)) {
    return { provider: fallbackProvider, model: spec.literal };
  }
  return { provider: spec.provider, model: spec.model, preset: spec };
}

/**
 * Resolve the model request for the MAIN chat turn (#1998).
 *
 * Model precedence (chat call-site only — workflows keep resolving `large`):
 *   1. per-user `default_model` — applied only when the user's
 *      `default_provider` matches the effective provider (a stale pin must
 *      never ride a different provider). Routed through resolveModelRequest so
 *      `@alias` and tier refs keep working; an unresolvable ref (e.g. deleted
 *      alias) degrades to the tier path with a warning instead of failing chat.
 *   2. tier `large` from CONFIGURED tiers (user > repo > global).
 *   3. install `assistants.<p>.model` — outranks the BUILT-IN tier default
 *      only, never a configured tier ('inherit' means "SDK default", skip).
 *   4. built-in tier default.
 *
 * Title generation is NOT routed through this — it keeps the `small` tier.
 * With no user prefs and no `assistants.<p>.model`, this reduces byte-for-byte
 * to the previous `resolveModelRequest(aiProfile, 'large', provider)` call.
 * Exported for tests.
 */
export function resolveChatModelRequest(
  aiProfile: ReturnType<typeof buildAiProfile>,
  configuredProviderKey: string,
  userAiPrefs: UserAiPrefs,
  config: Pick<MergedConfig, 'assistants' | 'tiers'>
): ResolvedModelRequest {
  if (
    userAiPrefs.defaultModel !== undefined &&
    userAiPrefs.defaultProvider === configuredProviderKey
  ) {
    try {
      return resolveModelRequest(aiProfile, userAiPrefs.defaultModel, configuredProviderKey);
    } catch (err) {
      getLog().warn(
        { err: err as Error, defaultModel: userAiPrefs.defaultModel },
        'orchestrator.user_default_model_invalid'
      );
    }
  }
  const request = resolveModelRequest(aiProfile, 'large', configuredProviderKey);
  if (request.matchedTier === undefined) return request;

  const tierConfigured =
    config.tiers?.[request.matchedTier] !== undefined ||
    userAiPrefs.tiers?.[request.matchedTier] !== undefined;
  if (tierConfigured) return request;

  const installModel = config.assistants[request.provider]?.model;
  if (typeof installModel === 'string' && installModel !== '' && installModel !== 'inherit') {
    return { ...request, model: installModel };
  }
  return request;
}

/** A resolved title-generation request: which provider to call, with fully resolved options. */
export interface TitleRequest {
  provider: string;
  options: SendQueryOptions;
}

/**
 * Resolve provider + request options for conversation-title generation (#1855).
 *
 * Server entry points that fire title generation outside a full chat turn
 * (create-with-message, web workflow run) resolve the `small` tier here —
 * config tiers plus per-user prefs when a userId is available — instead of
 * letting the provider fall through to its raw config-default model, which
 * the active account may not support (e.g. `gpt-5.3-codex` on ChatGPT-plan
 * Codex accounts). Mirrors the chat path's title resolution in
 * `handleMessage` (#1873), which keeps its own inline resolution to reuse
 * the already-loaded config and profile.
 *
 * NEVER THROWS — degrades to `{ provider: fallbackProvider, options: {} }`
 * (the legacy behavior) so fire-and-forget callers stay safe.
 */
export async function resolveTitleRequest(
  fallbackProvider: string,
  userId?: string
): Promise<TitleRequest> {
  try {
    const config = await loadConfig();
    const userAiPrefs = userId ? await resolveUserAiPrefsForChat(userId) : {};
    let configuredProviderKey = userAiPrefs.defaultProvider ?? fallbackProvider;
    let aiProfile: ReturnType<typeof buildAiProfile>;
    try {
      aiProfile = buildAiProfile(configuredProviderKey, {
        repoTiers: config.tiers,
        repoAliases: config.aliases,
        userTiers: userAiPrefs.tiers,
        userAliases: userAiPrefs.aliases,
      });
    } catch (profileErr) {
      // Structurally invalid STORED prefs must not break title generation —
      // degrade to config-only (mirrors the chat path in handleMessage).
      getLog().warn({ err: profileErr as Error, userId }, 'orchestrator.title_prefs_invalid');
      configuredProviderKey = fallbackProvider;
      aiProfile = buildAiProfile(configuredProviderKey, {
        repoTiers: config.tiers,
        repoAliases: config.aliases,
      });
    }
    const titleRequest = resolveModelRequest(aiProfile, 'small', configuredProviderKey);
    const options: SendQueryOptions = {
      model: titleRequest.model,
      assistantConfig: { ...(config.assistants[titleRequest.provider] ?? {}) },
    };
    if (titleRequest.preset) {
      applyPresetToRequestOptions(titleRequest.provider, titleRequest.preset, options);
    }
    return { provider: titleRequest.provider, options };
  } catch (err) {
    getLog().warn(
      { err: err as Error, fallbackProvider },
      'orchestrator.title_request_resolve_failed'
    );
    return { provider: fallbackProvider, options: {} };
  }
}

// ─── Types ──────────────────────────────────────────────────────────────────

export interface WorkflowInvocation {
  workflowName: string;
  projectName: string;
  remainingMessage: string;
  synthesizedPrompt?: string;
}

export interface ProjectRegistration {
  projectName: string;
  projectPath: string;
}

export interface OrchestratorCommands {
  workflowInvocation: WorkflowInvocation | null;
  projectRegistration: ProjectRegistration | null;
}

// ─── Command Parsing ────────────────────────────────────────────────────────

// Prefix patterns: fire as soon as the command keyword is seen.
const INVOKE_WORKFLOW_PREFIX_RE = /^\/invoke-workflow\s/m;
const REGISTER_PROJECT_PREFIX_RE = /^\/register-project\s/m;

// Full-command patterns: fire once all required tokens are present.
// These determine when accumulation can stop — further chunks cannot add
// required parse tokens and could corrupt already-captured ones.
//
// INVOKE_WORKFLOW_FULL_RE uses a test() object because the stop condition must account
// for the optional --prompt parameter:
//   - If --prompt "..." is present with a closing quote → fully parsed.
//   - If --prompt is started but not closed → keep accumulating for the closing quote.
//   - If no --prompt and the line is terminated (\n) → fully parsed (no more params).
//   - If no --prompt and EOS (no \n yet) → keep accumulating in case --prompt follows.
// A plain regex would fire as soon as --project <token> matched, dropping a --prompt
// that arrives in a later chunk and causing synthesizedPrompt to be lost.
const INVOKE_WORKFLOW_FULL_RE = {
  test(text: string): boolean {
    // Match the invoke-workflow line up to and including its terminator (\n) or end of string.
    const lineMatch = /^\/invoke-workflow[^\r\n]*(\r?\n|$)/m.exec(text);
    if (!lineMatch) return false;
    const line = lineMatch[0].replace(/(\r?\n)?$/, '');
    // Must have workflow name and --project token before we consider stopping.
    if (!/--project[\s=]+\S+/.test(line)) return false;
    const isEos = !lineMatch[0].endsWith('\n');
    // Check for optional --prompt parameter (system prompt specifies it follows --project).
    const promptKeywordMatch = /--prompt\s+/.exec(line);
    if (promptKeywordMatch) {
      const afterPrompt = line.slice(promptKeywordMatch.index + promptKeywordMatch[0].length);
      if (afterPrompt.startsWith('"')) {
        return /^"(?:[^"\\]|\\.)*"/.test(afterPrompt);
      }
      if (afterPrompt.startsWith("'")) {
        return /^'(?:[^'\\]|\\.)*'/.test(afterPrompt);
      }
      // Unquoted --prompt value: require line terminator.
      return !isEos;
    }
    // No --prompt yet: require line terminator so a --prompt in a later chunk is not missed.
    return !isEos;
  },
};
// REGISTER_PROJECT_FULL_RE uses a test() object instead of a plain regex because the
// stop condition must be conservative:
//   - Unquoted paths: require the line to be terminated (\n or end of stream preceded
//     by a non-whitespace char) so a space-containing path like "/home/user/my project"
//     is not declared complete after "my" arrives.
//   - Quoted paths: require the closing quote so we don't stop mid-path.
// This mirrors parseOrchestratorCommands' /^..\s+(.+)$/m pattern for the path capture.
const REGISTER_PROJECT_FULL_RE = {
  test(text: string): boolean {
    // Match the register-project line up to and including its terminator (\n) or end of string.
    const lineMatch = /^\/register-project[^\r\n]*(\r?\n|$)/m.exec(text);
    if (!lineMatch) return false;
    // Only treat end-of-string as a line terminator when at least one non-whitespace
    // character follows the project name — avoids matching a partial "/register-project "
    // line that was cut mid-word.
    const isEos = !lineMatch[0].endsWith('\n');
    const line = lineMatch[0].replace(/(\r?\n)?$/, '');
    const rest = line.replace(/^\/register-project\s+/, '');
    if (rest === line) return false; // no whitespace after command keyword
    const nameEnd = rest.search(/\s/);
    if (nameEnd === -1) return false; // no path token yet
    const projectPath = rest.slice(nameEnd).trimStart();
    if (!projectPath) return false;
    if (projectPath.startsWith('"')) {
      // Quoted path: require closing quote
      return /^"(?:[^"\\]|\\.)*"/.test(projectPath);
    }
    if (projectPath.startsWith("'")) {
      return /^'(?:[^'\\]|\\.)*'/.test(projectPath);
    }
    // Unquoted path: require line terminator so we don't freeze on a partial path with spaces
    return !isEos;
  },
};

/**
 * Strip markdown bold/italic decorators from slash-command lines.
 * Pi and other models occasionally emit **\/register-project ...** or
 * *\/invoke-workflow ...* instead of a bare slash command. The leading
 * asterisks cause both prefix and full-command regexes to miss the line.
 * Only lines whose first non-asterisk character is '/' are affected.
 */
function normalizeCommandText(text: string): string {
  return text.replace(/^\s*\*+(\/[^\n]*?)\**\s*$/gm, '$1');
}

/** Returns true once accumulated text contains a complete orchestrator command. */
function isCommandFullyParsed(accumulated: string): boolean {
  const normalized = normalizeCommandText(accumulated);
  return INVOKE_WORKFLOW_FULL_RE.test(normalized) || REGISTER_PROJECT_FULL_RE.test(normalized);
}

/**
 * Resolve the env-only per-user AI-provider credential bag for a direct-chat
 * turn (Phase 2). Drops deliveries that require file writes (Codex
 * `CODEX_HOME/auth.json` for the ChatGPT subscription path) because chat has
 * no per-call scratch directory — those rely on the workflow inject path that
 * provides an `artifactsDir`.
 *
 * NEVER THROWS — returns `{}` on any failure so the chat turn falls back to
 * whatever process-global env was already in place.
 */
async function resolveUserProviderEnvForChat(userId: string): Promise<Record<string, string>> {
  try {
    const creds = await listDecryptedUserProviderCredentials(userId);
    const env: Record<string, string> = {};
    for (const { provider, cred } of creds) {
      try {
        // artifactsDir intentionally empty: chat doesn't host file deliveries.
        const result = deliverCredential(provider, cred, { artifactsDir: '' });
        if (!result.files?.length) Object.assign(env, result.env);
      } catch (err) {
        getLog().error(
          { err: err as Error, userId, provider },
          'orchestrator.provider_creds_deliver_failed'
        );
      }
    }
    return env;
  } catch (err) {
    getLog().warn({ err: err as Error, userId }, 'orchestrator.user_provider_env_resolve_failed');
    return {};
  }
}

/**
 * Conversations (DB ids) already nudged about a tier fallback. Process-lifetime
 * memory is intentional and sufficient: the nudge is a discovery aid, not
 * state — a server restart re-nudging once per conversation is acceptable.
 */
const tierFallbackNudgedConversations = new Set<string>();

/**
 * Resolve the user's personal AI prefs (tiers / aliases / default assistant)
 * for a direct-chat turn (Phase 3). Folded into `buildAiProfile` as the
 * highest-precedence layer.
 *
 * NEVER THROWS — returns `{}` on any failure so model resolution falls back
 * to install-wide config exactly as before.
 */
async function resolveUserAiPrefsForChat(userId: string): Promise<UserAiPrefs> {
  try {
    return await getUserAiPrefs(userId);
  } catch (err) {
    getLog().warn({ err: err as Error, userId }, 'orchestrator.user_ai_prefs_resolve_failed');
    return {};
  }
}

/**
 * Find a codebase by exact name or by last path segment (e.g., "repo" matches "owner/repo").
 * Case-insensitive. Used in both the parse phase and the dispatch phase.
 */
function findCodebaseByName(
  codebases: readonly Codebase[],
  projectName: string
): Codebase | undefined {
  const projectLower = projectName.toLowerCase();
  return codebases.find(c => {
    const nameLower = c.name.toLowerCase();
    return nameLower === projectLower || nameLower.endsWith(`/${projectLower}`);
  });
}

/**
 * Resolve a codebase by name using 4-tier fuzzy matching.
 * Tiers: exact → case-insensitive → prefix → substring.
 * Returns undefined if not found; throws on ambiguity within a tier.
 *
 * Mirrors `resolveWorkflowName` (packages/workflows/src/router.ts) but uses
 * prefix instead of suffix for tier 3 — project names don't follow the
 * `archon-X` suffix convention workflows use.
 */
function resolveCodebaseName(name: string, codebases: readonly Codebase[]): Codebase | undefined {
  const exact = codebases.find(c => c.name === name);
  if (exact) return exact;

  const lowerName = name.toLowerCase();

  function checkTier(matches: readonly Codebase[], logEvent: string): Codebase | undefined {
    if (matches.length === 1) {
      getLog().debug({ requested: name, matched: matches[0].name }, logEvent);
      return matches[0];
    }
    if (matches.length > 1) {
      const candidates = matches.map(c => `  - ${c.name}`).join('\n');
      throw new Error(`Ambiguous project name '${name}'. Did you mean:\n${candidates}`);
    }
    return undefined;
  }

  return (
    checkTier(
      codebases.filter(c => c.name.toLowerCase() === lowerName),
      'project.set_resolve_case_insensitive_match'
    ) ??
    checkTier(
      codebases.filter(c => c.name.toLowerCase().startsWith(lowerName)),
      'project.set_resolve_prefix_match'
    ) ??
    checkTier(
      codebases.filter(c => c.name.toLowerCase().includes(lowerName)),
      'project.set_resolve_substring_match'
    )
  );
}

/**
 * Parse orchestrator commands from AI response text.
 * Scans for /invoke-workflow and /register-project patterns.
 */
export function parseOrchestratorCommands(
  response: string,
  codebases: readonly Codebase[],
  workflows: readonly WorkflowDefinition[]
): OrchestratorCommands {
  const result: OrchestratorCommands = {
    workflowInvocation: null,
    projectRegistration: null,
  };

  // Strip markdown bold/italic decorators from slash command lines before matching.
  // Pi models occasionally emit **\/register-project ...** or **\/invoke-workflow ...**.
  const normalizedResponse = normalizeCommandText(response);

  // Parse /invoke-workflow {name} --project {project-name}
  // Use (\S+) for project name to avoid capturing trailing text on the same line
  // (e.g., when AI appends tool call indicators or continues text after the command).
  // --project MUST appear before --prompt; this order is specified in the system prompt
  // template. Commands with --prompt before --project will not match.
  const invokePattern = /^\/invoke-workflow\s+(\S+)\s+--project[\s=]+(\S+)/m;
  const invokeMatch = invokePattern.exec(normalizedResponse);
  if (invokeMatch) {
    const workflowName = invokeMatch[1].trim();
    const projectName = invokeMatch[2].trim();

    // Validate workflow exists
    const workflow = findWorkflow(workflowName, [...workflows]);
    if (workflow) {
      // Validate project exists (case-insensitive, supports partial name matching)
      // e.g., "Archon" matches "coleam00/Archon"
      const matchedCodebase = findCodebaseByName(codebases, projectName);
      if (matchedCodebase) {
        // Extract message before the command
        const commandIndex = normalizedResponse.indexOf(invokeMatch[0]);
        const remainingMessage = normalizedResponse.slice(0, commandIndex).trim();

        // Extract optional --prompt "..." parameter (double or single quotes)
        const commandText = normalizedResponse.slice(commandIndex);
        const promptPattern = /--prompt\s+(?:"([^"]+)"|'([^']+)')/;
        const promptMatch = promptPattern.exec(commandText);
        const rawPrompt = (promptMatch?.[1] ?? promptMatch?.[2])?.trim();
        const synthesizedPrompt = rawPrompt || undefined;

        if (promptMatch && !synthesizedPrompt) {
          getLog().warn({ workflowName, projectName }, 'synthesized_prompt_empty_discarded');
        }

        result.workflowInvocation = {
          workflowName: workflow.name,
          projectName: matchedCodebase.name,
          remainingMessage,
          synthesizedPrompt,
        };
      }
    }
  }

  // Parse /register-project {name} {path}
  const registerPattern = /^\/register-project\s+(\S+)\s+(.+)$/m;
  const registerMatch = registerPattern.exec(normalizedResponse);
  if (registerMatch) {
    result.projectRegistration = {
      projectName: registerMatch[1].trim(),
      projectPath: registerMatch[2].trim(),
    };
  }

  return result;
}

// ─── Batch Mode Helpers ─────────────────────────────────────────────────────

/**
 * Filter emoji tool indicators from Claude Code SDK responses.
 * These prefixed sections (🔧, 💭, 📝, etc.) are useful for streaming UIs
 * but garble batch-mode text output on platforms like Slack/GitHub/CLI.
 */
function filterToolIndicators(assistantMessages: string[]): string {
  if (assistantMessages.length === 0) return '';

  const allMessages = assistantMessages.join('\n\n---\n\n');
  const sections = allMessages.split('\n\n');

  // Tool indicators from Claude Code SDK responses:
  // 🔧 (U+1F527) - tool usage, 💭 (U+1F4AD) - thinking, 📝 (U+1F4DD) - writing,
  // ✏️ (U+270F+FE0F) - editing, 🗑️ (U+1F5D1+FE0F) - deleting,
  // 📂 (U+1F4C2) - folder, 🔍 (U+1F50D) - search
  const toolIndicatorRegex =
    /^(?:\u{1F527}|\u{1F4AD}|\u{1F4DD}|\u{270F}\u{FE0F}|\u{1F5D1}\u{FE0F}|\u{1F4C2}|\u{1F50D})/u;
  const cleanSections = sections.filter(section => {
    const trimmed = section.trim();
    return !toolIndicatorRegex.test(trimmed);
  });

  const finalMessage = cleanSections.join('\n\n').trim();

  // If we filtered everything out, fall back to all messages joined
  return finalMessage || allMessages;
}

// ─── Workflow Dispatch ──────────────────────────────────────────────────────

interface WorkflowDispatchOptions {
  force?: boolean;
  resumeRunId?: string;
  resumeRun?: WorkflowRun;
  /**
   * Keys the engine dropped from the workflow's YAML (#2213). Mirrored into the
   * conversation before the run starts — chat and the console are where most
   * runs are STARTED, so a warning that only reaches the CLI misses the moment
   * of consequence.
   *
   * Deliberately unset on every resume path: delivery happens at most ONCE, at
   * the run's original chat/console start. That is not the same as "the warning
   * already fired" — delivery lives only in `dispatchOrchestratorWorkflow`, so a
   * run started by `archon workflow run` (which warns on stderr instead) and
   * later resumed with `/workflow resume` in chat never produced a chat warning,
   * and neither did any run predating this feature. Resuming does not re-derive
   * one; the author's durable surfaces are `validate`, `list` and the console
   * picker.
   */
  parseWarnings?: readonly string[];
}

const FAILED_RUN_PROMPT_PREVIEW_MAX = 160;

function escapeWorkflowCommandArg(value: string): string {
  return value.replace(/[\\"`]/g, '\\$&');
}

function formatPriorRunPromptPreview(message: string | null): string {
  const normalized = (message ?? '').replace(/\s+/g, ' ').trim();
  if (!normalized) {
    return '(no message stored)';
  }
  if (normalized.length <= FAILED_RUN_PROMPT_PREVIEW_MAX) {
    return normalized;
  }
  return `${normalized.slice(0, FAILED_RUN_PROMPT_PREVIEW_MAX)}…`;
}

function buildFailedRunResumePrompt(
  workflowName: string,
  resumableRun: WorkflowRun,
  userMessage: string
): string {
  const escapedMessage = escapeWorkflowCommandArg(userMessage);
  const baseCommand = `/workflow run ${workflowName}`;
  const priorPreview = formatPriorRunPromptPreview(resumableRun.user_message);
  // This prompt fires for any non-paused resumable run — that includes a stale
  // 'running' orphan (started but never finished), not only 'failed' runs, so
  // the wording must track the actual status rather than hardcoding "failed".
  const stateLabel = resumableRun.status === 'running' ? 'interrupted' : resumableRun.status;

  return [
    '---',
    '',
    `Found a prior ${stateLabel} run of **${workflowName}** (run \`${resumableRun.id}\`).`,
    '',
    '**Run prompt was:**',
    '',
    `> ${priorPreview}`,
    '',
    '---',
    '',
    '**Choose how to proceed:**',
    '',
    '**1. Resume that run** (re-runs the prompt shown above, not your current message):',
    '```',
    `/workflow resume ${resumableRun.id}`,
    '```',
    '',
    '**2. Discard the failed run, then start fresh with your current message:**',
    '```',
    `/workflow abandon ${resumableRun.id}`,
    '```',
    'then re-run your command:',
    '```',
    `${baseCommand} "${escapedMessage}"`,
    '```',
    '',
    '**3. Start fresh with your current message, leave the failed run as-is** (skips the resume check):',
    '```',
    `${baseCommand} --force "${escapedMessage}"`,
    '```',
  ].join('\n');
}

/**
 * Dispatch a workflow after the orchestrator resolves a project.
 * Auto-attaches the project to the conversation, resolves isolation, and executes.
 *
 * TODO(#988): Move to operations/ once dispatchBackgroundWorkflow is extracted
 * from the orchestrator (currently coupled to SSE bridging infrastructure).
 */
interface WorkflowDispatchRuntime {
  codebaseBaseBranch: string | undefined;
  resolveChildIsolation: ReturnType<typeof createChildWorktreeResolver> | undefined;
  cwd: string;
}

async function dispatchOrchestratorWorkflow(
  platform: IPlatformAdapter,
  conversationId: string,
  conversation: Conversation,
  codebase: Codebase,
  workflow: WorkflowDefinition,
  userMessage: string,
  isolationHints?: HandleMessageContext['isolationHints'],
  userId?: string,
  /**
   * Discovery source of the workflow — telemetry only (bundled workflows
   * report their real name, custom ones report "custom"). Optional: callers
   * that don't have it readily in scope omit it and the run reports "custom".
   */
  source?: WorkflowSource,
  options?: WorkflowDispatchOptions
): Promise<void> {
  const runtime = await prepareWorkflowDispatchRuntime(
    platform,
    conversationId,
    conversation,
    codebase,
    workflow,
    isolationHints,
    userId,
    options
  );
  if (!runtime) return;

  await dispatchPreparedWorkflow({
    platform,
    conversationId,
    conversation,
    codebase,
    workflow,
    userMessage,
    isolationHints,
    userId,
    source,
    options,
    ...runtime,
  });
}

async function prepareWorkflowDispatchRuntime(
  platform: IPlatformAdapter,
  conversationId: string,
  conversation: Conversation,
  codebase: Codebase,
  workflow: WorkflowDefinition,
  isolationHints: HandleMessageContext['isolationHints'],
  userId: string | undefined,
  options: WorkflowDispatchOptions | undefined
): Promise<WorkflowDispatchRuntime | null> {
  const codebaseBaseBranch = codebase.default_branch?.trim() || undefined;
  const resolveChildIsolation = buildChildIsolationResolver(
    platform,
    codebase,
    codebaseBaseBranch,
    userId
  );

  if (!(await ensureWorkflowRequirements(platform, conversationId, workflow, userId))) return null;
  await sendWorkflowParseWarnings(platform, conversationId, workflow, options?.parseWarnings);
  await db.updateConversation(conversation.id, { codebase_id: codebase.id });

  const cwd = await resolveWorkflowDispatchCwd(
    platform,
    conversationId,
    conversation,
    codebase,
    workflow,
    isolationHints,
    userId
  );
  if (cwd === null) return null;

  return { codebaseBaseBranch, resolveChildIsolation, cwd };
}

function buildChildIsolationResolver(
  platform: IPlatformAdapter,
  codebase: Codebase,
  codebaseBaseBranch: string | undefined,
  userId: string | undefined
): ReturnType<typeof createChildWorktreeResolver> | undefined {
  if (codebase.kind === 'folder') return undefined;
  return createChildWorktreeResolver({
    codebaseId: codebase.id,
    codebaseName: codebase.name,
    canonicalRepoPath: codebase.default_cwd,
    baseBranch: codebaseBaseBranch,
    createdByPlatform: platform.getPlatformType(),
    createdByUserId: userId,
  });
}

async function ensureWorkflowRequirements(
  platform: IPlatformAdapter,
  conversationId: string,
  workflow: WorkflowDefinition,
  userId: string | undefined
): Promise<boolean> {
  if (!isPerUserGitHubEnabled() || !workflow.requires?.length) return true;

  const githubConnected = userId ? Boolean(await getDecryptedAccessToken(userId)) : false;
  try {
    assertWorkflowRequirementsMet(workflow, { githubConnected });
    return true;
  } catch (err) {
    if (!(err instanceof WorkflowRequirementError)) throw err;
    getLog().info(
      { workflowName: workflow.name, conversationId, userId, requirement: err.requirement },
      'workflow.requirement_unmet'
    );
    await platform.sendMessage(conversationId, err.message);
    return false;
  }
}

async function sendWorkflowParseWarnings(
  platform: IPlatformAdapter,
  conversationId: string,
  workflow: WorkflowDefinition,
  parseWarnings: readonly string[] | undefined
): Promise<void> {
  if (!parseWarnings?.length) return;

  const lines = parseWarnings.map(w => `- ${w}`).join('\n');
  try {
    await platform.sendMessage(
      conversationId,
      `⚠️ \`${workflow.name}\` declares keys the engine ignores:\n${lines}`
    );
  } catch (error) {
    getLog().warn(
      { err: toError(error), conversationId, workflowName: workflow.name },
      'workflow.parse_warning_delivery_failed'
    );
  }
}

async function resolveWorkflowDispatchCwd(
  platform: IPlatformAdapter,
  conversationId: string,
  conversation: Conversation,
  codebase: Codebase,
  workflow: WorkflowDefinition,
  isolationHints: HandleMessageContext['isolationHints'],
  userId: string | undefined
): Promise<string | null> {
  if (workflow.worktree?.enabled === false) {
    getLog().info(
      { workflowName: workflow.name, conversationId, codebaseId: codebase.id },
      'workflow.worktree_disabled_by_policy'
    );
    return codebase.default_cwd;
  }

  try {
    const result = await validateAndResolveIsolation(
      { ...conversation, codebase_id: codebase.id },
      codebase,
      platform,
      conversationId,
      isolationHints,
      false,
      userId
    );
    return result.cwd;
  } catch (error) {
    if (!(error instanceof IsolationBlockedError)) throw error;
    getLog().warn(
      {
        reason: error.reason,
        conversationId,
        codebaseId: codebase.id,
        workflowName: workflow.name,
      },
      'isolation_blocked'
    );
    return null;
  }
}

type PreparedWorkflowDispatch = {
  platform: IPlatformAdapter;
  conversationId: string;
  conversation: Conversation;
  codebase: Codebase;
  workflow: WorkflowDefinition;
  userMessage: string;
  isolationHints: HandleMessageContext['isolationHints'];
  userId?: string;
  source?: WorkflowSource;
  options?: WorkflowDispatchOptions;
} & WorkflowDispatchRuntime;

async function dispatchPreparedWorkflow(context: PreparedWorkflowDispatch): Promise<void> {
  const resumableRun = await findDispatchResumableRun(context);
  if (await stopForMissingResumePath(context)) return;

  if (resumableRun?.working_path) {
    await dispatchResumableWorkflow(context, resumableRun);
    return;
  }

  if (context.platform.getPlatformType() === 'web' && !context.workflow.interactive) {
    await dispatchFreshBackgroundWorkflow(context);
    return;
  }

  await dispatchFreshForegroundWorkflow(context, context.cwd);
}

async function findDispatchResumableRun(
  context: PreparedWorkflowDispatch
): Promise<WorkflowRun | null> {
  if (context.options?.force) return null;
  return (
    context.options?.resumeRun ??
    (await workflowDb.findResumableRunByParentConversation(
      context.workflow.name,
      context.conversation.id,
      context.codebase.id
    ))
  );
}

async function stopForMissingResumePath(context: PreparedWorkflowDispatch): Promise<boolean> {
  if (!context.options?.resumeRun || context.options.resumeRun.working_path) return false;
  getLog().warn(
    {
      runId: context.options.resumeRun.id,
      workflowName: context.workflow.name,
      platformType: context.platform.getPlatformType(),
    },
    'orchestrator.resume_missing_working_path'
  );
  await context.platform.sendMessage(
    context.conversationId,
    `Cannot resume ${context.options.resumeRun.id}: missing working path.`
  );
  return true;
}

async function dispatchResumableWorkflow(
  context: PreparedWorkflowDispatch,
  resumableRun: NonNullable<Awaited<ReturnType<typeof findDispatchResumableRun>>>
): Promise<void> {
  const workingPath = resumableRun.working_path;
  if (!workingPath) return;

  if (resumableRun.status !== 'paused' && resumableRun.id !== context.options?.resumeRunId) {
    getLog().info(
      {
        workflowName: context.workflow.name,
        resumableRunId: resumableRun.id,
        platformType: context.platform.getPlatformType(),
      },
      'orchestrator.failed_resume_user_prompted'
    );
    await context.platform.sendMessage(
      context.conversationId,
      buildFailedRunResumePrompt(context.workflow.name, resumableRun, context.userMessage)
    );
    return;
  }

  getLog().info(
    {
      workflowName: context.workflow.name,
      resumableRunId: resumableRun.id,
      workingPath: resumableRun.working_path,
      platformType: context.platform.getPlatformType(),
    },
    'orchestrator.foreground_resume_detected'
  );
  const deps = createWorkflowDeps();
  const prepared = await hydrateDispatchResumableRun(context, deps, resumableRun);
  if (prepared === 'lost-race') return;

  if (!prepared) {
    await context.platform.sendMessage(
      context.conversationId,
      `⚠️ Prior run for **${context.workflow.name}** had no completed nodes; starting fresh in the same worktree.`
    );
  }

  if (prepared) {
    await executeWorkflow(
      deps,
      context.platform,
      context.conversationId,
      workingPath,
      context.workflow,
      context.userMessage,
      context.conversation.id,
      { ...workflowExecutionOptions(context), ...prepared }
    );
    return;
  }

  await executeWorkflow(
    deps,
    context.platform,
    context.conversationId,
    workingPath,
    context.workflow,
    context.userMessage,
    context.conversation.id,
    workflowExecutionOptions(context)
  );
}

async function hydrateDispatchResumableRun(
  context: PreparedWorkflowDispatch,
  deps: ReturnType<typeof createWorkflowDeps>,
  resumableRun: NonNullable<Awaited<ReturnType<typeof findDispatchResumableRun>>>
): Promise<Awaited<ReturnType<typeof hydrateResumableRun>> | 'lost-race'> {
  try {
    return await hydrateResumableRun(deps, resumableRun);
  } catch (err) {
    if (!(err instanceof workflowDb.WorkflowNotResumableError)) throw err;
    getLog().info(
      { workflowName: context.workflow.name, runId: resumableRun.id, status: err.currentStatus },
      'orchestrator.resume_lost_race'
    );
    await context.platform.sendMessage(
      context.conversationId,
      `⚠️ **${context.workflow.name}** is already being resumed (status: ${err.currentStatus}). ` +
        'No action taken — follow the existing run for progress.'
    );
    return 'lost-race';
  }
}

async function dispatchFreshBackgroundWorkflow(context: PreparedWorkflowDispatch): Promise<void> {
  await dispatchBackgroundWorkflow(
    {
      platform: context.platform,
      conversationId: context.conversationId,
      cwd: context.cwd,
      originalMessage: context.userMessage,
      conversationDbId: context.conversation.id,
      codebaseId: context.codebase.id,
      availableWorkflows: [context.workflow],
      isolationHints: context.isolationHints,
      userId: context.userId,
      source: context.source,
      parseWarnings: context.options?.parseWarnings,
    },
    context.workflow
  );
}

async function dispatchFreshForegroundWorkflow(
  context: PreparedWorkflowDispatch,
  cwd: string
): Promise<void> {
  await executeWorkflow(
    createWorkflowDeps(),
    context.platform,
    context.conversationId,
    cwd,
    context.workflow,
    context.userMessage,
    context.conversation.id,
    workflowExecutionOptions(context)
  );
}

function workflowExecutionOptions(context: PreparedWorkflowDispatch): ExecuteWorkflowOptions {
  return {
    codebaseId: context.codebase.id,
    parentConversationId: context.conversation.id,
    userId: context.userId,
    source: context.source,
    parseWarnings: context.options?.parseWarnings,
    baseBranch: context.codebaseBaseBranch,
    resolveChildIsolation: context.resolveChildIsolation,
  };
}

// ─── Session Helpers ────────────────────────────────────────────────────────

async function tryPersistSessionId(
  sessionId: string,
  assistantSessionId: string | null
): Promise<void> {
  try {
    await sessionDb.updateSession(sessionId, assistantSessionId);
  } catch (error) {
    getLog().error(
      { err: error as Error, sessionId, persistedValue: assistantSessionId },
      'session_id_persist_failed'
    );
  }
}

// ─── Extracted Helpers ──────────────────────────────────────────────────────

/** Copy parent conversation's project context to child thread if missing */
async function inheritThreadContext(
  platform: IPlatformAdapter,
  conversation: Conversation,
  parentConversationId: string | undefined,
  conversationId: string
): Promise<Conversation> {
  if (!parentConversationId || conversation.codebase_id) return conversation;

  const parentConversation = await db.getConversationByPlatformId(
    platform.getPlatformType(),
    parentConversationId
  );
  if (!parentConversation?.codebase_id) return conversation;

  try {
    await db.updateConversation(conversation.id, {
      codebase_id: parentConversation.codebase_id,
      cwd: parentConversation.cwd,
    });
    const refreshed = await db.getOrCreateConversation(platform.getPlatformType(), conversationId);
    getLog().debug({ conversationId, parentConversationId }, 'thread_context_inherited');
    return refreshed;
  } catch (err) {
    if (err instanceof ConversationNotFoundError) {
      getLog().warn({ conversationId: conversation.id }, 'thread_inheritance_failed');
      return conversation;
    }
    throw err;
  }
}

interface DiscoverResult {
  workflows: WorkflowWithSource[];
  errors: readonly WorkflowLoadError[];
  syncResult?: WorkspaceSyncResult;
  syncError?: string;
  config?: MergedConfig;
  codebase?: Codebase | null;
  /** Remote name used for the workspace sync (undefined when no sync ran). */
  remote?: string;
}

/** Discover global + repo-specific workflows, merge by name (repo overrides global) */
async function discoverAllWorkflows(conversation: Conversation): Promise<DiscoverResult> {
  let workflows: WorkflowWithSource[] = [];
  const allErrors: WorkflowLoadError[] = [];
  let syncResult: WorkspaceSyncResult | undefined;
  let syncError: string | undefined;
  let config: MergedConfig | undefined;
  let codebase: Codebase | null | undefined;
  let remote: string | undefined;

  try {
    // Home-scoped workflows at ~/.archon/workflows/ are discovered automatically
    // by discoverWorkflowsWithConfig — no option needed.
    const result = await discoverWorkflowsWithConfig(getArchonWorkspacesPath(), loadConfig);
    workflows = [...result.workflows];
    allErrors.push(...result.errors);
  } catch (error) {
    const err = error as Error;
    getLog().warn({ err, errorType: err.constructor.name }, 'global_workflow_discovery_failed');
  }

  if (conversation.codebase_id) {
    try {
      codebase = await codebaseDb.getCodebase(conversation.codebase_id);
      if (codebase) {
        // Sync canonical source with remote before the AI reads codebase state.
        // This path must remain non-destructive: users and agents can write to source/.
        // Non-fatal: if fetch fails (network, no remote), proceed with local state.
        // Folder projects have no git repo to sync — skip entirely.
        if (codebase.kind === 'folder') {
          getLog().debug(
            { codebaseId: codebase.id, path: codebase.default_cwd },
            'workspace.sync_skipped_folder_project'
          );
        } else {
          try {
            // Resolve the git remote: explicit repo config wins, otherwise
            // auto-detect ('origin' if present, else the sole remote).
            const repoPath = toRepoPath(codebase.default_cwd);
            const repoConf = await loadRepoConfig(codebase.default_cwd);
            remote =
              repoConf.worktree?.remote?.trim() || (await getDefaultRemote(repoPath)) || undefined;
            syncResult = await syncWorkspace(
              repoPath,
              codebase.default_branch ? toBranchName(codebase.default_branch) : undefined,
              { remote }
            );
            getLog().debug(
              {
                codebaseId: codebase.id,
                repoPath: codebase.default_cwd,
                remote,
                ...syncResult,
              },
              'workspace.sync_completed'
            );
          } catch (err) {
            const error = err as Error;
            syncError = error.message;
            getLog().warn({ err: error, codebaseId: codebase.id }, 'workspace.sync_failed');
          }
        }
        const workflowCwd = conversation.cwd ?? codebase.default_cwd;
        await syncArchonToWorktree(workflowCwd);
        // Load config once for this codebase path; reuse below to avoid a second disk read
        const loadedConfig = await loadConfig(workflowCwd);
        config = loadedConfig;
        const repoResult = await discoverWorkflowsWithConfig(workflowCwd, () =>
          Promise.resolve(loadedConfig)
        );
        const workflowMap = new Map(workflows.map(w => [w.workflow.name, w]));
        for (const rw of repoResult.workflows) {
          workflowMap.set(rw.workflow.name, rw);
        }
        workflows = Array.from(workflowMap.values());
        allErrors.push(...repoResult.errors);
      }
    } catch (error) {
      getLog().warn({ err: error as Error }, 'repo_workflow_discovery_failed');
    }
  }

  return { workflows, errors: allErrors, syncResult, syncError, config, codebase, remote };
}

/** Build the user-facing prompt with message and optional contexts */
function buildFullPrompt(
  message: string,
  issueContext: string | undefined,
  threadContext: string | undefined,
  attachedFiles?: AttachedFile[],
  workflowContext?: string
): string {
  const contextSuffix = issueContext ? '\n\n---\n\n## Additional Context\n\n' + issueContext : '';

  const fileSuffix =
    attachedFiles && attachedFiles.length > 0
      ? '\n\n---\n\n## Attached Files\n\nThe user has uploaded the following files. Use your file reading tools (Read, View) to access them:\n\n' +
        attachedFiles
          .map(f => `- ${f.name} (${f.mimeType}, ${String(f.size)} bytes): ${f.path}`)
          .join('\n')
      : '';

  const workflowContextSuffix = workflowContext ? '\n\n---\n\n' + workflowContext : '';

  if (threadContext) {
    return (
      '## Thread Context (previous messages)\n\n' +
      threadContext +
      workflowContextSuffix +
      '\n\n---\n\n## Current Request\n\n' +
      message +
      contextSuffix +
      fileSuffix
    );
  }

  return (
    workflowContextSuffix + '\n\n---\n\n## User Message\n\n' + message + contextSuffix + fileSuffix
  );
}

// ─── Main Handler ───────────────────────────────────────────────────────────

/**
 * Handle a message through the orchestrator agent.
 * Single entry point for all platforms — routes slash commands deterministically,
 * and routes everything else through the AI orchestrator which knows all projects
 * and workflows upfront.
 */
export async function handleMessage(
  platform: IPlatformAdapter,
  conversationId: string,
  message: string,
  context?: HandleMessageContext
): Promise<void> {
  const {
    issueContext,
    threadContext,
    parentConversationId,
    isolationHints,
    attachedFiles,
    userId,
  } = context ?? {};
  try {
    getLog().debug({ conversationId, userId }, 'orchestrator_message_received');

    // 1. Get/create conversation and inherit thread context.
    // userId is recorded on the conversation row only on first creation —
    // first-user-wins. The row's user_id is provenance plus a fallback for
    // execution identity; each turn's prefs/credentials resolve from the
    // SENDER when the adapter supplied one (see executionUserId below).
    // Per-message attribution happens on workflow_runs.
    let conversation = await db.getOrCreateConversation(
      platform.getPlatformType(),
      conversationId,
      undefined,
      parentConversationId,
      userId
    );
    conversation = await inheritThreadContext(
      platform,
      conversation,
      parentConversationId,
      conversationId
    );

    if (
      await handlePreAiRouting({
        platform,
        conversationId,
        conversation,
        message,
        isolationHints,
        userId,
      })
    ) {
      return;
    }

    const discoveredCodebase = await handleAiBoundMessage({
      platform,
      conversationId,
      conversation,
      message,
      issueContext,
      threadContext,
      attachedFiles,
      isolationHints,
      userId,
    });

    // Direct-chat turns may have written to source/. If there is local-only state
    // (uncommitted edits, unpushed commits), surface a one-line reminder so the
    // user can push or commit + push before the next worktree creation or
    // re-clone reclaims that work. No-op when no codebase is attached.
    // Use the codebase already fetched by discoverAllWorkflows — no second DB call.
    if (discoveredCodebase) {
      try {
        await reportUnpushedWorkInSource(conversationId, discoveredCodebase, platform);
      } catch (err) {
        getLog().warn(
          { err: err as Error, conversationId, codebaseId: conversation.codebase_id },
          'orchestrator.post_message_reminder_failed'
        );
      }
    }

    getLog().debug({ conversationId }, 'orchestrator_message_completed');
  } catch (error) {
    const err = toError(error);
    getLog().error({ err, conversationId }, 'orchestrator_message_failed');
    const userMessage = classifyAndFormatError(err);
    try {
      await platform.sendMessage(conversationId, userMessage);
    } catch (sendError) {
      getLog().error({ err: toError(sendError), conversationId }, 'error_notification_failed');
    }
  }
}
interface PreAiRoutingContext {
  platform: IPlatformAdapter;
  conversationId: string;
  conversation: Conversation;
  message: string;
  isolationHints: HandleMessageContext['isolationHints'];
  userId?: string;
}

async function handlePreAiRouting(context: PreAiRoutingContext): Promise<boolean> {
  if (await handleNaturalLanguageApproval(context)) return true;
  return handleDeterministicCommand(context);
}

async function handleNaturalLanguageApproval(context: PreAiRoutingContext): Promise<boolean> {
  const { platform, conversationId, conversation, message } = context;
  if (message.startsWith('/')) return false;

  const pausedRun = await workflowDb.getPausedWorkflowRun(conversation.id);
  const pausedApprovalRaw = pausedRun?.metadata.approval;
  const gateAlreadyResolved =
    pausedApprovalRaw !== undefined &&
    isApprovalContext(pausedApprovalRaw) &&
    isGateResolved(pausedApprovalRaw);
  if (!pausedRun || gateAlreadyResolved) return false;

  const approvalRaw = pausedRun.metadata.approval;
  const hasValidApproval =
    approvalRaw != null &&
    typeof approvalRaw === 'object' &&
    'nodeId' in approvalRaw &&
    typeof (approvalRaw as Record<string, unknown>).nodeId === 'string';

  if (!hasValidApproval) {
    await platform.sendMessage(
      conversationId,
      'A workflow is paused but its approval context is missing. ' +
        `Use \`/workflow approve ${pausedRun.id}\` or \`/workflow reject ${pausedRun.id}\`.`
    );
    return true;
  }

  if (isGuardedWorkflowRun(pausedRun)) {
    await platform.sendMessage(
      conversationId,
      `This guarded run requires an explicit human decision. Resolve run ${pausedRun.id} using its operator approval controls or an explicit /workflow approve or /workflow reject command. This message did not approve the gate.`
    );
    return true;
  }

  await approveAndResumePausedRun({
    ...context,
    pausedRun,
    approval: approvalRaw as ApprovalContext,
  });
  return true;
}

async function approveAndResumePausedRun(
  context: PreAiRoutingContext & {
    pausedRun: Awaited<ReturnType<typeof workflowDb.getPausedWorkflowRun>> & object;
    approval: ApprovalContext;
  }
): Promise<void> {
  const {
    platform,
    conversationId,
    conversation,
    message,
    isolationHints,
    userId,
    pausedRun,
    approval,
  } = context;
  getLog().info(
    {
      conversationId,
      workflowRunId: pausedRun.id,
      nodeId: approval.nodeId,
      workflowName: pausedRun.workflow_name,
    },
    'orchestrator.natural_language_approval_started'
  );

  try {
    await approveWorkflow(pausedRun.id, message);
    await resumeApprovedWorkflowRun(
      platform,
      conversationId,
      conversation,
      pausedRun,
      isolationHints,
      userId
    );
    getLog().info(
      { conversationId, workflowRunId: pausedRun.id, workflowName: pausedRun.workflow_name },
      'orchestrator.natural_language_approval_completed'
    );
  } catch (error) {
    getLog().error(
      { err: error as Error, workflowRunId: pausedRun.id, conversationId },
      'orchestrator.natural_language_approval_failed'
    );
    await platform.sendMessage(
      conversationId,
      `Approval failed: ${(error as Error).message}. Try again or use \`/workflow approve ${pausedRun.id}\` explicitly.`
    );
  }
}

async function resumeApprovedWorkflowRun(
  platform: IPlatformAdapter,
  conversationId: string,
  conversation: Conversation,
  pausedRun: Awaited<ReturnType<typeof workflowDb.getPausedWorkflowRun>> & object,
  isolationHints: HandleMessageContext['isolationHints'],
  userId: string | undefined
): Promise<void> {
  const { workflows: discoveredWorkflows } = await discoverAllWorkflows(conversation);
  const allWorkflows: WorkflowDefinition[] = discoveredWorkflows.map(w => w.workflow);
  const workflow = findWorkflow(pausedRun.workflow_name, allWorkflows);
  const workflowSource = workflow
    ? discoveredWorkflows.find(w => w.workflow === workflow)?.source
    : undefined;
  if (!workflow) {
    await platform.sendMessage(
      conversationId,
      `Approved, but workflow \`${pausedRun.workflow_name}\` not found. ` +
        'The approval was recorded — use `/workflow list` to check available workflows.'
    );
    return;
  }

  const codebase = conversation.codebase_id
    ? await codebaseDb.getCodebase(conversation.codebase_id)
    : null;
  if (!codebase) {
    await platform.sendMessage(
      conversationId,
      'Approved, but no project is attached to this conversation. The approval was recorded — re-run the workflow to resume.'
    );
    return;
  }

  await platform.sendMessage(conversationId, `▶️ Resuming **${workflow.name}**...`);
  await dispatchOrchestratorWorkflow(
    platform,
    conversationId,
    conversation,
    codebase,
    workflow,
    pausedRun.user_message,
    isolationHints,
    userId,
    workflowSource,
    { resumeRunId: pausedRun.id, resumeRun: pausedRun }
  );
}

async function handleDeterministicCommand(context: PreAiRoutingContext): Promise<boolean> {
  const { platform, conversationId, conversation, message, isolationHints, userId } = context;
  if (!message.startsWith('/')) return false;

  const { command } = commandHandler.parseCommand(message);
  if (!isDeterministicCommand(command)) return false;

  getLog().debug({ command, conversationId }, 'deterministic_command');
  const handled = await handleProjectMutationCommand(
    command,
    message,
    conversation,
    platform,
    conversationId
  );
  if (handled) {
    await platform.sendMessage(conversationId, handled);
    return true;
  }

  const result = await commandHandler.handleCommand(conversation, message);
  await platform.sendMessage(conversationId, result.message);
  if (result.workflow) {
    await handleWorkflowRunCommand(
      platform,
      conversationId,
      conversation,
      result.workflow.definition,
      result.workflow.args ?? message,
      isolationHints,
      userId,
      {
        force: result.workflow.force,
        resumeRunId: result.workflow.resumeRunId,
        resumeRun: result.workflow.resumeRun,
        parseWarnings: result.workflow.parseWarnings,
      }
    );
  }
  return true;
}

const DETERMINISTIC_COMMANDS = new Set([
  'help',
  'status',
  'reset',
  'workflow',
  'register-project',
  'update-project',
  'remove-project',
  'setproject',
  'commands',
  'init',
  'worktree',
]);

function isDeterministicCommand(command: string): boolean {
  return DETERMINISTIC_COMMANDS.has(command);
}

async function handleProjectMutationCommand(
  command: string,
  message: string,
  conversation: Conversation,
  platform: IPlatformAdapter,
  conversationId: string
): Promise<string | null> {
  if (command === 'register-project')
    return handleRegisterProject(message, platform, conversationId);
  if (command === 'update-project') return handleUpdateProject(message);
  if (command === 'remove-project') return handleRemoveProject(message);
  if (command === 'setproject') return handleSetProject(message, conversation);
  return null;
}

interface AiBoundMessageContext {
  platform: IPlatformAdapter;
  conversationId: string;
  conversation: Conversation;
  message: string;
  issueContext?: string;
  threadContext?: string;
  attachedFiles?: HandleMessageContext['attachedFiles'];
  isolationHints: HandleMessageContext['isolationHints'];
  userId?: string;
}

async function handleAiBoundMessage(
  context: AiBoundMessageContext
): Promise<Codebase | null | undefined> {
  persistInboundUserMessage(context);
  const chatInput = await prepareChatInput(context);
  const chatExecution = await prepareChatExecution(context, chatInput);
  maybeStartTitleGeneration(context, chatInput, chatExecution);

  const aiClient = getAgentProvider(chatExecution.providerKey);
  getLog().debug(
    {
      assistantType: context.conversation.ai_assistant_type,
      resolvedAssistantType: chatExecution.providerKey,
    },
    'sending_to_ai'
  );

  attachManageRunTool(context, chatInput, chatExecution, chatExecution.requestOptions);
  await runChatMode(context, chatInput, chatExecution, aiClient);
  return chatInput.discoveredCodebase;
}

function persistInboundUserMessage(context: AiBoundMessageContext): void {
  const { platform, conversation, message, userId, conversationId } = context;
  if (isWebAdapter(platform)) return;

  messageDb.addMessage(conversation.id, 'user', message, undefined, userId).catch((e: unknown) => {
    const err = e instanceof Error ? e : new Error(String(e));
    getLog().warn(
      { err, errorType: err.constructor.name, conversationId },
      'orchestrator.user_message_persist_failed'
    );
  });
}

interface ChatInput {
  codebases: readonly Codebase[];
  workflowsWithSource: readonly WorkflowWithSource[];
  workflows: readonly WorkflowDefinition[];
  discoveredConfig?: MergedConfig;
  discoveredCodebase?: Codebase | null;
  fullPrompt: string;
  cwd: string;
  session: { id: string; assistant_session_id: string | null };
}

async function prepareChatInput(context: AiBoundMessageContext): Promise<ChatInput> {
  const { conversation, message, issueContext, threadContext, attachedFiles } = context;
  const codebases = await codebaseDb.listCodebases();
  const discovered = await discoverAllWorkflows(conversation);
  warnWorkflowDiscoveryErrors(discovered.errors);
  await emitWorkspaceSyncStatus(context, discovered);
  const workflowContext = await loadWorkflowContext(conversation.id, context.conversationId);
  const fullPrompt = buildFullPrompt(
    message,
    issueContext,
    threadContext,
    attachedFiles,
    workflowContext
  );
  const cwd = await resolveChatCwd(conversation, codebases);
  const session = await getOrCreateActiveSession(conversation);

  return {
    codebases,
    workflowsWithSource: discovered.workflows,
    workflows: discovered.workflows.map(ws => ws.workflow),
    discoveredConfig: discovered.config,
    discoveredCodebase: discovered.codebase,
    fullPrompt,
    cwd,
    session,
  };
}

function warnWorkflowDiscoveryErrors(errors: readonly WorkflowLoadError[]): void {
  if (errors.length === 0) return;
  getLog().warn({ errorCount: errors.length, errors }, 'workflow.discovery_errors_present');
}

async function emitWorkspaceSyncStatus(
  context: AiBoundMessageContext,
  discovered: DiscoverResult
): Promise<void> {
  const { platform, conversationId } = context;
  if (!platform.sendStructuredEvent) return;

  if (discovered.syncError) {
    await platform.sendStructuredEvent(conversationId, {
      type: 'system',
      content: 'Sync failed — using local state',
    });
    return;
  }
  if (discovered.syncResult?.state === 'diverged') {
    await platform.sendStructuredEvent(conversationId, {
      type: 'system',
      content: `Local source/ has diverged from ${discovered.remote ?? 'origin'}/${discovered.syncResult.branch} — manual merge or rebase needed`,
    });
    return;
  }
  if (discovered.syncResult?.state === 'in_sync' && discovered.syncResult.updated) {
    await platform.sendStructuredEvent(conversationId, {
      type: 'system',
      content: `Fast-forwarded to ${discovered.remote ?? 'origin'}/${discovered.syncResult.branch} — ${discovered.syncResult.previousHead} → ${discovered.syncResult.newHead}`,
    });
  }
}

async function loadWorkflowContext(
  conversationDbId: string,
  conversationId: string
): Promise<string | undefined> {
  try {
    const recentResultMessages = await messageDb.getRecentWorkflowResultMessages(
      conversationDbId,
      3
    );
    if (recentResultMessages.length === 0) return undefined;
    const workflowResults = recentResultMessages.map(msg =>
      workflowResultContextFromMessage(msg, conversationId)
    );
    return formatWorkflowContextSection(workflowResults);
  } catch (error) {
    getLog().warn(
      { err: error as Error, conversationId },
      'orchestrator.workflow_context_fetch_failed'
    );
    return undefined;
  }
}

function workflowResultContextFromMessage(
  msg: Awaited<ReturnType<typeof messageDb.getRecentWorkflowResultMessages>>[number],
  conversationId: string
): WorkflowResultContext {
  let workflowName = 'unknown';
  let runId = 'unknown';
  try {
    const parsed = typeof msg.metadata === 'string' ? JSON.parse(msg.metadata) : msg.metadata;
    const meta = parsed as { workflowResult?: { workflowName?: string; runId?: string } };
    workflowName = meta.workflowResult?.workflowName ?? 'unknown';
    runId = meta.workflowResult?.runId ?? 'unknown';
  } catch (metaErr) {
    getLog().warn(
      { err: metaErr as Error, conversationId, messageId: msg.id },
      'orchestrator.workflow_result_metadata_parse_failed'
    );
  }
  return { workflowName, runId, summary: msg.content };
}

async function resolveChatCwd(
  conversation: Conversation,
  codebases: readonly Codebase[]
): Promise<string> {
  const scopedCodebase =
    conversation.codebase_id !== null
      ? codebases.find(c => c.id === conversation.codebase_id)
      : undefined;
  if (scopedCodebase !== undefined) return conversation.cwd ?? scopedCodebase.default_cwd;
  if (conversation.codebase_id !== null) {
    getLog().warn(
      { codebaseId: conversation.codebase_id },
      'orchestrator.scoped_codebase_not_found'
    );
  }
  return ensureArchonWorkspacesPath();
}

async function getOrCreateActiveSession(
  conversation: Conversation
): Promise<{ id: string; assistant_session_id: string | null }> {
  await db.touchConversation(conversation.id);
  const session = await sessionDb.getActiveSession(conversation.id);
  if (session) return session;
  return sessionDb.transitionSession(conversation.id, 'first-message', {
    ai_assistant_type: conversation.ai_assistant_type,
  });
}

interface ChatExecution {
  providerKey: string;
  configuredProviderKey: string;
  aiProfile: ReturnType<typeof buildAiProfile>;
  config: MergedConfig;
  userAiPrefs: UserAiPrefs;
  effectiveEnv: Record<string, string>;
  scopedCaps: ReturnType<typeof getProviderCapabilities> | null;
  requestOptions: SendQueryOptions;
}

async function prepareChatExecution(
  context: AiBoundMessageContext,
  chatInput: ChatInput
): Promise<ChatExecution> {
  const config = chatInput.discoveredConfig ?? (await loadConfig());
  const executionUserId = resolveExecutionUserId(context);
  const userAiPrefs = executionUserId ? await resolveUserAiPrefsForChat(executionUserId) : {};
  const { configuredProviderKey, aiProfile } = buildChatAiProfile(
    context,
    config,
    userAiPrefs,
    executionUserId
  );
  const chatRequest = resolveChatModelRequest(aiProfile, configuredProviderKey, userAiPrefs, {
    assistants: config.assistants,
    tiers: config.tiers,
  });
  await maybeSendTierFallbackNudge(context, chatRequest);

  const providerKey = chatRequest.provider;
  const effectiveEnv = await resolveEffectiveChatEnv(context, config, executionUserId);
  warnUnsupportedEnvInjection(providerKey, effectiveEnv);
  const scopedCaps =
    context.conversation.codebase_id !== null ? getProviderCapabilities(providerKey) : null;
  const requestOptions = buildChatRequestOptions(
    context,
    chatInput,
    config,
    providerKey,
    chatRequest,
    effectiveEnv,
    scopedCaps
  );

  return {
    providerKey,
    configuredProviderKey,
    aiProfile,
    config,
    userAiPrefs,
    effectiveEnv,
    scopedCaps,
    requestOptions,
  };
}

function resolveExecutionUserId(context: AiBoundMessageContext): string | undefined {
  const executionUserId = context.userId ?? context.conversation.user_id ?? undefined;
  if (!context.userId && context.conversation.user_id && isPerUserProviderKeysEnabled()) {
    getLog().warn(
      { conversationId: context.conversationId, fallbackUserId: context.conversation.user_id },
      'orchestrator.execution_identity_creator_fallback'
    );
  }
  return executionUserId;
}

function buildChatAiProfile(
  context: AiBoundMessageContext,
  config: MergedConfig,
  userAiPrefs: UserAiPrefs,
  executionUserId: string | undefined
): { configuredProviderKey: string; aiProfile: ReturnType<typeof buildAiProfile> } {
  let configuredProviderKey = userAiPrefs.defaultProvider ?? context.conversation.ai_assistant_type;
  try {
    return {
      configuredProviderKey,
      aiProfile: buildAiProfile(configuredProviderKey, {
        repoTiers: config.tiers,
        repoAliases: config.aliases,
        userTiers: userAiPrefs.tiers,
        userAliases: userAiPrefs.aliases,
      }),
    };
  } catch (profileErr) {
    getLog().error(
      { err: profileErr as Error, userId: executionUserId },
      'orchestrator.user_ai_prefs_profile_invalid'
    );
    configuredProviderKey = context.conversation.ai_assistant_type;
    return {
      configuredProviderKey,
      aiProfile: buildAiProfile(configuredProviderKey, {
        repoTiers: config.tiers,
        repoAliases: config.aliases,
      }),
    };
  }
}

async function maybeSendTierFallbackNudge(
  context: AiBoundMessageContext,
  chatRequest: ReturnType<typeof resolveChatModelRequest>
): Promise<void> {
  if (
    chatRequest.matchedTier === undefined ||
    chatRequest.matchedTier === 'large' ||
    tierFallbackNudgedConversations.has(context.conversation.id)
  ) {
    return;
  }

  tierFallbackNudgedConversations.add(context.conversation.id);
  getLog().warn(
    {
      requestedTier: 'large',
      matchedTier: chatRequest.matchedTier,
      provider: chatRequest.provider,
      model: chatRequest.model,
    },
    'orchestrator.tier_fallback_nudge'
  );
  try {
    await context.platform.sendMessage(
      context.conversationId,
      `ℹ️ Model tier 'large' isn't configured — using the '${chatRequest.matchedTier}' preset ` +
        `(${chatRequest.provider}/${chatRequest.model ?? ''}). Set it in Settings → Model Tiers ` +
        'or `archon ai tier set large <provider> <model>`.'
    );
  } catch (nudgeErr) {
    getLog().warn(
      { err: nudgeErr as Error, conversationId: context.conversationId },
      'orchestrator.tier_fallback_nudge_delivery_failed'
    );
  }
}

async function resolveEffectiveChatEnv(
  context: AiBoundMessageContext,
  config: MergedConfig,
  executionUserId: string | undefined
): Promise<Record<string, string>> {
  const dbEnvVars = await loadCodebaseEnvVarsForChat(context.conversation);
  const userProviderEnv =
    isPerUserProviderKeysEnabled() && executionUserId
      ? await resolveUserProviderEnvForChat(executionUserId)
      : {};
  return { ...(config.envVars ?? {}), ...dbEnvVars, ...userProviderEnv };
}

async function loadCodebaseEnvVarsForChat(
  conversation: Conversation
): Promise<Record<string, string>> {
  if (!conversation.codebase_id) return {};
  try {
    return await getCodebaseEnvVars(conversation.codebase_id);
  } catch (error) {
    getLog().warn(
      { err: error as Error, codebaseId: conversation.codebase_id },
      'codebase_env_vars_load_failed'
    );
    return {};
  }
}

function warnUnsupportedEnvInjection(
  providerKey: string,
  effectiveEnv: Record<string, string>
): void {
  if (Object.keys(effectiveEnv).length === 0) return;
  const providerCaps = getProviderCapabilities(providerKey);
  if (providerCaps.envInjection) return;
  getLog().warn(
    { provider: providerKey, envVarCount: Object.keys(effectiveEnv).length },
    'orchestrator.unsupported_env_injection'
  );
}

function buildChatRequestOptions(
  context: AiBoundMessageContext,
  chatInput: ChatInput,
  config: MergedConfig,
  providerKey: string,
  chatRequest: ReturnType<typeof resolveChatModelRequest>,
  effectiveEnv: Record<string, string>,
  scopedCaps: ReturnType<typeof getProviderCapabilities> | null
): SendQueryOptions {
  let systemAppend = buildOrchestratorSystemAppend(
    context.conversation,
    chatInput.codebases,
    chatInput.workflows
  );
  if (scopedCaps !== null && !scopedCaps.nativeTools)
    systemAppend += `\n\n${buildRunManagementSection()}`;
  const systemPrompt =
    providerKey === 'claude'
      ? { type: 'preset' as const, preset: 'claude_code' as const, append: systemAppend }
      : systemAppend;
  const requestOptions: SendQueryOptions = {
    assistantConfig: { ...(config.assistants[providerKey] ?? {}) },
    env: Object.keys(effectiveEnv).length > 0 ? effectiveEnv : undefined,
    model: chatRequest.model,
    systemPrompt,
  };
  if (chatRequest.preset)
    applyPresetToRequestOptions(providerKey, chatRequest.preset, requestOptions);
  return requestOptions;
}

function maybeStartTitleGeneration(
  context: AiBoundMessageContext,
  chatInput: ChatInput,
  chatExecution: ChatExecution
): void {
  const { conversation, message } = context;
  if (conversation.title || message.startsWith('/')) return;

  const titleRequest = resolveModelRequest(
    chatExecution.aiProfile,
    'small',
    chatExecution.configuredProviderKey
  );
  const titleOptions: SendQueryOptions = {
    model: titleRequest.model,
    assistantConfig: { ...(chatExecution.config.assistants[titleRequest.provider] ?? {}) },
    env:
      Object.keys(chatExecution.effectiveEnv).length > 0 ? chatExecution.effectiveEnv : undefined,
  };
  if (titleRequest.preset) {
    applyPresetToRequestOptions(titleRequest.provider, titleRequest.preset, titleOptions);
  }
  void generateAndSetTitle(
    conversation.id,
    message,
    titleRequest.provider,
    chatInput.cwd,
    undefined,
    titleOptions.assistantConfig,
    titleOptions
  );
}

function attachManageRunTool(
  context: AiBoundMessageContext,
  chatInput: ChatInput,
  chatExecution: ChatExecution,
  requestOptions: SendQueryOptions
): void {
  const scopedCodebaseId = context.conversation.codebase_id;
  if (scopedCodebaseId === null || !chatExecution.scopedCaps?.nativeTools) return;

  requestOptions.nativeTools = [
    buildManageRunTool({
      codebaseId: scopedCodebaseId,
      startWorkflow: (workflowName, msg) =>
        startManagedWorkflow(context, chatInput, scopedCodebaseId, workflowName, msg),
    }),
  ];
}

async function startManagedWorkflow(
  context: AiBoundMessageContext,
  chatInput: ChatInput,
  scopedCodebaseId: string,
  workflowName: string,
  msg: string
): Promise<string> {
  let wf: WorkflowDefinition | undefined;
  try {
    wf = resolveWorkflowName(workflowName, chatInput.workflows);
  } catch (e: unknown) {
    return toError(e).message;
  }
  if (wf === undefined)
    return `No workflow named "${workflowName}". Available: ${chatInput.workflows.map(w => w.name).join(', ')}`;

  try {
    await dispatchBackgroundWorkflow(
      {
        platform: context.platform,
        conversationId: context.conversationId,
        cwd: chatInput.cwd,
        originalMessage: msg.length > 0 ? msg : `Run ${wf.name}`,
        conversationDbId: context.conversation.id,
        codebaseId: scopedCodebaseId,
        availableWorkflows: chatInput.workflows,
        userId: context.userId,
      },
      wf
    );
  } catch (e: unknown) {
    const err = toError(e);
    getLog().error(
      {
        err,
        workflow: wf.name,
        codebaseId: scopedCodebaseId,
        conversationId: context.conversationId,
      },
      'manage_run.start_failed'
    );
    return `Failed to start workflow "${wf.name}": ${err.message}`;
  }
  return `Started workflow "${wf.name}" in the background — it'll appear in the runs list and the workflow dock shortly.`;
}

async function runChatMode(
  context: AiBoundMessageContext,
  chatInput: ChatInput,
  chatExecution: ChatExecution,
  aiClient: ReturnType<typeof getAgentProvider>
): Promise<void> {
  const args = [
    context.platform,
    context.conversationId,
    context.message,
    chatInput.codebases,
    chatInput.workflowsWithSource,
    aiClient,
    chatInput.fullPrompt,
    chatInput.cwd,
    chatInput.session,
    context.isolationHints,
    context.conversation,
    context.issueContext,
    chatExecution.requestOptions,
    context.userId,
  ] as const;

  if (context.platform.getStreamingMode() === 'stream') {
    await handleStreamMode(...args);
  } else {
    await handleBatchMode(...args);
  }
}

// ─── Streaming Mode ─────────────────────────────────────────────────────────

/**
 * Stream mode: send text chunks immediately for real-time UX (web, Telegram stream).
 * If an orchestrator command is detected, retract streamed text and dispatch.
 */
interface AiResultInfo {
  cost?: number;
  tokens?: TokenUsage;
  stopReason?: string;
}

interface StreamModeState {
  allMessages: string[];
  newSessionId?: string;
  commandDetected: boolean;
  commandFullyParsed: boolean;
  lastResult?: AiResultInfo;
  failed: boolean;
}

async function handleStreamMode(
  platform: IPlatformAdapter,
  conversationId: string,
  originalMessage: string,
  codebases: readonly Codebase[],
  workflows: readonly WorkflowWithSource[],
  aiClient: ReturnType<typeof getAgentProvider>,
  fullPrompt: string,
  cwd: string,
  session: { id: string; assistant_session_id: string | null },
  isolationHints: HandleMessageContext['isolationHints'],
  conversation: Conversation,
  issueContext?: string,
  requestOptions?: SendQueryOptions,
  userId?: string
): Promise<void> {
  const turnStartedAt = Date.now();
  const state: StreamModeState = {
    allMessages: [],
    commandDetected: false,
    commandFullyParsed: false,
    failed: false,
  };

  for await (const msg of aiClient.sendQuery(
    fullPrompt,
    cwd,
    session.assistant_session_id ?? undefined,
    requestOptions
  )) {
    await handleStreamChunk({
      msg,
      state,
      platform,
      conversationId,
      session,
      aiClient,
      requestOptions,
      turnStartedAt,
    });
    if (state.failed) return;
  }

  await finishStreamMode({
    state,
    platform,
    conversationId,
    conversation,
    codebases,
    workflows,
    originalMessage,
    isolationHints,
    issueContext,
    userId,
    aiClient,
    requestOptions,
    turnStartedAt,
    session,
  });
}

interface StreamChunkContext {
  msg: MessageChunk;
  state: StreamModeState;
  platform: IPlatformAdapter;
  conversationId: string;
  session: { id: string; assistant_session_id: string | null };
  aiClient: ReturnType<typeof getAgentProvider>;
  requestOptions?: SendQueryOptions;
  turnStartedAt: number;
}

async function handleStreamChunk(context: StreamChunkContext): Promise<void> {
  const { msg } = context;
  if (msg.type === 'assistant' && msg.content) {
    await handleStreamAssistantChunk(context, msg.content);
    return;
  }
  if (msg.type === 'tool' && msg.toolName) {
    await handleStreamToolChunk(context, msg);
    return;
  }
  if (msg.type === 'tool_result' && msg.toolName) {
    await handleStreamToolResultChunk(context, msg);
    return;
  }
  if (msg.type === 'result') await handleAiResultChunk(context, msg);
}

async function handleStreamAssistantChunk(
  context: StreamChunkContext,
  content: string
): Promise<void> {
  const { state, platform, conversationId } = context;
  if (!state.commandFullyParsed) state.allMessages.push(content);

  if (!state.commandDetected) {
    const detected = updateCommandDetection(state, state.allMessages.join(''));
    if (!detected) await platform.sendMessage(conversationId, content);
    return;
  }

  if (!state.commandFullyParsed) updateCommandDetection(state, state.allMessages.join(''));
}

async function handleStreamToolChunk(
  context: StreamChunkContext,
  msg: Extract<MessageChunk, { type: 'tool' }>
): Promise<void> {
  const { state, platform, conversationId } = context;
  if (state.commandDetected) return;

  await platform.sendMessage(conversationId, formatToolCall(msg.toolName, msg.toolInput), {
    category: 'tool_call_formatted',
  });
  if (platform.sendStructuredEvent) await platform.sendStructuredEvent(conversationId, msg);
}

async function handleStreamToolResultChunk(
  context: StreamChunkContext,
  msg: Extract<MessageChunk, { type: 'tool_result' }>
): Promise<void> {
  const { state, platform, conversationId } = context;
  if (!state.commandDetected && platform.sendStructuredEvent) {
    await platform.sendStructuredEvent(conversationId, msg);
  }
}

function updateCommandDetection(
  state: StreamModeState | BatchModeState,
  accumulated: string
): boolean {
  const normalizedAccumulated = normalizeCommandText(accumulated);
  const hasPrefix =
    INVOKE_WORKFLOW_PREFIX_RE.test(normalizedAccumulated) ||
    REGISTER_PROJECT_PREFIX_RE.test(normalizedAccumulated);
  if (!hasPrefix) return false;

  state.commandDetected = true;
  if (isCommandFullyParsed(accumulated)) state.commandFullyParsed = true;
  return true;
}

async function handleAiResultChunk(
  context: StreamChunkContext | BatchChunkContext,
  msg: Extract<MessageChunk, { type: 'result' }>
): Promise<void> {
  const { state, conversationId, session, platform } = context;
  if (msg.isError && msg.errorSubtype === 'error_during_execution') {
    getLog().warn(
      {
        conversationId,
        errorSubtype: msg.errorSubtype,
        staleSessionId: msg.sessionId,
        errors: msg.errors,
        stopReason: msg.stopReason,
      },
      'clearing_stale_session_id'
    );
    await tryPersistSessionId(session.id, null);
    state.newSessionId = undefined;
  } else if (msg.sessionId) {
    state.newSessionId = msg.sessionId;
  }

  if (msg.isError && msg.errorSubtype !== 'success') {
    await handleAiResultError(context, msg);
    return;
  }

  if (!state.commandDetected && platform.sendStructuredEvent) {
    await platform.sendStructuredEvent(conversationId, msg);
  }
  state.lastResult = { cost: msg.cost, tokens: msg.tokens, stopReason: msg.stopReason };
}

async function handleAiResultError(
  context: StreamChunkContext | BatchChunkContext,
  msg: Extract<MessageChunk, { type: 'result' }>
): Promise<void> {
  const { state, conversationId, platform, aiClient, requestOptions, turnStartedAt } = context;
  getLog().warn(
    {
      conversationId,
      errorSubtype: msg.errorSubtype,
      errors: msg.errors,
      stopReason: msg.stopReason,
    },
    'ai_result_error'
  );
  const errorDetail = [msg.errorSubtype, ...(msg.errors ?? [])].filter(Boolean).join(': ');
  await platform.sendMessage(
    conversationId,
    classifyAndFormatError(new Error(errorDetail || 'AI result error'))
  );
  if (state.newSessionId) await tryPersistSessionId(context.session.id, state.newSessionId);
  captureChatTurn({
    platform: platform.getPlatformType(),
    provider: aiClient.getType(),
    model: requestOptions?.model,
    durationMs: Date.now() - turnStartedAt,
    outcome: 'failed',
  });
  state.failed = true;
}

interface FinishStreamContext {
  state: StreamModeState;
  platform: IPlatformAdapter;
  conversationId: string;
  conversation: Conversation;
  codebases: readonly Codebase[];
  workflows: readonly WorkflowWithSource[];
  originalMessage: string;
  isolationHints: HandleMessageContext['isolationHints'];
  issueContext?: string;
  userId?: string;
  aiClient: ReturnType<typeof getAgentProvider>;
  requestOptions?: SendQueryOptions;
  turnStartedAt: number;
  session: { id: string; assistant_session_id: string | null };
}

async function finishStreamMode(context: FinishStreamContext): Promise<void> {
  const { state, session, conversationId } = context;
  if (state.newSessionId) await tryPersistSessionId(session.id, state.newSessionId);
  if (state.allMessages.length === 0) {
    getLog().debug({ conversationId }, 'no_ai_response');
    return;
  }

  const fullResponse = state.allMessages.join('');
  if (await dispatchParsedStreamCommand(context, fullResponse)) return;

  persistAssistantMessageIfNeeded(
    context.platform,
    context.conversation,
    conversationId,
    fullResponse
  );
  await maybeSendResultFooter(context.platform, conversationId, state.lastResult);
  captureCompletedChatTurn(context, state.lastResult);
}

async function dispatchParsedStreamCommand(
  context: FinishStreamContext,
  fullResponse: string
): Promise<boolean> {
  const commands = parseOrchestratorCommands(
    fullResponse,
    context.codebases,
    context.workflows.map(ws => ws.workflow)
  );
  if (commands.workflowInvocation) {
    if (context.platform.emitRetract) await context.platform.emitRetract(context.conversationId);
    await handleWorkflowInvocationResult(
      context.platform,
      context.conversationId,
      context.conversation,
      context.codebases,
      context.workflows,
      commands.workflowInvocation,
      context.originalMessage,
      context.isolationHints,
      context.issueContext,
      context.userId
    );
    return true;
  }
  if (!commands.projectRegistration) return false;

  if (context.platform.emitRetract) await context.platform.emitRetract(context.conversationId);
  await handleProjectRegistrationResult(
    context.platform,
    context.conversationId,
    fullResponse,
    commands.projectRegistration
  );
  return true;
}

function persistAssistantMessageIfNeeded(
  platform: IPlatformAdapter,
  conversation: Conversation,
  conversationId: string,
  content: string
): void {
  if (isWebAdapter(platform) || !content) return;
  messageDb.addMessage(conversation.id, 'assistant', content).catch((e: unknown) => {
    const err = e instanceof Error ? e : new Error(String(e));
    getLog().warn(
      { err, errorType: err.constructor.name, conversationId },
      'orchestrator.assistant_message_persist_failed'
    );
  });
}

function captureCompletedChatTurn(
  context: FinishStreamContext | FinishBatchContext,
  lastResult: AiResultInfo | undefined
): void {
  captureChatTurn({
    platform: context.platform.getPlatformType(),
    provider: context.aiClient.getType(),
    model: context.requestOptions?.model,
    durationMs: Date.now() - context.turnStartedAt,
    costUsd: lastResult?.cost,
    tokensIn: lastResult?.tokens?.input,
    tokensOut: lastResult?.tokens?.output,
    outcome: 'completed',
  });
}

// ─── Batch Mode ─────────────────────────────────────────────────────────────

/**
 * Batch mode: accumulate all chunks, filter tool indicators, send final clean summary.
 * Used by Slack, GitHub, Discord (batch), and CLI.
 */
interface BatchModeState extends StreamModeState {
  allChunks: { type: string; content: string }[];
  assistantMessages: string[];
  assistantChunksTruncated: boolean;
  totalChunksTruncated: boolean;
}

async function handleBatchMode(
  platform: IPlatformAdapter,
  conversationId: string,
  originalMessage: string,
  codebases: readonly Codebase[],
  workflows: readonly WorkflowWithSource[],
  aiClient: ReturnType<typeof getAgentProvider>,
  fullPrompt: string,
  cwd: string,
  session: { id: string; assistant_session_id: string | null },
  isolationHints: HandleMessageContext['isolationHints'],
  conversation: Conversation,
  issueContext?: string,
  requestOptions?: SendQueryOptions,
  userId?: string
): Promise<void> {
  const turnStartedAt = Date.now();
  const state: BatchModeState = {
    allMessages: [],
    allChunks: [],
    assistantMessages: [],
    assistantChunksTruncated: false,
    totalChunksTruncated: false,
    commandDetected: false,
    commandFullyParsed: false,
    failed: false,
  };

  for await (const msg of aiClient.sendQuery(
    fullPrompt,
    cwd,
    session.assistant_session_id ?? undefined,
    requestOptions
  )) {
    await handleBatchChunk({
      msg,
      state,
      platform,
      conversationId,
      session,
      aiClient,
      requestOptions,
      turnStartedAt,
    });
    if (state.failed) return;
  }

  await finishBatchMode({
    state,
    platform,
    conversationId,
    conversation,
    codebases,
    workflows,
    originalMessage,
    isolationHints,
    issueContext,
    userId,
    aiClient,
    requestOptions,
    turnStartedAt,
    session,
  });
}

interface BatchChunkContext extends Omit<StreamChunkContext, 'state'> {
  state: BatchModeState;
}

async function handleBatchChunk(context: BatchChunkContext): Promise<void> {
  const { msg, state } = context;
  if (msg.type === 'assistant' && msg.content) {
    handleBatchAssistantChunk(state, msg.content);
  } else if (msg.type === 'tool' && msg.toolName) {
    handleBatchToolChunk(state, msg);
  } else if (msg.type === 'result') {
    await handleAiResultChunk(context, msg);
  }
  enforceBatchTotalChunkCap(state);
}

function handleBatchAssistantChunk(state: BatchModeState, content: string): void {
  state.allChunks.push({ type: 'assistant', content });
  if (!state.commandFullyParsed) state.assistantMessages.push(content);

  enforceBatchAssistantChunkCap(state);
  if (!state.commandDetected || !state.commandFullyParsed) {
    updateCommandDetection(state, state.assistantMessages.join(''));
  }
}

function enforceBatchAssistantChunkCap(state: BatchModeState): void {
  if (
    state.commandDetected ||
    state.commandFullyParsed ||
    state.assistantMessages.length <= MAX_BATCH_ASSISTANT_CHUNKS
  ) {
    return;
  }
  state.assistantMessages.shift();
  state.assistantChunksTruncated = true;
}

function handleBatchToolChunk(
  state: BatchModeState,
  msg: Extract<MessageChunk, { type: 'tool' }>
): void {
  if (state.commandDetected) return;
  state.allChunks.push({ type: 'tool', content: formatToolCall(msg.toolName, msg.toolInput) });
  getLog().debug({ toolName: msg.toolName }, 'tool_call');
}

function enforceBatchTotalChunkCap(state: BatchModeState): void {
  if (state.allChunks.length <= MAX_BATCH_TOTAL_CHUNKS) return;
  state.allChunks.shift();
  state.totalChunksTruncated = true;
}

interface FinishBatchContext extends Omit<FinishStreamContext, 'state'> {
  state: BatchModeState;
}

async function finishBatchMode(context: FinishBatchContext): Promise<void> {
  const { state, session, conversationId } = context;
  if (state.newSessionId) await tryPersistSessionId(session.id, state.newSessionId);
  logBatchTruncation(state);
  getLog().debug(
    { totalChunks: state.allChunks.length, assistantMessages: state.assistantMessages.length },
    'batch_mode_chunks_received'
  );

  const finalMessage = filterToolIndicators(state.assistantMessages);
  if (!finalMessage) {
    getLog().debug({ conversationId }, 'no_ai_response');
    return;
  }

  if (await dispatchParsedBatchCommand(context, finalMessage)) return;

  getLog().debug({ messageLength: finalMessage.length }, 'sending_final_message');
  await context.platform.sendMessage(conversationId, finalMessage);
  persistAssistantMessageIfNeeded(
    context.platform,
    context.conversation,
    conversationId,
    finalMessage
  );
  await maybeSendResultFooter(context.platform, conversationId, state.lastResult);
  captureCompletedChatTurn(context, state.lastResult);
}

function logBatchTruncation(state: BatchModeState): void {
  if (!state.assistantChunksTruncated && !state.totalChunksTruncated) return;
  getLog().warn(
    {
      assistantChunksTruncated: state.assistantChunksTruncated,
      totalChunksTruncated: state.totalChunksTruncated,
      maxAssistantChunks: MAX_BATCH_ASSISTANT_CHUNKS,
      maxTotalChunks: MAX_BATCH_TOTAL_CHUNKS,
    },
    'batch_mode_chunks_truncated'
  );
}

async function dispatchParsedBatchCommand(
  context: FinishBatchContext,
  finalMessage: string
): Promise<boolean> {
  const commands = parseOrchestratorCommands(
    context.state.assistantMessages.join(''),
    context.codebases,
    context.workflows.map(ws => ws.workflow)
  );
  if (commands.workflowInvocation) {
    if (context.platform.emitRetract) await context.platform.emitRetract(context.conversationId);
    await handleWorkflowInvocationResult(
      context.platform,
      context.conversationId,
      context.conversation,
      context.codebases,
      context.workflows,
      commands.workflowInvocation,
      context.originalMessage,
      context.isolationHints,
      context.issueContext,
      context.userId
    );
    return true;
  }
  if (!commands.projectRegistration) return false;

  if (context.platform.emitRetract) await context.platform.emitRetract(context.conversationId);
  await handleProjectRegistrationResult(
    context.platform,
    context.conversationId,
    finalMessage,
    commands.projectRegistration
  );
  return true;
}

/**
 * Call the adapter's optional `sendResultFooter` hook with the final result
 * metadata from a direct-chat turn. Skips when the adapter doesn't implement
 * it, when there's no metadata to surface, or when the call itself fails —
 * cost footers are informational and must not block the conversation.
 */
async function maybeSendResultFooter(
  platform: IPlatformAdapter,
  conversationId: string,
  info: { cost?: number; tokens?: TokenUsage; stopReason?: string } | undefined
): Promise<void> {
  if (!info) return;
  if (info.cost === undefined && info.tokens === undefined) return;
  if (!platform.sendResultFooter) return;
  try {
    await platform.sendResultFooter(conversationId, info);
  } catch (error) {
    getLog().warn({ err: toError(error), conversationId }, 'orchestrator.result_footer_failed');
  }
}

// ─── Orchestrator Command Handlers ──────────────────────────────────────────

/**
 * Handle a parsed /invoke-workflow command from AI response.
 */
async function handleWorkflowInvocationResult(
  platform: IPlatformAdapter,
  conversationId: string,
  conversation: Conversation,
  codebases: readonly Codebase[],
  workflows: readonly WorkflowWithSource[],
  invocation: WorkflowInvocation,
  originalMessage: string,
  isolationHints: HandleMessageContext['isolationHints'],
  issueContext?: string,
  userId?: string
): Promise<void> {
  const { workflowName, projectName, remainingMessage } = invocation;

  // Send explanation text before dispatching
  if (remainingMessage) {
    await platform.sendMessage(conversationId, remainingMessage);
  }

  // Find the codebase and workflow (supports partial name matching)
  const codebase = findCodebaseByName(codebases, projectName);
  // Keep the discovery ENTRY, not just the definition: it carries the parse
  // warnings this path used to discard (#2213).
  const workflowEntry = workflows.find(ws => ws.workflow.name === workflowName);
  const workflow = findWorkflow(
    workflowName,
    workflows.map(ws => ws.workflow)
  );

  if (codebase && workflow) {
    const workflowPrompt = invocation.synthesizedPrompt ?? originalMessage;
    getLog().debug(
      {
        source: invocation.synthesizedPrompt ? 'synthesized' : 'original',
        promptLength: workflowPrompt.length,
        workflowName,
        hasIssueContext: !!issueContext,
        issueContextLength: issueContext?.length ?? 0,
      },
      'workflow_prompt_resolved'
    );
    await dispatchOrchestratorWorkflow(
      platform,
      conversationId,
      conversation,
      codebase,
      workflow,
      workflowPrompt,
      isolationHints,
      userId,
      workflowEntry?.source,
      { parseWarnings: workflowEntry?.parseWarnings }
    );
    return;
  }

  // Fallback: send error about missing project or workflow
  if (!codebase) {
    const projectList = codebases.map(c => `- ${c.name}`).join('\n');
    await platform.sendMessage(
      conversationId,
      `I couldn't find a project matching "${projectName}". Here are your registered projects:\n${projectList || '(none)'}\n\nPlease specify which project you'd like to use.`
    );
  } else if (!workflow) {
    getLog().warn({ workflowName, projectName }, 'workflow_not_found_in_dispatch');
    await platform.sendMessage(
      conversationId,
      `Workflow \`${workflowName}\` is not available. Use \`/workflow list\` to see available workflows.`
    );
  }
}

/**
 * Handle a parsed /register-project command from AI response.
 */
async function handleProjectRegistrationResult(
  platform: IPlatformAdapter,
  conversationId: string,
  fullResponse: string,
  registration: ProjectRegistration
): Promise<void> {
  const { projectName, projectPath } = registration;

  // Normalize before extraction so that Mode A's bold markers ('**') are
  // stripped from the command line; otherwise textBeforeReg would include a
  // trailing '**' when the model wrapped the command in markdown bold.
  const normalizedForExtraction = normalizeCommandText(fullResponse);
  // Match line-anchored to avoid landing on a prose mention of "/register-project".
  const regLineMatch = /^\/register-project\b/m.exec(normalizedForExtraction);
  if (!regLineMatch) {
    // Parsing already succeeded upstream from raw concatenated assistant chunks.
    // If extraction on filtered text fails, skip preamble extraction but still
    // execute registration to avoid silently dropping a valid command.
    getLog().warn({ conversationId }, 'orchestrator.extract_no_line_match');
  }
  const textBeforeReg = regLineMatch
    ? normalizedForExtraction.slice(0, regLineMatch.index).trim()
    : '';
  if (textBeforeReg) {
    await platform.sendMessage(conversationId, textBeforeReg);
  }

  // Register the project
  const regResult = await handleRegisterProject(
    `/register-project ${projectName} ${projectPath}`,
    platform,
    conversationId
  );
  await platform.sendMessage(conversationId, regResult);
}

// ─── Internal Helpers ───────────────────────────────────────────────────────

/**
 * Handle /register-project command.
 * Creates a codebase DB entry for a cloned project.
 */
async function handleRegisterProject(
  message: string,
  _platform: IPlatformAdapter,
  _conversationId: string
): Promise<string> {
  const { args } = commandHandler.parseCommand(message);
  if (args.length < 2) {
    return 'Usage: /register-project <name> <path>';
  }

  const [projectName, ...pathParts] = args;
  const projectPath = pathParts.join(' ');

  // Validate path exists
  if (!existsSync(projectPath)) {
    return `Path does not exist: ${projectPath}`;
  }

  // Canonicalize symlinks so the stored default_cwd matches what the CLI gate and
  // `archon doctor` look up (both resolve against process.cwd(), which resolves
  // symlinks — e.g. macOS /tmp → /private/tmp). Mirrors registerFolder; without
  // it a symlinked path registers under one path but is looked up under another.
  // Best-effort: existsSync already validated the path, so fall back to it if
  // realpath fails for a rare reason (permission on a parent, race).
  let canonicalPath = projectPath;
  try {
    canonicalPath = realpathSync(projectPath);
  } catch (err) {
    getLog().warn({ err: err as Error, projectPath }, 'project.register_realpath_failed');
  }

  // Check if codebase already exists with this name
  const existing = await codebaseDb.listCodebases();
  const alreadyExists = existing.find(c => c.name.toLowerCase() === projectName.toLowerCase());

  if (alreadyExists) {
    return `Project "${projectName}" is already registered (path: ${alreadyExists.default_cwd}).`;
  }

  // Use config default provider instead of hardcoding 'claude'
  const config = await loadConfig();

  // Detect whether the path is a git repository. Non-git paths (multi-repo roots
  // or plain ops folders) register as folder projects — run-in-place, no branch.
  // findRepoRoot returns null ONLY for a definitive "not a git repository"; it
  // throws for genuine failures (git missing, timeout, permission). Since `kind`
  // is persisted and mis-setting it to 'folder' permanently strips a real repo's
  // worktree/branch capability, we do NOT silently treat a throw as folder: log
  // loudly and tell the user so they can re-register after resolving the error.
  let repoRoot: string | null = null;
  let repoDetectFailed = false;
  try {
    repoRoot = await findRepoRoot(canonicalPath);
  } catch (err) {
    repoDetectFailed = true;
    getLog().warn(
      { err: err as Error, projectPath: canonicalPath },
      'project.register_repo_detect_failed'
    );
  }
  const kind: 'repo' | 'folder' = repoRoot ? 'repo' : 'folder';
  const detectedBranch = kind === 'repo' ? await detectCurrentGitBranch(canonicalPath) : null;
  const codebase = await codebaseDb.createCodebase({
    name: projectName,
    default_cwd: canonicalPath,
    default_branch: detectedBranch,
    ai_assistant_type: config.assistant,
    kind,
  });

  getLog().info(
    { name: projectName, path: canonicalPath, id: codebase.id, kind },
    'project.register_completed'
  );
  let kindNote = kind === 'folder' ? '\nKind: folder project (no git — runs in place)' : '';
  if (repoDetectFailed) {
    kindNote +=
      '\n⚠️ Could not determine git status (git error) — registered as a folder project. ' +
      'If this should be a git repo, resolve the error and re-register.';
  }
  return `Project "${projectName}" registered successfully!\nPath: ${canonicalPath}\nID: ${codebase.id}${kindNote}`;
}

async function detectCurrentGitBranch(projectPath: string): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync(
      'git',
      ['-C', projectPath, 'rev-parse', '--abbrev-ref', 'HEAD'],
      { timeout: 5000 }
    );
    const branch = stdout.trim();
    return branch && branch !== 'HEAD' ? branch : null;
  } catch {
    return null;
  }
}

/**
 * Handle /update-project command.
 * Updates the path for an existing registered project.
 */
async function handleUpdateProject(message: string): Promise<string> {
  const { args } = commandHandler.parseCommand(message);
  if (args.length < 2) {
    return 'Usage: /update-project <name> <new-path>';
  }

  const [projectName, ...pathParts] = args;
  const newPath = pathParts.join(' ');

  // Validate path exists
  if (!existsSync(newPath)) {
    return `Path does not exist: ${newPath}`;
  }

  // Find existing codebase by name
  const existing = await codebaseDb.listCodebases();
  const codebase = existing.find(c => c.name.toLowerCase() === projectName.toLowerCase());

  if (!codebase) {
    return `Project "${projectName}" not found. Use /register-project to create it.`;
  }

  try {
    await codebaseDb.updateCodebase(codebase.id, { default_cwd: newPath });
  } catch (err) {
    getLog().warn({ err: err as Error, codebaseId: codebase.id, newPath }, 'project.update_failed');
    // Row gone (deleted between the fetch above and the UPDATE) is the only
    // case where "removed" is the honest answer; anything else is an
    // operational DB failure and should say so instead of blaming data state.
    if (err instanceof codebaseDb.CodebaseNotFoundError) {
      return `Project "${projectName}" could not be updated — it appears to have been removed. Use /register-project to re-create it.`;
    }
    return `Project "${projectName}" could not be updated — database error. Please try again.`;
  }
  getLog().info(
    { name: projectName, oldPath: codebase.default_cwd, newPath, id: codebase.id },
    'project.update_completed'
  );
  return `Project "${projectName}" updated.\nOld path: ${codebase.default_cwd}\nNew path: ${newPath}`;
}

/**
 * Handle /remove-project command.
 * Deletes a registered project from the database.
 */
async function handleRemoveProject(message: string): Promise<string> {
  const { args } = commandHandler.parseCommand(message);
  if (args.length < 1) {
    return 'Usage: /remove-project <name>';
  }

  const projectName = args[0];

  // Find existing codebase by name
  const existing = await codebaseDb.listCodebases();
  const codebase = existing.find(c => c.name.toLowerCase() === projectName.toLowerCase());

  if (!codebase) {
    return `Project "${projectName}" not found.`;
  }

  await codebaseDb.deleteCodebase(codebase.id);
  getLog().info({ name: projectName, id: codebase.id }, 'project.remove_completed');
  return `Project "${projectName}" removed.\nPath was: ${codebase.default_cwd}`;
}

/**
 * Handle /setproject command. Four effects:
 * 1. Binds the conversation to the resolved codebase (writes `codebase_id`).
 * 2. Clears `cwd` — the project root remains codebase.default_cwd;
 *    conversation.cwd is only an explicit runtime override.
 * 3. Clears `isolation_env_id` — the old project's worktree no longer applies.
 * 4. Deactivates the active AI session ('project-changed'), so the next
 *    message starts fresh in the new project's context.
 * Uses 4-tier fuzzy name resolution (exact → case-insensitive → prefix →
 * substring) via resolveCodebaseName. Updates by the DB primary key
 * (conversation.id), never the platform conversation id.
 */
async function handleSetProject(message: string, conversation: Conversation): Promise<string> {
  const { args } = commandHandler.parseCommand(message);
  if (args.length < 1) {
    return 'Usage: /setproject <project-name>';
  }

  const projectName = args.join(' ');
  const codebases = await codebaseDb.listCodebases();

  let codebase: Codebase | undefined;
  try {
    codebase = resolveCodebaseName(projectName, codebases);
  } catch (err) {
    return (err as Error).message;
  }

  if (!codebase) {
    const available = codebases.map(c => c.name).join(', ');
    return available
      ? `Project "${projectName}" not found.\nRegistered projects: ${available}`
      : `Project "${projectName}" not found. No projects registered — use /register-project.`;
  }

  // Deactivate the old session BEFORE rebinding the conversation: if either
  // session step throws, the switch aborts with the conversation untouched
  // (next message just starts a fresh session in the OLD project). The reverse
  // order would leave a rebound conversation with the old project's session
  // still active — resuming old-project context under the new project's cwd.
  const session = await sessionDb.getActiveSession(conversation.id);
  if (session) {
    await safeDeactivateSession(session.id, 'setproject');
  }

  // Intentionally non-destructive: clearing isolation_env_id detaches the
  // conversation from its worktree WITHOUT destroying it — the worktree may
  // hold uncommitted work and the user may switch back (project-switch is not
  // terminal, unlike conversation-closed). The env row stays active until
  // /worktree remove or the periodic isolation cleanup reaps it; we surface
  // that to the user below instead of tearing it down.
  const detachedWorktree = conversation.isolation_env_id !== null;
  await db.updateConversation(conversation.id, {
    codebase_id: codebase.id,
    cwd: null,
    isolation_env_id: null,
  });
  if (detachedWorktree) {
    getLog().info(
      { conversationId: conversation.id, isolationEnvId: conversation.isolation_env_id },
      'project.set_worktree_detached'
    );
  }

  getLog().info(
    { conversationId: conversation.id, projectName: codebase.name, codebaseId: codebase.id },
    'project.set_completed'
  );
  let reply = `Project set to **${codebase.name}**\nWorking directory: ${codebase.default_cwd}`;
  if (detachedWorktree) {
    // Don't suggest `/worktree remove` here: it reads isolation_env_id from
    // THIS conversation, which we just cleared — it would short-circuit with
    // "not using a worktree". Cleanup tools that operate on the environments
    // table directly are the working remedies.
    reply +=
      '\n\nNote: the previous worktree was detached but left in place — clean it up with `archon isolation cleanup` or from the project’s Environments list in the web UI.';
  }
  return reply;
}

/**
 * Handle /workflow run command when project context may be missing.
 * Implements Edge Case E2 from the plan.
 */
async function handleWorkflowRunCommand(
  platform: IPlatformAdapter,
  conversationId: string,
  conversation: Conversation,
  workflow: WorkflowDefinition,
  userMessage: string,
  isolationHints?: HandleMessageContext['isolationHints'],
  userId?: string,
  options?: WorkflowDispatchOptions
): Promise<void> {
  // Check if conversation has a project
  if (conversation.codebase_id) {
    const codebase = await codebaseDb.getCodebase(conversation.codebase_id);
    if (!codebase) {
      await platform.sendMessage(conversationId, 'Codebase not found.');
      return;
    }

    // Route through dispatchOrchestratorWorkflow so validateAndResolveIsolation
    // always runs — ensures a worktree is created regardless of how the codebase
    // was registered (local path or GitHub URL clone).
    await dispatchOrchestratorWorkflow(
      platform,
      conversationId,
      conversation,
      codebase,
      workflow,
      userMessage,
      isolationHints,
      userId,
      undefined,
      options
    );
    return;
  }

  // No project attached — apply E2 logic
  const codebases = await codebaseDb.listCodebases();

  if (codebases.length === 0) {
    await platform.sendMessage(
      conversationId,
      'No projects registered. Ask me to set up a project first.'
    );
    return;
  }

  if (codebases.length === 1) {
    // Auto-select the only project
    const codebase = codebases[0];
    const workflowCwd = conversation.cwd ?? codebase.default_cwd;
    try {
      await syncArchonToWorktree(workflowCwd);
    } catch (error) {
      getLog().debug(
        { err: error as Error, workflowCwd },
        'workflow_sync_before_validation_failed'
      );
    }

    let discovery;
    try {
      discovery = await discoverWorkflowsWithConfig(workflowCwd, loadConfig);
    } catch (error) {
      const err = error as Error;
      getLog().error({ err, cwd: workflowCwd }, 'workflow_discovery_failed');
      await platform.sendMessage(
        conversationId,
        `Failed to load workflows: ${err.message}\n\nCheck .archon/workflows/ for YAML syntax issues.`
      );
      return;
    }

    const resolvedEntry =
      discovery.workflows.find(w => w.workflow.name === workflow.name) ??
      discovery.workflows.find(w => w.workflow.name.toLowerCase() === workflow.name.toLowerCase());
    const resolvedWorkflow = resolvedEntry?.workflow;

    if (!resolvedWorkflow) {
      const loadError = discovery.errors.find(
        e =>
          e.filename.replace(/\.ya?ml$/, '') === workflow.name ||
          e.filename === `${workflow.name}.yaml` ||
          e.filename === `${workflow.name}.yml`
      );
      if (loadError) {
        await platform.sendMessage(
          conversationId,
          `Workflow \`${workflow.name}\` failed to load: ${loadError.error}\n\nFix the YAML file and try again.`
        );
        return;
      }

      await platform.sendMessage(
        conversationId,
        `Workflow \`${workflow.name}\` not found.\n\nUse /workflow list to see available workflows.`
      );
      return;
    }

    await db.updateConversation(conversation.id, { codebase_id: codebase.id });
    await dispatchOrchestratorWorkflow(
      platform,
      conversationId,
      conversation,
      codebase,
      resolvedWorkflow,
      userMessage,
      isolationHints,
      userId,
      resolvedEntry?.source,
      // Warnings must describe the workflow that will EXECUTE. This branch
      // RE-RESOLVES the workflow against the single project's discovery, which
      // can land on a different file than the caller resolved (a project
      // workflow shadowing a same-named global one). Inheriting the caller's
      // warnings would then describe a workflow that is not running.
      { ...options, parseWarnings: resolvedEntry?.parseWarnings }
    );
    return;
  }

  // Multiple projects — ask user to choose
  const projectList = codebases.map(c => `- ${c.name}`).join('\n');
  await platform.sendMessage(
    conversationId,
    `Which project should this workflow run on?\n\n${projectList}\n\nReply with the project name, or use: /workflow run ${workflow.name} --project <name> "${userMessage}"`
  );
}
