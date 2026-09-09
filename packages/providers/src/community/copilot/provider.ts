/**
 * GitHub Copilot provider (community tier).
 *
 * Implements `IAgentProvider` on top of @github/copilot-sdk. Resolves auth +
 * binary path + reasoning config, translates Archon workflow options
 * (tool restrictions, MCP servers, skills, agents, structured output) to the
 * SDK's `SessionConfig`, creates or resumes a session, and hands the
 * streaming bridge off to `bridgeSession` in `event-bridge.ts`.
 *
 * Module-scope invariant: type-only imports from @github/copilot-sdk. All
 * value imports (`CopilotClient`, `approveAll`) happen inside `sendQuery()`
 * via dynamic `await import(...)`. `provider-lazy-load.test.ts` asserts this
 * so a future SDK update that reads the filesystem at module load can't
 * break compiled-binary bootstrap.
 */
import { createLogger } from '@archon/paths';
import type {
  CopilotClientOptions,
  CopilotSession,
  CustomAgentConfig,
  MCPServerConfig,
  SessionConfig,
  SystemMessageConfig,
} from '@github/copilot-sdk';

import type {
  IAgentProvider,
  MessageChunk,
  ProviderCapabilities,
  SendQueryOptions,
} from '../../types';
import { loadMcpConfig } from '../../mcp/config';
import { resolveSkillDirectories } from '../../shared/skills';
import { augmentPromptForJsonSchema } from '../../shared/structured-output';
import { COPILOT_CAPABILITIES } from './capabilities';
import { parseCopilotConfig, type CopilotProviderDefaults } from './config';
import { resolveCopilotBinaryPath } from './binary-resolver';
import { bridgeSession } from './event-bridge';

// `ReasoningEffort` is defined in the SDK but not re-exported from its barrel
// (as of @github/copilot-sdk@0.2.2). Mirror the enum literally so we don't
// depend on an internal subpath.
type CopilotReasoningEffort = 'low' | 'medium' | 'high' | 'xhigh';

/**
 * Auth env vars, split by intent.
 *
 *   - `COPILOT_GITHUB_TOKEN` — Copilot-specific PAT. Setting it is a strong
 *     signal of intent ("use this for Copilot"), so it always wins.
 *   - `GH_TOKEN` / `GITHUB_TOKEN` — generic GitHub tokens. Most users have
 *     these set for `gh` CLI / clone helpers / webhooks, where classic PATs
 *     are fine. Those PATs typically lack Copilot entitlement, so picking
 *     them up automatically yields a misleading "Session was not created
 *     with authentication info" error from the SDK. We therefore ignore
 *     these unless the user explicitly opts in via `useLoggedInUser: false`.
 */
const COPILOT_TOKEN_ENV_KEY = 'COPILOT_GITHUB_TOKEN';
const GENERIC_GITHUB_TOKEN_ENV_KEYS = ['GH_TOKEN', 'GITHUB_TOKEN'] as const;

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.copilot');
  return cachedLog;
}

/**
 * No-op kept for back-compat with tests that previously called into the
 * singleton-reset API. The client is now constructed fresh per `sendQuery()`
 * so each request sees correct per-request env vars.
 */
export function resetCopilotSingleton(): void {
  // no-op
}

// ─── Warning collection ─────────────────────────────────────────────────────

/** Structured provider warning collected during translation; flushed as a system chunk. */
interface ProviderWarning {
  code: string;
  message: string;
}

// ─── Env + auth ─────────────────────────────────────────────────────────────

/**
 * Merge process.env with per-request env vars from the workflow node's
 * codebase-scoped env bag. Request env wins — matches the layering
 * Claude/Codex use for their SDK env handoff.
 */
function buildCopilotEnv(requestEnv?: Record<string, string>): Record<string, string> {
  const baseEnv = Object.fromEntries(
    Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined)
  );
  return { ...baseEnv, ...(requestEnv ?? {}) };
}

function resolveCopilotToken(env: Record<string, string>): string | undefined {
  const value = env[COPILOT_TOKEN_ENV_KEY];
  return value ? value : undefined;
}

function resolveGenericGitHubToken(env: Record<string, string>): string | undefined {
  for (const key of GENERIC_GITHUB_TOKEN_ENV_KEYS) {
    const value = env[key];
    if (value) return value;
  }
  return undefined;
}

// ─── Reasoning ──────────────────────────────────────────────────────────────

function normalizeReasoning(value: unknown): CopilotReasoningEffort | undefined {
  if (value === 'max') return 'xhigh';
  if (value === 'low' || value === 'medium' || value === 'high' || value === 'xhigh') return value;
  return undefined;
}

/**
 * Resolve Copilot's `reasoningEffort` from Archon's workflow inputs.
 * Precedence:
 *   nodeConfig.thinking > nodeConfig.effort > config.modelReasoningEffort
 *
 * Archon's `effort` schema is `'low' | 'medium' | 'high' | 'max'` — we map
 * `'max'` to the SDK's `'xhigh'`. The `'off'` sentinel disables reasoning.
 * The object form of `thinking` (Claude-specific) returns a warning.
 */
function resolveCopilotReasoning(
  nodeConfig: SendQueryOptions['nodeConfig'] | undefined,
  copilotConfig: CopilotProviderDefaults
): { effort: CopilotReasoningEffort | undefined; warning?: string } {
  if (!nodeConfig) {
    return { effort: copilotConfig.modelReasoningEffort };
  }

  const rawThinking = nodeConfig.thinking;
  const rawEffort = nodeConfig.effort;

  if (rawThinking === 'off' || rawEffort === 'off') return { effort: undefined };

  const fromThinking = normalizeReasoning(rawThinking);
  if (fromThinking) return { effort: fromThinking };

  const fromEffort = normalizeReasoning(rawEffort);
  if (fromEffort) return { effort: fromEffort };

  if (rawThinking !== undefined && rawThinking !== null && typeof rawThinking === 'object') {
    return {
      effort: undefined,
      warning:
        'Copilot ignored `thinking` (object form is Claude-specific). Use `effort: low|medium|high|max` instead.',
    };
  }

  if (typeof rawThinking === 'string' || typeof rawEffort === 'string') {
    const offender = typeof rawThinking === 'string' ? rawThinking : rawEffort;
    return {
      effort: undefined,
      warning: `Copilot ignored unknown reasoning level '${String(offender)}'. Valid: low, medium, high, xhigh, max, off.`,
    };
  }

  // Fall back to config-level default when nodeConfig provides nothing actionable.
  return { effort: copilotConfig.modelReasoningEffort };
}

// ─── System prompt ──────────────────────────────────────────────────────────

function resolveSystemMessage(requestOptions?: SendQueryOptions): SystemMessageConfig | undefined {
  const requestPrompt = requestOptions?.systemPrompt;
  const nodePrompt =
    typeof requestOptions?.nodeConfig?.systemPrompt === 'string'
      ? requestOptions.nodeConfig.systemPrompt
      : undefined;
  const content = requestPrompt ?? nodePrompt;
  if (typeof content === 'string' && content.length > 0) {
    return { mode: 'append', content };
  }
  return undefined;
}

// ─── Translations ───────────────────────────────────────────────────────────

/**
 * Translate Archon's per-node `allowed_tools` / `denied_tools` to Copilot's
 * `availableTools` / `excludedTools`. Copilot's spec: `availableTools` takes
 * precedence over `excludedTools`; we pass both through when present and let
 * the SDK enforce precedence.
 */
function applyToolRestrictions(
  sessionConfig: SessionConfig,
  nodeConfig: SendQueryOptions['nodeConfig']
): void {
  if (!nodeConfig) return;
  if (nodeConfig.allowed_tools !== undefined) {
    sessionConfig.availableTools = nodeConfig.allowed_tools;
  }
  if (nodeConfig.denied_tools !== undefined) {
    sessionConfig.excludedTools = nodeConfig.denied_tools;
  }
}

/**
 * Translate Archon's `nodeConfig.mcp` (JSON-file path) to Copilot's
 * `SessionConfig.mcpServers`. Reuses the shared `loadMcpConfig` helper so
 * env-var expansion and missing-var detection behave consistently across
 * providers.
 */
async function applyMcpServers(
  sessionConfig: SessionConfig,
  nodeConfig: SendQueryOptions['nodeConfig'],
  cwd: string,
  warnings: ProviderWarning[]
): Promise<void> {
  const mcpPath = nodeConfig?.mcp;
  if (typeof mcpPath !== 'string' || mcpPath.length === 0) return;

  const { servers, serverNames, missingVars } = await loadMcpConfig(mcpPath, cwd);

  if (missingVars.length > 0) {
    warnings.push({
      code: 'copilot.mcp_env_vars_missing',
      message: `Copilot MCP config references undefined env vars: ${missingVars.join(', ')}. Servers using them may fail at runtime.`,
    });
  }

  sessionConfig.mcpServers = servers as Record<string, MCPServerConfig>;
  getLog().info({ serverNames, missingVars }, 'copilot.mcp_loaded');
}

/**
 * Translate Archon's `nodeConfig.skills` (string names) to Copilot's
 * `SessionConfig.skillDirectories` (absolute paths). Unresolved names become
 * a single system warning so the user notices the typo/missing skill.
 */
function applySkills(
  sessionConfig: SessionConfig,
  nodeConfig: SendQueryOptions['nodeConfig'],
  cwd: string,
  warnings: ProviderWarning[]
): void {
  if (!nodeConfig?.skills || nodeConfig.skills.length === 0) return;

  const { paths, missing } = resolveSkillDirectories(cwd, nodeConfig.skills);

  if (missing.length > 0) {
    warnings.push({
      code: 'copilot.skills_missing',
      message: `Copilot ignored missing skills: ${missing.join(', ')}. Expected a directory with SKILL.md under .agents/skills/ or .claude/skills/ (project or home).`,
    });
  }

  if (paths.length > 0) {
    sessionConfig.skillDirectories = paths;
  }
  getLog().info({ resolved: paths.length, missing }, 'copilot.skills_resolved');
}

/**
 * Translate Archon's `nodeConfig.agents` (Record<name, AgentDef>) to
 * Copilot's `SessionConfig.customAgents`. Only the fields Copilot's
 * `CustomAgentConfig` supports pass through (description, prompt, tools).
 * Archon agent fields Copilot cannot represent (`model`, `disallowedTools`,
 * `skills`, `maxTurns`) surface as one consolidated warning per agent.
 *
 * We do NOT set `SessionConfig.agent` — Archon's workflow model invokes
 * sub-agents via the Task tool, not by switching active agent at session
 * start.
 */
function applyAgents(
  sessionConfig: SessionConfig,
  nodeConfig: SendQueryOptions['nodeConfig'],
  warnings: ProviderWarning[]
): void {
  const agents = nodeConfig?.agents;
  if (!agents) return;
  const entries = Object.entries(agents);
  if (entries.length === 0) return;

  const customAgents: CustomAgentConfig[] = entries.map(([name, def]) => {
    const ignored: string[] = [];
    if (def.model !== undefined) ignored.push('model');
    if (def.disallowedTools !== undefined) ignored.push('disallowedTools');
    if (def.skills !== undefined) ignored.push('skills');
    if (def.maxTurns !== undefined) ignored.push('maxTurns');

    if (ignored.length > 0) {
      warnings.push({
        code: 'copilot.agent_fields_ignored',
        message: `Copilot agent '${name}' ignored unsupported fields: ${ignored.join(', ')}. Copilot supports description, prompt, tools (allowlist) only.`,
      });
    }

    return {
      name,
      description: def.description,
      prompt: def.prompt,
      ...(def.tools !== undefined ? { tools: def.tools } : {}),
    };
  });

  sessionConfig.customAgents = customAgents;
  getLog().info(
    { count: customAgents.length, names: customAgents.map(a => a.name) },
    'copilot.agents_registered'
  );
}

// ─── SessionConfig assembly ─────────────────────────────────────────────────

/**
 * Single construction site for the Copilot SessionConfig. Callers add new
 * translations as `applyX(sessionConfig, ..., warnings)` calls below — keep
 * business logic here straight-through.
 */
async function buildSessionConfig(
  copilotConfig: CopilotProviderDefaults,
  requestOptions: SendQueryOptions | undefined,
  cwd: string,
  approveAll: SessionConfig['onPermissionRequest'],
  warnings: ProviderWarning[]
): Promise<SessionConfig> {
  const reasoning = resolveCopilotReasoning(requestOptions?.nodeConfig, copilotConfig);
  if (reasoning.warning) {
    warnings.push({ code: 'copilot.reasoning_ignored', message: reasoning.warning });
  }

  const requestedModel = requestOptions?.model?.trim() || undefined;
  const defaultModel = copilotConfig.model?.trim() || undefined;
  // Default to 'auto' so Copilot picks a model when neither request nor
  // config names one. Matches the shipping Copilot CLI default.
  const resolvedModel = requestedModel ?? defaultModel ?? 'auto';

  const sessionConfig: SessionConfig = {
    model: resolvedModel,
    reasoningEffort: reasoning.effort,
    workingDirectory: cwd,
    // configDir moved to the client level in copilot-sdk 1.0 (baseDirectory —
    // sets COPILOT_HOME on the spawned runtime); applied in sendQuery's
    // CopilotClientOptions, not here.
    streaming: true,
    systemMessage: resolveSystemMessage(requestOptions),
    enableConfigDiscovery: copilotConfig.enableConfigDiscovery ?? false,
    onPermissionRequest: approveAll,
  };

  applyToolRestrictions(sessionConfig, requestOptions?.nodeConfig);
  await applyMcpServers(sessionConfig, requestOptions?.nodeConfig, cwd, warnings);
  applySkills(sessionConfig, requestOptions?.nodeConfig, cwd, warnings);
  applyAgents(sessionConfig, requestOptions?.nodeConfig, warnings);

  return sessionConfig;
}

// ─── Error classification ──────────────────────────────────────────────────

/** Best-effort stringify that never yields '[object Object]'. */
function safeErrorString(value: unknown): string {
  if (value === undefined || value === null) return 'Unknown error';
  if (typeof value === 'string') return value || 'Unknown error';
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  try {
    const json = JSON.stringify(value);
    if (json && json !== '{}') return json;
  } catch {
    /* fall through */
  }
  return 'Unknown error';
}

function isModelAccessError(errorMessage: string): boolean {
  const normalized = errorMessage.toLowerCase();
  const hasModel = normalized.includes('model');
  const hasAvailabilitySignal =
    normalized.includes('not available') ||
    normalized.includes('not found') ||
    normalized.includes('unsupported');
  return hasModel && hasAvailabilitySignal;
}

/**
 * Classify common Copilot failure modes and return a more actionable Error.
 * Combines the thrown message with any `lastSessionError` collected via the
 * SDK's `session.error` event — the latter often carries the specific
 * model-access / auth detail while the thrown error is generic.
 */
function buildFriendlyCopilotError(error: unknown, lastSessionError?: string): Error {
  const thrownMessage =
    error instanceof Error && error.message ? error.message : safeErrorString(error);
  const parts = [thrownMessage, lastSessionError].filter(
    (m): m is string => typeof m === 'string' && m.length > 0
  );
  const combined = parts.join('\n');

  if (isModelAccessError(combined)) {
    return new Error(
      `Copilot model access error: ${combined}\n\n` +
        'Try a different model in the workflow node or set assistants.copilot.model in .archon/config.yaml.'
    );
  }

  const normalized = combined.toLowerCase();
  if (
    normalized.includes('auth') ||
    normalized.includes('login') ||
    normalized.includes('unauthorized') ||
    normalized.includes('forbidden')
  ) {
    return new Error(
      `Copilot authentication failed: ${combined}\n\n` +
        'Run `copilot login` (default), set COPILOT_GITHUB_TOKEN, or set ' +
        '`useLoggedInUser: false` in `.archon/config.yaml` to use GH_TOKEN / GITHUB_TOKEN.'
    );
  }

  return error instanceof Error ? error : new Error(combined);
}

interface CopilotAuthClientOptionsInput {
  cwd: string;
  mergedEnv: Record<string, string>;
  cliPath?: string;
  copilotConfig: CopilotProviderDefaults;
  copilotToken?: string;
  genericGithubToken?: string;
}

interface CopilotSessionResolution {
  session: CopilotSession;
  resumeFailed: boolean;
  forkedToFresh: boolean;
}

interface CopilotClientLike {
  createSession(config: SessionConfig): Promise<CopilotSession>;
  resumeSession(sessionId: string, config: SessionConfig): Promise<CopilotSession>;
  stop(): Promise<Error[]>;
}

function logUnsupportedCopilotOptions(
  log: ReturnType<typeof getLog>,
  requestOptions: SendQueryOptions | undefined
): void {
  if (requestOptions?.forkSession !== undefined) {
    log.debug(
      { option: 'forkSession', value: requestOptions.forkSession },
      'copilot.option_not_supported'
    );
  }
  if (requestOptions?.persistSession !== undefined) {
    log.debug(
      { option: 'persistSession', value: requestOptions.persistSession },
      'copilot.option_not_supported'
    );
  }
}

function buildCopilotClientOptions(input: CopilotAuthClientOptionsInput): {
  clientOpts: CopilotClientOptions;
  tokenSource: 'copilot-token' | 'generic-token' | 'logged-in-user';
} {
  const clientOpts: CopilotClientOptions = {
    workingDirectory: input.cwd,
    env: input.mergedEnv,
  };
  if (input.cliPath) clientOpts.connection = { kind: 'stdio', path: input.cliPath };
  if (input.copilotConfig.configDir) clientOpts.baseDirectory = input.copilotConfig.configDir;

  if (input.copilotToken) {
    clientOpts.gitHubToken = input.copilotToken;
    clientOpts.useLoggedInUser = false;
    return { clientOpts, tokenSource: 'copilot-token' };
  }

  if (input.copilotConfig.useLoggedInUser === false) {
    if (input.genericGithubToken) {
      clientOpts.gitHubToken = input.genericGithubToken;
    }
    clientOpts.useLoggedInUser = false;
    return {
      clientOpts,
      tokenSource: input.genericGithubToken ? 'generic-token' : 'logged-in-user',
    };
  }

  clientOpts.useLoggedInUser = true;
  return { clientOpts, tokenSource: 'logged-in-user' };
}

async function stopCopilotClientAfterSessionError(
  client: CopilotClientLike,
  log: ReturnType<typeof getLog>
): Promise<void> {
  try {
    await client.stop();
  } catch (stopErr) {
    log.debug({ err: stopErr }, 'copilot.client_stop_failed_after_session_error');
  }
}

async function resolveCopilotSession(input: {
  client: CopilotClientLike;
  sessionConfig: SessionConfig;
  resumeSessionId: string | undefined;
  wantsFork: boolean;
  cwd: string;
  log: ReturnType<typeof getLog>;
}): Promise<CopilotSessionResolution> {
  const { client, sessionConfig, resumeSessionId, wantsFork, cwd, log } = input;
  if (resumeSessionId && !wantsFork) {
    log.debug({ sessionId: resumeSessionId, cwd }, 'copilot.resume_attempt');
    try {
      return {
        session: await client.resumeSession(resumeSessionId, sessionConfig),
        resumeFailed: false,
        forkedToFresh: false,
      };
    } catch (err) {
      log.debug(
        { err, sessionId: resumeSessionId },
        'copilot.resume_failed_falling_back_to_create'
      );
      return {
        session: await client.createSession(sessionConfig),
        resumeFailed: true,
        forkedToFresh: false,
      };
    }
  }

  if (resumeSessionId && wantsFork) {
    log.warn(
      { requestedResumeSessionId: resumeSessionId },
      'copilot.fork_unsupported_creating_fresh_session'
    );
    return {
      session: await client.createSession(sessionConfig),
      resumeFailed: false,
      forkedToFresh: true,
    };
  }

  log.debug({ cwd }, 'copilot.create_session');
  return {
    session: await client.createSession(sessionConfig),
    resumeFailed: false,
    forkedToFresh: false,
  };
}

function copilotResumeWarning(resolution: CopilotSessionResolution): string | undefined {
  if (resolution.resumeFailed) {
    return 'Could not resume Copilot session — starting a fresh conversation.';
  }
  if (resolution.forkedToFresh) {
    return 'Copilot SDK does not support session forking; starting a fresh conversation to keep retries safe.';
  }
  return undefined;
}

async function stopCopilotClient(
  client: CopilotClientLike,
  log: ReturnType<typeof getLog>
): Promise<void> {
  try {
    const stopErrors = await client.stop();
    if (stopErrors.length > 0) {
      log.warn({ errors: stopErrors.map(e => e.message) }, 'copilot.client_stop_errors');
    }
  } catch (stopErr) {
    log.debug({ err: stopErr }, 'copilot.client_stop_threw');
  }
}

function logCopilotSessionStarted(input: {
  log: ReturnType<typeof getLog>;
  session: CopilotSession;
  sessionConfig: SessionConfig;
  cwd: string;
  tokenSource: 'copilot-token' | 'generic-token' | 'logged-in-user';
  resumed: boolean;
}): void {
  const { log, session, sessionConfig, cwd, tokenSource, resumed } = input;
  log.info(
    {
      sessionId: session.sessionId,
      model: sessionConfig.model,
      cwd,
      reasoningEffort: sessionConfig.reasoningEffort,
      hasSystemMessage: sessionConfig.systemMessage !== undefined,
      mcpServers: sessionConfig.mcpServers ? Object.keys(sessionConfig.mcpServers).length : 0,
      skills: sessionConfig.skillDirectories?.length ?? 0,
      agents: sessionConfig.customAgents?.length ?? 0,
      tokenSource,
      resumed,
    },
    'copilot.session_started'
  );
}

// ─── Provider class ─────────────────────────────────────────────────────────

/**
 * GitHub Copilot community provider. Implements `IAgentProvider` on top of
 * `@github/copilot-sdk`, translating Archon workflow options (tools, MCP,
 * skills, agents, structured output, reasoning) to the SDK's `SessionConfig`,
 * bridging its event stream via `bridgeSession()`, and surfacing provider
 * signals (translation warnings, fork workaround, resume fallback) to the
 * caller. Each `sendQuery()` constructs a fresh `CopilotClient` so
 * per-request env vars are honored.
 */
export class CopilotProvider implements IAgentProvider {
  getType(): string {
    return 'copilot';
  }

  getCapabilities(): ProviderCapabilities {
    return COPILOT_CAPABILITIES;
  }

  async *sendQuery(
    prompt: string,
    cwd: string,
    resumeSessionId?: string,
    requestOptions?: SendQueryOptions
  ): AsyncGenerator<MessageChunk> {
    const log = getLog();
    logUnsupportedCopilotOptions(log, requestOptions);

    const assistantConfig = requestOptions?.assistantConfig ?? {};
    const copilotConfig = parseCopilotConfig(assistantConfig);
    const mergedEnv = buildCopilotEnv(requestOptions?.env);
    const cliPath = await resolveCopilotBinaryPath(copilotConfig.copilotCliPath);

    const sdk = await import('@github/copilot-sdk');
    const { CopilotClient: copilotClientCtor, approveAll } = sdk;

    const warnings: ProviderWarning[] = [];
    const sessionConfig = await buildSessionConfig(
      copilotConfig,
      requestOptions,
      cwd,
      approveAll,
      warnings
    );

    for (const w of warnings) {
      yield { type: 'system', content: `⚠️ ${w.message}` };
    }

    const outputFormat = requestOptions?.outputFormat;
    const wantsStructured = outputFormat?.type === 'json_schema';
    const effectivePrompt = wantsStructured
      ? augmentPromptForJsonSchema(prompt, outputFormat.schema)
      : prompt;

    const { clientOpts, tokenSource } = buildCopilotClientOptions({
      cwd,
      mergedEnv,
      cliPath,
      copilotConfig,
      copilotToken: resolveCopilotToken(mergedEnv),
      genericGithubToken: resolveGenericGitHubToken(mergedEnv),
    });
    if (copilotConfig.logLevel) clientOpts.logLevel = copilotConfig.logLevel;
    const client = new copilotClientCtor(clientOpts);

    let resolution: CopilotSessionResolution;
    try {
      resolution = await resolveCopilotSession({
        client,
        sessionConfig,
        resumeSessionId,
        wantsFork: requestOptions?.forkSession === true,
        cwd,
        log,
      });
    } catch (err) {
      await stopCopilotClientAfterSessionError(client, log);
      throw buildFriendlyCopilotError(err);
    }

    const warning = copilotResumeWarning(resolution);
    if (warning) yield { type: 'system', content: `⚠️ ${warning}` };

    const { session } = resolution;
    logCopilotSessionStarted({
      log,
      session,
      sessionConfig,
      cwd,
      tokenSource,
      resumed: resumeSessionId !== undefined && !resolution.resumeFailed,
    });

    try {
      yield* bridgeSession(
        session,
        effectivePrompt,
        requestOptions?.abortSignal,
        wantsStructured ? outputFormat.schema : undefined
      );
      log.info({ sessionId: session.sessionId }, 'copilot.prompt_completed');
    } catch (err) {
      log.error({ err, sessionId: session.sessionId }, 'copilot.prompt_failed');
      throw buildFriendlyCopilotError(err);
    } finally {
      await stopCopilotClient(client, log);
    }
  }
}
