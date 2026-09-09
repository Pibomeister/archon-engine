/**
 * Claude Agent SDK wrapper
 * Provides async generator interface for streaming Claude responses
 *
 * Type Safety Pattern:
 * - Uses `Options` type from SDK for query configuration
 * - SDK message types have strict type checking for content blocks
 * - Content blocks are typed via inline assertions for clarity
 *
 * Authentication:
 * - Credentials reach the subprocess via process.env (already cleaned by
 *   stripCwdEnv) PLUS any per-request `requestOptions.env` (per-user delivered
 *   keys/subscriptions), merged LAST so it wins. `buildSubprocessEnv` does NOT
 *   filter tokens — it only logs which posture process.env shows (explicit
 *   token present vs not); the historical env-token allowlist was removed in
 *   #1067, so the log can read "global" while a per-request token authenticates.
 * - CLAUDE_USE_GLOBAL_AUTH is an Archon-only boot sentinel (set for solo
 *   installs with no creds — see server/src/boot/claude-auth-posture.ts). The
 *   Claude CLI itself ignores it; it neither gates nor filters env here.
 *
 * Binary resolution:
 * - In compiled binaries, `pathToClaudeCodeExecutable` is resolved from
 *   `CLAUDE_BIN_PATH` env or `assistants.claude.claudeBinaryPath` config;
 *   see ./binary-resolver.ts. In dev mode the resolver returns undefined
 *   and the SDK picks its bundled per-platform native binary (Mach-O/ELF/PE
 *   from `@anthropic-ai/claude-agent-sdk-<platform>` optional dep). Pre-0.2.x
 *   SDKs shipped `cli.js` in the package and dev mode resolved that JS file;
 *   the SDK switched to native binaries in the 0.2.x series. See
 *   `shouldPassNoEnvFile` for the implications on the `--no-env-file` flag.
 */
import {
  query,
  type Options,
  type HookCallback,
  type HookCallbackMatcher,
  type SDKAssistantMessageError,
  type SDKResultMessage,
  type ModelUsage,
} from '@anthropic-ai/claude-agent-sdk';
import { isIP } from 'node:net';
import type {
  IAgentProvider,
  SendQueryOptions,
  MessageChunk,
  TokenUsage,
  ProviderCapabilities,
  NodeConfig,
  ExecutionContext,
} from '../types';
import { parseClaudeConfig } from './config';
import { CLAUDE_CAPABILITIES } from './capabilities';
import {
  buildContainerSpawn,
  stopContainerOnAbort as defaultStopContainerOnAbort,
} from './container-spawn';
import { resolveClaudeBinaryPath } from './binary-resolver';
import { buildArchonMcpServer, ARCHON_TOOL_SERVER } from './native-tools';
import { createLogger } from '@archon/paths';
import { loadMcpConfig } from '../mcp/config';
import { withResumedOutcome, resumedOutcome } from '../shared/resumed';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.claude');
  return cachedLog;
}

/**
 * Content block type for assistant messages
 */
interface ContentBlock {
  type: 'text' | 'tool_use';
  text?: string;
  name?: string;
  input?: Record<string, unknown>;
  id?: string;
}

function normalizeClaudeUsage(usage?: {
  input_tokens?: number;
  output_tokens?: number;
  total_tokens?: number;
}): TokenUsage | undefined {
  if (!usage) return undefined;
  const input = usage.input_tokens;
  const output = usage.output_tokens;
  if (typeof input !== 'number' || typeof output !== 'number') return undefined;
  const total = usage.total_tokens;
  return {
    input,
    output,
    ...(typeof total === 'number' ? { total } : {}),
  };
}

/**
 * Pick the concrete model that did the bulk of a turn's work from the SDK's
 * per-model usage record.
 *
 * More than one entry is reachable for a single turn: a subagent pinned to
 * another model via `agents:`, or a `fallbackModel` takeover. Key insertion
 * order happens to put the main model first today, but nothing in the SDK
 * guarantees it — so select by greatest output-token count (the main model
 * produces the bulk of the output) and WARN whenever the record is ambiguous,
 * so a multi-model turn is visible instead of silently collapsed.
 *
 * `modelUsage` is non-optional in the SDK types but arrives over an IPC
 * boundary, so the absent/empty cases stay guarded — absence yields undefined
 * and the caller omits `resolvedModel` entirely rather than inventing a value.
 * On a tie (or output counts the SDK didn't send) the first key wins, which is
 * exactly the pre-#2314 behavior — safe, and the warning still fires.
 */
function selectResolvedModelId(
  modelUsage: Record<string, ModelUsage> | undefined
): string | undefined {
  if (!modelUsage) return undefined;
  const entries = Object.entries(modelUsage);
  if (entries.length === 0) return undefined;
  if (entries.length === 1) return entries[0][0];

  const outputTokensOf = (usage: ModelUsage): number =>
    Number.isFinite(usage.outputTokens) ? usage.outputTokens : 0;
  let selected = entries[0];
  for (const entry of entries.slice(1)) {
    if (outputTokensOf(entry[1]) > outputTokensOf(selected[1])) selected = entry;
  }
  getLog().warn(
    { models: entries.map(([id]) => id), selected: selected[0] },
    'claude.resolved_model_ambiguous'
  );
  return selected[0];
}

/**
 * Build environment for Claude subprocess.
 *
 * process.env is already clean at this point:
 * - stripCwdEnv() at entry point removed CWD .env keys + CLAUDECODE markers
 * - ~/.archon/.env loaded with override:true as the trusted source
 */
function buildSubprocessEnv(): NodeJS.ProcessEnv {
  // Using || intentionally: empty string should be treated as missing credential
  const hasExplicitTokens = Boolean(
    process.env.CLAUDE_CODE_OAUTH_TOKEN || process.env.CLAUDE_API_KEY
  );
  const authMode = hasExplicitTokens ? 'explicit' : 'global';
  getLog().info(
    { authMode },
    authMode === 'global' ? 'using_global_auth' : 'using_explicit_tokens'
  );
  return { ...process.env };
}

/**
 * Build the base env for a CONTAINER run. Deliberately does NOT spread
 * `process.env` — that is the first isolation boundary. The final docker-exec
 * boundary applies the same strict allowlist again in case the SDK reintroduces
 * ambient values. PATH/HOME come from the runner image.
 */
function buildContainerBaseEnv(): NodeJS.ProcessEnv {
  return { TERM: 'dumb' };
}

const CONTAINER_CLAUDE_ENV_ALLOWLIST: ReadonlySet<string> = new Set([
  'ANTHROPIC_API_KEY',
  'ANTHROPIC_OAUTH_TOKEN',
  'CLAUDE_CODE_DISABLE_1M_CONTEXT',
  'CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS',
  'CLAUDE_CODE_OAUTH_TOKEN',
  'TERM',
]);

const CONTAINER_CLAUDE_ENV_MIRROR_SOURCES: ReadonlySet<string> = new Set(['CLAUDE_API_KEY']);

const CONTAINER_CLAUDE_ENV_HARD_DENY_KEYS: ReadonlySet<string> = new Set([
  'AWS_SECRET_ACCESS_KEY',
  'AWS_SESSION_TOKEN',
  'DATABASE_URL',
  'ANTHROPIC_BASE_URL',
  'DOCKER_HOST',
  'GH_TOKEN',
  'GITHUB_TOKEN',
  'LD_PRELOAD',
  'NODE_OPTIONS',
]);

const ANTHROPIC_PROVIDER_NAMES: ReadonlySet<string> = new Set(['anthropic', 'claude']);

function isDnsHostname(hostname: string): boolean {
  if (hostname.length === 0 || hostname.length > 253) return false;
  const labels = hostname.split('.');
  return labels.every(label => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(label));
}

function normalizeSealedAnthropicBaseUrl(rawBaseUrl: string): string {
  let url: URL;
  try {
    url = new URL(rawBaseUrl);
  } catch {
    throw new Error('Claude hardened provider origin is malformed.');
  }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
    throw new Error('Claude hardened provider origin must be an exact HTTPS API root.');
  }
  if (url.pathname !== '/') {
    throw new Error('Claude hardened provider origin must be the HTTPS API origin root.');
  }
  const hostname = url.hostname.replace(/^\[|\]$/g, '');
  if (hostname.toLowerCase() === 'localhost' || isIP(hostname) !== 0 || !isDnsHostname(hostname)) {
    throw new Error('Claude hardened provider origin must use a non-local DNS hostname.');
  }
  return url.origin;
}

function requiredSealedAnthropicBaseUrl(execContext: ExecutionContext | undefined): string {
  if (execContext?.kind !== 'container') {
    throw new Error(
      'Claude container execution requires exactly one sealed Anthropic provider origin.'
    );
  }
  const matches = (execContext.providerOrigins ?? []).filter(origin =>
    ANTHROPIC_PROVIDER_NAMES.has(origin.provider.toLowerCase())
  );
  if (matches.length !== 1) {
    throw new Error(
      'Claude container execution requires exactly one sealed Anthropic provider origin.'
    );
  }
  return normalizeSealedAnthropicBaseUrl(matches[0].baseUrl);
}

function assertRequestedAnthropicBaseUrlAllowed(
  requestedBaseUrl: string | undefined,
  sealedBaseUrl: string
): void {
  if (requestedBaseUrl === undefined) return;
  if (normalizeSealedAnthropicBaseUrl(requestedBaseUrl) !== sealedBaseUrl) {
    throw new Error(
      'Claude container execution ANTHROPIC_BASE_URL does not match sealed provider origin.'
    );
  }
}

function isHardDeniedContainerEnvKey(key: string): boolean {
  return (
    CONTAINER_CLAUDE_ENV_HARD_DENY_KEYS.has(key) ||
    key.startsWith('PG') ||
    key.startsWith('AWS_') ||
    key.startsWith('DOCKER_')
  );
}

function assertContainerEnvOverrideSafe(key: string, value: string | undefined): void {
  if (value === undefined) return;
  if (!isHardDeniedContainerEnvKey(key)) return;
  throw new Error(`Claude container execution does not allow request env key ${key}`);
}

function sanitizeContainerRequestEnv(
  requestOptions: SendQueryOptions | undefined
): NodeJS.ProcessEnv {
  const env = buildContainerBaseEnv();
  const requestEnv = requestOptions?.env;
  const sealedBaseUrl = requiredSealedAnthropicBaseUrl(requestOptions?.execContext);
  assertRequestedAnthropicBaseUrlAllowed(requestEnv?.ANTHROPIC_BASE_URL, sealedBaseUrl);
  if (sealedBaseUrl) env.ANTHROPIC_BASE_URL = sealedBaseUrl;
  if (!requestEnv) return env;
  const anthropicKey = requestEnv.ANTHROPIC_API_KEY || requestEnv.CLAUDE_API_KEY;
  for (const [key, value] of Object.entries(requestEnv)) {
    if (key !== 'ANTHROPIC_BASE_URL') assertContainerEnvOverrideSafe(key, value);
    if (value === undefined) continue;
    if (key === 'ANTHROPIC_BASE_URL') continue;
    if (CONTAINER_CLAUDE_ENV_MIRROR_SOURCES.has(key)) continue;
    if (!CONTAINER_CLAUDE_ENV_ALLOWLIST.has(key)) continue;
    env[key] = value;
  }
  if (anthropicKey && !env.ANTHROPIC_API_KEY && !env.CLAUDE_CODE_OAUTH_TOKEN) {
    env.ANTHROPIC_API_KEY = anthropicKey;
    getLog().debug('claude.api_key_mirrored');
  }
  return env;
}

/**
 * Resolve the environment delivered to the Claude subprocess for a request.
 *
 * This is the provider-level env-isolation enforcement point. A container run
 * (`execContext.kind === 'container'`) gets a minimal base plus only the narrow
 * Claude/container values Archon is allowed to provide; arbitrary project env,
 * host process env, and security-sensitive overrides do not cross. A host run
 * inherits the (already-cleaned) host env exactly as before. Exported so the
 * invariant can be unit-tested with a `process.env` canary.
 */
export function buildRequestSubprocessEnv(
  requestOptions: SendQueryOptions | undefined
): NodeJS.ProcessEnv {
  const isContainerRun = requestOptions?.execContext?.kind === 'container';
  if (isContainerRun) return sanitizeContainerRequestEnv(requestOptions);

  const subprocessEnv = buildSubprocessEnv();
  const env = requestOptions?.env ? { ...subprocessEnv, ...requestOptions.env } : subprocessEnv;
  // CLAUDE_API_KEY is Archon's variable name; the Claude Code CLI only reads
  // ANTHROPIC_API_KEY, so mirror it or solo .env installs never authenticate
  // (delivery.ts sets both vars on the per-user api_key path). Guarded on the
  // MERGED env, not process.env: a per-request CLAUDE_CODE_OAUTH_TOKEN (per-user
  // subscription delivered via requestOptions.env) must stay authoritative — the
  // CLI prefers ANTHROPIC_API_KEY over the OAuth token, so injecting the install
  // key alongside it would silently rebill the run. Truthiness is intentional:
  // empty string = missing credential. Never clobbers an explicit ANTHROPIC_API_KEY.
  if (env.CLAUDE_API_KEY && !env.ANTHROPIC_API_KEY && !env.CLAUDE_CODE_OAUTH_TOKEN) {
    env.ANTHROPIC_API_KEY = env.CLAUDE_API_KEY;
    getLog().debug('claude.api_key_mirrored');
  }
  return env;
}

/** Max retries for transient subprocess failures */
const MAX_SUBPROCESS_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 2000;

const RATE_LIMIT_PATTERNS = [
  'rate limit',
  'too many requests',
  '429',
  'overloaded',
  // "API Error: 400 due to tool use concurrency issues" — transient server-side
  // rejection of concurrent tool calls; retrying after backoff succeeds (#1341).
  'tool use concurrency',
];

/**
 * Message-text fallbacks for Anthropic errors the SDK does not yet type.
 *
 * Entries are consulted ONLY when the SDK's typed error code has resolved to
 * the catch-all 'unknown' class (see the ClaudeApiResultError branch in
 * classifyAndEnrichError) — they must never override a typed classification.
 * A matching entry reclassifies the error as rate_limit so the existing
 * backoff-retry applies.
 *
 * Admission contract — each entry must:
 *   1. Name the upstream error it matches.
 *   2. Link an upstream issue/reference requesting the error be properly typed.
 *   3. Be removed once the SDK types it.
 * Do NOT add entries for errors the SDK already classifies.
 *
 * This is deliberately a separate list from RATE_LIMIT_PATTERNS above: that
 * list matches raw subprocess text (no typed code exists at all), while this
 * one is a narrow escape hatch inside the typed classification path (#1797).
 */
const UNTYPED_TRANSIENT_PATTERNS: readonly string[] = [
  // Anthropic 400 "due to tool use concurrency issues" — transient server-side
  // rejection of concurrent tool calls; retrying after backoff succeeds (#1341).
  // TODO: link the upstream SDK issue requesting a typed code for this error,
  // and remove this entry once the SDK classifies it.
  'tool use concurrency',
];

const AUTH_PATTERNS = [
  'credit balance',
  'unauthorized',
  'authentication',
  'invalid token',
  '401',
  '403',
];
const SUBPROCESS_CRASH_PATTERNS = ['exited with code', 'killed', 'signal', 'operation aborted'];

function classifySubprocessError(
  errorMessage: string,
  stderrOutput: string
): 'rate_limit' | 'auth' | 'crash' | 'unknown' {
  const combined = `${errorMessage} ${stderrOutput}`.toLowerCase();
  if (RATE_LIMIT_PATTERNS.some(p => combined.includes(p))) return 'rate_limit';
  if (AUTH_PATTERNS.some(p => combined.includes(p))) return 'auth';
  if (SUBPROCESS_CRASH_PATTERNS.some(p => combined.includes(p))) return 'crash';
  return 'unknown';
}

/**
 * The Claude Code SDK surfaces API-level failures (auth not configured,
 * invalid key, billing, rate limit, model errors) as TEXT rather than
 * throwing: it synthesizes an assistant message (`message.model:
 * '<synthetic>'`, wrapper `error: SDKAssistantMessageError`) whose content is
 * the error prose, then emits a result with `subtype: 'success'` and
 * `is_error: true` — the same field pair as the legitimate stop-sequence
 * termination carve-out (#1425). Without structural detection the error prose
 * flows downstream as successful node output (#1797).
 *
 * This error carries the SDK's typed error code so retry classification is
 * structural — never matched against the message text.
 */
type SdkErrorCode = SDKAssistantMessageError | 'unknown';

export class ClaudeApiResultError extends Error {
  readonly sdkErrorCode: SdkErrorCode;

  constructor(sdkErrorCode: SdkErrorCode, resultText: string) {
    super(`Claude API error (${sdkErrorCode}): ${resultText}`);
    this.name = 'ClaudeApiResultError';
    this.sdkErrorCode = sdkErrorCode;
  }
}

/**
 * Map the SDK's typed assistant-message error code onto the existing
 * subprocess retry classes. Auth-shaped codes are non-retryable (operator
 * must fix credentials); transient API states reuse the existing
 * rate_limit/crash backoff. Everything else is 'unknown' — fail fast rather
 * than retry blindly.
 */
function classifySdkErrorCode(code: SdkErrorCode): 'rate_limit' | 'auth' | 'crash' | 'unknown' {
  switch (code) {
    case 'authentication_failed':
    case 'oauth_org_not_allowed':
    case 'billing_error':
      return 'auth';
    case 'rate_limit':
    case 'overloaded':
      return 'rate_limit';
    case 'server_error':
      return 'crash';
    default:
      return 'unknown';
  }
}

function getFirstEventTimeoutMs(): number {
  const raw = process.env.ARCHON_CLAUDE_FIRST_EVENT_TIMEOUT_MS;
  if (raw) {
    const parsed = Number(raw);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return 60_000;
}

function buildFirstEventHangDiagnostics(
  subprocessEnv: Record<string, string>,
  model: string | undefined
): Record<string, unknown> {
  return {
    subprocessEnvKeys: Object.keys(subprocessEnv),
    parentClaudeKeys: Object.keys(process.env).filter(
      k => k === 'CLAUDECODE' || k.startsWith('CLAUDE_CODE_') || k.startsWith('ANTHROPIC_')
    ),
    model,
    platform: process.platform,
    uid: getProcessUid(),
    isTTY: process.stdout.isTTY ?? false,
    claudeCode: process.env.CLAUDECODE,
    claudeCodeEntrypoint: process.env.CLAUDE_CODE_ENTRYPOINT,
  };
}

class FirstEventTimeoutError extends Error {}

/**
 * Wraps an async generator so that the first call to .next() must resolve
 * within `timeoutMs`. If it doesn't, aborts the controller and throws.
 */
export async function* withFirstMessageTimeout<T>(
  gen: AsyncGenerator<T>,
  controller: AbortController,
  timeoutMs: number,
  diagnostics: Record<string, unknown>
): AsyncGenerator<T> {
  let timerId: ReturnType<typeof setTimeout> | undefined;
  let firstValue: IteratorResult<T>;
  try {
    firstValue = await Promise.race([
      gen.next(),
      new Promise<never>((_, reject) => {
        timerId = setTimeout(() => {
          reject(new FirstEventTimeoutError());
        }, timeoutMs);
      }),
    ]);
  } catch (err) {
    if (err instanceof FirstEventTimeoutError) {
      controller.abort();
      getLog().error({ ...diagnostics, timeoutMs }, 'claude.first_event_timeout');
      throw new Error(
        'Claude Code subprocess produced no output within ' +
          timeoutMs +
          'ms. ' +
          'See logs for claude.first_event_timeout diagnostic dump. ' +
          'Details: https://github.com/coleam00/Archon/issues/1067'
      );
    }
    throw err;
  } finally {
    clearTimeout(timerId);
  }

  if (firstValue.done) return;
  yield firstValue.value;
  yield* gen;
}

/**
 * Returns the current process UID, or undefined on platforms that don't support it.
 */
export function getProcessUid(): number | undefined {
  return typeof process.getuid === 'function' ? process.getuid() : undefined;
}

// ─── SDK Hooks Building (absorbed from dag-executor) ───────────────────────

/** YAML hook matcher shape (matches @archon/workflows/schemas/dag-node WorkflowNodeHooks) */
interface YAMLHookMatcher {
  matcher?: string;
  response: unknown;
  timeout?: number;
}

type SDKHooksMap = Partial<
  Record<
    string,
    {
      matcher?: string;
      hooks: ((
        input: unknown,
        toolUseID: string | undefined,
        options: { signal: AbortSignal }
      ) => Promise<unknown>)[];
      timeout?: number;
    }[]
  >
>;

/**
 * Convert declarative YAML hook definitions to SDK HookCallbackMatcher arrays.
 */
export function buildSDKHooksFromYAML(
  nodeHooks: Record<string, YAMLHookMatcher[] | undefined>
): SDKHooksMap {
  const sdkHooks: SDKHooksMap = {};

  for (const [event, matchers] of Object.entries(nodeHooks)) {
    if (!matchers) continue;
    sdkHooks[event] = matchers.map(m => ({
      ...(m.matcher ? { matcher: m.matcher } : {}),
      hooks: [async (): Promise<unknown> => m.response],
      ...(m.timeout ? { timeout: m.timeout } : {}),
    }));
  }

  if (Object.keys(sdkHooks).length === 0) {
    getLog().warn(
      { nodeHooksKeys: Object.keys(nodeHooks) },
      'claude.hooks_build_produced_empty_map'
    );
  }

  return sdkHooks;
}

// ─── Provider Warning Type ───────────────────────────────────────────────

/**
 * Structured provider warning. Providers collect these during translation;
 * callers convert them to system chunks before streaming starts.
 */
interface ProviderWarning {
  code: string;
  message: string;
}

// ─── NodeConfig → SDK Options Translation ──────────────────────────────────

/**
 * Translate nodeConfig into Claude SDK-specific options.
 * Called inside sendQuery when nodeConfig is present (workflow path).
 * Returns structured warnings that the caller should yield as system chunks.
 */
function applyToolRestrictions(options: Options, nodeConfig: NodeConfig): void {
  if (nodeConfig.allowed_tools !== undefined) options.tools = nodeConfig.allowed_tools;
  if (nodeConfig.denied_tools !== undefined) options.disallowedTools = nodeConfig.denied_tools;
}

function applyHookConfig(options: Options, nodeConfig: NodeConfig): void {
  if (!nodeConfig.hooks) return;
  const builtHooks = buildSDKHooksFromYAML(
    nodeConfig.hooks as Record<string, YAMLHookMatcher[] | undefined>
  );
  if (Object.keys(builtHooks).length === 0) return;
  const existingHooks = options.hooks as SDKHooksMap | undefined;
  if (!options.hooks) (options as Record<string, unknown>).hooks = {};
  for (const [event, matchers] of Object.entries(builtHooks)) {
    if (!matchers) continue;
    const existing = existingHooks?.[event] as HookCallbackMatcher[] | undefined;
    (options.hooks as Record<string, HookCallbackMatcher[]>)[event] = existing
      ? [...(matchers as HookCallbackMatcher[]), ...existing]
      : (matchers as HookCallbackMatcher[]);
  }
}

function warnForMcpMissingVars(missingVars: string[]): ProviderWarning[] {
  if (missingVars.length === 0) return [];
  const uniqueVars = [...new Set(missingVars)];
  getLog().warn({ missingVars: uniqueVars }, 'claude.mcp_env_vars_missing');
  return [
    {
      code: 'mcp_env_vars_missing',
      message: `MCP config references undefined env vars: ${uniqueVars.join(', ')}. These will be empty strings — MCP servers may fail to authenticate.`,
    },
  ];
}

function warnForHaikuMcp(model: string | undefined): ProviderWarning[] {
  if (!model?.toLowerCase().includes('haiku')) return [];
  getLog().warn({ model }, 'claude.mcp_haiku_tool_search_unsupported');
  return [
    {
      code: 'mcp_haiku_tool_search',
      message:
        'Using Haiku model with MCP servers — tool search (lazy loading for many tools) is not supported on Haiku. Consider using Sonnet or Opus.',
    },
  ];
}

async function applyMcpConfig(
  options: Options,
  nodeConfig: NodeConfig,
  cwd: string
): Promise<ProviderWarning[]> {
  if (!nodeConfig.mcp) return [];
  const mcpPath = nodeConfig.mcp;
  const { servers, serverNames, missingVars } = await loadMcpConfig(mcpPath, cwd);
  options.mcpServers = servers as Options['mcpServers'];
  const mcpWildcards = serverNames.map(name => `mcp__${name}__*`);
  options.allowedTools = [...(options.allowedTools ?? []), ...mcpWildcards];
  getLog().info({ serverNames, mcpPath }, 'claude.mcp_config_loaded');
  return [...warnForMcpMissingVars(missingVars), ...warnForHaikuMcp(options.model)];
}

function applySkillsConfig(options: Options, nodeConfig: NodeConfig): void {
  if (!nodeConfig.skills) return;
  const skills = nodeConfig.skills;
  const agentId = 'dag-node-skills';
  const agentDef: {
    description: string;
    prompt: string;
    skills: string[];
    tools?: string[];
    model?: string;
  } = {
    description: 'DAG node with skills',
    prompt: `You have preloaded skills: ${skills.join(', ')}. Use them when relevant.`,
    skills,
  };
  if (options.tools) agentDef.tools = [...(options.tools as string[]), 'Skill'];
  if (options.model) agentDef.model = options.model;
  options.agents = { [agentId]: agentDef };
  options.agent = agentId;
  if (!options.allowedTools?.includes('Skill')) {
    options.allowedTools = [...(options.allowedTools ?? []), 'Skill'];
  }
  getLog().info({ skills, agentId }, 'claude.skills_agent_created');
}

function applyInlineAgentsConfig(options: Options, nodeConfig: NodeConfig): void {
  if (!nodeConfig.agents) return;
  if (
    Object.hasOwn(nodeConfig.agents, 'dag-node-skills') &&
    options.agents?.['dag-node-skills'] !== undefined
  ) {
    getLog().warn(
      { nodeSkills: nodeConfig.skills ?? [] },
      'claude.inline_agents_override_skills_wrapper'
    );
  }
  options.agents = {
    ...options.agents,
    ...(nodeConfig.agents as NonNullable<Options['agents']>),
  };
  getLog().info({ agentIds: Object.keys(nodeConfig.agents) }, 'claude.inline_agents_registered');
}

function applyScalarNodeConfig(options: Options, nodeConfig: NodeConfig): void {
  if (nodeConfig.effort !== undefined) options.effort = nodeConfig.effort as Options['effort'];
  if (nodeConfig.thinking !== undefined)
    options.thinking = nodeConfig.thinking as Options['thinking'];
  if (nodeConfig.sandbox !== undefined) options.sandbox = nodeConfig.sandbox as Options['sandbox'];
  if (nodeConfig.betas !== undefined) options.betas = nodeConfig.betas as Options['betas'];
  if (nodeConfig.output_format) {
    options.outputFormat = {
      type: 'json_schema',
      schema: nodeConfig.output_format,
    } as Options['outputFormat'];
  }
  if (nodeConfig.maxBudgetUsd !== undefined) options.maxBudgetUsd = nodeConfig.maxBudgetUsd;
  if (nodeConfig.systemPrompt !== undefined) options.systemPrompt = nodeConfig.systemPrompt;
  if (nodeConfig.fallbackModel !== undefined) options.fallbackModel = nodeConfig.fallbackModel;
}

function applyAgentProgressSummaries(options: Options, nodeConfig: NodeConfig): void {
  options.agentProgressSummaries = nodeConfig.agentProgressSummaries ?? true;
}

/**
 * Translate nodeConfig into Claude SDK-specific options.
 * Called inside sendQuery when nodeConfig is present (workflow path).
 * Returns structured warnings that the caller should yield as system chunks.
 */
async function applyNodeConfig(
  options: Options,
  nodeConfig: NodeConfig,
  cwd: string
): Promise<ProviderWarning[]> {
  applyToolRestrictions(options, nodeConfig);
  applyHookConfig(options, nodeConfig);
  const warnings = await applyMcpConfig(options, nodeConfig, cwd);
  applySkillsConfig(options, nodeConfig);
  applyInlineAgentsConfig(options, nodeConfig);
  applyScalarNodeConfig(options, nodeConfig);
  applyAgentProgressSummaries(options, nodeConfig);
  return warnings;
}

// ─── Base Options Builder ────────────────────────────────────────────────

/** Queued tool result from SDK hooks, consumed during stream normalization. */
interface ToolResultEntry {
  toolName: string;
  toolOutput: string;
  toolCallId?: string;
  toolOutcome: 'success' | 'error' | 'interrupted';
}

/** Bun-runnable JS extensions. `.ts`/`.tsx`/`.jsx` are excluded — the SDK has
 * never shipped those as entry points, so accepting them would only widen the
 * surface for misconfiguration. */
const BUN_JS_EXTENSIONS = ['.js', '.mjs', '.cjs'] as const;

/**
 * Decide whether the Claude subprocess should be spawned with `--no-env-file`.
 *
 * `--no-env-file` is a Bun flag (consumed by the Bun runtime, not by Claude
 * Code itself) that prevents auto-loading `.env` from the target repo cwd
 * into the spawned process. It only does anything when the SDK spawns a
 * Bun-runnable JS file via `bun cli.js …` — Bun parses the flag and skips
 * its env autoload. For native Claude Code binaries the flag is meaningless
 * and, worse, gets handed to the binary which rejects unknown options.
 *
 * The dev-mode `cliPath === undefined` path used to imply "JS executable"
 * because the SDK shipped `cli.js` inside its package. SDK 0.2.x switched
 * to per-platform native binaries (e.g. `@anthropic-ai/claude-agent-sdk-darwin-arm64/claude`),
 * so dev mode now resolves to a native executable and the historical
 * `undefined → true` heuristic is unsafe. Only return `true` when we have
 * an explicit Bun-runnable JS path (`.js`/`.mjs`/`.cjs`) — i.e. when the
 * operator pointed Archon at a legacy Bun/Node-runnable cli script.
 * Otherwise return `false`.
 *
 * Safety: target-repo `.env` leaks are prevented by `stripCwdEnv()` in
 * `@archon/paths` (#1067), which deletes CWD `.env` keys from
 * `process.env` at every Archon entry point before any subprocess is
 * spawned. The native Claude binary does not auto-load `.env` from its
 * cwd either (verified end-to-end with sentinel keys). `--no-env-file`
 * was belt-and-suspenders for the JS-via-Bun case only.
 *
 * Exported so the decision can be unit-tested without needing to mock
 * `BUNDLED_IS_BINARY` or run the full provider sendQuery pathway.
 */
export function shouldPassNoEnvFile(cliPath: string | undefined): boolean {
  if (cliPath === undefined) return false;
  return BUN_JS_EXTENSIONS.some(ext => cliPath.endsWith(ext));
}

/**
 * Build base Claude SDK options from cwd, request options, and assistant defaults.
 * Does not include nodeConfig translation — that is handled by applyNodeConfig.
 */
function buildStderrHandler(stderrLines: string[]): (data: string) => void {
  return (data: string): void => {
    const output = data.trim();
    if (!output) return;
    stderrLines.push(output);
    if (shouldLogClaudeStderrAsError(output))
      getLog().error({ stderr: output }, 'subprocess_error');
  };
}

function shouldLogClaudeStderrAsError(output: string): boolean {
  const lower = output.toLowerCase();
  const hasErrorSignal =
    lower.includes('error') ||
    lower.includes('fatal') ||
    lower.includes('failed') ||
    lower.includes('exception') ||
    output.includes('at ') ||
    output.includes('Error:');
  const isInfoMessage =
    output.includes('Spawning Claude Code') ||
    output.includes('--output-format') ||
    output.includes('--permission-mode');
  return hasErrorSignal && !isInfoMessage;
}

function resolveContainerExecContext(
  requestOptions: SendQueryOptions | undefined
): Extract<NonNullable<SendQueryOptions['execContext']>, { kind: 'container' }> | undefined {
  return requestOptions?.execContext?.kind === 'container' ? requestOptions.execContext : undefined;
}

function resolveSettingSources(
  requestOptions: SendQueryOptions | undefined,
  assistantDefaults: ReturnType<typeof parseClaudeConfig>,
  hasContainerExecContext: boolean
): Options['settingSources'] {
  if (hasContainerExecContext) return [];
  return (
    requestOptions?.nodeConfig?.settingSources ??
    assistantDefaults.settingSources ?? ['project', 'user']
  );
}

function addHostExecutableOptions(
  options: Options,
  cliPath: string | undefined,
  isJsExecutable: boolean,
  hasContainerExecContext: boolean
): void {
  if (hasContainerExecContext) return;
  if (cliPath !== undefined) options.pathToClaudeCodeExecutable = cliPath;
  if (isJsExecutable) options.executableArgs = ['--no-env-file'];
}

function addOptionalRequestOptions(
  options: Options,
  requestOptions: SendQueryOptions | undefined
): void {
  if (requestOptions?.outputFormat !== undefined)
    options.outputFormat = requestOptions.outputFormat;
  if (requestOptions?.maxBudgetUsd !== undefined)
    options.maxBudgetUsd = requestOptions.maxBudgetUsd;
  if (requestOptions?.fallbackModel !== undefined)
    options.fallbackModel = requestOptions.fallbackModel;
  if (requestOptions?.persistSession !== undefined)
    options.persistSession = requestOptions.persistSession;
  if (requestOptions?.forkSession !== undefined) options.forkSession = requestOptions.forkSession;
}

/**
 * Build base Claude SDK options from cwd, request options, and assistant defaults.
 * Does not include nodeConfig translation — that is handled by applyNodeConfig.
 */
function buildBaseClaudeOptions(
  cwd: string,
  requestOptions: SendQueryOptions | undefined,
  assistantDefaults: ReturnType<typeof parseClaudeConfig>,
  controller: AbortController,
  stderrLines: string[],
  toolResultQueue: ToolResultEntry[],
  env: NodeJS.ProcessEnv,
  cliPath: string | undefined
): Options {
  const isJsExecutable = shouldPassNoEnvFile(cliPath);
  getLog().debug({ cliPath: cliPath ?? null, isJsExecutable }, 'claude.subprocess_env_file_flag');
  const containerExecContext = resolveContainerExecContext(requestOptions);
  const hasContainerExecContext = containerExecContext !== undefined;
  const options: Options = {
    cwd,
    env,
    model: requestOptions?.model ?? assistantDefaults.model,
    abortController: controller,
    permissionMode: 'bypassPermissions',
    allowDangerouslySkipPermissions: true,
    systemPrompt: requestOptions?.systemPrompt ?? { type: 'preset', preset: 'claude_code' },
    settingSources: resolveSettingSources(
      requestOptions,
      assistantDefaults,
      hasContainerExecContext
    ),
    hooks: buildToolCaptureHooks(toolResultQueue),
    stderr: buildStderrHandler(stderrLines),
  };
  if (containerExecContext)
    options.spawnClaudeCodeProcess = buildContainerSpawn(containerExecContext, env);
  addHostExecutableOptions(options, cliPath, isJsExecutable, hasContainerExecContext);
  addOptionalRequestOptions(options, requestOptions);
  return options;
}

function assertClaudeContainerRequestSupported(
  requestOptions: SendQueryOptions | undefined,
  assistantDefaults: ReturnType<typeof parseClaudeConfig>
): void {
  if (requestOptions?.execContext?.kind !== 'container') return;
  if (requestOptions.nodeConfig?.mcp) {
    throw new Error(
      'Claude container execution does not support MCP config until controller-pinned MCP settings are implemented.'
    );
  }
  if (requestOptions.nativeTools && requestOptions.nativeTools.length > 0) {
    throw new Error(
      'Claude container execution does not support native tools until controller-pinned MCP settings are implemented.'
    );
  }
  if (requestOptions.nodeConfig?.hooks) {
    throw new Error(
      'Claude container execution does not support node hooks until controller-pinned hooks are implemented.'
    );
  }
  const settingSources =
    requestOptions.nodeConfig?.settingSources ?? assistantDefaults.settingSources;
  if (settingSources && settingSources.length > 0) {
    throw new Error(
      'Claude container execution does not support settingSources until controller-pinned settings bundles are implemented.'
    );
  }
}

// ─── Tool Capture Hooks ──────────────────────────────────────────────────

/**
 * Build SDK hooks that capture tool use results into a shared queue.
 * The queue is drained during stream normalization.
 */
function buildToolCaptureHooks(toolResultQueue: ToolResultEntry[]): Options['hooks'] {
  return {
    PostToolUse: [
      {
        hooks: [
          (async (input: Record<string, unknown>): Promise<{ continue: true }> => {
            try {
              const toolName = (input as { tool_name?: string }).tool_name ?? 'unknown';
              const toolUseId = (input as { tool_use_id?: string }).tool_use_id;
              const toolResponse = (input as { tool_response?: unknown }).tool_response;
              const output =
                typeof toolResponse === 'string'
                  ? toolResponse
                  : JSON.stringify(toolResponse ?? '');
              const maxLen = 10_000;
              toolResultQueue.push({
                toolName,
                toolOutput: output.length > maxLen ? output.slice(0, maxLen) + '...' : output,
                ...(toolUseId !== undefined ? { toolCallId: toolUseId } : {}),
                toolOutcome: 'success',
              });
            } catch (e) {
              getLog().error({ err: e, input }, 'claude.post_tool_use_hook_error');
            }
            return { continue: true };
          }) as HookCallback,
        ],
      },
    ],
    PostToolUseFailure: [
      {
        hooks: [
          (async (input: Record<string, unknown>): Promise<{ continue: true }> => {
            try {
              const toolName = (input as { tool_name?: string }).tool_name ?? 'unknown';
              const toolUseId = (input as { tool_use_id?: string }).tool_use_id;
              const rawError = (input as { error?: string }).error;
              if (rawError === undefined) {
                getLog().debug({ input }, 'claude.post_tool_use_failure_no_error_field');
              }
              const errorText = rawError ?? 'tool failed';
              const isInterrupt = (input as { is_interrupt?: boolean }).is_interrupt === true;
              const prefix = isInterrupt ? '⚠️ Interrupted' : '❌ Error';
              toolResultQueue.push({
                toolName,
                toolOutput: `${prefix}: ${errorText}`,
                ...(toolUseId !== undefined ? { toolCallId: toolUseId } : {}),
                toolOutcome: isInterrupt ? 'interrupted' : 'error',
              });
            } catch (e) {
              getLog().error({ err: e, input }, 'claude.post_tool_use_failure_hook_error');
            }
            return { continue: true };
          }) as HookCallback,
        ],
      },
    ],
  };
}

// ─── Stream Normalizer ───────────────────────────────────────────────────

/**
 * Normalize raw Claude SDK events into Archon MessageChunks.
 * Drains the tool result queue between events (populated by SDK hooks).
 */
interface PendingSdkError {
  code: SDKAssistantMessageError;
  text: string;
}

interface ClaudeSystemMessage {
  subtype?: string;
  mcp_servers?: { name: string; status: string }[];
  task_id?: string;
  tool_use_id?: string;
  description?: string;
  task_type?: string;
  prompt?: string;
  summary?: string;
  usage?: { total_tokens: number; tool_uses: number; duration_ms: number };
  last_tool_name?: string;
  status?: string;
  output_file?: string;
  skip_transcript?: boolean;
  tasks?: { task_id: string; task_type: string; description: string }[];
  hook_id?: string;
  hook_name?: string;
  hook_event?: string;
  outcome?: 'success' | 'error' | 'cancelled';
  exit_code?: number;
}

function* drainToolResultQueue(toolResultQueue: ToolResultEntry[]): Generator<MessageChunk> {
  while (toolResultQueue.length > 0) {
    const tr = toolResultQueue.shift();
    if (!tr) continue;
    yield {
      type: 'tool_result',
      toolName: tr.toolName,
      toolOutput: tr.toolOutput,
      ...(tr.toolCallId !== undefined ? { toolCallId: tr.toolCallId } : {}),
      toolOutcome: tr.toolOutcome,
    };
  }
}

function extractSyntheticSdkError(msg: unknown): PendingSdkError | undefined {
  const message = msg as {
    message: { content: ContentBlock[]; model?: string };
    error?: SDKAssistantMessageError;
  };
  if (message.error === undefined || message.message.model !== '<synthetic>') return undefined;
  const text = message.message.content
    .filter(b => b.type === 'text' && b.text)
    .map(b => b.text)
    .join('\n');
  getLog().warn({ errorCode: message.error, text }, 'claude.synthetic_error_message');
  return { code: message.error, text };
}

function* normalizeAssistantMessage(msg: unknown): Generator<MessageChunk> {
  const message = msg as { message: { content: ContentBlock[] } };
  for (const block of message.message.content) {
    if (block.type === 'text' && block.text) {
      yield { type: 'assistant', content: block.text };
    } else if (block.type === 'tool_use' && block.name) {
      yield {
        type: 'tool',
        toolName: block.name,
        toolInput: block.input ?? {},
        ...(block.id !== undefined ? { toolCallId: block.id } : {}),
      };
    }
  }
}

function* normalizeSystemInit(sysMsg: ClaudeSystemMessage): Generator<MessageChunk> {
  const failed = sysMsg.mcp_servers?.filter(s => s.status !== 'connected') ?? [];
  if (failed.length === 0) return;
  const names = failed.map(s => `${s.name} (${s.status})`).join(', ');
  yield { type: 'system', content: `MCP server connection failed: ${names}` };
}

function* normalizeTaskStarted(sysMsg: ClaudeSystemMessage): Generator<MessageChunk> {
  if (!sysMsg.task_id) return;
  if (sysMsg.skip_transcript === true) {
    getLog().debug(
      { taskId: sysMsg.task_id, taskType: sysMsg.task_type },
      'claude.task_started_housekeeping_suppressed'
    );
    return;
  }
  yield {
    type: 'task_started',
    taskId: sysMsg.task_id,
    description: sysMsg.description ?? '',
    ...(sysMsg.task_type !== undefined ? { taskType: sysMsg.task_type } : {}),
    ...(sysMsg.prompt !== undefined ? { prompt: sysMsg.prompt } : {}),
    ...(sysMsg.tool_use_id !== undefined ? { toolUseId: sysMsg.tool_use_id } : {}),
  };
}

function* normalizeTaskProgress(sysMsg: ClaudeSystemMessage): Generator<MessageChunk> {
  if (!sysMsg.task_id) return;
  yield {
    type: 'task_progress',
    taskId: sysMsg.task_id,
    description: sysMsg.description ?? '',
    ...(sysMsg.summary !== undefined ? { summary: sysMsg.summary } : {}),
    ...(sysMsg.usage !== undefined ? { usage: sysMsg.usage } : {}),
    ...(sysMsg.last_tool_name !== undefined ? { lastToolName: sysMsg.last_tool_name } : {}),
    ...(sysMsg.tool_use_id !== undefined ? { toolUseId: sysMsg.tool_use_id } : {}),
  };
}

function normalizeTaskNotificationStatus(
  status: string | undefined
): 'completed' | 'failed' | 'stopped' {
  if (status === 'completed' || status === 'failed' || status === 'stopped') return status;
  return 'stopped';
}

function* normalizeTaskNotification(sysMsg: ClaudeSystemMessage): Generator<MessageChunk> {
  if (!sysMsg.task_id) return;
  const status = sysMsg.status;
  if (status !== 'completed' && status !== 'failed' && status !== 'stopped') {
    getLog().warn({ taskId: sysMsg.task_id, status }, 'claude.task_notification_unknown_status');
  }
  yield {
    type: 'task_notification',
    taskId: sysMsg.task_id,
    status: normalizeTaskNotificationStatus(status),
    summary: sysMsg.summary ?? '',
    outputFile: sysMsg.output_file ?? '',
    ...(sysMsg.usage !== undefined ? { usage: sysMsg.usage } : {}),
    ...(sysMsg.tool_use_id !== undefined ? { toolUseId: sysMsg.tool_use_id } : {}),
  };
}

function* normalizeBackgroundTasks(sysMsg: ClaudeSystemMessage): Generator<MessageChunk> {
  const tasks = Array.isArray(sysMsg.tasks) ? sysMsg.tasks : [];
  yield {
    type: 'background_tasks',
    tasks: tasks.map(t => ({
      taskId: t.task_id,
      taskType: t.task_type,
      description: t.description,
    })),
  };
}

function* normalizeHookStarted(sysMsg: ClaudeSystemMessage): Generator<MessageChunk> {
  if (!sysMsg.hook_id) return;
  yield {
    type: 'hook_started',
    hookId: sysMsg.hook_id,
    hookName: sysMsg.hook_name ?? '',
    hookEvent: sysMsg.hook_event ?? '',
  };
}

function normalizeHookOutcome(
  outcome: ClaudeSystemMessage['outcome']
): 'success' | 'error' | 'cancelled' {
  if (outcome === 'success' || outcome === 'error' || outcome === 'cancelled') return outcome;
  return 'error';
}

function* normalizeHookResponse(sysMsg: ClaudeSystemMessage): Generator<MessageChunk> {
  if (!sysMsg.hook_id) return;
  yield {
    type: 'hook_response',
    hookId: sysMsg.hook_id,
    hookName: sysMsg.hook_name ?? '',
    hookEvent: sysMsg.hook_event ?? '',
    outcome: normalizeHookOutcome(sysMsg.outcome),
    ...(sysMsg.exit_code !== undefined ? { exitCode: sysMsg.exit_code } : {}),
  };
}

function* normalizeSystemMessage(msg: unknown): Generator<MessageChunk> {
  const sysMsg = msg as ClaudeSystemMessage;
  switch (sysMsg.subtype) {
    case 'init':
      yield* normalizeSystemInit(sysMsg);
      return;
    case 'task_started':
      yield* normalizeTaskStarted(sysMsg);
      return;
    case 'task_progress':
      yield* normalizeTaskProgress(sysMsg);
      return;
    case 'task_notification':
      yield* normalizeTaskNotification(sysMsg);
      return;
    case 'background_tasks_changed':
      yield* normalizeBackgroundTasks(sysMsg);
      return;
    case 'hook_started':
      yield* normalizeHookStarted(sysMsg);
      return;
    case 'hook_response':
      yield* normalizeHookResponse(sysMsg);
      return;
    default:
      getLog().debug({ subtype: sysMsg.subtype }, 'claude.system_message_unhandled');
  }
}

function throwConfirmedSyntheticError(
  resultMsg: SDKResultMessage,
  syntheticError: PendingSdkError | undefined,
  sdkErrors: string[] | undefined
): void {
  const code = syntheticError?.code ?? 'unknown';
  const resultText = 'result' in resultMsg ? resultMsg.result : undefined;
  const text =
    syntheticError?.text ||
    resultText ||
    sdkErrors?.join('; ') ||
    'API error result with no error text';
  getLog().error(
    {
      sessionId: resultMsg.session_id,
      errorCode: code,
      terminalReason: resultMsg.terminal_reason,
      apiErrorStatus: 'api_error_status' in resultMsg ? resultMsg.api_error_status : undefined,
      text,
    },
    'claude.result_api_error'
  );
  throw new ClaudeApiResultError(code, text);
}

function logResultStatus(resultMsg: SDKResultMessage, isSuccessWithErrorFlag: boolean): void {
  const sdkErrors = 'errors' in resultMsg ? resultMsg.errors : undefined;
  const isRealError = resultMsg.is_error && !isSuccessWithErrorFlag;
  if (isRealError) {
    getLog().error(
      {
        sessionId: resultMsg.session_id,
        errorSubtype: resultMsg.subtype,
        stopReason: resultMsg.stop_reason,
        errors: sdkErrors,
      },
      'claude.result_is_error'
    );
  } else if (isSuccessWithErrorFlag) {
    getLog().debug(
      { sessionId: resultMsg.session_id, stopReason: resultMsg.stop_reason },
      'claude.result_success_validated'
    );
  }
}

function* normalizeResultMessage(
  msg: unknown,
  syntheticError: PendingSdkError | undefined
): Generator<MessageChunk> {
  const resultMsg = msg as SDKResultMessage;
  const resolvedModelId = selectResolvedModelId(resultMsg.modelUsage);
  const tokens = normalizeClaudeUsage(resultMsg.usage);
  const sdkErrors = 'errors' in resultMsg ? resultMsg.errors : undefined;
  const isSuccessWithErrorFlag = resultMsg.is_error && resultMsg.subtype === 'success';
  if (isSuccessWithErrorFlag && (syntheticError || resultMsg.terminal_reason === 'api_error')) {
    throwConfirmedSyntheticError(resultMsg, syntheticError, sdkErrors);
  }
  if (syntheticError !== undefined && !resultMsg.is_error) {
    getLog().warn(
      { sessionId: resultMsg.session_id, errorCode: syntheticError.code },
      'claude.synthetic_error_not_confirmed'
    );
    yield { type: 'assistant', content: syntheticError.text };
  }
  const isRealError = resultMsg.is_error && !isSuccessWithErrorFlag;
  logResultStatus(resultMsg, isSuccessWithErrorFlag);
  yield {
    type: 'result',
    sessionId: resultMsg.session_id,
    ...(tokens ? { tokens } : {}),
    ...('structured_output' in resultMsg && resultMsg.structured_output !== undefined
      ? { structuredOutput: resultMsg.structured_output }
      : {}),
    ...(isRealError ? { isError: true, errorSubtype: resultMsg.subtype } : {}),
    ...(isRealError && sdkErrors?.length ? { errors: sdkErrors } : {}),
    ...(resultMsg.total_cost_usd !== undefined ? { cost: resultMsg.total_cost_usd } : {}),
    ...(resultMsg.stop_reason != null ? { stopReason: resultMsg.stop_reason } : {}),
    ...(resultMsg.num_turns !== undefined ? { numTurns: resultMsg.num_turns } : {}),
    ...(resolvedModelId ? { resolvedModel: { id: resolvedModelId } } : {}),
  };
}

function* normalizeRateLimitMessage(msg: unknown): Generator<MessageChunk> {
  const rateLimitMsg = msg as { rate_limit_info?: Record<string, unknown> };
  getLog().warn({ rateLimitInfo: rateLimitMsg.rate_limit_info }, 'claude.rate_limit_event');
  yield { type: 'rate_limit', rateLimitInfo: rateLimitMsg.rate_limit_info ?? {} };
}

/**
 * Normalize raw Claude SDK events into Archon MessageChunks.
 * Drains the tool result queue between events (populated by SDK hooks).
 */
async function* streamClaudeMessages(
  events: AsyncGenerator,
  toolResultQueue: ToolResultEntry[]
): AsyncGenerator<MessageChunk> {
  let pendingSdkError: PendingSdkError | undefined;
  for await (const msg of events) {
    yield* drainToolResultQueue(toolResultQueue);
    const event = msg as { type: string };
    if (event.type === 'assistant') {
      const syntheticError = extractSyntheticSdkError(msg);
      if (syntheticError) {
        pendingSdkError = syntheticError;
        continue;
      }
      yield* normalizeAssistantMessage(msg);
    } else if (event.type === 'system') {
      yield* normalizeSystemMessage(msg);
    } else if (event.type === 'rate_limit_event') {
      yield* normalizeRateLimitMessage(msg);
    } else if (event.type === 'result') {
      const syntheticError = pendingSdkError;
      pendingSdkError = undefined;
      yield* normalizeResultMessage(msg, syntheticError);
    }
  }
  if (pendingSdkError !== undefined) {
    getLog().error(
      { errorCode: pendingSdkError.code, text: pendingSdkError.text },
      'claude.synthetic_error_stream_ended'
    );
    throw new ClaudeApiResultError(pendingSdkError.code, pendingSdkError.text);
  }
  yield* drainToolResultQueue(toolResultQueue);
}

// ─── Error Classification & Retry ────────────────────────────────────────

/**
 * Classify a subprocess error and enrich with stderr context.
 * Returns null if the error should be retried (caller handles retry logic).
 */
function classifyAndEnrichError(
  error: Error,
  stderrLines: string[],
  controller: AbortController
): { enrichedError: Error; errorClass: string; shouldRetry: boolean } {
  // If the controller was aborted by withFirstMessageTimeout, the original
  // timeout error carries the diagnostic message and #1067 breadcrumb.
  // Preserve it instead of collapsing into a generic "Query aborted".
  if (controller.signal.aborted) {
    if (error.message.includes('produced no output within')) {
      return { enrichedError: error, errorClass: 'timeout', shouldRetry: false };
    }
    return {
      enrichedError: new Error('Query aborted'),
      errorClass: 'aborted',
      shouldRetry: false,
    };
  }

  // API failures the SDK surfaced as text (#1797) carry a typed error code —
  // classify by that code, never by matching the (arbitrary) message text.
  if (error instanceof ClaudeApiResultError) {
    let errorClass = classifySdkErrorCode(error.sdkErrorCode);
    // Exception for the SDK's catch-all codes only ('unknown'/'invalid_request'
    // — a 400 status maps here): they conflate transient server-side rejections
    // with true client errors, so the code alone carries no retry signal. For
    // those, and ONLY those, fall back to UNTYPED_TRANSIENT_PATTERNS (see its
    // admission contract) to reclassify known-transient errors as rate_limit
    // so the existing backoff applies (#1341). Specific typed codes above
    // remain authoritative and are never overridden by text.
    if (errorClass === 'unknown') {
      const message = error.message.toLowerCase();
      if (UNTYPED_TRANSIENT_PATTERNS.some(p => message.includes(p))) {
        errorClass = 'rate_limit';
      }
    }
    return {
      enrichedError: error,
      errorClass,
      shouldRetry: errorClass === 'rate_limit' || errorClass === 'crash',
    };
  }

  const stderrContext = stderrLines.join('\n');
  const errorClass = classifySubprocessError(error.message, stderrContext);

  if (errorClass === 'auth') {
    const enrichedError = new Error(
      `Claude Code auth error: ${error.message}${stderrContext ? ` (${stderrContext})` : ''}`
    );
    enrichedError.cause = error;
    return { enrichedError, errorClass, shouldRetry: false };
  }

  const enrichedMessage = stderrContext
    ? `Claude Code ${errorClass}: ${error.message} (stderr: ${stderrContext})`
    : `Claude Code ${errorClass}: ${error.message}`;
  const enrichedError = new Error(enrichedMessage);
  enrichedError.cause = error;
  const shouldRetry = errorClass === 'rate_limit' || errorClass === 'crash';
  return { enrichedError, errorClass, shouldRetry };
}

// ─── Claude Provider ───────────────────────────────────────────────────────

interface PreparedClaudeSendQuery {
  assistantDefaults: ReturnType<typeof parseClaudeConfig>;
  resolvedCliPath: string | undefined;
  env: NodeJS.ProcessEnv;
  nodeConfigWarnings: ProviderWarning[];
  containerExecContext?: Extract<
    NonNullable<SendQueryOptions['execContext']>,
    { kind: 'container' }
  >;
}

class RetryableClaudeAttemptError extends Error {
  constructor(readonly original: Error) {
    super(original.message);
    this.name = 'RetryableClaudeAttemptError';
  }
}

interface PreparedAttemptFailure {
  error: Error;
  retry: boolean;
}

interface ClaudeAttemptParams {
  prompt: string;
  cwd: string;
  resumeSessionId: string | undefined;
  requestOptions: SendQueryOptions | undefined;
  prepared: PreparedClaudeSendQuery;
  retryBaseDelayMs: number;
  stopContainer: (reason: 'abort' | 'deadline') => Promise<void> | undefined;
  setCurrentController: (controller: AbortController | undefined) => void;
}

async function prepareClaudeSendQuery(
  cwd: string,
  requestOptions: SendQueryOptions | undefined
): Promise<PreparedClaudeSendQuery> {
  const assistantDefaults = parseClaudeConfig(requestOptions?.assistantConfig ?? {});
  assertClaudeContainerRequestSupported(requestOptions, assistantDefaults);
  const containerExecContext = resolveContainerExecContext(requestOptions);
  const resolvedCliPath = containerExecContext
    ? undefined
    : await resolveClaudeBinaryPath(assistantDefaults.claudeBinaryPath);
  const env = buildRequestSubprocessEnv(requestOptions);
  const nodeConfigWarnings = requestOptions?.nodeConfig
    ? await applyNodeConfig({} as Options, requestOptions.nodeConfig, cwd)
    : [];
  return { assistantDefaults, resolvedCliPath, env, nodeConfigWarnings, containerExecContext };
}

function createContainerStopper(
  containerExecContext: PreparedClaudeSendQuery['containerExecContext'],
  stopContainerOnAbort: typeof defaultStopContainerOnAbort
): (reason: 'abort' | 'deadline') => Promise<void> | undefined {
  let containerStopPromise: Promise<void> | undefined;
  return (reason: 'abort' | 'deadline'): Promise<void> | undefined => {
    if (!containerExecContext) return undefined;
    if (!containerStopPromise) {
      containerStopPromise = stopContainerOnAbort(containerExecContext, reason);
      void containerStopPromise.catch(() => undefined);
    }
    return containerStopPromise;
  };
}

function createAttemptOptions(
  params: ClaudeAttemptParams,
  controller: AbortController,
  stderrLines: string[],
  toolResultQueue: ToolResultEntry[]
): Options {
  return buildBaseClaudeOptions(
    params.cwd,
    params.requestOptions,
    params.prepared.assistantDefaults,
    controller,
    stderrLines,
    toolResultQueue,
    params.prepared.env,
    params.prepared.resolvedCliPath
  );
}

async function applyPerAttemptOptions(
  options: Options,
  params: ClaudeAttemptParams,
  attempt: number
): Promise<void> {
  if (params.requestOptions?.nodeConfig) {
    await applyNodeConfig(options, params.requestOptions.nodeConfig, params.cwd);
  }
  registerNativeTools(options, params.requestOptions);
  applyResumeOptions(options, params.resumeSessionId, params.requestOptions, params.cwd, attempt);
}

function registerNativeTools(options: Options, requestOptions: SendQueryOptions | undefined): void {
  if (!requestOptions?.nativeTools || requestOptions.nativeTools.length === 0) return;
  const server = buildArchonMcpServer(requestOptions.nativeTools);
  options.mcpServers = { ...options.mcpServers, [ARCHON_TOOL_SERVER]: server };
  options.allowedTools = [...(options.allowedTools ?? []), `mcp__${ARCHON_TOOL_SERVER}__*`];
  getLog().info({ count: requestOptions.nativeTools.length }, 'claude.native_tools_registered');
}

function applyResumeOptions(
  options: Options,
  resumeSessionId: string | undefined,
  requestOptions: SendQueryOptions | undefined,
  cwd: string,
  attempt: number
): void {
  if (resumeSessionId) {
    options.resume = resumeSessionId;
    getLog().debug(
      { sessionId: resumeSessionId, forkSession: requestOptions?.forkSession },
      'resuming_session'
    );
    return;
  }
  getLog().debug({ cwd, attempt }, 'starting_new_session');
}

async function* runClaudeAttempt(
  params: ClaudeAttemptParams,
  attempt: number
): AsyncGenerator<MessageChunk> {
  const stderrLines: string[] = [];
  const toolResultQueue: ToolResultEntry[] = [];
  const controller = new AbortController();
  params.setCurrentController(controller);
  const options = createAttemptOptions(params, controller, stderrLines, toolResultQueue);
  await applyPerAttemptOptions(options, params, attempt);
  try {
    const rawEvents = query({ prompt: params.prompt, options });
    const timeoutMs = getFirstEventTimeoutMs();
    const diagnostics = buildFirstEventHangDiagnostics(
      options.env as Record<string, string>,
      options.model
    );
    const events = withFirstMessageTimeout(rawEvents, controller, timeoutMs, diagnostics);
    yield* withResumedOutcome(
      streamClaudeMessages(events, toolResultQueue),
      resumedOutcome(params.resumeSessionId, true)
    );
  } catch (error) {
    const failure = await prepareAttemptError(
      error as Error,
      stderrLines,
      controller,
      params,
      attempt
    );
    throw failure.retry ? new RetryableClaudeAttemptError(failure.error) : failure.error;
  }
}

async function prepareAttemptError(
  err: Error,
  stderrLines: string[],
  controller: AbortController,
  params: ClaudeAttemptParams,
  attempt: number
): Promise<PreparedAttemptFailure> {
  const stopPromise = controller.signal.aborted
    ? params.stopContainer(err.message.includes('produced no output within') ? 'deadline' : 'abort')
    : undefined;
  const { enrichedError, errorClass, shouldRetry } = classifyAndEnrichError(
    err,
    stderrLines,
    controller
  );
  if (stopPromise) await stopPromise;
  getLog().error(
    {
      err,
      stderrContext: stderrLines.join('\n'),
      errorClass,
      attempt,
      maxRetries: MAX_SUBPROCESS_RETRIES,
    },
    'query_error'
  );
  if (!shouldRetry || attempt >= MAX_SUBPROCESS_RETRIES) {
    return { error: enrichedError, retry: false };
  }
  const delayMs = params.retryBaseDelayMs * Math.pow(2, attempt);
  getLog().info({ attempt, delayMs, errorClass }, 'retrying_subprocess');
  await new Promise(resolve => setTimeout(resolve, delayMs));
  return { error: enrichedError, retry: true };
}

async function* runClaudeAttempts(params: ClaudeAttemptParams): AsyncGenerator<MessageChunk> {
  let lastError: Error | undefined;
  for (let attempt = 0; attempt <= MAX_SUBPROCESS_RETRIES; attempt++) {
    if (params.requestOptions?.abortSignal?.aborted) throw new Error('Query aborted');
    try {
      yield* runClaudeAttempt(params, attempt);
      return;
    } catch (error) {
      if (!(error instanceof RetryableClaudeAttemptError)) throw error;
      lastError = error.original;
    }
  }
  throw lastError ?? new Error('Claude Code query failed after retries');
}

async function* runPreparedClaudeQuery(params: ClaudeAttemptParams): AsyncGenerator<MessageChunk> {
  for (const warning of params.prepared.nodeConfigWarnings) {
    yield { type: 'system' as const, content: `⚠️ ${warning.message}` };
  }
  yield* runClaudeAttempts(params);
}

async function* runClaudeQueryWithAbortHandling(
  params: Omit<ClaudeAttemptParams, 'stopContainer' | 'setCurrentController'>,
  stopContainerOnAbort: typeof defaultStopContainerOnAbort
): AsyncGenerator<MessageChunk> {
  let currentController: AbortController | undefined;
  const stopContainer = createContainerStopper(
    params.prepared.containerExecContext,
    stopContainerOnAbort
  );
  const onAbort = (): void => {
    currentController?.abort();
    void stopContainer('abort');
  };
  params.requestOptions?.abortSignal?.addEventListener('abort', onAbort, { once: true });
  try {
    yield* runPreparedClaudeQuery({
      ...params,
      stopContainer,
      setCurrentController: controller => {
        currentController = controller;
      },
    });
  } finally {
    params.requestOptions?.abortSignal?.removeEventListener('abort', onAbort);
    currentController = undefined;
  }
}

/**
 * Claude AI agent provider.
 * Implements IAgentProvider with full SDK integration.
 *
 * sendQuery orchestrates the following internal helpers:
 * - buildBaseClaudeOptions: SDK option construction
 * - applyNodeConfig: workflow nodeConfig → SDK option translation + warnings
 * - streamClaudeMessages: raw SDK event normalization into MessageChunks
 * - classifyAndEnrichError: error classification for retry decisions
 */
export class ClaudeProvider implements IAgentProvider {
  private readonly retryBaseDelayMs: number;
  private readonly stopContainerOnAbort: typeof defaultStopContainerOnAbort;

  constructor(options?: {
    retryBaseDelayMs?: number;
    stopContainerOnAbort?: typeof defaultStopContainerOnAbort;
  }) {
    if (getProcessUid() === 0 && process.env.IS_SANDBOX !== '1') {
      throw new Error(
        'Claude Code SDK does not support bypassPermissions when running as root (UID 0). ' +
          'Run as a non-root user, set IS_SANDBOX=1, or use the Dockerfile which creates a non-root appuser.'
      );
    }
    this.retryBaseDelayMs = options?.retryBaseDelayMs ?? RETRY_BASE_DELAY_MS;
    this.stopContainerOnAbort = options?.stopContainerOnAbort ?? defaultStopContainerOnAbort;
  }

  getCapabilities(): ProviderCapabilities {
    return CLAUDE_CAPABILITIES;
  }

  /**
   * Send a query to Claude and stream responses.
   * Orchestrates option building, nodeConfig translation, streaming, and retry.
   */
  // Host requests preserve existing provider behavior. Container requests fail
  // closed here before SDK option construction can load project/user settings,
  // MCP files, hooks, or native in-process tools.
  async *sendQuery(
    prompt: string,
    cwd: string,
    resumeSessionId?: string,
    requestOptions?: SendQueryOptions
  ): AsyncGenerator<MessageChunk> {
    const prepared = await prepareClaudeSendQuery(cwd, requestOptions);
    yield* runClaudeQueryWithAbortHandling(
      {
        prompt,
        cwd,
        resumeSessionId,
        requestOptions,
        prepared,
        retryBaseDelayMs: this.retryBaseDelayMs,
      },
      this.stopContainerOnAbort
    );
  }

  getType(): string {
    return 'claude';
  }
}
