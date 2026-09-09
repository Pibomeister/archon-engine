/**
 * Codex SDK wrapper
 * Provides async generator interface for streaming Codex responses
 */
import { mkdtemp, writeFile, chmod, rm } from 'fs/promises';
import { randomBytes } from 'crypto';
import { tmpdir } from 'os';
import { dirname, join } from 'path';
import { isIP } from 'node:net';
import {
  Codex,
  type CodexOptions,
  type ThreadOptions,
  type TurnOptions,
  type TurnCompletedEvent,
  type ThreadStartedEvent,
} from '@openai/codex-sdk';
import type {
  IAgentProvider,
  SendQueryOptions,
  MessageChunk,
  TokenUsage,
  ProviderCapabilities,
  ExecutionContext,
} from '../types';
import { parseCodexConfig } from './config';
import { CODEX_CAPABILITIES } from './capabilities';
import { resolveCodexBinaryPath } from './binary-resolver';
import { createLogger } from '@archon/paths';
import { loadMcpConfig } from '../mcp/config';
import {
  hasOpenAdditionalProperties,
  normalizeJsonSchemaForOpenAiStrict,
} from '../shared/structured-output';
import { withResumedOutcome, resumedOutcome } from '../shared/resumed';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.codex');
  return cachedLog;
}

type CodexConfigOverrides = NonNullable<CodexOptions['config']>;
type CodexConfigValue = CodexConfigOverrides[string];
type CodexThread = ReturnType<Codex['startThread']>;

interface ProviderWarning {
  code: string;
  message: string;
}

interface CodexAttemptInput {
  codex: Codex;
  thread: CodexThread;
  threadOptions: ThreadOptions;
  turnOptions: TurnOptions;
  hasOutputFormat: boolean;
  effectivePrompt: string;
  cwd: string;
  resumeSessionId: string | undefined;
  sessionResumeFailed: boolean;
  requestOptions: SendQueryOptions | undefined;
}

interface CodexAttempt {
  index: number;
  controller: AbortController;
  dispose: () => void;
}

const CODEX_CONTAINER_ENV_ALLOWLIST = [
  'CODEX_API_KEY',
  'OPENAI_API_KEY',
  'OPENAI_BASE_URL',
  'CODEX_INTERNAL_ORIGINATOR_OVERRIDE',
] as const;

const CODEX_OPENAI_PROVIDER_NAMES = new Set(['openai', 'codex']);

function normalizeSealedOpenAiBaseUrl(rawBaseUrl: string): string {
  let url: URL;
  try {
    url = new URL(rawBaseUrl);
  } catch {
    throw new Error('Codex hardened provider origin is malformed.');
  }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
    throw new Error('Codex hardened provider origin must be an exact HTTPS API root.');
  }
  if (url.pathname.replace(/\/$/, '') !== '/v1') {
    throw new Error('Codex hardened provider origin must use the fixed /v1 API path.');
  }
  assertPublicHostname(url.hostname, 'Codex hardened provider origin');
  return url.toString().replace(/\/$/, '');
}

function assertPublicHostname(hostname: string, label: string): void {
  const normalized = hostname.toLowerCase().replace(/^\[(.*)]$/, '$1');
  if (normalized === 'localhost' || normalized.endsWith('.localhost')) {
    throw new Error(`${label} cannot target localhost.`);
  }
  if (isIP(normalized) !== 0) {
    throw new Error(`${label} cannot target an IP literal.`);
  }
  if (!isDnsHostname(normalized)) {
    throw new Error(`${label} must use an exact DNS hostname.`);
  }
}

function isDnsHostname(hostname: string): boolean {
  if (hostname.length < 1 || hostname.length > 253) return false;
  const withoutRootDot = hostname.endsWith('.') ? hostname.slice(0, -1) : hostname;
  if (withoutRootDot.length < 1) return false;
  return withoutRootDot.split('.').every(isDnsLabel);
}

function isDnsLabel(label: string): boolean {
  return /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label);
}

function sealedOpenAiBaseUrl(execContext: ExecutionContext | undefined): string | undefined {
  if (execContext?.kind !== 'container') return undefined;
  if (execContext.profile !== 'hardened') return undefined;
  const matches = (execContext.providerOrigins ?? []).filter(origin =>
    CODEX_OPENAI_PROVIDER_NAMES.has(origin.provider.toLowerCase())
  );
  if (matches.length !== 1) {
    throw new Error(
      'Codex hardened container execution requires exactly one controller-sealed OpenAI origin.'
    );
  }
  return normalizeSealedOpenAiBaseUrl(matches[0].baseUrl);
}

function buildSealedCodexOptions(
  requestEnv: Record<string, string> | undefined,
  codexConfigOverrides: CodexConfigOverrides | undefined,
  execContext: ExecutionContext | undefined
): { env?: Record<string, string>; baseUrl?: string; config?: CodexConfigOverrides } {
  const sealedBaseUrl = sealedOpenAiBaseUrl(execContext);
  if (!sealedBaseUrl) {
    return {
      ...(requestEnv && Object.keys(requestEnv).length > 0 ? { env: requestEnv } : {}),
      ...(codexConfigOverrides ? { config: codexConfigOverrides } : {}),
    };
  }
  const requestedBaseUrl = requestEnv?.OPENAI_BASE_URL;
  if (
    requestedBaseUrl !== undefined &&
    normalizeSealedOpenAiBaseUrl(requestedBaseUrl) !== sealedBaseUrl
  ) {
    throw new Error(
      'Codex hardened provider origin override does not match controller-sealed origin.'
    );
  }
  const env = requestEnv
    ? { ...requestEnv, OPENAI_BASE_URL: sealedBaseUrl }
    : { OPENAI_BASE_URL: sealedBaseUrl };
  const config = {
    ...codexConfigOverrides,
    ...sealedOpenAiConfig(sealedBaseUrl),
  };
  return {
    env,
    baseUrl: sealedBaseUrl,
    config,
  };
}

function sealedOpenAiConfig(sealedBaseUrl: string): CodexConfigOverrides {
  return {
    model_provider: 'archon-openai',
    openai_base_url: sealedBaseUrl,
    model_providers: {
      'archon-openai': {
        name: 'OpenAI',
        base_url: sealedBaseUrl,
        env_key: 'OPENAI_API_KEY',
        wire_api: 'responses',
        supports_websockets: false,
      },
    },
  };
}

function validateDockerToken(kind: string, value: string): void {
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/.test(value)) {
    throw new Error(`Invalid Codex container ${kind}: '${value}'.`);
  }
}

function shQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

export function buildCodexContainerWrapperScript(
  execContext: Extract<ExecutionContext, { kind: 'container' }>,
  executionToken = 'manual'
): string {
  if (execContext.profile !== 'hardened') {
    throw new Error('Codex container execution requires execContext.profile=hardened.');
  }
  validateDockerToken('id', execContext.containerId);
  validateDockerToken('execution token', executionToken);
  const execUser = execContext.execUser ?? 'archon';
  validateDockerToken('user', execUser);
  const envArgs = CODEX_CONTAINER_ENV_ALLOWLIST.map(name => `  --env ${name} \\`).join('\n');
  return [
    '#!/bin/sh',
    'set -eu',
    'exec docker exec -i \\',
    `  --user ${shQuote(execUser)} \\`,
    envArgs,
    `  --env ARCHON_CODEX_EXEC_TOKEN=${shQuote(executionToken)} \\`,
    `  ${shQuote(execContext.containerId)} codex "$@"`,
    '',
  ].join('\n');
}

async function createCodexContainerWrapper(
  execContext: Extract<ExecutionContext, { kind: 'container' }>
): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'archon-codex-container-'));
  const wrapperPath = join(dir, 'codex');
  await writeFile(
    wrapperPath,
    buildCodexContainerWrapperScript(execContext, randomBytes(8).toString('hex')),
    { mode: 0o700 }
  );
  await chmod(wrapperPath, 0o700);
  return wrapperPath;
}

function buildCodexContainerEnv(requestEnv?: Record<string, string>): Record<string, string> {
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? '/usr/local/bin:/usr/bin:/bin',
  };
  for (const key of CODEX_CONTAINER_ENV_ALLOWLIST) {
    const value = requestEnv?.[key];
    if (value !== undefined) env[key] = value;
  }
  return env;
}

interface ContainerAbortStopper {
  dispose: () => void;
  wait: () => Promise<void>;
}

async function stopContainer(containerId: string): Promise<void> {
  validateDockerToken('id', containerId);
  const proc = Bun.spawn(['docker', 'stop', containerId], {
    stdout: 'ignore',
    stderr: 'pipe',
  });
  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    const stderr = await new Response(proc.stderr).text();
    throw new Error(
      `Codex container cancellation failed: docker stop ${containerId} exited ${exitCode}.${stderr ? ` ${stderr.trim()}` : ''}`
    );
  }
}

// Codex SDK cancellation does not expose the in-container child PID. Hardened
// container cancellation is therefore per-run-container fail-stop: stop the owned
// container, preserve named volumes, and let runtime resume recreate it from volume
// state instead of trusting an agent-writable PID file.
function stopCodexContainerOnAbort(
  execContext: ExecutionContext | undefined,
  abortSignal: AbortSignal | undefined
): ContainerAbortStopper | undefined {
  if (execContext?.kind !== 'container' || !abortSignal) return undefined;
  if (execContext.profile !== 'hardened') {
    throw new Error('Codex container cancellation requires execContext.profile=hardened.');
  }
  let stopPromise: Promise<void> | undefined;
  const requestStop = (): void => {
    stopPromise ??= stopContainer(execContext.containerId);
  };
  if (abortSignal.aborted) {
    requestStop();
  } else {
    abortSignal.addEventListener('abort', requestStop, { once: true });
  }
  return {
    dispose: (): void => {
      abortSignal.removeEventListener('abort', requestStop);
    },
    wait: async (): Promise<void> => {
      if (stopPromise) await stopPromise;
    },
  };
}

// Singleton Codex instance (async because binary path resolution is async)
let codexInstance: Codex | null = null;
let codexInitPromise: Promise<Codex> | null = null;

/** Reset singleton state. Exported for tests only. */
export function resetCodexSingleton(): void {
  codexInstance = null;
  codexInitPromise = null;
}

/**
 * Get or create Codex SDK instance.
 */
async function getCodex(configCodexBinaryPath?: string): Promise<Codex> {
  if (codexInstance) return codexInstance;

  if (!codexInitPromise) {
    codexInitPromise = (async (): Promise<Codex> => {
      const codexPathOverride = await resolveCodexBinaryPath(configCodexBinaryPath);
      const instance = new Codex({ codexPathOverride });
      codexInstance = instance;
      return instance;
    })().catch(err => {
      codexInitPromise = null;
      throw err;
    });
  }
  return codexInitPromise;
}

/**
 * Build thread options for Codex SDK
 */
function buildThreadOptions(
  cwd: string,
  model?: string,
  assistantConfig?: Record<string, unknown>
): ThreadOptions {
  const config = parseCodexConfig(assistantConfig ?? {});
  return {
    workingDirectory: cwd,
    skipGitRepoCheck: true,
    sandboxMode: 'danger-full-access',
    networkAccessEnabled: true,
    approvalPolicy: 'never',
    model: model ?? config.model,
    modelReasoningEffort: config.modelReasoningEffort,
    webSearchMode: config.webSearchMode,
    additionalDirectories: config.additionalDirectories,
  };
}

function buildCodexEnv(requestEnv: Record<string, string>): Record<string, string> {
  const baseEnv = Object.fromEntries(
    Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined)
  );
  // Managed project env intentionally overrides inherited process env for project-scoped execution.
  return { ...baseEnv, ...requestEnv };
}

function buildMcpEnvSource(
  requestEnv?: Record<string, string>
): Record<string, string | undefined> {
  return requestEnv ? { ...process.env, ...requestEnv } : process.env;
}

function assertCodexContainerRequestSupported(requestOptions?: SendQueryOptions): void {
  if (requestOptions?.execContext?.kind !== 'container') return;
  if (
    requestOptions.execContext.profile === 'hardened' &&
    Object.hasOwn(requestOptions.env ?? {}, 'CODEX_HOME')
  ) {
    throw new Error(
      'Codex subscription configuration is not admitted for hardened container execution: an enforceable subscription output cap has not been verified.'
    );
  }
  if (requestOptions.nodeConfig?.mcp) {
    throw new Error(
      'Codex container execution does not support MCP config until controller-pinned MCP settings are implemented.'
    );
  }
  if (requestOptions.nativeTools && requestOptions.nativeTools.length > 0) {
    throw new Error(
      'Codex container execution does not support native tools until controller-pinned MCP settings are implemented.'
    );
  }
}

function createCodexAttempt(index: number, abortSignal: AbortSignal | undefined): CodexAttempt {
  const controller = new AbortController();
  const onCallerAbort = (): void => {
    controller.abort();
  };
  if (abortSignal) abortSignal.addEventListener('abort', onCallerAbort, { once: true });
  return {
    index,
    controller,
    dispose: (): void => {
      if (abortSignal) abortSignal.removeEventListener('abort', onCallerAbort);
    },
  };
}

const CODEX_MCP_PASSTHROUGH_KEYS = [
  'command',
  'args',
  'env',
  'url',
  'enabled',
  'required',
  'startup_timeout_sec',
  'startup_timeout_ms',
  'tool_timeout_sec',
  'enabled_tools',
  'disabled_tools',
  'supports_parallel_tool_calls',
  'cwd',
  'env_vars',
  'experimental_environment',
  'http_headers',
  'env_http_headers',
  'oauth_resource',
  'scopes',
  'bearer_token_env_var',
  'default_tools_approval_mode',
  'tools',
] as const;

function toCodexConfigValue(value: unknown): CodexConfigValue | undefined {
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  if (Array.isArray(value)) {
    const result: CodexConfigValue[] = [];
    for (const item of value) {
      const converted = toCodexConfigValue(item);
      if (converted !== undefined) result.push(converted);
    }
    return result;
  }

  if (typeof value === 'object' && value !== null) {
    const result: CodexConfigOverrides = {};
    for (const [key, nestedValue] of Object.entries(value)) {
      const converted = toCodexConfigValue(nestedValue);
      if (converted !== undefined) result[key] = converted;
    }
    return result;
  }

  return undefined;
}

function setCodexConfigValue(target: CodexConfigOverrides, key: string, value: unknown): void {
  const converted = toCodexConfigValue(value);
  if (converted !== undefined) {
    target[key] = converted;
  }
}

function convertMcpServerConfigForCodex(
  serverConfig: Record<string, unknown>
): CodexConfigOverrides {
  const result: CodexConfigOverrides = {};

  for (const key of CODEX_MCP_PASSTHROUGH_KEYS) {
    if (key in serverConfig) {
      setCodexConfigValue(result, key, serverConfig[key]);
    }
  }

  // Archon's MCP JSON format uses `headers`; Codex config uses `http_headers`.
  if ('headers' in serverConfig && !('http_headers' in result)) {
    setCodexConfigValue(result, 'http_headers', serverConfig.headers);
  }

  return result;
}

function buildCodexMcpConfigOverrides(
  servers: Record<string, unknown>
): CodexConfigOverrides | undefined {
  const mcpServers: CodexConfigOverrides = {};

  for (const [serverName, serverConfig] of Object.entries(servers)) {
    if (typeof serverConfig !== 'object' || serverConfig === null || Array.isArray(serverConfig)) {
      getLog().warn(
        { serverName, valueType: typeof serverConfig },
        'codex.mcp_server_config_not_object'
      );
      continue;
    }

    const converted = convertMcpServerConfigForCodex(serverConfig as Record<string, unknown>);
    if (Object.keys(converted).length > 0) {
      mcpServers[serverName] = converted;
    }
  }

  if (Object.keys(mcpServers).length === 0) return undefined;
  return { mcp_servers: mcpServers };
}

// Maps slugs that ChatGPT-plan accounts now reject (previously shipped as Archon
// suggestions/defaults) to a current, plan-accepted slug to suggest instead.
const CODEX_MODEL_FALLBACKS: Record<string, string> = {
  'gpt-5.3-codex': 'gpt-5.6-sol',
  'gpt-5.2-codex': 'gpt-5.6-sol',
  'gpt-5.2': 'gpt-5.6-sol',
};

function isModelAccessError(errorMessage: string): boolean {
  const m = errorMessage.toLowerCase();
  const hasModel = m.includes('model');
  const hasAvailabilitySignal =
    m.includes('not available') || m.includes('not found') || m.includes('access denied');
  return hasModel && hasAvailabilitySignal;
}

function buildModelAccessMessage(model?: string): string {
  const normalizedModel = model?.trim();
  const selectedModel = normalizedModel || 'the configured model';
  const suggested = normalizedModel ? CODEX_MODEL_FALLBACKS[normalizedModel] : undefined;

  const fixLine = suggested
    ? `To fix: update your model in ~/.archon/config.yaml:\n  assistants:\n    codex:\n      model: ${suggested}`
    : 'To fix: update your model in ~/.archon/config.yaml to one your account can access.';

  const workflowLine = suggested
    ? `Or set it per-workflow with \`model: ${suggested}\` in workflow YAML.`
    : 'Or set it per-workflow with a valid `model:` in workflow YAML.';

  return `❌ Model "${selectedModel}" is not available for your account.\n\n${fixLine}\n\n${workflowLine}`;
}

const MAX_SUBPROCESS_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 2000;
const RATE_LIMIT_PATTERNS = ['rate limit', 'too many requests', '429', 'overloaded'];
const AUTH_PATTERNS = [
  'credit balance',
  'unauthorized',
  'authentication',
  'invalid token',
  '401',
  '403',
];
const SUBPROCESS_CRASH_PATTERNS = ['exited with code', 'killed', 'signal', 'codex exec'];

function classifyCodexError(
  errorMessage: string
): 'rate_limit' | 'auth' | 'crash' | 'model_access' | 'unknown' {
  if (isModelAccessError(errorMessage)) return 'model_access';
  const m = errorMessage.toLowerCase();
  if (RATE_LIMIT_PATTERNS.some(p => m.includes(p))) return 'rate_limit';
  if (AUTH_PATTERNS.some(p => m.includes(p))) return 'auth';
  if (SUBPROCESS_CRASH_PATTERNS.some(p => m.includes(p))) return 'crash';
  return 'unknown';
}

function extractUsageFromCodexEvent(event: TurnCompletedEvent): TokenUsage | undefined {
  if (!event.usage) {
    getLog().warn({ eventType: event.type }, 'codex.usage_missing_on_turn_completed');
    return undefined;
  }
  const input = readCodexUsageCount(event.usage, 'input_tokens');
  const output = readCodexUsageCount(event.usage, 'output_tokens');
  return {
    input,
    output,
  };
}

function readCodexUsageCount(
  usage: TurnCompletedEvent['usage'],
  key: 'input_tokens' | 'output_tokens'
): number {
  const value = usage[key];
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Codex turn.completed usage ${key} is malformed.`);
  }
  return value;
}

// ─── Turn Options Builder ────────────────────────────────────────────────

/**
 * Build turn options for a single Codex turn.
 * Handles output schema from both requestOptions and nodeConfig (workflow path).
 */
function buildTurnOptions(requestOptions?: SendQueryOptions): {
  turnOptions: TurnOptions;
  hasOutputFormat: boolean;
} {
  const turnOptions: TurnOptions = {};
  // Preserve the original precedence: an explicit `outputFormat` wins over
  // `nodeConfig.output_format` even when its `.schema` is undefined. Note the
  // resulting asymmetry: if `outputFormat` is set but `.schema` is undefined,
  // `rawSchema` is undefined (no schema sent) yet `hasOutputFormat` is still
  // true — the stream accumulator runs and JSON.parses the response text.
  const rawSchema =
    requestOptions?.outputFormat !== undefined
      ? requestOptions.outputFormat.schema
      : requestOptions?.nodeConfig?.output_format;
  const hasOutputFormat = !!(
    requestOptions?.outputFormat ?? requestOptions?.nodeConfig?.output_format
  );
  if (rawSchema !== undefined) {
    // OpenAI Structured Outputs strict-mode requires additionalProperties:false
    // on every object schema (HTTP 400 invalid_json_schema otherwise). Workflow
    // authors write portable output_format schemas, so normalize here before
    // handing the schema to the Codex SDK. See issue #1843.
    if (hasOpenAdditionalProperties(rawSchema)) {
      // The normalizer is about to rewrite an open-record `additionalProperties`
      // (e.g. `{ type: 'string' }` or `true`) to `false`. OpenAI would 400 the
      // open form anyway, but the author never declared a closed object — warn
      // so the silent narrowing is visible rather than a surprise at runtime.
      getLog().warn({ schema: rawSchema }, 'codex.output_format_open_record_closed');
    }
    turnOptions.outputSchema = normalizeJsonSchemaForOpenAiStrict(rawSchema);
  }
  // Signal assignment is intentionally per-attempt (in sendQuery's retry
  // loop), not here. Reusing a single AbortSignal across retries can poison
  // later attempts once any earlier attempt's subprocess is SIGTERM'd.
  // See issue #1266.
  return { turnOptions, hasOutputFormat };
}

// ─── Effective Prompt Builder ────────────────────────────────────────────

/**
 * Fold the request/node-level systemPrompt into the user prompt.
 *
 * The Codex SDK (verified at @openai/codex-sdk 0.144.5) exposes NO
 * instructions/system-prompt channel on ThreadOptions or TurnOptions, so the
 * only delivery mechanism is prepending to the prompt string, separated by
 * the same `---` delimiter augmentPromptForJsonSchema uses. See issue #1837.
 *
 * Precedence mirrors the Pi provider: request-level systemPrompt wins over
 * node-level. Only string / string[] are supported; SystemPromptPreset
 * objects are Claude-specific and dropped with a WARN (the orchestrator
 * already sends non-Claude providers a plain string).
 *
 * The prepend intentionally repeats on EVERY turn, including resumed
 * threads: the provider cannot know whether a resumed session's earlier
 * turns carried the instructions (the session may predate this fix), and
 * both the resume-failure fallback and cold retry attempts start fresh
 * threads where first-turn-only logic would drop the instructions exactly
 * when they are most needed. This matches Claude, which receives the
 * systemPrompt on every query.
 */
function buildEffectivePrompt(prompt: string, requestOptions?: SendQueryOptions): string {
  const raw = requestOptions?.systemPrompt ?? requestOptions?.nodeConfig?.systemPrompt;
  if (raw === undefined) {
    return prompt;
  }
  let systemText: string | undefined;
  if (typeof raw === 'string') {
    systemText = raw;
  } else if (Array.isArray(raw)) {
    systemText = raw.join('\n\n');
  }
  if (systemText === undefined) {
    getLog().warn({ systemPromptType: typeof raw }, 'codex.system_prompt_dropped_preset');
    return prompt;
  }
  if (systemText.trim() === '') {
    return prompt;
  }
  return `${systemText}\n\n---\n\n${prompt}`;
}

// ─── Stream Normalizer ───────────────────────────────────────────────────

/** State maintained across Codex event stream normalization. */
interface CodexStreamState {
  lastTodoListSignature?: string;
  startedToolItemIds: Set<string>;
  completedToolItemIds: Set<string>;
  accumulatedText: string;
  resolvedThreadId: string | null | undefined;
  lastNonMcpError?: string;
}

function getMcpToolName(item: Record<string, unknown>): string {
  const server = item.server as string | undefined;
  const tool = item.tool as string | undefined;
  const toolInfo = server && tool ? `${server}/${tool}` : (tool ?? server ?? 'MCP tool');
  return `🔌 MCP: ${toolInfo}`;
}

/**
 * Normalize raw Codex SDK events into Archon MessageChunks.
 * Handles structured output normalization (Codex returns JSON inline in text).
 */
async function* streamCodexEvents(
  events: AsyncIterable<Record<string, unknown>>,
  hasOutputFormat: boolean,
  threadId: string | null | undefined,
  abortSignal?: AbortSignal,
  surfaceMcpClientErrors = false
): AsyncGenerator<MessageChunk> {
  const state: CodexStreamState = {
    startedToolItemIds: new Set<string>(),
    completedToolItemIds: new Set<string>(),
    accumulatedText: '',
    resolvedThreadId: threadId,
  };

  if (abortSignal?.aborted) {
    getLog().info('query_aborted_before_stream');
    throw new Error('Query aborted');
  }

  for await (const event of events) {
    if (abortSignal?.aborted) {
      getLog().info('query_aborted_between_events');
      throw new Error('Query aborted');
    }

    const terminal = yield* handleCodexStreamEvent(event, state, {
      hasOutputFormat,
      surfaceMcpClientErrors,
    });
    if (terminal) return;
  }

  // Reaching here means the iterator closed without yielding turn.completed
  // or turn.failed (both branches `return` immediately). Common cause: model
  // rejected by the API (model not supported, auth refused) before the turn
  // started. Surface as a fail-stop. The dag-executor's `msg.isError` branch
  // (dag-executor.ts: throws `Node '<id>' failed: SDK returned <subtype>`)
  // turns this into a thrown node failure — distinct from the empty-output
  // guard further down, which returns `{ state: 'failed' }` for AI nodes
  // that streamed nothing but never raised an isError.
  const message =
    state.lastNonMcpError ?? 'Codex stream closed without turn.completed or turn.failed';
  getLog().error({ message }, 'stream_incomplete');
  yield {
    type: 'result',
    sessionId: state.resolvedThreadId ?? undefined,
    isError: true,
    errorSubtype: 'codex_stream_incomplete',
    errors: [message],
  };
}

interface CodexStreamOptions {
  hasOutputFormat: boolean;
  surfaceMcpClientErrors: boolean;
}

async function* handleCodexStreamEvent(
  event: Record<string, unknown>,
  state: CodexStreamState,
  options: CodexStreamOptions
): AsyncGenerator<MessageChunk, boolean> {
  switch (event.type) {
    case 'thread.started':
      handleThreadStarted(event as unknown as ThreadStartedEvent, state);
      return false;
    case 'item.started':
      yield* handleItemStarted(event, state);
      return false;
    case 'error':
      yield* handleStreamError(event, state, options.surfaceMcpClientErrors);
      return false;
    case 'turn.failed':
      yield turnFailedResult(event, state.resolvedThreadId);
      return true;
    case 'item.completed':
      yield* handleItemCompleted(event, state, options.hasOutputFormat);
      return false;
    case 'turn.completed':
      yield* handleTurnCompleted(event as TurnCompletedEvent, state, options.hasOutputFormat);
      return true;
    default:
      return false;
  }
}

function handleThreadStarted(event: ThreadStartedEvent, state: CodexStreamState): void {
  const startedThreadId = event.thread_id;
  if (startedThreadId) {
    state.resolvedThreadId = startedThreadId;
    getLog().info({ threadId: startedThreadId }, 'codex.thread_started');
    return;
  }
  getLog().warn({ snapshotThreadId: state.resolvedThreadId }, 'codex.thread_started_missing_id');
}

async function* handleItemStarted(
  event: Record<string, unknown>,
  state: CodexStreamState
): AsyncGenerator<MessageChunk> {
  const item = event.item as Record<string, unknown>;
  const itemType = item.type as string;
  const itemId = item.id as string;
  getLog().debug({ eventType: event.type, itemType, itemId }, 'item_started');
  const toolName = startedToolName(item, itemType, itemId);
  if (toolName && itemId && !state.startedToolItemIds.has(itemId)) {
    state.startedToolItemIds.add(itemId);
    yield { type: 'tool', toolName, toolCallId: itemId };
  }
}

function startedToolName(
  item: Record<string, unknown>,
  itemType: string,
  itemId: string
): string | undefined {
  if (itemType === 'command_execution') return commandToolName(item, itemId);
  if (itemType === 'web_search') return webSearchToolName(item, itemId);
  if (itemType === 'mcp_tool_call') return getMcpToolName(item);
  return undefined;
}

function commandToolName(item: Record<string, unknown>, itemId: string): string | undefined {
  if (typeof item.command === 'string' && item.command.length > 0) return item.command;
  getLog().warn({ itemId }, 'command_execution_missing_command');
  return undefined;
}

function webSearchToolName(item: Record<string, unknown>, itemId: string): string | undefined {
  if (typeof item.query === 'string' && item.query.length > 0) return `🔍 Searching: ${item.query}`;
  getLog().debug({ itemId }, 'web_search_missing_query');
  return undefined;
}

async function* handleStreamError(
  event: Record<string, unknown>,
  state: CodexStreamState,
  surfaceMcpClientErrors: boolean
): AsyncGenerator<MessageChunk> {
  const errorEvent = event as { message: string };
  getLog().error({ message: errorEvent.message }, 'stream_error');
  const isMcpClientError = errorEvent.message.toLowerCase().includes('mcp client');
  if (!isMcpClientError) {
    state.lastNonMcpError = errorEvent.message;
  } else if (surfaceMcpClientErrors) {
    yield { type: 'system', content: `⚠️ ${errorEvent.message}` };
  }
}

function turnFailedResult(
  event: Record<string, unknown>,
  threadId: string | null | undefined
): MessageChunk {
  const errorObj = (event as { error?: { message?: string } }).error;
  const errorMessage = errorObj?.message ?? 'Unknown error';
  getLog().error({ errorMessage }, 'turn_failed');
  return {
    type: 'result',
    sessionId: threadId ?? undefined,
    isError: true,
    errorSubtype: 'codex_turn_failed',
    errors: [errorMessage],
  };
}

async function* handleItemCompleted(
  event: Record<string, unknown>,
  state: CodexStreamState,
  hasOutputFormat: boolean
): AsyncGenerator<MessageChunk> {
  const item = event.item as Record<string, unknown>;
  const itemType = item.type as string;
  const itemId = item.id as string;
  logCompletedItem(event, item, itemType);
  if (isDuplicateToolCompletion(itemType, itemId, state)) return;

  yield* completedItemChunks(item, itemType, itemId, state, hasOutputFormat);
}

function logCompletedItem(
  event: Record<string, unknown>,
  item: Record<string, unknown>,
  itemType: string
): void {
  const logContext: Record<string, unknown> = { eventType: event.type, itemType, itemId: item.id };
  if (itemType === 'command_execution' && item.command) logContext.command = item.command;
  getLog().debug(logContext, 'item_completed');
}

function isDuplicateToolCompletion(
  itemType: string,
  itemId: string,
  state: CodexStreamState
): boolean {
  const isToolItem =
    itemType === 'command_execution' || itemType === 'web_search' || itemType === 'mcp_tool_call';
  if (!isToolItem) return false;
  if (state.completedToolItemIds.has(itemId)) {
    getLog().warn({ itemId, itemType }, 'tool_item_duplicate_completion');
    return true;
  }
  state.completedToolItemIds.add(itemId);
  if (!state.startedToolItemIds.has(itemId)) {
    getLog().warn({ itemId, itemType }, 'tool_item_completed_without_start');
  }
  return false;
}

async function* completedItemChunks(
  item: Record<string, unknown>,
  itemType: string,
  itemId: string,
  state: CodexStreamState,
  hasOutputFormat: boolean
): AsyncGenerator<MessageChunk> {
  switch (itemType) {
    case 'agent_message':
      yield* completedAgentMessage(item, state, hasOutputFormat);
      break;
    case 'command_execution':
      yield* completedCommandExecution(item, itemId);
      break;
    case 'reasoning':
      yield* completedReasoning(item);
      break;
    case 'web_search':
      yield* completedWebSearch(item, itemId);
      break;
    case 'todo_list':
      yield* completedTodoList(item, state);
      break;
    case 'file_change':
      yield* completedFileChange(item);
      break;
    case 'mcp_tool_call':
      yield* completedMcpToolCall(item, itemId);
      break;
  }
}

async function* completedAgentMessage(
  item: Record<string, unknown>,
  state: CodexStreamState,
  hasOutputFormat: boolean
): AsyncGenerator<MessageChunk> {
  if (!item.text) return;
  if (hasOutputFormat) state.accumulatedText = item.text as string;
  yield { type: 'assistant', content: item.text as string };
}

async function* completedCommandExecution(
  item: Record<string, unknown>,
  itemId: string
): AsyncGenerator<MessageChunk> {
  if (!item.command) {
    getLog().warn({ itemId: item.id }, 'command_execution_missing_command');
    return;
  }
  const cmd = item.command as string;
  const exitCode = item.exit_code as number | null | undefined;
  const exitSuffix = exitCode != null && exitCode !== 0 ? `\n[exit code: ${String(exitCode)}]` : '';
  yield {
    type: 'tool_result',
    toolName: cmd,
    toolOutput: ((item.aggregated_output as string) ?? '') + exitSuffix,
    toolCallId: itemId,
    toolOutcome: commandToolOutcome(exitCode),
    ...(exitCode != null ? { exitCode } : {}),
  };
}

function commandToolOutcome(exitCode: number | null | undefined): 'success' | 'error' | 'unknown' {
  if (exitCode === 0) return 'success';
  if (exitCode == null) return 'unknown';
  return 'error';
}

async function* completedReasoning(item: Record<string, unknown>): AsyncGenerator<MessageChunk> {
  if (item.text) yield { type: 'thinking', content: item.text as string };
}

async function* completedWebSearch(
  item: Record<string, unknown>,
  itemId: string
): AsyncGenerator<MessageChunk> {
  if (!item.query) {
    getLog().debug({ itemId: item.id }, 'web_search_missing_query');
    return;
  }
  yield {
    type: 'tool_result',
    toolName: `🔍 Searching: ${item.query as string}`,
    toolOutput: '',
    toolCallId: itemId,
    toolOutcome: 'unknown',
  };
}

async function* completedTodoList(
  item: Record<string, unknown>,
  state: CodexStreamState
): AsyncGenerator<MessageChunk> {
  const items = item.items as { text?: string; completed?: boolean }[] | undefined;
  if (!Array.isArray(items) || items.length === 0) {
    getLog().debug({ itemId: item.id }, 'todo_list_empty_or_invalid');
    return;
  }
  const normalizedItems = items.map(t => ({
    text: typeof t.text === 'string' ? t.text : '(unnamed task)',
    completed: t.completed ?? false,
  }));
  const signature = JSON.stringify(normalizedItems);
  if (signature === state.lastTodoListSignature) return;
  state.lastTodoListSignature = signature;
  const taskList = normalizedItems.map(t => `${t.completed ? '✅' : '⬜'} ${t.text}`).join('\n');
  yield { type: 'system', content: `📋 Tasks:\n${taskList}` };
}

async function* completedFileChange(item: Record<string, unknown>): AsyncGenerator<MessageChunk> {
  const fileErrorMessage = fileChangeErrorMessage(item);
  const changes = item.changes as { kind: string; path?: string }[] | undefined;
  if (Array.isArray(changes) && changes.length > 0) {
    yield fileChangeSummary(item, changes, fileErrorMessage);
    return;
  }
  if ((item.status as string) === 'failed') {
    getLog().warn({ itemId: item.id, status: item.status }, 'file_change_failed_no_changes');
    const failMsg = fileErrorMessage
      ? `❌ File change failed: ${fileErrorMessage}`
      : '❌ File change failed';
    yield { type: 'system', content: failMsg };
    return;
  }
  getLog().debug({ itemId: item.id, status: item.status }, 'file_change_no_changes');
}

function fileChangeErrorMessage(item: Record<string, unknown>): string | undefined {
  const rawError = 'error' in item ? (item as { error?: unknown }).error : undefined;
  if (typeof rawError === 'string') return rawError;
  if (typeof rawError === 'object' && rawError !== null && 'message' in rawError) {
    return String((rawError as { message: unknown }).message);
  }
  return undefined;
}

function fileChangeSummary(
  item: Record<string, unknown>,
  changes: { kind: string; path?: string }[],
  fileErrorMessage: string | undefined
): MessageChunk {
  const statusIcon = (item.status as string) === 'failed' ? '❌' : '✅';
  const changeList = changes.map(fileChangeLine).join('\n');
  const errorSuffix =
    (item.status as string) === 'failed' && fileErrorMessage ? `\n${fileErrorMessage}` : '';
  return { type: 'system', content: `${statusIcon} File changes:\n${changeList}${errorSuffix}` };
}

function fileChangeLine(change: { kind: string; path?: string }): string {
  const icon = change.kind === 'add' ? '➕' : change.kind === 'delete' ? '➖' : '📝';
  return `${icon} ${change.path ?? '(unknown file)'}`;
}

async function* completedMcpToolCall(
  item: Record<string, unknown>,
  itemId: string
): AsyncGenerator<MessageChunk> {
  const server = item.server as string | undefined;
  const tool = item.tool as string | undefined;
  const mcpToolName = getMcpToolName(item);
  if ((item.status as string) === 'failed') {
    yield failedMcpToolCall(item, itemId, mcpToolName, server, tool);
    return;
  }
  yield successfulMcpToolCall(item, itemId, mcpToolName, server, tool);
}

function failedMcpToolCall(
  item: Record<string, unknown>,
  itemId: string,
  mcpToolName: string,
  server: string | undefined,
  tool: string | undefined
): MessageChunk {
  getLog().warn({ server, tool, error: item.error, itemId: item.id }, 'mcp_tool_call_failed');
  const mcpError = item.error as { message?: string } | undefined;
  return {
    type: 'tool_result',
    toolName: mcpToolName,
    toolOutput: mcpError?.message ? `❌ Error: ${mcpError.message}` : '❌ Error: MCP tool failed',
    toolCallId: itemId,
    toolOutcome: 'error',
  };
}

function successfulMcpToolCall(
  item: Record<string, unknown>,
  itemId: string,
  mcpToolName: string,
  server: string | undefined,
  tool: string | undefined
): MessageChunk {
  return {
    type: 'tool_result',
    toolName: mcpToolName,
    toolOutput: mcpToolOutput(item, itemId, server, tool),
    toolCallId: itemId,
    toolOutcome: 'success',
  };
}

function mcpToolOutput(
  item: Record<string, unknown>,
  itemId: string,
  server: string | undefined,
  tool: string | undefined
): string {
  const mcpResult = item.result as { content?: unknown } | undefined;
  if (!mcpResult?.content) return '';
  if (Array.isArray(mcpResult.content)) return JSON.stringify(mcpResult.content);
  getLog().warn(
    { itemId, server, tool, resultType: typeof mcpResult.content },
    'mcp_tool_call_unexpected_result_shape'
  );
  return '';
}

async function* handleTurnCompleted(
  event: TurnCompletedEvent,
  state: CodexStreamState,
  hasOutputFormat: boolean
): AsyncGenerator<MessageChunk> {
  getLog().debug('turn_completed');
  const usage = extractUsageFromCodexEvent(event);
  const structuredOutput = yield* parseStructuredOutput(state.accumulatedText, hasOutputFormat);
  yield {
    type: 'result',
    sessionId: state.resolvedThreadId ?? undefined,
    ...(usage ? { tokens: usage } : {}),
    ...(structuredOutput !== undefined ? { structuredOutput } : {}),
  };
}

async function* parseStructuredOutput(
  accumulatedText: string,
  hasOutputFormat: boolean
): AsyncGenerator<MessageChunk, unknown> {
  if (!hasOutputFormat || !accumulatedText) return undefined;
  try {
    const structuredOutput = JSON.parse(accumulatedText) as unknown;
    getLog().debug('codex.structured_output_parsed');
    return structuredOutput;
  } catch {
    getLog().warn(
      { outputPreview: accumulatedText.slice(0, 200) },
      'codex.structured_output_not_json'
    );
    yield {
      type: 'system',
      content:
        '⚠️ Structured output requested but Codex returned non-JSON text. ' +
        'Downstream $nodeId.output.field references may not evaluate correctly.',
    };
    return undefined;
  }
}

// ─── Error Classification & Retry ────────────────────────────────────────

/**
 * Classify a Codex error and determine retry eligibility.
 */
function classifyAndEnrichCodexError(
  error: Error,
  model?: string
): { enrichedError: Error; errorClass: string; shouldRetry: boolean } {
  const errorClass = classifyCodexError(error.message);

  if (errorClass === 'model_access') {
    return {
      enrichedError: new Error(buildModelAccessMessage(model)),
      errorClass,
      shouldRetry: false,
    };
  }

  if (errorClass === 'auth') {
    const enrichedError = new Error(`Codex auth error: ${error.message}`);
    enrichedError.cause = error;
    return { enrichedError, errorClass, shouldRetry: false };
  }

  const enrichedError = new Error(`Codex ${errorClass}: ${error.message}`);
  enrichedError.cause = error;
  const shouldRetry = errorClass === 'rate_limit' || errorClass === 'crash';
  return { enrichedError, errorClass, shouldRetry };
}

// ─── Codex Provider ──────────────────────────────────────────────────────

/**
 * Codex AI agent provider.
 * Implements IAgentProvider with Codex SDK integration.
 *
 * sendQuery orchestrates the following internal helpers:
 * - buildThreadOptions: SDK thread configuration
 * - buildTurnOptions: per-turn configuration (output schema, abort signal)
 * - buildEffectivePrompt: systemPrompt delivery via prompt prepend (no SDK channel)
 * - streamCodexEvents: raw SDK event normalization into MessageChunks
 * - classifyAndEnrichCodexError: error classification for retry decisions
 */
export class CodexProvider implements IAgentProvider {
  private readonly retryBaseDelayMs: number;

  constructor(options?: { retryBaseDelayMs?: number }) {
    this.retryBaseDelayMs = options?.retryBaseDelayMs ?? RETRY_BASE_DELAY_MS;
  }

  private async createCodexClient(
    configCodexBinaryPath: string | undefined,
    requestEnv?: Record<string, string>,
    codexConfigOverrides?: CodexConfigOverrides,
    execContext?: ExecutionContext
  ): Promise<{ codex: Codex; cleanup?: () => Promise<void> }> {
    if (execContext?.kind === 'container') {
      const sealed = buildSealedCodexOptions(
        buildCodexContainerEnv(requestEnv),
        codexConfigOverrides,
        execContext
      );
      const codexPathOverride = await createCodexContainerWrapper(execContext);
      return {
        codex: new Codex({
          codexPathOverride,
          ...sealed,
        }),
        cleanup: () => rm(dirname(codexPathOverride), { recursive: true, force: true }),
      };
    }

    const sealed = buildSealedCodexOptions(
      requestEnv ? buildCodexEnv(requestEnv) : undefined,
      codexConfigOverrides,
      execContext
    );
    if (!sealed.env && !sealed.config && !sealed.baseUrl) {
      return { codex: await getCodex(configCodexBinaryPath) };
    }

    try {
      const codexOptions: CodexOptions = {
        codexPathOverride: await resolveCodexBinaryPath(configCodexBinaryPath),
        ...sealed,
      };
      return { codex: new Codex(codexOptions) };
    } catch (error) {
      const err = error as Error;
      if (isModelAccessError(err.message)) {
        throw new Error(buildModelAccessMessage());
      }
      throw new Error(`Codex query failed: ${err.message}`);
    }
  }

  getCapabilities(): ProviderCapabilities {
    return CODEX_CAPABILITIES;
  }

  async *sendQuery(
    prompt: string,
    cwd: string,
    resumeSessionId?: string,
    requestOptions?: SendQueryOptions
  ): AsyncGenerator<MessageChunk> {
    const assistantConfig = requestOptions?.assistantConfig ?? {};
    const codexConfig = parseCodexConfig(assistantConfig);
    const providerWarnings: ProviderWarning[] = [];
    let codexConfigOverrides: CodexConfigOverrides | undefined;

    assertCodexContainerRequestSupported(requestOptions);

    if (requestOptions?.nodeConfig?.mcp) {
      const mcpPath = requestOptions.nodeConfig.mcp;
      const { servers, serverNames, missingVars } = await loadMcpConfig(
        mcpPath,
        cwd,
        buildMcpEnvSource(requestOptions.env)
      );
      codexConfigOverrides = buildCodexMcpConfigOverrides(servers);
      getLog().info({ serverNames, mcpPath }, 'codex.mcp_config_loaded');
      if (missingVars.length > 0) {
        const uniqueVars = [...new Set(missingVars)];
        getLog().warn({ missingVars: uniqueVars }, 'codex.mcp_env_vars_missing');
        providerWarnings.push({
          code: 'mcp_env_vars_missing',
          message: `MCP config references undefined env vars: ${uniqueVars.join(', ')}. These will be empty strings - MCP servers may fail to authenticate.`,
        });
      }
    }

    for (const warning of providerWarnings) {
      yield { type: 'system', content: `⚠️ ${warning.message}` };
    }

    // 1. Initialize SDK and build thread options
    const codexClient = await this.createCodexClient(
      codexConfig.codexBinaryPath,
      requestOptions?.env,
      codexConfigOverrides,
      requestOptions?.execContext
    );
    const codex = codexClient.codex;
    const containerAbortStopper = stopCodexContainerOnAbort(
      requestOptions?.execContext,
      requestOptions?.abortSignal
    );
    try {
      yield* this.runQueryWithCodex(
        codex,
        prompt,
        cwd,
        resumeSessionId,
        requestOptions,
        assistantConfig
      );
    } finally {
      containerAbortStopper?.dispose();
      await containerAbortStopper?.wait();
      await codexClient.cleanup?.();
    }
  }

  private async *runQueryWithCodex(
    codex: Codex,
    prompt: string,
    cwd: string,
    resumeSessionId: string | undefined,
    requestOptions: SendQueryOptions | undefined,
    assistantConfig: Record<string, unknown>
  ): AsyncGenerator<MessageChunk> {
    const threadOptions = buildThreadOptions(cwd, requestOptions?.model, assistantConfig);
    if (requestOptions?.abortSignal?.aborted) throw new Error('Query aborted');

    const initial = this.createInitialThread(
      codex,
      cwd,
      resumeSessionId,
      threadOptions,
      requestOptions
    );
    if (initial.sessionResumeFailed) {
      yield {
        type: 'system',
        content: '⚠️ Could not resume previous session. Starting fresh conversation.',
      };
    }

    const { turnOptions, hasOutputFormat } = buildTurnOptions(requestOptions);
    yield* this.runCodexAttempts({
      codex,
      thread: initial.thread,
      threadOptions,
      turnOptions,
      hasOutputFormat,
      effectivePrompt: buildEffectivePrompt(prompt, requestOptions),
      cwd,
      resumeSessionId,
      sessionResumeFailed: initial.sessionResumeFailed,
      requestOptions,
    });
  }

  private createInitialThread(
    codex: Codex,
    cwd: string,
    resumeSessionId: string | undefined,
    threadOptions: ThreadOptions,
    requestOptions: SendQueryOptions | undefined
  ): { thread: CodexThread; sessionResumeFailed: boolean } {
    if (!resumeSessionId) {
      getLog().debug({ cwd }, 'starting_new_thread');
      return {
        thread: this.startThreadOrThrow(codex, threadOptions, requestOptions?.model),
        sessionResumeFailed: false,
      };
    }
    getLog().debug({ sessionId: resumeSessionId }, 'resuming_thread');
    try {
      return {
        thread: codex.resumeThread(resumeSessionId, threadOptions),
        sessionResumeFailed: false,
      };
    } catch (error) {
      getLog().error({ err: error, sessionId: resumeSessionId }, 'resume_thread_failed');
      return {
        thread: this.startThreadOrThrow(codex, threadOptions, requestOptions?.model),
        sessionResumeFailed: true,
      };
    }
  }

  private startThreadOrThrow(
    codex: Codex,
    threadOptions: ThreadOptions,
    model: string | undefined
  ): CodexThread {
    try {
      return codex.startThread(threadOptions);
    } catch (error) {
      const err = error as Error;
      if (isModelAccessError(err.message)) throw new Error(buildModelAccessMessage(model));
      throw new Error(`Codex query failed: ${err.message}`);
    }
  }

  private async *runCodexAttempts(input: CodexAttemptInput): AsyncGenerator<MessageChunk> {
    let lastError: Error | undefined;
    let thread = input.thread;
    for (let attemptIndex = 0; attemptIndex <= MAX_SUBPROCESS_RETRIES; attemptIndex++) {
      if (input.requestOptions?.abortSignal?.aborted) throw new Error('Query aborted');
      const attempt = createCodexAttempt(attemptIndex, input.requestOptions?.abortSignal);
      input.turnOptions.signal = attempt.controller.signal;
      try {
        if (attempt.index > 0) {
          thread = this.startRetryThread(
            input.codex,
            input.threadOptions,
            input.cwd,
            attempt.index,
            input.requestOptions?.model
          );
        }
        yield* this.streamCodexAttempt(input, thread, attempt.index, attempt.controller);
        return;
      } catch (error) {
        lastError = await this.handleCodexAttemptError(error as Error, input, attempt.index);
      } finally {
        attempt.dispose();
      }
    }
    throw lastError ?? new Error('Codex query failed after retries');
  }

  private startRetryThread(
    codex: Codex,
    threadOptions: ThreadOptions,
    cwd: string,
    attempt: number,
    model: string | undefined
  ): CodexThread {
    getLog().debug({ cwd, attempt }, 'starting_new_thread');
    try {
      return codex.startThread(threadOptions);
    } catch (startError) {
      const err = startError as Error;
      if (isModelAccessError(err.message)) {
        getLog().debug({ attempt, errorClass: 'model_access' }, 'query_error_pre_retry');
        throw new Error(buildModelAccessMessage(model));
      }
      throw new Error(`Codex query failed: ${err.message}`);
    }
  }

  private async *streamCodexAttempt(
    input: CodexAttemptInput,
    thread: CodexThread,
    attempt: number,
    attemptController: AbortController
  ): AsyncGenerator<MessageChunk> {
    const result = await thread.runStreamed(input.effectivePrompt, input.turnOptions);
    yield* withResumedOutcome(
      streamCodexEvents(
        result.events as AsyncIterable<Record<string, unknown>>,
        input.hasOutputFormat,
        thread.id,
        attemptController.signal,
        Boolean(input.requestOptions?.nodeConfig?.mcp)
      ),
      resumedOutcome(input.resumeSessionId, !input.sessionResumeFailed && attempt === 0)
    );
  }

  private async handleCodexAttemptError(
    err: Error,
    input: CodexAttemptInput,
    attempt: number
  ): Promise<Error> {
    if (input.requestOptions?.abortSignal?.aborted) throw new Error('Query aborted');
    const { enrichedError, errorClass, shouldRetry } = classifyAndEnrichCodexError(
      err,
      input.requestOptions?.model
    );
    getLog().error({ err, errorClass, attempt, maxRetries: MAX_SUBPROCESS_RETRIES }, 'query_error');
    if (!shouldRetry || attempt >= MAX_SUBPROCESS_RETRIES) throw enrichedError;
    const delayMs = this.retryBaseDelayMs * Math.pow(2, attempt);
    getLog().info({ attempt, delayMs, errorClass }, 'retrying_query');
    await new Promise(resolve => setTimeout(resolve, delayMs));
    return enrichedError;
  }

  getType(): string {
    return 'codex';
  }
}
