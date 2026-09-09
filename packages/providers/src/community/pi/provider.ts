import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createLogger } from '@archon/paths';
// Type-only import — erased by TS, so it does NOT trigger Pi's config.js
// package.json read at module load (see the header note below). Used only to
// annotate the per-call ResourceLoader local.
import type { DefaultResourceLoader } from '@earendil-works/pi-coding-agent';

import type {
  IAgentProvider,
  MessageChunk,
  ProviderCapabilities,
  SendQueryOptions,
  SystemPromptInput,
} from '../../types';

import { PI_CAPABILITIES } from './capabilities';
import { parsePiConfig, resolvePiExtensionSettings } from './config';
import { parsePiModelRef } from './model-ref';
import { withResumedOutcome, resumedOutcome } from '../../shared/resumed';

// IMPORTANT: Do NOT add static `import { ... } from '@earendil-works/*'` here,
// and do NOT statically import sibling modules that themselves import runtime
// values from Pi (options-translator, resource-loader, session-resolver,
// ui-context-stub, event-bridge). Pi's `@earendil-works/pi-coding-agent/dist/config.js`
// runs `readFileSync(getPackageJsonPath(), "utf-8")` at module load; inside a
// compiled Archon binary `getPackageJsonPath()` resolves to
// `dirname(process.execPath) + "/package.json"` — a path that doesn't exist —
// and archon crashes at startup before any command runs (v0.3.7 symptom).
//
// All Pi SDK value bindings and Pi-dependent helper modules are dynamically
// imported inside `sendQuery()` below, which runs only when a Pi workflow is
// actually invoked. Type-only imports above are fine — TS erases them.
//
// Lazy-loading defers the crash from boot-time to sendQuery-time — but the
// crash still happens when Pi is actually used. `ensurePiPackageDirShim()`
// (see below) fixes the *runtime* half: before any dynamic Pi import in
// sendQuery, write a stub package.json to tmpdir and point Pi at it via
// its own documented `PI_PACKAGE_DIR` escape hatch.

// ─── Concurrency throttle ────────────────────────────────────────────────────

/**
 * Simple counting semaphore for capping concurrent Pi `session.prompt()` calls.
 * Pi/Minimax has no built-in SDK-level throttling; without this, large parallel
 * workflow batches (e.g. 10+ concurrent review PRs × 5 aspects each) hit rate
 * limits and cascade-fail. Module-level so it's shared across all PiProvider
 * instances within a process — Pi concurrency is global (one upstream backend).
 */
class Semaphore {
  private available: number;
  private readonly waiters: (() => void)[] = [];

  constructor(count: number) {
    this.available = count;
  }

  acquire(): Promise<void> {
    if (this.available > 0) {
      this.available--;
      return Promise.resolve();
    }
    return new Promise<void>(resolve => {
      this.waiters.push(resolve);
    });
  }

  release(): void {
    const next = this.waiters.shift();
    if (next) {
      next();
      return;
    }
    this.available++;
  }
}

let piSemaphore: Semaphore | undefined;

/**
 * Write a minimal package.json to a stable tmpdir and set `PI_PACKAGE_DIR`
 * so Pi's `config.js` short-circuits its `dirname(process.execPath)` walk
 * (which fails inside a compiled archon binary). Pi only reads three
 * optional fields from that package.json — `piConfig.name`, `piConfig.configDir`,
 * and `version` — so the stub is genuinely minimal. Idempotent: the file is
 * only written once per host (existsSync check), and the env var is set on
 * every call so multiple PiProvider instances stay consistent.
 *
 * Done on each sendQuery rather than at module load so (a) the file write
 * is paid only when Pi is actually used, and (b) the env var can't get
 * clobbered between registration and invocation.
 */
export function ensurePiPackageDirShim(): void {
  const shimDir = join(tmpdir(), 'archon-pi-shim');
  const shimPkgJson = join(shimDir, 'package.json');
  if (!existsSync(shimPkgJson)) {
    // `piConfig: {}` is explicit so Pi's defaults (`name: 'pi'`,
    // `configDir: '.pi'`) kick in — matches Pi's standalone behavior.
    try {
      mkdirSync(shimDir, { recursive: true });
      writeFileSync(
        shimPkgJson,
        JSON.stringify({
          name: 'archon-pi-shim',
          version: '0.0.0',
          piConfig: {},
        })
      );
    } catch (error) {
      // Surface as a classified error so the executor's catch sees a known
      // shape instead of a raw EACCES/ENOSPC from node:fs.
      const err = error as NodeJS.ErrnoException;
      throw new Error(`Pi shim setup failed at ${shimDir}: ${err.message}`);
    }
  }
  process.env.PI_PACKAGE_DIR = shimDir;
}

// ─── Bedrock backend registration (compiled-binary parity) ───────────────────

/**
 * Registrar for Pi's Bedrock backend module. Split out from
 * `ensureBedrockProviderRegistered` so tests can inject a spy without touching
 * the real SDK (Bun's `mock.module` is process-global and irreversible).
 */
export type BedrockRegistrar = () => Promise<void>;

/**
 * The default registrar: dynamically import the Pi SDK's Bedrock override hook
 * and the statically-bundled Bedrock module, then wire them together.
 *
 * Both specifiers are STRING LITERALS on purpose — that is the entire point of
 * this fix. Pi lazy-loads every backend via `import()`, and for all backends
 * except Bedrock the specifier is a string literal that Bun's `--compile`
 * static analysis can follow and embed. Bedrock's loader instead routes through
 * a computed-specifier indirection (`importNodeOnlyApi('./bedrock-converse-stream.ts')`
 * in pi-ai's `bedrock-converse-stream.lazy.js`) that Bun cannot resolve, so
 * `bedrock-converse-stream.js` + `@aws-sdk/client-bedrock-runtime` never get
 * bundled and a compiled Archon binary fails with `Cannot find module … /$bunfs/…`
 * on any `amazon-bedrock/*` model (issue #2154).
 *
 * Pi fixed the identical bug in their own binary (earendil-works/pi#2349,
 * PR #2350): `setBedrockProviderModule()` is checked FIRST inside the loader,
 * and is fed the module via the static `@earendil-works/pi-ai/bedrock-provider`
 * subpath, which Bun DOES bundle. Archon compiles its own CLI and never runs
 * Pi's bun entrypoint (`bun/register-bedrock.js`), so we mirror that shim here.
 *
 * The two subpaths match Pi's own 0.80.6 `bun/register-bedrock.js` shim exactly:
 * `setBedrockProviderModule` from `@earendil-works/pi-ai/compat` (the SDK moved
 * it off the package root into the compat entrypoint) and `bedrockProviderModule`
 * from `@earendil-works/pi-ai/bedrock-provider`. Both are safe to import inside a
 * compiled binary — Pi loads them in its own working binary — unlike
 * `@earendil-works/pi-coding-agent/config.js`, which reads a package.json next to
 * `process.execPath` (see the header note and `ensurePiPackageDirShim`).
 */
async function defaultBedrockRegistrar(): Promise<void> {
  const [compatModule, bedrockModule] = await Promise.all([
    import('@earendil-works/pi-ai/compat'),
    import('@earendil-works/pi-ai/bedrock-provider'),
  ]);
  compatModule.setBedrockProviderModule(bedrockModule.bedrockProviderModule);
}

let bedrockRegistrationPromise: Promise<void> | undefined;

/**
 * Register Pi's Bedrock backend override once per process. Idempotent: the
 * registrar runs on the first call and every later call reuses the cached
 * promise. Called from `sendQuery()` (not at module load), so it never
 * eagerly pulls the Pi SDK into module scope — preserving the lazy-load
 * invariant guarded by `provider-lazy-load.test.ts`.
 *
 * Registration failure is swallowed with a WARN rather than thrown: the hook
 * only matters for `amazon-bedrock/*` models, so a failure must not break
 * `anthropic/*`, `cursor/*`, or any other Pi backend. If a Bedrock node then
 * runs, Pi's own `importNodeOnlyApi` fallback still surfaces the original
 * `Cannot find module` error — i.e. degradation is strictly no worse than the
 * pre-fix behavior, and the WARN keeps it searchable.
 */
export function ensureBedrockProviderRegistered(
  registrar: BedrockRegistrar = defaultBedrockRegistrar
): Promise<void> {
  bedrockRegistrationPromise ??= registrar()
    .then(() => {
      getLog().debug('pi.bedrock_provider_register_completed');
    })
    .catch((err: unknown) => {
      getLog().warn({ err }, 'pi.bedrock_provider_register_failed');
    });
  return bedrockRegistrationPromise;
}

/** Test-only: reset the once-per-process registration cache. */
export function resetBedrockRegistrationForTest(): void {
  bedrockRegistrationPromise = undefined;
}

// Pi provider id → env var name used by pi-ai's getEnvApiKey(). Generated
// from the installed pi-ai SDK (full backend coverage) — see
// scripts/generate-pi-vendor-map.ts; `bun run check:pi-vendor-map` guards drift.
import { PI_PROVIDER_ENV_VARS } from './pi-vendor-map.generated';

// Pi provider id → OAuth-subscription env var. pi-ai's getApiKeyEnvVars lists
// the OAuth var ahead of the API-key var (e.g. anthropic →
// ["ANTHROPIC_OAUTH_TOKEN", "ANTHROPIC_API_KEY"]). Archon delivers subscriptions
// to env-only chat under this var (delivery.ts), but the per-user injection never
// writes to process.env — Pi only ingests requestOptions.env via the explicit
// bridge below, so the bridge must read the OAuth var too (#1984). github-copilot
// delivers its single COPILOT_GITHUB_TOKEN (already the API-key var); openai is
// shipped by delivery.ts as a CODEX_HOME/auth.json file (dropped in env-only chat),
// never an env var — so on this env channel anthropic is the only backend that
// needs a distinct OAuth var.
const PI_OAUTH_ENV_VARS: Readonly<Record<string, string>> = {
  anthropic: 'ANTHROPIC_OAUTH_TOKEN',
};

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.pi');
  return cachedLog;
}

// Structured-output prompt augmentation is shared across providers. Import
// once for local use and re-export so existing callers and tests keep their
// import path stable; new providers should import from `../../shared/structured-output`.
import { augmentPromptForJsonSchema } from '../../shared/structured-output';
export { augmentPromptForJsonSchema };

/**
 * Anthropic subscription OAuth access tokens are `sk-ant-oat…` (API keys are
 * `sk-ant-api…`). This is the same content-shape discriminator pi-ai's
 * createClient uses to pick OAuth vs API-key auth downstream, so Archon's
 * detection can never disagree with the SDK's.
 */
function isAnthropicOAuthToken(token: string | null | undefined): boolean {
  return typeof token === 'string' && token.startsWith('sk-ant-oat');
}

/**
 * Archon's default system prompt for Pi sessions that authenticate to
 * Anthropic with a SUBSCRIPTION OAuth token (Claude Pro/Max, `sk-ant-oat*`).
 *
 * WHY THIS EXISTS (load-bearing — do not drop without re-reading):
 * Pi's built-in coding-agent system prompt (pi-coding-agent's
 * `buildSystemPrompt`) embeds a self-referential "Pi documentation" block
 * ("...read only when the user asks about pi itself, its SDK, extensions,
 * themes, skills, or TUI...") plus an "operating inside pi, a coding agent
 * harness" identity line. That block is dense with third-party-coding-tool
 * vocabulary, and Anthropic's post-2026-04-04 subscription-OAuth enforcement
 * classifies any request carrying it as a third-party app — returning
 * `400 invalid_request_error "You're out of extra usage"` for Pro/Max OAuth
 * tokens, even though the same token works for first-party Claude Code.
 *
 * Supplying ANY custom system prompt makes pi-coding-agent take its
 * `customPrompt` branch, which omits the incriminating block entirely. pi-ai
 * still prepends the OAuth-required "You are Claude Code, Anthropic's official
 * CLI for Claude." block as system[0], so subscription tokens are accepted.
 * Verified at the wire level (PR #1831): [CC, this-prompt] → HTTP 200;
 * [CC, pi-default-with-docs-block] → HTTP 400.
 *
 * Scope is deliberately narrow: the fallback applies ONLY when the session
 * will use Anthropic subscription-OAuth auth. API-key sessions and
 * non-Anthropic backends keep Pi's built-in prompt (with its dynamic tool
 * list) — there is no benefit to replacing it there. Workflow- or
 * request-level `systemPrompt` still wins (see sendQuery step 4c).
 */
export const ARCHON_PI_ANTHROPIC_OAUTH_SYSTEM_PROMPT = `You are an expert coding assistant. You help users by reading files, executing commands, editing code, and writing new files.

Use the available tools to accomplish the task:
- read: examine file contents instead of cat/sed
- bash: run shell commands (ls, grep, find, build, test)
- edit: make precise, minimal text replacements; each match must be unique
- write: create new files or fully rewrite existing ones

Guidelines:
- Prefer reading files before editing them.
- Keep edits small and targeted; do not pad with unchanged context.
- Be concise in your responses.
- Show file paths clearly when working with files.`;

type PiCodingAgentModule = Awaited<typeof import('@earendil-works/pi-coding-agent')>;
type PiEventBridgeModule = Awaited<typeof import('./event-bridge')>;
type PiOptionsTranslatorModule = Awaited<typeof import('./options-translator')>;
type PiResourceLoaderModule = Awaited<typeof import('./resource-loader')>;
type PiSessionResolverModule = Awaited<typeof import('./session-resolver')>;
type PiUiContextModule = Awaited<typeof import('./ui-context-stub')>;
type PiNativeToolsModule = Awaited<typeof import('./native-tools')>;
type ParsedPiModelRef = NonNullable<ReturnType<typeof parsePiModelRef>>;
type PiAuthStorage = ReturnType<PiCodingAgentModule['AuthStorage']['create']>;
type PiModelRegistry = ReturnType<PiCodingAgentModule['ModelRegistry']['create']>;
type PiModel = NonNullable<ReturnType<PiModelRegistry['find']>>;
type PiSettingsManager = ReturnType<PiCodingAgentModule['SettingsManager']['inMemory']>;
type PiSessionManager = Awaited<
  ReturnType<PiSessionResolverModule['resolvePiSession']>
>['sessionManager'];
type PiAgentSession = Awaited<ReturnType<PiCodingAgentModule['createAgentSession']>>['session'];
type PiUiBridge = ReturnType<PiUiContextModule['createArchonUIBridge']>;

interface PiRuntimeModules {
  piCodingAgent: PiCodingAgentModule;
  bridgeSession: PiEventBridgeModule['bridgeSession'];
  resolvePiSkills: PiOptionsTranslatorModule['resolvePiSkills'];
  resolvePiThinkingLevel: PiOptionsTranslatorModule['resolvePiThinkingLevel'];
  resolvePiTools: PiOptionsTranslatorModule['resolvePiTools'];
  buildDefaultPiTools: PiOptionsTranslatorModule['buildDefaultPiTools'];
  createNoopResourceLoader: PiResourceLoaderModule['createNoopResourceLoader'];
  getOrCreateReloadedExtensionLoader: PiResourceLoaderModule['getOrCreateReloadedExtensionLoader'];
  resolvePiSession: PiSessionResolverModule['resolvePiSession'];
  createArchonUIBridge: PiUiContextModule['createArchonUIBridge'];
  createArchonUIContext: PiUiContextModule['createArchonUIContext'];
  buildPiNativeToolDefinitions: PiNativeToolsModule['buildPiNativeToolDefinitions'];
}

interface PiAuthContext {
  authStorage: PiAuthStorage;
  modelRegistry: PiModelRegistry;
}

interface PiNodeOptions {
  warnings: string[];
  thinkingLevel?: ReturnType<PiOptionsTranslatorModule['resolvePiThinkingLevel']>['level'];
  filteredTools: ReturnType<PiOptionsTranslatorModule['resolvePiTools']>['tools'];
  systemPrompt?: string;
  explicitSystemPrompt?: string;
  skillPaths: string[];
}

interface PiSessionRuntime {
  session: PiAgentSession;
  uiBridge?: PiUiBridge;
  model?: PiModel;
}

async function loadPiRuntimeModules(): Promise<PiRuntimeModules> {
  const [
    piCodingAgent,
    eventBridge,
    optionsTranslator,
    resourceLoader,
    sessionResolver,
    uiContext,
    nativeTools,
  ] = await Promise.all([
    import('@earendil-works/pi-coding-agent'),
    import('./event-bridge'),
    import('./options-translator'),
    import('./resource-loader'),
    import('./session-resolver'),
    import('./ui-context-stub'),
    import('./native-tools'),
  ]);
  return {
    piCodingAgent,
    bridgeSession: eventBridge.bridgeSession,
    resolvePiSkills: optionsTranslator.resolvePiSkills,
    resolvePiThinkingLevel: optionsTranslator.resolvePiThinkingLevel,
    resolvePiTools: optionsTranslator.resolvePiTools,
    buildDefaultPiTools: optionsTranslator.buildDefaultPiTools,
    createNoopResourceLoader: resourceLoader.createNoopResourceLoader,
    getOrCreateReloadedExtensionLoader: resourceLoader.getOrCreateReloadedExtensionLoader,
    resolvePiSession: sessionResolver.resolvePiSession,
    createArchonUIBridge: uiContext.createArchonUIBridge,
    createArchonUIContext: uiContext.createArchonUIContext,
    buildPiNativeToolDefinitions: nativeTools.buildPiNativeToolDefinitions,
  };
}

function applyPiConfigEnv(piConfig: ReturnType<typeof parsePiConfig>): void {
  if (!piConfig.env) return;
  const applied: string[] = [];
  for (const [key, value] of Object.entries(piConfig.env)) {
    if (process.env[key] !== undefined) continue;
    process.env[key] = value;
    applied.push(key);
  }
  if (applied.length > 0) {
    getLog().debug({ keys: applied }, 'pi.config_env_applied');
  }
}

function resolveRequiredPiModelRef(modelRef: string | undefined): ParsedPiModelRef {
  if (!modelRef) {
    throw new Error(
      'Pi provider requires a model. Set `model` on the workflow node or `assistants.pi.model` in .archon/config.yaml. ' +
        "Format: '<pi-provider-id>/<model-id>' (e.g. 'google/gemini-2.5-pro')."
    );
  }
  const parsed = parsePiModelRef(modelRef);
  if (!parsed) {
    throw new Error(
      `Invalid Pi model ref: '${modelRef}'. Expected format '<pi-provider-id>/<model-id>' (e.g. 'google/gemini-2.5-pro').`
    );
  }
  return parsed;
}

function createPiAuthContext(
  piCodingAgent: PiCodingAgentModule,
  parsed: ParsedPiModelRef,
  requestOptions: SendQueryOptions | undefined
): PiAuthContext {
  try {
    const archonAuthPath =
      (requestOptions?.env?.ARCHON_PI_AUTH_PATH ?? process.env.ARCHON_PI_AUTH_PATH)?.trim() ||
      undefined;
    const authStorage = piCodingAgent.AuthStorage.create(archonAuthPath);
    return { authStorage, modelRegistry: piCodingAgent.ModelRegistry.create(authStorage) };
  } catch (err) {
    const e = err as Error;
    getLog().error({ err: e, piProvider: parsed.provider }, 'pi.auth_storage_init_failed');
    throw new Error(
      `Pi auth storage init failed: ${e.message}. Check that ~/.pi/agent/auth.json ` +
        '(or $PI_CODING_AGENT_DIR/auth.json) is valid JSON and readable.'
    );
  }
}

function applyPiRuntimeCredential(
  authStorage: PiAuthStorage,
  parsed: ParsedPiModelRef,
  requestOptions: SendQueryOptions | undefined
): { envVarName: string | undefined; oauthVarName: string | undefined } {
  const envVarName = PI_PROVIDER_ENV_VARS[parsed.provider];
  const oauthVarName = PI_OAUTH_ENV_VARS[parsed.provider];
  const readEnvOverride = (name: string | undefined): string | undefined =>
    name ? (requestOptions?.env?.[name] ?? process.env[name]) : undefined;
  const envOverride = readEnvOverride(oauthVarName) ?? readEnvOverride(envVarName);
  if (envOverride) authStorage.setRuntimeApiKey(parsed.provider, envOverride);
  return { envVarName, oauthVarName };
}

function logPiStaticModelMiss(modelRegistry: PiModelRegistry, parsed: ParsedPiModelRef): void {
  const loadError = modelRegistry.getError?.();
  if (loadError) {
    getLog().warn(
      { piProvider: parsed.provider, modelId: parsed.modelId, loadError },
      'pi.model_registry_load_error'
    );
  }
  getLog().info(
    { piProvider: parsed.provider, modelId: parsed.modelId },
    'pi.model_not_in_static_catalog_deferring'
  );
}

async function resolveAndValidatePiCredential(input: {
  authStorage: PiAuthStorage;
  parsed: ParsedPiModelRef;
  model: PiModel | undefined;
  envVarName: string | undefined;
  oauthVarName: string | undefined;
}): Promise<string | null | undefined> {
  const { authStorage, parsed, model, envVarName, oauthVarName } = input;
  const resolvedKey =
    model || parsed.provider === 'anthropic'
      ? await authStorage.getApiKey(parsed.provider)
      : undefined;
  if (!model || resolvedKey) return resolvedKey;
  if (!envVarName) {
    getLog().info(
      {
        piProvider: parsed.provider,
        envHint: `Provider '${parsed.provider}' is not in the Archon adapter's env-var table — file an issue if you want a shortcut env var for it.`,
        loginHint: `Or run \`pi\` and type \`/login\` locally to authenticate '${parsed.provider}' via OAuth; credentials land in ~/.pi/agent/auth.json and are picked up automatically.`,
      },
      'pi.auth_missing'
    );
    return resolvedKey;
  }
  const varHint = oauthVarName ? `${oauthVarName} (subscription) or ${envVarName}` : envVarName;
  const envHint = `Set ${varHint} in the environment or codebase env vars (.archon/config.yaml env: section).`;
  const loginHint = `Or run \`pi\` and type \`/login\` locally to authenticate '${parsed.provider}' via OAuth; credentials land in ~/.pi/agent/auth.json and are picked up automatically.`;
  throw new Error(
    `Pi auth: no credentials for provider '${parsed.provider}'. ${envHint} ${loginHint}`
  );
}

function coercePiSystemPrompt(
  value: SystemPromptInput | undefined,
  source: 'request' | 'node'
): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value === 'string') return value;
  getLog().warn(
    { systemPromptType: typeof value, systemPromptSource: source },
    'pi.system_prompt_dropped_non_string'
  );
  return undefined;
}

function resolvePiNodeOptions(input: {
  modules: PiRuntimeModules;
  cwd: string;
  requestOptions: SendQueryOptions | undefined;
  parsed: ParsedPiModelRef;
  resolvedKey: string | null | undefined;
}): PiNodeOptions {
  const { modules, cwd, requestOptions, parsed, resolvedKey } = input;
  const nodeConfig = requestOptions?.nodeConfig;
  const warnings: string[] = [];
  const { level: thinkingLevel, warning: thinkingWarning } =
    modules.resolvePiThinkingLevel(nodeConfig);
  if (thinkingWarning) warnings.push(thinkingWarning);

  const { tools: filteredTools, unknownTools } = modules.resolvePiTools(
    cwd,
    nodeConfig,
    requestOptions?.env
  );
  if (unknownTools.length > 0) {
    warnings.push(
      `Pi ignored unknown tool names: ${unknownTools.join(', ')}. Pi's built-in tools: read, bash, edit, write, grep, find, ls.`
    );
  }

  const explicitSystemPrompt =
    coercePiSystemPrompt(requestOptions?.systemPrompt, 'request') ??
    coercePiSystemPrompt(nodeConfig?.systemPrompt, 'node');
  const usesAnthropicOAuth = parsed.provider === 'anthropic' && isAnthropicOAuthToken(resolvedKey);
  const systemPrompt =
    explicitSystemPrompt ??
    (usesAnthropicOAuth ? ARCHON_PI_ANTHROPIC_OAUTH_SYSTEM_PROMPT : undefined);

  const { paths: skillPaths, missing } = modules.resolvePiSkills(cwd, nodeConfig?.skills);
  if (missing.length > 0) {
    warnings.push(
      `Pi could not resolve skill names: ${missing.join(', ')}. Searched .agents/skills and .claude/skills (project + user-global). Each must be a directory containing SKILL.md.`
    );
  }

  return { warnings, thinkingLevel, filteredTools, systemPrompt, explicitSystemPrompt, skillPaths };
}

function createPiSettingsManager(
  piCodingAgent: PiCodingAgentModule,
  cwd: string
): PiSettingsManager {
  const fileSettings = piCodingAgent.SettingsManager.create(cwd);
  const settingsErrors = fileSettings.drainErrors();
  for (const { scope, error: err } of settingsErrors) {
    getLog().warn({ scope, err }, 'pi.settings_load_error');
  }

  const globalSettings = fileSettings.getGlobalSettings();
  const projectSettings = fileSettings.getProjectSettings();
  const seedSettings: Record<string, unknown> = { ...globalSettings };
  for (const key of Object.keys(projectSettings)) {
    const pv = (projectSettings as Record<string, unknown>)[key];
    if (pv === undefined) continue;
    const gv = seedSettings[key];
    seedSettings[key] =
      typeof pv === 'object' &&
      pv !== null &&
      !Array.isArray(pv) &&
      typeof gv === 'object' &&
      gv !== null &&
      !Array.isArray(gv)
        ? { ...(gv as Record<string, unknown>), ...(pv as Record<string, unknown>) }
        : pv;
  }
  return piCodingAgent.SettingsManager.inMemory(
    seedSettings as ReturnType<typeof fileSettings.getGlobalSettings>
  );
}

async function createPiResourceLoader(input: {
  modules: PiRuntimeModules;
  cwd: string;
  enableExtensions: boolean;
  systemPrompt?: string;
  skillPaths: string[];
  modelRegistry: PiModelRegistry;
}): Promise<DefaultResourceLoader> {
  const { modules, cwd, enableExtensions, systemPrompt, skillPaths, modelRegistry } = input;
  const loaderOptions = {
    ...(systemPrompt !== undefined ? { systemPrompt } : {}),
    ...(skillPaths.length > 0 ? { additionalSkillPaths: skillPaths } : {}),
  };
  if (!enableExtensions) return modules.createNoopResourceLoader(cwd, loaderOptions);

  const { loader, providerRegistrations } = await modules.getOrCreateReloadedExtensionLoader(
    cwd,
    loaderOptions
  );
  for (const { name, config, extensionPath } of providerRegistrations) {
    try {
      modelRegistry.registerProvider(name, config);
    } catch (err) {
      getLog().warn(
        { err, piExtensionProvider: name, extensionPath },
        'pi.extension_provider_reapply_failed'
      );
    }
  }
  if (providerRegistrations.length > 0) {
    getLog().debug({ count: providerRegistrations.length }, 'pi.extension_providers_reapplied');
  }
  return loader;
}

function buildPiCustomTools(input: {
  modules: PiRuntimeModules;
  cwd: string;
  requestOptions: SendQueryOptions | undefined;
  filteredTools: PiNodeOptions['filteredTools'];
}):
  | ReturnType<PiNativeToolsModule['buildPiNativeToolDefinitions']>
  | PiNodeOptions['filteredTools'] {
  const { modules, cwd, requestOptions, filteredTools } = input;
  const nativeToolDefs = requestOptions?.nativeTools?.length
    ? modules.buildPiNativeToolDefinitions(requestOptions.nativeTools)
    : [];
  const baseTools =
    filteredTools ??
    (nativeToolDefs.length > 0 ? modules.buildDefaultPiTools(cwd, requestOptions?.env) : undefined);
  return nativeToolDefs.length > 0 ? [...(baseTools ?? []), ...nativeToolDefs] : filteredTools;
}

async function bindPiExtensions(input: {
  modules: PiRuntimeModules;
  session: PiAgentSession;
  enableExtensions: boolean;
  interactive: boolean;
  extensionFlags: Record<string, string | boolean> | undefined;
}): Promise<PiUiBridge | undefined> {
  const { modules, session, enableExtensions, interactive, extensionFlags } = input;
  if (enableExtensions && extensionFlags) {
    const runner = session.extensionRunner;
    if (runner) {
      for (const [name, value] of Object.entries(extensionFlags)) runner.setFlagValue(name, value);
    }
  }

  const uiBridge = interactive ? modules.createArchonUIBridge() : undefined;
  if (uiBridge) {
    await session.bindExtensions({ uiContext: modules.createArchonUIContext(uiBridge) });
  } else if (enableExtensions) {
    await session.bindExtensions({});
  }
  return uiBridge;
}

async function resolveExtensionPiModel(input: {
  session: PiAgentSession;
  modelRegistry: PiModelRegistry;
  parsed: ParsedPiModelRef;
  model: PiModel | undefined;
}): Promise<PiModel | undefined> {
  const { session, modelRegistry, parsed } = input;
  let model = input.model;
  if (model) return model;
  model = modelRegistry.find(parsed.provider, parsed.modelId);
  if (!model) {
    session.dispose();
    throw new Error(
      `Pi model not found: provider='${parsed.provider}' model='${parsed.modelId}'. ` +
        'The model was not found in the static catalog or via any installed extension. ' +
        'Ensure the provider extension is installed (e.g. `pi install npm:pi-provider-kiro`) ' +
        'and `enableExtensions: true` is set in .archon/config.yaml.'
    );
  }
  try {
    await session.setModel(model);
  } catch (err) {
    session.dispose();
    throw err;
  }
  return model;
}

async function createPiSessionRuntime(input: {
  modules: PiRuntimeModules;
  cwd: string;
  authStorage: PiAuthStorage;
  modelRegistry: PiModelRegistry;
  sessionManager: PiSessionManager;
  settingsManager: PiSettingsManager;
  resourceLoader: DefaultResourceLoader;
  model: PiModel | undefined;
  parsed: ParsedPiModelRef;
  nodeOptions: PiNodeOptions;
  requestOptions: SendQueryOptions | undefined;
  enableExtensions: boolean;
  interactive: boolean;
  extensionFlags: Record<string, string | boolean> | undefined;
}): Promise<PiSessionRuntime> {
  const piCustomTools = buildPiCustomTools({
    modules: input.modules,
    cwd: input.cwd,
    requestOptions: input.requestOptions,
    filteredTools: input.nodeOptions.filteredTools,
  });
  const { session, modelFallbackMessage } = await input.modules.piCodingAgent.createAgentSession({
    cwd: input.cwd,
    ...(input.model ? { model: input.model } : {}),
    authStorage: input.authStorage,
    modelRegistry: input.modelRegistry,
    sessionManager: input.sessionManager,
    settingsManager: input.settingsManager,
    resourceLoader: input.resourceLoader,
    ...(input.nodeOptions.thinkingLevel ? { thinkingLevel: input.nodeOptions.thinkingLevel } : {}),
    ...(piCustomTools !== undefined
      ? { customTools: piCustomTools, noTools: 'builtin' as const }
      : {}),
  });
  if (modelFallbackMessage && input.model) {
    // The caller preserves generator yield order by turning this sentinel into a chunk.
    input.nodeOptions.warnings.push(modelFallbackMessage);
  }
  const uiBridge = await bindPiExtensions({
    modules: input.modules,
    session,
    enableExtensions: input.enableExtensions,
    interactive: input.interactive,
    extensionFlags: input.extensionFlags,
  });
  const model = await resolveExtensionPiModel({
    session,
    modelRegistry: input.modelRegistry,
    parsed: input.parsed,
    model: input.model,
  });
  return { session, uiBridge, model };
}

function logPiSessionStarted(input: {
  parsed: ParsedPiModelRef;
  cwd: string;
  nodeOptions: PiNodeOptions;
  enableExtensions: boolean;
  interactive: boolean;
  nodeId: string | undefined;
  resumeSessionId: string | undefined;
  resumeFailed: boolean;
}): void {
  getLog().info(
    {
      piProvider: input.parsed.provider,
      modelId: input.parsed.modelId,
      cwd: input.cwd,
      thinkingLevel: input.nodeOptions.thinkingLevel,
      toolCount: input.nodeOptions.filteredTools?.length,
      systemPromptSource:
        input.nodeOptions.explicitSystemPrompt !== undefined
          ? 'explicit'
          : input.nodeOptions.systemPrompt !== undefined
            ? 'anthropic-oauth-default'
            : 'pi-builtin',
      skillCount: input.nodeOptions.skillPaths.length,
      missingSkillCount: input.nodeOptions.warnings.filter(w =>
        w.startsWith('Pi could not resolve skill names')
      ).length,
      extensionsEnabled: input.enableExtensions,
      interactive: input.interactive,
      nodeId: input.nodeId,
      resumed: input.resumeSessionId !== undefined && !input.resumeFailed,
    },
    'pi.session_started'
  );
}

function* piSystemWarningChunks(warnings: string[]): Generator<MessageChunk> {
  for (const warning of warnings) {
    yield { type: 'system', content: `⚠️ ${warning}` };
  }
}

function piResumeWarning(resumeFailed: boolean): string[] {
  return resumeFailed ? ['Could not resume Pi session. Starting fresh conversation.'] : [];
}

async function acquirePiSemaphore(
  piConfig: ReturnType<typeof parsePiConfig>
): Promise<Semaphore | undefined> {
  const maxConcurrent = piConfig.maxConcurrent;
  if (maxConcurrent !== undefined && piSemaphore === undefined) {
    piSemaphore = new Semaphore(maxConcurrent);
    getLog().info({ maxConcurrent }, 'pi.semaphore_initialized');
  }
  const sem = piSemaphore;
  if (sem !== undefined) {
    getLog().debug('pi.semaphore_acquiring');
    await sem.acquire();
    getLog().debug('pi.semaphore_acquired');
  }
  return sem;
}

/**
 * Pi community provider — wraps `@earendil-works/pi-coding-agent`'s full
 * coding-agent harness. Each `sendQuery()` call creates a fresh session
 * (no reuse) so concurrent calls don't collide.
 */
export class PiProvider implements IAgentProvider {
  async *sendQuery(
    prompt: string,
    cwd: string,
    resumeSessionId?: string,
    requestOptions?: SendQueryOptions
  ): AsyncGenerator<MessageChunk> {
    ensurePiPackageDirShim();
    const bedrockReady = ensureBedrockProviderRegistered();
    const modules = await loadPiRuntimeModules();
    await bedrockReady;

    const piConfig = parsePiConfig(requestOptions?.assistantConfig ?? {});
    applyPiConfigEnv(piConfig);
    const parsed = resolveRequiredPiModelRef(requestOptions?.model ?? piConfig.model);
    const { authStorage, modelRegistry } = createPiAuthContext(
      modules.piCodingAgent,
      parsed,
      requestOptions
    );

    const model = modelRegistry.find(parsed.provider, parsed.modelId);
    if (!model) logPiStaticModelMiss(modelRegistry, parsed);
    const { envVarName, oauthVarName } = applyPiRuntimeCredential(
      authStorage,
      parsed,
      requestOptions
    );
    const resolvedKey = await resolveAndValidatePiCredential({
      authStorage,
      parsed,
      model,
      envVarName,
      oauthVarName,
    });

    const nodeOptions = resolvePiNodeOptions({ modules, cwd, requestOptions, parsed, resolvedKey });
    yield* piSystemWarningChunks(nodeOptions.warnings.splice(0));

    const { sessionManager, resumeFailed } = await modules.resolvePiSession(cwd, resumeSessionId);
    yield* piSystemWarningChunks(piResumeWarning(resumeFailed));

    const settingsManager = createPiSettingsManager(modules.piCodingAgent, cwd);
    const { enableExtensions, interactive, extensionFlags } = resolvePiExtensionSettings(
      piConfig,
      requestOptions?.nodeConfig?.nodeId,
      requestOptions?.nodeConfig?.pi
    );
    const resourceLoader = await createPiResourceLoader({
      modules,
      cwd,
      enableExtensions,
      systemPrompt: nodeOptions.systemPrompt,
      skillPaths: nodeOptions.skillPaths,
      modelRegistry,
    });

    logPiSessionStarted({
      parsed,
      cwd,
      nodeOptions,
      enableExtensions,
      interactive,
      nodeId: requestOptions?.nodeConfig?.nodeId,
      resumeSessionId,
      resumeFailed,
    });

    const runtime = await createPiSessionRuntime({
      modules,
      cwd,
      authStorage,
      modelRegistry,
      sessionManager,
      settingsManager,
      resourceLoader,
      model,
      parsed,
      nodeOptions,
      requestOptions,
      enableExtensions,
      interactive,
      extensionFlags,
    });
    yield* piSystemWarningChunks(nodeOptions.warnings.splice(0));

    const outputFormat = requestOptions?.outputFormat;
    const effectivePrompt = outputFormat
      ? augmentPromptForJsonSchema(prompt, outputFormat.schema)
      : prompt;
    const sem = await acquirePiSemaphore(piConfig);
    try {
      yield* withResumedOutcome(
        modules.bridgeSession(
          runtime.session,
          effectivePrompt,
          requestOptions?.abortSignal,
          outputFormat?.schema,
          runtime.uiBridge
        ),
        resumedOutcome(resumeSessionId, !resumeFailed)
      );
      getLog().info({ piProvider: parsed.provider }, 'pi.prompt_completed');
    } catch (err) {
      getLog().error({ err, piProvider: parsed.provider }, 'pi.prompt_failed');
      throw err;
    } finally {
      sem?.release();
    }
  }

  getType(): string {
    return 'pi';
  }

  getCapabilities(): ProviderCapabilities {
    return PI_CAPABILITIES;
  }
}
