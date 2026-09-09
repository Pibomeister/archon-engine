/**
 * Remote Coding Agent - Main Entry Point
 * Multi-platform AI coding assistant (Telegram, Discord, Slack, GitHub, Gitea)
 */

// Strip CWD .env keys FIRST — before any application imports read process.env.
// Bun auto-loads .env/.env.local/.env.development/.env.production from CWD;
// when `bun run dev:server` is run from inside a target repo those keys leak
// into the server process. stripCwdEnv() removes them before ~/.archon/.env loads.
import '@archon/paths/strip-cwd-env-boot';
// Pure env→bool boot helper (zero deps) — safe to import before the heavier
// application imports below. Decides the Claude global-auth sentinel posture.
import {
  shouldDefaultClaudeGlobalAuth,
  hasClaudeBootAuthPosture,
} from './boot/claude-auth-posture';

// Load environment variables — after CWD stripping, before application imports.
import { config } from 'dotenv';
import { resolve } from 'path';
import { existsSync } from 'fs';
import { BUNDLED_IS_BINARY, getArchonEnvPath } from '@archon/paths';

// In dev/source mode, load the repo root .env (platform tokens, API keys, etc.)
// import.meta.dir is frozen at build time, so skip in compiled binaries.
const envPath = BUNDLED_IS_BINARY ? undefined : resolve(import.meta.dir, '..', '..', '..', '.env');

if (envPath) {
  const dotenvResult = config({ path: envPath });
  if (dotenvResult.error) {
    // Use console.error since logger depends on env vars (LOG_LEVEL)
    console.error(`Failed to load .env from ${envPath}: ${dotenvResult.error.message}`);
    console.error('Hint: Copy .env.example to .env and configure your credentials.');
  }
}

// Load archon-owned env from ~/.archon/.env (user scope) and <cwd>/.archon/.env
// (repo scope, wins over user) with override: true. Keeps the server in sync
// with the CLI — see packages/paths/src/env-loader.ts and the three-path model
// (#1302 / #1303).
import { loadArchonEnv } from '@archon/paths/env-loader';
loadArchonEnv(process.cwd());

// Smart default: fall back to Claude Code's built-in OAuth (`claude /login`)
// ONLY for solo installs with no explicit credentials. Per-user installs
// (TOKEN_ENCRYPTION_KEY) deliver Claude auth per-request, so the global-auth
// sentinel is skipped there — it's misleading, not load-bearing (#1983).
if (shouldDefaultClaudeGlobalAuth(process.env)) {
  process.env.CLAUDE_USE_GLOBAL_AUTH = 'true';
}

import { registerBuiltinProviders, registerCommunityProviders } from '@archon/providers';
import { getVendorCatalog } from '@archon/core';

// Bootstrap provider registry before any provider lookups
registerBuiltinProviders();
registerCommunityProviders();
// Fail fast at boot (not on first API request) if any registration declares a
// credential vendor the delivery map can't deliver — that's a provider bug
// that must block startup, not surface as a runtime 500 (#1955).
getVendorCatalog();

import { OpenAPIHono, z } from '@hono/zod-openapi';
import { validationErrorHook } from './routes/openapi-defaults';
import {
  TelegramAdapter,
  GitHubAdapter,
  DiscordAdapter,
  SlackAdapter,
  SlackWorkflowBridge,
} from '@archon/adapters';
import { GiteaAdapter } from '@archon/adapters/community/forge/gitea';
import { GitLabAdapter } from '@archon/adapters/community/forge/gitlab';
import { WebAdapter } from './adapters/web';
import { MessagePersistence } from './adapters/web/persistence';
import { SSETransport } from './adapters/web/transport';
import { WorkflowEventBridge } from './adapters/web/workflow-bridge';
import { DashboardEventPoller } from './adapters/web/dashboard-event-poller';
import { PgNotifyListener } from './adapters/web/pg-notify-listener';
import { registerApiRoutes } from './routes/api';
import { registerGithubWebhookRoute } from './routes/webhooks';
import {
  handleMessage,
  pool,
  ConversationLockManager,
  classifyAndFormatError,
  startCleanupScheduler,
  stopCleanupScheduler,
  getDbNotificationListener,
  loadConfig,
  logConfig,
  getPort,
  createGitHubAppAuthProvider,
  loadAppPrivateKey,
  registerGitHubAppAuthProvider,
  isPerUserGitHubEnabled,
  isPerUserProviderKeysEnabled,
  getDatabaseType,
  assertEncryptionKeyAtBoot,
  assertProviderKeysKeyAtBoot,
  getDecryptedAccessToken,
  type GitHubAuth,
  type IGitHubAppAuthProvider,
} from '@archon/core';
import type { GlobalConfig, IPlatformAdapter } from '@archon/core';
import type { IdentityPlatform } from '@archon/core';
import * as userDb from '@archon/core/db/users';
import {
  createLogger,
  logArchonPaths,
  validateAppDefaultsPaths,
  shutdownTelemetry,
  captureArchonStarted,
  captureArchonActive,
} from '@archon/paths';
import { selectGitHubAuthMode, parseGitCredentialPath } from './github-auth-bootstrap';
import { isDiscordMentionRequired } from './discord-mention';
import {
  getAuth,
  closeAuth,
  isWebAuthEnabled,
  assertWebAuthAtBoot,
  getSignupMode,
  isArchonOwnedAuthPath,
} from './auth';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('server');
  return cachedLog;
}

/**
 * Resolve a platform-native user identifier (Slack U-id, Telegram chat id,
 * Discord snowflake) to an Archon user UUID via auto-create-on-first-sight.
 *
 * Contract: NEVER THROWS. On any failure, warn-log and return undefined so
 * message handling proceeds (writes user_id = NULL on the conversation/run
 * row). This invariant is load-bearing — message processing across three
 * adapters depends on it. Covered by resolve-user-id.test.ts.
 *
 * Exported for testability.
 */
export async function resolveUserId(
  platform: IdentityPlatform,
  platformUserId: string | number | undefined,
  displayName: string | undefined
): Promise<string | undefined> {
  if (platformUserId === undefined || platformUserId === '') {
    return undefined;
  }
  try {
    const user = await userDb.findOrCreateUserByPlatformIdentity(
      platform,
      String(platformUserId),
      displayName
    );
    return user.id;
  } catch (err) {
    getLog().warn(
      { err: err as Error, platform, platformUserId: String(platformUserId) },
      'server.user_resolve_failed'
    );
    return undefined;
  }
}

/**
 * Creates an error handler for message processing failures.
 * Logs the error and attempts to send a user-friendly message to the platform.
 */
function createMessageErrorHandler(
  platform: string,
  adapter: IPlatformAdapter,
  conversationId: string
): (error: unknown) => Promise<void> {
  return async (error: unknown): Promise<void> => {
    getLog().error({ err: error, platform, conversationId }, 'message_processing_failed');
    try {
      const userMessage = classifyAndFormatError(error as Error);
      await adapter.sendMessage(conversationId, userMessage);
    } catch (sendError) {
      getLog().error({ err: sendError, platform, conversationId }, 'error_message_send_failed');
    }
  };
}

/**
 * Handles unhandled promise rejections from the process.
 *
 * Exported for testability. Filters specifically for SDK cleanup races
 * ("Operation aborted" when the PostToolUse hook writes to a closed pipe after
 * a DAG node abort). Those are logged at error level but do not exit the process.
 * All other unhandled rejections are unexpected bugs — they are logged at fatal
 * level and the process exits immediately (Fail Fast principle).
 */
export function handleUnhandledRejection(reason: unknown): void {
  const message = (reason instanceof Error ? reason.message : String(reason)).toLowerCase();
  // SDK cleanup race: PostToolUse hook writes to a closed pipe after a DAG node
  // abort. Safe to absorb — these are transient artifacts, not application bugs.
  if (message.includes('operation aborted')) {
    getLog().error({ reason }, 'unhandled_rejection.sdk_cleanup_race');
    return;
  }
  // All other unhandled rejections are unexpected — crash loudly so they are
  // not silently swallowed (CLAUDE.md: "Fail Fast + Explicit Errors").
  getLog().fatal({ reason }, 'unhandled_rejection.fatal');
  process.exit(1);
}

export interface ServerOptions {
  /**
   * Override the web dist path (for CLI binary with downloaded web-dist).
   * Only effective in production mode (NODE_ENV=production or WEB_UI_DEV unset).
   */
  webDistPath?: string;
  /** Override the port. Range: 1–65535. */
  port?: number;
  /** Run in standalone web-only mode (no Telegram/Slack/GitHub/Discord adapters). */
  skipPlatformAdapters?: boolean;
}

interface DeploymentShape {
  dbKind: ReturnType<typeof getDatabaseType>;
  webAuthEnabled: boolean;
  multiUser: boolean;
  githubAuthMode: ReturnType<typeof selectGitHubAuthMode>['kind'];
  adapterSlack: boolean;
  adapterTelegram: boolean;
  adapterDiscord: boolean;
  adapterGitea: boolean;
  adapterGitlab: boolean;
}

function createDeploymentShape(): DeploymentShape {
  return {
    dbKind: getDatabaseType(),
    webAuthEnabled: isWebAuthEnabled(),
    multiUser: isPerUserProviderKeysEnabled(),
    githubAuthMode: selectGitHubAuthMode(process.env).kind,
    adapterSlack: Boolean(process.env.SLACK_BOT_TOKEN && process.env.SLACK_APP_TOKEN),
    adapterTelegram: Boolean(process.env.TELEGRAM_BOT_TOKEN),
    adapterDiscord: Boolean(process.env.DISCORD_BOT_TOKEN),
    adapterGitea: Boolean(
      process.env.GITEA_URL && process.env.GITEA_TOKEN && process.env.GITEA_WEBHOOK_SECRET
    ),
    adapterGitlab: Boolean(process.env.GITLAB_TOKEN && process.env.GITLAB_WEBHOOK_SECRET),
  };
}

function startTelemetryHeartbeat(deploymentShape: DeploymentShape): void {
  const telemetryHeartbeatIntervalMs = 24 * 60 * 60 * 1000;
  setInterval(() => {
    captureArchonActive({ surface: 'server', ...deploymentShape });
  }, telemetryHeartbeatIntervalMs).unref();
}

function assertAiCredentialPosture(): void {
  const hasClaudeCredentials = hasClaudeBootAuthPosture(process.env);
  const hasCodexCredentials = process.env.CODEX_ID_TOKEN && process.env.CODEX_ACCESS_TOKEN;

  if (!hasClaudeCredentials && !hasCodexCredentials) {
    getLog().fatal(
      {
        checked: {
          claude: ['CLAUDE_API_KEY', 'CLAUDE_CODE_OAUTH_TOKEN', 'CLAUDE_USE_GLOBAL_AUTH'],
          codex: ['CODEX_ID_TOKEN', 'CODEX_ACCESS_TOKEN'],
        },
        hints: [
          'Set CLAUDE_USE_GLOBAL_AUTH=true in .env (requires `claude /login` first)',
          'Or set CLAUDE_API_KEY in .env',
          'Or set CODEX_ID_TOKEN + CODEX_ACCESS_TOKEN in .env',
          'See .env.example for all options',
        ],
        envFile: BUNDLED_IS_BINARY ? getArchonEnvPath() : envPath,
      },
      'no_ai_credentials'
    );
    process.exit(1);
  }

  if (!hasClaudeCredentials) {
    getLog().warn(
      { checked: ['CLAUDE_API_KEY', 'CLAUDE_CODE_OAUTH_TOKEN', 'CLAUDE_USE_GLOBAL_AUTH'] },
      'claude_credentials_missing'
    );
  }
  if (!hasCodexCredentials) {
    getLog().warn(
      { checked: ['CODEX_ID_TOKEN', 'CODEX_ACCESS_TOKEN'] },
      'codex_credentials_missing'
    );
  }
}

interface WebRuntime {
  transport: SSETransport;
  persistence: MessagePersistence;
  workflowBridge: WorkflowEventBridge;
  webAdapter: WebAdapter;
  dashboardPoller: DashboardEventPoller;
  pgNotifyListener?: PgNotifyListener;
}

async function startWebRuntime(): Promise<WebRuntime> {
  const holder: { persistence?: MessagePersistence } = {};
  const transport = new SSETransport(conversationId => {
    // Flush (not clear!) — the orchestrator/workflow may still be writing messages
    // even though the SSE stream disconnected. Clearing the dbId mapping would cause
    // all subsequent messages to be lost (never persisted to DB).
    void holder.persistence?.flush(conversationId).catch((e: unknown) => {
      getLog().error({ conversationId, err: e }, 'transport_cleanup_flush_failed');
    });
  });
  const persistence = new MessagePersistence((conversationId, event) =>
    transport.emit(conversationId, event)
  );
  holder.persistence = persistence;
  const workflowBridge = new WorkflowEventBridge(transport);
  const webAdapter = new WebAdapter(transport, persistence, workflowBridge);
  await webAdapter.start();
  persistence.startPeriodicFlush();
  const { dashboardPoller, pgNotifyListener } = await startDashboardEvents(transport);
  return { transport, persistence, workflowBridge, webAdapter, dashboardPoller, pgNotifyListener };
}

async function startDashboardEvents(transport: SSETransport): Promise<{
  dashboardPoller: DashboardEventPoller;
  pgNotifyListener?: PgNotifyListener;
}> {
  const dashboardPoller = new DashboardEventPoller();
  const dbNotifier = getDbNotificationListener();
  if (dbNotifier) {
    dashboardPoller.start(transport, 10_000);
    const pgNotifyListener = new PgNotifyListener(dbNotifier, dashboardPoller);
    await pgNotifyListener.start();
    return { dashboardPoller, pgNotifyListener };
  }
  dashboardPoller.start(transport, 1_500);
  return { dashboardPoller };
}

interface PlatformRuntime {
  github: GitHubAdapter | null;
  githubAppAuthProvider: IGitHubAppAuthProvider | null;
  gitea: GiteaAdapter | null;
  gitlab: GitLabAdapter | null;
  discord: DiscordAdapter | null;
  slack: SlackAdapter | null;
  slackBridge: SlackWorkflowBridge | null;
}

function emptyPlatformRuntime(): PlatformRuntime {
  return {
    github: null,
    githubAppAuthProvider: null,
    gitea: null,
    gitlab: null,
    discord: null,
    slack: null,
    slackBridge: null,
  };
}

async function startPlatformAdapters(
  config: GlobalConfig,
  lockManager: ConversationLockManager,
  activePlatforms: string[],
  skipPlatformAdapters: boolean | undefined
): Promise<PlatformRuntime> {
  if (skipPlatformAdapters) {
    getLog().info('platform_adapters_skipped');
    return emptyPlatformRuntime();
  }

  warnIfNoPlatformConfigured();
  const runtime = emptyPlatformRuntime();
  const githubRuntime = await startGitHubAdapter(config, lockManager, activePlatforms);
  runtime.github = githubRuntime.github;
  runtime.githubAppAuthProvider = githubRuntime.githubAppAuthProvider;
  runtime.gitea = await startGiteaAdapter(config, lockManager, activePlatforms);
  runtime.gitlab = await startGitLabAdapter(config, lockManager, activePlatforms);
  runtime.discord = await startDiscordAdapter(lockManager, activePlatforms);
  const slackRuntime = await startSlackAdapter(lockManager, activePlatforms);
  runtime.slack = slackRuntime.slack;
  runtime.slackBridge = slackRuntime.slackBridge;
  return runtime;
}

function warnIfNoPlatformConfigured(): void {
  const hasTelegram = Boolean(process.env.TELEGRAM_BOT_TOKEN);
  const hasDiscord = Boolean(process.env.DISCORD_BOT_TOKEN);
  const ghAuthMode = selectGitHubAuthMode(process.env);
  const hasGitHub = ghAuthMode.kind !== 'none';
  const hasGitea = Boolean(
    process.env.GITEA_URL && process.env.GITEA_TOKEN && process.env.GITEA_WEBHOOK_SECRET
  );
  const hasGitLab = Boolean(process.env.GITLAB_TOKEN && process.env.GITLAB_WEBHOOK_SECRET);
  if (!hasTelegram && !hasDiscord && !hasGitHub && !hasGitea && !hasGitLab) {
    getLog().warn('no_platform_adapters_configured');
  }
}

async function startGitHubAdapter(
  config: GlobalConfig,
  lockManager: ConversationLockManager,
  activePlatforms: string[]
): Promise<Pick<PlatformRuntime, 'github' | 'githubAppAuthProvider'>> {
  const ghAuthMode = selectGitHubAuthMode(process.env);
  if (ghAuthMode.kind === 'conflict') throw new Error(ghAuthMode.message);
  if (ghAuthMode.kind === 'app') {
    return startGitHubAppAdapter(config, lockManager, activePlatforms);
  }
  if (ghAuthMode.kind === 'pat') {
    return startGitHubPatAdapter(config, lockManager, activePlatforms);
  }
  getLog().info('github_adapter_skipped');
  return { github: null, githubAppAuthProvider: null };
}

async function startGitHubAppAdapter(
  config: GlobalConfig,
  lockManager: ConversationLockManager,
  activePlatforms: string[]
): Promise<Pick<PlatformRuntime, 'github' | 'githubAppAuthProvider'>> {
  const appId = process.env.GITHUB_APP_ID;
  const webhookSecret = process.env.WEBHOOK_SECRET;
  if (!appId || !webhookSecret) {
    throw new Error('GitHub App mode misconfigured: GITHUB_APP_ID and WEBHOOK_SECRET required');
  }
  const privateKey = loadAppPrivateKey();
  assertEncryptionKeyAtBoot();
  if (!isPerUserGitHubEnabled()) {
    getLog().warn(
      'github_app.per_user_disabled — set TOKEN_ENCRYPTION_KEY (and GITHUB_APP_CLIENT_ID) to enable per-user GitHub identity'
    );
  }
  const defaultInstallationId = process.env.GITHUB_APP_INSTALLATION_ID
    ? Number(process.env.GITHUB_APP_INSTALLATION_ID)
    : undefined;
  const githubAppAuthProvider = createGitHubAppAuthProvider({
    appId,
    privateKey,
    slug: process.env.GITHUB_APP_SLUG ?? 'archon',
    defaultInstallationId,
  });
  registerGitHubAppAuthProvider(githubAppAuthProvider);
  const botMention =
    process.env.GITHUB_BOT_MENTION || process.env.BOT_DISPLAY_NAME || config.botName;
  const auth: GitHubAuth = { kind: 'app', provider: githubAppAuthProvider };
  const getUserToken = isPerUserGitHubEnabled()
    ? async (userId: string): Promise<string | undefined> =>
        (await getDecryptedAccessToken(userId)) ?? undefined
    : undefined;
  const github = new GitHubAdapter(auth, webhookSecret, lockManager, botMention, { getUserToken });
  await github.start();
  activePlatforms.push('GitHub (App)');
  getLog().info(
    { slug: githubAppAuthProvider.slug, defaultInstallationId },
    'github.adapter_mode_app'
  );
  return { github, githubAppAuthProvider };
}

async function startGitHubPatAdapter(
  config: GlobalConfig,
  lockManager: ConversationLockManager,
  activePlatforms: string[]
): Promise<Pick<PlatformRuntime, 'github' | 'githubAppAuthProvider'>> {
  const patToken = process.env.GITHUB_TOKEN;
  const webhookSecret = process.env.WEBHOOK_SECRET;
  if (!patToken || !webhookSecret) {
    throw new Error('GitHub PAT mode misconfigured: GITHUB_TOKEN and WEBHOOK_SECRET required');
  }
  const botMention =
    process.env.GITHUB_BOT_MENTION || process.env.BOT_DISPLAY_NAME || config.botName;
  const auth: GitHubAuth = { kind: 'pat', token: patToken };
  const github = new GitHubAdapter(auth, webhookSecret, lockManager, botMention);
  await github.start();
  activePlatforms.push('GitHub');
  getLog().info('github.adapter_mode_pat');
  return { github, githubAppAuthProvider: null };
}

async function startGiteaAdapter(
  config: GlobalConfig,
  lockManager: ConversationLockManager,
  activePlatforms: string[]
): Promise<GiteaAdapter | null> {
  if (!(process.env.GITEA_URL && process.env.GITEA_TOKEN && process.env.GITEA_WEBHOOK_SECRET)) {
    getLog().info('gitea_adapter_skipped');
    return null;
  }
  const botMention =
    process.env.GITEA_BOT_MENTION || process.env.BOT_DISPLAY_NAME || config.botName;
  const gitea = new GiteaAdapter(
    process.env.GITEA_URL,
    process.env.GITEA_TOKEN,
    process.env.GITEA_WEBHOOK_SECRET,
    lockManager,
    botMention
  );
  await gitea.start();
  activePlatforms.push('Gitea');
  return gitea;
}

async function startGitLabAdapter(
  config: GlobalConfig,
  lockManager: ConversationLockManager,
  activePlatforms: string[]
): Promise<GitLabAdapter | null> {
  if (!(process.env.GITLAB_TOKEN && process.env.GITLAB_WEBHOOK_SECRET)) {
    getLog().info('gitlab_adapter_skipped');
    return null;
  }
  const botMention =
    process.env.GITLAB_BOT_MENTION || process.env.BOT_DISPLAY_NAME || config.botName;
  const gitlab = new GitLabAdapter(
    process.env.GITLAB_TOKEN,
    process.env.GITLAB_WEBHOOK_SECRET,
    lockManager,
    process.env.GITLAB_URL || undefined,
    botMention
  );
  await gitlab.start();
  activePlatforms.push('GitLab');
  return gitlab;
}

async function startDiscordAdapter(
  lockManager: ConversationLockManager,
  activePlatforms: string[]
): Promise<DiscordAdapter | null> {
  if (!process.env.DISCORD_BOT_TOKEN) {
    getLog().info('discord_adapter_skipped');
    return null;
  }
  const discordStreamingMode = (process.env.DISCORD_STREAMING_MODE ?? 'batch') as
    | 'stream'
    | 'batch';
  const discord = new DiscordAdapter(process.env.DISCORD_BOT_TOKEN, discordStreamingMode);
  registerDiscordMessageHandler(discord, lockManager);
  try {
    await discord.start();
    activePlatforms.push('Discord');
    return discord;
  } catch (error) {
    logDiscordStartFailure(error);
    return null;
  }
}

function registerDiscordMessageHandler(
  discordAdapter: DiscordAdapter,
  lockManager: ConversationLockManager
): void {
  const discordRequireMention = isDiscordMentionRequired();
  discordAdapter.onMessage(async ({ message, platformUserId, displayName }) => {
    let conversationId = discordAdapter.getConversationId(message);
    if (!message.content) return;
    const isDM = !message.guild;
    if (!isDM && discordRequireMention && !discordAdapter.isBotMentioned(message)) return;
    const content = discordAdapter.stripBotMention(message);
    if (!content) return;
    conversationId = await discordAdapter.ensureThread(conversationId, message);
    const thread = await getDiscordThreadContext(discordAdapter, message);
    const userId = await resolveUserId('discord', platformUserId, displayName);
    lockManager
      .acquireLock(conversationId, async () => {
        await handleMessage(discordAdapter, conversationId, content, {
          ...thread,
          isolationHints: { workflowType: 'thread', workflowId: conversationId },
          userId,
        });
      })
      .catch(createMessageErrorHandler('Discord', discordAdapter, conversationId));
  });
}

async function getDiscordThreadContext(
  discordAdapter: DiscordAdapter,
  message: Parameters<Parameters<DiscordAdapter['onMessage']>[0]>[0]['message']
): Promise<{ threadContext?: string; parentConversationId?: string }> {
  if (!discordAdapter.isThread(message)) return {};
  const history = await discordAdapter.fetchThreadHistory(message);
  const threadContext = history.length > 1 ? history.slice(0, -1).join('\n') : undefined;
  const parentConversationId = discordAdapter.getParentChannelId(message) ?? undefined;
  return { threadContext, parentConversationId };
}

function logDiscordStartFailure(error: unknown): void {
  const err = error as Error;
  const isPrivilegedIntentError = err.message?.includes('disallowed intents');
  const hint = isPrivilegedIntentError
    ? 'Enable "Message Content Intent" in the Discord Developer Portal ' +
      '(your application > Bot > Privileged Gateway Intents) and restart, ' +
      'or unset DISCORD_BOT_TOKEN if you do not want the Discord adapter.'
    : 'Verify DISCORD_BOT_TOKEN is valid, or unset it to disable the Discord adapter.';
  getLog().error({ err, hint }, 'discord.start_failed_continuing_without_adapter');
}

async function startSlackAdapter(
  lockManager: ConversationLockManager,
  activePlatforms: string[]
): Promise<Pick<PlatformRuntime, 'slack' | 'slackBridge'>> {
  if (!(process.env.SLACK_BOT_TOKEN && process.env.SLACK_APP_TOKEN)) {
    getLog().info('slack_adapter_skipped');
    return { slack: null, slackBridge: null };
  }
  const slackStreamingMode = (process.env.SLACK_STREAMING_MODE ?? 'batch') as 'stream' | 'batch';
  const slack = new SlackAdapter(
    process.env.SLACK_BOT_TOKEN,
    process.env.SLACK_APP_TOKEN,
    slackStreamingMode
  );
  registerSlackMessageHandler(slack, lockManager);
  const slackBridge = new SlackWorkflowBridge(slack);
  slackBridge.attach();
  await slack.start();
  activePlatforms.push('Slack');
  return { slack, slackBridge };
}

function registerSlackMessageHandler(
  slackAdapter: SlackAdapter,
  lockManager: ConversationLockManager
): void {
  slackAdapter.onMessage(async event => {
    const conversationId = slackAdapter.getConversationId(event);
    if (!event.text) return;
    const content = slackAdapter.stripBotMention(event.text);
    if (!content) return;
    const thread = await getSlackThreadContext(slackAdapter, event);
    const userId = await resolveUserId('slack', event.user, event.displayName);
    lockManager
      .acquireLock(conversationId, async () => {
        await handleMessage(slackAdapter, conversationId, content, {
          ...thread,
          isolationHints: { workflowType: 'thread', workflowId: conversationId },
          userId,
        });
      })
      .catch(createMessageErrorHandler('Slack', slackAdapter, conversationId));
  });
}

async function getSlackThreadContext(
  slackAdapter: SlackAdapter,
  event: Parameters<Parameters<SlackAdapter['onMessage']>[0]>[0]
): Promise<{ threadContext?: string; parentConversationId?: string }> {
  if (!slackAdapter.isThread(event)) return {};
  const history = await slackAdapter.fetchThreadHistory(event);
  const threadContext = history.length > 1 ? history.slice(0, -1).join('\n') : undefined;
  const parentConversationId = slackAdapter.getParentConversationId(event) ?? undefined;
  return { threadContext, parentConversationId };
}

function createServerApp(
  webAdapter: WebAdapter,
  lockManager: ConversationLockManager,
  activePlatforms: readonly string[],
  github: GitHubAdapter | null
): OpenAPIHono {
  const app = new OpenAPIHono({ defaultHook: validationErrorHook });
  app.onError((err, c) => {
    getLog().error({ err, path: c.req.path, method: c.req.method }, 'unhandled_request_error');
    return c.json({ error: 'Internal server error' }, 500);
  });
  registerWebAuthRoutes(app);
  registerApiRoutes(app, webAdapter, lockManager, activePlatforms);
  if (github) {
    registerGithubWebhookRoute(app, github);
    getLog().info('github_webhook_registered');
  }
  return app;
}

function registerWebAuthRoutes(app: OpenAPIHono): void {
  const webAuth = getAuth();
  if (!webAuth) return;
  app.on(['POST', 'GET'], '/api/auth/*', (c, next) => {
    if (isArchonOwnedAuthPath(c.req.path)) return next();
    return webAuth.handler(c.req.raw);
  });
  getLog().info('web_auth.handler_registered');
  if (getSignupMode() === 'disabled') {
    getLog().warn(
      {
        hint: 'Set ARCHON_AUTH_ALLOWED_EMAILS to invite users, or ARCHON_AUTH_OPEN_SIGNUP=true for open signup.',
      },
      'web_auth.signup_disabled_no_allowlist'
    );
  }
}

function registerInternalGitCredentialRoute(app: OpenAPIHono, github: GitHubAdapter | null): void {
  if (github?.getAuthMode() !== 'app') return;
  const gitCredentialRequestSchema = z.object({
    host: z.string().optional(),
    path: z.string().optional(),
  });

  app.post('/internal/git-credential', async c => {
    try {
      const raw = await c.req.json().catch(() => null);
      const parseResult = gitCredentialRequestSchema.safeParse(raw);
      if (!parseResult.success || parseResult.data.host !== 'github.com') {
        return c.json({ error: 'unsupported host' }, 400);
      }
      const parsed = parseGitCredentialPath(parseResult.data.path ?? '');
      if (!parsed) return c.json({ error: 'unparseable path' }, 400);
      const token = await github.getInstallationToken(parsed.owner, parsed.repo);
      return c.json({ token });
    } catch (err) {
      getLog().error({ err }, 'internal.git_credential_resolve_failed');
      return c.json({ error: 'resolution failed' }, 500);
    }
  });
  getLog().info('internal_git_credential_endpoint_registered');
}

function registerForgeWebhookRoutes(
  app: OpenAPIHono,
  gitea: GiteaAdapter | null,
  gitlab: GitLabAdapter | null
): void {
  if (gitea) registerGiteaWebhookRoute(app, gitea);
  if (gitlab) registerGitLabWebhookRoute(app, gitlab);
}

function registerGiteaWebhookRoute(app: OpenAPIHono, gitea: GiteaAdapter): void {
  app.post('/webhooks/gitea', async c => {
    const eventType = c.req.header('x-gitea-event');
    try {
      const signature = c.req.header('x-gitea-signature');
      if (!signature) return c.json({ error: 'Missing signature header' }, 400);
      const payload = await c.req.text();
      gitea.handleWebhook(payload, signature).catch((error: unknown) => {
        getLog().error({ err: error, eventType }, 'gitea_webhook_processing_error');
      });
      return c.text('OK', 200);
    } catch (error) {
      getLog().error({ err: error, eventType }, 'gitea_webhook_endpoint_error');
      return c.json({ error: 'Internal server error' }, 500);
    }
  });
  getLog().info('gitea_webhook_registered');
}

function registerGitLabWebhookRoute(app: OpenAPIHono, gitlab: GitLabAdapter): void {
  app.post('/webhooks/gitlab', async c => {
    const eventType = c.req.header('x-gitlab-event');
    try {
      const token = c.req.header('x-gitlab-token');
      if (!token) return c.json({ error: 'Missing token header' }, 400);
      const payload = await c.req.text();
      gitlab.handleWebhook(payload, token).catch((error: unknown) => {
        getLog().error({ err: error, eventType }, 'gitlab.webhook_processing_error');
      });
      return c.text('OK', 200);
    } catch (error) {
      getLog().error({ err: error, eventType }, 'gitlab.webhook_endpoint_error');
      return c.json({ error: 'Internal server error' }, 500);
    }
  });
  getLog().info('gitlab_webhook_registered');
}

function registerHealthRoutes(app: OpenAPIHono, lockManager: ConversationLockManager): void {
  app.get('/health', c => c.json({ status: 'ok' }));
  app.get('/health/db', async c => {
    try {
      await pool.query('SELECT 1');
      return c.json({ status: 'ok', database: 'connected' });
    } catch (error) {
      getLog().error({ err: error }, 'health_check_db_failed');
      return c.json({ status: 'error', database: 'disconnected' }, 500);
    }
  });
  app.get('/health/concurrency', c => {
    const { active, queuedTotal, maxConcurrent } = lockManager.getStats();
    return c.json({ status: 'ok', active, queuedTotal, maxConcurrent });
  });
}

async function registerStaticRoutes(app: OpenAPIHono, opts: ServerOptions): Promise<void> {
  if (process.env.NODE_ENV !== 'production' && process.env.WEB_UI_DEV) return;
  const { serveStatic } = await import('hono/bun');
  const pathModule = await import('path');
  const webDistPath =
    opts.webDistPath ??
    pathModule.join(pathModule.dirname(pathModule.dirname(import.meta.dir)), 'web', 'dist');
  if (!existsSync(webDistPath)) {
    getLog().warn({ webDistPath }, 'web_dist_not_found');
  }
  app.use('/assets/*', serveStatic({ root: webDistPath }));
  app.use('/favicon.png', serveStatic({ root: webDistPath, path: 'favicon.png' }));
  app.get('*', serveStatic({ root: webDistPath, path: 'index.html' }));
}

function assertSafeInternalBind(
  githubAppAuthProvider: IGitHubAppAuthProvider | null,
  hostname: string
): void {
  if (!githubAppAuthProvider || hostname === '127.0.0.1' || hostname === 'localhost') return;
  if (process.env.ARCHON_ALLOW_INTERNAL_ON_PUBLIC_BIND === '1') {
    getLog().warn({ hostname }, 'github_app.internal_endpoint_exposed_acknowledged');
    return;
  }
  getLog().fatal({ hostname }, 'github_app.internal_endpoint_public_bind_rejected');
  throw new Error(
    'GitHub App mode is active but the server is bound to a non-loopback ' +
      `interface (${hostname}). The /internal/git-credential endpoint hands out ` +
      'live installation tokens — exposing it would leak credentials to the network. ' +
      'Either bind to 127.0.0.1 (HOST=127.0.0.1), or, if your reverse proxy already ' +
      'drops /internal/* and the upstream needs a non-loopback bind, opt out by ' +
      'setting ARCHON_ALLOW_INTERNAL_ON_PUBLIC_BIND=1.'
  );
}

function warnIfHeaderTrustPublic(hostname: string): void {
  const webAuthHeaderTrustActive =
    Boolean(process.env.ARCHON_WEB_AUTH_HEADER) || isPerUserGitHubEnabled() || isWebAuthEnabled();
  if (!webAuthHeaderTrustActive || hostname === '127.0.0.1' || hostname === 'localhost') return;
  getLog().warn(
    { hostname, headerName: process.env.ARCHON_WEB_AUTH_HEADER || 'X-Archon-User' },
    'web_auth.header_trust_on_public_bind'
  );
}

function serveHttp(app: OpenAPIHono, port: number): void {
  const hostname = process.env.HOST || '0.0.0.0';
  const server = Bun.serve({
    fetch: app.fetch,
    hostname,
    port,
    idleTimeout: 255,
  });
  getLog().info({ port: server.port, hostname }, 'server_listening');
}

export async function startServer(opts: ServerOptions = {}): Promise<void> {
  getLog().info('server_starting');
  // Anonymous once-per-boot startup event (self-gates on opt-out). Flushed by
  // the shutdownTelemetry() call in the SIGINT/SIGTERM shutdown handler.
  // Deployment shape is categorical only — booleans/enums derived from which
  // integrations are configured, never the config values themselves. The
  // adapter gates mirror the env checks the adapter-init section below uses
  // (loadArchonEnv() ran at module load, so process.env is final here).
  const deploymentShape = createDeploymentShape();
  captureArchonStarted({ surface: 'server', ...deploymentShape });

  // Daily heartbeat so long-running servers stay visible in active-install
  // metrics (a boot-only event undercounts server installs after day one).
  // unref() so the timer never keeps the process alive on shutdown.
  // captureArchonActive is synchronous fire-and-forget (errors swallowed
  // internally) — if it ever becomes async, this callback must not return
  // its promise unhandled.
  startTelemetryHeartbeat(deploymentShape);

  // Phase 2: validate the encryption key the moment per-user provider keys are
  // enabled, regardless of GitHub App configuration. TOKEN_ENCRYPTION_KEY alone
  // is the gate — a malformed key must fail boot here rather than at the first
  // PUT /api/auth/providers/* (when an operator is already wired in).
  // No-op when the feature is disabled.
  assertProviderKeysKeyAtBoot();

  // Database auto-detected: SQLite (default) or PostgreSQL (if DATABASE_URL set)
  // No required environment variables - SQLite works out of the box

  // Validate AI assistant credentials (warn if missing, don't fail).
  // A per-user install (TOKEN_ENCRYPTION_KEY) is a valid posture even with no
  // shared Claude key — auth is delivered per request from the encrypted store,
  // so it must NOT trip the no-credentials exit (#1983).
  assertAiCredentialPosture();

  // Test database connection
  try {
    await pool.query('SELECT 1');
    getLog().info('database_connected');
  } catch (error) {
    getLog().fatal({ err: error }, 'database_connection_failed');
    process.exit(1);
  }

  const config = await loadConfig();
  logConfig(config);

  // Start cleanup scheduler
  startCleanupScheduler();

  // Note: orphaned-run cleanup intentionally NOT called at server startup.
  // Running it here killed parallel workflow runs from other processes
  // (CLI, adapters) by flipping their `running` rows to `failed` mid-flight.
  // Same lesson the CLI already learned — see packages/cli/src/cli.ts:256-258.
  // Per CLAUDE.md "No Autonomous Lifecycle Mutation Across Process Boundaries":
  // surface ambiguous state to users and provide a one-click action instead.
  // Users transition a stuck `running` row via the per-row Cancel/Abandon
  // buttons in the Web UI dashboard, or `archon workflow abandon <run-id>`.
  // (`archon workflow cleanup` is a separate command that deletes OLD terminal
  // rows for disk hygiene — it does not handle stuck `running` rows.)
  // See #1216.

  // Log Archon paths configuration
  logArchonPaths();

  // Validate app defaults paths (non-blocking, just logs warnings)
  await validateAppDefaultsPaths();

  // Initialize conversation lock manager
  const maxConcurrent = parseInt(process.env.MAX_CONCURRENT_CONVERSATIONS ?? '10');
  const lockManager = new ConversationLockManager(maxConcurrent);
  getLog().info({ maxConcurrent }, 'lock_manager_initialized');

  // Initialize web adapter (always enabled)
  // Note: Circular references between transport/persistence/workflowBridge are safe because:
  // - transport's cleanup callback references persistence/workflowBridge (declared after, but
  //   only invoked from a grace period timer — well after all constructors complete)
  // - persistence's emitEvent closure references transport.emit (same lazy pattern)
  const { persistence, webAdapter, dashboardPoller, pgNotifyListener } = await startWebRuntime();

  // Stream workflow runs started in ANY process (incl. the `archon` CLI / `--detach`)
  // to the console dashboard. The in-process WorkflowEventBridge only sees runs
  // executed inside the server; this poller tails the events table. On Postgres,
  // LISTEN/NOTIFY wakes it for near-instant push (poll becomes a slow backstop);
  // on SQLite it polls fast.
  // Mutable — pushed to as each adapter starts, read by the /api/health endpoint.
  // Must be a live reference because Telegram starts after the HTTP listener begins
  // accepting requests, so a snapshot taken at registration time would miss it.
  const activePlatforms: string[] = ['Web'];

  const platforms = await startPlatformAdapters(
    config,
    lockManager,
    activePlatforms,
    opts.skipPlatformAdapters
  );
  const { github, githubAppAuthProvider, gitea, gitlab, discord, slack, slackBridge } = platforms;

  // Fail fast on a misconfigured web-auth secret before binding a socket: when
  // web auth is enabled, BETTER_AUTH_SECRET must be long enough to be a real
  // signing key. No-op when web auth is disabled.
  assertWebAuthAtBoot();

  // Setup Hono server
  const app = createServerApp(webAdapter, lockManager, activePlatforms, github);
  registerInternalGitCredentialRoute(app, github);
  registerForgeWebhookRoutes(app, gitea, gitlab);
  registerHealthRoutes(app, lockManager);
  await registerStaticRoutes(app, opts);

  const hostname = process.env.HOST || '0.0.0.0';
  assertSafeInternalBind(githubAppAuthProvider, hostname);
  warnIfHeaderTrustPublic(hostname);

  const port = opts.port ?? (await getPort());
  serveHttp(app, port);

  // Initialize Telegram adapter (conditional, skipped in CLI serve mode)
  let telegram: TelegramAdapter | null = null;
  if (!opts.skipPlatformAdapters && process.env.TELEGRAM_BOT_TOKEN) {
    const streamingMode = (process.env.TELEGRAM_STREAMING_MODE ?? 'stream') as 'stream' | 'batch';
    telegram = new TelegramAdapter(process.env.TELEGRAM_BOT_TOKEN, streamingMode);
    const telegramAdapter = telegram; // Capture for use in callback

    // Register message handler (auth is handled internally by adapter)
    telegramAdapter.onMessage(
      async ({ conversationId, message, userId: telegramUserId, displayName }) => {
        // Resolve Telegram user id (numeric) → Archon user UUID.
        const userId = await resolveUserId('telegram', telegramUserId, displayName);

        // Fire-and-forget: handler returns immediately, processing happens async
        lockManager
          .acquireLock(conversationId, async () => {
            await handleMessage(telegramAdapter, conversationId, message, {
              isolationHints: { workflowType: 'thread', workflowId: conversationId },
              userId,
            });
          })
          .catch(createMessageErrorHandler('Telegram', telegramAdapter, conversationId));
      }
    );

    try {
      await telegramAdapter.start();
      activePlatforms.push('Telegram');
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      getLog().error({ err: error, errorType: error.constructor.name }, 'telegram.start_failed');
      telegram = null; // Don't include in active platforms or shutdown
    }
  } else if (!opts.skipPlatformAdapters) {
    getLog().info('telegram_adapter_skipped');
  }

  // Graceful shutdown
  const shutdown = (): void => {
    getLog().info('server_shutting_down');
    stopCleanupScheduler();
    persistence.stopPeriodicFlush();

    // Flush all buffered messages before stopping adapters
    persistence
      .flushAll()
      .catch((e: unknown) => {
        getLog().error({ err: e }, 'shutdown_flush_failed');
      })
      .then(async () => {
        // Stop adapters (these should not throw, but be defensive)
        try {
          telegram?.stop();
          discord?.stop();
          // Detach Slack workflow bridge BEFORE stopping the adapter so a
          // pending debounced chat.update can't fire against a closed socket.
          slackBridge?.detach();
          slack?.stop();
          gitea?.stop();
          gitlab?.stop();
          pgNotifyListener?.stop();
          dashboardPoller.stop();
          await webAdapter.stop();
        } catch (error) {
          getLog().error({ err: error }, 'adapter_stop_error');
        }

        // Flush queued telemetry events before pool closes the process.
        await shutdownTelemetry();

        // Release the dedicated Better Auth pool (no-op when web auth is off).
        await closeAuth();

        return pool.end();
      })
      .then(() => {
        getLog().info('database_pool_closed');
        process.exit(0);
      })
      .catch((error: unknown) => {
        getLog().error({ err: error }, 'database_pool_close_failed');
        process.exit(1);
      });
  };

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);

  // Guard against SDK cleanup races: when a DAG node is aborted mid-execution,
  // the Claude Agent SDK's PostToolUse hook may be in-flight. After the hook
  // returns { continue: true }, handleControlRequest() tries to write() back to
  // the subprocess pipe — but the pipe is already closed (abort fired). The
  // write() throws "Operation aborted", which becomes an unhandled rejection
  // because it occurs AFTER the for-await generator loop exits (and thus outside
  // the try/catch in claude.ts). These are SDK cleanup races, not fatal app errors.
  process.on('unhandledRejection', handleUnhandledRejection);

  getLog().info({ activePlatforms, port }, 'server_ready');

  // Non-blocking: warn at startup if gh CLI auth is unavailable
  checkGhAuth().catch((err: unknown) => {
    getLog().debug({ err }, 'gh_auth.check_unexpected_error');
  });
}

/**
 * Run `gh auth status` and warn if it fails.
 * Helps diagnose expired tokens or missing auth before workflows fail.
 */
async function checkGhAuth(): Promise<void> {
  const { execFileAsync } = await import('@archon/git');
  try {
    await execFileAsync('gh', ['auth', 'status'], { timeout: 10_000 });
    getLog().info('gh_auth.status_ok');
  } catch {
    getLog().warn(
      'gh_auth.status_failed — gh CLI is not authenticated. Workflows using gh commands may fail. ' +
        'Run `gh auth login` or set GH_TOKEN in .env to fix this.'
    );
  }
}

// Run the application when executed directly (not imported as a library)
if (import.meta.main) {
  startServer().catch(error => {
    getLog().fatal({ err: error }, 'startup_failed');
    process.exit(1);
  });
}
