import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';

const trace: string[] = [];

function record(label: string): void {
  trace.push(label);
}

const logger = {
  fatal: mock(() => undefined),
  error: mock(() => undefined),
  warn: mock(() => undefined),
  info: mock((message?: unknown) => {
    if (typeof message === 'string') record(message);
  }),
  debug: mock(() => undefined),
  trace: mock(() => undefined),
  child: mock(function (this: unknown) {
    return this;
  }),
  bindings: mock(() => ({ module: 'test' })),
  isLevelEnabled: mock(() => true),
  level: 'info' as const,
};

class FakeLockManager {
  constructor(maxConcurrent: number) {
    record(`lock:${String(maxConcurrent)}`);
  }

  getStats(): { active: number; queuedTotal: number; maxConcurrent: number } {
    return { active: 0, queuedTotal: 0, maxConcurrent: 10 };
  }
}

class FakeSlackAdapter {
  constructor() {
    record('slack:construct');
  }
  onMessage(): void {
    record('slack:onMessage');
  }
  async start(): Promise<void> {
    record('slack:start');
  }
  stop(): void {
    record('slack:stop');
  }
}

class FakeSlackWorkflowBridge {
  constructor() {
    record('slackBridge:construct');
  }
  attach(): void {
    record('slackBridge:attach');
  }
  detach(): void {
    record('slackBridge:detach');
  }
}

class FakeTelegramAdapter {
  constructor() {
    record('telegram:construct');
  }
  onMessage(): void {
    record('telegram:onMessage');
  }
  async start(): Promise<void> {
    record('telegram:start');
  }
  stop(): void {
    record('telegram:stop');
  }
}

class FakeWebAdapter {
  async start(): Promise<void> {
    record('web:start');
  }
  async stop(): Promise<void> {
    record('web:stop');
  }
  emitLockEvent(): void {}
  emitSSE(): void {}
}

class FakePersistence {
  constructor() {
    record('persistence:construct');
  }
  startPeriodicFlush(): void {
    record('persistence:startPeriodicFlush');
  }
  stopPeriodicFlush(): void {
    record('persistence:stopPeriodicFlush');
  }
  async flushAll(): Promise<void> {
    record('persistence:flushAll');
  }
}

class FakeDashboardPoller {
  start(): void {
    record('dashboard:start');
  }
  stop(): void {
    record('dashboard:stop');
  }
}

const mockPool = {
  query: mock(async () => {
    record('db:query');
  }),
  end: mock(async () => {
    record('db:end');
  }),
};

let capturedShutdown: (() => void) | undefined;
let originalServe: typeof Bun.serve;
let originalOnce: typeof process.once;
let originalExit: typeof process.exit;
const envBackup = { ...process.env };

mock.module('@archon/paths/strip-cwd-env-boot', () => ({}));
mock.module('dotenv', () => ({ config: mock(() => ({})) }));
mock.module('@archon/providers', () => ({
  registerBuiltinProviders: mock(() => record('providers:builtin')),
  registerCommunityProviders: mock(() => record('providers:community')),
}));
mock.module('@archon/paths/env-loader', () => ({ loadArchonEnv: mock(() => record('env:load')) }));
mock.module('@archon/paths', () => ({
  BUNDLED_IS_BINARY: true,
  getArchonEnvPath: mock(() => '/tmp/archon.env'),
  createLogger: mock(() => logger),
  logArchonPaths: mock(() => record('paths:log')),
  validateAppDefaultsPaths: mock(async () => record('paths:validate')),
  shutdownTelemetry: mock(async () => record('telemetry:shutdown')),
  captureArchonStarted: mock(() => record('telemetry:started')),
  captureArchonActive: mock(() => record('telemetry:active')),
}));
mock.module('@archon/adapters', () => ({
  TelegramAdapter: FakeTelegramAdapter,
  GitHubAdapter: class {},
  DiscordAdapter: class {},
  SlackAdapter: FakeSlackAdapter,
  SlackWorkflowBridge: FakeSlackWorkflowBridge,
}));
mock.module('@archon/adapters/community/forge/gitea', () => ({ GiteaAdapter: class {} }));
mock.module('@archon/adapters/community/forge/gitlab', () => ({ GitLabAdapter: class {} }));
mock.module('./adapters/web', () => ({ WebAdapter: FakeWebAdapter }));
mock.module('./adapters/web/persistence', () => ({ MessagePersistence: FakePersistence }));
mock.module('./adapters/web/transport', () => ({ SSETransport: class {} }));
mock.module('./adapters/web/workflow-bridge', () => ({ WorkflowEventBridge: class {} }));
mock.module('./adapters/web/dashboard-event-poller', () => ({
  DashboardEventPoller: FakeDashboardPoller,
}));
mock.module('./adapters/web/pg-notify-listener', () => ({ PgNotifyListener: class {} }));
mock.module('./routes/api', () => ({ registerApiRoutes: mock(() => record('routes:api')) }));
mock.module('./routes/webhooks', () => ({
  registerGithubWebhookRoute: mock(() => record('routes:github')),
}));
mock.module('./routes/openapi-defaults', () => ({ validationErrorHook: mock(() => undefined) }));
mock.module('./auth', () => ({
  getAuth: mock(() => null),
  closeAuth: mock(async () => record('auth:close')),
  isWebAuthEnabled: mock(() => false),
  assertWebAuthAtBoot: mock(() => record('auth:web-assert')),
  getSignupMode: mock(() => 'disabled'),
  isArchonOwnedAuthPath: mock(() => false),
}));
mock.module('./github-auth-bootstrap', () => ({
  selectGitHubAuthMode: mock(() => ({ kind: 'none' })),
  parseGitCredentialPath: mock(() => null),
}));
mock.module('./discord-mention', () => ({ isDiscordMentionRequired: mock(() => true) }));
mock.module('@archon/git', () => ({ execFileAsync: mock(async () => record('gh:status')) }));
mock.module('@archon/core/db/users', () => ({ findOrCreateUserByPlatformIdentity: mock() }));
mock.module('@archon/core', () => ({
  getVendorCatalog: mock(() => record('vendors:catalog')),
  handleMessage: mock(async () => undefined),
  pool: mockPool,
  ConversationLockManager: FakeLockManager,
  classifyAndFormatError: mock(() => 'formatted'),
  startCleanupScheduler: mock(() => record('cleanup:start')),
  stopCleanupScheduler: mock(() => record('cleanup:stop')),
  getDbNotificationListener: mock(() => undefined),
  loadConfig: mock(async () => {
    record('config:load');
    return { botName: 'Archon' };
  }),
  logConfig: mock(() => record('config:log')),
  getPort: mock(async () => 3210),
  createGitHubAppAuthProvider: mock(),
  loadAppPrivateKey: mock(() => 'private-key'),
  registerGitHubAppAuthProvider: mock(),
  isPerUserGitHubEnabled: mock(() => false),
  isPerUserProviderKeysEnabled: mock(() => false),
  getDatabaseType: mock(() => 'sqlite'),
  assertEncryptionKeyAtBoot: mock(() => record('auth:github-key-assert')),
  assertProviderKeysKeyAtBoot: mock(() => record('auth:provider-key-assert')),
  getDecryptedAccessToken: mock(async () => undefined),
}));

beforeEach(() => {
  trace.length = 0;
  capturedShutdown = undefined;
  process.env = { ...envBackup };
  process.env.CLAUDE_API_KEY = 'test-claude-key';
  process.env.MAX_CONCURRENT_CONVERSATIONS = '4';
  delete process.env.TELEGRAM_BOT_TOKEN;
  delete process.env.SLACK_BOT_TOKEN;
  delete process.env.SLACK_APP_TOKEN;
  delete process.env.GEMINI_API_KEY;
  delete process.env.OPENAI_API_KEY;

  originalServe = Bun.serve;
  originalOnce = process.once;
  originalExit = process.exit;

  Bun.serve = mock(options => {
    record(`serve:${String(options.port)}`);
    return { port: options.port };
  }) as typeof Bun.serve;
  process.once = mock((event: string | symbol, listener: (...args: unknown[]) => void) => {
    if (event === 'SIGINT') capturedShutdown = () => listener();
    record(`process.once:${String(event)}`);
    return process;
  }) as typeof process.once;
  process.exit = mock(((code?: string | number | null) => {
    record(`process.exit:${String(code)}`);
    return undefined as never;
  }) as typeof process.exit);
});

afterEach(() => {
  Bun.serve = originalServe;
  process.once = originalOnce;
  process.exit = originalExit;
  process.env = { ...envBackup };
});

describe('startServer lifecycle ordering', () => {
  test('skipPlatformAdapters starts web-only and shuts down by flushing before stops', async () => {
    const { startServer } = await import('./index');

    await startServer({ port: 4321, skipPlatformAdapters: true, webDistPath: '/tmp/missing-web' });
    capturedShutdown?.();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(trace).toEqual([
      'env:load',
      'providers:builtin',
      'providers:community',
      'vendors:catalog',
      'server_starting',
      'telemetry:started',
      'auth:provider-key-assert',
      'db:query',
      'database_connected',
      'config:load',
      'config:log',
      'cleanup:start',
      'paths:log',
      'paths:validate',
      'lock:4',
      'persistence:construct',
      'web:start',
      'persistence:startPeriodicFlush',
      'dashboard:start',
      'platform_adapters_skipped',
      'auth:web-assert',
      'routes:api',
      'serve:4321',
      'process.once:SIGINT',
      'process.once:SIGTERM',
      'server_shutting_down',
      'cleanup:stop',
      'persistence:stopPeriodicFlush',
      'persistence:flushAll',
      'dashboard:stop',
      'web:stop',
      'telemetry:shutdown',
      'auth:close',
      'db:end',
      'gh:status',
      'database_pool_closed',
      'process.exit:0',
      'gh_auth.status_ok',
    ]);
  });

  test('Slack bridge attaches before Slack start and Telegram starts after listen', async () => {
    process.env.SLACK_BOT_TOKEN = 'xoxb-test';
    process.env.SLACK_APP_TOKEN = 'xapp-test';
    process.env.TELEGRAM_BOT_TOKEN = 'telegram-test';

    const { startServer } = await import('./index');

    await startServer({ port: 4322, webDistPath: '/tmp/missing-web' });

    expect(trace.indexOf('slackBridge:attach')).toBeLessThan(trace.indexOf('slack:start'));
    expect(trace.indexOf('serve:4322')).toBeLessThan(trace.indexOf('telegram:start'));
    expect(trace).toContain('routes:api');
  });
});
