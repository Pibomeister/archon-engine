import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { buildRequestSubprocessEnv } from './provider';

/**
 * Env-isolation enforcement point: a container run must receive ONLY the
 * Archon-managed env bag over a minimal base — host `process.env` must NEVER
 * cross into the container. A host run keeps inheriting the host env unchanged.
 */
describe('buildRequestSubprocessEnv — container env isolation', () => {
  const HOST_CANARIES = {
    ARCHON_HOST_CANARY_SECRET: 'leaked-host-secret',
    GH_TOKEN: 'host-gh-token',
    GITHUB_TOKEN: 'host-github-token',
    DATABASE_URL: 'postgres://host-canary',
    PGHOST: 'host-db',
    AWS_SECRET_ACCESS_KEY: 'host-aws-secret',
    AWS_SESSION_TOKEN: 'host-aws-session',
    DOCKER_HOST: 'tcp://host-docker',
    CLAUDE_API_KEY: 'host-claude-key',
    ANTHROPIC_API_KEY: 'host-anthropic-key',
    LD_PRELOAD: '/tmp/host-hook.so',
    NODE_OPTIONS: '--require /tmp/host-hook.js',
  } as const;
  let savedEnv: Record<string, string | undefined>;

  beforeEach(() => {
    savedEnv = {};
    for (const [key, value] of Object.entries(HOST_CANARIES)) {
      savedEnv[key] = process.env[key];
      process.env[key] = value;
    }
  });
  afterEach(() => {
    for (const key of Object.keys(HOST_CANARIES)) {
      const value = savedEnv[key];
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  test('container run excludes host/process/project env and keeps only allowed managed values', () => {
    const env = buildRequestSubprocessEnv({
      execContext: {
        kind: 'container',
        profile: 'hardened',
        containerId: 'c1',
        providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test' }],
      },
      env: {
        ANTHROPIC_API_KEY: 'sk-managed',
        ANTHROPIC_BASE_URL: 'https://api.anthropic.test',
        CLAUDE_CODE_DISABLE_1M_CONTEXT: '1',
        CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS: '1',
        HTTPS_PROXY: 'http://127.0.0.1:18080',
        NODE_EXTRA_CA_CERTS: '/run/archon/ca.pem',
        SSL_CERT_FILE: '/run/archon/ca.pem',
        CODEBASE_VAR: 'x',
        ARTIFACTS_DIR: '/a',
        HOME: '/Users/sdk',
        PATH: '/host/bin',
      },
    });
    expect(env.ARCHON_HOST_CANARY_SECRET).toBeUndefined();
    expect(env.GH_TOKEN).toBeUndefined();
    expect(env.GITHUB_TOKEN).toBeUndefined();
    expect(env.DATABASE_URL).toBeUndefined();
    expect(env.PGHOST).toBeUndefined();
    expect(env.AWS_SECRET_ACCESS_KEY).toBeUndefined();
    expect(env.AWS_SESSION_TOKEN).toBeUndefined();
    expect(env.DOCKER_HOST).toBeUndefined();
    expect(env.LD_PRELOAD).toBeUndefined();
    expect(env.NODE_OPTIONS).toBeUndefined();
    expect(env.CLAUDE_API_KEY).toBeUndefined();
    expect(env.ANTHROPIC_API_KEY).toBe('sk-managed');
    expect(env.ANTHROPIC_BASE_URL).toBe('https://api.anthropic.test');
    expect(env.CLAUDE_CODE_DISABLE_1M_CONTEXT).toBe('1');
    expect(env.CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS).toBe('1');
    expect(env.HTTPS_PROXY).toBeUndefined();
    expect(env.NODE_EXTRA_CA_CERTS).toBeUndefined();
    expect(env.SSL_CERT_FILE).toBeUndefined();
    expect(env.CODEBASE_VAR).toBeUndefined();
    expect(env.ARTIFACTS_DIR).toBeUndefined();
    expect(env.HOME).toBeUndefined();
    expect(env.PATH).toBeUndefined();
    expect(env.TERM).toBe('dumb');
  });

  test('host run INHERITS host process.env (canary present) — unchanged behavior', () => {
    const env = buildRequestSubprocessEnv({ env: { FOO: 'bar' } });
    expect(env.ARCHON_HOST_CANARY_SECRET).toBe('leaked-host-secret');
    expect(env.GH_TOKEN).toBe('host-gh-token');
    expect(env.DATABASE_URL).toBe('postgres://host-canary');
    expect(env.FOO).toBe('bar');
  });

  test('container run rejects off-policy Anthropic base URL overrides', () => {
    expect(() =>
      buildRequestSubprocessEnv({
        execContext: {
          kind: 'container',
          profile: 'hardened',
          containerId: 'c1',
          providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test' }],
        },
        env: { ANTHROPIC_BASE_URL: 'https://evil.example' },
      })
    ).toThrow(
      'Claude container execution ANTHROPIC_BASE_URL does not match sealed provider origin'
    );
  });

  test('container run rejects unsealed Anthropic base URL overrides', () => {
    expect(() =>
      buildRequestSubprocessEnv({
        execContext: { kind: 'container', profile: 'hardened', containerId: 'c1' },
        env: { ANTHROPIC_BASE_URL: 'https://api.anthropic.test' },
      })
    ).toThrow('Claude container execution requires exactly one sealed Anthropic provider origin');
  });

  test('container run rejects missing Anthropic provider origin even without request override', () => {
    expect(() =>
      buildRequestSubprocessEnv({
        execContext: { kind: 'container', profile: 'hardened', containerId: 'c1' },
        env: { ANTHROPIC_API_KEY: 'sk-managed' },
      })
    ).toThrow('Claude container execution requires exactly one sealed Anthropic provider origin');
  });

  test('container run rejects duplicate Anthropic provider origins', () => {
    expect(() =>
      buildRequestSubprocessEnv({
        execContext: {
          kind: 'container',
          profile: 'hardened',
          containerId: 'c1',
          providerOrigins: [
            { provider: 'anthropic', baseUrl: 'https://api.anthropic.test' },
            { provider: 'claude', baseUrl: 'https://api.claude.test' },
          ],
        },
        env: { ANTHROPIC_API_KEY: 'sk-managed' },
      })
    ).toThrow('Claude container execution requires exactly one sealed Anthropic provider origin');
  });

  test('container run rejects localhost, IP literal, and invalid DNS origins', () => {
    for (const baseUrl of [
      'https://localhost',
      'https://127.0.0.1',
      'https://[::ffff:127.0.0.1]',
      'https://bad_host.test',
    ]) {
      expect(() =>
        buildRequestSubprocessEnv({
          execContext: {
            kind: 'container',
            profile: 'hardened',
            containerId: 'c1',
            providerOrigins: [{ provider: 'anthropic', baseUrl }],
          },
          env: { ANTHROPIC_API_KEY: 'sk-managed' },
        })
      ).toThrow('Claude hardened provider origin must use a non-local DNS hostname');
    }
  });

  test('container run rejects path-scoped sealed Anthropic origins', () => {
    expect(() =>
      buildRequestSubprocessEnv({
        execContext: {
          kind: 'container',
          profile: 'hardened',
          containerId: 'c1',
          providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test/v1' }],
        },
        env: { ANTHROPIC_BASE_URL: 'https://api.anthropic.test/v1' },
      })
    ).toThrow('Claude hardened provider origin must be the HTTPS API origin root');
  });

  test('container run rejects explicit dangerous env overrides without logging values', () => {
    expect(() =>
      buildRequestSubprocessEnv({
        execContext: {
          kind: 'container',
          profile: 'hardened',
          containerId: 'c1',
          providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test' }],
        },
        env: { GH_TOKEN: 'explicit-gh-secret' },
      })
    ).toThrow('Claude container execution does not allow request env key GH_TOKEN');

    expect(() =>
      buildRequestSubprocessEnv({
        execContext: {
          kind: 'container',
          profile: 'hardened',
          containerId: 'c1',
          providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test' }],
        },
        env: { PGHOST: 'explicit-db-host' },
      })
    ).toThrow('Claude container execution does not allow request env key PGHOST');
  });

  test('container run mirrors CLAUDE_API_KEY -> ANTHROPIC_API_KEY', () => {
    const env = buildRequestSubprocessEnv({
      execContext: {
        kind: 'container',
        profile: 'hardened',
        containerId: 'c1',
        providerOrigins: [{ provider: 'anthropic', baseUrl: 'https://api.anthropic.test' }],
      },
      env: { CLAUDE_API_KEY: 'sk-claude' },
    });
    expect(env.ANTHROPIC_API_KEY).toBe('sk-claude');
    expect(env.CLAUDE_API_KEY).toBeUndefined();
    expect(env.ARCHON_HOST_CANARY_SECRET).toBeUndefined();
    expect(env.GH_TOKEN).toBeUndefined();
    expect(env.DATABASE_URL).toBeUndefined();
  });
});
