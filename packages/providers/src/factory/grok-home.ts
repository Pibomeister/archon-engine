import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export function factoryGrokEnv(optionsEnv?: Record<string, string>): NodeJS.ProcessEnv {
  const grokHome = optionsEnv?.GROK_HOME ?? process.env.GROK_HOME;
  const home = optionsEnv?.HOME ?? grokHome;
  const env: NodeJS.ProcessEnv = {
    PATH: optionsEnv?.PATH ?? process.env.PATH ?? '/usr/bin:/bin',
    TMPDIR: optionsEnv?.TMPDIR ?? '/tmp',
    LANG: optionsEnv?.LANG ?? process.env.LANG ?? 'C.UTF-8',
    LC_ALL: optionsEnv?.LC_ALL ?? process.env.LC_ALL ?? 'C.UTF-8',
  };
  if (home) env.HOME = home;
  if (grokHome) env.GROK_HOME = grokHome;
  env.GROK_DISABLE_API_KEY_AUTH = '1';
  env.GROK_CLAUDE_MCPS_ENABLED = '0';
  env.GROK_CURSOR_MCPS_ENABLED = '0';
  return env;
}

export function pathInside(parent: string, child: string): boolean {
  return child === parent || child.startsWith(parent.endsWith('/') ? parent : `${parent}/`);
}

export function writeFactoryHome(home: string, sandboxBody: string): void {
  mkdirSync(home, { recursive: true, mode: 0o700 });
  writeFileSync(join(home, 'sandbox.toml'), sandboxBody, { encoding: 'utf8', mode: 0o600 });
  writeFileSync(
    join(home, 'config.toml'),
    '[compat.claude]\nmcps = false\n\n[compat.cursor]\nmcps = false\n',
    { encoding: 'utf8', mode: 0o600 }
  );
}
