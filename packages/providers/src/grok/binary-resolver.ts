import { existsSync as _existsSync } from 'node:fs';
import { delimiter } from 'node:path';

export function fileExists(path: string): boolean {
  return _existsSync(path);
}

export function resolveGrokBinaryPath(configGrokBinaryPath?: string): string {
  const envPath = process.env.GROK_BIN_PATH;
  if (envPath && fileExists(envPath)) return envPath;
  if (configGrokBinaryPath && fileExists(configGrokBinaryPath)) return configGrokBinaryPath;
  const pathEntries = (process.env.PATH ?? '').split(delimiter);
  for (const entry of pathEntries) {
    if (!entry) continue;
    const candidate = entry.endsWith('/') || entry.endsWith('\\') ? `${entry}grok` : `${entry}/grok`;
    if (fileExists(candidate)) return candidate;
  }
  throw new Error('grok_binary_missing');
}
