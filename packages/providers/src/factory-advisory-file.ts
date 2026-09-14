import { constants, open, lstat } from 'node:fs/promises';

export type FactoryAdvisoryStatus = 'blocked' | 'needs-human-review' | 'already-satisfied';

export interface FactoryAdvisoryObservation {
  version: 'archon.factory-advisory.v1';
  status: FactoryAdvisoryStatus;
  reason: string;
  evidence?: string;
}

export type FactoryAdvisoryReadResult =
  | { accepted: true; observation: FactoryAdvisoryObservation }
  | {
      accepted: false;
      reason: 'missing' | 'symlink' | 'not-regular' | 'oversized' | 'stale' | 'invalid';
    };

export interface FactoryAdvisoryReadOptions {
  maxBytes?: number;
  maxAgeMs?: number;
  nowMs?: number;
}

const DEFAULT_MAX_BYTES = 8192;
const DEFAULT_MAX_AGE_MS = 30 * 60 * 1000;
const STAT_NOFOLLOW = constants.O_RDONLY | ('O_NOFOLLOW' in constants ? constants.O_NOFOLLOW : 0);

function asCleanString(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  if (!trimmed || trimmed.length > 2048) return undefined;
  return trimmed;
}

function parseObservation(value: unknown): FactoryAdvisoryObservation | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (record.version !== 'archon.factory-advisory.v1') return undefined;
  if (
    record.status !== 'blocked' &&
    record.status !== 'needs-human-review' &&
    record.status !== 'already-satisfied'
  )
    return undefined;
  const reason = asCleanString(record.reason);
  if (!reason) return undefined;
  const evidence = record.evidence === undefined ? undefined : asCleanString(record.evidence);
  if (record.evidence !== undefined && !evidence) return undefined;
  return {
    version: 'archon.factory-advisory.v1',
    status: record.status,
    reason,
    ...(evidence ? { evidence } : {}),
  };
}

export async function readFactoryAdvisoryFile(
  path: string,
  options: FactoryAdvisoryReadOptions = {}
): Promise<FactoryAdvisoryReadResult> {
  const maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
  const maxAgeMs = options.maxAgeMs ?? DEFAULT_MAX_AGE_MS;
  const nowMs = options.nowMs ?? Date.now();
  let before;
  try {
    before = await lstat(path);
  } catch {
    return { accepted: false, reason: 'missing' };
  }
  if (before.isSymbolicLink()) return { accepted: false, reason: 'symlink' };
  if (!before.isFile()) return { accepted: false, reason: 'not-regular' };
  if (before.size > maxBytes) return { accepted: false, reason: 'oversized' };
  if (nowMs - before.mtimeMs > maxAgeMs) return { accepted: false, reason: 'stale' };

  let handle;
  try {
    handle = await open(path, STAT_NOFOLLOW);
    const after = await handle.stat();
    if (!after.isFile()) return { accepted: false, reason: 'not-regular' };
    if (after.dev !== before.dev || after.ino !== before.ino)
      return { accepted: false, reason: 'invalid' };
    if (after.size > maxBytes) return { accepted: false, reason: 'oversized' };
    const text = await handle.readFile({ encoding: 'utf8' });
    const observation = parseObservation(JSON.parse(text));
    return observation ? { accepted: true, observation } : { accepted: false, reason: 'invalid' };
  } catch {
    return { accepted: false, reason: 'invalid' };
  } finally {
    await handle?.close();
  }
}
