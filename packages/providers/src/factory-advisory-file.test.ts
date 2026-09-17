import { removeTempTree } from '@archon/paths/test-utils';
import { expect, test } from 'bun:test';
import { mkdtemp, symlink, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readFactoryAdvisoryFile } from './factory-advisory-file';

async function withTempDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), 'factory-advisory-'));
  try {
    return await fn(dir);
  } finally {
    await removeTempTree(dir);
  }
}

test('accepts a bounded advisory status from the trusted invocation path', async () => {
  await withTempDir(async dir => {
    const path = join(dir, 'status.json');
    await writeFile(
      path,
      JSON.stringify({
        version: 'archon.factory-advisory.v1',
        status: 'already-satisfied',
        reason: 'acceptance check already passes',
        evidence: 'verify.json passed before execution',
      })
    );

    expect(await readFactoryAdvisoryFile(path)).toEqual({
      accepted: true,
      observation: {
        version: 'archon.factory-advisory.v1',
        status: 'already-satisfied',
        reason: 'acceptance check already passes',
        evidence: 'verify.json passed before execution',
      },
    });
  });
});

test('rejects unusable advisory files without converting them into authority', async () => {
  await withTempDir(async dir => {
    const missing = join(dir, 'missing.json');
    const badStatus = join(dir, 'bad-status.json');
    const tooLarge = join(dir, 'large.json');
    const stale = join(dir, 'stale.json');
    const real = join(dir, 'real.json');
    const linked = join(dir, 'linked.json');

    await writeFile(
      badStatus,
      '{"version":"archon.factory-advisory.v1","status":"done","reason":"x"}'
    );
    await writeFile(tooLarge, 'x'.repeat(32));
    await writeFile(
      stale,
      '{"version":"archon.factory-advisory.v1","status":"blocked","reason":"x"}'
    );
    await utimes(stale, new Date(0), new Date(0));
    await writeFile(
      real,
      '{"version":"archon.factory-advisory.v1","status":"blocked","reason":"x"}'
    );
    await symlink(real, linked);

    expect(await readFactoryAdvisoryFile(missing)).toEqual({ accepted: false, reason: 'missing' });
    expect(await readFactoryAdvisoryFile(badStatus)).toEqual({
      accepted: false,
      reason: 'invalid',
    });
    expect(await readFactoryAdvisoryFile(tooLarge, { maxBytes: 16 })).toEqual({
      accepted: false,
      reason: 'oversized',
    });
    expect(await readFactoryAdvisoryFile(stale, { nowMs: 60_000, maxAgeMs: 1_000 })).toEqual({
      accepted: false,
      reason: 'stale',
    });
    expect(await readFactoryAdvisoryFile(linked)).toEqual({ accepted: false, reason: 'symlink' });
  });
});
