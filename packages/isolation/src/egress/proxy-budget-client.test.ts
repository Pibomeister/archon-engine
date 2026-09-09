import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createProxyBudgetClientForTest } from './proxy-budget-client';
import type { ProxyBudgetClient } from './proxy-budget-client';

const tempRoots: string[] = [];
const clients: ProxyBudgetClient[] = [];

afterEach(async () => {
  await Promise.allSettled(clients.splice(0).map(client => client.close()));
  for (const root of tempRoots.splice(0)) rmSync(root, { recursive: true, force: true });
});

test('performs valid ready, reserve, settle, status, and getReservation exchanges', async () => {
  const client = spawnFixture(`
    const reservations = new Map();
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', data => {
      for (const line of data.toString('utf8').trim().split('\\n').filter(Boolean)) {
        const request = JSON.parse(line);
        if (request.command === 'reserve') {
          const result = {
            reservationId: 'reservation-1',
            requestHash: request.payload.requestHash,
            inputCeiling: request.payload.inputCeiling,
            outputCeiling: request.payload.outputCeiling,
            status: 'pending',
            createdAtMs: 111,
          };
          reservations.set(result.reservationId, result);
          process.stdout.write(JSON.stringify({ id: request.id, ok: true, result }) + '\\n');
        } else if (request.command === 'settle') {
          const saved = { ...reservations.get(request.payload.reservationId), status: 'settled' };
          reservations.set(saved.reservationId, saved);
          process.stdout.write(JSON.stringify({ id: request.id, ok: true, result: saved }) + '\\n');
        } else if (request.command === 'getReservation') {
          process.stdout.write(JSON.stringify({ id: request.id, ok: true, result: reservations.get(request.payload.reservationId) }) + '\\n');
        } else if (request.command === 'status') {
          process.stdout.write(JSON.stringify({ id: request.id, ok: true, result: { grant: { schema: 'archon.proxy-budget-grant.v1', rootChainId: 'chain', runId: 'run', workflowDigest: 'sha256:workflow', policyDigest: 'sha256:policy', deadlineEpochMs: 2000, inputTokenLimit: 100, outputTokenLimit: 100, totalTokenLimit: 150 }, acceptingReservations: true, pendingReservations: 0, unknownReservations: 0, consumedInputTokens: 0, consumedOutputTokens: 0, consumedTotalTokens: 0, remainingInputTokens: 100, remainingOutputTokens: 100, remainingTotalTokens: 150 } }) + '\\n');
        }
      }
    });
  `);

  await client.ready();
  const reserved = await client.reserveBudget({
    requestHash: 'sha256:req',
    inputCeiling: 10,
    outputCeiling: 5,
  });
  expect(reserved).toEqual({
    reservationId: 'reservation-1',
    requestHash: 'sha256:req',
    inputCeiling: 10,
    outputCeiling: 5,
    status: 'pending',
    createdAtMs: 111,
  });
  await expect(
    client.settleBudget({ reservationId: 'reservation-1', inputTokens: 8, outputTokens: 3 })
  ).resolves.toMatchObject({
    status: 'settled',
  });
  await expect(client.getReservation('reservation-1')).resolves.toMatchObject({
    status: 'settled',
  });
  await expect(client.getBudgetStatus()).resolves.toMatchObject({
    acceptingReservations: true,
    pendingReservations: 0,
  });
});

test('rejects malformed readiness and child exit before readiness', async () => {
  const malformed = spawnFixture(`process.stdout.write('{not-json\\n');`, {
    readyTimeoutMs: 5_000,
  });
  await expect(malformed.ready()).rejects.toThrow('Proxy budget child returned malformed JSON.');

  const exited = spawnFixture(`process.exit(7);`, { readyTimeoutMs: 5_000 });
  await expect(exited.ready()).rejects.toThrow('Proxy budget child exited before close (code 7).');
});

test('times out readiness and operations then terminates the child', async () => {
  const neverReady = spawnFixture(`setInterval(() => {}, 1000);`, { readyTimeoutMs: 20 });
  await expect(neverReady.ready()).rejects.toThrow('Proxy budget child readiness timed out.');

  const noResponse = spawnFixture(
    `
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.resume();
  `,
    { operationTimeoutMs: 20 }
  );
  await noResponse.ready();
  await expect(
    noResponse.reserveBudget({ requestHash: 'sha256:lost-ack', inputCeiling: 1, outputCeiling: 1 })
  ).rejects.toThrow('Proxy budget operation timed out.');
});

test('fails closed on oversized, unknown-id, duplicate, and invalid envelopes', async () => {
  const oversized = spawnFixture(`
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', () => process.stdout.write('x'.repeat(17000) + '\\n'));
    setInterval(() => {}, 1000);
  `);
  await oversized.ready();
  await expect(oversized.getBudgetStatus()).rejects.toThrow(
    'Proxy budget child response line is too large.'
  );

  const unknown = spawnFixture(`
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', () => process.stdout.write(JSON.stringify({ id: 'attacker', ok: true, result: {} }) + '\\n'));
  `);
  await unknown.ready();
  await expect(unknown.getBudgetStatus()).rejects.toThrow(
    'Proxy budget child response ID is not the outstanding request.'
  );

  const duplicate = spawnFixture(`
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', data => {
      const request = JSON.parse(data.toString('utf8'));
      const response = JSON.stringify({ id: request.id, ok: true, result: { grant: { schema: 'archon.proxy-budget-grant.v1', rootChainId: 'chain', runId: 'run', workflowDigest: 'sha256:workflow', policyDigest: 'sha256:policy', deadlineEpochMs: 2000, inputTokenLimit: 100, outputTokenLimit: 100, totalTokenLimit: 150 }, acceptingReservations: true, pendingReservations: 0, unknownReservations: 0, consumedInputTokens: 0, consumedOutputTokens: 0, consumedTotalTokens: 0, remainingInputTokens: 100, remainingOutputTokens: 100, remainingTotalTokens: 150 } }) + '\\n';
      process.stdout.write(response);
      process.stdout.write(response);
    });
  `);
  await duplicate.ready();
  await expect(duplicate.getBudgetStatus()).resolves.toMatchObject({ acceptingReservations: true });
  await expect(
    duplicate.reserveBudget({
      requestHash: 'sha256:after-duplicate',
      inputCeiling: 1,
      outputCeiling: 1,
    })
  ).rejects.toThrow(
    /Proxy budget client is closed|Proxy budget child response ID is not the outstanding request/
  );

  const invalid = spawnFixture(`
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', data => {
      const request = JSON.parse(data.toString('utf8'));
      process.stdout.write(JSON.stringify({ id: request.id, ok: 'yes', result: {} }) + '\\n');
    });
  `);
  await invalid.ready();
  await expect(invalid.getBudgetStatus()).rejects.toThrow(
    'Proxy budget child returned an invalid response envelope.'
  );
});

test('rejects ok reserve responses with invalid or mismatched result shapes', async () => {
  const missingResult = spawnFixture(`
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', data => {
      const request = JSON.parse(data.toString('utf8'));
      process.stdout.write(JSON.stringify({ id: request.id, ok: true, result: { status: 'pending' } }) + '\\n');
    });
  `);
  await missingResult.ready();
  await expect(
    missingResult.reserveBudget({
      requestHash: 'sha256:bad-result',
      inputCeiling: 3,
      outputCeiling: 2,
    })
  ).rejects.toThrow('reservationId must be a non-empty string.');

  const mismatched = spawnFixture(`
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', data => {
      const request = JSON.parse(data.toString('utf8'));
      process.stdout.write(JSON.stringify({ id: request.id, ok: true, result: { reservationId: 'reservation-mismatch', requestHash: 'sha256:other', inputCeiling: request.payload.inputCeiling, outputCeiling: request.payload.outputCeiling, status: 'pending', createdAtMs: 1 } }) + '\\n');
    });
  `);
  await mismatched.ready();
  await expect(
    mismatched.reserveBudget({ requestHash: 'sha256:expected', inputCeiling: 3, outputCeiling: 2 })
  ).rejects.toThrow('Budget reservation result does not match the requested operation.');
  await expect(mismatched.getBudgetStatus()).rejects.toThrow('Proxy budget client is closed.');
});

test('matches command errors to unique IDs without retrying lost acknowledgements', async () => {
  const client = spawnFixture(`
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.on('data', data => {
      const request = JSON.parse(data.toString('utf8'));
      process.stdout.write(JSON.stringify({ id: request.id, ok: false, error: 'budget exhausted literal' }) + '\\n');
    });
  `);

  await client.ready();
  await expect(
    client.reserveBudget({ requestHash: 'sha256:exhausted', inputCeiling: 99, outputCeiling: 99 })
  ).rejects.toThrow('budget exhausted literal');
});

test('cancels an outstanding request and closes the owned child', async () => {
  const client = spawnFixture(
    `
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.resume();
    setInterval(() => {}, 1000);
  `,
    { operationTimeoutMs: 5_000 }
  );
  const controller = new AbortController();

  await client.ready();
  const promise = client.reserveBudget(
    { requestHash: 'sha256:cancel', inputCeiling: 1, outputCeiling: 1 },
    { signal: controller.signal }
  );
  controller.abort();

  await expect(promise).rejects.toThrow('Proxy budget operation was cancelled.');
  await expect(client.getBudgetStatus()).rejects.toThrow('Proxy budget client is closed.');
});

test('keeps valid far-future deadline clients alive instead of overflowing Node timers', async () => {
  const client = spawnFixture(
    `
    process.stdout.write(JSON.stringify({ ok: true, event: 'ready', mode: 'resume' }) + '\\n');
    process.stdin.resume();
    setInterval(() => {}, 1000);
    process.stdin.on('data', data => {
      const request = JSON.parse(data.toString('utf8'));
      process.stdout.write(JSON.stringify({ id: request.id, ok: true, result: { grant: { schema: 'archon.proxy-budget-grant.v1', rootChainId: 'chain', runId: 'run', workflowDigest: 'sha256:workflow', policyDigest: 'sha256:policy', deadlineEpochMs: 1893456000000, inputTokenLimit: 100, outputTokenLimit: 100, totalTokenLimit: 150 }, acceptingReservations: true, pendingReservations: 0, unknownReservations: 0, consumedInputTokens: 0, consumedOutputTokens: 0, consumedTotalTokens: 0, remainingInputTokens: 100, remainingOutputTokens: 100, remainingTotalTokens: 150 } }) + '\\n');
    });
  `,
    { wallDeadlineEpochMs: Date.now() + 2_147_483_647 + 60_000 }
  );

  await client.ready();
  await new Promise(resolve => setTimeout(resolve, 50));
  await expect(client.getBudgetStatus()).resolves.toMatchObject({ acceptingReservations: true });
  await client.close();
});

function spawnFixture(
  source: string,
  overrides: Partial<Parameters<typeof createProxyBudgetClientForTest>[0]> = {}
): ProxyBudgetClient {
  const root = mkdtempSync(join(tmpdir(), 'archon-proxy-budget-client-'));
  tempRoots.push(root);
  const script = join(root, 'fixture.js');
  writeFileSync(script, `if (process.versions.bun) process.exit(42);\n${source}`);
  const client = createProxyBudgetClientForTest({
    executable: process.env.ARCHON_NODE_BIN ?? 'node',
    args: [script],
    env: { PATH: process.env.PATH ?? '/usr/bin:/bin' },
    readyTimeoutMs: 1_000,
    operationTimeoutMs: 1_000,
    wallTimeoutMs: 5_000,
    maxLineBytes: 16_384,
    maxOutputBacklogBytes: 65_536,
    ...overrides,
  });
  clients.push(client);
  return client;
}
