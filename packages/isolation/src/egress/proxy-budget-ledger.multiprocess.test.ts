import { describe, expect, test, afterEach } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { createProxyBudgetLedger } from './proxy-budget-ledger';
import type { ProxyBudgetGrant, ProxyBudgetWorkflowBinding } from './proxy-budget-ledger';

const tempRoots: string[] = [];
const rootBinding = {
  runId: 'root-run',
  workflowDigest: 'sha256:root',
  policyDigest: 'sha256:policy',
};
const childBinding = {
  runId: 'run-child',
  workflowDigest: 'sha256:child',
  policyDigest: 'sha256:policy',
};
const grant: ProxyBudgetGrant = {
  schema: 'archon.proxy-budget-grant.v2',
  rootChainId: 'root-run',
  deadlineEpochMs: 2_000_000,
  inputTokenLimit: 200,
  outputTokenLimit: 200,
  totalTokenLimit: 100,
  workflowBindings: [rootBinding, childBinding],
};

afterEach(() => {
  for (const root of tempRoots.splice(0)) rmSync(root, { recursive: true, force: true });
});

describe('proxy budget ledger multiprocess SQLite contention', () => {
  test('two authorized processes cannot both exceed the shared remaining total', async () => {
    const { dbPath, goPath } = createLedger();
    const results = await raceReservations(dbPath, goPath, 59, 1);

    expect(results.filter(result => result.ok)).toHaveLength(1);
    expect(results.filter(result => !result.ok).map(result => result.error)).toEqual([
      expect.stringMatching(
        /^(Total token budget exhausted|Pending budget reservation blocks additional requests)\.$/
      ),
    ]);
    const reopened = createProxyBudgetLedger({ mode: 'resume', dbPath, grant });
    expect(reopened.getBudgetStatus()).toMatchObject({ consumedTotalTokens: 60 });
    reopened.close();
  });

  test('race after prior root consumption persists exactly one child/root winner', async () => {
    const { dbPath, goPath } = createLedger();
    const ledger = createProxyBudgetLedger({ mode: 'resume', dbPath, grant });
    const prior = ledger.reserveBudget({
      workflowBinding: rootBinding,
      requestHash: 'sha256:prior',
      inputCeiling: 39,
      outputCeiling: 1,
      nowMs: 999,
    });
    ledger.settleBudget({
      workflowBinding: rootBinding,
      reservationId: prior.reservationId,
      inputTokens: 39,
      outputTokens: 1,
    });
    ledger.close();

    const results = await raceReservations(dbPath, goPath, 39, 1);

    expect(results.filter(result => result.ok)).toHaveLength(1);
    const reopened = createProxyBudgetLedger({ mode: 'resume', dbPath, grant });
    expect(reopened.getBudgetStatus()).toMatchObject({ consumedTotalTokens: 80 });
    reopened.close();
  });
});

function createLedger(): { dbPath: string; goPath: string } {
  const root = mkdtempSync(join(tmpdir(), 'archon-proxy-budget-race-'));
  tempRoots.push(root);
  const dbPath = join(root, 'ledger.sqlite');
  const ledger = createProxyBudgetLedger({ mode: 'create', dbPath, grant });
  ledger.close();
  return { dbPath, goPath: join(root, 'go') };
}

async function raceReservations(
  dbPath: string,
  goPath: string,
  inputCeiling: number,
  outputCeiling: number
): Promise<WorkerResult[]> {
  const workerPath = join(
    dirname(fileURLToPath(import.meta.url)),
    'proxy-budget-ledger.multiprocess-worker.ts'
  );
  const grantJson = JSON.stringify(grant);
  const first = runWorker(
    dbPath,
    grantJson,
    rootBinding,
    'sha256:root-race',
    inputCeiling,
    outputCeiling,
    goPath,
    workerPath
  );
  const second = runWorker(
    dbPath,
    grantJson,
    childBinding,
    'sha256:child-race',
    inputCeiling,
    outputCeiling,
    goPath,
    workerPath
  );
  writeFileSync(goPath, 'go');
  return await Promise.all([first, second]);
}

function runWorker(
  dbPath: string,
  grantJson: string,
  binding: ProxyBudgetWorkflowBinding,
  requestHash: string,
  inputCeiling: number,
  outputCeiling: number,
  goPath: string,
  workerPath: string
): Promise<WorkerResult> {
  return new Promise(resolve => {
    const child = spawn(process.execPath, [
      workerPath,
      dbPath,
      grantJson,
      JSON.stringify(binding),
      requestHash,
      String(inputCeiling),
      String(outputCeiling),
      goPath,
    ]);
    let stdout = '';
    child.stdout.on('data', chunk => {
      stdout += String(chunk);
    });
    child.once('exit', () => {
      resolve(JSON.parse(stdout) as WorkerResult);
    });
  });
}

interface WorkerResult {
  ok: boolean;
  error?: string;
  reservationId?: string;
}
