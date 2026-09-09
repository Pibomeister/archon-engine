import { existsSync } from 'node:fs';
import { createProxyBudgetLedger } from './proxy-budget-ledger';
import type { ProxyBudgetGrant, ProxyBudgetWorkflowBinding } from './proxy-budget-ledger';

const [, , dbPath, grantJson, bindingJson, requestHash, inputText, outputText, goPath] =
  process.argv;

try {
  if (
    !dbPath ||
    !grantJson ||
    !bindingJson ||
    !requestHash ||
    !inputText ||
    !outputText ||
    !goPath
  ) {
    throw new Error('Missing multiprocess worker arguments.');
  }
  while (!existsSync(goPath)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  const ledger = createProxyBudgetLedger({
    mode: 'resume',
    dbPath,
    grant: JSON.parse(grantJson) as ProxyBudgetGrant,
  });
  try {
    const reservation = ledger.reserveBudget({
      workflowBinding: JSON.parse(bindingJson) as ProxyBudgetWorkflowBinding,
      requestHash,
      inputCeiling: Number(inputText),
      outputCeiling: Number(outputText),
      nowMs: 1_000,
    });
    ledger.settleBudget({
      workflowBinding: JSON.parse(bindingJson) as ProxyBudgetWorkflowBinding,
      reservationId: reservation.reservationId,
      inputTokens: Number(inputText),
      outputTokens: Number(outputText),
    });
    process.stdout.write(JSON.stringify({ ok: true, reservationId: reservation.reservationId }));
  } finally {
    ledger.close();
  }
} catch (error) {
  process.stdout.write(
    JSON.stringify({ ok: false, error: error instanceof Error ? error.message : 'unknown' })
  );
  process.exitCode = 1;
}
