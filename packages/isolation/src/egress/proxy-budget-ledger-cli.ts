import { lstatSync, mkdirSync, readFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { stdin as input, stdout as output } from 'node:process';
import { createProxyBudgetLedger } from './proxy-budget-ledger';
import type { ProxyBudgetGrant, ProxyBudgetLedger } from './proxy-budget-ledger';

const GRANT_PATH = '/archon-proxy-private/budget.json';
const LEDGER_PATH = '/archon-budget/ledger.sqlite';
const MAX_LINE_BYTES = 16_384;
const MAX_ID_BYTES = 128;
const MAX_GRANT_BYTES = 16_384;

interface CommandEnvelope {
  id: string;
  command: string;
  payload?: unknown;
}

interface ReservePayload {
  requestHash: string;
  inputCeiling: number;
  outputCeiling: number;
}

interface SettlePayload {
  reservationId: string;
  inputTokens: number;
  outputTokens: number;
}

interface UnknownPayload {
  reservationId: string;
  reason: string;
}

function readMode(args: string[]): 'create' | 'resume' | 'status' {
  if (args.length === 0) return 'resume';
  if (args.length === 1 && args[0] === '--create') return 'create';
  if (args.length === 1 && args[0] === '--status') return 'status';
  throw new Error('Unsupported proxy budget CLI arguments.');
}

function readGrant(): ProxyBudgetGrant {
  const stat = lstatSync(GRANT_PATH);
  if (stat.isSymbolicLink()) throw new Error('Budget grant path must not be a symlink.');
  if (!stat.isFile()) throw new Error('Budget grant path must be a regular file.');
  if (stat.size <= 0 || stat.size > MAX_GRANT_BYTES)
    throw new Error('Budget grant file size is invalid.');
  if ((stat.mode & 0o077) !== 0) throw new Error('Budget grant file permissions are too broad.');
  return JSON.parse(readFileSync(GRANT_PATH, 'utf8')) as ProxyBudgetGrant;
}

let ledger: ProxyBudgetLedger | undefined;
try {
  const mode = readMode(process.argv.slice(2));
  if (mode !== 'status') mkdirSync(dirname(LEDGER_PATH), { recursive: true });
  const grant = readGrant();
  ledger = createProxyBudgetLedger({ mode, dbPath: LEDGER_PATH, grant });
  if (mode === 'status') {
    writeResponse({ ok: true, event: 'status', result: ledger.getBudgetStatus() });
    process.exitCode = 0;
  } else {
    writeResponse({ ok: true, event: 'ready', mode });
    await runJsonlLoop(ledger);
  }
} catch (error) {
  writeResponse({ ok: false, error: readErrorMessage(error) });
  process.exitCode = 1;
} finally {
  ledger?.close();
}

async function runJsonlLoop(ledger: ProxyBudgetLedger): Promise<void> {
  let buffered = '';
  let bufferedBytes = 0;
  for await (const chunk of input) {
    const text = chunkToString(chunk);
    buffered += text;
    const remaining = processCompleteLines(buffered, ledger);
    buffered = remaining;
    bufferedBytes = Buffer.byteLength(buffered, 'utf8');
    if (bufferedBytes > MAX_LINE_BYTES) throw new Error('Command line is too large.');
  }
  if (buffered.length > 0) writeResponse(handleLine(buffered, ledger));
}

function processCompleteLines(buffered: string, ledger: ProxyBudgetLedger): string {
  let start = 0;
  let newlineIndex = buffered.indexOf('\n', start);
  while (newlineIndex >= 0) {
    writeResponse(handleLine(buffered.slice(start, newlineIndex), ledger));
    start = newlineIndex + 1;
    newlineIndex = buffered.indexOf('\n', start);
  }
  return buffered.slice(start);
}

function chunkToString(chunk: unknown): string {
  if (Buffer.isBuffer(chunk)) return chunk.toString('utf8');
  if (typeof chunk === 'string') return chunk;
  throw new Error('Unsupported command stream chunk.');
}

function handleLine(line: string, ledger: ProxyBudgetLedger): object {
  try {
    if (Buffer.byteLength(line, 'utf8') > MAX_LINE_BYTES)
      throw new Error('Command line is too large.');
    const envelope = parseEnvelope(line);
    return handleParsedCommand(envelope, ledger);
  } catch (error) {
    return { ok: false, error: readErrorMessage(error) };
  }
}

function handleParsedCommand(envelope: CommandEnvelope, ledger: ProxyBudgetLedger): object {
  try {
    return handleCommand(envelope, ledger);
  } catch (error) {
    return { id: envelope.id, ok: false, error: readErrorMessage(error) };
  }
}

function parseEnvelope(line: string): CommandEnvelope {
  const parsed = JSON.parse(line) as Partial<CommandEnvelope>;
  if (!parsed || typeof parsed !== 'object') throw new Error('Command must be a JSON object.');
  if (typeof parsed.id !== 'string' || parsed.id.length === 0)
    throw new Error('Command ID is required.');
  if (Buffer.byteLength(parsed.id, 'utf8') > MAX_ID_BYTES)
    throw new Error('Command ID is too large.');
  if (typeof parsed.command !== 'string' || parsed.command.length === 0)
    throw new Error('Command name is required.');
  assertExactKeys(
    parsed as Record<string, unknown>,
    ['id', 'command', 'payload'],
    'Command envelope'
  );
  return { id: parsed.id, command: parsed.command, payload: parsed.payload };
}

function handleCommand(envelope: CommandEnvelope, ledger: ProxyBudgetLedger): object {
  const result = dispatchCommand(envelope.command, envelope.payload, ledger);
  return { id: envelope.id, ok: true, result };
}

function dispatchCommand(command: string, payload: unknown, ledger: ProxyBudgetLedger): object {
  if (command === 'reserve') return ledger.reserveBudget(readReservePayload(payload));
  if (command === 'settle') return ledger.settleBudget(readSettlePayload(payload));
  if (command === 'markUnknown') return ledger.markReservationUnknown(readUnknownPayload(payload));
  if (command === 'getReservation')
    return ledger.getReservation(readReservationIdPayload(payload).reservationId);
  if (command === 'status') return ledger.getBudgetStatus();
  throw new Error('Unsupported proxy budget command.');
}

function readReservePayload(payload: unknown): ReservePayload {
  const record = readPayload(payload, ['requestHash', 'inputCeiling', 'outputCeiling']);
  return {
    requestHash: readString(record.requestHash, 'requestHash'),
    inputCeiling: readNumber(record.inputCeiling, 'inputCeiling'),
    outputCeiling: readNumber(record.outputCeiling, 'outputCeiling'),
  };
}

function readSettlePayload(payload: unknown): SettlePayload {
  const record = readPayload(payload, ['reservationId', 'inputTokens', 'outputTokens']);
  return {
    reservationId: readString(record.reservationId, 'reservationId'),
    inputTokens: readNumber(record.inputTokens, 'inputTokens'),
    outputTokens: readNumber(record.outputTokens, 'outputTokens'),
  };
}

function readUnknownPayload(payload: unknown): UnknownPayload {
  const record = readPayload(payload, ['reservationId', 'reason']);
  return {
    reservationId: readString(record.reservationId, 'reservationId'),
    reason: readString(record.reason, 'reason'),
  };
}

function readReservationIdPayload(payload: unknown): { reservationId: string } {
  const record = readPayload(payload, ['reservationId']);
  return { reservationId: readString(record.reservationId, 'reservationId') };
}

function readPayload(payload: unknown, allowedKeys: string[]): Record<string, unknown> {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new Error('Command payload must be an object.');
  }
  const record = payload as Record<string, unknown>;
  assertExactKeys(record, allowedKeys, 'Command payload');
  return record;
}

function assertExactKeys(
  record: Record<string, unknown>,
  allowedKeys: string[],
  label: string
): void {
  if (Object.keys(record).some(key => !allowedKeys.includes(key))) {
    throw new Error(`${label} contains unsupported settings.`);
  }
}

function readString(value: unknown, field: string): string {
  if (typeof value !== 'string') throw new Error(`${field} must be a string.`);
  return value;
}

function readNumber(value: unknown, field: string): number {
  if (typeof value !== 'number') throw new Error(`${field} must be a number.`);
  return value;
}

function readErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown proxy budget error.';
}

function writeResponse(response: object): void {
  output.write(`${JSON.stringify(response)}\n`);
}
