import type { MessageChunk, TokenUsage } from '../types';

export function parseGrokStreamingLine(line: string): MessageChunk | undefined {
  const trimmed = line.trim();
  if (!trimmed) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed) as unknown;
  } catch {
    throw new Error('grok_stream_json_invalid');
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('grok_stream_json_invalid');
  }
  const row = parsed as Record<string, unknown>;
  const type = row.type;
  if (type === 'text' && typeof row.data === 'string') {
    return { type: 'assistant', content: row.data };
  }
  if (type === 'thought' && typeof row.data === 'string') {
    return { type: 'thinking', content: row.data };
  }
  if (type === 'tool_call') {
    return {
      type: 'tool',
      toolName: typeof row.toolName === 'string' ? row.toolName : 'unknown',
      ...(typeof row.toolCallId === 'string' ? { toolCallId: row.toolCallId } : {}),
      ...(isRecord(row.rawInput) ? { toolInput: row.rawInput } : {}),
    };
  }
  if (type === 'tool_call_update') {
    return {
      type: 'tool_result',
      toolName: typeof row.toolName === 'string' ? row.toolName : 'unknown',
      toolOutput: stringifyUnknown(row.rawOutput ?? row.content ?? ''),
      ...(typeof row.toolCallId === 'string' ? { toolCallId: row.toolCallId } : {}),
      toolOutcome:
        row.status === 'completed' ? 'success' : row.status === 'failed' ? 'error' : 'unknown',
    };
  }
  if (type === 'end') {
    return {
      type: 'result',
      ...(typeof row.sessionId === 'string' ? { sessionId: row.sessionId } : {}),
      ...(typeof row.stopReason === 'string' ? { stopReason: row.stopReason } : {}),
      ...(typeof row.num_turns === 'number' ? { numTurns: row.num_turns } : {}),
      ...(usageFrom(row.usage) ? { tokens: usageFrom(row.usage) } : {}),
    };
  }
  if (type === 'error') {
    return {
      type: 'result',
      isError: true,
      errors: [typeof row.message === 'string' ? row.message : 'grok_stream_error'],
    };
  }
  return undefined;
}

function usageFrom(value: unknown): TokenUsage | undefined {
  if (!isRecord(value)) return undefined;
  const input = value.input_tokens;
  const output = value.output_tokens;
  if (typeof input !== 'number' || typeof output !== 'number') return undefined;
  return { input, output };
}

function stringifyUnknown(value: unknown): string {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
