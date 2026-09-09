import { describe, test, expect, mock, beforeEach } from 'bun:test';

// Mock logger before importing any module that transitively imports @archon/paths
const mockLogger = {
  fatal: mock(() => undefined),
  error: mock(() => undefined),
  warn: mock(() => undefined),
  info: mock(() => undefined),
  debug: mock(() => undefined),
  trace: mock(() => undefined),
  child: mock(function (this: unknown) {
    return this;
  }),
  bindings: mock(() => ({ module: 'test' })),
  isLevelEnabled: mock(() => true),
  level: 'info' as const,
};

mock.module('@archon/paths', () => ({
  createLogger: mock(() => mockLogger),
}));

import { WebAdapter } from './web';
import { MAX_TOOL_OUTPUT_CHARS } from './web/truncate';
import type { SSETransport } from './web/transport';
import type { MessagePersistence } from './web/persistence';
import type { WorkflowEventBridge } from './web/workflow-bridge';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeAdapter(): {
  adapter: WebAdapter;
  emitted: string[];
  appendToolResultCalls: unknown[][];
} {
  const emitted: string[] = [];
  const appendToolResultCalls: unknown[][] = [];

  const mockTransport = {
    emit: mock(async (_id: string, event: string) => {
      emitted.push(event);
    }),
  } as unknown as SSETransport;

  const mockPersistence = {
    appendToolResult: mock((_id: string, name: string, output: string, duration: number) => {
      appendToolResultCalls.push([_id, name, output, duration]);
    }),
    appendToolCall: mock(() => {}),
    appendText: mock(() => {}),
    flush: mock(async () => {}),
    finalizeRunningTools: mock(() => {}),
  } as unknown as MessagePersistence;

  const mockBridge = {
    emitOutput: mock(() => {}),
    registerOutputCallback: mock(() => {}),
    removeOutputCallback: mock(() => {}),
    setStepTransitionCallback: mock(() => {}),
    start: mock(() => {}),
    stop: mock(() => {}),
    bridgeWorkerEvents: mock(() => () => {}),
  } as unknown as WorkflowEventBridge;

  const adapter = new WebAdapter(mockTransport, mockPersistence, mockBridge);
  return { adapter, emitted, appendToolResultCalls };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockLogger.warn.mockClear();
  mockLogger.error.mockClear();
});

describe('WebAdapter.sendStructuredEvent — tool_result output bounding', () => {
  test('pairs stable tool_call and tool_result IDs before truncating SSE output', async () => {
    const { adapter, emitted, appendToolResultCalls } = makeAdapter();

    await adapter.sendStructuredEvent('conv-1', {
      type: 'tool',
      toolName: 'bash',
      toolInput: { command: 'pwd' },
      toolCallId: 'call-stable',
    });
    await adapter.sendStructuredEvent('conv-1', {
      type: 'tool_result',
      toolName: 'bash',
      toolOutput: 'ok',
      toolCallId: 'call-stable',
    });

    expect(emitted.map(event => JSON.parse(event) as Record<string, unknown>)).toEqual([
      {
        type: 'tool_call',
        toolCallId: 'call-stable',
        name: 'bash',
        input: { command: 'pwd' },
        timestamp: expect.any(Number),
      },
      {
        type: 'tool_result',
        toolCallId: 'call-stable',
        name: 'bash',
        output: 'ok',
        duration: expect.any(Number),
        timestamp: expect.any(Number),
      },
    ]);
    expect(appendToolResultCalls[0]?.slice(0, 3)).toEqual(['conv-1', 'bash', 'ok']);
    expect(mockLogger.warn).not.toHaveBeenCalledWith(
      expect.anything(),
      'web_adapter.tool_result_unmatched'
    );
  });

  test('uses fallback counters and reverse name matching when no stable ID exists', async () => {
    const { adapter, emitted } = makeAdapter();

    await adapter.sendStructuredEvent('conv-2', { type: 'tool', toolName: 'bash' });
    await adapter.sendStructuredEvent('conv-2', { type: 'tool', toolName: 'bash' });
    await adapter.sendStructuredEvent('conv-2', {
      type: 'tool_result',
      toolName: 'bash',
      toolOutput: 'second result',
    });

    const payloads = emitted.map(event => JSON.parse(event) as Record<string, unknown>);
    expect(payloads[0]?.toolCallId).toBe('conv-2-tool-1');
    expect(payloads[1]?.toolCallId).toBe('conv-2-tool-2');
    expect(payloads[2]?.toolCallId).toBe('conv-2-tool-2');
  });

  test('truncates SSE event output when toolOutput exceeds the cap', async () => {
    const { adapter, emitted } = makeAdapter();
    const largeOutput = 'x'.repeat(MAX_TOOL_OUTPUT_CHARS + 50_000);

    await adapter.sendStructuredEvent('conv-1', {
      type: 'tool_result',
      toolName: 'bash',
      toolOutput: largeOutput,
    });

    expect(emitted.length).toBe(1);
    const parsed = JSON.parse(emitted[0]!) as { output: string };
    expect(parsed.output.length).toBeLessThan(largeOutput.length);
    expect(parsed.output).toContain('[truncated');
    expect(parsed.output).toContain('full output preserved on the server');
  });

  test('passes SSE event output through unchanged when within the cap', async () => {
    const { adapter, emitted } = makeAdapter();
    const smallOutput = 'small tool output';

    await adapter.sendStructuredEvent('conv-1', {
      type: 'tool_result',
      toolName: 'bash',
      toolOutput: smallOutput,
    });

    expect(emitted.length).toBe(1);
    const parsed = JSON.parse(emitted[0]!) as { output: string };
    expect(parsed.output).toBe(smallOutput);
  });

  test('persists full untruncated output to DB regardless of the SSE cap', async () => {
    const { adapter, appendToolResultCalls } = makeAdapter();
    const largeOutput = 'z'.repeat(MAX_TOOL_OUTPUT_CHARS + 50_000);

    await adapter.sendStructuredEvent('conv-1', {
      type: 'tool_result',
      toolName: 'bash',
      toolOutput: largeOutput,
    });

    expect(appendToolResultCalls.length).toBe(1);
    // Third argument to appendToolResult is the output — must be the full string
    expect(appendToolResultCalls[0]![2]).toBe(largeOutput);
  });
});
