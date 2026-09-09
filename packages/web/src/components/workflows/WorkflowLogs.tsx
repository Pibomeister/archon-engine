import { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { MessageList } from '@/components/chat/MessageList';
import { useSSE } from '@/hooks/useSSE';
import { getMessages } from '@/lib/api';
import { ensureUtc, formatDurationMs } from '@/lib/format';
import type { MessageResponse } from '@/lib/api';
import { workflowSSEHandlers } from '@/stores/workflow-store';
import type { ChatMessage, ToolCallDisplay, ErrorDisplay } from '@/lib/types';
import type { ToolEvent } from './WorkflowExecution';

interface WorkflowLogsProps {
  conversationId: string;
  startedAt?: number;
  isRunning?: boolean;
  currentlyExecuting?: { nodeName: string; startedAt: number } | null;
  toolEvents?: ToolEvent[];
  /** Timestamp of the selected node's start — used to scroll the message list. */
  scrollToNodeTimestamp?: number | null;
  /** Incremented on every user node click to trigger scroll. */
  nodeScrollTrigger?: number;
}

interface MessageMetadata {
  error?: ErrorDisplay;
  toolCalls?: {
    name: string;
    input: Record<string, unknown>;
    output?: string;
    duration?: number;
  }[];
}

function parseMessageMetadata(row: MessageResponse): MessageMetadata {
  try {
    return JSON.parse(row.metadata) as MessageMetadata;
  } catch {
    console.warn('[WorkflowLogs] Corrupted message metadata', { messageId: row.id });
    return {};
  }
}

function persistedToolCalls(
  row: MessageResponse,
  meta: MessageMetadata,
  timestamp: number
): ToolCallDisplay[] | undefined {
  return meta.toolCalls?.map((tc, i) => ({
    id: `${row.id}-tool-${String(i)}`,
    name: tc.name,
    input: tc.input,
    output: tc.output,
    duration: tc.duration,
    startedAt: timestamp,
    isExpanded: false,
  }));
}

function hydrateMessage(row: MessageResponse): ChatMessage {
  const meta = parseMessageMetadata(row);
  const timestamp = new Date(ensureUtc(row.created_at)).getTime();
  return {
    id: row.id,
    role: row.role,
    content: row.content,
    error: meta.error,
    toolCalls: persistedToolCalls(row, meta, timestamp),
    timestamp,
    isStreaming: false,
  };
}

function metadataToolIndex(messages: ChatMessage[]): { name: string; timestamp: number }[] {
  return messages.flatMap(message =>
    (message.toolCalls ?? []).map(tool => ({ name: tool.name, timestamp: tool.startedAt }))
  );
}

function isDuplicateToolEvent(
  event: ToolEvent,
  timestamp: number,
  metadataTools: readonly { name: string; timestamp: number }[],
  claimedMetadata: Set<number>
): boolean {
  const matchIndex = metadataTools.findIndex(
    (tool, index) =>
      !claimedMetadata.has(index) &&
      tool.name === event.name &&
      Math.abs(tool.timestamp - timestamp) < 60_000
  );
  if (matchIndex < 0) return false;
  claimedMetadata.add(matchIndex);
  return true;
}

function toolCallFromEvent(event: ToolEvent, timestamp: number): ToolCallDisplay {
  return {
    id: event.id,
    name: event.name,
    input: event.input,
    startedAt: timestamp,
    isExpanded: false,
    duration: event.duration,
  };
}

function findToolTarget(messages: ChatMessage[], timestamp: number): ChatMessage | undefined {
  let target: ChatMessage | undefined;
  for (const message of messages) {
    if (message.timestamp <= timestamp) target = message;
    else break;
  }
  return target ?? messages[0];
}

function attachToolEvent(
  messages: ChatMessage[],
  event: ToolEvent,
  metadataTools: readonly { name: string; timestamp: number }[],
  claimedMetadata: Set<number>,
  unattached: ToolCallDisplay[]
): void {
  const timestamp = new Date(ensureUtc(event.createdAt)).getTime();
  if (isDuplicateToolEvent(event, timestamp, metadataTools, claimedMetadata)) return;
  const toolCall = toolCallFromEvent(event, timestamp);
  const target = findToolTarget(messages, timestamp);
  if (target === undefined) {
    unattached.push(toolCall);
    return;
  }
  target.toolCalls ??= [];
  if (!target.toolCalls.some(tc => tc.id === event.id)) target.toolCalls.push(toolCall);
}

function appendSyntheticToolMessage(messages: ChatMessage[], unattached: ToolCallDisplay[]): void {
  if (unattached.length === 0) return;
  const earliestTs = Math.min(...unattached.map(tc => tc.startedAt));
  messages.push({
    id: `synthetic-tools-${String(earliestTs)}`,
    role: 'assistant',
    content: '',
    toolCalls: unattached,
    timestamp: earliestTs,
    isStreaming: false,
  });
  messages.sort((a, b) => a.timestamp - b.timestamp);
}

function attachWorkflowToolEvents(messages: ChatMessage[], toolEvents?: ToolEvent[]): void {
  if (toolEvents === undefined || toolEvents.length === 0) return;
  const assistantMsgs = messages.filter(m => m.role === 'assistant');
  const metadataTools = metadataToolIndex(assistantMsgs);
  const claimedMetadata = new Set<number>();
  const unattached: ToolCallDisplay[] = [];
  for (const event of toolEvents) {
    attachToolEvent(assistantMsgs, event, metadataTools, claimedMetadata, unattached);
  }
  appendSyntheticToolMessage(messages, unattached);
}

function hydrateMessages(
  rows: MessageResponse[],
  startedAt?: number,
  toolEvents?: ToolEvent[]
): ChatMessage[] {
  const hydrated = rows.map(hydrateMessage);
  const filtered = startedAt ? hydrated.filter(m => m.timestamp >= startedAt) : hydrated;
  attachWorkflowToolEvents(filtered, toolEvents);
  return filtered;
}

function activeToolCalls(message: ChatMessage): ToolCallDisplay[] {
  return (message.toolCalls ?? []).filter(tc => tc.duration === undefined && !tc.output);
}

function pruneSseMessage(message: ChatMessage): { message: ChatMessage | null; changed: boolean } {
  if (message.isStreaming) return { message, changed: false };
  const activeTools = activeToolCalls(message);
  if (activeTools.length === 0) return { message: null, changed: true };
  const changed = activeTools.length < (message.toolCalls?.length ?? 0);
  return { message: changed ? { ...message, toolCalls: activeTools } : message, changed };
}

function pruneSseMessages(prev: ChatMessage[]): ChatMessage[] {
  let changed = false;
  const result: ChatMessage[] = [];
  for (const message of prev) {
    const pruned = pruneSseMessage(message);
    changed ||= pruned.changed;
    if (pruned.message !== null) result.push(pruned.message);
  }
  return changed ? result : prev;
}

function completedDbTools(messages: ChatMessage[]): { name: string; duration: number }[] {
  return messages.flatMap(message =>
    (message.toolCalls ?? [])
      .filter(tool => tool.duration !== undefined)
      .map(tool => ({ name: tool.name, duration: tool.duration ?? 0 }))
  );
}

function sseInFlightToolCounts(messages: ChatMessage[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const message of messages) {
    for (const tool of activeToolCalls(message))
      counts.set(tool.name, (counts.get(tool.name) ?? 0) + 1);
  }
  return counts;
}

function suppressInFlightDbTools(
  message: ChatMessage,
  counts: Map<string, number>,
  suppressed: Map<string, number>,
  now: number
): ChatMessage {
  if (!message.toolCalls?.length) return message;
  let hasUnsuppressedInFlight = false;
  const filteredTools = message.toolCalls.filter(tool => {
    if (tool.duration !== undefined || !!tool.output) return true;
    const limit = counts.get(tool.name) ?? 0;
    const current = suppressed.get(tool.name) ?? 0;
    if (current < limit) {
      suppressed.set(tool.name, current + 1);
      return false;
    }
    hasUnsuppressedInFlight = true;
    return true;
  });
  if (filteredTools.length === 0) return { ...message, toolCalls: undefined };
  const changed = filteredTools.length !== message.toolCalls.length || hasUnsuppressedInFlight;
  return changed
    ? {
        ...message,
        toolCalls: filteredTools,
        ...(hasUnsuppressedInFlight ? { timestamp: now } : {}),
      }
    : message;
}

function filterDbMessagesForLiveTools(
  dbMessages: ChatMessage[],
  sseMessages: ChatMessage[],
  isRunning: boolean
): ChatMessage[] {
  const counts = sseInFlightToolCounts(sseMessages);
  if (counts.size === 0 && !isRunning) return dbMessages;
  const suppressed = new Map<string, number>();
  const now = Date.now();
  const mapped = dbMessages.map(message =>
    suppressInFlightDbTools(message, counts, suppressed, now)
  );
  return mapped.every((message, index) => message === dbMessages[index]) ? dbMessages : mapped;
}

function dbTextContentSet(messages: ChatMessage[]): Set<string> {
  return new Set(
    messages
      .filter(message => message.role === 'assistant' && message.content)
      .map(message => message.content)
  );
}

function isToolInDb(
  tool: ToolCallDisplay,
  dbTools: readonly { name: string; duration: number }[]
): boolean {
  const duration = tool.duration;
  if (duration === undefined) return false;
  return dbTools.some(dt => dt.name === tool.name && Math.abs(dt.duration - duration) < 500);
}

function shouldSkipSseText(message: ChatMessage, dbTextContents: Set<string>): boolean {
  if (!message.content) return false;
  if (dbTextContents.has(message.content)) return true;
  return [...dbTextContents].some(content => content.startsWith(message.content));
}

function dedupeSseMessages(sseMessages: ChatMessage[], dbMessages: ChatMessage[]): ChatMessage[] {
  const dbTools = completedDbTools(dbMessages);
  const dbTextContents = dbTextContentSet(dbMessages);
  const deduped: ChatMessage[] = [];
  for (const message of sseMessages) {
    if (!message.toolCalls?.length) {
      if (!shouldSkipSseText(message, dbTextContents) && (message.isStreaming || message.content))
        deduped.push(message);
      continue;
    }
    const uniqueTools = message.toolCalls.filter(tool => !isToolInDb(tool, dbTools));
    if (uniqueTools.length > 0 || message.isStreaming || message.content) {
      deduped.push({ ...message, toolCalls: uniqueTools.length > 0 ? uniqueTools : undefined });
    }
  }
  return deduped;
}

function mergeWorkflowMessages(
  queryMessages: ChatMessage[] | undefined,
  sseMessages: ChatMessage[],
  isRunning: boolean | undefined,
  gracePolling: boolean
): ChatMessage[] {
  const dbMessages = queryMessages ?? [];
  if (!isRunning && !gracePolling) return dbMessages;
  if (sseMessages.length === 0) return dbMessages;
  if (dbMessages.length === 0) return sseMessages;
  const filteredDbMessages = filterDbMessagesForLiveTools(
    dbMessages,
    sseMessages,
    isRunning === true
  );
  const dedupedSse = dedupeSseMessages(sseMessages, filteredDbMessages);
  if (dedupedSse.length === 0) return filteredDbMessages;
  return [...filteredDbMessages, ...dedupedSse].sort((a, b) => a.timestamp - b.timestamp);
}

/**
 * Read-only chat view for a workflow's worker conversation.
 * Loads historical messages via React Query polling and streams live updates via SSE.
 */
export function WorkflowLogs({
  conversationId,
  startedAt,
  isRunning,
  currentlyExecuting,
  toolEvents,
  scrollToNodeTimestamp,
  nodeScrollTrigger,
}: WorkflowLogsProps): React.ReactElement {
  const [sseMessages, setSseMessages] = useState<ChatMessage[]>([]);
  const queryClient = useQueryClient();
  const prevIsRunningRef = useRef(isRunning);
  const [gracePolling, setGracePolling] = useState(false);
  const [scrollTrigger, setScrollTrigger] = useState(0);

  // Tick timer for live elapsed display on "currently executing" indicator
  const [, setExecTick] = useState(0);
  useEffect(() => {
    if (!isRunning || !currentlyExecuting) return;
    const interval = setInterval(() => {
      setExecTick(t => t + 1);
    }, 1000);
    return (): void => {
      clearInterval(interval);
    };
  }, [isRunning, currentlyExecuting]);

  // Poll for messages from DB — 3s while running (or during grace period), disabled when terminal.
  // staleTime: 0 ensures post-completion navigation always fetches fresh data on mount.
  const { data: queryMessages } = useQuery({
    queryKey: ['workflowMessages', conversationId],
    queryFn: async (): Promise<ChatMessage[]> => {
      const rows = await getMessages(conversationId);
      return hydrateMessages(rows, startedAt, toolEvents);
    },
    refetchInterval: isRunning || gracePolling ? 3000 : false,
    staleTime: 0,
  });

  // When workflow transitions from running → terminal, keep polling for 6 more seconds
  // (2 extra cycles) to catch late DB flushes, then do a final invalidation.
  // Also force-scroll to bottom so the user sees the final output.
  useEffect(() => {
    if (prevIsRunningRef.current && !isRunning) {
      // Finalize any in-flight SSE tool calls that never received tool_result.
      // This is a safety net for when onLockChange fires late or is missed.
      const now = Date.now();
      setSseMessages(prev =>
        prev.map(msg => {
          const hasOpenTool = msg.toolCalls?.some(tc => tc.duration === undefined && !tc.output);
          if (!hasOpenTool && !msg.isStreaming) return msg;
          return {
            ...msg,
            isStreaming: false,
            toolCalls: msg.toolCalls?.map(tc =>
              tc.duration === undefined && !tc.output ? { ...tc, duration: now - tc.startedAt } : tc
            ),
          };
        })
      );

      setGracePolling(true);
      setScrollTrigger(prev => prev + 1);
      const timer = setTimeout(() => {
        setGracePolling(false);
        // Final invalidation to pick up late DB flushes
        void queryClient.invalidateQueries({ queryKey: ['workflowMessages', conversationId] });
        setScrollTrigger(prev => prev + 1);
      }, 6000);
      return (): void => {
        clearTimeout(timer);
      };
    }
    prevIsRunningRef.current = isRunning;
    return undefined;
  }, [isRunning, conversationId, queryClient]);

  // When DB messages arrive, prune SSE messages to avoid duplicates.
  // DB is canonical for completed content. SSE messages may carry both completed
  // tool calls (already in DB after flush) and in-progress ones (not yet in DB).
  // We strip completed tools from SSE messages and drop fully-completed ones.
  // This mirrors ChatInterface's hydration merge pattern.
  useEffect(() => {
    if (!queryMessages || queryMessages.length === 0) return;
    setSseMessages(pruneSseMessages);
  }, [queryMessages]);

  // Merge DB messages (canonical) with SSE-only messages (live streaming).
  //
  // Strategy: DB is always canonical for persisted content. SSE messages are
  // pruned by the effect above whenever new DB data arrives, so only active
  // (streaming / in-progress) SSE messages remain. This prevents duplicates
  // where both DB and SSE contain the same completed tool calls.
  const messages = useMemo(
    (): ChatMessage[] => mergeWorkflowMessages(queryMessages, sseMessages, isRunning, gracePolling),
    [queryMessages, sseMessages, isRunning, gracePolling]
  );

  const onText = useCallback((content: string): void => {
    setSseMessages(prev => {
      const last = prev[prev.length - 1];
      // Workflow status messages (🚀 start, ✅ complete) should be their own message,
      // matching ChatInterface's behavior and persistence segmentation. Without this,
      // all text concatenates into one giant streaming message, breaking text dedup
      // against DB messages (which are stored as separate segments).
      const isWorkflowStatus = /^[\u{1F680}\u{2705}]/u.test(content);

      if (last?.role === 'assistant' && last.isStreaming) {
        const lastIsWorkflowStatus = /^[\u{1F680}\u{2705}]/u.test(last.content);

        if ((isWorkflowStatus && last.content) || (lastIsWorkflowStatus && !isWorkflowStatus)) {
          // Close the current streaming message and start a new one when:
          // 1. Incoming is a workflow status and current has content
          // 2. Current is a workflow status and incoming is regular text
          return [
            ...prev.slice(0, -1),
            { ...last, isStreaming: false },
            {
              id: `msg-${String(Date.now())}`,
              role: 'assistant' as const,
              content,
              timestamp: Date.now(),
              isStreaming: true,
              toolCalls: [],
            },
          ];
        }
        return [...prev.slice(0, -1), { ...last, content: last.content + content }];
      }
      return [
        ...prev,
        {
          id: `msg-${String(Date.now())}`,
          role: 'assistant' as const,
          content,
          timestamp: Date.now(),
          isStreaming: true,
          toolCalls: [],
        },
      ];
    });
  }, []);

  const onToolCall = useCallback((name: string, input: Record<string, unknown>): void => {
    setSseMessages(prev => {
      const now = Date.now();
      let last = prev[prev.length - 1];

      // If no assistant message exists yet (tool arrives before any text),
      // create one so the tool card has a home in the message list.
      if (last?.role !== 'assistant') {
        last = {
          id: `msg-${String(now)}`,
          role: 'assistant' as const,
          content: '',
          timestamp: now,
          isStreaming: false,
          toolCalls: [],
        };
        const newTool: ToolCallDisplay = {
          id: `${last.id}-tool-0`,
          name,
          input,
          startedAt: now,
          isExpanded: false,
        };
        return [...prev, { ...last, toolCalls: [newTool] }];
      }

      const updatedExistingTools = (last.toolCalls ?? []).map(tc =>
        !tc.output && tc.duration === undefined ? { ...tc, duration: now - tc.startedAt } : tc
      );
      const newTool: ToolCallDisplay = {
        id: `${last.id}-tool-${String(updatedExistingTools.length)}`,
        name,
        input,
        startedAt: now,
        isExpanded: false,
      };
      return [
        ...prev.slice(0, -1),
        {
          ...last,
          isStreaming: false,
          toolCalls: [...updatedExistingTools, newTool],
        },
      ];
    });
  }, []);

  const onToolResult = useCallback((name: string, output: string, duration: number): void => {
    setSseMessages(prev => {
      const last = prev[prev.length - 1];
      if (last?.role === 'assistant' && last.toolCalls) {
        const updatedTools = last.toolCalls.map(tc =>
          tc.name === name && !tc.output ? { ...tc, output, duration } : tc
        );
        return [...prev.slice(0, -1), { ...last, toolCalls: updatedTools }];
      }
      return prev;
    });
  }, []);

  const onError = useCallback((error: ErrorDisplay): void => {
    setSseMessages(prev => {
      const last = prev[prev.length - 1];
      if (last?.role === 'assistant') {
        return [...prev.slice(0, -1), { ...last, isStreaming: false, error }];
      }
      return [
        ...prev,
        {
          id: `msg-${String(Date.now())}`,
          role: 'assistant',
          content: '',
          error,
          timestamp: Date.now(),
        },
      ];
    });
  }, []);

  const onLockChange = useCallback((isLocked: boolean): void => {
    if (!isLocked) {
      const now = Date.now();
      setSseMessages(prev =>
        prev.map(msg => {
          const needsToolFix = msg.toolCalls?.some(tc => !tc.output && tc.duration === undefined);
          const needsStreamFix = msg.isStreaming;
          if (!needsToolFix && !needsStreamFix) return msg;
          return {
            ...msg,
            isStreaming: false,
            toolCalls: needsToolFix
              ? msg.toolCalls?.map(tc =>
                  !tc.output && tc.duration === undefined
                    ? { ...tc, duration: now - tc.startedAt }
                    : tc
                )
              : msg.toolCalls,
          };
        })
      );
    }
  }, []);

  const onSessionInfo = useCallback((_sessionId: string, _cost?: number): void => {
    // No-op for read-only view
  }, []);

  useSSE(conversationId, {
    onText,
    onToolCall,
    onToolResult,
    onError,
    onLockChange,
    onSessionInfo,
    ...workflowSSEHandlers,
  });

  // If workflow is running but no message is currently streaming,
  // append a thinking placeholder so the three pulsing dots appear at the bottom.
  const displayMessages = useMemo((): ChatMessage[] => {
    if (!isRunning) return messages;
    const hasActiveStream = messages.some(m => m.isStreaming);
    if (hasActiveStream) return messages;
    return [
      ...messages,
      {
        id: 'workflow-thinking',
        role: 'assistant' as const,
        content: '',
        timestamp: Date.now(),
        isStreaming: true,
      },
    ];
  }, [messages, isRunning]);

  const isStreaming = displayMessages.some(m => m.isStreaming);

  // Show loading indicator while waiting for first messages
  if (displayMessages.length === 0 && (isRunning || queryMessages === undefined)) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <div className="flex flex-col items-center gap-3 text-text-tertiary">
          <span className="inline-block h-6 w-6 animate-spin rounded-full border-2 border-accent border-t-transparent" />
          <p className="text-sm">Loading workflow logs...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col flex-1 overflow-hidden min-h-0">
      {isRunning && currentlyExecuting && (
        <div className="px-4 py-2 bg-surface-secondary border-b border-border flex items-center gap-2 text-sm shrink-0">
          <span className="inline-block w-2 h-2 rounded-full bg-accent animate-pulse" />
          <span className="text-text-secondary">Currently executing:</span>
          <span className="font-medium text-text-primary">{currentlyExecuting.nodeName}</span>
          <span className="text-text-tertiary text-xs">
            ({formatDurationMs(Date.now() - currentlyExecuting.startedAt)})
          </span>
        </div>
      )}
      <MessageList
        messages={displayMessages}
        isStreaming={isStreaming}
        scrollTrigger={scrollTrigger}
        scrollToTimestamp={scrollToNodeTimestamp}
        scrollToTrigger={nodeScrollTrigger}
      />
    </div>
  );
}
