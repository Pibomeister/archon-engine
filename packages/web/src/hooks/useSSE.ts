import { useEffect, useRef, useState, useCallback } from 'react';
import type {
  SSEEvent,
  ErrorDisplay,
  LoopIterationEvent,
  WorkflowStatusEvent,
  WorkflowArtifactEvent,
  WorkflowDispatchEvent,
  WorkflowOutputPreviewEvent,
  WorkflowTaskActivityEvent,
  WorkflowHookActivityEvent,
  DagNodeEvent,
} from '@/lib/types';
import { SSE_BASE_URL } from '@/lib/api';

function parseSSEEvent(raw: string): SSEEvent | null {
  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    if (!parsed || typeof parsed.type !== 'string') {
      console.error('[SSE] Malformed event: missing type field', { raw });
      return null;
    }
    return parsed as unknown as SSEEvent;
  } catch (parseErr) {
    console.error('[SSE] Failed to parse event:', {
      raw,
      error: (parseErr as Error).message,
    });
    return null;
  }
}

interface SSEHandlers {
  onText: (content: string, workflowResult?: { workflowName: string; runId: string }) => void;
  onToolCall: (name: string, input: Record<string, unknown>, toolCallId?: string) => void;
  onToolResult: (name: string, output: string, duration: number, toolCallId?: string) => void;
  onError: (error: ErrorDisplay) => void;
  onLockChange: (locked: boolean, queuePosition?: number) => void;
  onSessionInfo: (sessionId: string, cost?: number) => void;
  onWorkflowStatus?: (event: WorkflowStatusEvent) => void;
  onWorkflowArtifact?: (event: WorkflowArtifactEvent) => void;
  onDagNode?: (event: DagNodeEvent) => void;
  onLoopIteration?: (event: LoopIterationEvent) => void;
  onWorkflowDispatch?: (event: WorkflowDispatchEvent) => void;
  onWorkflowOutputPreview?: (event: WorkflowOutputPreviewEvent) => void;
  onTaskActivity?: (event: WorkflowTaskActivityEvent) => void;
  onHookActivity?: (event: WorkflowHookActivityEvent) => void;
  onWarning?: (message: string) => void;
  onRetract?: () => void;
  onSystemStatus?: (content: string) => void;
}

interface SSEDispatchContext {
  handlers: SSEHandlers;
  textBufferRef: React.MutableRefObject<string>;
  flushTimerRef: React.MutableRefObject<ReturnType<typeof setTimeout> | null>;
  pendingWorkflowResultRef: React.MutableRefObject<
    { workflowName: string; runId: string } | undefined
  >;
  flushText: () => void;
}

function flushBufferedText(context: SSEDispatchContext): void {
  if (!context.textBufferRef.current) return;
  if (context.flushTimerRef.current) {
    clearTimeout(context.flushTimerRef.current);
    context.flushTimerRef.current = null;
  }
  context.flushText();
}

function clearBufferedText(context: SSEDispatchContext): void {
  if (context.flushTimerRef.current) {
    clearTimeout(context.flushTimerRef.current);
    context.flushTimerRef.current = null;
  }
  context.textBufferRef.current = '';
  context.pendingWorkflowResultRef.current = undefined;
}

function handleTextEvent(
  data: Extract<SSEEvent, { type: 'text' }>,
  context: SSEDispatchContext
): void {
  context.textBufferRef.current += data.content;
  if ('workflowResult' in data && data.workflowResult && typeof data.workflowResult === 'object') {
    context.pendingWorkflowResultRef.current = data.workflowResult as {
      workflowName: string;
      runId: string;
    };
  }
  if (!context.flushTimerRef.current)
    context.flushTimerRef.current = setTimeout(context.flushText, 50);
}

function handleWorkflowStatusEvent(
  data: Extract<SSEEvent, { type: 'workflow_status' }>,
  handlers: SSEHandlers
): void {
  handlers.onWorkflowStatus?.(data);
  if (data.status === 'completed' || data.status === 'failed' || data.status === 'cancelled') {
    handlers.onLockChange(false);
  }
}

function dispatchStreamEvent(data: SSEEvent, context: SSEDispatchContext): boolean {
  const h = context.handlers;
  switch (data.type) {
    case 'text':
      handleTextEvent(data, context);
      return true;
    case 'tool_call':
      flushBufferedText(context);
      h.onToolCall(data.name, data.input, data.toolCallId);
      return true;
    case 'tool_result':
      flushBufferedText(context);
      h.onToolResult(data.name, data.output, data.duration, data.toolCallId);
      return true;
    case 'conversation_lock':
      if (!data.locked) flushBufferedText(context);
      h.onLockChange(data.locked, data.queuePosition);
      return true;
    case 'retract':
      clearBufferedText(context);
      h.onRetract?.();
      return true;
    default:
      return false;
  }
}

function dispatchWorkflowEvent(data: SSEEvent, handlers: SSEHandlers): boolean {
  switch (data.type) {
    case 'workflow_status':
      handleWorkflowStatusEvent(data, handlers);
      return true;
    case 'workflow_artifact':
      handlers.onWorkflowArtifact?.(data);
      return true;
    case 'dag_node':
      handlers.onDagNode?.(data);
      return true;
    case 'workflow_step':
      handlers.onLoopIteration?.(data);
      return true;
    case 'workflow_dispatch':
    case 'workflow_output_preview':
    case 'workflow_task_activity':
    case 'workflow_hook_activity':
      return false;
    default:
      return false;
  }
}

function dispatchWorkflowAuxEvent(data: SSEEvent, context: SSEDispatchContext): boolean {
  const h = context.handlers;
  switch (data.type) {
    case 'workflow_dispatch':
      flushBufferedText(context);
      h.onWorkflowDispatch?.(data);
      return true;
    case 'workflow_output_preview':
      h.onWorkflowOutputPreview?.(data);
      return true;
    case 'workflow_task_activity':
      h.onTaskActivity?.(data);
      return true;
    case 'workflow_hook_activity':
      h.onHookActivity?.(data);
      return true;
    default:
      return false;
  }
}

function dispatchSystemEvent(data: SSEEvent, handlers: SSEHandlers): boolean {
  switch (data.type) {
    case 'error':
      handlers.onError({
        message: data.message,
        classification: data.classification ?? 'transient',
        suggestedActions: data.suggestedActions ?? [],
      });
      return true;
    case 'session_info':
      handlers.onSessionInfo(data.sessionId, data.cost);
      return true;
    case 'warning':
      handlers.onWarning?.(data.message);
      return true;
    case 'system_status':
      handlers.onSystemStatus?.(data.content);
      return true;
    case 'heartbeat':
      return true;
    default:
      return false;
  }
}

function dispatchSSEEvent(data: SSEEvent, context: SSEDispatchContext): void {
  if (dispatchStreamEvent(data, context)) return;
  if (dispatchWorkflowEvent(data, context.handlers)) return;
  if (dispatchWorkflowAuxEvent(data, context)) return;
  if (dispatchSystemEvent(data, context.handlers)) return;
  console.warn('[SSE] Unknown event type', { type: (data as { type: string }).type });
}

function notifyHandlerError(data: SSEEvent, handlers: SSEHandlers, handlerError: unknown): void {
  console.error('[SSE] Handler error for event type:', data.type, handlerError);
  try {
    handlers.onError({
      message: `Failed to process ${data.type} event. UI may be out of sync.`,
      classification: 'transient',
      suggestedActions: ['Refresh the page if chat appears stuck'],
    });
  } catch {
    // Avoid infinite loop if onError itself throws
  }
}

export function useSSE(
  conversationId: string | null,
  handlers: SSEHandlers
): { connected: boolean } {
  const [connected, setConnected] = useState(false);
  const handlersRef = useRef(handlers);
  handlersRef.current = handlers;

  // Text batching: accumulate text for 50ms before dispatching
  const textBufferRef = useRef('');
  const flushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingWorkflowResultRef = useRef<{ workflowName: string; runId: string } | undefined>(
    undefined
  );

  const flushText = useCallback((): void => {
    if (textBufferRef.current) {
      handlersRef.current.onText(textBufferRef.current, pendingWorkflowResultRef.current);
      textBufferRef.current = '';
      pendingWorkflowResultRef.current = undefined;
    }
    flushTimerRef.current = null;
  }, []);

  useEffect(() => {
    if (!conversationId) return;

    const eventSource = new EventSource(
      `${SSE_BASE_URL}/api/stream/${encodeURIComponent(conversationId)}`
    );

    eventSource.onopen = (): void => {
      setConnected(true);
    };

    eventSource.onerror = (): void => {
      // Only mark disconnected when the connection is permanently closed,
      // not during transient CONNECTING reconnection attempts (prevents flicker)
      if (eventSource.readyState === EventSource.CLOSED) {
        setConnected(false);
        handlersRef.current.onError({
          message: 'Lost connection to server. Please refresh the page.',
          classification: 'transient',
          suggestedActions: ['Refresh the page', 'Check that the server is running'],
        });
      } else if (eventSource.readyState === EventSource.CONNECTING) {
        console.warn('[SSE] Connection error, reconnecting...', { conversationId });
      }
    };

    eventSource.onmessage = (event: MessageEvent): void => {
      const data = parseSSEEvent(event.data as string);
      if (!data) {
        handlersRef.current.onError({
          message: 'Received malformed response from server',
          classification: 'transient',
          suggestedActions: ['Refresh the page if chat appears stuck'],
        });
        return;
      }

      const context: SSEDispatchContext = {
        handlers: handlersRef.current,
        textBufferRef,
        flushTimerRef,
        pendingWorkflowResultRef,
        flushText,
      };
      try {
        dispatchSSEEvent(data, context);
      } catch (handlerError) {
        notifyHandlerError(data, handlersRef.current, handlerError);
      }
    };

    return (): void => {
      eventSource.close();
      setConnected(false);
      if (flushTimerRef.current) {
        clearTimeout(flushTimerRef.current);
        flushText();
      }
    };
  }, [conversationId, flushText]);

  return { connected };
}
