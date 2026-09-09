import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router';
import { useMutation, useQuery } from '@tanstack/react-query';
import { CheckCircle, ChevronRight, Loader2, Pause, XCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { approveWorkflowRun, getWorkflowRunByWorker, rejectWorkflowRun } from '@/lib/api';
import { useWorkflowStore } from '@/stores/workflow-store';
import { ConfirmRunActionDialog } from '@/components/dashboard/ConfirmRunActionDialog';
import { StatusIcon } from '@/components/workflows/StatusIcon';
import { formatDurationMs } from '@/lib/format';
import { isTerminalStatus } from '@/lib/workflow-utils';
import type { DagNodeState } from '@/lib/types';

interface WorkflowProgressCardProps {
  workflowName: string;
  workerConversationId: string;
}

type RunDataByWorker = Awaited<ReturnType<typeof getWorkflowRunByWorker>>;
type LiveWorkflowState =
  ReturnType<typeof useWorkflowStore.getState>['workflows'] extends Map<string, infer T>
    ? T
    : never;

function deriveProgressState(
  runData: RunDataByWorker | undefined,
  liveState: LiveWorkflowState | undefined
): {
  status: string | undefined;
  dagNodes: DagNodeState[];
  currentTool: LiveWorkflowState['currentTool'] | null;
  approval: LiveWorkflowState['approval'] | null;
  error: LiveWorkflowState['error'];
  startedAt: number | undefined;
  completedAt: number | undefined;
  completedCount: number;
  totalNodes: number;
  isRunning: boolean;
  isPaused: boolean;
  finalDuration: number | null;
} {
  const status = liveState?.status ?? runData?.run?.status;
  const dagNodes: DagNodeState[] = liveState?.dagNodes ?? [];
  const startedAt = liveState?.startedAt;
  const completedAt = liveState?.completedAt;
  return {
    status,
    dagNodes,
    currentTool: liveState?.currentTool ?? null,
    approval: liveState?.approval ?? null,
    error: liveState?.error,
    startedAt,
    completedAt,
    completedCount: countCompleted(dagNodes),
    totalNodes: dagNodes.length,
    isRunning: isRunningStatus(status),
    isPaused: status === 'paused',
    finalDuration: completedAt && startedAt ? completedAt - startedAt : null,
  };
}

function isRunningStatus(status: string | undefined): boolean {
  return status === 'running' || status === 'pending';
}

function countCompleted(nodes: DagNodeState[]): number {
  return nodes.filter(node => node.status === 'completed').length;
}

function DurationPill({
  isRunning,
  elapsed,
  finalDuration,
}: {
  isRunning: boolean;
  elapsed: number;
  finalDuration: number | null;
}): React.ReactElement | null {
  const duration = isRunning && elapsed > 0 ? elapsed : finalDuration;
  if (duration === null) return null;
  const cls = isRunning ? 'bg-primary/20 text-primary' : 'bg-surface-elevated text-text-secondary';
  return (
    <span className={`rounded-full px-2 py-0.5 text-[10px] ${cls}`}>
      {formatDurationMs(duration)}
    </span>
  );
}

function LoadingProgressCard({ workflowName }: { workflowName: string }): React.ReactElement {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs max-w-md">
      <Loader2 className="h-3.5 w-3.5 animate-spin text-primary shrink-0" />
      <span className="truncate text-text-primary font-medium">{workflowName}</span>
      <span className="text-text-tertiary">Starting...</span>
    </div>
  );
}

function ErrorProgressCard({
  workflowName,
  onRetry,
}: {
  workflowName: string;
  onRetry: () => void;
}): React.ReactElement {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs max-w-md">
      <span className="text-error text-xs shrink-0">&#x26A0;</span>
      <span className="truncate text-text-primary font-medium">{workflowName}</span>
      <button
        onClick={onRetry}
        className="text-primary hover:text-accent-bright transition-colors shrink-0"
      >
        Retry
      </button>
    </div>
  );
}

function ProgressNodeList({ nodes }: { nodes: DagNodeState[] }): React.ReactElement | null {
  if (nodes.length === 0) return null;
  return (
    <div className="space-y-0.5 px-3 py-2">
      {nodes.map(node => (
        <div key={node.nodeId} className="flex items-center gap-2 text-xs py-0.5">
          <span className="shrink-0">
            <StatusIcon status={node.status} />
          </span>
          <span className="truncate flex-1 text-text-secondary">{node.name}</span>
          {node.duration !== undefined ? (
            <span className="shrink-0 text-[10px] text-text-tertiary">
              {formatDurationMs(node.duration)}
            </span>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function ApprovalBanner({
  workflowName,
  approvalMessage,
  runId,
  approvePending,
  rejectPending,
  approve,
  reject,
  mutationError,
}: {
  workflowName: string;
  approvalMessage: string | undefined;
  runId: string | undefined;
  approvePending: boolean;
  rejectPending: boolean;
  approve: () => void;
  reject: (reason?: string) => void;
  mutationError: unknown;
}): React.ReactElement {
  const disabled = !runId || approvePending || rejectPending;
  return (
    <div className="border-t border-border px-3 py-2 space-y-2">
      <div className="rounded-md bg-warning/5 border border-warning/20 px-3 py-2 flex items-start gap-2">
        <Pause className="h-3.5 w-3.5 text-warning shrink-0 mt-0.5" />
        <p className="text-xs text-text-secondary">{approvalMessage ?? 'Waiting for approval'}</p>
      </div>
      <div className="flex items-center gap-2">
        <button
          onClick={approve}
          disabled={disabled}
          className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-success/80 hover:bg-success/10 hover:text-success transition-colors disabled:opacity-50"
        >
          <CheckCircle className="h-3.5 w-3.5" />
          Approve
        </button>
        <ConfirmRunActionDialog
          trigger={
            <button
              disabled={disabled}
              className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-error/80 hover:bg-error/10 hover:text-error transition-colors disabled:opacity-50"
            >
              <XCircle className="h-3.5 w-3.5" />
              Reject
            </button>
          }
          title="Reject workflow?"
          description={
            <>
              Reject the paused workflow <strong>{workflowName}</strong>. If the approval node
              defines an <code>on_reject</code> prompt, it runs with your reason as{' '}
              <code>$REJECTION_REASON</code>; otherwise the run is cancelled.
            </>
          }
          confirmLabel="Reject"
          reasonInput={{
            label: 'Reason (optional)',
            placeholder: 'Why are you rejecting? Visible to the on_reject prompt.',
          }}
          onConfirm={reject}
        />
      </div>
      {mutationError instanceof Error ? (
        <p className="text-xs text-error">{mutationError.message}</p>
      ) : null}
    </div>
  );
}

export function WorkflowProgressCard({
  workflowName,
  workerConversationId,
}: WorkflowProgressCardProps): React.ReactElement {
  const navigate = useNavigate();

  // REST polling for run data (stops when terminal)
  const {
    data: runData,
    isError,
    refetch,
  } = useQuery({
    queryKey: ['workflowRunByWorker', workerConversationId],
    queryFn: () => getWorkflowRunByWorker(workerConversationId),
    refetchInterval: (query): number | false => {
      const status = query.state.data?.run?.status;
      if (status === 'completed' || status === 'failed' || status === 'cancelled') return false;
      return 3000;
    },
  });

  const runId = runData?.run?.id;

  // Live SSE state from Zustand store
  const liveState = useWorkflowStore(state => (runId ? state.workflows.get(runId) : undefined));
  const progress = deriveProgressState(runData, liveState);
  const {
    status,
    dagNodes,
    currentTool,
    approval,
    error,
    startedAt,
    completedCount,
    totalNodes,
    isRunning,
    isPaused,
    finalDuration,
  } = progress;

  // Expand/collapse state
  const [expanded, setExpanded] = useState(false);
  const userToggled = useRef(false);

  // Auto-expand when running or paused, auto-collapse when terminal (unless user toggled)
  useEffect(() => {
    if (userToggled.current) return;
    if (isRunning || isPaused) {
      setExpanded(true);
    } else if (isTerminalStatus(status)) {
      setExpanded(false);
    }
  }, [isRunning, isPaused, status]);

  // Live elapsed timer
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!isRunning || !startedAt) return;
    setElapsed(Date.now() - startedAt);
    const interval = setInterval(() => {
      setElapsed(Date.now() - startedAt);
    }, 1000);
    return (): void => {
      clearInterval(interval);
    };
  }, [isRunning, startedAt]);

  // Approve/reject mutations
  const approveMutation = useMutation({
    mutationFn: () => approveWorkflowRun(runId ?? ''),
  });
  const rejectMutation = useMutation({
    mutationFn: (reason?: string) => rejectWorkflowRun(runId ?? '', reason),
  });
  const mutationError = approveMutation.error ?? rejectMutation.error;

  const handleHeaderClick = (): void => {
    userToggled.current = true;
    setExpanded(prev => !prev);
  };

  const handleViewFullScreen = (): void => {
    if (runId) {
      navigate(`/legacy/workflows/runs/${runId}`);
    } else {
      navigate(`/legacy/chat/${encodeURIComponent(workerConversationId)}`);
    }
  };

  // Loading state: no run data yet
  if (!runData && !isError) return <LoadingProgressCard workflowName={workflowName} />;

  // Error state: couldn't fetch run
  if (isError && !runData) {
    return <ErrorProgressCard workflowName={workflowName} onRetry={() => void refetch()} />;
  }

  return (
    <div
      className={cn(
        'rounded-lg border border-border bg-surface transition-colors max-w-md overflow-hidden',
        isRunning && 'border-l-2 border-l-primary',
        isPaused && 'border-l-2 border-l-warning'
      )}
    >
      {/* Header bar - always visible, clickable */}
      <button
        onClick={handleHeaderClick}
        className="flex h-9 w-full items-center gap-2 px-3 text-left"
      >
        <ChevronRight
          className={cn(
            'h-3.5 w-3.5 shrink-0 text-text-tertiary transition-transform duration-150',
            expanded && 'rotate-90'
          )}
        />
        <span className="shrink-0">
          <StatusIcon status={status ?? 'pending'} />
        </span>
        <span className="truncate text-xs font-medium text-text-primary">{workflowName}</span>
        {totalNodes > 0 && (
          <span className="shrink-0 text-[10px] text-text-secondary">
            {String(completedCount)}/{String(totalNodes)} nodes
          </span>
        )}
        <span className="ml-auto shrink-0">
          <DurationPill isRunning={isRunning} elapsed={elapsed} finalDuration={finalDuration} />
        </span>
      </button>

      {/* Expanded body */}
      {expanded && (
        <div className="border-t border-border">
          {/* Node list */}
          <ProgressNodeList nodes={dagNodes} />

          {/* Approval request banner */}
          {isPaused && (
            <ApprovalBanner
              workflowName={workflowName}
              approvalMessage={approval?.message}
              runId={runId}
              approvePending={approveMutation.isPending}
              rejectPending={rejectMutation.isPending}
              approve={() => {
                approveMutation.mutate();
              }}
              reject={(reason): void => {
                rejectMutation.mutate(reason);
              }}
              mutationError={mutationError}
            />
          )}

          {/* Current tool activity */}
          {currentTool?.status === 'running' && (
            <div className="flex items-center gap-2 px-3 py-1.5 text-xs border-t border-border">
              <Loader2 className="h-3 w-3 animate-spin text-primary shrink-0" />
              <span className="truncate font-mono text-primary">{currentTool.name}</span>
            </div>
          )}

          {/* Error message */}
          {status === 'failed' && error && (
            <div
              className="px-3 py-1.5 text-xs text-error border-t border-border truncate"
              title={error}
            >
              {error.slice(0, 120)}
            </div>
          )}

          {/* Footer: View Full Screen */}
          <div className="border-t border-border px-3 py-1.5">
            <button
              onClick={handleViewFullScreen}
              className="text-[10px] text-primary hover:text-accent-bright transition-colors"
            >
              View Full Screen &rarr;
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
