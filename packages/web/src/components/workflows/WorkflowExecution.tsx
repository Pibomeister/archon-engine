import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router';
import { MessageSquare } from 'lucide-react';
import { useQuery, useQueryClient } from '@tanstack/react-query';

import { DagNodeProgress } from './DagNodeProgress';
import { StepLogs } from './StepLogs';
import { WorkflowLogs } from './WorkflowLogs';
import { WorkflowDagViewer } from './WorkflowDagViewer';
import { ArtifactSummary } from './ArtifactSummary';
import { ChatInterface } from '@/components/chat/ChatInterface';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from '@/components/ui/resizable';
import { useWorkflowStore } from '@/stores/workflow-store';
import { getWorkflowRun, getWorkflowRunByWorker, getCodebase, getWorkflow } from '@/lib/api';
import { ensureUtc, formatDurationMs } from '@/lib/format';
import { selectInitialNode } from '@/lib/select-initial-node';
import type {
  WorkflowState,
  ArtifactType,
  WorkflowRunStatus,
  DagNodeState,
  WorkflowStepStatus,
  LoopIterationInfo,
} from '@/lib/types';

import type { WorkflowEventResponse } from '@/lib/api';

/** Tool call event extracted from workflow_events for display in WorkflowLogs. */
export interface ToolEvent {
  id: string;
  name: string;
  input: Record<string, unknown>;
  stepName?: string;
  stepIndex?: number;
  createdAt: string;
  duration?: number;
}

const TERMINAL_STATUSES: readonly WorkflowRunStatus[] = ['completed', 'failed', 'cancelled'];

function isTerminal(status: WorkflowRunStatus): boolean {
  return TERMINAL_STATUSES.includes(status);
}

interface WorkflowRunQueryData {
  workflowState: WorkflowState;
  workerPlatformId: string | null;
  parentPlatformId: string | null;
  conversationPlatformId: string | null;
  workingPath: string | null;
  codebaseId: string | null;
  events: WorkflowEventResponse[];
}

interface WorkflowExecutionProps {
  runId: string;
}

function StatusBadge({ status }: { status: string }): React.ReactElement {
  const colors: Record<string, string> = {
    pending: 'bg-accent/20 text-accent',
    running: 'bg-accent/20 text-accent',
    completed: 'bg-success/20 text-success',
    failed: 'bg-error/20 text-error',
    cancelled: 'bg-surface text-text-secondary',
  };
  return (
    <span
      className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[status] ?? 'bg-surface text-text-secondary'}`}
    >
      {status}
    </span>
  );
}

type WorkflowRunDetail = Awaited<ReturnType<typeof getWorkflowRun>>;

function eventNodeId(event: WorkflowEventResponse): string {
  return event.step_name ?? (event.data.nodeId as string | undefined) ?? '';
}
function nodeStatusForEvent(eventType: string): WorkflowStepStatus {
  if (eventType === 'node_started') return 'running';
  if (eventType === 'node_completed') return 'completed';
  if (eventType === 'node_failed') return 'failed';
  return 'skipped';
}
function applyNodeTransition(
  nodeMap: Map<string, DagNodeState>,
  event: WorkflowEventResponse
): void {
  const nodeId = eventNodeId(event);
  if (!nodeId) return;
  const status = nodeStatusForEvent(event.event_type);
  const existing = nodeMap.get(nodeId);
  if (existing && status === 'running') return;
  nodeMap.set(nodeId, {
    nodeId,
    name: nodeId,
    status,
    duration: event.data.duration_ms as number | undefined,
    error: event.data.error as string | undefined,
    reason: event.data.reason as 'when_condition' | 'trigger_rule' | undefined,
  });
}
function loopStatusForEvent(eventType: string): LoopIterationInfo['status'] {
  if (eventType === 'loop_iteration_started') return 'running';
  if (eventType === 'loop_iteration_completed') return 'completed';
  return 'failed';
}
function applyLoopIteration(
  nodeMap: Map<string, DagNodeState>,
  event: WorkflowEventResponse
): void {
  const nodeId = event.step_name ?? '';
  const existing = nodeMap.get(nodeId);
  const iteration = event.data.iteration as number | undefined;
  if (!nodeId || !existing || iteration === undefined) return;
  const iterState: LoopIterationInfo = {
    iteration,
    status: loopStatusForEvent(event.event_type),
    duration: event.data.duration_ms as number | undefined,
  };
  const iterations = [...(existing.iterations ?? [])];
  const iterIdx = iterations.findIndex(it => it.iteration === iteration);
  if (iterIdx >= 0) iterations[iterIdx] = iterState;
  else iterations.push(iterState);
  nodeMap.set(nodeId, {
    ...existing,
    currentIteration: iteration,
    maxIterations: (event.data.maxIterations as number | undefined) ?? existing.maxIterations,
    iterations,
  });
}
function dagNodesFromEvents(events: WorkflowEventResponse[]): DagNodeState[] {
  const nodeMap = new Map<string, DagNodeState>();
  for (const event of events)
    if (event.event_type.startsWith('node_')) applyNodeTransition(nodeMap, event);
  for (const event of events)
    if (event.event_type.startsWith('loop_iteration_')) applyLoopIteration(nodeMap, event);
  return Array.from(nodeMap.values());
}
function artifactsFromEvents(events: WorkflowEventResponse[]): WorkflowState['artifacts'] {
  return events
    .filter(event => event.event_type === 'workflow_artifact')
    .map(event => ({
      type: (event.data.artifactType as ArtifactType) ?? 'commit',
      label: (event.data.label as string) ?? '',
      url: event.data.url as string | undefined,
      path: event.data.path as string | undefined,
    }))
    .filter(artifact => artifact.label || artifact.url || artifact.path);
}
function toWorkflowRunQueryData(data: WorkflowRunDetail): WorkflowRunQueryData {
  return {
    workflowState: {
      runId: data.run.id,
      workflowName: data.run.workflow_name,
      status: data.run.status,
      dagNodes: dagNodesFromEvents(data.events),
      artifacts: artifactsFromEvents(data.events),
      startedAt: new Date(ensureUtc(data.run.started_at)).getTime(),
      completedAt: data.run.completed_at
        ? new Date(ensureUtc(data.run.completed_at)).getTime()
        : undefined,
    },
    workerPlatformId: data.run.worker_platform_id ?? null,
    parentPlatformId: data.run.parent_platform_id ?? null,
    conversationPlatformId: data.run.conversation_platform_id ?? null,
    workingPath: data.run.working_path ?? null,
    codebaseId: data.run.codebase_id ?? null,
    events: data.events,
  };
}
function matchingCompletedTool(
  completedEvents: WorkflowEventResponse[],
  usedCompleted: Set<string>,
  event: WorkflowEventResponse
): WorkflowEventResponse | undefined {
  const eventTime = new Date(event.created_at).getTime();
  const toolName = event.data.tool_name as string;
  const stepName = event.step_name ?? undefined;
  return completedEvents.find(
    candidate =>
      !usedCompleted.has(candidate.id) &&
      (candidate.data.tool_name as string) === toolName &&
      new Date(candidate.created_at).getTime() >= eventTime &&
      (candidate.step_name ?? undefined) === stepName
  );
}
function toolEventsFromEvents(events: WorkflowEventResponse[]): ToolEvent[] {
  const completedEvents = events
    .filter(event => event.event_type === 'tool_completed')
    .sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());
  const usedCompleted = new Set<string>();
  return events
    .filter(event => event.event_type === 'tool_called')
    .sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime())
    .map(event => {
      const completed = matchingCompletedTool(completedEvents, usedCompleted, event);
      if (completed) usedCompleted.add(completed.id);
      return {
        id: event.id,
        name: event.data.tool_name as string,
        input: (event.data.tool_input as Record<string, unknown>) ?? {},
        stepName: event.step_name ?? undefined,
        stepIndex: event.step_index ?? undefined,
        createdAt: event.created_at,
        duration: completed ? (completed.data.duration_ms as number | undefined) : undefined,
      };
    });
}
function mergeWorkflowState(
  runId: string,
  initialData: WorkflowState | null,
  liveWorkflow: WorkflowState | undefined
): WorkflowState | null {
  if (!liveWorkflow) return initialData;
  if (!initialData) return liveWorkflow;
  if (isTerminal(initialData.status) && !isTerminal(liveWorkflow.status)) {
    console.warn('[WorkflowExecution] REST overrides stale SSE status', {
      runId,
      restStatus: initialData.status,
      sseStatus: liveWorkflow.status,
    });
    return initialData;
  }
  return {
    ...initialData,
    status: liveWorkflow.status,
    completedAt: liveWorkflow.completedAt ?? initialData.completedAt,
    error: liveWorkflow.error ?? initialData.error,
    dagNodes: liveWorkflow.dagNodes.length > 0 ? liveWorkflow.dagNodes : initialData.dagNodes,
    artifacts: liveWorkflow.artifacts.length > 0 ? liveWorkflow.artifacts : initialData.artifacts,
    currentIteration: liveWorkflow.currentIteration ?? initialData.currentIteration,
    maxIterations: liveWorkflow.maxIterations ?? initialData.maxIterations,
  };
}
function currentlyExecutingFromEvents(
  events: WorkflowEventResponse[] | undefined,
  workflow: WorkflowState | null
): { nodeName: string; startedAt: number } | null {
  if (!events || workflow?.status !== 'running') return null;
  const startedNodes = new Set<string>();
  const completedNodes = new Set<string>();
  for (const event of events) {
    const nodeId = event.step_name ?? '';
    if (event.event_type === 'node_started') startedNodes.add(nodeId);
    if (['node_completed', 'node_failed', 'node_skipped'].includes(event.event_type))
      completedNodes.add(nodeId);
  }
  for (const nodeId of startedNodes) {
    if (completedNodes.has(nodeId)) continue;
    const startEvent = events.find(
      event => event.event_type === 'node_started' && event.step_name === nodeId
    );
    if (startEvent)
      return { nodeName: nodeId, startedAt: new Date(ensureUtc(startEvent.created_at)).getTime() };
  }
  return null;
}
function formatStepLogLine(event: WorkflowEventResponse): string {
  const ts = new Date(ensureUtc(event.created_at)).toLocaleTimeString();
  switch (event.event_type) {
    case 'loop_iteration_started':
      return `[${ts}] Iteration ${String(event.data.iteration)}/${String((event.data.maxIterations as number | undefined) ?? '?')} started`;
    case 'loop_iteration_completed': {
      const dur = event.data.duration_ms as number | undefined;
      const durStr = dur !== undefined ? ` (${String(Math.round(dur / 100) / 10)}s)` : '';
      return `[${ts}] Iteration ${String(event.data.iteration)} completed${durStr}`;
    }
    case 'loop_iteration_failed':
      return `[${ts}] Iteration ${String(event.data.iteration)} failed: ${(event.data.error as string | undefined) ?? 'Unknown error'}`;
    case 'node_started':
      return `[${ts}] Node started: ${event.step_name ?? 'node'}`;
    case 'node_completed':
      return `[${ts}] Node completed: ${event.step_name ?? 'node'}`;
    case 'node_failed':
      return `[${ts}] Node failed: ${event.step_name ?? 'node'}: ${(event.data.error as string | undefined) ?? 'Unknown error'}`;
    case 'node_skipped':
      return `[${ts}] Node skipped: ${event.step_name ?? 'node'}`;
    default:
      return `[${ts}] ${event.event_type}${event.step_name ? `: ${event.step_name}` : ''}`;
  }
}
function stepLogLinesForNode(
  events: WorkflowEventResponse[] | undefined,
  selectedDagNode: string | null
): string[] {
  return selectedDagNode !== null
    ? (events ?? []).filter(event => event.step_name === selectedDagNode).map(formatStepLogLine)
    : [];
}
function nodeStartTimesFromEvents(
  events: WorkflowEventResponse[] | undefined
): Map<string, number> {
  const map = new Map<string, number>();
  for (const event of events ?? [])
    if (event.event_type === 'node_started' && event.step_name)
      map.set(event.step_name, new Date(ensureUtc(event.created_at)).getTime());
  return map;
}
function terminalCompletedAt(
  initialData: WorkflowState | null,
  workflow: WorkflowState,
  startedAt: number
): number {
  if (initialData && isTerminal(initialData.status) && initialData.completedAt)
    return initialData.completedAt;
  return workflow.completedAt ?? (startedAt ? Date.now() : 0);
}

function WorkflowLogsPanel({
  logsPlatformId,
  selectedStepHasEvents,
  isRunning,
  conversationStartedAt,
  currentlyExecuting,
  toolEvents,
  scrollToNodeTimestamp,
  nodeScrollTrigger,
  runId,
  stepLogLines,
  artifacts,
}: {
  logsPlatformId: string | null;
  selectedStepHasEvents: boolean;
  isRunning: boolean;
  conversationStartedAt: number | undefined;
  currentlyExecuting: { nodeName: string; startedAt: number } | null;
  toolEvents: ToolEvent[];
  scrollToNodeTimestamp: number | null;
  nodeScrollTrigger: number;
  runId: string;
  stepLogLines: string[];
  artifacts: WorkflowState['artifacts'];
}): React.ReactElement {
  const content =
    logsPlatformId && !selectedStepHasEvents && !isRunning ? (
      <div className="flex-1 flex items-center justify-center text-text-secondary text-sm">
        No output available for this step.
      </div>
    ) : logsPlatformId ? (
      <WorkflowLogs
        conversationId={logsPlatformId}
        startedAt={conversationStartedAt}
        isRunning={isRunning}
        currentlyExecuting={currentlyExecuting}
        toolEvents={toolEvents}
        scrollToNodeTimestamp={scrollToNodeTimestamp}
        nodeScrollTrigger={nodeScrollTrigger}
      />
    ) : (
      <StepLogs runId={runId} lines={stepLogLines} />
    );
  return (
    <div className="flex-1 flex flex-col overflow-hidden min-h-0 h-full">
      <div className="flex-1 flex flex-col overflow-hidden min-h-0">{content}</div>
      {!isRunning && artifacts.length > 0 ? (
        <div className="border-t border-border p-3">
          <ArtifactSummary artifacts={artifacts} runId={runId} />
        </div>
      ) : null}
    </div>
  );
}

type DagDefinitionNodes =
  | NonNullable<Awaited<ReturnType<typeof getWorkflow>>['workflow']>['nodes']
  | null;

function WorkflowGraphPanel({
  dagDefinitionNodes,
  dagDefinitionErrorMessage,
  workflowDefPending,
  workflow,
  isRunning,
  currentlyExecuting,
  selectedDagNode,
  onNodeClick,
  onRetry,
}: {
  dagDefinitionNodes: DagDefinitionNodes;
  dagDefinitionErrorMessage: string | null;
  workflowDefPending: boolean;
  workflow: WorkflowState;
  isRunning: boolean;
  currentlyExecuting: { nodeName: string; startedAt: number } | null;
  selectedDagNode: string | null;
  onNodeClick: (nodeId: string) => void;
  onRetry: () => void;
}): React.ReactElement {
  if (dagDefinitionNodes)
    return (
      <WorkflowDagViewer
        dagNodes={dagDefinitionNodes}
        liveStatus={workflow.dagNodes}
        isRunning={isRunning}
        currentlyExecuting={currentlyExecuting ?? undefined}
        selectedNodeId={selectedDagNode}
        onNodeClick={onNodeClick}
      />
    );
  if (dagDefinitionErrorMessage)
    return (
      <div className="flex flex-col items-center justify-center h-full text-text-secondary px-4 text-center">
        <p className="text-error mb-1">Failed to load workflow graph</p>
        <p className="text-xs mb-3">{dagDefinitionErrorMessage}</p>
        <button
          type="button"
          onClick={onRetry}
          className="text-xs text-primary hover:text-accent-bright transition-colors"
        >
          Retry
        </button>
      </div>
    );
  if (workflowDefPending)
    return (
      <div className="flex items-center justify-center h-full text-text-secondary">
        <span className="inline-block h-5 w-5 animate-spin rounded-full border-2 border-accent border-t-transparent mr-2" />
        Loading graph...
      </div>
    );
  return (
    <div className="flex items-center justify-center h-full text-text-secondary px-4 text-center">
      <p>Workflow graph unavailable for this run.</p>
    </div>
  );
}

function WorkflowExecutionBody({
  isDag,
  activeView,
  parentPlatformId,
  workingPath,
  dagDefinitionNodes,
  dagDefinitionErrorMessage,
  workflowDefPending,
  workflow,
  isRunning,
  currentlyExecuting,
  selectedDagNode,
  onNodeClick,
  onRetryGraph,
  logsPanel,
}: {
  isDag: boolean;
  activeView: 'graph' | 'logs' | 'chat';
  parentPlatformId: string | null;
  workingPath: string | null;
  dagDefinitionNodes: DagDefinitionNodes;
  dagDefinitionErrorMessage: string | null;
  workflowDefPending: boolean;
  workflow: WorkflowState;
  isRunning: boolean;
  currentlyExecuting: { nodeName: string; startedAt: number } | null;
  selectedDagNode: string | null;
  onNodeClick: (nodeId: string) => void;
  onRetryGraph: () => void;
  logsPanel: React.ReactElement;
}): React.ReactElement {
  if (isDag && activeView === 'graph')
    return (
      <ResizablePanelGroup orientation="horizontal" className="flex-1 min-h-0">
        <ResizablePanel defaultSize={60} minSize={30}>
          <WorkflowGraphPanel
            dagDefinitionNodes={dagDefinitionNodes}
            dagDefinitionErrorMessage={dagDefinitionErrorMessage}
            workflowDefPending={workflowDefPending}
            workflow={workflow}
            isRunning={isRunning}
            currentlyExecuting={currentlyExecuting}
            selectedDagNode={selectedDagNode}
            onNodeClick={onNodeClick}
            onRetry={onRetryGraph}
          />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel defaultSize={40} minSize={20}>
          {logsPanel}
        </ResizablePanel>
      </ResizablePanelGroup>
    );
  if (isDag && activeView === 'chat' && parentPlatformId)
    return (
      <div className="flex flex-col flex-1 overflow-hidden min-h-0">
        <ChatInterface conversationId={parentPlatformId} cwdOverride={workingPath} />
      </div>
    );
  return (
    <div className="flex flex-1 overflow-hidden min-h-0">
      <div className="w-64 border-r border-border overflow-auto">
        <DagNodeProgress
          nodes={workflow.dagNodes}
          activeNodeId={selectedDagNode}
          onNodeClick={onNodeClick}
        />
      </div>
      {logsPanel}
    </div>
  );
}

function WorkflowExecutionHeader({
  workflow,
  codebaseName,
  workerRunId,
  elapsed,
  onBack,
  onRunDetails,
}: {
  workflow: WorkflowState;
  codebaseName: string | null;
  workerRunId: string | null;
  elapsed: number;
  onBack: () => void;
  onRunDetails: () => void;
}): React.ReactElement {
  return (
    <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
      <button
        onClick={onBack}
        className="text-text-secondary hover:text-text-primary transition-colors text-sm"
        title="Back"
      >
        &larr;
      </button>
      <div className="flex items-center gap-2 min-w-0">
        <h2 className="font-semibold text-text-primary truncate">{workflow.workflowName}</h2>
        <StatusBadge status={workflow.status} />
      </div>
      <div className="flex items-center gap-2 ml-auto shrink-0">
        {codebaseName ? <span className="text-xs text-text-secondary">{codebaseName}</span> : null}
        {workerRunId ? (
          <button
            onClick={onRunDetails}
            className="flex items-center gap-1 text-xs text-primary hover:text-accent-bright transition-colors"
            title="View workflow run details"
          >
            <span>Run Details</span>
          </button>
        ) : null}
        <span className="text-xs text-text-secondary">{formatDurationMs(elapsed)}</span>
      </div>
    </div>
  );
}

function WorkflowExecutionTabs({
  isDag,
  activeView,
  parentPlatformId,
  onActiveViewChange,
}: {
  isDag: boolean;
  activeView: 'graph' | 'logs' | 'chat';
  parentPlatformId: string | null;
  onActiveViewChange: (view: 'graph' | 'logs' | 'chat') => void;
}): React.ReactElement | null {
  if (!isDag) return null;
  return (
    <div className="flex items-center px-4 py-1.5 border-b border-border">
      <Tabs
        value={activeView}
        onValueChange={value => {
          onActiveViewChange(value as 'graph' | 'logs' | 'chat');
        }}
      >
        <TabsList>
          <TabsTrigger value="graph">Graph</TabsTrigger>
          <TabsTrigger value="logs">Logs</TabsTrigger>
          {parentPlatformId ? (
            <TabsTrigger value="chat">
              <MessageSquare className="h-3 w-3 mr-1" />
              Chat
            </TabsTrigger>
          ) : null}
        </TabsList>
      </Tabs>
    </div>
  );
}

function queryErrorMessage(error: unknown): string | null {
  if (error === null || error === undefined) return null;
  return error instanceof Error ? error.message : JSON.stringify(error);
}
function useCodebaseDetails(
  codebaseId: string | null,
  runId: string
): { codebaseName: string | null; codebaseCwd: string | null } {
  const [codebaseName, setCodebaseName] = useState<string | null>(null);
  const [codebaseCwd, setCodebaseCwd] = useState<string | null>(null);
  const fetchedCodebaseIdRef = useRef<string | null>(null);
  useEffect(() => {
    fetchedCodebaseIdRef.current = null;
    setCodebaseName(null);
    setCodebaseCwd(null);
  }, [runId]);
  useEffect(() => {
    if (!codebaseId || fetchedCodebaseIdRef.current === codebaseId) return;
    fetchedCodebaseIdRef.current = codebaseId;
    void getCodebase(codebaseId)
      .then(cb => {
        setCodebaseName(cb.name);
        setCodebaseCwd(cb.default_cwd);
      })
      .catch((err: unknown) => {
        console.warn('[WorkflowExecution] Failed to load codebase name', {
          codebaseId,
          error: err instanceof Error ? err.message : err,
        });
      });
  }, [codebaseId]);
  return { codebaseName, codebaseCwd };
}
function useWorkerRunId(workerPlatformId: string | null, runId: string): string | null {
  const [workerRunId, setWorkerRunId] = useState<string | null>(null);
  useEffect(() => {
    setWorkerRunId(null);
  }, [runId]);
  useEffect(() => {
    if (!workerPlatformId) return;
    getWorkflowRunByWorker(workerPlatformId)
      .then(result => {
        if (result) setWorkerRunId(result.run.id);
      })
      .catch((err: unknown) => {
        console.warn('[WorkflowExecution] Failed to look up worker run', {
          workerPlatformId,
          error: err instanceof Error ? err.message : err,
        });
      });
  }, [workerPlatformId]);
  return workerRunId;
}
function useTerminalInvalidation(
  runId: string,
  liveStatus: WorkflowRunStatus | undefined,
  initialData: WorkflowState | null,
  queryClient: ReturnType<typeof useQueryClient>
): void {
  useEffect(() => {
    if (!liveStatus || !isTerminal(liveStatus)) return;
    if (initialData && isTerminal(initialData.status)) return;
    void queryClient.invalidateQueries({ queryKey: ['workflowRun', runId] });
  }, [runId, liveStatus, initialData, queryClient]);
}
function useAutoSelectedDagNode(
  workflow: WorkflowState | null,
  runId: string
): [string | null, (nodeId: string) => void, number] {
  const [selectedDagNode, setSelectedDagNode] = useState<string | null>(null);
  const [nodeScrollTrigger, setNodeScrollTrigger] = useState(0);
  useEffect(() => {
    setSelectedDagNode(null);
    setNodeScrollTrigger(0);
  }, [runId]);
  useEffect(() => {
    if (selectedDagNode !== null) return;
    const nodeId = selectInitialNode(workflow?.dagNodes);
    if (nodeId) setSelectedDagNode(nodeId);
  }, [selectedDagNode, workflow?.dagNodes]);
  const handleNodeClick = useCallback((nodeId: string): void => {
    setSelectedDagNode(nodeId);
    setNodeScrollTrigger(prev => prev + 1);
  }, []);
  return [selectedDagNode, handleNodeClick, nodeScrollTrigger];
}
function useRunningTick(status: WorkflowRunStatus | undefined): void {
  const [, setTick] = useState(0);
  useEffect(() => {
    if (status !== 'running' && status !== 'pending') return;
    const interval = setInterval((): void => {
      setTick(t => t + 1);
    }, 1000);
    return (): void => {
      clearInterval(interval);
    };
  }, [status]);
}

function WorkflowExecutionReady({
  runId,
  workflow,
  initialData,
  queryData,
  queryClient,
  navigate,
  codebaseName,
  codebaseCwd,
  workerRunId,
  parentPlatformId,
  conversationPlatformId,
  workingPath,
  activeView,
  setActiveView,
  dagDefinitionNodes,
  dagDefinitionErrorMessage,
  workflowDefPending,
  isDag,
  toolEvents,
}: {
  runId: string;
  workflow: WorkflowState;
  initialData: WorkflowState | null;
  queryData: WorkflowRunQueryData | undefined;
  queryClient: ReturnType<typeof useQueryClient>;
  navigate: ReturnType<typeof useNavigate>;
  codebaseName: string | null;
  codebaseCwd: string | null;
  workerRunId: string | null;
  parentPlatformId: string | null;
  conversationPlatformId: string | null;
  workingPath: string | null;
  activeView: 'graph' | 'logs' | 'chat';
  setActiveView: (view: 'graph' | 'logs' | 'chat') => void;
  dagDefinitionNodes: DagDefinitionNodes;
  dagDefinitionErrorMessage: string | null;
  workflowDefPending: boolean;
  isDag: boolean;
  toolEvents: ToolEvent[];
}): React.ReactElement {
  const [selectedDagNode, handleNodeClick, nodeScrollTrigger] = useAutoSelectedDagNode(
    workflow,
    runId
  );
  useRunningTick(workflow.status);
  const currentlyExecuting = useMemo(
    () => currentlyExecutingFromEvents(queryData?.events, workflow),
    [queryData?.events, workflow]
  );
  const stepLogLines = useMemo(
    () => stepLogLinesForNode(queryData?.events, selectedDagNode),
    [queryData?.events, selectedDagNode]
  );
  const selectedStepHasEvents = useMemo(
    () =>
      selectedDagNode !== null &&
      (queryData?.events ?? []).some(event => event.step_name === selectedDagNode),
    [queryData?.events, selectedDagNode]
  );
  const nodeStartTimes = useMemo(
    () => nodeStartTimesFromEvents(queryData?.events),
    [queryData?.events]
  );
  const startedAt = initialData?.startedAt ?? 0;
  const elapsed = startedAt
    ? Math.max(0, terminalCompletedAt(initialData, workflow, startedAt) - startedAt)
    : 0;
  const isRunning = workflow.status === 'running' || workflow.status === 'pending';
  const logsPlatformId = queryData?.workerPlatformId ?? conversationPlatformId;
  const scrollToNodeTimestamp = selectedDagNode
    ? (nodeStartTimes.get(selectedDagNode) ?? null)
    : null;
  const logsPanel = (
    <WorkflowLogsPanel
      logsPlatformId={logsPlatformId}
      selectedStepHasEvents={selectedStepHasEvents}
      isRunning={isRunning}
      conversationStartedAt={initialData?.startedAt}
      currentlyExecuting={currentlyExecuting}
      toolEvents={toolEvents}
      scrollToNodeTimestamp={scrollToNodeTimestamp}
      nodeScrollTrigger={nodeScrollTrigger}
      runId={runId}
      stepLogLines={stepLogLines}
      artifacts={workflow.artifacts}
    />
  );
  const retryGraph = (): void => {
    queryClient
      .resetQueries({ queryKey: ['workflowDefinition', initialData?.workflowName, codebaseCwd] })
      .catch((err: unknown) => {
        console.error('[WorkflowExecution] Retry resetQueries failed', {
          workflowName: initialData?.workflowName,
          error: err instanceof Error ? err.message : err,
        });
      });
  };
  return (
    <div className="flex flex-col h-full min-h-0 overflow-hidden">
      <WorkflowExecutionHeader
        workflow={workflow}
        codebaseName={codebaseName}
        workerRunId={workerRunId}
        elapsed={elapsed}
        onBack={() => {
          if (window.history.length > 1) navigate(-1);
          else navigate('/legacy/workflows');
        }}
        onRunDetails={() => {
          if (workerRunId) navigate(`/legacy/workflows/runs/${workerRunId}`);
        }}
      />
      <WorkflowExecutionTabs
        isDag={isDag}
        activeView={activeView}
        parentPlatformId={parentPlatformId}
        onActiveViewChange={setActiveView}
      />
      <WorkflowExecutionBody
        isDag={isDag}
        activeView={activeView}
        parentPlatformId={parentPlatformId}
        workingPath={workingPath}
        dagDefinitionNodes={dagDefinitionNodes}
        dagDefinitionErrorMessage={dagDefinitionErrorMessage}
        workflowDefPending={workflowDefPending}
        workflow={workflow}
        isRunning={isRunning}
        currentlyExecuting={currentlyExecuting}
        selectedDagNode={selectedDagNode}
        onNodeClick={handleNodeClick}
        onRetryGraph={retryGraph}
        logsPanel={logsPanel}
      />
    </div>
  );
}

function useWorkflowRunQueryData(runId: string): {
  data: WorkflowRunQueryData | undefined;
  error: unknown;
} {
  return useQuery({
    queryKey: ['workflowRun', runId],
    queryFn: async (): Promise<WorkflowRunQueryData> =>
      toWorkflowRunQueryData(await getWorkflowRun(runId)),
    refetchInterval: query => {
      const status = query.state.data?.workflowState.status;
      return status && isTerminal(status) ? false : 3000;
    },
    staleTime: 0,
  });
}

function useWorkflowDefinitionQuery(
  initialData: WorkflowState | null,
  codebaseCwd: string | null
): {
  workflowDef: Awaited<ReturnType<typeof getWorkflow>> | undefined;
  workflowDefError: unknown;
  workflowDefPending: boolean;
} {
  const query = useQuery({
    queryKey: ['workflowDefinition', initialData?.workflowName, codebaseCwd],
    queryFn: () => getWorkflow(initialData?.workflowName ?? '', codebaseCwd ?? undefined),
    enabled: !!initialData?.workflowName,
    staleTime: Infinity,
  });
  return {
    workflowDef: query.data,
    workflowDefError: query.error,
    workflowDefPending: query.isPending,
  };
}

function hasDagDefinition(
  dagDefinitionNodes: DagDefinitionNodes,
  initialData: WorkflowState | null
): boolean {
  return dagDefinitionNodes !== null || (initialData?.dagNodes.length ?? 0) > 0;
}

export function WorkflowExecution({ runId }: WorkflowExecutionProps): React.ReactElement {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const liveWorkflow = useWorkflowStore(state => state.workflows.get(runId));
  const [activeView, setActiveView] = useState<'graph' | 'logs' | 'chat'>('graph');
  useEffect(() => {
    setActiveView('graph');
  }, [runId]);
  const { data: queryData, error: queryError } = useWorkflowRunQueryData(runId);
  const initialData = queryData?.workflowState ?? null;
  const error = queryErrorMessage(queryError);
  const toolEvents = useMemo(
    () => toolEventsFromEvents(queryData?.events ?? []),
    [queryData?.events]
  );
  const { codebaseName, codebaseCwd } = useCodebaseDetails(queryData?.codebaseId ?? null, runId);
  const { workflowDef, workflowDefError, workflowDefPending } = useWorkflowDefinitionQuery(
    initialData,
    codebaseCwd
  );
  const dagDefinitionNodes = workflowDef?.workflow?.nodes ?? null;
  const dagDefinitionErrorMessage = queryErrorMessage(workflowDefError);
  const workflow = mergeWorkflowState(runId, initialData, liveWorkflow);
  useTerminalInvalidation(runId, liveWorkflow?.status, initialData, queryClient);
  const workerRunId = useWorkerRunId(queryData?.workerPlatformId ?? null, runId);
  if (error)
    return (
      <div className="flex items-center justify-center h-full text-error">
        <p>Failed to load workflow run: {error}</p>
      </div>
    );
  if (!workflow)
    return (
      <div className="flex items-center justify-center h-full text-text-secondary">
        <p>Loading workflow execution...</p>
      </div>
    );
  return (
    <WorkflowExecutionReady
      runId={runId}
      workflow={workflow}
      initialData={initialData}
      queryData={queryData}
      queryClient={queryClient}
      navigate={navigate}
      codebaseName={codebaseName}
      codebaseCwd={codebaseCwd}
      workerRunId={workerRunId}
      parentPlatformId={queryData?.parentPlatformId ?? null}
      conversationPlatformId={queryData?.conversationPlatformId ?? null}
      workingPath={queryData?.workingPath ?? null}
      activeView={activeView}
      setActiveView={setActiveView}
      dagDefinitionNodes={dagDefinitionNodes}
      dagDefinitionErrorMessage={dagDefinitionErrorMessage}
      workflowDefPending={workflowDefPending}
      isDag={hasDagDefinition(dagDefinitionNodes, initialData)}
      toolEvents={toolEvents}
    />
  );
}
