import { useState, useMemo, useCallback, useEffect, useRef } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useSearchParams } from 'react-router';
import { Workflow } from 'lucide-react';
import {
  listDashboardRuns,
  cancelWorkflowRun,
  resumeWorkflowRun,
  abandonWorkflowRun,
  deleteWorkflowRun,
  approveWorkflowRun,
  rejectWorkflowRun,
  listCodebases,
  getHealth,
  type DashboardCounts,
  type DashboardRunResponse,
} from '@/lib/api';
import type { WorkflowRunStatus } from '@/lib/types';
import { ensureUtc } from '@/lib/format';
import { StatusSummaryBar } from '@/components/dashboard/StatusSummaryBar';
import { WorkflowRunGroup } from '@/components/dashboard/WorkflowRunGroup';
import { WorkflowRunCard } from '@/components/dashboard/WorkflowRunCard';
import { WorkflowHistoryTable } from '@/components/dashboard/WorkflowHistoryTable';
import { useDashboardSSE } from '@/hooks/useDashboardSSE';
import { useWorkflowStore } from '@/stores/workflow-store';

const DEFAULT_PAGE_SIZE = 10;
const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;
type PageSize = (typeof PAGE_SIZE_OPTIONS)[number];

/** Date range presets. "all" means no date filter. */
type DateRange = 'today' | '7d' | '30d' | 'all';

function parseDateRange(value: string | null): DateRange {
  return value === 'today' || value === '7d' || value === '30d' || value === 'all' ? value : 'all';
}

function parsePageSize(value: string | null): PageSize {
  const numeric = Number(value ?? '0');
  return PAGE_SIZE_OPTIONS.includes(numeric as PageSize)
    ? (numeric as PageSize)
    : DEFAULT_PAGE_SIZE;
}

function getDateBounds(range: DateRange): { after?: string; before?: string } {
  if (range === 'all') return {};
  const now = new Date();
  const start = new Date(now);
  if (range === 'today') {
    start.setHours(0, 0, 0, 0);
  } else if (range === '7d') {
    start.setDate(start.getDate() - 7);
  } else if (range === '30d') {
    start.setDate(start.getDate() - 30);
  }
  return { after: start.toISOString() };
}

interface ActiveRunGroups {
  multiRunGroups: { parentPlatformId: string | null; runs: DashboardRunResponse[] }[];
  singletonRuns: DashboardRunResponse[];
}

function activeStatuses(run: DashboardRunResponse): boolean {
  return run.status === 'running' || run.status === 'pending' || run.status === 'paused';
}

function historyStatuses(run: DashboardRunResponse): boolean {
  return run.status === 'completed' || run.status === 'failed' || run.status === 'cancelled';
}

function groupActiveRuns(activeRuns: DashboardRunResponse[]): ActiveRunGroups {
  const groups = new Map<
    string,
    { parentPlatformId: string | null; runs: DashboardRunResponse[] }
  >();
  for (const run of activeRuns) {
    const key = run.parent_platform_id ?? '__standalone__';
    const group = groups.get(key) ?? { parentPlatformId: run.parent_platform_id, runs: [] };
    group.runs.push(run);
    groups.set(key, group);
  }
  const multiRunGroups: ActiveRunGroups['multiRunGroups'] = [];
  const singletonRuns: DashboardRunResponse[] = [];
  for (const group of groups.values()) {
    if (group.runs.length > 1) multiRunGroups.push(group);
    else if (group.runs[0] !== undefined) singletonRuns.push(group.runs[0]);
  }
  return { multiRunGroups, singletonRuns };
}

interface DashboardActionHandlers {
  onCancel: (runId: string) => Promise<void>;
  onResume: (runId: string) => Promise<void>;
  onAbandon: (runId: string) => Promise<void>;
  onDelete: (runId: string) => Promise<void>;
  onApprove: (runId: string) => Promise<void>;
  onReject: (runId: string, reason?: string) => Promise<void>;
}

function ActiveRunsSection({
  groups,
  health,
  actions,
}: {
  groups: ActiveRunGroups;
  health: Awaited<ReturnType<typeof getHealth>> | undefined;
  actions: DashboardActionHandlers;
}): React.ReactElement | null {
  if (groups.singletonRuns.length === 0 && groups.multiRunGroups.length === 0) return null;
  return (
    <section>
      <h2 className="mb-3 text-sm font-semibold text-text-secondary">Active Workflows</h2>
      <div className="space-y-6">
        {groups.singletonRuns.length > 0 && (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {groups.singletonRuns.map(run => (
              <WorkflowRunCard
                key={run.id}
                run={run}
                isDocker={health?.is_docker}
                isWsl={health?.is_wsl}
                wslDistro={health?.wsl_distro}
                {...actions}
              />
            ))}
          </div>
        )}
        {groups.multiRunGroups.map(group => (
          <WorkflowRunGroup
            key={group.parentPlatformId ?? 'standalone'}
            parentPlatformId={group.parentPlatformId}
            runs={group.runs}
            isDocker={health?.is_docker}
            isWsl={health?.is_wsl}
            wslDistro={health?.wsl_distro}
            {...actions}
          />
        ))}
      </div>
    </section>
  );
}

function EmptyDashboardRuns(): React.ReactElement {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-16">
      <Workflow className="h-10 w-10 text-text-tertiary" />
      <p className="text-sm text-text-tertiary">No workflow runs found</p>
    </div>
  );
}

function PaginationControls({
  page,
  pageSize,
  total,
  totalPages,
  hasMore,
  setPage,
  setPageSize,
}: {
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
  hasMore: boolean;
  setPage: (page: number) => void;
  setPageSize: (size: number) => void;
}): React.ReactElement {
  return (
    <div className="flex items-center justify-between pt-2">
      <div className="flex items-center gap-3">
        <span className="text-xs text-text-tertiary">
          Showing {String(page * pageSize + 1)}&ndash;
          {String(Math.min((page + 1) * pageSize, total))} of {String(total)} runs
        </span>
        <select
          value={pageSize}
          onChange={(e): void => {
            setPageSize(Number(e.target.value));
          }}
          className="rounded-md border border-border bg-surface-elevated px-2 py-1 text-xs text-text-primary focus:border-primary focus:outline-none"
        >
          {PAGE_SIZE_OPTIONS.map(size => (
            <option key={size} value={size}>
              {String(size)} per page
            </option>
          ))}
        </select>
      </div>
      <div className="flex items-center gap-2">
        <button
          onClick={(): void => {
            setPage(page - 1);
          }}
          disabled={page === 0}
          className="rounded-md border border-border bg-surface-elevated px-3 py-1 text-xs text-text-secondary transition-colors hover:bg-surface disabled:opacity-40 disabled:cursor-not-allowed"
        >
          Previous
        </button>
        <span className="text-xs text-text-tertiary">
          Page {String(page + 1)} of {String(Math.max(1, totalPages))}
        </span>
        <button
          onClick={(): void => {
            setPage(page + 1);
          }}
          disabled={!hasMore}
          className="rounded-md border border-border bg-surface-elevated px-3 py-1 text-xs text-text-secondary transition-colors hover:bg-surface disabled:opacity-40 disabled:cursor-not-allowed"
        >
          Next
        </button>
      </div>
    </div>
  );
}

export function DashboardPage(): React.ReactElement {
  const queryClient = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();

  // Connect to multiplexed dashboard SSE stream (all workflow events → Zustand store)
  useDashboardSSE();

  const hydrateWorkflow = useWorkflowStore(state => state.hydrateWorkflow);

  // Hydrate filter state from URL (supports bookmarkable views)
  const statusFilter = searchParams.get('status') ?? null;
  const searchQuery = searchParams.get('q') ?? '';
  const projectFilter = searchParams.get('project') ?? null;
  const dateRange = parseDateRange(searchParams.get('range'));
  const page = Math.max(0, Number(searchParams.get('page') ?? '0'));
  const pageSize = parsePageSize(searchParams.get('pageSize'));

  // Debounced search: type instantly in the input, but delay the server request
  const [searchInput, setSearchInput] = useState(searchQuery);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Sync searchInput when URL changes externally (e.g., back/forward)
  useEffect(() => {
    setSearchInput(searchParams.get('q') ?? '');
  }, [searchParams]);

  /** Helper to update URL params (replaces history entry to avoid back-spam). */
  const updateParams = useCallback(
    (updates: Record<string, string | null>) => {
      setSearchParams(
        prev => {
          const next = new URLSearchParams(prev);
          for (const [k, v] of Object.entries(updates)) {
            if (v === null || v === '' || v === '0' || v === 'all') {
              next.delete(k);
            } else {
              next.set(k, v);
            }
          }
          return next;
        },
        { replace: true }
      );
    },
    [setSearchParams]
  );

  const setStatusFilter = useCallback(
    (status: string | null) => {
      updateParams({ status, page: null });
    },
    [updateParams]
  );
  const setProjectFilter = useCallback(
    (project: string | null) => {
      updateParams({ project, page: null });
    },
    [updateParams]
  );
  const setDateRange = useCallback(
    (range: DateRange) => {
      updateParams({ range, page: null });
    },
    [updateParams]
  );
  const setPage = useCallback(
    (p: number) => {
      updateParams({ page: p === 0 ? null : String(p) });
    },
    [updateParams]
  );

  const setPageSize = useCallback(
    (size: number) => {
      updateParams({
        pageSize: size === DEFAULT_PAGE_SIZE ? null : String(size),
        page: null,
      });
    },
    [updateParams]
  );

  const handleSearchChange = useCallback(
    (value: string) => {
      setSearchInput(value);
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
        updateParams({ q: value || null, page: null });
      }, 300);
    },
    [updateParams]
  );

  // Compute date bounds from range preset
  const dateBounds = useMemo(() => getDateBounds(dateRange), [dateRange]);

  // Server-side fetch with all filters
  const {
    data: dashboardData,
    isLoading,
    isError,
    error: fetchError,
    dataUpdatedAt,
  } = useQuery({
    queryKey: [
      'dashboardRuns',
      {
        status: statusFilter,
        codebaseId: projectFilter,
        search: searchQuery,
        dateRange,
        page,
        pageSize,
      },
    ],
    queryFn: () =>
      listDashboardRuns({
        status: (statusFilter as WorkflowRunStatus) ?? undefined,
        codebaseId: projectFilter ?? undefined,
        search: searchQuery || undefined,
        after: dateBounds.after,
        before: dateBounds.before,
        limit: pageSize,
        offset: page * pageSize,
      }),
    refetchInterval: 5_000,
  });

  const runs = dashboardData?.runs ?? [];
  const total = dashboardData?.total ?? 0;
  const counts: DashboardCounts = dashboardData?.counts ?? {
    all: 0,
    running: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
    pending: 0,
    paused: 0,
  };

  // Hydrate Zustand store from REST-polled data for active runs.
  // Only sets initial state if the run isn't already tracked by SSE.
  useEffect(() => {
    for (const run of runs) {
      if (run.status === 'running' || run.status === 'pending' || run.status === 'paused') {
        hydrateWorkflow({
          runId: run.id,
          workflowName: run.workflow_name,
          status: run.status,
          dagNodes: [],
          artifacts: [],
          startedAt: new Date(ensureUtc(run.started_at)).getTime(),
          currentTool: null,
        });
      }
    }
  }, [runs, hydrateWorkflow]);

  const { data: codebases } = useQuery({
    queryKey: ['codebases'],
    queryFn: () => listCodebases(),
  });

  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: getHealth,
    staleTime: 10_000,
    refetchOnWindowFocus: true,
    refetchInterval: 30_000,
  });

  // Split into active and history (from server-filtered results)
  const activeRuns = useMemo(() => runs.filter(activeStatuses), [runs]);
  const activeGroups = useMemo(() => groupActiveRuns(activeRuns), [activeRuns]);
  const historyRuns = useMemo(() => runs.filter(historyStatuses), [runs]);

  const [actionError, setActionError] = useState<string | null>(null);

  async function runAction(
    action: (runId: string) => Promise<unknown>,
    runId: string,
    fallbackMessage: string
  ): Promise<void> {
    try {
      setActionError(null);
      await action(runId);
      void queryClient.invalidateQueries({ queryKey: ['dashboardRuns'] });
    } catch (err) {
      setActionError(err instanceof Error ? err.message : fallbackMessage);
    }
  }

  const handleCancel = (runId: string): Promise<void> =>
    runAction(cancelWorkflowRun, runId, 'Failed to cancel workflow');
  const handleResume = (runId: string): Promise<void> =>
    runAction(resumeWorkflowRun, runId, 'Failed to resume workflow');
  const handleAbandon = (runId: string): Promise<void> =>
    runAction(abandonWorkflowRun, runId, 'Failed to abandon workflow');
  const handleDelete = (runId: string): Promise<void> =>
    runAction(deleteWorkflowRun, runId, 'Failed to delete workflow run');
  const handleApprove = (runId: string): Promise<void> =>
    runAction(approveWorkflowRun, runId, 'Failed to approve workflow');
  // Reject differs from the rest of the lifecycle actions because it takes a
  // second argument (the optional reason). Inline it rather than squeezing
  // through `runAction`'s `(id) => Promise` signature with a closure — keeps
  // `runAction` usefully narrow for the single-arg actions above.
  async function handleReject(runId: string, reason?: string): Promise<void> {
    try {
      setActionError(null);
      await rejectWorkflowRun(runId, reason);
      void queryClient.invalidateQueries({ queryKey: ['dashboardRuns'] });
    } catch (err) {
      setActionError(err instanceof Error ? err.message : 'Failed to reject workflow');
    }
  }

  const actions: DashboardActionHandlers = {
    onCancel: handleCancel,
    onResume: handleResume,
    onAbandon: handleAbandon,
    onDelete: handleDelete,
    onApprove: handleApprove,
    onReject: handleReject,
  };

  const totalPages = Math.ceil(total / pageSize);
  const hasMore = page + 1 < totalPages;

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex-1 overflow-auto p-6 space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold text-text-primary">Mission Control</h1>
          {dataUpdatedAt > 0 && (
            <span className="text-xs text-text-tertiary">
              Last updated {new Date(dataUpdatedAt).toLocaleTimeString()}
            </span>
          )}
        </div>

        {/* Status Summary Bar — receives real server counts */}
        <StatusSummaryBar
          counts={counts}
          activeFilter={statusFilter}
          onFilterChange={setStatusFilter}
          searchQuery={searchInput}
          onSearchChange={handleSearchChange}
          projectFilter={projectFilter}
          onProjectFilterChange={setProjectFilter}
          dateRange={dateRange}
          onDateRangeChange={setDateRange}
          codebases={codebases}
          health={health}
        />

        {actionError && (
          <div className="rounded-md border border-error/30 bg-error/5 px-4 py-3 text-sm text-error">
            {actionError}
          </div>
        )}

        {isLoading ? (
          <div className="flex items-center justify-center py-12">
            <span className="text-sm text-text-tertiary">Loading...</span>
          </div>
        ) : isError ? (
          <div className="flex flex-col items-center justify-center gap-3 py-16">
            <p className="text-sm text-error">
              Failed to load workflow runs
              {fetchError instanceof Error ? `: ${fetchError.message}` : ''}
            </p>
          </div>
        ) : runs.length === 0 ? (
          <EmptyDashboardRuns />
        ) : (
          <>
            <ActiveRunsSection groups={activeGroups} health={health} actions={actions} />
            {historyRuns.length > 0 && (
              <section>
                <h2 className="mb-3 text-sm font-semibold text-text-secondary">History</h2>
                <WorkflowHistoryTable runs={historyRuns} onDelete={handleDelete} />
              </section>
            )}
            <PaginationControls
              page={page}
              pageSize={pageSize}
              total={total}
              totalPages={totalPages}
              hasMore={hasMore}
              setPage={setPage}
              setPageSize={setPageSize}
            />
          </>
        )}
      </div>
    </div>
  );
}
