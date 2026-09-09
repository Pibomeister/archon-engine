import type { ReactElement } from 'react';
import { formatElapsed, formatRelativeToBaseline, formatClock } from '../lib/format';
import { useStreamContext } from '../lib/stream-context';

interface NodeDividerProps {
  /** `step_name` — the scroll-anchor target for the graph panel. */
  nodeId: string;
  nodeName: string;
  /** Folded lifecycle status; `running` = the node is still in-flight. */
  status: 'running' | 'completed' | 'failed' | 'skipped';
  durationMs: number | null;
  timestamp: string;
  /** From `node_completed` — surfaced inline so per-node spend is visible. */
  costUsd?: number | null;
  numTurns?: number | null;
  /** From `node_completed` — surfaced under the System detail toggle. */
  stopReason?: string | null;
  /** Only set for `skipped` — `when_condition` / `trigger_rule`. */
  skipReason?: string | null;
  /** Only set for `skipped` — the evaluated gating expression. */
  skipExpr?: string | null;
  /** When true, surface skip reason / stop reason inline. */
  showDetail?: boolean;
}

const STATUS_LABEL: Record<NodeDividerProps['status'], string> = {
  running: 'running',
  completed: 'completed',
  failed: 'failed',
  skipped: 'skipped',
};

const STATUS_COLOR: Record<NodeDividerProps['status'], string> = {
  running: 'text-text-tertiary',
  completed: 'text-success',
  failed: 'text-error',
  skipped: 'text-text-tertiary',
};

/**
 * Thin divider heading one DAG node — exactly one per node, folded from its
 * transitions (started + terminal, plus any resume-time skip).
 *   left gutter:  relative timestamp (mono)
 *   left label:   node name in mono
 *   right label:  status + duration (when terminal)
 *
 * The id targets a scrollIntoView from the graph panel.
 */
function optionalMetric(
  value: number | null | undefined,
  formatter: (value: number) => string
): string {
  return value !== null && value !== undefined && value > 0 ? ` · ${formatter(value)}` : '';
}

function costLabel(costUsd: number): string {
  return `$${costUsd >= 0.01 ? costUsd.toFixed(2) : costUsd.toFixed(4)}`;
}

function hasDetail(value: string | null | undefined): value is string {
  return value !== null && value !== undefined && value.length > 0;
}

function StopDetail({ stopReason }: { stopReason: string }): ReactElement {
  return (
    <div className="ml-[68px] flex flex-wrap items-baseline gap-x-2 font-mono text-[10px] text-text-tertiary">
      <span>stop</span>
      <span className="text-text-secondary">{stopReason}</span>
    </div>
  );
}

function SkipDetail({
  skipReason,
  skipExpr,
}: {
  skipReason: string;
  skipExpr?: string | null;
}): ReactElement {
  return (
    <div className="ml-[68px] flex flex-wrap items-baseline gap-x-2 font-mono text-[10px] text-text-tertiary">
      <span>reason</span>
      <span className="text-text-secondary">{skipReason}</span>
      {hasDetail(skipExpr) ? (
        <>
          <span>expr</span>
          <span className="text-text-secondary">{skipExpr}</span>
        </>
      ) : null}
    </div>
  );
}

export function NodeDivider({
  nodeId,
  nodeName,
  status,
  durationMs,
  timestamp,
  costUsd,
  numTurns,
  stopReason,
  skipReason,
  skipExpr,
  showDetail = false,
}: NodeDividerProps): ReactElement {
  const { runStartedAt } = useStreamContext();
  const displayed = formatRelativeToBaseline(timestamp, runStartedAt);
  const wallClock = formatClock(timestamp);
  const dur = optionalMetric(durationMs, (value: number) =>
    formatElapsed(Math.floor(value / 1000))
  );
  const cost = optionalMetric(costUsd, costLabel);
  const turns = optionalMetric(numTurns, (value: number) => `${value}t`);
  const showStopDetail = status !== 'skipped' && showDetail && hasDetail(stopReason);
  const showSkipDetail = status === 'skipped' && showDetail && hasDetail(skipReason);

  // One divider per node now, so the scroll-anchor id is always present and
  // keyed by nodeId (matches the graph panel's getElementById target).
  return (
    <div
      id={`node-transition-${nodeId}`}
      className="flex flex-col gap-1 border-b border-border/60 py-[11px]"
    >
      <div className="flex items-center gap-4">
        <time
          dateTime={timestamp}
          title={wallClock}
          className="w-14 shrink-0 font-mono text-[11.5px] tabular-nums text-text-tertiary"
        >
          {displayed}
        </time>
        <span className="font-mono text-[13px] font-semibold text-text-primary">{nodeName}</span>
        {/* Dashed leader line (design v3 .log-line). */}
        <div
          className="h-px flex-1"
          style={{
            background:
              'repeating-linear-gradient(90deg, var(--border) 0 4px, transparent 4px 8px)',
          }}
          aria-hidden
        />
        <span className={`font-mono text-[11.5px] ${STATUS_COLOR[status]}`}>
          {STATUS_LABEL[status]}
          {dur}
          {cost}
          {turns}
        </span>
      </div>
      {showStopDetail ? <StopDetail stopReason={stopReason} /> : null}
      {showSkipDetail ? <SkipDetail skipReason={skipReason} skipExpr={skipExpr} /> : null}
    </div>
  );
}
