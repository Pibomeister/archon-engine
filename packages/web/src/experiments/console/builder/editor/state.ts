/**
 * Editor state + reducer for `BuilderPage`. Pure module: every action is a
 * plain object and mutating actions carry their own timestamp (`at`) so the
 * history coalescing window is deterministic under test — the page's dispatch
 * wrapper stamps `Date.now()`.
 *
 * The reducer owns the canonical `BuilderWorkflow` plus the UI-only canvas
 * state PR-1 deliberately excluded from the data layer: positions, selection,
 * undo history, and the clipboard envelope.
 */
import { VARIANT_REGISTRY } from '../variants';
import type { BuilderNode, BuilderWorkflow, VariantId } from '../types';
import { NODE_HEIGHT, NODE_WIDTH, layoutWithDagre } from '../flow/layout';
import { builderToFlowEdges, edgeId } from '../flow/to-flow';
import type { XYPosition } from '../flow/types';
import {
  canRedo,
  canUndo,
  createHistory,
  pushSnapshot,
  redo,
  undo,
  type History,
  type Snapshot,
} from './history';
import { copySelection, pasteEnvelope, type ClipboardEnvelope } from './clipboard';
import { align, distributeH, distributeV, type AlignMode, type NodeRect } from './align';

export interface EditorState {
  workflow: BuilderWorkflow;
  positions: ReadonlyMap<string, XYPosition>;
  selectedNodes: ReadonlySet<string>;
  selectedEdges: ReadonlySet<string>;
  history: History;
  clipboard: ClipboardEnvelope | null;
}

/** Measured node dimensions, passed by the page from the live canvas. */
export interface NodeSize {
  width: number;
  height: number;
}

/**
 * Valid node ids — mirrors the engine's id grammar (the `when:` atom pattern
 * in validation/when-grammar.ts): letters/digits/underscore/hyphen, no leading
 * digit. Enforced on rename so an id can never contain the `->` edge-id
 * separator or break `$<id>.output` references.
 */
export const NODE_ID_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_-]*$/;

export type EditorAction =
  | { type: 'add-node'; variant: VariantId; position: XYPosition; at: number }
  | { type: 'patch-node'; node: BuilderNode; at: number }
  | { type: 'rename-node'; id: string; nextId: string; at: number }
  | { type: 'remove-nodes'; ids: readonly string[]; at: number }
  | { type: 'add-edge'; source: string; target: string; at: number }
  | { type: 'remove-selection'; at: number }
  | { type: 'move-nodes'; moves: readonly { id: string; position: XYPosition }[]; at: number }
  | { type: 'set-selection'; nodeIds: ReadonlySet<string>; edgeIds: ReadonlySet<string> }
  | {
      // Per-element selection deltas from the canvas. xyflow only reports
      // selection through node/edge `select` changes in controlled mode, so
      // this merges those deltas into the canonical selection sets.
      type: 'apply-selection';
      nodes: readonly { id: string; selected: boolean }[];
      edges: readonly { id: string; selected: boolean }[];
    }
  | { type: 'select-all' }
  | { type: 'copy' }
  | { type: 'cut'; at: number }
  | { type: 'paste'; at: number }
  | { type: 'align'; mode: AlignMode; sizes?: ReadonlyMap<string, NodeSize>; at: number }
  | { type: 'distribute'; axis: 'h' | 'v'; sizes?: ReadonlyMap<string, NodeSize>; at: number }
  | { type: 'auto-arrange'; at: number }
  | { type: 'undo' }
  | { type: 'redo' };

/** Initial editor state: dagre-layout every node, nothing selected. */
export function createEditorState(workflow: BuilderWorkflow): EditorState {
  const edges = builderToFlowEdges(workflow).map(e => ({ source: e.source, target: e.target }));
  return {
    workflow,
    positions: layoutWithDagre(
      workflow.nodes.map(n => n.id),
      edges
    ),
    selectedNodes: new Set(),
    selectedEdges: new Set(),
    history: createHistory(),
    clipboard: null,
  };
}

function snapshotOf(state: EditorState): Snapshot {
  return { workflow: state.workflow, positions: state.positions };
}

/** Push the pre-edit snapshot under `kind`, coalescing per `history.ts`. */
function remember(state: EditorState, kind: string, at: number): History {
  return pushSnapshot(state.history, kind, snapshotOf(state), at);
}

/** `variant-1`, `variant-2`, … skipping taken ids. */
function uniqueNodeId(variant: VariantId, taken: ReadonlySet<string>): string {
  let n = 1;
  while (taken.has(`${variant}-${n}`)) n += 1;
  return `${variant}-${n}`;
}

function nodeIds(workflow: BuilderWorkflow): Set<string> {
  return new Set(workflow.nodes.map(n => n.id));
}

function setsEqual(a: ReadonlySet<string>, b: ReadonlySet<string>): boolean {
  if (a.size !== b.size) return false;
  for (const v of a) if (!b.has(v)) return false;
  return true;
}

/**
 * Selected nodes as rects for the alignment kernels. Measured sizes from the
 * live canvas take precedence so align/distribute agree with smart-guide
 * snapping; the fixed constants are the pre-measurement fallback.
 */
function selectionRects(state: EditorState, sizes?: ReadonlyMap<string, NodeSize>): NodeRect[] {
  const rects: NodeRect[] = [];
  for (const id of state.selectedNodes) {
    const position = state.positions.get(id);
    if (position === undefined) continue;
    rects.push({
      id,
      position,
      width: sizes?.get(id)?.width ?? NODE_WIDTH,
      height: sizes?.get(id)?.height ?? NODE_HEIGHT,
    });
  }
  return rects;
}

function withPositions(
  state: EditorState,
  updates: ReadonlyMap<string, XYPosition>
): ReadonlyMap<string, XYPosition> {
  const next = new Map(state.positions);
  for (const [id, pos] of updates) next.set(id, pos);
  return next;
}

/** Drop `removed` ids from every remaining node's `depends_on`. */
function stripDeps(nodes: readonly BuilderNode[], removed: ReadonlySet<string>): BuilderNode[] {
  return nodes.map(node => {
    const deps = node.base.depends_on;
    if (!deps?.some(d => removed.has(d))) return node;
    const filtered = deps.filter(d => !removed.has(d));
    return {
      ...node,
      base: { ...node.base, depends_on: filtered.length > 0 ? filtered : undefined },
    } as BuilderNode;
  });
}

function addNode(
  state: EditorState,
  action: Extract<EditorAction, { type: 'add-node' }>
): EditorState {
  const id = uniqueNodeId(action.variant, nodeIds(state.workflow));
  const node = {
    id,
    variant: action.variant,
    base: {},
    data: VARIANT_REGISTRY[action.variant].defaultData(),
  } as BuilderNode;
  return {
    ...state,
    history: remember(state, 'add-node', action.at),
    workflow: { ...state.workflow, nodes: [...state.workflow.nodes, node] },
    positions: withPositions(state, new Map([[id, action.position]])),
    selectedNodes: new Set([id]),
    selectedEdges: new Set(),
  };
}

function patchNode(
  state: EditorState,
  action: Extract<EditorAction, { type: 'patch-node' }>
): EditorState {
  if (!state.workflow.nodes.some(n => n.id === action.node.id)) return state;
  return {
    ...state,
    history: remember(state, `patch:${action.node.id}`, action.at),
    workflow: {
      ...state.workflow,
      nodes: state.workflow.nodes.map(n => (n.id === action.node.id ? action.node : n)),
    },
  };
}

function renameNode(
  state: EditorState,
  action: Extract<EditorAction, { type: 'rename-node' }>
): EditorState {
  const ids = nodeIds(state.workflow);
  if (!ids.has(action.id)) return state;
  const nextId = action.nextId.trim();
  if (
    nextId.length === 0 ||
    nextId === action.id ||
    !NODE_ID_PATTERN.test(nextId) ||
    ids.has(nextId)
  ) {
    return state;
  }
  const nodes = state.workflow.nodes.map(node => renamedNode(node, action.id, nextId));
  const positions = renamePosition(state.positions, action.id, nextId);
  const selectedNodes = new Set(state.selectedNodes);
  if (selectedNodes.delete(action.id)) selectedNodes.add(nextId);
  return {
    ...state,
    history: remember(state, 'rename-node', action.at),
    workflow: { ...state.workflow, nodes },
    positions,
    selectedNodes,
    selectedEdges: new Set(),
  };
}

function renamedNode(node: BuilderNode, id: string, nextId: string): BuilderNode {
  if (node.id === id) return { ...node, id: nextId } as BuilderNode;
  const deps = node.base.depends_on;
  if (!deps?.includes(id)) return node;
  return {
    ...node,
    base: { ...node.base, depends_on: deps.map(d => (d === id ? nextId : d)) },
  } as BuilderNode;
}

function renamePosition(
  positions: ReadonlyMap<string, XYPosition>,
  id: string,
  nextId: string
): ReadonlyMap<string, XYPosition> {
  const next = new Map(positions);
  const pos = next.get(id);
  if (pos === undefined) return next;
  next.delete(id);
  next.set(nextId, pos);
  return next;
}

function removeNodes(
  state: EditorState,
  action: Extract<EditorAction, { type: 'remove-nodes' }>
): EditorState {
  const removed = new Set(action.ids);
  if (removed.size === 0) return state;
  const kept = state.workflow.nodes.filter(n => !removed.has(n.id));
  if (kept.length === state.workflow.nodes.length) return state;
  const positions = new Map(state.positions);
  for (const id of removed) positions.delete(id);
  const selectedNodes = new Set(state.selectedNodes);
  for (const id of removed) selectedNodes.delete(id);
  return {
    ...state,
    history: remember(state, 'remove-nodes', action.at),
    workflow: { ...state.workflow, nodes: stripDeps(kept, removed) },
    positions,
    selectedNodes,
    selectedEdges: new Set(),
  };
}

function addEdge(
  state: EditorState,
  action: Extract<EditorAction, { type: 'add-edge' }>
): EditorState {
  if (action.source === action.target) return state;
  const ids = nodeIds(state.workflow);
  if (!ids.has(action.source) || !ids.has(action.target)) return state;
  const target = state.workflow.nodes.find(n => n.id === action.target);
  if (target === undefined || (target.base.depends_on ?? []).includes(action.source)) return state;
  const nodes = state.workflow.nodes.map(node =>
    node.id === action.target
      ? ({
          ...node,
          base: { ...node.base, depends_on: [...(node.base.depends_on ?? []), action.source] },
        } as BuilderNode)
      : node
  );
  return {
    ...state,
    history: remember(state, 'add-edge', action.at),
    workflow: { ...state.workflow, nodes },
  };
}

function removeSelection(
  state: EditorState,
  action: Extract<EditorAction, { type: 'remove-selection' }>
): EditorState {
  const removedNodes = state.selectedNodes;
  const removedEdges = state.selectedEdges;
  if (removedNodes.size === 0 && removedEdges.size === 0) return state;
  const kept = state.workflow.nodes.filter(n => !removedNodes.has(n.id));
  const nodes = kept.map(node => removeSelectedDeps(node, removedNodes, removedEdges));
  const anyNodeRemoved = kept.length !== state.workflow.nodes.length;
  const anyDepRemoved = nodes.some((n, i) => n !== kept[i]);
  if (!anyNodeRemoved && !anyDepRemoved) return state;
  const positions = new Map(state.positions);
  for (const id of removedNodes) positions.delete(id);
  return {
    ...state,
    history: remember(state, 'remove-selection', action.at),
    workflow: { ...state.workflow, nodes },
    positions,
    selectedNodes: new Set(),
    selectedEdges: new Set(),
  };
}

function removeSelectedDeps(
  node: BuilderNode,
  removedNodes: ReadonlySet<string>,
  removedEdges: ReadonlySet<string>
): BuilderNode {
  const deps = node.base.depends_on;
  if (deps === undefined) return node;
  const filtered = deps.filter(
    dep => !removedNodes.has(dep) && !removedEdges.has(edgeId(dep, node.id))
  );
  if (filtered.length === deps.length) return node;
  return {
    ...node,
    base: { ...node.base, depends_on: filtered.length > 0 ? filtered : undefined },
  } as BuilderNode;
}

function moveNodes(
  state: EditorState,
  action: Extract<EditorAction, { type: 'move-nodes' }>
): EditorState {
  if (action.moves.length === 0) return state;
  return {
    ...state,
    history: remember(state, 'move-nodes', action.at),
    positions: withPositions(state, new Map(action.moves.map(m => [m.id, m.position]))),
  };
}

function applySelection(
  state: EditorState,
  action: Extract<EditorAction, { type: 'apply-selection' }>
): EditorState {
  const selectedNodes = applySelectionDeltas(state.selectedNodes, action.nodes);
  const selectedEdges = applySelectionDeltas(state.selectedEdges, action.edges);
  if (
    setsEqual(selectedNodes, state.selectedNodes) &&
    setsEqual(selectedEdges, state.selectedEdges)
  )
    return state;
  return { ...state, selectedNodes, selectedEdges };
}

function applySelectionDeltas(
  current: ReadonlySet<string>,
  deltas: readonly { id: string; selected: boolean }[]
): ReadonlySet<string> {
  const next = new Set(current);
  for (const change of deltas) {
    if (change.selected) next.add(change.id);
    else next.delete(change.id);
  }
  return next;
}

function copySelectionAction(state: EditorState): EditorState {
  const envelope = copySelection(state.workflow, state.selectedNodes, state.positions);
  return envelope === null ? state : { ...state, clipboard: envelope };
}

function cutSelection(
  state: EditorState,
  action: Extract<EditorAction, { type: 'cut' }>
): EditorState {
  const envelope = copySelection(state.workflow, state.selectedNodes, state.positions);
  if (envelope === null) return state;
  const next = removeNodes(
    { ...state, clipboard: envelope },
    { type: 'remove-nodes', ids: [...state.selectedNodes], at: action.at }
  );
  return { ...next, history: { ...next.history, lastKind: 'cut' } };
}

function pasteClipboard(
  state: EditorState,
  action: Extract<EditorAction, { type: 'paste' }>
): EditorState {
  if (state.clipboard === null) return state;
  const { nodes, positions } = pasteEnvelope(state.clipboard, nodeIds(state.workflow));
  if (nodes.length === 0) return state;
  return {
    ...state,
    history: remember(state, 'paste', action.at),
    workflow: { ...state.workflow, nodes: [...state.workflow.nodes, ...nodes] },
    positions: mergePastedPositions(state.positions, nodes, positions),
    selectedNodes: new Set(nodes.map(n => n.id)),
    selectedEdges: new Set(),
  };
}

function mergePastedPositions(
  current: ReadonlyMap<string, XYPosition>,
  nodes: readonly BuilderNode[],
  positions: ReadonlyMap<string, XYPosition>
): ReadonlyMap<string, XYPosition> {
  const merged = new Map(current);
  for (const node of nodes) {
    const pos = positions.get(node.id);
    if (pos === undefined) {
      console.warn(`[builder] pasted node '${node.id}' has no position; placing at {40,40}`);
    }
    merged.set(node.id, pos ?? { x: 40, y: 40 });
  }
  return merged;
}

function alignSelection(
  state: EditorState,
  action: Extract<EditorAction, { type: 'align' }>
): EditorState {
  const rects = selectionRects(state, action.sizes);
  if (rects.length < 2) return state;
  return {
    ...state,
    history: remember(state, 'align', action.at),
    positions: withPositions(state, align(action.mode, rects)),
  };
}

function distributeSelection(
  state: EditorState,
  action: Extract<EditorAction, { type: 'distribute' }>
): EditorState {
  const rects = selectionRects(state, action.sizes);
  if (rects.length < 3) return state;
  const next = action.axis === 'h' ? distributeH(rects) : distributeV(rects);
  return {
    ...state,
    history: remember(state, 'distribute', action.at),
    positions: withPositions(state, next),
  };
}

function autoArrange(
  state: EditorState,
  action: Extract<EditorAction, { type: 'auto-arrange' }>
): EditorState {
  const edges = builderToFlowEdges(state.workflow).map(e => ({
    source: e.source,
    target: e.target,
  }));
  return {
    ...state,
    history: remember(state, 'auto-arrange', action.at),
    positions: layoutWithDagre(
      state.workflow.nodes.map(n => n.id),
      edges
    ),
  };
}

function restoreSnapshot(
  state: EditorState,
  result: { history: History; snapshot: Snapshot } | null
): EditorState {
  if (result === null) return state;
  return {
    ...state,
    history: result.history,
    workflow: result.snapshot.workflow,
    positions: result.snapshot.positions,
    selectedNodes: new Set(),
    selectedEdges: new Set(),
  };
}

export function editorReducer(state: EditorState, action: EditorAction): EditorState {
  switch (action.type) {
    case 'add-node':
      return addNode(state, action);
    case 'patch-node':
      return patchNode(state, action);
    case 'rename-node':
      return renameNode(state, action);
    case 'remove-nodes':
      return removeNodes(state, action);
    case 'add-edge':
      return addEdge(state, action);
    case 'remove-selection':
      return removeSelection(state, action);
    case 'move-nodes':
      return moveNodes(state, action);
    case 'set-selection':
      return { ...state, selectedNodes: action.nodeIds, selectedEdges: action.edgeIds };
    case 'apply-selection':
      return applySelection(state, action);
    case 'select-all':
      return { ...state, selectedNodes: nodeIds(state.workflow), selectedEdges: new Set() };
    case 'copy':
      return copySelectionAction(state);
    case 'cut':
      return cutSelection(state, action);
    case 'paste':
      return pasteClipboard(state, action);
    case 'align':
      return alignSelection(state, action);
    case 'distribute':
      return distributeSelection(state, action);
    case 'auto-arrange':
      return autoArrange(state, action);
    case 'undo':
      return restoreSnapshot(state, undo(state.history, snapshotOf(state)));
    case 'redo':
      return restoreSnapshot(state, redo(state.history, snapshotOf(state)));
  }
}

export { canUndo, canRedo };
