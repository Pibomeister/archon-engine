/**
 * Workflow loader - discovers and parses workflow YAML files
 */
import type { WorkflowDefinition, WorkflowLoadError, DagNode, WorkflowNodeHooks } from './schemas';
import {
  isBashNode,
  isLoopNode,
  isLoopGroupNode,
  isApprovalNode,
  isCancelNode,
  isScriptNode,
  isIncludeNode,
  isWorkflowNode,
  isPersistableNode,
} from './schemas';
import { createLogger } from '@archon/paths';
import {
  isRegisteredProvider,
  getRegisteredProviders,
  getProviderCapabilities,
} from '@archon/providers';
import {
  dagNodeSchema,
  BASH_NODE_AI_FIELDS,
  SCRIPT_NODE_AI_FIELDS,
  LOOP_NODE_AI_FIELDS,
  LOOP_GROUP_NODE_AI_FIELDS,
  INCLUDE_NODE_IGNORED_FIELDS,
  WORKFLOW_NODE_IGNORED_FIELDS,
  KNOWN_DAG_NODE_KEYS,
  KNOWN_NODE_NESTED_KEYS,
  effortLevelSchema,
  thinkingConfigSchema,
  sandboxSettingsSchema,
  betasSchema,
} from './schemas/dag-node';
import type { NestedKeySpec } from './schemas/dag-node';
import {
  modelReasoningEffortSchema,
  webSearchModeSchema,
  workflowRequirementSchema,
  workflowEvidencePolicySchema,
  workflowHardenedPolicySchema,
  workflowBudgetPolicySchema,
  KNOWN_WORKFLOW_KEYS,
  KNOWN_WORKFLOW_NESTED_KEYS,
  WORKFLOW_ONLY_KEYS,
} from './schemas/workflow';
import type {
  WorkflowRequirement,
  WorkflowEvidencePolicy,
  WorkflowHardenedPolicy,
  WorkflowBudgetPolicy,
} from './schemas/workflow';
import { workflowNodeHooksSchema } from './schemas/hooks';
import { z } from '@hono/zod-openapi';

/** Lazy-initialized logger (deferred so test mocks can intercept createLogger) */
let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('workflow.loader');
  return cachedLog;
}

/**
 * Parse an optional, schema-validated workflow field with warn-and-drop
 * semantics: a present-but-invalid value is logged and dropped (returns
 * undefined) rather than rejecting the whole workflow, so a typo in one field
 * doesn't abort the discovery pass. Mirrors the policy used for `tags` /
 * `interactive`. `extra` merges into the warning payload (e.g. the list of
 * valid enum options).
 *
 * The return type is inferred from the schema (`z.output<S>`), so
 * preprocess-based schemas (e.g. `thinkingConfigSchema`, whose input is
 * `unknown`) still resolve to their parsed output type rather than their
 * input type. zod v4 removed `ZodTypeDef` as the middle type parameter, so the
 * old `z.ZodType<T, z.ZodTypeDef, unknown>` form no longer compiles.
 */
function parseOptionalField<S extends z.ZodType>(
  raw: unknown,
  schema: S,
  filename: string,
  event: string,
  extra?: Record<string, unknown>
): z.output<S> | undefined {
  const result = schema.safeParse(raw);
  if (result.success) return result.data;
  if (raw !== undefined) {
    getLog().warn({ filename, value: raw, ...extra }, event);
  }
  return undefined;
}

/**
 * Parse YAML using Bun's native YAML parser
 */
function parseYaml(content: string): unknown {
  return Bun.YAML.parse(content);
}

/**
 * Format a Zod validation error issue into a human-readable string for a named node.
 */
function formatNodeIssue(id: string, issue: z.ZodIssue): string {
  const pathStr = issue.path.length > 0 ? `'${issue.path.join('.')}' ` : '';
  return `Node '${id}': ${pathStr}${issue.message}`;
}

/**
 * The one shape of a `$nodeId.output` reference. Both scanners below build their own
 * RegExp from it — a `g`-flagged one for the multi-match dangling-ref sweep and a plain one
 * for `fan_out.items` — because a `g` regex carries mutable `lastIndex` and sharing a single
 * instance across call sites is how that turns into skipped matches. Sharing the SOURCE is
 * the part that matters: a second hand-written copy inside a function that already warns
 * "KEEP IN SYNC" is exactly the drift that warning is about.
 */
const OUTPUT_REF_SOURCE = String.raw`\$([a-zA-Z_][a-zA-Z0-9_-]*)\.output`;

/**
 * The node's `id` for messages, falling back to its 1-based position when the
 * id is missing or blank (the schema reports that separately as an error).
 */
function nodeIdForMessages(raw: unknown, index: number): string {
  const rawId =
    raw !== null && typeof raw === 'object' && 'id' in raw
      ? String((raw as Record<string, unknown>).id)
      : '';
  return rawId.trim() || `#${String(index + 1)}`;
}

/**
 * Guidance for a key the engine drops, appended to the unknown-key warning.
 *
 * `interactive` gets its own text because it is the reported failure (#2213):
 * an author writes it expecting a human gate, the key is dropped, and the run
 * proceeds unattended. Both escapes offered here actually gate — in particular
 * `loop.gate_message` ALONE does not: the executor requires
 * `loop.interactive && loop.gate_message` (dag-executor.ts, `runLoopNode` /
 * `runLoopGroupNode`), so naming only `gate_message` would hand the author a
 * loop with a message and no gate.
 */
function unknownNodeKeyHint(key: string): string {
  if (key === 'interactive') {
    return (
      " Nothing on this node gates. For a human gate, use an 'approval:' node; to gate each" +
      " iteration of a loop, set BOTH 'loop.interactive: true' and 'loop.gate_message'" +
      " ('gate_message' on its own does not gate). Workflow-level 'interactive:' is a" +
      ' different setting, and only on the web UI — it keeps the run in the foreground' +
      ' there; chat platforms already run in the foreground, so it does nothing for them.'
    );
  }
  if (WORKFLOW_ONLY_KEYS.has(key)) {
    return ` ('${key}' is valid at workflow level, not on individual nodes.)`;
  }
  return '';
}

/**
 * Record one unknown-key warning, both for callers and for the run-time log.
 *
 * `id` is the bare node or workflow id — a stable value a log consumer can
 * filter on. `label` is its human rendering (it may carry a breadcrumb, e.g.
 * `Node 'refine' → loop_group node 'check'`) and appears only inside the
 * message prose, never as a structured field.
 */
function pushUnknownKeyWarning(
  id: string,
  label: string,
  key: string,
  hint: string,
  event: string,
  warnings: string[]
): void {
  const message = `${label}: unknown key '${key}' will be ignored.${hint}`;
  warnings.push(message);
  // Carry the prose, not just the payload: the run path (`archon workflow run`)
  // reads this log line and never reads the warning string (#2213).
  getLog().warn({ id, key, warning: message }, event);
}

/**
 * Warn about keys Zod silently stripped from a nested config object, recursing
 * through the sub-objects `spec` describes. `keyPath` is the dotted prefix that
 * locates the key inside the node (e.g. `approval.on_reject.`).
 */
function collectUnknownConfigKeys(
  raw: unknown,
  spec: NestedKeySpec,
  id: string,
  label: string,
  keyPath: string,
  event: string,
  warnings: string[]
): void {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return;
  const obj = raw as Record<string, unknown>;

  if (spec.kind === 'record') {
    for (const [entryKey, entryValue] of Object.entries(obj)) {
      collectUnknownConfigKeys(
        entryValue,
        spec.entry,
        id,
        label,
        `${keyPath}${entryKey}.`,
        event,
        warnings
      );
    }
    return;
  }

  for (const key of Object.keys(obj)) {
    if (!spec.keys.has(key)) {
      pushUnknownKeyWarning(id, label, `${keyPath}${key}`, '', event, warnings);
      continue;
    }
    const child = spec.children?.get(key);
    if (child) {
      collectUnknownConfigKeys(obj[key], child, id, label, `${keyPath}${key}.`, event, warnings);
    }
  }
}

/**
 * Warn about unknown keys on a raw node that Zod silently stripped (#2213).
 * Catches misplaced workflow-level keys (`interactive:` on a command node),
 * typos (`contxt:` instead of `context:`), and the same mistakes one level down
 * inside `approval:` / `retry:` / `loop:` / `agents:`.
 *
 * Recurses into a `loop_group` body: those entries are full DAG nodes parsed by
 * the same schema, so they strip unknown keys just as silently — and a body node
 * is exactly where an `interactive: true` gate is most likely to be attempted.
 */
function collectUnknownNodeKeys(raw: unknown, id: string, label: string, warnings: string[]): void {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return;
  const obj = raw as Record<string, unknown>;

  for (const key of Object.keys(obj)) {
    if (!KNOWN_DAG_NODE_KEYS.has(key)) {
      pushUnknownKeyWarning(
        id,
        label,
        key,
        unknownNodeKeyHint(key),
        'node_unknown_key_ignored',
        warnings
      );
      continue;
    }
    const nested = KNOWN_NODE_NESTED_KEYS.get(key);
    if (nested) {
      collectUnknownConfigKeys(
        obj[key],
        nested,
        id,
        label,
        `${key}.`,
        'node_unknown_key_ignored',
        warnings
      );
    }
  }

  const group = obj.loop_group;
  if (group === null || typeof group !== 'object' || Array.isArray(group)) return;
  const body = (group as Record<string, unknown>).nodes;
  if (!Array.isArray(body)) return;
  body.forEach((bodyNode: unknown, i: number) => {
    const bodyId = nodeIdForMessages(bodyNode, i);
    collectUnknownNodeKeys(bodyNode, bodyId, `${label} → loop_group node '${bodyId}'`, warnings);
  });
}

/**
 * Validate and parse a single DagNode from raw YAML data.
 * Replaces the former parseDagNode + parseRetryConfig + parseToolList +
 * parseNodeHooks + parseIdleTimeout functions.
 */
function parseDagNode(
  raw: unknown,
  index: number,
  errors: string[],
  warnings: string[]
): DagNode | null {
  // Extract id early for error messages (may be empty/invalid — schema will catch it)
  const id = nodeIdForMessages(raw, index);

  const result = dagNodeSchema.safeParse(raw);
  if (!result.success) {
    for (const issue of result.error.issues) {
      errors.push(formatNodeIssue(id, issue));
    }
    return null;
  }

  const node = result.data;

  collectUnknownNodeKeys(raw, id, `Node '${id}'`, warnings);

  // Warn about AI-specific fields on non-AI nodes (runtime behavior, not schema errors)
  let nonAiNode: { type: string; fields: readonly string[] } | undefined;
  if (isCancelNode(node)) {
    nonAiNode = { type: 'cancel', fields: BASH_NODE_AI_FIELDS };
  } else if (isIncludeNode(node)) {
    nonAiNode = { type: 'include', fields: INCLUDE_NODE_IGNORED_FIELDS };
  } else if (isWorkflowNode(node)) {
    nonAiNode = { type: 'workflow', fields: WORKFLOW_NODE_IGNORED_FIELDS };
  } else if (isApprovalNode(node)) {
    nonAiNode = { type: 'approval', fields: BASH_NODE_AI_FIELDS };
  } else if (isLoopNode(node)) {
    nonAiNode = { type: 'loop', fields: LOOP_NODE_AI_FIELDS };
  } else if (isLoopGroupNode(node)) {
    nonAiNode = { type: 'loop_group', fields: LOOP_GROUP_NODE_AI_FIELDS };
  } else if (isScriptNode(node)) {
    nonAiNode = { type: 'script', fields: SCRIPT_NODE_AI_FIELDS };
  } else if ('bash' in node && typeof node.bash === 'string') {
    nonAiNode = { type: 'bash', fields: BASH_NODE_AI_FIELDS };
  }
  if (nonAiNode) {
    const presentAiFields = nonAiNode.fields.filter(
      f => (raw as Record<string, unknown>)[f] !== undefined
    );
    if (presentAiFields.length > 0) {
      getLog().warn(
        { id: node.id, fields: presentAiFields },
        `${nonAiNode.type}_node_ai_fields_ignored`
      );
    }
  }

  return node;
}

/**
 * Validate DAG structure: unique IDs, depends_on references exist, no cycles,
 * and $nodeId.output refs in when:/prompt: fields point to known nodes.
 * Returns error message or null if valid.
 *
 * Exported so the include-expander can re-run the same structural checks on the
 * fully-flattened, namespaced node list after inlining (duplicate-id collisions,
 * cycles introduced by rewired edges, unknown deps).
 */
function collectDagNodeIds(
  nodes: DagNode[],
  enclosingIds?: ReadonlySet<string>
): { ids: Set<string>; error: string | null } {
  const ids = new Set<string>();
  for (const node of nodes) {
    if (ids.has(node.id)) return { ids, error: `Duplicate node id: '${node.id}'` };
    if (enclosingIds?.has(node.id)) {
      return { ids, error: `Node id '${node.id}' shadows a node id in the enclosing DAG` };
    }
    ids.add(node.id);
  }
  return { ids, error: null };
}

function validateDependsOnRefs(nodes: DagNode[], ids: ReadonlySet<string>): string | null {
  for (const node of nodes) {
    for (const dep of node.depends_on ?? []) {
      if (!ids.has(dep)) return `Node '${node.id}' depends_on unknown node '${dep}'`;
    }
  }
  return null;
}

function buildDagEdges(nodes: DagNode[]): {
  inDegree: Map<string, number>;
  dependents: Map<string, string[]>;
} {
  const inDegree = new Map<string, number>();
  const dependents = new Map<string, string[]>();
  for (const node of nodes) {
    inDegree.set(node.id, node.depends_on?.length ?? 0);
    for (const dep of node.depends_on ?? []) {
      const existing = dependents.get(dep) ?? [];
      existing.push(node.id);
      dependents.set(dep, existing);
    }
  }
  return { inDegree, dependents };
}

function validateDagAcyclic(nodes: DagNode[]): string | null {
  const { inDegree, dependents } = buildDagEdges(nodes);
  const queue = nodes.filter(n => (inDegree.get(n.id) ?? 0) === 0).map(n => n.id);
  let visited = 0;
  while (queue.length > 0) {
    const nodeId = queue.shift();
    if (nodeId === undefined) break;
    visited++;
    for (const dep of dependents.get(nodeId) ?? []) {
      const newDegree = (inDegree.get(dep) ?? 0) - 1;
      inDegree.set(dep, newDegree);
      if (newDegree === 0) queue.push(dep);
    }
  }
  if (visited >= nodes.length) return null;
  const cycleNodes = nodes.filter(n => (inDegree.get(n.id) ?? 0) > 0).map(n => n.id);
  return `Cycle detected among nodes: ${cycleNodes.join(', ')}`;
}

function stripMarkdownCode(s: string): string {
  return s.replace(/```[\s\S]*?```/g, '').replace(/`[^`\n]*`/g, '');
}

function collectOutputRefSources(node: DagNode): string[] {
  const sources: string[] = [];
  if (node.when) sources.push(node.when);
  if ('prompt' in node && typeof node.prompt === 'string')
    sources.push(stripMarkdownCode(node.prompt));
  if (isBashNode(node)) sources.push(node.bash);
  if (isScriptNode(node)) sources.push(node.script);
  if (isWorkflowNode(node)) {
    if (node.input) sources.push(node.input);
    if (node.fan_out) sources.push(node.fan_out.items);
  }
  if (isCancelNode(node)) sources.push(node.cancel);
  if (isApprovalNode(node)) sources.push(node.approval.message);
  if (isLoopNode(node)) {
    if (typeof node.loop.prompt === 'string') sources.push(stripMarkdownCode(node.loop.prompt));
    if (node.loop.until_bash) sources.push(node.loop.until_bash);
  }
  if (isLoopGroupNode(node) && node.loop_group.until_bash) sources.push(node.loop_group.until_bash);
  return sources;
}

function validateOutputRefs(
  nodes: DagNode[],
  ids: ReadonlySet<string>,
  enclosingIds?: ReadonlySet<string>
): string | null {
  const outputRefPattern = new RegExp(OUTPUT_REF_SOURCE, 'g');
  for (const node of nodes) {
    for (const source of collectOutputRefSources(node)) {
      let m: RegExpExecArray | null;
      outputRefPattern.lastIndex = 0;
      while ((m = outputRefPattern.exec(source)) !== null) {
        const refNodeId = m[1];
        if (refNodeId !== undefined && !ids.has(refNodeId) && !enclosingIds?.has(refNodeId)) {
          return `Node '${node.id}' references unknown node '$${refNodeId}.output'`;
        }
      }
    }
  }
  return null;
}

function transitiveDepsOf(nodeId: string, directDeps: ReadonlyMap<string, string[]>): Set<string> {
  const seen = new Set<string>();
  const stack = [...(directDeps.get(nodeId) ?? [])];
  while (stack.length > 0) {
    const dep = stack.pop();
    if (dep === undefined || seen.has(dep)) continue;
    seen.add(dep);
    stack.push(...(directDeps.get(dep) ?? []));
  }
  return seen;
}

function validateFanOutItemDeps(nodes: DagNode[]): string | null {
  const directDeps = new Map<string, string[]>(nodes.map(n => [n.id, n.depends_on ?? []]));
  for (const node of nodes) {
    if (!isWorkflowNode(node) || !node.fan_out) continue;
    const refMatch = new RegExp(OUTPUT_REF_SOURCE).exec(node.fan_out.items);
    const producerId = refMatch?.[1];
    if (producerId === undefined) continue;
    if (!transitiveDepsOf(node.id, directDeps).has(producerId)) {
      return `Node '${node.id}' fan_out.items references '$${producerId}.output', which is not an upstream dependency — add '${producerId}' to '${node.id}'.depends_on so its item array is produced first`;
    }
  }
  return null;
}

function validateLoopGroupBody(
  node: DagNode,
  ids: ReadonlySet<string>,
  enclosingIds?: ReadonlySet<string>
): string | null {
  if (!isLoopGroupNode(node)) return null;
  if (node.loop_group.nodes.find(isIncludeNode)) {
    return `loop_group '${node.id}' body: 'include' is not supported inside a loop_group body`;
  }
  if (node.loop_group.nodes.find(isWorkflowNode)) {
    return `loop_group '${node.id}' body: 'workflow' (sub-run) is not supported inside a loop_group body`;
  }
  const scopeIds = new Set([...(enclosingIds ?? []), ...ids]);
  const bodyError = validateDagStructure(node.loop_group.nodes, scopeIds);
  return bodyError ? `loop_group '${node.id}' body: ${bodyError}` : null;
}

function validateLoopGroupBodies(
  nodes: DagNode[],
  ids: ReadonlySet<string>,
  enclosingIds?: ReadonlySet<string>
): string | null {
  for (const node of nodes) {
    const error = validateLoopGroupBody(node, ids, enclosingIds);
    if (error) return error;
  }
  return null;
}

export function validateDagStructure(
  nodes: DagNode[],
  enclosingIds?: ReadonlySet<string>
): string | null {
  const { ids, error: idError } = collectDagNodeIds(nodes, enclosingIds);
  if (idError) return idError;

  const depError = validateDependsOnRefs(nodes, ids);
  if (depError) return depError;

  const cycleError = validateDagAcyclic(nodes);
  if (cycleError) return cycleError;

  const outputRefError = validateOutputRefs(nodes, ids, enclosingIds);
  if (outputRefError) return outputRefError;

  const fanOutError = validateFanOutItemDeps(nodes);
  if (fanOutError) return fanOutError;

  return validateLoopGroupBodies(nodes, ids, enclosingIds);
}

export type ParseResult =
  | { workflow: WorkflowDefinition; error: null; warnings: string[] }
  | { workflow: null; error: WorkflowLoadError; warnings?: never };

type RawWorkflow = Record<string, unknown>;

interface ParsedDagNodes {
  dagNodes: DagNode[];
  parseWarnings: string[];
}

interface WorkflowPolicyFields {
  interactive?: boolean;
  worktreePolicy?: { enabled?: boolean };
  containerPolicy?: { enabled?: boolean; write_back?: 'approve' | 'auto' };
  evidencePolicy?: WorkflowEvidencePolicy;
  hardenedPolicy?: WorkflowHardenedPolicy;
  budgetPolicy?: WorkflowBudgetPolicy;
  mutatesCheckout?: boolean;
  tags?: string[];
  requires?: WorkflowRequirement[];
  effort?: z.output<typeof effortLevelSchema>;
  thinking?: z.output<typeof thinkingConfigSchema>;
  fallbackModel?: string;
  betas?: string[];
  sandbox?: z.output<typeof sandboxSettingsSchema>;
}

function workflowValidationError(filename: string, error: string): ParseResult {
  return { workflow: null, error: { filename, error, errorType: 'validation_error' } };
}

function validateWorkflowHeader(raw: RawWorkflow, filename: string): ParseResult | null {
  if (!raw.name || typeof raw.name !== 'string') {
    getLog().warn({ filename }, 'workflow_missing_name');
    return workflowValidationError(filename, "Missing required field 'name'");
  }
  if (!raw.description || typeof raw.description !== 'string') {
    getLog().warn({ filename }, 'workflow_missing_description');
    return workflowValidationError(filename, "Missing required field 'description'");
  }
  return null;
}

function validateWorkflowNodeContainer(raw: RawWorkflow, filename: string): ParseResult | null {
  if (Array.isArray(raw.steps) && raw.steps.length > 0) {
    return workflowValidationError(
      filename,
      '`steps:` format has been removed. Workflows now use `nodes:` (DAG) format exclusively. Your bundled defaults are already updated — custom workflows need manual migration. See docs/sequential-dag-migration-guide.md for conversion patterns, or run: claude "Read docs/sequential-dag-migration-guide.md then convert .archon/workflows/<file> to nodes: format"'
    );
  }
  if (!Array.isArray(raw.nodes) || raw.nodes.length === 0) {
    getLog().warn({ filename }, 'workflow_missing_nodes');
    return workflowValidationError(filename, "Workflow must have 'nodes:' configuration");
  }
  return null;
}

function parseDagNodes(raw: RawWorkflow, filename: string): ParsedDagNodes | ParseResult {
  const rawNodes = raw.nodes as unknown[];
  const validationErrors: string[] = [];
  const parseWarnings: string[] = [];
  const dagNodes = rawNodes
    .map((n: unknown, i: number) => parseDagNode(n, i, validationErrors, parseWarnings))
    .filter((n): n is DagNode => n !== null);
  if (dagNodes.length !== rawNodes.length) {
    getLog().warn({ filename, validationErrors }, 'dag_node_validation_failed');
    return workflowValidationError(
      filename,
      `DAG node validation failed: ${validationErrors.join('; ')}`
    );
  }
  const structureError = validateDagStructure(dagNodes);
  if (structureError) {
    getLog().warn({ filename, structureError }, 'dag_structure_invalid');
    return workflowValidationError(filename, structureError);
  }
  return { dagNodes, parseWarnings };
}

function validateWorkflowProviders(
  filename: string,
  provider: string | undefined,
  dagNodes: readonly DagNode[]
): ParseResult | null {
  if (provider && !isRegisteredProvider(provider)) {
    return workflowValidationError(
      filename,
      `Unknown provider '${provider}'. Registered: ${getRegisteredProviders()
        .map(p => p.id)
        .join(', ')}`
    );
  }
  for (const node of dagNodes) {
    if (node.provider !== undefined && !isRegisteredProvider(node.provider)) {
      return workflowValidationError(
        filename,
        `Node '${node.id}': unknown provider '${node.provider}'. Registered: ${getRegisteredProviders()
          .map(p => p.id)
          .join(', ')}`
      );
    }
  }
  return null;
}

function validatePersistSessionCapabilities(
  filename: string,
  dagNodes: readonly DagNode[],
  provider: string | undefined,
  workflowPersistSessions: boolean
): ParseResult | null {
  for (const node of dagNodes) {
    if (!isPersistableNode(node)) continue;
    if ('context' in node && node.context === 'fresh') continue;
    const nodePersist = 'persist_session' in node ? node.persist_session : undefined;
    const effectivePersist = nodePersist ?? workflowPersistSessions;
    if (!effectivePersist) continue;
    const explicitProvider = ('provider' in node ? node.provider : undefined) ?? provider;
    if (explicitProvider && isRegisteredProvider(explicitProvider)) {
      const caps = getProviderCapabilities(explicitProvider);
      if (!caps.sessionResume) {
        return workflowValidationError(
          filename,
          `Node '${node.id}' has persist_session: true but provider '${explicitProvider}' does not support sessionResume. Remove persist_session, or use a provider with sessionResume capability.`
        );
      }
    }
  }
  return null;
}

function hasInteractiveLoop(nodes: readonly DagNode[]): boolean {
  return nodes.some(
    n =>
      (isLoopNode(n) && n.loop.interactive === true) ||
      (isLoopGroupNode(n) &&
        (n.loop_group.interactive === true || hasInteractiveLoop(n.loop_group.nodes)))
  );
}

function hasSignalCompletesWithoutInteractive(nodes: readonly DagNode[]): boolean {
  return nodes.some(
    n =>
      (isLoopNode(n) && n.loop.signal_completes === true && n.loop.interactive !== true) ||
      (isLoopGroupNode(n) &&
        ((n.loop_group.signal_completes === true && n.loop_group.interactive !== true) ||
          hasSignalCompletesWithoutInteractive(n.loop_group.nodes)))
  );
}

function parseInteractivePolicy(
  raw: RawWorkflow,
  filename: string,
  dagNodes: readonly DagNode[]
): boolean | undefined {
  const interactive = typeof raw.interactive === 'boolean' ? raw.interactive : undefined;
  if (raw.interactive !== undefined && typeof raw.interactive !== 'boolean') {
    getLog().warn({ filename, value: raw.interactive }, 'invalid_interactive_value_ignored');
  }
  if (!interactive && hasInteractiveLoop(dagNodes)) {
    getLog().warn({ filename }, 'interactive_loop_in_non_interactive_workflow');
  }
  if (hasSignalCompletesWithoutInteractive(dagNodes)) {
    getLog().warn({ filename }, 'signal_completes_without_interactive_ignored');
  }
  return interactive;
}

function parseWorktreePolicy(
  raw: RawWorkflow,
  filename: string
): { enabled?: boolean } | undefined {
  if (raw.worktree === undefined) return undefined;
  if (typeof raw.worktree !== 'object' || raw.worktree === null || Array.isArray(raw.worktree)) {
    getLog().warn({ filename, value: raw.worktree }, 'invalid_worktree_block_ignored');
    return undefined;
  }
  const rawEnabled = (raw.worktree as Record<string, unknown>).enabled;
  if (typeof rawEnabled === 'boolean') return { enabled: rawEnabled };
  if (rawEnabled !== undefined) {
    getLog().warn({ filename, value: rawEnabled }, 'invalid_worktree_enabled_value_ignored');
  }
  return undefined;
}

function parseContainerPolicy(
  raw: RawWorkflow,
  filename: string
): { enabled?: boolean; write_back?: 'approve' | 'auto' } | undefined {
  if (raw.container === undefined) return undefined;
  if (typeof raw.container !== 'object' || raw.container === null || Array.isArray(raw.container)) {
    getLog().warn({ filename, value: raw.container }, 'invalid_container_block_ignored');
    return undefined;
  }
  const rawContainer = raw.container as Record<string, unknown>;
  const policy: { enabled?: boolean; write_back?: 'approve' | 'auto' } = {};
  if (typeof rawContainer.enabled === 'boolean') policy.enabled = rawContainer.enabled;
  else if (rawContainer.enabled !== undefined) {
    getLog().warn(
      { filename, value: rawContainer.enabled },
      'invalid_container_enabled_value_ignored'
    );
  }
  if (rawContainer.write_back === 'approve' || rawContainer.write_back === 'auto') {
    policy.write_back = rawContainer.write_back;
  } else if (rawContainer.write_back !== undefined) {
    getLog().warn(
      { filename, value: rawContainer.write_back },
      'invalid_container_write_back_value_ignored'
    );
  }
  return policy.enabled !== undefined || policy.write_back !== undefined ? policy : undefined;
}

function parseStrictWorkflowPolicy<T>(
  rawValue: unknown,
  schema: z.ZodType<T>,
  filename: string,
  message: string
): T | ParseResult | undefined {
  if (rawValue === undefined) return undefined;
  const parsed = schema.safeParse(rawValue);
  if (parsed.success) return parsed.data;
  return workflowValidationError(filename, message);
}

function parseMutatesCheckout(raw: RawWorkflow, filename: string): boolean | undefined {
  if (raw.mutates_checkout === undefined) return undefined;
  if (typeof raw.mutates_checkout === 'boolean') return raw.mutates_checkout;
  getLog().warn(
    { filename, value: raw.mutates_checkout },
    'invalid_mutates_checkout_value_ignored'
  );
  return undefined;
}

function parseTags(raw: RawWorkflow, filename: string): string[] | undefined {
  if (Array.isArray(raw.tags)) {
    return [
      ...new Set(
        raw.tags
          .filter((t): t is string => typeof t === 'string')
          .map(t => t.trim())
          .filter(t => t.length > 0)
      ),
    ];
  }
  if (raw.tags !== undefined)
    getLog().warn({ filename, value: raw.tags }, 'invalid_tags_block_ignored');
  return undefined;
}

function parseRequires(raw: RawWorkflow, filename: string): WorkflowRequirement[] | undefined {
  if (!Array.isArray(raw.requires)) {
    if (raw.requires !== undefined) {
      getLog().warn({ filename, value: raw.requires }, 'invalid_workflow_requires_block_ignored');
    }
    return undefined;
  }
  const valid: WorkflowRequirement[] = [];
  for (const entry of raw.requires) {
    const parsed = workflowRequirementSchema.safeParse(entry);
    if (parsed.success) valid.push(parsed.data);
    else getLog().warn({ filename, value: entry }, 'invalid_workflow_requires_entry_ignored');
  }
  const deduped = [...new Set(valid)];
  return deduped.length > 0 ? deduped : undefined;
}

function parseFallbackModel(raw: RawWorkflow, filename: string): string | undefined {
  const trimmed = typeof raw.fallbackModel === 'string' ? raw.fallbackModel.trim() : '';
  const fallbackModel = trimmed.length > 0 ? trimmed : undefined;
  if (raw.fallbackModel !== undefined && fallbackModel === undefined) {
    getLog().warn(
      { filename, value: raw.fallbackModel, expected: 'non-empty string' },
      'invalid_workflow_fallback_model_value_ignored'
    );
  }
  return fallbackModel;
}

function parseBetas(raw: RawWorkflow, filename: string): string[] | undefined {
  if (raw.betas === undefined) return undefined;
  const cleaned = Array.isArray(raw.betas)
    ? raw.betas
        .filter((b): b is string => typeof b === 'string')
        .map(b => b.trim())
        .filter(b => b.length > 0)
    : [];
  const betasResult = betasSchema.safeParse(cleaned);
  if (betasResult.success) return betasResult.data;
  getLog().warn({ filename, value: raw.betas }, 'invalid_workflow_betas_value_ignored');
  return undefined;
}

function parseWorkflowPolicies(
  raw: RawWorkflow,
  filename: string,
  dagNodes: readonly DagNode[]
): WorkflowPolicyFields | ParseResult {
  const evidencePolicy = parseStrictWorkflowPolicy(
    raw.evidence_policy,
    workflowEvidencePolicySchema,
    filename,
    "Invalid evidence_policy: expected { required: boolean }. When required is true, the run is refused terminal 'completed' unless $ARTIFACTS_DIR/evidence.json exists."
  );
  if (evidencePolicy && 'error' in evidencePolicy) return evidencePolicy;
  const hardenedPolicy = parseStrictWorkflowPolicy(
    raw.hardened,
    workflowHardenedPolicySchema,
    filename,
    'Invalid hardened policy: expected exactly { required: boolean }. Unsupported hardened fields are rejected so security settings cannot be silently dropped.'
  );
  if (hardenedPolicy && 'error' in hardenedPolicy) return hardenedPolicy;
  const budgetPolicy = parseStrictWorkflowPolicy(
    raw.budget,
    workflowBudgetPolicySchema,
    filename,
    'Invalid budget policy: expected exactly { required: boolean }. Unsupported budget fields are rejected so durable budget requirements cannot be silently dropped.'
  );
  if (budgetPolicy && 'error' in budgetPolicy) return budgetPolicy;

  return {
    interactive: parseInteractivePolicy(raw, filename, dagNodes),
    worktreePolicy: parseWorktreePolicy(raw, filename),
    containerPolicy: parseContainerPolicy(raw, filename),
    evidencePolicy,
    hardenedPolicy,
    budgetPolicy,
    mutatesCheckout: parseMutatesCheckout(raw, filename),
    tags: parseTags(raw, filename),
    requires: parseRequires(raw, filename),
    effort: parseOptionalField(
      raw.effort,
      effortLevelSchema,
      filename,
      'invalid_workflow_effort_value_ignored',
      {
        valid: effortLevelSchema.options,
      }
    ),
    thinking: parseOptionalField(
      raw.thinking,
      thinkingConfigSchema,
      filename,
      'invalid_workflow_thinking_value_ignored'
    ),
    fallbackModel: parseFallbackModel(raw, filename),
    betas: parseBetas(raw, filename),
    sandbox: parseOptionalField(
      raw.sandbox,
      sandboxSettingsSchema,
      filename,
      'invalid_workflow_sandbox_value_ignored'
    ),
  };
}

function collectWorkflowUnknownKeyWarnings(raw: RawWorkflow, parseWarnings: string[]): void {
  const workflowName = raw.name as string;
  const workflowLabel = `Workflow '${workflowName}'`;
  for (const key of Object.keys(raw)) {
    if (!KNOWN_WORKFLOW_KEYS.has(key)) {
      const hint = KNOWN_DAG_NODE_KEYS.has(key)
        ? ` ('${key}' is valid on individual nodes, not at workflow level.)`
        : '';
      pushUnknownKeyWarning(
        workflowName,
        workflowLabel,
        key,
        hint,
        'workflow_unknown_key_ignored',
        parseWarnings
      );
      continue;
    }
    const nested = KNOWN_WORKFLOW_NESTED_KEYS.get(key);
    if (nested) {
      collectUnknownConfigKeys(
        raw[key],
        nested,
        workflowName,
        workflowLabel,
        `${key}.`,
        'workflow_unknown_key_ignored',
        parseWarnings
      );
    }
  }
}

function buildWorkflowDefinition(
  raw: RawWorkflow,
  dagNodes: DagNode[],
  provider: string | undefined,
  model: string | undefined,
  modelReasoningEffort: z.output<typeof modelReasoningEffortSchema> | undefined,
  webSearchMode: z.output<typeof webSearchModeSchema> | undefined,
  workflowPersistSessions: boolean,
  policies: WorkflowPolicyFields
): WorkflowDefinition {
  return {
    name: raw.name as string,
    description: raw.description as string,
    provider,
    model,
    modelReasoningEffort,
    webSearchMode,
    interactive: policies.interactive,
    ...(policies.mutatesCheckout !== undefined
      ? { mutates_checkout: policies.mutatesCheckout }
      : {}),
    ...(policies.effort !== undefined ? { effort: policies.effort } : {}),
    ...(policies.thinking !== undefined ? { thinking: policies.thinking } : {}),
    ...(policies.fallbackModel !== undefined ? { fallbackModel: policies.fallbackModel } : {}),
    ...(policies.betas !== undefined ? { betas: policies.betas } : {}),
    ...(policies.sandbox !== undefined ? { sandbox: policies.sandbox } : {}),
    ...(workflowPersistSessions ? { persist_sessions: true } : {}),
    nodes: dagNodes,
    ...(policies.worktreePolicy ? { worktree: policies.worktreePolicy } : {}),
    ...(policies.containerPolicy ? { container: policies.containerPolicy } : {}),
    ...(policies.evidencePolicy !== undefined ? { evidence_policy: policies.evidencePolicy } : {}),
    ...(policies.hardenedPolicy !== undefined ? { hardened: policies.hardenedPolicy } : {}),
    ...(policies.budgetPolicy !== undefined ? { budget: policies.budgetPolicy } : {}),
    ...(policies.tags !== undefined ? { tags: policies.tags } : {}),
    ...(policies.requires !== undefined ? { requires: policies.requires } : {}),
  };
}

/**
 * Parse and validate a workflow YAML file
 */
export function parseWorkflow(content: string, filename: string): ParseResult {
  try {
    const raw = parseYaml(content) as RawWorkflow;
    if (!raw || typeof raw !== 'object') {
      return workflowValidationError(filename, 'YAML file is empty or does not contain an object');
    }
    const headerError = validateWorkflowHeader(raw, filename);
    if (headerError) return headerError;
    const nodeContainerError = validateWorkflowNodeContainer(raw, filename);
    if (nodeContainerError) return nodeContainerError;

    const parsedNodes = parseDagNodes(raw, filename);
    if ('error' in parsedNodes) return parsedNodes;
    const { dagNodes, parseWarnings } = parsedNodes;

    const provider =
      typeof raw.provider === 'string' && raw.provider.length > 0 ? raw.provider : undefined;
    const model = typeof raw.model === 'string' ? raw.model : undefined;
    const providerError = validateWorkflowProviders(filename, provider, dagNodes);
    if (providerError) return providerError;

    const workflowPersistSessions = raw.persist_sessions === true;
    const persistError = validatePersistSessionCapabilities(
      filename,
      dagNodes,
      provider,
      workflowPersistSessions
    );
    if (persistError) return persistError;

    const modelReasoningEffort = parseOptionalField(
      raw.modelReasoningEffort,
      modelReasoningEffortSchema,
      filename,
      'invalid_model_reasoning_effort',
      { valid: modelReasoningEffortSchema.options }
    );
    const webSearchMode = parseOptionalField(
      raw.webSearchMode,
      webSearchModeSchema,
      filename,
      'invalid_web_search_mode',
      { valid: webSearchModeSchema.options }
    );
    const policies = parseWorkflowPolicies(raw, filename, dagNodes);
    if ('error' in policies) return policies;
    collectWorkflowUnknownKeyWarnings(raw, parseWarnings);

    return {
      workflow: buildWorkflowDefinition(
        raw,
        dagNodes,
        provider,
        model,
        modelReasoningEffort,
        webSearchMode,
        workflowPersistSessions,
        policies
      ),
      error: null,
      warnings: parseWarnings,
    };
  } catch (error) {
    const err = error as Error;
    const linePattern = /line (\d+)/i;
    const lineMatch = linePattern.exec(err.message);
    const lineInfo = lineMatch ? ` (near line ${lineMatch[1]})` : '';
    getLog().error(
      {
        err,
        filename,
        lineInfo: lineInfo || undefined,
        contentPreview: content.slice(0, 200) + (content.length > 200 ? '...' : ''),
      },
      'workflow_parse_failed'
    );
    return {
      workflow: null,
      error: {
        filename,
        error: `YAML parse error${lineInfo}: ${err.message}`,
        errorType: 'parse_error',
      },
    };
  }
}

// ---------------------------------------------------------------------------
// parseNodeHooks is preserved as an export for backward compatibility
// (used by hooks.test.ts). The implementation now uses workflowNodeHooksSchema.
// ---------------------------------------------------------------------------

/**
 * Parse and validate per-node hooks from raw YAML input.
 * Uses workflowNodeHooksSchema internally.
 * Returns undefined for absent, empty, or invalid hooks.
 */
export function parseNodeHooks(
  raw: unknown,
  context: { id: string; errors: string[] }
): WorkflowNodeHooks | undefined {
  if (raw === undefined) return undefined;

  const result = workflowNodeHooksSchema.safeParse(raw);
  if (!result.success) {
    for (const issue of result.error.issues) {
      const pathStr = issue.path.length > 0 ? `'${issue.path.join('.')}' ` : '';
      context.errors.push(`'${context.id}': hooks ${pathStr}${issue.message}`);
    }
    return undefined;
  }

  // Filter out events with empty matcher arrays and return undefined for empty result
  // (preserves original behavior: hooks is only set when there are actual matchers)
  const filtered = Object.fromEntries(
    Object.entries(result.data).filter(
      ([, matchers]) => Array.isArray(matchers) && matchers.length > 0
    )
  ) as WorkflowNodeHooks;

  return Object.keys(filtered).length > 0 ? filtered : undefined;
}
