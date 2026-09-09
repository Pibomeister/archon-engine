import type { WorkflowDefinition, DagNode } from '@/lib/api';
import { cn } from '@/lib/utils';

interface YamlCodeViewProps {
  definition: WorkflowDefinition | null;
  mode: 'split' | 'full';
}

/** Serialize a single value — handles strings with newlines, objects, arrays. */
function serializeValue(value: unknown, currentIndent: number): string {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (typeof value === 'boolean' || typeof value === 'number') {
    return String(value);
  }
  if (typeof value === 'string') {
    // Multi-line strings use block scalar
    if (value.includes('\n')) {
      const lines = value.split('\n');
      return '|\n' + lines.map(l => ' '.repeat(currentIndent + 2) + l).join('\n');
    }
    // Quote strings that could be ambiguous
    if (
      value === '' ||
      value === 'true' ||
      value === 'false' ||
      value === 'null' ||
      /^[\d.]+$/.test(value) ||
      value.includes(':') ||
      value.includes('#') ||
      value.includes('"') ||
      value.includes("'")
    ) {
      return JSON.stringify(value);
    }
    return value;
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    return (
      '\n' +
      value
        .map(v => ' '.repeat(currentIndent + 2) + '- ' + serializeValue(v, currentIndent + 4))
        .join('\n')
    );
  }
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const entries = Object.entries(obj).filter(([, v]) => v !== undefined);
    if (entries.length === 0) return '{}';
    return (
      '\n' +
      entries
        .map(
          ([k, v]) =>
            ' '.repeat(currentIndent + 2) + k + ': ' + serializeValue(v, currentIndent + 2)
        )
        .join('\n')
    );
  }
  // Fallback for unexpected types — should not be reached after all type guards above
  return JSON.stringify(value);
}

function appendScalarNodeFields(
  lines: string[],
  node: DagNode,
  pad: string,
  baseIndent: number
): void {
  const fields: [keyof DagNode, string, (value: unknown) => string][] = [
    ['command', 'command', (value): string => String(value)],
    ['prompt', 'prompt', (value): string => serializeValue(value, baseIndent + 2)],
    ['bash', 'bash', (value): string => serializeValue(value, baseIndent + 2)],
    ['timeout', 'timeout', (value): string => String(value)],
    ['when', 'when', (value): string => JSON.stringify(value)],
    ['trigger_rule', 'trigger_rule', (value): string => String(value)],
    ['provider', 'provider', (value): string => String(value)],
    ['model', 'model', (value): string => String(value)],
    ['context', 'context', (value): string => String(value)],
    ['output_format', 'output_format', (value): string => serializeValue(value, baseIndent + 2)],
    ['idle_timeout', 'idle_timeout', (value): string => String(value)],
    ['mcp', 'mcp', (value): string => String(value)],
  ];
  for (const [key, label, formatter] of fields) {
    const value = node[key];
    if (value !== undefined && value !== null && value !== '')
      lines.push(`${pad}  ${label}: ${formatter(value)}`);
  }
}

function appendStringList(
  lines: string[],
  pad: string,
  label: string,
  values: readonly string[] | undefined
): void {
  if (values === undefined || values.length === 0) return;
  lines.push(`${pad}  ${label}:`);
  for (const value of values) lines.push(`${pad}    - ${value}`);
}

function appendRetry(lines: string[], node: DagNode, pad: string): void {
  if (!node.retry) return;
  lines.push(`${pad}  retry:`);
  lines.push(`${pad}    max_attempts: ${node.retry.max_attempts}`);
  if (node.retry.delay_ms !== undefined) lines.push(`${pad}    delay_ms: ${node.retry.delay_ms}`);
  if (node.retry.on_error) lines.push(`${pad}    on_error: ${node.retry.on_error}`);
}

/** Serialize a DagNode to YAML-like lines. */
function serializeDagNode(node: DagNode, baseIndent: number): string {
  const lines: string[] = [];
  const pad = ' '.repeat(baseIndent);

  lines.push(`${pad}- id: ${node.id}`);
  appendScalarNodeFields(lines, node, pad, baseIndent);
  appendStringList(lines, pad, 'depends_on', node.depends_on);
  appendStringList(lines, pad, 'allowed_tools', node.allowed_tools);
  appendStringList(lines, pad, 'denied_tools', node.denied_tools);
  appendStringList(lines, pad, 'skills', node.skills);
  appendRetry(lines, node, pad);

  return lines.join('\n');
}

/** Convert a WorkflowDefinition into a YAML-like string for preview. */
export function serializeToYaml(def: WorkflowDefinition): string {
  const lines: string[] = [];

  lines.push(`name: ${def.name}`);
  if (def.description) {
    lines.push(`description: ${serializeValue(def.description, 0)}`);
  }

  if (def.provider) {
    lines.push(`provider: ${def.provider}`);
  }
  if (def.model) {
    lines.push(`model: ${def.model}`);
  }
  if (def.modelReasoningEffort) {
    lines.push(`modelReasoningEffort: ${def.modelReasoningEffort}`);
  }
  if (def.webSearchMode) {
    lines.push(`webSearchMode: ${def.webSearchMode}`);
  }

  lines.push('');

  lines.push('nodes:');
  for (const node of def.nodes) {
    lines.push(serializeDagNode(node, 2));
  }

  return lines.join('\n') + '\n';
}

export function YamlCodeView({ definition, mode }: YamlCodeViewProps): React.ReactElement {
  const yamlText = definition ? serializeToYaml(definition) : '';

  return (
    <div className="flex h-full flex-col bg-surface-inset">
      {mode === 'full' && (
        <div className="flex items-center border-b border-border px-3 py-2">
          <span className="text-xs text-text-tertiary">Read-only YAML preview</span>
        </div>
      )}
      <pre
        className={cn(
          'flex-1 overflow-auto p-4',
          'font-mono text-xs leading-relaxed text-text-primary',
          'whitespace-pre-wrap break-words'
        )}
      >
        {yamlText || '# No workflow definition'}
      </pre>
    </div>
  );
}
