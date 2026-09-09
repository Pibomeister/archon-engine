/**
 * Structural validation: node-id hygiene (non-empty, unique) and per-variant
 * required-field checks. Hand-rolled to match the engine's superRefine intent
 * without depending on a runtime schema.
 */
import type { BuilderNode, BuilderWorkflow, Issue } from '../types';
import { makeIssue } from './make-issue';

/** Empty (or whitespace-only) ids and duplicate ids across the node list. */
function checkIds(nodes: BuilderNode[]): Issue[] {
  const issues: Issue[] = [];
  const seen = new Set<string>();
  for (const node of nodes) {
    const id = node.id.trim();
    if (id.length === 0) {
      issues.push(
        makeIssue({
          rule: 'structural.id.empty',
          severity: 'error',
          source: 'client-instant',
          message: 'node id must not be empty',
          path: { nodeId: node.id, field: 'id' },
        })
      );
      continue;
    }
    if (seen.has(id)) {
      issues.push(
        makeIssue({
          rule: 'structural.id.duplicate',
          severity: 'error',
          source: 'client-instant',
          message: `duplicate node id '${id}'`,
          path: { nodeId: id, field: 'id' },
        })
      );
    }
    seen.add(id);
  }
  return issues;
}

function missingIssue(node: BuilderNode, field: string, message: string): Issue {
  return makeIssue({
    rule: 'structural.field.missing',
    severity: 'error',
    source: 'client-instant',
    message,
    path: { nodeId: node.id, field },
  });
}

function invalidIssue(node: BuilderNode, field: string, message: string): Issue {
  return makeIssue({
    rule: 'structural.field.invalid',
    severity: 'error',
    source: 'client-instant',
    message,
    path: { nodeId: node.id, field },
  });
}

function checkScriptRequiredFields(node: Extract<BuilderNode, { variant: 'script' }>): Issue[] {
  const issues: Issue[] = [];
  if (node.data.script.trim().length === 0) {
    issues.push(missingIssue(node, 'script', 'script must not be empty'));
  }
  if (node.data.runtime !== 'bun' && node.data.runtime !== 'uv') {
    issues.push(invalidIssue(node, 'runtime', "script requires runtime 'bun' or 'uv'"));
  }
  return issues;
}

function checkLoopRequiredFields(node: Extract<BuilderNode, { variant: 'loop' }>): Issue[] {
  const issues: Issue[] = [];
  if (node.data.prompt !== undefined && node.data.command !== undefined) {
    issues.push(
      invalidIssue(
        node,
        'loop.command',
        "loop accepts exactly one of 'prompt' or 'command', not both"
      )
    );
  } else if (node.data.command?.trim().length === 0) {
    issues.push(missingIssue(node, 'loop.command', 'loop requires a command name'));
  } else if ((node.data.prompt ?? '').trim().length === 0) {
    issues.push(missingIssue(node, 'loop.prompt', 'loop requires a prompt (or a command file)'));
  }

  if (node.data.until.trim().length === 0) {
    issues.push(missingIssue(node, 'loop.until', "loop requires an 'until' signal"));
  }
  if (!Number.isInteger(node.data.max_iterations) || node.data.max_iterations <= 0) {
    issues.push(
      invalidIssue(node, 'loop.max_iterations', 'loop requires a positive integer max_iterations')
    );
  }
  return issues;
}

/** Per-variant required-field checks (mirrors the engine's mode-field rules). */
function checkRequiredFields(node: BuilderNode): Issue[] {
  switch (node.variant) {
    case 'prompt':
      return node.data.prompt.trim().length === 0
        ? [missingIssue(node, 'prompt', 'prompt must not be empty')]
        : [];
    case 'command':
      return node.data.command.trim().length === 0
        ? [missingIssue(node, 'command', 'command must not be empty')]
        : [];
    case 'bash':
      return node.data.bash.trim().length === 0
        ? [missingIssue(node, 'bash', 'bash script must not be empty')]
        : [];
    case 'script':
      return checkScriptRequiredFields(node);
    case 'loop':
      return checkLoopRequiredFields(node);
    case 'approval':
      return node.data.message.trim().length === 0
        ? [missingIssue(node, 'approval.message', 'approval requires a message')]
        : [];
    case 'cancel':
      return node.data.reason.trim().length === 0
        ? [missingIssue(node, 'cancel', 'cancel requires a reason')]
        : [];
  }
}

/** Validate node-id hygiene and per-variant required fields. */
export function validateStructural(workflow: BuilderWorkflow): Issue[] {
  const issues: Issue[] = checkIds(workflow.nodes);
  for (const node of workflow.nodes) {
    issues.push(...checkRequiredFields(node));
  }
  return issues;
}
