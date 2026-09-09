import { createHash } from 'node:crypto';
import type { ExecutionContext } from '@archon/providers/types';
import type { IWorkflowPlatform, WorkflowConfig } from './deps';
import type { ControllerActionNode, WorkflowRun } from './schemas';

import { CONTROLLER_ACTION_KINDS } from './schemas/dag-node';
import type { ControllerActionKind } from './schemas/dag-node';
export { CONTROLLER_ACTION_KINDS };
export type { ControllerActionKind };

export interface ControllerActionManifest {
  id: string;
  digest: string;
  input: Record<string, unknown>;
}

export type ControllerActionManifestDigestEnvelope = Pick<ControllerActionManifest, 'id' | 'input'>;

export interface ControllerActionGrant {
  runId: string;
  workflowName: string;
  workflowDigest: string;
  nodeId: string;
  action: ControllerActionKind;
  phase: string;
  actionManifest: ControllerActionManifest;
}

export interface ControllerActionHandlerContext {
  workflowRun: WorkflowRun;
  workflowName: string;
  workflowDigest: string;
  node: ControllerActionNode;
  actionManifest: ControllerActionManifest;
  signal: AbortSignal;
  deadlineAt: number;
  cwd: string;
  artifactsDir: string;
  stateDir: string;
  logDir: string;
  baseBranch: string;
  docsDir: string;
  config: WorkflowConfig;
  platform: IWorkflowPlatform;
  conversationId: string;
  execContext: ExecutionContext;
}

export type ControllerActionHandler = (
  ctx: ControllerActionHandlerContext
) => Promise<string | Record<string, unknown> | undefined>;

export type ControllerActionHandlers = Partial<
  Record<ControllerActionKind, ControllerActionHandler>
>;

export function isControllerActionKind(value: string): value is ControllerActionKind {
  return (CONTROLLER_ACTION_KINDS as readonly string[]).includes(value);
}

export function normalizeControllerActionOutput(
  output: string | Record<string, unknown> | undefined
): string {
  if (output === undefined) return '';
  if (typeof output === 'string') return output;
  return JSON.stringify(output);
}

function stableSerialize(value: unknown): string {
  if (value === undefined) return 'null';
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableSerialize).join(',')}]`;

  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .filter(key => record[key] !== undefined)
    .map(key => `${JSON.stringify(key)}:${stableSerialize(record[key])}`)
    .join(',')}}`;
}

function digestStable(value: unknown): string {
  return createHash('sha256').update(stableSerialize(value)).digest('hex');
}

function cloneManifestInput(input: Record<string, unknown>): Record<string, unknown> {
  return JSON.parse(JSON.stringify(input)) as Record<string, unknown>;
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const nested of Object.values(value as Record<string, unknown>)) {
      deepFreeze(nested);
    }
  }
  return value;
}

export function computeControllerActionManifestDigest(
  envelope: ControllerActionManifestDigestEnvelope
): string {
  assertValidManifestId(envelope.id);
  return digestStable({ id: envelope.id, input: envelope.input });
}

export function isControllerActionManifestSealed(manifest: ControllerActionManifest): boolean {
  return (
    isValidManifestId(manifest.id) &&
    manifest.digest ===
      computeControllerActionManifestDigest({ id: manifest.id, input: manifest.input })
  );
}

export function sealControllerActionManifest(
  manifest: ControllerActionManifest
): ControllerActionManifest {
  if (!isControllerActionManifestSealed(manifest)) {
    throw new Error('Controller action manifest is not sealed.');
  }
  return deepFreeze({
    id: manifest.id,
    digest: manifest.digest,
    input: cloneManifestInput(manifest.input),
  });
}

export function computeControllerWorkflowDigest(workflow: unknown): string {
  return digestStable(workflow);
}

function isValidManifestId(id: string): boolean {
  return id.length > 0 && id.length <= 128 && id.trim() === id && /^[A-Za-z0-9._:-]+$/.test(id);
}

function assertValidManifestId(id: string): void {
  if (isValidManifestId(id)) return;
  throw new Error(`Invalid controller action manifest id '${id}'.`);
}
