import { join } from 'node:path';

import { createLogger } from '@archon/paths';

import type {
  IAgentProvider,
  MessageChunk,
  ProviderCapabilities,
  SendQueryOptions,
} from '../../types';

import { getOrderedAgents } from './agent-config';
import { OPENCODE_CAPABILITIES } from './capabilities';
import { parseModelRef, parseOpencodeConfig } from './config';
import { classifyOpencodeError, enrichOpencodeError } from './errors';
import { materializeAgents } from './agent-fs';
import { streamMultiAgentOpencodeSession } from './multi-agent';
import {
  acquireEmbeddedRuntime,
  disposeInstanceForDirectory,
  releaseEmbeddedRuntime,
} from './runtime';
import { resolveSessionId, streamOpencodeSession } from './session';
import { withResumedOutcome, resumedOutcome } from '../../shared/resumed';

export { parseModelRef } from './config';
export { resetEmbeddedRuntime } from './runtime';

const MAX_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 2000;

let cachedLog: ReturnType<typeof createLogger> | undefined;

function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.opencode');
  return cachedLog;
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

type OpencodeModelRef = NonNullable<ReturnType<typeof parseModelRef>>;

interface OpencodeSendContext {
  prompt: string;
  cwd: string;
  resumeSessionId: string | undefined;
  requestOptions: SendQueryOptions | undefined;
  parsedModel: OpencodeModelRef;
  nodeAgents: NonNullable<SendQueryOptions['nodeConfig']>['agents'] | undefined;
  nodeId: string | undefined;
  orderedAgents: ReturnType<typeof getOrderedAgents>;
  hasAgentConfig: boolean;
  isMultiAgent: boolean;
  sessionCwd: string;
}

interface OpencodeRetryState {
  lastError: Error | undefined;
  recoveredAgentNotFound: boolean;
}

interface OpencodeRuntimeLease {
  client: import('./runtime').OpencodeClientLike;
  release: () => void;
}

function resolveOpencodeModel(requestOptions: SendQueryOptions | undefined): OpencodeModelRef {
  const assistantConfig = parseOpencodeConfig(requestOptions?.assistantConfig ?? {});
  const modelRef = requestOptions?.model ?? assistantConfig.model;
  const parsedModel = modelRef ? parseModelRef(modelRef) : undefined;
  if (modelRef && !parsedModel) {
    throw new Error(
      `Invalid OpenCode model ref: '${modelRef}'. Expected format '<provider>/<model>' (for example 'anthropic/claude-3-5-sonnet').`
    );
  }
  if (!parsedModel) {
    throw new Error(
      'OpenCode requires a model to be specified. ' +
        'Set model in assistants config (e.g., model: anthropic/claude-3-5-sonnet).'
    );
  }
  if (assistantConfig.baseUrl) {
    throw new Error(
      'OpenCode external baseUrl mode is no longer supported. ' +
        'Archon now requires managed embedded OpenCode runtime for fully controlled agent lifecycle.'
    );
  }
  return parsedModel;
}

function createOpencodeSendContext(
  prompt: string,
  cwd: string,
  resumeSessionId: string | undefined,
  requestOptions: SendQueryOptions | undefined
): OpencodeSendContext {
  const parsedModel = resolveOpencodeModel(requestOptions);
  const nodeAgents = requestOptions?.nodeConfig?.agents;
  const nodeId = requestOptions?.nodeConfig?.nodeId;
  const orderedAgents = getOrderedAgents(requestOptions?.nodeConfig);
  const hasAgentConfig = orderedAgents.length > 0;
  const isMultiAgent = orderedAgents.length > 1;
  const sessionCwd = hasAgentConfig && nodeId ? join(cwd, '.archon-opencode', nodeId) : cwd;
  return {
    prompt,
    cwd,
    resumeSessionId,
    requestOptions,
    parsedModel,
    nodeAgents,
    nodeId,
    orderedAgents,
    hasAgentConfig,
    isMultiAgent,
    sessionCwd,
  };
}

async function acquireOpencodeLease(
  abortSignal: AbortSignal | undefined
): Promise<OpencodeRuntimeLease> {
  const embedded = await acquireEmbeddedRuntime(abortSignal);
  return {
    client: embedded.client,
    release: (): void => {
      releaseEmbeddedRuntime(embedded);
    },
  };
}

async function materializeOpencodeAgents(
  ctx: OpencodeSendContext,
  runtime: OpencodeRuntimeLease
): Promise<void> {
  if (!ctx.hasAgentConfig) return;
  if (ctx.isMultiAgent) {
    await materializeAgents(ctx.sessionCwd, ctx.nodeAgents ?? {});
    await disposeInstanceForDirectory(runtime.client, ctx.sessionCwd);
    return;
  }
  if (!ctx.nodeAgents) return;
  await materializeAgents(ctx.sessionCwd, ctx.nodeAgents);
  await disposeInstanceForDirectory(runtime.client, ctx.sessionCwd);
}

async function* streamOpencodeAttempt(
  ctx: OpencodeSendContext,
  runtime: OpencodeRuntimeLease
): AsyncGenerator<MessageChunk> {
  await materializeOpencodeAgents(ctx, runtime);
  if (ctx.isMultiAgent) {
    if (!ctx.nodeId) {
      throw new Error(
        'OpenCode multi-agent execution requires a nodeId in nodeConfig. ' +
          'Ensure the workflow node sets nodeConfig.nodeId.'
      );
    }
    yield* withResumedOutcome(
      streamMultiAgentOpencodeSession(
        runtime.client,
        ctx.sessionCwd,
        ctx.nodeId,
        ctx.prompt,
        ctx.parsedModel,
        ctx.requestOptions
      ),
      resumedOutcome(ctx.resumeSessionId, false)
    );
    return;
  }
  const { sessionId, resumed } = await resolveSessionId(
    runtime.client,
    ctx.sessionCwd,
    ctx.resumeSessionId
  );
  if (ctx.resumeSessionId && !resumed) {
    yield {
      type: 'system',
      content: '⚠️ Could not resume OpenCode session. Starting fresh conversation.',
    };
  }
  yield* withResumedOutcome(
    streamOpencodeSession(
      runtime.client,
      ctx.sessionCwd,
      sessionId,
      ctx.prompt,
      ctx.parsedModel,
      ctx.requestOptions
    ),
    resumedOutcome(ctx.resumeSessionId, resumed)
  );
}

function shouldRetryOpencodeError(
  errorClass: ReturnType<typeof classifyOpencodeError>,
  ctx: OpencodeSendContext,
  retryState: OpencodeRetryState
): boolean {
  return (
    errorClass === 'rate_limit' ||
    errorClass === 'crash' ||
    (errorClass === 'agent_not_found' && ctx.hasAgentConfig && !retryState.recoveredAgentNotFound)
  );
}

async function handleOpencodeAttemptError(
  error: unknown,
  attempt: number,
  ctx: OpencodeSendContext,
  retryState: OpencodeRetryState,
  retryBaseDelayMs: number
): Promise<void> {
  const errorClass = classifyOpencodeError(
    error,
    ctx.requestOptions?.abortSignal?.aborted === true
  );
  const enrichedError = enrichOpencodeError(error, errorClass);
  getLog().error(
    { err: error, errorClass, attempt, maxRetries: MAX_RETRIES },
    'opencode.query_failed'
  );
  if (!shouldRetryOpencodeError(errorClass, ctx, retryState) || attempt >= MAX_RETRIES - 1)
    throw enrichedError;
  if (errorClass === 'agent_not_found') {
    retryState.recoveredAgentNotFound = true;
    getLog().info({ attempt, sessionCwd: ctx.sessionCwd }, 'opencode.retrying_after_agent_refresh');
  }
  const delayMs = retryBaseDelayMs * 2 ** attempt;
  getLog().info({ attempt, delayMs, errorClass }, 'opencode.retrying_query');
  await delay(delayMs);
  if (retryState.lastError) enrichedError.cause = retryState.lastError;
  retryState.lastError = enrichedError;
}

export class OpencodeProvider implements IAgentProvider {
  private readonly retryBaseDelayMs: number;

  constructor(options?: { retryBaseDelayMs?: number }) {
    this.retryBaseDelayMs = options?.retryBaseDelayMs ?? RETRY_BASE_DELAY_MS;
  }

  async *sendQuery(
    prompt: string,
    cwd: string,
    resumeSessionId?: string,
    requestOptions?: SendQueryOptions
  ): AsyncGenerator<MessageChunk> {
    const ctx = createOpencodeSendContext(prompt, cwd, resumeSessionId, requestOptions);
    const retryState: OpencodeRetryState = { lastError: undefined, recoveredAgentNotFound: false };

    for (let attempt = 0; attempt < MAX_RETRIES; attempt += 1) {
      if (requestOptions?.abortSignal?.aborted) throw new Error('OpenCode query aborted');
      const runtime = await acquireOpencodeLease(requestOptions?.abortSignal);
      try {
        yield* streamOpencodeAttempt(ctx, runtime);
        return;
      } catch (error) {
        await handleOpencodeAttemptError(error, attempt, ctx, retryState, this.retryBaseDelayMs);
      } finally {
        runtime.release();
      }
    }

    throw retryState.lastError ?? new Error(`OpenCode query failed after ${MAX_RETRIES} retries`);
  }

  getType(): string {
    return 'opencode';
  }

  getCapabilities(): ProviderCapabilities {
    return OPENCODE_CAPABILITIES;
  }
}
