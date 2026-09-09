import { createLogger } from '@archon/paths';

import type { MessageChunk, SendQueryOptions } from '../../types';

import {
  adaptNamedAgentForOpencode,
  resolvePromptForAgent,
  selectSingleAgent,
  type NamedAgentConfig,
} from './agent-config';
import { errorMessage } from './errors';
import type { OpencodeClientLike } from './runtime';
import { normalizeTokens } from './tokens';

let cachedLog: ReturnType<typeof createLogger> | undefined;

function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.opencode');
  return cachedLog;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

export async function resolveSessionId(
  client: OpencodeClientLike,
  cwd: string,
  resumeSessionId: string | undefined
): Promise<{ sessionId: string; resumed: boolean }> {
  if (resumeSessionId) {
    try {
      const existing = await client.session.get({
        path: { id: resumeSessionId },
        query: { directory: cwd },
      });
      const sessionId = existing.data?.id;
      if (typeof sessionId === 'string' && sessionId.length > 0) {
        return { sessionId, resumed: true };
      }
    } catch (error) {
      getLog().warn({ err: error, resumeSessionId, cwd }, 'opencode.session_resume_failed');
    }
  }

  const created = await client.session.create({ query: { directory: cwd } });
  const sessionId = created.data?.id;
  if (!sessionId) {
    throw new Error('OpenCode failed to create a session');
  }

  return { sessionId, resumed: false };
}

export function createSessionPromptBody(
  prompt: string,
  model: { providerID: string; modelID: string },
  requestOptions: SendQueryOptions | undefined,
  agentOverride?: NamedAgentConfig
): Record<string, unknown> {
  const singleAgent = agentOverride ?? selectSingleAgent(requestOptions?.nodeConfig?.agents);
  const adaptedAgentConfig = singleAgent ? adaptNamedAgentForOpencode(singleAgent) : undefined;
  const effectivePrompt = resolvePromptForAgent(singleAgent, prompt);
  const promptBody: Record<string, unknown> = {
    parts: [{ type: 'text', text: effectivePrompt }],
    model: adaptedAgentConfig?.model ?? model,
    ...(adaptedAgentConfig?.agent ? { agent: adaptedAgentConfig.agent } : {}),
    ...(adaptedAgentConfig?.tools ? { tools: adaptedAgentConfig.tools } : {}),
    ...(requestOptions?.systemPrompt ? { system: requestOptions.systemPrompt } : {}),
  };

  if (requestOptions?.outputFormat?.type === 'json_schema') {
    promptBody.format = {
      type: 'json_schema',
      schema: requestOptions.outputFormat.schema,
    };
  }

  return promptBody;
}

export async function promptSession(
  client: OpencodeClientLike,
  cwd: string,
  sessionId: string,
  promptBody: Record<string, unknown>
): Promise<void> {
  await client.session.promptAsync({
    path: { id: sessionId },
    query: { directory: cwd },
    body: promptBody,
  });
}

async function readStructuredOutput(
  client: OpencodeClientLike,
  cwd: string,
  sessionId: string,
  messageId: string | undefined
): Promise<unknown> {
  if (!messageId) return undefined;

  try {
    const response = await client.session.message({
      path: { id: sessionId, messageID: messageId },
      query: { directory: cwd },
    });
    const info = response.data?.info;
    if (isRecord(info) && 'structured_output' in info) {
      return info.structured_output;
    }
  } catch (error) {
    getLog().warn({ err: error, sessionId, messageId }, 'opencode.structured_output_lookup_failed');
  }

  return undefined;
}

interface OpencodeRawEvent {
  type?: string;
  properties?: Record<string, unknown>;
}

interface OpencodeSessionStreamState {
  latestAssistantInfo: Record<string, unknown> | undefined;
  lastAssistantMessageId: string | undefined;
  aborted: boolean;
  resultYielded: boolean;
}

function handleOpencodeMessageUpdated(
  sessionId: string,
  properties: Record<string, unknown>,
  state: OpencodeSessionStreamState
): void {
  const info = isRecord(properties.info) ? properties.info : undefined;
  if (info?.role !== 'assistant' || info.sessionID !== sessionId) return;
  state.latestAssistantInfo = info;
  if (typeof info.id === 'string') state.lastAssistantMessageId = info.id;
}

function textFromPart(properties: Record<string, unknown>, part: Record<string, unknown>): string {
  const delta = typeof properties.delta === 'string' ? properties.delta : undefined;
  return delta ?? (typeof part.text === 'string' ? part.text : '');
}

function opencodeToolResultChunk(
  callId: string,
  toolName: string,
  state: Record<string, unknown> | undefined,
  status: 'completed' | 'error'
): MessageChunk {
  return {
    type: 'tool_result',
    toolName,
    toolOutput:
      status === 'completed'
        ? typeof state?.output === 'string'
          ? state.output
          : ''
        : typeof state?.error === 'string'
          ? state.error
          : 'Tool failed',
    toolCallId: callId,
    toolOutcome: status === 'completed' ? 'success' : 'error',
  };
}

function opencodeToolChunks(
  part: Record<string, unknown>,
  seenToolCalls: Set<string>,
  completedToolCalls: Set<string>
): MessageChunk[] {
  const callId = typeof part.callID === 'string' ? part.callID : undefined;
  const toolName = typeof part.tool === 'string' ? part.tool : 'unknown';
  const state = isRecord(part.state) ? part.state : undefined;
  const status = typeof state?.status === 'string' ? state.status : undefined;
  const chunks: MessageChunk[] = [];
  if (callId && !seenToolCalls.has(callId)) {
    seenToolCalls.add(callId);
    const toolInput = isRecord(state?.input) ? state.input : undefined;
    chunks.push({
      type: 'tool',
      toolName,
      ...(toolInput ? { toolInput } : {}),
      toolCallId: callId,
    });
  }
  if (callId && !completedToolCalls.has(callId) && (status === 'completed' || status === 'error')) {
    completedToolCalls.add(callId);
    chunks.push(opencodeToolResultChunk(callId, toolName, state, status));
  }
  return chunks;
}

function opencodePartChunks(
  sessionId: string,
  properties: Record<string, unknown>,
  seenToolCalls: Set<string>,
  completedToolCalls: Set<string>
): MessageChunk[] {
  const part = isRecord(properties.part) ? properties.part : undefined;
  if (part?.sessionID !== sessionId || typeof part.type !== 'string') return [];
  if (part.type === 'text') {
    const text = textFromPart(properties, part);
    return text ? [{ type: 'assistant', content: text }] : [];
  }
  if (part.type === 'reasoning') {
    const text = textFromPart(properties, part);
    return text ? [{ type: 'thinking', content: text }] : [];
  }
  if (part.type === 'tool') return opencodeToolChunks(part, seenToolCalls, completedToolCalls);
  return [];
}

function throwIfOpencodeSessionError(sessionId: string, properties: Record<string, unknown>): void {
  const eventSessionId =
    typeof properties.sessionID === 'string' ? properties.sessionID : undefined;
  if (eventSessionId && eventSessionId !== sessionId) return;
  const rawError = isRecord(properties.error) ? properties.error : properties;
  const err = new Error(errorMessage(rawError));
  err.cause = rawError;
  throw err;
}

async function buildOpencodeIdleResult(
  client: OpencodeClientLike,
  cwd: string,
  sessionId: string,
  state: OpencodeSessionStreamState
): Promise<MessageChunk> {
  const structuredOutput = await readStructuredOutput(
    client,
    cwd,
    sessionId,
    state.lastAssistantMessageId
  );
  const tokens = normalizeTokens(state.latestAssistantInfo);
  return {
    type: 'result',
    sessionId,
    ...(tokens ? { tokens } : {}),
    ...(structuredOutput !== undefined ? { structuredOutput } : {}),
    ...(typeof state.latestAssistantInfo?.cost === 'number'
      ? { cost: state.latestAssistantInfo.cost }
      : {}),
    ...(typeof state.latestAssistantInfo?.finish === 'string'
      ? { stopReason: state.latestAssistantInfo.finish }
      : {}),
    ...(typeof state.latestAssistantInfo?.modelID === 'string' &&
    state.latestAssistantInfo.modelID.length > 0
      ? { resolvedModel: { id: state.latestAssistantInfo.modelID } }
      : {}),
  };
}

function throwOpencodeAborted(
  sessionId: string,
  cwd: string,
  requestOptions: SendQueryOptions | undefined
): never {
  const abortReason = requestOptions?.abortSignal?.reason;
  throw new Error(
    `OpenCode query aborted (session: ${sessionId}, cwd: ${cwd})` +
      (abortReason ? `: ${String(abortReason)}` : '')
  );
}

export async function* streamOpencodeSession(
  client: OpencodeClientLike,
  cwd: string,
  sessionId: string,
  prompt: string,
  model: { providerID: string; modelID: string },
  requestOptions: SendQueryOptions | undefined
): AsyncGenerator<MessageChunk> {
  const events = await client.event.subscribe({ query: { directory: cwd } });
  const streamController = new AbortController();
  const seenToolCalls = new Set<string>();
  const completedToolCalls = new Set<string>();
  const state: OpencodeSessionStreamState = {
    latestAssistantInfo: undefined,
    lastAssistantMessageId: undefined,
    aborted: requestOptions?.abortSignal?.aborted === true,
    resultYielded: false,
  };

  const abortHandler = (): void => {
    state.aborted = true;
    void client.session
      .abort({ path: { id: sessionId }, query: { directory: cwd } })
      .catch((error): void => {
        getLog().debug({ err: error, sessionId }, 'opencode.session_abort_failed');
      });
    streamController.abort();
  };

  requestOptions?.abortSignal?.addEventListener('abort', abortHandler, { once: true });

  try {
    const promptBody = createSessionPromptBody(prompt, model, requestOptions);
    await promptSession(client, cwd, sessionId, promptBody);

    for await (const rawEvent of abortableStream(events.stream, streamController.signal)) {
      const event = rawEvent as OpencodeRawEvent;
      const properties = isRecord(event.properties) ? event.properties : {};
      if (event.type === 'message.updated')
        handleOpencodeMessageUpdated(sessionId, properties, state);
      else if (event.type === 'message.part.updated') {
        for (const chunk of opencodePartChunks(
          sessionId,
          properties,
          seenToolCalls,
          completedToolCalls
        ))
          yield chunk;
      } else if (event.type === 'session.error') throwIfOpencodeSessionError(sessionId, properties);
      else if (event.type === 'session.idle' && properties.sessionID === sessionId) {
        yield await buildOpencodeIdleResult(client, cwd, sessionId, state);
        state.resultYielded = true;
        return;
      }
    }

    if (!state.resultYielded && !state.aborted) yield { type: 'result', sessionId };
    if (state.aborted) throwOpencodeAborted(sessionId, cwd, requestOptions);
  } finally {
    requestOptions?.abortSignal?.removeEventListener('abort', abortHandler);
    streamController.abort();
  }
}
export async function* abortableStream(
  stream: AsyncIterable<unknown>,
  signal: AbortSignal
): AsyncGenerator<unknown, void, unknown> {
  const iterator = stream[Symbol.asyncIterator]();

  while (true) {
    if (signal.aborted) {
      await iterator.return?.().catch(() => undefined);
      return;
    }

    const nextPromise = iterator.next();
    const result = await Promise.race([
      nextPromise,
      new Promise<IteratorResult<unknown>>(resolve => {
        const onAbort = (): void => {
          signal.removeEventListener('abort', onAbort);
          resolve({ done: true, value: undefined });
        };
        signal.addEventListener('abort', onAbort, { once: true });
        void nextPromise.finally((): void => {
          signal.removeEventListener('abort', onAbort);
        });
      }),
    ]);

    if (result.done) {
      await iterator.return?.().catch(() => undefined);
      return;
    }
    yield result.value;
  }
}
