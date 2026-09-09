import { createLogger } from '@archon/paths';

import type { MessageChunk, SendQueryOptions, TokenUsage } from '../../types';
import { getOrderedAgents, type NamedAgentConfig } from './agent-config';
import { errorMessage } from './errors';
import type { OpencodeClientLike } from './runtime';
import {
  abortableStream,
  createSessionPromptBody,
  promptSession,
  resolveSessionId,
} from './session';
import { normalizeTokens } from './tokens';

interface ProviderModel {
  providerID: string;
  modelID: string;
}

interface AgentRunState {
  agent: NamedAgentConfig;
  cwd: string;
  sessionId: string;
  chunks: MessageChunk[];
  latestAssistantInfo?: Record<string, unknown>;
  lastAssistantMessageId?: string;
  done: boolean;
}

let cachedLog: ReturnType<typeof createLogger> | undefined;

function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.opencode');
  return cachedLog;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
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

function withAgentNodeConfig(
  requestOptions: SendQueryOptions | undefined,
  agent: NamedAgentConfig
): SendQueryOptions | undefined {
  if (!requestOptions) {
    return {
      nodeConfig: {
        agents: { [agent.key]: agent.config },
      },
    };
  }
  return {
    ...requestOptions,
    nodeConfig: {
      ...(requestOptions.nodeConfig ?? {}),
      agents: { [agent.key]: agent.config },
    },
  };
}

function formatBufferedAssistantOutput(states: AgentRunState[]): string {
  return states
    .map(state => {
      const assistantText = state.chunks
        .filter(
          (chunk): chunk is Extract<MessageChunk, { type: 'assistant' }> =>
            chunk.type === 'assistant'
        )
        .map(chunk => chunk.content)
        .join('');
      const thinkingText = state.chunks
        .filter(
          (chunk): chunk is Extract<MessageChunk, { type: 'thinking' }> => chunk.type === 'thinking'
        )
        .map(chunk => chunk.content)
        .join('');
      const sections: string[] = [`## ${state.agent.key}`];
      if (thinkingText) {
        sections.push(`<thinking>\n${thinkingText}\n</thinking>`);
      }
      sections.push(assistantText || '(no output)');
      return sections.join('\n\n');
    })
    .join('\n\n---\n\n');
}

function collectToolChunksForEmission(states: AgentRunState[]): MessageChunk[] {
  return states.flatMap(state =>
    state.chunks.filter(chunk => chunk.type === 'tool' || chunk.type === 'tool_result')
  );
}

interface MultiAgentRawEvent {
  type?: string;
  properties?: Record<string, unknown>;
}

interface MultiAgentStreamState {
  aborted: boolean;
  eventCount: number;
}

function logMultiAgentEvent(nodeId: string, rawEvent: unknown, state: MultiAgentStreamState): void {
  state.eventCount++;
  if (state.eventCount > 5) return;
  getLog().info(
    { nodeId, eventCount: state.eventCount, eventType: (rawEvent as { type?: string })?.type },
    'opencode.multi_agent_event_received'
  );
}

async function abortMultiAgentSessions(
  client: OpencodeClientLike,
  sessionToAgent: Map<string, AgentRunState>
): Promise<void> {
  await Promise.all(
    Array.from(sessionToAgent.values()).map(state =>
      client.session
        .abort({ path: { id: state.sessionId }, query: { directory: state.cwd } })
        .catch(error => {
          getLog().debug(
            { err: error, sessionId: state.sessionId, agent: state.agent.key },
            'opencode.multi_agent_abort_failed'
          );
        })
    )
  );
}

async function createMultiAgentStates(
  client: OpencodeClientLike,
  cwd: string,
  agents: NamedAgentConfig[],
  sessionToAgent: Map<string, AgentRunState>
): Promise<AgentRunState[]> {
  const states = await Promise.all(
    agents.map(async agent => {
      const { sessionId } = await resolveSessionId(client, cwd, undefined);
      getLog().info({ agent: agent.key, sessionId, cwd }, 'opencode.multi_agent_session_created');
      const state: AgentRunState = { agent, cwd, sessionId, chunks: [], done: false };
      sessionToAgent.set(sessionId, state);
      return state;
    })
  );
  return states;
}

async function promptMultiAgentStates(
  client: OpencodeClientLike,
  cwd: string,
  prompt: string,
  model: ProviderModel,
  requestOptions: SendQueryOptions | undefined,
  states: AgentRunState[]
): Promise<void> {
  await Promise.all(
    states.map(async state => {
      const agentRequestOptions = withAgentNodeConfig(requestOptions, state.agent);
      const promptBody = createSessionPromptBody(prompt, model, agentRequestOptions, state.agent);
      getLog().info(
        { agent: state.agent.key, sessionId: state.sessionId },
        'opencode.multi_agent_prompt_sending'
      );
      await promptSession(client, cwd, state.sessionId, promptBody);
      getLog().info(
        { agent: state.agent.key, sessionId: state.sessionId },
        'opencode.multi_agent_prompt_sent'
      );
    })
  );
}

function handleMultiAgentMessageUpdated(
  properties: Record<string, unknown>,
  sessionToAgent: Map<string, AgentRunState>
): void {
  const info = isRecord(properties.info) ? properties.info : undefined;
  const sessionId = typeof info?.sessionID === 'string' ? info.sessionID : undefined;
  const state = sessionId ? sessionToAgent.get(sessionId) : undefined;
  if (!state || info?.role !== 'assistant') return;
  state.latestAssistantInfo = info;
  if (typeof info.id === 'string') state.lastAssistantMessageId = info.id;
}

function scopedMultiAgentToolOutput(
  status: 'completed' | 'error',
  stateRecord: Record<string, unknown> | undefined
): string {
  if (status === 'completed')
    return typeof stateRecord?.output === 'string' ? stateRecord.output : '';
  return typeof stateRecord?.error === 'string' ? stateRecord.error : 'Tool failed';
}

function scopedMultiAgentToolChunks(
  agentKey: string,
  part: Record<string, unknown>,
  seenToolCalls: Set<string>,
  completedToolCalls: Set<string>
): MessageChunk[] {
  const rawCallId = typeof part.callID === 'string' ? part.callID : undefined;
  const scopedCallId = rawCallId ? `${agentKey}:${rawCallId}` : undefined;
  if (!scopedCallId) return [];
  const toolName = typeof part.tool === 'string' ? part.tool : 'unknown';
  const stateRecord = isRecord(part.state) ? part.state : undefined;
  const status = typeof stateRecord?.status === 'string' ? stateRecord.status : undefined;
  const chunks: MessageChunk[] = [];
  if (!seenToolCalls.has(scopedCallId)) {
    seenToolCalls.add(scopedCallId);
    const toolInput = isRecord(stateRecord?.input) ? stateRecord.input : undefined;
    chunks.push({
      type: 'tool',
      toolName,
      ...(toolInput ? { toolInput } : {}),
      toolCallId: scopedCallId,
    });
  }
  if (!completedToolCalls.has(scopedCallId) && (status === 'completed' || status === 'error')) {
    completedToolCalls.add(scopedCallId);
    chunks.push({
      type: 'tool_result',
      toolName,
      toolOutput: scopedMultiAgentToolOutput(status, stateRecord),
      toolCallId: scopedCallId,
      toolOutcome: status === 'completed' ? 'success' : 'error',
    });
  }
  return chunks;
}

function handleMultiAgentPartUpdated(
  properties: Record<string, unknown>,
  sessionToAgent: Map<string, AgentRunState>,
  seenToolCalls: Set<string>,
  completedToolCalls: Set<string>
): void {
  const part = isRecord(properties.part) ? properties.part : undefined;
  const sessionId = typeof part?.sessionID === 'string' ? part.sessionID : undefined;
  const state = sessionId ? sessionToAgent.get(sessionId) : undefined;
  if (!state || typeof part?.type !== 'string') return;
  if (part.type === 'text' || part.type === 'reasoning') {
    const delta = typeof properties.delta === 'string' ? properties.delta : undefined;
    const text = delta ?? (typeof part.text === 'string' ? part.text : '');
    if (text)
      state.chunks.push({ type: part.type === 'text' ? 'assistant' : 'thinking', content: text });
  } else if (part.type === 'tool') {
    state.chunks.push(
      ...scopedMultiAgentToolChunks(state.agent.key, part, seenToolCalls, completedToolCalls)
    );
  }
}

async function throwMultiAgentSessionError(
  client: OpencodeClientLike,
  properties: Record<string, unknown>,
  sessionToAgent: Map<string, AgentRunState>
): Promise<void> {
  const sessionId = typeof properties.sessionID === 'string' ? properties.sessionID : undefined;
  const state = sessionId ? sessionToAgent.get(sessionId) : undefined;
  if (!state) return;
  await abortMultiAgentSessions(client, sessionToAgent);
  const rawError = isRecord(properties.error) ? properties.error : properties;
  const err = new Error(`[${state.agent.key}] ${errorMessage(rawError)}`);
  err.cause = rawError;
  throw err;
}

function markMultiAgentIdle(
  nodeId: string,
  properties: Record<string, unknown>,
  states: AgentRunState[],
  sessionToAgent: Map<string, AgentRunState>
): boolean {
  const sessionId = typeof properties.sessionID === 'string' ? properties.sessionID : undefined;
  const state = sessionId ? sessionToAgent.get(sessionId) : undefined;
  if (!state) return false;
  state.done = true;
  getLog().info(
    {
      nodeId,
      agent: state.agent.key,
      sessionId,
      doneCount: states.filter(s => s.done).length,
      totalCount: states.length,
    },
    'opencode.multi_agent_session_idle'
  );
  return states.every(candidate => candidate.done);
}

function aggregateMultiAgentTokens(states: AgentRunState[]): TokenUsage | undefined {
  return states.reduce<TokenUsage | undefined>((acc, candidate) => {
    const next = normalizeTokens(candidate.latestAssistantInfo);
    if (!next) return acc;
    if (!acc) return { ...next };
    return {
      input: acc.input + next.input,
      output: acc.output + next.output,
      total: (acc.total ?? acc.input + acc.output) + (next.total ?? next.input + next.output),
      cost: (acc.cost ?? 0) + (next.cost ?? 0),
    };
  }, undefined);
}

async function readMultiAgentStructuredOutputs(
  client: OpencodeClientLike,
  states: AgentRunState[]
): Promise<Record<string, unknown> | undefined> {
  const results = await Promise.all(
    states.map(async state => {
      const output = await readStructuredOutput(
        client,
        state.cwd,
        state.sessionId,
        state.lastAssistantMessageId
      );
      return output !== undefined ? ([state.agent.key, output] as const) : undefined;
    })
  );
  const filtered: [string, unknown][] = [];
  for (const entry of results) {
    if (entry !== undefined) filtered.push([entry[0], entry[1]]);
  }
  return filtered.length > 0 ? Object.fromEntries(filtered) : undefined;
}

async function buildMultiAgentResultChunks(
  client: OpencodeClientLike,
  states: AgentRunState[]
): Promise<MessageChunk[]> {
  const structuredOutputs = await readMultiAgentStructuredOutputs(client, states);
  return [
    ...collectToolChunksForEmission(states),
    { type: 'assistant', content: formatBufferedAssistantOutput(states) },
    {
      type: 'result',
      ...(aggregateMultiAgentTokens(states) ? { tokens: aggregateMultiAgentTokens(states) } : {}),
      ...(structuredOutputs ? { structuredOutput: structuredOutputs } : {}),
    },
  ];
}

function throwMultiAgentAborted(
  nodeId: string,
  agentCount: number,
  cwd: string,
  requestOptions: SendQueryOptions | undefined
): never {
  const abortReason = requestOptions?.abortSignal?.reason;
  throw new Error(
    `OpenCode query aborted (nodeId: ${nodeId}, agents: ${agentCount}, cwd: ${cwd})` +
      (abortReason ? `: ${String(abortReason)}` : '')
  );
}

export async function* streamMultiAgentOpencodeSession(
  client: OpencodeClientLike,
  cwd: string,
  nodeId: string,
  prompt: string,
  model: ProviderModel,
  requestOptions: SendQueryOptions | undefined
): AsyncGenerator<MessageChunk> {
  const agents = getOrderedAgents(requestOptions?.nodeConfig);
  if (agents.length <= 1)
    throw new Error('streamMultiAgentOpencodeSession requires multiple agents');
  getLog().info({ nodeId, agentCount: agents.length, cwd }, 'opencode.multi_agent_starting');

  const events = await client.event.subscribe({ query: { directory: cwd } });
  getLog().info({ nodeId }, 'opencode.multi_agent_events_subscribed');
  const streamController = new AbortController();
  const streamState: MultiAgentStreamState = {
    aborted: requestOptions?.abortSignal?.aborted === true,
    eventCount: 0,
  };
  const seenToolCalls = new Set<string>();
  const completedToolCalls = new Set<string>();
  const sessionToAgent = new Map<string, AgentRunState>();

  const abortHandler = (): void => {
    streamState.aborted = true;
    void abortMultiAgentSessions(client, sessionToAgent);
    streamController.abort();
  };
  requestOptions?.abortSignal?.addEventListener('abort', abortHandler, { once: true });

  try {
    getLog().info({ nodeId }, 'opencode.multi_agent_creating_sessions');
    const states = await createMultiAgentStates(client, cwd, agents, sessionToAgent);
    getLog().info({ nodeId, sessionCount: states.length }, 'opencode.multi_agent_prompting');
    await promptMultiAgentStates(client, cwd, prompt, model, requestOptions, states);
    getLog().info({ nodeId }, 'opencode.multi_agent_all_prompts_sent');
    getLog().info({ nodeId }, 'opencode.multi_agent_listening');

    for await (const rawEvent of abortableStream(events.stream, streamController.signal)) {
      logMultiAgentEvent(nodeId, rawEvent, streamState);
      const event = rawEvent as MultiAgentRawEvent;
      const properties = isRecord(event.properties) ? event.properties : {};
      if (event.type === 'message.updated')
        handleMultiAgentMessageUpdated(properties, sessionToAgent);
      else if (event.type === 'message.part.updated')
        handleMultiAgentPartUpdated(properties, sessionToAgent, seenToolCalls, completedToolCalls);
      else if (event.type === 'session.error')
        await throwMultiAgentSessionError(client, properties, sessionToAgent);
      else if (
        event.type === 'session.idle' &&
        markMultiAgentIdle(nodeId, properties, states, sessionToAgent)
      ) {
        for (const chunk of await buildMultiAgentResultChunks(client, states)) yield chunk;
        getLog().info({ nodeId }, 'opencode.multi_agent_completed');
        return;
      }
    }

    getLog().info(
      { nodeId, aborted: streamState.aborted, eventCount: streamState.eventCount },
      'opencode.multi_agent_loop_exited'
    );
    if (streamState.aborted) throwMultiAgentAborted(nodeId, agents.length, cwd, requestOptions);
    throw new Error('OpenCode multi-agent stream ended before all agents completed');
  } finally {
    requestOptions?.abortSignal?.removeEventListener('abort', abortHandler);
    streamController.abort();
  }
}
