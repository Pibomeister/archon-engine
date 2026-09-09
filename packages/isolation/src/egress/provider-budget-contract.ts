export type TrustedProvider = 'openai' | 'anthropic';

export interface TrustedProviderBudgetPolicy {
  provider: TrustedProvider;
  host: string;
  model: string;
  maxInputTokens: number;
  maxOutputTokens: number;
  anthropicBeta?: boolean;
  allowedHeaders?: Record<string, string | readonly string[]>;
}

export interface TrustedProviderRequest {
  method: string;
  host: string;
  path: string;
  body: unknown;
  anthropicBeta?: boolean;
  headers?: Record<string, string>;
}

export interface ProviderTokenReservation {
  input: number;
  output: number;
}

export interface PreparedTrustedProviderRequest {
  body: Buffer;
  reservation: ProviderTokenReservation;
}

export interface ProviderTokenUsage {
  input: number;
  output: number;
  total: number;
}

export type ProviderUsageSettlement =
  | { state: 'complete'; usage: ProviderTokenUsage }
  | { state: 'unknown'; reason: string };

interface SseOptions {
  maxBytes?: number;
}

interface SseEvent {
  event: string | undefined;
  data: string;
}

interface AnthropicStreamState {
  input?: number;
  output?: number;
  sawStart: boolean;
  sawStop: boolean;
  unsafeReason?: string;
}

const DEFAULT_SSE_MAX_BYTES = 262_144;
const MAX_JSON_DEPTH = 12;
const MAX_NATIVE_TOOL_PAYLOAD_BYTES = 65_536;
const OPENAI_REQUEST_FIELDS = [
  'client_metadata',
  'include',
  'input',
  'max_output_tokens',
  'model',
  'parallel_tool_calls',
  'prompt_cache_key',
  'reasoning',
  'store',
  'stream',
  'text',
  'tool_choice',
  'tools',
  'truncation',
];
const ANTHROPIC_REQUEST_FIELDS = [
  'max_tokens',
  'messages',
  'metadata',
  'model',
  'output_config',
  'stream',
  'system',
  'thinking',
  'tools',
];
const PROVIDER_HEADER_PREFIXES = ['anthropic-', 'openai-'];
const MODELED_PROVIDER_HEADERS = [
  'anthropic-beta',
  'anthropic-dangerous-direct-browser-access',
  'anthropic-version',
  'openai-beta',
];
const MODELED_ANTHROPIC_BETAS = [
  'claude-code-20250219',
  'effort-2025-11-24',
  'interleaved-thinking-2025-05-14',
  'mid-conversation-system-2026-04-07',
  'structured-outputs-2025-12-15',
];
const OPENAI_MESSAGE_ROLES = ['user', 'assistant', 'system', 'developer'];
const ANTHROPIC_MESSAGE_ROLES = ['user', 'assistant', 'system'];
const JSON_SCHEMA_FIELDS = [
  '$schema',
  'additionalProperties',
  'allOf',
  'anyOf',
  'const',
  'default',
  'description',
  'encrypted',
  'enum',
  'exclusiveMaximum',
  'exclusiveMinimum',
  'format',
  'items',
  'maxItems',
  'maxLength',
  'maximum',
  'maxProperties',
  'minItems',
  'minLength',
  'minimum',
  'multipleOf',
  'not',
  'oneOf',
  'pattern',
  'patternProperties',
  'properties',
  'propertyNames',
  'required',
  'title',
  'type',
];
const OPENAI_ADDITIONAL_TOOL_NAMES = ['exec', 'request_user_input', 'wait'];
const OPENAI_CUSTOM_TOOL_NAMES = ['exec'];
const OPENAI_FUNCTION_TOOL_NAMES = [
  'request_user_input',
  'wait',
  'followup_task',
  'interrupt_agent',
  'list_agents',
  'send_message',
  'spawn_agent',
  'wait_agent',
];
const OPENAI_COLLABORATION_TOOL_NAMES = [
  'followup_task',
  'interrupt_agent',
  'list_agents',
  'send_message',
  'spawn_agent',
  'wait_agent',
];
const OPENAI_INCLUDE_VALUES = ['reasoning.encrypted_content'];

export function reserveTrustedProviderRequest(
  policy: TrustedProviderBudgetPolicy,
  request: TrustedProviderRequest
): ProviderTokenReservation {
  return prepareTrustedProviderRequest(policy, request).reservation;
}

export function prepareTrustedProviderRequest(
  policy: TrustedProviderBudgetPolicy,
  request: TrustedProviderRequest
): PreparedTrustedProviderRequest {
  assertPolicy(policy);
  assertRequestTarget(policy, request);
  const body = objectRecord(request.body, 'request body');
  const preparedBody = prepareRequestBody(policy, body);
  const output =
    policy.provider === 'openai'
      ? validateOpenAiRequest(policy, preparedBody)
      : validateAnthropicRequest(policy, request, preparedBody);
  safeSum([policy.maxInputTokens, output], 'Trusted provider token reservation');
  return {
    body: Buffer.from(JSON.stringify(preparedBody)),
    reservation: { input: policy.maxInputTokens, output },
  };
}

export function parseProviderCompletionUsage(
  provider: TrustedProvider,
  payload: unknown
): ProviderUsageSettlement {
  try {
    const body = objectRecord(payload, 'provider response');
    return provider === 'openai' ? parseOpenAiCompletion(body) : parseAnthropicCompletion(body);
  } catch (error) {
    return unknownSettlement(error);
  }
}

export function assertTrustedProviderBudgetPolicy(policy: TrustedProviderBudgetPolicy): void {
  assertPolicy(policy);
}

export function parseProviderUsageSse(
  provider: TrustedProvider,
  chunks: Iterable<string | Uint8Array>,
  options: SseOptions = {}
): ProviderUsageSettlement {
  try {
    const events = collectSseEvents(chunks, options.maxBytes ?? DEFAULT_SSE_MAX_BYTES);
    return provider === 'openai' ? parseOpenAiSse(events) : parseAnthropicSse(events);
  } catch (error) {
    if (isRejectedStreamError(error)) throw error;
    return unknownSettlement(error);
  }
}

function assertPolicy(policy: TrustedProviderBudgetPolicy): void {
  if (policy.provider !== 'openai' && policy.provider !== 'anthropic') {
    throw new Error('Trusted provider policy has unsupported provider.');
  }
  if (!isNonEmptyString(policy.host))
    throw new Error('Trusted provider policy requires exact host.');
  if (!isNonEmptyString(policy.model))
    throw new Error('Trusted provider policy requires exact model.');
  assertSafePositiveInt(policy.maxInputTokens, 'maxInputTokens');
  assertSafePositiveInt(policy.maxOutputTokens, 'maxOutputTokens');
  assertAllowedProviderHeaders(policy.allowedHeaders);
}

function assertRequestTarget(
  policy: TrustedProviderBudgetPolicy,
  request: TrustedProviderRequest
): void {
  if (request.method !== 'POST') throw new Error('Trusted provider request requires POST.');
  if (request.host !== policy.host) throw new Error('Trusted provider request host is not pinned.');
  const expectedPath = policy.provider === 'openai' ? '/v1/responses' : '/v1/messages';
  if (request.path !== expectedPath)
    throw new Error('Trusted provider request endpoint is not allowed.');
  validateProviderHeaders(policy, request.headers ?? {});
}

function assertAllowedProviderHeaders(
  headers: Record<string, string | readonly string[]> | undefined
): void {
  if (headers === undefined) return;
  const names = new Set<string>();
  for (const [name, value] of Object.entries(headers)) {
    const normalized = name.toLowerCase();
    if (names.has(normalized)) throw new Error('Trusted provider policy has duplicate headers.');
    names.add(normalized);
    if (!isModeledProviderHeader(normalized) || normalized !== name) {
      throw new Error('Trusted provider policy has unsupported header allowance.');
    }
    const values = headerValues(value);
    if (values.length === 0) throw new Error('Trusted provider policy has empty header values.');
    assertUniqueHeaderValues(values);
    for (const allowedValue of values) validateHeaderValue(name, allowedValue);
  }
}

function assertUniqueHeaderValues(values: readonly string[]): void {
  const seen = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) throw new Error('Trusted provider policy has duplicate header values.');
    seen.add(value);
  }
}

function validateProviderHeaders(
  policy: TrustedProviderBudgetPolicy,
  headers: Record<string, string>
): void {
  const allowed = policy.allowedHeaders ?? {};
  for (const [name, value] of Object.entries(headers)) {
    const normalized = name.toLowerCase();
    if (name !== normalized && isProviderHeaderName(normalized)) {
      throw new Error(`Trusted provider request header '${name}' is not normalized.`);
    }
    if (!isProviderHeaderName(normalized)) continue;
    if (!isModeledProviderHeader(normalized)) {
      throw new Error(`Trusted provider request header '${name}' is unsupported.`);
    }
    validateHeaderValue(normalized, value);
    if (!headerValues(allowed[normalized]).includes(value)) {
      throw new Error(`Trusted provider request header '${name}' is not pinned.`);
    }
  }
}

function isModeledProviderHeader(name: string): boolean {
  return MODELED_PROVIDER_HEADERS.includes(name);
}

function isProviderHeaderName(name: string): boolean {
  return PROVIDER_HEADER_PREFIXES.some(prefix => name.startsWith(prefix));
}

function headerValues(value: string | readonly string[] | undefined): string[] {
  if (value === undefined) return [];
  return typeof value === 'string' ? [value] : [...value];
}

function validateHeaderValue(name: string, value: unknown): void {
  if (!isNonEmptyString(value) || value.trim() !== value || /[\r\n]/u.test(value)) {
    throw new Error(`Trusted provider header '${name}' value is unsafe.`);
  }
  if (name === 'anthropic-beta') validateAnthropicBetaHeader(value);
}

function validateAnthropicBetaHeader(value: string): void {
  const tokens = value.split(',');
  if (tokens.length === 0) throw new Error('Anthropic beta header is empty.');
  const seen = new Set<string>();
  for (const token of tokens) {
    if (!MODELED_ANTHROPIC_BETAS.includes(token) || seen.has(token)) {
      throw new Error('Anthropic beta header contains unsupported token.');
    }
    seen.add(token);
  }
}

function validateOpenAiRequest(
  policy: TrustedProviderBudgetPolicy,
  body: Record<string, unknown>
): number {
  assertAllowedKeys(body, OPENAI_REQUEST_FIELDS, 'OpenAI request');
  assertPinnedModel(policy, body.model);
  if (body.store !== false) throw new Error('OpenAI request requires store:false.');
  if (body.truncation !== undefined && body.truncation !== 'disabled')
    throw new Error('OpenAI request requires truncation:disabled.');
  if (body.stream !== undefined && typeof body.stream !== 'boolean') {
    throw new Error('OpenAI stream must be boolean when present.');
  }
  validateOpenAiNativeFields(body);
  validateOpenAiInput(body.input);
  validateOpenAiTools(optionalArray(body.tools, 'OpenAI tools'));
  return readOutputCap(body.max_output_tokens, policy.maxOutputTokens, 'max_output_tokens');
}

function validateOpenAiNativeFields(body: Record<string, unknown>): void {
  validateOpenAiClientMetadata(body.client_metadata);
  validateOpenAiInclude(body.include);
  if (body.parallel_tool_calls !== undefined && typeof body.parallel_tool_calls !== 'boolean') {
    throw new Error('OpenAI parallel_tool_calls must be boolean.');
  }
  if (body.prompt_cache_key !== undefined && !isNonEmptyString(body.prompt_cache_key)) {
    throw new Error('OpenAI prompt_cache_key must be string.');
  }
  validateBoundedJson(body.reasoning, 'OpenAI reasoning');
  validateBoundedJson(body.text, 'OpenAI text');
  validateBoundedJson(body.tool_choice, 'OpenAI tool_choice');
}

function validateOpenAiClientMetadata(metadata: unknown): void {
  if (metadata === undefined) return;
  const record = objectRecord(metadata, 'OpenAI client_metadata');
  if ('user_id' in record) throw new Error('OpenAI client_metadata user_id is not allowed.');
  validateBoundedJson(record, 'OpenAI client_metadata');
}

function validateOptionalStringArray(value: unknown, label: string): void {
  if (value === undefined) return;
  if (!Array.isArray(value) || !value.every(isNonEmptyString)) {
    throw new Error(`${label} must be a string array.`);
  }
}

function validateOpenAiInclude(value: unknown): void {
  validateOptionalStringArray(value, 'OpenAI include');
  if (value === undefined) return;
  for (const item of value as string[]) {
    if (!OPENAI_INCLUDE_VALUES.includes(item))
      throw new Error('OpenAI include value is unsupported.');
  }
}

function prepareRequestBody(
  policy: TrustedProviderBudgetPolicy,
  body: Record<string, unknown>
): Record<string, unknown> {
  if (policy.provider !== 'openai' || body.max_output_tokens !== undefined) return { ...body };
  return { ...body, max_output_tokens: policy.maxOutputTokens };
}

function validateAnthropicRequest(
  policy: TrustedProviderBudgetPolicy,
  request: TrustedProviderRequest,
  body: Record<string, unknown>
): number {
  assertAllowedKeys(body, ANTHROPIC_REQUEST_FIELDS, 'Anthropic request');
  assertPinnedModel(policy, body.model);
  if (policy.anthropicBeta === true && request.anthropicBeta !== true) {
    throw new Error('Anthropic beta request marker is required by policy.');
  }
  validateAnthropicMessages(body.messages);
  validateAnthropicSystem(body.system);
  validateAnthropicMetadata(body.metadata);
  validateBoundedJson(body.output_config, 'Anthropic output_config');
  validateBoundedJson(body.thinking, 'Anthropic thinking');
  validateAnthropicTools(optionalArray(body.tools, 'Anthropic tools'));
  if (body.stream !== undefined && typeof body.stream !== 'boolean') {
    throw new Error('Anthropic stream must be boolean when present.');
  }
  return readOutputCap(body.max_tokens, policy.maxOutputTokens, 'max_tokens');
}

function assertPinnedModel(policy: TrustedProviderBudgetPolicy, model: unknown): void {
  if (model !== policy.model) throw new Error('Trusted provider request model is not pinned.');
}

function readOutputCap(value: unknown, max: number, field: string): number {
  assertSafePositiveInt(value, field);
  const cap = value as number;
  if (cap > max) throw new Error(`${field} exceeds trusted model output cap.`);
  return cap;
}

function validateOpenAiInput(input: unknown): void {
  if (isNonEmptyString(input)) return;
  if (!Array.isArray(input) || input.length === 0) throw new Error('OpenAI input must be text.');
  for (const item of input) validateOpenAiInputItem(item);
}

function validateOpenAiInputItem(item: unknown): void {
  const record = objectRecord(item, 'OpenAI input item');
  if (record.type === 'additional_tools') {
    validateOpenAiAdditionalTools(record);
    return;
  }
  if (record.type === 'message') {
    validateOpenAiNativeMessage(record);
    return;
  }
  if (record.type === 'agent_message') {
    validateOpenAiAgentMessage(record);
    return;
  }
  if (record.type === 'custom_tool_call') {
    validateOpenAiCustomToolCall(record);
    return;
  }
  if (record.type === 'custom_tool_call_output') {
    validateOpenAiCustomToolCallOutput(record);
    return;
  }
  if (record.type === 'function_call') {
    validateOpenAiFunctionCall(record);
    return;
  }
  if (record.type === 'function_call_output') {
    validateOpenAiFunctionCallOutput(record);
    return;
  }
  if (record.type === 'reasoning') {
    validateOpenAiReasoningItem(record);
    return;
  }
  if ('role' in record) {
    assertAllowedKeys(record, ['content', 'role'], 'OpenAI message input');
    if (!OPENAI_MESSAGE_ROLES.includes(String(record.role)))
      throw new Error('Unsupported OpenAI role.');
    validateOpenAiContent(record.content);
    return;
  }
  validateOpenAiTextBlock(record, ['input_text'], 'OpenAI input block');
}

function validateOpenAiReasoningItem(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['content', 'encrypted_content', 'id', 'status', 'summary', 'type'],
    'OpenAI reasoning item'
  );
  validateOptionalId(record.id, 'OpenAI reasoning item');
  validateOptionalStatus(record.status, 'OpenAI reasoning item');
  validateBoundedNativeJson(record.content, 'OpenAI reasoning item content');
  validateBoundedNativeJson(record.summary, 'OpenAI reasoning item summary');
  if (record.encrypted_content !== undefined)
    validateBoundedToolString(record.encrypted_content, 'OpenAI reasoning encrypted content');
}

function validateOpenAiAgentMessage(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['author', 'content', 'internal_chat_message_metadata_passthrough', 'recipient', 'type'],
    'OpenAI agent message'
  );
  validateBoundedActor(record.author, 'OpenAI agent message author');
  validateBoundedActor(record.recipient, 'OpenAI agent message recipient');
  validateOpenAiAgentMessageContent(record.content);
  validateBoundedJson(
    record.internal_chat_message_metadata_passthrough,
    'OpenAI agent message metadata'
  );
}

function validateOpenAiNativeMessage(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['content', 'internal_chat_message_metadata_passthrough', 'role', 'type'],
    'OpenAI native message'
  );
  if (!OPENAI_MESSAGE_ROLES.includes(String(record.role)))
    throw new Error('Unsupported OpenAI role.');
  validateOpenAiContent(record.content);
  validateBoundedJson(
    record.internal_chat_message_metadata_passthrough,
    'OpenAI native message metadata'
  );
}

function validateOpenAiCustomToolCall(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    [
      'call_id',
      'id',
      'input',
      'internal_chat_message_metadata_passthrough',
      'name',
      'namespace',
      'status',
      'type',
    ],
    'OpenAI custom tool call'
  );
  validateOpenAiCallId(record.call_id, 'OpenAI custom tool call');
  validateKnownToolName(record.name, OPENAI_CUSTOM_TOOL_NAMES, 'OpenAI custom tool call');
  validateOptionalToolNamespace(record.namespace, 'OpenAI custom tool call');
  validateOptionalStatus(record.status, 'OpenAI custom tool call');
  validateOptionalId(record.id, 'OpenAI custom tool call');
  validateBoundedJson(
    record.internal_chat_message_metadata_passthrough,
    'OpenAI custom tool call metadata'
  );
  validateBoundedToolString(record.input, 'OpenAI custom tool call input');
}

function validateOpenAiCustomToolCallOutput(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['call_id', 'internal_chat_message_metadata_passthrough', 'output', 'type'],
    'OpenAI custom tool call output'
  );
  validateOpenAiCallId(record.call_id, 'OpenAI custom tool call output');
  validateBoundedJson(
    record.internal_chat_message_metadata_passthrough,
    'OpenAI custom tool call output metadata'
  );
  validateOpenAiToolOutput(record.output, 'OpenAI custom tool call output');
}

function validateOpenAiFunctionCall(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    [
      'arguments',
      'call_id',
      'id',
      'internal_chat_message_metadata_passthrough',
      'name',
      'namespace',
      'status',
      'type',
    ],
    'OpenAI function call'
  );
  validateOpenAiCallId(record.call_id, 'OpenAI function call');
  validateKnownToolName(record.name, OPENAI_FUNCTION_TOOL_NAMES, 'OpenAI function call');
  validateOptionalToolNamespace(record.namespace, 'OpenAI function call');
  validateOptionalStatus(record.status, 'OpenAI function call');
  validateOptionalId(record.id, 'OpenAI function call');
  validateBoundedJson(
    record.internal_chat_message_metadata_passthrough,
    'OpenAI function call metadata'
  );
  validateBoundedToolString(record.arguments, 'OpenAI function call arguments');
}

function validateOpenAiFunctionCallOutput(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['call_id', 'internal_chat_message_metadata_passthrough', 'output', 'type'],
    'OpenAI function call output'
  );
  validateOpenAiCallId(record.call_id, 'OpenAI function call output');
  validateBoundedJson(
    record.internal_chat_message_metadata_passthrough,
    'OpenAI function call output metadata'
  );
  validateBoundedToolString(record.output, 'OpenAI function call output');
}

function validateOpenAiAdditionalTools(record: Record<string, unknown>): void {
  assertAllowedKeys(record, ['role', 'tools', 'type'], 'OpenAI additional_tools input');
  if (record.role !== 'developer')
    throw new Error('OpenAI additional_tools requires developer role.');
  const tools = optionalArray(record.tools, 'OpenAI additional_tools tools');
  if (!tools || tools.length === 0) throw new Error('OpenAI additional_tools requires tools.');
  for (const tool of tools) validateOpenAiAdditionalTool(tool);
}

function validateOpenAiAdditionalTool(tool: unknown): void {
  const record = objectRecord(tool, 'OpenAI additional tool');
  rejectRemoteToolFields(record, 'OpenAI additional tool');
  if (record.type === 'function') {
    validateOpenAiNativeFunctionTool(record);
    return;
  }
  if (record.type === 'custom') {
    validateOpenAiNativeCustomTool(record);
    return;
  }
  if (record.type === 'namespace') {
    validateOpenAiNativeNamespaceTool(record);
    return;
  }
  throw new Error('OpenAI additional tool type is unsupported.');
}

function validateOpenAiNativeNamespaceTool(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['description', 'name', 'tools', 'type'],
    'OpenAI native namespace tool'
  );
  if (record.name !== 'collaboration') {
    throw new Error('OpenAI native namespace tool is not modeled.');
  }
  validateOptionalDescription(record.description, 'OpenAI native namespace tool');
  const tools = optionalArray(record.tools, 'OpenAI native namespace tools');
  if (!tools || tools.length === 0) throw new Error('OpenAI native namespace requires tools.');
  for (const tool of tools) validateOpenAiCollaborationTool(tool);
}

function validateOpenAiCollaborationTool(tool: unknown): void {
  const record = objectRecord(tool, 'OpenAI collaboration tool');
  rejectRemoteToolFields(record, 'OpenAI collaboration tool');
  if (record.type !== 'function') {
    throw new Error('OpenAI collaboration tool type is unsupported.');
  }
  assertAllowedKeys(
    record,
    ['description', 'name', 'parameters', 'strict', 'type'],
    'OpenAI collaboration tool'
  );
  validateKnownToolName(record.name, OPENAI_COLLABORATION_TOOL_NAMES, 'OpenAI collaboration tool');
  validateOptionalDescription(record.description, 'OpenAI collaboration tool');
  validateOptionalStrict(record.strict, 'OpenAI collaboration tool');
  validateJsonSchema(record.parameters, 'OpenAI collaboration parameters');
}

function validateOpenAiNativeFunctionTool(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['description', 'name', 'parameters', 'strict', 'type'],
    'OpenAI native function tool'
  );
  validateKnownToolName(record.name, OPENAI_ADDITIONAL_TOOL_NAMES, 'OpenAI native function tool');
  validateOptionalDescription(record.description, 'OpenAI native function tool');
  validateOptionalStrict(record.strict, 'OpenAI native function tool');
  validateJsonSchema(record.parameters, 'OpenAI native function parameters');
}

function validateOpenAiNativeCustomTool(record: Record<string, unknown>): void {
  assertAllowedKeys(record, ['description', 'format', 'name', 'type'], 'OpenAI native custom tool');
  validateKnownToolName(record.name, OPENAI_ADDITIONAL_TOOL_NAMES, 'OpenAI native custom tool');
  validateOptionalDescription(record.description, 'OpenAI native custom tool');
  validateCustomGrammar(record.format);
}

function validateKnownToolName(value: unknown, names: readonly string[], label: string): void {
  validateClientToolName(value, label);
  if (!names.includes(value as string)) throw new Error(`${label} is not modeled.`);
}

function validateOptionalDescription(value: unknown, label: string): void {
  if (value !== undefined && typeof value !== 'string')
    throw new Error(`${label} description invalid.`);
}

function validateOptionalStrict(value: unknown, label: string): void {
  if (value !== undefined && typeof value !== 'boolean')
    throw new Error(`${label} strict invalid.`);
}

function validateCustomGrammar(value: unknown): void {
  const record = objectRecord(value, 'OpenAI custom grammar');
  assertAllowedKeys(record, ['definition', 'syntax', 'type'], 'OpenAI custom grammar');
  if (
    record.type !== 'grammar' ||
    record.syntax !== 'lark' ||
    !isNonEmptyString(record.definition)
  ) {
    throw new Error('OpenAI custom grammar is unsupported.');
  }
}

function rejectRemoteToolFields(record: Record<string, unknown>, label: string): void {
  for (const key of ['connector_id', 'mcp', 'server_label', 'server_url', 'url']) {
    if (key in record) throw new Error(`${label} contains remote field '${key}'.`);
  }
  if (String(record.name).startsWith('mcp__')) throw new Error(`${label} rejects MCP tools.`);
}

function validateOpenAiContent(content: unknown): void {
  if (isNonEmptyString(content)) return;
  if (!Array.isArray(content) || content.length === 0)
    throw new Error('OpenAI content must be text.');
  for (const block of content)
    validateOpenAiTextBlock(block, ['input_text', 'output_text'], 'OpenAI content');
}

function validateOpenAiAgentMessageContent(content: unknown): void {
  if (!Array.isArray(content) || content.length === 0)
    throw new Error('OpenAI agent message content must be an array.');
  for (const block of content) {
    const record = objectRecord(block, 'OpenAI agent message content');
    if (record.type === 'encrypted_content') {
      validateOpenAiEncryptedContentBlock(record);
      continue;
    }
    validateOpenAiTextBlock(record, ['input_text', 'output_text'], 'OpenAI agent message content');
  }
}

function validateOpenAiEncryptedContentBlock(record: Record<string, unknown>): void {
  assertAllowedKeys(record, ['encrypted_content', 'type'], 'OpenAI encrypted content');
  validateBoundedToolString(record.encrypted_content, 'OpenAI encrypted content');
}

function validateOpenAiTextBlock(block: unknown, allowedTypes: string[], label: string): void {
  const record = objectRecord(block, label);
  assertAllowedKeys(record, ['author', 'recipient', 'text', 'type'], label);
  if ('author' in record && !isNonEmptyString(record.author))
    throw new Error(`${label} author invalid.`);
  if ('recipient' in record && !isNonEmptyString(record.recipient))
    throw new Error(`${label} recipient invalid.`);
  if (!allowedTypes.includes(String(record.type)) || !isNonEmptyString(record.text)) {
    throw new Error(`${label} must be text only.`);
  }
}

function validateOpenAiTools(tools: unknown[] | undefined): void {
  if (!tools) return;
  for (const tool of tools) validateOpenAiTool(tool);
}

function validateOpenAiTool(tool: unknown): void {
  const record = objectRecord(tool, 'OpenAI tool');
  if (record.type === 'function') {
    assertAllowedKeys(
      record,
      ['description', 'name', 'parameters', 'type'],
      'OpenAI function tool'
    );
    validateClientToolName(record.name, 'OpenAI function tool');
    if (record.description !== undefined && typeof record.description !== 'string') {
      throw new Error('OpenAI function description must be string.');
    }
    if (record.parameters !== undefined) validateJsonSchema(record.parameters, 'OpenAI parameters');
    return;
  }
  if (record.type === 'custom') {
    assertAllowedKeys(record, ['description', 'name', 'type'], 'OpenAI custom tool');
    validateClientToolName(record.name, 'OpenAI custom tool');
    return;
  }
  throw new Error('OpenAI request rejects server tools and unknown tool aliases.');
}

function validateAnthropicMessages(messages: unknown): void {
  if (!Array.isArray(messages) || messages.length === 0)
    throw new Error('Anthropic messages required.');
  for (const message of messages) validateAnthropicMessage(message);
}

function validateAnthropicMessage(message: unknown): void {
  const record = objectRecord(message, 'Anthropic message');
  assertAllowedKeys(record, ['content', 'role'], 'Anthropic message');
  if (!ANTHROPIC_MESSAGE_ROLES.includes(String(record.role)))
    throw new Error('Unsupported Anthropic role.');
  validateAnthropicContent(record.content);
}

function validateAnthropicContent(content: unknown): void {
  if (isNonEmptyString(content)) return;
  if (!Array.isArray(content) || content.length === 0)
    throw new Error('Anthropic content must be modeled text or local tool continuation.');
  for (const block of content) validateAnthropicContentBlock(block);
}

function validateAnthropicContentBlock(block: unknown): void {
  const record = objectRecord(block, 'Anthropic content');
  if (record.type === 'text') {
    validateAnthropicTextBlock(record);
    return;
  }
  if (record.type === 'tool_use') {
    validateAnthropicToolUseBlock(record);
    return;
  }
  if (record.type === 'tool_result') {
    validateAnthropicToolResultBlock(record);
    return;
  }
  throw new Error('Anthropic content must be modeled text or local tool continuation.');
}

function validateAnthropicTextBlock(record: Record<string, unknown>): void {
  assertAllowedKeys(record, ['cache_control', 'text', 'type'], 'Anthropic content');
  if (!isNonEmptyString(record.text)) throw new Error('Anthropic content must be text.');
  validateAnthropicCacheControl(record.cache_control);
}

function validateAnthropicToolUseBlock(record: Record<string, unknown>): void {
  assertAllowedKeys(record, ['id', 'input', 'name', 'type'], 'Anthropic content');
  validateAnthropicToolUseId(record.id, 'Anthropic tool_use');
  validateClientToolName(record.name, 'Anthropic tool_use');
  validateBoundedNativeJson(record.input, 'Anthropic tool_use input');
}

function validateAnthropicToolResultBlock(record: Record<string, unknown>): void {
  assertAllowedKeys(
    record,
    ['cache_control', 'content', 'is_error', 'tool_use_id', 'type'],
    'Anthropic content'
  );
  validateAnthropicToolUseId(record.tool_use_id, 'Anthropic tool_result');
  if (record.is_error !== undefined && typeof record.is_error !== 'boolean') {
    throw new Error('Anthropic tool_result is_error must be boolean.');
  }
  validateAnthropicToolResultContent(record.content);
  validateAnthropicCacheControl(record.cache_control);
}

function validateAnthropicToolResultContent(content: unknown): void {
  if (isNonEmptyString(content)) {
    validateBoundedToolString(content, 'Anthropic tool_result content');
    return;
  }
  if (!Array.isArray(content) || content.length === 0) {
    throw new Error('Anthropic tool_result content must be bounded text.');
  }
  for (const block of content) {
    const record = objectRecord(block, 'Anthropic tool_result content');
    assertAllowedKeys(record, ['text', 'type'], 'Anthropic tool_result content');
    if (record.type !== 'text' || !isNonEmptyString(record.text)) {
      throw new Error('Anthropic tool_result content must be text.');
    }
  }
  if (Buffer.byteLength(JSON.stringify(content)) > MAX_NATIVE_TOOL_PAYLOAD_BYTES) {
    throw new Error('Anthropic tool_result content exceeds bounded size.');
  }
}

function validateAnthropicSystem(system: unknown): void {
  if (system === undefined || isNonEmptyString(system)) return;
  if (!Array.isArray(system) || system.length === 0)
    throw new Error('Anthropic system prompt must be text only.');
  for (const block of system) {
    const record = objectRecord(block, 'Anthropic system');
    assertAllowedKeys(record, ['cache_control', 'text', 'type'], 'Anthropic system');
    if (record.type !== 'text' || !isNonEmptyString(record.text)) {
      throw new Error('Anthropic system prompt must be text only.');
    }
    validateAnthropicCacheControl(record.cache_control);
  }
}

function validateAnthropicMetadata(metadata: unknown): void {
  if (metadata === undefined) return;
  const record = objectRecord(metadata, 'Anthropic metadata');
  validateBoundedJson(record, 'Anthropic metadata');
}

function validateAnthropicCacheControl(value: unknown): void {
  if (value === undefined) return;
  const record = objectRecord(value, 'Anthropic cache_control');
  assertAllowedKeys(record, ['type'], 'Anthropic cache_control');
  if (record.type !== 'ephemeral') throw new Error('Anthropic cache_control is unsupported.');
}

function validateAnthropicTools(tools: unknown[] | undefined): void {
  if (!tools) return;
  for (const tool of tools) validateAnthropicTool(tool);
}

function validateAnthropicTool(tool: unknown): void {
  const record = objectRecord(tool, 'Anthropic tool');
  if ('type' in record) throw new Error('Anthropic request rejects server tools.');
  assertAllowedKeys(record, ['description', 'input_schema', 'name'], 'Anthropic tool');
  validateClientToolName(record.name, 'Anthropic tool');
  if (record.description !== undefined && typeof record.description !== 'string') {
    throw new Error('Anthropic tool description must be string.');
  }
  validateJsonSchema(record.input_schema, 'Anthropic input_schema');
}

function validateClientToolName(value: unknown, label: string): void {
  if (!isNonEmptyString(value) || !/^[A-Za-z0-9_-]{1,64}$/.test(value)) {
    throw new Error(`${label} requires a bounded client-defined name.`);
  }
}

function validateOpenAiCallId(value: unknown, label: string): void {
  if (!isNonEmptyString(value) || !/^[A-Za-z0-9_-]{1,128}$/.test(value)) {
    throw new Error(`${label} requires a bounded call_id.`);
  }
}

function validateAnthropicToolUseId(value: unknown, label: string): void {
  if (!isNonEmptyString(value) || !/^[A-Za-z0-9_-]{1,128}$/.test(value)) {
    throw new Error(`${label} requires a bounded tool_use id.`);
  }
}

function validateOptionalToolNamespace(value: unknown, label: string): void {
  if (value === undefined) return;
  if (!isNonEmptyString(value) || !/^[A-Za-z0-9_-]{1,64}$/.test(value)) {
    throw new Error(`${label} namespace is unsupported.`);
  }
}

function validateOptionalStatus(value: unknown, label: string): void {
  if (value === undefined) return;
  if (value !== 'completed' && value !== 'in_progress') throw new Error(`${label} status invalid.`);
}

function validateOptionalId(value: unknown, label: string): void {
  if (value !== undefined && !isNonEmptyString(value)) throw new Error(`${label} id invalid.`);
}

function validateBoundedActor(value: unknown, label: string): void {
  if (!isNonEmptyString(value) || Buffer.byteLength(value) > 128) {
    throw new Error(`${label} invalid.`);
  }
}

function validateBoundedToolString(value: unknown, label: string): void {
  if (!isNonEmptyString(value) || Buffer.byteLength(value) > MAX_NATIVE_TOOL_PAYLOAD_BYTES) {
    throw new Error(`${label} must be bounded text.`);
  }
}

function validateBoundedNativeJson(value: unknown, label: string): void {
  if (value === undefined) return;
  if (Buffer.byteLength(JSON.stringify(value)) > MAX_NATIVE_TOOL_PAYLOAD_BYTES) {
    throw new Error(`${label} exceeds bounded size.`);
  }
  validateBoundedJson(value, label);
}

function validateOpenAiToolOutput(value: unknown, label: string): void {
  if (isNonEmptyString(value)) {
    validateBoundedToolString(value, label);
    return;
  }
  if (!Array.isArray(value) || value.length === 0) throw new Error(`${label} must be text.`);
  for (const item of value) validateOpenAiTextBlock(item, ['input_text'], label);
  if (Buffer.byteLength(JSON.stringify(value)) > MAX_NATIVE_TOOL_PAYLOAD_BYTES) {
    throw new Error(`${label} exceeds bounded size.`);
  }
}

function validateJsonSchema(schema: unknown, label: string, depth = 0): void {
  if (depth > MAX_JSON_DEPTH) throw new Error(`${label} is too deeply nested.`);
  const record = objectRecord(schema, label);
  assertAllowedKeys(record, JSON_SCHEMA_FIELDS, label);
  for (const [key, value] of Object.entries(record)) {
    if (key === 'properties') {
      validateJsonSchemaProperties(value, label, depth + 1);
    } else {
      validateJsonSchemaValue(value, label, depth + 1);
    }
  }
}

function validateJsonSchemaProperties(value: unknown, label: string, depth: number): void {
  const properties = objectRecord(value, `${label}.properties`);
  for (const propertySchema of Object.values(properties)) {
    validateJsonSchema(propertySchema, label, depth);
  }
}

function validateJsonSchemaValue(value: unknown, label: string, depth: number): void {
  if (value === null || ['string', 'number', 'boolean'].includes(typeof value)) return;
  if (Array.isArray(value)) {
    for (const entry of value) validateJsonSchemaValue(entry, label, depth);
    return;
  }
  validateJsonSchema(value, label, depth);
}

function parseOpenAiCompletion(body: Record<string, unknown>): ProviderUsageSettlement {
  if (body.status !== 'completed')
    return { state: 'unknown', reason: 'OpenAI response not completed' };
  const usage = objectRecord(body.usage, 'OpenAI usage');
  const input = safeCount(usage.input_tokens, 'input_tokens');
  const output = safeCount(usage.output_tokens, 'output_tokens');
  const total = safeCount(usage.total_tokens, 'total_tokens');
  if (input + output !== total) throw new Error('OpenAI total_tokens is inconsistent.');
  return { state: 'complete', usage: { input, output, total } };
}

function parseAnthropicCompletion(body: Record<string, unknown>): ProviderUsageSettlement {
  if (body.stop_reason === 'pause_turn')
    return { state: 'unknown', reason: 'Anthropic pause_turn' };
  const usage = objectRecord(body.usage, 'Anthropic usage');
  const input = anthropicInputUsage(usage);
  const output = safeCount(usage.output_tokens, 'output_tokens');
  return { state: 'complete', usage: { input, output, total: input + output } };
}

function anthropicInputUsage(usage: Record<string, unknown>): number {
  const base = safeCount(usage.input_tokens, 'input_tokens');
  const cacheRead = optionalSafeCount(usage.cache_read_input_tokens, 'cache_read_input_tokens');
  const cacheCreate = optionalSafeCount(
    usage.cache_creation_input_tokens,
    'cache_creation_input_tokens'
  );
  return safeSum([base, cacheRead, cacheCreate], 'Anthropic input usage');
}

function parseOpenAiSse(events: SseEvent[]): ProviderUsageSettlement {
  let terminal: ProviderUsageSettlement | undefined;
  for (const event of events) {
    if (terminal) throw new Error('SSE event found after terminal completion.');
    if (event.event === 'response.completed')
      terminal = parseOpenAiCompletion(openAiSseResponse(event));
    if (event.event === 'response.incomplete' || event.event === 'response.failed') {
      terminal = { state: 'unknown', reason: `OpenAI ${event.event}` };
    }
  }
  return terminal ?? { state: 'unknown', reason: 'stream ended without completed usage' };
}

function openAiSseResponse(event: SseEvent): Record<string, unknown> {
  const data = objectRecord(JSON.parse(event.data), 'OpenAI SSE data');
  if (data.response !== undefined) return objectRecord(data.response, 'OpenAI SSE response');
  return data;
}

function parseAnthropicSse(events: SseEvent[]): ProviderUsageSettlement {
  const state: AnthropicStreamState = { sawStart: false, sawStop: false };
  for (const event of events) applyAnthropicEvent(state, event);
  if (state.unsafeReason) return { state: 'unknown', reason: state.unsafeReason };
  if (
    !state.sawStart ||
    !state.sawStop ||
    state.output === undefined ||
    state.input === undefined
  ) {
    return { state: 'unknown', reason: 'Anthropic stream ended without completed usage' };
  }
  return {
    state: 'complete',
    usage: { input: state.input, output: state.output, total: state.input + state.output },
  };
}

function applyAnthropicEvent(state: AnthropicStreamState, event: SseEvent): void {
  if (state.sawStop) throw new Error('SSE event found after terminal completion.');
  if (event.event === 'error') state.unsafeReason = 'Anthropic stream error';
  if (event.event === 'message_start') applyAnthropicStart(state, event);
  if (event.event === 'message_delta') applyAnthropicDelta(state, event);
  if (event.event === 'content_block_start') rejectAnthropicServerTool(state, event);
  if (event.event === 'message_stop') state.sawStop = true;
}

function applyAnthropicStart(state: AnthropicStreamState, event: SseEvent): void {
  const data = objectRecord(JSON.parse(event.data), 'Anthropic message_start');
  const message = objectRecord(data.message, 'Anthropic start message');
  state.input = anthropicInputUsage(objectRecord(message.usage, 'Anthropic start usage'));
  state.sawStart = true;
}

function applyAnthropicDelta(state: AnthropicStreamState, event: SseEvent): void {
  const data = objectRecord(JSON.parse(event.data), 'Anthropic message_delta');
  const delta = data.delta === undefined ? undefined : objectRecord(data.delta, 'Anthropic delta');
  if (delta?.stop_reason === 'pause_turn') state.unsafeReason = 'Anthropic pause_turn';
  state.output = safeCount(
    objectRecord(data.usage, 'Anthropic delta usage').output_tokens,
    'output_tokens'
  );
}

function rejectAnthropicServerTool(state: AnthropicStreamState, event: SseEvent): void {
  const data = objectRecord(JSON.parse(event.data), 'Anthropic content_block_start');
  const block = objectRecord(data.content_block, 'Anthropic content block');
  if (typeof block.type === 'string' && block.type.includes('server_tool')) {
    state.unsafeReason = 'Anthropic server tool stream';
  }
}

function collectSseEvents(chunks: Iterable<string | Uint8Array>, maxBytes: number): SseEvent[] {
  const events: SseEvent[] = [];
  let buffer = '';
  let totalBytes = 0;
  const decoder = new TextDecoder();
  for (const chunk of chunks) {
    const text = typeof chunk === 'string' ? chunk : decoder.decode(chunk, { stream: true });
    totalBytes += Buffer.byteLength(text);
    if (totalBytes > maxBytes) throw new Error('Provider usage SSE stream is too large.');
    buffer += text;
    drainSseBuffer(
      events,
      () => buffer,
      value => (buffer = value)
    );
  }
  buffer += decoder.decode();
  if (buffer.trim().length > 0) return events;
  return events;
}

function drainSseBuffer(
  events: SseEvent[],
  read: () => string,
  write: (value: string) => void
): void {
  let buffer = read();
  let separator = findSseTextSeparator(buffer);
  while (separator) {
    const rawEvent = buffer.slice(0, separator.frameEnd);
    const event = parseSseEvent(rawEvent);
    if (event) events.push(event);
    buffer = buffer.slice(separator.nextFrameStart);
    separator = findSseTextSeparator(buffer);
  }
  write(buffer);
}

function findSseTextSeparator(
  buffer: string
): { frameEnd: number; nextFrameStart: number } | undefined {
  for (let index = 0; index < buffer.length - 1; index += 1) {
    if (buffer[index] === '\n' && buffer[index + 1] === '\n') {
      return { frameEnd: index, nextFrameStart: index + 2 };
    }
    if (
      buffer[index] === '\r' &&
      buffer[index + 1] === '\n' &&
      buffer[index + 2] === '\r' &&
      buffer[index + 3] === '\n'
    ) {
      return { frameEnd: index, nextFrameStart: index + 4 };
    }
  }
  return undefined;
}

function parseSseEvent(raw: string): SseEvent | undefined {
  const lines = raw.split(/\r?\n/);
  let event: string | undefined;
  const dataLines: string[] = [];
  for (const line of lines) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
  }
  if (!event && dataLines.length === 0) return undefined;
  return { event, data: dataLines.join('\n') };
}

function objectRecord(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function assertAllowedKeys(
  record: Record<string, unknown>,
  allowed: readonly string[],
  label: string
): void {
  const extra = Object.keys(record).find(key => !allowed.includes(key));
  if (extra) {
    const type = typeof record.type === 'string' ? ` type '${record.type}'` : '';
    throw new Error(
      `${label}${type} contains unsupported field '${extra}' among keys ${Object.keys(record)
        .sort()
        .join(',')}.`
    );
  }
}

function optionalArray(value: unknown, label: string): unknown[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) throw new Error(`${label} must be an array.`);
  return [...value] as unknown[];
}

function validateBoundedJson(value: unknown, label: string, depth = 0): void {
  if (
    value === undefined ||
    value === null ||
    ['string', 'number', 'boolean'].includes(typeof value)
  ) {
    return;
  }
  if (depth > MAX_JSON_DEPTH) throw new Error(`${label} is too deeply nested.`);
  if (Array.isArray(value)) {
    for (const item of value) validateBoundedJson(item, label, depth + 1);
    return;
  }
  if (typeof value !== 'object') throw new Error(`${label} contains unsupported value.`);
  for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
    if (key.length === 0) throw new Error(`${label} contains empty key.`);
    validateBoundedJson(nested, label, depth + 1);
  }
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function assertSafePositiveInt(value: unknown, label: string): void {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${label} must be a safe positive integer.`);
  }
}

function safeCount(value: unknown, label: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} must be a safe non-negative integer.`);
  }
  return value;
}

function optionalSafeCount(value: unknown, label: string): number {
  return value === undefined ? 0 : safeCount(value, label);
}

function safeSum(values: number[], label: string): number {
  const total = values.reduce((sum, value) => sum + value, 0);
  if (!Number.isSafeInteger(total)) throw new Error(`${label} exceeds safe integer range.`);
  return total;
}

function unknownSettlement(error: unknown): ProviderUsageSettlement {
  return { state: 'unknown', reason: error instanceof Error ? error.message : 'invalid usage' };
}

function isRejectedStreamError(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.message.includes('after terminal') || error.message.includes('too large'))
  );
}
