import { describe, expect, test } from 'bun:test';
import {
  parseProviderCompletionUsage,
  parseProviderUsageSse,
  prepareTrustedProviderRequest,
  reserveTrustedProviderRequest,
  type TrustedProviderBudgetPolicy,
} from './provider-budget-contract';

const OPENAI_POLICY: TrustedProviderBudgetPolicy = {
  provider: 'openai',
  host: 'api.openai.com',
  model: 'gpt-5.1-pinned',
  maxInputTokens: 120_000,
  maxOutputTokens: 16_000,
};

const ANTHROPIC_POLICY: TrustedProviderBudgetPolicy = {
  provider: 'anthropic',
  host: 'api.anthropic.com',
  model: 'claude-sonnet-4-5-pinned',
  maxInputTokens: 180_000,
  maxOutputTokens: 8_192,
  anthropicBeta: true,
  allowedHeaders: { 'anthropic-version': '2023-06-01' },
};

const NATIVE_OPENAI_TOOLS = [
  {
    type: 'custom',
    name: 'exec',
    description: 'synthetic local executor',
    format: {
      type: 'grammar',
      syntax: 'lark',
      definition: 'start: /[\\s\\S]+/',
    },
  },
  {
    type: 'function',
    name: 'wait',
    description: 'synthetic wait',
    strict: false,
    parameters: {
      type: 'object',
      properties: { cell_id: { type: 'string' } },
      required: ['cell_id'],
      additionalProperties: false,
    },
  },
  {
    type: 'function',
    name: 'request_user_input',
    description: 'synthetic local user input',
    strict: false,
    parameters: {
      type: 'object',
      properties: { questions: { type: 'array', items: { type: 'object' } } },
      required: ['questions'],
      additionalProperties: false,
    },
  },
  {
    type: 'namespace',
    name: 'collaboration',
    description: 'synthetic collaboration namespace',
    tools: [
      {
        type: 'function',
        name: 'spawn_agent',
        description: 'synthetic spawn child',
        strict: false,
        parameters: {
          type: 'object',
          properties: { task: { type: 'string' } },
          required: ['task'],
          additionalProperties: false,
        },
      },
      {
        type: 'function',
        name: 'followup_task',
        description: 'synthetic followup',
        strict: false,
        parameters: { type: 'object', properties: { task_id: { type: 'string' } } },
      },
      {
        type: 'function',
        name: 'send_message',
        description: 'synthetic message',
        strict: false,
        parameters: { type: 'object', properties: { task_id: { type: 'string' } } },
      },
      {
        type: 'function',
        name: 'wait_agent',
        description: 'synthetic wait child',
        strict: false,
        parameters: { type: 'object', properties: { task_id: { type: 'string' } } },
      },
    ],
  },
];

describe('reserveTrustedProviderRequest', () => {
  test('reserves pinned model input ceiling and explicit OpenAI output cap', () => {
    expect(
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        method: 'POST',
        host: 'api.openai.com',
        path: '/v1/responses',
        body: {
          model: 'gpt-5.1-pinned',
          input: [{ role: 'user', content: [{ type: 'input_text', text: 'hello' }] }],
          max_output_tokens: 1000,
          store: false,
          stream: true,
          truncation: 'disabled',
          tools: [
            {
              type: 'function',
              name: 'lookup',
              description: 'local function',
              parameters: {
                type: 'object',
                properties: { id: { type: 'string' } },
                required: ['id'],
              },
            },
          ],
        },
      })
    ).toEqual({ input: 120_000, output: 1000 });
  });

  test('reserves pinned Anthropic input ceiling and explicit output cap with beta fixture support', () => {
    expect(
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        headers: { 'anthropic-version': '2023-06-01' },
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 512,
          metadata: { session_id: 'synthetic', user_id: 'native-user' },
          output_config: {
            effort: 'high',
            format: { type: 'json_schema', schema: { type: 'object' } },
          },
          thinking: { type: 'disabled' },
          system: [{ type: 'text', text: 'text only', cache_control: { type: 'ephemeral' } }],
          messages: [
            {
              role: 'user',
              content: [{ type: 'text', text: 'hello', cache_control: { type: 'ephemeral' } }],
            },
          ],
          tools: [
            {
              name: 'lookup',
              description: 'local tool',
              input_schema: {
                type: 'object',
                properties: { id: { type: 'string' } },
              },
            },
          ],
        },
      })
    ).toEqual({ input: 180_000, output: 512 });
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        headers: { 'anthropic-version': '2023-06-01' },
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 512,
          system: [{ type: 'tool_use', text: 'no' }],
          messages: [{ role: 'user', content: 'hello' }],
        },
      })
    ).toThrow(/system prompt/);
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        headers: { 'anthropic-version': '2023-06-01' },
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 512,
          messages: [
            {
              role: 'user',
              content: [{ type: 'text', text: 'hello', cache_control: { type: 'disk' } }],
            },
          ],
        },
      })
    ).toThrow(/cache_control/);
  });

  test('admits only modeled Claude local tool continuation content', () => {
    expect(
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        headers: { 'anthropic-version': '2023-06-01' },
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 512,
          messages: [
            {
              role: 'assistant',
              content: [
                {
                  type: 'tool_use',
                  id: 'toolu_bash_1',
                  name: 'Bash',
                  input: { command: 'printf ok' },
                },
              ],
            },
            {
              role: 'user',
              content: [
                {
                  type: 'tool_result',
                  tool_use_id: 'toolu_bash_1',
                  content: 'ok',
                  is_error: false,
                  cache_control: { type: 'ephemeral' },
                },
              ],
            },
          ],
        },
      })
    ).toEqual({ input: 180_000, output: 512 });

    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        headers: { 'anthropic-version': '2023-06-01' },
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 512,
          messages: [
            {
              role: 'assistant',
              content: [{ type: 'server_tool_use', id: 'srv_1', name: 'web_search', input: {} }],
            },
          ],
        },
      })
    ).toThrow(/modeled text or local tool continuation/);

    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        headers: { 'anthropic-version': '2023-06-01' },
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 512,
          messages: [
            {
              role: 'assistant',
              content: [
                {
                  type: 'tool_use',
                  id: 'toolu_bash_1',
                  name: 'Bash',
                  input: { command: 'printf ok' },
                  server_url: 'https://remote.tools.test',
                },
              ],
            },
          ],
        },
      })
    ).toThrow(/unsupported field 'server_url'/);
  });

  test('requires exact pinned values for provider feature headers', () => {
    const base = {
      method: 'POST',
      host: 'api.anthropic.com',
      path: '/v1/messages',
      anthropicBeta: true,
      body: {
        model: 'claude-sonnet-4-5-pinned',
        max_tokens: 512,
        messages: [{ role: 'user', content: 'hello' }],
      },
    } as const;

    expect(
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        ...base,
        headers: { 'anthropic-version': '2023-06-01' },
      })
    ).toEqual({ input: 180_000, output: 512 });
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        ...base,
        headers: { 'anthropic-version': '2024-01-01' },
      })
    ).toThrow(/header 'anthropic-version'/);
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        ...base,
        headers: { 'anthropic-beta': 'context-1m-2025-08-07' },
      })
    ).toThrow(/unsupported token/);
    expect(() =>
      reserveTrustedProviderRequest(
        { ...ANTHROPIC_POLICY, allowedHeaders: { 'anthropic-context': '1m' } },
        { ...base, headers: { 'anthropic-context': '1m' } }
      )
    ).toThrow(/unsupported header/);
    expect(() =>
      reserveTrustedProviderRequest(
        { ...ANTHROPIC_POLICY, allowedHeaders: { 'Anthropic-Version': '2023-06-01' } },
        { ...base, headers: { 'anthropic-version': '2023-06-01' } }
      )
    ).toThrow(/unsupported header/);
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        ...base,
        headers: { 'Anthropic-Version': '2023-06-01' },
      })
    ).toThrow(/not normalized/);
    expect(
      reserveTrustedProviderRequest(
        {
          ...ANTHROPIC_POLICY,
          allowedHeaders: {
            'anthropic-beta': [
              'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24',
              'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24,structured-outputs-2025-12-15',
            ],
            'anthropic-version': '2023-06-01',
          },
        },
        {
          ...base,
          headers: {
            'anthropic-beta':
              'claude-code-20250219,interleaved-thinking-2025-05-14,mid-conversation-system-2026-04-07,effort-2025-11-24,structured-outputs-2025-12-15',
            'anthropic-version': '2023-06-01',
          },
        }
      )
    ).toEqual({ input: 180_000, output: 512 });
    expect(() =>
      reserveTrustedProviderRequest(
        {
          ...ANTHROPIC_POLICY,
          allowedHeaders: { 'anthropic-beta': 'claude-code-20250219,context-1m-2025-08-07' },
        },
        { ...base, headers: { 'anthropic-beta': 'claude-code-20250219,context-1m-2025-08-07' } }
      )
    ).toThrow(/unsupported token/);
    expect(() =>
      reserveTrustedProviderRequest(
        {
          ...ANTHROPIC_POLICY,
          allowedHeaders: { 'anthropic-beta': 'claude-code-20250219,claude-code-20250219' },
        },
        { ...base, headers: { 'anthropic-beta': 'claude-code-20250219,claude-code-20250219' } }
      )
    ).toThrow(/unsupported token/);
  });

  test('rejects unknown hosts, models, endpoints, aliases and missing output caps', () => {
    const base = {
      method: 'POST',
      host: 'api.openai.com',
      path: '/v1/responses',
      body: {
        model: 'gpt-5.1-pinned',
        input: 'hello',
        max_output_tokens: 10,
        store: false,
        truncation: 'disabled',
      },
    } as const;

    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, { ...base, host: 'evil.example' })
    ).toThrow(/host/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...base,
        body: { ...base.body, model: 'gpt-latest' },
      })
    ).toThrow(/model/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, { ...base, path: '/v1/chat/completions' })
    ).toThrow(/endpoint/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...base,
        body: { ...base.body, max_tokens: 10 },
      } as never)
    ).toThrow(/unsupported/);
    expect(
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...base,
        body: { model: 'gpt-5.1-pinned', input: 'hello', store: false, truncation: 'disabled' },
      })
    ).toEqual({ input: 120_000, output: 16_000 });
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        body: { model: 'claude-sonnet-4-5-pinned', messages: [{ role: 'user', content: 'hello' }] },
      })
    ).toThrow(/max_tokens/);
  });

  test('prepares OpenAI missing output cap by injecting the policy cap without mutating caller body', () => {
    const body = {
      model: 'gpt-5.1-pinned',
      input: 'hello',
      store: false,
      stream: true,
      truncation: 'disabled',
    };

    const prepared = prepareTrustedProviderRequest(OPENAI_POLICY, {
      method: 'POST',
      host: 'api.openai.com',
      path: '/v1/responses',
      body,
    });

    expect(prepared.reservation).toEqual({ input: 120_000, output: 16_000 });
    expect(JSON.parse(prepared.body.toString('utf8'))).toEqual({
      model: 'gpt-5.1-pinned',
      input: 'hello',
      store: false,
      stream: true,
      truncation: 'disabled',
      max_output_tokens: 16_000,
    });
    expect(body).toEqual({
      model: 'gpt-5.1-pinned',
      input: 'hello',
      store: false,
      stream: true,
      truncation: 'disabled',
    });
  });

  test('admits captured native OpenAI request without optional truncation field', () => {
    expect(
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        method: 'POST',
        host: 'api.openai.com',
        path: '/v1/responses',
        body: {
          model: 'gpt-5.1-pinned',
          input: 'hello',
          max_output_tokens: 1000,
          store: false,
          stream: true,
        },
      })
    ).toEqual({ input: 120_000, output: 1000 });
  });
  test('admits captured native OpenAI ResponsesLite local tools and modeled fields', () => {
    const body = {
      model: 'gpt-5.1-pinned',
      input: [
        { type: 'additional_tools', role: 'developer', tools: NATIVE_OPENAI_TOOLS },
        {
          type: 'message',
          role: 'developer',
          content: [{ type: 'input_text', text: 'policy text' }],
          internal_chat_message_metadata_passthrough: { scope: 'synthetic' },
        },
        {
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: 'hello' }],
        },
      ],
      client_metadata: { originator: 'archon-test' },
      include: ['reasoning.encrypted_content'],
      parallel_tool_calls: true,
      prompt_cache_key: 'archon-test-cache',
      reasoning: { effort: 'high', summary: 'auto' },
      store: false,
      stream: true,
      text: { format: { type: 'text' } },
      tool_choice: 'auto',
      truncation: 'disabled',
    };

    const prepared = prepareTrustedProviderRequest(OPENAI_POLICY, {
      method: 'POST',
      host: 'api.openai.com',
      path: '/v1/responses',
      body,
    });

    expect(prepared.reservation).toEqual({ input: 120_000, output: 16_000 });
    expect(JSON.parse(prepared.body.toString('utf8'))).toEqual({
      ...body,
      max_output_tokens: 16_000,
    });
  });

  test('admits pinned native OpenAI client tool-call follow-up items', () => {
    expect(
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        method: 'POST',
        host: 'api.openai.com',
        path: '/v1/responses',
        body: {
          model: 'gpt-5.1-pinned',
          input: [
            {
              type: 'custom_tool_call',
              call_id: 'call_exec_1',
              name: 'exec',
              input: "text('exec-ok')",
              internal_chat_message_metadata_passthrough: { turn_id: 'turn-synthetic' },
            },
            {
              type: 'custom_tool_call_output',
              call_id: 'call_exec_1',
              internal_chat_message_metadata_passthrough: { turn_id: 'turn-synthetic' },
              output: [
                { type: 'input_text', text: 'Script completed\nWall time 0.01 seconds\nOutput:\n' },
                { type: 'input_text', text: 'exec-ok' },
              ],
            },
            {
              type: 'reasoning',
              id: 'rs_synthetic',
              status: 'completed',
              summary: [{ type: 'summary_text', text: 'bounded native reasoning metadata' }],
              encrypted_content: 'gAAAAABsynthetic-pinned-continuation',
            },
            {
              type: 'agent_message',
              author: 'canary_child',
              recipient: 'root',
              content: [
                { type: 'input_text', text: 'child final', author: 'canary_child' },
                { type: 'encrypted_content', encrypted_content: 'gAAAAABsynthetic-child-state' },
              ],
              internal_chat_message_metadata_passthrough: { turn_id: 'turn-synthetic' },
            },
            {
              type: 'function_call',
              call_id: 'call_wait_1',
              namespace: 'collaboration',
              name: 'wait_agent',
              arguments: '{"timeout_ms":10000}',
              internal_chat_message_metadata_passthrough: { turn_id: 'turn-synthetic' },
            },
            {
              type: 'function_call_output',
              call_id: 'call_wait_1',
              internal_chat_message_metadata_passthrough: { turn_id: 'turn-synthetic' },
              output: '{"message":"Wait completed.","timed_out":false}',
            },
          ],
          max_output_tokens: 1000,
          store: false,
          stream: true,
          truncation: 'disabled',
        },
      })
    ).toEqual({ input: 120_000, output: 1000 });
  });

  test('rejects malformed native OpenAI encrypted continuation shapes', () => {
    const baseBody = {
      model: 'gpt-5.1-pinned',
      input: [
        {
          type: 'agent_message',
          author: 'child',
          recipient: 'root',
          content: [{ type: 'encrypted_content', encrypted_content: 'sealed' }],
        },
      ],
      max_output_tokens: 1000,
      store: false,
      stream: true,
      truncation: 'disabled',
    };
    const request = {
      method: 'POST',
      host: 'api.openai.com',
      path: '/v1/responses',
    } as const;

    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'agent_message',
              author: 'child',
              recipient: 'root',
              content: [{ type: 'encrypted_content', encrypted_content: 'sealed', url: 'file' }],
            },
          ],
        },
      })
    ).toThrow(/unsupported/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'reasoning',
              encrypted_content: 'x'.repeat(65_537),
            },
          ],
        },
      })
    ).toThrow(/bounded text/);
  });

  test('rejects unmodeled native OpenAI server, MCP, namespace and remote tools', () => {
    const baseBody = {
      model: 'gpt-5.1-pinned',
      input: [{ type: 'additional_tools', role: 'developer', tools: NATIVE_OPENAI_TOOLS }],
      max_output_tokens: 10,
      store: false,
      truncation: 'disabled',
    };
    const request = {
      method: 'POST',
      host: 'api.openai.com',
      path: '/v1/responses',
    } as const;

    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'additional_tools',
              role: 'developer',
              tools: [
                { type: 'function', name: 'mcp__remote__tool', parameters: { type: 'object' } },
              ],
            },
          ],
        },
      })
    ).toThrow(/MCP/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'additional_tools',
              role: 'developer',
              tools: [{ type: 'function', name: 'web_search', parameters: { type: 'object' } }],
            },
          ],
        },
      })
    ).toThrow(/not modeled/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'additional_tools',
              role: 'developer',
              tools: [{ type: 'namespace', name: 'browser', tools: [] }],
            },
          ],
        },
      })
    ).toThrow(/namespace tool is not modeled/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'additional_tools',
              role: 'developer',
              tools: [{ type: 'deferred', name: 'wait' }],
            },
          ],
        },
      })
    ).toThrow(/unsupported/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'additional_tools',
              role: 'developer',
              tools: [
                {
                  type: 'namespace',
                  name: 'collaboration',
                  tools: [{ type: 'function', name: 'read_host', parameters: { type: 'object' } }],
                },
              ],
            },
          ],
        },
      })
    ).toThrow(/collaboration tool is not modeled/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: { ...baseBody, parallel_tool_calls: 'true' },
      })
    ).toThrow(/parallel_tool_calls/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: { ...baseBody, include: ['unknown.output'] },
      })
    ).toThrow(/include/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        ...request,
        body: {
          ...baseBody,
          input: [
            {
              type: 'additional_tools',
              role: 'developer',
              tools: [{ type: 'function', name: 'wait', server_url: 'https://example.test' }],
            },
          ],
        },
      })
    ).toThrow(/remote field/);
  });

  test('preserves explicit smaller OpenAI output cap and still rejects unknown fields', () => {
    const body = {
      model: 'gpt-5.1-pinned',
      input: 'hello',
      max_output_tokens: 123,
      store: false,
      stream: true,
      truncation: 'disabled',
    };

    const prepared = prepareTrustedProviderRequest(OPENAI_POLICY, {
      method: 'POST',
      host: 'api.openai.com',
      path: '/v1/responses',
      body,
    });

    expect(prepared.reservation).toEqual({ input: 120_000, output: 123 });
    expect(JSON.parse(prepared.body.toString('utf8'))).toEqual(body);
    expect(() =>
      prepareTrustedProviderRequest(OPENAI_POLICY, {
        method: 'POST',
        host: 'api.openai.com',
        path: '/v1/responses',
        body: { ...body, unexpected: true },
      })
    ).toThrow(/unsupported/);
  });

  test('rejects unsafe caps instead of using remaining budget or approximate input counts', () => {
    const body = {
      model: 'gpt-5.1-pinned',
      input: 'tiny request',
      store: false,
      truncation: 'disabled',
    };
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        method: 'POST',
        host: 'api.openai.com',
        path: '/v1/responses',
        body: { ...body, max_output_tokens: 16_001 },
      })
    ).toThrow(/max_output_tokens/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        method: 'POST',
        host: 'api.openai.com',
        path: '/v1/responses',
        body: { ...body, max_output_tokens: 1.5 },
      })
    ).toThrow(/max_output_tokens/);
    expect(() =>
      reserveTrustedProviderRequest(OPENAI_POLICY, {
        method: 'POST',
        host: 'api.openai.com',
        path: '/v1/responses',
        body: { ...body, max_output_tokens: 1, stream: 'true' },
      })
    ).toThrow(/stream/);
    expect(() =>
      reserveTrustedProviderRequest(
        { ...OPENAI_POLICY, maxInputTokens: Number.MAX_SAFE_INTEGER, maxOutputTokens: 1 },
        {
          method: 'POST',
          host: 'api.openai.com',
          path: '/v1/responses',
          body: { ...body, max_output_tokens: 1 },
        }
      )
    ).toThrow(/reservation/);
  });

  test('rejects remote inputs, stateful continuations, background mode and server tools', () => {
    const base = {
      method: 'POST',
      host: 'api.openai.com',
      path: '/v1/responses',
      body: {
        model: 'gpt-5.1-pinned',
        input: 'hello',
        max_output_tokens: 10,
        store: false,
        truncation: 'disabled',
      },
    } as const;
    const hostileBodies = [
      { ...base.body, previous_response_id: 'resp_1' },
      { ...base.body, conversation: 'conv_1' },
      { ...base.body, prompt: { id: 'pmpt_1' } },
      { ...base.body, background: true },
      { ...base.body, input: [{ type: 'input_image', image_url: 'https://example.com/a.png' }] },
      { ...base.body, tools: [{ type: 'web_search_preview' }] },
    ];

    for (const body of hostileBodies) {
      expect(() => reserveTrustedProviderRequest(OPENAI_POLICY, { ...base, body })).toThrow();
    }
  });

  test('rejects malformed nested client tool schemas and Anthropic server tool types', () => {
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 10,
          messages: [{ role: 'user', content: 'hello' }],
          tools: [{ type: 'web_search_20250305', name: 'web_search' }],
        },
      })
    ).toThrow(/server tools/);
    expect(() =>
      reserveTrustedProviderRequest(ANTHROPIC_POLICY, {
        method: 'POST',
        host: 'api.anthropic.com',
        path: '/v1/messages',
        anthropicBeta: true,
        body: {
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 10,
          messages: [{ role: 'user', content: 'hello' }],
          tools: [
            {
              name: 'lookup',
              input_schema: {
                type: 'object',
                properties: { nested: { type: 'object', extra: true } },
              },
            },
          ],
        },
      })
    ).toThrow(/unsupported/);
  });
});

describe('parseProviderCompletionUsage', () => {
  test('parses OpenAI completed usage without double-counting cache or reasoning details', () => {
    expect(
      parseProviderCompletionUsage('openai', {
        status: 'completed',
        incomplete_details: null,
        usage: {
          input_tokens: 123,
          input_tokens_details: { cached_tokens: 100, cache_write_tokens: 7 },
          output_tokens: 45,
          output_tokens_details: { reasoning_tokens: 10 },
          total_tokens: 168,
        },
      })
    ).toEqual({ state: 'complete', usage: { input: 123, output: 45, total: 168 } });
  });

  test('treats OpenAI incomplete, missing, contradictory and malformed usage as unknown', () => {
    const fixtures = [
      { status: 'incomplete', usage: null },
      { status: 'completed', usage: null },
      { status: 'completed', usage: { input_tokens: 1, output_tokens: 2, total_tokens: 4 } },
      { status: 'completed', usage: { input_tokens: -1, output_tokens: 2, total_tokens: 1 } },
      { status: 'completed', usage: { input_tokens: 1.5, output_tokens: 2, total_tokens: 3.5 } },
      {
        status: 'completed',
        usage: { input_tokens: Number.MAX_SAFE_INTEGER + 1, output_tokens: 2, total_tokens: 3 },
      },
    ];
    for (const fixture of fixtures) {
      expect(parseProviderCompletionUsage('openai', fixture)).toEqual({
        state: 'unknown',
        reason: expect.any(String),
      });
    }
  });

  test('parses Anthropic completed usage and sums cache input fields exactly once', () => {
    expect(
      parseProviderCompletionUsage('anthropic', {
        stop_reason: 'end_turn',
        usage: {
          input_tokens: 50,
          cache_read_input_tokens: 100_000,
          cache_creation_input_tokens: 3,
          output_tokens: 45,
        },
      })
    ).toEqual({ state: 'complete', usage: { input: 100_053, output: 45, total: 100_098 } });
  });

  test('treats Anthropic pause_turn, missing and malformed usage as unknown', () => {
    const fixtures = [
      { stop_reason: 'pause_turn', usage: { input_tokens: 1, output_tokens: 1 } },
      { stop_reason: 'end_turn' },
      { stop_reason: 'end_turn', usage: { input_tokens: 1, output_tokens: -1 } },
      { stop_reason: 'end_turn', usage: { input_tokens: 1, output_tokens: Number.NaN } },
    ];
    for (const fixture of fixtures) {
      expect(parseProviderCompletionUsage('anthropic', fixture)).toEqual({
        state: 'unknown',
        reason: expect.any(String),
      });
    }
  });
});

describe('parseProviderUsageSse', () => {
  test('parses fragmented OpenAI response.completed SSE usage', () => {
    const sse = [
      'event: response.completed\n',
      'data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":12,',
      '"output_tokens":8,"output_tokens_details":{"reasoning_tokens":3},"total_tokens":20}}}\n\n',
    ];
    expect(parseProviderUsageSse('openai', sse)).toEqual({
      state: 'complete',
      usage: { input: 12, output: 8, total: 20 },
    });
  });

  test('ignores forged completion text and returns unknown on interrupted streams', () => {
    const forged = [
      'event: response.output_text.delta\n',
      'data: {"delta":"event: response.completed data: {\\"usage\\":{}}"}\n\n',
    ];
    expect(parseProviderUsageSse('openai', forged)).toEqual({
      state: 'unknown',
      reason: 'stream ended without completed usage',
    });
  });

  test('rejects OpenAI terminal contradictions and trailing events after completion', () => {
    const completed =
      'event: response.completed\ndata: {"response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}\n\n';
    expect(() =>
      parseProviderUsageSse('openai', [completed, 'event: response.failed\ndata: {}\n\n'])
    ).toThrow(/after terminal/);
  });

  test('parses Anthropic stream from message_start, cumulative delta usage and message_stop', () => {
    expect(
      parseProviderUsageSse('anthropic', [
        'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":7,"cache_read_input_tokens":20,"cache_creation_input_tokens":1,"output_tokens":0}}}\n\n',
        'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":11}}\n\n',
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
      ])
    ).toEqual({ state: 'complete', usage: { input: 28, output: 11, total: 39 } });
  });

  test('returns unknown for Anthropic stream errors, pause_turn, server tools and missing stop', () => {
    const fixtures = [
      ['event: error\ndata: {"type":"error"}\n\n'],
      [
        'event: message_start\ndata: {"message":{"usage":{"input_tokens":1,"output_tokens":0}}}\n\n',
        'event: message_delta\ndata: {"delta":{"stop_reason":"pause_turn"},"usage":{"output_tokens":1}}\n\n',
        'event: message_stop\ndata: {}\n\n',
      ],
      [
        'event: message_start\ndata: {"message":{"usage":{"input_tokens":1,"output_tokens":0}}}\n\n',
        'event: content_block_start\ndata: {"content_block":{"type":"server_tool_use"}}\n\n',
        'event: message_delta\ndata: {"usage":{"output_tokens":1}}\n\n',
        'event: message_stop\ndata: {}\n\n',
      ],
      [
        'event: message_start\ndata: {"message":{"usage":{"input_tokens":1,"output_tokens":0}}}\n\n',
      ],
    ];
    for (const fixture of fixtures) {
      expect(parseProviderUsageSse('anthropic', fixture)).toEqual({
        state: 'unknown',
        reason: expect.any(String),
      });
    }
  });

  test('enforces a bounded SSE byte cap', () => {
    expect(() => parseProviderUsageSse('openai', ['x'.repeat(1025)], { maxBytes: 1024 })).toThrow(
      /too large/
    );
  });
});
