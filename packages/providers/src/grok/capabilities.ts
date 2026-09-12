import type { ProviderCapabilities } from '../types';

const GROK_KNOWN_TOOL_NAMES = [
  'read_file',
  'grep',
  'list_dir',
  'search_replace',
  'run_terminal_cmd',
  'web_search',
  'web_fetch',
  'Agent',
  'search_tool',
  'use_tool',
] as const;

export const GROK_FACTORY_IMPLEMENT_TOOLS = [
  'read_file',
  'grep',
  'list_dir',
  'search_replace',
] as const;

export const GROK_FACTORY_DISALLOWED_TOOLS = [
  'Agent',
  'web_search',
  'web_fetch',
  'search_tool',
  'use_tool',
] as const;

export const GROK_CAPABILITIES: ProviderCapabilities = {
  sessionResume: true,
  sessionFork: false,
  mcp: false,
  hooks: false,
  skills: false,
  agents: false,
  toolRestrictions: true,
  knownToolNames: GROK_KNOWN_TOOL_NAMES,
  structuredOutput: 'enforced',
  envInjection: true,
  costControl: false,
  effortControl: true,
  thinkingControl: true,
  fallbackModel: false,
  sandbox: true,
  settingSources: false,
  nativeTools: false,
  containerExec: false,
};
