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
