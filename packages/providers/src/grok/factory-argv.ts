import type { FactoryProviderScope } from '../factory-sandbox';
import { GROK_FACTORY_DISALLOWED_TOOLS, GROK_FACTORY_IMPLEMENT_TOOLS } from './capabilities';

const NATIVE_GROK_SESSION_UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

const FORBIDDEN_FLAG_RE = /--worktree|--yolo|--always-approve|bypassPermissions/u;

export function requireNativeGrokSessionUuid(sessionId: string): string {
  if (!NATIVE_GROK_SESSION_UUID.test(sessionId)) {
    throw new Error('grok_exact_resume_native_uuid_required');
  }
  return sessionId;
}

export function permissionGlob(root: string): string {
  const normalized = root.startsWith('/') ? root : `/${root}`;
  return `${normalized}/**`;
}

function containsPath(parent: string, child: string): boolean {
  return child === parent || child.startsWith(parent.endsWith('/') ? parent : `${parent}/`);
}

export function buildGrokFactoryArgv(input: {
  command: string;
  prompt: string;
  cwd: string;
  model: string;
  scope: FactoryProviderScope;
  resumeSessionId?: string;
  grokHome?: string;
}): string[] {
  const readableRoots = input.scope.readableRoots ?? [];
  const allow = [
    ...input.scope.writableRoots.flatMap(root => [
      `Edit(${permissionGlob(root)})`,
      `Write(${permissionGlob(root)})`,
      `Read(${permissionGlob(root)})`,
    ]),
    ...readableRoots.map(root => `Read(${permissionGlob(root)})`),
  ];
  const deny = [
    ...input.scope.deniedRoots.flatMap(root => {
      const editWrite = [`Edit(${permissionGlob(root)})`, `Write(${permissionGlob(root)})`];
      const readDenied = readableRoots.some(readable => containsPath(root, readable))
        ? []
        : [`Read(${permissionGlob(root)})`];
      return [...editWrite, ...readDenied];
    }),
    ...(input.grokHome
      ? [
          `Edit(${permissionGlob(input.grokHome)})`,
          `Write(${permissionGlob(input.grokHome)})`,
          `Read(${permissionGlob(input.grokHome)})`,
        ]
      : []),
    'MCPTool(*)',
  ];
  const flags = [
    input.command,
    '--oauth',
    '--no-leader',
    '--sandbox',
    'archon-factory',
    '--permission-mode',
    'dontAsk',
    '--no-subagents',
    '--disable-web-search',
    '--cwd',
    input.cwd,
    '--model',
    input.model,
    '--output-format',
    'streaming-json',
    '--tools',
    GROK_FACTORY_IMPLEMENT_TOOLS.join(','),
    '--disallowed-tools',
    GROK_FACTORY_DISALLOWED_TOOLS.join(','),
    ...allow.flatMap(rule => ['--allow', rule]),
    ...deny.flatMap(rule => ['--deny', rule]),
    ...(input.resumeSessionId
      ? ['--resume', requireNativeGrokSessionUuid(input.resumeSessionId)]
      : []),
  ];
  if (flags.some(value => FORBIDDEN_FLAG_RE.test(value))) {
    throw new Error('grok_factory_argv_forbidden');
  }
  return [
    ...flags,
    '--prompt-json',
    JSON.stringify([{ type: 'text', text: input.prompt }]),
  ];
}
