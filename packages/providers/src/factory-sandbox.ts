/** Native SDK scope selected by the trusted factory broker, never workflow YAML. */
import { realpathSync } from 'node:fs';
import { isAbsolute, relative, resolve, sep } from 'node:path';
import type { Options } from '@anthropic-ai/claude-agent-sdk';
import type { ThreadOptions } from '@openai/codex-sdk';

export interface FactoryProviderScope {
  workspaceRoot: string;
  writableRoots: string[];
  deniedRoots: string[];
  readableRoots?: string[];
}

function contains(parent: string, child: string): boolean {
  const path = relative(parent, child);
  return path === '' || (path !== '..' && !path.startsWith('..' + sep) && !isAbsolute(path));
}

function canonicalRoot(path: string): string {
  if (!isAbsolute(path) || resolve(path) === '/' || realpathSync(path) !== path) {
    throw new Error('factory_provider_scope_path_invalid');
  }
  return path;
}

export function validateFactoryProviderScope(
  scope: FactoryProviderScope,
  cwd: string
): FactoryProviderScope {
  if (process.platform !== 'darwin' && process.platform !== 'linux') {
    throw new Error('factory_provider_platform_unqualified');
  }
  const workspaceRoot = canonicalRoot(scope.workspaceRoot);
  if (canonicalRoot(cwd) !== workspaceRoot) throw new Error('factory_provider_workspace_mismatch');
  const writableRoots = [...new Set(scope.writableRoots.map(canonicalRoot))];
  const deniedRoots = [...new Set(scope.deniedRoots.map(canonicalRoot))];
  // Codex0.150.1's macOS sandbox keeps /tmp writable even when a named
  // profile explicitly denies a subtree. Never promise protection there.
  if (process.platform === 'darwin') {
    const nativeTemp = realpathSync('/tmp');
    if (deniedRoots.some(path => contains(nativeTemp, path) || contains(path, nativeTemp))) {
      throw new Error('factory_provider_protected_tmp_root_unqualified');
    }
  }
  if (!writableRoots.includes(workspaceRoot) || deniedRoots.length === 0) {
    throw new Error('factory_provider_scope_overlap');
  }
  for (const writable of writableRoots) {
    if (deniedRoots.some(denied => contains(writable, denied) || contains(denied, writable))) {
      throw new Error('factory_provider_scope_overlap');
    }
  }
  return {
    workspaceRoot,
    writableRoots,
    deniedRoots,
    readableRoots: [...new Set((scope.readableRoots ?? []).map(canonicalRoot))],
  };
}

/**
 * The Codex SDK flattens object config keys with dots. Filesystem entries use
 * absolute paths, so that serialization turns one path into several config
 * segments and silently drops the intended permission. Keep the filesystem
 * map as one TOML inline table and pass it through the SDK unchanged.
 */
export function factoryCodexConfigOverrides(scope: FactoryProviderScope, cwd: string): string[] {
  const checked = validateFactoryProviderScope(scope, cwd);
  const filesystem: Record<string, string> = { ':minimal': 'read' };
  for (const path of checked.readableRoots ?? []) filesystem[path] = 'read';
  for (const path of checked.writableRoots) filesystem[path] = 'write';
  for (const path of checked.deniedRoots) filesystem[path] = 'deny';
  const inlineTable =
    '{' +
    Object.entries(filesystem)
      .map(([path, mode]) => `${JSON.stringify(path)} = ${JSON.stringify(mode)}`)
      .join(', ') +
    '}';
  return [
    'default_permissions="archon-factory"',
    `permissions.archon-factory.filesystem=${inlineTable}`,
    'permissions.archon-factory.network.enabled=true',
  ];
}

export function factoryCodexScope(
  scope: FactoryProviderScope,
  cwd: string
): Partial<ThreadOptions> {
  validateFactoryProviderScope(scope, cwd);
  return {
    // A --sandbox flag would replace the narrower named permissions profile.
    sandboxMode: undefined,
    additionalDirectories: [],
    approvalPolicy: 'never',
  };
}

export function factoryClaudeScope(scope: FactoryProviderScope, cwd: string): Partial<Options> {
  const checked = validateFactoryProviderScope(scope, cwd);
  // Read/Edit permission rules use // for absolute paths; sandbox filesystem
  // paths use ordinary absolute paths. Both layers are required for file tools
  // and Bash descendants respectively. Do not load mergeable project settings.
  return {
    permissionMode: 'dontAsk',
    allowDangerouslySkipPermissions: false,
    settingSources: [],
    additionalDirectories: checked.writableRoots.filter(path => path !== checked.workspaceRoot),
    sandbox: {
      enabled: true,
      failIfUnavailable: true,
      autoAllowBashIfSandboxed: true,
      allowUnsandboxedCommands: false,
      filesystem: {
        allowWrite: checked.writableRoots,
        denyWrite: checked.deniedRoots,
        denyRead: checked.deniedRoots,
        allowRead: checked.readableRoots,
      },
    },
    settings: {
      permissions: {
        defaultMode: 'dontAsk',
        disableBypassPermissionsMode: 'disable',
        allow: [
          ...checked.writableRoots.map(path => `Edit(/${path}/**)`),
          ...(checked.readableRoots ?? []).map(path => `Read(/${path}/**)`),
        ],
        deny: checked.deniedRoots.flatMap(path => [`Edit(/${path}/**)`, `Read(/${path}/**)`]),
      },
    },
  };
}
