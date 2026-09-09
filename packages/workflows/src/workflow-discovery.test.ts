import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import { mkdir, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

const mockLogger = {
  fatal: mock(() => undefined),
  error: mock(() => undefined),
  warn: mock(() => undefined),
  info: mock(() => undefined),
  debug: mock(() => undefined),
  trace: mock(() => undefined),
  child: mock(function () {
    return mockLogger;
  }),
  bindings: mock(() => ({ module: 'test' })),
  isLevelEnabled: mock(() => true),
  level: 'info',
};

const realArchonPaths = await import('@archon/paths');
mock.module('@archon/paths', () => ({
  ...realArchonPaths,
  createLogger: mock(() => mockLogger),
}));

import { clearRegistry, registerBuiltinProviders } from '@archon/providers';
clearRegistry();
registerBuiltinProviders();

import { discoverWorkflows, resetLegacyHomeWarningForTests } from './workflow-discovery';

describe('workflow discovery', () => {
  let testDir: string;
  const originalArchonHome = process.env.ARCHON_HOME;
  const originalArchonDocker = process.env.ARCHON_DOCKER;

  beforeEach(async () => {
    testDir = join(
      tmpdir(),
      `workflow-discovery-${Date.now()}-${Math.random().toString(36).slice(2)}`
    );
    process.env.ARCHON_HOME = join(testDir, 'home');
    delete process.env.ARCHON_DOCKER;
    resetLegacyHomeWarningForTests();
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
    if (originalArchonHome === undefined) delete process.env.ARCHON_HOME;
    else process.env.ARCHON_HOME = originalArchonHome;
    if (originalArchonDocker === undefined) delete process.env.ARCHON_DOCKER;
    else process.env.ARCHON_DOCKER = originalArchonDocker;
  });

  it('loads home and project scopes, with project files overriding home files by filename', async () => {
    const homeWorkflows = join(process.env.ARCHON_HOME ?? '', 'workflows');
    const projectWorkflows = join(testDir, '.archon', 'workflows');
    await mkdir(homeWorkflows, { recursive: true });
    await mkdir(projectWorkflows, { recursive: true });
    await writeFile(
      join(homeWorkflows, 'shared.yaml'),
      'name: shared-home\ndescription: home\nnodes:\n  - id: home\n    prompt: home\n'
    );
    await writeFile(
      join(projectWorkflows, 'shared.yaml'),
      'name: shared-project\ndescription: project\nnodes:\n  - id: project\n    prompt: project\n'
    );
    await writeFile(
      join(homeWorkflows, 'global-only.yaml'),
      'name: global-only\ndescription: global\nnodes:\n  - id: global\n    prompt: global\n'
    );

    const result = await discoverWorkflows(testDir, { loadDefaults: false });

    expect(result.errors).toEqual([]);
    expect(
      result.workflows
        .map(entry => [entry.workflow.name, entry.source])
        .sort(([leftName], [rightName]) => leftName.localeCompare(rightName))
    ).toEqual([
      ['global-only', 'global'],
      ['shared-project', 'project'],
    ]);
  });

  it('drops duplicate workflow names from surviving files and reports each colliding filename', async () => {
    const workflowDir = join(testDir, '.archon', 'workflows');
    await mkdir(workflowDir, { recursive: true });
    await writeFile(
      join(workflowDir, 'a.yaml'),
      'name: duplicate\ndescription: first\nnodes:\n  - id: first\n    prompt: first\n'
    );
    await writeFile(
      join(workflowDir, 'b.yaml'),
      'name: duplicate\ndescription: second\nnodes:\n  - id: second\n    prompt: second\n'
    );
    await writeFile(
      join(workflowDir, 'ok.yaml'),
      'name: ok\ndescription: ok\nnodes:\n  - id: ok\n    prompt: ok\n'
    );

    const result = await discoverWorkflows(testDir, { loadDefaults: false });

    expect(result.workflows.map(entry => entry.workflow.name)).toEqual(['ok']);
    const sortedErrors = result.errors.toSorted((left, right) =>
      left.filename.localeCompare(right.filename)
    );
    expect(sortedErrors.map(error => [error.filename, error.errorType])).toEqual([
      ['a.yaml', 'validation_error'],
      ['b.yaml', 'validation_error'],
    ]);
    expect(sortedErrors.map(error => error.error)).toEqual([
      "Duplicate workflow name 'duplicate' — also declared in b.yaml. Workflow names must be unique; same-name files do not override each other (overrides are by filename).",
      "Duplicate workflow name 'duplicate' — also declared in a.yaml. Workflow names must be unique; same-name files do not override each other (overrides are by filename).",
    ]);
  });
});
