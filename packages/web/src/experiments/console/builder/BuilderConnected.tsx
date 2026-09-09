/**
 * Connected workflow builder route (PR-3). Replaces the fixture-backed
 * `BuilderRoute`: it resolves a `:name` param + the selected project (`cwd`),
 * loads a real workflow via the `loadWorkflow` skill verb, renders the
 * controlled `BuilderPage`, and persists edits through `saveWorkflow` with full
 * create / rename / delete. Bundled workflows open read-only and Save-as writes
 * a project override.
 *
 * Nav guard: the app is a non-data `<BrowserRouter>`, so `useBlocker` is
 * unavailable. We use `beforeunload` (reload/close) plus a
 * `confirmIfDirty` wrapper around this header's OWN navigation controls. The
 * browser Back button and `ProjectRail` clicks are NOT intercepted — a known
 * limitation; a data-router migration is out of scope for PR-3.
 *
 * House rules: no `console.*`; all failures surface as `Issue[]` in the panel.
 */
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactElement,
  type ReactNode,
} from 'react';
import { useNavigate, useParams, useLocation, useSearchParams } from 'react-router';
import { BuilderPage } from './BuilderPage';
import { fromWorkflowDefinition, toWorkflowDefinition } from './model';
import { runValidation } from './validation';
import { makeIssue } from './validation/make-issue';
import { useBuilderProject } from './connect/use-builder-project';
import {
  blockingErrors,
  clientIssue,
  errorDetail,
  errorToIssues,
  isReadOnlySource,
  isValidWorkflowName,
  planRename,
  renameReasonMessage,
  saveTargetFor,
  validationFailureToIssues,
} from './connect/save-logic';
import type { BuilderWorkflow, Issue, WireWorkflowDefinition } from './types';
import {
  loadWorkflow,
  saveWorkflow,
  deleteWorkflow,
  validateWorkflow,
  listWorkflows,
  type LoadedWorkflow,
  type WorkflowSource,
} from '../skills/workflows';
import { listProjects, type WorkflowListResult } from '../skills';
import { useEntity, invalidate } from '../store/cache';
import { K } from '../store/keys';
import { HttpError } from '../lib/http';
import type { Project } from '../primitives/project';

/** Router navigation state carried into the connected route. */
interface BuilderNavState {
  /** A freshly-seeded workflow for create mode (no server load). */
  createSeed?: BuilderWorkflow;
  /** Non-fatal notices to seed the panel after a navigation (e.g. rename delete-failed). */
  notices?: Issue[];
}

const IDLE_LIST_KEY = 'workflows:idle';

function EmptyState({ children }: { children: ReactNode }): ReactElement {
  return (
    <div className="flex h-full items-center justify-center p-8 text-center text-[12.5px] text-text-tertiary">
      <div className="max-w-md">{children}</div>
    </div>
  );
}

interface BuilderHeaderProps {
  projectId: string | undefined;
  projects: Project[];
  cwd: string | undefined;
  name: string | undefined;
  openOptions: string[];
  workflowOpen: boolean;
  dirty: boolean;
  canSave: boolean;
  saveLabel: string;
  busy: boolean;
  isCreateMode: boolean;
  readOnly: boolean;
  onPickProject: (id: string) => void;
  onOpenWorkflow: (workflow: string) => void;
  onNew: () => void;
  onSave: () => void;
  onRename: () => void;
  onDelete: () => void;
}

function BuilderHeader({
  projectId,
  projects,
  cwd,
  name,
  openOptions,
  workflowOpen,
  dirty,
  canSave,
  saveLabel,
  busy,
  isCreateMode,
  readOnly,
  onPickProject,
  onOpenWorkflow,
  onNew,
  onSave,
  onRename,
  onDelete,
}: BuilderHeaderProps): ReactElement {
  return (
    <header className="flex flex-wrap items-center gap-3 border-b border-border px-4 py-2">
      <h1 className="text-[14px] font-semibold text-text-primary">Workflow Builder</h1>
      <span className="rounded-full border border-border px-2 py-0.5 font-mono text-[10px] uppercase tracking-widest text-text-tertiary">
        beta
      </span>
      <label className="flex items-center gap-2 text-[11.5px] text-text-tertiary">
        Project
        <select
          value={projectId ?? ''}
          onChange={event => {
            onPickProject(event.target.value);
          }}
          className="max-w-[220px] rounded-[8px] border border-border bg-surface px-2 py-1 font-mono text-[12px] text-text-primary outline-none focus:border-accent-bright/60"
        >
          <option value="">Select a project…</option>
          {projects.map(project => (
            <option key={project.id} value={project.id}>
              {project.name}
            </option>
          ))}
        </select>
      </label>
      {cwd !== undefined ? (
        <label className="flex items-center gap-2 text-[11.5px] text-text-tertiary">
          Workflow
          <select
            value={name ?? ''}
            onChange={event => {
              onOpenWorkflow(event.target.value);
            }}
            className="max-w-[220px] rounded-[8px] border border-border bg-surface px-2 py-1 font-mono text-[12px] text-text-primary outline-none focus:border-accent-bright/60"
          >
            <option value="">Open a workflow…</option>
            {openOptions.map(option => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
      ) : null}
      {cwd !== undefined ? (
        <button
          type="button"
          onClick={onNew}
          className="rounded-[8px] border border-border bg-surface px-2.5 py-1 text-[12px] text-text-secondary transition-colors hover:bg-surface-hover hover:text-text-primary"
        >
          New
        </button>
      ) : null}
      <div className="flex-1" />
      {workflowOpen ? (
        <div className="flex items-center gap-2">
          {dirty ? (
            <span
              title="Unsaved changes"
              aria-label="Unsaved changes"
              className="h-2 w-2 rounded-full bg-warning"
            />
          ) : null}
          <button
            type="button"
            disabled={!canSave}
            onClick={onSave}
            className="rounded-[8px] bg-accent-bright px-3 py-1 text-[12px] font-semibold text-white/95 transition-opacity hover:brightness-110 disabled:pointer-events-none disabled:opacity-40"
          >
            {saveLabel}
          </button>
          {!isCreateMode ? (
            <button
              type="button"
              disabled={busy}
              onClick={onRename}
              className="rounded-[8px] border border-border bg-surface px-2.5 py-1 text-[12px] text-text-secondary transition-colors hover:bg-surface-hover hover:text-text-primary disabled:opacity-40"
            >
              Rename
            </button>
          ) : null}
          {!readOnly && !isCreateMode ? (
            <button
              type="button"
              disabled={busy}
              onClick={onDelete}
              className="rounded-[8px] px-2.5 py-1 text-[12px] text-error transition-colors hover:bg-error/10 disabled:opacity-40"
            >
              Delete
            </button>
          ) : null}
        </div>
      ) : null}
    </header>
  );
}

interface BuilderBodyProps {
  selectedProject: Project | undefined;
  workflowLoadError: unknown;
  notFound: boolean;
  name: string | undefined;
  existingNames: string[];
  imported: { workflow: BuilderWorkflow; issues: Issue[] } | null;
  currentWorkflow: BuilderWorkflow | null;
  editorKey: string;
  handleChange: (workflow: BuilderWorkflow) => void;
  extraIssues: Issue[];
  onNew: () => void;
}

function BuilderBody({
  selectedProject,
  workflowLoadError,
  notFound,
  name,
  existingNames,
  imported,
  currentWorkflow,
  editorKey,
  handleChange,
  extraIssues,
  onNew,
}: BuilderBodyProps): ReactElement {
  if (selectedProject === undefined) {
    return (
      <EmptyState>
        Select a project to load its workflows. Workflows are discovered and saved per project (the
        project's <span className="font-mono">cwd</span>).
      </EmptyState>
    );
  }
  if (workflowLoadError !== undefined) {
    return (
      <EmptyState>
        <p>
          Failed to load <span className="font-mono">{name}</span>: {errorDetail(workflowLoadError)}
        </p>
        <p className="mt-2">The workflow may still exist — retry once the server is reachable.</p>
      </EmptyState>
    );
  }
  if (notFound) {
    return (
      <EmptyState>
        <p>
          No workflow named <span className="font-mono">{name}</span> was found in{' '}
          <span className="font-mono">{selectedProject.name}</span>.
        </p>
        <p className="mt-2">
          It may live in a subfolder (not loadable via the single-name route) or not exist yet.
        </p>
        <button
          type="button"
          onClick={onNew}
          className="mt-3 rounded-[8px] border border-border bg-surface px-2.5 py-1 text-[12px] text-text-secondary transition-colors hover:bg-surface-hover hover:text-text-primary"
        >
          Create a new workflow
        </button>
      </EmptyState>
    );
  }
  if (name === undefined) {
    return existingNames.length === 0 ? (
      <EmptyState>
        <p>
          <span className="font-mono">{selectedProject.name}</span> has no project workflows yet.
        </p>
        <button
          type="button"
          onClick={onNew}
          className="mt-3 rounded-[8px] border border-border bg-surface px-2.5 py-1 text-[12px] text-text-secondary transition-colors hover:bg-surface-hover hover:text-text-primary"
        >
          Create the first workflow
        </button>
      </EmptyState>
    ) : (
      <EmptyState>
        Pick a workflow from the <span className="font-semibold">Workflow</span> menu above to start
        editing, or create a new one.
      </EmptyState>
    );
  }
  if (imported !== null && currentWorkflow !== null) {
    return (
      <BuilderPage
        key={editorKey}
        initialWorkflow={imported.workflow}
        onChange={handleChange}
        extraIssues={extraIssues}
      />
    );
  }
  return <EmptyState>Loading workflow…</EmptyState>;
}

interface BuilderActionContext {
  currentWorkflow: BuilderWorkflow | null;
  cwd: string | undefined;
  name: string | undefined;
  effectiveSource: WorkflowSource;
  isCreateMode: boolean;
  navigate: ReturnType<typeof useNavigate>;
  projectQuery: string;
  existingNames: string[];
  setServerIssues: (issues: Issue[]) => void;
  setDirty: (dirty: boolean) => void;
  setSourceOverride: (source: WorkflowSource | null) => void;
  setBusy: (busy: boolean) => void;
}

async function saveCurrentWorkflow(context: BuilderActionContext): Promise<void> {
  const { currentWorkflow, cwd, name } = context;
  if (currentWorkflow === null || cwd === undefined || name === undefined) return;
  const definition: WireWorkflowDefinition = { ...toWorkflowDefinition(currentWorkflow), name };
  const blocking = blockingErrors(runValidation(currentWorkflow));
  if (blocking.length > 0) {
    context.setServerIssues([
      clientIssue(
        'save.blocked',
        `Cannot save: fix ${String(blocking.length)} blocking error(s) first.`
      ),
    ]);
    return;
  }
  context.setBusy(true);
  try {
    const validation = await validateWorkflow(definition);
    if (!validation.valid) {
      context.setServerIssues(validationFailureToIssues(validation.errors));
      return;
    }
    const saved = await saveWorkflow(name, definition, {
      cwd,
      source: saveTargetFor(context.effectiveSource),
    });
    context.setServerIssues([]);
    context.setDirty(false);
    context.setSourceOverride(saved.source);
    invalidate(K.workflows(cwd));
    invalidate(K.workflow(cwd, name));
    if (context.isCreateMode)
      context.navigate(`/console/builder/${encodeURIComponent(name)}${context.projectQuery}`, {
        replace: true,
      });
  } catch (e) {
    context.setServerIssues(errorToIssues(e, 'save.failed', 'Save failed (unknown error).'));
  } finally {
    context.setBusy(false);
  }
}

async function deleteCurrentWorkflow(context: BuilderActionContext): Promise<void> {
  const { name, cwd } = context;
  if (name === undefined || cwd === undefined) return;
  if (!window.confirm(`Delete workflow "${name}"? This removes the YAML file.`)) return;
  context.setBusy(true);
  try {
    await deleteWorkflow(name, { cwd, source: saveTargetFor(context.effectiveSource) });
    invalidate(K.workflows(cwd));
    invalidate(K.workflow(cwd, name));
    context.navigate(`/console/builder${context.projectQuery}`);
  } catch (e) {
    context.setServerIssues(errorToIssues(e, 'delete.failed', 'Delete failed (unknown error).'));
  } finally {
    context.setBusy(false);
  }
}

async function renameCurrentWorkflow(context: BuilderActionContext): Promise<void> {
  const { name, cwd, currentWorkflow } = context;
  if (name === undefined || cwd === undefined || currentWorkflow === null) return;
  const raw = window.prompt('Rename workflow to:', name);
  if (raw === null) return;
  const to = raw.trim();
  const plan = planRename({ from: name, to, existingNames: context.existingNames });
  if (!plan.ok) {
    context.setServerIssues([clientIssue('rename.blocked', renameReasonMessage(plan.reason, to))]);
    return;
  }
  context.setBusy(true);
  try {
    await saveRenamedWorkflow(context, to);
  } catch (e) {
    context.setServerIssues(errorToIssues(e, 'rename.failed', 'Rename failed (unknown error).'));
  } finally {
    context.setBusy(false);
  }
}

async function saveRenamedWorkflow(context: BuilderActionContext, to: string): Promise<void> {
  const currentWorkflow = context.currentWorkflow;
  const name = context.name;
  const cwd = context.cwd;
  if (currentWorkflow === null || name === undefined || cwd === undefined) return;
  const definition: WireWorkflowDefinition = { ...toWorkflowDefinition(currentWorkflow), name: to };
  const validation = await validateWorkflow(definition);
  if (!validation.valid) {
    context.setServerIssues(validationFailureToIssues(validation.errors));
    return;
  }
  await saveWorkflow(to, definition, { cwd, source: saveTargetFor(context.effectiveSource) });
  const notices = await deleteRenamedSource(context, to);
  invalidate(K.workflows(cwd));
  invalidate(K.workflow(cwd, name));
  invalidate(K.workflow(cwd, to));
  context.setDirty(false);
  context.navigate(`/console/builder/${encodeURIComponent(to)}${context.projectQuery}`, {
    state: notices.length > 0 ? ({ notices } satisfies BuilderNavState) : undefined,
  });
}

async function deleteRenamedSource(context: BuilderActionContext, to: string): Promise<Issue[]> {
  const name = context.name;
  const cwd = context.cwd;
  if (name === undefined || cwd === undefined) return [];
  try {
    await deleteWorkflow(name, { cwd, source: saveTargetFor(context.effectiveSource) });
    return [];
  } catch (delErr) {
    return [
      makeIssue({
        rule: 'rename.delete.failed',
        severity: 'warning',
        source: 'server',
        message: `Renamed to "${to}", but removing the old file "${name}" failed (${errorDetail(delErr)}). Delete it manually.`,
        path: {},
      }),
    ];
  }
}

function createNewWorkflow(context: BuilderActionContext): void {
  if (context.cwd === undefined) return;
  const raw = window.prompt('New workflow name:', '');
  if (raw === null) return;
  const nm = raw.trim();
  if (!isValidWorkflowName(nm)) {
    context.setServerIssues([
      clientIssue('new.invalid-name', renameReasonMessage('invalid-name', nm)),
    ]);
    return;
  }
  if (context.existingNames.includes(nm)) {
    context.setServerIssues([clientIssue('new.collision', renameReasonMessage('collision', nm))]);
    return;
  }
  const seed: BuilderWorkflow = {
    name: nm,
    description: 'New workflow.',
    meta: {},
    nodes: [
      {
        id: 'step-1',
        variant: 'prompt',
        base: {},
        data: { prompt: 'Describe what this step should do.' },
      },
    ],
  };
  context.navigate(`/console/builder/${encodeURIComponent(nm)}${context.projectQuery}`, {
    state: { createSeed: seed } satisfies BuilderNavState,
  });
}

function matchingCreateSeed(
  navState: BuilderNavState | null,
  name: string | undefined
): BuilderWorkflow | undefined {
  return navState?.createSeed !== undefined && navState.createSeed.name === name
    ? navState.createSeed
    : undefined;
}

function importedWorkflow(
  isCreateMode: boolean,
  createSeed: BuilderWorkflow | undefined,
  loaded: LoadedWorkflow | null | undefined
): { workflow: BuilderWorkflow; issues: Issue[] } | null {
  if (isCreateMode && createSeed !== undefined) return { workflow: createSeed, issues: [] };
  return loaded?.definition !== undefined ? fromWorkflowDefinition(loaded.definition) : null;
}

function loadedWorkflowSource(
  isCreateMode: boolean,
  loaded: LoadedWorkflow | null | undefined
): WorkflowSource {
  return isCreateMode ? 'project' : (loaded?.source ?? 'project');
}

function workflowLoadStates(
  name: string | undefined,
  isCreateMode: boolean,
  loadError: unknown
): { loadError: unknown; notFound: boolean; workflowLoadError: unknown } {
  const activeError = name !== undefined && !isCreateMode ? loadError : undefined;
  const notFound = activeError instanceof HttpError && activeError.status === 404;
  return {
    loadError: activeError,
    notFound,
    workflowLoadError: activeError !== undefined && !notFound ? activeError : undefined,
  };
}

function sortedOpenOptions(existingNames: string[], name: string | undefined): string[] {
  const names = new Set(existingNames);
  if (name !== undefined) names.add(name);
  return [...names].sort((a, b) => a.localeCompare(b));
}

function useBuilderProjectQuerySync(
  projectId: string | undefined,
  searchParams: URLSearchParams,
  setSearchParams: ReturnType<typeof useSearchParams>[1]
): void {
  useEffect(() => {
    if (projectId === undefined || searchParams.get('project') === projectId) return;
    const next = new URLSearchParams(searchParams);
    next.set('project', projectId);
    setSearchParams(next, { replace: true });
  }, [projectId, searchParams, setSearchParams]);
}

function useBeforeUnloadGuard(dirty: boolean): void {
  useEffect(() => {
    if (!dirty) return undefined;
    const handler = (event: BeforeUnloadEvent): void => {
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', handler);
    return (): void => {
      window.removeEventListener('beforeunload', handler);
    };
  }, [dirty]);
}

function useResetImportedWorkflow({
  imported,
  editorKey,
  isCreateMode,
  navState,
  setCurrentWorkflow,
  setDirty,
  setSourceOverride,
  setServerIssues,
}: {
  imported: { workflow: BuilderWorkflow; issues: Issue[] } | null;
  editorKey: string;
  isCreateMode: boolean;
  navState: BuilderNavState | null;
  setCurrentWorkflow: React.Dispatch<React.SetStateAction<BuilderWorkflow | null>>;
  setDirty: React.Dispatch<React.SetStateAction<boolean>>;
  setSourceOverride: React.Dispatch<React.SetStateAction<WorkflowSource | null>>;
  setServerIssues: React.Dispatch<React.SetStateAction<Issue[]>>;
}): void {
  const initedKeyRef = useRef<string | null>(null);
  useEffect(() => {
    if (imported === null) {
      initedKeyRef.current = null;
      setCurrentWorkflow(null);
      setDirty(false);
      setSourceOverride(null);
      return;
    }
    if (initedKeyRef.current === editorKey) return;
    initedKeyRef.current = editorKey;
    setCurrentWorkflow(imported.workflow);
    setDirty(isCreateMode);
    setSourceOverride(null);
    setServerIssues(navState?.notices ?? []);
  }, [
    imported,
    editorKey,
    isCreateMode,
    navState?.notices,
    setCurrentWorkflow,
    setDirty,
    setSourceOverride,
    setServerIssues,
  ]);
}

function canSaveWorkflow(
  busy: boolean,
  readOnly: boolean,
  dirty: boolean,
  isCreateMode: boolean
): boolean {
  return !busy && (readOnly || dirty || isCreateMode);
}

function isWorkflowOpen(
  name: string | undefined,
  imported: { workflow: BuilderWorkflow; issues: Issue[] } | null
): boolean {
  return name !== undefined && imported !== null;
}

function builderProjectPath(id: string | undefined): string {
  return id === undefined
    ? '/console/builder'
    : `/console/builder?project=${encodeURIComponent(id)}`;
}

export function BuilderConnected(): ReactElement {
  const { name } = useParams<{ name?: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const { projectId, setProjectId } = useBuilderProject();

  const navState = location.state as BuilderNavState | null;
  const createSeed = matchingCreateSeed(navState, name);
  const isCreateMode = createSeed !== undefined && name !== undefined;

  const projectsView = useEntity<Project[]>(K.projects, () => listProjects());
  const projects = projectsView.data ?? [];
  const selectedProject = projects.find(p => p.id === projectId);
  const cwd = selectedProject?.path;

  useBuilderProjectQuerySync(projectId, searchParams, setSearchParams);

  // Workflow list for the open-picker + rename collision checks.
  const listKey = cwd !== undefined ? K.workflows(cwd) : IDLE_LIST_KEY;
  const listView = useEntity<WorkflowListResult>(listKey, () =>
    cwd !== undefined ? listWorkflows(cwd) : Promise.resolve({ workflows: [], recommended: [] })
  );
  const existingNames = useMemo(
    () => (listView.data?.workflows ?? []).map(w => w.name),
    [listView.data]
  );

  // Single-workflow load (skipped in create mode — the seed is authoritative).
  const idle = name === undefined || cwd === undefined || isCreateMode;
  const loadKey =
    idle || cwd === undefined || name === undefined ? 'builder:idle' : K.workflow(cwd, name);
  const loadView = useEntity<LoadedWorkflow | null>(loadKey, () =>
    idle || cwd === undefined || name === undefined
      ? Promise.resolve<LoadedWorkflow | null>(null)
      : loadWorkflow(name, cwd)
  );

  const loadedSource = loadedWorkflowSource(isCreateMode, loadView.data);
  const imported = useMemo(
    () => importedWorkflow(isCreateMode, createSeed, loadView.data),
    [isCreateMode, createSeed, loadView.data]
  );

  // Editing state — reset whenever the imported workflow changes (workflow switch).
  const [currentWorkflow, setCurrentWorkflow] = useState<BuilderWorkflow | null>(null);
  const [dirty, setDirty] = useState(false);
  const [serverIssues, setServerIssues] = useState<Issue[]>([]);
  // Source can flip after a bundled Save-as (bundled → project override).
  const [sourceOverride, setSourceOverride] = useState<WorkflowSource | null>(null);
  const [busy, setBusy] = useState(false);

  const effectiveSource = sourceOverride ?? loadedSource;
  const readOnly = isReadOnlySource(effectiveSource);

  const editorKey = `${cwd ?? ''}:${name ?? ''}:${String(isCreateMode)}`;
  useResetImportedWorkflow({
    imported,
    editorKey,
    isCreateMode,
    navState,
    setCurrentWorkflow,
    setDirty,
    setSourceOverride,
    setServerIssues,
  });

  useBeforeUnloadGuard(dirty);

  const confirmIfDirty = useCallback(
    (action: () => void): void => {
      if (dirty && !window.confirm('You have unsaved changes. Discard them?')) return;
      action();
    },
    [dirty]
  );

  const handleChange = useCallback((bw: BuilderWorkflow): void => {
    setCurrentWorkflow(bw);
    setDirty(true);
  }, []);

  const extraIssues = useMemo(
    () => [...(imported?.issues ?? []), ...serverIssues],
    [imported, serverIssues]
  );

  const projectQuery = `?project=${encodeURIComponent(projectId ?? '')}`;

  const actionContext: BuilderActionContext = {
    currentWorkflow,
    cwd,
    name,
    effectiveSource,
    isCreateMode,
    navigate,
    projectQuery,
    existingNames,
    setServerIssues,
    setDirty,
    setSourceOverride,
    setBusy,
  };

  const doSave = useCallback(async (): Promise<void> => {
    await saveCurrentWorkflow(actionContext);
  }, [actionContext]);

  const doDelete = useCallback(async (): Promise<void> => {
    await deleteCurrentWorkflow(actionContext);
  }, [actionContext]);

  const doRename = useCallback(async (): Promise<void> => {
    await renameCurrentWorkflow(actionContext);
  }, [actionContext]);

  const doNew = useCallback((): void => {
    createNewWorkflow(actionContext);
  }, [actionContext]);

  // --- Navigation controls (dirty-guarded) ---------------------------------
  const onPickProject = useCallback(
    (id: string): void => {
      confirmIfDirty(() => {
        // The "Select a project…" option has value ""; normalize it to the
        // hook's no-selection contract (`undefined`) and omit `?project=`.
        const next = id === '' ? undefined : id;
        setProjectId(next);
        navigate(builderProjectPath(next));
      });
    },
    [confirmIfDirty, setProjectId, navigate]
  );

  const onOpenWorkflow = useCallback(
    (wf: string): void => {
      if (wf === '' || wf === name) return;
      confirmIfDirty(() => {
        navigate(`/console/builder/${encodeURIComponent(wf)}${projectQuery}`);
      });
    },
    [confirmIfDirty, navigate, projectQuery, name]
  );

  // Ensure the currently-open name is selectable even when it isn't in the list
  // yet — e.g. a create-mode name (no file on disk), or before the list fetch
  // resolves. Dedupe.
  const openOptions = useMemo(() => sortedOpenOptions(existingNames, name), [existingNames, name]);

  const saveLabel = readOnly ? 'Save as' : 'Save';
  const canSave = canSaveWorkflow(busy, readOnly, dirty, isCreateMode);
  const workflowOpen = isWorkflowOpen(name, imported);
  // Split a genuine 404 (workflow doesn't exist → offer New) from other load
  // failures (500/403/network) — the latter must NOT masquerade as "not found",
  // which would mislead and invite a duplicate via "Create a new workflow".
  const { notFound, workflowLoadError } = workflowLoadStates(name, isCreateMode, loadView.error);
  // Surface list-load failures instead of masking them as empty states — a
  // transient backend error otherwise reads as "no projects / no workflows".
  const listFetchError = projectsView.error ?? listView.error;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <BuilderHeader
        projectId={projectId}
        projects={projects}
        cwd={cwd}
        name={name}
        openOptions={openOptions}
        workflowOpen={workflowOpen}
        dirty={dirty}
        canSave={canSave}
        saveLabel={saveLabel}
        busy={busy}
        isCreateMode={isCreateMode}
        readOnly={readOnly}
        onPickProject={onPickProject}
        onOpenWorkflow={onOpenWorkflow}
        onNew={() => {
          confirmIfDirty(doNew);
        }}
        onSave={() => {
          void doSave();
        }}
        onRename={() => {
          void doRename();
        }}
        onDelete={() => {
          void doDelete();
        }}
      />

      {listFetchError !== undefined ? (
        <div
          title={listFetchError.message}
          className="border-b border-error/30 bg-error/10 px-4 py-1.5 font-mono text-[11px] text-error"
        >
          Failed to load: {listFetchError.message}
        </div>
      ) : null}

      {workflowOpen && readOnly ? (
        <div className="border-b border-border bg-warning/10 px-4 py-1.5 text-[11.5px] text-text-secondary">
          Bundled workflow — read-only. <span className="font-semibold">Save as</span> writes a
          project override that shadows the bundled default.
        </div>
      ) : null}

      <div className="min-h-0 flex-1">
        <BuilderBody
          selectedProject={selectedProject}
          workflowLoadError={workflowLoadError}
          notFound={notFound}
          name={name}
          existingNames={existingNames}
          imported={imported}
          currentWorkflow={currentWorkflow}
          editorKey={editorKey}
          handleChange={handleChange}
          extraIssues={extraIssues}
          onNew={() => {
            confirmIfDirty(doNew);
          }}
        />
      </div>
    </div>
  );
}
