// --- Types ---
export type {
  IsolationProviderType,
  IsolationWorkflowType,
  EnvironmentStatus,
  IssueIsolationRequest,
  PRIsolationRequest,
  ReviewIsolationRequest,
  ThreadIsolationRequest,
  TaskIsolationRequest,
  IsolationRequest,
  AdoptedWorktreeMetadata,
  CreatedWorktreeMetadata,
  WorktreeMetadata,
  WorktreeEnvironment,
  IsolatedEnvironment,
  DestroyOptions,
  WorktreeDestroyOptions,
  DestroyResult,
  IIsolationProvider,
  IsolationHints,
  IsolationBlockReason,
  IsolationEnvironmentRow,
  WorktreeCreateConfig,
  RepoConfigLoader,
  WorktreeStatusBreakdown,
  CreateEnvironmentParams,
  ResolveRequest,
  ResolutionMethod,
  IsolationResolution,
  ExecutionContext,
  WriteBackFinalizeResult,
  WriteBackApplySummary,
  BackendPrepareRequest,
  PreparedEnv,
  TrustedContainerArtifactSnapshot,
  IIsolationBackend,
  ContainerBackendConfig,
  HardenedProxyBudgetSeed,
  RestrictedEgressPolicyConfig,
  VerifiedProxyBudgetStatus,
} from './types';

export { isPRIsolationRequest, CONTAINER_LABELS } from './types';

// --- Backend seam (folder projects) ---
export { resolveFolderBackend } from './backend-router';
export type { ResolveFolderBackendOptions } from './backend-router';
export { InPlaceBackend } from './backends/in-place';
export { ContainerBackend } from './backends/container';
export type { ContainerBackendDeps } from './backends/container';

// --- Container backend primitives (docker CLI wrapper) ---
export { dockerCli, dockerPreflight, extractDockerError } from './container/docker-exec';
export { snapshotContainerArtifacts } from './container/artifact-snapshot';
export { normalizeEgressPolicy, encodeEgressPolicy, assertPublicAddress } from './egress/policy';
export type { RestrictedEgressPolicy, RestrictedEgressTarget } from './egress/policy';
export { encodeStrictEgressPolicy, decodeStrictEgressPolicy } from './egress/strict-policy';
export { assertTrustedProviderBudgetPolicy } from './egress/provider-budget-contract';
export type { TrustedProviderBudgetPolicy } from './egress/provider-budget-contract';
export type { ProxyBudgetGrant } from './egress/proxy-budget-ledger';
export type { DockerRunner, DockerExecOptions, DockerExecResult } from './container/docker-exec';
export type {
  ArtifactSnapshotFile,
  ArtifactSnapshotOptions,
  ArtifactSnapshotResult,
  TrustedArtifactVolumeMetadata,
} from './container/artifact-snapshot';
export { BrowserObservationService } from './browser/browser-observation';
export type {
  ApplicationDescriptor,
  BrowserObservationRequest,
  BrowserObservationResult,
  BrowserObservationServiceOptions,
  BrowserPolicy,
  CandidateSourceDescriptor,
  CandidateSourceFile,
} from './browser/browser-observation';

// --- Store ---
export type { IIsolationStore } from './store';

// --- Errors ---
export { IsolationBlockedError, classifyIsolationError } from './errors';

// --- Factory ---
export { getIsolationProvider, configureIsolation, resetIsolationProvider } from './factory';

// --- Resolver ---
export { IsolationResolver } from './resolver';
export type { IsolationResolverDeps } from './resolver';

// --- Provider ---
export { WorktreeProvider } from './providers/worktree';

// --- PR state lookup ---
export { getPrState } from './pr-state';
export type { PrState } from './pr-state';

// --- Worktree copy utility ---
export {
  copyWorktreeFiles,
  copyWorktreeFile,
  parseCopyFileEntry,
  isPathWithinRoot,
} from './worktree-copy';
export type { CopyFileEntry } from './worktree-copy';
