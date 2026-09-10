import { createHash, createHmac, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  copyFileSync,
  readFileSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { basename, dirname, join, relative, resolve } from 'node:path';
import { BrowserObservationService } from '@archon/isolation';
import { getArchonHome } from '@archon/paths';
import {
  assertTrustedProviderBudgetPolicy,
  decodeStrictEgressPolicy,
  encodeStrictEgressPolicy,
} from '@archon/isolation';
import type {
  BackendPrepareRequest,
  HardenedProxyBudgetSeed,
  ProxyBudgetGrant,
  RestrictedEgressPolicyConfig,
  TrustedProviderBudgetPolicy,
  BrowserObservationRequest,
  BrowserObservationResult,
  BrowserPolicy,
  CandidateSourceDescriptor,
  CandidateSourceFile,
} from '@archon/isolation';
import type {
  ControllerActionGrant,
  ControllerActionHandlerContext,
  ControllerActionHandlers,
  ControllerActionManifest,
} from '@archon/workflows/controller-actions';
import {
  computeControllerActionManifestDigest,
  computeControllerWorkflowDigest,
  isControllerActionManifestSealed,
} from '@archon/workflows/controller-actions';
import type { WorkflowBudgetGrant, WorkflowTokenBudget } from '@archon/workflows/budget';
import type { IWorkflowStore, WorkflowEventRecord } from '@archon/workflows/store';
import type { WorkflowDefinition, WorkflowSource } from '@archon/workflows/schemas/workflow';

const PRIVATE_DIR_MODE = 0o700;
const PRIVATE_FILE_MODE = 0o600;
const DEFAULT_TOTAL_TOKENS = 8_000_000;
const DEFAULT_DEADLINE_MS = 90 * 60 * 1000;
const NESTED_REPO_SCAN_DEPTH = 3;
const TRUSTED_GIT_PATH = '/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin';
const MAX_ORACLE_FILES = 16;
const MAX_ORACLE_FILE_BYTES = 1024 * 1024;
const MAX_CANDIDATE_FILES = 10_000;
const MAX_CANDIDATE_TOTAL_BYTES = 100 * 1024 * 1024;
const MAX_CANDIDATE_BUNDLE_BYTES = 200 * 1024 * 1024;
const DEFAULT_GIT_COMMAND_TIMEOUT_MS = 30_000;
const OPERATOR_POLICY_SCHEMA = 'archon.hardened-controller-planning-policy.v1';
const FREEZE_RECEIPT_SCHEMA = 'archon.planning-oracle-freeze-receipt.v1';
const APPROVAL_RECEIPT_SCHEMA = 'archon.planning-approval-receipt.v1';
const CANDIDATE_IMPORT_RECEIPT_SCHEMA = 'archon.candidate-import-receipt.v1';
const CANDIDATE_BLACKBOX_RECEIPT_SCHEMA = 'archon.candidate-blackbox-test-receipt.v1';
const STATIC_WEB_PROFILE = 'static-web-http-v1';
const NODE_HTTP_PROFILE = 'node-http-app-v1';
type CandidateBlackboxProfile = typeof STATIC_WEB_PROFILE | typeof NODE_HTTP_PROFILE;
const HMAC_KEY_BASENAME = 'controller-approval.hmac';
const SESSION_BINDING_BASENAME = 'controller-session-binding.json';
const COPIED_APPROVAL_POLICY_BASENAME = 'operator-planning-approval-policy.json';
export const HARDENED_CONTROLLER_POLICY_SCHEMA = 'archon.hardened-controller-session.v2';

export interface HardenedControllerRepoInput {
  targetPath: string;
  sourcePath?: string;
}

export interface HardenedControllerSessionInput {
  runId: string;
  workflow: WorkflowDefinition;
  workflowSource?: WorkflowSource;
  sourceRoot: string;
  repoInputs?: readonly HardenedControllerRepoInput[];
  conversationId: string;
  userMessage: string;
  image: string;
  requestedImage?: string;
  budget?: {
    deadlineAt?: string;
    tokens?: Partial<WorkflowTokenBudget> & { total?: number };
    authoritativeConsumed?: { input: number; output: number };
  };
}

export interface HardenedControllerRepoDescriptor {
  targetPath: string;
  commit: string;
  treeOid: string;
  originKind: 'shallow-git';
  originPath: string;
  originDigest: string;
  fileCount: number;
}

export interface HardenedControllerPolicyMetadata {
  schema: typeof HARDENED_CONTROLLER_POLICY_SCHEMA;
  runId: string;
  workflowName: string;
  workflowDigest: string;
  budgetGrant: WorkflowBudgetGrant;
  workflowSource?: WorkflowSource;
  image: string;
  requestedImage: string;
  conversationDigest: string;
  userMessageDigest: string;
  seedManifestDigest: string;
  repoInputs: readonly HardenedControllerRepoDescriptor[];
  approvalPolicyDigest?: string;
  approvalPolicyPath?: string;
  hmacKeyPath: string;
  sessionBindingPath: string;
  controllerActionGrants: readonly ControllerActionGrant[];
  egressPolicyB64?: string;
  proxyBudgetSeedDigest?: string;
}

export interface HardenedControllerSession {
  runId: string;
  workflowDigest: string;
  seed?: BackendPrepareRequest['seed'];
  workflowBudgetGrants: readonly WorkflowBudgetGrant[];
  controllerActionGrants: readonly ControllerActionGrant[];
  policyPath: string;
  privateDir: string;
  policyMetadata: HardenedControllerPolicyMetadata;
}

interface GitSeedSource {
  repoRoot: string;
  commit: string;
  sourcePrefix: string;
  treeOid: string;
  objectFormat: 'sha1' | 'sha256';
  bare?: boolean;
  gitDeadlineAt?: number;
}

export interface QuarantinedCandidateInput {
  quarantineGitDir: string;
  commit: string;
  treeOid: string;
  destination: string;
  maxFiles?: number;
  maxTotalBytes?: number;
  deadlineMs?: number;
}

export interface QuarantinedCandidateBundleImportInput {
  bundlePath: string;
  bundleSha256: string;
  candidateCommit: string;
  candidateTreeOid: string;
  trustedBaselineCommit: string;
  trustedBaselineTreeOid: string;
  trustedBaselineGitDir?: string;
  trustedBaselineOriginDigest?: string;
  quarantineGitDir: string;
  destination: string;
  maxBundleBytes?: number;
  maxFiles?: number;
  maxTotalBytes?: number;
  deadlineMs?: number;
}

export interface QuarantinedCandidateBundleImportDescriptor {
  schema: 'archon.quarantined-candidate-import.v1';
  authority: 'none';
  bundlePath: string;
  bundleSha256: string;
  candidateCommit: string;
  candidateTreeOid: string;
  trustedBaselineCommit: string;
  trustedBaselineTreeOid: string;
  trustedBaselineGitDir?: string;
  trustedBaselineOriginDigest?: string;
  quarantineGitDir: string;
  content: QuarantinedCandidateDescriptor;
}

export interface QuarantinedCandidateDescriptor {
  schema: 'archon.quarantined-candidate-content.v1';
  authority: 'none';
  quarantineGitDir: string;
  commit: string;
  treeOid: string;
  destination: string;
  fileCount: number;
  totalBytes: number;
  files: readonly SeedManifestFile[];
}

interface GitSeedEntry {
  mode: string;
  oid: string;
  path: string;
}

export interface SeedManifestFile {
  path: string;
  gitOid: string;
  sha256: string;
  size: number;
  executable: boolean;
}

interface OperatorApprovalPolicy {
  schema: typeof OPERATOR_POLICY_SCHEMA;
  version: 1;
  workflowDigest: string;
  grants: OperatorApprovalGrant[];
  egress?: OperatorEgressPolicy;
  providerBudget?: OperatorProviderBudgetPolicy;
  validatorSource?: OperatorValidatorSourcePolicy;
}

interface OperatorProviderBudgetPolicy {
  policies: readonly TrustedProviderBudgetPolicy[];
}

interface OperatorValidatorSourcePolicy {
  nodeModulesPath: string;
}

interface ValidatorDigestGrant {
  phase: string;
  actionManifest?: ControllerActionManifest;
  validatorCorePackageDigest?: string;
  validatorPackageDigest?: string;
}

interface OperatorEgressPolicy {
  image: string;
  policy: RestrictedEgressPolicyConfig;
}

interface OperatorApprovalGrant {
  nodeId: string;
  action: 'finalize-evidence' | 'verify-approval';
  phase: 'planning-freeze' | 'planning-approval' | 'candidate-import' | 'candidate-blackbox-test';
  oracleFiles?: string[];
  repositoryTarget?: string;
  bundleArtifact?: 'run/candidate.bundle';
  candidateArtifact?: 'run/candidate.json';
  approvalNodeId?: string;
  freezeNodeId?: string;
  candidateImportNodeId?: string;
  approvalReceiptNodeId?: string;
  profile?: CandidateBlackboxProfile;
  acceptancePolicyPath?: string;
  appRoot?: string;
  startupEntrypoint?: string;
  port?: number;
  staticHelperImage?: string;
  verifierImage?: string;
  validatorPackageDigest?: string;
  validatorCorePackageDigest?: string;
}

interface LoadedApprovalPolicy {
  path: string;
  digest: string;
  policy: OperatorApprovalPolicy;
}

export interface HardenedControllerActionDeps {
  session: HardenedControllerSession;
  store: IWorkflowStore;
  snapshotArtifacts?: (envId: string, destinationDir: string) => Promise<unknown>;
  browserObservationService?: Pick<BrowserObservationService, 'observe'>;
  playwrightNodeModules?: string | (() => string | undefined);
}

export function createRefusingHardenedControllerActions(): ControllerActionHandlers {
  const refuse = async (): Promise<string> => {
    throw new Error(
      'Hardened controller action is not wired to an independently verified receipt path.'
    );
  };
  return {
    'verify-approval': refuse,
    'finalize-evidence': refuse,
    publish: refuse,
    backfill: refuse,
  };
}

export function createHardenedControllerActions(
  deps: HardenedControllerActionDeps
): ControllerActionHandlers {
  const refusing = createRefusingHardenedControllerActions();
  return {
    ...refusing,
    'finalize-evidence': ctx => finalizeHardenedEvidence(ctx, deps),
    'verify-approval': ctx => verifyPlanningApproval(ctx, deps),
  };
}

async function finalizeHardenedEvidence(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps
): Promise<Record<string, unknown>> {
  if (ctx.node.phase === 'planning-freeze') return freezePlanningOracle(ctx, deps);
  if (ctx.node.phase === 'candidate-import') return importCandidateEvidence(ctx, deps);
  if (ctx.node.phase === 'candidate-blackbox-test') return runCandidateBlackboxTest(ctx, deps);
  throw new Error(
    'Only planning-freeze, candidate-import, and candidate-blackbox-test are supported.'
  );
}

async function importCandidateEvidence(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps
): Promise<Record<string, unknown>> {
  await assertControllerLive(ctx, deps);
  const input = readManifestInput(ctx.actionManifest);
  assertManifestBound(ctx, deps.session, input);
  const binding = candidateImportBinding(input, deps.session);
  const receiptPath = receiptPathFor(deps.session.privateDir, ctx.node.id, 'candidate-import');
  if (existsSync(receiptPath)) {
    const existing = readSealedReceipt(receiptPath, deps.session);
    assertCandidateImportReceiptBound(existing, ctx, input, binding, deps.session);
    assertCandidateImportMaterializationUnchanged(existing, ctx, deps.session, ctx.deadlineAt);
    await assertControllerLive(ctx, deps);
    return candidateImportOutputFromReceipt(existing, receiptPath);
  }
  const envId = readIsolationEnvId(ctx.workflowRun.metadata);
  if (!deps.snapshotArtifacts)
    throw new Error('Candidate import requires artifact snapshot support.');
  const snapshotDir = fixedSnapshotDir(deps.session.privateDir, ctx.actionManifest.id);
  if (existsSync(snapshotDir)) throw new Error('Candidate import found an incomplete snapshot.');
  await deps.snapshotArtifacts(envId, snapshotDir);
  await assertControllerLive(ctx, deps);
  const candidate = readCandidateProposal(join(snapshotDir, binding.candidateArtifact));
  await assertControllerLive(ctx, deps);
  const bundlePath = join(snapshotDir, binding.bundleArtifact);
  const bundleSha256 = gitFileDigest(readFileSync(bundlePath)).sha256;
  const quarantineParent = join(deps.session.privateDir, 'quarantine');
  const contentParent = join(deps.session.privateDir, 'candidate-content');
  mkdirSync(quarantineParent, { recursive: false, mode: PRIVATE_DIR_MODE });
  mkdirSync(contentParent, { recursive: false, mode: PRIVATE_DIR_MODE });
  const importResult = importQuarantinedCandidateBundleBefore(
    {
      bundlePath,
      bundleSha256,
      candidateCommit: candidate.commit,
      candidateTreeOid: candidate.tree,
      trustedBaselineCommit: binding.baseline.commit,
      trustedBaselineTreeOid: binding.baseline.treeOid,
      trustedBaselineGitDir: binding.baselineGitDir,
      trustedBaselineOriginDigest: binding.baseline.originDigest,
      quarantineGitDir: join(quarantineParent, `${ctx.node.id}.git`),
      destination: join(contentParent, ctx.node.id),
    },
    ctx.deadlineAt
  );
  await assertControllerLive(ctx, deps);
  const receipt = buildCandidateImportReceipt({
    ctx,
    input,
    session: deps.session,
    binding,
    envId,
    bundleSha256,
    importResult,
  });
  const sealed = sealReceipt(receipt, deps.session);
  writeJsonExclusive(receiptPath, sealed);
  return candidateImportOutputFromReceipt(receipt, receiptPath);
}

interface CandidateImportReceiptInput {
  ctx: ControllerActionHandlerContext;
  input: Record<string, unknown>;
  session: HardenedControllerSession;
  binding: CandidateImportBinding;
  envId: string;
  bundleSha256: string;
  importResult: QuarantinedCandidateBundleImportDescriptor;
}

function buildCandidateImportReceipt(
  details: CandidateImportReceiptInput
): Record<string, unknown> {
  const { ctx, input, session, binding, envId, bundleSha256, importResult } = details;
  return {
    schema: CANDIDATE_IMPORT_RECEIPT_SCHEMA,
    runId: ctx.workflowRun.id,
    workflowName: ctx.workflowName,
    workflowDigest: ctx.workflowDigest,
    controllerActionNodeId: ctx.node.id,
    actionManifestId: ctx.actionManifest.id,
    actionManifestDigest: ctx.actionManifest.digest,
    policyDigest: stringInput(input, 'policyDigest'),
    importedAt: new Date().toISOString(),
    isolationEnvId: envId,
    image: session.policyMetadata.image,
    requestedImage: session.policyMetadata.requestedImage,
    seedManifestDigest: session.policyMetadata.seedManifestDigest,
    sourceBindingDigest: stringInput(input, 'sourceBindingDigest'),
    repositoryTarget: binding.repositoryTarget,
    trustedBaselineCommit: binding.baseline.commit,
    trustedBaselineTreeOid: binding.baseline.treeOid,
    trustedBaselineOriginDigest: binding.baseline.originDigest,
    trustedBaselineGitDir: binding.baselineGitDir,
    candidateCommit: importResult.candidateCommit,
    candidateTreeOid: importResult.candidateTreeOid,
    bundlePath: importResult.bundlePath,
    bundleSha256,
    quarantineGitDir: importResult.quarantineGitDir,
    contentManifestDigest: digestStable(importResult.content.files),
    content: importResult.content,
    authority: 'controller-private-import-only',
    publicOutputAuthority: 'none',
    provenance:
      'controller-private candidate import receipt; authenticates imported commit/tree only, not correctness or release readiness',
  };
}

function candidateImportOutputFromReceipt(
  receipt: Record<string, unknown>,
  receiptPath: string
): Record<string, unknown> {
  return {
    schema: 'archon.candidate-import-result.v1',
    authority: 'none',
    repositoryTarget: stringInput(receipt, 'repositoryTarget'),
    candidate: {
      commit: stringInput(receipt, 'candidateCommit'),
      tree: stringInput(receipt, 'candidateTreeOid'),
    },
    import: candidateImportDescriptorFromReceipt(receipt),
    receipt: receiptPath,
    importedOnly: true,
  };
}

async function runCandidateBlackboxTest(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps
): Promise<Record<string, unknown>> {
  await assertControllerLive(ctx, deps);
  const input = readManifestInput(ctx.actionManifest);
  assertManifestBound(ctx, deps.session, input);
  const binding = blackboxBinding(input);
  const receiptPath = receiptPathFor(deps.session.privateDir, ctx.node.id, 'candidate-blackbox');
  if (existsSync(receiptPath)) {
    const existing = readSealedReceipt(receiptPath, deps.session);
    await assertCandidateBlackboxReceiptBound(existing, ctx, deps, binding);
    await assertControllerLive(ctx, deps);
    return candidateBlackboxOutputFromReceipt(existing, receiptPath);
  }
  const dependencies = await readBlackboxDependencies(ctx, deps, binding);
  const policy = readFrozenBrowserPolicy(dependencies.freeze, binding.acceptancePolicyPath);
  const candidateSource = candidateSourceFromImport(dependencies.importReceipt, binding);
  const validator = snapshotValidatorNodeModules(ctx, deps, binding);
  const observation = await runBrowserObservation(
    ctx,
    deps,
    binding,
    candidateSource,
    policy,
    validator.nodeModules
  );
  await assertControllerLive(ctx, deps);
  if (observation.authority !== 'none') {
    throw new Error('Candidate black-box raw observation authority changed.');
  }
  if (observation.status !== 'passed') {
    throw new Error('Candidate black-box verifier did not pass every frozen criterion.');
  }
  const receipt = buildCandidateBlackboxReceipt({
    ctx,
    input,
    binding,
    dependencies,
    policy,
    candidateSource,
    validator,
    observation,
  });
  const sealed = sealReceipt(receipt, deps.session);
  writeJsonExclusive(receiptPath, sealed);
  return candidateBlackboxOutputFromReceipt(receipt, receiptPath);
}

interface CandidateBlackboxBinding {
  repositoryTarget: string;
  candidateImportNodeId: string;
  freezeNodeId: string;
  approvalReceiptNodeId: string;
  profile: CandidateBlackboxProfile;
  acceptancePolicyPath: string;
  appRoot: string;
  startupEntrypoint?: string;
  port: number;
  staticHelperImage: string;
  verifierImage: string;
  validatorPackageDigest: string;
  validatorCorePackageDigest: string;
}

interface CandidateBlackboxDependencies {
  importReceiptPath: string;
  importReceiptDigest: string;
  importReceipt: Record<string, unknown>;
  freezeReceiptPath: string;
  freezeReceiptDigest: string;
  freeze: Record<string, unknown>;
  approvalReceiptPath: string;
  approvalReceiptDigest: string;
  approval: Record<string, unknown>;
}

interface CandidateBlackboxReceiptInput {
  ctx: ControllerActionHandlerContext;
  input: Record<string, unknown>;
  binding: CandidateBlackboxBinding;
  dependencies: CandidateBlackboxDependencies;
  policy: BrowserPolicy;
  candidateSource: CandidateSourceDescriptor;
  validator: ValidatorModuleSnapshot;
  observation: BrowserObservationResult;
}

interface ValidatorModuleSnapshot {
  nodeModules: string;
  playwrightManifest: Record<string, unknown>[];
  coreManifest: Record<string, unknown>[];
}

function buildCandidateBlackboxReceipt(
  details: CandidateBlackboxReceiptInput
): Record<string, unknown> {
  const { ctx, input, binding, dependencies, policy, candidateSource, validator, observation } =
    details;
  return {
    schema: CANDIDATE_BLACKBOX_RECEIPT_SCHEMA,
    runId: ctx.workflowRun.id,
    workflowName: ctx.workflowName,
    workflowDigest: ctx.workflowDigest,
    controllerActionNodeId: ctx.node.id,
    actionManifestId: ctx.actionManifest.id,
    actionManifestDigest: ctx.actionManifest.digest,
    policyDigest: stringInput(input, 'policyDigest'),
    sourceBindingDigest: stringInput(input, 'sourceBindingDigest'),
    seedManifestDigest: stringInput(input, 'seedManifestDigest'),
    image: stringInput(input, 'image'),
    requestedImage: stringInput(input, 'requestedImage'),
    completedAt: new Date().toISOString(),
    repositoryTarget: binding.repositoryTarget,
    candidateImportNodeId: binding.candidateImportNodeId,
    candidateImportReceiptPath: dependencies.importReceiptPath,
    candidateImportReceiptDigest: dependencies.importReceiptDigest,
    candidateCommit: candidateSource.commit,
    candidateTreeOid: candidateSource.tree,
    candidateContentManifestDigest: stringInput(
      dependencies.importReceipt,
      'contentManifestDigest'
    ),
    freezeNodeId: binding.freezeNodeId,
    freezeReceiptPath: dependencies.freezeReceiptPath,
    freezeReceiptDigest: dependencies.freezeReceiptDigest,
    freezeBindingId: stringInput(dependencies.freeze, 'bindingId'),
    oracleDigest: stringInput(dependencies.freeze, 'oracleDigest'),
    approvalReceiptNodeId: binding.approvalReceiptNodeId,
    approvalReceiptPath: dependencies.approvalReceiptPath,
    approvalReceiptDigest: dependencies.approvalReceiptDigest,
    profile: binding.profile,
    acceptancePolicyPath: binding.acceptancePolicyPath,
    acceptancePolicyDigest: digestStable(policy),
    acceptanceCriteria: policy.required.map(criterion => criterion.id),
    appRoot: binding.appRoot,
    ...(binding.startupEntrypoint ? { startupEntrypoint: binding.startupEntrypoint } : {}),
    port: binding.port,
    candidateSourceDigest: candidateSource.contentDigest,
    staticHelperImage: binding.staticHelperImage,
    staticHelperImageId: observation.app.imageId,
    verifierImage: binding.verifierImage,
    verifierImageId: observation.verifier.imageId,
    verifierPlaywrightVersion: observation.verifier.playwrightVersion,
    validatorPackageDigest: binding.validatorPackageDigest,
    validatorCorePackageDigest: binding.validatorCorePackageDigest,
    validatorNodeModulesPath: validator.nodeModules,
    validatorPackageManifest: validator.playwrightManifest,
    validatorCorePackageManifest: validator.coreManifest,
    rawObservationAuthority: observation.authority,
    rawObservationStatus: observation.status,
    rawObservationDigest: digestStable(observation),
    evidence: digestBrowserEvidence(observation),
    security: observation.security,
    authority: 'controller-private-browser-blackbox-criteria',
    publicOutputAuthority: 'none',
    provenance:
      'controller-private candidate black-box receipt; authenticates frozen browser criteria only, not release readiness or general API correctness',
  };
}

function candidateBlackboxOutputFromReceipt(
  receipt: Record<string, unknown>,
  receiptPath: string
): Record<string, unknown> {
  return {
    schema: 'archon.candidate-blackbox-test-result.v1',
    authority: 'none',
    status: stringInput(receipt, 'rawObservationStatus'),
    repositoryTarget: stringInput(receipt, 'repositoryTarget'),
    candidate: {
      commit: stringInput(receipt, 'candidateCommit'),
      tree: stringInput(receipt, 'candidateTreeOid'),
      contentDigest: stringInput(receipt, 'candidateSourceDigest'),
    },
    profile: stringInput(receipt, 'profile'),
    acceptancePolicyDigest: stringInput(receipt, 'acceptancePolicyDigest'),
    receipt: receiptPath,
    verifiedOnly: true,
  };
}

async function freezePlanningOracle(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps
): Promise<Record<string, unknown>> {
  await assertControllerLive(ctx, deps);
  const input = readManifestInput(ctx.actionManifest);
  assertManifestBound(ctx, deps.session, input);
  if (ctx.node.phase !== 'planning-freeze') throw new Error('Only planning-freeze is supported.');
  assertNotCancelled(ctx.signal);
  const oracleFiles = readOracleFilesInput(input);
  const receiptPath = receiptPathFor(deps.session.privateDir, ctx.node.id, 'freeze');
  if (existsSync(receiptPath)) {
    const existing = readSealedReceipt(receiptPath, deps.session);
    assertFreezeReceiptBound(existing, ctx, input);
    assertFreezeSnapshotUnchanged(existing, oracleFiles);
    await assertControllerLive(ctx, deps);
    return {
      binding_id: stringInput(existing, 'bindingId'),
      oracle_digest: stringInput(existing, 'oracleDigest'),
      receipt: receiptPath,
    };
  }
  const envId = readIsolationEnvId(ctx.workflowRun.metadata);
  if (!deps.snapshotArtifacts)
    throw new Error('Planning oracle freeze requires artifact snapshot support.');
  const snapshotDir = fixedSnapshotDir(deps.session.privateDir, ctx.actionManifest.id);
  if (existsSync(snapshotDir)) {
    throw new Error('Planning oracle freeze found an unsealed partial snapshot.');
  }
  await deps.snapshotArtifacts(envId, snapshotDir);
  await assertControllerLive(ctx, deps);
  const oracle = readOracleFiles(snapshotDir, oracleFiles);
  const receipt = {
    schema: FREEZE_RECEIPT_SCHEMA,
    runId: ctx.workflowRun.id,
    workflowName: ctx.workflowName,
    workflowDigest: ctx.workflowDigest,
    controllerActionNodeId: ctx.node.id,
    actionManifestId: ctx.actionManifest.id,
    actionManifestDigest: ctx.actionManifest.digest,
    policyDigest: stringInput(input, 'policyDigest'),
    frozenAt: new Date().toISOString(),
    isolationEnvId: envId,
    image: deps.session.policyMetadata.image,
    requestedImage: deps.session.policyMetadata.requestedImage,
    seedManifestDigest: deps.session.policyMetadata.seedManifestDigest,
    sourceBindingDigest: stringInput(input, 'sourceBindingDigest'),
    snapshotDir,
    oracleFiles: oracle.files,
    oracleDigest: digestStable(oracle.files),
    provenance: 'controller-owned artifact snapshot; no agent-supplied paths trusted',
  };
  const bindingId = randomBytes(32).toString('hex');
  await assertControllerLive(ctx, deps);
  const sealed = sealReceipt({ ...receipt, bindingId }, deps.session);
  writeJsonExclusive(receiptPath, sealed);
  return { binding_id: bindingId, oracle_digest: receipt.oracleDigest, receipt: receiptPath };
}

async function verifyPlanningApproval(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps
): Promise<Record<string, unknown>> {
  await assertControllerLive(ctx, deps);
  const input = readManifestInput(ctx.actionManifest);
  assertManifestBound(ctx, deps.session, input);
  if (ctx.node.phase !== 'planning-approval')
    throw new Error('Only planning-approval is supported.');
  const approvalNodeId = stringInput(input, 'approvalNodeId');
  const freezeNodeId = stringInput(input, 'freezeNodeId');
  const freeze = readSealedReceipt(
    receiptPathFor(deps.session.privateDir, freezeNodeId, 'freeze'),
    deps.session
  );
  const freezeGrant = freezeGrantFor(deps.session, freezeNodeId);
  assertFreezeReceiptMatchesGrant(freeze, freezeGrant, deps.session);
  assertFreezeSnapshotUnchanged(freeze, readOracleFilesInput(freezeGrant.actionManifest.input));
  const bindingId = stringInput(freeze, 'bindingId');
  const oracleDigest = stringInput(freeze, 'oracleDigest');
  const approval = await readHumanApproval(
    deps.store,
    ctx.workflowRun.id,
    freezeNodeId,
    approvalNodeId,
    bindingId,
    oracleDigest
  );
  await assertControllerLive(ctx, deps);
  const receiptPath = receiptPathFor(deps.session.privateDir, ctx.node.id, 'approval');
  if (existsSync(receiptPath)) {
    const existing = readSealedReceipt(receiptPath, deps.session);
    assertApprovalReceiptBound(
      existing,
      ctx,
      deps.session,
      approvalNodeId,
      freezeNodeId,
      freeze,
      approval
    );
    return {
      approved: true,
      binding_id: bindingId,
      oracle_digest: oracleDigest,
      receipt: receiptPath,
    };
  }
  const receipt = {
    schema: APPROVAL_RECEIPT_SCHEMA,
    runId: ctx.workflowRun.id,
    workflowName: ctx.workflowName,
    workflowDigest: ctx.workflowDigest,
    controllerActionNodeId: ctx.node.id,
    approvalNodeId,
    freezeNodeId,
    bindingId,
    oracleDigest,
    freezeCompletedOrder: approval.freezeCompletedOrder,
    approvalRequestedOrder: approval.requestedOrder,
    approvalCompletedOrder: approval.completedOrder,
    approvalReceivedOrder: approval.receivedOrder,
    image: deps.session.policyMetadata.image,
    requestedImage: deps.session.policyMetadata.requestedImage,
    seedManifestDigest: deps.session.policyMetadata.seedManifestDigest,
    sourceBindingDigest: stringInput(input, 'sourceBindingDigest'),
    decision: approval.decision,
    provenance: 'protected controller approval event; no actor field available',
  };
  await assertControllerLive(ctx, deps);
  const sealed = sealReceipt(receipt, deps.session);
  writeJsonExclusive(receiptPath, sealed);
  return {
    approved: true,
    binding_id: bindingId,
    oracle_digest: oracleDigest,
    receipt: receiptPath,
  };
}

export function importQuarantinedCandidateBundle(
  input: QuarantinedCandidateBundleImportInput
): QuarantinedCandidateBundleImportDescriptor {
  return importQuarantinedCandidateBundleBefore(
    input,
    Date.now() + boundedDeadlineMs(input.deadlineMs)
  );
}

function importQuarantinedCandidateBundleBefore(
  input: QuarantinedCandidateBundleImportInput,
  deadlineAt: number
): QuarantinedCandidateBundleImportDescriptor {
  const homeRoot = assertPrivateControllerRoot();
  const bundlePath = assertPrivateBundleFile(homeRoot, input.bundlePath, input.maxBundleBytes);
  assertBundleDigest(bundlePath, input.bundleSha256);
  const objectFormat = objectFormatFromBoundOids(
    input.candidateCommit,
    input.trustedBaselineCommit
  );
  const candidateCommit = assertObjectId(input.candidateCommit, objectFormat, 'commit');
  const candidateTreeOid = assertObjectId(input.candidateTreeOid, objectFormat, 'tree');
  const trustedBaselineCommit = assertObjectId(input.trustedBaselineCommit, objectFormat, 'commit');
  const trustedBaselineTreeOid = assertObjectId(input.trustedBaselineTreeOid, objectFormat, 'tree');
  const quarantineGitDir = createFreshPrivateBareQuarantine(
    homeRoot,
    input.quarantineGitDir,
    objectFormat,
    deadlineAt
  );
  seedTrustedBaselineObjects(
    homeRoot,
    input,
    quarantineGitDir,
    objectFormat,
    trustedBaselineCommit,
    trustedBaselineTreeOid,
    deadlineAt
  );
  fetchVerifiedBundle(bundlePath, quarantineGitDir, candidateCommit, deadlineAt);
  assertImportedQuarantineRepository(
    quarantineGitDir,
    candidateCommit,
    candidateTreeOid,
    trustedBaselineCommit,
    trustedBaselineTreeOid,
    deadlineAt
  );
  assertNoSharedOrLinkedGitFiles(quarantineGitDir);
  const content = materializeQuarantinedCandidateBefore(
    {
      quarantineGitDir,
      commit: candidateCommit,
      treeOid: candidateTreeOid,
      destination: input.destination,
      maxFiles: input.maxFiles,
      maxTotalBytes: input.maxTotalBytes,
    },
    deadlineAt
  );
  remainingGitTimeout(deadlineAt);
  return Object.freeze({
    schema: 'archon.quarantined-candidate-import.v1' as const,
    authority: 'none' as const,
    bundlePath,
    bundleSha256: input.bundleSha256.toLowerCase(),
    candidateCommit,
    candidateTreeOid,
    trustedBaselineCommit,
    trustedBaselineTreeOid,
    ...(input.trustedBaselineGitDir
      ? {
          trustedBaselineGitDir: resolve(input.trustedBaselineGitDir),
          trustedBaselineOriginDigest: input.trustedBaselineOriginDigest,
        }
      : {}),
    quarantineGitDir,
    content,
  });
}

export function materializeQuarantinedCandidate(
  input: QuarantinedCandidateInput
): QuarantinedCandidateDescriptor {
  return materializeQuarantinedCandidateBefore(
    input,
    Date.now() + boundedDeadlineMs(input.deadlineMs)
  );
}

function materializeQuarantinedCandidateBefore(
  input: QuarantinedCandidateInput,
  deadlineAt: number
): QuarantinedCandidateDescriptor {
  const homeRoot = assertPrivateControllerRoot();
  const quarantineGitDir = assertPrivateChildDirectory(
    homeRoot,
    input.quarantineGitDir,
    'Hardened controller quarantine Git directory'
  );
  assertFreshPrivateDestination(homeRoot, input.destination);
  const objectFormat = readBareObjectFormat(quarantineGitDir, deadlineAt);
  const commit = assertObjectId(input.commit, objectFormat, 'commit');
  const treeOid = assertObjectId(input.treeOid, objectFormat, 'tree');
  assertQuarantineRepository(quarantineGitDir, commit, treeOid, deadlineAt);
  const source: GitSeedSource = {
    repoRoot: quarantineGitDir,
    commit,
    treeOid,
    sourcePrefix: '',
    objectFormat,
    bare: true,
    gitDeadlineAt: deadlineAt,
  };
  const entries = listGitSeedEntries(source);
  assertCandidateEntryBounds(source, entries, input.maxFiles, input.maxTotalBytes);
  const destination = resolve(input.destination);
  mkdirSync(destination, { recursive: false, mode: PRIVATE_DIR_MODE });
  chmodSync(destination, PRIVATE_DIR_MODE);
  const files = writeGitSeedFiles(source, destination);
  remainingGitTimeout(deadlineAt);
  return Object.freeze({
    schema: 'archon.quarantined-candidate-content.v1' as const,
    authority: 'none' as const,
    quarantineGitDir,
    commit,
    treeOid,
    destination,
    fileCount: files.length,
    totalBytes: files.reduce((sum, file) => sum + file.size, 0),
    files: Object.freeze(files),
  });
}

export function prepareHardenedControllerSession(
  input: HardenedControllerSessionInput
): HardenedControllerSession {
  assertRunId(input.runId);
  assertResolvedImageId(input.image);
  const sourceRoot = assertSourceRoot(input.sourceRoot);
  const workflowDigest = computeControllerWorkflowDigest(input.workflow);
  const privateDir = createPrivateRunDir(input.runId);
  assertOutsideSourceRoot(sourceRoot, privateDir);
  const seedPath = join(privateDir, 'seed');
  mkdirSync(seedPath, { recursive: false, mode: PRIVATE_DIR_MODE });
  chmodSync(seedPath, PRIVATE_DIR_MODE);

  const seedManifest = materializeControllerSeed(sourceRoot, seedPath, input.repoInputs);
  const seedManifestPath = join(privateDir, 'seed-manifest.json');
  writeJson(seedManifestPath, seedManifest);
  const seedManifestDigest = digestString(JSON.stringify(seedManifest));

  const budgetGrant = buildBudgetGrant(input, workflowDigest);
  const approvalPolicy = loadOperatorApprovalPolicy(sourceRoot, workflowDigest);
  const copiedPolicyPath = approvalPolicy
    ? copyApprovalPolicy(approvalPolicy, privateDir)
    : undefined;
  const hmacKeyPath = writeHmacKey(privateDir);
  const controllerActionGrants = approvalPolicy
    ? buildControllerActionGrants(input, workflowDigest, seedManifest, approvalPolicy, hmacKeyPath)
    : [];
  const egressPolicyB64 = approvalPolicy?.policy.egress
    ? normalizeOperatorEgressPolicy(input.image, approvalPolicy.policy.egress)
    : undefined;
  const proxyBudgetSeed = approvalPolicy?.policy.providerBudget
    ? buildProxyBudgetSeed(
        input.runId,
        workflowDigest,
        input.image,
        egressPolicyB64,
        budgetGrant,
        approvalPolicy.policy.providerBudget
      )
    : undefined;
  if (egressPolicyB64 && !proxyBudgetSeed) {
    throw new Error(
      'Hardened controller provider egress requires explicit provider budget policy.'
    );
  }
  if (proxyBudgetSeed && !egressPolicyB64) {
    throw new Error('Hardened controller provider budget policy requires egress authority.');
  }
  const sessionBindingPath = join(privateDir, SESSION_BINDING_BASENAME);
  const policyMetadata = buildPolicyMetadata(input, workflowDigest, seedManifest, {
    budgetGrant,
    approvalPolicy,
    copiedPolicyPath,
    hmacKeyPath,
    sessionBindingPath,
    controllerActionGrants,
    egressPolicyB64,
    proxyBudgetSeedDigest: proxyBudgetSeed ? digestStable(proxyBudgetSeed) : undefined,
  });
  writeSessionBinding(policyMetadata);
  const policyPath = join(privateDir, 'hardened-controller-policy.json');
  writePolicy(policyPath, policyMetadata, seedPath, seedManifestDigest, budgetGrant);

  return Object.freeze({
    runId: input.runId,
    workflowDigest,
    seed: Object.freeze({ kind: 'directory' as const, path: seedPath, allowGitMetadata: true }),
    workflowBudgetGrants: Object.freeze([freezeBudgetGrant(budgetGrant)]),
    controllerActionGrants: Object.freeze(controllerActionGrants),
    policyPath,
    privateDir,
    policyMetadata: freezePolicyMetadata(policyMetadata),
  });
}

export function resumeHardenedControllerSession(input: {
  runId: string;
  workflow: WorkflowDefinition;
  image: string;
  policyMetadata: unknown;
  budget: NonNullable<HardenedControllerSessionInput['budget']>;
}): HardenedControllerSession {
  assertRunId(input.runId);
  assertResolvedImageId(input.image);
  const workflowDigest = computeControllerWorkflowDigest(input.workflow);
  const policyMetadata = assertPolicyMetadata(input.policyMetadata);
  if (policyMetadata.runId !== input.runId || policyMetadata.workflowDigest !== workflowDigest) {
    throw new Error(`Cannot resume hardened run '${input.runId}': pinned policy does not match.`);
  }
  if (policyMetadata.image !== input.image) {
    throw new Error(`Cannot resume hardened run '${input.runId}': container image changed.`);
  }
  verifyCopiedApprovalPolicy(policyMetadata);
  verifyPrivateSessionBinding(policyMetadata);
  const budgetGrant = assertResumeBudgetGrant(input.budget, policyMetadata);
  const privateDir = dirname(policyMetadata.hmacKeyPath);
  return Object.freeze({
    runId: input.runId,
    workflowDigest,
    workflowBudgetGrants: Object.freeze([freezeBudgetGrant(budgetGrant)]),
    controllerActionGrants: Object.freeze([...policyMetadata.controllerActionGrants]),
    policyPath: policyMetadata.approvalPolicyPath ?? '',
    privateDir,
    policyMetadata: freezePolicyMetadata(policyMetadata),
  });
}

export function getHardenedControllerEgressPolicy(
  session: HardenedControllerSession
): RestrictedEgressPolicyConfig | undefined {
  verifyCopiedApprovalPolicy(session.policyMetadata);
  verifyPrivateSessionBinding(session.policyMetadata);
  const encoded = session.policyMetadata.egressPolicyB64;
  if (!encoded) return undefined;
  assertCopiedEgressMatches(session.policyMetadata, encoded);
  if (!session.policyMetadata.proxyBudgetSeedDigest) {
    throw new Error('Cannot use hardened egress: provider budget seed is missing.');
  }
  return configFromStrictEgressPolicy(encoded);
}

export function getHardenedControllerProxyBudgetSeed(
  session: HardenedControllerSession
): HardenedProxyBudgetSeed | undefined {
  verifyCopiedApprovalPolicy(session.policyMetadata);
  verifyPrivateSessionBinding(session.policyMetadata);
  const expectedDigest = session.policyMetadata.proxyBudgetSeedDigest;
  if (!expectedDigest) {
    if (session.policyMetadata.egressPolicyB64) {
      throw new Error('Cannot use hardened egress: provider budget seed is missing.');
    }
    return undefined;
  }
  if (!session.policyMetadata.egressPolicyB64) {
    throw new Error('Cannot use hardened provider budget: egress authority is missing.');
  }
  assertCopiedEgressMatches(session.policyMetadata, session.policyMetadata.egressPolicyB64);
  const copied = readCopiedOperatorProviderBudget(session.policyMetadata);
  if (!copied) {
    throw new Error('Cannot use hardened provider budget: copied operator policy is missing.');
  }
  const budgetGrant = assertBudgetGrantRecord(session.policyMetadata.budgetGrant);
  const seed = buildProxyBudgetSeed(
    session.policyMetadata.runId,
    session.policyMetadata.workflowDigest,
    session.policyMetadata.image,
    session.policyMetadata.egressPolicyB64,
    budgetGrant,
    copied
  );
  if (digestStable(seed) !== expectedDigest) {
    throw new Error('Cannot use hardened provider budget: seed digest changed.');
  }
  return freezeProxyBudgetSeed(seed);
}

export function getHardenedControllerValidatorNodeModules(
  session: HardenedControllerSession
): string | undefined {
  verifyCopiedApprovalPolicy(session.policyMetadata);
  verifyPrivateSessionBinding(session.policyMetadata);
  if (!sessionRequiresBlackboxValidator(session)) return undefined;
  const copied = readCopiedOperatorValidatorSource(session.policyMetadata);
  if (!copied) {
    throw new Error('Candidate black-box validator source is missing from copied operator policy.');
  }
  const nodeModulesPath = assertOperatorPrivateValidatorNodeModules(copied.nodeModulesPath);
  assertValidatorSourceMatchesBlackboxGrants(nodeModulesPath, session.controllerActionGrants);
  return nodeModulesPath;
}

export function assertHardenedControllerResumeRoute(
  runId: string,
  isolation: unknown,
  required: boolean
): void {
  assertRunId(runId);
  if (isolation === 'container') return;
  const root = join(getArchonHome(), 'controller-runs');
  const state = lstatSync(root, { throwIfNoEntry: false });
  let captured = false;
  if (state) {
    assertPrivateDirectory(root);
    const prefix = `${runId}-`;
    captured = readdirSync(root).some(
      name =>
        name.startsWith(prefix) &&
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
          name.slice(prefix.length)
        )
    );
  }
  if (required || captured)
    throw new Error('Cannot resume hardened run through a host isolation route.');
}

function assertResolvedImageId(image: string): void {
  if (!/^sha256:[0-9a-f]{64}$/.test(image)) {
    throw new Error('Hardened controller image must be resolved to an immutable sha256 image id.');
  }
}

function assertRunId(runId: string): void {
  if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(runId)) {
    throw new Error('Hardened controller run id is missing or malformed.');
  }
}

function assertSourceRoot(path: string): string {
  const original = resolve(path);
  const originalStat = lstatSync(original);
  if (originalStat.isSymbolicLink()) {
    throw new Error('Hardened controller source root must not be a symlink.');
  }
  const resolved = realpathSync(original);
  if (!statSync(resolved).isDirectory()) {
    throw new Error('Hardened controller source root must be a directory.');
  }
  return resolved;
}

function createPrivateRunDir(runId: string): string {
  const root = join(getArchonHome(), 'controller-runs');
  ensurePrivateDirectory(root);
  const dir = join(root, `${runId}-${randomUUID()}`);
  mkdirSync(dir, { recursive: false, mode: PRIVATE_DIR_MODE });
  chmodSync(dir, PRIVATE_DIR_MODE);
  assertPrivateDirectory(dir);
  return dir;
}

function ensurePrivateDirectory(path: string): void {
  if (!existsSync(path)) {
    mkdirSync(path, { recursive: true, mode: PRIVATE_DIR_MODE });
    chmodSync(path, PRIVATE_DIR_MODE);
  }
  assertPrivateDirectory(path);
}

function assertPrivateDirectory(path: string): void {
  const stat = lstatSync(path);
  if (!stat.isDirectory() || stat.isSymbolicLink()) {
    throw new Error(`Hardened controller private path '${path}' must be a real directory.`);
  }
  const getUid = process.getuid;
  if (getUid && stat.uid !== getUid()) {
    throw new Error(`Hardened controller private path '${path}' is not owned by this user.`);
  }
  if ((stat.mode & 0o077) !== 0) {
    throw new Error(
      `Hardened controller private path '${path}' must not be group/world accessible.`
    );
  }
}

function boundedDeadlineMs(deadlineMs: number | undefined): number {
  if (deadlineMs === undefined) return DEFAULT_GIT_COMMAND_TIMEOUT_MS;
  if (!Number.isSafeInteger(deadlineMs) || deadlineMs <= 0 || deadlineMs > DEFAULT_DEADLINE_MS) {
    throw new Error('Quarantined candidate Git deadline is malformed.');
  }
  return deadlineMs;
}

function objectFormatFromBoundOids(
  candidateCommit: string,
  trustedBaselineCommit: string
): 'sha1' | 'sha256' {
  if (/^[0-9a-f]{40}$/i.test(candidateCommit) && /^[0-9a-f]{40}$/i.test(trustedBaselineCommit)) {
    return 'sha1';
  }
  if (/^[0-9a-f]{64}$/i.test(candidateCommit) && /^[0-9a-f]{64}$/i.test(trustedBaselineCommit)) {
    return 'sha256';
  }
  throw new Error('Quarantined candidate commit oid is malformed.');
}

function assertPrivateBundleFile(
  homeRoot: string,
  path: string,
  maxBundleBytes: number | undefined,
  basePath?: string
): string {
  const original = resolve(path);
  if (basePath) {
    assertNoSymlinkPathComponentsFromBase(basePath, original, 'Quarantined candidate bundle');
  }
  const stat = lstatSync(original);
  if (stat.isSymbolicLink()) throw new Error('Quarantined candidate bundle must not be a symlink.');
  const resolved = realpathSync(original);
  assertPathInsideRoot(homeRoot, resolved, 'Quarantined candidate bundle');
  assertPrivatePathOwnership(resolved, 'Quarantined candidate bundle');
  if (!stat.isFile()) throw new Error('Quarantined candidate bundle must be a regular file.');
  if (stat.nlink !== 1) throw new Error('Quarantined candidate bundle must not be hardlinked.');
  const sizeLimit = maxBundleBytes ?? MAX_CANDIDATE_BUNDLE_BYTES;
  if (!Number.isSafeInteger(sizeLimit) || sizeLimit <= 0 || stat.size > sizeLimit) {
    throw new Error('Quarantined candidate bundle size exceeds the configured limit.');
  }
  return resolved;
}

function assertBundleDigest(bundlePath: string, expectedDigest: string): void {
  if (!/^[0-9a-f]{64}$/i.test(expectedDigest)) {
    throw new Error('Quarantined candidate bundle digest is malformed.');
  }
  const actual = gitFileDigest(readFileSync(bundlePath)).sha256;
  if (actual !== expectedDigest.toLowerCase()) {
    throw new Error('Quarantined candidate bundle digest does not match trusted binding.');
  }
}

function createFreshPrivateBareQuarantine(
  homeRoot: string,
  path: string,
  objectFormat: 'sha1' | 'sha256',
  deadlineAt: number
): string {
  const resolved = resolve(path);
  const parent = realpathSync(dirname(resolved));
  assertPathInsideRoot(homeRoot, join(parent, basename(resolved)), 'Quarantined candidate import');
  assertPrivateDirectory(parent);
  if (existsSync(resolved)) throw new Error('Quarantined candidate quarantine already exists.');
  gitBareInitWithObjectFormat(resolved, objectFormat, deadlineAt);
  chmodSync(resolved, PRIVATE_DIR_MODE);
  rmSync(join(resolved, 'hooks'), { recursive: true, force: true });
  assertPrivateDirectory(resolved);
  return realpathSync(resolved);
}

function gitBareInitWithObjectFormat(
  path: string,
  objectFormat: 'sha1' | 'sha256',
  deadlineAt: number
): void {
  const args =
    objectFormat === 'sha256'
      ? ['init', '--bare', `--object-format=${objectFormat}`, path]
      : ['init', '--bare', path];
  execFileSync('git', safeGitGlobalArgs(args), {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: safeGitEnv(),
    timeout: remainingGitTimeout(deadlineAt),
  });
}

function seedTrustedBaselineObjects(
  homeRoot: string,
  input: QuarantinedCandidateBundleImportInput,
  quarantineGitDir: string,
  objectFormat: 'sha1' | 'sha256',
  trustedBaselineCommit: string,
  trustedBaselineTreeOid: string,
  deadlineAt: number
): void {
  assertTrustedBaselinePair(input);
  if (!input.trustedBaselineGitDir) return;
  const baselineGitDir = assertTrustedBaselineSource(
    homeRoot,
    input,
    objectFormat,
    trustedBaselineCommit,
    trustedBaselineTreeOid,
    deadlineAt
  );
  copyPinnedObjects(
    {
      repoRoot: baselineGitDir,
      commit: trustedBaselineCommit,
      treeOid: trustedBaselineTreeOid,
      sourcePrefix: '',
      objectFormat,
      bare: true,
      gitDeadlineAt: deadlineAt,
    },
    quarantineGitDir,
    true
  );
  markShallowCommit(quarantineGitDir, trustedBaselineCommit, true);
}

function assertTrustedBaselinePair(input: QuarantinedCandidateBundleImportInput): void {
  const hasPath =
    typeof input.trustedBaselineGitDir === 'string' && input.trustedBaselineGitDir !== '';
  const hasDigest =
    typeof input.trustedBaselineOriginDigest === 'string' &&
    input.trustedBaselineOriginDigest !== '';
  if (hasPath === hasDigest) return;
  throw new Error(
    'Quarantined candidate trusted baseline Git directory and origin digest must be supplied together.'
  );
}

function assertTrustedBaselineSource(
  homeRoot: string,
  input: QuarantinedCandidateBundleImportInput,
  objectFormat: 'sha1' | 'sha256',
  trustedBaselineCommit: string,
  trustedBaselineTreeOid: string,
  deadlineAt: number
): string {
  if (!input.trustedBaselineOriginDigest) {
    throw new Error('Quarantined candidate trusted baseline origin digest is required.');
  }
  if (!/^[0-9a-f]{64}$/i.test(input.trustedBaselineOriginDigest)) {
    throw new Error('Quarantined candidate trusted baseline origin digest is malformed.');
  }
  const gitDir = assertPrivateChildDirectory(
    homeRoot,
    input.trustedBaselineGitDir ?? '',
    'Quarantined candidate trusted baseline Git directory'
  );
  if (readBareObjectFormat(gitDir, deadlineAt) !== objectFormat) {
    throw new Error('Quarantined candidate trusted baseline object format does not match.');
  }
  assertNoSharedOrLinkedGitFiles(gitDir);
  assertBaselineSourceRepository(gitDir, trustedBaselineCommit, trustedBaselineTreeOid, deadlineAt);
  if (
    digestSeedOrigin(gitDir, trustedBaselineCommit, deadlineAt) !==
    input.trustedBaselineOriginDigest
  ) {
    throw new Error('Quarantined candidate trusted baseline source digest does not match.');
  }
  return gitDir;
}

function assertBaselineSourceRepository(
  gitDir: string,
  trustedBaselineCommit: string,
  trustedBaselineTreeOid: string,
  deadlineAt: number
): void {
  assertBareGitRepository(gitDir, deadlineAt);
  assertNoQuarantineObjectIndirections(gitDir, deadlineAt);
  assertNoQuarantineRemotes(gitDir, deadlineAt);
  assertPinnedCommitTree(gitDir, trustedBaselineCommit, trustedBaselineTreeOid, deadlineAt);
  gitBare(gitDir, ['fsck', '--strict', '--no-dangling'], deadlineAt);
}

function fetchVerifiedBundle(
  bundlePath: string,
  gitDir: string,
  candidateCommit: string,
  deadlineAt: number
): void {
  assertBundleHasOnlySealedCandidate(bundlePath, gitDir, candidateCommit, deadlineAt);
  verifySelfContainedBundle(gitDir, bundlePath, deadlineAt);
  fetchCandidateBundle(gitDir, bundlePath, deadlineAt);
}

function verifySelfContainedBundle(gitDir: string, bundlePath: string, deadlineAt: number): void {
  const timeout = remainingGitTimeout(deadlineAt);
  try {
    execFileSync('git', safeGitGlobalArgs(['--git-dir', gitDir, 'bundle', 'verify', bundlePath]), {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: safeGitEnv(),
      timeout,
    });
  } catch (error) {
    const stderr = error instanceof Error && 'stderr' in error ? String(error.stderr) : '';
    if (stderr.includes('prerequisite')) {
      throw new Error('Quarantined candidate bundle contains unsupported prerequisite commits.');
    }
    throw new Error(`Quarantined candidate bundle is invalid: ${(error as Error).message}`);
  }
}

function fetchCandidateBundle(gitDir: string, bundlePath: string, deadlineAt: number): void {
  try {
    gitBare(
      gitDir,
      ['fetch', '--no-tags', bundlePath, 'refs/candidates/sealed:refs/candidates/sealed'],
      deadlineAt
    );
  } catch (error) {
    const message = (error as Error).message;
    if (
      message.includes('did not send all necessary objects') ||
      message.includes('Failed to traverse parents')
    ) {
      throw new Error(
        'Quarantined candidate bundle is unsupported: shallow history omitted baseline parent objects.'
      );
    }
    throw error;
  }
}

function assertBundleHasOnlySealedCandidate(
  bundlePath: string,
  gitDir: string,
  candidateCommit: string,
  deadlineAt: number
): void {
  const heads = gitBareText(gitDir, ['bundle', 'list-heads', bundlePath], deadlineAt)
    .split('\n')
    .filter(Boolean);
  if (heads.length !== 1) {
    throw new Error('Quarantined candidate bundle must contain exactly refs/candidates/sealed.');
  }
  if (heads[0] !== `${candidateCommit} refs/candidates/sealed`) {
    throw new Error(
      'Quarantined candidate bundle refs/candidates/sealed does not match candidate.'
    );
  }
}

function assertImportedQuarantineRepository(
  gitDir: string,
  candidateCommit: string,
  candidateTreeOid: string,
  trustedBaselineCommit: string,
  trustedBaselineTreeOid: string,
  deadlineAt: number
): void {
  assertQuarantineRepository(gitDir, candidateCommit, candidateTreeOid, deadlineAt);
  assertPinnedCommitTree(gitDir, trustedBaselineCommit, trustedBaselineTreeOid, deadlineAt);
  assertNoQuarantineRemotes(gitDir, deadlineAt);
  gitBare(gitDir, ['fsck', '--strict', '--no-dangling'], deadlineAt);
  try {
    gitBare(
      gitDir,
      ['merge-base', '--is-ancestor', trustedBaselineCommit, candidateCommit],
      deadlineAt
    );
  } catch {
    throw new Error('Quarantined candidate baseline is not an ancestor of the candidate commit.');
  }
}

function assertNoQuarantineRemotes(gitDir: string, deadlineAt: number): void {
  const remotes = gitBareText(gitDir, ['remote'], deadlineAt);
  if (remotes) throw new Error('Quarantined candidate repository must not contain remotes.');
}

function assertNoSharedOrLinkedGitFiles(gitDir: string): void {
  const stack = [gitDir];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) continue;
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const child = join(current, entry.name);
      const stat = lstatSync(child);
      if (stat.isSymbolicLink())
        throw new Error('Quarantined candidate repository contains symlinks.');
      if (stat.isFile() && stat.nlink !== 1) {
        throw new Error('Quarantined candidate repository must not share object files.');
      }
      if (entry.isDirectory()) stack.push(child);
    }
  }
}

function chmodPrivateGitDirectoryTree(path: string): void {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink())
    throw new Error('Private Git directory tree must not contain symlinks.');
  if (stat.isDirectory()) {
    chmodSync(path, PRIVATE_DIR_MODE);
    for (const entry of readdirSync(path)) chmodPrivateGitDirectoryTree(join(path, entry));
    return;
  }
  if (stat.isFile()) chmodSync(path, PRIVATE_FILE_MODE);
}

function assertPrivateControllerRoot(): string {
  const homeRoot = resolve(getArchonHome(), 'controller-runs');
  assertPrivateDirectory(homeRoot);
  return realpathSync(homeRoot);
}

function assertPrivateChildDirectory(homeRoot: string, path: string, label: string): string {
  const original = resolve(path);
  if (lstatSync(original).isSymbolicLink()) throw new Error(`${label} must not be a symlink.`);
  const resolved = realpathSync(original);
  assertPathInsideRoot(homeRoot, resolved, label);
  assertPrivateDirectory(resolved);
  return resolved;
}

function assertPathInsideRoot(root: string, path: string, label: string): void {
  const rel = relative(root, path);
  if (!rel || rel.startsWith('..') || rel.startsWith('/')) {
    throw new Error(`${label} must live under the private controller-runs directory.`);
  }
}

function assertFreshPrivateDestination(homeRoot: string, destination: string): void {
  const resolved = resolve(destination);
  const parent = realpathSync(dirname(resolved));
  assertPathInsideRoot(
    homeRoot,
    join(parent, basename(resolved)),
    'Quarantined candidate destination'
  );
  if (existsSync(resolved)) throw new Error('Quarantined candidate destination already exists.');
  assertPrivateDirectory(parent);
}

function assertObjectId(
  value: string,
  objectFormat: 'sha1' | 'sha256',
  label: 'commit' | 'tree'
): string {
  const expectedLength = objectFormat === 'sha1' ? 40 : 64;
  const pattern = new RegExp(`^[0-9a-f]{${expectedLength}}$`, 'i');
  if (!pattern.test(value)) throw new Error(`Quarantined candidate ${label} oid is malformed.`);
  return value.toLowerCase();
}

function readBareObjectFormat(gitDir: string, deadlineAt: number): 'sha1' | 'sha256' {
  const format = gitBareText(gitDir, ['rev-parse', '--show-object-format'], deadlineAt);
  if (format !== 'sha1' && format !== 'sha256') {
    throw new Error(`Unsupported Git object format for quarantined candidate: ${format}`);
  }
  return format;
}

function assertQuarantineRepository(
  gitDir: string,
  commit: string,
  treeOid: string,
  deadlineAt: number
): void {
  assertBareGitRepository(gitDir, deadlineAt);
  assertNoQuarantineObjectIndirections(gitDir, deadlineAt);
  assertSealedRef(gitDir, commit, deadlineAt);
  assertPinnedCommitTree(gitDir, commit, treeOid, deadlineAt);
}

function assertBareGitRepository(gitDir: string, deadlineAt: number): void {
  if (gitBareText(gitDir, ['rev-parse', '--is-bare-repository'], deadlineAt) !== 'true') {
    throw new Error('Quarantined candidate source must be a bare Git repository.');
  }
}

function assertNoQuarantineObjectIndirections(gitDir: string, deadlineAt: number): void {
  if (existsSync(join(gitDir, 'objects', 'info', 'alternates'))) {
    throw new Error('Quarantined candidate repository must not use alternate object stores.');
  }
  if (existsSync(join(gitDir, 'info', 'grafts'))) {
    throw new Error('Quarantined candidate repository must not use grafts.');
  }
  if (gitBareText(gitDir, ['for-each-ref', 'refs/replace', '--format=%(refname)'], deadlineAt)) {
    throw new Error('Quarantined candidate repository must not use replace refs.');
  }
}

function assertSealedRef(gitDir: string, commit: string, deadlineAt: number): void {
  const sealed = gitBareText(gitDir, ['rev-parse', 'refs/candidates/sealed^{commit}'], deadlineAt);
  if (sealed !== commit) {
    throw new Error('Quarantined candidate sealed ref does not match the pinned commit.');
  }
}

function assertPinnedCommitTree(
  gitDir: string,
  commit: string,
  treeOid: string,
  deadlineAt: number
): void {
  assertGitObjectType(gitDir, commit, 'commit', deadlineAt);
  assertGitObjectType(gitDir, treeOid, 'tree', deadlineAt);
  const actualTree = gitBareText(gitDir, ['rev-parse', `${commit}^{tree}`], deadlineAt);
  if (actualTree !== treeOid) {
    throw new Error('Quarantined candidate tree does not match the pinned commit.');
  }
}

function assertGitObjectType(
  gitDir: string,
  oid: string,
  expected: 'commit' | 'tree',
  deadlineAt: number
): void {
  let actual: string;
  try {
    actual = gitBareText(gitDir, ['cat-file', '-t', oid], deadlineAt);
  } catch {
    throw new Error(`Quarantined candidate ${expected} object is invalid.`);
  }
  if (actual !== expected) throw new Error(`Quarantined candidate ${expected} object is invalid.`);
}

function assertCandidateEntryBounds(
  source: GitSeedSource,
  entries: GitSeedEntry[],
  maxFiles: number | undefined,
  maxTotalBytes: number | undefined
): void {
  const fileLimit = maxFiles ?? MAX_CANDIDATE_FILES;
  if (!Number.isSafeInteger(fileLimit) || fileLimit <= 0 || entries.length > fileLimit) {
    throw new Error('Quarantined candidate file count exceeds the configured limit.');
  }
  const byteLimit = maxTotalBytes ?? MAX_CANDIDATE_TOTAL_BYTES;
  if (!Number.isSafeInteger(byteLimit) || byteLimit <= 0) {
    throw new Error('Quarantined candidate byte limit is malformed.');
  }
  const total = entries.reduce((sum, entry) => sum + seedEntrySize(source, entry), 0);
  if (total > byteLimit) throw new Error('Quarantined candidate byte total exceeds the limit.');
}

function seedEntrySize(source: GitSeedSource, entry: GitSeedEntry): number {
  assertSeedEntryAllowed(entry);
  const raw = gitForSource(source, ['cat-file', '-s', entry.oid]).toString('utf8').trim();
  const size = Number(raw);
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new Error(`Quarantined candidate blob size is malformed for '${entry.path}'.`);
  }
  return size;
}

function assertBlobIntegrity(
  bytes: Buffer,
  objectFormat: 'sha1' | 'sha256',
  entry: GitSeedEntry
): void {
  if (gitBlobOid(bytes, objectFormat) !== entry.oid) {
    throw new Error(`Quarantined candidate blob '${entry.path}' failed oid verification.`);
  }
}

function materializeControllerSeed(
  sourceRoot: string,
  seedPath: string,
  repoInputs: readonly HardenedControllerRepoInput[] | undefined
): Record<string, unknown> {
  if (repoInputs && repoInputs.length > 0) {
    return materializeDeclaredRepoSeed(sourceRoot, seedPath, repoInputs);
  }
  return materializeCommittedGitSeed(sourceRoot, seedPath);
}

function materializeCommittedGitSeed(
  sourceRoot: string,
  seedPath: string
): Record<string, unknown> {
  const source = resolveGitSeedSource(sourceRoot);
  assertNoUndeclaredNestedRepos(sourceRoot);
  assertCleanCommittedInputs(source);
  const files = writeGitSeedFiles(source, seedPath);
  return {
    schema: 'archon.hardened-controller-seed.v1',
    kind: 'single-git-tree',
    source: {
      kind: 'git-tree',
      commit: source.commit,
      treeOid: source.treeOid,
      sourcePrefix: source.sourcePrefix,
    },
    repoInputs: [],
    files,
  };
}

function materializeDeclaredRepoSeed(
  sourceRoot: string,
  seedPath: string,
  repoInputs: readonly HardenedControllerRepoInput[]
): Record<string, unknown> {
  const originDir = join(seedPath, '.archon', 'controller-origins');
  mkdirSync(originDir, { recursive: true, mode: PRIVATE_DIR_MODE });
  const targets = new Set<string>();
  const repos = repoInputs.map(input =>
    materializeDeclaredRepoInput(sourceRoot, seedPath, originDir, input, targets)
  );
  return {
    schema: 'archon.hardened-controller-seed.v1',
    kind: 'declared-repo-inputs',
    repoInputs: repos,
  };
}

function materializeDeclaredRepoInput(
  sourceRoot: string,
  seedPath: string,
  originDir: string,
  input: HardenedControllerRepoInput,
  targets: Set<string>
): HardenedControllerRepoDescriptor {
  const targetPath = assertDeclaredTargetPath(input.targetPath, targets);
  const source = resolveDeclaredRepoSource(sourceRoot, input.sourcePath ?? targetPath);
  assertCleanCommittedInputs(source);
  const originPath = join(originDir, `${bundleName(targetPath)}.git`);
  const targetRoot = safeDestination(seedPath, targetPath);
  mkdirSync(targetRoot, { recursive: true, mode: PRIVATE_DIR_MODE });
  hydrateDeclaredRepoFromPinnedObjects(targetRoot, originPath, source);
  return {
    targetPath,
    commit: source.commit,
    treeOid: source.treeOid,
    originKind: 'shallow-git',
    originPath: relative(seedPath, originPath),
    originDigest: digestSeedOrigin(originPath, source.commit),
    fileCount: listGitSeedEntries(source).length,
  };
}

function hydrateDeclaredRepoFromPinnedObjects(
  targetRoot: string,
  originPath: string,
  source: GitSeedSource
): void {
  git(targetRoot, ['init']);
  gitBareInit(originPath);
  copyPinnedObjects(source, targetRoot);
  copyPinnedObjects(source, originPath, true);
  markShallowCommit(targetRoot, source.commit);
  markShallowCommit(originPath, source.commit, true);
  gitBare(originPath, ['update-ref', 'refs/heads/archon-seed', source.commit]);
  git(targetRoot, ['checkout', '--detach', source.commit]);
  git(targetRoot, ['remote', 'add', 'origin', relative(targetRoot, originPath)]);
  rmSync(join(targetRoot, '.git', 'hooks'), { recursive: true, force: true });
  chmodPrivateGitDirectoryTree(originPath);
}

function assertDeclaredTargetPath(path: string, targets: Set<string>): string {
  const trimmed = path.trim().replace(/\\/g, '/').replace(/\/+$/, '');
  if (!trimmed || trimmed === '.' || trimmed.startsWith('/') || isForbiddenSeedPath(trimmed)) {
    throw new Error(`Hardened controller repo input target '${path}' is not allowed.`);
  }
  safeDestination('/', trimmed);
  for (const existing of targets) {
    if (
      existing === trimmed ||
      existing.startsWith(`${trimmed}/`) ||
      trimmed.startsWith(`${existing}/`)
    ) {
      throw new Error(`Hardened controller repo input target '${trimmed}' overlaps '${existing}'.`);
    }
  }
  targets.add(trimmed);
  return trimmed;
}

function resolveDeclaredRepoSource(sourceRoot: string, inputPath: string): GitSeedSource {
  const candidate = resolve(sourceRoot, inputPath);
  const sourceRel = relative(sourceRoot, candidate);
  if (sourceRel.startsWith('..') || sourceRel === '' || sourceRel.startsWith('/')) {
    throw new Error(
      'Hardened controller repo input source must be inside the controller source root.'
    );
  }
  assertNoSymlinkPathComponents(sourceRoot, candidate, inputPath);
  const resolved = realpathSync(candidate);
  const realRel = relative(sourceRoot, resolved);
  if (realRel.startsWith('..') || realRel === '' || realRel.startsWith('/')) {
    throw new Error(
      'Hardened controller repo input source must resolve inside the controller source root.'
    );
  }
  if (lstatSync(resolved).isSymbolicLink()) {
    throw new Error(`Hardened controller repo input source '${inputPath}' must not be a symlink.`);
  }
  const source = resolveGitSeedSource(resolved);
  if (source.sourcePrefix) {
    throw new Error(
      `Hardened controller repo input source '${inputPath}' must be a Git repository root.`
    );
  }
  return source;
}

function assertNoSymlinkPathComponents(
  sourceRoot: string,
  candidate: string,
  inputPath: string
): void {
  const parts = relative(sourceRoot, candidate).split('/').filter(Boolean);
  let current = sourceRoot;
  for (const part of parts) {
    current = join(current, part);
    if (lstatSync(current).isSymbolicLink()) {
      throw new Error(
        `Hardened controller repo input source '${inputPath}' must not contain symlink components.`
      );
    }
  }
}

function bundleName(targetPath: string): string {
  const base = basename(targetPath).replace(/[^A-Za-z0-9_.-]/g, '_') || 'repo';
  return `${base}-${digestString(targetPath).slice(0, 12)}`;
}

function copyPinnedObjects(source: GitSeedSource, destinationGitRoot: string, bare = false): void {
  for (const oid of listPinnedObjectOids(source)) {
    const type = gitForSourceText(source, ['cat-file', '-t', oid]);
    const bytes = gitForSource(source, ['cat-file', type, oid]);
    writeGitObject(destinationGitRoot, type, bytes, bare, source.gitDeadlineAt);
  }
}

function listPinnedObjectOids(source: GitSeedSource): string[] {
  const treeOids = gitForSource(source, [
    'ls-tree',
    '-r',
    '-t',
    '--full-tree',
    '--format=%(objectname)',
    source.commit,
  ])
    .toString('utf8')
    .split('\n')
    .filter(Boolean);
  return [...new Set([source.commit, source.treeOid, ...treeOids])];
}

function gitBareInit(path: string): void {
  execFileSync('git', safeGitGlobalArgs(['init', '--bare', path]), {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: safeGitEnv(),
  });
}

function markShallowCommit(gitRoot: string, commit: string, bare = false): void {
  const shallowPath = bare ? join(gitRoot, 'shallow') : join(gitRoot, '.git', 'shallow');
  writeFileSync(shallowPath, `${commit}\n`, { mode: PRIVATE_FILE_MODE });
}

function writeGitObject(
  gitRoot: string,
  type: string,
  bytes: Buffer,
  bare: boolean,
  deadlineAt: number | undefined
): void {
  const args = bare
    ? ['--git-dir', gitRoot, 'hash-object', '-w', '-t', type, '--stdin']
    : ['-C', gitRoot, 'hash-object', '-w', '-t', type, '--stdin'];
  execFileSync('git', safeGitGlobalArgs(args), {
    input: bytes,
    stdio: ['pipe', 'pipe', 'pipe'],
    env: safeGitEnv(),
    ...(deadlineAt !== undefined ? { timeout: remainingGitTimeout(deadlineAt) } : {}),
  });
}

function remainingGitTimeout(deadlineAt: number | undefined): number | undefined {
  if (deadlineAt === undefined) return undefined;
  const remaining = deadlineAt - Date.now();
  if (!Number.isSafeInteger(remaining) || remaining <= 0) {
    throw new Error('Quarantined candidate materialization exceeded its deadline.');
  }
  return Math.min(remaining, DEFAULT_GIT_COMMAND_TIMEOUT_MS);
}

function gitBare(gitRoot: string, args: string[], deadlineAt?: number): Buffer {
  const timeout = remainingGitTimeout(deadlineAt);
  try {
    return execFileSync('git', safeGitGlobalArgs(['--git-dir', gitRoot, ...args]), {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: safeGitEnv(),
      ...(timeout !== undefined ? { timeout } : {}),
    });
  } catch (error) {
    throw new Error(`Failed to build hardened committed seed: ${(error as Error).message}`);
  }
}

function gitBareText(gitRoot: string, args: string[], deadlineAt?: number): string {
  return gitBare(gitRoot, args, deadlineAt).toString('utf8').trim();
}

function digestSeedOrigin(originPath: string, commit: string, deadlineAt?: number): string {
  return digestString(
    [
      commit,
      readFileSync(join(originPath, 'shallow'), 'utf8'),
      gitBare(originPath, ['show-ref'], deadlineAt).toString('utf8'),
    ].join('\n')
  );
}

function assertNoUndeclaredNestedRepos(sourceRoot: string): void {
  const stack = [{ path: sourceRoot, depth: 0 }];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) continue;
    for (const entry of readdirSync(current.path, { withFileTypes: true })) {
      const child = join(current.path, entry.name);
      if (entry.name === '.git') {
        if (current.path !== sourceRoot) throw undeclaredPinnedInputsError();
        continue;
      }
      if (!entry.isDirectory() || current.depth >= NESTED_REPO_SCAN_DEPTH) continue;
      if (shouldSkipNestedRepoScan(entry.name)) continue;
      stack.push({ path: child, depth: current.depth + 1 });
    }
  }
}

function shouldSkipNestedRepoScan(name: string): boolean {
  return name === 'node_modules' || name === '.archon' || name === '.cache';
}

function undeclaredPinnedInputsError(): Error {
  return new Error(
    'Hardened controller committed seed cannot infer nested repository inputs. ' +
      'Multi-repo folder roots require controller-declared pinned repo inputs, which are not wired yet.'
  );
}

function resolveGitSeedSource(sourceRoot: string): GitSeedSource {
  let repoRoot: string;
  try {
    repoRoot = rawGitText(sourceRoot, ['rev-parse', '--show-toplevel']);
  } catch {
    throw undeclaredPinnedInputsError();
  }
  const commit = gitText(repoRoot, ['rev-parse', 'HEAD']);
  const sourcePrefix = gitText(sourceRoot, ['rev-parse', '--show-prefix']);
  const treeish = sourcePrefix
    ? `${commit}:${sourcePrefix.replace(/\/$/, '')}`
    : `${commit}^{tree}`;
  const treeOid = gitText(repoRoot, ['rev-parse', treeish]);
  const objectFormat = readObjectFormat(repoRoot);
  return { repoRoot, commit, sourcePrefix, treeOid, objectFormat };
}

function readObjectFormat(repoRoot: string): 'sha1' | 'sha256' {
  const format = gitText(repoRoot, ['rev-parse', '--show-object-format']);
  if (format !== 'sha1' && format !== 'sha256') {
    throw new Error(`Unsupported Git object format for hardened seed: ${format}`);
  }
  return format;
}

function assertCleanCommittedInputs(source: GitSeedSource): void {
  assertIndexMatchesPinnedTree(source);
  assertTrackedWorktreeMatchesPinnedTree(source);
}

function assertIndexMatchesPinnedTree(source: GitSeedSource): void {
  const expected = new Map(listGitSeedEntries(source).map(entry => [entry.path, entry]));
  const actual = readIndexEntries(source);
  if (actual.size !== expected.size) {
    throw new Error('Hardened controller source index differs from the pinned commit.');
  }
  for (const [path, entry] of expected) {
    const staged = actual.get(path);
    if (staged?.mode !== entry.mode || staged.oid !== entry.oid) {
      throw new Error('Hardened controller source index differs from the pinned commit.');
    }
  }
}

function readIndexEntries(source: GitSeedSource): Map<string, GitSeedEntry> {
  const args = ['ls-files', '--stage', '-z'];
  if (source.sourcePrefix) args.push('--', source.sourcePrefix.replace(/\/$/, ''));
  const output = git(source.repoRoot, args).toString('utf8');
  const entries = output.split('\0').filter(Boolean).map(parseGitIndexEntry);
  return new Map(entries.map(entry => [entry.path, entry]));
}

function parseGitIndexEntry(entry: string): GitSeedEntry {
  const tab = entry.indexOf('\t');
  if (tab < 0) throw new Error('Malformed git index entry while checking hardened seed.');
  const [mode, oid] = entry.slice(0, tab).split(' ');
  if (!mode || !oid) throw new Error('Malformed git index entry while checking hardened seed.');
  return { mode, oid, path: entry.slice(tab + 1) };
}

function assertTrackedWorktreeMatchesPinnedTree(source: GitSeedSource): void {
  for (const entry of listGitSeedEntries(source)) {
    assertSeedEntryAllowed(entry);
    assertTrackedFileMatchesPinnedBlob(source, entry);
  }
}

function assertTrackedFileMatchesPinnedBlob(source: GitSeedSource, entry: GitSeedEntry): void {
  const path = join(source.repoRoot, entry.path);
  assertNoSymlinkPathComponents(source.repoRoot, path, entry.path);
  const stat = lstatSync(path);
  if (!stat.isFile()) {
    throw new Error(`Hardened controller seed path '${entry.path}' must be a regular file.`);
  }
  const expectedExecutable = entry.mode === '100755';
  if (((stat.mode & 0o111) !== 0) !== expectedExecutable) {
    throw new Error(`Hardened controller seed path '${entry.path}' mode differs from Git.`);
  }
  const bytes = readFileSync(path);
  if (gitBlobOid(bytes, source.objectFormat) !== entry.oid) {
    throw new Error(
      `Hardened controller seed path '${entry.path}' differs from the pinned commit.`
    );
  }
}

function assertSeedEntryAllowed(entry: GitSeedEntry): void {
  if (entry.mode !== '100644' && entry.mode !== '100755') {
    throw new Error(`Hardened controller seed only supports regular files: '${entry.path}'.`);
  }
  if (isForbiddenSeedPath(entry.path)) {
    throw new Error(`Hardened controller seed contains a forbidden path: '${entry.path}'.`);
  }
}

function writeGitSeedFiles(source: GitSeedSource, seedPath: string): SeedManifestFile[] {
  return listGitSeedEntries(source).map(entry => writeGitSeedFile(source, entry, seedPath));
}

function writeGitSeedFile(
  source: GitSeedSource,
  entry: GitSeedEntry,
  seedPath: string
): SeedManifestFile {
  assertSeedEntryAllowed(entry);
  const relativePath = relativeGitPath(source.sourcePrefix, entry.path);
  const destination = safeDestination(seedPath, relativePath);
  mkdirSync(dirname(destination), { recursive: true, mode: PRIVATE_DIR_MODE });
  const bytes = gitForSource(source, ['cat-file', 'blob', entry.oid]);
  assertBlobIntegrity(bytes, source.objectFormat, entry);

  writeFileSync(destination, bytes, {
    flag: 'wx',
    mode: entry.mode === '100755' ? 0o700 : PRIVATE_FILE_MODE,
  });
  const written = gitFileDigest(bytes);
  return {
    path: relativePath,
    gitOid: entry.oid,
    sha256: written.sha256,
    size: written.size,
    executable: entry.mode === '100755',
  };
}

function listGitSeedEntries(source: GitSeedSource): GitSeedEntry[] {
  const args = ['ls-tree', '-r', '-z', '--full-tree', source.commit];
  if (source.sourcePrefix) args.push('--', source.sourcePrefix.replace(/\/$/, ''));
  const output = gitForSource(source, args);
  return output.toString('utf8').split('\0').filter(Boolean).map(parseGitTreeEntry);
}

function parseGitTreeEntry(entry: string): GitSeedEntry {
  const tab = entry.indexOf('\t');
  if (tab < 0) throw new Error('Malformed git tree entry while building hardened seed.');
  const [mode, type, oid] = entry.slice(0, tab).split(' ');
  if (type !== 'blob' || !mode || !oid) {
    throw new Error(
      `Hardened controller seed only supports regular files: '${entry.slice(tab + 1)}'.`
    );
  }
  return { mode, oid, path: entry.slice(tab + 1) };
}

function relativeGitPath(sourcePrefix: string, gitPath: string): string {
  if (!sourcePrefix) return gitPath;
  const prefix = sourcePrefix.endsWith('/') ? sourcePrefix : `${sourcePrefix}/`;
  if (!gitPath.startsWith(prefix)) {
    throw new Error(`Git seed path '${gitPath}' is outside source prefix '${sourcePrefix}'.`);
  }
  return gitPath.slice(prefix.length);
}

function safeDestination(seedPath: string, relativePath: string): string {
  if (!relativePath || relativePath.startsWith('/') || relativePath.split('/').includes('..')) {
    throw new Error(`Hardened controller seed path '${relativePath}' is unsafe.`);
  }
  const destination = resolve(seedPath, relativePath);
  const rel = relative(seedPath, destination);
  if (rel.startsWith('..') || rel === '') {
    throw new Error(`Hardened controller seed path '${relativePath}' escapes the seed directory.`);
  }
  return destination;
}

function isForbiddenSeedPath(path: string): boolean {
  return path.split('/').some(isForbiddenSeedPart);
}

function isForbiddenSeedPart(part: string): boolean {
  return (
    part === '.git' ||
    part === '.aws' ||
    part === '.docker' ||
    part === '.netrc' ||
    part === '.npmrc' ||
    part === '.env' ||
    part.startsWith('.env.')
  );
}

function loadOperatorApprovalPolicy(
  sourceRoot: string,
  workflowDigest: string
): LoadedApprovalPolicy | undefined {
  const policyPath = join(getArchonHome(), 'controller-policy', 'planning-approval.json');
  if (!existsSync(policyPath)) return undefined;
  assertPrivateFilePath(policyPath);
  assertOutsideSourceRoot(sourceRoot, policyPath);
  const raw = readFileSync(policyPath);
  const policy = parseOperatorApprovalPolicy(JSON.parse(raw.toString('utf8')));
  if (policy.workflowDigest !== workflowDigest) {
    throw new Error('Hardened controller approval policy does not match workflow digest.');
  }
  return { path: policyPath, digest: gitFileDigest(raw).sha256, policy };
}

function assertPrivateFilePath(path: string, label = 'Hardened controller policy'): void {
  let current = resolve(path);
  const root = resolve(getArchonHome());
  if (relative(root, current).startsWith('..')) {
    throw new Error(`${label} must live under the Archon home.`);
  }
  while (current !== dirname(current)) {
    assertPrivatePathOwnership(current, label);
    if (current === root) break;
    current = dirname(current);
  }
  if (!lstatSync(path).isFile()) throw new Error(`${label} must be a regular file.`);
}

function assertPrivatePathOwnership(path: string, label: string): void {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink()) throw new Error(`${label} path must not contain symlinks.`);
  const getUid = process.getuid;
  if (getUid && stat.uid !== getUid()) throw new Error(`${label} path is not owned by this user.`);
  if ((stat.mode & 0o077) !== 0) throw new Error(`${label} path must be private.`);
}

function assertOutsideSourceRoot(sourceRoot: string, path: string): void {
  const rel = relative(sourceRoot, realpathSync(path));
  if (!rel.startsWith('..') && !rel.startsWith('/')) {
    throw new Error('Hardened controller policy must be outside the workspace source root.');
  }
}

function parseOperatorApprovalPolicy(value: unknown): OperatorApprovalPolicy {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Malformed approval policy.');
  const record = value as Record<string, unknown>;
  assertExactKeys(
    record,
    [
      'egress',
      'grants',
      'providerBudget',
      'schema',
      'validatorSource',
      'version',
      'workflowDigest',
    ],
    'approval policy'
  );
  if (record.schema !== OPERATOR_POLICY_SCHEMA || record.version !== 1)
    throw new Error('Unsupported approval policy schema.');
  if (typeof record.workflowDigest !== 'string' || !Array.isArray(record.grants))
    throw new Error('Malformed approval policy.');
  const grants = record.grants.map(parseOperatorApprovalGrant);
  const egress = parseOptionalOperatorEgressPolicy(record.egress);
  const providerBudget = parseOptionalOperatorProviderBudget(record.providerBudget);
  const validatorSource = parseOptionalOperatorValidatorSource(record.validatorSource);
  if (egress && !providerBudget) {
    throw new Error('Approval policy provider egress requires explicit provider budget settings.');
  }
  if (providerBudget && !egress) {
    throw new Error('Approval policy provider budget settings require egress.');
  }
  assertUniqueApprovalGrants(grants);
  if (grants.some(grant => grant.phase === 'candidate-blackbox-test') && !validatorSource) {
    throw new Error('Candidate black-box validator source is missing from operator policy.');
  }
  return {
    schema: OPERATOR_POLICY_SCHEMA,
    version: 1,
    workflowDigest: record.workflowDigest,
    grants,
    ...(egress ? { egress } : {}),
    ...(providerBudget ? { providerBudget } : {}),
    ...(validatorSource ? { validatorSource } : {}),
  };
}

function parseOptionalOperatorValidatorSource(
  value: unknown
): OperatorValidatorSourcePolicy | undefined {
  if (value === undefined) return undefined;
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Malformed approval policy validator source.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(record, ['nodeModulesPath'], 'approval policy validator source');
  return {
    nodeModulesPath: stringInput(record, 'nodeModulesPath'),
  };
}

function parseOptionalOperatorEgressPolicy(value: unknown): OperatorEgressPolicy | undefined {
  if (value === undefined) return undefined;
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Malformed approval policy egress.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(record, ['image', 'policy'], 'approval policy egress');
  if (typeof record.image !== 'string') throw new Error('Malformed approval policy egress.');
  assertResolvedImageId(record.image);
  const encoded = assertCanonicalEgressConfig(record.policy);
  return { image: record.image, policy: configFromStrictEgressPolicy(encoded) };
}

function normalizeOperatorEgressPolicy(image: string, egress: OperatorEgressPolicy): string {
  if (egress.image !== image) {
    throw new Error('Hardened controller egress policy does not match the resolved image.');
  }
  return assertCanonicalEgressConfig(egress.policy);
}

function parseOptionalOperatorProviderBudget(
  value: unknown
): OperatorProviderBudgetPolicy | undefined {
  if (value === undefined) return undefined;
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Malformed approval policy provider budget.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(record, ['policies'], 'approval policy provider budget');
  if (!Array.isArray(record.policies) || record.policies.length === 0) {
    throw new Error('Approval policy provider budget requires policies.');
  }
  return { policies: Object.freeze(record.policies.map(parseTrustedProviderBudgetPolicy)) };
}

function parseTrustedProviderBudgetPolicy(value: unknown): TrustedProviderBudgetPolicy {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Malformed trusted provider budget policy.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(
    record,
    [
      'allowedHeaders',
      'anthropicBeta',
      'host',
      'maxInputTokens',
      'maxOutputTokens',
      'model',
      'provider',
    ],
    'trusted provider budget policy'
  );
  if (record.provider !== 'openai' && record.provider !== 'anthropic') {
    throw new Error('Trusted provider budget policy has unsupported provider.');
  }
  const provider: TrustedProviderBudgetPolicy['provider'] = record.provider;
  if (typeof record.host !== 'string' || record.host.trim() !== record.host || !record.host) {
    throw new Error('Trusted provider budget policy requires exact host.');
  }
  if (typeof record.model !== 'string' || record.model.trim() !== record.model || !record.model) {
    throw new Error('Trusted provider budget policy requires exact model.');
  }
  assertPositiveSafeTokenCount(record.maxInputTokens, 'provider max input tokens');
  assertPositiveSafeTokenCount(record.maxOutputTokens, 'provider max output tokens');
  const anthropicBeta = record.anthropicBeta;
  if (provider === 'anthropic' && typeof anthropicBeta !== 'boolean') {
    throw new Error('Trusted Anthropic provider budget policy requires explicit beta setting.');
  }
  if (provider === 'openai' && anthropicBeta !== undefined) {
    throw new Error('Trusted OpenAI provider budget policy must not set Anthropic beta.');
  }
  const basePolicy = {
    provider,
    host: record.host,
    model: record.model,
    maxInputTokens: record.maxInputTokens,
    maxOutputTokens: record.maxOutputTokens,
    ...(record.allowedHeaders !== undefined
      ? { allowedHeaders: copyTrustedProviderAllowedHeaders(record.allowedHeaders) }
      : {}),
  };
  const policy: TrustedProviderBudgetPolicy =
    provider === 'anthropic'
      ? { ...basePolicy, anthropicBeta: anthropicBeta as boolean }
      : basePolicy;
  assertTrustedProviderBudgetPolicy(policy);
  return freezeTrustedProviderPolicy(policy);
}

function copyTrustedProviderAllowedHeaders(
  value: unknown
): TrustedProviderBudgetPolicy['allowedHeaders'] {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Trusted provider budget policy allowedHeaders must be an object.');
  }
  const headers: NonNullable<TrustedProviderBudgetPolicy['allowedHeaders']> = {};
  for (const [name, headerValue] of Object.entries(value as Record<string, unknown>)) {
    headers[name] = Array.isArray(headerValue)
      ? Object.freeze([...headerValue])
      : (headerValue as string);
  }
  assertTrustedProviderBudgetPolicy({
    provider: 'openai',
    host: 'validator.local',
    model: 'validator-model',
    maxInputTokens: 1,
    maxOutputTokens: 1,
    allowedHeaders: headers,
  });
  return Object.freeze(headers);
}

function assertCanonicalEgressConfig(policy: unknown): string {
  return assertCanonicalEgressPolicyB64(
    encodeStrictEgressPolicy(policy as RestrictedEgressPolicyConfig)
  );
}

function assertCanonicalEgressPolicyB64(encoded: string): string {
  const normalized = configFromStrictEgressPolicy(encoded);
  const canonical = encodeStrictEgressPolicy(normalized);
  if (canonical !== encoded) {
    throw new Error('Cannot resume hardened run: persisted egress policy is not canonical.');
  }
  return canonical;
}

function configFromStrictEgressPolicy(encoded: string): RestrictedEgressPolicyConfig {
  const policy = decodeStrictEgressPolicy(encoded);
  return {
    targets: policy.transport.targets,
    connectTimeoutMs: policy.transport.connectTimeoutMs,
    dnsTimeoutMs: policy.transport.dnsTimeoutMs,
    idleTimeoutMs: policy.transport.idleTimeoutMs,
    maxTunnelMs: policy.transport.maxTunnelMs,
    maxConcurrentConnections: policy.transport.maxConcurrentConnections,
    httpGrants: policy.grants,
  };
}

function parseOperatorApprovalGrant(value: unknown): OperatorApprovalGrant {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Malformed approval policy grant.');
  const record = value as Record<string, unknown>;
  const action = record.action;
  const phase = record.phase;
  if (action === 'finalize-evidence' && phase === 'planning-freeze') {
    assertExactKeys(record, ['action', 'nodeId', 'oracleFiles', 'phase'], 'approval policy grant');
    const oracleFiles = readOracleFilesInput(record);
    assertUniqueStrings(oracleFiles, 'Planning oracle file');
    return { nodeId: stringInput(record, 'nodeId'), action, phase, oracleFiles };
  }
  if (action === 'finalize-evidence' && phase === 'candidate-import') {
    assertExactKeys(
      record,
      ['action', 'bundleArtifact', 'candidateArtifact', 'nodeId', 'phase', 'repositoryTarget'],
      'approval policy grant'
    );
    return {
      nodeId: stringInput(record, 'nodeId'),
      action,
      phase,
      repositoryTarget: assertSafeRelativePath(
        stringInput(record, 'repositoryTarget'),
        'Candidate import repository target'
      ),
      bundleArtifact: fixedArtifactInput(record, 'bundleArtifact', 'run/candidate.bundle'),
      candidateArtifact: fixedArtifactInput(record, 'candidateArtifact', 'run/candidate.json'),
    };
  }
  if (action === 'finalize-evidence' && phase === 'candidate-blackbox-test') {
    const profile = fixedProfileInput(record);
    assertExactKeys(
      record,
      [
        'acceptancePolicyPath',
        'action',
        'appRoot',
        'approvalReceiptNodeId',
        'candidateImportNodeId',
        'freezeNodeId',
        'nodeId',
        'phase',
        'port',
        'profile',
        'repositoryTarget',
        'staticHelperImage',
        ...(profile === NODE_HTTP_PROFILE ? ['startupEntrypoint'] : []),
        'validatorCorePackageDigest',
        'validatorPackageDigest',
        'verifierImage',
      ],
      'approval policy grant'
    );
    return {
      nodeId: stringInput(record, 'nodeId'),
      action,
      phase,
      repositoryTarget: assertSafeRelativePath(
        stringInput(record, 'repositoryTarget'),
        'Candidate black-box repository target'
      ),
      candidateImportNodeId: stringInput(record, 'candidateImportNodeId'),
      freezeNodeId: stringInput(record, 'freezeNodeId'),
      approvalReceiptNodeId: stringInput(record, 'approvalReceiptNodeId'),
      profile,
      acceptancePolicyPath: assertSafeRelativePath(
        stringInput(record, 'acceptancePolicyPath'),
        'Candidate black-box acceptance policy'
      ),
      appRoot: candidateSourceRelativePath(stringInput(record, 'appRoot'), 'appRoot', true),
      ...(profile === NODE_HTTP_PROFILE
        ? {
            startupEntrypoint: candidateStartupEntrypointInput(record),
          }
        : {}),
      port: boundedPortInput(record),
      staticHelperImage: assertImmutableImageRef(
        stringInput(record, 'staticHelperImage'),
        'Candidate black-box static helper image'
      ),
      verifierImage: assertImmutableImageRef(
        stringInput(record, 'verifierImage'),
        'Candidate black-box verifier image'
      ),
      validatorPackageDigest: assertSha256Hex(
        stringInput(record, 'validatorPackageDigest'),
        'Candidate black-box validator package digest'
      ),
      validatorCorePackageDigest: assertSha256Hex(
        stringInput(record, 'validatorCorePackageDigest'),
        'Candidate black-box validator core package digest'
      ),
    };
  }
  if (action === 'verify-approval' && phase === 'planning-approval') {
    assertExactKeys(
      record,
      ['action', 'approvalNodeId', 'freezeNodeId', 'nodeId', 'phase'],
      'approval policy grant'
    );
    return {
      nodeId: stringInput(record, 'nodeId'),
      action,
      phase,
      approvalNodeId: stringInput(record, 'approvalNodeId'),
      freezeNodeId: stringInput(record, 'freezeNodeId'),
    };
  }
  throw new Error(
    'Approval policy may only grant planning freeze, candidate import, candidate black-box test, or planning approval verification.'
  );
}

function fixedProfileInput(record: Record<string, unknown>): CandidateBlackboxProfile {
  if (record.profile === STATIC_WEB_PROFILE || record.profile === NODE_HTTP_PROFILE) {
    return record.profile;
  }
  throw new Error('Candidate black-box profile is unsupported.');
}

function candidateStartupEntrypointInput(record: Record<string, unknown>): string {
  const entrypoint = candidateSourceRelativePath(
    stringInput(record, 'startupEntrypoint'),
    'startupEntrypoint'
  );
  if (entrypoint.startsWith('-')) {
    throw new Error('Candidate black-box startup entrypoint must not be parsed as a Node option.');
  }
  return entrypoint;
}

function boundedPortInput(record: Record<string, unknown>): number {
  const port = numberInput(record, 'port');
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535) {
    throw new Error('Candidate black-box port is malformed.');
  }
  return port;
}

function fixedArtifactInput<T extends 'run/candidate.bundle' | 'run/candidate.json'>(
  record: Record<string, unknown>,
  key: string,
  expected: T
): T {
  if (record[key] !== expected) throw new Error(`Candidate import ${key} must be ${expected}.`);
  return expected;
}

function assertCandidateGrantMatchesSeed(
  grant: OperatorApprovalGrant,
  seedManifest: Record<string, unknown>
): void {
  if (grant.phase !== 'candidate-import' && grant.phase !== 'candidate-blackbox-test') return;
  const target = grant.repositoryTarget;
  if (!target) throw new Error('Candidate repository target is missing.');
  const repos = readRepoDescriptors(seedManifest);
  if (repos.length === 0) throw new Error('Candidate actions require declared repository inputs.');
  if (!repos.some(repo => repo.targetPath === target)) {
    throw new Error(`Candidate repository target '${target}' is not a declared input.`);
  }
}

function assertExactKeys(
  record: Record<string, unknown>,
  allowed: readonly string[],
  label: string
): void {
  const allowedSet = new Set(allowed);
  const extras = Object.keys(record).filter(key => !allowedSet.has(key));
  if (extras.length > 0) throw new Error(`${label} contains unsupported keys.`);
}

function assertUniqueApprovalGrants(grants: OperatorApprovalGrant[]): void {
  const seen = new Set<string>();
  for (const grant of grants) {
    const key = `${grant.nodeId}:${grant.action}:${grant.phase}`;
    if (seen.has(key)) throw new Error('Approval policy grants must be unique.');
    seen.add(key);
  }
}

function assertUniqueStrings(values: readonly string[], label: string): void {
  const seen = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) throw new Error(`${label} entries must be unique.`);
    seen.add(value);
  }
}

function copyApprovalPolicy(policy: LoadedApprovalPolicy, privateDir: string): string {
  const target = join(privateDir, COPIED_APPROVAL_POLICY_BASENAME);
  copyFileSync(policy.path, target);
  chmodSync(target, PRIVATE_FILE_MODE);
  if (gitFileDigest(readFileSync(target)).sha256 !== policy.digest)
    throw new Error('Approval policy changed while copying.');
  return target;
}

function writeHmacKey(privateDir: string): string {
  const path = join(privateDir, HMAC_KEY_BASENAME);
  writeFileSync(path, randomBytes(32), { mode: PRIVATE_FILE_MODE });
  return path;
}

function buildControllerActionGrants(
  input: HardenedControllerSessionInput,
  workflowDigest: string,
  seedManifest: Record<string, unknown>,
  approvalPolicy: LoadedApprovalPolicy,
  hmacKeyPath: string | undefined
): ControllerActionGrant[] {
  if (!hmacKeyPath) throw new Error('Hardened approval policy requires a private HMAC key.');
  assertOperatorValidatorSourceMatchesGrants(approvalPolicy.policy, approvalPolicy.policy.grants);
  return approvalPolicy.policy.grants.map(policyGrant => {
    assertOperatorGrantMatchesWorkflow(input.workflow, policyGrant);
    assertCandidateGrantMatchesSeed(policyGrant, seedManifest);
    const manifest = buildActionManifest(
      input,
      workflowDigest,
      seedManifest,
      approvalPolicy,
      policyGrant
    );
    return {
      runId: input.runId,
      workflowName: input.workflow.name,
      workflowDigest,
      nodeId: policyGrant.nodeId,
      action: policyGrant.action,
      phase: policyGrant.phase,
      actionManifest: manifest,
    };
  });
}

function assertOperatorGrantMatchesWorkflow(
  workflow: WorkflowDefinition,
  grant: OperatorApprovalGrant
): void {
  const node = workflow.nodes.find(candidate => candidate.id === grant.nodeId);
  if (!node || !('controller_action' in node)) {
    throw new Error(`Approval policy grant '${grant.nodeId}' does not match a controller node.`);
  }
  if (node.controller_action !== grant.action || node.phase !== grant.phase) {
    throw new Error(
      `Approval policy grant '${grant.nodeId}' does not match workflow action phase.`
    );
  }
  if (grant.action === 'verify-approval') assertApprovalGrantNodes(workflow, grant);
  if (grant.phase === 'candidate-blackbox-test') assertBlackboxGrantNodes(workflow, grant);
}

function assertApprovalGrantNodes(
  workflow: WorkflowDefinition,
  grant: OperatorApprovalGrant
): void {
  const approvalNode = workflow.nodes.find(candidate => candidate.id === grant.approvalNodeId);
  if (!approvalNode || !('approval' in approvalNode)) {
    throw new Error(`Approval policy grant '${grant.nodeId}' references a missing approval node.`);
  }
  const freezeNode = workflow.nodes.find(candidate => candidate.id === grant.freezeNodeId);
  if (
    !freezeNode ||
    !('controller_action' in freezeNode) ||
    freezeNode.controller_action !== 'finalize-evidence' ||
    freezeNode.phase !== 'planning-freeze'
  ) {
    throw new Error(`Approval policy grant '${grant.nodeId}' references a missing freeze node.`);
  }
}

function assertBlackboxGrantNodes(
  workflow: WorkflowDefinition,
  grant: OperatorApprovalGrant
): void {
  const importNode = workflow.nodes.find(candidate => candidate.id === grant.candidateImportNodeId);
  if (
    !importNode ||
    !('controller_action' in importNode) ||
    importNode.controller_action !== 'finalize-evidence' ||
    importNode.phase !== 'candidate-import'
  ) {
    throw new Error(`Approval policy grant '${grant.nodeId}' references a missing import node.`);
  }
  const freezeNode = workflow.nodes.find(candidate => candidate.id === grant.freezeNodeId);
  if (
    !freezeNode ||
    !('controller_action' in freezeNode) ||
    freezeNode.controller_action !== 'finalize-evidence' ||
    freezeNode.phase !== 'planning-freeze'
  ) {
    throw new Error(`Approval policy grant '${grant.nodeId}' references a missing freeze node.`);
  }
  const approvalNode = workflow.nodes.find(
    candidate => candidate.id === grant.approvalReceiptNodeId
  );
  if (
    !approvalNode ||
    !('controller_action' in approvalNode) ||
    approvalNode.controller_action !== 'verify-approval' ||
    approvalNode.phase !== 'planning-approval'
  ) {
    throw new Error(
      `Approval policy grant '${grant.nodeId}' references a missing approval receipt node.`
    );
  }
}

function buildActionManifest(
  input: HardenedControllerSessionInput,
  workflowDigest: string,
  seedManifest: Record<string, unknown>,
  approvalPolicy: LoadedApprovalPolicy,
  policyGrant: OperatorApprovalGrant
): ControllerActionManifest {
  const id = `planning:${input.runId}:${policyGrant.nodeId}`;
  const manifestInput = {
    runId: input.runId,
    workflowName: input.workflow.name,
    workflowDigest,
    policyDigest: approvalPolicy.digest,
    image: input.image,
    requestedImage: input.requestedImage ?? input.image,
    seedManifestDigest: digestString(JSON.stringify(seedManifest)),
    sourceBindingDigest: sourceBindingDigest(seedManifest),
    nodeId: policyGrant.nodeId,
    action: policyGrant.action,
    phase: policyGrant.phase,
    ...(policyGrant.oracleFiles ? { oracleFiles: policyGrant.oracleFiles } : {}),
    ...(policyGrant.approvalNodeId ? { approvalNodeId: policyGrant.approvalNodeId } : {}),
    ...(policyGrant.freezeNodeId ? { freezeNodeId: policyGrant.freezeNodeId } : {}),
    ...(policyGrant.candidateImportNodeId
      ? { candidateImportNodeId: policyGrant.candidateImportNodeId }
      : {}),
    ...(policyGrant.approvalReceiptNodeId
      ? { approvalReceiptNodeId: policyGrant.approvalReceiptNodeId }
      : {}),
    ...(policyGrant.repositoryTarget ? { repositoryTarget: policyGrant.repositoryTarget } : {}),
    ...(policyGrant.bundleArtifact ? { bundleArtifact: policyGrant.bundleArtifact } : {}),
    ...(policyGrant.candidateArtifact ? { candidateArtifact: policyGrant.candidateArtifact } : {}),
    ...(policyGrant.profile ? { profile: policyGrant.profile } : {}),
    ...(policyGrant.acceptancePolicyPath
      ? { acceptancePolicyPath: policyGrant.acceptancePolicyPath }
      : {}),
    ...(policyGrant.appRoot ? { appRoot: policyGrant.appRoot } : {}),
    ...(policyGrant.startupEntrypoint ? { startupEntrypoint: policyGrant.startupEntrypoint } : {}),
    ...(policyGrant.port ? { port: policyGrant.port } : {}),
    ...(policyGrant.staticHelperImage ? { staticHelperImage: policyGrant.staticHelperImage } : {}),
    ...(policyGrant.verifierImage ? { verifierImage: policyGrant.verifierImage } : {}),
    ...(policyGrant.validatorPackageDigest
      ? { validatorPackageDigest: policyGrant.validatorPackageDigest }
      : {}),
    ...(policyGrant.validatorCorePackageDigest
      ? { validatorCorePackageDigest: policyGrant.validatorCorePackageDigest }
      : {}),
  };
  const digest = computeControllerActionManifestDigest({ id, input: manifestInput });
  return { id, digest, input: manifestInput };
}

function readManifestInput(manifest: ControllerActionManifest): Record<string, unknown> {
  return manifest.input;
}

function assertManifestBound(
  ctx: ControllerActionHandlerContext,
  session: HardenedControllerSession,
  input: Record<string, unknown>
): void {
  if (
    stringInput(input, 'runId') !== ctx.workflowRun.id ||
    stringInput(input, 'runId') !== session.runId
  ) {
    throw new Error('Controller action manifest run binding does not match.');
  }
  if (stringInput(input, 'workflowName') !== ctx.workflowName) {
    throw new Error('Controller action manifest workflow binding does not match.');
  }
  if (stringInput(input, 'workflowDigest') !== ctx.workflowDigest) {
    throw new Error('Controller action manifest workflow digest does not match.');
  }
  if (stringInput(input, 'nodeId') !== ctx.node.id) {
    throw new Error('Controller action manifest node binding does not match.');
  }
  if (stringInput(input, 'action') !== ctx.node.controller_action) {
    throw new Error('Controller action manifest action binding does not match.');
  }
  if (stringInput(input, 'phase') !== ctx.node.phase) {
    throw new Error('Controller action manifest phase binding does not match.');
  }
  if (stringInput(input, 'policyDigest') !== session.policyMetadata.approvalPolicyDigest) {
    throw new Error('Controller action manifest policy binding does not match.');
  }
  if (stringInput(input, 'image') !== session.policyMetadata.image) {
    throw new Error('Controller action manifest image binding does not match.');
  }
  if (stringInput(input, 'requestedImage') !== session.policyMetadata.requestedImage) {
    throw new Error('Controller action manifest requested image binding does not match.');
  }
  if (stringInput(input, 'seedManifestDigest') !== session.policyMetadata.seedManifestDigest) {
    throw new Error('Controller action manifest source binding does not match.');
  }
  if (stringInput(input, 'sourceBindingDigest') !== sourceBindingDigest(session.policyMetadata)) {
    throw new Error('Controller action manifest source binding does not match.');
  }
}

interface CandidateProposal {
  commit: string;
  tree: string;
}

interface CandidateImportBinding {
  repositoryTarget: string;
  bundleArtifact: 'run/candidate.bundle';
  candidateArtifact: 'run/candidate.json';
  baseline: HardenedControllerRepoDescriptor;
  baselineGitDir: string;
}

interface CandidateContentRecord {
  schema: 'archon.quarantined-candidate-content.v1';
  authority: 'none';
  quarantineGitDir: string;
  commit: string;
  treeOid: string;
  destination: string;
  fileCount: number;
  totalBytes: number;
  files: readonly SeedManifestFile[];
}

function assertCandidateImportReceiptBound(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  input: Record<string, unknown>,
  binding: CandidateImportBinding,
  session: HardenedControllerSession
): void {
  if (receipt.schema !== CANDIDATE_IMPORT_RECEIPT_SCHEMA) {
    throw new Error('Candidate import receipt schema is invalid.');
  }
  assertCandidateReceiptContext(receipt, ctx, input, session);
  assertCandidateReceiptRepository(receipt, binding);
  const content = readCandidateContentRecord(receipt.content);
  if (stringInput(receipt, 'candidateCommit') !== content.commit) {
    throw new Error('Candidate import receipt candidate commit changed.');
  }
  if (stringInput(receipt, 'candidateTreeOid') !== content.treeOid) {
    throw new Error('Candidate import receipt candidate tree changed.');
  }
  if (stringInput(receipt, 'quarantineGitDir') !== content.quarantineGitDir) {
    throw new Error('Candidate import receipt quarantine changed.');
  }
  if (stringInput(receipt, 'contentManifestDigest') !== digestStable(content.files)) {
    throw new Error('Candidate import receipt content manifest changed.');
  }
}

function assertCandidateReceiptContext(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  input: Record<string, unknown>,
  session: HardenedControllerSession
): void {
  if (stringInput(receipt, 'runId') !== ctx.workflowRun.id) {
    throw new Error('Candidate import receipt run changed.');
  }
  if (stringInput(receipt, 'workflowDigest') !== ctx.workflowDigest) {
    throw new Error('Candidate import receipt workflow digest changed.');
  }
  if (stringInput(receipt, 'controllerActionNodeId') !== ctx.node.id) {
    throw new Error('Candidate import receipt node changed.');
  }
  if (stringInput(receipt, 'actionManifestDigest') !== ctx.actionManifest.digest) {
    throw new Error('Candidate import receipt action manifest digest changed.');
  }
  assertCandidateReceiptSource(receipt, input, session);
}

function assertCandidateReceiptSource(
  receipt: Record<string, unknown>,
  input: Record<string, unknown>,
  session: HardenedControllerSession
): void {
  if (stringInput(receipt, 'policyDigest') !== stringInput(input, 'policyDigest')) {
    throw new Error('Candidate import receipt policy changed.');
  }
  if (stringInput(receipt, 'image') !== session.policyMetadata.image) {
    throw new Error('Candidate import receipt image changed.');
  }
  if (stringInput(receipt, 'requestedImage') !== session.policyMetadata.requestedImage) {
    throw new Error('Candidate import receipt requested image changed.');
  }
  if (stringInput(receipt, 'seedManifestDigest') !== session.policyMetadata.seedManifestDigest) {
    throw new Error('Candidate import receipt source changed.');
  }
  if (stringInput(receipt, 'sourceBindingDigest') !== sourceBindingDigest(session.policyMetadata)) {
    throw new Error('Candidate import receipt source changed.');
  }
}

function assertCandidateReceiptRepository(
  receipt: Record<string, unknown>,
  binding: CandidateImportBinding
): void {
  if (stringInput(receipt, 'repositoryTarget') !== binding.repositoryTarget) {
    throw new Error('Candidate import receipt repository target changed.');
  }
  if (stringInput(receipt, 'trustedBaselineCommit') !== binding.baseline.commit) {
    throw new Error('Candidate import receipt baseline commit changed.');
  }
  if (stringInput(receipt, 'trustedBaselineTreeOid') !== binding.baseline.treeOid) {
    throw new Error('Candidate import receipt baseline tree changed.');
  }
  if (stringInput(receipt, 'trustedBaselineOriginDigest') !== binding.baseline.originDigest) {
    throw new Error('Candidate import receipt baseline origin changed.');
  }
  if (stringInput(receipt, 'trustedBaselineGitDir') !== binding.baselineGitDir) {
    throw new Error('Candidate import receipt baseline source changed.');
  }
}

function assertCandidateImportMaterializationUnchanged(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  session: HardenedControllerSession,
  deadlineAt: number
): void {
  const homeRoot = assertPrivateControllerRoot();
  const content = readCandidateContentRecord(receipt.content);
  assertCandidateImportFixedLayout(receipt, content, ctx, session, homeRoot);
  assertCandidateImportBundleUnchanged(receipt, ctx, session, homeRoot);
  assertCandidateContentFilesUnchanged(content, homeRoot);
  assertImportedQuarantineRepository(
    content.quarantineGitDir,
    stringInput(receipt, 'candidateCommit'),
    stringInput(receipt, 'candidateTreeOid'),
    stringInput(receipt, 'trustedBaselineCommit'),
    stringInput(receipt, 'trustedBaselineTreeOid'),
    deadlineAt
  );
  assertNoSharedOrLinkedGitFiles(content.quarantineGitDir);
}

function assertCandidateImportFixedLayout(
  receipt: Record<string, unknown>,
  content: CandidateContentRecord,
  ctx: ControllerActionHandlerContext,
  session: HardenedControllerSession,
  homeRoot: string
): void {
  const privateDir = realPrivateRunDirectory(homeRoot, session.privateDir);
  const expectedQuarantine = realPrivateChildPath(
    homeRoot,
    join(privateDir, 'quarantine', `${ctx.node.id}.git`),
    'Candidate import quarantine'
  );
  assertNoSymlinkPathComponentsFromBase(
    session.privateDir,
    join(session.privateDir, 'quarantine', `${ctx.node.id}.git`),
    'Candidate import quarantine'
  );
  const expectedDestination = realPrivateChildPath(
    homeRoot,
    join(privateDir, 'candidate-content', ctx.node.id),
    'Candidate import content root'
  );
  assertNoSymlinkPathComponentsFromBase(
    session.privateDir,
    join(session.privateDir, 'candidate-content', ctx.node.id),
    'Candidate import content root'
  );
  const actualQuarantine = realPrivateChildPath(
    homeRoot,
    content.quarantineGitDir,
    'Candidate import quarantine'
  );
  const actualDestination = realPrivateChildPath(
    homeRoot,
    content.destination,
    'Candidate import content root'
  );
  if (actualQuarantine !== expectedQuarantine) {
    throw new Error('Candidate import receipt quarantine path changed.');
  }
  if (stringInput(receipt, 'quarantineGitDir') !== expectedQuarantine) {
    throw new Error('Candidate import receipt quarantine path changed.');
  }
  if (actualDestination !== expectedDestination) {
    throw new Error('Candidate import receipt content root changed.');
  }
}

function assertCandidateImportBundleUnchanged(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  session: HardenedControllerSession,
  homeRoot: string
): void {
  const expectedBundlePath = realPrivateFilePath(
    homeRoot,
    join(fixedSnapshotDir(session.privateDir, ctx.actionManifest.id), 'run/candidate.bundle'),
    'Candidate import bundle'
  );
  assertNoSymlinkPathComponentsFromBase(
    session.privateDir,
    join(fixedSnapshotDir(session.privateDir, ctx.actionManifest.id), 'run/candidate.bundle'),
    'Candidate import bundle'
  );
  const bundlePath = assertPrivateBundleFile(
    homeRoot,
    stringInput(receipt, 'bundlePath'),
    undefined
  );
  if (bundlePath !== expectedBundlePath) {
    throw new Error('Candidate import receipt bundle path changed.');
  }
  assertBundleDigest(bundlePath, stringInput(receipt, 'bundleSha256'));
}

function assertCandidateContentFilesUnchanged(
  content: CandidateContentRecord,
  homeRoot: string
): void {
  const root = realPrivateChildPath(homeRoot, content.destination, 'Candidate import content root');
  let totalBytes = 0;
  const seen = new Set<string>();
  const expected = new Set(content.files.map(file => file.path));
  const expectedDirectories = expectedContentDirectories(content.files);
  const actual = new Set<string>();
  const actualDirectories = new Set<string>();
  assertExpectedContentAncestorDirectories(root, expectedDirectories);
  enumerateCandidateContentFiles(root, root, actual, actualDirectories);
  for (const file of content.files) {
    const destination = safeDestination(root, file.path);
    const stat = lstatSync(destination);
    if (!stat.isFile() || stat.isSymbolicLink()) {
      throw new Error('Candidate import receipt content file changed.');
    }
    if (stat.nlink !== 1) {
      throw new Error('Candidate import receipt content hardlink changed.');
    }
    if (((stat.mode & 0o111) !== 0) !== file.executable) {
      throw new Error('Candidate import receipt content mode changed.');
    }
    const bytes = readFileSync(destination);
    const digest = gitFileDigest(bytes);
    if (digest.sha256 !== file.sha256 || digest.size !== file.size) {
      throw new Error('Candidate import receipt content bytes changed.');
    }
    totalBytes += file.size;
    seen.add(file.path);
  }
  if (!sameStringSet(actual, expected)) {
    throw new Error('Candidate import receipt content manifest changed.');
  }
  if (!sameStringSet(actualDirectories, expectedDirectories)) {
    throw new Error('Candidate import receipt content directory manifest changed.');
  }
  if (seen.size !== content.fileCount || totalBytes !== content.totalBytes) {
    throw new Error('Candidate import receipt content manifest changed.');
  }
}

function expectedContentDirectories(files: readonly SeedManifestFile[]): Set<string> {
  const expected = new Set<string>();
  for (const file of files) {
    const parts = file.path.split('/').slice(0, -1);
    for (let index = 0; index < parts.length; index++) {
      expected.add(parts.slice(0, index + 1).join('/'));
    }
  }
  return expected;
}

function realPrivateChildPath(homeRoot: string, path: string, label: string): string {
  const resolved = assertPrivateChildDirectory(homeRoot, path, label);
  assertNoSymlinkPathComponents(homeRoot, resolved, label);
  return resolved;
}

function realPrivateRunDirectory(homeRoot: string, path: string): string {
  const resolved = assertPrivateChildDirectory(
    homeRoot,
    path,
    'Candidate import private run directory'
  );
  assertNoSymlinkPathComponents(homeRoot, resolved, 'Candidate import private run directory');
  return resolved;
}

function realPrivateFilePath(homeRoot: string, path: string, label: string): string {
  const original = resolve(path);
  if (lstatSync(original).isSymbolicLink()) throw new Error(`${label} must not be a symlink.`);
  const resolved = realpathSync(original);
  assertPathInsideRoot(homeRoot, resolved, label);
  assertNoSymlinkPathComponents(homeRoot, resolved, label);
  return resolved;
}

function assertNoSymlinkPathComponentsFromBase(
  basePath: string,
  targetPath: string,
  label: string
): void {
  const base = resolve(basePath);
  const target = resolve(targetPath);
  const rel = relative(base, target);
  if (!rel || rel.startsWith('..') || rel.startsWith('/')) {
    throw new Error(`${label} must stay under the private run directory.`);
  }
  let current = base;
  for (const part of rel.split('/').filter(Boolean)) {
    current = join(current, part);
    if (lstatSync(current).isSymbolicLink()) {
      throw new Error(`${label} must not contain symlink path components.`);
    }
  }
}

function assertExpectedContentAncestorDirectories(root: string, directories: Set<string>): void {
  for (const directory of directories) {
    const path = safeDestination(root, directory);
    const stat = lstatSync(path);
    if (!stat.isDirectory() || stat.isSymbolicLink()) {
      throw new Error('Candidate import receipt content directory changed.');
    }
    assertPrivateDirectory(path);
  }
}

function enumerateCandidateContentFiles(
  root: string,
  current: string,
  actual: Set<string>,
  actualDirectories: Set<string>
): void {
  if (current !== root) assertPathInsideRoot(root, current, 'Candidate import content path');
  for (const entry of readdirSync(current, { withFileTypes: true })) {
    const child = join(current, entry.name);
    const relativePath = relative(root, child);
    const stat = lstatSync(child);
    if (stat.isSymbolicLink()) {
      throw new Error('Candidate import receipt content symlink changed.');
    }
    if (stat.isDirectory()) {
      assertPrivateDirectory(child);
      actualDirectories.add(relativePath);
      enumerateCandidateContentFiles(root, child, actual, actualDirectories);
      continue;
    }
    if (!stat.isFile()) {
      throw new Error('Candidate import receipt content file changed.');
    }
    if (stat.nlink !== 1) {
      throw new Error('Candidate import receipt content hardlink changed.');
    }
    actual.add(relativePath);
  }
}

function sameStringSet(left: Set<string>, right: Set<string>): boolean {
  if (left.size !== right.size) return false;
  for (const value of left) if (!right.has(value)) return false;
  return true;
}

function candidateImportDescriptorFromReceipt(
  receipt: Record<string, unknown>
): QuarantinedCandidateBundleImportDescriptor {
  const content = readCandidateContentRecord(receipt.content);
  return Object.freeze({
    schema: 'archon.quarantined-candidate-import.v1' as const,
    authority: 'none' as const,
    bundlePath: stringInput(receipt, 'bundlePath'),
    bundleSha256: stringInput(receipt, 'bundleSha256'),
    candidateCommit: stringInput(receipt, 'candidateCommit'),
    candidateTreeOid: stringInput(receipt, 'candidateTreeOid'),
    trustedBaselineCommit: stringInput(receipt, 'trustedBaselineCommit'),
    trustedBaselineTreeOid: stringInput(receipt, 'trustedBaselineTreeOid'),
    trustedBaselineGitDir: stringInput(receipt, 'trustedBaselineGitDir'),
    trustedBaselineOriginDigest: stringInput(receipt, 'trustedBaselineOriginDigest'),
    quarantineGitDir: content.quarantineGitDir,
    content: Object.freeze({ ...content, files: Object.freeze([...content.files]) }),
  });
}

function readCandidateContentRecord(value: unknown): CandidateContentRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Candidate import receipt content is missing.');
  }
  const content = value as Record<string, unknown>;
  if (
    content.schema !== 'archon.quarantined-candidate-content.v1' ||
    content.authority !== 'none'
  ) {
    throw new Error('Candidate import receipt content schema is invalid.');
  }
  const files = readSeedManifestFiles(content.files, 'candidate import content manifest');
  return {
    schema: 'archon.quarantined-candidate-content.v1',
    authority: 'none',
    quarantineGitDir: stringInput(content, 'quarantineGitDir'),
    commit: stringInput(content, 'commit'),
    treeOid: stringInput(content, 'treeOid'),
    destination: stringInput(content, 'destination'),
    fileCount: numberInput(content, 'fileCount'),
    totalBytes: numberInput(content, 'totalBytes'),
    files,
  };
}

function readSeedManifestFiles(value: unknown, label: string): SeedManifestFile[] {
  if (!Array.isArray(value)) throw new Error(`${label} is malformed.`);
  return value.map(file => readSeedManifestFile(file, label));
}

function readSeedManifestFile(value: unknown, label: string): SeedManifestFile {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label} file is malformed.`);
  }
  const record = value as Record<string, unknown>;
  const executable = record.executable;
  if (typeof executable !== 'boolean') throw new Error(`${label} file is malformed.`);
  return {
    path: assertSafeRelativePath(stringInput(record, 'path'), label),
    gitOid: stringInput(record, 'gitOid'),
    sha256: stringInput(record, 'sha256'),
    size: numberInput(record, 'size'),
    executable,
  };
}

function candidateImportBinding(
  input: Record<string, unknown>,
  session: HardenedControllerSession
): CandidateImportBinding {
  const repositoryTarget = assertSafeRelativePath(
    stringInput(input, 'repositoryTarget'),
    'Candidate import repository target'
  );
  const baseline = session.policyMetadata.repoInputs.find(
    repo => repo.targetPath === repositoryTarget
  );
  if (!baseline) throw new Error('Candidate import repository target is not declared.');
  const bundleArtifact = fixedArtifactInput(input, 'bundleArtifact', 'run/candidate.bundle');
  const candidateArtifact = fixedArtifactInput(input, 'candidateArtifact', 'run/candidate.json');
  const seedRoot = join(session.privateDir, 'seed');
  return {
    repositoryTarget,
    bundleArtifact,
    candidateArtifact,
    baseline,
    baselineGitDir: join(seedRoot, baseline.originPath),
  };
}

function readCandidateProposal(path: string): CandidateProposal {
  const raw = readFileSync(path, 'utf8');
  const value = JSON.parse(raw) as unknown;
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Candidate proposal is malformed.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(record, ['commit', 'schema', 'tree'], 'candidate proposal');
  if (record.schema !== 'archon.candidate-proposal.v1') {
    throw new Error('Candidate proposal schema is unsupported.');
  }
  return { commit: stringInput(record, 'commit'), tree: stringInput(record, 'tree') };
}

function readOracleFilesInput(input: Record<string, unknown>): string[] {
  const raw = input.oracleFiles;
  if (!Array.isArray(raw) || raw.length === 0 || raw.length > MAX_ORACLE_FILES) {
    throw new Error('Planning oracle files must be a non-empty bounded list.');
  }
  return raw.map((value, index) => {
    if (typeof value !== 'string')
      throw new Error(`Planning oracle file ${String(index)} is malformed.`);
    return assertSafeRelativePath(value, 'Planning oracle file');
  });
}

function sourceBindingDigest(
  value: Record<string, unknown> | HardenedControllerPolicyMetadata
): string {
  const seedManifestDigest =
    'seedManifestDigest' in value && typeof value.seedManifestDigest === 'string'
      ? value.seedManifestDigest
      : digestString(JSON.stringify(value));
  const repoInputs =
    'repoInputs' in value && Array.isArray(value.repoInputs)
      ? value.repoInputs
      : readRepoDescriptors(value as Record<string, unknown>);
  return digestStable({ seedManifestDigest, repoInputs });
}

function blackboxBinding(input: Record<string, unknown>): CandidateBlackboxBinding {
  const profile = fixedProfileInput(input);
  const port = numberInput(input, 'port');
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535) {
    throw new Error('Candidate black-box port is malformed.');
  }
  return {
    repositoryTarget: assertSafeRelativePath(
      stringInput(input, 'repositoryTarget'),
      'Candidate black-box repository target'
    ),
    candidateImportNodeId: stringInput(input, 'candidateImportNodeId'),
    freezeNodeId: stringInput(input, 'freezeNodeId'),
    approvalReceiptNodeId: stringInput(input, 'approvalReceiptNodeId'),
    profile,
    acceptancePolicyPath: assertSafeRelativePath(
      stringInput(input, 'acceptancePolicyPath'),
      'Candidate black-box acceptance policy'
    ),
    appRoot: candidateSourceRelativePath(stringInput(input, 'appRoot'), 'appRoot', true),
    ...(profile === NODE_HTTP_PROFILE
      ? {
          startupEntrypoint: candidateStartupEntrypointInput(input),
        }
      : {}),
    port,
    staticHelperImage: assertImmutableImageRef(
      stringInput(input, 'staticHelperImage'),
      'Candidate black-box static helper image'
    ),
    verifierImage: assertImmutableImageRef(
      stringInput(input, 'verifierImage'),
      'Candidate black-box verifier image'
    ),
    validatorPackageDigest: assertSha256Hex(
      stringInput(input, 'validatorPackageDigest'),
      'Candidate black-box validator package digest'
    ),
    validatorCorePackageDigest: assertSha256Hex(
      stringInput(input, 'validatorCorePackageDigest'),
      'Candidate black-box validator core package digest'
    ),
  };
}

async function readBlackboxDependencies(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps,
  binding: CandidateBlackboxBinding
): Promise<CandidateBlackboxDependencies> {
  const importGrant = candidateImportGrantFor(deps.session, binding.candidateImportNodeId);
  const importReceiptPath = receiptPathFor(
    deps.session.privateDir,
    binding.candidateImportNodeId,
    'candidate-import'
  );
  const importReceipt = readSealedReceipt(importReceiptPath, deps.session);
  assertCandidateImportReceiptBound(
    importReceipt,
    contextForGrant(ctx, importGrant),
    importGrant.actionManifest.input,
    candidateImportBinding(importGrant.actionManifest.input, deps.session),
    deps.session
  );
  assertCandidateImportMaterializationUnchanged(
    importReceipt,
    contextForGrant(ctx, importGrant),
    deps.session,
    ctx.deadlineAt
  );
  const freezeGrant = freezeGrantFor(deps.session, binding.freezeNodeId);
  const freezeReceiptPath = receiptPathFor(deps.session.privateDir, binding.freezeNodeId, 'freeze');
  const freeze = readSealedReceipt(freezeReceiptPath, deps.session);
  assertFreezeReceiptMatchesGrant(freeze, freezeGrant, deps.session);
  assertFreezeSnapshotUnchanged(freeze, readOracleFilesInput(freezeGrant.actionManifest.input));
  const approval = await readAndVerifyApprovalReceiptNow(ctx, deps, binding, freeze);
  assertImportMatchesBlackboxBinding(importReceipt, binding);
  return {
    importReceiptPath,
    importReceiptDigest: sealedReceiptDigest(importReceiptPath),
    importReceipt,
    freezeReceiptPath,
    freezeReceiptDigest: sealedReceiptDigest(freezeReceiptPath),
    freeze,
    ...approval,
  };
}

async function readAndVerifyApprovalReceiptNow(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps,
  binding: CandidateBlackboxBinding,
  freeze: Record<string, unknown>
): Promise<
  Pick<CandidateBlackboxDependencies, 'approvalReceiptPath' | 'approvalReceiptDigest' | 'approval'>
> {
  const approvalGrant = approvalGrantFor(deps.session, binding.approvalReceiptNodeId);
  const approvalNodeId = stringInput(approvalGrant.actionManifest.input, 'approvalNodeId');
  const approval = await readHumanApproval(
    deps.store,
    ctx.workflowRun.id,
    binding.freezeNodeId,
    approvalNodeId,
    stringInput(freeze, 'bindingId'),
    stringInput(freeze, 'oracleDigest')
  );
  const approvalReceiptPath = receiptPathFor(
    deps.session.privateDir,
    binding.approvalReceiptNodeId,
    'approval'
  );
  const receipt = readSealedReceipt(approvalReceiptPath, deps.session);
  assertApprovalReceiptBound(
    receipt,
    contextForGrant(ctx, approvalGrant),
    deps.session,
    approvalNodeId,
    binding.freezeNodeId,
    freeze,
    approval
  );
  return {
    approvalReceiptPath,
    approvalReceiptDigest: sealedReceiptDigest(approvalReceiptPath),
    approval: receipt,
  };
}

function candidateImportGrantFor(
  session: HardenedControllerSession,
  nodeId: string
): ControllerActionGrant {
  const grant = session.controllerActionGrants.find(
    candidate =>
      candidate.nodeId === nodeId &&
      candidate.action === 'finalize-evidence' &&
      candidate.phase === 'candidate-import'
  );
  if (!grant) throw new Error('Referenced candidate import grant is not authorized.');
  return grant;
}

function approvalGrantFor(
  session: HardenedControllerSession,
  nodeId: string
): ControllerActionGrant {
  const grant = session.controllerActionGrants.find(
    candidate =>
      candidate.nodeId === nodeId &&
      candidate.action === 'verify-approval' &&
      candidate.phase === 'planning-approval'
  );
  if (!grant) throw new Error('Referenced approval receipt grant is not authorized.');
  return grant;
}

function contextForGrant(
  ctx: ControllerActionHandlerContext,
  grant: ControllerActionGrant
): ControllerActionHandlerContext {
  return {
    ...ctx,
    node: { id: grant.nodeId, controller_action: grant.action, phase: grant.phase },
    actionManifest: grant.actionManifest,
  };
}

function assertImportMatchesBlackboxBinding(
  receipt: Record<string, unknown>,
  binding: CandidateBlackboxBinding
): void {
  if (stringInput(receipt, 'repositoryTarget') !== binding.repositoryTarget) {
    throw new Error('Candidate black-box repository target does not match imported candidate.');
  }
}

function readFrozenBrowserPolicy(
  freeze: Record<string, unknown>,
  acceptancePolicyPath: string
): BrowserPolicy {
  const oracleFiles = readOracleFileRecords(freeze);
  const frozen = oracleFiles.find(file => file.path === acceptancePolicyPath);
  if (!frozen) {
    throw new Error('Candidate black-box acceptance policy is not in the frozen oracle.');
  }
  const snapshotDir = stringInput(freeze, 'snapshotDir');
  const path = safeDestination(snapshotDir, acceptancePolicyPath);
  assertNoSymlinkPathComponents(snapshotDir, path, acceptancePolicyPath);
  const bytes = readFileSync(path);
  if (gitFileDigest(bytes).sha256 !== frozen.sha256 || bytes.byteLength !== frozen.size) {
    throw new Error('Candidate black-box acceptance policy bytes changed.');
  }
  return parseBrowserPolicy(JSON.parse(bytes.toString('utf8')));
}

function readOracleFileRecords(receipt: Record<string, unknown>): {
  path: string;
  sha256: string;
  size: number;
}[] {
  const files = receipt.oracleFiles;
  if (!Array.isArray(files)) throw new Error('Freeze receipt oracle files are malformed.');
  return files.map(file => {
    if (!file || typeof file !== 'object' || Array.isArray(file)) {
      throw new Error('Freeze receipt oracle files are malformed.');
    }
    const record = file as Record<string, unknown>;
    return {
      path: stringInput(record, 'path'),
      sha256: assertSha256Hex(stringInput(record, 'sha256'), 'Frozen oracle file digest'),
      size: numberInput(record, 'size'),
    };
  });
}

function parseBrowserPolicy(value: unknown): BrowserPolicy {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Candidate black-box acceptance policy is malformed.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(record, ['required'], 'candidate black-box acceptance policy');
  if (!Array.isArray(record.required) || record.required.length === 0) {
    throw new Error('Candidate black-box acceptance policy needs required criteria.');
  }
  return { required: record.required.map(parseBrowserCriterion) };
}

function parseBrowserCriterion(value: unknown): BrowserPolicy['required'][number] {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Candidate black-box criterion is malformed.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(
    record,
    ['assertions', 'criterion', 'id', 'path'],
    'candidate black-box criterion'
  );
  const assertions = record.assertions;
  if (!Array.isArray(assertions) || assertions.length === 0) {
    throw new Error('Candidate black-box criterion assertions are malformed.');
  }
  return {
    id: stringInput(record, 'id'),
    criterion: stringInput(record, 'criterion'),
    path: browserPathInput(record),
    assertions: assertions.map(parseBrowserAssertion),
  };
}

function parseBrowserAssertion(
  value: unknown
): BrowserPolicy['required'][number]['assertions'][number] {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Candidate black-box assertion is malformed.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(record, ['text', 'type', 'value'], 'candidate black-box assertion');
  const assertion = {
    type: stringInput(record, 'type'),
    value: stringInput(record, 'value'),
    ...(record.text !== undefined ? { text: stringInput(record, 'text') } : {}),
  };
  if (!['click', 'fill', 'selector', 'testid', 'text', 'title', 'url'].includes(assertion.type)) {
    throw new Error('Candidate black-box assertion type is unsupported.');
  }
  return assertion;
}

function browserPathInput(record: Record<string, unknown>): string {
  const path = stringInput(record, 'path');
  if (!path.startsWith('/') || path.startsWith('//')) {
    throw new Error('Candidate black-box criterion path must be same-origin.');
  }
  return path;
}

function candidateSourceFromImport(
  receipt: Record<string, unknown>,
  binding: CandidateBlackboxBinding
): CandidateSourceDescriptor {
  const content = readCandidateContentRecord(receipt.content);
  const files = content.files.map(file => candidateSourceFile(content.destination, file));
  const source: Omit<CandidateSourceDescriptor, 'contentDigest'> = {
    profile: binding.profile,
    commit: stringInput(receipt, 'candidateCommit'),
    tree: stringInput(receipt, 'candidateTreeOid'),
    appRoot: binding.appRoot,
    ...(binding.startupEntrypoint ? { startup: { entrypoint: binding.startupEntrypoint } } : {}),
    files,
  };
  const withDigest: CandidateSourceDescriptor = {
    ...source,
    contentDigest: candidateSourceDigest(source),
  };
  assertCandidateSourceCoversAppRoot(withDigest);
  return withDigest;
}

function candidateSourceFile(root: string, file: SeedManifestFile): CandidateSourceFile {
  const path = candidateSourceRelativePath(file.path, 'path');
  const bytes = readFileSync(safeDestination(root, path));
  const digest = gitFileDigest(bytes);
  if (digest.sha256 !== file.sha256 || digest.size !== file.size) {
    throw new Error('Candidate black-box source bytes changed.');
  }
  return {
    type: 'file',
    path,
    size: digest.size,
    sha256: digest.sha256,
    contentBase64: bytes.toString('base64'),
  };
}

function candidateSourceDigest(source: Omit<CandidateSourceDescriptor, 'contentDigest'>): string {
  const descriptor: Record<string, unknown> = {
    profile: source.profile,
    commit: source.commit,
    tree: source.tree,
    appRoot: source.appRoot,
    files: candidateSourceFileDigestManifest(source.files),
  };
  if (source.startup) descriptor.startup = source.startup;
  return digestStable(descriptor);
}

function candidateSourceFileDigestManifest(
  files: CandidateSourceFile[]
): Record<string, unknown>[] {
  const manifest = files
    .map(file => ({ path: file.path, sha256: file.sha256, size: file.size, type: file.type }))
    .sort((left, right) => compareCodepoint(left.path, right.path));
  return manifest;
}

function compareCodepoint(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function assertCandidateSourceCoversAppRoot(source: CandidateSourceDescriptor): void {
  if (source.appRoot === '.') return;
  if (!source.files.some(file => file.path.startsWith(`${source.appRoot}/`))) {
    throw new Error('Candidate black-box appRoot is not present in the imported candidate.');
  }
}

async function runBrowserObservation(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps,
  binding: CandidateBlackboxBinding,
  candidateSource: CandidateSourceDescriptor,
  policy: BrowserPolicy,
  validatorNodeModules: string
): Promise<BrowserObservationResult> {
  const service =
    deps.browserObservationService ??
    new BrowserObservationService(undefined, {
      trustedCandidateHelperImage: binding.staticHelperImage,
      trustedVerifierImage: binding.verifierImage,
    });
  const request: BrowserObservationRequest = {
    runId: ctx.workflowRun.id,
    app: {
      image: binding.staticHelperImage,
      commit: candidateSource.commit,
      tree: candidateSource.tree,
      port: binding.port,
      candidateSource,
    },
    policy,
    playwrightNodeModules: validatorNodeModules,
    evidenceDir: join(deps.session.privateDir, 'browser-evidence', ctx.node.id),
    verifierImage: binding.verifierImage,
    signal: ctx.signal,
    totalTimeoutMs: Math.max(1, ctx.deadlineAt - Date.now()),
  };
  const observation = await service.observe(request);
  assertObservationMatchesBinding(observation, ctx, binding, candidateSource, policy);
  return observation;
}

function snapshotValidatorNodeModules(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps,
  binding: CandidateBlackboxBinding
): ValidatorModuleSnapshot {
  const target = join(deps.session.privateDir, 'validator-node-modules', ctx.node.id);
  if (!existsSync(target)) snapshotValidatorSourceToTarget(deps, target);
  const snapshot = {
    nodeModules: target,
    playwrightManifest: validatorPackageManifest(join(target, 'playwright')),
    coreManifest: validatorPackageManifest(join(target, 'playwright-core')),
  };
  assertValidatorSnapshotDigest(snapshot, binding);
  return snapshot;
}

function snapshotValidatorSourceToTarget(deps: HardenedControllerActionDeps, target: string): void {
  const nodeModules = configuredPlaywrightNodeModules(deps);
  if (!nodeModules) {
    throw new Error('Candidate black-box validator node modules path is not configured.');
  }
  mkdirSync(target, { recursive: true, mode: PRIVATE_DIR_MODE });
  copyValidatorPackageTree(nodeModules, target, 'playwright');
  copyValidatorPackageTree(nodeModules, target, 'playwright-core');
}

function configuredPlaywrightNodeModules(deps: HardenedControllerActionDeps): string | undefined {
  const configured = deps.playwrightNodeModules;
  return typeof configured === 'function' ? configured() : configured;
}

function copyValidatorPackageTree(
  sourceNodeModules: string,
  targetNodeModules: string,
  name: string
): void {
  const sourceRoot = realpathSync(resolve(sourceNodeModules));
  const source = join(sourceRoot, name);
  assertNoSymlinkPathComponentsFromBase(sourceRoot, source, 'Validator package');
  const sourceStat = lstatSync(source);
  if (!sourceStat.isDirectory() || sourceStat.isSymbolicLink()) {
    throw new Error('Candidate black-box validator package must be a regular directory.');
  }
  assertCuratedValidatorPackage(source, name);
  copyValidatorTree(source, join(targetNodeModules, name));
}

function assertOperatorValidatorSourceMatchesGrants(
  policy: OperatorApprovalPolicy,
  grants: readonly OperatorApprovalGrant[]
): void {
  const blackboxGrants = grants.filter(grant => grant.phase === 'candidate-blackbox-test');
  if (blackboxGrants.length === 0) return;
  if (!policy.validatorSource) {
    throw new Error('Candidate black-box validator source is missing from operator policy.');
  }
  const nodeModulesPath = assertOperatorPrivateValidatorNodeModules(
    policy.validatorSource.nodeModulesPath
  );
  assertValidatorSourceMatchesBlackboxGrants(nodeModulesPath, blackboxGrants);
}

function assertValidatorSourceMatchesBlackboxGrants(
  nodeModulesPath: string,
  grants: readonly ValidatorDigestGrant[]
): void {
  assertCuratedValidatorPackage(join(nodeModulesPath, 'playwright'), 'playwright');
  assertCuratedValidatorPackage(join(nodeModulesPath, 'playwright-core'), 'playwright-core');
  const playwrightManifest = validatorPackageManifest(join(nodeModulesPath, 'playwright'));
  const coreManifest = validatorPackageManifest(join(nodeModulesPath, 'playwright-core'));
  const playwrightDigest = digestStable(playwrightManifest);
  const coreDigest = digestStable(coreManifest);
  for (const grant of grants) {
    if (grant.phase !== 'candidate-blackbox-test') continue;
    const expectedPlaywright =
      grant.validatorPackageDigest ??
      stringInput(requiredActionManifest(grant).input, 'validatorPackageDigest');
    const expectedCore =
      grant.validatorCorePackageDigest ??
      stringInput(requiredActionManifest(grant).input, 'validatorCorePackageDigest');
    if (expectedPlaywright !== playwrightDigest || expectedCore !== coreDigest) {
      throw new Error('Candidate black-box validator source digest does not match policy.');
    }
  }
}

function requiredActionManifest(grant: ValidatorDigestGrant): ControllerActionManifest {
  if (!grant.actionManifest) {
    throw new Error('Candidate black-box validator source policy is malformed.');
  }
  return grant.actionManifest;
}

function assertOperatorPrivateValidatorNodeModules(path: string): string {
  if (path !== path.trim() || !path.startsWith('/')) {
    throw new Error('Candidate black-box validator source path must be absolute.');
  }
  const homeRoot = realpathSync(resolve(getArchonHome()));
  const resolved = assertPrivateChildDirectory(
    homeRoot,
    path,
    'Candidate black-box validator source'
  );
  assertNoSymlinkPathComponents(homeRoot, resolved, 'Candidate black-box validator source');
  return resolved;
}

function assertCuratedValidatorPackage(root: string, expectedName: string): void {
  const packageJson = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')) as unknown;
  if (!packageJson || typeof packageJson !== 'object' || Array.isArray(packageJson)) {
    throw new Error('Candidate black-box validator package metadata is malformed.');
  }
  if ((packageJson as Record<string, unknown>).name !== expectedName) {
    throw new Error('Candidate black-box validator package is not the curated Playwright tree.');
  }
}

function copyValidatorTree(source: string, target: string): void {
  const stat = lstatSync(source);
  if (stat.isSymbolicLink()) throw new Error('Candidate black-box validator package symlink.');
  if (stat.isDirectory()) {
    mkdirSync(target, { recursive: true, mode: 0o755 });
    for (const entry of readdirSync(source, { withFileTypes: true })) {
      copyValidatorTree(join(source, entry.name), join(target, entry.name));
    }
    return;
  }
  if (!stat.isFile() || stat.nlink !== 1) {
    throw new Error('Candidate black-box validator package contains unsupported file type.');
  }
  copyFileSync(source, target);
  chmodSync(target, 0o644);
}

function validatorPackageManifest(root: string): Record<string, unknown>[] {
  const files: Record<string, unknown>[] = [];
  enumerateValidatorPackage(root, root, files);
  return files.sort((left, right) => compareCodepoint(String(left.path), String(right.path)));
}

function enumerateValidatorPackage(
  root: string,
  current: string,
  files: Record<string, unknown>[]
): void {
  if (current !== root)
    assertPathInsideRoot(root, current, 'Candidate black-box validator package');
  const stat = lstatSync(current);
  if (stat.isSymbolicLink()) throw new Error('Candidate black-box validator package symlink.');
  if (stat.isDirectory()) {
    if (stat.uid !== process.getuid?.()) {
      throw new Error('Candidate black-box validator package directory is not owned by this user.');
    }
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      enumerateValidatorPackage(root, join(current, entry.name), files);
    }
    return;
  }
  if (!stat.isFile() || stat.nlink !== 1) {
    throw new Error('Candidate black-box validator package contains unsupported file type.');
  }
  const bytes = readFileSync(current);
  files.push({
    path: relative(root, current)
      .split(/[\\/]+/)
      .join('/'),
    sha256: gitFileDigest(bytes).sha256,
    size: bytes.byteLength,
    type: 'file',
  });
}

function assertValidatorSnapshotDigest(
  snapshot: ValidatorModuleSnapshot,
  binding: CandidateBlackboxBinding
): void {
  if (digestStable(snapshot.playwrightManifest) !== binding.validatorPackageDigest) {
    throw new Error('Candidate black-box validator package digest changed.');
  }
  if (digestStable(snapshot.coreManifest) !== binding.validatorCorePackageDigest) {
    throw new Error('Candidate black-box validator core package digest changed.');
  }
}

function assertObservationMatchesBinding(
  observation: BrowserObservationResult,
  ctx: ControllerActionHandlerContext,
  binding: CandidateBlackboxBinding,
  source: CandidateSourceDescriptor,
  policy: BrowserPolicy
): void {
  if (observation.authority !== 'none') {
    throw new Error('Candidate black-box raw observation authority changed.');
  }
  if (observation.runId !== ctx.workflowRun.id) {
    throw new Error('Candidate black-box observation run changed.');
  }
  if (observation.policyDigest !== digestStable(policy)) {
    throw new Error('Candidate black-box observation policy digest changed.');
  }
  assertObservedImage(binding.staticHelperImage, observation.app.imageId, 'static helper');
  assertObservedImage(binding.verifierImage, observation.verifier.imageId, 'verifier');
  if (observation.app.commit !== source.commit || observation.app.tree !== source.tree) {
    throw new Error('Candidate black-box observation candidate binding changed.');
  }
  if (observation.observedOrigin !== `http://127.0.0.1:${binding.port}`) {
    throw new Error('Candidate black-box observation origin changed.');
  }
  if (observation.viewport.width !== 1280 || observation.viewport.height !== 720) {
    throw new Error('Candidate black-box observation viewport changed.');
  }
  assertObservedCriteria(policy, observation);
  assertBrowserEvidenceReadable(observation);
}

function assertObservedCriteria(
  policy: BrowserPolicy,
  observation: BrowserObservationResult
): void {
  const expected = new Set(policy.required.map(criterion => criterion.id));
  const expectedEvidence = new Set(policy.required.map(criterion => criterion.id));
  for (const criterion of observation.criteria) {
    const policyCriterion = policy.required.find(expected => expected.id === criterion.id);
    if (!policyCriterion || !expected.delete(criterion.id) || criterion.status !== 'passed') {
      throw new Error('Candidate black-box verifier criterion result changed.');
    }
    if (criterion.path !== policyCriterion.path) {
      throw new Error('Candidate black-box verifier criterion path changed.');
    }
    assertObservedAssertions(policyCriterion, criterion);
  }
  if (expected.size > 0) throw new Error('Candidate black-box verifier missed criteria.');
  for (const screenshot of observation.evidence.screenshots) {
    if (screenshot.criterion) expectedEvidence.delete(screenshot.criterion);
  }
  if (expectedEvidence.size > 0) {
    throw new Error('Candidate black-box verifier missed criterion evidence.');
  }
}

function assertObservedAssertions(
  expected: BrowserPolicy['required'][number],
  observed: BrowserObservationResult['criteria'][number]
): void {
  if (
    !Array.isArray(observed.assertions) ||
    observed.assertions.length !== expected.assertions.length
  ) {
    throw new Error('Candidate black-box verifier assertions changed.');
  }
  for (const [index, assertion] of expected.assertions.entries()) {
    const result = observed.assertions[index];
    const resultText = (result as unknown as Record<string, unknown> | undefined)?.text;
    if (
      result?.status !== 'passed' ||
      result.type !== assertion.type ||
      result.value !== assertion.value ||
      resultText !== assertion.text
    ) {
      throw new Error('Candidate black-box verifier assertion result changed.');
    }
  }
}

function assertBrowserEvidenceReadable(observation: BrowserObservationResult): void {
  for (const evidence of [...observation.evidence.screenshots, ...observation.evidence.traces]) {
    if (
      gitFileDigest(readPrivateFile(evidence.controllerPath, 'Candidate black-box evidence'))
        .sha256 !== evidence.sha256
    ) {
      throw new Error('Candidate black-box evidence bytes changed.');
    }
  }
}

function digestBrowserEvidence(observation: BrowserObservationResult): Record<string, unknown> {
  return {
    screenshots: observation.evidence.screenshots.map(evidence => digestEvidenceEntry(evidence)),
    traces: observation.evidence.traces.map(evidence => digestEvidenceEntry(evidence)),
  };
}

function digestEvidenceEntry(evidence: {
  criterion?: string;
  path: string;
  controllerPath: string;
  sha256: string;
}): Record<string, unknown> {
  const digest = gitFileDigest(
    readPrivateFile(evidence.controllerPath, 'Candidate black-box evidence')
  );
  if (digest.sha256 !== evidence.sha256) {
    throw new Error('Candidate black-box evidence bytes changed.');
  }
  return {
    ...(evidence.criterion ? { criterion: evidence.criterion } : {}),
    path: evidence.path,
    controllerPath: evidence.controllerPath,
    sha256: evidence.sha256,
    size: digest.size,
  };
}

async function assertCandidateBlackboxReceiptBound(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps,
  binding: CandidateBlackboxBinding
): Promise<void> {
  if (receipt.schema !== CANDIDATE_BLACKBOX_RECEIPT_SCHEMA) {
    throw new Error('Candidate black-box receipt schema is invalid.');
  }
  assertCandidateBlackboxReceiptContext(receipt, ctx, deps.session);
  const dependencies = await readBlackboxDependencies(ctx, deps, binding);
  const policy = readFrozenBrowserPolicy(dependencies.freeze, binding.acceptancePolicyPath);
  const candidateSource = candidateSourceFromImport(dependencies.importReceipt, binding);
  if (stringInput(receipt, 'rawObservationStatus') !== 'passed') {
    throw new Error('Candidate black-box receipt is not passing.');
  }
  assertBlackboxReceiptDependencies(receipt, dependencies, binding);
  assertBlackboxReceiptPolicy(receipt, policy, candidateSource);
  assertBlackboxValidatorSnapshot(receipt, binding);
  assertBlackboxEvidenceUnchanged(receipt);
}

function assertCandidateBlackboxReceiptContext(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  session: HardenedControllerSession
): void {
  if (stringInput(receipt, 'runId') !== ctx.workflowRun.id) {
    throw new Error('Candidate black-box receipt run changed.');
  }
  if (stringInput(receipt, 'workflowDigest') !== ctx.workflowDigest) {
    throw new Error('Candidate black-box receipt workflow changed.');
  }
  if (stringInput(receipt, 'controllerActionNodeId') !== ctx.node.id) {
    throw new Error('Candidate black-box receipt node changed.');
  }
  if (stringInput(receipt, 'actionManifestDigest') !== ctx.actionManifest.digest) {
    throw new Error('Candidate black-box receipt action manifest changed.');
  }
  assertApprovalSourceBinding(receipt, session);
}

function assertBlackboxReceiptDependencies(
  receipt: Record<string, unknown>,
  dependencies: CandidateBlackboxDependencies,
  binding: CandidateBlackboxBinding
): void {
  if (stringInput(receipt, 'candidateImportReceiptDigest') !== dependencies.importReceiptDigest) {
    throw new Error('Candidate black-box import receipt digest changed.');
  }
  if (stringInput(receipt, 'freezeReceiptDigest') !== dependencies.freezeReceiptDigest) {
    throw new Error('Candidate black-box freeze receipt digest changed.');
  }
  if (stringInput(receipt, 'approvalReceiptDigest') !== dependencies.approvalReceiptDigest) {
    throw new Error('Candidate black-box approval receipt digest changed.');
  }
  if (stringInput(receipt, 'repositoryTarget') !== binding.repositoryTarget) {
    throw new Error('Candidate black-box repository target changed.');
  }
  const startupEntrypoint = optionalStringInput(receipt, 'startupEntrypoint');
  if (startupEntrypoint !== binding.startupEntrypoint) {
    throw new Error('Candidate black-box startup entrypoint changed.');
  }
  if (stringInput(receipt, 'staticHelperImage') !== binding.staticHelperImage) {
    throw new Error('Candidate black-box static helper image changed.');
  }
  if (stringInput(receipt, 'verifierImage') !== binding.verifierImage) {
    throw new Error('Candidate black-box verifier image changed.');
  }
}

function assertBlackboxReceiptPolicy(
  receipt: Record<string, unknown>,
  policy: BrowserPolicy,
  source: CandidateSourceDescriptor
): void {
  if (stringInput(receipt, 'acceptancePolicyDigest') !== digestStable(policy)) {
    throw new Error('Candidate black-box acceptance policy digest changed.');
  }
  if (stringInput(receipt, 'candidateCommit') !== source.commit) {
    throw new Error('Candidate black-box candidate commit changed.');
  }
  if (stringInput(receipt, 'candidateTreeOid') !== source.tree) {
    throw new Error('Candidate black-box candidate tree changed.');
  }
  if (stringInput(receipt, 'candidateSourceDigest') !== source.contentDigest) {
    throw new Error('Candidate black-box candidate source digest changed.');
  }
}

function assertBlackboxValidatorSnapshot(
  receipt: Record<string, unknown>,
  binding: CandidateBlackboxBinding
): void {
  const snapshot = {
    nodeModules: stringInput(receipt, 'validatorNodeModulesPath'),
    playwrightManifest: validatorPackageManifest(
      join(stringInput(receipt, 'validatorNodeModulesPath'), 'playwright')
    ),
    coreManifest: validatorPackageManifest(
      join(stringInput(receipt, 'validatorNodeModulesPath'), 'playwright-core')
    ),
  };
  assertValidatorSnapshotDigest(snapshot, binding);
  if (
    digestStable(readManifestRecords(receipt.validatorPackageManifest)) !==
    binding.validatorPackageDigest
  ) {
    throw new Error('Candidate black-box validator package digest changed.');
  }
  if (
    digestStable(readManifestRecords(receipt.validatorCorePackageManifest)) !==
    binding.validatorCorePackageDigest
  ) {
    throw new Error('Candidate black-box validator core package digest changed.');
  }
}

function readManifestRecords(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) {
    throw new Error('Candidate black-box validator package manifest is malformed.');
  }
  return value.map(entry => {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error('Candidate black-box validator package manifest is malformed.');
    }
    return entry as Record<string, unknown>;
  });
}

function assertBlackboxEvidenceUnchanged(receipt: Record<string, unknown>): void {
  const evidence = receipt.evidence;
  if (!evidence || typeof evidence !== 'object' || Array.isArray(evidence)) {
    throw new Error('Candidate black-box evidence manifest is malformed.');
  }
  for (const entry of [
    ...evidenceRecords((evidence as Record<string, unknown>).screenshots),
    ...evidenceRecords((evidence as Record<string, unknown>).traces),
  ]) {
    if (
      gitFileDigest(readPrivateFile(entry.controllerPath, 'Candidate black-box evidence'))
        .sha256 !== entry.sha256
    ) {
      throw new Error('Candidate black-box evidence bytes changed.');
    }
  }
}

function evidenceRecords(value: unknown): { controllerPath: string; sha256: string }[] {
  if (!Array.isArray(value)) throw new Error('Candidate black-box evidence manifest is malformed.');
  return value.map(entry => {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error('Candidate black-box evidence manifest is malformed.');
    }
    const record = entry as Record<string, unknown>;
    return {
      controllerPath: stringInput(record, 'controllerPath'),
      sha256: assertSha256Hex(stringInput(record, 'sha256'), 'Candidate black-box evidence digest'),
    };
  });
}

function sealedReceiptDigest(path: string): string {
  return gitFileDigest(readPrivateFile(path, 'Hardened controller receipt')).sha256;
}

function assertImmutableImageRef(value: string, label: string): string {
  if (/^sha256:[0-9a-f]{64}$/i.test(value) || /@sha256:[0-9a-f]{64}$/i.test(value)) {
    return value;
  }
  throw new Error(`${label} must be pinned by immutable sha256 digest.`);
}

function assertObservedImage(expected: string, actualImageId: string, label: string): void {
  const digest = expected.startsWith('sha256:')
    ? expected
    : `sha256:${expected.split('@sha256:')[1]}`;
  if (actualImageId.toLowerCase() !== digest.toLowerCase()) {
    throw new Error(`Candidate black-box ${label} image id changed.`);
  }
}

function assertSha256Hex(value: string, label: string): string {
  if (!/^[0-9a-f]{64}$/i.test(value)) throw new Error(`${label} is malformed.`);
  return value.toLowerCase();
}

function candidateSourceRelativePath(value: string, field: string, allowDot = false): string {
  if (allowDot && value === '.') return value;
  if (
    value !== value.trim() ||
    value === '' ||
    value === '.' ||
    value.startsWith('/') ||
    value.includes('\\') ||
    value.split('/').includes('..') ||
    value.includes('//')
  ) {
    throw new Error(`Candidate black-box ${field} must be a safe relative path.`);
  }
  return value;
}

function fixedSnapshotDir(privateDir: string, manifestId: string): string {
  const safeId = assertSafeRelativePath(manifestId, 'controller action manifest id').replaceAll(
    ':',
    '_'
  );
  return join(privateDir, 'snapshots', safeId);
}

function freezeGrantFor(
  session: HardenedControllerSession,
  freezeNodeId: string
): ControllerActionGrant {
  const grant = session.controllerActionGrants.find(
    candidate =>
      candidate.nodeId === freezeNodeId &&
      candidate.action === 'finalize-evidence' &&
      candidate.phase === 'planning-freeze'
  );
  if (!grant) throw new Error('Referenced freeze grant is not authorized.');
  return grant;
}

function assertApprovalReceiptBound(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  session: HardenedControllerSession,
  approvalNodeId: string,
  freezeNodeId: string,
  freeze: Record<string, unknown>,
  approval: PlanningApprovalEvidence
): void {
  if (receipt.schema !== APPROVAL_RECEIPT_SCHEMA) {
    throw new Error('Approval receipt schema is invalid.');
  }
  if (stringInput(receipt, 'runId') !== ctx.workflowRun.id)
    throw new Error('Approval receipt run changed.');
  if (stringInput(receipt, 'workflowDigest') !== ctx.workflowDigest) {
    throw new Error('Approval receipt workflow digest changed.');
  }
  if (stringInput(receipt, 'controllerActionNodeId') !== ctx.node.id) {
    throw new Error('Approval receipt node changed.');
  }
  if (stringInput(receipt, 'approvalNodeId') !== approvalNodeId) {
    throw new Error('Approval receipt approval node changed.');
  }
  if (stringInput(receipt, 'freezeNodeId') !== freezeNodeId) {
    throw new Error('Approval receipt freeze node changed.');
  }
  if (stringInput(receipt, 'bindingId') !== stringInput(freeze, 'bindingId')) {
    throw new Error('Approval receipt binding changed.');
  }
  if (stringInput(receipt, 'oracleDigest') !== stringInput(freeze, 'oracleDigest')) {
    throw new Error('Approval receipt oracle changed.');
  }
  assertApprovalReceiptEventOrders(receipt, approval);
  assertApprovalSourceBinding(receipt, session);
}

interface PlanningApprovalEvidence {
  decision: 'approved';
  requestMessage: string;
  freezeCompletedOrder: number;
  requestedOrder: number;
  completedOrder: number;
  receivedOrder: number;
}

function assertApprovalReceiptEventOrders(
  receipt: Record<string, unknown>,
  approval: PlanningApprovalEvidence
): void {
  if (
    numberInput(receipt, 'freezeCompletedOrder') !== approval.freezeCompletedOrder ||
    numberInput(receipt, 'approvalRequestedOrder') !== approval.requestedOrder ||
    numberInput(receipt, 'approvalCompletedOrder') !== approval.completedOrder ||
    numberInput(receipt, 'approvalReceivedOrder') !== approval.receivedOrder
  ) {
    throw new Error('Approval receipt no longer matches current approval events.');
  }
}

function assertApprovalSourceBinding(
  receipt: Record<string, unknown>,
  session: HardenedControllerSession
): void {
  if (stringInput(receipt, 'image') !== session.policyMetadata.image) {
    throw new Error('Approval receipt image changed.');
  }
  if (stringInput(receipt, 'requestedImage') !== session.policyMetadata.requestedImage) {
    throw new Error('Approval receipt image changed.');
  }
  if (stringInput(receipt, 'seedManifestDigest') !== session.policyMetadata.seedManifestDigest) {
    throw new Error('Approval receipt source changed.');
  }
  if (stringInput(receipt, 'sourceBindingDigest') !== sourceBindingDigest(session.policyMetadata)) {
    throw new Error('Approval receipt source changed.');
  }
}

function assertFreezeReceiptMatchesGrant(
  receipt: Record<string, unknown>,
  grant: ControllerActionGrant,
  session: HardenedControllerSession
): void {
  if (receipt.schema !== FREEZE_RECEIPT_SCHEMA)
    throw new Error('Freeze receipt schema is invalid.');
  if (stringInput(receipt, 'runId') !== session.runId)
    throw new Error('Freeze receipt run changed.');
  if (stringInput(receipt, 'workflowDigest') !== session.workflowDigest) {
    throw new Error('Freeze receipt workflow digest changed.');
  }
  if (stringInput(receipt, 'controllerActionNodeId') !== grant.nodeId) {
    throw new Error('Freeze receipt node changed.');
  }
  if (stringInput(receipt, 'actionManifestId') !== grant.actionManifest.id) {
    throw new Error('Freeze receipt action manifest changed.');
  }
  if (stringInput(receipt, 'actionManifestDigest') !== grant.actionManifest.digest) {
    throw new Error('Freeze receipt action manifest digest changed.');
  }
  stringInput(receipt, 'frozenAt');
  stringInput(receipt, 'isolationEnvId');
  assertFreezeSourceBinding(receipt, session);
}

function assertFreezeSourceBinding(
  receipt: Record<string, unknown>,
  session: HardenedControllerSession
): void {
  if (stringInput(receipt, 'policyDigest') !== session.policyMetadata.approvalPolicyDigest) {
    throw new Error('Freeze receipt policy changed.');
  }
  if (stringInput(receipt, 'image') !== session.policyMetadata.image) {
    throw new Error('Freeze receipt image changed.');
  }
  if (stringInput(receipt, 'requestedImage') !== session.policyMetadata.requestedImage) {
    throw new Error('Freeze receipt image changed.');
  }
  if (stringInput(receipt, 'seedManifestDigest') !== session.policyMetadata.seedManifestDigest) {
    throw new Error('Freeze receipt source changed.');
  }
  if (stringInput(receipt, 'sourceBindingDigest') !== sourceBindingDigest(session.policyMetadata)) {
    throw new Error('Freeze receipt source changed.');
  }
}

function assertFreezeReceiptBound(
  receipt: Record<string, unknown>,
  ctx: ControllerActionHandlerContext,
  input: Record<string, unknown>
): void {
  if (receipt.schema !== FREEZE_RECEIPT_SCHEMA)
    throw new Error('Freeze receipt schema is invalid.');
  if (stringInput(receipt, 'runId') !== ctx.workflowRun.id)
    throw new Error('Freeze receipt run changed.');
  if (stringInput(receipt, 'workflowDigest') !== ctx.workflowDigest) {
    throw new Error('Freeze receipt workflow digest changed.');
  }
  if (stringInput(receipt, 'controllerActionNodeId') !== ctx.node.id) {
    throw new Error('Freeze receipt node changed.');
  }
  if (stringInput(receipt, 'actionManifestId') !== ctx.actionManifest.id) {
    throw new Error('Freeze receipt action manifest changed.');
  }
  if (stringInput(receipt, 'actionManifestDigest') !== ctx.actionManifest.digest) {
    throw new Error('Freeze receipt action manifest digest changed.');
  }
  if (stringInput(receipt, 'policyDigest') !== stringInput(input, 'policyDigest')) {
    throw new Error('Freeze receipt policy changed.');
  }
  stringInput(receipt, 'frozenAt');
  if (stringInput(receipt, 'isolationEnvId') !== readIsolationEnvId(ctx.workflowRun.metadata)) {
    throw new Error('Freeze receipt isolation environment changed.');
  }
  if (stringInput(receipt, 'image') !== stringInput(input, 'image')) {
    throw new Error('Freeze receipt image changed.');
  }
  if (stringInput(receipt, 'requestedImage') !== stringInput(input, 'requestedImage')) {
    throw new Error('Freeze receipt image changed.');
  }
  if (stringInput(receipt, 'seedManifestDigest') !== stringInput(input, 'seedManifestDigest')) {
    throw new Error('Freeze receipt source changed.');
  }
  if (stringInput(receipt, 'sourceBindingDigest') !== stringInput(input, 'sourceBindingDigest')) {
    throw new Error('Freeze receipt source changed.');
  }
}

function assertFreezeSnapshotUnchanged(
  receipt: Record<string, unknown>,
  oracleFiles: string[]
): void {
  const snapshotDir = stringInput(receipt, 'snapshotDir');
  const current = readOracleFiles(snapshotDir, oracleFiles).files;
  if (digestStable(current) !== stringInput(receipt, 'oracleDigest')) {
    throw new Error('Freeze receipt snapshot bytes changed.');
  }
}

async function assertControllerLive(
  ctx: ControllerActionHandlerContext,
  deps: HardenedControllerActionDeps
): Promise<void> {
  assertNotCancelled(ctx.signal);
  if (Date.now() > ctx.deadlineAt) throw new Error('Controller action exceeded deadline.');
  const status = await deps.store.getWorkflowRunStatus(ctx.workflowRun.id);
  assertNotCancelled(ctx.signal);
  if (Date.now() > ctx.deadlineAt) throw new Error('Controller action exceeded deadline.');
  if (status !== 'running') {
    throw new Error(`Controller action refused because workflow run is ${status ?? 'missing'}.`);
  }
}

function assertNotCancelled(signal: AbortSignal): void {
  if (signal.aborted) throw new Error('Controller action cancelled.');
}

function readIsolationEnvId(metadata: unknown): string {
  if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata))
    throw new Error('Workflow run metadata is missing.');
  const envId = (metadata as Record<string, unknown>).isolation_env_id;
  if (typeof envId !== 'string' || envId.length === 0)
    throw new Error('Workflow run isolation env id is missing.');
  return envId;
}

function readOracleFiles(
  snapshotDir: string,
  oracleFiles: string[]
): { files: Record<string, unknown>[] } {
  return {
    files: oracleFiles.map(file => {
      const path = safeDestination(snapshotDir, file);
      assertNoSymlinkPathComponents(snapshotDir, path, file);
      const stat = lstatSync(path);
      if (!stat.isFile() || stat.isSymbolicLink())
        throw new Error(`Planning oracle file '${file}' is not regular.`);
      if (stat.size > MAX_ORACLE_FILE_BYTES)
        throw new Error(`Planning oracle file '${file}' exceeds the size limit.`);
      const bytes = readFileSync(path);
      return { path: file, sha256: gitFileDigest(bytes).sha256, size: bytes.byteLength };
    }),
  };
}

async function readHumanApproval(
  store: IWorkflowStore,
  runId: string,
  freezeNodeId: string,
  approvalNodeId: string,
  bindingId: string,
  oracleDigest: string
): Promise<PlanningApprovalEvidence> {
  const run = await store.getWorkflowRun(runId);
  const approval = readApprovalMetadata(run?.metadata, approvalNodeId);
  if (approval.resolved !== 'approved') throw new Error('Planning approval is not approved.');
  const events = indexWorkflowEvents(await readWorkflowEvents(store, runId));
  const freezeCompleted = latestMatchingFreezeCompletion(
    events,
    freezeNodeId,
    bindingId,
    oracleDigest
  );
  const requested = latestEvent(events, 'approval_requested', approvalNodeId);
  const received = latestEvent(events, 'approval_received', approvalNodeId);
  const completed = latestEvent(events, 'node_completed', approvalNodeId);
  if (!freezeCompleted || !requested || !received || !completed) {
    throw new Error('Planning approval audit events are incomplete.');
  }
  const requestMessage = stringEventData(requested.event, 'message');
  assertApprovalMessageBound(requestMessage, bindingId, oracleDigest);
  assertApprovalEventOrder(freezeCompleted, requested, completed, received);
  if (
    eventDecision(received.event) !== 'approved' ||
    eventDecision(completed.event) !== 'approved'
  ) {
    throw new Error('Latest planning approval decision is not approved.');
  }
  return {
    decision: 'approved',
    requestMessage,
    freezeCompletedOrder: freezeCompleted.order,
    requestedOrder: requested.order,
    completedOrder: completed.order,
    receivedOrder: received.order,
  };
}

function readApprovalMetadata(metadata: unknown, approvalNodeId: string): Record<string, unknown> {
  if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata))
    throw new Error('Approval metadata is missing.');
  const approval = (metadata as Record<string, unknown>).approval;
  if (!approval || typeof approval !== 'object' || Array.isArray(approval))
    throw new Error('Approval metadata is missing.');
  const record = approval as Record<string, unknown>;
  if (record.type !== 'approval' || record.nodeId !== approvalNodeId)
    throw new Error('Approval metadata does not match the planning gate.');
  return record;
}

async function readWorkflowEvents(
  store: IWorkflowStore,
  runId: string
): Promise<WorkflowEventRecord[]> {
  if (!store.listWorkflowEvents)
    throw new Error('Workflow store cannot read approval audit events.');
  return store.listWorkflowEvents(runId);
}

interface OrderedWorkflowEvent {
  event: WorkflowEventRecord;
  order: number;
}

function indexWorkflowEvents(events: WorkflowEventRecord[]): OrderedWorkflowEvent[] {
  const seen = new Set<number>();
  return events.map(event => {
    const order = event.event_order;
    if (typeof order !== 'number' || !Number.isSafeInteger(order) || order < 0 || seen.has(order)) {
      throw new Error(
        'Planning approval database event order is missing, malformed, or duplicated.'
      );
    }
    seen.add(order);
    return { event, order };
  });
}

function latestEvent(
  events: OrderedWorkflowEvent[],
  type: string,
  stepName: string
): OrderedWorkflowEvent | undefined {
  return events
    .filter(item => item.event.event_type === type && item.event.step_name === stepName)
    .sort((a, b) => b.order - a.order)[0];
}

function latestMatchingFreezeCompletion(
  events: OrderedWorkflowEvent[],
  freezeNodeId: string,
  bindingId: string,
  oracleDigest: string
): OrderedWorkflowEvent | undefined {
  return events
    .filter(
      item => item.event.event_type === 'node_completed' && item.event.step_name === freezeNodeId
    )
    .filter(item => freezeCompletionMatches(item.event, bindingId, oracleDigest))
    .sort((a, b) => b.order - a.order)[0];
}

function freezeCompletionMatches(
  event: WorkflowEventRecord,
  bindingId: string,
  oracleDigest: string
): boolean {
  const output = eventOutputObject(event);
  return output?.binding_id === bindingId && output?.oracle_digest === oracleDigest;
}

function eventOutputObject(event: WorkflowEventRecord): Record<string, unknown> | undefined {
  const data = eventData(event);
  const raw = data.node_output ?? data.output;
  if (typeof raw === 'string') return parseObjectJson(raw);
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) return raw as Record<string, unknown>;
  return undefined;
}

function parseObjectJson(raw: string): Record<string, unknown> | undefined {
  try {
    const value = JSON.parse(raw) as unknown;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return value as Record<string, unknown>;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function assertApprovalEventOrder(
  freezeCompleted: OrderedWorkflowEvent,
  requested: OrderedWorkflowEvent,
  completed: OrderedWorkflowEvent,
  received: OrderedWorkflowEvent
): void {
  if (
    requested.order <= freezeCompleted.order ||
    completed.order <= requested.order ||
    received.order <= completed.order
  ) {
    throw new Error('Planning approval events are stale or out of order.');
  }
}

function eventDecision(event: WorkflowEventRecord): string | undefined {
  const data = eventData(event);
  return typeof data.approval_decision === 'string'
    ? data.approval_decision
    : typeof data.decision === 'string'
      ? data.decision
      : undefined;
}

function stringEventData(event: WorkflowEventRecord, key: string): string {
  const value = eventData(event)[key];
  if (typeof value !== 'string') throw new Error(`Approval event ${key} is missing.`);
  return value;
}

function eventData(event: WorkflowEventRecord): Record<string, unknown> {
  if (typeof event.data === 'string') return JSON.parse(event.data) as Record<string, unknown>;
  if (!event.data || typeof event.data !== 'object') return {};
  return event.data;
}

function assertApprovalMessageBound(
  message: string,
  bindingId: string,
  oracleDigest: string
): void {
  if (!message.includes(bindingId) || !message.includes(oracleDigest)) {
    throw new Error('Planning approval message did not bind the frozen oracle digest.');
  }
}

function receiptPathFor(privateDir: string, nodeId: string, kind: string): string {
  const dir = join(privateDir, 'controller-receipts');
  mkdirSync(dir, { recursive: true, mode: PRIVATE_DIR_MODE });
  return join(
    dir,
    `${assertSafeRelativePath(nodeId, 'receipt node').replaceAll('/', '_')}-${kind}.json`
  );
}

function sealReceipt(
  receipt: Record<string, unknown>,
  session: HardenedControllerSession
): Record<string, unknown> {
  const hmacKeyPath = session.policyMetadata.hmacKeyPath;
  if (!hmacKeyPath) throw new Error('Hardened controller HMAC key is missing.');
  assertPrivateSessionPathLayout(session.policyMetadata);
  const hmac = createHmac('sha256', readPrivateHmacKey(hmacKeyPath))
    .update(digestStable(receipt))
    .digest('hex');
  return { ...receipt, hmac };
}

function readSealedReceipt(
  path: string,
  session: HardenedControllerSession
): Record<string, unknown> {
  const receipt = JSON.parse(
    readPrivateFile(path, 'Hardened controller receipt').toString('utf8')
  ) as Record<string, unknown>;
  const hmac = receipt.hmac;
  const unsigned = { ...receipt };
  delete unsigned.hmac;
  if (
    typeof hmac !== 'string' ||
    !equalHexHmac(String(sealReceipt(unsigned, session).hmac), hmac)
  ) {
    throw new Error('Hardened controller receipt HMAC is invalid.');
  }
  return unsigned;
}

function equalHexHmac(expected: string, actual: string): boolean {
  if (!/^[0-9a-f]{64}$/i.test(expected) || !/^[0-9a-f]{64}$/i.test(actual)) return false;
  return timingSafeEqual(Buffer.from(expected, 'hex'), Buffer.from(actual, 'hex'));
}

function readPrivateFile(path: string, label: string): Buffer {
  assertPrivateFilePath(path, label);
  return readFileSync(path);
}

function readPrivateHmacKey(path: string): Buffer {
  const key = readPrivateFile(path, 'Hardened controller HMAC key');
  if (key.byteLength !== 32) throw new Error('Hardened controller HMAC key is malformed.');
  return key;
}

function writeSessionBinding(metadata: HardenedControllerPolicyMetadata): void {
  const path = metadata.sessionBindingPath;
  if (!path) throw new Error('Hardened controller session binding path is missing.');
  const body = { policyDigest: digestStable(metadata) };
  writeJson(path, { ...body, hmac: hmacForBody(body, metadata) });
}

function verifyPrivateSessionBinding(metadata: HardenedControllerPolicyMetadata): void {
  assertPrivateSessionPathLayout(metadata);
  if (!metadata.sessionBindingPath) {
    throw new Error('Cannot resume hardened run: private session binding is missing.');
  }
  const binding = JSON.parse(
    readPrivateFile(metadata.sessionBindingPath, 'Hardened controller session binding').toString(
      'utf8'
    )
  ) as Record<string, unknown>;
  const expected = { policyDigest: digestStable(metadata) };
  if (binding.policyDigest !== expected.policyDigest || typeof binding.hmac !== 'string') {
    throw new Error('Cannot resume hardened run: private session binding changed.');
  }
  if (!equalHexHmac(hmacForBody(expected, metadata), binding.hmac)) {
    throw new Error('Cannot resume hardened run: private session binding changed.');
  }
}

function hmacForBody(
  body: Record<string, unknown>,
  metadata: HardenedControllerPolicyMetadata
): string {
  if (!metadata.hmacKeyPath) throw new Error('Hardened controller HMAC key is missing.');
  assertPrivateSessionPathLayout(metadata);
  return createHmac('sha256', readPrivateHmacKey(metadata.hmacKeyPath))
    .update(digestStable(body))
    .digest('hex');
}

function stringInput(input: Record<string, unknown>, key: string): string {
  const value = input[key];
  if (typeof value !== 'string' || value.trim() !== value || value.length === 0) {
    throw new Error(`Controller policy field '${key}' is missing or malformed.`);
  }
  return value;
}

function optionalStringInput(input: Record<string, unknown>, key: string): string | undefined {
  if (!(key in input)) return undefined;
  return stringInput(input, key);
}

function numberInput(input: Record<string, unknown>, key: string): number {
  const value = input[key];
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`Controller policy field '${key}' is missing or malformed.`);
  }
  return value;
}

function assertSafeRelativePath(path: string, label: string): string {
  const normalized = path.trim().replace(/\\/g, '/').replace(/^\.\//, '');
  if (!normalized || normalized.startsWith('/') || normalized.split('/').includes('..')) {
    throw new Error(`${label} '${path}' must be a safe relative path.`);
  }
  if (isForbiddenSeedPath(normalized)) throw new Error(`${label} '${path}' is forbidden.`);
  return normalized;
}

function isSealedControllerGrantRecord(value: unknown): boolean {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  if (!isSupportedPlanningAction(record.action)) return false;
  if (typeof record.runId !== 'string' || typeof record.workflowName !== 'string') return false;
  if (typeof record.workflowDigest !== 'string' || typeof record.nodeId !== 'string') return false;
  if (typeof record.phase !== 'string') return false;
  const manifest = record.actionManifest;
  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) return false;
  return isControllerActionManifestSealed(manifest as ControllerActionManifest);
}

function isSupportedPlanningAction(
  value: unknown
): value is 'finalize-evidence' | 'verify-approval' {
  return value === 'finalize-evidence' || value === 'verify-approval';
}

function buildBudgetGrant(
  input: HardenedControllerSessionInput,
  workflowDigest: string
): WorkflowBudgetGrant {
  const total = input.budget?.tokens?.total ?? DEFAULT_TOTAL_TOKENS;
  assertPositiveSafeTokenCount(total, 'total token limit');
  if (input.budget?.authoritativeConsumed !== undefined) {
    rejectUnsupportedAuthoritativeConsumption();
  }
  const deadlineAt =
    input.budget?.deadlineAt ?? new Date(Date.now() + DEFAULT_DEADLINE_MS).toISOString();
  const deadlineAtMs = Date.parse(deadlineAt);
  if (!Number.isFinite(deadlineAtMs)) {
    throw new Error('Hardened controller budget deadline is malformed.');
  }
  if (deadlineAtMs <= Date.now()) {
    throw new Error('Hardened controller budget deadline has already expired.');
  }
  const tokens = input.budget?.tokens;
  if (tokens?.input !== undefined) assertPositiveSafeTokenCount(tokens.input, 'input token limit');
  if (tokens?.output !== undefined) {
    assertPositiveSafeTokenCount(tokens.output, 'output token limit');
  }
  return {
    runId: input.runId,
    workflowName: input.workflow.name,
    workflowDigest,
    deadlineAt,
    tokens: {
      total,
      ...(tokens?.input !== undefined ? { input: tokens.input } : {}),
      ...(tokens?.output !== undefined ? { output: tokens.output } : {}),
    },
  };
}

function freezeBudgetGrant(grant: WorkflowBudgetGrant): WorkflowBudgetGrant {
  return Object.freeze({
    ...grant,
    tokens: Object.freeze({ ...grant.tokens }),
  });
}

function buildProxyBudgetSeed(
  runId: string,
  workflowDigest: string,
  image: string,
  egressPolicyB64: string | undefined,
  budgetGrant: WorkflowBudgetGrant,
  providerBudget: OperatorProviderBudgetPolicy
): HardenedProxyBudgetSeed {
  if (!egressPolicyB64) {
    throw new Error('Hardened provider budget seed requires egress authority.');
  }
  const deadlineEpochMs = Date.parse(budgetGrant.deadlineAt);
  if (!Number.isSafeInteger(deadlineEpochMs) || deadlineEpochMs <= Date.now()) {
    throw new Error('Hardened provider budget deadline is malformed or expired.');
  }
  const providerPolicies = providerBudget.policies.map(freezeTrustedProviderPolicy);
  const inputTokenLimit = budgetGrant.tokens.input ?? budgetGrant.tokens.total;
  const outputTokenLimit = budgetGrant.tokens.output ?? budgetGrant.tokens.total;
  for (const policy of providerPolicies) {
    if (policy.maxInputTokens > inputTokenLimit || policy.maxOutputTokens > outputTokenLimit) {
      throw new Error('Trusted provider budget policy exceeds workflow budget limits.');
    }
  }
  const policyDigest = digestStable({ egressPolicyB64, image, providerPolicies });
  const grant: ProxyBudgetGrant = Object.freeze({
    schema: 'archon.proxy-budget-grant.v1',
    rootChainId: runId,
    runId,
    workflowDigest,
    policyDigest,
    deadlineEpochMs,
    inputTokenLimit,
    outputTokenLimit,
    totalTokenLimit: budgetGrant.tokens.total,
  });
  return freezeProxyBudgetSeed({ grant, providerPolicies });
}

function freezeProxyBudgetSeed(seed: HardenedProxyBudgetSeed): HardenedProxyBudgetSeed {
  return Object.freeze({
    grant: Object.freeze({ ...seed.grant }),
    providerPolicies: Object.freeze(seed.providerPolicies.map(freezeTrustedProviderPolicy)),
  });
}

function freezeTrustedProviderPolicy(
  policy: TrustedProviderBudgetPolicy
): TrustedProviderBudgetPolicy {
  return Object.freeze({
    ...policy,
    ...(policy.allowedHeaders
      ? { allowedHeaders: freezeTrustedProviderAllowedHeaders(policy.allowedHeaders) }
      : {}),
  });
}

function freezeTrustedProviderAllowedHeaders(
  headers: NonNullable<TrustedProviderBudgetPolicy['allowedHeaders']>
): NonNullable<TrustedProviderBudgetPolicy['allowedHeaders']> {
  const frozen: NonNullable<TrustedProviderBudgetPolicy['allowedHeaders']> = {};
  for (const [name, value] of Object.entries(headers)) {
    frozen[name] = Array.isArray(value) ? Object.freeze([...value]) : value;
  }
  return Object.freeze(frozen);
}

function freezePolicyMetadata(
  metadata: HardenedControllerPolicyMetadata
): HardenedControllerPolicyMetadata {
  return Object.freeze({
    ...metadata,
    budgetGrant: freezeBudgetGrant(metadata.budgetGrant),
    repoInputs: Object.freeze([...metadata.repoInputs]),
    controllerActionGrants: Object.freeze([...metadata.controllerActionGrants]),
  });
}

function assertPositiveSafeTokenCount(value: unknown, label: string): asserts value is number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Hardened controller budget ${label} must be a positive safe integer.`);
  }
}

function assertBudgetGrantRecord(value: unknown): WorkflowBudgetGrant {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Cannot resume hardened run: persisted budget grant is missing.');
  }
  const record = value as Record<string, unknown>;
  assertExactKeys(
    record,
    ['authoritativeConsumed', 'deadlineAt', 'runId', 'tokens', 'workflowDigest', 'workflowName'],
    'persisted budget grant'
  );
  if (
    typeof record.runId !== 'string' ||
    typeof record.workflowName !== 'string' ||
    typeof record.workflowDigest !== 'string' ||
    typeof record.deadlineAt !== 'string' ||
    !Number.isFinite(Date.parse(record.deadlineAt))
  ) {
    throw new Error('Cannot resume hardened run: persisted budget grant is malformed.');
  }
  const tokens = assertBudgetTokenRecord(record.tokens, 'persisted budget grant');
  if (record.authoritativeConsumed !== undefined) {
    rejectUnsupportedAuthoritativeConsumption();
  }
  return {
    runId: record.runId,
    workflowName: record.workflowName,
    workflowDigest: record.workflowDigest,
    deadlineAt: record.deadlineAt,
    tokens,
  };
}

function assertBudgetTokenRecord(value: unknown, label: string): WorkflowTokenBudget {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`Cannot resume hardened run: ${label} tokens are missing.`);
  }
  const tokens = value as Record<string, unknown>;
  assertExactKeys(tokens, ['input', 'output', 'total'], `${label} tokens`);
  assertPositiveSafeTokenCount(tokens.total, 'total token limit');
  if (tokens.input !== undefined) {
    assertPositiveSafeTokenCount(tokens.input, 'input token limit');
  }
  if (tokens.output !== undefined) {
    assertPositiveSafeTokenCount(tokens.output, 'output token limit');
  }
  return {
    total: tokens.total,
    ...(tokens.input !== undefined ? { input: tokens.input } : {}),
    ...(tokens.output !== undefined ? { output: tokens.output } : {}),
  };
}

function rejectUnsupportedAuthoritativeConsumption(): never {
  throw new Error(
    'Hardened controller budget authoritative consumption is not supported without a durable usage ledger.'
  );
}

function assertResumeBudgetGrant(
  budget: NonNullable<HardenedControllerSessionInput['budget']>,
  metadata: HardenedControllerPolicyMetadata
): WorkflowBudgetGrant {
  const grant = assertBudgetGrantRecord(metadata.budgetGrant);
  if (
    grant.runId !== metadata.runId ||
    grant.workflowName !== metadata.workflowName ||
    grant.workflowDigest !== metadata.workflowDigest
  ) {
    throw new Error('Cannot resume hardened run: persisted budget grant binding is malformed.');
  }
  if (Date.parse(grant.deadlineAt) <= Date.now()) {
    throw new Error('Cannot resume hardened run: persisted budget grant deadline has expired.');
  }
  const supplied = assertSuppliedResumeBudget(budget);
  if (
    supplied.deadlineAt !== grant.deadlineAt ||
    supplied.tokens.total !== grant.tokens.total ||
    supplied.tokens.input !== grant.tokens.input ||
    supplied.tokens.output !== grant.tokens.output
  ) {
    throw new Error('Cannot resume hardened run: budget grant does not match persisted policy.');
  }
  return grant;
}

function assertSuppliedResumeBudget(
  budget: NonNullable<HardenedControllerSessionInput['budget']>
): Pick<WorkflowBudgetGrant, 'deadlineAt' | 'tokens'> {
  if (budget.authoritativeConsumed !== undefined) {
    rejectUnsupportedAuthoritativeConsumption();
  }
  if (typeof budget.deadlineAt !== 'string' || !Number.isFinite(Date.parse(budget.deadlineAt))) {
    throw new Error('Cannot resume hardened run: supplied budget deadline is malformed.');
  }
  return {
    deadlineAt: budget.deadlineAt,
    tokens: assertBudgetTokenRecord(budget.tokens, 'supplied budget'),
  };
}

function buildPolicyMetadata(
  input: HardenedControllerSessionInput,
  workflowDigest: string,
  seedManifest: Record<string, unknown>,
  policy: {
    budgetGrant: WorkflowBudgetGrant;
    approvalPolicy?: LoadedApprovalPolicy;
    copiedPolicyPath?: string;
    hmacKeyPath: string;
    sessionBindingPath: string;
    controllerActionGrants: readonly ControllerActionGrant[];
    egressPolicyB64?: string;
    proxyBudgetSeedDigest?: string;
  }
): HardenedControllerPolicyMetadata {
  return {
    schema: HARDENED_CONTROLLER_POLICY_SCHEMA,
    runId: input.runId,
    workflowName: input.workflow.name,
    workflowDigest,
    budgetGrant: { ...policy.budgetGrant, tokens: { ...policy.budgetGrant.tokens } },
    ...(input.workflowSource ? { workflowSource: input.workflowSource } : {}),
    image: input.image,
    requestedImage: input.requestedImage ?? input.image,
    conversationDigest: digestString(input.conversationId),
    userMessageDigest: digestString(input.userMessage),
    seedManifestDigest: digestString(JSON.stringify(seedManifest)),
    repoInputs: readRepoDescriptors(seedManifest),
    ...(policy.approvalPolicy ? { approvalPolicyDigest: policy.approvalPolicy.digest } : {}),
    ...(policy.copiedPolicyPath ? { approvalPolicyPath: policy.copiedPolicyPath } : {}),
    hmacKeyPath: policy.hmacKeyPath,
    sessionBindingPath: policy.sessionBindingPath,
    controllerActionGrants: [...policy.controllerActionGrants],
    ...(policy.egressPolicyB64 ? { egressPolicyB64: policy.egressPolicyB64 } : {}),
    ...(policy.proxyBudgetSeedDigest
      ? { proxyBudgetSeedDigest: policy.proxyBudgetSeedDigest }
      : {}),
  };
}

function writePolicy(
  path: string,
  metadata: HardenedControllerPolicyMetadata,
  seedPath: string,
  seedManifestDigest: string,
  budgetGrant: WorkflowBudgetGrant
): void {
  writeJson(path, { ...metadata, seedPath, seedManifestDigest, budgetGrant });
}

function readRepoDescriptors(
  seedManifest: Record<string, unknown>
): HardenedControllerRepoDescriptor[] {
  const repos = seedManifest.repoInputs;
  if (!Array.isArray(repos)) return [];
  return repos.map(repo => ({ ...(repo as HardenedControllerRepoDescriptor) }));
}

function verifyCopiedApprovalPolicy(policy: HardenedControllerPolicyMetadata): void {
  if (!policy.approvalPolicyDigest && !policy.approvalPolicyPath) return;
  assertPrivateSessionPathLayout(policy);
  if (!policy.approvalPolicyDigest || !policy.approvalPolicyPath) {
    throw new Error('Cannot resume hardened run: persisted approval policy binding is incomplete.');
  }
  if (
    gitFileDigest(readPrivateFile(policy.approvalPolicyPath, 'Hardened controller approval policy'))
      .sha256 !== policy.approvalPolicyDigest
  ) {
    throw new Error('Cannot resume hardened run: copied approval policy digest changed.');
  }
}

function readCopiedOperatorEgressPolicy(
  metadata: HardenedControllerPolicyMetadata
): OperatorEgressPolicy | undefined {
  if (!metadata.approvalPolicyPath) return undefined;
  const raw = readPrivateFile(metadata.approvalPolicyPath, 'Hardened controller approval policy');
  const policy = parseOperatorApprovalPolicy(JSON.parse(raw.toString('utf8')));
  if (policy.workflowDigest !== metadata.workflowDigest) {
    throw new Error('Cannot use hardened egress: copied operator policy workflow changed.');
  }
  return policy.egress;
}

function readCopiedOperatorProviderBudget(
  metadata: HardenedControllerPolicyMetadata
): OperatorProviderBudgetPolicy | undefined {
  if (!metadata.approvalPolicyPath) return undefined;
  const raw = readPrivateFile(metadata.approvalPolicyPath, 'Hardened controller approval policy');
  const policy = parseOperatorApprovalPolicy(JSON.parse(raw.toString('utf8')));
  if (policy.workflowDigest !== metadata.workflowDigest) {
    throw new Error(
      'Cannot use hardened provider budget: copied operator policy workflow changed.'
    );
  }
  return policy.providerBudget;
}

function readCopiedOperatorValidatorSource(
  metadata: HardenedControllerPolicyMetadata
): OperatorValidatorSourcePolicy | undefined {
  if (!metadata.approvalPolicyPath) return undefined;
  const raw = readPrivateFile(metadata.approvalPolicyPath, 'Hardened controller approval policy');
  const policy = parseOperatorApprovalPolicy(JSON.parse(raw.toString('utf8')));
  if (policy.workflowDigest !== metadata.workflowDigest) {
    throw new Error(
      'Candidate black-box validator source copied operator policy workflow changed.'
    );
  }
  return policy.validatorSource;
}

function sessionRequiresBlackboxValidator(session: HardenedControllerSession): boolean {
  return session.controllerActionGrants.some(grant => grant.phase === 'candidate-blackbox-test');
}

function assertCopiedEgressMatches(
  metadata: HardenedControllerPolicyMetadata,
  encoded: string
): void {
  const copied = readCopiedOperatorEgressPolicy(metadata);
  if (!copied) {
    throw new Error('Cannot use hardened egress: copied operator policy is missing.');
  }
  const copiedEncoded = normalizeOperatorEgressPolicy(metadata.image, copied);
  if (copiedEncoded !== encoded) {
    throw new Error('Cannot use hardened egress: copied operator egress policy changed.');
  }
}

function assertPolicyMetadata(value: unknown): HardenedControllerPolicyMetadata {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Cannot resume hardened run: persisted controller policy is missing.');
  }
  const record = value as Record<string, unknown>;
  if (record.schema !== HARDENED_CONTROLLER_POLICY_SCHEMA) {
    throw new Error(
      'Cannot resume hardened run: persisted controller policy schema is unsupported.'
    );
  }
  assertExactKeys(
    record,
    [
      'approvalPolicyDigest',
      'approvalPolicyPath',
      'budgetGrant',
      'controllerActionGrants',
      'conversationDigest',
      'egressPolicyB64',
      'hmacKeyPath',
      'image',
      'proxyBudgetSeedDigest',
      'repoInputs',
      'requestedImage',
      'runId',
      'schema',
      'seedManifestDigest',
      'sessionBindingPath',
      'userMessageDigest',
      'workflowDigest',
      'workflowName',
      'workflowSource',
    ],
    'persisted controller policy'
  );
  if (typeof record.runId !== 'string' || typeof record.workflowDigest !== 'string') {
    throw new Error('Cannot resume hardened run: persisted controller policy is malformed.');
  }
  if (typeof record.workflowName !== 'string' || typeof record.image !== 'string') {
    throw new Error('Cannot resume hardened run: persisted controller policy is malformed.');
  }
  if (typeof record.requestedImage !== 'string') {
    throw new Error('Cannot resume hardened run: persisted controller policy is malformed.');
  }
  const budgetGrant = assertBudgetGrantRecord(record.budgetGrant);
  if (
    budgetGrant.runId !== record.runId ||
    budgetGrant.workflowName !== record.workflowName ||
    budgetGrant.workflowDigest !== record.workflowDigest
  ) {
    throw new Error('Cannot resume hardened run: persisted budget grant binding is malformed.');
  }
  if (!Array.isArray(record.repoInputs) || !Array.isArray(record.controllerActionGrants)) {
    throw new Error('Cannot resume hardened run: persisted controller policy is malformed.');
  }
  assertPrivateSessionPathLayout(record);
  assertPersistedPolicyAuthorities(record);
  return record as unknown as HardenedControllerPolicyMetadata;
}

function assertPersistedPolicyAuthorities(record: Record<string, unknown>): void {
  const controllerActionGrants = record.controllerActionGrants;
  if (!Array.isArray(controllerActionGrants)) {
    throw new Error('Cannot resume hardened run: persisted controller policy is malformed.');
  }
  if (controllerActionGrants.length > 0 && typeof record.approvalPolicyDigest !== 'string') {
    throw new Error('Cannot resume hardened run: persisted approval policy binding is missing.');
  }
  if (record.egressPolicyB64 !== undefined) assertPersistedEgressPolicy(record);
  if (record.proxyBudgetSeedDigest !== undefined) assertPersistedProxyBudgetSeed(record);
  if (record.proxyBudgetSeedDigest !== undefined && record.egressPolicyB64 === undefined) {
    throw new Error('Cannot resume hardened run: provider budget seed lacks egress authority.');
  }
  if (!controllerActionGrants.every(isSealedControllerGrantRecord)) {
    throw new Error(
      'Cannot resume hardened run: persisted controller action grants are malformed.'
    );
  }
}

function assertPersistedEgressPolicy(record: Record<string, unknown>): void {
  if (typeof record.egressPolicyB64 !== 'string') {
    throw new Error('Cannot resume hardened run: persisted egress policy is malformed.');
  }
  assertCanonicalEgressPolicyB64(record.egressPolicyB64);
  if (
    typeof record.approvalPolicyDigest !== 'string' ||
    typeof record.approvalPolicyPath !== 'string' ||
    typeof record.proxyBudgetSeedDigest !== 'string'
  ) {
    throw new Error(
      'Cannot resume hardened run: persisted egress policy lacks operator authority.'
    );
  }
}

function assertPersistedProxyBudgetSeed(record: Record<string, unknown>): void {
  if (typeof record.proxyBudgetSeedDigest !== 'string') {
    throw new Error('Cannot resume hardened run: persisted provider budget seed is malformed.');
  }
  if (!/^[0-9a-f]{64}$/i.test(record.proxyBudgetSeedDigest)) {
    throw new Error('Cannot resume hardened run: persisted provider budget seed is malformed.');
  }
  if (
    typeof record.approvalPolicyDigest !== 'string' ||
    typeof record.approvalPolicyPath !== 'string'
  ) {
    throw new Error(
      'Cannot resume hardened run: persisted provider budget seed lacks operator authority.'
    );
  }
}

function assertPrivateSessionPathLayout(record: {
  runId?: unknown;
  hmacKeyPath?: unknown;
  sessionBindingPath?: unknown;
  approvalPolicyPath?: unknown;
}): void {
  if (typeof record.hmacKeyPath !== 'string' || typeof record.sessionBindingPath !== 'string') {
    throw new Error('Cannot resume hardened run: persisted private session binding is missing.');
  }
  if (typeof record.runId !== 'string') {
    throw new Error('Cannot resume hardened run: persisted controller policy is malformed.');
  }
  const runDir = assertControllerPrivateRunDir(record.runId, dirname(record.hmacKeyPath));
  assertFixedSessionFile(record.hmacKeyPath, runDir, HMAC_KEY_BASENAME, 'HMAC key');
  assertFixedSessionFile(
    record.sessionBindingPath,
    runDir,
    SESSION_BINDING_BASENAME,
    'session binding'
  );
  if (record.approvalPolicyPath !== undefined) {
    if (typeof record.approvalPolicyPath !== 'string') {
      throw new Error(
        'Cannot resume hardened run: persisted approval policy binding is malformed.'
      );
    }
    assertFixedSessionFile(
      record.approvalPolicyPath,
      runDir,
      COPIED_APPROVAL_POLICY_BASENAME,
      'copied approval policy'
    );
  }
}

function assertControllerPrivateRunDir(runId: string, rawDir: string): string {
  assertRunId(runId);
  const dir = resolve(rawDir);
  const root = resolve(getArchonHome(), 'controller-runs');
  const name = basename(dir);
  const prefix = `${runId}-`;
  if (
    dirname(dir) !== root ||
    !name.startsWith(prefix) ||
    !isStrictUuid(name.slice(prefix.length))
  ) {
    throw new Error('Cannot resume hardened run: private session path is not controller-owned.');
  }
  assertPrivateDirectory(root);
  assertPrivateDirectory(dir);
  return dir;
}

function assertFixedSessionFile(
  path: string,
  runDir: string,
  expectedBasename: string,
  label: string
): void {
  const resolved = resolve(path);
  if (dirname(resolved) !== runDir || basename(resolved) !== expectedBasename) {
    throw new Error(`Cannot resume hardened run: private ${label} path is not controller-owned.`);
  }
}

function isStrictUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);
}

function writeJson(path: string, value: unknown): void {
  writeFileSync(path, JSON.stringify(value, null, 2), { mode: PRIVATE_FILE_MODE });
  chmodSync(path, PRIVATE_FILE_MODE);
}

function writeJsonExclusive(path: string, value: unknown): void {
  writeFileSync(path, JSON.stringify(value, null, 2), { mode: PRIVATE_FILE_MODE, flag: 'wx' });
  chmodSync(path, PRIVATE_FILE_MODE);
}

function gitText(cwd: string, args: string[]): string {
  return git(cwd, args).toString('utf8').trim();
}

function rawGitText(cwd: string, args: string[]): string {
  return execFileSync('git', safeGitGlobalArgs(['-C', cwd, ...args]), {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: safeGitEnv(),
  })
    .toString('utf8')
    .trim();
}

function git(cwd: string, args: string[]): Buffer {
  try {
    return execFileSync('git', safeGitGlobalArgs(['-C', cwd, ...args]), {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: safeGitEnv(),
    });
  } catch (error) {
    throw new Error(`Failed to build hardened committed seed: ${(error as Error).message}`);
  }
}

function gitForSource(source: GitSeedSource, args: string[]): Buffer {
  return source.bare
    ? gitBare(source.repoRoot, args, source.gitDeadlineAt)
    : git(source.repoRoot, args);
}

function gitForSourceText(source: GitSeedSource, args: string[]): string {
  return gitForSource(source, args).toString('utf8').trim();
}

function safeGitGlobalArgs(args: string[]): string[] {
  return [
    '-c',
    'core.hooksPath=/dev/null',
    '-c',
    'core.fsmonitor=false',
    '-c',
    'core.attributesFile=/dev/null',
    '-c',
    'diff.external=',
    ...args,
  ];
}

function safeGitEnv(): NodeJS.ProcessEnv {
  return {
    PATH: TRUSTED_GIT_PATH,
    HOME: '/nonexistent',
    TMPDIR: process.env.TMPDIR ?? '/tmp',
    GIT_CONFIG_NOSYSTEM: '1',
    GIT_CONFIG_GLOBAL: '/dev/null',
    GIT_CONFIG_SYSTEM: '/dev/null',
    GIT_CONFIG_COUNT: '0',
    GIT_EXTERNAL_DIFF: '',
    GIT_NO_REPLACE_OBJECTS: '1',
  };
}

function stableSerialize(value: unknown): string {
  if (value === undefined) return 'null';
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableSerialize).join(',')}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .filter(key => record[key] !== undefined)
    .map(key => `${JSON.stringify(key)}:${stableSerialize(record[key])}`)
    .join(',')}}`;
}

function digestStable(value: unknown): string {
  return createHash('sha256').update(stableSerialize(value)).digest('hex');
}

function gitBlobOid(bytes: Buffer, objectFormat: 'sha1' | 'sha256'): string {
  return createHash(objectFormat).update(`blob ${bytes.byteLength}\0`).update(bytes).digest('hex');
}

function gitFileDigest(bytes: Buffer): { sha256: string; size: number } {
  return { sha256: createHash('sha256').update(bytes).digest('hex'), size: bytes.byteLength };
}

function digestString(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}
