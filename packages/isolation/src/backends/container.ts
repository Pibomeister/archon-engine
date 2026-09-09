/**
 * Hardened container isolation backend for folder projects.
 *
 * The hardened profile seeds a per-run Docker named volume from the controller
 * environment, removes live VCS metadata before any agent-facing execution, and
 * starts an unprivileged container over that volume. Agent turns and deterministic
 * subprocesses run via `docker exec` inside the container; the live worktree,
 * host home, credential files, and Docker socket are never mounted into the
 * agent-facing container.
 */

import { randomUUID } from 'crypto';
import { chmod, mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { encodeStrictEgressPolicy, decodeStrictEgressPolicy } from '../egress/strict-policy';
import { digestBudgetPolicy, digestProxyBudgetSeed } from '../egress/strict-proxy-launcher';
import { createEgressTlsMaterial } from '../egress/tls-material';
import type { EgressTlsMaterial } from '../egress/tls-material';
import type { BranchName } from '@archon/git';
import { createLogger } from '@archon/paths';
import type {
  ProviderOrigin,
  WriteBackFinalizeResult,
  WriteBackApplySummary,
} from '@archon/providers/types';
import {
  snapshotContainerArtifacts,
  type ArtifactSnapshotResult,
} from '../container/artifact-snapshot';
import type {
  BackendPrepareRequest,
  ContainerBackendConfig,
  HardenedProxyBudgetSeed,
  IIsolationBackend,
  PreparedEnv,
  VerifiedProxyBudgetStatus,
} from '../types';
import { CONTAINER_LABELS } from '../types';
import type { IIsolationStore } from '../store';
import {
  dockerCli,
  dockerPreflight,
  extractDockerError,
  type DockerRunner,
} from '../container/docker-exec';
import { isProxyBudgetGrantV2 } from '../egress/proxy-budget-ledger';
import type {
  BudgetStatus,
  ProxyBudgetGrant,
  ProxyBudgetGrantV1,
} from '../egress/proxy-budget-ledger';

const log = createLogger('isolation.container');

const NO_BRANCH_SENTINEL = '' as unknown as BranchName;
const READY_SENTINEL = '/tmp/archon-container-ready';
const READY_TIMEOUT_MS = 20_000;
const READY_POLL_INTERVAL_MS = 250;
const AGENT_USER = 'archon';
const AGENT_HOME = '/home/archon';
const AGENT_ARTIFACTS_DIR = '/archon-artifacts';
const SEED_TARGET_ROOT = '/seed-workspace';
const EGRESS_SOCKET_ROOT = '/archon-egress';
const EGRESS_SOCKET_PATH = `${EGRESS_SOCKET_ROOT}/proxy.sock`;
const EGRESS_PUBLIC_CA_PATH = `${EGRESS_SOCKET_ROOT}/ca.crt`;
const PROXY_PRIVATE_ROOT = '/archon-proxy-private';
const PROXY_POLICY_PATH = `${PROXY_PRIVATE_ROOT}/policy.json`;
const PROXY_LEAF_KEY_PATH = `${PROXY_PRIVATE_ROOT}/leaf.key`;
const PROXY_LEAF_CERT_PATH = `${PROXY_PRIVATE_ROOT}/leaf.crt`;
const PROXY_CA_CERT_PATH = `${PROXY_PRIVATE_ROOT}/ca.crt`;
const PROXY_BUDGET_GRANT_PATH = `${PROXY_PRIVATE_ROOT}/budget.json`;
const PROXY_PROVIDER_POLICIES_PATH = `${PROXY_PRIVATE_ROOT}/provider-policies.json`;
const PROXY_BUDGET_ROOT = '/archon-budget';
const PROXY_BUDGET_LEDGER_CLI = '/usr/local/lib/archon/egress/proxy-budget-ledger-cli.ts';
const STRICT_PROXY_CLI = '/usr/local/lib/archon/egress/strict-https-proxy-cli.mjs';
const AGENT_PROXY_PORT = 18080;
const AGENT_CPUS = '2';
const CONTROLLER_HELPER_CPUS = '1';
const RESOURCE_LIMIT_POLICY_VERSION = 1;

function providerBaseUrl(provider: string, host: string): string {
  return provider === 'anthropic' ? `https://${host}` : `https://${host}/v1`;
}

export interface ContainerBackendDeps {
  store: IIsolationStore;
  config: ContainerBackendConfig;
  dockerRunner?: DockerRunner;
}

interface ContainerEnvMetadata {
  containerId: string;
  containerName: string;
  volume: string;
  profile: 'hardened';
  workspaceVolume: string;
  homeVolume: string;
  artifactsVolume: string;
  agentArtifactsDir: string;
  egressVolume?: string;
  tlsVolume?: string;
  tlsValidUntil?: string;
  proxyContainerName?: string;
  budgetVolume?: string;
  budgetPolicyDigest?: string;
  proxyBudgetSeedDigest?: string;
  egressPolicyB64?: string;
  image: string;
  requestedImage?: string;
  ownerRunId?: string;
  resourceId: string;
  workspacePath: string;
  resourceLimits: ContainerResourceLimits;
  isolationMode: 'hardened';
  [key: string]: unknown;
}

interface ContainerResourceLimits {
  policyVersion: 1;
  agentCpus: '2';
  controllerHelperCpus: '1';
  memoryMb: number;
  pidsLimit: number;
}

interface ProxyBudgetSeed extends HardenedProxyBudgetSeed {
  digest: string;
  seedDigest: string;
}

interface PrepareEgressSetup {
  egressVolume: string;
  tlsVolume: string;
  budgetVolume: string;
  proxyContainerName: string;
  policyB64: string;
  tlsMaterial: EgressTlsMaterial;
  budgetSeed: ProxyBudgetSeed;
}

interface StrictEgressResumeMetadata {
  proxyContainerName: string;
  tlsVolume: string;
  egressVolume: string;
  budgetVolume: string;
  tlsValidUntil: string;
  policyB64: string;
  budgetPolicyDigest: string;
}

export class ContainerBackend implements IIsolationBackend {
  readonly id = 'container' as const;

  private readonly store: IIsolationStore;
  private readonly config: ContainerBackendConfig;
  private readonly docker: DockerRunner;
  private resolvedImageId: string | undefined;

  constructor(deps: ContainerBackendDeps) {
    this.store = deps.store;
    this.config = deps.config;
    this.docker = deps.dockerRunner ?? dockerCli;
  }

  async resolveImage(): Promise<string> {
    assertHardenedContainerConfig(this.config);
    this.resolvedImageId ??= await dockerPreflight(this.config.image, this.docker);
    return this.resolvedImageId;
  }

  async prepare(req: BackendPrepareRequest): Promise<PreparedEnv> {
    const hostRoot = req.codebase.defaultCwd;
    const seed = resolveSeed(req);
    const ownerRunId = resolveOwnerRunId(req.ownerRunId);
    const { image } = this.config;

    const imageId = await this.resolveImage();
    const providerOrigins = this.providerOriginsFromProxyBudget();

    const resourceId = randomUUID();
    const containerName = `archon-${resourceId}`;
    const seedName = `${containerName}-seed`;
    const workspaceVolume = `archon-${resourceId}-workspace`;
    const homeVolume = `archon-${resourceId}-home`;
    const artifactsVolume = `archon-${resourceId}-artifacts`;
    const egress = await this.buildPrepareEgress(resourceId, containerName, imageId);
    const egressVolume = egress?.egressVolume;
    const tlsVolume = egress?.tlsVolume;
    const budgetVolume = egress?.budgetVolume;
    const proxyContainerName = egress?.proxyContainerName;
    const resourceLimits = this.buildResourceLimits();

    log.info(
      { codebaseId: req.codebase.id, resourceId, image, hostRoot },
      'isolation.container_prepare_started'
    );

    try {
      await this.createManagedVolumes(
        [workspaceVolume, homeVolume, artifactsVolume, egressVolume, tlsVolume, budgetVolume],
        req.codebase.id,
        containerName,
        ownerRunId
      );
      await this.seedWorkspaceVolume(
        seedName,
        workspaceVolume,
        seed.path,
        imageId,
        seed.allowGitMetadata,
        ownerRunId
      );
      await this.initializeArtifactsVolume(
        `${containerName}-artifacts-init`,
        artifactsVolume,
        imageId,
        ownerRunId
      );
      await this.prepareStrictEgress(
        egress,
        `${containerName}-egress-init`,
        `${containerName}-budget-init`,
        req.codebase.id,
        imageId,
        ownerRunId
      );
      const containerId = await this.startHardenedContainer(
        containerName,
        workspaceVolume,
        homeVolume,
        artifactsVolume,
        egressVolume,
        hostRoot,
        req.codebase.id,
        imageId,
        resourceLimits,
        ownerRunId
      );
      await this.waitForReady(containerId);

      const metadata: ContainerEnvMetadata = {
        containerId,
        containerName,
        volume: workspaceVolume,
        profile: 'hardened',
        workspaceVolume,
        homeVolume,
        artifactsVolume,
        agentArtifactsDir: AGENT_ARTIFACTS_DIR,
        ...(egressVolume ? { egressVolume } : {}),
        ...(tlsVolume ? { tlsVolume } : {}),
        ...(egress ? { tlsValidUntil: egress.tlsMaterial.validUntil } : {}),
        ...(proxyContainerName ? { proxyContainerName } : {}),
        ...(budgetVolume ? { budgetVolume } : {}),
        ...(egress
          ? {
              budgetPolicyDigest: egress.budgetSeed.digest,
              proxyBudgetSeedDigest: egress.budgetSeed.seedDigest,
              egressPolicyB64: egress.policyB64,
            }
          : {}),
        image: imageId,
        requestedImage: image,
        ...(ownerRunId ? { ownerRunId } : {}),
        resourceId,
        isolationMode: 'hardened',
        workspacePath: hostRoot,
        resourceLimits,
      };
      const row = await this.store.create({
        codebase_id: req.codebase.id,
        workflow_type: 'task',
        workflow_id: resourceId,
        provider: 'container',
        working_path: hostRoot,
        branch_name: NO_BRANCH_SENTINEL,
        metadata,
      });

      log.info({ envId: row.id, containerId, resourceId }, 'isolation.container_prepare_completed');
      return {
        cwd: hostRoot,
        execContext: {
          kind: 'container',
          profile: 'hardened',
          containerId,
          execUser: AGENT_USER,
          agentArtifactsDir: AGENT_ARTIFACTS_DIR,
          ...(providerOrigins ? { providerOrigins } : {}),
        },
        envId: row.id,
        agentArtifactsDir: AGENT_ARTIFACTS_DIR,
        artifactSnapshot: { workspaceVolume: artifactsVolume, image: imageId, resourceId },
      };
    } catch (err) {
      await this.removeContainerAndVolumes(
        containerName,
        [
          workspaceVolume,
          homeVolume,
          artifactsVolume,
          egressVolume,
          tlsVolume,
          budgetVolume,
        ].filter(isString),
        seedName,
        proxyContainerName
      );
      throw err;
    }
  }

  async destroy(envId: string): Promise<void> {
    const row = await this.store.getById(envId);
    if (!row) {
      log.warn({ envId }, 'isolation.container_destroy_row_missing');
      return;
    }
    const meta = requireHardenedMetadata(envId, row.metadata as Partial<ContainerEnvMetadata>);
    const containerName = meta.containerName;
    const proxyContainerName = meta.proxyContainerName;
    const volumes = collectVolumes(meta);

    const failures: string[] = [];
    for (const handle of [containerName, proxyContainerName].filter(isString)) {
      const err = await this.removeIgnoringNotFound(['rm', '-f', handle]);
      if (err) failures.push(`container ${handle}: ${err}`);
    }
    for (const volume of volumes) {
      const err = await this.removeIgnoringNotFound(['volume', 'rm', '-f', volume]);
      if (err) failures.push(`volume ${volume}: ${err}`);
    }

    if (failures.length > 0) {
      log.error({ envId, failures }, 'isolation.container_destroy_failed');
      throw new Error(
        `Failed to remove the isolation container/volume for env '${envId}': ` + failures.join('; ')
      );
    }

    await this.store.updateStatus(envId, 'destroyed').catch(err => {
      log.warn({ envId, err: err as Error }, 'isolation.container_destroy_status_update_failed');
    });

    log.info({ envId, containerName, volumes }, 'isolation.container_destroy_completed');
  }

  async suspend(envId: string): Promise<void> {
    const meta = requireHardenedMetadata(envId, await this.loadMetadata(envId));
    const handle = meta.containerName;
    const proxyHandle = meta.proxyContainerName;
    const failures: string[] = [];
    for (const name of [handle, proxyHandle].filter(isString)) {
      try {
        await this.docker(['stop', name]);
      } catch (err) {
        const detail = extractDockerError(err);
        if (/no such container|is not running/i.test(detail)) {
          log.debug({ envId, handle: name, detail }, 'isolation.container_suspend_already_stopped');
        } else {
          failures.push(`${name}: ${detail}`);
          log.error({ envId, handle: name, detail }, 'isolation.container_suspend_failed');
        }
      }
    }
    if (failures.length)
      throw new Error(`Failed to suspend env '${envId}': ${failures.join('; ')}`);
    log.info({ envId, handle }, 'isolation.container_suspended');
  }

  async resumeEnv(
    envId: string,
    binding?: Parameters<NonNullable<IIsolationBackend['resumeEnv']>>[1]
  ): Promise<PreparedEnv> {
    const {
      containerName,
      workspaceVolume,
      homeVolume,
      artifactsVolume,
      egressVolume,
      tlsVolume,
      tlsValidUntil,
      proxyContainerName,
      budgetVolume,
      budgetPolicyDigest,
      proxyBudgetSeedDigest,
      workspacePath,
      image,
      resourceId,
      egressPolicyB64,
      resourceLimits,
      ownerRunId,
    } = requireHardenedMetadata(envId, await this.loadMetadata(envId));
    if (
      binding &&
      (egressPolicyB64 !== binding.egressPolicyB64 ||
        image !== binding.image ||
        ownerRunId !== binding.ownerRunId ||
        proxyBudgetSeedDigest !== binding.proxyBudgetSeedDigest)
    ) {
      throw new Error(
        'Cannot resume strict egress: isolation policy differs from controller authority.'
      );
    }
    if (binding) {
      await this.verifyResumeResourceOwnership(
        [
          workspaceVolume,
          homeVolume,
          artifactsVolume,
          egressVolume,
          tlsVolume,
          budgetVolume,
        ].filter(isString),
        [containerName, proxyContainerName].filter(isString),
        binding
      );
    }
    const row = await this.store.getById(envId);
    const codebaseId = row?.codebase_id ?? '';

    const presence = await this.describeContainer(containerName);
    if (presence === 'running') {
      await this.ensureEgressProxy(
        proxyContainerName,
        tlsVolume,
        egressVolume,
        budgetVolume,
        tlsValidUntil,
        codebaseId,
        image,
        egressPolicyB64,
        budgetPolicyDigest,
        proxyBudgetSeedDigest,
        ownerRunId
      );
      const containerId = await this.getContainerId(containerName);
      log.info({ envId, containerName }, 'isolation.container_resume_reused_running');
      return this.preparedEnvFor(
        containerId,
        workspacePath,
        envId,
        artifactsVolume,
        image,
        resourceId
      );
    }
    if (presence === 'stopped') {
      await this.ensureEgressProxy(
        proxyContainerName,
        tlsVolume,
        egressVolume,
        budgetVolume,
        tlsValidUntil,
        codebaseId,
        image,
        egressPolicyB64,
        budgetPolicyDigest,
        proxyBudgetSeedDigest,
        ownerRunId
      );
      await this.docker(['start', containerName]);
      const containerId = await this.getContainerId(containerName);
      await this.waitForReady(containerId);
      log.info({ envId, containerName }, 'isolation.container_resume_restarted');
      return this.preparedEnvFor(
        containerId,
        workspacePath,
        envId,
        artifactsVolume,
        image,
        resourceId
      );
    }

    if (
      !(await this.volumeExists(workspaceVolume)) ||
      !(await this.volumeExists(homeVolume)) ||
      !(await this.volumeExists(artifactsVolume)) ||
      (egressVolume !== undefined && !(await this.volumeExists(egressVolume))) ||
      (tlsVolume !== undefined && !(await this.volumeExists(tlsVolume))) ||
      (budgetVolume !== undefined && !(await this.volumeExists(budgetVolume)))
    ) {
      throw new Error(
        `Cannot resume container env '${envId}': one or more per-run volumes are gone, ` +
          'so the un-applied isolated state is lost. Start a fresh --container run.'
      );
    }
    await this.ensureEgressProxy(
      proxyContainerName,
      tlsVolume,
      egressVolume,
      budgetVolume,
      tlsValidUntil,
      codebaseId,
      image,
      egressPolicyB64,
      budgetPolicyDigest,
      proxyBudgetSeedDigest,
      ownerRunId
    );
    const containerId = await this.startHardenedContainer(
      containerName,
      workspaceVolume,
      homeVolume,
      artifactsVolume,
      egressVolume,
      workspacePath,
      codebaseId,
      image,
      resourceLimits,
      ownerRunId
    );
    await this.waitForReady(containerId);
    log.info(
      { envId, containerName, workspaceVolume, homeVolume },
      'isolation.container_resume_recreated'
    );
    return this.preparedEnvFor(
      containerId,
      workspacePath,
      envId,
      artifactsVolume,
      image,
      resourceId
    );
  }

  async snapshotArtifacts(envId: string, destinationDir: string): Promise<ArtifactSnapshotResult> {
    const { artifactsVolume, image, resourceId } = requireHardenedMetadata(
      envId,
      await this.loadMetadata(envId)
    );
    await this.suspend(envId);
    return snapshotContainerArtifacts(
      this.docker,
      { workspaceVolume: artifactsVolume, image, resourceId },
      { destinationDir }
    );
  }

  async readProxyBudgetStatus(
    envId: string,
    binding: Parameters<NonNullable<IIsolationBackend['readProxyBudgetStatus']>>[1]
  ): Promise<VerifiedProxyBudgetStatus> {
    const meta = requireHardenedMetadata(envId, await this.loadMetadata(envId));
    const metadata = this.assertProxyBudgetStatusAuthority(envId, meta, binding);
    await this.verifyResumeResourceOwnership(
      [metadata.egressVolume, metadata.tlsVolume, metadata.budgetVolume],
      [metadata.proxyContainerName],
      binding
    );
    await this.assertStrictEgressVolumesExist(metadata);
    const budgetSeed = this.resolveProxyBudgetSeed(metadata.policyB64, meta.image);
    if (
      metadata.budgetPolicyDigest !== budgetSeed.digest ||
      binding.proxyBudgetSeedDigest !== budgetSeed.seedDigest
    ) {
      throw new Error('Cannot read proxy budget status: controller seed binding drifted.');
    }
    const rawStatus = await this.readFixedProxyBudgetStatus(metadata, meta.image, meta.ownerRunId);
    return normalizeVerifiedProxyBudgetStatus(envId, rawStatus, budgetSeed.grant);
  }

  async finalize(envId: string): Promise<WriteBackFinalizeResult> {
    await this.loadMetadata(envId);
    throw new Error(
      `Cannot finalize container env '${envId}': hardened container write-back is not implemented. ` +
        'The live worktree was never mounted into the agent container; preserve the named volumes ' +
        'for controller-owned publication/write-back support.'
    );
  }

  async applyChanges(envId: string): Promise<WriteBackApplySummary> {
    await this.loadMetadata(envId);
    throw new Error(
      `Cannot apply container env '${envId}': hardened container write-back is not implemented. ` +
        'Refusing to copy untrusted agent artifacts to the live worktree without a controller action.'
    );
  }

  async discardChanges(envId: string): Promise<void> {
    log.info({ envId }, 'isolation.container_changes_discarded');
  }

  private async loadMetadata(envId: string): Promise<Partial<ContainerEnvMetadata>> {
    const row = await this.store.getById(envId);
    if (!row) throw new Error(`Container env '${envId}' not found (its tracking row is gone).`);
    return row.metadata as Partial<ContainerEnvMetadata>;
  }

  private preparedEnvFor(
    containerId: string,
    cwd: string,
    envId: string,
    artifactsVolume?: string,
    image?: string,
    resourceId?: string
  ): PreparedEnv {
    const providerOrigins = this.providerOriginsFromProxyBudget();
    return {
      cwd,
      execContext: {
        kind: 'container',
        profile: 'hardened',
        containerId,
        execUser: AGENT_USER,
        agentArtifactsDir: AGENT_ARTIFACTS_DIR,
        ...(providerOrigins ? { providerOrigins } : {}),
      },
      envId,
      ...(artifactsVolume && image && resourceId
        ? {
            agentArtifactsDir: AGENT_ARTIFACTS_DIR,
            artifactSnapshot: { workspaceVolume: artifactsVolume, image, resourceId },
          }
        : {}),
    };
  }

  private async buildPrepareEgress(
    resourceId: string,
    containerName: string,
    imageId: string
  ): Promise<PrepareEgressSetup | undefined> {
    const policy = this.config.egressPolicy;
    if (!policy) return undefined;
    const policyB64 = encodeStrictEgressPolicy(policy);
    return {
      egressVolume: `archon-${resourceId}-egress`,
      tlsVolume: `archon-${resourceId}-egress-tls`,
      budgetVolume: `archon-${resourceId}-budget`,
      proxyContainerName: `${containerName}-egress-proxy`,
      policyB64,
      tlsMaterial: await createEgressTlsMaterial(policy.targets.map(target => target.host)),
      budgetSeed: this.resolveProxyBudgetSeed(policyB64, imageId),
    };
  }

  private async createManagedVolumes(
    volumes: (string | undefined)[],
    codebaseId: string,
    containerName: string,
    ownerRunId?: string
  ): Promise<void> {
    for (const volume of volumes.filter(isString)) {
      await this.createManagedVolume(volume, codebaseId, containerName, ownerRunId);
    }
  }

  private async prepareStrictEgress(
    egress: PrepareEgressSetup | undefined,
    egressInitName: string,
    budgetInitName: string,
    codebaseId: string,
    imageId: string,
    ownerRunId?: string
  ): Promise<void> {
    if (!egress) return;
    await this.stageStrictEgressVolumes(
      egressInitName,
      egress.tlsVolume,
      egress.egressVolume,
      egress.budgetVolume,
      imageId,
      egress.policyB64,
      egress.tlsMaterial,
      egress.budgetSeed,
      ownerRunId
    );
    await this.initializeProxyBudgetLedger(
      budgetInitName,
      egress.tlsVolume,
      egress.budgetVolume,
      imageId,
      ownerRunId
    );
    await this.startEgressProxy(
      egress.proxyContainerName,
      egress.tlsVolume,
      egress.egressVolume,
      egress.budgetVolume,
      codebaseId,
      imageId,
      egress.policyB64,
      egress.budgetSeed.digest,
      ownerRunId
    );
  }

  private async createManagedVolume(
    volume: string,
    codebaseId: string,
    containerName: string,
    ownerRunId?: string
  ): Promise<void> {
    await this.docker([
      'volume',
      'create',
      '--label',
      `${CONTAINER_LABELS.managed}=true`,
      '--label',
      `${CONTAINER_LABELS.codebaseId}=${codebaseId}`,
      '--label',
      `${CONTAINER_LABELS.envId}=${containerName}`,
      ...ownerRunLabelArgs(ownerRunId),
      volume,
    ]);
  }

  private async verifyResumeResourceOwnership(
    volumes: string[],
    containers: string[],
    binding: { image: string; ownerRunId: string }
  ): Promise<void> {
    const label = CONTAINER_LABELS.ownerRunId;
    for (const volume of volumes) {
      const { stdout } = await this.docker([
        'volume',
        'inspect',
        '--format',
        `{{index .Labels "${label}"}}`,
        volume,
      ]);
      if (stdout.trim() !== binding.ownerRunId)
        throw new Error('Cannot resume: volume ownership differs from controller authority.');
    }
    for (const container of containers) {
      if ((await this.describeContainer(container)) === 'missing') continue;
      const { stdout } = await this.docker([
        'inspect',
        '--format',
        `{{index .Config.Labels "${label}"}}\n{{.Image}}`,
        container,
      ]);
      const [owner, image] = stdout.trim().split('\n');
      if (owner !== binding.ownerRunId || image !== binding.image)
        throw new Error('Cannot resume: container identity differs from controller authority.');
    }
  }

  private async seedWorkspaceVolume(
    seedName: string,
    workspaceVolume: string,
    seedRoot: string,
    imageRef: string,
    allowGitMetadata: boolean,
    ownerRunId?: string
  ): Promise<void> {
    await this.docker([
      'create',
      '--name',
      seedName,
      '--label',
      `${CONTAINER_LABELS.managed}=true`,
      '--label',
      `${CONTAINER_LABELS.envId}=${seedName}`,
      ...ownerRunLabelArgs(ownerRunId),
      '--network',
      'none',
      '--cpus',
      CONTROLLER_HELPER_CPUS,
      '--cap-drop',
      'ALL',
      '--cap-add',
      'CHOWN',
      '--security-opt',
      'no-new-privileges',
      '-v',
      `${workspaceVolume}:${SEED_TARGET_ROOT}`,
      '--entrypoint',
      'sleep',
      imageRef,
      'infinity',
    ]);
    try {
      await this.docker(['cp', `${seedRoot}/.`, `${seedName}:${SEED_TARGET_ROOT}/`], {
        timeout: 120_000,
      });
      await this.docker(['start', seedName]);
      await this.docker([
        'exec',
        '-u',
        '0',
        seedName,
        'sh',
        '-c',
        buildSeedValidationScript(SEED_TARGET_ROOT, allowGitMetadata),
      ]);
    } finally {
      await this.removeIgnoringNotFound(['rm', '-f', seedName]);
    }
  }

  private async initializeArtifactsVolume(
    initName: string,
    artifactsVolume: string,
    imageRef: string,
    ownerRunId?: string
  ): Promise<void> {
    await this.docker([
      'run',
      '--rm',
      '--name',
      initName,
      '--label',
      `${CONTAINER_LABELS.managed}=true`,
      '--label',
      `${CONTAINER_LABELS.envId}=${initName}`,
      ...ownerRunLabelArgs(ownerRunId),
      '--network',
      'none',
      '--cpus',
      CONTROLLER_HELPER_CPUS,
      '--cap-drop',
      'ALL',
      '--cap-add',
      'CHOWN',
      '--security-opt',
      'no-new-privileges',
      '-v',
      `${artifactsVolume}:${AGENT_ARTIFACTS_DIR}`,
      '--entrypoint',
      'sh',
      imageRef,
      '-c',
      `umask 077 && mkdir -p ${AGENT_ARTIFACTS_DIR}/run ${AGENT_ARTIFACTS_DIR}/state ${AGENT_ARTIFACTS_DIR}/logs && chown -R ${AGENT_USER}:${AGENT_USER} ${AGENT_ARTIFACTS_DIR}`,
    ]);
  }

  private async startHardenedContainer(
    containerName: string,
    workspaceVolume: string,
    homeVolume: string,
    artifactsVolume: string,
    egressVolume: string | undefined,
    hostRoot: string,
    codebaseId: string,
    imageRef: string,
    resourceLimits: ContainerResourceLimits,
    ownerRunId?: string
  ): Promise<string> {
    const args = [
      'run',
      '-d',
      '--name',
      containerName,
      '--label',
      `${CONTAINER_LABELS.managed}=true`,
      '--label',
      `${CONTAINER_LABELS.codebaseId}=${codebaseId}`,
      '--label',
      `${CONTAINER_LABELS.envId}=${containerName}`,
      ...ownerRunLabelArgs(ownerRunId),
      '--restart',
      'no',
      '--user',
      AGENT_USER,
      '--read-only',
      '--cap-drop',
      'ALL',
      '--security-opt',
      'no-new-privileges',
      '--memory',
      `${resourceLimits.memoryMb}m`,
      '--cpus',
      resourceLimits.agentCpus,
      '--pids-limit',
      String(resourceLimits.pidsLimit),
      '--network',
      this.config.network,
      '--tmpfs',
      '/tmp:rw,nosuid,nodev,size=256m',
      '--tmpfs',
      '/run:rw,nosuid,nodev,size=16m',
      '-v',
      `${workspaceVolume}:${hostRoot}`,
      '-v',
      `${homeVolume}:${AGENT_HOME}`,
      '-v',
      `${artifactsVolume}:${AGENT_ARTIFACTS_DIR}`,
      ...agentEgressArgs(egressVolume),
      '-e',
      `ARCHON_WORKSPACE_PATH=${hostRoot}`,
      '-e',
      `HOME=${AGENT_HOME}`,
      '-e',
      `ARCHON_AGENT_ARTIFACTS_DIR=${AGENT_ARTIFACTS_DIR}`,
      '-e',
      `CLAUDE_CONFIG_DIR=${AGENT_HOME}/.claude`,
      imageRef,
    ];
    const { stdout } = await this.docker(args);
    return stdout.trim();
  }

  private async startEgressProxy(
    proxyContainerName: string,
    tlsVolume: string,
    egressVolume: string,
    budgetVolume: string,
    codebaseId: string,
    imageRef: string,
    policyB64: string | undefined,
    budgetPolicyDigest: string | undefined,
    ownerRunId?: string
  ): Promise<void> {
    if (!policyB64) throw new Error('Cannot start strict egress proxy without a frozen policy.');
    if (!budgetPolicyDigest)
      throw new Error('Cannot start strict egress proxy without a budget policy digest.');
    await this.docker([
      'run',
      '-d',
      '--name',
      proxyContainerName,
      '--label',
      `${CONTAINER_LABELS.managed}=true`,
      '--label',
      `${CONTAINER_LABELS.codebaseId}=${codebaseId}`,
      '--label',
      `${CONTAINER_LABELS.envId}=${proxyContainerName}`,
      ...ownerRunLabelArgs(ownerRunId),
      '--restart',
      'no',
      '--user',
      AGENT_USER,
      '--read-only',
      '--cap-drop',
      'ALL',
      '--security-opt',
      'no-new-privileges',
      '--memory',
      '128m',
      '--cpus',
      CONTROLLER_HELPER_CPUS,
      '--pids-limit',
      '128',
      '--network',
      'bridge',
      '--tmpfs',
      '/tmp:rw,nosuid,nodev,size=16m',
      '-v',
      `${egressVolume}:${EGRESS_SOCKET_ROOT}`,
      '-v',
      `${tlsVolume}:${PROXY_PRIVATE_ROOT}:ro`,
      '-v',
      `${budgetVolume}:${PROXY_BUDGET_ROOT}`,
      '-e',
      `ARCHON_EGRESS_SOCKET=${EGRESS_SOCKET_PATH}`,
      '-e',
      `ARCHON_EGRESS_POLICY_B64=${policyB64}`,
      '-e',
      `ARCHON_PROXY_IMAGE_ID=${imageRef}`,
      '-e',
      `ARCHON_PROXY_BUDGET_POLICY_DIGEST=${budgetPolicyDigest}`,
      '--entrypoint',
      'node',
      imageRef,
      STRICT_PROXY_CLI,
    ]);
    await this.waitForProxyReady(proxyContainerName);
  }

  private resolveProxyBudgetSeed(egressPolicyB64: string, imageRef: string): ProxyBudgetSeed {
    const budget = this.config.proxyBudget;
    if (!budget) throw new Error('Strict egress requires a controller-seeded proxy budget.');
    const providerPolicies = budget.providerPolicies.map(policy => ({ ...policy }));
    const digest = digestBudgetPolicy({ egressPolicyB64, image: imageRef, providerPolicies });
    assertSingleRunProxyBudgetGrant(budget.grant);
    if (budget.grant.policyDigest !== digest) {
      throw new Error('Proxy budget seed does not match frozen egress policy and image.');
    }
    if (budget.grant.deadlineEpochMs <= Date.now()) {
      throw new Error('Proxy budget seed deadline is expired.');
    }
    const seed = { grant: { ...budget.grant }, providerPolicies };
    return { ...seed, digest, seedDigest: digestProxyBudgetSeed(seed) };
  }

  private providerOriginsFromProxyBudget(): ProviderOrigin[] | undefined {
    const budget = this.config.proxyBudget;
    if (!budget) return undefined;
    const origins = new Map<string, ProviderOrigin>();
    for (const policy of budget.providerPolicies) {
      const next = {
        provider: policy.provider,
        baseUrl: providerBaseUrl(policy.provider, policy.host),
      };
      const existing = origins.get(policy.provider);
      if (existing && existing.baseUrl !== next.baseUrl) {
        throw new Error(`Conflicting provider origins for budget provider '${policy.provider}'.`);
      }
      origins.set(policy.provider, next);
    }
    return origins.size > 0 ? [...origins.values()] : undefined;
  }

  private buildResourceLimits(): ContainerResourceLimits {
    return {
      policyVersion: RESOURCE_LIMIT_POLICY_VERSION,
      agentCpus: AGENT_CPUS,
      controllerHelperCpus: CONTROLLER_HELPER_CPUS,
      memoryMb: this.config.memoryMb,
      pidsLimit: this.config.pidsLimit,
    };
  }

  private async ensureEgressProxy(
    proxyContainerName: string | undefined,
    tlsVolume: string | undefined,
    egressVolume: string | undefined,
    budgetVolume: string | undefined,
    tlsValidUntil: string | undefined,
    codebaseId: string,
    imageRef: string,
    policyB64: string | undefined,
    budgetPolicyDigest: string | undefined,
    proxyBudgetSeedDigest: string | undefined,
    ownerRunId?: string
  ): Promise<void> {
    const metadata = normalizeStrictEgressResumeMetadata({
      proxyContainerName,
      tlsVolume,
      egressVolume,
      budgetVolume,
      tlsValidUntil,
      policyB64,
      budgetPolicyDigest,
      proxyBudgetSeedDigest,
    });
    if (!metadata) return;
    assertTlsStillValid(metadata.tlsValidUntil);
    await this.assertStrictEgressVolumesExist(metadata);
    const presence = await this.describeContainer(metadata.proxyContainerName);
    if (presence === 'running') {
      await this.assertProxyContainerBindingFromMetadata(metadata, imageRef);
      return;
    }
    if (presence === 'stopped') {
      await this.assertProxyContainerBindingFromMetadata(metadata, imageRef);
      await this.docker(['start', metadata.proxyContainerName]);
      await this.waitForProxyReady(metadata.proxyContainerName);
      return;
    }
    await this.startEgressProxy(
      metadata.proxyContainerName,
      metadata.tlsVolume,
      metadata.egressVolume,
      metadata.budgetVolume,
      codebaseId,
      imageRef,
      metadata.policyB64,
      metadata.budgetPolicyDigest,
      ownerRunId
    );
  }

  private async assertStrictEgressVolumesExist(
    metadata: StrictEgressResumeMetadata
  ): Promise<void> {
    const checks = [metadata.egressVolume, metadata.tlsVolume, metadata.budgetVolume];
    for (const volume of checks) {
      if (!(await this.volumeExists(volume))) {
        throw new Error(
          'Cannot resume strict egress: frozen egress, TLS or budget material volume is missing.'
        );
      }
    }
  }

  private async assertProxyContainerBindingFromMetadata(
    metadata: StrictEgressResumeMetadata,
    imageRef: string
  ): Promise<void> {
    await this.assertProxyContainerBinding(
      metadata.proxyContainerName,
      metadata.tlsVolume,
      metadata.egressVolume,
      metadata.budgetVolume,
      imageRef,
      metadata.policyB64,
      metadata.budgetPolicyDigest
    );
  }

  private async assertProxyContainerBinding(
    proxyContainerName: string,
    tlsVolume: string,
    egressVolume: string,
    budgetVolume: string,
    imageRef: string,
    policyB64: string,
    budgetPolicyDigest: string
  ): Promise<void> {
    const { stdout } = await this.docker([
      'inspect',
      '-f',
      '{{json .Config.Env}}\n{{.Image}}\n{{json .Mounts}}',
      proxyContainerName,
    ]);
    const [envJson, image, mountsJson] = stdout.trim().split('\n');
    const env = JSON.parse(envJson ?? '[]') as unknown;
    const mounts = JSON.parse(mountsJson ?? '[]') as unknown;
    if (
      !Array.isArray(env) ||
      !env.includes(`ARCHON_EGRESS_POLICY_B64=${policyB64}`) ||
      !env.includes(`ARCHON_EGRESS_SOCKET=${EGRESS_SOCKET_PATH}`) ||
      !env.includes(`ARCHON_PROXY_IMAGE_ID=${imageRef}`) ||
      !env.includes(`ARCHON_PROXY_BUDGET_POLICY_DIGEST=${budgetPolicyDigest}`) ||
      image !== imageRef ||
      !hasVolumeMount(mounts, tlsVolume, PROXY_PRIVATE_ROOT, false) ||
      !hasVolumeMount(mounts, egressVolume, EGRESS_SOCKET_ROOT, true) ||
      !hasVolumeMount(mounts, budgetVolume, PROXY_BUDGET_ROOT, true)
    ) {
      throw new Error('Cannot resume strict egress: proxy container binding drifted.');
    }
  }

  private assertProxyBudgetStatusAuthority(
    envId: string,
    meta: ContainerEnvMetadata,
    binding: Parameters<NonNullable<IIsolationBackend['readProxyBudgetStatus']>>[1]
  ): StrictEgressResumeMetadata {
    const metadata = normalizeStrictEgressResumeMetadata({
      proxyContainerName: meta.proxyContainerName,
      tlsVolume: meta.tlsVolume,
      egressVolume: meta.egressVolume,
      budgetVolume: meta.budgetVolume,
      tlsValidUntil: meta.tlsValidUntil,
      policyB64: meta.egressPolicyB64,
      budgetPolicyDigest: meta.budgetPolicyDigest,
      proxyBudgetSeedDigest: meta.proxyBudgetSeedDigest,
    });
    if (!metadata) throw new Error(`Container env '${envId}' has no proxy budget ledger.`);
    if (
      metadata.policyB64 !== binding.egressPolicyB64 ||
      meta.image !== binding.image ||
      meta.ownerRunId !== binding.ownerRunId ||
      meta.proxyBudgetSeedDigest !== binding.proxyBudgetSeedDigest
    ) {
      throw new Error('Cannot read proxy budget status: controller authority differs.');
    }
    assertTlsStillValid(metadata.tlsValidUntil);
    return metadata;
  }

  private async readFixedProxyBudgetStatus(
    metadata: StrictEgressResumeMetadata,
    imageRef: string,
    ownerRunId?: string
  ): Promise<BudgetStatus> {
    const presence = await this.describeContainer(metadata.proxyContainerName);
    const result =
      presence === 'running'
        ? await this.readStatusFromBoundRunningProxy(metadata, imageRef)
        : await this.runProxyStatusHelper(metadata, imageRef, ownerRunId);
    return parseProxyBudgetStatusOutput(result.stdout);
  }

  private async readStatusFromBoundRunningProxy(
    metadata: StrictEgressResumeMetadata,
    imageRef: string
  ): Promise<{ stdout: string; stderr: string }> {
    await this.assertProxyContainerBindingFromMetadata(metadata, imageRef);
    return this.runProxyStatusInRunningProxy(metadata.proxyContainerName);
  }

  private async runProxyStatusInRunningProxy(
    proxyContainerName: string
  ): Promise<{ stdout: string; stderr: string }> {
    return await this.docker([
      'exec',
      '-u',
      AGENT_USER,
      proxyContainerName,
      '/usr/local/bin/bun',
      PROXY_BUDGET_LEDGER_CLI,
      '--status',
    ]);
  }

  private async runProxyStatusHelper(
    metadata: StrictEgressResumeMetadata,
    imageRef: string,
    ownerRunId?: string
  ): Promise<{ stdout: string; stderr: string }> {
    const helperName = `archon-budget-status-${randomUUID()}`;
    return await this.docker([
      'run',
      '--rm',
      '--name',
      helperName,
      '--label',
      `${CONTAINER_LABELS.managed}=true`,
      '--label',
      `${CONTAINER_LABELS.envId}=${helperName}`,
      ...ownerRunLabelArgs(ownerRunId),
      '--network',
      'none',
      '--user',
      AGENT_USER,
      '--read-only',
      '--memory',
      '128m',
      '--cpus',
      CONTROLLER_HELPER_CPUS,
      '--pids-limit',
      '64',
      '--cap-drop',
      'ALL',
      '--security-opt',
      'no-new-privileges',
      '-v',
      `${metadata.tlsVolume}:${PROXY_PRIVATE_ROOT}:ro`,
      '-v',
      `${metadata.budgetVolume}:${PROXY_BUDGET_ROOT}`,
      '--entrypoint',
      '/usr/local/bin/bun',
      imageRef,
      PROXY_BUDGET_LEDGER_CLI,
      '--status',
    ]);
  }

  private async stageStrictEgressVolumes(
    initName: string,
    tlsVolume: string,
    egressVolume: string,
    budgetVolume: string,
    imageRef: string,
    policyB64: string,
    material: EgressTlsMaterial,
    budgetSeed: ProxyBudgetSeed,
    ownerRunId?: string
  ): Promise<void> {
    const tempDir = await mkdtemp(join(tmpdir(), 'archon-egress-material-'));
    await chmod(tempDir, 0o700);
    let cleanupFailure: string | undefined;
    try {
      await writeFile(join(tempDir, 'policy.json'), Buffer.from(policyB64, 'base64'), {
        mode: 0o600,
      });
      await writeFile(join(tempDir, 'leaf.key'), material.privateKey, { mode: 0o600 });
      await writeFile(join(tempDir, 'leaf.crt'), material.certificate, { mode: 0o600 });
      await writeFile(join(tempDir, 'ca.crt'), material.caCertificate, { mode: 0o600 });
      await writeFile(join(tempDir, 'budget.json'), JSON.stringify(budgetSeed.grant), {
        mode: 0o600,
      });
      await writeFile(
        join(tempDir, 'provider-policies.json'),
        JSON.stringify(budgetSeed.providerPolicies),
        { mode: 0o600 }
      );
      await this.docker([
        'create',
        '--name',
        initName,
        '--label',
        `${CONTAINER_LABELS.managed}=true`,
        '--label',
        `${CONTAINER_LABELS.envId}=${initName}`,
        ...ownerRunLabelArgs(ownerRunId),
        '--network',
        'none',
        '--user',
        '0',
        '--read-only',
        '--memory',
        '128m',
        '--cpus',
        CONTROLLER_HELPER_CPUS,
        '--pids-limit',
        '64',
        '--cap-drop',
        'ALL',
        '--cap-add',
        'CHOWN',
        '--security-opt',
        'no-new-privileges',
        '-v',
        `${tlsVolume}:${PROXY_PRIVATE_ROOT}`,
        '-v',
        `${egressVolume}:${EGRESS_SOCKET_ROOT}`,
        '-v',
        `${budgetVolume}:${PROXY_BUDGET_ROOT}`,
        '--entrypoint',
        'sleep',
        imageRef,
        'infinity',
      ]);
      await this.docker(['cp', `${tempDir}/.`, `${initName}:${PROXY_PRIVATE_ROOT}/`], {
        timeout: 120_000,
      });
      await this.docker(['start', initName]);
      await this.docker(['exec', '-u', '0', initName, 'sh', '-c', buildStrictEgressVolumeScript()]);
    } finally {
      try {
        cleanupFailure = await this.removeIgnoringNotFound(['rm', '-f', initName]);
      } finally {
        await rm(tempDir, { recursive: true, force: true });
      }
    }
    if (cleanupFailure) {
      throw new Error(
        `Failed to remove strict egress staging container '${initName}': ${cleanupFailure}`
      );
    }
  }

  private async initializeProxyBudgetLedger(
    initName: string,
    tlsVolume: string,
    budgetVolume: string,
    imageRef: string,
    ownerRunId?: string
  ): Promise<void> {
    await this.docker([
      'run',
      '--rm',
      '--name',
      initName,
      '--label',
      `${CONTAINER_LABELS.managed}=true`,
      '--label',
      `${CONTAINER_LABELS.envId}=${initName}`,
      ...ownerRunLabelArgs(ownerRunId),
      '--network',
      'none',
      '--user',
      AGENT_USER,
      '--read-only',
      '--memory',
      '128m',
      '--cpus',
      CONTROLLER_HELPER_CPUS,
      '--pids-limit',
      '64',
      '--cap-drop',
      'ALL',
      '--security-opt',
      'no-new-privileges',
      '-v',
      `${tlsVolume}:${PROXY_PRIVATE_ROOT}:ro`,
      '-v',
      `${budgetVolume}:${PROXY_BUDGET_ROOT}`,
      '--entrypoint',
      '/usr/local/bin/bun',
      imageRef,
      PROXY_BUDGET_LEDGER_CLI,
      '--create',
    ]);
  }

  private async waitForProxyReady(proxyContainerName: string): Promise<void> {
    const deadline = Date.now() + READY_TIMEOUT_MS;
    while (Date.now() < deadline) {
      try {
        await this.probeStrictProxy(proxyContainerName);
        return;
      } catch {
        const state = await this.containerState(proxyContainerName);
        if (state === 'exited') {
          const logs = await this.containerLogs(proxyContainerName);
          throw new Error(`Egress proxy exited before becoming ready. Logs:\n${logs}`);
        }
        await sleep(READY_POLL_INTERVAL_MS);
      }
    }
    const logs = await this.containerLogs(proxyContainerName);
    throw new Error(
      `Egress proxy did not become ready within ${READY_TIMEOUT_MS}ms. Logs:\n${logs}`
    );
  }

  private async probeStrictProxy(proxyContainerName: string): Promise<void> {
    await this.docker(
      ['exec', proxyContainerName, 'node', '-e', buildStrictProxyProbeScript(EGRESS_SOCKET_PATH)],
      { timeout: 5_000 }
    );
  }

  private async describeContainer(nameOrId: string): Promise<'running' | 'stopped' | 'missing'> {
    try {
      const { stdout } = await this.docker(['inspect', '-f', '{{.State.Running}}', nameOrId]);
      return stdout.trim() === 'true' ? 'running' : 'stopped';
    } catch (err) {
      const detail = extractDockerError(err);
      if (/no such (object|container)/i.test(detail)) return 'missing';
      throw new Error(`Failed to inspect container '${nameOrId}': ${detail}`);
    }
  }

  private async getContainerId(nameOrId: string): Promise<string> {
    const { stdout } = await this.docker(['inspect', '-f', '{{.Id}}', nameOrId]);
    return stdout.trim();
  }

  private async volumeExists(volume: string): Promise<boolean> {
    try {
      await this.docker(['volume', 'inspect', volume]);
      return true;
    } catch (err) {
      const detail = extractDockerError(err);
      if (/no such volume/i.test(detail)) return false;
      throw new Error(`Failed to inspect volume '${volume}': ${detail}`);
    }
  }

  private async removeIgnoringNotFound(args: string[]): Promise<string | undefined> {
    try {
      await this.docker(args);
      return undefined;
    } catch (err) {
      const detail = extractDockerError(err);
      if (/no such (container|volume)/i.test(detail)) {
        log.debug({ args, detail }, 'isolation.container_destroy_already_gone');
        return undefined;
      }
      return detail;
    }
  }

  private async removeContainerAndVolumes(
    containerName: string,
    volumes: string[],
    seedName?: string,
    proxyContainerName?: string
  ): Promise<void> {
    const handles = [seedName, containerName, proxyContainerName].filter(isString);
    for (const handle of handles) {
      await this.docker(['rm', '-f', handle]).catch(err => {
        const detail = extractDockerError(err);
        if (!/no such container/i.test(detail)) {
          log.warn({ handle, detail }, 'isolation.container_prepare_container_cleanup_failed');
        }
      });
    }
    for (const volume of volumes) {
      await this.docker(['volume', 'rm', '-f', volume]).catch(err => {
        log.warn(
          { volume, detail: extractDockerError(err) },
          'isolation.container_prepare_volume_cleanup_failed'
        );
      });
    }
  }

  private async waitForReady(containerId: string): Promise<void> {
    const deadline = Date.now() + READY_TIMEOUT_MS;
    while (Date.now() < deadline) {
      try {
        await this.docker(['exec', containerId, 'test', '-f', READY_SENTINEL], { timeout: 5_000 });
        return;
      } catch {
        const state = await this.containerState(containerId);
        if (state === 'exited') {
          const logs = await this.containerLogs(containerId);
          throw new Error(`Container exited before becoming ready. Logs:\n${logs}`);
        }
        await sleep(READY_POLL_INTERVAL_MS);
      }
    }
    const logs = await this.containerLogs(containerId);
    throw new Error(`Container did not become ready within ${READY_TIMEOUT_MS}ms. Logs:\n${logs}`);
  }

  private async containerState(containerId: string): Promise<'running' | 'exited' | 'unknown'> {
    try {
      const { stdout } = await this.docker(['inspect', '-f', '{{.State.Status}}', containerId], {
        timeout: 5_000,
      });
      const status = stdout.trim();
      if (status === 'running') return 'running';
      if (status === 'exited' || status === 'dead') return 'exited';
      return 'unknown';
    } catch {
      return 'unknown';
    }
  }

  private async containerLogs(containerId: string): Promise<string> {
    try {
      const { stdout, stderr } = await this.docker(['logs', '--tail', '80', containerId], {
        timeout: 5_000,
      });
      return `${stdout}${stderr}`.trim();
    } catch (err) {
      return `failed to read logs: ${extractDockerError(err)}`;
    }
  }
}

function resolveSeed(req: BackendPrepareRequest): { path: string; allowGitMetadata: boolean } {
  if (req.seed?.kind === 'directory' && req.seed.path.trim().length > 0) {
    return { path: req.seed.path, allowGitMetadata: req.seed.allowGitMetadata === true };
  }
  throw new Error(
    'Hardened container isolation requires a controller-prepared committed-input seed directory. ' +
      'Refusing to copy the live workspace/root into the agent container.'
  );
}

function buildSeedValidationScript(root: string, allowGitMetadata = false): string {
  const quotedRoot = shellQuote(root);
  const rejectExpression = [
    ...(allowGitMetadata ? [] : ["-name '.git'"]),
    "-name '.env*'",
    "-name '.npmrc'",
    "-name '.netrc'",
    "-name '.aws'",
    "-name '.docker'",
    "-path '*/.config/gh'",
  ].join(' -o ');
  return [
    'set -eu',
    `normalize_root_access() {
      chown root:root "$1"
      chmod u+rwx "$1"
      find "$1" -type d -exec chown root:root {} \\; -exec chmod u+rwx {} \\;
      bad_owner="$(find "$1" -type d ! -user 0 -print -quit)"
      test -z "$bad_owner"
    }`,
    `normalize_root_access ${quotedRoot}`,
    `bad_seed="$(find ${quotedRoot} \\( -type l -o \\( ${rejectExpression} \\) \\) -print -quit)"`,
    'if [ -n "$bad_seed" ]; then echo \'seed contains forbidden credential, link or VCS path\' >&2; exit 1; fi',
    `chown -R ${AGENT_USER}:${AGENT_USER} ${quotedRoot}`,
    `if [ -d ${quotedRoot}/.archon/controller-origins ]; then normalize_root_access ${quotedRoot}/.archon/controller-origins; chown -R root:root ${quotedRoot}/.archon/controller-origins; chmod -R a-w,a+rX ${quotedRoot}/.archon/controller-origins; fi`,
  ].join('\n');
}

function requireHardenedMetadata(
  envId: string,
  meta: Partial<ContainerEnvMetadata>
): ContainerEnvMetadata {
  const reason = validateHardenedMetadata(meta);
  if (!reason) return meta as ContainerEnvMetadata;
  throw new Error(
    `Cannot use container env '${envId}': its hardened metadata is invalid (${reason}). ` +
      'Legacy or tampered metadata is diagnostic-only; start a fresh --container run.'
  );
}

function validateHardenedMetadata(meta: Partial<ContainerEnvMetadata>): string | undefined {
  if (meta.profile !== 'hardened') return 'missing hardened profile';
  if (meta.isolationMode !== 'hardened') return 'missing hardened isolation mode';
  if (!isString(meta.resourceId)) return 'missing resource id';
  if (!isDockerToken(meta.resourceId)) return 'invalid resource id';
  if (!isString(meta.image) || !/^sha256:[0-9a-f]{64}$/.test(meta.image))
    return 'missing immutable image id';
  if (!isString(meta.workspacePath)) return 'missing workspace path';
  if (meta.agentArtifactsDir !== AGENT_ARTIFACTS_DIR) return 'unexpected artifact root';
  const expected = expectedManagedNames(meta.resourceId);
  if (meta.containerName !== expected.containerName) return 'unexpected container name';
  if (!isString(meta.containerId)) return 'missing container id';
  if (meta.containerId !== expected.containerName && !isDockerToken(meta.containerId))
    return 'invalid container id';
  if (meta.ownerRunId !== undefined && !isDockerToken(meta.ownerRunId))
    return 'invalid owner run id';
  if (meta.volume !== expected.workspaceVolume) return 'unexpected legacy volume alias';
  if (meta.workspaceVolume !== expected.workspaceVolume) return 'unexpected workspace volume';
  if (meta.homeVolume !== expected.homeVolume) return 'unexpected home volume';
  if (meta.artifactsVolume !== expected.artifactsVolume) return 'unexpected artifact volume';
  const resourceLimitError = validateResourceLimits(meta.resourceLimits);
  if (resourceLimitError) return resourceLimitError;
  return validateEgressMetadata(meta, expected);
}

function validateResourceLimits(limits: unknown): string | undefined {
  if (!limits || typeof limits !== 'object') return 'missing frozen resource limits';
  const record = limits as Partial<ContainerResourceLimits>;
  if (record.policyVersion !== RESOURCE_LIMIT_POLICY_VERSION)
    return 'unsupported resource limit policy';
  if (record.agentCpus !== AGENT_CPUS) return 'unexpected agent cpu limit';
  if (record.controllerHelperCpus !== CONTROLLER_HELPER_CPUS)
    return 'unexpected controller helper cpu limit';
  if (!Number.isInteger(record.memoryMb) || (record.memoryMb ?? 0) <= 0)
    return 'invalid frozen memory limit';
  if (!Number.isInteger(record.pidsLimit) || (record.pidsLimit ?? 0) <= 0)
    return 'invalid frozen process limit';
  return undefined;
}

function validateEgressMetadata(
  meta: Partial<ContainerEnvMetadata>,
  expected: ReturnType<typeof expectedManagedNames>
): string | undefined {
  const anyEgress = Boolean(
    meta.egressVolume ||
    meta.tlsVolume ||
    meta.tlsValidUntil ||
    meta.proxyContainerName ||
    meta.budgetVolume ||
    meta.budgetPolicyDigest ||
    meta.proxyBudgetSeedDigest ||
    meta.egressPolicyB64
  );
  if (!anyEgress) return undefined;
  if (meta.egressVolume !== expected.egressVolume) return 'unexpected egress volume';
  if (meta.tlsVolume !== expected.tlsVolume) return 'unexpected TLS material volume';
  if (!isValidIsoDate(meta.tlsValidUntil)) return 'invalid TLS expiry timestamp';
  if (meta.proxyContainerName !== expected.proxyContainerName) return 'unexpected proxy name';
  if (meta.budgetVolume !== expected.budgetVolume) return 'unexpected proxy budget volume';
  if (!isString(meta.budgetPolicyDigest)) return 'missing proxy budget policy digest';
  if (!isString(meta.proxyBudgetSeedDigest)) return 'missing proxy budget seed digest';
  if (!isString(meta.egressPolicyB64)) return 'missing frozen egress policy';
  try {
    decodeStrictEgressPolicy(meta.egressPolicyB64);
  } catch {
    return 'invalid frozen strict egress policy';
  }
  return undefined;
}

function expectedManagedNames(resourceId: string): {
  containerName: string;
  workspaceVolume: string;
  homeVolume: string;
  artifactsVolume: string;
  egressVolume: string;
  tlsVolume: string;
  proxyContainerName: string;
  budgetVolume: string;
} {
  const containerName = `archon-${resourceId}`;
  return {
    containerName,
    workspaceVolume: `${containerName}-workspace`,
    homeVolume: `${containerName}-home`,
    artifactsVolume: `${containerName}-artifacts`,
    egressVolume: `${containerName}-egress`,
    tlsVolume: `${containerName}-egress-tls`,
    proxyContainerName: `${containerName}-egress-proxy`,
    budgetVolume: `${containerName}-budget`,
  };
}

function collectVolumes(meta: ContainerEnvMetadata): string[] {
  return [
    meta.workspaceVolume,
    meta.homeVolume,
    meta.artifactsVolume,
    meta.egressVolume,
    meta.tlsVolume,
    meta.budgetVolume,
  ].filter(isString);
}

function hasVolumeMount(
  mounts: unknown,
  name: string,
  destination: string,
  writable: boolean
): boolean {
  if (!Array.isArray(mounts)) return false;
  return mounts.some(mount => {
    if (!mount || typeof mount !== 'object') return false;
    const record = mount as Record<string, unknown>;
    return (
      record.Type === 'volume' &&
      record.Name === name &&
      record.Destination === destination &&
      record.RW === writable
    );
  });
}

function assertHardenedContainerConfig(config: ContainerBackendConfig): void {
  if (config.network !== 'none') {
    throw new Error(
      `Unsupported hardened container network '${config.network}': agent containers must use network none.`
    );
  }
  if (!Number.isInteger(config.memoryMb) || config.memoryMb <= 0) {
    throw new Error(
      'Unsupported hardened container memory limit: memoryMb must be a positive integer.'
    );
  }
  if (!Number.isInteger(config.pidsLimit) || config.pidsLimit <= 0) {
    throw new Error(
      'Unsupported hardened container process limit: pidsLimit must be a positive integer.'
    );
  }
}

function buildStrictEgressVolumeScript(): string {
  return [
    'set -eu',
    `chown root:root ${PROXY_PRIVATE_ROOT}`,
    `chmod 0700 ${PROXY_PRIVATE_ROOT}`,
    `chown root:root ${PROXY_POLICY_PATH} ${PROXY_LEAF_KEY_PATH} ${PROXY_LEAF_CERT_PATH} ${PROXY_CA_CERT_PATH} ${PROXY_BUDGET_GRANT_PATH} ${PROXY_PROVIDER_POLICIES_PATH}`,
    `chmod 0400 ${PROXY_POLICY_PATH} ${PROXY_LEAF_KEY_PATH} ${PROXY_LEAF_CERT_PATH} ${PROXY_CA_CERT_PATH} ${PROXY_BUDGET_GRANT_PATH} ${PROXY_PROVIDER_POLICIES_PATH}`,
    `chown root:root ${EGRESS_SOCKET_ROOT}`,
    `chmod 0700 ${EGRESS_SOCKET_ROOT}`,
    `install -o root -g root -m 0444 ${PROXY_CA_CERT_PATH} ${EGRESS_PUBLIC_CA_PATH}`,
    `chmod 0500 ${PROXY_PRIVATE_ROOT}`,
    `chown ${AGENT_USER}:${AGENT_USER} ${PROXY_POLICY_PATH} ${PROXY_LEAF_KEY_PATH} ${PROXY_LEAF_CERT_PATH} ${PROXY_CA_CERT_PATH} ${PROXY_BUDGET_GRANT_PATH} ${PROXY_PROVIDER_POLICIES_PATH}`,
    `chown ${AGENT_USER}:${AGENT_USER} ${PROXY_PRIVATE_ROOT}`,
    `chown root:root ${PROXY_BUDGET_ROOT}`,
    `chmod 0700 ${PROXY_BUDGET_ROOT}`,
    `chown ${AGENT_USER}:${AGENT_USER} ${PROXY_BUDGET_ROOT}`,
    `chmod 0770 ${EGRESS_SOCKET_ROOT}`,
    `chown ${AGENT_USER}:${AGENT_USER} ${EGRESS_SOCKET_ROOT}`,
  ].join('\n');
}

function buildStrictProxyProbeScript(socketPath: string): string {
  return [
    "const net = require('node:net');",
    `const client = net.createConnection(${JSON.stringify(socketPath)});`,
    "const request = 'CONNECT archon-readiness.invalid:443 HTTP/1.1\\r\\nHost: archon-readiness.invalid:443\\r\\n\\r\\n';",
    "let data = '';",
    'const timer = setTimeout(() => { client.destroy(); process.exit(1); }, 3000);',
    "client.on('connect', () => client.write(request));",
    "client.on('data', chunk => { data += chunk.toString('utf8'); if (data.includes('\\r\\n')) client.end(); });",
    "client.on('error', () => { clearTimeout(timer); process.exit(1); });",
    "client.on('close', () => { clearTimeout(timer); process.exit(/^HTTP\\/1\\.1 (400|403)\\b/.test(data) ? 0 : 1); });",
  ].join('\n');
}

function normalizeStrictEgressResumeMetadata(input: {
  proxyContainerName: string | undefined;
  tlsVolume: string | undefined;
  egressVolume: string | undefined;
  budgetVolume: string | undefined;
  tlsValidUntil: string | undefined;
  policyB64: string | undefined;
  budgetPolicyDigest: string | undefined;
  proxyBudgetSeedDigest: string | undefined;
}): StrictEgressResumeMetadata | undefined {
  const values = Object.values(input);
  if (values.every(value => value === undefined)) return undefined;
  if (!values.every(isString)) {
    throw new Error('Cannot resume strict egress: frozen proxy metadata is incomplete.');
  }
  const metadata = input as StrictEgressResumeMetadata & { proxyBudgetSeedDigest: string };
  if (!/^[0-9a-f]{64}$/i.test(metadata.proxyBudgetSeedDigest)) {
    throw new Error('Cannot resume strict egress: proxy budget seed digest is invalid.');
  }
  return metadata;
}

function assertTlsStillValid(value: string): void {
  if (!isValidIsoDate(value) || Date.parse(value) <= Date.now()) {
    throw new Error('Cannot resume strict egress: TLS material is expired.');
  }
}

function isValidIsoDate(value: unknown): value is string {
  if (typeof value !== 'string') return false;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function resolveOwnerRunId(ownerRunId: string | undefined): string | undefined {
  if (ownerRunId === undefined) return undefined;
  if (!isDockerToken(ownerRunId)) {
    throw new Error(`Invalid ownerRunId '${ownerRunId}': expected a Docker label-safe run id.`);
  }
  return ownerRunId;
}

function ownerRunLabelArgs(ownerRunId: string | undefined): string[] {
  if (!ownerRunId) return [];
  return ['--label', `${CONTAINER_LABELS.ownerRunId}=${ownerRunId}`];
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function agentEgressArgs(egressVolume: string | undefined): string[] {
  if (!egressVolume) return [];
  return [
    '-v',
    `${egressVolume}:${EGRESS_SOCKET_ROOT}:ro`,
    '-e',
    `ARCHON_PROXY_SOCKET=${EGRESS_SOCKET_PATH}`,
    '-e',
    `ARCHON_PROXY_PORT=${AGENT_PROXY_PORT}`,
    '-e',
    `HTTP_PROXY=http://127.0.0.1:${AGENT_PROXY_PORT}`,
    '-e',
    `HTTPS_PROXY=http://127.0.0.1:${AGENT_PROXY_PORT}`,
    '-e',
    `NODE_EXTRA_CA_CERTS=${EGRESS_PUBLIC_CA_PATH}`,
    '-e',
    `CODEX_CA_CERTIFICATE=${EGRESS_PUBLIC_CA_PATH}`,
    '-e',
    `SSL_CERT_FILE=${EGRESS_PUBLIC_CA_PATH}`,
    '-e',
    'NO_PROXY=localhost,127.0.0.1,::1',
  ];
}

function isString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isDockerToken(value: unknown): value is string {
  return typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value);
}

function assertSingleRunProxyBudgetGrant(
  grant: ProxyBudgetGrant
): asserts grant is ProxyBudgetGrantV1 {
  if (isProxyBudgetGrantV2(grant)) {
    throw new Error('Proxy budget shared-chain status is not wired.');
  }
}

function parseProxyBudgetStatusOutput(stdout: string): BudgetStatus {
  const lines = stdout
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean);
  if (lines.length !== 1) {
    throw new Error('Proxy budget status output is malformed.');
  }
  const envelope = JSON.parse(lines[0]) as Record<string, unknown>;
  if (envelope.ok !== true || envelope.event !== 'status') {
    throw new Error('Proxy budget status command failed.');
  }
  return readBudgetStatus(envelope.result);
}

function normalizeVerifiedProxyBudgetStatus(
  envId: string,
  status: BudgetStatus,
  expectedGrant: ProxyBudgetGrant
): VerifiedProxyBudgetStatus {
  assertSameGrant(status.grant, expectedGrant);
  assertSingleRunProxyBudgetGrant(status.grant);
  const statusGrant = status.grant;
  if (status.pendingReservations > 0 || status.unknownReservations > 0) {
    throw new Error('Proxy budget ledger has pending or unknown reservations.');
  }
  if (!status.acceptingReservations) {
    throw new Error('Proxy budget ledger is not accepting exact reservations.');
  }
  return {
    source: 'controller-proxy-ledger',
    envId,
    grant: { ...statusGrant },
    consumed: {
      input: status.consumedInputTokens,
      output: status.consumedOutputTokens,
    },
    pendingReservations: status.pendingReservations,
    unknownReservations: status.unknownReservations,
    acceptingReservations: status.acceptingReservations,
  };
}

function readBudgetStatus(value: unknown): BudgetStatus {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Proxy budget status result is malformed.');
  }
  const status = value as BudgetStatus;
  const grant = readBudgetGrant(status.grant);
  const consumedInputTokens = readSafeCount(status.consumedInputTokens, 'consumed input tokens');
  const consumedOutputTokens = readSafeCount(status.consumedOutputTokens, 'consumed output tokens');
  const consumedTotalTokens = readSafeCount(status.consumedTotalTokens, 'consumed total tokens');
  const expectedTotal = consumedInputTokens + consumedOutputTokens;
  if (!Number.isSafeInteger(expectedTotal) || expectedTotal !== consumedTotalTokens) {
    throw new Error('Proxy budget status totals are malformed.');
  }
  return {
    grant,
    pendingReservations: readSafeCount(status.pendingReservations, 'pending reservations'),
    unknownReservations: readSafeCount(status.unknownReservations, 'unknown reservations'),
    consumedInputTokens,
    consumedOutputTokens,
    consumedTotalTokens,
    remainingInputTokens: readSafeCount(status.remainingInputTokens, 'remaining input tokens'),
    remainingOutputTokens: readSafeCount(status.remainingOutputTokens, 'remaining output tokens'),
    remainingTotalTokens: readSafeCount(status.remainingTotalTokens, 'remaining total tokens'),
    acceptingReservations: readBooleanField(status.acceptingReservations, 'accepting reservations'),
  };
}

function readBudgetGrant(value: unknown): ProxyBudgetGrantV1 {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Proxy budget status grant is malformed.');
  }
  const grant = value as ProxyBudgetGrant;
  if (grant.schema !== 'archon.proxy-budget-grant.v1') {
    throw new Error('Proxy budget status grant is malformed.');
  }
  return {
    schema: grant.schema,
    rootChainId: readStringField(grant.rootChainId, 'root chain id'),
    runId: readStringField(grant.runId, 'run id'),
    workflowDigest: readStringField(grant.workflowDigest, 'workflow digest'),
    policyDigest: readStringField(grant.policyDigest, 'policy digest'),
    deadlineEpochMs: readPositiveSafeCount(grant.deadlineEpochMs, 'deadline'),
    inputTokenLimit: readSafeCount(grant.inputTokenLimit, 'input token limit'),
    outputTokenLimit: readSafeCount(grant.outputTokenLimit, 'output token limit'),
    totalTokenLimit: readSafeCount(grant.totalTokenLimit, 'total token limit'),
  };
}

function assertSameGrant(actual: ProxyBudgetGrant, expected: ProxyBudgetGrant): void {
  assertSingleRunProxyBudgetGrant(actual);
  assertSingleRunProxyBudgetGrant(expected);
  if (
    actual.schema !== expected.schema ||
    actual.rootChainId !== expected.rootChainId ||
    actual.runId !== expected.runId ||
    actual.workflowDigest !== expected.workflowDigest ||
    actual.policyDigest !== expected.policyDigest ||
    actual.deadlineEpochMs !== expected.deadlineEpochMs ||
    actual.inputTokenLimit !== expected.inputTokenLimit ||
    actual.outputTokenLimit !== expected.outputTokenLimit ||
    actual.totalTokenLimit !== expected.totalTokenLimit
  ) {
    throw new Error('Proxy budget status grant binding drifted.');
  }
}

function readStringField(value: unknown, label: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`Proxy budget status ${label} is malformed.`);
  }
  return value;
}

function readBooleanField(value: unknown, label: string): boolean {
  if (typeof value !== 'boolean') {
    throw new Error(`Proxy budget status ${label} is malformed.`);
  }
  return value;
}

function readSafeCount(value: unknown, label: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Proxy budget status ${label} is malformed.`);
  }
  return value;
}

function readPositiveSafeCount(value: unknown, label: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Proxy budget status ${label} is malformed.`);
  }
  return value;
}
