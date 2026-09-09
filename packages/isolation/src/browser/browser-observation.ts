import { createHash, randomUUID } from 'crypto';
import { inflateSync } from 'zlib';
import { readdirSync, type Dirent } from 'fs';
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { basename, dirname, join, posix as posixPath, resolve } from 'path';
import effectiveSeccompProfileAsset from '../../docker/playwright-seccomp.no-io-uring.chroot.json';
import {
  snapshotContainerArtifacts,
  type ArtifactSnapshotResult,
} from '../container/artifact-snapshot';
import { dockerCli, extractDockerError, type DockerRunner } from '../container/docker-exec';

export const PLAYWRIGHT_VERSION = '1.60.0';
export const PLAYWRIGHT_IMAGE = 'mcr.microsoft.com/playwright:v1.60.0-noble';
export const PLAYWRIGHT_UPSTREAM_SECCOMP_PROFILE_DIGEST =
  'cc3e61cabda6bbc1e53e54d27ba4d55a9d3be829b6dd1a596f4a7b31b1cc7849';
export const PLAYWRIGHT_EFFECTIVE_SECCOMP_PROFILE_DIGEST =
  '153cb94e0bb74823af2e2e4e8548fc5a0895639e4bf16fa98ecac258b5641019';

const LABEL_COMMIT = 'org.opencontainers.image.revision';
const LABEL_TREE = 'archon.goodword.tree';
const OWNER_LABEL = 'archon.browser-observation.owner';
const SAFE_TOKEN = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,120}$/;
const HEX40 = /^[0-9a-f]{40}$/i;
const HEX64 = /^[0-9a-f]{64}$/i;
const IMAGE_ID = /^sha256:[0-9a-f]{64}$/i;
const ASSERTION_TYPES = new Set(['text', 'selector', 'url', 'title', 'testid', 'click', 'fill']);
const STATIC_WEB_PROFILE = 'static-web-http-v1';
const MAX_CANDIDATE_FILES = 1000;
const MAX_CANDIDATE_FILE_BYTES = 16 * 1024 * 1024;
const MAX_CANDIDATE_TOTAL_BYTES = 64 * 1024 * 1024;
const BASE64_BYTES = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

export interface BrowserAssertion {
  type: string;
  value: string;
  text?: string;
}

export interface BrowserCriterion {
  id: string;
  criterion: string;
  path: string;
  assertions: BrowserAssertion[];
}

export interface BrowserPolicy {
  required: BrowserCriterion[];
}

export interface CandidateSourceFile {
  type: 'file';
  path: string;
  size: number;
  sha256: string;
  contentBase64: string;
}

export interface CandidateSourceDescriptor {
  profile: typeof STATIC_WEB_PROFILE;
  commit: string;
  tree: string;
  appRoot: string;
  contentDigest: string;
  files: CandidateSourceFile[];
}

export interface ApplicationDescriptor {
  image: string;
  commit: string;
  tree: string;
  port: number;
  command?: string[];
  candidateSource?: CandidateSourceDescriptor;
}

export interface BrowserObservationRequest {
  runId: string;
  app: ApplicationDescriptor;
  policy: BrowserPolicy;
  playwrightNodeModules: string;
  evidenceDir: string;
  verifierImage?: string;
  viewport?: { width: number; height: number };
  timeoutMs?: number;
  totalTimeoutMs?: number;
  signal?: AbortSignal;
  canaryUrl?: string;
}

export interface BrowserObservationServiceOptions {
  trustedCandidateHelperImage?: string;
  trustedVerifierImage?: string;
}

interface NormalizedCandidateSourceFile extends Omit<CandidateSourceFile, 'contentBase64'> {
  bytes: Buffer;
}

interface NormalizedCandidateSourceDescriptor extends Omit<CandidateSourceDescriptor, 'files'> {
  files: NormalizedCandidateSourceFile[];
}

interface NormalizedApplicationDescriptor extends Omit<ApplicationDescriptor, 'candidateSource'> {
  command: string[];
  candidateSource?: NormalizedCandidateSourceDescriptor;
}

interface NormalizedBrowserObservationRequest extends Omit<
  Required<BrowserObservationRequest>,
  'app' | 'signal'
> {
  app: NormalizedApplicationDescriptor;
  playwrightNodeModules: string;
  evidenceDir: string;
  signal?: AbortSignal;
}

export interface BrowserObservationResult {
  authority: 'none';
  status: 'passed' | 'failed';
  runId: string;
  policyDigest: string;
  observedOrigin: string;
  app: { image: string; imageId: string; commit: string; tree: string };
  verifier: { image: string; imageId: string; playwrightVersion: string };
  viewport: { width: number; height: number };
  criteria: ObservedCriterion[];
  security: SecurityObservation;
  evidence: { screenshots: ImportedEvidenceFile[]; traces: ImportedEvidenceFile[] };
}

export interface ObservedCriterion {
  id: string;
  path: string;
  status: 'passed' | 'failed' | 'skipped';
  observed_origin?: string;
  assertions?: ObservedAssertion[];
  error?: string;
}

export interface ObservedAssertion {
  type: string;
  value: string;
  status: 'passed' | 'failed' | 'skipped';
  error?: string;
}

export interface EvidenceFile {
  criterion?: string;
  path: string;
  sha256: string;
}

export interface ImportedEvidenceFile extends EvidenceFile {
  controllerPath: string;
}

interface RawObservation {
  criteria?: ObservedCriterion[];
  evidence?: { screenshots?: EvidenceFile[]; traces?: EvidenceFile[] };
  observedOrigin?: string;
  canary?: { ok: boolean; error?: string };
  fatal?: string;
  security?: SecurityObservation;
}

interface SecurityObservation {
  status: Record<string, string>;
  identity: { uid: number; gid: number };
  arch: string;
  controls: Record<
    string,
    {
      blocked: boolean;
      code: number | null;
      signal: string | null;
      error?: string;
      errno?: number;
      result?: number;
    }
  >;
}

interface DockerImageInfo {
  id: string;
  labels: Record<string, string>;
}

interface Resources {
  seccompProfilePath: string;
  podName: string;
  appName: string;
  verifierName: string;
  definitionsVolume: string;
  evidenceVolume: string;
  candidateVolume: string;
  resourceId: string;
  ownerId: string;
  labels: string[];
  containerIds: string[];
  verifierId?: string;
}

interface ObservationControl {
  deadlineAt: number;
  signal?: AbortSignal;
}

export class BrowserObservationService {
  private readonly trustedCandidateHelperImage: string | undefined;
  private readonly trustedVerifierImage: string | undefined;

  constructor(
    private readonly docker: DockerRunner = dockerCli,
    options: BrowserObservationServiceOptions = {}
  ) {
    this.trustedCandidateHelperImage = options.trustedCandidateHelperImage;
    this.trustedVerifierImage = options.trustedVerifierImage;
  }

  async observe(request: BrowserObservationRequest): Promise<BrowserObservationResult> {
    const startedAt = Date.now();
    throwIfAborted(request.signal);
    const normalized = await normalizeRequest(request, {
      trustedCandidateHelperImage: this.trustedCandidateHelperImage,
      trustedVerifierImage: this.trustedVerifierImage,
    });
    const control = observationControl(normalized, startedAt);
    const docker = controlledDockerRunner(this.docker, control);
    validatePolicy(normalized.policy);
    const policyDigest = canonicalDigest(normalized.policy);
    const appImage = await inspectApplicationImage(docker, normalized.app);
    const verifierImage = await inspectVerifierImage(docker, normalized.verifierImage);
    const tempDir = await mkdtemp(join(tmpdir(), 'archon-browser-observe-'));
    const resources = resourceNames(normalized.runId, await writeSeccompProfile(tempDir));
    const ownedDocker = ownedDockerRunner(docker, resources);

    let result: BrowserObservationResult;
    try {
      await this.createVolumes(ownedDocker, resources, Boolean(normalized.app.candidateSource));
      await this.initializeEvidenceVolume(ownedDocker, resources, verifierImage.id);
      await this.seedDefinitions(
        ownedDocker,
        tempDir,
        resources,
        normalized,
        verifierImage.id,
        policyDigest
      );
      await this.seedCandidateSource(ownedDocker, tempDir, resources, normalized.app, appImage.id);
      await this.verifyCandidateSourceVolume(
        ownedDocker,
        tempDir,
        resources,
        normalized.app,
        appImage.id
      );
      const podId = await this.startPod(ownedDocker, resources, verifierImage.id);
      await this.startApplication(ownedDocker, resources, podId, normalized.app, appImage.id);
      await controlledDelay(750, control);
      try {
        await this.runVerifier(ownedDocker, resources, podId, normalized, verifierImage.id);
      } catch (error) {
        const detail = await this.snapshotVerifierFailure(
          ownedDocker,
          resources,
          normalized.evidenceDir,
          verifierImage.id,
          error
        );
        throw new Error(`Browser verifier failed: ${detail}`);
      }
      const snapshot = await this.snapshotEvidence(
        ownedDocker,
        resources,
        normalized.evidenceDir,
        verifierImage.id
      );
      const raw = await readSnapshotJson(snapshot, 'raw-result.json');
      const digest = await readSnapshotText(snapshot, 'policy.sha256');
      if (digest.trim() !== policyDigest) {
        throw new Error('Verifier policy digest drifted during execution.');
      }
      const evidence = await this.importEvidence(snapshot, raw.evidence);
      result = finalizeResult(normalized, raw, evidence, policyDigest, appImage, verifierImage);
      remainingTimeout(control);
    } finally {
      await rm(tempDir, { recursive: true, force: true }).catch(() => undefined);
      await this.cleanup(resources);
    }
    remainingTimeout(control);
    return result;
  }

  private async createVolumes(
    docker: DockerRunner,
    resources: Resources,
    needsCandidateVolume: boolean
  ): Promise<void> {
    const volumes = [resources.definitionsVolume, resources.evidenceVolume];
    if (needsCandidateVolume) volumes.push(resources.candidateVolume);
    for (const volume of volumes) {
      await docker([
        'volume',
        'create',
        ...resources.labels.flatMap(label => ['--label', label]),
        volume,
      ]);
    }
  }

  private async initializeEvidenceVolume(
    docker: DockerRunner,
    resources: Resources,
    imageId: string
  ): Promise<void> {
    await docker([
      'run',
      '--rm',
      ...labelArgs(resources),
      ...containerHardening('none', '0:0'),
      '--cap-add',
      'CHOWN',
      '--memory',
      '128m',
      '--cpus',
      '0.25',
      '--pids-limit',
      '64',
      '--mount',
      `type=volume,src=${resources.evidenceVolume},dst=/evidence`,
      '--entrypoint',
      'sh',
      imageId,
      '-lc',
      'mkdir -p /evidence/browser-evidence && chown -R 1000:1000 /evidence',
    ]);
  }

  private async seedDefinitions(
    docker: DockerRunner,
    tempDir: string,
    resources: Resources,
    request: NormalizedBrowserObservationRequest,
    verifierImageId: string,
    policyDigest: string
  ): Promise<void> {
    await mkdir(tempDir, { recursive: true });
    const input = {
      artifacts: '/evidence',
      origin: `http://127.0.0.1:${request.app.port}`,
      viewport: request.viewport,
      timeoutMs: request.timeoutMs,
      required: request.policy.required.map(item => ({
        id: item.id,
        path: item.path,
        assertions: item.assertions,
      })),
      expectedOrigin: `http://127.0.0.1:${request.app.port}`,
      policyDigest,
      canaryUrl: request.canaryUrl,
    };
    const inputPath = join(tempDir, 'input.json');
    const runnerPath = join(tempDir, 'runner.cjs');
    await writeFile(inputPath, `${JSON.stringify(input, null, 2)}\n`, 'utf8');
    await writeFile(runnerPath, RUNNER_JS, 'utf8');

    const seedName = `${resources.verifierName}-seed`;
    const seedId = await this.createSeedContainer(docker, seedName, resources, verifierImageId);
    try {
      await docker(['start', seedId]);
      await docker(['exec', seedId, 'sh', '-lc', 'mkdir -p /defs/node_modules']);
      await docker(['cp', inputPath, `${seedId}:/defs/input.json`]);
      await docker(['cp', runnerPath, `${seedId}:/defs/runner.cjs`]);
      await docker([
        'cp',
        join(request.playwrightNodeModules, 'playwright'),
        `${seedId}:/defs/node_modules/.`,
      ]);
      await docker([
        'cp',
        join(request.playwrightNodeModules, 'playwright-core'),
        `${seedId}:/defs/node_modules/.`,
      ]);
    } finally {
      await this.removeOwnedContainer(seedId, resources.ownerId);
    }
  }

  private async createSeedContainer(
    docker: DockerRunner,
    seedName: string,
    resources: Resources,
    imageId: string
  ): Promise<string> {
    const result = await docker([
      'create',
      '--name',
      seedName,
      ...labelArgs(resources),
      ...containerHardening('none', '0:0'),
      '--memory',
      '256m',
      '--cpus',
      '0.25',
      '--pids-limit',
      '64',
      '--mount',
      `type=volume,src=${resources.definitionsVolume},dst=/defs`,
      '--entrypoint',
      'sh',
      imageId,
      '-lc',
      'mkdir -p /defs/node_modules && sleep 300',
    ]);
    return result.stdout.trim() || seedName;
  }

  private async seedCandidateSource(
    docker: DockerRunner,
    tempDir: string,
    resources: Resources,
    app: NormalizedApplicationDescriptor,
    imageId: string
  ): Promise<void> {
    if (!app.candidateSource) return;
    const candidateRoot = join(tempDir, 'candidate');
    await writeCandidateFiles(candidateRoot, app.candidateSource);
    const seedName = `${resources.appName}-candidate-seed`;
    const seedId = await this.createCandidateSeedContainer(docker, seedName, resources, imageId);
    try {
      await docker(['start', seedId]);
      await docker(['cp', `${candidateRoot}/.`, `${seedId}:/candidate`]);
    } finally {
      await this.removeOwnedContainer(seedId, resources.ownerId);
    }
  }

  private async verifyCandidateSourceVolume(
    docker: DockerRunner,
    tempDir: string,
    resources: Resources,
    app: NormalizedApplicationDescriptor,
    imageId: string
  ): Promise<void> {
    if (!app.candidateSource) return;
    const snapshot = await snapshotContainerArtifacts(
      docker,
      {
        workspaceVolume: resources.candidateVolume,
        image: imageId,
        resourceId: resources.resourceId,
        readerProfile: 'node-1000',
      },
      {
        destinationDir: join(tempDir, 'candidate-snapshot'),
        maxTotalBytes: MAX_CANDIDATE_TOTAL_BYTES,
        maxFileBytes: MAX_CANDIDATE_FILE_BYTES,
        maxFiles: MAX_CANDIDATE_FILES,
      }
    );
    validateCandidateSnapshot(app.candidateSource, snapshot);
  }

  private async createCandidateSeedContainer(
    docker: DockerRunner,
    seedName: string,
    resources: Resources,
    imageId: string
  ): Promise<string> {
    const result = await docker([
      'create',
      '--name',
      seedName,
      ...labelArgs(resources),
      ...containerHardening('none', '0:0'),
      '--memory',
      '256m',
      '--cpus',
      '0.25',
      '--pids-limit',
      '64',
      '--mount',
      `type=volume,src=${resources.candidateVolume},dst=/candidate`,
      '--entrypoint',
      'sh',
      imageId,
      '-lc',
      'mkdir -p /candidate && sleep 300',
    ]);
    return result.stdout.trim() || seedName;
  }

  private async startPod(
    docker: DockerRunner,
    resources: Resources,
    imageId: string
  ): Promise<string> {
    const result = await docker([
      'run',
      '-d',
      '--name',
      resources.podName,
      ...labelArgs(resources),
      ...containerHardening('none'),
      '--memory',
      '64m',
      '--cpus',
      '0.25',
      '--pids-limit',
      '64',
      '--entrypoint',
      'sleep',
      imageId,
      '300',
    ]);
    const id = result.stdout.trim() || resources.podName;
    return id;
  }

  private async startApplication(
    docker: DockerRunner,
    resources: Resources,
    podId: string,
    app: NormalizedApplicationDescriptor,
    appImageId: string
  ): Promise<void> {
    await docker([
      'run',
      '-d',
      '--name',
      resources.appName,
      ...labelArgs(resources),
      ...containerHardening(`container:${podId}`),
      '--memory',
      '512m',
      '--cpus',
      '1',
      '--pids-limit',
      '256',
      ...candidateEntrypointArgs(app),
      ...candidateMountArgs(resources, app),
      appImageId,
      ...app.command,
    ]);
  }

  private async runVerifier(
    docker: DockerRunner,
    resources: Resources,
    podId: string,
    request: NormalizedBrowserObservationRequest,
    verifierImageId: string
  ): Promise<void> {
    const verifierId = await this.createVerifierContainer(
      docker,
      resources,
      podId,
      verifierImageId
    );
    resources.verifierId = verifierId;
    await docker(['start', '-a', verifierId], {
      timeout: Math.max(60_000, request.timeoutMs * request.policy.required.length + 30_000),
    });
  }

  private async createVerifierContainer(
    docker: DockerRunner,
    resources: Resources,
    podId: string,
    verifierImageId: string
  ): Promise<string> {
    const result = await docker([
      'create',
      '--name',
      resources.verifierName,
      ...labelArgs(resources),
      ...containerHardening(`container:${podId}`, '1000:1000', resources.seccompProfilePath),
      '--memory',
      '768m',
      '--cpus',
      '1',
      '--pids-limit',
      '256',
      '--mount',
      `type=volume,src=${resources.definitionsVolume},dst=/defs,readonly`,
      '--mount',
      `type=volume,src=${resources.evidenceVolume},dst=/evidence`,
      '--entrypoint',
      'node',
      verifierImageId,
      '/defs/runner.cjs',
      '/defs/input.json',
      '/evidence/raw-result.json',
    ]);
    return result.stdout.trim() || resources.verifierName;
  }

  private async snapshotVerifierFailure(
    docker: DockerRunner,
    resources: Resources,
    evidenceDir: string,
    imageId: string,
    error: unknown
  ): Promise<string> {
    if (!resources.verifierId) return extractDockerError(error);
    await this.removeOwnedContainer(resources.verifierId, resources.ownerId);
    const snapshot = await this.snapshotEvidence(docker, resources, evidenceDir, imageId).catch(
      () => undefined
    );
    if (!snapshot) return extractDockerError(error);
    const raw = await readSnapshotText(snapshot, 'raw-result.json').catch(() => '');
    return raw.trim() || extractDockerError(error);
  }

  private async snapshotEvidence(
    docker: DockerRunner,
    resources: Resources,
    evidenceDir: string,
    imageId: string
  ): Promise<ArtifactSnapshotResult> {
    return snapshotContainerArtifacts(
      docker,
      {
        workspaceVolume: resources.evidenceVolume,
        image: imageId,
        resourceId: resources.resourceId,
        readerProfile: 'node-1000',
      },
      {
        destinationDir: evidenceDir,
        maxTotalBytes: 64 * 1024 * 1024,
        maxFileBytes: 16 * 1024 * 1024,
        maxFiles: 1000,
      }
    );
  }

  private async importEvidence(
    snapshot: ArtifactSnapshotResult,
    evidence: RawObservation['evidence']
  ): Promise<BrowserObservationResult['evidence']> {
    const normalized = normalizeEvidence(evidence);
    const imported = new Map(snapshot.files.map(file => [file.path, file]));
    for (const entry of [...normalized.screenshots, ...normalized.traces]) {
      const file = imported.get(entry.path);
      if (!file) throw new Error(`Verifier evidence file was not imported: ${entry.path}.`);
      if (file.sha256 !== entry.sha256) {
        throw new Error(`Imported browser evidence digest mismatch for ${entry.path}.`);
      }
      const controllerPath = join(snapshot.snapshotDir, entry.path);
      await validateImportedEvidenceBytes(controllerPath, entry.path);
      entry.controllerPath = controllerPath;
    }
    return normalized;
  }

  private async cleanup(resources: Resources): Promise<void> {
    const failures: string[] = [];
    for (const id of [...resources.containerIds].reverse()) {
      await this.removeOwnedContainer(id, resources.ownerId).catch(error =>
        failures.push(extractDockerError(error))
      );
    }
    for (const volume of [
      resources.definitionsVolume,
      resources.evidenceVolume,
      resources.candidateVolume,
    ]) {
      await this.removeOwnedVolume(volume, resources.ownerId).catch(error =>
        failures.push(extractDockerError(error))
      );
    }
    if (failures.length > 0) {
      throw new Error(`Browser observation cleanup failed: ${failures.join('; ')}`);
    }
  }

  private async removeOwnedContainer(id: string, expectedOwner: string): Promise<void> {
    const owner = await inspectContainerOwner(this.docker, id).catch(error => {
      if (/no such (container|object):/i.test(extractDockerError(error))) return undefined;
      throw error;
    });
    if (owner === undefined) return;
    if (!owner) throw new Error(`Refusing to remove unlabeled container ${id}.`);
    if (owner !== expectedOwner) throw new Error(`Refusing to remove non-owned container ${id}.`);
    await this.docker(['rm', '-f', id]);
  }

  private async removeOwnedVolume(name: string, expectedOwner: string): Promise<void> {
    const owner = await inspectVolumeOwner(this.docker, name).catch(error => {
      if (/no such volume(?::|$)/i.test(extractDockerError(error))) return undefined;
      throw error;
    });
    if (owner === undefined) return;
    if (!owner) throw new Error(`Refusing to remove unlabeled volume ${name}.`);
    if (owner !== expectedOwner) throw new Error(`Refusing to remove non-owned volume ${name}.`);
    await this.docker(['volume', 'rm', '-f', name]);
  }
}

async function normalizeRequest(
  request: BrowserObservationRequest,
  options: BrowserObservationServiceOptions
): Promise<NormalizedBrowserObservationRequest> {
  const viewport = request.viewport ?? { width: 1280, height: 720 };
  const playwrightNodeModules = resolve(request.playwrightNodeModules);
  const evidenceDir = resolve(request.evidenceDir);
  await validatePlaywrightNodeModules(playwrightNodeModules);
  const timeoutMs = positiveMilliseconds('timeoutMs', request.timeoutMs ?? 10_000);
  const policySize = Math.max(1, request.policy.required.length);
  const totalTimeoutMs = positiveMilliseconds(
    'totalTimeoutMs',
    request.totalTimeoutMs ?? Math.max(90_000, timeoutMs * policySize + 45_000)
  );
  const candidateSource = normalizeCandidateSource(request.app, options);
  const verifierImage = normalizeVerifierImage(request.verifierImage, options.trustedVerifierImage);
  return {
    ...request,
    playwrightNodeModules,
    evidenceDir,
    verifierImage,
    timeoutMs,
    totalTimeoutMs,
    viewport,
    canaryUrl: request.canaryUrl ?? 'http://host.docker.internal/',
    app: {
      ...request.app,
      candidateSource,
      command: normalizedAppCommand(request.app, candidateSource),
    },
  };
}

function normalizeVerifierImage(
  image: string | undefined,
  trustedVerifierImage: string | undefined
): string {
  const resolved = image ?? PLAYWRIGHT_IMAGE;
  if (resolved === PLAYWRIGHT_IMAGE) return resolved;
  if (!trustedVerifierImage) {
    throw new Error('Custom verifier image requires a controller-trusted verifier image.');
  }
  if (!isImmutableImageRef(trustedVerifierImage)) {
    throw new Error('Controller-trusted verifier image must be immutable.');
  }
  if (resolved !== trustedVerifierImage) {
    throw new Error('Verifier image does not match controller-trusted verifier image.');
  }
  return resolved;
}

function normalizedAppCommand(
  app: ApplicationDescriptor,
  candidateSource: NormalizedCandidateSourceDescriptor | undefined
): string[] {
  if (candidateSource) return staticWebCommand(app.port, candidateSource.appRoot);
  return app.command ?? ['sh', '-lc', `python3 -m http.server ${app.port} --bind 127.0.0.1`];
}

function normalizeCandidateSource(
  app: ApplicationDescriptor,
  options: BrowserObservationServiceOptions
): NormalizedCandidateSourceDescriptor | undefined {
  const source = app.candidateSource;
  if (!source) return undefined;
  validateTrustedCandidateHelperImage(app, options.trustedCandidateHelperImage);
  if (app.command) throw new Error('Candidate source profile uses a fixed controller command.');
  validateCandidateBinding(app, source);
  const appRoot = normalizeCandidatePath(source.appRoot, 'appRoot', { directory: true });
  const files = normalizeCandidateFiles(source.files, appRoot);
  const contentDigest = candidateContentDigest(source, appRoot, files);
  if (contentDigest !== source.contentDigest) {
    throw new Error('Candidate source content digest does not match the file manifest.');
  }
  return { ...source, appRoot, files };
}

function validateTrustedCandidateHelperImage(
  app: ApplicationDescriptor,
  trustedImage: string | undefined
): void {
  if (!trustedImage) {
    throw new Error('Candidate source mode requires a controller-trusted helper image.');
  }
  if (!isImmutableImageRef(trustedImage)) {
    throw new Error('Controller-trusted candidate helper image must be immutable.');
  }
  if (app.image !== trustedImage) {
    throw new Error('Candidate source app image does not match controller-trusted helper image.');
  }
}

function validateCandidateBinding(
  app: ApplicationDescriptor,
  source: CandidateSourceDescriptor
): void {
  if (source.profile !== STATIC_WEB_PROFILE) {
    throw new Error(`Unsupported candidate source profile '${source.profile}'.`);
  }
  if (!HEX40.test(source.commit) || !HEX40.test(source.tree)) {
    throw new Error('Candidate source commit/tree must be 40-character git object IDs.');
  }
  if (source.commit !== app.commit || source.tree !== app.tree) {
    throw new Error('Candidate source commit/tree binding does not match the app descriptor.');
  }
  if (!HEX64.test(source.contentDigest)) {
    throw new Error('Candidate source content digest must be sha256 hex.');
  }
}

function normalizeCandidateFiles(
  files: CandidateSourceFile[],
  appRoot: string
): NormalizedCandidateSourceFile[] {
  if (!Array.isArray(files) || files.length === 0 || files.length > MAX_CANDIDATE_FILES) {
    throw new Error('Candidate source file manifest size is outside controller limits.');
  }
  const seen = new Set<string>();
  let total = 0;
  return files.map(file => {
    const normalized = normalizeCandidateFile(file, appRoot);
    if (seen.has(normalized.path))
      throw new Error(`Candidate source duplicate path: ${file.path}.`);
    seen.add(normalized.path);
    total += normalized.bytes.length;
    if (total > MAX_CANDIDATE_TOTAL_BYTES) {
      throw new Error('Candidate source total bytes exceed controller limits.');
    }
    return normalized;
  });
}

function normalizeCandidateFile(
  file: CandidateSourceFile,
  appRoot: string
): NormalizedCandidateSourceFile {
  if (file?.type !== 'file') throw new Error('Candidate source manifest only supports files.');
  const path = normalizeCandidatePath(file.path, 'path');
  if (!candidatePathWithinRoot(path, appRoot)) {
    throw new Error(`Candidate source file is outside appRoot: ${file.path}.`);
  }
  const bytes = decodeCandidateFileBytes(file);
  if (bytes.length > MAX_CANDIDATE_FILE_BYTES) {
    throw new Error(`Candidate source file exceeds controller size limit: ${file.path}.`);
  }
  if (
    bytes.length !== file.size ||
    createHash('sha256').update(bytes).digest('hex') !== file.sha256
  ) {
    throw new Error(`Candidate source file size or digest mismatch: ${file.path}.`);
  }
  return { type: 'file', path, size: bytes.length, sha256: file.sha256, bytes };
}

function decodeCandidateFileBytes(file: CandidateSourceFile): Buffer {
  if (!Number.isSafeInteger(file.size) || file.size < 0 || !HEX64.test(file.sha256)) {
    throw new Error(`Candidate source file metadata is invalid: ${file.path}.`);
  }
  if (typeof file.contentBase64 !== 'string' || !BASE64_BYTES.test(file.contentBase64)) {
    throw new Error(`Candidate source file content must be canonical base64: ${file.path}.`);
  }
  return Buffer.from(file.contentBase64, 'base64');
}

function normalizeCandidatePath(
  value: string,
  field: string,
  options: { directory?: boolean } = {}
): string {
  if (typeof value !== 'string' || value.includes('\0') || value.includes('\\')) {
    throw new Error(`Candidate source ${field} must be a relative POSIX path.`);
  }
  if (options.directory && value === '.') return '.';
  if (value === '' || value === '.' || value.startsWith('/') || value.includes('//')) {
    throw new Error(`Candidate source ${field} must be a relative POSIX path.`);
  }
  const normalized = posixPath.normalize(value);
  if (normalized !== value || normalized === '..' || normalized.startsWith('../')) {
    throw new Error(`Candidate source ${field} must not traverse directories.`);
  }
  return normalized;
}

function candidatePathWithinRoot(path: string, appRoot: string): boolean {
  return appRoot === '.' || path.startsWith(`${appRoot}/`);
}

function candidateContentDigest(
  source: CandidateSourceDescriptor,
  appRoot: string,
  files: NormalizedCandidateSourceFile[]
): string {
  const manifest = files
    .map(file => ({ path: file.path, sha256: file.sha256, size: file.size, type: file.type }))
    .sort((a, b) => compareCodepoint(a.path, b.path));
  return canonicalDigest({
    appRoot,
    commit: source.commit,
    files: manifest,
    profile: source.profile,
    tree: source.tree,
  });
}

function compareCodepoint(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function validateCandidateSnapshot(
  source: NormalizedCandidateSourceDescriptor,
  snapshot: ArtifactSnapshotResult
): void {
  if (snapshot.files.length !== source.files.length) {
    throw new Error('Candidate source volume file count drifted after staging.');
  }
  const actual = new Map(snapshot.files.map(file => [file.path, file]));
  for (const expected of source.files) {
    const file = actual.get(expected.path);
    if (file?.size !== expected.size || file.sha256 !== expected.sha256) {
      throw new Error(`Candidate source volume file drifted after staging: ${expected.path}.`);
    }
  }
  validateCandidateSnapshotDirs(source, snapshot.snapshotDir);
}

function validateCandidateSnapshotDirs(
  source: NormalizedCandidateSourceDescriptor,
  snapshotDir: string
): void {
  const expected = candidateAncestorDirs(source.files);
  const actual = collectSnapshotDirs(snapshotDir, snapshotDir);
  if (actual.size !== expected.size) {
    throw new Error('Candidate source volume directory set drifted after staging.');
  }
  for (const dir of actual) {
    if (!expected.has(dir)) {
      throw new Error(`Candidate source volume contains unexpected directory: ${dir}.`);
    }
  }
}

function candidateAncestorDirs(files: NormalizedCandidateSourceFile[]): Set<string> {
  const dirs = new Set<string>();
  for (const file of files) {
    let current = posixPath.dirname(file.path);
    while (current && current !== '.') {
      dirs.add(current);
      current = posixPath.dirname(current);
    }
  }
  return dirs;
}

function collectSnapshotDirs(root: string, current: string): Set<string> {
  const dirs = new Set<string>();
  collectSnapshotDirsSync(root, current, dirs);
  return dirs;
}

function collectSnapshotDirsSync(root: string, current: string, dirs: Set<string>): void {
  for (const entry of readdirSyncSafe(current)) {
    if (!entry.isDirectory()) continue;
    const abs = join(current, entry.name);
    dirs.add(posixPathRelative(root, abs));
    collectSnapshotDirsSync(root, abs, dirs);
  }
}

function readdirSyncSafe(path: string): Dirent[] {
  return readdirSync(path, { withFileTypes: true });
}

function posixPathRelative(root: string, path: string): string {
  return path
    .slice(root.length + 1)
    .split(/[\\/]+/)
    .join('/');
}

function positiveMilliseconds(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`Browser observation ${name} must be a positive integer.`);
  }
  return value;
}

function observationControl(
  request: NormalizedBrowserObservationRequest,
  startedAt: number
): ObservationControl {
  throwIfAborted(request.signal);
  return {
    deadlineAt: startedAt + request.totalTimeoutMs,
    ...(request.signal ? { signal: request.signal } : {}),
  };
}

function ownedDockerRunner(docker: DockerRunner, resources: Resources): DockerRunner {
  return (args, options) => {
    if (args[0] !== 'create' && args[0] !== 'run') return docker(args, options);
    const nameIndex = args.indexOf('--name');
    const name =
      nameIndex < 0
        ? `archon-browser-helper-${resources.resourceId}-${randomUUID()}`
        : args[nameIndex + 1];
    if (!name) throw new Error('Browser container creation requires an owned name.');
    resources.containerIds.push(name);
    return docker(
      [
        args[0],
        ...(nameIndex < 0 ? ['--name', name] : []),
        ...labelArgs(resources),
        ...args.slice(1),
      ],
      options
    );
  };
}

function controlledDockerRunner(docker: DockerRunner, control: ObservationControl): DockerRunner {
  return (args, options) =>
    docker(args, {
      ...options,
      timeout: remainingTimeout(control, options?.timeout),
      ...(control.signal ? { signal: control.signal } : {}),
    });
}

function remainingTimeout(control: ObservationControl, requested?: number): number {
  throwIfAborted(control.signal);
  const remaining = control.deadlineAt - Date.now();
  if (remaining < 1) throw new Error('Browser observation total deadline exceeded.');
  if (requested === undefined) return remaining;
  return Math.min(requested, remaining);
}

async function controlledDelay(ms: number, control: ObservationControl): Promise<void> {
  const timeout = remainingTimeout(control, ms);
  await delay(timeout);
  if (timeout < ms) throw new Error('Browser observation total deadline exceeded.');
  throwIfAborted(control.signal);
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (!signal?.aborted) return;
  throw new Error('Browser observation cancelled by controller.');
}

async function validatePlaywrightNodeModules(nodeModules: string): Promise<void> {
  const packageJson = JSON.parse(
    await readFile(join(nodeModules, 'playwright', 'package.json'), 'utf8')
  ) as { version?: unknown };
  if (packageJson.version !== PLAYWRIGHT_VERSION) {
    throw new Error(`Installed Playwright package must be ${PLAYWRIGHT_VERSION}.`);
  }
  await stat(join(nodeModules, 'playwright-core', 'package.json'));
}

async function writeCandidateFiles(
  root: string,
  source: NormalizedCandidateSourceDescriptor
): Promise<void> {
  await mkdir(root, { recursive: true });
  for (const file of source.files) {
    const target = join(root, file.path);
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, file.bytes);
  }
}

function validatePolicy(policy: BrowserPolicy): void {
  if (!Array.isArray(policy.required) || policy.required.length === 0) {
    throw new Error('Browser observation policy needs at least one required criterion.');
  }
  const ids = new Set<string>();
  for (const criterion of policy.required) {
    if (!criterion || typeof criterion.id !== 'string' || ids.has(criterion.id)) {
      throw new Error('Browser observation criterion ids must be unique strings.');
    }
    ids.add(criterion.id);
    if (typeof criterion.criterion !== 'string' || criterion.criterion.trim() === '') {
      throw new Error(`Criterion ${criterion.id} needs human criterion text.`);
    }
    if (
      typeof criterion.path !== 'string' ||
      !criterion.path.startsWith('/') ||
      criterion.path.startsWith('//')
    ) {
      throw new Error(`Criterion ${criterion.id} path must be same-origin.`);
    }
    if (!Array.isArray(criterion.assertions) || criterion.assertions.length === 0) {
      throw new Error(`Criterion ${criterion.id} needs frozen assertions.`);
    }
    for (const assertion of criterion.assertions) validateAssertion(criterion.id, assertion);
  }
}

function validateAssertion(id: string, assertion: BrowserAssertion): void {
  if (!assertion || !ASSERTION_TYPES.has(assertion.type)) {
    throw new Error(`Criterion ${id} has unsupported assertion type.`);
  }
  if (typeof assertion.value !== 'string' || assertion.value === '') {
    throw new Error(`Criterion ${id} assertion value must be nonempty.`);
  }
  if (assertion.type === 'fill' && (typeof assertion.text !== 'string' || assertion.text === '')) {
    throw new Error(`Criterion ${id} fill assertion needs text.`);
  }
}

async function inspectApplicationImage(
  docker: DockerRunner,
  app: NormalizedApplicationDescriptor
): Promise<DockerImageInfo> {
  if (!isImmutableImageRef(app.image)) {
    throw new Error('Application image must be immutable by digest or exact image id.');
  }
  if (!HEX40.test(app.commit) || !HEX40.test(app.tree)) {
    throw new Error('Application commit/tree binding must be 40-character git object IDs.');
  }
  const info = await inspectImage(docker, app.image);
  if (app.candidateSource) return info;
  if (info.labels[LABEL_COMMIT] !== app.commit || info.labels[LABEL_TREE] !== app.tree) {
    throw new Error('Application image labels do not match controller commit/tree descriptor.');
  }
  return info;
}

async function inspectVerifierImage(docker: DockerRunner, image: string): Promise<DockerImageInfo> {
  if (image !== PLAYWRIGHT_IMAGE && !isImmutableImageRef(image)) {
    throw new Error(`Unsupported Playwright image '${image}'; expected ${PLAYWRIGHT_IMAGE}.`);
  }
  const info = await inspectImage(docker, image);
  const version = info.labels['com.microsoft.playwright.version'];
  if (!version && image === PLAYWRIGHT_IMAGE) return info;
  if (version && version !== PLAYWRIGHT_VERSION) {
    throw new Error(`Playwright image version mismatch: ${version}`);
  }
  return info;
}

async function inspectImage(docker: DockerRunner, image: string): Promise<DockerImageInfo> {
  const result = await docker(['image', 'inspect', image, '--format', '{{json .}}']);
  const data = JSON.parse(result.stdout) as {
    Id?: unknown;
    Config?: { Labels?: Record<string, string> };
  };
  if (typeof data.Id !== 'string' || !IMAGE_ID.test(data.Id)) {
    throw new Error(`Docker image '${image}' did not resolve to immutable image id.`);
  }
  return { id: data.Id, labels: data.Config?.Labels ?? {} };
}

function isImmutableImageRef(image: string): boolean {
  return IMAGE_ID.test(image) || /@sha256:[0-9a-f]{64}$/i.test(image);
}

function resourceNames(runId: string, seccompProfilePath: string): Resources {
  if (!SAFE_TOKEN.test(runId)) {
    throw new Error('Browser observation run id is not Docker-label safe.');
  }
  const suffix = `${runId}-${randomUUID().slice(0, 8)}`;
  const ownerId = randomUUID();
  const resourceId = `${runId}-${randomUUID().slice(0, 8)}`;
  const labels = [
    `archon.browser-observation.run=${runId}`,
    `archon.browser-observation.resource=${resourceId}`,
    `${OWNER_LABEL}=${ownerId}`,
  ];
  return {
    seccompProfilePath,
    resourceId,
    ownerId,
    podName: `archon-browser-pod-${suffix}`,
    appName: `archon-browser-app-${suffix}`,
    verifierName: `archon-browser-verifier-${suffix}`,
    definitionsVolume: `archon-browser-defs-${suffix}`,
    evidenceVolume: `archon-browser-evidence-${suffix}`,
    candidateVolume: `archon-browser-candidate-${suffix}`,
    labels,
    containerIds: [],
  };
}

function labelArgs(resources: Resources): string[] {
  return resources.labels.flatMap(label => ['--label', label]);
}

function candidateMountArgs(resources: Resources, app: NormalizedApplicationDescriptor): string[] {
  if (!app.candidateSource) return [];
  return ['--mount', `type=volume,src=${resources.candidateVolume},dst=/candidate,readonly`];
}

function candidateEntrypointArgs(app: NormalizedApplicationDescriptor): string[] {
  if (!app.candidateSource) return [];
  return ['--entrypoint', 'node'];
}

function staticWebCommand(port: number, appRoot: string): string[] {
  return ['-e', STATIC_WEB_SERVER_JS, String(port), candidateRootMountPath(appRoot)];
}

function candidateRootMountPath(appRoot: string): string {
  return appRoot === '.' ? '/candidate' : `/candidate/${appRoot}`;
}

function containerHardening(
  network: string,
  user = '1000:1000',
  seccompProfilePath?: string
): string[] {
  const args = [
    '--init',
    '--read-only',
    '--user',
    user,
    '--shm-size',
    '256m',
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges',
    '--network',
    network,
    '--tmpfs',
    '/tmp:rw,nosuid,nodev,size=128m',
  ];
  if (seccompProfilePath) args.push('--security-opt', `seccomp=${seccompProfilePath}`);
  return args;
}

async function writeSeccompProfile(tempDir: string): Promise<string> {
  const path = join(tempDir, 'seccomp_profile.json');
  validatePinnedSeccompProfile();
  await writeFile(path, PLAYWRIGHT_SECCOMP_PROFILE, 'utf8');
  return path;
}

function validatePinnedSeccompProfile(): void {
  const effectiveDigest = createHash('sha256').update(PLAYWRIGHT_SECCOMP_PROFILE).digest('hex');
  if (effectiveDigest !== PLAYWRIGHT_EFFECTIVE_SECCOMP_PROFILE_DIGEST) {
    throw new Error('Pinned effective Playwright seccomp profile digest mismatch.');
  }
  const parsed = JSON.parse(PLAYWRIGHT_SECCOMP_PROFILE) as {
    defaultAction?: unknown;
    archMap?: unknown;
    syscalls?: unknown;
  };
  if (parsed.defaultAction !== 'SCMP_ACT_ERRNO') {
    throw new Error('Pinned Playwright seccomp profile must default-deny with ERRNO.');
  }
  if (
    !Array.isArray(parsed.archMap) ||
    !Array.isArray(parsed.syscalls) ||
    parsed.syscalls.length === 0
  ) {
    throw new Error('Pinned Playwright seccomp profile must include archMap and syscall rules.');
  }
  const allowed = new Set(
    parsed.syscalls.flatMap(rule => {
      if (!rule || typeof rule !== 'object') return [];
      const names = (rule as Record<string, unknown>).names;
      return Array.isArray(names)
        ? names.filter((name): name is string => typeof name === 'string')
        : [];
    })
  );
  for (const blocked of ['io_uring_setup', 'io_uring_register', 'io_uring_enter']) {
    if (allowed.has(blocked))
      throw new Error(`Pinned Playwright seccomp profile allows ${blocked}.`);
  }
  if (!allowed.has('chroot')) {
    throw new Error('Pinned Playwright seccomp profile must allow Chromium chroot.');
  }
}

const PLAYWRIGHT_SECCOMP_PROFILE = `${JSON.stringify(effectiveSeccompProfileAsset, null, 2)}\n`;

async function inspectContainerOwner(docker: DockerRunner, id: string): Promise<string> {
  const result = await docker(['inspect', '-f', `{{ index .Config.Labels "${OWNER_LABEL}" }}`, id]);
  return result.stdout.trim();
}

async function inspectVolumeOwner(docker: DockerRunner, name: string): Promise<string> {
  const result = await docker([
    'volume',
    'inspect',
    name,
    '--format',
    `{{ index .Labels "${OWNER_LABEL}" }}`,
  ]);
  return result.stdout.trim();
}

async function readSnapshotJson(
  snapshot: ArtifactSnapshotResult,
  path: string
): Promise<RawObservation> {
  const text = await readSnapshotText(snapshot, path);
  const parsed = JSON.parse(text) as unknown;
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`Verifier evidence ${path} is not an object.`);
  }
  return parsed as RawObservation;
}

async function readSnapshotText(snapshot: ArtifactSnapshotResult, path: string): Promise<string> {
  const file = snapshot.files.find(entry => entry.path === path);
  if (!file) throw new Error(`Verifier evidence file was not imported: ${path}.`);
  return readFile(join(snapshot.snapshotDir, path), 'utf8');
}

async function validateImportedEvidenceBytes(
  controllerPath: string,
  relPath: string
): Promise<void> {
  const bytes = await readFile(controllerPath);
  if (relPath.endsWith('.png')) {
    validatePng(bytes, relPath);
    return;
  }
  if (relPath.endsWith('.zip')) validateZip(bytes, relPath);
}

const PNG_SIGNATURE = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const MAX_PNG_DECODED_BYTES = 64 * 1024 * 1024;
const MAX_PNG_COMPRESSED_BYTES = 16 * 1024 * 1024;

interface PngHeader {
  expectedBytes: number;
  rowBytes: number;
}

function validatePng(bytes: Buffer, relPath: string): void {
  if (bytes.length < 57 || !bytes.subarray(0, PNG_SIGNATURE.length).equals(PNG_SIGNATURE)) {
    throw new Error(`Imported browser screenshot is not a PNG: ${relPath}.`);
  }
  let offset = PNG_SIGNATURE.length;
  let header: PngHeader | undefined;
  let sawIdat = false;
  let sawIend = false;
  let compressedBytes = 0;
  const idatChunks: Buffer[] = [];
  while (offset < bytes.length) {
    const chunk = readPngChunk(bytes, offset, relPath);
    validatePngChunk(bytes, chunk.typeOffset, chunk.dataEnd, chunk.expectedCrc, relPath);
    if (chunk.type === 'IHDR') {
      if (offset !== PNG_SIGNATURE.length || header)
        throw new Error(`Imported browser PNG IHDR is invalid: ${relPath}.`);
      header = validateIhdr(bytes.subarray(chunk.dataStart, chunk.dataEnd), relPath);
    } else if (chunk.type === 'IDAT') {
      if (!header || sawIend)
        throw new Error(`Imported browser PNG chunk order is invalid: ${relPath}.`);
      sawIdat = true;
      compressedBytes += chunk.length;
      if (compressedBytes > MAX_PNG_COMPRESSED_BYTES)
        throw new Error(`Imported browser PNG compressed data is too large: ${relPath}.`);
      idatChunks.push(bytes.subarray(chunk.dataStart, chunk.dataEnd));
    } else if (chunk.type === 'IEND') {
      validateIend(chunk.length, chunk.crcEnd, bytes.length, relPath);
      sawIend = true;
      break;
    } else if (!header || sawIdat || sawIend || /^[A-Z]/.test(chunk.type)) {
      throw new Error(`Imported browser PNG chunk order is invalid: ${relPath}.`);
    }
    offset = chunk.crcEnd;
  }
  if (!header || !sawIdat || !sawIend) {
    throw new Error(`Imported browser PNG is missing required chunks: ${relPath}.`);
  }
  validatePngPixels(idatChunks, header, relPath);
}

function readPngChunk(
  bytes: Buffer,
  offset: number,
  relPath: string
): {
  length: number;
  type: string;
  typeOffset: number;
  dataStart: number;
  dataEnd: number;
  crcEnd: number;
  expectedCrc: number;
} {
  if (offset + 12 > bytes.length) throw new Error(`Imported browser PNG is truncated: ${relPath}.`);
  const length = bytes.readUInt32BE(offset);
  const typeOffset = offset + 4;
  const dataStart = offset + 8;
  const dataEnd = dataStart + length;
  const crcEnd = dataEnd + 4;
  if (length > MAX_PNG_COMPRESSED_BYTES || crcEnd > bytes.length) {
    throw new Error(`Imported browser PNG chunk is invalid: ${relPath}.`);
  }
  const type = bytes.subarray(typeOffset, dataStart).toString('latin1');
  if (!/^[A-Za-z]{2}[A-Z][A-Za-z]$/.test(type)) {
    throw new Error(`Imported browser PNG chunk type is invalid: ${relPath}.`);
  }
  return {
    length,
    type,
    typeOffset,
    dataStart,
    dataEnd,
    crcEnd,
    expectedCrc: bytes.readUInt32BE(dataEnd),
  };
}

function validatePngChunk(
  bytes: Buffer,
  typeOffset: number,
  dataEnd: number,
  expected: number,
  relPath: string
): void {
  const actual = crc32(bytes.subarray(typeOffset, dataEnd));
  if (actual !== expected) throw new Error(`Imported browser PNG CRC is invalid: ${relPath}.`);
}

function validateIhdr(data: Buffer, relPath: string): PngHeader {
  if (data.length !== 13) throw new Error(`Imported browser PNG IHDR is invalid: ${relPath}.`);
  const width = data.readUInt32BE(0);
  const height = data.readUInt32BE(4);
  const bitDepth = data[8];
  const colorType = data[9];
  const compression = data[10];
  const filter = data[11];
  const interlace = data[12];
  const channels = colorType === 2 ? 3 : colorType === 6 ? 4 : 0;
  if (!channels || bitDepth !== 8 || compression !== 0 || filter !== 0 || interlace !== 0) {
    throw new Error(`Imported browser PNG IHDR format is unsupported: ${relPath}.`);
  }
  return expectedPngScanlineBytes(width, height, channels, relPath);
}

function expectedPngScanlineBytes(
  width: number,
  height: number,
  channels: number,
  relPath: string
): PngHeader {
  if (width < 1 || height < 1) {
    throw new Error(`Imported browser PNG dimensions are invalid: ${relPath}.`);
  }
  const rowBytes = 1 + width * channels;
  const expectedBytes = rowBytes * height;
  if (!Number.isSafeInteger(expectedBytes) || expectedBytes > MAX_PNG_DECODED_BYTES) {
    throw new Error(`Imported browser PNG dimensions exceed controller limits: ${relPath}.`);
  }
  return { expectedBytes, rowBytes };
}

function validateIend(length: number, crcEnd: number, totalBytes: number, relPath: string): void {
  if (length !== 0) throw new Error(`Imported browser PNG IEND is invalid: ${relPath}.`);
  if (crcEnd !== totalBytes) throw new Error(`Imported browser PNG has trailing data: ${relPath}.`);
}

function validatePngPixels(idatChunks: Buffer[], header: PngHeader, relPath: string): void {
  let decoded: Buffer;
  try {
    decoded = inflateSync(Buffer.concat(idatChunks), { maxOutputLength: header.expectedBytes });
  } catch {
    throw new Error(`Imported browser PNG pixel data is invalid: ${relPath}.`);
  }
  if (decoded.length !== header.expectedBytes) {
    throw new Error(`Imported browser PNG pixel data length is invalid: ${relPath}.`);
  }
  validatePngFilters(decoded, header.rowBytes, relPath);
}

function validatePngFilters(decoded: Buffer, rowBytes: number, relPath: string): void {
  for (let offset = 0; offset < decoded.length; offset += rowBytes) {
    const filterType = decoded[offset];
    if (filterType === undefined || filterType > 4) {
      throw new Error(`Imported browser PNG row filter is invalid: ${relPath}.`);
    }
  }
}

function validateZip(bytes: Buffer, relPath: string): void {
  const eocd = findZipEocd(bytes);
  if (eocd < 0) throw new Error(`Imported browser trace is not a ZIP: ${relPath}.`);
  const entries = bytes.readUInt16LE(eocd + 10);
  const directorySize = bytes.readUInt32LE(eocd + 12);
  const directoryOffset = bytes.readUInt32LE(eocd + 16);
  if (entries < 1 || directorySize < 46 || directoryOffset + directorySize > eocd) {
    throw new Error(`Imported browser trace ZIP central directory is invalid: ${relPath}.`);
  }
  if (bytes.readUInt32LE(directoryOffset) !== 0x02014b50) {
    throw new Error(`Imported browser trace ZIP central directory is missing: ${relPath}.`);
  }
}

function findZipEocd(bytes: Buffer): number {
  const min = Math.max(0, bytes.length - 65_557);
  for (let offset = bytes.length - 22; offset >= min; offset--) {
    if (bytes.readUInt32LE(offset) === 0x06054b50) return offset;
  }
  return -1;
}

function crc32(bytes: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ byte) & 0xff];
  }
  return (crc ^ 0xffffffff) >>> 0;
}

const CRC32_TABLE = Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit++) {
    value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }
  return value >>> 0;
});

function finalizeResult(
  request: NormalizedBrowserObservationRequest,
  raw: RawObservation,
  evidence: BrowserObservationResult['evidence'],
  policyDigest: string,
  appImage: DockerImageInfo,
  verifierImage: DockerImageInfo
): BrowserObservationResult {
  const criteria = raw.criteria ?? [];
  validateRawObservation(request, raw, criteria);
  validateEvidenceCoverage(request, evidence);
  const security = raw.security;
  if (!security) throw new Error('Verifier did not report kernel security status.');
  const failed = criteria.some(row => row.status !== 'passed');
  return {
    authority: 'none',
    status: failed ? 'failed' : 'passed',
    runId: request.runId,
    policyDigest,
    observedOrigin: raw.observedOrigin,
    app: {
      image: request.app.image,
      imageId: appImage.id,
      commit: request.app.commit,
      tree: request.app.tree,
    },
    verifier: {
      image: request.verifierImage,
      imageId: verifierImage.id,
      playwrightVersion: PLAYWRIGHT_VERSION,
    },
    viewport: request.viewport,
    criteria,
    security,
    evidence,
  };
}

function validateRawObservation(
  request: NormalizedBrowserObservationRequest,
  raw: RawObservation,
  criteria: ObservedCriterion[]
): asserts raw is RawObservation & { observedOrigin: string } {
  if (raw.fatal) throw new Error(`Verifier fatal error: ${raw.fatal}`);
  const expectedOrigin = `http://127.0.0.1:${request.app.port}`;
  if (raw.observedOrigin !== expectedOrigin) {
    throw new Error('Verifier observed the wrong application origin.');
  }
  if (raw.canary?.ok !== false)
    throw new Error('Verifier did not prove host/controller canary isolation.');
  validateSecurityObservation(raw.security);
  if (criteria.length !== request.policy.required.length) {
    throw new Error('Verifier did not execute every required criterion.');
  }
  const expectedById = new Map(request.policy.required.map(item => [item.id, item]));
  for (const row of criteria) validateCriterionRow(row, expectedById, expectedOrigin);
  if (expectedById.size > 0) throw new Error('Verifier did not execute every required criterion.');
}

function validateCriterionRow(
  row: ObservedCriterion,
  expectedById: Map<string, BrowserCriterion>,
  expectedOrigin: string
): void {
  const expected = expectedById.get(row.id);
  if (!expected) throw new Error(`Verifier reported unexpected criterion '${row.id}'.`);
  expectedById.delete(row.id);
  if (row.status !== 'passed' && row.status !== 'failed')
    throw new Error(`Verifier missing or skipped criterion result '${row.id}'.`);
  if (row.path !== expected.path) throw new Error(`Verifier criterion path mismatch '${row.id}'.`);
  if (row.observed_origin !== expectedOrigin) {
    throw new Error(
      `Verifier criterion '${row.id}' left the approved origin: ${row.observed_origin} != ${expectedOrigin}; status=${row.status}; error=${row.error ?? ''}; assertions=${JSON.stringify(row.assertions ?? [])}.`
    );
  }
  validateCriterionAssertions(row, expected.assertions);
  if (row.status === 'passed' && row.assertions?.some(assertion => assertion.status !== 'passed')) {
    throw new Error(`Verifier criterion '${row.id}' contradicts its failed assertions.`);
  }
}

function validateCriterionAssertions(row: ObservedCriterion, expected: BrowserAssertion[]): void {
  const actualAssertions = row.assertions ?? [];
  if (actualAssertions.length !== expected.length) {
    throw new Error(`Verifier assertion count mismatch for '${row.id}'.`);
  }
  for (let index = 0; index < expected.length; index++) {
    validateAssertionResult(row.id, actualAssertions[index], expected[index]);
  }
}

function validateAssertionResult(
  criterionId: string,
  assertion: ObservedAssertion | undefined,
  expected: BrowserAssertion | undefined
): void {
  if (!assertion || assertion.type !== expected?.type || assertion.value !== expected.value) {
    throw new Error(`Verifier assertion mismatch for '${criterionId}'.`);
  }
  if (assertion.status !== 'passed' && assertion.status !== 'failed') {
    throw new Error(`Verifier skipped assertion for '${criterionId}'.`);
  }
}

function validateSecurityObservation(security: SecurityObservation | undefined): void {
  if (!security) throw new Error('Verifier did not report kernel security status.');
  const status = security.status;
  if (status.Seccomp !== '2') throw new Error('Verifier seccomp filter is not active.');
  if (status.NoNewPrivs !== '1') throw new Error('Verifier no-new-privileges is not active.');
  if (security.identity.uid !== 1000 || security.identity.gid !== 1000) {
    throw new Error('Verifier did not run as UID/GID 1000.');
  }
  if (!['x64', 'arm64'].includes(security.arch)) {
    throw new Error(`Verifier reported unsupported architecture '${security.arch}'.`);
  }
  if (!/^0+$/.test(status.CapEff ?? '') || !/^0+$/.test(status.CapBnd ?? '')) {
    throw new Error('Verifier retained Linux capabilities.');
  }
  for (const name of ['mount', 'bpf', 'init_module', 'chroot', 'parent_setns']) {
    const control = security.controls[name];
    if (
      !control?.blocked ||
      typeof control.blocked !== 'boolean' ||
      control.code !== 0 ||
      control.signal !== null ||
      control.result !== -1 ||
      (control.errno !== 1 && control.errno !== 38)
    ) {
      throw new Error(`Verifier negative security control '${name}' was not blocked.`);
    }
  }
}

function validateEvidenceCoverage(
  request: NormalizedBrowserObservationRequest,
  evidence: BrowserObservationResult['evidence']
): void {
  const expected = new Set(request.policy.required.map(criterion => criterion.id));
  const paths = new Set<string>();
  for (const screenshot of evidence.screenshots) {
    if (
      !screenshot.criterion ||
      !expected.delete(screenshot.criterion) ||
      !screenshot.path.endsWith('.png')
    ) {
      throw new Error('Verifier screenshot criteria are missing, duplicated, or mismatched.');
    }
    paths.add(screenshot.path);
  }
  if (expected.size || paths.size !== evidence.screenshots.length || !evidence.traces.length) {
    throw new Error('Verifier screenshot or trace execution evidence is incomplete.');
  }
  for (const trace of evidence.traces) {
    if (!trace.path.endsWith('.zip') || paths.has(trace.path)) {
      throw new Error('Verifier trace evidence is duplicated or has the wrong format.');
    }
    paths.add(trace.path);
  }
}

function normalizeEvidence(
  evidence: RawObservation['evidence']
): BrowserObservationResult['evidence'] {
  const screenshots = (evidence?.screenshots ?? []).map(entry => validateEvidenceEntry(entry));
  const traces = (evidence?.traces ?? []).map(entry => validateEvidenceEntry(entry));
  return { screenshots, traces };
}

function validateEvidenceEntry(entry: EvidenceFile): ImportedEvidenceFile {
  if (
    !entry.path.startsWith('browser-evidence/') ||
    entry.path.includes('..') ||
    !HEX64.test(entry.sha256)
  ) {
    throw new Error(
      'Verifier evidence entries must be relative browser-evidence paths with digests.'
    );
  }
  if (basename(entry.path) === '') {
    throw new Error('Verifier evidence entry needs a filename.');
  }
  return { ...entry, controllerPath: '' };
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function canonicalDigest(data: unknown): string {
  return createHash('sha256').update(canonicalJson(data)).digest('hex');
}

function canonicalJson(data: unknown): string {
  if (data === null || typeof data !== 'object') return JSON.stringify(data);
  if (Array.isArray(data)) return `[${data.map(canonicalJson).join(',')}]`;
  const record = data as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map(key => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(',')}}`;
}

const STATIC_WEB_SERVER_JS = String.raw`
const profile = 'static-web-http-v1';
const http = require('http');
const fs = require('fs');
const path = require('path');
const port = Number(process.argv[1]);
const root = path.resolve(process.argv[2]);
if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('invalid port');
const types = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.css', 'text/css; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml; charset=utf-8'],
  ['.txt', 'text/plain; charset=utf-8'],
]);
function resolveRequest(url) {
  let pathname;
  try {
    pathname = new URL(url, 'http://127.0.0.1').pathname;
  } catch (_error) {
    return { status: 400 };
  }
  let decoded;
  try {
    decoded = decodeURIComponent(pathname);
  } catch (_error) {
    return { status: 400 };
  }
  if (decoded.includes('\0')) return { status: 400 };
  const rel = decoded.replace(/^\/+/, '') || 'index.html';
  const target = path.resolve(root, rel);
  if (target !== root && !target.startsWith(root + path.sep)) return { status: 403 };
  return { status: 200, target };
}
function sendFile(res, file) {
  fs.readFile(file, (error, bytes) => {
    if (error) {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('not found');
      return;
    }
    res.writeHead(200, {
      'content-type': types.get(path.extname(file).toLowerCase()) || 'application/octet-stream',
      'cache-control': 'no-store',
    });
    res.end(bytes);
  });
}
http.createServer((req, res) => {
  const resolved = resolveRequest(req.url || '/');
  if (resolved.status !== 200) {
    res.writeHead(resolved.status, { 'content-type': 'text/plain; charset=utf-8' });
    res.end(resolved.status === 400 ? 'bad request' : 'forbidden');
    return;
  }
  fs.stat(resolved.target, (error, info) => {
    if (error) return sendFile(res, resolved.target);
    sendFile(res, info.isDirectory() ? path.join(resolved.target, 'index.html') : resolved.target);
  });
}).listen(port, '127.0.0.1');
`;

export const RUNNER_JS = String.raw`
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawnSync } = require('child_process');
const { chromium } = require(process.env.ARCHON_PLAYWRIGHT_MODULE || 'playwright');

const input = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const out = process.argv[3];
const timeout = input.timeoutMs || 10000;
const expectedOrigin = input.expectedOrigin || input.origin.replace(/\/$/, '');
const results = [];
const evidence = { screenshots: [], traces: [] };

function canonicalJson(data) {
  if (data === null || typeof data !== 'object') return JSON.stringify(data);
  if (Array.isArray(data)) return '[' + data.map(canonicalJson).join(',') + ']';
  return '{' + Object.keys(data).sort().map(key => JSON.stringify(key) + ':' + canonicalJson(data[key])).join(',') + '}';
}
function digest(data) { return crypto.createHash('sha256').update(canonicalJson(data)).digest('hex'); }
function safeName(id) { return id.replace(/[^a-zA-Z0-9._-]+/g, '-').slice(0, 64) || 'criterion'; }
function screenshotName(index, id) {
  const h = crypto.createHash('sha256').update(id).digest('hex').slice(0, 12);
  return 'browser-evidence/' + String(index + 1).padStart(3, '0') + '-' + safeName(id) + '-' + h + '.png';
}
function originOf(value) { try { return new URL(value).origin; } catch (_err) { return ''; } }
function assertOrigin(page, stage) {
  const observed = originOf(page.url());
  if (observed !== expectedOrigin) throw new Error(stage + ' changed origin to ' + (observed || page.url()));
  return observed;
}
async function checkAssertion(page, assertion) {
  if (assertion.type === 'text') return page.getByText(assertion.value, { exact: false }).first().waitFor({ timeout });
  if (assertion.type === 'selector') return page.locator(assertion.value).first().waitFor({ timeout });
  if (assertion.type === 'testid') return page.getByTestId(assertion.value).first().waitFor({ timeout });
  if (assertion.type === 'click') return page.locator(assertion.value).first().click({ timeout });
  if (assertion.type === 'fill') return page.locator(assertion.value).first().fill(assertion.text, { timeout });
  if (assertion.type === 'url') { if (!page.url().includes(assertion.value)) throw new Error('url missing ' + assertion.value); return; }
  if (assertion.type === 'title') { const title = await page.title(); if (!title.includes(assertion.value)) throw new Error('title missing ' + assertion.value); return; }
  throw new Error('unsupported assertion type ' + assertion.type);
}
function readSecurityStatus() {
  const status = {};
  const text = fs.readFileSync('/proc/self/status', 'utf8');
  for (const key of ['Seccomp', 'NoNewPrivs', 'CapEff', 'CapBnd']) {
    const match = text.match(new RegExp('^' + key + ':\\s*(.+)$', 'm'));
    status[key] = match ? match[1].trim() : '';
  }
  return status;
}
function checkSecurity() {
  const arch = process.arch;
  const syscalls = arch === 'x64'
    ? { bpf: 321, init_module: 175, setns: 308, mount: 165 }
    : arch === 'arm64'
      ? { bpf: 280, init_module: 105, setns: 268, mount: 40 }
      : null;
  if (!syscalls) throw new Error('unsupported security probe arch: ' + arch);
  function syscallBlocked(name, number, args) {
    const code = 'import ctypes,json; libc=ctypes.CDLL(None, use_errno=True); r=libc.syscall(' + [number].concat(args).join(',') + '); e=ctypes.get_errno(); print(json.dumps({"name":"' + name + '","result":r,"errno":e}))';
    const out = spawnSync('python3', ['-c', code], { encoding: 'utf8', timeout: 2000 });
    if (out.status !== 0 || out.signal || !out.stdout.trim()) throw new Error('security syscall probe failed: ' + name + ' ' + (out.stderr || out.error?.message || 'no-output'));
    const parsed = JSON.parse(out.stdout.trim());
    return { blocked: parsed.result === -1 && (parsed.errno === 1 || parsed.errno === 38), code: out.status, signal: out.signal, errno: parsed.errno, result: parsed.result };
  }
  function setnsBlocked(number) {
    const code = 'import ctypes,json,os; fd=os.open("/proc/1/ns/mnt", os.O_RDONLY); libc=ctypes.CDLL(None, use_errno=True); r=libc.syscall(' + number + ',fd,0); e=ctypes.get_errno(); os.close(fd); print(json.dumps({"name":"parent_setns","result":r,"errno":e}))';
    const out = spawnSync('python3', ['-c', code], { encoding: 'utf8', timeout: 2000 });
    if (out.status !== 0 || out.signal || !out.stdout.trim()) throw new Error('security syscall probe failed: parent_setns ' + (out.stderr || out.error?.message || 'no-output'));
    const parsed = JSON.parse(out.stdout.trim());
    return { blocked: parsed.result === -1 && (parsed.errno === 1 || parsed.errno === 38), code: out.status, signal: out.signal, errno: parsed.errno, result: parsed.result };
  }
  function chrootBlocked() {
    const code = 'import ctypes,json; libc=ctypes.CDLL(None, use_errno=True); r=libc.chroot(b"/"); e=ctypes.get_errno(); print(json.dumps({"result":r,"errno":e}))';
    const out = spawnSync('python3', ['-c', code], { encoding: 'utf8', timeout: 2000 });
    if (out.status !== 0 || out.signal || !out.stdout.trim()) throw new Error('security chroot probe failed');
    const parsed = JSON.parse(out.stdout.trim());
    return { blocked: parsed.result === -1 && (parsed.errno === 1 || parsed.errno === 38), code: out.status, signal: out.signal, errno: parsed.errno, result: parsed.result };
  }
  const controls = {
    mount: syscallBlocked('mount', syscalls.mount, [0, 0, 0, 0, 0]),
    bpf: syscallBlocked('bpf', syscalls.bpf, [0, 0, 0]),
    init_module: syscallBlocked('init_module', syscalls.init_module, [0, 0, 0]),
    chroot: chrootBlocked(),
    parent_setns: setnsBlocked(syscalls.setns),
  };
  for (const [name, control] of Object.entries(controls)) {
    if (control.blocked !== true || (control.errno !== undefined && ![1, 38].includes(control.errno))) {
      throw new Error('security control not blocked: ' + name + ' ' + JSON.stringify(control));
    }
  }
  return {
    status: readSecurityStatus(),
    identity: { uid: process.getuid(), gid: process.getgid() },
    arch,
    controls,
  };
}
async function maybeCheckCanary(context) {
  if (!input.canaryUrl) return { ok: false };
  const canaryPage = await context.newPage();
  try {
    await canaryPage.goto(input.canaryUrl, { waitUntil: 'domcontentloaded', timeout: Math.min(timeout, 1000) });
    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err.message || err) };
  } finally {
    await canaryPage.close().catch(() => {});
  }
}
(async () => {
  fs.mkdirSync(path.join(input.artifacts, 'browser-evidence'), { recursive: true });
  fs.writeFileSync(path.join(input.artifacts, 'policy.sha256'), input.policyDigest + '\n');
  const browser = await chromium.launch({ headless: true, chromiumSandbox: true });
  const context = await browser.newContext({ viewport: input.viewport });
  await context.tracing.start({ screenshots: true, snapshots: true });
  const security = checkSecurity();
  const canary = await maybeCheckCanary(context);
  const page = await context.newPage();
  for (let index = 0; index < input.required.length; index++) {
    const criterion = input.required[index];
    const row = { id: criterion.id, path: criterion.path, status: 'passed', assertions: [], observed_origin: expectedOrigin };
    try {
      const target = new URL(criterion.path, expectedOrigin).toString();
      if (originOf(target) !== expectedOrigin) throw new Error('cross-origin navigation refused');
      await page.goto(target, { waitUntil: 'domcontentloaded', timeout });
      row.observed_origin = assertOrigin(page, 'navigation');
      for (const assertion of criterion.assertions) {
        try {
          await checkAssertion(page, assertion);
          await page.waitForLoadState('domcontentloaded', { timeout: Math.min(timeout, 1000) }).catch(() => {});
          await page.waitForTimeout(100);
          row.observed_origin = assertOrigin(page, 'assertion ' + assertion.type);
          row.assertions.push({ type: assertion.type, value: assertion.value, status: 'passed' });
        } catch (err) {
          row.observed_origin = originOf(page.url()) || row.observed_origin;
          row.assertions.push({ type: assertion.type, value: assertion.value, status: 'failed', error: String(err.message || err) });
          row.status = 'failed';
        }
      }
    } catch (err) {
      row.status = 'failed';
      row.error = String(err.message || err);
      row.observed_origin = originOf(page.url()) || row.observed_origin;
    }
    if (originOf(page.url()) !== expectedOrigin) await page.goto(expectedOrigin, { waitUntil: 'domcontentloaded', timeout }).catch(() => {});
    assertOrigin(page, 'before screenshot');
    const shot = screenshotName(index, criterion.id);
    await page.screenshot({ path: path.join(input.artifacts, shot), fullPage: true });
    const shotHash = crypto.createHash('sha256').update(fs.readFileSync(path.join(input.artifacts, shot))).digest('hex');
    evidence.screenshots.push({ criterion: criterion.id, path: shot, sha256: shotHash });
    results.push(row);
  }
  const trace = 'browser-evidence/trace.zip';
  await context.tracing.stop({ path: path.join(input.artifacts, trace) });
  const traceHash = crypto.createHash('sha256').update(fs.readFileSync(path.join(input.artifacts, trace))).digest('hex');
  evidence.traces.push({ path: trace, sha256: traceHash });
  await browser.close();
  const raw = { observedOrigin: expectedOrigin, policyDigest: digest({ required: input.required }), criteria: results, evidence, canary, security };
  fs.writeFileSync(out, JSON.stringify(raw, null, 2) + '\n');
})().catch(err => {
  fs.writeFileSync(out, JSON.stringify({ fatal: String(err.stack || err), criteria: [], evidence: { screenshots: [], traces: [] } }, null, 2) + '\n');
  process.exit(2);
});
`;
