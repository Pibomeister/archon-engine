import { createHash, randomUUID } from 'crypto';
import { lstat, mkdir, rm, rename, writeFile, chmod } from 'fs/promises';
import { dirname, resolve, sep } from 'path';
import { extractDockerError, type DockerRunner } from './docker-exec';

const VOLUME_ROOT = '/snapshot-volume';
const DEFAULT_MAX_TOTAL_BYTES = 64 * 1024 * 1024;
const DEFAULT_MAX_FILE_BYTES = 16 * 1024 * 1024;
const DEFAULT_MAX_FILES = 10_000;
const READER_CPUS = '1';

export interface TrustedArtifactVolumeMetadata {
  /** Controller-created per-run workspace/artifact volume. Must not come from agent output. */
  workspaceVolume: string;
  /** Immutable runner image ID captured by the controller at run creation. */
  image: string;
  /** Stable run/resource identity used only for labels and diagnostics. */
  resourceId: string;
  readerProfile?: 'bun-archon' | 'node-1000';
}

export interface ArtifactSnapshotOptions {
  /** Fresh controller-owned destination directory. It is atomically replaced from a staging dir. */
  destinationDir: string;
  maxTotalBytes?: number;
  maxFileBytes?: number;
  maxFiles?: number;
  dockerTimeoutMs?: number;
}

export interface ArtifactSnapshotFile {
  path: string;
  size: number;
  sha256: string;
  mode: number;
}

export interface ArtifactSnapshotResult {
  snapshotDir: string;
  files: ArtifactSnapshotFile[];
  totalBytes: number;
  image: string;
}

type ReaderEntry = ReaderDirEntry | ReaderFileEntry | ReaderRejectEntry;

interface ReaderDirEntry {
  type: 'dir';
  path: string;
  mode?: number;
}

interface ReaderFileEntry {
  type: 'file';
  path: string;
  mode?: number;
  size: number;
  sha256: string;
  contentBase64: string;
}

interface ReaderRejectEntry {
  type: 'reject';
  path: string;
  reason: string;
}

interface SnapshotLimits {
  maxTotalBytes: number;
  maxFileBytes: number;
  maxFiles: number;
}

/**
 * Copy a stopped hardened container run's trusted named volume into a fresh
 * controller-owned snapshot directory without host bind mounts or tar extraction.
 *
 * The caller must stop all writers first. This helper intentionally accepts only
 * controller-tracked volume metadata, not an arbitrary container id or path from
 * agent output. Resume/write-back integration should call it at a controller
 * phase boundary after the per-run container is stopped.
 */
export async function snapshotContainerArtifacts(
  docker: DockerRunner,
  metadata: TrustedArtifactVolumeMetadata,
  options: ArtifactSnapshotOptions
): Promise<ArtifactSnapshotResult> {
  validateMetadata(metadata);
  const limits = normalizeLimits(options);
  const snapshotDir = resolve(options.destinationDir);
  await assertFreshDestination(snapshotDir);
  const stagingDir = `${snapshotDir}.tmp-${randomUUID()}`;

  let staged = false;
  try {
    const stdout = await readVolumeJsonl(docker, metadata, limits, options.dockerTimeoutMs);
    const result = await materializeSnapshot(
      stdout,
      stagingDir,
      snapshotDir,
      metadata.image,
      limits
    );
    await rename(stagingDir, snapshotDir);
    staged = true;
    return result;
  } finally {
    if (!staged) await rm(stagingDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

async function assertFreshDestination(destination: string): Promise<void> {
  try {
    await lstat(destination);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
    throw error;
  }
  throw new Error(`Snapshot destination already exists: ${destination}`);
}

async function readVolumeJsonl(
  docker: DockerRunner,
  metadata: TrustedArtifactVolumeMetadata,
  limits: SnapshotLimits,
  timeout?: number
): Promise<string> {
  const readerName = `archon-snapshot-${metadata.resourceId}-${randomUUID()}`;
  const nodeReader = metadata.readerProfile === 'node-1000';
  try {
    const { stdout } = await docker(
      [
        'run',
        '--rm',
        '--name',
        readerName,
        '--label',
        'diy.archon.managed=true',
        '--label',
        `diy.archon.env-id=${metadata.resourceId}`,
        '--user',
        nodeReader ? '1000:1000' : 'archon',
        '--read-only',
        '--cap-drop',
        'ALL',
        '--security-opt',
        'no-new-privileges',
        '--network',
        'none',
        '--memory',
        '256m',
        '--cpus',
        READER_CPUS,
        '--pids-limit',
        '128',
        '--tmpfs',
        '/tmp:rw,nosuid,nodev,size=16m',
        '-v',
        `${metadata.workspaceVolume}:${VOLUME_ROOT}:ro`,
        '--entrypoint',
        nodeReader ? 'node' : 'bun',
        metadata.image,
        ...(nodeReader ? ['--input-type=module'] : []),
        '--eval',
        buildReaderScript(),
        VOLUME_ROOT,
        String(limits.maxFileBytes),
        String(limits.maxFiles),
        String(limits.maxTotalBytes),
      ],
      { timeout: timeout ?? 120_000, maxBuffer: limits.maxTotalBytes * 2 + 1024 * 1024 }
    );
    return stdout;
  } catch {
    let cleanup = 'removed';
    try {
      await docker(['rm', '-f', readerName], { timeout: 10_000 });
    } catch (error) {
      if (!/no such container/i.test(extractDockerError(error))) cleanup = 'failed';
    }
    throw new Error(`Artifact snapshot reader failed: ${readerName}; cleanup=${cleanup}`);
  }
}

async function materializeSnapshot(
  stdout: string,
  stagingDir: string,
  snapshotDir: string,
  image: string,
  limits: SnapshotLimits
): Promise<ArtifactSnapshotResult> {
  await mkdir(stagingDir, { recursive: true, mode: 0o700 });
  const files: ArtifactSnapshotFile[] = [];
  let totalBytes = 0;
  let entryCount = 0;
  const seenPaths = new Set<string>();

  for (const line of stdout.split('\n')) {
    if (!line.trim()) continue;
    entryCount++;
    if (entryCount > limits.maxFiles)
      throw new Error(`Snapshot exceeds max entry count ${limits.maxFiles}`);
    const entry = parseReaderEntry(line);
    if (entry.type === 'reject') throw new Error(`Snapshot refused ${entry.path}: ${entry.reason}`);
    const target = safeTargetPath(stagingDir, entry.path);
    if (seenPaths.has(target)) throw new Error(`Duplicate snapshot path '${entry.path}'`);
    seenPaths.add(target);
    if (entry.type === 'dir') {
      await mkdir(target, { recursive: true, mode: 0o700 });
      continue;
    }
    if (entry.size > limits.maxFileBytes)
      throw new Error(`Snapshot file '${entry.path}' exceeds max size`);
    totalBytes += entry.size;
    if (totalBytes > limits.maxTotalBytes)
      throw new Error(`Snapshot exceeds max total bytes ${limits.maxTotalBytes}`);
    const bytes = Buffer.from(entry.contentBase64, 'base64');
    validateFileDigest(entry, bytes);
    await mkdir(dirname(target), { recursive: true, mode: 0o700 });
    await writeFile(target, bytes, { flag: 'wx', mode: privateFileMode(entry.mode) });
    await chmod(target, privateFileMode(entry.mode));
    files.push({
      path: entry.path,
      size: bytes.length,
      sha256: entry.sha256,
      mode: privateFileMode(entry.mode),
    });
  }

  return { snapshotDir, files, totalBytes, image };
}

function parseReaderEntry(line: string): ReaderEntry {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new Error('Snapshot reader emitted invalid JSONL');
  }
  if (!parsed || typeof parsed !== 'object')
    throw new Error('Snapshot reader emitted non-object JSONL');
  const record = parsed as Record<string, unknown>;
  if (record.type === 'dir' && typeof record.path === 'string')
    return { type: 'dir', path: record.path, mode: numberOrUndefined(record.mode) };
  if (
    record.type === 'reject' &&
    typeof record.path === 'string' &&
    typeof record.reason === 'string'
  )
    return { type: 'reject', path: record.path, reason: record.reason };
  if (record.type === 'file') return parseFileEntry(record);
  throw new Error('Snapshot reader emitted unsupported entry');
}

function parseFileEntry(record: Record<string, unknown>): ReaderFileEntry {
  if (typeof record.path !== 'string') throw new Error('Snapshot file entry is missing path');
  if (typeof record.size !== 'number' || !Number.isSafeInteger(record.size) || record.size < 0)
    throw new Error(`Snapshot file '${record.path}' is missing size`);
  if (typeof record.sha256 !== 'string')
    throw new Error(`Snapshot file '${record.path}' is missing digest`);
  if (typeof record.contentBase64 !== 'string')
    throw new Error(`Snapshot file '${record.path}' is missing content`);
  return {
    type: 'file',
    path: record.path,
    mode: numberOrUndefined(record.mode),
    size: record.size,
    sha256: record.sha256,
    contentBase64: record.contentBase64,
  };
}

function validateFileDigest(entry: ReaderFileEntry, bytes: Buffer): void {
  if (bytes.length !== entry.size) throw new Error(`Snapshot file '${entry.path}' size mismatch`);
  const digest = createHash('sha256').update(bytes).digest('hex');
  if (digest !== entry.sha256) throw new Error(`Snapshot file '${entry.path}' digest mismatch`);
}

function safeTargetPath(root: string, relPath: string): string {
  if (!relPath || relPath.startsWith('/') || relPath.includes('\0'))
    throw new Error(`Unsafe snapshot path '${relPath}'`);
  const parts = relPath.split('/');
  if (parts.some(part => !part || part === '.' || part === '..'))
    throw new Error(`Unsafe snapshot path '${relPath}'`);
  const target = resolve(root, ...parts);
  if (target !== root && target.startsWith(`${root}${sep}`)) return target;
  throw new Error(`Unsafe snapshot path '${relPath}'`);
}

function validateMetadata(metadata: TrustedArtifactVolumeMetadata): void {
  validateDockerToken('workspace volume', metadata.workspaceVolume);
  validateDockerToken('resource id', metadata.resourceId);
  if (
    metadata.readerProfile !== undefined &&
    metadata.readerProfile !== 'bun-archon' &&
    metadata.readerProfile !== 'node-1000'
  )
    throw new Error('Unsupported artifact reader profile');
  if (!/^sha256:[0-9a-f]{64}$/.test(metadata.image))
    throw new Error('Snapshot requires an immutable image ID');
}

function validateDockerToken(label: string, value: string): void {
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/.test(value)) {
    throw new Error(`Invalid ${label} '${value}'.`);
  }
}

function normalizeLimits(options: ArtifactSnapshotOptions): SnapshotLimits {
  return {
    maxTotalBytes: positiveLimit(options.maxTotalBytes, DEFAULT_MAX_TOTAL_BYTES, 'maxTotalBytes'),
    maxFileBytes: positiveLimit(options.maxFileBytes, DEFAULT_MAX_FILE_BYTES, 'maxFileBytes'),
    maxFiles: positiveLimit(options.maxFiles, DEFAULT_MAX_FILES, 'maxFiles'),
  };
}

function positiveLimit(value: number | undefined, fallback: number, label: string): number {
  const limit = value ?? fallback;
  if (!Number.isSafeInteger(limit) || limit < 1)
    throw new Error(`${label} must be a positive integer`);
  return limit;
}

function privateFileMode(mode: number | undefined): number {
  return mode !== undefined && Number.isInteger(mode) && (mode & 0o111) !== 0 ? 0o700 : 0o600;
}

function numberOrUndefined(value: unknown): number | undefined {
  return typeof value === 'number' ? value : undefined;
}

export function buildReaderScript(): string {
  return String.raw`
import { createHash } from 'crypto';
import { lstat, readdir, readFile } from 'fs/promises';

const root = process.argv[1];
const maxFileBytes = Number(process.argv[2]);
const maxFiles = Number(process.argv[3]);
const maxTotalBytes = Number(process.argv[4]);
let emittedEntries = 0;
let totalBytes = 0;

function emit(entry) {
  process.stdout.write(JSON.stringify(entry) + '\n');
}

function reject(path, reason) {
  emit({ type: 'reject', path, reason });
  throw new Error('Snapshot reader refused unsafe input');
}

function safeRel(path) {
  return path.split('/').filter(Boolean).join('/');
}

async function walk(abs, rel) {
  if (rel && ++emittedEntries > maxFiles) return reject(rel, 'too-many-entries');
  const stat = await lstat(abs);
  if (stat.isSymbolicLink()) return reject(rel, 'symlink');
  if (stat.isDirectory()) {
    if (rel) emit({ type: 'dir', path: rel, mode: stat.mode });
    const names = await readdir(abs);
    for (const name of names.sort()) {
      await walk(abs + '/' + name, safeRel(rel + '/' + name));
    }
    return;
  }
  if (stat.nlink && stat.nlink > 1) return reject(rel, 'hardlink');
  if (!stat.isFile()) return reject(rel, 'special-file');
  if (stat.size > maxFileBytes) return reject(rel, 'file-too-large');
  totalBytes += stat.size;
  if (totalBytes > maxTotalBytes) return reject(rel, 'too-many-bytes');
  const bytes = await readFile(abs);
  const digest = createHash('sha256').update(bytes).digest('hex');
  emit({ type: 'file', path: rel, mode: stat.mode, size: bytes.length, sha256: digest, contentBase64: bytes.toString('base64') });
}

await walk(root, '');
`;
}
