import { afterEach, describe, expect, test, setSystemTime } from 'bun:test';
import { createHash, randomUUID } from 'crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'fs/promises';
import { connect } from 'net';
import { deflateSync } from 'zlib';
import { tmpdir } from 'os';
import { join, resolve } from 'path';
import { dockerCli, type DockerRunner } from '../container/docker-exec';
import {
  BrowserObservationService,
  PLAYWRIGHT_EFFECTIVE_SECCOMP_PROFILE_DIGEST,
  PLAYWRIGHT_IMAGE,
  PLAYWRIGHT_UPSTREAM_SECCOMP_PROFILE_DIGEST,
  PLAYWRIGHT_VERSION,
  type BrowserObservationRequest,
} from './browser-observation';

const SHA_A = 'a'.repeat(40);
const SHA_B = 'b'.repeat(40);
const APP_IMAGE = `registry.example/goodword-web@sha256:${'c'.repeat(64)}`;
const APP_IMAGE_ID = `sha256:${'1'.repeat(64)}`;
const RUNNER_IMAGE = `registry.example/archon-runner@sha256:${'d'.repeat(64)}`;
const RUNNER_IMAGE_ID = `sha256:${'3'.repeat(64)}`;
const TRUSTED_VERIFIER_IMAGE = `registry.example/playwright@sha256:${'e'.repeat(64)}`;
const TRUSTED_VERIFIER_IMAGE_ID = `sha256:${'4'.repeat(64)}`;
const OFFICIAL_NO_LABEL_VERIFIER_IMAGE = `mcr.microsoft.com/playwright@sha256:${'f'.repeat(64)}`;
const OFFICIAL_NO_LABEL_VERIFIER_IMAGE_ID = `sha256:${'5'.repeat(64)}`;
const MISMATCHED_VERIFIER_IMAGE = `registry.example/playwright@sha256:${'6'.repeat(64)}`;
const MISMATCHED_VERIFIER_IMAGE_ID = `sha256:${'6'.repeat(64)}`;
const VERIFIER_IMAGE_ID = `sha256:${'2'.repeat(64)}`;
const MINIMAL_PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/fteaysAAAAASUVORK5CYII=',
  'base64'
);
const MINIMAL_ZIP = Buffer.from(
  'UEsDBBQAAAAIAFARKF1Dv6ajBAAAAAIAAAALAAAAdHJhY2UudHJhY2WrrgUAUEsBAhQDFAAAAAgAUBEoXUO/pqMEAAAAAgAAAAsAAAAAAAAAAAAAAIABAAAAAHRyYWNlLnRyYWNlUEsFBgAAAAABAAEAOQAAAC0AAAAAAA==',
  'base64'
);

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir) await rm(dir, { recursive: true, force: true });
  }
});

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

async function fixtureNodeModules(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'archon-browser-node-modules-'));
  cleanupDirs.push(root);
  const nodeModules = join(root, 'node_modules');
  await mkdir(join(nodeModules, 'playwright'), { recursive: true });
  await mkdir(join(nodeModules, 'playwright-core'), { recursive: true });
  await writeFile(
    join(nodeModules, 'playwright', 'package.json'),
    JSON.stringify({ version: PLAYWRIGHT_VERSION })
  );
  await writeFile(join(nodeModules, 'playwright-core', 'package.json'), JSON.stringify({}));
  return nodeModules;
}

async function request(
  overrides: Partial<BrowserObservationRequest> = {}
): Promise<BrowserObservationRequest> {
  return {
    runId: 'run-browser-1',
    app: {
      image: APP_IMAGE,
      commit: SHA_A,
      tree: SHA_B,
      port: 4173,
      command: ['node', 'server.js'],
    },
    verifierImage: PLAYWRIGHT_IMAGE,
    playwrightNodeModules: await fixtureNodeModules(),
    evidenceDir: join(tmpdir(), `archon-browser-import-${randomUUID()}`),
    policy: policy(),
    timeoutMs: 1000,
    ...overrides,
  };
}

function policy(): BrowserObservationRequest['policy'] {
  return {
    required: [
      {
        id: 'browser-1',
        criterion: 'Welcome appears',
        path: '/',
        assertions: [{ type: 'text', value: 'Welcome' }],
      },
    ],
  };
}

function candidateSource(
  overrides: Partial<NonNullable<BrowserObservationRequest['app']['candidateSource']>> = {}
): NonNullable<BrowserObservationRequest['app']['candidateSource']> {
  const content = Buffer.from(
    '<html><title>Goodword</title><body><input id="name"><button>Go</button><p>Welcome</p></body></html>'
  );
  const file = {
    type: 'file' as const,
    path: 'dist/index.html',
    size: content.length,
    sha256: createHash('sha256').update(content).digest('hex'),
    contentBase64: content.toString('base64'),
  };
  const files = overrides.files ?? [file];
  return {
    profile: 'static-web-http-v1',
    commit: SHA_A,
    tree: SHA_B,
    appRoot: 'dist',
    contentDigest: canonicalDigest({
      appRoot: overrides.appRoot ?? 'dist',
      commit: overrides.commit ?? SHA_A,
      files: files
        .map(entry => ({
          path: entry.path,
          sha256: entry.sha256,
          size: entry.size,
          type: entry.type,
        }))
        .sort((a, b) => compareCodepoint(a.path, b.path)),
      profile: overrides.profile ?? 'static-web-http-v1',
      tree: overrides.tree ?? SHA_B,
    }),
    ...overrides,
    files,
  };
}

function compareCodepoint(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

async function candidateRequest(
  overrides: Partial<BrowserObservationRequest> = {}
): Promise<BrowserObservationRequest> {
  return request({
    app: {
      image: RUNNER_IMAGE,
      commit: SHA_A,
      tree: SHA_B,
      port: 4173,
      candidateSource: candidateSource(),
    },
    ...overrides,
  });
}

function candidateService(docker: DockerRunner): BrowserObservationService {
  return new BrowserObservationService(docker, { trustedCandidateHelperImage: RUNNER_IMAGE });
}

function imageInspect(id: string, labels: Record<string, string>): string {
  return `${JSON.stringify({ Id: id, Config: { Labels: labels } })}\n`;
}

function rawResult(
  origin = 'http://127.0.0.1:4173',
  overrides: Record<string, unknown> = {}
): string {
  return `${JSON.stringify({
    observedOrigin: origin,
    canary: { ok: false, error: 'blocked' },
    security: securityObservation(),
    criteria: [
      {
        id: 'browser-1',
        path: '/',
        status: 'passed',
        observed_origin: origin,
        assertions: [{ type: 'text', value: 'Welcome', status: 'passed' }],
      },
    ],
    evidence: {
      screenshots: [
        {
          criterion: 'browser-1',
          path: 'browser-evidence/001-browser-1-abc.png',
          sha256: createHash('sha256').update(MINIMAL_PNG).digest('hex'),
        },
      ],
      traces: [
        {
          path: 'browser-evidence/trace.zip',
          sha256: createHash('sha256').update(MINIMAL_ZIP).digest('hex'),
        },
      ],
    },
    ...overrides,
  })}\n`;
}

function securityObservation() {
  return {
    status: {
      Seccomp: '2',
      NoNewPrivs: '1',
      CapEff: '0000000000000000',
      CapBnd: '0000000000000000',
    },
    identity: { uid: 1000, gid: 1000 },
    arch: 'arm64',
    controls: {
      mount: { blocked: true, code: 0, signal: null, errno: 1, result: -1 },
      bpf: { blocked: true, code: 0, signal: null, errno: 1, result: -1 },
      init_module: { blocked: true, code: 0, signal: null, errno: 1, result: -1 },
      chroot: { blocked: true, code: 0, signal: null, errno: 1, result: -1 },
      parent_setns: { blocked: true, code: 0, signal: null, errno: 1, result: -1 },
    },
  };
}

function jsonlFile(path: string, bytes: Buffer): string {
  return `${JSON.stringify({
    type: 'file',
    path,
    mode: 33152,
    size: bytes.length,
    sha256: createHash('sha256').update(bytes).digest('hex'),
    contentBase64: bytes.toString('base64'),
  })}\n`;
}

function snapshotJsonl(
  overrides: { png?: Buffer; zip?: Buffer; raw?: string; includePolicy?: boolean } = {}
): string {
  const png = overrides.png ?? MINIMAL_PNG;
  const zip = overrides.zip ?? MINIMAL_ZIP;
  const entries = [
    jsonlFile('raw-result.json', Buffer.from(overrides.raw ?? rawResult())),
    ...(overrides.includePolicy === false
      ? []
      : [jsonlFile('policy.sha256', Buffer.from(`${canonicalDigest(policy())}\n`))]),
    jsonlFile('browser-evidence/001-browser-1-abc.png', png),
    jsonlFile('browser-evidence/trace.zip', zip),
  ];
  return entries.join('');
}

function candidateSnapshotJsonl(source = candidateSource()): string {
  return source.files
    .map(file =>
      jsonlFile(file.path, Buffer.from(file.contentBase64, 'base64')).replace(
        /"mode":33152/,
        '"mode":33060'
      )
    )
    .join('');
}

function rawWithEvidenceDigests(png: Buffer, zip: Buffer): string {
  return rawResult('http://127.0.0.1:4173', {
    evidence: {
      screenshots: [
        {
          criterion: 'browser-1',
          path: 'browser-evidence/001-browser-1-abc.png',
          sha256: createHash('sha256').update(png).digest('hex'),
        },
      ],
      traces: [
        {
          path: 'browser-evidence/trace.zip',
          sha256: createHash('sha256').update(zip).digest('hex'),
        },
      ],
    },
  });
}

function corruptPngCrc(bytes: Buffer): Buffer {
  const copy = Buffer.from(bytes);
  copy[29] ^= 0xff;
  return copy;
}

function corruptPngIdat(bytes: Buffer): Buffer {
  const copy = Buffer.from(bytes);
  const idat = copy.indexOf(Buffer.from('IDAT'));
  copy[idat + 8] ^= 0xff;
  return copy;
}

function pngWith(
  options: {
    width?: number;
    height?: number;
    idat?: Buffer;
    includeIdat?: boolean;
    includeIend?: boolean;
    ancillary?: Buffer;
  } = {}
): Buffer {
  const width = options.width ?? 1;
  const height = options.height ?? 1;
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  ihdr[10] = 0;
  ihdr[11] = 0;
  ihdr[12] = 0;
  const chunks = [pngChunk('IHDR', ihdr)];
  if (options.ancillary) chunks.push(pngChunk('tEXt', options.ancillary));
  if (options.includeIdat !== false)
    chunks.push(pngChunk('IDAT', options.idat ?? deflateSync(Buffer.from([0, 0, 0, 0, 0]))));
  if (options.includeIend !== false) chunks.push(pngChunk('IEND', Buffer.alloc(0)));
  return Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), ...chunks]);
}

function pngChunk(type: string, data: Buffer): Buffer {
  const typeBytes = Buffer.from(type, 'ascii');
  const chunk = Buffer.alloc(12 + data.length);
  chunk.writeUInt32BE(data.length, 0);
  typeBytes.copy(chunk, 4);
  data.copy(chunk, 8);
  chunk.writeUInt32BE(testCrc32(Buffer.concat([typeBytes, data])), 8 + data.length);
  return chunk;
}

function testCrc32(bytes: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of bytes) crc = (crc >>> 8) ^ TEST_CRC32_TABLE[(crc ^ byte) & 0xff]!;
  return (crc ^ 0xffffffff) >>> 0;
}

const TEST_CRC32_TABLE = Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit++) {
    value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }
  return value >>> 0;
});

function fakeDocker(
  options: { raw?: string; snapshot?: string; candidateSnapshot?: string } = {}
): DockerRunner & {
  calls: string[][];
  owners: Map<string, string>;
} {
  const calls: string[][] = [];
  const owners = new Map<string, string>();
  const runner = (async (args: string[]) => {
    calls.push(args);
    rememberCreatedOwner(args, owners);
    return fakeDockerResponse(args, owners, options);
  }) as DockerRunner & { calls: string[][]; owners: Map<string, string> };
  runner.calls = calls;
  runner.owners = owners;
  return runner;
}

function rememberCreatedOwner(args: string[], owners: Map<string, string>): void {
  if ((args[0] === 'create' || args[0] === 'run') && args.includes('--name')) {
    owners.set(
      args[args.indexOf('--name') + 1] ?? '',
      labelValue(args, 'archon.browser-observation.owner')
    );
  }
}

function fakeDockerResponse(
  args: string[],
  owners: Map<string, string>,
  options: { raw?: string; snapshot?: string; candidateSnapshot?: string }
): { stdout: string; stderr: string } {
  const image = fakeImageInspectResponse(args);
  if (image) return image;
  if (isCandidateSnapshotReader(args)) {
    return { stdout: options.candidateSnapshot ?? candidateSnapshotJsonl(), stderr: '' };
  }
  if (args[0] === 'run' && args.includes('--entrypoint') && args.includes('node')) {
    return { stdout: options.snapshot ?? snapshotJsonl({ raw: options.raw }), stderr: '' };
  }
  if (args[0] === 'run' && args.includes('cat') && args.includes('/evidence/raw-result.json')) {
    return { stdout: options.raw ?? rawResult(), stderr: '' };
  }
  if (args[0] === 'run' && args.includes('cat') && args.includes('/evidence/policy.sha256')) {
    return { stdout: canonicalDigest(policy()) + '\n', stderr: '' };
  }
  return fakeResourceResponse(args, owners);
}

function fakeImageInspectResponse(args: string[]): { stdout: string; stderr: string } | undefined {
  const joined = args.join(' ');
  if (joined.startsWith('image inspect ' + APP_IMAGE)) {
    return imageResponse(APP_IMAGE_ID, {
      'org.opencontainers.image.revision': SHA_A,
      'archon.goodword.tree': SHA_B,
    });
  }
  if (joined.startsWith('image inspect ' + RUNNER_IMAGE)) {
    return imageResponse(RUNNER_IMAGE_ID, {
      'org.opencontainers.image.revision': 'f'.repeat(40),
      'archon.goodword.tree': 'e'.repeat(40),
    });
  }
  if (joined.startsWith('image inspect ' + TRUSTED_VERIFIER_IMAGE)) {
    return imageResponse(TRUSTED_VERIFIER_IMAGE_ID, {
      'com.microsoft.playwright.version': PLAYWRIGHT_VERSION,
    });
  }
  if (joined.startsWith('image inspect ' + OFFICIAL_NO_LABEL_VERIFIER_IMAGE)) {
    return imageResponse(OFFICIAL_NO_LABEL_VERIFIER_IMAGE_ID, {});
  }
  if (joined.startsWith('image inspect ' + MISMATCHED_VERIFIER_IMAGE)) {
    return imageResponse(MISMATCHED_VERIFIER_IMAGE_ID, {
      'com.microsoft.playwright.version': '1.59.0',
    });
  }
  if (joined.startsWith('image inspect ' + PLAYWRIGHT_IMAGE)) {
    return imageResponse(VERIFIER_IMAGE_ID, {
      'com.microsoft.playwright.version': PLAYWRIGHT_VERSION,
    });
  }
  return undefined;
}

function imageResponse(
  id: string,
  labels: Record<string, string>
): { stdout: string; stderr: string } {
  return { stdout: imageInspect(id, labels), stderr: '' };
}

function isCandidateSnapshotReader(args: string[]): boolean {
  return (
    args[0] === 'run' &&
    args.includes('-v') &&
    args.some(arg => arg.includes('archon-browser-candidate-'))
  );
}

function fakeResourceResponse(
  args: string[],
  owners: Map<string, string>
): { stdout: string; stderr: string } {
  if (args[0] === 'inspect' && args[1] === '-f') {
    if (!owners.has(args.at(-1) ?? '')) throw new Error('No such object: ' + args.at(-1));
    return { stdout: `${owners.get(args.at(-1) ?? '') ?? ''}\n`, stderr: '' };
  }
  if (args[0] === 'volume' && args[1] === 'inspect') {
    if (!owners.has(args[2] ?? ''))
      throw new Error('Error response from daemon: get ' + args[2] + ': no such volume');
    return { stdout: `${owners.get(args[2]) ?? ''}\n`, stderr: '' };
  }
  if (args[0] === 'volume' && args[1] === 'create') {
    owners.set(args.at(-1) ?? '', labelValue(args, 'archon.browser-observation.owner'));
  }
  if (args[0] === 'create' || (args[0] === 'run' && args.includes('-d'))) {
    const id = `${args[args.indexOf('--name') + 1]}-id`;
    owners.set(id, labelValue(args, 'archon.browser-observation.owner'));
    return { stdout: `${id}\n`, stderr: '' };
  }
  return { stdout: '', stderr: '' };
}

function labelValue(args: string[], key: string): string {
  for (let i = 0; i < args.length - 1; i++) {
    if (args[i] === '--label' && args[i + 1]?.startsWith(`${key}=`)) {
      return args[i + 1].slice(key.length + 1);
    }
  }
  return '';
}

async function staticServerScriptFromService(): Promise<string> {
  const docker = fakeDocker();
  await candidateService(docker).observe(await candidateRequest());
  const appRun = docker.calls.find(
    call =>
      call[0] === 'run' &&
      call.includes('--name') &&
      call[call.indexOf('--name') + 1]?.startsWith('archon-browser-app-')
  );
  const imageIndex = appRun?.indexOf(RUNNER_IMAGE_ID) ?? -1;
  const script = imageIndex >= 0 ? appRun?.[imageIndex + 2] : undefined;
  if (!script) throw new Error('candidate static server script was not captured');
  return script;
}

async function rawHttpStatus(port: number, target: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const socket = connect(port, '127.0.0.1');
    let data = '';
    socket.setTimeout(2000);
    socket.on('connect', () => socket.write(`GET ${target} HTTP/1.1\r\nHost: x\r\n\r\n`));
    socket.on('data', chunk => {
      data += chunk.toString('utf8');
      const status = data.match(/^HTTP\/1\.1 (\d{3})/)?.[1];
      if (status) {
        socket.destroy();
        resolve(Number(status));
      }
    });
    socket.on('end', () => resolve(Number(data.match(/^HTTP\/1\.1 (\d{3})/)?.[1])));
    socket.on('timeout', () => reject(new Error('raw HTTP request timed out')));
    socket.on('error', reject);
  });
}

async function withStaticServer(
  script: string,
  root: string,
  run: (port: number) => Promise<void>
): Promise<void> {
  const port = 32000 + Math.floor(Math.random() * 10000);
  const proc = Bun.spawn(['node', '-e', script, String(port), root], {
    stdout: 'pipe',
    stderr: 'pipe',
  });
  try {
    for (let attempt = 0; attempt < 20; attempt++) {
      try {
        await fetch(`http://127.0.0.1:${port}/`, { signal: AbortSignal.timeout(250) });
        break;
      } catch {
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    }
    await run(port);
  } finally {
    proc.kill();
    await proc.exited.catch(() => undefined);
  }
}

describe('BrowserObservationService command construction', () => {
  test('starts app and verifier in one network-none pod with isolated volumes and immutable image ids', async () => {
    const docker = fakeDocker();
    const service = candidateService(docker);
    const result = await service.observe(await request());

    expect(result.status).toBe('passed');
    expect(result.authority).toBe('none');
    expect(result.evidence.screenshots[0]?.controllerPath).toContain('browser-evidence');
    const joined = docker.calls.map(call => call.join(' ')).join('\n');
    expect(joined).toContain('--network none');
    expect(joined).toContain('--network container:');
    expect(joined).toContain('--cap-drop ALL');
    expect(joined).toContain('--read-only');
    expect(joined).toContain('--user 1000:1000');
    expect(joined).toContain('--shm-size 256m');
    expect(joined).toContain('--security-opt no-new-privileges');
    expect(joined).toContain('--security-opt seccomp=');
    expect(PLAYWRIGHT_UPSTREAM_SECCOMP_PROFILE_DIGEST).toBe(
      'cc3e61cabda6bbc1e53e54d27ba4d55a9d3be829b6dd1a596f4a7b31b1cc7849'
    );
    expect(PLAYWRIGHT_EFFECTIVE_SECCOMP_PROFILE_DIGEST).toBe(
      '153cb94e0bb74823af2e2e4e8548fc5a0895639e4bf16fa98ecac258b5641019'
    );
    expect(joined).toContain('--pids-limit 256');
    expect(joined).toContain('type=volume,src=archon-browser-defs-');
    expect(joined).toContain('dst=/defs,readonly');
    expect(joined).toContain('dst=/evidence');
    expect(joined).toContain(APP_IMAGE_ID);
    expect(joined).toContain(VERIFIER_IMAGE_ID);
    expect(joined).not.toContain('archon.browser-observation.owner=controller');
    expect(joined).not.toContain('/var/run/docker.sock');
    expect(joined).not.toContain(`${process.env.HOME ?? '/'}:`);
    expect(joined).not.toContain('--add-host=host.docker.internal:host-gateway');
    expect(joined).not.toContain('--ipc=host');
    expect(joined).not.toContain('SYS_ADMIN');
    expect(docker.calls.some(call => call[0] === 'inspect' && call[1] === '-f')).toBe(true);
    expect(docker.calls.filter(call => call[0] === 'volume' && call[1] === 'rm').length).toBe(2);
  });

  test('serves controller-sealed candidate source from a read-only candidate volume', async () => {
    const docker = fakeDocker();
    const service = candidateService(docker);
    const result = await service.observe(await candidateRequest());

    expect(result.status).toBe('passed');
    expect(result.authority).toBe('none');
    expect(result.app.imageId).toBe(RUNNER_IMAGE_ID);
    expect(result.app.commit).toBe(SHA_A);
    expect(result.app.tree).toBe(SHA_B);
    const appRun = docker.calls.find(
      call =>
        call[0] === 'run' &&
        call.includes('--name') &&
        call[call.indexOf('--name') + 1]?.startsWith('archon-browser-app-')
    );
    expect(appRun).toBeDefined();
    expect(appRun?.join(' ')).toContain('dst=/candidate,readonly');
    expect(appRun?.join(' ')).toContain('static-web-http-v1');
    expect(appRun).not.toContain('node server.js');
    expect(docker.calls.some(call => call[0] === 'cp' && call[2]?.endsWith(':/candidate'))).toBe(
      true
    );
    expect(docker.calls.filter(call => call[0] === 'volume' && call[1] === 'rm').length).toBe(3);
  });

  test('rejects candidate volume drift after Docker staging verification', async () => {
    const drifted = candidateSource({
      files: [
        {
          ...candidateSource().files[0]!,
          contentBase64: Buffer.from('mutated').toString('base64'),
          size: 7,
          sha256: createHash('sha256').update('mutated').digest('hex'),
        },
      ],
    });
    const docker = fakeDocker({ candidateSnapshot: candidateSnapshotJsonl(drifted) });
    const service = candidateService(docker);

    await expect(service.observe(await candidateRequest())).rejects.toThrow(/volume file drifted/);
    expect(docker.calls.some(call => call[0] === 'run' && call.includes('-v'))).toBe(true);
    expect(docker.calls.filter(call => call[0] === 'volume' && call[1] === 'rm').length).toBe(3);
  });

  test('rejects extra empty directories in the staged candidate volume', async () => {
    const docker = fakeDocker({
      candidateSnapshot: `${JSON.stringify({ type: 'dir', path: 'dist' })}\n${JSON.stringify({
        type: 'dir',
        path: 'dist/empty',
      })}\n${candidateSnapshotJsonl()}`,
    });
    const service = candidateService(docker);

    await expect(service.observe(await candidateRequest())).rejects.toThrow(
      /directory set drifted|unexpected directory/
    );
    expect(docker.calls.filter(call => call[0] === 'volume' && call[1] === 'rm').length).toBe(3);
  });

  test('rejects candidate source byte, type, digest, path, and collision drift before Docker', async () => {
    const valid = candidateSource();
    const first = valid.files[0]!;
    const mutations = [
      {
        source: { ...valid, files: [{ ...first, type: 'symlink' as 'file' }] },
        pattern: /only supports files/,
      },
      {
        source: { ...valid, files: [{ ...first, sha256: '0'.repeat(64) }] },
        pattern: /size or digest mismatch/,
      },
      { source: { ...valid, contentDigest: '1'.repeat(64) }, pattern: /content digest/ },
      {
        source: { ...valid, files: [first, { ...first }] },
        pattern: /duplicate path/,
      },
      {
        source: { ...valid, files: [{ ...first, path: '../dist/index.html' }] },
        pattern: /traverse|relative POSIX/,
      },
      {
        source: { ...valid, files: [{ ...first, path: 'public/index.html' }] },
        pattern: /outside appRoot/,
      },
    ];
    for (const mutation of mutations) {
      const docker = fakeDocker();
      const service = candidateService(docker);
      await expect(
        service.observe(
          await candidateRequest({
            app: {
              image: RUNNER_IMAGE,
              commit: SHA_A,
              tree: SHA_B,
              port: 4173,
              candidateSource: mutation.source,
            },
          })
        )
      ).rejects.toThrow(mutation.pattern);
      expect(docker.calls.length).toBe(0);
    }
  });

  test('rejects candidate source command override and descriptor binding drift', async () => {
    const service = candidateService(fakeDocker());
    await expect(
      service.observe(
        await candidateRequest({
          app: {
            image: RUNNER_IMAGE,
            commit: SHA_A,
            tree: SHA_B,
            port: 4173,
            command: ['sh', '-lc', 'evil'],
            candidateSource: candidateSource(),
          },
        })
      )
    ).rejects.toThrow(/fixed controller command/);

    await expect(
      service.observe(
        await candidateRequest({
          app: {
            image: RUNNER_IMAGE,
            commit: SHA_A,
            tree: SHA_B,
            port: 4173,
            candidateSource: candidateSource({ commit: '9'.repeat(40) }),
          },
        })
      )
    ).rejects.toThrow(/commit\/tree binding/);
  });

  test('requires a controller-trusted immutable helper image for candidate mode', async () => {
    const noTrusted = fakeDocker();
    await expect(
      new BrowserObservationService(noTrusted).observe(await candidateRequest())
    ).rejects.toThrow(/controller-trusted helper image/);
    expect(noTrusted.calls.length).toBe(0);

    const wrongTrusted = fakeDocker();
    await expect(
      new BrowserObservationService(wrongTrusted, {
        trustedCandidateHelperImage: `registry.example/archon-runner@sha256:${'e'.repeat(64)}`,
      }).observe(await candidateRequest())
    ).rejects.toThrow(/does not match controller-trusted helper image/);
    expect(wrongTrusted.calls.length).toBe(0);
  });

  test('freezes candidate helper trust against post-construction option mutation', async () => {
    const options = { trustedCandidateHelperImage: RUNNER_IMAGE };
    const service = new BrowserObservationService(fakeDocker(), options);
    options.trustedCandidateHelperImage = `registry.example/archon-runner@sha256:${'e'.repeat(64)}`;

    await expect(service.observe(await candidateRequest())).resolves.toMatchObject({
      app: { imageId: RUNNER_IMAGE_ID },
      authority: 'none',
      status: 'passed',
    });
  });

  test('rejects appRoot-only candidate source drift before Docker', async () => {
    const valid = candidateSource();
    const docker = fakeDocker();
    const service = candidateService(docker);

    await expect(
      service.observe(
        await candidateRequest({
          app: {
            image: RUNNER_IMAGE,
            commit: SHA_A,
            tree: SHA_B,
            port: 4173,
            candidateSource: { ...valid, appRoot: '.' },
          },
        })
      )
    ).rejects.toThrow(/content digest/);
    expect(docker.calls.length).toBe(0);
  });

  test('fixed static candidate server rejects malformed encoding and traversal without crashing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'archon-static-server-'));
    cleanupDirs.push(root);
    await writeFile(join(root, 'index.html'), '<p>Welcome</p>');
    const script = await staticServerScriptFromService();

    await withStaticServer(script, root, async port => {
      expect(await rawHttpStatus(port, '/')).toBe(200);
      expect(await rawHttpStatus(port, '/%')).toBe(400);
      expect(await rawHttpStatus(port, '/%00')).toBe(400);
      expect(await rawHttpStatus(port, '/..%2fsecret')).toBe(403);
      expect(await rawHttpStatus(port, '/')).toBe(200);
    });
  });

  test('cleans candidate volume after candidate app creation acknowledgement is lost', async () => {
    const base = fakeDocker();
    const controller = new AbortController();
    let createdName = '';
    const service = new BrowserObservationService(
      async (args, options) => {
        const result = await base(args, options);
        const name = args.includes('--name') ? (args[args.indexOf('--name') + 1] ?? '') : '';
        if (!createdName && args[0] === 'run' && name.startsWith('archon-browser-app-')) {
          createdName = name;
          base.owners.set(createdName, labelValue(args, 'archon.browser-observation.owner'));
          controller.abort();
          throw new Error('Docker creation acknowledgement lost');
        }
        return result;
      },
      { trustedCandidateHelperImage: RUNNER_IMAGE }
    );

    await expect(
      service.observe(await candidateRequest({ signal: controller.signal }))
    ).rejects.toThrow();
    expect(createdName).not.toBe('');
    expect(base.calls.some(call => call[0] === 'rm' && call.includes(createdName))).toBe(true);
    expect(base.calls.filter(call => call[0] === 'volume' && call[1] === 'rm').length).toBe(3);
  });

  test('rejects mutable, short image ids, and label-mismatched app image descriptors', async () => {
    const service = new BrowserObservationService(fakeDocker());
    await expect(
      service.observe(
        await request({ app: { ...(await request()).app, image: 'goodword:latest' } })
      )
    ).rejects.toThrow(/immutable/);

    const badIdService = new BrowserObservationService(async args => {
      if (args[0] === 'image') return { stdout: imageInspect('sha256:1234', {}), stderr: '' };
      return { stdout: '', stderr: '' };
    });
    await expect(badIdService.observe(await request())).rejects.toThrow(/immutable image id/);

    const badLabelService = new BrowserObservationService(async (args, options) => {
      if (args[0] === 'image' && args[2] === APP_IMAGE) {
        return {
          stdout: imageInspect(APP_IMAGE_ID, {
            'org.opencontainers.image.revision': 'f'.repeat(40),
            'archon.goodword.tree': SHA_B,
          }),
          stderr: '',
        };
      }
      return fakeDocker()(args, options);
    });
    await expect(badLabelService.observe(await request())).rejects.toThrow(/labels do not match/);
  });

  test('rejects unsupported Playwright images instead of falling back to latest', async () => {
    const service = new BrowserObservationService(fakeDocker());
    await expect(
      service.observe(await request({ verifierImage: 'mcr.microsoft.com/playwright:latest' }))
    ).rejects.toThrow(/controller-trusted verifier image|Unsupported Playwright image/);
  });

  test('rejects custom immutable verifier images without controller trust', async () => {
    const untrusted = fakeDocker();
    await expect(
      new BrowserObservationService(untrusted).observe(
        await request({ verifierImage: OFFICIAL_NO_LABEL_VERIFIER_IMAGE })
      )
    ).rejects.toThrow(/controller-trusted verifier image/);
    expect(untrusted.calls.length).toBe(0);

    const trustedNoLabel = fakeDocker();
    await expect(
      new BrowserObservationService(trustedNoLabel, {
        trustedVerifierImage: OFFICIAL_NO_LABEL_VERIFIER_IMAGE,
      }).observe(await request({ verifierImage: OFFICIAL_NO_LABEL_VERIFIER_IMAGE }))
    ).resolves.toMatchObject({
      verifier: {
        image: OFFICIAL_NO_LABEL_VERIFIER_IMAGE,
        imageId: OFFICIAL_NO_LABEL_VERIFIER_IMAGE_ID,
      },
      authority: 'none',
      status: 'passed',
    });
  });

  test('rejects trusted verifier images when a present version label mismatches', async () => {
    const docker = fakeDocker();
    await expect(
      new BrowserObservationService(docker, {
        trustedVerifierImage: MISMATCHED_VERIFIER_IMAGE,
      }).observe(await request({ verifierImage: MISMATCHED_VERIFIER_IMAGE }))
    ).rejects.toThrow(/Playwright image version mismatch/);
    expect(docker.calls.some(call => call[0] === 'create' || call[0] === 'run')).toBe(false);
  });

  test('freezes verifier image trust against post-construction option mutation', async () => {
    const options = { trustedVerifierImage: TRUSTED_VERIFIER_IMAGE };
    const service = new BrowserObservationService(fakeDocker(), options);
    options.trustedVerifierImage = OFFICIAL_NO_LABEL_VERIFIER_IMAGE;

    await expect(
      service.observe(await request({ verifierImage: TRUSTED_VERIFIER_IMAGE }))
    ).resolves.toMatchObject({
      verifier: { image: TRUSTED_VERIFIER_IMAGE, imageId: TRUSTED_VERIFIER_IMAGE_ID },
      authority: 'none',
      status: 'passed',
    });
  });

  test('rejects malformed browser policy before container startup', async () => {
    const docker = fakeDocker();
    const service = new BrowserObservationService(docker);
    await expect(
      service.observe(
        await request({
          policy: {
            required: [
              { id: 'bad', criterion: 'bad', path: 'http://evil.example', assertions: [] },
            ],
          },
        })
      )
    ).rejects.toThrow(/same-origin|frozen assertions/);
    expect(docker.calls.length).toBe(0);
  });

  test('stops the owned verifier before importing failure evidence after a lost attachment', async () => {
    const docker = fakeDocker();
    const service = new BrowserObservationService(async (args, options) => {
      const result = await docker(args, options);
      if (args[0] === 'start' && args[1] === '-a') throw new Error('attach connection lost');
      return result;
    });
    await expect(service.observe(await request())).rejects.toThrow('Browser verifier failed');
    const start = docker.calls.find(args => args[0] === 'start' && args[1] === '-a');
    if (!start?.[2]) throw new Error('fixture did not start verifier');
    const removal = docker.calls.findIndex(args => args[0] === 'rm' && args.includes(start[2]));
    const snapshot = docker.calls.findIndex(
      args => args[0] === 'run' && args.includes('--entrypoint') && args.includes('node')
    );
    expect(removal).toBeGreaterThanOrEqual(0);
    expect(snapshot).toBeGreaterThan(removal);
  });

  test('bounds every docker call by the shared browser observation deadline', async () => {
    const base = fakeDocker();
    const timeouts: number[] = [];
    const service = new BrowserObservationService(async (args, options) => {
      if (options?.timeout !== undefined) timeouts.push(options.timeout);
      return base(args, options);
    });

    await service.observe(await request({ totalTimeoutMs: 5_000 }));

    expect(timeouts.length).toBeGreaterThan(0);
    expect(timeouts.every(timeout => timeout >= 1 && timeout <= 5_000)).toBe(true);
    expect(Math.min(...timeouts)).toBeLessThan(5_000);
  });

  test('propagates controller cancellation and still removes owned resources', async () => {
    const base = fakeDocker();
    const controller = new AbortController();
    let verifierSawSignal = false;
    const service = new BrowserObservationService(async (args, options) => {
      const result = await base(args, options);
      if (args[0] === 'start' && args[1] === '-a') {
        verifierSawSignal = options?.signal === controller.signal;
        controller.abort();
        throw new Error('controller cancelled verifier attachment');
      }
      return result;
    });

    await expect(
      service.observe(await request({ signal: controller.signal, totalTimeoutMs: 90_000 }))
    ).rejects.toThrow(/Browser verifier failed/);
    expect(verifierSawSignal).toBe(true);
    expect(base.calls.filter(call => call[0] === 'volume' && call[1] === 'rm').length).toBe(2);
  });

  for (const stage of ['snapshot', 'cleanup'] as const) {
    for (const interruption of ['cancel', 'deadline'] as const) {
      test(`refuses ${interruption} after ${stage} instead of returning passed evidence`, async () => {
        const base = fakeDocker();
        const controller = new AbortController();
        const service = new BrowserObservationService(async (args, options) => {
          const result = await base(args, options);
          if (
            (stage === 'snapshot' &&
              args[0] === 'run' &&
              args.includes('--entrypoint') &&
              args.includes('node')) ||
            (stage === 'cleanup' && args[0] === 'volume' && args[1] === 'rm')
          ) {
            if (interruption === 'cancel') controller.abort();
            else setSystemTime(new Date(Date.now() + 120_000));
          }
          return result;
        });
        try {
          await expect(
            service.observe(await request({ signal: controller.signal, totalTimeoutMs: 90_000 }))
          ).rejects.toThrow(/cancelled|deadline exceeded/);
          expect(base.calls.filter(call => call[0] === 'volume' && call[1] === 'rm').length).toBe(
            2
          );
        } finally {
          setSystemTime();
        }
      });
    }
  }

  for (const role of ['initializer', 'seed', 'pod', 'app', 'verifier', 'snapshot'] as const) {
    test(`drains owned ${role} after creation succeeds but acknowledgement is lost`, async () => {
      const base = fakeDocker();
      const controller = new AbortController();
      let createdName = '';
      const service = new BrowserObservationService(async (args, options) => {
        const result = await base(args, options);
        const name = args.includes('--name') ? (args[args.indexOf('--name') + 1] ?? '') : '';
        const selected = matchesBrowserContainerRole(role, name, args);
        if (!createdName && (args[0] === 'create' || args[0] === 'run') && selected) {
          createdName = name || 'unnamed-leaked-container';
          base.owners.set(createdName, labelValue(args, 'archon.browser-observation.owner'));
          controller.abort();
          throw new Error('Docker creation acknowledgement lost');
        }
        return result;
      });
      await expect(service.observe(await request({ signal: controller.signal }))).rejects.toThrow();
      expect(createdName).not.toBe('');
      expect(base.calls.some(call => call[0] === 'rm' && call.includes(createdName))).toBe(true);
      expect(base.calls.some(call => call[0] === 'rm' && call.includes('foreign-sibling'))).toBe(
        false
      );
    });
  }

  test('preserves early creation error when exact fixture volumes do not exist', async () => {
    const base = fakeDocker();
    const service = new BrowserObservationService(async (args, options) => {
      if (args[0] === 'volume' && args[1] === 'create') throw new Error('volume creation failed');
      return base(args, options);
    });
    await expect(service.observe(await request())).rejects.toThrow('volume creation failed');
    expect(base.calls.filter(call => call[0] === 'volume' && call[1] === 'rm')).toHaveLength(0);
  });

  test('requires verifier policy digest file instead of substituting expected digest', async () => {
    const service = new BrowserObservationService(
      fakeDocker({ snapshot: snapshotJsonl({ includePolicy: false }) })
    );
    await expect(service.observe(await request())).rejects.toThrow(/policy\.sha256/);
  });

  test('rejects invalid evidence bytes, empty execution, missing security, and assertion mismatch', async () => {
    const wrongLengthPng = pngWith({ idat: deflateSync(Buffer.from([0])) });
    const corruptCompressedPng = pngWith({ idat: Buffer.from('789c0000', 'hex') });
    const excessiveDimensionPng = pngWith({
      width: 16_777_216,
      idat: deflateSync(Buffer.from([0])),
    });
    const missingIdatPng = pngWith({
      includeIdat: false,
      ancillary: Buffer.from('missing-idat-padding'),
    });
    const missingIendPng = pngWith({ includeIend: false });
    const truncatedIdatPng = pngWith().subarray(0, pngWith().length - 6);
    const invalidOrderPng = pngWith({ ancillary: Buffer.from('before') });
    for (const [name, png, pattern] of [
      ['magic-only', Buffer.from('89504e470d0a1a0a', 'hex'), /not a PNG/],
      ['truncated', MINIMAL_PNG.subarray(0, 20), /not a PNG|truncated|chunk/],
      ['corrupt-crc', corruptPngCrc(MINIMAL_PNG), /CRC/],
      ['corrupt-idat-crc', corruptPngIdat(MINIMAL_PNG), /CRC/],
      ['crc-valid-wrong-length', wrongLengthPng, /pixel data length/],
      ['crc-valid-corrupt-compressed', corruptCompressedPng, /pixel data is invalid/],
      ['cap-dimensions', excessiveDimensionPng, /dimensions exceed controller limits/],
      ['missing-idat', missingIdatPng, /missing required chunks/],
      ['missing-iend', missingIendPng, /missing required chunks/],
      ['truncated-idat-or-iend', truncatedIdatPng, /truncated|chunk|trailing|missing/],
      [
        'invalid-chunk-order',
        Buffer.concat([invalidOrderPng, pngChunk('tEXt', Buffer.from('after'))]),
        /trailing data|chunk order/,
      ],
    ] as const) {
      await expect(
        new BrowserObservationService(
          fakeDocker({
            snapshot: snapshotJsonl({ raw: rawWithEvidenceDigests(png, MINIMAL_ZIP), png }),
          })
        ).observe(await request()),
        name
      ).rejects.toThrow(pattern);
    }

    await expect(
      new BrowserObservationService(
        fakeDocker({
          snapshot: snapshotJsonl({
            raw: rawWithEvidenceDigests(MINIMAL_PNG, Buffer.from('PKgarbage')),
            zip: Buffer.from('PKgarbage'),
          }),
        })
      ).observe(await request())
    ).rejects.toThrow(/ZIP/);

    await expect(
      new BrowserObservationService(
        fakeDocker({ raw: rawResult('http://127.0.0.1:4173', { criteria: [] }) })
      ).observe(await request())
    ).rejects.toThrow(/did not execute every required criterion/);

    await expect(
      new BrowserObservationService(
        fakeDocker({ raw: rawResult('http://127.0.0.1:4173', { security: undefined }) })
      ).observe(await request())
    ).rejects.toThrow(/kernel security status/);

    await expect(
      new BrowserObservationService(
        fakeDocker({
          raw: rawResult('http://127.0.0.1:4173', {
            criteria: [
              {
                id: 'browser-1',
                path: '/',
                status: 'passed',
                observed_origin: 'http://127.0.0.1:4173',
                assertions: [],
              },
            ],
          }),
        })
      ).observe(await request())
    ).rejects.toThrow(/assertion count mismatch/);
  }, 15_000);

  test('rejects security probes that never executed or returned unrelated errors', async () => {
    for (const control of [
      { blocked: true, code: 127, signal: null, errno: 1, result: -1 },
      { blocked: true, code: 0, signal: null, errno: 22, result: -1 },
    ]) {
      const security = securityObservation();
      security.controls.mount = control;
      await expect(
        new BrowserObservationService(
          fakeDocker({
            raw: rawResult(undefined, { security }),
          })
        ).observe(await request())
      ).rejects.toThrow(/security control/);
    }
  });

  test('rejects absent evidence and contradictory successful observations', async () => {
    const row = {
      id: 'browser-1',
      path: '/',
      status: 'passed',
      observed_origin: 'http://127.0.0.1:4173',
      assertions: [{ type: 'text', value: 'Welcome', status: 'passed' }],
    };
    for (const override of [
      { observedOrigin: undefined },
      { canary: undefined },
      { evidence: undefined },
      { evidence: { screenshots: [], traces: [] } },
      { criteria: [{ ...row, path: '/wrong' }] },
      { criteria: [{ ...row, observed_origin: undefined }] },
      {
        criteria: [{ ...row, assertions: [{ type: 'text', value: 'Welcome', status: 'failed' }] }],
      },
      {
        criteria: [
          { ...row, assertions: [{ type: 'text', value: 'Welcome', status: 'invented' }] },
        ],
      },
    ]) {
      const service = new BrowserObservationService(
        fakeDocker({ raw: rawResult(undefined, override) })
      );
      await expect(service.observe(await request())).rejects.toThrow();
    }
  }, 20_000);

  test('rejects wrong observed origin, skipped criteria, and reachable host canary', async () => {
    const service = new BrowserObservationService(
      fakeDocker({
        raw: rawResult('http://127.0.0.1:9999', {
          canary: { ok: true },
          criteria: [
            {
              id: 'browser-1',
              path: '/',
              status: 'skipped',
              observed_origin: 'http://127.0.0.1:9999',
            },
          ],
        }),
      })
    );
    await expect(service.observe(await request())).rejects.toThrow(
      /wrong application origin|canary|skipped/
    );
  });
});

function matchesBrowserContainerRole(role: string, name: string, args: string[]): boolean {
  switch (role) {
    case 'initializer':
      return args.includes('CHOWN');
    case 'seed':
      return name.endsWith('-seed');
    case 'pod':
      return name.startsWith('archon-browser-pod-');
    case 'app':
      return name.startsWith('archon-browser-app-');
    case 'verifier':
      return name.startsWith('archon-browser-verifier-') && !name.endsWith('-seed');
    case 'snapshot':
      return args.includes('--entrypoint') && args.includes('node');
    default:
      throw new Error('Unknown browser fixture role');
  }
}

describe('actual Docker browser observation fixture', () => {
  test.skipIf(process.env.ARCHON_RUN_ACTUAL_DOCKER_BROWSER_CANDIDATE !== '1')(
    'serves sealed static candidate source from a read-only volume without an app Dockerfile',
    async () => {
      const nodeModules = process.env.ARCHON_PLAYWRIGHT_NODE_MODULES;
      const runnerImage = process.env.ARCHON_CONTAINER_TEST_IMAGE;
      if (!nodeModules) throw new Error('ARCHON_PLAYWRIGHT_NODE_MODULES is required.');
      if (!runnerImage) throw new Error('ARCHON_CONTAINER_TEST_IMAGE is required.');
      await docker(['version', '--format', '{{.Server.Version}}'], 10_000);
      await ensureDockerImage();
      const root = await mkdtemp(join(tmpdir(), 'archon-browser-candidate-docker-'));
      cleanupDirs.push(root);
      const evidenceDir = join(root, 'controller-evidence');
      const html = Buffer.from(
        `<html><title>Goodword</title><body><input id="name"><button onclick="document.getElementById('result').textContent='Hello Archon'">Go</button><p id="result"></p></body></html>`
      );
      const file = {
        type: 'file' as const,
        path: 'dist/index.html',
        size: html.length,
        sha256: createHash('sha256').update(html).digest('hex'),
        contentBase64: html.toString('base64'),
      };
      const service = new BrowserObservationService(undefined, {
        trustedCandidateHelperImage: runnerImage,
      });
      const result = await service.observe(
        await request({
          runId: `candidate-${randomUUID().slice(0, 8)}`,
          app: {
            image: runnerImage,
            commit: SHA_A,
            tree: SHA_B,
            port: 4173,
            candidateSource: candidateSource({ files: [file] }),
          },
          playwrightNodeModules: resolve(nodeModules),
          evidenceDir,
          timeoutMs: 5000,
          canaryUrl: 'http://host.docker.internal/',
          policy: {
            required: [
              {
                id: 'browser-candidate-interaction',
                criterion: 'Fill input, click Go, and observe result text',
                path: '/',
                assertions: [
                  { type: 'fill', value: '#name', text: 'Archon' },
                  { type: 'click', value: 'button' },
                  { type: 'text', value: 'Hello Archon' },
                ],
              },
            ],
          },
        })
      );
      if (result.status !== 'passed') throw new Error(JSON.stringify(result.criteria, null, 2));
      expect(result.status).toBe('passed');
      expect(result.authority).toBe('none');
      expect(result.app.image).toBe(runnerImage);
      expect(result.criteria[0]?.assertions?.map(assertion => assertion.status)).toEqual([
        'passed',
        'passed',
        'passed',
      ]);
      const survivors = await docker([
        'ps',
        '-aq',
        '--filter',
        `label=archon.browser-observation.run=${result.runId}`,
      ]);
      expect(survivors.trim()).toBe('');
      const volumes = await docker([
        'volume',
        'ls',
        '-q',
        '--filter',
        `label=archon.browser-observation.run=${result.runId}`,
      ]);
      expect(volumes.trim()).toBe('');
    },
    240_000
  );

  test.skipIf(process.env.ARCHON_RUN_ACTUAL_DOCKER_BROWSER !== '1')(
    'executes Chromium sandboxed in cap-dropped verifier and imports evidence before cleanup',
    async () => {
      const nodeModules = process.env.ARCHON_PLAYWRIGHT_NODE_MODULES;
      if (!nodeModules) throw new Error('ARCHON_PLAYWRIGHT_NODE_MODULES is required.');
      const playwrightDigestRef = await ensureDockerImage();
      const root = await mkdtemp(join(tmpdir(), 'archon-browser-docker-'));
      cleanupDirs.push(root);
      const imageTag = `archon-browser-app-fixture:${randomUUID()}`;
      let imageBuilt = false;
      const evidenceDir = join(root, 'controller-evidence');
      try {
        await writeFile(
          join(root, 'Dockerfile'),
          `FROM ${playwrightDigestRef}\n` +
            `LABEL org.opencontainers.image.revision=${SHA_A}\n` +
            `LABEL archon.goodword.tree=${SHA_B}\n` +
            'RUN mkdir -p /app && chown 1000:1000 /app\n' +
            'USER 1000:1000\n' +
            'WORKDIR /app\n' +
            'COPY --chown=1000:1000 index.html /app/index.html\n' +
            'CMD ["python3", "-m", "http.server", "4173", "--bind", "127.0.0.1"]\n',
          'utf8'
        );
        await writeFile(
          join(root, 'index.html'),
          '<html><title>Goodword</title><body><input id="name"><button onclick="document.getElementById(\'result\').textContent=\'Hello Archon\'">Go</button><p id="result"></p></body></html>',
          'utf8'
        );
        await docker(['build', '-t', imageTag, root], 180_000);
        imageBuilt = true;
        const appImageId = (
          await docker(['image', 'inspect', imageTag, '--format', '{{.Id}}'])
        ).trim();
        const service = new BrowserObservationService();
        const result = await service.observe(
          await request({
            runId: `docker-${randomUUID().slice(0, 8)}`,
            app: {
              image: appImageId,
              commit: SHA_A,
              tree: SHA_B,
              port: 4173,
            },
            playwrightNodeModules: resolve(nodeModules),
            evidenceDir,
            timeoutMs: 5000,
            canaryUrl: 'http://host.docker.internal/',
            policy: {
              required: [
                {
                  id: 'browser-interaction',
                  criterion: 'Fill input, click Go, and observe result text',
                  path: '/',
                  assertions: [
                    { type: 'fill', value: '#name', text: 'Archon' },
                    { type: 'click', value: 'button' },
                    { type: 'text', value: 'Hello Archon' },
                  ],
                },
              ],
            },
          })
        );
        if (result.status !== 'passed') throw new Error(JSON.stringify(result.criteria, null, 2));
        expect(result.status).toBe('passed');
        expect(result.authority).toBe('none');
        expect(result.criteria[0]?.assertions?.[0]?.status).toBe('passed');
        expect(result.security.identity.uid).toBe(1000);
        expect(result.security.identity.gid).toBe(1000);
        expect(['x64', 'arm64']).toContain(result.security.arch);
        expect(result.security.status.Seccomp).toBe('2');
        expect(result.security.status.NoNewPrivs).toBe('1');
        expect(result.security.status.CapEff).toMatch(/^0+$/);
        expect(result.security.status.CapBnd).toMatch(/^0+$/);
        expect(result.security.controls.mount?.blocked).toBe(true);
        expect(result.security.controls.bpf?.blocked).toBe(true);
        expect(result.security.controls.init_module?.blocked).toBe(true);
        expect(result.security.controls.chroot?.blocked).toBe(true);
        expect(result.security.controls.parent_setns?.blocked).toBe(true);
        expect(result.observedOrigin).toBe('http://127.0.0.1:4173');
        expect(result.evidence.screenshots.length).toBe(1);
        expect(result.evidence.traces.length).toBe(1);
        for (const entry of [...result.evidence.screenshots, ...result.evidence.traces]) {
          expect(entry.controllerPath.startsWith(evidenceDir)).toBe(true);
          expect(await exists(entry.controllerPath)).toBe(true);
          expect(
            createHash('sha256')
              .update(await readFile(entry.controllerPath))
              .digest('hex')
          ).toBe(entry.sha256);
        }
        const survivors = await docker([
          'ps',
          '-aq',
          '--filter',
          `label=archon.browser-observation.run=${result.runId}`,
        ]);
        expect(survivors.trim()).toBe('');
        const volumes = await docker([
          'volume',
          'ls',
          '-q',
          '--filter',
          `label=archon.browser-observation.run=${result.runId}`,
        ]);
        expect(volumes.trim()).toBe('');
        await verifyActualCreationCancellation(nodeModules, appImageId, root);
      } finally {
        if (imageBuilt) await docker(['image', 'rm', '-f', imageTag], 60_000);
      }
    },
    240_000
  );
});

async function verifyActualCreationCancellation(
  nodeModules: string,
  appImageId: string,
  root: string
): Promise<void> {
  const runId = `cancel-${randomUUID()}`;
  const siblingName = `archon-browser-sibling-${randomUUID()}`;
  const siblingOwner = randomUUID();
  const controller = new AbortController();
  let cancelledName = '';
  try {
    await docker([
      'run',
      '-d',
      '--name',
      siblingName,
      '--label',
      `archon.browser-observation.owner=${siblingOwner}`,
      '--network',
      'none',
      '--user',
      '1000:1000',
      '--read-only',
      '--cap-drop',
      'ALL',
      '--security-opt',
      'no-new-privileges',
      '--memory',
      '64m',
      '--cpus',
      '0.25',
      '--pids-limit',
      '32',
      appImageId,
      'sleep',
      '300',
    ]);
    const service = new BrowserObservationService(async (args, options) => {
      const result = await dockerCli(args, options);
      const name = args[args.indexOf('--name') + 1] ?? '';
      if (args[0] === 'run' && args.includes('--name') && name.startsWith('archon-browser-app-')) {
        cancelledName = name;
        controller.abort();
        throw new Error('Injected lost acknowledgement after real Docker app creation');
      }
      return result;
    });
    await expect(
      service.observe(
        await request({
          runId,
          signal: controller.signal,
          app: { image: appImageId, commit: SHA_A, tree: SHA_B, port: 4173 },
          playwrightNodeModules: resolve(nodeModules),
          evidenceDir: join(root, 'cancelled-evidence'),
        })
      )
    ).rejects.toThrow('lost acknowledgement');
    expect(cancelledName).not.toBe('');
    expect(
      (
        await docker(['ps', '-aq', '--filter', `label=archon.browser-observation.run=${runId}`])
      ).trim()
    ).toBe('');
    expect(
      (
        await docker([
          'volume',
          'ls',
          '-q',
          '--filter',
          `label=archon.browser-observation.run=${runId}`,
        ])
      ).trim()
    ).toBe('');
    expect((await docker(['inspect', '-f', '{{.State.Running}}', siblingName])).trim()).toBe(
      'true'
    );
  } finally {
    await removeActualSiblingFixture(siblingName, siblingOwner);
  }
}

async function removeActualSiblingFixture(
  siblingName: string,
  siblingOwner: string
): Promise<void> {
  const owner = await docker([
    'inspect',
    '-f',
    '{{index .Config.Labels "archon.browser-observation.owner"}}',
    siblingName,
  ]).catch(error => {
    if (/no such (container|object):/i.test(String(error))) return undefined;
    throw error;
  });
  if (owner !== undefined) {
    if (owner.trim() !== siblingOwner)
      throw new Error('Refusing to remove non-owned browser sibling fixture');
    await docker(['rm', '-f', siblingName]);
  }
}

async function ensureDockerImage(): Promise<string> {
  await docker(['version', '--format', '{{.Server.Version}}'], 10_000);
  try {
    return (
      await docker(
        ['image', 'inspect', PLAYWRIGHT_IMAGE, '--format', '{{index .RepoDigests 0}}'],
        15_000
      )
    ).trim();
  } catch {
    await docker(['pull', PLAYWRIGHT_IMAGE], 180_000);
    return (
      await docker(
        ['image', 'inspect', PLAYWRIGHT_IMAGE, '--format', '{{index .RepoDigests 0}}'],
        15_000
      )
    ).trim();
  }
}

async function docker(args: string[], timeoutMs = 60_000): Promise<string> {
  const proc = Bun.spawn(['docker', ...args], { stdout: 'pipe', stderr: 'pipe' });
  const timeout = setTimeout(() => proc.kill(), timeoutMs);
  try {
    const [exitCode, stdout, stderr] = await Promise.all([
      proc.exited,
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    if (exitCode !== 0)
      throw new Error(stderr.trim() || stdout.trim() || `docker ${args.join(' ')}`);
    return stdout;
  } finally {
    clearTimeout(timeout);
  }
}

async function exists(path: string): Promise<boolean> {
  try {
    const { stat } = await import('fs/promises');
    return (await stat(path)).isDirectory() || (await stat(path)).isFile();
  } catch {
    return false;
  }
}
