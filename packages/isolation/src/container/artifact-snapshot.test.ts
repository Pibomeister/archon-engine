import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'crypto';
import { mkdtemp, mkdir, readFile, rm, stat } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import {
  snapshotContainerArtifacts,
  type TrustedArtifactVolumeMetadata,
} from './artifact-snapshot';
import { dockerCli, type DockerRunner } from './docker-exec';

const META: TrustedArtifactVolumeMetadata = {
  workspaceVolume: 'archon-test-workspace',
  image: 'sha256:' + '1'.repeat(64),
  resourceId: 'run-123',
};

function jsonl(...entries: unknown[]): string {
  return `${entries.map(entry => JSON.stringify(entry)).join('\n')}\n`;
}

function fileEntry(path: string, text: string): unknown {
  const digest = new Bun.CryptoHasher('sha256').update(text).digest('hex');
  return {
    type: 'file',
    path,
    mode: 0o644,
    size: Buffer.byteLength(text),
    sha256: digest,
    contentBase64: Buffer.from(text).toString('base64'),
  };
}

async function tempDest(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'archon-snapshot-test-'));
  return join(root, 'snapshot');
}

function fakeDocker(stdout: string): DockerRunner & { calls: string[][] } {
  const calls: string[][] = [];
  const runner = (async (args: string[]) => {
    calls.push(args);
    return { stdout, stderr: '' };
  }) as DockerRunner & { calls: string[][] };
  runner.calls = calls;
  return runner;
}

describe('snapshotContainerArtifacts', () => {
  test('runs a read-only non-root networkless reader and atomically materializes validated files', async () => {
    const dest = await tempDest();
    const docker = fakeDocker(
      jsonl({ type: 'dir', path: 'logs', mode: 0o755 }, fileEntry('logs/out.txt', 'ok'))
    );

    const result = await snapshotContainerArtifacts(docker, META, { destinationDir: dest });

    expect(await readFile(join(dest, 'logs/out.txt'), 'utf8')).toBe('ok');
    expect(result.files).toEqual([
      {
        path: 'logs/out.txt',
        size: 2,
        sha256: '2689367b205c16ce32ed4200942b8b8b1e262dfc70d9bc9fbc77c49699a4f1df',
        mode: 0o600,
      },
    ]);
    expect(result.totalBytes).toBe(2);
    const joined = docker.calls[0]?.join(' ') ?? '';
    expect(joined).toContain('run --rm');
    expect(joined).toContain('--user archon');
    expect(joined).toContain('--read-only');
    expect(joined).toContain('--cap-drop ALL');
    expect(joined).toContain('--network none');
    expect(joined).toContain('--cpus 1');
    expect(joined).toContain('archon-test-workspace:/snapshot-volume:ro');
    expect(joined).toContain('--entrypoint bun');
    await rm(join(dest, '..'), { recursive: true, force: true });
  });

  test('supports the fixed non-root Node reader without broadening extraction authority', async () => {
    const dest = await tempDest();
    const docker = fakeDocker(jsonl(fileEntry('result.txt', 'ok')));
    try {
      await snapshotContainerArtifacts(
        docker,
        { ...META, readerProfile: 'node-1000' },
        {
          destinationDir: dest,
        }
      );
      expect(await readFile(join(dest, 'result.txt'), 'utf8')).toBe('ok');
      const command = docker.calls[0]?.join(' ') ?? '';
      expect(command).toContain('--user 1000:1000');
      expect(command).toContain('--entrypoint node');
      expect(command).toContain('--input-type=module --eval');
      expect(command).toContain('--network none');
      expect(command).toContain('--cap-drop ALL');
      expect(command).not.toContain('seccomp=');
      const invalidDocker = fakeDocker('');
      await expect(
        snapshotContainerArtifacts(
          invalidDocker,
          {
            ...META,
            readerProfile: 'sh' as never,
          },
          { destinationDir: dest }
        )
      ).rejects.toThrow(/reader profile/);
      expect(invalidDocker.calls).toEqual([]);
    } finally {
      await rm(join(dest, '..'), { recursive: true, force: true });
    }
  });

  test('clamps untrusted writable modes to private controller permissions', async () => {
    const dest = await tempDest();
    const writable = fileEntry('logs/plain', 'ok') as Record<string, unknown>;
    writable.mode = 0o666;
    const executable = fileEntry('logs/script', 'ok') as Record<string, unknown>;
    executable.mode = 0o777;
    try {
      await snapshotContainerArtifacts(
        fakeDocker(jsonl({ type: 'dir', path: 'logs', mode: 0o777 }, writable, executable)),
        META,
        { destinationDir: dest }
      );
      expect((await stat(join(dest, 'logs'))).mode & 0o777).toBe(0o700);
      expect((await stat(join(dest, 'logs/plain'))).mode & 0o777).toBe(0o600);
      expect((await stat(join(dest, 'logs/script'))).mode & 0o777).toBe(0o700);
    } finally {
      await rm(join(dest, '..'), { recursive: true, force: true });
    }
  });

  test('rejects traversal, absolute paths, symlinks, hardlinks, devices, and digest mismatches', async () => {
    await expect(
      snapshotContainerArtifacts(fakeDocker(jsonl(fileEntry('../x', 'bad'))), META, {
        destinationDir: await tempDest(),
      })
    ).rejects.toThrow(/Unsafe snapshot path/);
    await expect(
      snapshotContainerArtifacts(fakeDocker(jsonl(fileEntry('/x', 'bad'))), META, {
        destinationDir: await tempDest(),
      })
    ).rejects.toThrow(/Unsafe snapshot path/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(jsonl({ type: 'reject', path: 'link', reason: 'symlink' })),
        META,
        { destinationDir: await tempDest() }
      )
    ).rejects.toThrow(/symlink/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(jsonl({ type: 'reject', path: 'two', reason: 'hardlink' })),
        META,
        { destinationDir: await tempDest() }
      )
    ).rejects.toThrow(/hardlink/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(jsonl({ type: 'reject', path: 'dev', reason: 'special-file' })),
        META,
        { destinationDir: await tempDest() }
      )
    ).rejects.toThrow(/special-file/);
    const corrupt = fileEntry('a.txt', 'ok') as Record<string, unknown>;
    corrupt.sha256 = '0'.repeat(64);
    await expect(
      snapshotContainerArtifacts(fakeDocker(jsonl(corrupt)), META, {
        destinationDir: await tempDest(),
      })
    ).rejects.toThrow(/digest mismatch/);
  });

  test('rejects duplicate paths, directory floods, mutable image tags and existing destinations', async () => {
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(jsonl(fileEntry('proof', 'first'), fileEntry('proof', 'second'))),
        META,
        { destinationDir: await tempDest() }
      )
    ).rejects.toThrow(/Duplicate snapshot path/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(jsonl({ type: 'dir', path: 'a' }, { type: 'dir', path: 'b' })),
        META,
        { destinationDir: await tempDest(), maxFiles: 1 }
      )
    ).rejects.toThrow(/max entry count/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(''),
        { ...META, image: 'runner:latest' },
        { destinationDir: await tempDest() }
      )
    ).rejects.toThrow(/immutable image/);
    const dest = await tempDest();
    await mkdir(dest);
    const docker = fakeDocker('');
    await expect(
      snapshotContainerArtifacts(docker, META, { destinationDir: dest })
    ).rejects.toThrow(/already exists/);
    expect(docker.calls).toHaveLength(0);
  });

  test('enforces file count, file size, total byte, and metadata bounds', async () => {
    await expect(
      snapshotContainerArtifacts(fakeDocker(jsonl(fileEntry('a', 'abc'))), META, {
        destinationDir: await tempDest(),
        maxFileBytes: 2,
      })
    ).rejects.toThrow(/exceeds max size/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(jsonl(fileEntry('a', 'ab'), fileEntry('b', 'cd'))),
        META,
        { destinationDir: await tempDest(), maxTotalBytes: 3 }
      )
    ).rejects.toThrow(/exceeds max total bytes/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(jsonl(fileEntry('a', 'a'), fileEntry('b', 'b'))),
        META,
        { destinationDir: await tempDest(), maxFiles: 1 }
      )
    ).rejects.toThrow(/exceeds max entry count/);
    await expect(
      snapshotContainerArtifacts(
        fakeDocker(''),
        { ...META, workspaceVolume: '../host' },
        { destinationDir: await tempDest() }
      )
    ).rejects.toThrow(/Invalid workspace volume/);
  });

  for (const readerProfile of ['bun-archon', 'node-1000'] as const) {
    test.skipIf(process.env.ARCHON_RUN_DOCKER_SNAPSHOT_TEST !== '1')(
      `snapshots a real random Docker volume through the ${readerProfile} reader`,
      async () => {
        const image = process.env.ARCHON_SNAPSHOT_TEST_IMAGE ?? 'archon-runner:hardened-test';
        const { stdout } = await dockerCli(['image', 'inspect', '--format', '{{.Id}}', image]);
        const imageId = stdout.trim();
        const suffix = randomUUID();
        const volume = `archon-snapshot-it-${suffix}`;
        const dest = await tempDest();
        const cleanup = async (): Promise<void> => {
          const owner = await dockerCli([
            'volume',
            'inspect',
            '--format',
            '{{ index .Labels "diy.archon.env-id" }}',
            volume,
          ]);
          expect(owner.stdout.trim()).toBe(suffix);
          await dockerCli(['volume', 'rm', volume]);
          await rm(join(dest, '..'), { recursive: true, force: true });
        };

        await dockerCli([
          'volume',
          'create',
          '--label',
          'diy.archon.managed=true',
          '--label',
          `diy.archon.env-id=${suffix}`,
          volume,
        ]);
        try {
          await dockerCli([
            'run',
            '--rm',
            '--name',
            `archon-snapshot-seed-${suffix}`,
            '--label',
            `diy.archon.env-id=${suffix}`,
            '--user',
            '0:0',
            '--read-only',
            '--memory',
            '128m',
            '--cpus',
            '1',
            '--pids-limit',
            '64',
            '--network',
            'none',
            '--cap-drop',
            'ALL',
            '--security-opt',
            'no-new-privileges',
            '-v',
            `${volume}:/snapshot-volume`,
            '--entrypoint',
            'sh',
            imageId,
            '-c',
            'mkdir -p /snapshot-volume/logs && printf fixture-ok > /snapshot-volume/logs/result.txt',
          ]);

          const result = await snapshotContainerArtifacts(
            dockerCli,
            { workspaceVolume: volume, image: imageId, resourceId: suffix, readerProfile },
            { destinationDir: dest, maxTotalBytes: 1024 * 1024 }
          );

          expect(await readFile(join(dest, 'logs/result.txt'), 'utf8')).toBe('fixture-ok');
          expect(result.files[0]?.path).toBe('logs/result.txt');
          expect(result.image).toBe(imageId);
        } finally {
          await cleanup();
        }
      },
      60_000
    );
  }

  test('does not leave staging directories behind after rejection', async () => {
    const dest = await tempDest();
    await expect(
      snapshotContainerArtifacts(fakeDocker(jsonl(fileEntry('../x', 'bad'))), META, {
        destinationDir: dest,
      })
    ).rejects.toThrow(/Unsafe snapshot path/);
    await expect(stat(dest)).rejects.toThrow();
    await rm(join(dest, '..'), { recursive: true, force: true });
  });
});
