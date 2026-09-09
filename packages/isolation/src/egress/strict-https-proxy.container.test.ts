import { expect, test } from 'bun:test';
import { execFileSync, spawnSync } from 'child_process';
import { randomUUID } from 'crypto';
import { join } from 'path';

const IMAGE = process.env.ARCHON_STRICT_HTTPS_TEST_IMAGE;
const EXPECTED_IMAGE_ID = process.env.ARCHON_STRICT_HTTPS_EXPECTED_IMAGE_ID;
const OWNER_LABEL = 'archon.test.strict-https.owner';

test.skipIf(process.env.ARCHON_RUN_STRICT_HTTPS_CONTAINER_TEST !== '1')(
  'strict HTTPS fixture passes in the pinned unprivileged runner Node runtime',
  async () => {
    assertConfiguredImage();
    assertImageIdentity(IMAGE, EXPECTED_IMAGE_ID);
    const build = await Bun.build({
      entrypoints: [join(import.meta.dir, 'strict-https-proxy.node-runner.ts')],
      target: 'node',
    });
    expect(build.success, build.logs.join('\n')).toBe(true);
    expect(build.outputs.length).toBe(1);
    const program = `${runtimeProbe()}\n${await build.outputs[0]!.text()}`;
    const owner = randomUUID();
    const name = `archon-strict-https-${owner}`;
    try {
      const output = execFileSync('docker', fixtureArgs(name, owner), {
        input: program,
        encoding: 'utf8',
        timeout: 60_000,
        maxBuffer: 4 * 1024 * 1024,
      });
      expect(output).toContain('STRICT_HTTPS_FIXTURE=PASS');
      expect(output).toContain('STRICT_HTTPS_SANDBOX=PASS');
      expect(output).toContain(
        'ok preserves fragmented UTF-8 SSE bytes and supports CRLF terminal boundaries'
      );
      expect(output).toContain('ok bounds terminal SSE hold and incomplete frame buffering');
      process.stdout.write(output);
    } finally {
      removeOwnedFixture(name, owner);
    }
  },
  75_000
);

function assertConfiguredImage(): asserts IMAGE is string {
  if (!IMAGE || !EXPECTED_IMAGE_ID) {
    throw new Error(
      'ARCHON_STRICT_HTTPS_TEST_IMAGE and ARCHON_STRICT_HTTPS_EXPECTED_IMAGE_ID are required for strict HTTPS container tests.'
    );
  }
}

function assertImageIdentity(image: string, expectedId: string): void {
  const actual = execFileSync('docker', ['image', 'inspect', '--format', '{{.Id}}', image], {
    encoding: 'utf8',
    timeout: 10_000,
  }).trim();
  expect(actual).toBe(expectedId);
}

function runtimeProbe(): string {
  return `
import { readFileSync as readStrictFixtureStatus } from 'node:fs';
if (process.getuid() !== 1000 || process.getgid() !== 1000) throw new Error('Wrong fixture identity');
const strictFixtureStatus = readStrictFixtureStatus('/proc/self/status', 'utf8');
for (const [field, value] of [['CapEff', '0000000000000000'], ['CapBnd', '0000000000000000'], ['NoNewPrivs', '1'], ['Seccomp', '2']]) {
  if (!strictFixtureStatus.split('\\n').some(line => line.startsWith(field + ':') && line.slice(field.length + 1).trim() === value)) {
    throw new Error('Wrong fixture security setting: ' + field);
  }
}
process.stdout.write('STRICT_HTTPS_SANDBOX=PASS node=' + process.version + '\\n');
`;
}

function fixtureArgs(name: string, owner: string): string[] {
  return [
    'run',
    '--pull=never',
    '--rm',
    '-i',
    '--name',
    name,
    '--label',
    `${OWNER_LABEL}=${owner}`,
    '--network',
    'none',
    '--read-only',
    '--user',
    '1000:1000',
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges',
    '--memory',
    '256m',
    '--cpus',
    '1',
    '--pids-limit',
    '64',
    '--tmpfs',
    '/tmp:rw,nosuid,nodev,noexec,size=32m,uid=1000,gid=1000,mode=0700',
    '--entrypoint',
    'node',
    IMAGE!,
    '--input-type=module',
  ];
}

function removeOwnedFixture(name: string, owner: string): void {
  const result = spawnSync(
    'docker',
    ['inspect', '--format', `{{index .Config.Labels "${OWNER_LABEL}"}}`, name],
    { encoding: 'utf8', timeout: 10_000 }
  );
  if (result.error) throw result.error;
  if (result.status !== 0) {
    if (/No such (?:object|container)/i.test(result.stderr)) return;
    throw new Error(`Cannot verify strict HTTPS fixture ownership: ${result.stderr}`);
  }
  if (result.stdout.trim() !== owner) {
    throw new Error('Refusing to remove a strict HTTPS fixture with a different owner.');
  }
  execFileSync('docker', ['rm', '-f', name], { timeout: 10_000 });
}
