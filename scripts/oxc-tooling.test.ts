import { describe, expect, test } from 'bun:test';
import { spawnSync, type SpawnSyncReturns } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = fileURLToPath(new URL('..', import.meta.url));
const oxlintBin = join(repoRoot, 'node_modules/.bin/oxlint');
const oxfmtBin = join(repoRoot, 'node_modules/.bin/oxfmt');
const oxlintConfig = join(repoRoot, '.oxlintrc.json');
const oxlintComplexityConfig = join(repoRoot, '.oxlintrc.complexity.json');
const oxfmtConfig = join(repoRoot, '.oxfmtrc.json');
const oxfmtIgnore = join(repoRoot, '.oxfmtignore');

describe('Oxc tooling configuration', () => {
  test('lint-staged commands use direct binaries and receive only staged file paths', () => {
    const config = readLintStagedConfig();

    expect(config['*.{ts,tsx}']).toEqual([
      'oxlint -c .oxlintrc.json --deny-warnings --no-error-on-unmatched-pattern --fix',
      'oxlint -c .oxlintrc.complexity.json --allow correctness --deny complexity --deny-warnings',
      'eslint --fix --max-warnings 0 --no-warn-ignored',
      'oxfmt --config .oxfmtrc.json --ignore-path .oxfmtignore --write',
    ]);
    expect(config['*.{json,md,yaml,yml}']).toEqual([
      'oxfmt --config .oxfmtrc.json --ignore-path .oxfmtignore --write',
    ]);
  });

  test('oxfmt check-write-check preserves protected evidence and seccomp files', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'archon-oxfmt-'));
    try {
      const localConfig = await createProtectedFixtureTree(dir);

      const localIgnore = join(dir, '.oxfmtignore');
      expect(
        run(oxfmtBin, ['--config', localConfig, '--ignore-path', localIgnore, '--check', '.'], dir)
          .status
      ).not.toBe(0);
      runOk(oxfmtBin, ['--config', localConfig, '--ignore-path', localIgnore, '--write', '.'], dir);
      runOk(oxfmtBin, ['--config', localConfig, '--ignore-path', localIgnore, '--check', '.'], dir);
      assertProtectedFilesUnchanged(dir);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('oxfmt lint-staged command formats only supplied fixture paths', () => {
    const dir = mkdtempSync(join(tmpdir(), 'archon-oxfmt-argv-'));
    try {
      createFormatterArgvFixtures(dir);
      const command = readLintStagedConfig()['*.{ts,tsx}'][3].split(' ');
      const target = join(dir, 'target.ts');
      const sibling = join(dir, 'sibling.ts');

      runOk(oxfmtBin, [...command.slice(1), target], dir);

      expect(readFileSync(target, 'utf8')).toBe("const value = { name: 'archon' };\n");
      expect(readFileSync(sibling, 'utf8')).toBe("const value={name:'archon'}\n");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('oxlint catches syntax and complexity failures while repaired fixtures pass', () => {
    const dir = mkdtempSync(join(tmpdir(), 'archon-oxlint-'));
    try {
      const fixtures = createLintFixtures(dir);

      runFail(oxlintBin, correctnessArgs(fixtures.malformed), dir, 'Unexpected token');
      runFail(oxlintBin, complexityArgs(fixtures.complex), dir, 'Maximum allowed is 20');
      runFail(oxlintBin, complexityArgs(fixtures.complexTest), dir, 'Maximum allowed is 20');
      runOk(oxlintBin, correctnessArgs(fixtures.repaired), dir);
      runOk(oxlintBin, complexityArgs(fixtures.repaired), dir);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('configured correctness hook accepts one ignored test fixture without hiding normal errors', () => {
    const dir = mkdtempSync(join(tmpdir(), 'archon-oxlint-ignored-test-'));
    try {
      writeFileSync(join(dir, '.oxlintrc.json'), readFileSync(oxlintConfig, 'utf8'));
      writeFileSync(join(dir, 'ignored.test.ts'), "export const fixture = 'ignored';\n");
      const correctnessHook = readLintStagedConfig()['*.{ts,tsx}'][0].split(' ');

      runFail(
        oxlintBin,
        ['-c', '.oxlintrc.json', '--deny-warnings', 'ignored.test.ts'],
        dir,
        'No files found to lint'
      );
      runOk(oxlintBin, [...correctnessHook.slice(1), 'ignored.test.ts'], dir);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

function readLintStagedConfig(): Record<string, string[]> {
  return JSON.parse(readFileSync(join(repoRoot, '.lintstagedrc.json'), 'utf8')) as Record<
    string,
    string[]
  >;
}

async function createProtectedFixtureTree(dir: string): Promise<string> {
  await mkdir(join(dir, 'src'), { recursive: true });
  await mkdir(join(dir, '.omx/hardening-20260907/taskevidence'), { recursive: true });
  await mkdir(join(dir, 'runtime/.omx/hardening-20260907/taskevidence'), { recursive: true });
  await mkdir(join(dir, 'packages/isolation/docker'), { recursive: true });
  writeFileSync(join(dir, '.oxfmtrc.json'), readFileSync(oxfmtConfig, 'utf8'));
  writeFileSync(join(dir, '.oxfmtignore'), readFileSync(oxfmtIgnore, 'utf8'));
  writeFileSync(join(dir, 'src/needs-format.ts'), "const value={name:'archon'}\n");
  writeFileSync(join(dir, '.omx/hardening-20260907/taskevidence/raw.json'), '{"raw":true}');
  writeFileSync(join(dir, 'runtime/.omx/hardening-20260907/taskevidence/raw.json'), '{"raw":true}');
  writeFileSync(
    join(dir, 'packages/isolation/docker/playwright-seccomp.json'),
    '{"defaultAction":"SCMP_ACT_ERRNO"}'
  );
  writeFileSync(
    join(dir, 'packages/isolation/docker/playwright-seccomp.no-io-uring.chroot.json'),
    '{"defaultAction":"SCMP_ACT_ERRNO"}'
  );
  return join(dir, '.oxfmtrc.json');
}

function assertProtectedFilesUnchanged(dir: string): void {
  expect(readFileSync(join(dir, 'src/needs-format.ts'), 'utf8')).toBe(
    "const value = { name: 'archon' };\n"
  );
  expect(readFileSync(join(dir, '.omx/hardening-20260907/taskevidence/raw.json'), 'utf8')).toBe(
    '{"raw":true}'
  );
  expect(
    readFileSync(join(dir, 'runtime/.omx/hardening-20260907/taskevidence/raw.json'), 'utf8')
  ).toBe('{"raw":true}');
  expect(readFileSync(join(dir, 'packages/isolation/docker/playwright-seccomp.json'), 'utf8')).toBe(
    '{"defaultAction":"SCMP_ACT_ERRNO"}'
  );
  expect(
    readFileSync(
      join(dir, 'packages/isolation/docker/playwright-seccomp.no-io-uring.chroot.json'),
      'utf8'
    )
  ).toBe('{"defaultAction":"SCMP_ACT_ERRNO"}');
}

function createFormatterArgvFixtures(dir: string): void {
  writeFileSync(join(dir, '.oxfmtrc.json'), readFileSync(oxfmtConfig, 'utf8'));
  writeFileSync(join(dir, '.oxfmtignore'), readFileSync(oxfmtIgnore, 'utf8'));
  writeFileSync(join(dir, 'target.ts'), "const value={name:'archon'}\n");
  writeFileSync(join(dir, 'sibling.ts'), "const value={name:'archon'}\n");
}

function createLintFixtures(dir: string): Record<string, string> {
  const malformed = join(dir, 'malformed.ts');
  const complex = join(dir, 'complex.ts');
  const complexTest = join(dir, 'complex.test.ts');
  const repaired = join(dir, 'repaired.ts');
  writeFileSync(malformed, 'const = ;\n');
  writeFileSync(complex, complexFunctionFixture());
  writeFileSync(complexTest, complexFunctionFixture());
  writeFileSync(
    repaired,
    "export function classify(value: boolean): string {\n  return value ? 'yes' : 'no';\n}\n"
  );
  return { malformed, complex, complexTest, repaired };
}

function correctnessArgs(file: string): string[] {
  return ['-c', oxlintConfig, '--deny-warnings', '--no-error-on-unmatched-pattern', file];
}

function complexityArgs(file: string): string[] {
  return [
    '-c',
    oxlintComplexityConfig,
    '--allow',
    'correctness',
    '--deny',
    'complexity',
    '--deny-warnings',
    file,
  ];
}

function complexFunctionFixture(): string {
  return `export function classify(value: number): string {
  if (value === 1) return '1';
  if (value === 2) return '2';
  if (value === 3) return '3';
  if (value === 4) return '4';
  if (value === 5) return '5';
  if (value === 6) return '6';
  if (value === 7) return '7';
  if (value === 8) return '8';
  if (value === 9) return '9';
  if (value === 10) return '10';
  if (value === 11) return '11';
  if (value === 12) return '12';
  if (value === 13) return '13';
  if (value === 14) return '14';
  if (value === 15) return '15';
  if (value === 16) return '16';
  if (value === 17) return '17';
  if (value === 18) return '18';
  if (value === 19) return '19';
  if (value === 20) return '20';
  if (value === 21) return '21';
  return 'other';
}
`;
}

function runOk(command: string, args: string[], cwd: string): void {
  const result = run(command, args, cwd);
  const output = `${result.stdout}\n${result.stderr}`;
  expect(result.error).toBeUndefined();
  expect(result.status, output).toBe(0);
}

function runFail(command: string, args: string[], cwd: string, expectedDiagnostic: string): void {
  const result = run(command, args, cwd);
  expect(result.error).toBeUndefined();
  expect(typeof result.status).toBe('number');
  expect(result.status, 'command unexpectedly passed').not.toBe(0);
  expect(`${result.stdout}\n${result.stderr}`).toContain(expectedDiagnostic);
}

function run(command: string, args: string[], cwd: string): SpawnSyncReturns<string> {
  return spawnSync(command, args, { cwd, encoding: 'utf8' });
}
