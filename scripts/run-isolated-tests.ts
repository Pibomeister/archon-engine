import { relative, resolve } from 'node:path';

/**
 * Run each leg of a test suite in its own process and report every result.
 *
 * The test scripts used to join these same invocations with `&&`, which keeps the process
 * isolation they need but stops at the first red leg: a failure in the first of forty-five legs
 * hid the other forty-four, and a green run only ever proved that nothing failed before the first
 * thing that did. The root suite was the worst case -- its second leg failing meant
 * `.archon/scripts/` never ran at all. This runs every leg, then fails if any leg failed.
 *
 * Legs are separated by `---` and each leg is a whole command, run verbatim in its own process,
 * so the existing grouping is preserved exactly -- files that shared a `bun test` process still
 * share one, directory arguments stay directory arguments, and the root suite's workspace-wide
 * `bun --filter '*' --parallel test` leg is just another leg.
 *
 * Legs are spawned as argv, never through a shell. The shell that invoked this script has already
 * resolved the quoting in package.json, so `--filter '*'` arrives as a literal `*` and reaches the
 * child unexpanded -- which is what it means. Reconstructing a shell string here would re-expose
 * it to globbing.
 */

const REPO_ROOT = resolve(import.meta.dir, '..');
const LEG_SEPARATOR = '---';

function parseLegs(args: readonly string[]): string[][] {
  const legs: string[][] = [];
  let current: string[] = [];
  for (const arg of args) {
    if (arg === LEG_SEPARATOR) {
      if (current.length > 0) legs.push(current);
      current = [];
      continue;
    }
    current.push(arg);
  }
  if (current.length > 0) legs.push(current);
  return legs;
}

const legs = parseLegs(process.argv.slice(2));

if (legs.length === 0) {
  console.error(`run-isolated-tests: no test legs given (separate legs with ${LEG_SEPARATOR})`);
  process.exit(2);
}

interface LegResult {
  args: string[];
  code: number;
}

const results: LegResult[] = [];

for (const args of legs) {
  const child = Bun.spawnSync({
    cmd: args,
    cwd: process.cwd(),
    stdout: 'inherit',
    stderr: 'inherit',
    env: process.env,
  });
  // A leg killed by a signal reports exitCode null; treat that as failure, not success.
  results.push({ args, code: child.exitCode ?? 1 });
}

const failed = results.filter(result => result.code !== 0);
const label = relative(REPO_ROOT, process.cwd()) || '.';

console.log(
  `\n${label}: ${results.length - failed.length}/${results.length} test legs passed` +
    (failed.length > 0 ? `, ${failed.length} failed` : '')
);

if (failed.length > 0) {
  console.log('\nFailed legs:');
  for (const result of failed) {
    console.log(`  ${result.args.join(' ')} (exit ${result.code})`);
  }
  process.exit(1);
}
