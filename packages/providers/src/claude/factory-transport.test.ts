import { expect, test } from 'bun:test';
import { createFactoryClaudeTransport } from './factory-transport';

// Drives a real POSIX child through `/bin/sh`, which Windows has no equivalent of —
// there the spawn fails with ENOENT before the transport behaviour is ever exercised.
test.skipIf(process.platform === 'win32')(
  'stdout EOF is not transport-close proof while the native child remains alive',
  async () => {
    const tracker = createFactoryClaudeTransport();
    const child = tracker.spawn({
      command: '/bin/sh',
      args: ['-c', 'exec 1>&-; read line; exit 0'],
      env: { PATH: process.env.PATH },
      signal: new AbortController().signal,
    });
    try {
      await new Promise<void>(accept => {
        child.stdout.on('end', () => accept());
        child.stdout.resume();
      });
      let closed = false;
      const closing = tracker.waitForClosed().then(() => {
        closed = true;
      });
      await new Promise<void>(accept => {
        setImmediate(accept);
      });
      expect(closed).toBe(false);
      expect(child.exitCode).toBeNull();
      child.stdin.end();
      await closing;
      expect(closed).toBe(true);
      expect(child.exitCode).toBe(0);
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
    }
  }
);

test('absence of a native transport never counts as confirmed closure', async () => {
  await expect(createFactoryClaudeTransport().waitForClosed()).rejects.toThrow(
    'factory_provider_transport_missing'
  );
});
