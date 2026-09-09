import { beforeEach, describe, expect, mock, test } from 'bun:test';

const mockExecFileAsync = mock(
  async (_cmd: string, _args: string[]): Promise<{ stdout: string; stderr: string }> => ({
    stdout: '',
    stderr: '',
  })
);

mock.module('@archon/git', () => ({
  execFileAsync: mockExecFileAsync,
}));

import { runGuardedContainerSubprocess } from './container-context';
import type { ExecutionContext } from '@archon/providers/types';

const EXEC_CONTEXT: ExecutionContext = {
  kind: 'container',
  profile: 'hardened',
  containerId: 'owned-container-1',
};

describe('runGuardedContainerSubprocess', () => {
  beforeEach(() => {
    mockExecFileAsync.mockReset();
    mockExecFileAsync.mockResolvedValue({ stdout: 'ok', stderr: '' });
  });

  test('runs docker args with the bounded timeout when healthy', async () => {
    const result = await runGuardedContainerSubprocess(
      EXEC_CONTEXT,
      ['exec', 'owned-container-1'],
      {
        timeout: 1000,
      }
    );

    expect(result).toEqual({ stdout: 'ok', stderr: '' });
    expect(mockExecFileAsync).toHaveBeenCalledWith('docker', ['exec', 'owned-container-1'], {
      timeout: 1000,
    });
  });

  test('expired deadline fails before spawning docker', async () => {
    await expect(
      runGuardedContainerSubprocess(EXEC_CONTEXT, ['exec', 'owned-container-1'], {
        timeout: 1000,
        deadlineAt: Date.now() - 1,
      })
    ).rejects.toThrow(/deadline expired/);

    expect(mockExecFileAsync).not.toHaveBeenCalled();
  });

  test('malformed deadline is rejected before spawning docker', async () => {
    await expect(
      runGuardedContainerSubprocess(EXEC_CONTEXT, ['exec', 'owned-container-1'], {
        timeout: 1000,
        deadlineAt: Number.NaN,
      })
    ).rejects.toThrow(/Invalid container subprocess deadline/);

    expect(mockExecFileAsync).not.toHaveBeenCalled();
  });

  test('timeout stops the exact owned container and awaits stop completion', async () => {
    const command = Promise.withResolvers<{ stdout: string; stderr: string }>();
    const stop = Promise.withResolvers<{ stdout: string; stderr: string }>();
    let settled = false;
    mockExecFileAsync.mockImplementation(async (_cmd, args) => {
      if (args[0] === 'stop') return await stop.promise;
      return await command.promise;
    });

    const result = runGuardedContainerSubprocess(EXEC_CONTEXT, ['exec', 'owned-container-1'], {
      timeout: 5,
    }).catch(err => {
      settled = true;
      throw err;
    });
    await Bun.sleep(25);

    expect(mockExecFileAsync).toHaveBeenCalledWith('docker', ['stop', 'owned-container-1'], {
      timeout: 30000,
    });
    command.resolve({ stdout: 'late-success', stderr: '' });
    await Bun.sleep(5);
    expect(settled).toBe(false);
    stop.resolve({ stdout: '', stderr: '' });
    await expect(result).rejects.toThrow(/timed out/);
  });

  test('non-running status stops the owned container without overlapping polls', async () => {
    const command = Promise.withResolvers<{ stdout: string; stderr: string }>();
    let activePolls = 0;
    let maxPolls = 0;
    mockExecFileAsync.mockImplementation(async (_cmd, args) => {
      if (args[0] === 'stop') return { stdout: '', stderr: '' };
      return await command.promise;
    });

    await expect(
      runGuardedContainerSubprocess(EXEC_CONTEXT, ['exec', 'owned-container-1'], {
        timeout: 1000,
        statusPollMs: 1,
        getRunStatus: async () => {
          activePolls += 1;
          maxPolls = Math.max(maxPolls, activePolls);
          await Bun.sleep(5);
          activePolls -= 1;
          return 'cancelled';
        },
      })
    ).rejects.toThrow(/cancelled/);

    expect(maxPolls).toBe(1);
    expect(mockExecFileAsync).toHaveBeenCalledWith('docker', ['stop', 'owned-container-1'], {
      timeout: 30000,
    });
  });

  test('delayed status result after exec completion does not stop a later container', async () => {
    const statusEntered = Promise.withResolvers<void>();
    const finishStatus = Promise.withResolvers<void>();
    mockExecFileAsync.mockImplementation(async (_cmd, args) => {
      if (args[0] === 'stop') return { stdout: '', stderr: '' };
      await statusEntered.promise;
      return { stdout: 'completed', stderr: '' };
    });

    const result = await runGuardedContainerSubprocess(
      EXEC_CONTEXT,
      ['exec', 'owned-container-1'],
      {
        timeout: 1000,
        statusPollMs: 1,
        getRunStatus: async () => {
          statusEntered.resolve();
          await finishStatus.promise;
          return 'cancelled';
        },
      }
    );
    finishStatus.resolve();
    await Bun.sleep(10);

    expect(result).toEqual({ stdout: 'completed', stderr: '' });
    expect(mockExecFileAsync).not.toHaveBeenCalledWith('docker', ['stop', 'owned-container-1'], {
      timeout: 30000,
    });
  });

  test('delayed status rejection after exec completion does not stop the container', async () => {
    const statusEntered = Promise.withResolvers<void>();
    const finishStatus = Promise.withResolvers<void>();
    mockExecFileAsync.mockImplementation(async (_cmd, args) => {
      if (args[0] === 'stop') return { stdout: '', stderr: '' };
      await statusEntered.promise;
      return { stdout: 'completed', stderr: '' };
    });

    const result = await runGuardedContainerSubprocess(
      EXEC_CONTEXT,
      ['exec', 'owned-container-1'],
      {
        timeout: 1000,
        statusPollMs: 1,
        getRunStatus: async () => {
          statusEntered.resolve();
          await finishStatus.promise;
          throw new Error('stale cancelled status');
        },
      }
    );
    finishStatus.resolve();
    await Bun.sleep(10);

    expect(result).toEqual({ stdout: 'completed', stderr: '' });
    expect(mockExecFileAsync).not.toHaveBeenCalledWith('docker', ['stop', 'owned-container-1'], {
      timeout: 30000,
    });
  });

  test('execFile timeout rejection still stops the owned container', async () => {
    const execTimeout = Object.assign(new Error('Command failed: docker exec timed out'), {
      killed: true,
      signal: 'SIGTERM',
    });
    const stop = Promise.withResolvers<{ stdout: string; stderr: string }>();
    let settled = false;
    mockExecFileAsync.mockImplementation(async (_cmd, args) => {
      if (args[0] === 'stop') return await stop.promise;
      throw execTimeout;
    });

    const result = runGuardedContainerSubprocess(EXEC_CONTEXT, ['exec', 'owned-container-1'], {
      timeout: 1000,
    }).catch(err => {
      settled = true;
      throw err;
    });
    await Bun.sleep(5);

    expect(mockExecFileAsync).toHaveBeenCalledWith('docker', ['stop', 'owned-container-1'], {
      timeout: 30000,
    });
    expect(settled).toBe(false);
    stop.resolve({ stdout: '', stderr: '' });
    await expect(result).rejects.toThrow(/timed out/);
  });

  test('status callback error stops the container and propagates privately', async () => {
    const command = Promise.withResolvers<{ stdout: string; stderr: string }>();
    mockExecFileAsync.mockImplementation(async (_cmd, args) => {
      if (args[0] === 'stop') return { stdout: '', stderr: '' };
      return await command.promise;
    });

    await expect(
      runGuardedContainerSubprocess(EXEC_CONTEXT, ['exec', 'owned-container-1'], {
        timeout: 1000,
        statusPollMs: 1,
        getRunStatus: () => {
          throw new Error('private state malformed');
        },
      })
    ).rejects.toThrow(/private state malformed/);
  });

  test('stop failure is awaited and surfaced', async () => {
    const command = Promise.withResolvers<{ stdout: string; stderr: string }>();
    mockExecFileAsync.mockImplementation(async (_cmd, args) => {
      if (args[0] === 'stop') throw new Error('docker stop failed');
      return await command.promise;
    });

    await expect(
      runGuardedContainerSubprocess(EXEC_CONTEXT, ['exec', 'owned-container-1'], { timeout: 5 })
    ).rejects.toThrow(/docker stop failed/);
  });
});
