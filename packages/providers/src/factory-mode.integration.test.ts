import { removeTempTree, openAnonymousFixtureFd } from '@archon/paths/test-utils';
import { expect, test } from 'bun:test';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { closeSync, fstatSync, mkdtempSync, mkdirSync, realpathSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { AddressInfo } from 'node:net';

const registry = resolve(import.meta.dir, 'registry.ts');
const capabilities = resolve(import.meta.dir, 'codex/capabilities.ts');

test('actual registry consumes a one-shot FD and broker lease without forwarding its capability', async () => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'factory-provider-broker-')));
  const requests: Array<Record<string, unknown>> = [];
  const capability = 'FIXTURE_ONLY_NARROW_CAPABILITY_0123456789';
  const server = createServer((req, res) => {
    expect(req.headers.authorization).toBe('Bearer ' + capability);
    let bytes = '';
    req.on('data', chunk => {
      bytes += String(chunk);
    });
    req.on('end', () => {
      const body = JSON.parse(bytes) as Record<string, unknown>;
      requests.push({ path: req.url, ...body });
      res.setHeader('content-type', 'application/json');
      res.end(
        JSON.stringify(
          req.url === '/acquire'
            ? {
                version: 'archon.provider-admission.v1',
                invocationId: body.invocationId,
                requestDigest: body.requestDigest,
                leaseId: 'fixture-lease',
                leaseExpiresAt: new Date(Date.now() + 60000).toISOString(),
                budgetReservationId: 'budget:fixture',
                admittedActiveExecutionSeconds: 60,
              }
            : { outcome: body.outcome }
        )
      );
    });
  });
  try {
    await new Promise<void>(accept => {
      server.listen(0, '127.0.0.1', accept);
    });
    const worktree = join(root, 'worktree');
    const manual = join(root, 'manual');
    mkdirSync(worktree);
    mkdirSync(manual);
    const managedRun = {
      machineId: 'machine:fixture',
      hostEpoch: 'epoch:fixture',
      factoryJobId: 'job:fixture',
      logicalChainId: 'chain:fixture',
      attemptId: 'attempt:fixture',
      readySnapshotId: 'ready:fixture',
      readyDigest: 'sha256:fixture',
      runtimeBundleId: 'runtime:fixture',
      runtimeBindingDigest: 'sha256:runtime',
      projectId: 'project:fixture',
      launchKey: 'launch:fixture',
      commandId: 'command:fixture',
      originalReadyBaseRevision: 'a'.repeat(40),
      executionBaseRevision: 'b'.repeat(40),
      repairAttemptId: 'repair:fixture',
      worktreePath: worktree,
    };
    const config = {
      version: 'factory.provider-broker.config.v1',
      transport: 'http+loopback',
      endpoint: 'http://127.0.0.1:' + (server.address() as AddressInfo).port,
      capability,
      expiresAt: new Date(Date.now() + 60000).toISOString(),
      managedRun,
      providerPolicy: {
        providers: [{ provider: 'codex', models: ['fixture-model'], purpose: 'implementation' }],
        allowedWriteRoots: [worktree],
        allowedReadRoots: [worktree],
        deniedRoots: [manual],
        limits: { maxInvocations: 1, maxRunMs: 60000, maxExecutionMs: 60000 },
      },
    };
    const runner = join(root, 'runner.ts');
    writeFileSync(
      runner,
      `import { registerProvider,getAgentProvider } from ${JSON.stringify(registry)};
import { CODEX_CAPABILITIES } from ${JSON.stringify(capabilities)};
import { readSync } from 'node:fs';
let constructed=0; let fdClosed=false; try { readSync(3,Buffer.alloc(1),0,1,null); } catch { fdClosed=true; }
registerProvider({id:'codex',displayName:'fixture',builtIn:true,credentials:{kind:'static',specs:[]},capabilities:CODEX_CAPABILITIES,parseRunConfig:(x)=>x,factory:()=>{constructed++;return {getType:()=> 'codex',getCapabilities:()=>CODEX_CAPABILITIES,async *sendQuery(_p,_c,_r,opts){yield {type:'result'};opts.factoryTransportClosed();}};}});
for await (const chunk of getAgentProvider('codex').sendQuery('private fixture prompt',process.cwd(),undefined,{model:'fixture-model',factoryInvocation:{runId:'run:fixture',nodeId:'implement',reask:0}})) {}
console.log(JSON.stringify({constructed,fdClosed,capabilityInEnv:JSON.stringify(process.env).includes('FIXTURE_ONLY_NARROW_CAPABILITY'),capabilityInArgv:JSON.stringify(process.argv).includes('FIXTURE_ONLY_NARROW_CAPABILITY')}));
`
    );
    const descriptor = openAnonymousFixtureFd(root, JSON.stringify(config));
    expect(fstatSync(descriptor).nlink).toBe(0);
    expect(fstatSync(descriptor).mode & 0o777).toBe(0o600);
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(process.execPath, [runner, '--factory-provider-broker-fd', '3'], {
        cwd: worktree,
        env: { PATH: process.env.PATH!, HOME: root },
        stdio: ['ignore', 'pipe', 'pipe', descriptor],
      });
    } finally {
      closeSync(descriptor);
    }
    let stdout = '';
    let stderr = '';
    child.stdout!.on('data', chunk => {
      stdout += String(chunk);
    });
    child.stderr!.on('data', chunk => {
      stderr += String(chunk);
    });
    const code = await new Promise<number | null>((accept, reject) => {
      child.once('error', reject);
      child.once('exit', accept);
    });
    expect(code, stdout + stderr).toBe(0);
    expect(JSON.parse(stdout.trim().split('\n').at(-1)!)).toEqual({
      constructed: 1,
      fdClosed: true,
      capabilityInEnv: false,
      capabilityInArgv: false,
    });
    expect(requests.map(item => item.path)).toEqual(['/acquire', '/settle']);
    expect(requests[0]!.factoryBinding).toEqual(
      Object.fromEntries(Object.entries(managedRun).filter(([key]) => key !== 'worktreePath'))
    );
    expect(requests[1]!.outcome).toBe('released');
    expect(requests[1]!.invocationId).toBe(requests[0]!.invocationId);
    expect(requests[1]!.signals).toEqual([
      expect.objectContaining({
        kind: 'factory-invocation-outcome',
        outcome: 'completed',
        invocationId: requests[0]!.invocationId,
        requestDigest: requests[0]!.requestDigest,
        leaseId: 'fixture-lease',
      }),
    ]);
  } finally {
    await new Promise<void>(accept => {
      server.close(() => accept());
    });
    await removeTempTree(root);
  }
});
