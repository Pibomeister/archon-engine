import { describe, expect, test } from 'bun:test';
import {
  createAdmittedProvider,
  type AdmissionBroker,
  type AdmissionRequest,
} from './factory-admission';
import type { IAgentProvider, MessageChunk, ProviderRegistration } from './types';
import { CODEX_CAPABILITIES } from './codex/capabilities';

const context = { runId: 'run-1', nodeId: 'implement' };
const options = { model: 'qualified-model', factoryInvocation: context };
function fixture(run?: () => AsyncGenerator<MessageChunk>) {
  const requests: AdmissionRequest[] = [];
  const settled: string[] = [];
  let active = false;
  let constructions = 0;
  let executions = 0;
  const broker: AdmissionBroker = {
    async acquire(request) {
      requests.push(request);
      if (active) throw new Error('account-busy');
      active = true;
      return {
        invocationId: request.invocationId,
        requestDigest: request.requestDigest,
        leaseId: 'lease-1',
        version: 'archon.provider-admission.v1',
        leaseExpiresAt: '2099-01-01T00:00:00.000Z',
      };
    },
    async settle(_lease, outcome) {
      settled.push(outcome);
      if (outcome === 'released') active = false;
    },
  };
  const entry: ProviderRegistration = {
    id: 'codex',
    displayName: 'Fixture Codex',
    capabilities: CODEX_CAPABILITIES,
    builtIn: true,
    parseRunConfig: raw => raw,
    credentials: { kind: 'static', specs: [] },
    factory() {
      constructions++;
      return {
        getType: () => 'codex',
        getCapabilities: () => CODEX_CAPABILITIES,
        async *sendQuery(_prompt, _cwd, _resume, opts) {
          executions++;
          if (run) yield* run();
          else yield { type: 'result' };
          opts?.factoryTransportClosed?.();
        },
      };
    },
  };
  return {
    entry,
    broker,
    requests,
    settled,
    counts: () => ({ constructions, executions, active }),
  };
}
async function consume(provider: IAgentProvider, queryOptions = options) {
  const chunks: MessageChunk[] = [];
  for await (const chunk of provider.sendQuery(
    'private prompt',
    '/qualified/worktree',
    undefined,
    queryOptions
  ))
    chunks.push(chunk);
  return chunks;
}

describe('factory provider exclusive stream admission', () => {
  test('missing broker prevents construction and execution', async () => {
    const f = fixture();
    await expect(consume(createAdmittedProvider(f.entry, undefined))).rejects.toThrow(
      'factory_provider_broker_required'
    );
    expect(f.counts()).toEqual({ constructions: 0, executions: 0, active: false });
  });
  test('missing exact invocation context fails before broker or construction', async () => {
    const f = fixture();
    await expect(
      consume(createAdmittedProvider(f.entry, f.broker), {
        model: 'qualified-model',
      } as typeof options)
    ).rejects.toThrow('factory_provider_invocation_required');
    expect(f.requests).toHaveLength(0);
    expect(f.counts().constructions).toBe(0);
  });
  test('unsupported community providers fail closed', async () => {
    const f = fixture();
    await expect(
      consume(createAdmittedProvider({ ...f.entry, id: 'pi' }, f.broker))
    ).rejects.toThrow('factory_provider_not_qualified');
    expect(f.counts().constructions).toBe(0);
  });
  test('managed grok is a qualified factory provider', async () => {
    const f = fixture();
    await consume(createAdmittedProvider({ ...f.entry, id: 'grok' }, f.broker));
    expect(f.counts()).toEqual({ constructions: 1, executions: 1, active: false });
    expect(f.requests[0]?.provider).toBe('grok');
  });
  test('a declined or mismatched broker response never starts a provider', async () => {
    const f = fixture();
    f.broker.acquire = async request => ({
      invocationId: 'different',
      requestDigest: request.requestDigest,
      leaseId: 'lease',
      version: 'archon.provider-admission.v1',
      leaseExpiresAt: '2099-01-01T00:00:00.000Z',
    });
    await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow(
      'factory_provider_lease_mismatch'
    );
    expect(f.counts().constructions).toBe(0);
  });
  test('normal result and stream exhaustion release one exclusive lease', async () => {
    const f = fixture();
    await consume(createAdmittedProvider(f.entry, f.broker));
    expect(f.settled).toEqual(['released']);
    expect(f.counts()).toEqual({ constructions: 1, executions: 1, active: false });
    expect(JSON.stringify(f.requests)).not.toContain('private prompt');
    expect(f.requests[0]?.context).toEqual(context);
  });
  test('admission freezes effectful model options across the asynchronous broker boundary', async () => {
    const f = fixture();
    const mutable = { ...options, factoryInvocation: { ...context } };
    const acquire = f.broker.acquire;
    f.broker.acquire = async request => {
      mutable.model = 'unapproved-model';
      mutable.factoryInvocation.nodeId = 'different-node';
      return acquire(request);
    };
    let actual: unknown;
    f.entry.factory = () => ({
      getType: () => 'codex',
      getCapabilities: () => CODEX_CAPABILITIES,
      async *sendQuery(_prompt, _cwd, _resume, opts) {
        actual = opts;
        yield { type: 'result' };
        opts?.factoryTransportClosed?.();
      },
    });
    await consume(createAdmittedProvider(f.entry, f.broker), mutable);
    expect(actual).toMatchObject(options);
    expect(f.requests[0]?.context).toEqual(context);
  });
  test('a live stream blocks competing account work while internal retries retain its lease', async () => {
    let finish!: () => void;
    const gate = new Promise<void>(resolve => {
      finish = resolve;
    });
    let attempts = 0;
    const f = fixture(async function* () {
      attempts++; // SDK attempt one; the provider retains ownership across retry.
      await gate;
      attempts++;
      yield { type: 'result' };
    });
    const first = consume(createAdmittedProvider(f.entry, f.broker));
    await Promise.resolve();
    await Promise.resolve();
    await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow(
      'factory_provider_admission_denied'
    );
    expect(f.counts().constructions).toBe(1);
    finish();
    await first;
    expect(attempts).toBe(2);
    expect(f.settled).toEqual(['released']);
  });
  test('crash and result-less close quarantine rather than lending the account again', async () => {
    for (const run of [
      async function* (): AsyncGenerator<MessageChunk> {
        throw new Error('uncertain transport');
      },
      async function* (): AsyncGenerator<MessageChunk> {
        yield { type: 'assistant', content: 'partial' };
      },
    ]) {
      const f = fixture(run);
      await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow();
      expect(f.settled).toEqual(['quarantined']);
      expect(f.counts().active).toBe(true);
      await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow(
        'factory_provider_admission_denied'
      );
      expect(f.counts().constructions).toBe(1);
    }
  });
  test('consumer cancellation after a result still quarantines until stream closes', async () => {
    const f = fixture();
    for await (const _chunk of createAdmittedProvider(f.entry, f.broker).sendQuery(
      'prompt',
      '/qualified/worktree',
      undefined,
      options
    ))
      break;
    expect(f.settled).toEqual(['quarantined']);
  });
  test('a DAG consumer may stop on a result already backed by native closure', async () => {
    const f = fixture();
    f.entry.factory = () => ({
      getType: () => 'codex',
      getCapabilities: () => CODEX_CAPABILITIES,
      async *sendQuery(_p, _c, _r, opts) {
        opts?.factoryTransportClosed?.();
        yield { type: 'result' };
      },
    });
    for await (const _chunk of createAdmittedProvider(f.entry, f.broker).sendQuery(
      'prompt',
      '/qualified/worktree',
      undefined,
      options
    ))
      break;
    expect(f.settled).toEqual(['released']);
  });
  test('settlement failure remains an error and never claims a released account', async () => {
    const f = fixture();
    f.broker.settle = async () => {
      throw new Error('lost ack');
    };
    await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow(
      'factory_provider_settlement_uncertain'
    );
    expect(f.counts().active).toBe(true);
  });
  test('a result without transport-close proof cannot release the account', async () => {
    const f = fixture();
    f.entry.factory = () => ({
      getType: () => 'codex',
      getCapabilities: () => CODEX_CAPABILITIES,
      async *sendQuery() {
        yield { type: 'result' };
      },
    });
    await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow(
      'factory_provider_transport_uncertain'
    );
    expect(f.settled).toEqual(['quarantined']);
  });
  test('unfinished background work retains quarantine even after native transport exit', async () => {
    const f = fixture(async function* () {
      yield {
        type: 'background_tasks',
        tasks: [{ taskId: 'active', taskType: 'agent', description: 'still active' }],
      };
      yield { type: 'result' };
    });
    await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow(
      'factory_provider_transport_uncertain'
    );
    expect(f.settled).toEqual(['quarantined']);
  });
  test('lease expiry aborts the stream and preserves quarantine', async () => {
    const f = fixture();
    const acquire = f.broker.acquire;
    f.broker.acquire = async request => ({
      ...(await acquire(request)),
      leaseExpiresAt: new Date(Date.now() + 25).toISOString(),
    });
    f.entry.factory = () => ({
      getType: () => 'codex',
      getCapabilities: () => CODEX_CAPABILITIES,
      async *sendQuery(_p, _c, _r, opts) {
        await new Promise<void>(accept => {
          opts!.abortSignal!.addEventListener('abort', () => accept(), { once: true });
        });
        yield { type: 'result' };
        opts?.factoryTransportClosed?.();
      },
    });
    await expect(consume(createAdmittedProvider(f.entry, f.broker))).rejects.toThrow(
      'factory_provider_transport_uncertain'
    );
    expect(f.settled).toEqual(['quarantined']);
  });
});
