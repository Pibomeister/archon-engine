import { expect, test } from 'bun:test';
import golden from '../test/fixtures/factory-provider-broker.v1.json';
import {
  configSchema,
  leaseSchema,
  factoryBindingSchema,
  factoryMarkerForConfig,
} from './factory-mode';
import { factoryRequestDigest } from './factory-admission';

test('the shared golden wire fixture matches runtime schemas and the independent canonical digest', () => {
  expect(golden.config).toEqual(configSchema.parse(golden.config));
  expect(golden.lease).toEqual(leaseSchema.parse(golden.lease));
  const { requestDigest, ...body } = golden.acquire;
  expect(factoryRequestDigest(body)).toBe(requestDigest);
  expect(factoryBindingSchema.parse(golden.acquire.factoryBinding)).toEqual(
    golden.acquire.factoryBinding
  );
  expect(golden.settle.requestDigest).toBe(requestDigest);
  expect(golden.settleAck.outcome).toBe(golden.settle.outcome);
  expect<unknown>(golden.parentMarker).toEqual(
    factoryMarkerForConfig(configSchema.parse(golden.config))
  );
  expect<unknown>(golden.successorConfig).toEqual(configSchema.parse(golden.successorConfig));
  expect(golden.successorConfig.successor.parentBindingDigest).toBe(
    factoryRequestDigest(golden.parentMarker)
  );
});

test('legacy config aliases and incomplete lease receipts fail closed', () => {
  expect(
    configSchema.safeParse({
      version: golden.config.version,
      endpoint: { kind: 'http-loopback', baseUrl: golden.config.endpoint },
      factory: golden.config.managedRun,
    }).success
  ).toBe(false);
  const { leaseExpiresAt, ...withoutExpiry } = golden.lease;
  void leaseExpiresAt;
  expect(leaseSchema.safeParse(withoutExpiry).success).toBe(false);
  const { version, ...withoutVersion } = golden.lease;
  void version;
  expect(leaseSchema.safeParse(withoutVersion).success).toBe(false);
});

test('strict broker bindings retain the bridge execution identity while legacy envelopes remain valid', () => {
  const extended = structuredClone(golden.config);
  const executionIdentity = {
    launchKey: 'launch:fixture',
    commandId: 'command:fixture',
    originalReadyBaseRevision: 'a'.repeat(40),
    executionBaseRevision: 'b'.repeat(40),
    repairAttemptId: 'repair:fixture',
  };
  Object.assign(extended.managedRun, executionIdentity);

  const parsed = configSchema.parse(extended);
  const { worktreePath, ...binding } = parsed.managedRun;
  void worktreePath;
  expect(factoryBindingSchema.parse(binding)).toMatchObject(executionIdentity);
  expect(factoryMarkerForConfig(parsed)).toMatchObject(executionIdentity);
  expect(configSchema.parse(golden.config).managedRun).not.toHaveProperty('commandId');
});

test('factory provider policy accepts only finite explicit limit values', () => {
  type Limits = {
    maxInvocations: number;
    maxRunMs: number;
    maxExecutionMs: number;
    maxInputTokens?: number;
    maxOutputTokens?: number;
    maxConsecutiveNoWorkAttempts?: number;
  };
  const limited = structuredClone(golden.config) as typeof golden.config & {
    providerPolicy: typeof golden.config.providerPolicy & {
      limits?: Limits;
    };
  };
  limited.providerPolicy.limits = {
    maxInvocations: 3,
    maxRunMs: 3_600_000,
    maxExecutionMs: 1_800_000,
    maxInputTokens: 250_000,
    maxOutputTokens: 60_000,
    maxConsecutiveNoWorkAttempts: 2,
  };
  expect(configSchema.parse(limited).providerPolicy.limits).toEqual(limited.providerPolicy.limits);

  const invalid = structuredClone(limited) as typeof limited & {
    providerPolicy: typeof limited.providerPolicy & { limits: Limits };
  };
  invalid.providerPolicy.limits.maxInvocations = Number.POSITIVE_INFINITY;
  expect(configSchema.safeParse(invalid).success).toBe(false);
});
