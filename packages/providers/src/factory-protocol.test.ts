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
