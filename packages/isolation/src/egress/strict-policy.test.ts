import { expect, test } from 'bun:test';
import { encodeStrictEgressPolicy, decodeStrictEgressPolicy } from './strict-policy';

const CONFIG = {
  targets: [{ host: 'registry.npmjs.org', port: 443 }],
  httpGrants: [{ host: 'registry.npmjs.org', port: 443, methods: ['GET'], pathPrefixes: ['/'] }],
};

test('freezes explicit transport and HTTP grants with bounded defaults', () => {
  const policy = decodeStrictEgressPolicy(encodeStrictEgressPolicy(CONFIG));
  expect(policy.schema).toBe('archon.strict-egress.v1');
  expect(policy.transport.targets).toEqual([{ host: 'registry.npmjs.org', port: 443 }]);
  expect(policy.grants).toEqual([
    {
      host: 'registry.npmjs.org',
      port: 443,
      methods: ['GET'],
      paths: [],
      pathPrefixes: ['/'],
      maxBodyBytes: 1_048_576,
    },
  ]);
});

test('refuses legacy egress, missing grants and unknown security settings', () => {
  expect(() => encodeStrictEgressPolicy({ targets: CONFIG.targets } as never)).toThrow();
  expect(() => encodeStrictEgressPolicy({ ...CONFIG, httpGrants: [] })).toThrow();
  expect(() => encodeStrictEgressPolicy({ ...CONFIG, privileged: true } as never)).toThrow();
  expect(() =>
    encodeStrictEgressPolicy({
      ...CONFIG,
      httpGrants: [{ ...CONFIG.httpGrants[0], bypass: true }],
    } as never)
  ).toThrow();
  expect(() =>
    decodeStrictEgressPolicy(
      Buffer.from(JSON.stringify({ targets: CONFIG.targets })).toString('base64')
    )
  ).toThrow();
});

test('requires exact coverage without adding host or port authority', () => {
  expect(() =>
    encodeStrictEgressPolicy({
      ...CONFIG,
      httpGrants: [{ ...CONFIG.httpGrants[0], host: 'evil.example' }],
    } as never)
  ).toThrow(/outside/);
  expect(() =>
    encodeStrictEgressPolicy({
      ...CONFIG,
      targets: [...CONFIG.targets, { host: 'other.example', port: 443 }],
    })
  ).toThrow(/Every/);
  expect(() =>
    encodeStrictEgressPolicy({
      ...CONFIG,
      httpGrants: [{ ...CONFIG.httpGrants[0], port: 8443 }],
    } as never)
  ).toThrow(/outside/);
});

test('rejects noncanonical and oversized encodings rather than silently dropping bytes', () => {
  const encoded = encodeStrictEgressPolicy(CONFIG);
  expect(() => decodeStrictEgressPolicy(encoded + '\n')).toThrow(/encoding/);
  expect(() => decodeStrictEgressPolicy('a'.repeat(90_001))).toThrow(/encoding/);
});
