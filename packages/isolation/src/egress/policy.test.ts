import { describe, expect, test } from 'bun:test';
import {
  assertPublicAddress,
  decodeEgressPolicy,
  encodeEgressPolicy,
  isAllowedTarget,
  normalizeEgressPolicy,
} from './policy';

describe('restricted egress policy', () => {
  test('normalizes exact host allowlist and rejects IP literals', () => {
    const policy = normalizeEgressPolicy({
      targets: [
        { host: ' Registry.NPMJS.org. ', port: 443 },
        { host: 'registry.npmjs.org', port: 443 },
      ],
    });

    expect(policy.targets).toEqual([{ host: 'registry.npmjs.org', port: 443 }]);
    expect(isAllowedTarget(policy, 'registry.npmjs.org', 443)).toBe(true);
    expect(isAllowedTarget(policy, 'registry.npmjs.org', 80)).toBe(false);
    expect(isAllowedTarget(policy, 'api.openai.com', 443)).toBe(false);
    expect(() => normalizeEgressPolicy({ targets: [{ host: '127.0.0.1', port: 443 }] })).toThrow(
      /IP literal/
    );
  });

  test('fails closed on empty targets, invalid ports and invalid timeouts', () => {
    expect(() => normalizeEgressPolicy({ targets: [] })).toThrow(/at least one/);
    expect(() => normalizeEgressPolicy({ targets: [{ host: 'example.com', port: 0 }] })).toThrow(
      /Invalid restricted egress port/
    );
    expect(() =>
      normalizeEgressPolicy({ targets: [{ host: 'example.com', port: 443 }], idleTimeoutMs: -1 })
    ).toThrow(/Invalid restricted egress timeout/);
  });

  test('rejects private, loopback and documentation resolved addresses', () => {
    for (const address of [
      '127.0.0.1',
      '10.0.0.7',
      '172.16.0.1',
      '192.168.1.5',
      '169.254.1.1',
      '192.0.2.1',
      '198.18.0.1',
      '198.51.100.1',
      '203.0.113.1',
      '224.0.0.1',
      '240.0.0.1',
      '::1',
      '0:0:0:0:0:0:0:1',
      '::ffff:127.0.0.1',
      '::ffff:7f00:1',
      'ff02::1',
      'fc00::1',
      'fe80::1',
      'fec0::1',
      '2001:db8::1',
      '100::1',
      '2002::1',
    ]) {
      expect(() => assertPublicAddress(address)).toThrow(/not public/);
    }
    expect(() => assertPublicAddress('8.8.8.8')).not.toThrow();
    expect(() => assertPublicAddress('2606:4700:4700::1111')).not.toThrow();
  });

  test('rejects unsupported policy and target keys', () => {
    expect(() =>
      normalizeEgressPolicy({
        targets: [{ host: 'registry.npmjs.org', port: 443 }],
        network: 'bridge',
      } as never)
    ).toThrow(/unsupported key 'network'/);
    expect(() =>
      normalizeEgressPolicy({
        targets: [{ host: 'registry.npmjs.org', port: 443, credentials: true }],
      } as never)
    ).toThrow(/unsupported key 'credentials'/);
    const encoded = Buffer.from(
      JSON.stringify({ targets: [{ host: 'registry.npmjs.org', port: 443 }], dockerSocket: true }),
      'utf8'
    ).toString('base64');
    expect(() => decodeEgressPolicy(encoded)).toThrow(/unsupported key 'dockerSocket'/);
  });

  test('base64 round-trips the normalized controller-pinned policy', () => {
    const encoded = encodeEgressPolicy({ targets: [{ host: 'registry.npmjs.org', port: 443 }] });
    expect(decodeEgressPolicy(encoded)).toEqual({
      targets: [{ host: 'registry.npmjs.org', port: 443 }],
      connectTimeoutMs: 10000,
      dnsTimeoutMs: 10000,
      idleTimeoutMs: 30000,
      maxTunnelMs: 300000,
      maxConcurrentConnections: 64,
    });
  });
});
