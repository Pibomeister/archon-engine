import { execFile } from 'child_process';
import { existsSync } from 'fs';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { X509Certificate, createPrivateKey } from 'crypto';
import { promisify } from 'util';
import { describe, expect, test } from 'bun:test';

import type { EgressTlsMaterial } from './tls-material';
import { createEgressTlsMaterial, validateEgressTlsMaterial } from './tls-material';

const OPENSSL = '/usr/bin/openssl';
const PRIVATE_KEY_MARKER = '-----BEGIN PRIVATE KEY-----';
const PACKAGE_DIR = join(dirname(fileURLToPath(import.meta.url)), '../..');
const execFileAsync = promisify(execFile);

describe('createEgressTlsMaterial', () => {
  test('creates a validated CA and server leaf for exact normalized DNS SANs', async () => {
    const material = await createEgressTlsMaterial([
      'Example.COM.',
      'api.example.com',
      'example.com',
    ]);

    const ca = new X509Certificate(material.caCertificate);
    const leaf = new X509Certificate(material.certificate);
    const leafKey = createPrivateKey(material.privateKey);

    expect(Object.keys(material).sort()).toEqual([
      'caCertificate',
      'certificate',
      'privateKey',
      'validUntil',
    ]);
    expect(ca.ca).toBe(true);
    expect(leaf.ca).toBe(false);
    expect(leaf.checkPrivateKey(leafKey)).toBe(true);
    expect(Boolean(leaf.checkIssued(ca))).toBe(true);
    expect(leaf.verify(ca.publicKey)).toBe(true);
    expect(leaf.keyUsage).toContain('1.3.6.1.5.5.7.3.1');
    expect(parseDnsSans(leaf)).toEqual(['example.com', 'api.example.com']);
    expect(leaf.checkHost('example.com')).toBe('example.com');
    expect(leaf.checkHost('api.example.com')).toBe('api.example.com');
    expect(leaf.checkHost('extra.example.com')).toBeUndefined();
    expect(material.validUntil).toBe(earliestValidTo(ca, leaf).toISOString());
    expect(new Date(leaf.validTo).getTime()).toBeGreaterThan(Date.now());
    expect(new Date(leaf.validTo).getTime() - Date.now()).toBeLessThanOrEqual(26 * 60 * 60 * 1000);
  });

  test('does not return the generated CA private key', async () => {
    const material = await createEgressTlsMaterial(['allowed.example']);
    const ca = new X509Certificate(material.caCertificate);
    const leafKey = createPrivateKey(material.privateKey);

    expect(Object.keys(material).sort()).toEqual([
      'caCertificate',
      'certificate',
      'privateKey',
      'validUntil',
    ]);
    expect(material.caCertificate).not.toContain(PRIVATE_KEY_MARKER);
    expect(material.certificate).not.toContain(PRIVATE_KEY_MARKER);
    expect(material.privateKey).toContain(PRIVATE_KEY_MARKER);
    expect(ca.checkPrivateKey(leafKey)).toBe(false);
  });

  test('uses distinct keys and certificates for each invocation', async () => {
    const first = await createEgressTlsMaterial(['allowed.example']);
    const second = await createEgressTlsMaterial(['allowed.example']);

    expect(first.privateKey).not.toBe(second.privateKey);
    expect(first.certificate).not.toBe(second.certificate);
    expect(first.caCertificate).not.toBe(second.caCertificate);
  });

  test('rejects malicious or unsupported host input before certificate generation', async () => {
    await expect(createEgressTlsMaterial([])).rejects.toThrow(/at least one host/);
    await expect(createEgressTlsMaterial(['127.0.0.1'])).rejects.toThrow(/IP literal/);
    await expect(createEgressTlsMaterial(['*.example.com'])).rejects.toThrow(
      /Invalid restricted egress host/
    );
    await expect(
      createEgressTlsMaterial(['example.com\nDNS.2 = injected.example'])
    ).rejects.toThrow(/Invalid restricted egress host/);
    await expect(createEgressTlsMaterial(['localhost:443'])).rejects.toThrow(
      /Invalid restricted egress host/
    );
  });

  test('bounds host input and SAN count', async () => {
    await expect(
      createEgressTlsMaterial(Array.from({ length: 65 }, () => 'allowed.example'))
    ).rejects.toThrow(/input exceeds/);
    await expect(
      createEgressTlsMaterial(Array.from({ length: 33 }, (_, index) => `host-${index}.example.com`))
    ).rejects.toThrow(/host count exceeds/);
  });

  test('exports pure validation for generated material', async () => {
    const material = await createEgressTlsMaterial(['allowed.example']);

    expect(() => validateEgressTlsMaterial(material, ['allowed.example'])).not.toThrow();
    expect(() => validateEgressTlsMaterial(material, ['other.example'])).toThrow(/SAN set/);
    const materialWithExtraKey = { ...material, caPrivateKey: 'forbidden' };
    expect(() => validateEgressTlsMaterial(materialWithExtraKey, ['allowed.example'])).toThrow(
      /unexpected keys/
    );
    expect(() =>
      validateEgressTlsMaterial({ ...material, validUntil: '2099-01-01T00:00:00.000Z' }, [
        'allowed.example',
      ])
    ).toThrow(/validUntil/);
    expect(() =>
      validateEgressTlsMaterial({ ...material, privateKey: material.caCertificate }, [
        'allowed.example',
      ])
    ).toThrow();
  });

  test('uses the earlier CA expiry as validUntil when the leaf expires later', async () => {
    const material = await createFixtureMaterial({
      caDays: 1,
      leafDays: 1,
      leafDelayMs: 1200,
      host: 'ca-short.example',
    });
    const ca = new X509Certificate(material.caCertificate);
    const leaf = new X509Certificate(material.certificate);

    expect(new Date(ca.validTo).getTime()).toBeLessThan(new Date(leaf.validTo).getTime());
    expect(material.validUntil).toBe(new Date(ca.validTo).toISOString());
    expect(() => validateEgressTlsMaterial(material, ['ca-short.example'])).not.toThrow();
    expect(() =>
      validateEgressTlsMaterial({ ...material, validUntil: new Date(leaf.validTo).toISOString() }, [
        'ca-short.example',
      ])
    ).toThrow(/validUntil/);
  });

  test('removes private material after failure once keys exist without touching shared temp dirs', async () => {
    const root = await mkdtemp(join(tmpdir(), 'archon-owned-tls-failure-'));
    try {
      const home = join(root, 'home');
      const childTmp = join(root, 'tmp');
      const bundlePath = await buildNodeBundle(root);
      await writeFile(bundlePath, injectSignLeafFailure(await readFile(bundlePath, 'utf8')));
      await mkdirPrivate(home);
      await mkdirPrivate(childTmp);

      const script = `
        import { createEgressTlsMaterial } from ${JSON.stringify(`file://${bundlePath}`)};
        try {
          await createEgressTlsMaterial(['failure.example']);
          throw new Error('expected injected failure');
        } catch (error) {
          if (!String(error?.message ?? '').includes('injected sign leaf failure')) throw error;
        }
        const entries = await import('fs/promises').then(fs => fs.readdir(process.env.TMPDIR));
        const leaked = entries.filter(name => name.startsWith('archon-egress-tls-'));
        if (leaked.length > 0) throw new Error('leaked TLS temps: ' + leaked.join(','));
        console.log('TLS_FAILURE_CLEANUP_OK');
      `;

      const { stdout, stderr } = await execFileAsync(
        resolveNodeExecutable(),
        ['--input-type=module', '-e', script],
        {
          cwd: PACKAGE_DIR,
          env: { HOME: home, TMPDIR: childTmp, PATH: process.env.PATH ?? '' },
          timeout: 10_000,
          maxBuffer: 64 * 1024,
        }
      );

      expect(stdout.trim()).toBe('TLS_FAILURE_CLEANUP_OK');
      expect(stderr).toBe('');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test('validates in a plain Node subprocess without validToDate or validFromDate', async () => {
    const material = await createEgressTlsMaterial(['node18.example']);
    const root = await mkdtemp(join(tmpdir(), 'archon-node-tls-test-'));
    try {
      const home = join(root, 'home');
      const childTmp = join(root, 'tmp');
      const bundlePath = await buildNodeBundle(root);
      await mkdirPrivate(home);
      await mkdirPrivate(childTmp);

      const script = `
        import { X509Certificate } from 'crypto';
        import { validateEgressTlsMaterial } from ${JSON.stringify(`file://${bundlePath}`)};
        if (X509Certificate.prototype && 'validToDate' in X509Certificate.prototype) {
          delete X509Certificate.prototype.validToDate;
        }
        if (X509Certificate.prototype && 'validFromDate' in X509Certificate.prototype) {
          delete X509Certificate.prototype.validFromDate;
        }
        validateEgressTlsMaterial(JSON.parse(process.env.ARCHON_TLS_MATERIAL), ['node18.example']);
        console.log('TLS_NODE_VALIDATION_OK');
      `;

      const { stdout, stderr } = await execFileAsync(
        resolveNodeExecutable(),
        ['--input-type=module', '-e', script],
        {
          cwd: PACKAGE_DIR,
          env: {
            ARCHON_TLS_MATERIAL: JSON.stringify(material),
            HOME: home,
            TMPDIR: childTmp,
            PATH: process.env.PATH ?? '',
          },
          timeout: 10_000,
          maxBuffer: 64 * 1024,
        }
      );

      expect(stdout.trim()).toBe('TLS_NODE_VALIDATION_OK');
      expect(stderr).toBe('');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test('does not execute a PATH-supplied OpenSSL replacement', async () => {
    const root = await mkdtemp(join(tmpdir(), 'archon-owned-tls-path-'));
    try {
      const bin = join(root, 'bin');
      await mkdirPrivate(bin);
      await writeFile(join(bin, 'openssl'), '#!/bin/sh\nexit 97\n', { mode: 0o700 });
      const script = `import { createEgressTlsMaterial } from './src/egress/tls-material.ts'; await createEgressTlsMaterial(['fixed-path.example']); console.log('FIXED_OPENSSL_OK');`;
      const { stdout } = await execFileAsync(process.execPath, ['--no-env-file', '-e', script], {
        cwd: PACKAGE_DIR,
        env: { HOME: root, TMPDIR: root, PATH: bin },
        timeout: 25_000,
        maxBuffer: 65_536,
      });
      expect(stdout.trim()).toBe('FIXED_OPENSSL_OK');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function buildNodeBundle(root: string): Promise<string> {
  const bundlePath = join(root, 'tls-material.mjs');
  await execFileAsync(
    process.execPath,
    [
      'build',
      'src/egress/tls-material.ts',
      '--target=node',
      '--format=esm',
      '--outfile',
      bundlePath,
    ],
    { cwd: PACKAGE_DIR, timeout: 10_000, maxBuffer: 64 * 1024 }
  );
  return bundlePath;
}

function injectSignLeafFailure(bundle: string): string {
  const marker = 'await runner.run("sign leaf certificate", [';
  if (!bundle.includes(marker)) throw new Error('compiled TLS helper did not contain sign marker');
  return bundle.replace(marker, 'throw new Error("injected sign leaf failure");\n    ' + marker);
}

async function createFixtureMaterial(options: {
  caDays: number;
  leafDays: number;
  leafDelayMs?: number;
  host: string;
}): Promise<EgressTlsMaterial> {
  const root = await mkdtemp(join(tmpdir(), 'archon-owned-tls-fixture-'));
  try {
    const paths = fixturePaths(root);
    await writePrivate(paths.caConfig, buildCaConfig());
    await writePrivate(paths.leafExt, buildLeafExt(options.host));
    await runOpenSsl([
      'genpkey',
      '-algorithm',
      'RSA',
      '-pkeyopt',
      'rsa_keygen_bits:2048',
      '-out',
      paths.caKey,
    ]);
    await runOpenSsl([
      'req',
      '-x509',
      '-new',
      '-key',
      paths.caKey,
      '-sha256',
      '-days',
      String(options.caDays),
      '-subj',
      '/CN=Fixture CA',
      '-out',
      paths.caCert,
      '-config',
      paths.caConfig,
      '-extensions',
      'v3_ca',
    ]);
    await runOpenSsl([
      'genpkey',
      '-algorithm',
      'RSA',
      '-pkeyopt',
      'rsa_keygen_bits:2048',
      '-out',
      paths.leafKey,
    ]);
    await runOpenSsl([
      'req',
      '-new',
      '-key',
      paths.leafKey,
      '-subj',
      '/CN=Fixture Leaf',
      '-out',
      paths.leafCsr,
    ]);
    if (options.leafDelayMs) await delay(options.leafDelayMs);
    await runOpenSsl([
      'x509',
      '-req',
      '-in',
      paths.leafCsr,
      '-CA',
      paths.caCert,
      '-CAkey',
      paths.caKey,
      '-CAserial',
      paths.caSerial,
      '-CAcreateserial',
      '-sha256',
      '-days',
      String(options.leafDays),
      '-out',
      paths.leafCert,
      '-extfile',
      paths.leafExt,
      '-extensions',
      'v3_leaf',
    ]);
    const caCertificate = await readFile(paths.caCert, 'utf8');
    const certificate = await readFile(paths.leafCert, 'utf8');
    return {
      caCertificate,
      certificate,
      privateKey: await readFile(paths.leafKey, 'utf8'),
      validUntil: earliestValidTo(
        new X509Certificate(caCertificate),
        new X509Certificate(certificate)
      ).toISOString(),
    };
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function runOpenSsl(args: string[]): Promise<void> {
  await execFileAsync(OPENSSL, args, {
    env: { LC_ALL: 'C' },
    timeout: 10_000,
    maxBuffer: 64 * 1024,
  });
}

async function mkdirPrivate(path: string): Promise<void> {
  await mkdir(path, { recursive: true, mode: 0o700 });
  await chmod(path, 0o700);
}

async function writePrivate(path: string, content: string): Promise<void> {
  await writeFile(path, content, { mode: 0o600 });
  await chmod(path, 0o600);
}

function fixturePaths(root: string): Record<string, string> {
  return {
    caConfig: join(root, 'ca.cnf'),
    caKey: join(root, 'ca.key'),
    caCert: join(root, 'ca.crt'),
    caSerial: join(root, 'ca.srl'),
    leafExt: join(root, 'leaf.ext'),
    leafKey: join(root, 'leaf.key'),
    leafCsr: join(root, 'leaf.csr'),
    leafCert: join(root, 'leaf.crt'),
  };
}

function buildCaConfig(): string {
  return [
    '[req]',
    'distinguished_name = dn',
    'prompt = no',
    '[dn]',
    'CN = Fixture CA',
    '[v3_ca]',
    'basicConstraints = critical,CA:TRUE',
    'keyUsage = critical,keyCertSign,cRLSign',
    'subjectKeyIdentifier = hash',
    '',
  ].join('\n');
}

function buildLeafExt(host: string): string {
  return [
    '[v3_leaf]',
    'basicConstraints = critical,CA:FALSE',
    'keyUsage = critical,digitalSignature,keyEncipherment',
    'extendedKeyUsage = serverAuth',
    'subjectAltName = @alt_names',
    'subjectKeyIdentifier = hash',
    'authorityKeyIdentifier = keyid,issuer',
    '[alt_names]',
    `DNS.1 = ${host}`,
    '',
  ].join('\n');
}

function parseDnsSans(certificate: X509Certificate): string[] {
  return certificate.subjectAltName?.split(', ').map(entry => entry.replace(/^DNS:/, '')) ?? [];
}

function earliestValidTo(caCertificate: X509Certificate, certificate: X509Certificate): Date {
  const caValidTo = new Date(caCertificate.validTo);
  const leafValidTo = new Date(certificate.validTo);
  return caValidTo.getTime() <= leafValidTo.getTime() ? caValidTo : leafValidTo;
}

function resolveNodeExecutable(): string {
  for (const candidate of ['/opt/homebrew/bin/node', '/usr/local/bin/node']) {
    if (existsSync(candidate)) return candidate;
  }
  return 'node';
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
