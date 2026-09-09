import { execFile } from 'child_process';
import { constants } from 'fs';
import { access, chmod, mkdtemp, readFile, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { promisify } from 'util';
import { createPrivateKey, X509Certificate } from 'crypto';

import { normalizeEgressHost } from './policy';

export interface EgressTlsMaterial {
  caCertificate: string;
  certificate: string;
  privateKey: string;
  validUntil: string;
}

const OPENSSL = '/usr/bin/openssl';
const MAX_INPUT_HOSTS = 64;
const MAX_TLS_HOSTS = 32;
const OPENSSL_DEADLINE_MS = 20_000;
const OPENSSL_MAX_OUTPUT_BYTES = 64 * 1024;
const FILE_MODE = 0o600;
const DIR_MODE = 0o700;
const MATERIAL_KEYS = ['caCertificate', 'certificate', 'privateKey', 'validUntil'];
const execFileAsync = promisify(execFile);

export async function createEgressTlsMaterial(hosts: string[]): Promise<EgressTlsMaterial> {
  const normalizedHosts = normalizeTlsHosts(hosts);
  await assertOpenSslAvailable();

  const tempDir = await mkdtemp(join(tmpdir(), 'archon-egress-tls-'));
  await chmod(tempDir, DIR_MODE);

  const runner = new OpenSslRunner(Date.now() + OPENSSL_DEADLINE_MS);
  try {
    const paths = tlsPaths(tempDir);
    await writePrivateFile(paths.caConfig, buildCaConfig());
    await writePrivateFile(paths.leafExt, buildLeafExt(normalizedHosts));

    await runner.run('generate CA key', [
      'genpkey',
      '-algorithm',
      'RSA',
      '-pkeyopt',
      'rsa_keygen_bits:2048',
      '-out',
      paths.caKey,
    ]);
    await chmod(paths.caKey, FILE_MODE);

    await runner.run('generate CA certificate', [
      'req',
      '-x509',
      '-new',
      '-key',
      paths.caKey,
      '-sha256',
      '-days',
      '1',
      '-subj',
      '/CN=Archon Egress Ephemeral CA',
      '-out',
      paths.caCert,
      '-config',
      paths.caConfig,
      '-extensions',
      'v3_ca',
    ]);
    await chmod(paths.caCert, FILE_MODE);

    await runner.run('generate leaf key', [
      'genpkey',
      '-algorithm',
      'RSA',
      '-pkeyopt',
      'rsa_keygen_bits:2048',
      '-out',
      paths.leafKey,
    ]);
    await chmod(paths.leafKey, FILE_MODE);

    await runner.run('generate leaf request', [
      'req',
      '-new',
      '-key',
      paths.leafKey,
      '-subj',
      '/CN=Archon Egress Leaf',
      '-out',
      paths.leafCsr,
    ]);
    await chmod(paths.leafCsr, FILE_MODE);

    await runner.run('sign leaf certificate', [
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
      '1',
      '-out',
      paths.leafCert,
      '-extfile',
      paths.leafExt,
      '-extensions',
      'v3_leaf',
    ]);
    await chmod(paths.caSerial, FILE_MODE);
    await chmod(paths.leafCert, FILE_MODE);

    const certificate = await readFile(paths.leafCert, 'utf8');
    const material = {
      caCertificate: await readFile(paths.caCert, 'utf8'),
      certificate,
      privateKey: await readFile(paths.leafKey, 'utf8'),
      validUntil: earliestValidTo(
        new X509Certificate(await readFile(paths.caCert, 'utf8')),
        new X509Certificate(certificate)
      ).toISOString(),
    };
    validateEgressTlsMaterial(material, normalizedHosts);
    return material;
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

class OpenSslRunner {
  constructor(private readonly deadlineMs: number) {}

  async run(step: string, args: string[]): Promise<void> {
    const timeout = this.remainingMs();
    try {
      await execFileAsync(OPENSSL, args, {
        env: { LC_ALL: 'C' },
        maxBuffer: OPENSSL_MAX_OUTPUT_BYTES,
        timeout,
        windowsHide: true,
      });
    } catch (error) {
      throw new Error(`OpenSSL failed closed while trying to ${step}: ${safeFailureDetail(error)}`);
    }
  }

  private remainingMs(): number {
    const remaining = this.deadlineMs - Date.now();
    if (remaining <= 0) throw new Error('OpenSSL failed closed after exceeding the TLS deadline.');
    return remaining;
  }
}

function normalizeTlsHosts(hosts: string[]): string[] {
  if (!Array.isArray(hosts)) throw new Error('TLS material hosts must be an array.');
  if (hosts.length === 0) throw new Error('TLS material requires at least one host.');
  if (hosts.length > MAX_INPUT_HOSTS)
    throw new Error('TLS material host input exceeds the allowed limit.');

  const uniqueHosts = [...new Set(hosts.map(host => normalizeEgressHost(host)))];
  if (uniqueHosts.length === 0) throw new Error('TLS material requires at least one host.');
  if (uniqueHosts.length > MAX_TLS_HOSTS)
    throw new Error('TLS material host count exceeds the allowed limit.');
  return uniqueHosts;
}

async function assertOpenSslAvailable(): Promise<void> {
  try {
    await access(OPENSSL, constants.X_OK);
  } catch {
    throw new Error('OpenSSL is unavailable at the required path.');
  }
}

async function writePrivateFile(path: string, content: string): Promise<void> {
  await writeFile(path, content, { mode: FILE_MODE });
  await chmod(path, FILE_MODE);
}

function buildCaConfig(): string {
  return [
    '[req]',
    'distinguished_name = dn',
    'prompt = no',
    '[dn]',
    'CN = Archon Egress Ephemeral CA',
    '[v3_ca]',
    'basicConstraints = critical,CA:TRUE',
    'keyUsage = critical,keyCertSign,cRLSign',
    'subjectKeyIdentifier = hash',
    '',
  ].join('\n');
}

function buildLeafExt(hosts: string[]): string {
  const altNames = hosts.map((host, index) => `DNS.${index + 1} = ${host}`);
  return [
    '[v3_leaf]',
    'basicConstraints = critical,CA:FALSE',
    'keyUsage = critical,digitalSignature,keyEncipherment',
    'extendedKeyUsage = serverAuth',
    'subjectAltName = @alt_names',
    'subjectKeyIdentifier = hash',
    'authorityKeyIdentifier = keyid,issuer',
    '[alt_names]',
    ...altNames,
    '',
  ].join('\n');
}

export function validateEgressTlsMaterial(material: EgressTlsMaterial, hosts: string[]): void {
  assertExactMaterialKeys(material);
  const expectedHosts = normalizeTlsHosts(hosts);
  const caCertificate = new X509Certificate(material.caCertificate);
  const certificate = new X509Certificate(material.certificate);
  const privateKey = createPrivateKey(material.privateKey);

  if (!caCertificate.ca) throw new Error('Generated TLS CA certificate is not a CA.');
  if (!caCertificate.verify(caCertificate.publicKey)) {
    throw new Error('Generated TLS CA certificate is not self-signed.');
  }
  if (certificate.ca) throw new Error('Generated TLS leaf certificate is a CA.');
  if (!certificate.checkPrivateKey(privateKey)) {
    throw new Error('Generated TLS leaf certificate does not match its private key.');
  }
  if (!certificate.checkIssued(caCertificate) || !certificate.verify(caCertificate.publicKey)) {
    throw new Error('Generated TLS leaf certificate was not issued by the generated CA.');
  }
  assertServerAuth(certificate);
  assertExactDnsSans(certificate, expectedHosts);
  assertValidNow(caCertificate, 'CA');
  assertValidNow(certificate, 'leaf');

  if (material.validUntil !== earliestValidTo(caCertificate, certificate).toISOString()) {
    throw new Error('Generated TLS material validUntil does not match the leaf certificate.');
  }
}

function assertExactMaterialKeys(material: EgressTlsMaterial): void {
  const keys = Object.keys(material).sort();
  if (keys.length !== MATERIAL_KEYS.length)
    throw new Error('Generated TLS material has unexpected keys.');
  for (const [index, key] of MATERIAL_KEYS.entries()) {
    if (keys[index] !== key) throw new Error('Generated TLS material has unexpected keys.');
  }
}

function assertServerAuth(certificate: X509Certificate): void {
  if (certificate.keyUsage?.includes('1.3.6.1.5.5.7.3.1')) return;
  throw new Error('Generated TLS leaf certificate is missing serverAuth usage.');
}

function assertExactDnsSans(certificate: X509Certificate, expectedHosts: string[]): void {
  const actualHosts = parseDnsSans(certificate.subjectAltName);
  if (actualHosts.length !== expectedHosts.length) {
    throw new Error('Generated TLS leaf certificate SAN count does not match hosts.');
  }
  const actualSet = new Set(actualHosts);
  if (actualSet.size !== actualHosts.length) {
    throw new Error('Generated TLS leaf certificate contains duplicate SAN entries.');
  }
  for (const host of expectedHosts) {
    if (!actualSet.has(host))
      throw new Error('Generated TLS leaf certificate SAN set is incomplete.');
    if (certificate.checkHost(host) !== host) {
      throw new Error('Generated TLS leaf certificate failed host validation.');
    }
  }
}

function parseDnsSans(subjectAltName: string | undefined): string[] {
  if (!subjectAltName) return [];
  return subjectAltName.split(', ').map(parseDnsSanEntry);
}

function parseDnsSanEntry(entry: string): string {
  if (!entry.startsWith('DNS:'))
    throw new Error('Generated TLS leaf certificate contains non-DNS SANs.');
  return entry.slice('DNS:'.length);
}

function assertValidNow(certificate: X509Certificate, label: string): void {
  const now = Date.now();
  const validFrom = x509ValidFrom(certificate).getTime();
  const validTo = x509ValidTo(certificate).getTime();
  if (validFrom > now || validTo <= now) {
    throw new Error(`Generated TLS ${label} certificate is not currently valid.`);
  }
  if (validTo - now > 26 * 60 * 60 * 1000) {
    throw new Error(`Generated TLS ${label} certificate validity exceeds the allowed window.`);
  }
}

function earliestValidTo(caCertificate: X509Certificate, certificate: X509Certificate): Date {
  const caValidTo = x509ValidTo(caCertificate);
  const leafValidTo = x509ValidTo(certificate);
  return caValidTo.getTime() <= leafValidTo.getTime() ? caValidTo : leafValidTo;
}

function x509ValidFrom(certificate: X509Certificate): Date {
  return new Date(certificate.validFrom);
}

function x509ValidTo(certificate: X509Certificate): Date {
  return new Date(certificate.validTo);
}

function tlsPaths(tempDir: string): Record<TlsPathKey, string> {
  return {
    caConfig: join(tempDir, 'ca.cnf'),
    caKey: join(tempDir, 'ca.key'),
    caCert: join(tempDir, 'ca.crt'),
    caSerial: join(tempDir, 'ca.srl'),
    leafExt: join(tempDir, 'leaf.ext'),
    leafKey: join(tempDir, 'leaf.key'),
    leafCsr: join(tempDir, 'leaf.csr'),
    leafCert: join(tempDir, 'leaf.crt'),
  };
}

type TlsPathKey =
  | 'caConfig'
  | 'caKey'
  | 'caCert'
  | 'caSerial'
  | 'leafExt'
  | 'leafKey'
  | 'leafCsr'
  | 'leafCert';

function safeFailureDetail(error: unknown): string {
  if (isNodeError(error) && error.code === 'ETIMEDOUT') return 'deadline exceeded';
  if (isNodeError(error) && typeof error.signal === 'string')
    return `terminated by ${error.signal}`;
  if (isNodeError(error) && typeof error.code === 'number') return `exit code ${error.code}`;
  return 'command failed';
}

function isNodeError(error: unknown): error is { code?: unknown; signal?: unknown } {
  return typeof error === 'object' && error !== null;
}
