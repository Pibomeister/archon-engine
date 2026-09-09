import { isIP } from 'net';

export interface RestrictedEgressTarget {
  host: string;
  port: number;
}

export interface RestrictedEgressPolicy {
  targets: RestrictedEgressTarget[];
  connectTimeoutMs?: number;
  dnsTimeoutMs?: number;
  idleTimeoutMs?: number;
  maxTunnelMs?: number;
  maxConcurrentConnections?: number;
}

export interface NormalizedEgressTarget {
  host: string;
  port: number;
}

export interface NormalizedEgressPolicy {
  targets: NormalizedEgressTarget[];
  connectTimeoutMs: number;
  dnsTimeoutMs: number;
  idleTimeoutMs: number;
  maxTunnelMs: number;
  maxConcurrentConnections: number;
}

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
const DEFAULT_DNS_TIMEOUT_MS = 10_000;
const DEFAULT_IDLE_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_TUNNEL_MS = 300_000;
const DEFAULT_MAX_CONNECTIONS = 64;
const MIN_PORT = 1;
const MAX_PORT = 65_535;
const HOST_PATTERN =
  /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/;
const POLICY_KEYS = new Set([
  'targets',
  'connectTimeoutMs',
  'dnsTimeoutMs',
  'idleTimeoutMs',
  'maxTunnelMs',
  'maxConcurrentConnections',
]);
const TARGET_KEYS = new Set(['host', 'port']);

const PRIVATE_IPV4_RANGES: [number, number][] = [
  [0x00000000, 0x00ffffff],
  [0x0a000000, 0x0affffff],
  [0x64000000, 0x647fffff],
  [0x7f000000, 0x7fffffff],
  [0xa9fe0000, 0xa9feffff],
  [0xac100000, 0xac1fffff],
  [0xc0000000, 0xc00000ff],
  [0xc0000200, 0xc00002ff],
  [0xc0a80000, 0xc0a8ffff],
  [0xc6120000, 0xc613ffff],
  [0xc6336400, 0xc63364ff],
  [0xcb007100, 0xcb0071ff],
  [0xe0000000, 0xffffffff],
];
const PRIVATE_IPV6_PREFIXES: { bytes: number[]; maskBits: number }[] = [
  { bytes: [0xff], maskBits: 8 },
  { bytes: [0xfc], maskBits: 7 },
  { bytes: [0xfe, 0x80], maskBits: 10 },
  { bytes: [0xfe, 0xc0], maskBits: 10 },
  { bytes: [0x20, 0x01, 0x0d, 0xb8], maskBits: 32 },
  { bytes: [0x20, 0x02], maskBits: 16 },
  { bytes: [0x01, 0, 0, 0, 0, 0, 0, 0], maskBits: 64 },
  { bytes: [0x20, 0x01, 0, 0x02], maskBits: 48 },
];

export function normalizeEgressPolicy(policy: RestrictedEgressPolicy): NormalizedEgressPolicy {
  const raw = assertRecord(policy, 'Restricted egress policy');
  assertKnownKeys(raw, POLICY_KEYS, 'Restricted egress policy');
  const targetsValue = raw.targets;
  if (!Array.isArray(targetsValue) || targetsValue.length === 0) {
    throw new Error('Restricted egress policy must include at least one allowed target.');
  }
  const seen = new Set<string>();
  const targets = targetsValue
    .map(normalizeTarget)
    .filter(target => keepUniqueTarget(target, seen));
  return {
    targets,
    connectTimeoutMs: normalizeTimeout(
      optionalNumber(raw, 'connectTimeoutMs'),
      DEFAULT_CONNECT_TIMEOUT_MS
    ),
    dnsTimeoutMs: normalizeTimeout(optionalNumber(raw, 'dnsTimeoutMs'), DEFAULT_DNS_TIMEOUT_MS),
    idleTimeoutMs: normalizeTimeout(optionalNumber(raw, 'idleTimeoutMs'), DEFAULT_IDLE_TIMEOUT_MS),
    maxTunnelMs: normalizeTimeout(optionalNumber(raw, 'maxTunnelMs'), DEFAULT_MAX_TUNNEL_MS),
    maxConcurrentConnections: normalizeConnectionLimit(
      optionalNumber(raw, 'maxConcurrentConnections')
    ),
  };
}

export function isAllowedTarget(
  policy: NormalizedEgressPolicy,
  host: string,
  port: number
): boolean {
  const normalizedHost = normalizeHost(host);
  return policy.targets.some(target => target.host === normalizedHost && target.port === port);
}

export function assertPublicAddress(address: string): void {
  if (isPublicAddress(address)) return;
  throw new Error(`Resolved address '${address}' is not public.`);
}

export function isPublicAddress(address: string): boolean {
  const family = isIP(address);
  if (family === 4) return isPublicIpv4(address);
  if (family === 6) return isPublicIpv6(address);
  return false;
}

export function encodeEgressPolicy(policy: RestrictedEgressPolicy): string {
  return Buffer.from(JSON.stringify(normalizeEgressPolicy(policy)), 'utf8').toString('base64');
}

export function decodeEgressPolicy(value: string): NormalizedEgressPolicy {
  const parsed = JSON.parse(
    Buffer.from(value, 'base64').toString('utf8')
  ) as RestrictedEgressPolicy;
  return normalizeEgressPolicy(parsed);
}

export function normalizeEgressHost(value: string): string {
  return normalizeHost(value);
}

function keepUniqueTarget(target: NormalizedEgressTarget, seen: Set<string>): boolean {
  const key = `${target.host}:${target.port}`;
  if (seen.has(key)) return false;
  seen.add(key);
  return true;
}

function normalizeTarget(target: unknown): NormalizedEgressTarget {
  const raw = assertRecord(target, 'Restricted egress target');
  assertKnownKeys(raw, TARGET_KEYS, 'Restricted egress target');
  const hostValue = raw.host;
  if (typeof hostValue !== 'string')
    throw new Error('Restricted egress target host must be a string.');
  const portValue = raw.port;
  const host = normalizeHost(hostValue);
  if (typeof portValue !== 'number') {
    throw new Error(`Invalid restricted egress port for '${host}': ${String(portValue)}`);
  }
  if (!Number.isInteger(portValue) || portValue < MIN_PORT || portValue > MAX_PORT) {
    throw new Error(`Invalid restricted egress port for '${host}': ${String(portValue)}`);
  }
  return { host, port: portValue };
}

function assertRecord(value: unknown, subject: string): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value))
    return value as Record<string, unknown>;
  throw new Error(`${subject} must be an object.`);
}

function assertKnownKeys(
  value: Record<string, unknown>,
  allowed: Set<string>,
  subject: string
): void {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new Error(`${subject} contains unsupported key '${key}'.`);
  }
}

function optionalNumber(value: Record<string, unknown>, key: string): number | undefined {
  const raw = value[key];
  if (raw === undefined) return undefined;
  if (typeof raw === 'number') return raw;
  throw new Error(`Invalid restricted egress numeric setting '${key}'.`);
}

function normalizeHost(value: string): string {
  const host = value.trim().toLowerCase().replace(/\.$/, '');
  if (host.length === 0 || host.length > 253 || !HOST_PATTERN.test(host)) {
    throw new Error(`Invalid restricted egress host '${value}'.`);
  }
  if (isIP(host) !== 0)
    throw new Error(`Restricted egress host '${value}' must not be an IP literal.`);
  return host;
}

function normalizeTimeout(value: number | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isInteger(value) || value <= 0 || value > 300_000) {
    throw new Error(`Invalid restricted egress timeout '${value}'.`);
  }
  return value;
}

function normalizeConnectionLimit(value: number | undefined): number {
  if (value === undefined) return DEFAULT_MAX_CONNECTIONS;
  if (!Number.isInteger(value) || value <= 0 || value > 1024) {
    throw new Error(`Invalid restricted egress connection limit '${String(value)}'.`);
  }
  return value;
}

function isPublicIpv4(address: string): boolean {
  const octets = parseIpv4(address);
  if (!octets) return false;
  return !PRIVATE_IPV4_RANGES.some(([start, end]) => withinRange(ipv4ToNumber(octets), start, end));
}

function isPublicIpv6(address: string): boolean {
  const bytes = parseIpv6Bytes(address);
  if (!bytes) return false;
  const mapped = ipv4MappedBytes(bytes);
  if (mapped) return isPublicIpv4(mapped.join('.'));
  return (
    !isUnspecified(bytes) &&
    !isLoopback(bytes) &&
    !PRIVATE_IPV6_PREFIXES.some(prefix => matchesPrefix(bytes, prefix))
  );
}

function ipv4ToNumber(octets: number[]): number {
  return ((octets[0] << 24) >>> 0) + (octets[1] << 16) + (octets[2] << 8) + octets[3];
}

function withinRange(value: number, start: number, end: number): boolean {
  return value >= start && value <= end;
}

function isUnspecified(bytes: number[]): boolean {
  return bytes.every(byte => byte === 0);
}

function isLoopback(bytes: number[]): boolean {
  return bytes.slice(0, 15).every(byte => byte === 0) && bytes[15] === 1;
}

function matchesPrefix(bytes: number[], prefix: { bytes: number[]; maskBits: number }): boolean {
  const fullBytes = Math.floor(prefix.maskBits / 8);
  const partialBits = prefix.maskBits % 8;
  if (!matchesFullPrefixBytes(bytes, prefix.bytes, fullBytes)) return false;
  if (partialBits === 0) return true;
  const mask = (0xff << (8 - partialBits)) & 0xff;
  return (bytes[fullBytes] & mask) === (prefix.bytes[fullBytes] & mask);
}

function matchesFullPrefixBytes(bytes: number[], prefix: number[], length: number): boolean {
  return prefix.slice(0, length).every((value, index) => bytes[index] === value);
}

function parseIpv4(address: string): number[] | null {
  const parts = address.split('.');
  if (parts.length !== 4) return null;
  const octets = parts.map(part => Number(part));
  if (octets.some((part, index) => !validIpv4Octet(part, parts[index] ?? ''))) return null;
  return octets;
}

function validIpv4Octet(value: number, raw: string): boolean {
  return /^\d+$/.test(raw) && Number.isInteger(value) && value >= 0 && value <= 255;
}

function parseIpv6Bytes(address: string): number[] | null {
  const normalized = address.toLowerCase().split('%')[0] ?? '';
  const expanded = expandIpv6(normalized);
  if (!expanded) return null;
  return expanded.flatMap(part => [(part >> 8) & 0xff, part & 0xff]);
}

function expandIpv6(address: string): number[] | null {
  const [headRaw, tailRaw, extra] = address.split('::');
  if (extra !== undefined) return null;
  const head = parseIpv6Part(headRaw ?? '');
  const tail = tailRaw === undefined ? [] : parseIpv6Part(tailRaw);
  if (!head || !tail) return null;
  if (tailRaw === undefined) return head.length === 8 ? head : null;
  const missing = 8 - head.length - tail.length;
  if (missing < 1) return null;
  return [...head, ...Array.from<number>({ length: missing }).fill(0), ...tail];
}

function parseIpv6Part(value: string): number[] | null {
  if (value === '') return [];
  const pieces = value.split(':');
  const last = pieces.at(-1) ?? '';
  if (last.includes('.')) return parseIpv6WithDottedTail(pieces);
  return parseHextets(pieces);
}

function parseIpv6WithDottedTail(pieces: string[]): number[] | null {
  const ipv4 = parseIpv4(pieces.at(-1) ?? '');
  if (!ipv4) return null;
  const head = parseHextets(pieces.slice(0, -1));
  if (!head) return null;
  return [...head, (ipv4[0] << 8) + ipv4[1], (ipv4[2] << 8) + ipv4[3]];
}

function parseHextets(pieces: string[]): number[] | null {
  if (pieces.some(piece => !/^[0-9a-f]{1,4}$/.test(piece))) return null;
  return pieces.map(piece => Number.parseInt(piece, 16));
}

function ipv4MappedBytes(bytes: number[]): number[] | null {
  const prefix = bytes.slice(0, 10).every(byte => byte === 0);
  if (!prefix || bytes[10] !== 0xff || bytes[11] !== 0xff) return null;
  return bytes.slice(12, 16);
}
