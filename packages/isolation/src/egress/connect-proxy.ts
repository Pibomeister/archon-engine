import { lookup } from 'dns/promises';
import { chmod, rm } from 'fs/promises';
import { createConnection, createServer, Socket } from 'net';
import type { Server } from 'net';
import {
  assertPublicAddress,
  decodeEgressPolicy,
  isAllowedTarget,
  normalizeEgressHost,
} from './policy';
import type { NormalizedEgressPolicy } from './policy';

const MAX_HEADER_BYTES = 8192;
const MAX_CLIENT_HELLO_BYTES = 16_384;
const TLS_HANDSHAKE_RECORD = 22;
const TLS_CLIENT_HELLO = 1;
const EXTENSION_SERVER_NAME = 0;
const EXTENSION_ENCRYPTED_CLIENT_HELLO = 0xfe0d;

export type HostResolver = (host: string) => Promise<{ address: string; family: 4 | 6 }[]>;
export type TargetConnector = (address: { address: string; family: 4 | 6 }, port: number) => Socket;

export interface RestrictedProxyOptions {
  socketPath: string;
  policy: NormalizedEgressPolicy;
  resolveHost?: HostResolver;
  connectTarget?: TargetConnector;
}

type DenyOrNeedMore = { status: 'deny'; reason: string } | { status: 'need-more' };
type ClientHelloDecision = { status: 'allow'; sni: string } | DenyOrNeedMore;
type HandshakeDecision = { status: 'allow'; payload: Buffer } | DenyOrNeedMore;

interface ActiveConnection {
  client: Socket;
  upstream?: Socket;
  phaseTimers: NodeJS.Timeout[];
  deadlineTimers: NodeJS.Timeout[];
  released: boolean;
  clientClosed: boolean;
  pendingAsync: boolean;
}

export async function startRestrictedConnectProxy(
  options: RestrictedProxyOptions
): Promise<Server> {
  await rm(options.socketPath, { force: true });
  const active = new Set<ActiveConnection>();
  const server = createServer(client => {
    handleClient(
      client,
      options.policy,
      active,
      options.resolveHost ?? resolveHostWithDns,
      options.connectTarget ?? connectSocket
    );
  });
  server.on('close', () => {
    for (const connection of active) closeConnection(connection);
  });
  await listenOnSocket(server, options.socketPath);
  await chmod(options.socketPath, 0o660);
  return server;
}

export async function runRestrictedConnectProxyFromEnv(): Promise<void> {
  const socketPath = process.env.ARCHON_EGRESS_SOCKET ?? '/archon-egress/proxy.sock';
  const policy = decodeEgressPolicy(process.env.ARCHON_EGRESS_POLICY_B64 ?? '');
  await startRestrictedConnectProxy({ socketPath, policy });
  process.stdout.write(`archon-restricted-connect-proxy: ready ${socketPath}\n`);
}

function handleClient(
  client: Socket,
  policy: NormalizedEgressPolicy,
  active: Set<ActiveConnection>,
  resolveHost: HostResolver,
  connectTarget: TargetConnector
): void {
  if (active.size >= policy.maxConcurrentConnections) {
    rejectSocket(client, 503, 'connection limit');
    return;
  }
  const connection = createActiveConnection(client, active, policy);
  let header = Buffer.alloc(0);
  client.on('data', chunk => {
    header = Buffer.concat([header, chunk]);
    if (header.length > MAX_HEADER_BYTES) {
      rejectConnection(connection, 431, 'headers too large');
      return;
    }
    const end = header.indexOf('\r\n\r\n');
    if (end === -1) return;
    client.removeAllListeners('data');
    void connectFromHeader(connection, header, end, policy, active, resolveHost, connectTarget);
  });
}

function createActiveConnection(
  client: Socket,
  active: Set<ActiveConnection>,
  policy: NormalizedEgressPolicy
): ActiveConnection {
  const connection: ActiveConnection = {
    client,
    phaseTimers: [],
    deadlineTimers: [],
    released: false,
    clientClosed: false,
    pendingAsync: false,
  };
  active.add(connection);
  connection.phaseTimers.push(
    setTimeout(() => {
      rejectConnection(connection, 504, 'idle timeout');
    }, policy.idleTimeoutMs)
  );
  connection.deadlineTimers.push(
    setTimeout(() => {
      rejectConnection(connection, 504, 'tunnel timeout');
    }, policy.maxTunnelMs)
  );
  client.once('close', () => {
    connection.clientClosed = true;
    if (!connection.pendingAsync) releaseConnection(connection, active);
  });
  client.once('error', () => {
    closeConnection(connection);
  });
  return connection;
}

async function connectFromHeader(
  connection: ActiveConnection,
  header: Buffer,
  end: number,
  policy: NormalizedEgressPolicy,
  active: Set<ActiveConnection>,
  resolveHost: HostResolver,
  connectTarget: TargetConnector
): Promise<void> {
  const parsed = parseConnectHeader(header.subarray(0, end).toString('latin1'));
  if (!parsed) {
    rejectConnection(connection, 400, 'malformed CONNECT');
    return;
  }
  if (!isAllowedTarget(policy, parsed.host, parsed.port)) {
    rejectConnection(connection, 403, 'target denied');
    return;
  }
  connection.pendingAsync = true;
  try {
    const address = await resolvePublicAddress(parsed.host, policy.dnsTimeoutMs, resolveHost);
    if (!isConnectionOpen(connection)) return;
    startTlsGate(
      connection,
      header.subarray(end + 4),
      parsed.host,
      address,
      parsed.port,
      policy,
      connectTarget
    );
  } catch {
    if (isConnectionOpen(connection)) rejectConnection(connection, 403, 'target denied');
  } finally {
    connection.pendingAsync = false;
    if (connection.clientClosed) releaseConnection(connection, active);
  }
}

export function parseConnectHeader(raw: string): { host: string; port: number } | null {
  const lines = raw.split('\r\n');
  const [method, authority, version] = (lines[0] ?? '').split(' ');
  if (method !== 'CONNECT' || version !== 'HTTP/1.1' || !authority) return null;
  if (lines.some(line => line.includes('\0'))) return null;
  const parsed = parseAuthority(authority);
  if (!parsed || !validPort(parsed.port)) return null;
  try {
    return { host: normalizeEgressHost(parsed.host), port: parsed.port };
  } catch {
    return null;
  }
}

function parseAuthority(authority: string): { host: string; port: number } | null {
  if (authority.startsWith('[')) return null;
  const parts = authority.split(':');
  if (parts.length !== 2) return null;
  if (parts[0]?.includes('/') || parts[0]?.includes('@')) return null;
  return { host: parts[0] ?? '', port: Number(parts[1]) };
}

async function resolvePublicAddress(
  host: string,
  timeoutMs: number,
  resolveHost: HostResolver
): Promise<{ address: string; family: 4 | 6 }> {
  const records = await withTimeout(resolveHost(host), timeoutMs);
  for (const record of records) {
    assertPublicAddress(record.address);
    return { address: record.address, family: record.family };
  }
  throw new Error('no usable public address');
}

function startTlsGate(
  connection: ActiveConnection,
  initial: Buffer,
  expectedHost: string,
  address: { address: string; family: 4 | 6 },
  port: number,
  policy: NormalizedEgressPolicy,
  connectTarget: TargetConnector
): void {
  let buffered = initial;
  connection.client.write('HTTP/1.1 200 Connection Established\r\n\r\n');
  const onData = (chunk: Buffer): void => {
    buffered = Buffer.concat([buffered, chunk]);
    processClientHelloBuffer();
  };
  const processClientHelloBuffer = (): void => {
    const decision = evaluateClientHello(buffered, expectedHost);
    if (decision.status === 'need-more') return;
    connection.client.off('data', onData);
    if (decision.status === 'deny') {
      closeConnection(connection);
      return;
    }
    if (!isConnectionOpen(connection)) return;
    connectUpstream(connection, buffered, address, port, policy, connectTarget);
    buffered = Buffer.alloc(0);
  };
  clearPhaseTimers(connection);
  connection.phaseTimers.push(
    setTimeout(() => {
      closeConnection(connection);
    }, policy.idleTimeoutMs)
  );
  connection.client.on('data', onData);
  processClientHelloBuffer();
}

function evaluateClientHello(buffered: Buffer, expectedHost: string): ClientHelloDecision {
  if (buffered.length > MAX_CLIENT_HELLO_BYTES) return denyClientHello('oversize client hello');
  const parsed = parseClientHelloSni(buffered);
  if (parsed.status !== 'allow') return parsed;
  return parsed.sni === expectedHost ? parsed : denyClientHello('sni mismatch');
}

function parseClientHelloSni(buffered: Buffer): ClientHelloDecision {
  if (buffered.length < 5) return { status: 'need-more' };
  const handshake = collectHandshake(buffered);
  if (handshake.status !== 'allow') return handshake;
  return parseHandshakeSni(handshake.payload);
}

function collectHandshake(buffered: Buffer): HandshakeDecision {
  let offset = 0;
  let payload = Buffer.alloc(0);
  while (offset + 5 <= buffered.length) {
    if (buffered[offset] !== TLS_HANDSHAKE_RECORD) return denyClientHello('non-tls record');
    const recordLength = buffered.readUInt16BE(offset + 3);
    if (recordLength === 0) return denyClientHello('empty tls record');
    const recordEnd = offset + 5 + recordLength;
    if (recordEnd > buffered.length) return { status: 'need-more' };
    payload = Buffer.concat([payload, buffered.subarray(offset + 5, recordEnd)]);
    const decision = completeHandshakePayload(payload);
    if (decision !== null) return decision;
    offset = recordEnd;
  }
  return { status: 'need-more' };
}

function completeHandshakePayload(payload: Buffer): HandshakeDecision | null {
  if (payload.length < 4) return null;
  if (payload[0] !== TLS_CLIENT_HELLO) return denyClientHello('not a client hello');
  const handshakeLength = readUInt24BE(payload, 1);
  const total = handshakeLength + 4;
  if (total > MAX_CLIENT_HELLO_BYTES) return denyClientHello('oversize client hello');
  if (payload.length < total) return null;
  return { status: 'allow', payload: payload.subarray(4, total) };
}

function parseHandshakeSni(clientHello: Buffer): ClientHelloDecision {
  const extensionsOffset = getExtensionsOffset(clientHello);
  if (extensionsOffset === null) return denyClientHello('malformed client hello');
  if (extensionsOffset + 2 > clientHello.length) return denyClientHello('missing sni');
  const extensionsLength = clientHello.readUInt16BE(extensionsOffset);
  const extensionsEnd = extensionsOffset + 2 + extensionsLength;
  if (extensionsEnd > clientHello.length) return denyClientHello('malformed extensions');
  return parseExtensions(clientHello.subarray(extensionsOffset + 2, extensionsEnd));
}

function getExtensionsOffset(clientHello: Buffer): number | null {
  let offset = 34;
  if (offset + 1 > clientHello.length) return null;
  offset += 1 + clientHello[offset];
  if (offset + 2 > clientHello.length) return null;
  offset += 2 + clientHello.readUInt16BE(offset);
  if (offset + 1 > clientHello.length) return null;
  offset += 1 + clientHello[offset];
  return offset <= clientHello.length ? offset : null;
}

function parseExtensions(extensions: Buffer): ClientHelloDecision {
  let offset = 0;
  let sni: string | null = null;
  while (offset + 4 <= extensions.length) {
    const type = extensions.readUInt16BE(offset);
    const length = extensions.readUInt16BE(offset + 2);
    const end = offset + 4 + length;
    if (end > extensions.length) return denyClientHello('malformed extension');
    if (type === EXTENSION_ENCRYPTED_CLIENT_HELLO) return denyClientHello('encrypted client hello');
    if (type === EXTENSION_SERVER_NAME)
      sni = parseSniExtension(extensions.subarray(offset + 4, end));
    offset = end;
  }
  if (offset !== extensions.length || !sni) return denyClientHello('missing sni');
  return { status: 'allow', sni };
}

function parseSniExtension(extension: Buffer): string | null {
  if (extension.length < 5) return null;
  const listLength = extension.readUInt16BE(0);
  if (listLength + 2 !== extension.length || extension[2] !== 0) return null;
  const nameLength = extension.readUInt16BE(3);
  if (nameLength + 5 !== extension.length) return null;
  try {
    return normalizeEgressHost(extension.subarray(5).toString('ascii'));
  } catch {
    return null;
  }
}

function denyClientHello(reason: string): DenyOrNeedMore {
  return { status: 'deny', reason };
}

function readUInt24BE(buffer: Buffer, offset: number): number {
  return (buffer[offset] << 16) + (buffer[offset + 1] << 8) + buffer[offset + 2];
}

function connectUpstream(
  connection: ActiveConnection,
  rest: Buffer,
  address: { address: string; family: 4 | 6 },
  port: number,
  policy: NormalizedEgressPolicy,
  connectTarget: TargetConnector
): void {
  const upstream = connectTarget(address, port);
  connection.upstream = upstream;
  connection.phaseTimers.push(
    setTimeout(() => {
      rejectConnection(connection, 504, 'connect timeout');
    }, policy.connectTimeoutMs)
  );
  upstream.once('connect', () => {
    finishConnect(connection, rest, policy);
  });
  upstream.once('close', () => {
    closeConnection(connection);
  });
  upstream.once('error', () => {
    rejectConnection(connection, 502, 'connect failed');
  });
}

function finishConnect(
  connection: ActiveConnection,
  rest: Buffer,
  policy: NormalizedEgressPolicy
): void {
  clearPhaseTimers(connection);
  const upstream = connection.upstream;
  if (!upstream) return;
  if (rest.length > 0) upstream.write(rest);
  armTunnelIdleTimeout(connection, policy.idleTimeoutMs);
  connection.client.pipe(upstream);
  upstream.pipe(connection.client);
}

function isConnectionOpen(connection: ActiveConnection): boolean {
  return !connection.released && !connection.clientClosed && !connection.client.destroyed;
}

function validPort(port: number): boolean {
  return Number.isInteger(port) && port >= 1 && port <= 65_535;
}

function rejectConnection(connection: ActiveConnection, status: number, message: string): void {
  clearAllTimers(connection);
  connection.upstream?.destroy();
  closeClientWithResponse(connection.client, status, message);
}

function rejectSocket(client: Socket, status: number, message: string): void {
  closeClientWithResponse(client, status, message);
}

function armTunnelIdleTimeout(connection: ActiveConnection, idleTimeoutMs: number): void {
  connection.client.setTimeout(idleTimeoutMs, () => {
    closeConnection(connection);
  });
  connection.upstream?.setTimeout(idleTimeoutMs, () => {
    closeConnection(connection);
  });
}

function closeClientWithResponse(client: Socket, status: number, message: string): void {
  if (client.destroyed) return;
  client.end(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\n\r\n`);
  setTimeout(() => client.destroy(), 250).unref();
}

function closeConnection(connection: ActiveConnection): void {
  clearAllTimers(connection);
  connection.upstream?.destroy();
  connection.client.destroy();
}

function releaseConnection(connection: ActiveConnection, active: Set<ActiveConnection>): void {
  if (connection.released) return;
  connection.released = true;
  active.delete(connection);
  clearAllTimers(connection);
  connection.upstream?.destroy();
}

function clearPhaseTimers(connection: ActiveConnection): void {
  for (const timer of connection.phaseTimers.splice(0)) clearTimeout(timer);
}

function clearAllTimers(connection: ActiveConnection): void {
  clearPhaseTimers(connection);
  for (const timer of connection.deadlineTimers.splice(0)) clearTimeout(timer);
  connection.client.setTimeout(0);
  connection.upstream?.setTimeout(0);
}

function listenOnSocket(server: Server, socketPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => {
      server.off('error', reject);
      resolve();
    });
  });
}

function connectSocket(address: { address: string; family: 4 | 6 }, port: number): Socket {
  return createConnection({ host: address.address, port, family: address.family });
}

async function resolveHostWithDns(host: string): Promise<{ address: string; family: 4 | 6 }[]> {
  const records = await lookup(host, { all: true, verbatim: true });
  return records.map(record => ({
    address: record.address,
    family: toAddressFamily(record.family),
  }));
}

function toAddressFamily(value: number): 4 | 6 {
  if (value === 4 || value === 6) return value;
  throw new Error(`Unsupported DNS address family '${value}'.`);
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error('timeout'));
    }, timeoutMs);
    promise.then(
      value => {
        clearTimeout(timer);
        resolve(value);
      },
      err => {
        clearTimeout(timer);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    );
  });
}
