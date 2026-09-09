import { lookup } from 'dns/promises';
import { createHash } from 'crypto';
import { chmod, rm } from 'fs/promises';
import { request as httpsRequest } from 'https';
import { Socket } from 'net';
import { createServer as createNetServer } from 'net';
import { TLSSocket, createSecureContext, connect as tlsConnect } from 'tls';
import type { ConnectionOptions, SecureContext } from 'tls';
import type { IncomingMessage, ClientRequest } from 'http';
import type { RequestOptions } from 'https';
import type { Server, Socket as NetSocket } from 'net';
import { assertPublicAddress, isAllowedTarget, normalizeEgressHost } from './policy';
import { parseConnectHeader } from './connect-proxy';
import {
  parseProviderCompletionUsage,
  parseProviderUsageSse,
  prepareTrustedProviderRequest,
  type ProviderTokenUsage,
  type TrustedProvider,
  type TrustedProviderBudgetPolicy,
} from './provider-budget-contract';
import type { NormalizedEgressPolicy } from './policy';
import type { HostResolver } from './connect-proxy';
import type { BudgetStatus } from './proxy-budget-ledger';

const MAX_CONNECT_HEADER_BYTES = 8192;
const MAX_HTTP_HEADER_BYTES = 16_384;
const DEFAULT_MAX_BODY_BYTES = 1_048_576;
const STRICT_GRANT_KEYS = new Set([
  'host',
  'port',
  'methods',
  'paths',
  'pathPrefixes',
  'maxBodyBytes',
]);
const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);

export interface StrictHttpGrant {
  host: string;
  port: number;
  methods: string[];
  paths?: string[];
  pathPrefixes?: string[];
  maxBodyBytes?: number;
}

export interface StrictHttpsConnectProxyOptions {
  socketPath: string;
  policy: NormalizedEgressPolicy;
  tls: {
    key: string | Buffer;
    cert: string | Buffer;
  };
  grants: StrictHttpGrant[];
  resolveHost?: HostResolver;
  connectTarget?: (
    address: { address: string; family: 4 | 6 },
    port: number,
    servername: string
  ) => NetSocket;
  upstreamCa?: string | Buffer | (string | Buffer)[];
  accounting?: StrictProviderAccountingOptions;
}

export interface StrictProviderAccountingOptions {
  policies: TrustedProviderBudgetPolicy[];
  client: StrictProviderBudgetClient;
  maxResponseBytes?: number;
}

export interface StrictProviderBudgetReservationInput {
  provider: TrustedProvider;
  host: string;
  method: string;
  path: string;
  model: string;
  requestHash: string;
  inputCeiling: number;
  outputCeiling: number;
}

export interface StrictProviderBudgetReservation {
  reservationId: string;
  deadlineEpochMs?: number;
}

export interface StrictProviderBudgetClient {
  getStatus(): Promise<BudgetStatus>;
  reserve(input: StrictProviderBudgetReservationInput): Promise<StrictProviderBudgetReservation>;
  settleComplete(input: {
    reservationId: string;
    usage: ProviderTokenUsage;
    signal?: AbortSignal;
  }): Promise<void>;
  settleUnknown(input: { reservationId: string; reason: string }): Promise<void>;
  close(): Promise<void>;
}

interface NormalizedStrictGrant {
  host: string;
  port: number;
  methods: Set<string>;
  paths: Set<string>;
  pathPrefixes: string[];
  maxBodyBytes: number;
}

interface StrictConnection {
  raw: Socket;
  tls?: TLSSocket;
  active: Set<StrictConnection>;
  timers: NodeJS.Timeout[];
  released: boolean;
  clientClosed: boolean;
  pendingAsync: boolean;
  target?: { host: string; port: number; address: { address: string; family: 4 | 6 } };
  upstreamRequest?: ClientRequest;
  upstreamResponse?: IncomingMessage;
  upstreamSocket?: NetSocket;
}

interface AccountedReservation {
  policy: TrustedProviderBudgetPolicy;
  reservationId: string;
  body: Buffer;
  maxResponseBytes: number;
}

interface ResponseEvidence {
  chunks: Buffer[];
  bytes: number;
  overLimit: boolean;
  settled: boolean;
}

interface TerminalSseHold {
  incomplete: Buffer;
  held: Buffer[];
  heldBytes: number;
  holding: boolean;
  overLimit: boolean;
  maxBytes: number;
}

export async function startStrictHttpsConnectProxy(
  options: StrictHttpsConnectProxyOptions
): Promise<Server> {
  const grants = normalizeStrictGrants(options.grants);
  if (!options.tls.key || !options.tls.cert)
    throw new Error('Strict HTTPS proxy requires TLS key and cert.');
  const secureContext = createSecureContext({ key: options.tls.key, cert: options.tls.cert });
  await rm(options.socketPath, { force: true });
  const active = new Set<StrictConnection>();
  const server = createNetServer(raw => {
    if (active.size >= options.policy.maxConcurrentConnections) {
      writeProxyResponse(raw, 503, 'connection limit');
      return;
    }
    handleRawClient(raw, active, secureContext, options, grants);
  });
  closeActiveConnectionsBeforeServerClose(server, active);
  await listenOnSocket(server, options.socketPath);
  await chmod(options.socketPath, 0o660);
  return server;
}

export function normalizeStrictHttpGrants(grants: StrictHttpGrant[]): StrictHttpGrant[] {
  return normalizeStrictGrants(grants).map(grant => ({
    host: grant.host,
    port: grant.port,
    methods: [...grant.methods].sort(),
    paths: [...grant.paths].sort(),
    pathPrefixes: [...new Set(grant.pathPrefixes)].sort(),
    maxBodyBytes: grant.maxBodyBytes,
  }));
}

function normalizeStrictGrants(grants: StrictHttpGrant[]): NormalizedStrictGrant[] {
  if (!Array.isArray(grants) || grants.length === 0)
    throw new Error('Strict HTTPS proxy requires at least one HTTP grant.');
  return grants.map(grant => {
    assertGrantRecord(grant);
    const host = normalizeEgressHost(grant.host);
    const methods = normalizeMethods(grant.methods);
    const paths = new Set((grant.paths ?? []).map(normalizeAllowedPath));
    const pathPrefixes = (grant.pathPrefixes ?? []).map(normalizeAllowedPathPrefix);
    if (paths.size === 0 && pathPrefixes.length === 0)
      throw new Error(`Strict HTTPS grant for '${host}' requires a path or path prefix.`);
    return {
      host,
      port: normalizeGrantPort(grant.port, host),
      methods,
      paths,
      pathPrefixes,
      maxBodyBytes: normalizeMaxBodyBytes(grant.maxBodyBytes),
    };
  });
}

function assertGrantRecord(grant: StrictHttpGrant): void {
  if (!grant || typeof grant !== 'object' || Array.isArray(grant))
    throw new Error('Strict HTTPS grant must be an object.');
  for (const key of Object.keys(grant)) {
    if (!STRICT_GRANT_KEYS.has(key))
      throw new Error(`Strict HTTPS grant contains unsupported key '${key}'.`);
  }
}

function normalizeMethods(methods: string[]): Set<string> {
  if (!Array.isArray(methods) || methods.length === 0)
    throw new Error('Strict HTTPS grant requires at least one method.');
  if (methods.some(method => typeof method !== 'string' || !/^[A-Z]+$/.test(method)))
    throw new Error('Strict HTTPS grant contains an invalid method.');
  return new Set(methods);
}

function normalizeGrantPort(port: number, host: string): number {
  if (!Number.isInteger(port) || port < 1 || port > 65_535)
    throw new Error(`Invalid strict HTTPS grant port for '${host}'.`);
  return port;
}

function normalizeMaxBodyBytes(value: number | undefined): number {
  if (value === undefined) return DEFAULT_MAX_BODY_BYTES;
  if (!Number.isInteger(value) || value < 0 || value > 10_485_760)
    throw new Error('Invalid strict HTTPS max body size.');
  return value;
}

function normalizeAllowedPath(path: string): string {
  if (!isSafeOriginPath(path)) throw new Error(`Invalid strict HTTPS grant path '${path}'.`);
  return path;
}

function normalizeAllowedPathPrefix(prefix: string): string {
  const normalized = normalizeAllowedPath(prefix);
  if (!normalized.endsWith('/'))
    throw new Error(`Strict HTTPS path prefix '${prefix}' must end with '/'.`);
  return normalized;
}

function handleRawClient(
  raw: Socket,
  active: Set<StrictConnection>,
  context: SecureContext,
  options: StrictHttpsConnectProxyOptions,
  grants: NormalizedStrictGrant[]
): void {
  const connection: StrictConnection = {
    raw,
    active,
    timers: [],
    released: false,
    clientClosed: false,
    pendingAsync: false,
  };
  active.add(connection);
  armTimer(connection, options.policy.maxTunnelMs, () => {
    closeConnection(connection);
  });
  raw.once('close', () => {
    handleClientClosed(connection);
  });
  raw.once('error', () => {
    closeConnection(connection);
  });
  readConnectRequest(connection, context, options, grants);
}

function readConnectRequest(
  connection: StrictConnection,
  context: SecureContext,
  options: StrictHttpsConnectProxyOptions,
  grants: NormalizedStrictGrant[]
): void {
  let buffered = Buffer.alloc(0);
  const onData = (chunk: Buffer): void => {
    buffered = Buffer.concat([buffered, chunk]);
    if (buffered.length > MAX_CONNECT_HEADER_BYTES) {
      rejectConnection(connection, 431, 'headers too large');
      return;
    }
    const headerEnd = buffered.indexOf('\r\n\r\n');
    if (headerEnd === -1) return;
    connection.raw.off('data', onData);
    const rest = buffered.subarray(headerEnd + 4);
    void acceptConnect(connection, buffered.subarray(0, headerEnd), rest, context, options, grants);
  };
  connection.raw.setTimeout(options.policy.idleTimeoutMs, () => {
    rejectConnection(connection, 504, 'idle timeout');
  });
  connection.raw.on('data', onData);
}

async function acceptConnect(
  connection: StrictConnection,
  header: Buffer,
  rest: Buffer,
  context: SecureContext,
  options: StrictHttpsConnectProxyOptions,
  grants: NormalizedStrictGrant[]
): Promise<void> {
  const parsed = parseConnectHeader(header.toString('latin1'));
  if (!parsed) {
    rejectConnection(connection, 400, 'malformed CONNECT');
    return;
  }
  if (!isAllowedStrictTarget(options.policy, grants, parsed.host, parsed.port)) {
    rejectConnection(connection, 403, 'target denied');
    return;
  }
  connection.pendingAsync = true;
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    if (!connection.released && !connection.clientClosed)
      rejectConnection(connection, 403, 'target denied');
  }, options.policy.dnsTimeoutMs);
  try {
    const address = firstPublicAddress(
      await (options.resolveHost ?? resolveHostWithDns)(parsed.host)
    );
    if (timedOut || connection.released || connection.clientClosed) return;
    connection.target = { host: parsed.host, port: parsed.port, address };
    connection.raw.unshift(rest);
    writeProxyResponse(connection.raw, 200, 'Connection Established', false);
    startDownstreamTls(connection, context, options, grants);
  } catch {
    if (!timedOut && !connection.released && !connection.clientClosed)
      rejectConnection(connection, 403, 'target denied');
  } finally {
    clearTimeout(timer);
    connection.pendingAsync = false;
    if (connection.clientClosed) releaseConnection(connection);
  }
}

function isAllowedStrictTarget(
  policy: NormalizedEgressPolicy,
  grants: NormalizedStrictGrant[],
  host: string,
  port: number
): boolean {
  return (
    isAllowedTarget(policy, host, port) &&
    grants.some(grant => grant.host === host && grant.port === port)
  );
}

function startDownstreamTls(
  connection: StrictConnection,
  context: SecureContext,
  options: StrictHttpsConnectProxyOptions,
  grants: NormalizedStrictGrant[]
): void {
  connection.raw.setTimeout(0);
  const tls = new TLSSocket(connection.raw, { isServer: true, secureContext: context });
  connection.tls = tls;
  tls.once('secure', () => {
    handleSecureDownstream(connection, options, grants);
  });
  tls.once('error', () => {
    closeConnection(connection);
  });
  tls.once('close', () => {
    handleClientClosed(connection);
  });
}

function handleSecureDownstream(
  connection: StrictConnection,
  options: StrictHttpsConnectProxyOptions,
  grants: NormalizedStrictGrant[]
): void {
  const target = connection.target;
  const tls = connection.tls;
  if (!target || !tls) {
    closeConnection(connection);
    return;
  }
  if (normalizeTlsServername(tls.servername) !== target.host) {
    closeConnection(connection);
    return;
  }
  readHttpRequest(connection, options, grants);
}

function normalizeTlsServername(servername: string | false | null | undefined): string | null {
  if (!servername) return null;
  try {
    return normalizeEgressHost(servername);
  } catch {
    return null;
  }
}

function readHttpRequest(
  connection: StrictConnection,
  options: StrictHttpsConnectProxyOptions,
  grants: NormalizedStrictGrant[]
): void {
  let buffered = Buffer.alloc(0);
  const tls = connection.tls;
  if (!tls) {
    closeConnection(connection);
    return;
  }
  const onData = (chunk: Buffer): void => {
    buffered = Buffer.concat([buffered, chunk]);
    const headerEnd = buffered.indexOf('\r\n\r\n');
    const headerBytes = headerEnd === -1 ? buffered.length : headerEnd + 4;
    if (headerBytes > MAX_HTTP_HEADER_BYTES) {
      tls.off('data', onData);
      sendDownstreamError(connection, 431, 'headers too large');
      return;
    }
    if (headerEnd === -1) return;
    tls.off('data', onData);
    void proxyHttpRequest(
      connection,
      buffered.subarray(0, headerEnd),
      buffered.subarray(headerEnd + 4),
      options,
      grants
    );
  };
  tls.setTimeout(options.policy.idleTimeoutMs, () => {
    closeConnection(connection);
  });
  tls.on('data', onData);
}

async function proxyHttpRequest(
  connection: StrictConnection,
  header: Buffer,
  initialBody: Buffer,
  options: StrictHttpsConnectProxyOptions,
  grants: NormalizedStrictGrant[]
): Promise<void> {
  const request = parseHttpRequest(header.toString('latin1'));
  const target = connection.target;
  if (!request || !target) {
    sendDownstreamError(connection, 400, 'bad request');
    return;
  }
  if (!requestMatchesTarget(request, target.host, target.port)) {
    sendDownstreamError(connection, 421, 'misdirected request');
    return;
  }
  const grant = findGrant(grants, target.host, target.port, request.method, request.path);
  if (!grant) {
    sendDownstreamError(connection, 403, 'request denied');
    return;
  }
  if (isWebSocketOrHttp2Attempt(request.headers)) {
    sendDownstreamError(connection, 501, 'upgrade not supported');
    return;
  }
  try {
    await forwardRequest(connection, request, initialBody, grant, options);
  } catch (error) {
    console.error(`STRICT_PROXY_FORWARD_ERROR ${errorMessage(error)}`);
    if (!connection.released) sendDownstreamError(connection, 502, 'upstream failed');
  }
}

interface ParsedHttpRequest {
  method: string;
  path: string;
  headers: Map<string, string>;
}

function parseHttpRequest(raw: string): ParsedHttpRequest | null {
  const lines = raw.split('\r\n');
  const [method, target, version] = (lines.shift() ?? '').split(' ');
  if (!method || !target || version !== 'HTTP/1.1') return null;
  if (!isSafeOriginPath(target)) return null;
  const parsedHeaders = parseHeaders(lines);
  if (!parsedHeaders) return null;
  return { method: method.toUpperCase(), path: target, ...parsedHeaders };
}

function parseHeaders(lines: string[]): Pick<ParsedHttpRequest, 'headers'> | null {
  const headers = new Map<string, string>();
  for (const line of lines) {
    const separator = line.indexOf(':');
    if (separator <= 0 || line.includes('\0')) return null;
    const name = line.slice(0, separator).trim().toLowerCase();
    const value = line.slice(separator + 1).trim();
    if (!/^[!#$%&'*+.^_`|~0-9a-z-]+$/.test(name)) return null;
    if (headers.has(name)) return null;
    headers.set(name, value);
  }
  return { headers };
}

function isSafeOriginPath(path: string): boolean {
  if (!path.startsWith('/') || path.startsWith('//') || path.includes('://')) return false;
  if (path.includes('#') || hasControlByte(path) || path.includes('\\')) return false;
  if (/%(?:2e|2f|5c|25)/iu.test(path)) return false;
  try {
    const decoded = decodeURIComponent(path);
    if (decoded.includes('..') || decoded.includes('\\')) return false;
  } catch {
    return false;
  }
  return !path.split(/[?#]/, 1)[0].split('/').includes('..');
}

function hasControlByte(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 32 || code === 127) return true;
  }
  return false;
}

function requestMatchesTarget(request: ParsedHttpRequest, host: string, port: number): boolean {
  if (request.headers.has(':authority')) return false;
  const hostHeader = request.headers.get('host');
  if (!hostHeader) return false;
  const authority = parseHttpAuthority(hostHeader);
  if (!authority) return false;
  return authority.host === host && authority.port === port;
}

function parseHttpAuthority(value: string): { host: string; port: number } | null {
  if (value.startsWith('[') || value.includes('@') || value.includes('/')) return null;
  const parts = value.split(':');
  if (parts.length > 2) return null;
  try {
    return { host: normalizeEgressHost(parts[0] ?? ''), port: parts[1] ? Number(parts[1]) : 443 };
  } catch {
    return null;
  }
}

function findGrant(
  grants: NormalizedStrictGrant[],
  host: string,
  port: number,
  method: string,
  path: string
): NormalizedStrictGrant | null {
  return (
    grants.find(
      grant =>
        grant.host === host &&
        grant.port === port &&
        grant.methods.has(method) &&
        pathAllowed(grant, path)
    ) ?? null
  );
}

function pathAllowed(grant: NormalizedStrictGrant, path: string): boolean {
  return grant.paths.has(path) || grant.pathPrefixes.some(prefix => path.startsWith(prefix));
}

function isWebSocketOrHttp2Attempt(headers: Map<string, string>): boolean {
  const connection = headers.get('connection')?.toLowerCase() ?? '';
  return headers.has('upgrade') || connection.includes('upgrade') || headers.has('http2-settings');
}

async function forwardRequest(
  connection: StrictConnection,
  request: ParsedHttpRequest,
  initialBody: Buffer,
  grant: NormalizedStrictGrant,
  options: StrictHttpsConnectProxyOptions
): Promise<void> {
  const target = connection.target;
  const downstream = connection.tls;
  if (!target || !downstream) {
    closeConnection(connection);
    return;
  }
  const body = await readBoundedBody(
    downstream,
    initialBody,
    request.headers,
    grant.maxBodyBytes,
    options.policy.idleTimeoutMs
  );
  if (connection.released) return;
  if (!body.ok) {
    sendDownstreamError(connection, body.status, body.message);
    return;
  }
  const accounting = await reserveAccountedRequest(options, target, request, body.body, grant);
  if (connection.released || connection.clientClosed) {
    await markAccountingUnknown(options, accounting, 'downstream closed before upstream request');
    return;
  }
  const upstreamBody = accounting?.body ?? body.body;
  const upstream = await createUpstreamRequest(connection, target, request, upstreamBody, options);
  if (connection.released) {
    upstream.response.destroy();
    await markAccountingUnknown(options, accounting, 'downstream closed after upstream request');
    return;
  }
  connection.upstreamResponse = upstream.response;
  streamUpstreamResponse(connection, downstream, upstream, options, accounting);
}

async function readBoundedBody(
  stream: TLSSocket,
  initial: Buffer,
  headers: Map<string, string>,
  limit: number,
  idleTimeoutMs: number
): Promise<{ ok: true; body: Buffer } | { ok: false; status: number; message: string }> {
  const length = contentLength(headers);
  if (length === null) return { ok: false, status: 400, message: 'bad content length' };
  if (initial.length > limit) return { ok: false, status: 413, message: 'body too large' };
  if (length !== undefined && length > limit)
    return { ok: false, status: 413, message: 'body too large' };
  if (headers.has('transfer-encoding'))
    return { ok: false, status: 501, message: 'chunked request not supported' };
  if (length === undefined)
    return initial.length === 0
      ? { ok: true, body: Buffer.alloc(0) }
      : { ok: false, status: 400, message: 'unexpected body' };
  return await readFixedBody(stream, initial, length, idleTimeoutMs);
}

function contentLength(headers: Map<string, string>): number | undefined | null {
  const raw = headers.get('content-length');
  if (raw === undefined) return undefined;
  if (!/^\d+$/.test(raw)) return null;
  const value = Number(raw);
  return Number.isSafeInteger(value) ? value : null;
}

function readFixedBody(
  stream: TLSSocket,
  initial: Buffer,
  length: number,
  idleTimeoutMs: number
): Promise<{ ok: true; body: Buffer } | { ok: false; status: number; message: string }> {
  return new Promise(resolve => {
    let body = initial;
    let settled = false;
    const timer = setTimeout(() => {
      finish({ ok: false, status: 408, message: 'body timeout' });
    }, idleTimeoutMs);
    const onData = (chunk: Buffer): void => {
      body = Buffer.concat([body, chunk]);
      if (body.length > length) finish({ ok: false, status: 413, message: 'body too large' });
      if (body.length === length) finish({ ok: true, body });
    };
    const finish = (
      result: { ok: true; body: Buffer } | { ok: false; status: number; message: string }
    ): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      stream.off('data', onData);
      stream.off('close', interrupted);
      stream.off('end', interrupted);
      stream.off('error', interrupted);
      resolve(result);
    };
    const interrupted = (): void => {
      finish({ ok: false, status: 400, message: 'incomplete body' });
    };
    stream.on('data', onData);
    stream.once('close', interrupted);
    stream.once('end', interrupted);
    stream.once('error', interrupted);
    if (stream.destroyed || stream.readableEnded) {
      interrupted();
      return;
    }
    if (body.length === length) finish({ ok: true, body });
    if (body.length > length) finish({ ok: false, status: 413, message: 'body too large' });
  });
}

async function reserveAccountedRequest(
  options: StrictHttpsConnectProxyOptions,
  target: NonNullable<StrictConnection['target']>,
  request: ParsedHttpRequest,
  body: Buffer,
  grant: NormalizedStrictGrant
): Promise<AccountedReservation | undefined> {
  if (!options.accounting) return undefined;
  const maxResponseBytes = normalizeAccountingResponseBytes(options.accounting.maxResponseBytes);
  const path = originPathname(request.path);
  const requestBody = parseJsonBody(body);
  const policy = matchingAccountingPolicy(
    options.accounting.policies,
    target.host,
    path,
    requestBody
  );
  const prepared = prepareTrustedProviderRequest(policy, {
    method: request.method,
    host: target.host,
    path,
    body: requestBody,
    anthropicBeta: hasAnthropicBetaMarker(request.path),
    headers: requestFeatureHeaders(request.headers),
  });
  assertPreparedBodyWithinGrant(prepared.body, grant.maxBodyBytes);
  const reserved = await options.accounting.client.reserve({
    provider: policy.provider,
    host: target.host,
    method: request.method,
    path: request.path,
    model: policy.model,
    requestHash: requestDigest(request, target.host, prepared.body),
    inputCeiling: prepared.reservation.input,
    outputCeiling: prepared.reservation.output,
  });
  assertLiveReservation(reserved);
  return {
    policy,
    reservationId: reserved.reservationId,
    body: prepared.body,
    maxResponseBytes,
  };
}

function matchingAccountingPolicy(
  policies: TrustedProviderBudgetPolicy[],
  host: string,
  path: string,
  body: unknown
): TrustedProviderBudgetPolicy {
  const model = requestModel(body);
  const policy = policies.find(
    candidate =>
      providerEndpoint(candidate.provider) === path &&
      candidate.host === host &&
      candidate.model === model
  );
  if (!policy) throw new Error('No trusted provider budget policy matched upstream request.');
  return policy;
}

function requestModel(body: unknown): string {
  const record = body && typeof body === 'object' && !Array.isArray(body) ? body : {};
  const model = (record as Record<string, unknown>).model;
  if (typeof model !== 'string' || model.length === 0) {
    throw new Error('Trusted provider request model is required.');
  }
  return model;
}

function providerEndpoint(provider: TrustedProvider): string {
  return provider === 'openai' ? '/v1/responses' : '/v1/messages';
}

function parseJsonBody(body: Buffer): unknown {
  try {
    return JSON.parse(body.toString('utf8')) as unknown;
  } catch {
    throw new Error('Trusted provider request body must be JSON.');
  }
}

function originPathname(path: string): string {
  return path.split('?', 1)[0] ?? path;
}

function hasAnthropicBetaMarker(path: string): boolean {
  const query = path.split('?', 2)[1];
  return new URLSearchParams(query ?? '').get('beta') === 'true';
}

function requestFeatureHeaders(headers: Map<string, string>): Record<string, string> {
  const featureHeaders: Record<string, string> = {};
  for (const [name, value] of headers) {
    if (name.startsWith('anthropic-') || name.startsWith('openai-')) featureHeaders[name] = value;
  }
  return featureHeaders;
}

function requestDigest(request: ParsedHttpRequest, host: string, body: Buffer): string {
  return createHash('sha256')
    .update(request.method)
    .update('\0')
    .update(host)
    .update('\0')
    .update(request.path)
    .update('\0')
    .update(stableFeatureHeaderProfile(requestFeatureHeaders(request.headers)))
    .update('\0')
    .update(body)
    .digest('hex');
}

function stableFeatureHeaderProfile(headers: Record<string, string>): string {
  return JSON.stringify(
    Object.fromEntries(Object.entries(headers).sort(([left], [right]) => left.localeCompare(right)))
  );
}

function assertLiveReservation(reservation: StrictProviderBudgetReservation): void {
  if (!reservation.reservationId) throw new Error('Budget reservation ID is required.');
  if (reservation.deadlineEpochMs !== undefined && reservation.deadlineEpochMs <= Date.now()) {
    throw new Error('Budget reservation deadline has expired.');
  }
}

function normalizeAccountingResponseBytes(value: number | undefined): number {
  if (value === undefined) return 262_144;
  if (!Number.isSafeInteger(value) || value <= 0 || value > 10_485_760) {
    throw new Error('Invalid accounting response byte limit.');
  }
  return value;
}

function assertPreparedBodyWithinGrant(body: Buffer, maxBodyBytes: number): void {
  if (body.length > maxBodyBytes) throw new Error('Prepared provider request body is too large.');
}

async function createUpstreamRequest(
  connection: StrictConnection,
  target: NonNullable<StrictConnection['target']>,
  incoming: ParsedHttpRequest,
  body: Buffer,
  options: StrictHttpsConnectProxyOptions
): Promise<{
  statusLine: string;
  headers: string;
  response: IncomingMessage;
  chunked: boolean;
}> {
  return await new Promise((resolve, reject) => {
    const upstream = httpsRequest(
      {
        host: target.address.address,
        port: target.port,
        method: incoming.method,
        path: incoming.path,
        servername: target.host,
        ca: options.upstreamCa,
        createConnection: socketOptions =>
          createVerifiedUpstreamSocket(connection, socketOptions, target, options),
        headers: sanitizedHeaders(incoming, target.host, target.port, body.length),
        timeout: options.policy.connectTimeoutMs,
      },
      response => {
        const noBody = hasNoResponseBody(incoming.method, response.statusCode ?? 502);
        const chunked = !noBody && shouldChunkResponse(response);
        resolve({
          statusLine: `HTTP/1.1 ${response.statusCode ?? 502} ${response.statusMessage ?? 'Bad Gateway'}\r\n`,
          headers: serializeResponseHeaders(
            response,
            chunked,
            shouldStripContentLength(response.statusCode ?? 502)
          ),
          response,
          chunked,
        });
      }
    );
    connection.upstreamRequest = upstream;
    upstream.once('timeout', () => upstream.destroy(new Error('upstream timeout')));
    upstream.once('error', reject);
    upstream.end(body);
  });
}

function createVerifiedUpstreamSocket(
  connection: StrictConnection,
  socketOptions: RequestOptions,
  target: NonNullable<StrictConnection['target']>,
  options: StrictHttpsConnectProxyOptions
): TLSSocket {
  const overrideSocket = options.connectTarget?.(target.address, target.port, target.host);
  const tlsOptions = normalizeTlsOptions(socketOptions);
  const upstreamSocket: TLSSocket = overrideSocket
    ? tlsConnect({ ...tlsOptions, socket: overrideSocket })
    : tlsConnect(tlsOptions);
  connection.upstreamSocket = upstreamSocket;
  return upstreamSocket;
}

function normalizeTlsOptions(options: RequestOptions): ConnectionOptions {
  return {
    host: typeof options.host === 'string' ? options.host : undefined,
    port: typeof options.port === 'number' ? options.port : undefined,
    servername: options.servername,
    ca: options.ca,
    rejectUnauthorized: true,
  };
}

function sanitizedHeaders(
  request: ParsedHttpRequest,
  host: string,
  port: number,
  bodyLength: number
): Record<string, string> {
  const headers: Record<string, string> = { host: `${host}:${port}`, connection: 'close' };
  for (const [name, value] of request.headers) {
    if (HOP_BY_HOP_HEADERS.has(name) || name === 'host' || name === 'content-length') continue;
    headers[name] = value;
  }
  if (bodyLength > 0) headers['content-length'] = String(bodyLength);
  return headers;
}

function streamUpstreamResponse(
  connection: StrictConnection,
  downstream: TLSSocket,
  upstream: {
    statusLine: string;
    headers: string;
    response: IncomingMessage;
    chunked: boolean;
  },
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation | undefined
): void {
  downstream.write(upstream.statusLine);
  downstream.write(upstream.headers);
  const evidence = accounting ? createResponseEvidence() : undefined;
  const terminalHold = accounting
    ? createTerminalSseHold(upstream.response, accounting.maxResponseBytes)
    : undefined;
  upstream.response.once('error', () => {
    void settleInterruptedAccounting(options, accounting, evidence, 'upstream response error');
    closeConnection(connection);
  });
  upstream.response.once('aborted', () => {
    void settleInterruptedAccounting(
      options,
      accounting,
      evidence,
      'upstream response interrupted'
    );
    closeConnection(connection);
  });
  const resumeUpstream = (): void => {
    upstream.response.resume();
  };
  downstream.on('drain', resumeUpstream);
  downstream.once('close', () => {
    downstream.off('drain', resumeUpstream);
    void settleInterruptedAccounting(
      options,
      accounting,
      evidence,
      'downstream closed before accounted completion'
    );
  });
  upstream.response.on('data', (chunk: Buffer) => {
    collectResponseEvidence(evidence, chunk, accounting?.maxResponseBytes);
    const chunks = writableResponseChunks(terminalHold, chunk);
    if (terminalHold?.overLimit) {
      if (evidence) evidence.overLimit = true;
      upstream.response.destroy(new Error('accounted SSE terminal hold too large'));
      return;
    }
    if (!writeResponseChunks(downstream, chunks, upstream.chunked)) upstream.response.pause();
  });
  upstream.response.once('end', () => {
    void finishAccountedUpstreamResponse(
      connection,
      downstream,
      upstream,
      options,
      accounting,
      evidence,
      terminalHold
    );
  });
}

function createResponseEvidence(): ResponseEvidence {
  return { chunks: [], bytes: 0, overLimit: false, settled: false };
}

function createTerminalSseHold(
  response: IncomingMessage,
  maxBytes = Number.MAX_SAFE_INTEGER
): TerminalSseHold | undefined {
  const contentType = headerValue(response.headers['content-type']).toLowerCase();
  return contentType.includes('text/event-stream')
    ? {
        incomplete: Buffer.alloc(0),
        held: [],
        heldBytes: 0,
        holding: false,
        overLimit: false,
        maxBytes,
      }
    : undefined;
}

function writableResponseChunks(hold: TerminalSseHold | undefined, chunk: Buffer): Buffer[] {
  if (!hold) return [chunk];
  hold.incomplete = Buffer.concat([hold.incomplete, chunk]);
  const writable: Buffer[] = [];
  drainSseFrames(hold, writable);
  enforceTerminalHoldLimit(hold);
  return hold.overLimit ? [] : writable;
}

function drainSseFrames(hold: TerminalSseHold, writable: Buffer[]): void {
  for (;;) {
    const boundary = findSseFrameBoundary(hold.incomplete);
    if (!boundary) return;
    const frame = hold.incomplete.subarray(0, boundary.frameEnd);
    hold.incomplete = hold.incomplete.subarray(boundary.nextFrameStart);
    if (hold.holding || isTerminalSseFrame(frame)) {
      hold.holding = true;
      holdTerminalFrame(hold, frame);
    } else {
      writable.push(Buffer.from(frame));
    }
  }
}

function holdTerminalFrame(hold: TerminalSseHold, frame: Buffer): void {
  const copy = Buffer.from(frame);
  hold.held.push(copy);
  hold.heldBytes += copy.length;
}

function enforceTerminalHoldLimit(hold: TerminalSseHold): void {
  if (hold.heldBytes + hold.incomplete.length <= hold.maxBytes) return;
  hold.overLimit = true;
  hold.held = [];
  hold.incomplete = Buffer.alloc(0);
  hold.heldBytes = 0;
}

function findSseFrameBoundary(
  buffer: Buffer
): { frameEnd: number; nextFrameStart: number } | undefined {
  for (let index = 0; index < buffer.length - 1; index += 1) {
    if (buffer[index] === 10 && buffer[index + 1] === 10) {
      return { frameEnd: index + 2, nextFrameStart: index + 2 };
    }
    if (
      buffer[index] === 13 &&
      buffer[index + 1] === 10 &&
      buffer[index + 2] === 13 &&
      buffer[index + 3] === 10
    ) {
      return { frameEnd: index + 4, nextFrameStart: index + 4 };
    }
  }
  return undefined;
}

function flushTerminalSseHold(
  hold: TerminalSseHold | undefined,
  downstream: TLSSocket,
  chunked: boolean
): void {
  if (!hold || hold.overLimit) return;
  if (hold.incomplete.length > 0) {
    holdTerminalFrame(hold, hold.incomplete);
    hold.incomplete = Buffer.alloc(0);
  }
  for (const chunk of hold.held) writeResponseChunk(downstream, chunk, chunked);
  hold.held = [];
  hold.heldBytes = 0;
}

function isTerminalSseFrame(frame: Buffer): boolean {
  const text = frame.toString('utf8');
  if (text.includes('event: response.completed')) return true;
  if (text.includes('event: response.failed')) return true;
  if (text.includes('event: response.incomplete')) return true;
  if (text.includes('event: message_stop')) return true;
  return text.includes('event: message_delta') && text.includes('stop_reason');
}

function collectResponseEvidence(
  evidence: ResponseEvidence | undefined,
  chunk: Buffer,
  limit: number | undefined
): void {
  if (!evidence || evidence.overLimit) return;
  evidence.bytes += chunk.length;
  if (limit !== undefined && evidence.bytes > limit) {
    evidence.overLimit = true;
    evidence.chunks = [];
    return;
  }
  evidence.chunks.push(Buffer.from(chunk));
}

async function finishAccountedUpstreamResponse(
  connection: StrictConnection,
  downstream: TLSSocket,
  upstream: {
    statusLine: string;
    headers: string;
    response: IncomingMessage;
    chunked: boolean;
  },
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation | undefined,
  evidence: ResponseEvidence | undefined,
  terminalHold: TerminalSseHold | undefined
): Promise<void> {
  if (accounting && evidence) {
    const known = await settleCompletedAccounting(
      options,
      accounting,
      upstream.response,
      downstream,
      evidence
    );
    if (!known) {
      closeConnection(connection);
      return;
    }
  }
  flushTerminalSseHold(terminalHold, downstream, upstream.chunked);
  finishUpstreamResponse(connection, downstream, upstream.chunked);
}

async function settleCompletedAccounting(
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation,
  response: IncomingMessage,
  downstream: TLSSocket,
  evidence: ResponseEvidence
): Promise<boolean> {
  if (evidence.settled) return true;
  if (evidence.overLimit) {
    evidence.settled = true;
    await markAccountingUnknown(options, accounting, 'upstream response too large');
    return false;
  }
  try {
    const settled = await settleAccountedResponse(
      options,
      accounting,
      response,
      downstream,
      Buffer.concat(evidence.chunks)
    );
    evidence.settled = true;
    return settled;
  } catch (error) {
    if (!evidence.settled) {
      evidence.settled = true;
      await tryMarkAccountingUnknown(options, accounting, errorMessage(error));
    }
    return false;
  }
}

async function settleInterruptedAccounting(
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation | undefined,
  evidence: ResponseEvidence | undefined,
  reason: string
): Promise<void> {
  if (!accounting || !evidence || evidence.settled) return;
  evidence.settled = true;
  await tryMarkAccountingUnknown(options, accounting, reason);
}

async function settleAccountedResponse(
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation,
  response: IncomingMessage,
  downstream: TLSSocket,
  body: Buffer
): Promise<boolean> {
  const settlement = parseAccountedResponse(accounting.policy.provider, response, body);
  if (settlement.state === 'complete') {
    await settleKnownUsage(options, accounting, downstream, settlement.usage);
    return true;
  }
  await markAccountingUnknown(options, accounting, settlement.reason);
  return false;
}

async function settleKnownUsage(
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation,
  downstream: TLSSocket,
  usage: ProviderTokenUsage
): Promise<void> {
  if (downstream.destroyed) throw new Error('downstream closed before known settlement');
  const controller = new AbortController();
  const onClose = (): void => {
    controller.abort();
  };
  downstream.once('close', onClose);
  try {
    await options.accounting?.client.settleComplete({
      reservationId: accounting.reservationId,
      usage,
      signal: controller.signal,
    });
  } finally {
    downstream.off('close', onClose);
  }
}

function parseAccountedResponse(
  provider: TrustedProvider,
  response: IncomingMessage,
  body: Buffer
): ReturnType<typeof parseProviderCompletionUsage> {
  if ((response.statusCode ?? 502) < 200 || (response.statusCode ?? 502) >= 300) {
    return { state: 'unknown', reason: 'upstream status was not successful' };
  }
  const contentType = headerValue(response.headers['content-type']).toLowerCase();
  if (contentType.includes('text/event-stream')) {
    return parseProviderUsageSse(provider, [body]);
  }
  if (contentType.includes('application/json')) {
    return parseProviderCompletionUsage(provider, JSON.parse(body.toString('utf8')));
  }
  return { state: 'unknown', reason: 'upstream content type is not accounted' };
}

function headerValue(value: string | string[] | undefined): string {
  if (Array.isArray(value)) return value.join(',');
  return value ?? '';
}

async function markAccountingUnknown(
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation | undefined,
  reason: string
): Promise<void> {
  if (!accounting) return;
  await options.accounting?.client.settleUnknown({
    reservationId: accounting.reservationId,
    reason,
  });
}

async function tryMarkAccountingUnknown(
  options: StrictHttpsConnectProxyOptions,
  accounting: AccountedReservation,
  reason: string
): Promise<void> {
  try {
    await markAccountingUnknown(options, accounting, reason);
  } catch {
    return;
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'upstream accounting failed';
}

function writeResponseChunks(downstream: TLSSocket, chunks: Buffer[], chunked: boolean): boolean {
  let writable = true;
  for (const chunk of chunks) {
    writable = writeResponseChunk(downstream, chunk, chunked) && writable;
  }
  return writable;
}

function writeResponseChunk(downstream: TLSSocket, chunk: Buffer, chunked: boolean): boolean {
  if (chunk.length === 0) return true;
  if (!chunked) return downstream.write(chunk);
  downstream.cork();
  const wroteSize = downstream.write(`${chunk.length.toString(16)}\r\n`);
  const wroteChunk = downstream.write(chunk);
  const wroteEnd = downstream.write('\r\n');
  downstream.uncork();
  return wroteSize && wroteChunk && wroteEnd;
}

function finishUpstreamResponse(
  connection: StrictConnection,
  downstream: TLSSocket,
  chunked: boolean
): void {
  connection.upstreamRequest = undefined;
  connection.upstreamResponse = undefined;
  connection.upstreamSocket = undefined;
  if (chunked) downstream.write('0\r\n\r\n');
  downstream.end();
}

function serializeResponseHeaders(
  response: IncomingMessage,
  chunked: boolean,
  stripContentLength: boolean
): string {
  const lines: string[] = [];
  for (const [name, value] of Object.entries(response.headers)) {
    if (skipResponseHeader(name, chunked, stripContentLength)) continue;
    if (Array.isArray(value)) for (const item of value) lines.push(`${name}: ${item}`);
    else if (value !== undefined) lines.push(`${name}: ${value}`);
  }
  if (chunked) lines.push('transfer-encoding: chunked');
  lines.push('connection: close');
  return `${lines.join('\r\n')}\r\n\r\n`;
}

function shouldChunkResponse(response: IncomingMessage): boolean {
  return response.headers['content-length'] === undefined;
}

function hasNoResponseBody(method: string, statusCode: number): boolean {
  return (
    method === 'HEAD' ||
    (statusCode >= 100 && statusCode < 200) ||
    statusCode === 204 ||
    statusCode === 304
  );
}

function shouldStripContentLength(statusCode: number): boolean {
  return (statusCode >= 100 && statusCode < 200) || statusCode === 204;
}

function skipResponseHeader(name: string, chunked: boolean, stripContentLength: boolean): boolean {
  const lower = name.toLowerCase();
  return (
    HOP_BY_HOP_HEADERS.has(lower) || ((chunked || stripContentLength) && lower === 'content-length')
  );
}

function firstPublicAddress(records: { address: string; family: 4 | 6 }[]): {
  address: string;
  family: 4 | 6;
} {
  for (const record of records) {
    assertPublicAddress(record.address);
    return { address: record.address, family: record.family };
  }
  throw new Error('no usable public address');
}

function sendDownstreamError(connection: StrictConnection, status: number, message: string): void {
  connection.tls?.end(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\n\r\n`);
  setTimeout(() => {
    closeConnection(connection);
  }, 50).unref();
}

function rejectConnection(connection: StrictConnection, status: number, message: string): void {
  writeProxyResponse(connection.raw, status, message);
  closeConnection(connection);
}

function writeProxyResponse(socket: Socket, status: number, message: string, close = true): void {
  if (socket.destroyed) return;
  socket.write(
    `HTTP/1.1 ${status} ${message}\r\nConnection: ${close ? 'close' : 'keep-alive'}\r\n\r\n`
  );
  if (close) setTimeout(() => socket.destroy(), 50).unref();
}

function closeConnection(connection: StrictConnection): void {
  if (connection.pendingAsync) connection.clientClosed = true;
  clearTimers(connection);
  connection.tls?.setTimeout(0);
  connection.raw.setTimeout(0);
  connection.upstreamRequest?.destroy();
  connection.upstreamResponse?.destroy();
  connection.upstreamSocket?.destroy();
  connection.tls?.destroy();
  connection.raw.destroy();
  if (!connection.pendingAsync) releaseConnection(connection);
}

function handleClientClosed(connection: StrictConnection): void {
  connection.clientClosed = true;
  if (connection.pendingAsync) return;
  closeConnection(connection);
}

function releaseConnection(connection: StrictConnection): void {
  if (connection.released) return;
  connection.released = true;
  clearTimers(connection);
  connection.active.delete(connection);
  connection.tls?.setTimeout(0);
  connection.raw.setTimeout(0);
}

function armTimer(connection: StrictConnection, timeoutMs: number, callback: () => void): void {
  const timer = setTimeout(callback, timeoutMs);
  connection.timers.push(timer);
}

function clearTimers(connection: StrictConnection): void {
  for (const timer of connection.timers.splice(0)) clearTimeout(timer);
}

function closeActiveConnectionsBeforeServerClose(
  server: Server,
  active: Set<StrictConnection>
): void {
  const originalClose = server.close.bind(server);
  server.close = (callback?: (err?: Error) => void): Server => {
    for (const connection of Array.from(active)) closeConnection(connection);
    return originalClose(callback);
  };
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

async function resolveHostWithDns(host: string): Promise<{ address: string; family: 4 | 6 }[]> {
  const records = await lookup(host, { all: true, verbatim: true });
  return records.map(record => ({ address: record.address, family: record.family === 6 ? 6 : 4 }));
}
