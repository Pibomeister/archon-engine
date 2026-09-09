import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'fs/promises';
import { createConnection, createServer } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { normalizeEgressPolicy } from './policy';
import { startRestrictedConnectProxy } from './connect-proxy';
import type { HostResolver } from './connect-proxy';
import type { Server } from 'net';

const openServers: Server[] = [];

describe('restricted CONNECT proxy', () => {
  afterEach(async () => {
    await Promise.all(openServers.splice(0).map(closeServer));
  });

  test('rejects non-CONNECT requests through the actual proxy', async () => {
    const socketPath = await tempSocketPath();
    openServers.push(
      await startRestrictedConnectProxy({
        socketPath,
        policy: policy(),
        resolveHost: resolveTo('93.184.216.34', 4),
      })
    );

    await expect(
      proxyExchange(socketPath, 'GET http://allowed.example/ HTTP/1.1\r\n\r\n')
    ).resolves.toContain('400 malformed CONNECT');
  });

  test('rejects denied hostnames before DNS', async () => {
    let dnsCalls = 0;
    const socketPath = await tempSocketPath();
    openServers.push(
      await startRestrictedConnectProxy({
        socketPath,
        policy: policy(),
        resolveHost: async () => {
          dnsCalls += 1;
          return [{ address: '93.184.216.34', family: 4 }];
        },
      })
    );

    await expect(connectRequest(socketPath, 'denied.example', 443)).resolves.toContain(
      '403 target denied'
    );
    expect(dnsCalls).toBe(0);
  });

  test('rejects allowed hostnames that resolve to private or reserved addresses', async () => {
    for (const address of ['127.0.0.1', '192.0.2.1', '::ffff:7f00:1', '0:0:0:0:0:0:0:1']) {
      const socketPath = await tempSocketPath();
      openServers.push(
        await startRestrictedConnectProxy({
          socketPath,
          policy: policy(),
          resolveHost: resolveTo(address, address.includes(':') ? 6 : 4),
        })
      );

      await expect(connectRequest(socketPath, 'allowed.example', 443)).resolves.toContain(
        '403 target denied'
      );
    }
  });

  test('denies mismatched, absent, and encrypted ClientHello SNI before upstream dial', async () => {
    for (const hello of [
      createClientHello('attacker.example'),
      createClientHello(null),
      createClientHello('allowed.example', true),
    ]) {
      const socketPath = await tempSocketPath();
      let dialCount = 0;
      openServers.push(
        await startRestrictedConnectProxy({
          socketPath,
          policy: policy(),
          resolveHost: resolveTo('93.184.216.34', 4),
          connectTarget: () => {
            dialCount += 1;
            return createConnection({ host: '93.184.216.34', port: 443 });
          },
        })
      );

      await expect(tlsExchange(socketPath, 'allowed.example', hello)).resolves.toContain(
        '200 Connection Established'
      );
      expect(dialCount).toBe(0);
    }
  });

  test('allows fragmented ClientHello only when SNI matches CONNECT host', async () => {
    const upstream = createServer(socket => {
      socket.destroy();
    });
    openServers.push(upstream);
    const upstreamPort = await listenOnTcp(upstream);
    const socketPath = await tempSocketPath();
    let dialCount = 0;
    openServers.push(
      await startRestrictedConnectProxy({
        socketPath,
        policy: policy(),
        resolveHost: resolveTo('93.184.216.34', 4),
        connectTarget: () => {
          dialCount += 1;
          return createConnection({ host: '127.0.0.1', port: upstreamPort });
        },
      })
    );

    const hello = createClientHello('allowed.example');
    const first = hello.subarray(0, 7);
    const second = hello.subarray(7);
    const response = await tlsExchange(socketPath, 'allowed.example', [first, second]);

    expect(response).toContain('200 Connection Established');
    expect(dialCount).toBe(1);
  });

  test('does not dial after client closes while DNS is still pending', async () => {
    const socketPath = await tempSocketPath();
    let resolveDns: (records: { address: string; family: 4 | 6 }[]) => void = () => undefined;
    let dialCount = 0;
    const resolverStarted = Promise.withResolvers<void>();
    openServers.push(
      await startRestrictedConnectProxy({
        socketPath,
        policy: policy({ maxConcurrentConnections: 1, dnsTimeoutMs: 1000 }),
        resolveHost: async () => {
          resolverStarted.resolve();
          return await new Promise(resolve => {
            resolveDns = resolve;
          });
        },
        connectTarget: () => {
          dialCount += 1;
          return createConnection({ host: '93.184.216.34', port: 443 });
        },
      })
    );
    const first = createConnection({ path: socketPath });
    await onceConnected(first);
    first.write('CONNECT allowed.example:443 HTTP/1.1\r\nHost: allowed.example:443\r\n\r\n');
    await resolverStarted.promise;
    first.destroy();

    await expect(connectRequest(socketPath, 'allowed.example', 443)).resolves.toContain(
      '503 connection limit'
    );
    resolveDns([{ address: '93.184.216.34', family: 4 }]);
    await Bun.sleep(25);
    expect(dialCount).toBe(0);
  });

  test('enforces maximum concurrent proxy clients', async () => {
    const socketPath = await tempSocketPath();
    openServers.push(
      await startRestrictedConnectProxy({
        socketPath,
        policy: policy({ maxConcurrentConnections: 1, idleTimeoutMs: 1000 }),
        resolveHost: resolveTo('93.184.216.34', 4),
      })
    );
    const held = createConnection({ path: socketPath });
    await onceConnected(held);

    await expect(connectRequest(socketPath, 'allowed.example', 443)).resolves.toContain(
      '503 connection limit'
    );
    held.destroy();
  });
});

function policy(
  overrides: {
    idleTimeoutMs?: number;
    maxConcurrentConnections?: number;
    dnsTimeoutMs?: number;
  } = {}
) {
  return normalizeEgressPolicy({
    targets: [{ host: 'allowed.example', port: 443 }],
    connectTimeoutMs: 50,
    dnsTimeoutMs: overrides.dnsTimeoutMs ?? 50,
    idleTimeoutMs: overrides.idleTimeoutMs ?? 250,
    maxTunnelMs: 1000,
    maxConcurrentConnections: overrides.maxConcurrentConnections ?? 8,
  });
}

function resolveTo(address: string, family: 4 | 6): HostResolver {
  return async () => [{ address, family }];
}

async function tempSocketPath(): Promise<string> {
  return join(await mkdtemp(join(tmpdir(), 'archon-proxy-test-')), 'proxy.sock');
}

function connectRequest(socketPath: string, host: string, port: number): Promise<string> {
  return proxyExchange(
    socketPath,
    `CONNECT ${host}:${port} HTTP/1.1\r\nHost: ${host}:${port}\r\n\r\n`
  );
}

function proxyExchange(socketPath: string, request: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ path: socketPath });
    let response = '';
    socket.setEncoding('utf8');
    socket.once('error', reject);
    socket.on('data', chunk => {
      response += chunk;
    });
    socket.once('connect', () => socket.write(request));
    socket.once('close', () => resolve(response));
  });
}

function onceConnected(socket: ReturnType<typeof createConnection>): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.once('connect', resolve);
    socket.once('error', reject);
  });
}

function closeServer(server: Server): Promise<void> {
  return new Promise(resolve => server.close(() => resolve()));
}

function tlsExchange(socketPath: string, host: string, chunks: Buffer | Buffer[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ path: socketPath });
    const payloads = Array.isArray(chunks) ? chunks : [chunks];
    let response = '';
    let sentTls = false;
    socket.setEncoding('utf8');
    socket.once('error', reject);
    socket.on('data', chunk => {
      response += chunk;
      if (!sentTls && response.includes('200 Connection Established')) {
        sentTls = true;
        for (const payload of payloads) socket.write(payload);
      }
    });
    socket.once('connect', () => {
      socket.write(httpConnectRequest(host));
    });
    socket.once('close', () => resolve(response));
  });
}

function httpConnectRequest(host: string): Buffer {
  return Buffer.concat([
    Buffer.from(`CONNECT ${host}:443 HTTP/1.1`, 'ascii'),
    Buffer.from([13, 10]),
    Buffer.from(`Host: ${host}:443`, 'ascii'),
    Buffer.from([13, 10, 13, 10]),
  ]);
}

function createClientHello(host: string | null, includeEch = false): Buffer {
  const extensions = Buffer.concat([
    host ? sniExtension(host) : Buffer.alloc(0),
    includeEch ? extension(0xfe0d, Buffer.alloc(0)) : Buffer.alloc(0),
  ]);
  const body = Buffer.concat([
    Buffer.from([0x03, 0x03]),
    Buffer.alloc(32),
    Buffer.from([0]),
    Buffer.from([0, 2, 0x13, 0x01]),
    Buffer.from([1, 0]),
    uint16(extensions.length),
    extensions,
  ]);
  const handshake = Buffer.concat([Buffer.from([1]), uint24(body.length), body]);
  return Buffer.concat([Buffer.from([22, 0x03, 0x03]), uint16(handshake.length), handshake]);
}

function sniExtension(host: string): Buffer {
  const name = Buffer.from(host, 'ascii');
  const serverName = Buffer.concat([Buffer.from([0]), uint16(name.length), name]);
  return extension(0, Buffer.concat([uint16(serverName.length), serverName]));
}

function extension(type: number, data: Buffer): Buffer {
  return Buffer.concat([uint16(type), uint16(data.length), data]);
}

function uint16(value: number): Buffer {
  const buffer = Buffer.alloc(2);
  buffer.writeUInt16BE(value);
  return buffer;
}

function uint24(value: number): Buffer {
  return Buffer.from([(value >> 16) & 0xff, (value >> 8) & 0xff, value & 0xff]);
}

function listenOnTcp(server: Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      const address = server.address();
      if (typeof address === 'object' && address) resolve(address.port);
      else reject(new Error('TCP fixture did not expose a port.'));
    });
  });
}
