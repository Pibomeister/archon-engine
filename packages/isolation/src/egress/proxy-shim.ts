import { createConnection, createServer, Socket } from 'net';

const socketPath = process.env.ARCHON_PROXY_SOCKET;
const listenPort = Number(process.env.ARCHON_PROXY_PORT ?? '18080');

if (!socketPath) process.exit(0);

const server = createServer(client => {
  forwardToSocket(client, socketPath);
});

server.listen(listenPort, '127.0.0.1', () => {
  process.stdout.write(`archon-proxy-shim: forwarding 127.0.0.1:${listenPort} to ${socketPath}\n`);
});

function forwardToSocket(client: Socket, path: string): void {
  const upstream = createConnection({ path });
  client.on('error', () => upstream.destroy());
  upstream.on('error', () => client.destroy());
  client.on('close', () => upstream.destroy());
  upstream.on('close', () => client.destroy());
  client.pipe(upstream);
  upstream.pipe(client);
}
