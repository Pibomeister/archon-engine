import { mkdtemp, readFile, rm, writeFile } from 'fs/promises';
import { join } from 'path';
import { execFileSync } from 'child_process';
import { createHash } from 'crypto';
import { createConnection } from 'net';
import { createServer as createHttpsServer } from 'https';
import { connect as tlsConnect, createServer as createTlsServer, TLSSocket } from 'tls';
import { Readable } from 'stream';
import type { ServerResponse } from 'http';
import { normalizeEgressPolicy } from './policy';
import type { NormalizedEgressPolicy } from './policy';
import { startStrictHttpsConnectProxy } from './strict-https-proxy';
import type {
  StrictHttpGrant,
  StrictProviderAccountingOptions,
  StrictProviderBudgetClient,
  StrictProviderBudgetReservationInput,
} from './strict-https-proxy';
import type { BudgetStatus } from './proxy-budget-ledger';
import type { Server as NetServer } from 'net';
import type { Server as HttpsServer } from 'https';
import type { Server as TlsServer } from 'tls';

const servers: (NetServer | HttpsServer | TlsServer)[] = [];
const requests: {
  method: string;
  url: string;
  headers: Record<string, string | string[] | undefined>;
  body: string;
}[] = [];

async function main(): Promise<void> {
  const temp = await mkdtemp(join('/tmp', 'as-node-'));
  try {
    const certs = await generateCerts(temp);
    await run('allows approved GET and strips proxy hop-by-hop headers', async () => {
      const fixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
      const proxy = await startProxy(temp, certs, fixture.port);
      const response = await strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'GET',
        '/v1/models',
        '',
        {
          'Proxy-Authorization': 'Bearer secret',
          Connection: 'keep-alive',
        }
      );
      assertIncludes(response, 'HTTP/1.1 200 OK');
      assertIncludes(response, 'ok:GET:/v1/models:');
      assertEqual(
        requests.at(-1)?.headers['proxy-authorization'],
        undefined,
        'proxy auth stripped'
      );
      assertEqual(requests.at(-1)?.headers.connection, 'close', 'fixed upstream connection');
      await closeAll();
    });

    await run('allows approved POST body within grant bound', async () => {
      const fixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
      const proxy = await startProxy(temp, certs, fixture.port);
      const response = await strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/submit',
        'hello'
      );
      assertIncludes(response, 'HTTP/1.1 200 OK');
      assertIncludes(response, 'ok:POST:/v1/submit:hello');
      await closeAll();
    });

    await run(
      'accounts trusted provider requests before upstream and settles terminal usage',
      async () => {
        const fixture = await startProviderFixture(certs.upstreamKey, certs.upstreamCert);
        const client = new RecordingBudgetClient();
        const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
        const body = JSON.stringify({
          model: 'gpt-5.1-pinned',
          input: 'hello',
          store: false,
          stream: true,
          truncation: 'disabled',
        });
        const transformedBody = JSON.stringify({
          model: 'gpt-5.1-pinned',
          input: 'hello',
          store: false,
          stream: true,
          truncation: 'disabled',
          max_output_tokens: 100,
        });

        const first = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          body
        );
        const second = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          body
        );

        assertIncludes(first, 'HTTP/1.1 200 OK');
        assertIncludes(second, 'HTTP/1.1 200 OK');
        assertEqual(client.reservations.length, 2, 'fresh reservation count');
        assertEqual(
          client.reservations[0]?.requestHash,
          client.reservations[1]?.requestHash,
          'retry hash'
        );
        assertEqual(client.settled[0]?.usage.input, 6, 'OpenAI settled input');
        assertEqual(client.settled[0]?.usage.output, 4, 'OpenAI settled output');
        assertEqual(
          requests.filter(request => request.url === '/v1/responses').length,
          2,
          'upstream attempts'
        );
        assertEqual(
          requests.at(-1)?.body,
          transformedBody,
          'missing cap transformed upstream body'
        );
        assertEqual(
          client.reservations[0]?.requestHash,
          requestHash('POST', 'allowed.example', '/v1/responses', transformedBody, {}),
          'request hash covers transformed bytes'
        );

        const smallerBody = JSON.stringify({
          model: 'gpt-5.1-pinned',
          input: 'hello',
          max_output_tokens: 7,
          store: false,
          stream: true,
          truncation: 'disabled',
        });
        assertIncludes(
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'allowed.example',
            'POST',
            '/v1/responses',
            smallerBody
          ),
          'HTTP/1.1 200 OK'
        );
        assertEqual(requests.at(-1)?.body, smallerBody, 'explicit smaller cap preserved');
        assertEqual(client.reservations.at(-1)?.outputCeiling, 7, 'explicit smaller reserve cap');

        const nativeBody = JSON.stringify(nativeOpenAiBody());
        assertIncludes(
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'allowed.example',
            'POST',
            '/v1/responses',
            nativeBody
          ),
          'HTTP/1.1 200 OK'
        );
        const transformedNative = { ...nativeOpenAiBody(), max_output_tokens: 100 };
        assertEqual(
          requests.at(-1)?.body,
          JSON.stringify(transformedNative),
          'native OpenAI body cap transformed upstream'
        );

        const hostileCount = requests.length;
        const denied = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          JSON.stringify({
            model: 'gpt-5.1-pinned',
            input: 'hello',
            max_output_tokens: 10,
            store: false,
            truncation: 'disabled',
            previous_response_id: 'resp_1',
          })
        );
        assertIncludes(denied, '502 upstream failed');
        assertEqual(requests.length, hostileCount, 'denied request never reached upstream');

        const remoteToolCount = requests.length;
        const remoteTool = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          JSON.stringify({
            ...nativeOpenAiBody(),
            input: [
              {
                type: 'additional_tools',
                role: 'developer',
                tools: [
                  {
                    type: 'function',
                    name: 'mcp__remote__tool',
                    parameters: { type: 'object' },
                  },
                ],
              },
            ],
          })
        );
        assertIncludes(remoteTool, '502 upstream failed');
        assertEqual(requests.length, remoteToolCount, 'remote tool never reached upstream');

        const excessCount = requests.length;
        const excess = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          JSON.stringify({
            model: 'gpt-5.1-pinned',
            input: 'hello',
            max_output_tokens: 101,
            store: false,
            truncation: 'disabled',
          })
        );
        assertIncludes(excess, '502 upstream failed');
        assertEqual(requests.length, excessCount, 'excess cap never reached upstream');

        const failingClient = new RecordingBudgetClient({ failReserve: true });
        const failingProxy = await startAccountingProxy(temp, certs, fixture.port, failingClient);
        const failedReserveCount = requests.length;
        const failedReserve = await strictRequest(
          failingProxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          body
        );
        assertIncludes(failedReserve, '502 upstream failed');
        assertEqual(requests.length, failedReserveCount, 'reserve failure never reached upstream');

        const slowClient = new RecordingBudgetClient({ reserveDelayMs: 100 });
        const slowProxy = await startAccountingProxy(temp, certs, fixture.port, slowClient);
        const slowReserveCount = requests.length;
        await strictRequest(
          slowProxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          body,
          {},
          'allowed.example:443',
          { disconnectAfterMs: 20 }
        );
        await sleep(150);
        assertEqual(
          requests.length,
          slowReserveCount,
          'cancel during reserve never reached upstream'
        );
        assertEqual(slowClient.unknown.length, 1, 'cancel during reserve marked unknown');
        await closeAll();
      }
    );

    await run('streams accounted SSE bytes before terminal settlement', async () => {
      const fixture = await startSlowOpenAiSseFixture(certs.upstreamKey, certs.upstreamCert);
      const client = new RecordingBudgetClient();
      const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
      const probe = strictRequestProbe(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        JSON.stringify({
          model: 'gpt-5.1-pinned',
          input: 'hello',
          max_output_tokens: 10,
          store: false,
          stream: true,
          truncation: 'disabled',
        }),
        {},
        'allowed.example:443',
        'response.output_text.delta'
      );

      const firstBody = await probe.firstBody;
      assertIncludes(firstBody, 'response.output_text.delta');
      assertEqual(client.settled.length, 0, 'not settled before terminal event');
      fixture.releaseTerminal();
      const response = await probe.done;
      assertIncludes(response, 'HTTP/1.1 200 OK');
      assertEqual(client.settled.length, 1, 'settled after terminal event');
      await closeAll();
    });

    await run('serializes two accounting lifecycles and rejects a third waiter', async () => {
      const fixture = await startSlowOpenAiSseFixture(certs.upstreamKey, certs.upstreamCert);
      const client = new RecordingBudgetClient();
      const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
      const requestCount = requests.length;
      const body = JSON.stringify({
        model: 'gpt-5.1-pinned',
        input: 'hello',
        max_output_tokens: 10,
        store: false,
        stream: true,
        truncation: 'disabled',
      });

      const first = strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      await waitForRequestCount(requestCount + 1);
      const second = strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      await sleep(25);
      assertEqual(
        requests.length,
        requestCount + 1,
        'queued request did not reserve or reach upstream'
      );
      const third = await strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      assertIncludes(third, 'HTTP/1.1 503 accounting capacity');
      assertEqual(client.reservations.length, 1, 'third request rejected before reserve');

      fixture.releaseTerminal();
      assertIncludes(await first, 'HTTP/1.1 200 OK');
      assertIncludes(await second, 'HTTP/1.1 200 OK');
      assertEqual(client.reservations.length, 2, 'two serialized reservations');
      assertEqual(client.settled.length, 2, 'two durable settlements');
      assertEqual(client.settled[0]?.usage.total, 10, 'first literal settled total');
      assertEqual(client.settled[1]?.usage.total, 10, 'second literal settled total');
      await closeAll();
    });

    await run('removes an aborted accounting waiter without reserving tokens', async () => {
      const fixture = await startSlowOpenAiSseFixture(certs.upstreamKey, certs.upstreamCert);
      const client = new RecordingBudgetClient();
      const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
      const requestCount = requests.length;
      const body = JSON.stringify({
        model: 'gpt-5.1-pinned',
        input: 'hello',
        max_output_tokens: 10,
        store: false,
        stream: true,
        truncation: 'disabled',
      });
      const first = strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      await waitForRequestCount(requestCount + 1);
      await strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body,
        {},
        'allowed.example:443',
        { disconnectAfterMs: 20 }
      );
      fixture.releaseTerminal();
      await first;
      await sleep(25);
      assertEqual(client.reservations.length, 1, 'aborted waiter consumed no reservation');
      assertEqual(client.settled.length, 1, 'active request still settled');
      await closeAll();
    });

    await run(
      'preserves fragmented UTF-8 SSE bytes and supports CRLF terminal boundaries',
      async () => {
        const fixture = await startAdversarialSseFixture(certs.upstreamKey, certs.upstreamCert, {
          kind: 'unicode-crlf',
        });
        const client = new RecordingBudgetClient();
        const proxy = await startAccountingProxy(temp, certs, fixture.port, client, 4096);
        const response = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          JSON.stringify({
            model: 'gpt-5.1-pinned',
            input: 'hello',
            max_output_tokens: 10,
            store: false,
            stream: true,
            truncation: 'disabled',
          })
        );
        assertIncludes(response, 'HTTP/1.1 200 OK');
        assertIncludes(response, 'delta":"€"');
        assertNotIncludes(response, '�');
        assertIncludes(response, 'event: response.completed');
        assertEqual(client.settled.length, 1, 'CRLF terminal settled');
        assertEqual(client.unknown.length, 0, 'CRLF terminal not unknown');
        await closeAll();
      }
    );

    await run('bounds terminal SSE hold and incomplete frame buffering', async () => {
      const body = JSON.stringify({
        model: 'gpt-5.1-pinned',
        input: 'hello',
        max_output_tokens: 10,
        store: false,
        stream: true,
        truncation: 'disabled',
      });

      const tailFixture = await startAdversarialSseFixture(certs.upstreamKey, certs.upstreamCert, {
        kind: 'terminal-large-tail',
      });
      const tailClient = new RecordingBudgetClient();
      const tailProxy = await startAccountingProxy(temp, certs, tailFixture.port, tailClient, 192);
      const tailResponsePromise = strictRequest(
        tailProxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      await waitForReserveAttempts(tailClient, 1);
      const blockedAfterUnknown = strictRequest(
        tailProxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      const tailResponse = await tailResponsePromise;
      assertIncludes(tailResponse, 'HTTP/1.1 200 OK');
      assertNotIncludes(tailResponse, 'event: response.completed');
      assertEqual(tailClient.settled.length, 0, 'large tail not settled known');
      assertEqual(tailClient.unknown.length, 1, 'large tail marked unknown');
      await blockedAfterUnknown;
      assertEqual(tailClient.reserveAttempts, 2, 'unknown release woke next reserve attempt');
      assertEqual(tailClient.reservations.length, 1, 'unknown state remained globally fail closed');
      await closeAll();

      const incompleteFixture = await startAdversarialSseFixture(
        certs.upstreamKey,
        certs.upstreamCert,
        { kind: 'giant-incomplete' }
      );
      const incompleteClient = new RecordingBudgetClient();
      const incompleteProxy = await startAccountingProxy(
        temp,
        certs,
        incompleteFixture.port,
        incompleteClient,
        192
      );
      const incompleteResponse = await strictRequest(
        incompleteProxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      assertIncludes(incompleteResponse, 'HTTP/1.1 200 OK');
      assertNotIncludes(incompleteResponse, 'response.completed');
      assertEqual(incompleteClient.settled.length, 0, 'giant incomplete not settled known');
      assertEqual(incompleteClient.unknown.length, 1, 'giant incomplete marked unknown');
      await closeAll();
    });

    await run(
      'holds reservation when downstream cancels before provider terminal event',
      async () => {
        const fixture = await startSlowOpenAiSseFixture(certs.upstreamKey, certs.upstreamCert);
        const client = new RecordingBudgetClient();
        const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
        const cancelled = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          JSON.stringify({
            model: 'gpt-5.1-pinned',
            input: 'hello',
            max_output_tokens: 10,
            store: false,
            stream: true,
            truncation: 'disabled',
          }),
          {},
          'allowed.example:443',
          { disconnectAfterMs: 40 }
        );
        fixture.releaseTerminal();
        assertIncludes(cancelled, 'response.output_text.delta');
        assertEqual(client.settled.length, 0, 'cancelled stream not settled complete');
        assertEqual(client.unknown.length, 1, 'cancelled stream marked unknown');
        await closeAll();
      }
    );

    await run('holds reservation when downstream cancels while settlement is pending', async () => {
      const fixture = await startProviderFixture(certs.upstreamKey, certs.upstreamCert);
      const client = new RecordingBudgetClient({ settleDelayMs: 100 });
      const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
      const cancelled = await strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        JSON.stringify({
          model: 'gpt-5.1-pinned',
          input: 'hello',
          max_output_tokens: 10,
          store: false,
          stream: true,
          truncation: 'disabled',
        }),
        {},
        'allowed.example:443',
        { disconnectAfterMs: 20 }
      );
      assertIncludes(cancelled, 'HTTP/1.1 200 OK');
      assertEqual(client.settled.length, 0, 'pending settlement was not committed');
      assertEqual(client.unknown.length, 1, 'pending settlement became unknown');
      await closeAll();
    });

    await run('terminal settlement owns the lifecycle while downstream closes', async () => {
      const fixture = await startProviderFixture(certs.upstreamKey, certs.upstreamCert);
      const client = new RecordingBudgetClient({ settleDelayMs: 100, ignoreSettleAbort: true });
      const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
      const requestCount = requests.length;
      const body = JSON.stringify({
        model: 'gpt-5.1-pinned',
        input: 'hello',
        max_output_tokens: 10,
        store: false,
        stream: true,
        truncation: 'disabled',
      });
      const first = strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body,
        {},
        'allowed.example:443',
        { disconnectAfterMs: 20 }
      );
      await waitForRequestCount(requestCount + 1);
      const second = strictRequest(
        proxy.socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/responses',
        body
      );
      await sleep(40);
      assertEqual(client.reservations.length, 1, 'lease held during terminal settlement RPC');
      await first;
      assertIncludes(await second, 'HTTP/1.1 200 OK');
      assertEqual(client.settled.length, 2, 'known terminal settlement committed once per request');
      assertEqual(client.unknown.length, 0, 'downstream close did not race terminal ownership');
      await closeAll();
    });

    await run(
      'holds reservations on unknown provider completion and blocks the chain',
      async () => {
        const fixture = await startProviderFixture(
          certs.upstreamKey,
          certs.upstreamCert,
          'unknown'
        );
        const client = new RecordingBudgetClient();
        const proxy = await startAccountingProxy(temp, certs, fixture.port, client);
        const body = JSON.stringify({
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 10,
          metadata: { session_id: 'synthetic' },
          output_config: { effort: 'high' },
          system: [{ type: 'text', text: 'policy', cache_control: { type: 'ephemeral' } }],
          messages: [
            {
              role: 'user',
              content: [{ type: 'text', text: 'hello', cache_control: { type: 'ephemeral' } }],
            },
          ],
        });

        const unknown = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/messages?beta=true',
          body
        );
        const blockedCount = requests.length;
        const blocked = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/messages?beta=true',
          body
        );

        assertIncludes(unknown, 'HTTP/1.1 200 OK');
        assertIncludes(blocked, '502 upstream failed');
        assertEqual(client.unknown.length, 1, 'unknown ack count');
        assertEqual(requests.length, blockedCount, 'unknown chain blocks upstream');
        await closeAll();
      }
    );

    await run(
      'accounts Anthropic cache usage and rejects interrupted or contradictory streams',
      async () => {
        const cacheFixture = await startProviderFixture(
          certs.upstreamKey,
          certs.upstreamCert,
          'anthropic-cache'
        );
        const client = new RecordingBudgetClient();
        const proxy = await startAccountingProxy(temp, certs, cacheFixture.port, client);
        const body = JSON.stringify({
          model: 'claude-sonnet-4-5-pinned',
          max_tokens: 10,
          messages: [{ role: 'user', content: 'hello' }],
        });
        const cache = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/messages?beta=true',
          body,
          { 'Anthropic-Version': '2023-06-01' }
        );
        assertIncludes(cache, 'HTTP/1.1 200 OK');
        assertEqual(client.settled.at(-1)?.usage.input, 28, 'Anthropic cache input');
        assertEqual(
          requests.at(-1)?.headers['anthropic-version'],
          '2023-06-01',
          'pinned Anthropic version forwarded'
        );
        assertEqual(
          client.reservations.at(-1)?.requestHash,
          requestHash('POST', 'allowed.example', '/v1/messages?beta=true', body, {
            'anthropic-version': '2023-06-01',
          }),
          'request hash covers pinned provider feature headers'
        );
        const featureHeaderCount = requests.length;
        const deniedFeatureHeader = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/messages?beta=true',
          body,
          { 'Anthropic-Beta': 'context-1m-2025-08-07' }
        );
        assertIncludes(deniedFeatureHeader, '502 upstream failed');
        assertEqual(
          requests.length,
          featureHeaderCount,
          'unsupported feature header never reached upstream'
        );
        const noBetaCount = requests.length;
        const noBeta = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/messages',
          body
        );
        assertIncludes(noBeta, '502 upstream failed');
        assertEqual(requests.length, noBetaCount, 'missing beta query never reached upstream');
        await closeAll();

        const badStreamFixture = await startProviderFixture(
          certs.upstreamKey,
          certs.upstreamCert,
          'bad-sse'
        );
        const badClient = new RecordingBudgetClient();
        const badProxy = await startAccountingProxy(temp, certs, badStreamFixture.port, badClient);
        const bad = await strictRequest(
          badProxy.socketPath,
          certs.caCert,
          'allowed.example',
          'POST',
          '/v1/responses',
          JSON.stringify({
            model: 'gpt-5.1-pinned',
            input: 'hello',
            max_output_tokens: 10,
            store: false,
            stream: true,
            truncation: 'disabled',
          })
        );
        assertIncludes(bad, 'HTTP/1.1 200 OK');
        assertEqual(badClient.unknown.length, 1, 'bad stream unknown ack');
        await closeAll();
      }
    );

    await run('separates fragmented HTTP header bounds from allowed body bytes', async () => {
      const fixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
      const socketPath = join(temp, 'fragmented-body.sock');
      servers.push(
        await startStrictHttpsConnectProxy({
          socketPath,
          policy: policy(),
          tls: { key: certs.proxyKey, cert: certs.proxyCert },
          grants: [{ ...grant(), maxBodyBytes: 32_768 }],
          upstreamCa: certs.caCert,
          resolveHost: async () => [{ address: '93.184.216.34', family: 4 }],
          connectTarget: () => createConnection({ host: '127.0.0.1', port: fixture.port }),
        })
      );
      const response = await strictRequest(
        socketPath,
        certs.caCert,
        'allowed.example',
        'POST',
        '/v1/submit',
        'b'.repeat(20_000),
        { 'X-Padding': 'p'.repeat(1024) },
        'allowed.example:443',
        { splitAt: 512 }
      );
      assertIncludes(response, 'HTTP/1.1 200 OK');
      assertEqual(requests.at(-1)?.body.length, 20_000, 'complete approved body');
      await closeAll();
    });

    await run('preserves HEAD and no-body response framing', async () => {
      const fixture = createHttpsServer(
        { key: certs.upstreamKey, cert: certs.upstreamCert },
        (req, res) => {
          const status =
            req.url === '/v1/no-content' ? 204 : req.url === '/v1/not-modified' ? 304 : 200;
          res.writeHead(status, { 'content-length': '17' });
          res.end();
        }
      );
      servers.push(fixture);
      const proxy = await startProxy(temp, certs, await listenTcp(fixture));
      for (const [method, path, statusLine, lengthHeader] of [
        ['HEAD', '/v1/models', 'HTTP/1.1 200 OK', 'content-length: 17'],
        ['GET', '/v1/no-content', 'HTTP/1.1 204 No Content', ''],
        ['GET', '/v1/not-modified', 'HTTP/1.1 304 Not Modified', 'content-length: 17'],
      ] as const) {
        const response = await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          method,
          path
        );
        const parts = response.split('\r\n\r\n');
        assertEqual(parts.length, 2, 'exactly one response header block');
        const [headers = '', body] = parts;
        assertIncludes(headers, statusLine);
        assertNotIncludes(headers, 'transfer-encoding');
        assertEqual(body, '', 'no body or terminal chunk');
        if (lengthHeader) assertIncludes(headers, lengthHeader);
        else assertNotIncludes(headers, 'content-length');
      }
      await closeAll();
    });

    await run('releases partial upload readers immediately on client cancellation', async () => {
      const fixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
      const proxy = await startProxy(temp, certs, fixture.port, { maxConcurrentConnections: 1 });
      const readerSockets = new Set<TLSSocket>();
      const originalOn = readTlsOnMethod();
      TLSSocket.prototype.on = function (
        this: TLSSocket,
        event: string | symbol,
        listener: (...args: unknown[]) => void
      ): TLSSocket {
        if (event === 'data' && listener.name === 'onData') readerSockets.add(this);
        return originalOn.call(this, event, listener);
      } as typeof TLSSocket.prototype.on;
      try {
        const priorRequests = requests.length;
        for (let i = 0; i < 4; i++) {
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'allowed.example',
            'POST',
            '/v1/submit',
            'pending!',
            {},
            'allowed.example:443',
            { declaredBodyBytes: 9, disconnectAfterMs: 25 }
          );
          await sleep(10);
        }
        const retainedReaders = [...readerSockets].filter(
          socket =>
            socket.destroyed &&
            socket.listeners('data').some(listener => listener.name === 'onData')
        );
        assertEqual(retainedReaders.length, 0, 'canceled body readers retained');
        assertEqual(requests.length, priorRequests, 'no incomplete upload forwarded');
      } finally {
        TLSSocket.prototype.on = originalOn as typeof TLSSocket.prototype.on;
        await closeAll();
      }
    });

    await run('bounds response buffering while the downstream client is paused', async () => {
      const fixture = await startStreamingFixture(certs.upstreamKey, certs.upstreamCert);
      const proxy = await startProxy(temp, certs, fixture.port);
      let peakBufferedBytes = 0;
      const originalWrite = readTlsWriteMethod();
      TLSSocket.prototype.write = function (
        this: TLSSocket,
        chunk: string | Buffer | Uint8Array,
        encodingOrCallback?: BufferEncoding | ((err?: Error) => void),
        callback?: (err?: Error) => void
      ): boolean {
        const result = originalWrite.call(this, chunk, encodingOrCallback, callback);
        peakBufferedBytes = Math.max(peakBufferedBytes, this.writableLength);
        return result;
      } as typeof TLSSocket.prototype.write;
      try {
        await strictRequest(
          proxy.socketPath,
          certs.caCert,
          'allowed.example',
          'GET',
          '/v1/models',
          '',
          {},
          'allowed.example:443',
          { pauseForMs: 500 }
        );
        if (peakBufferedBytes > 1_048_576) {
          throw new Error(`Unbounded paused-client response buffer: ${peakBufferedBytes}`);
        }
      } finally {
        TLSSocket.prototype.write = originalWrite as typeof TLSSocket.prototype.write;
        await closeAll();
      }
    });

    await run(
      'rejects mismatched SNI, Host, port, denied path, traversal, and upgrades',
      async () => {
        const truncatedFixture = await startTruncatedFixture(certs.upstreamKey, certs.upstreamCert);
        const truncatedProxy = await startProxy(temp, certs, truncatedFixture.port);
        const truncated = await strictRequest(
          truncatedProxy.socketPath,
          certs.caCert,
          'allowed.example',
          'GET',
          '/v1/models'
        );
        assertIncludes(truncated, 'HTTP/1.1 200 OK');
        assertNotIncludes(truncated, '0\r\n\r\n');
        await closeAll();

        const fixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
        const proxy = await startProxy(temp, certs, fixture.port);
        assertIncludes(
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'attacker.example',
            'GET',
            '/v1/models'
          ),
          'TLS_ERROR'
        );
        assertIncludes(
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models',
            '',
            {},
            'evil.example:443'
          ),
          '421 misdirected request'
        );
        assertIncludes(
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models',
            '',
            {},
            'allowed.example:444'
          ),
          '421 misdirected request'
        );
        assertIncludes(
          await strictRequest(proxy.socketPath, certs.caCert, 'allowed.example', 'GET', '/admin'),
          '403 request denied'
        );
        assertIncludes(
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/%2e%2e/secrets'
          ),
          '400 bad request'
        );
        assertIncludes(
          await strictRequest(
            proxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models',
            '',
            { Upgrade: 'websocket' }
          ),
          '501 upgrade not supported'
        );

        await closeAll();

        const timeoutFixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
        const timeoutProxy = await startProxy(temp, certs, timeoutFixture.port, {
          idleTimeoutMs: 100,
          maxConcurrentConnections: 1,
        });
        const idleClient = createConnection({ path: timeoutProxy.socketPath });
        await onceConnected(idleClient);
        await sleep(200);
        assertIncludes(
          await strictRequest(
            timeoutProxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models'
          ),
          '200 OK'
        );
        await closeAll();

        const cancelFixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
        const cancelProxy = await startProxy(temp, certs, cancelFixture.port, {
          maxConcurrentConnections: 1,
        });
        const cancelledClient = createConnection({ path: cancelProxy.socketPath });
        await onceConnected(cancelledClient);
        cancelledClient.destroy();
        await sleep(50);
        assertIncludes(
          await strictRequest(
            cancelProxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models'
          ),
          '200 OK'
        );
      }
    );

    await run(
      'rejects private DNS, bad upstream certificate, missing config, large headers/body, and frees slots',
      async () => {
        const socketPath = join(temp, `proxy-${Date.now()}.sock`);
        await assertRejects(
          () =>
            startStrictHttpsConnectProxy({
              socketPath,
              policy: policy(),
              tls: { key: certs.proxyKey, cert: certs.proxyCert },
              grants: [],
            }),
          'at least one HTTP grant'
        );
        await assertRejects(
          () =>
            startStrictHttpsConnectProxy({
              socketPath,
              policy: policy(),
              tls: { key: '', cert: certs.proxyCert },
              grants: grants(),
            }),
          'requires TLS key'
        );
        await assertRejects(
          () =>
            startStrictHttpsConnectProxy({
              socketPath,
              policy: policy(),
              tls: { key: certs.proxyKey, cert: certs.proxyCert },
              grants: [unsupportedGrant()],
            }),
          'unsupported key'
        );
        await assertRejects(
          () =>
            startStrictHttpsConnectProxy({
              socketPath,
              policy: policy(),
              tls: { key: certs.proxyKey, cert: certs.proxyCert },
              grants: [{ ...grant(), methods: ['get'] }],
            }),
          'invalid method'
        );

        const privateProxy = await startStrictHttpsConnectProxy({
          socketPath: join(temp, 'private.sock'),
          policy: policy(),
          tls: { key: certs.proxyKey, cert: certs.proxyCert },
          grants: grants(),
          resolveHost: async () => [{ address: '127.0.0.1', family: 4 }],
        });
        servers.push(privateProxy);
        assertIncludes(await rawConnect(join(temp, 'private.sock')), '403 target denied');
        await closeAll();

        let resolvePendingDns: (records: { address: string; family: 4 | 6 }[]) => void = () =>
          undefined;
        let resolverCalls = 0;
        let markResolverStarted: () => void = () => undefined;
        const resolverStarted = new Promise<void>(resolve => {
          markResolverStarted = resolve;
        });
        const pendingProxySocket = join(temp, 'pending-dns.sock');
        const pendingProxy = await startStrictHttpsConnectProxy({
          socketPath: pendingProxySocket,
          policy: policy({ maxConcurrentConnections: 1 }),
          tls: { key: certs.proxyKey, cert: certs.proxyCert },
          grants: grants(),
          resolveHost: async () => {
            resolverCalls += 1;
            if (resolverCalls > 1) return [{ address: '93.184.216.34', family: 4 }];
            markResolverStarted();
            return await new Promise(dnsResolve => {
              resolvePendingDns = dnsResolve;
            });
          },
        });
        servers.push(pendingProxy);
        const disconnected = createConnection({ path: pendingProxySocket });
        await onceConnected(disconnected);
        disconnected.write(
          'CONNECT allowed.example:443 HTTP/1.1\r\nHost: allowed.example:443\r\n\r\n'
        );
        await resolverStarted;
        disconnected.destroy();
        assertIncludes(await rawConnect(pendingProxySocket), '503 connection limit');
        resolvePendingDns([{ address: '93.184.216.34', family: 4 }]);
        await sleep(100);
        assertIncludes(await rawConnect(pendingProxySocket), '200 Connection Established');
        await closeAll();

        let resolveTimedOutDns: (records: { address: string; family: 4 | 6 }[]) => void = () =>
          undefined;
        let timedOutResolverCalls = 0;
        let timedOutUpstreamDials = 0;
        const timeoutProxySocket = join(temp, 'timeout-dns.sock');
        const timeoutProxy = await startStrictHttpsConnectProxy({
          socketPath: timeoutProxySocket,
          policy: policy({ dnsTimeoutMs: 80, maxConcurrentConnections: 1 }),
          tls: { key: certs.proxyKey, cert: certs.proxyCert },
          grants: grants(),
          connectTarget: () => {
            timedOutUpstreamDials += 1;
            throw new Error('late upstream dial');
          },
          resolveHost: async () => {
            timedOutResolverCalls += 1;
            if (timedOutResolverCalls > 1) return [{ address: '93.184.216.34', family: 4 }];
            return await new Promise(dnsResolve => {
              resolveTimedOutDns = dnsResolve;
            });
          },
        });
        servers.push(timeoutProxy);
        assertIncludes(await rawConnect(timeoutProxySocket), '403 target denied');
        assertIncludes(await rawConnect(timeoutProxySocket), '503 connection limit');
        assertEqual(timedOutResolverCalls, 1, 'timed-out DNS reservation retained');
        resolveTimedOutDns([{ address: '93.184.216.34', family: 4 }]);
        await sleep(100);
        assertEqual(timedOutUpstreamDials, 0, 'no late upstream connection');
        assertIncludes(await rawConnect(timeoutProxySocket), '200 Connection Established');
        await closeAll();

        const bad = await generateLeaf(
          temp,
          'bad-upstream',
          'allowed.example',
          certs.badCaKey,
          certs.badCaCert
        );
        const badFixture = await startFixture(bad.key, bad.cert);
        const badProxy = await startProxy(temp, certs, badFixture.port);
        assertIncludes(
          await strictRequest(
            badProxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models'
          ),
          '502 upstream failed'
        );
        await closeAll();

        const wrongName = await generateLeaf(
          temp,
          'wrong-name-upstream',
          'other.example',
          certs.caKey,
          certs.caCert
        );
        const wrongNameFixture = await startFixture(wrongName.key, wrongName.cert);
        const wrongNameProxy = await startProxy(temp, certs, wrongNameFixture.port);
        assertIncludes(
          await strictRequest(
            wrongNameProxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models'
          ),
          '502 upstream failed'
        );
        await closeAll();

        const slowFixture = await startFixture(certs.upstreamKey, certs.upstreamCert, 500);
        const deadlineProxy = await startProxy(temp, certs, slowFixture.port, {
          maxConcurrentConnections: 1,
          maxTunnelMs: 100,
        });
        assertIncludes(
          await strictRequest(
            deadlineProxy.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models'
          ),
          'TLS_ERROR'
        );
        await sleep(150);
        await closeAll();

        const fixture = await startFixture(certs.upstreamKey, certs.upstreamCert);
        const limited = await startProxy(temp, certs, fixture.port, {
          maxConcurrentConnections: 1,
        });
        await sleep(100);
        assertIncludes(
          await strictRequest(
            limited.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models',
            '',
            {
              'X-Fill': 'a'.repeat(20_000),
            }
          ),
          '431 headers too large'
        );
        await sleep(100);
        assertIncludes(
          await strictRequest(
            limited.socketPath,
            certs.caCert,
            'allowed.example',
            'POST',
            '/v1/submit',
            '01234567890'
          ),
          '413 body too large'
        );
        await sleep(100);
        assertIncludes(
          await strictRequest(
            limited.socketPath,
            certs.caCert,
            'allowed.example',
            'GET',
            '/v1/models'
          ),
          '200 OK'
        );
        await closeAll();
      }
    );
  } finally {
    await closeAll();
    await rm(temp, { recursive: true, force: true });
  }
}

async function startProxy(
  temp: string,
  certs: Certs,
  fixturePort: number,
  overrides: {
    dnsTimeoutMs?: number;
    idleTimeoutMs?: number;
    maxConcurrentConnections?: number;
    maxTunnelMs?: number;
  } = {}
): Promise<{ socketPath: string }> {
  const socketPath = join(temp, `proxy-${Date.now()}-${Math.random().toString(16).slice(2)}.sock`);
  const server = await startStrictHttpsConnectProxy({
    socketPath,
    policy: policy(overrides),
    tls: { key: certs.proxyKey, cert: certs.proxyCert },
    grants: grants(),
    upstreamCa: certs.caCert,
    resolveHost: async () => [{ address: '93.184.216.34', family: 4 }],
    connectTarget: () => createConnection({ host: '127.0.0.1', port: fixturePort }),
  });
  servers.push(server);
  return { socketPath };
}

async function startAccountingProxy(
  temp: string,
  certs: Certs,
  fixturePort: number,
  client: StrictProviderBudgetClient,
  maxResponseBytes = 1048576
): Promise<{ socketPath: string }> {
  const socketPath = join(
    temp,
    `accounting-${Date.now()}-${Math.random().toString(16).slice(2)}.sock`
  );
  const server = await startStrictHttpsConnectProxy({
    socketPath,
    policy: policy(),
    tls: { key: certs.proxyKey, cert: certs.proxyCert },
    grants: [{ ...grant(), maxBodyBytes: 4096 }],
    upstreamCa: certs.caCert,
    accounting: { ...accounting(client), maxResponseBytes },
    resolveHost: async () => [{ address: '93.184.216.34', family: 4 }],
    connectTarget: () => createConnection({ host: '127.0.0.1', port: fixturePort }),
  });
  servers.push(server);
  return { socketPath };
}

function accounting(client: StrictProviderBudgetClient): StrictProviderAccountingOptions {
  return {
    client,
    policies: [
      {
        provider: 'openai',
        host: 'allowed.example',
        model: 'gpt-other-pinned',
        maxInputTokens: 500,
        maxOutputTokens: 50,
      },
      {
        provider: 'openai',
        host: 'allowed.example',
        model: 'gpt-5.1-pinned',
        maxInputTokens: 1000,
        maxOutputTokens: 100,
      },
      {
        provider: 'anthropic',
        host: 'allowed.example',
        model: 'claude-sonnet-4-5-pinned',
        maxInputTokens: 1000,
        maxOutputTokens: 100,
        anthropicBeta: true,
        allowedHeaders: { 'anthropic-version': '2023-06-01' },
      },
    ],
  };
}

function nativeOpenAiBody(): Record<string, unknown> {
  return {
    model: 'gpt-5.1-pinned',
    input: [
      { type: 'additional_tools', role: 'developer', tools: nativeOpenAiTools() },
      { type: 'message', role: 'developer', content: [{ type: 'input_text', text: 'policy' }] },
      { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'hello' }] },
    ],
    client_metadata: { originator: 'archon-test' },
    include: ['reasoning.encrypted_content'],
    parallel_tool_calls: false,
    prompt_cache_key: 'archon-test-cache',
    reasoning: { effort: 'high', summary: 'auto' },
    store: false,
    stream: true,
    text: { format: { type: 'text' } },
    tool_choice: 'auto',
    truncation: 'disabled',
  };
}

function nativeOpenAiTools(): Record<string, unknown>[] {
  return [
    {
      type: 'custom',
      name: 'exec',
      description: 'synthetic local executor',
      format: { type: 'grammar', syntax: 'lark', definition: 'start: /[\\s\\S]+/' },
    },
    {
      type: 'function',
      name: 'wait',
      strict: false,
      parameters: { type: 'object', properties: { cell_id: { type: 'string' } } },
    },
  ];
}

function policy(
  overrides: {
    dnsTimeoutMs?: number;
    idleTimeoutMs?: number;
    maxConcurrentConnections?: number;
    maxTunnelMs?: number;
  } = {}
): NormalizedEgressPolicy {
  return normalizeEgressPolicy({
    targets: [{ host: 'allowed.example', port: 443 }],
    connectTimeoutMs: 500,
    dnsTimeoutMs: overrides.dnsTimeoutMs ?? 500,
    idleTimeoutMs: overrides.idleTimeoutMs ?? 500,
    maxTunnelMs: overrides.maxTunnelMs ?? 3000,
    maxConcurrentConnections: overrides.maxConcurrentConnections ?? 8,
  });
}

function grant(): StrictHttpGrant {
  return {
    host: 'allowed.example',
    port: 443,
    methods: ['GET', 'POST', 'HEAD'],
    paths: ['/v1/models', '/v1/submit'],
    pathPrefixes: ['/v1/'],
    maxBodyBytes: 10,
  };
}

function unsupportedGrant(): StrictHttpGrant {
  const value = { ...grant(), credentials: true };
  return value;
}

function grants(): StrictHttpGrant[] {
  return [grant()];
}

async function startFixture(key: string, cert: string, delayMs = 0): Promise<{ port: number }> {
  const server = createHttpsServer({ key, cert }, (req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      requests.push({ method: req.method ?? '', url: req.url ?? '', headers: req.headers, body });
      setTimeout(() => {
        res.writeHead(200, { 'content-type': 'text/plain' });
        res.end(`ok:${req.method}:${req.url}:${body}`);
      }, delayMs);
    });
  });
  servers.push(server);
  return { port: await listenTcp(server) };
}

async function startTruncatedFixture(key: string, cert: string): Promise<{ port: number }> {
  const server = createTlsServer({ key, cert }, socket => {
    socket.once('data', () => {
      socket.write('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\npartial\r\n');
      setTimeout(() => socket.destroy(), 25);
    });
  });
  servers.push(server);
  return { port: await listenTcp(server) };
}

async function startStreamingFixture(key: string, cert: string): Promise<{ port: number }> {
  const server = createHttpsServer({ key, cert }, (_req, res) => {
    const stream = Readable.from(
      (function* (): Generator<Buffer> {
        for (let i = 0; i < 256; i += 1) yield Buffer.alloc(65_536, 65);
      })()
    );
    res.once('close', () => stream.destroy());
    stream.pipe(res);
  });
  servers.push(server);
  return { port: await listenTcp(server) };
}

async function startAdversarialSseFixture(
  key: string,
  cert: string,
  options: { kind: 'unicode-crlf' | 'terminal-large-tail' | 'giant-incomplete' }
): Promise<{ port: number }> {
  const server = createHttpsServer({ key, cert }, (req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      requests.push({ method: req.method ?? '', url: req.url ?? '', headers: req.headers, body });
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      writeAdversarialSse(res, options.kind);
    });
  });
  servers.push(server);
  return { port: await listenTcp(server) };
}

function writeAdversarialSse(
  res: ServerResponse,
  kind: 'unicode-crlf' | 'terminal-large-tail' | 'giant-incomplete'
): void {
  if (kind === 'unicode-crlf') {
    const euroFrame = Buffer.from(
      'event: response.output_text.delta\r\ndata: {"type":"response.output_text.delta","delta":"€"}\r\n\r\n'
    );
    const split = euroFrame.indexOf(Buffer.from('€'));
    res.write(euroFrame.subarray(0, split + 1));
    res.write(euroFrame.subarray(split + 1));
    res.end(
      'event: response.completed\r\ndata: {"response":{"status":"completed","usage":{"input_tokens":6,"output_tokens":4,"total_tokens":10}}}\r\n\r\n'
    );
    return;
  }
  if (kind === 'terminal-large-tail') {
    res.write(
      'event: response.completed\ndata: {"response":{"status":"completed","usage":{"input_tokens":6,"output_tokens":4,"total_tokens":10}}}\n\n'
    );
    res.end(`event: response.output_text.delta\ndata: {"delta":"${'x'.repeat(4096)}"}\n\n`);
    return;
  }
  res.end(`event: response.completed\ndata: ${'x'.repeat(4096)}`);
}

type ProviderFixtureMode = 'openai' | 'unknown' | 'anthropic-cache' | 'bad-sse';

async function startProviderFixture(
  key: string,
  cert: string,
  mode: ProviderFixtureMode = 'openai'
): Promise<{ port: number }> {
  const server = createHttpsServer({ key, cert }, (req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      requests.push({ method: req.method ?? '', url: req.url ?? '', headers: req.headers, body });
      writeProviderResponse(res, mode);
    });
  });
  servers.push(server);
  return { port: await listenTcp(server) };
}

async function startSlowOpenAiSseFixture(
  key: string,
  cert: string
): Promise<{ port: number; releaseTerminal: () => void }> {
  let releaseTerminal = (): void => undefined;
  const terminalReleased = new Promise<void>(resolve => {
    releaseTerminal = resolve;
  });
  const server = createHttpsServer({ key, cert }, async (req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      requests.push({ method: req.method ?? '', url: req.url ?? '', headers: req.headers, body });
    });
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(
      'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello"}\n\n'
    );
    await terminalReleased;
    res.end(
      'event: response.completed\ndata: {"response":{"status":"completed","usage":{"input_tokens":6,"output_tokens":4,"total_tokens":10}}}\n\n'
    );
  });
  servers.push(server);
  return { port: await listenTcp(server), releaseTerminal };
}

function writeProviderResponse(res: ServerResponse, mode: ProviderFixtureMode): void {
  if (mode === 'bad-sse') {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.end(
      'event: response.completed\ndata: {"response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}\n\n' +
        'event: response.failed\ndata: {}\n\n'
    );
    return;
  }
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify(providerResponse(mode)));
}

function providerResponse(mode: ProviderFixtureMode): unknown {
  if (mode === 'unknown') return { stop_reason: 'pause_turn', usage: null };
  if (mode === 'anthropic-cache') {
    return {
      stop_reason: 'end_turn',
      usage: {
        input_tokens: 7,
        cache_read_input_tokens: 20,
        cache_creation_input_tokens: 1,
        output_tokens: 11,
      },
    };
  }
  return {
    status: 'completed',
    incomplete_details: null,
    usage: {
      input_tokens: 6,
      input_tokens_details: { cached_tokens: 2 },
      output_tokens: 4,
      output_tokens_details: { reasoning_tokens: 1 },
      total_tokens: 10,
    },
  };
}

class RecordingBudgetClient implements StrictProviderBudgetClient {
  reserveAttempts = 0;
  readonly reservations: StrictProviderBudgetReservationInput[] = [];
  readonly settled: {
    reservationId: string;
    usage: { input: number; output: number; total: number };
  }[] = [];
  readonly unknown: { reservationId: string; reason: string }[] = [];

  constructor(
    private readonly options: {
      failReserve?: boolean;
      reserveDelayMs?: number;
      settleDelayMs?: number;
      ignoreSettleAbort?: boolean;
    } = {}
  ) {}

  async getStatus(): Promise<BudgetStatus> {
    return {
      grant: {
        schema: 'archon.proxy-budget-grant.v1',
        rootChainId: 'test-root',
        runId: 'test-run',
        workflowDigest: 'sha256:test-workflow',
        policyDigest: 'sha256:test-policy',
        deadlineEpochMs: Date.now() + 60_000,
        inputTokenLimit: 10_000,
        outputTokenLimit: 10_000,
        totalTokenLimit: 20_000,
      },
      pendingReservations: 0,
      unknownReservations: 0,
      consumedInputTokens: 0,
      consumedOutputTokens: 0,
      consumedTotalTokens: 0,
      remainingInputTokens: 10_000,
      remainingOutputTokens: 10_000,
      remainingTotalTokens: 20_000,
      acceptingReservations: true,
    };
  }

  async reserve(input: StrictProviderBudgetReservationInput): Promise<{ reservationId: string }> {
    this.reserveAttempts += 1;
    if (this.options.reserveDelayMs !== undefined) await sleep(this.options.reserveDelayMs);
    if (this.options.failReserve) throw new Error('reserve failed');
    if (this.unknown.length > 0) throw new Error('unknown reservation blocks additional requests');
    const reservationId = `reservation-${this.reservations.length + 1}`;
    this.reservations.push(input);
    return { reservationId };
  }

  async settleComplete(input: {
    reservationId: string;
    usage: { input: number; output: number; total: number };
    signal?: AbortSignal;
  }): Promise<void> {
    if (this.options.settleDelayMs !== undefined) {
      if (this.options.ignoreSettleAbort) await sleep(this.options.settleDelayMs);
      else await sleepUntilSettledOrAborted(this.options.settleDelayMs, input.signal);
    }
    if (input.signal?.aborted && !this.options.ignoreSettleAbort)
      throw new Error('settlement aborted');
    this.settled.push(input);
  }

  async settleUnknown(input: { reservationId: string; reason: string }): Promise<void> {
    this.unknown.push(input);
  }

  async close(): Promise<void> {
    await Promise.resolve();
  }
}

function sleepUntilSettledOrAborted(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    const abort = (): void => {
      clearTimeout(timer);
      reject(new Error('settlement aborted'));
    };
    signal?.addEventListener('abort', abort, { once: true });
  });
}

type TlsOn = (
  this: TLSSocket,
  event: string | symbol,
  listener: (...args: unknown[]) => void
) => TLSSocket;

type TlsWrite = (
  this: TLSSocket,
  chunk: string | Buffer | Uint8Array,
  encodingOrCallback?: BufferEncoding | ((err?: Error) => void),
  callback?: (err?: Error) => void
) => boolean;

function readTlsOnMethod(): TlsOn {
  const value: unknown = Reflect.get(TLSSocket.prototype, 'on');
  if (typeof value !== 'function') throw new Error('missing TLSSocket.on');
  return value as TlsOn;
}

function readTlsWriteMethod(): TlsWrite {
  const value: unknown = Reflect.get(TLSSocket.prototype, 'write');
  if (typeof value !== 'function') throw new Error('missing TLSSocket.write');
  return value as TlsWrite;
}

interface RequestProbeOptions {
  splitAt?: number;
  pauseForMs?: number;
  declaredBodyBytes?: number;
  disconnectAfterMs?: number;
}

function strictRequest(
  socketPath: string,
  ca: string,
  sni: string,
  method: string,
  path: string,
  body = '',
  headers: Record<string, string> = {},
  hostHeader = 'allowed.example:443',
  probe: RequestProbeOptions = {}
): Promise<string> {
  return new Promise(resolve => {
    const raw = createConnection({ path: socketPath });
    let proxy = Buffer.alloc(0);
    raw.on('data', chunk => {
      proxy = Buffer.concat([proxy, chunk]);
      if (!proxy.toString('latin1').includes('\r\n\r\n')) return;
      raw.removeAllListeners('data');
      const tls: TLSSocket = tlsConnect({ socket: raw, servername: sni, ca });
      let response = '';
      tls.setEncoding('utf8');
      tls.once('secureConnect', () => {
        if (probe.pauseForMs !== undefined) {
          tls.pause();
          setTimeout(() => tls.destroy(), probe.pauseForMs);
        }
        if (probe.disconnectAfterMs !== undefined) {
          setTimeout(() => tls.destroy(), probe.disconnectAfterMs);
        }
        const extra = Object.entries(headers)
          .map(([name, value]) => `${name}: ${value}\r\n`)
          .join('');
        const request = `${method} ${path} HTTP/1.1\r\nHost: ${hostHeader}\r\n${extra}Content-Length: ${probe.declaredBodyBytes ?? Buffer.byteLength(body)}\r\n\r\n${body}`;
        if (probe.splitAt === undefined) tls.write(request);
        else {
          tls.write(request.slice(0, probe.splitAt));
          setTimeout(() => tls.write(request.slice(probe.splitAt)), 10);
        }
      });
      tls.on('data', (chunk: string) => {
        response += chunk;
      });
      tls.once('error', (error: Error) => {
        resolve(`TLS_ERROR ${error.message}`);
      });
      tls.once('close', () => {
        resolve(response || 'TLS_ERROR closed');
      });
    });
    raw.once('connect', () =>
      raw.write('CONNECT allowed.example:443 HTTP/1.1\r\nHost: allowed.example:443\r\n\r\n')
    );
    raw.once('error', error => {
      resolve(`SOCKET_ERROR ${error.message}`);
    });
  });
}

function strictRequestProbe(
  socketPath: string,
  ca: string,
  sni: string,
  method: string,
  path: string,
  body: string,
  headers: Record<string, string>,
  hostHeader: string,
  firstBodyNeedle: string
): { firstBody: Promise<string>; done: Promise<string> } {
  let resolveFirst = (_value: string): void => undefined;
  const firstBody = new Promise<string>(resolve => {
    resolveFirst = resolve;
  });
  const done = runStrictRequestProbe(
    socketPath,
    ca,
    sni,
    buildHttpRequest(method, path, body, headers, hostHeader),
    firstBodyNeedle,
    resolveFirst
  );
  return { firstBody, done };
}

function runStrictRequestProbe(
  socketPath: string,
  ca: string,
  sni: string,
  request: string,
  firstBodyNeedle: string,
  resolveFirst: (value: string) => void
): Promise<string> {
  return new Promise(resolve => {
    const raw = createConnection({ path: socketPath });
    let proxy = Buffer.alloc(0);
    raw.on('data', chunk => {
      proxy = Buffer.concat([proxy, chunk]);
      if (!proxy.toString('latin1').includes('\r\n\r\n')) return;
      raw.removeAllListeners('data');
      attachTlsProbe(raw, ca, sni, request, firstBodyNeedle, resolveFirst, resolve);
    });
    raw.once('connect', () =>
      raw.write('CONNECT allowed.example:443 HTTP/1.1\r\nHost: allowed.example:443\r\n\r\n')
    );
    raw.once('error', error => {
      resolve(`SOCKET_ERROR ${error.message}`);
    });
  });
}

function attachTlsProbe(
  raw: ReturnType<typeof createConnection>,
  ca: string,
  sni: string,
  request: string,
  firstBodyNeedle: string,
  resolveFirst: (value: string) => void,
  resolveDone: (value: string) => void
): void {
  const tls: TLSSocket = tlsConnect({ socket: raw, servername: sni, ca });
  let response = '';
  let firstResolved = false;
  tls.setEncoding('utf8');
  tls.once('secureConnect', () => {
    tls.write(request);
  });
  tls.on('data', (chunk: string) => {
    response += chunk;
    if (!firstResolved && response.includes(firstBodyNeedle)) {
      firstResolved = true;
      resolveFirst(response);
    }
  });
  tls.once('error', (error: Error) => {
    resolveDone(`TLS_ERROR ${error.message}`);
  });
  tls.once('close', () => {
    if (!firstResolved) resolveFirst(response || 'TLS_ERROR closed');
    resolveDone(response || 'TLS_ERROR closed');
  });
}

function buildHttpRequest(
  method: string,
  path: string,
  body: string,
  headers: Record<string, string>,
  hostHeader: string
): string {
  const extra = Object.entries(headers)
    .map(([name, value]) => `${name}: ${value}\r\n`)
    .join('');
  return `${method} ${path} HTTP/1.1\r\nHost: ${hostHeader}\r\n${extra}Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`;
}

function requestHash(
  method: string,
  host: string,
  path: string,
  body: string,
  featureHeaders: Record<string, string>
): string {
  return createHash('sha256')
    .update(method)
    .update('\0')
    .update(host)
    .update('\0')
    .update(path)
    .update('\0')
    .update(JSON.stringify(Object.fromEntries(Object.entries(featureHeaders).sort())))
    .update('\0')
    .update(body)
    .digest('hex');
}

function rawConnect(socketPath: string, extraHeader = ''): Promise<string> {
  return new Promise(resolve => {
    const raw = createConnection({ path: socketPath });
    let response = '';
    raw.setEncoding('utf8');
    raw.on('data', chunk => {
      response += chunk;
    });
    raw.once('connect', () =>
      raw.write(
        `CONNECT allowed.example:443 HTTP/1.1\r\nHost: allowed.example:443\r\n${extraHeader}\r\n`
      )
    );
    raw.once('close', () => {
      resolve(response);
    });
    raw.once('error', error => {
      resolve(`SOCKET_ERROR ${error.message}`);
    });
  });
}

async function generateCerts(temp: string): Promise<Certs> {
  const ca = await generateCa(temp, 'ca');
  const badCa = await generateCa(temp, 'bad-ca');
  const proxy = await generateLeaf(temp, 'proxy', 'allowed.example', ca.key, ca.cert);
  const upstream = await generateLeaf(temp, 'upstream', 'allowed.example', ca.key, ca.cert);
  return {
    caKey: ca.key,
    caCert: ca.cert,
    badCaKey: badCa.key,
    badCaCert: badCa.cert,
    proxyKey: proxy.key,
    proxyCert: proxy.cert,
    upstreamKey: upstream.key,
    upstreamCert: upstream.cert,
  };
}

interface Certs {
  caKey: string;
  caCert: string;
  badCaKey: string;
  badCaCert: string;
  proxyKey: string;
  proxyCert: string;
  upstreamKey: string;
  upstreamCert: string;
}

async function generateCa(temp: string, name: string): Promise<{ key: string; cert: string }> {
  const keyPath = join(temp, `${name}.key`);
  const certPath = join(temp, `${name}.crt`);
  execFileSync(
    'openssl',
    [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-days',
      '1',
      '-subj',
      `/CN=${name}`,
      '-addext',
      'basicConstraints=critical,CA:TRUE',
      '-addext',
      'keyUsage=critical,keyCertSign,cRLSign',
      '-keyout',
      keyPath,
      '-out',
      certPath,
    ],
    { stdio: 'ignore' }
  );
  return { key: await readFile(keyPath, 'utf8'), cert: await readFile(certPath, 'utf8') };
}

async function generateLeaf(
  temp: string,
  name: string,
  host: string,
  caKey: string,
  caCert: string
): Promise<{ key: string; cert: string }> {
  const caKeyPath = join(temp, `${name}-ca.key`);
  const caCertPath = join(temp, `${name}-ca.crt`);
  const keyPath = join(temp, `${name}.key`);
  const csrPath = join(temp, `${name}.csr`);
  const certPath = join(temp, `${name}.crt`);
  const extPath = join(temp, `${name}.ext`);
  await writeFile(caKeyPath, caKey, { mode: 0o600 });
  await writeFile(caCertPath, caCert);
  await writeFile(extPath, `subjectAltName=DNS:${host}\nextendedKeyUsage=serverAuth\n`);
  execFileSync(
    'openssl',
    [
      'req',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-subj',
      `/CN=${host}`,
      '-keyout',
      keyPath,
      '-out',
      csrPath,
    ],
    { stdio: 'ignore' }
  );
  execFileSync(
    'openssl',
    [
      'x509',
      '-req',
      '-in',
      csrPath,
      '-CA',
      caCertPath,
      '-CAkey',
      caKeyPath,
      '-CAcreateserial',
      '-days',
      '1',
      '-out',
      certPath,
      '-extfile',
      extPath,
    ],
    { stdio: 'ignore' }
  );
  return { key: await readFile(keyPath, 'utf8'), cert: await readFile(certPath, 'utf8') };
}

function listenTcp(server: HttpsServer | TlsServer): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (typeof address === 'object' && address) resolve(address.port);
      else reject(new Error('fixture did not listen'));
    });
  });
}

async function closeAll(): Promise<void> {
  await Promise.all(
    servers.splice(0).map(
      server =>
        new Promise<void>(resolve =>
          server.close(() => {
            resolve();
          })
        )
    )
  );
}

async function run(name: string, test: () => Promise<void>): Promise<void> {
  try {
    await test();
    process.stdout.write(`ok ${name}\n`);
  } catch (error) {
    await closeAll();
    throw new Error(`${name}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function onceConnected(socket: ReturnType<typeof createConnection>): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.once('connect', resolve);
    socket.once('error', reject);
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitForRequestCount(expected: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (requests.length >= expected) return;
    await sleep(5);
  }
  throw new Error(`Timed out waiting for ${expected} upstream requests.`);
}

async function waitForReserveAttempts(
  client: RecordingBudgetClient,
  expected: number
): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (client.reserveAttempts >= expected) return;
    await sleep(5);
  }
  throw new Error(`Timed out waiting for ${expected} reservation attempts.`);
}

function assertIncludes(value: string, expected: string): void {
  if (!value.includes(expected))
    throw new Error(`expected ${JSON.stringify(value)} to include ${JSON.stringify(expected)}`);
}

function assertNotIncludes(value: string, expected: string): void {
  if (value.includes(expected))
    throw new Error(`expected ${JSON.stringify(value)} not to include ${JSON.stringify(expected)}`);
}

function assertEqual(actual: unknown, expected: unknown, label: string): void {
  if (actual !== expected)
    throw new Error(`${label}: expected ${String(expected)}, got ${String(actual)}`);
}

async function assertRejects(
  fn: () => undefined | Promise<unknown>,
  expected: string
): Promise<void> {
  try {
    await fn();
  } catch (error) {
    assertIncludes(error instanceof Error ? error.message : String(error), expected);
    return;
  }
  throw new Error(`expected rejection containing ${expected}`);
}

main()
  .then(() => {
    process.stdout.write('STRICT_HTTPS_FIXTURE=PASS\n');
  })
  .catch(error => {
    console.error(error);
    process.exit(1);
  });
