import { once } from 'node:events';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { finished } from 'node:stream/promises';
import { setTimeout as realSetTimeout } from 'node:timers';

import pino from 'pino';
import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import httpTransport, { type HttpTransportOptions } from '../src/index.js';

type ReceivedRequest = {
  batch: Array<Record<string, unknown>>;
  body: string;
  headers: IncomingMessage['headers'];
  response: ServerResponse;
};

type Receiver = {
  requests: ReceivedRequest[];
  url: string;
  close: () => Promise<void>;
};

const receivers: Receiver[] = [];

afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(receivers.splice(0).map((receiver) => receiver.close()));
  vi.restoreAllMocks();
});

async function startReceiver(
  respond: (request: ReceivedRequest) => void = ({ response }) => {
    response.writeHead(204);
    response.end();
  }
): Promise<Receiver> {
  const requests: ReceivedRequest[] = [];
  const server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    request.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      const received = {
        batch: JSON.parse(body) as Array<Record<string, unknown>>,
        body,
        headers: request.headers,
        response,
      };
      requests.push(received);
      respond(received);
    });
  });

  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Expected a TCP receiver address');
  }

  const receiver = {
    requests,
    url: `http://127.0.0.1:${address.port}/logs`,
    close: () => closeServer(server),
  };
  receivers.push(receiver);
  return receiver;
}

async function closeServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function closeTransport(transport: ReturnType<typeof httpTransport>): Promise<void> {
  const completion = finished(transport);
  transport.end();
  await completion;
}

async function destroyTransport(transport: ReturnType<typeof httpTransport>, error: Error): Promise<void> {
  const completion = finished(transport);
  transport.destroy(error);
  await completion;
}

function batchIndexes(requests: ReceivedRequest[]): number[][] {
  return requests.map((request) => request.batch.map((record) => Number(record.index)));
}

describe('http transport delivery lifecycle', () => {
  it('drains every full and partial batch during close in FIFO order', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({ url: receiver.url, batchInterval: 60_000, batchSize: 2 });
    const logger = pino(transport);

    for (let index = 0; index < 5; index += 1) {
      logger.info({ index }, 'record');
    }

    await closeTransport(transport);

    expect(batchIndexes(receiver.requests)).toEqual([[0, 1], [2, 3], [4]]);
  });

  it('flushes a partial batch when its interval elapses', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({ url: receiver.url, batchInterval: 5, batchSize: 2 });
    pino(transport).info({ index: 0 }, 'record');

    await vi.waitFor(() => expect(receiver.requests).toHaveLength(1));
    await closeTransport(transport);

    expect(batchIndexes(receiver.requests)).toEqual([[0]]);
  });

  it('sends an overdue partial tail immediately after a blocked full batch', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const firstRequest = Promise.withResolvers<void>();
    const lastRequest = Promise.withResolvers<void>();
    let release: (() => void) | undefined;
    const receiver = await startReceiver(({ batch, response }) => {
      if (batch[0]?.index === 0) {
        release = () => response.writeHead(204).end();
        firstRequest.resolve();
      } else {
        response.writeHead(204).end();
        if (batch[0]?.index === 4) lastRequest.resolve();
      }
    });
    const transport = httpTransport({ url: receiver.url, batchSize: 2, batchInterval: 100 });
    transport.write('{"index":0}\n{"index":1}\n');
    await firstRequest.promise;
    transport.write('{"index":2}\n{"index":3}\n{"index":4}\n');
    await vi.advanceTimersByTimeAsync(300);
    release?.();
    const arrived = await Promise.race([
      lastRequest.promise.then(() => true),
      new Promise<boolean>((resolve) => realSetTimeout(() => resolve(false), 200)),
    ]);
    await closeTransport(transport);
    expect(arrived).toBe(true);
    expect(batchIndexes(receiver.requests)).toEqual([[0, 1], [2, 3], [4]]);
  });

  it('flushes partial batches without closing and does not wait for future records', async () => {
    const responses: ServerResponse[] = [];
    const receiver = await startReceiver(({ response }) => responses.push(response));
    const transport = httpTransport({ url: receiver.url, batchSize: 1, batchInterval: 60_000 });
    transport.write('{"index":0}\n');
    await vi.waitFor(() => expect(responses).toHaveLength(1));
    const flushes = Promise.all([transport.flush(), transport.flush()]);
    transport.write('{"index":1}\n');
    responses[0]?.writeHead(204).end();
    await flushes;
    await vi.waitFor(() => expect(responses).toHaveLength(2));
    expect(transport.destroyed).toBe(false);
    responses[1]?.writeHead(204).end();
    transport.write('{"index":2}\n');
    await vi.waitFor(() => expect(responses).toHaveLength(3));
    responses[2]?.writeHead(204).end();
    await closeTransport(transport);
    expect(batchIndexes(receiver.requests)).toEqual([[0], [1], [2]]);
  });

  it('flushes a partial batch on demand and accepts another partial batch afterward', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({ url: receiver.url, batchInterval: 60_000 });
    transport.write('{"index":0}\n');
    await transport.flush();
    expect(batchIndexes(receiver.requests)).toEqual([[0]]);
    transport.write('{"index":1}\n');
    await transport.flush();
    await closeTransport(transport);
    expect(batchIndexes(receiver.requests)).toEqual([[0], [1]]);
  });

  it('reports terminal delivery errors through flush and close', async () => {
    const receiver = await startReceiver(({ response }) => response.writeHead(503).end());
    const transport = httpTransport({ url: receiver.url, maxRetries: 0, silent: true });
    transport.write('{"index":0}\n');
    await expect(transport.flush()).rejects.toThrow('HTTP 503');
    await expect(closeTransport(transport)).rejects.toThrow('HTTP 503');
  });

  it('supports Pino logger flush callbacks when embedded directly', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({ url: receiver.url, batchInterval: 60_000 });
    const logger = pino(transport);
    logger.info({ index: 0 });
    await new Promise<void>((resolve, reject) => logger.flush((error) => (error ? reject(error) : resolve())));
    expect(batchIndexes(receiver.requests)).toEqual([[0]]);
    await closeTransport(transport);
  });

  it('reports delivery errors to a flush callback without an unhandled rejection', async () => {
    const receiver = await startReceiver(({ response }) => response.writeHead(503).end());
    const transport = httpTransport({ url: receiver.url, maxRetries: 0, silent: true });
    transport.write('{"index":0}\n');
    const error = await new Promise<Error | undefined>((resolve) => {
      void transport.flush(resolve);
    });
    expect(error?.message).toContain('HTTP 503');
    await expect(closeTransport(transport)).rejects.toThrow('HTTP 503');
  });

  it('sends immediately when a buffer smaller than the batch size fills', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({
      url: receiver.url,
      batchSize: 100,
      maxBufferSize: 10,
      batchInterval: 60_000,
    });
    for (let index = 0; index < 10; index++) transport.write(`${JSON.stringify({ index })}\n`);
    await vi.waitFor(() => expect(receiver.requests).toHaveLength(1));
    await closeTransport(transport);
    expect(batchIndexes(receiver.requests)).toEqual([Array.from({ length: 10 }, (_, index) => index)]);
  });

  it('caps complete batch bodies using UTF-8 bytes and preserves serialized Pino fields', async () => {
    const receiver = await startReceiver();
    const records = Array.from({ length: 5 }, (_, index) => ({ index, msg: '🦊', nested: { ok: true } }));
    const maxBatchBytes = Buffer.byteLength(JSON.stringify(records.slice(0, 2)));
    const transport = httpTransport({ url: receiver.url, maxBatchBytes, batchInterval: 60_000 });
    for (const record of records) transport.write(`${JSON.stringify(record)}\n`);
    await closeTransport(transport);
    expect(batchIndexes(receiver.requests)).toEqual([[0, 1], [2, 3], [4]]);
    expect(receiver.requests.flatMap(({ batch }) => batch)).toEqual(records);
    for (const { body, headers } of receiver.requests) {
      expect(Buffer.byteLength(body)).toBeLessThanOrEqual(maxBatchBytes);
      expect(Number(headers['content-length'])).toBe(Buffer.byteLength(body));
    }
  });

  it('drops oversized records without evicting valid waiting records', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({
      url: receiver.url,
      maxBatchBytes: 30,
      maxBufferBytes: 30,
      batchInterval: 60_000,
      silent: true,
    });
    transport.write('{"index":0}\n');
    transport.write(`${JSON.stringify({ index: 1, msg: 'x'.repeat(50) })}\n`);
    await closeTransport(transport);
    expect(batchIndexes(receiver.requests)).toEqual([[0]]);
  });

  it('drops the oldest waiting records under byte pressure while protecting the active request', async () => {
    let release: (() => void) | undefined;
    const receiver = await startReceiver(({ batch, response }) => {
      if (batch[0]?.index === 0) release = () => response.writeHead(204).end();
      else response.writeHead(204).end();
    });
    const transport = httpTransport({
      url: receiver.url,
      batchSize: 1,
      maxBufferBytes: 22,
      silent: true,
    });
    transport.write('{"index":0}\n');
    await vi.waitFor(() => expect(release).toBeDefined());
    transport.write('{"index":1}\n{"index":2}\n{"index":3}\n');
    release?.();
    await closeTransport(transport);
    expect(batchIndexes(receiver.requests)).toEqual([[0], [2], [3]]);
  });

  it('sends an existing partial batch before a new record would overflow an idle byte buffer', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({ url: receiver.url, maxBufferBytes: 20, batchInterval: 60_000 });
    transport.write('{"index":0}\n{"index":1}\n');
    await closeTransport(transport);
    expect(batchIndexes(receiver.requests)).toEqual([[0], [1]]);
  });

  it('preserves FIFO order across sustained queue compaction', async () => {
    const firstRequest = Promise.withResolvers<void>();
    let release: (() => void) | undefined;
    const receiver = await startReceiver(({ batch, response }) => {
      if (batch[0]?.index === 0) {
        release = () => response.writeHead(204).end();
        firstRequest.resolve();
      } else response.writeHead(204).end();
    });
    const transport = httpTransport({ url: receiver.url, batchSize: 100 });
    for (let index = 0; index < 100; index++) transport.write(`${JSON.stringify({ index })}\n`);
    await firstRequest.promise;
    for (let index = 100; index < 10000; index++) transport.write(`${JSON.stringify({ index })}\n`);
    release?.();
    await closeTransport(transport);
    expect(receiver.requests.flatMap(({ batch }) => batch.map(({ index }) => index))).toEqual(
      Array.from({ length: 10000 }, (_, index) => index)
    );
  });

  it('retains JSON validation, Pino metadata, and primitive normalization', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({ url: receiver.url, batchInterval: 60_000 });
    const unknown = vi.fn();
    transport.on('unknown', unknown);
    transport.write('not-json\nnull\n{"index":0,"level":30,"time":123}\n');
    expect(Reflect.get(transport, 'lastObj')).toEqual({ index: 0, level: 30, time: 123 });
    expect(Reflect.get(transport, 'lastLevel')).toBe(30);
    expect(Reflect.get(transport, 'lastTime')).toBe(123);
    transport.write('42\n');
    const primitive = Reflect.get(transport, 'lastObj');
    await closeTransport(transport);
    expect(unknown).toHaveBeenCalledTimes(2);
    expect(receiver.requests[0]?.batch).toEqual([{ index: 0, level: 30, time: 123 }, primitive]);
  });

  it('waits for an in-flight request before close settles', async () => {
    let releaseFirstResponse: (() => void) | undefined;
    let requestCount = 0;
    const receiver = await startReceiver(({ response }) => {
      requestCount += 1;
      if (requestCount === 1) {
        releaseFirstResponse = () => {
          response.writeHead(204);
          response.end();
        };
        return;
      }

      if (requestCount > 1) {
        response.writeHead(204);
        response.end();
      }
    });
    const transport = httpTransport({ url: receiver.url, batchInterval: 60_000, batchSize: 2 });
    const logger = pino(transport);
    logger.info({ index: 0 }, 'record');
    logger.info({ index: 1 }, 'record');
    logger.info({ index: 2 }, 'record');
    logger.info({ index: 3 }, 'record');

    await vi.waitFor(() => expect(receiver.requests).toHaveLength(1));

    let settled = false;
    const completion = closeTransport(transport).then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    releaseFirstResponse?.();
    await completion;

    expect(batchIndexes(receiver.requests)).toEqual([
      [0, 1],
      [2, 3],
    ]);
  });

  it('retries failed requests and keeps later batches in order', async () => {
    let attempts = 0;
    const receiver = await startReceiver(({ response }) => {
      attempts += 1;
      response.writeHead(attempts === 1 ? 500 : 204);
      response.end();
    });
    const transport = httpTransport({
      url: receiver.url,
      batchInterval: 60_000,
      batchSize: 1,
      maxRetries: 1,
      retryDelay: 1,
    });
    const logger = pino(transport);
    logger.info({ index: 0 }, 'first');
    logger.info({ index: 1 }, 'second');

    await closeTransport(transport);

    expect(batchIndexes(receiver.requests)).toEqual([[0], [0], [1]]);
    expect(receiver.requests[0]?.body).toBe(receiver.requests[1]?.body);
  });

  it('continues later batches but rejects close after a terminal delivery failure', async () => {
    let attempts = 0;
    const receiver = await startReceiver(({ response }) => {
      attempts += 1;
      response.writeHead(attempts === 1 ? 503 : 204);
      response.end();
    });
    const transport = httpTransport({
      url: receiver.url,
      batchInterval: 60_000,
      batchSize: 1,
      maxRetries: 0,
      silent: true,
    });
    const logger = pino(transport);
    logger.info({ index: 0 }, 'first');
    logger.info({ index: 1 }, 'second');

    await expect(closeTransport(transport)).rejects.toThrow('HTTP 503');
    expect(batchIndexes(receiver.requests)).toEqual([[0], [1]]);
  });

  it('drains buffered records before propagating a source error', async () => {
    const receiver = await startReceiver();
    const transport = httpTransport({
      url: receiver.url,
      batchInterval: 60_000,
      batchSize: 100,
      maxRetries: 0,
    });
    pino(transport).info({ index: 0 }, 'record before source failure');

    await expect(destroyTransport(transport, new Error('source failed'))).rejects.toThrow('source failed');
    expect(batchIndexes(receiver.requests)).toEqual([[0]]);
  });

  it('preserves both source and terminal delivery errors during close', async () => {
    const receiver = await startReceiver(({ response }) => {
      response.writeHead(503);
      response.end();
    });
    const transport = httpTransport({
      url: receiver.url,
      batchInterval: 60_000,
      batchSize: 100,
      maxRetries: 0,
      silent: true,
    });
    pino(transport).info({ index: 0 }, 'record before combined failure');

    let failure: unknown;
    try {
      await destroyTransport(transport, new Error('source failed'));
    } catch (error) {
      failure = error;
    }

    expect(failure).toBeInstanceOf(AggregateError);
    const aggregate = failure as AggregateError;
    expect(aggregate.message).toBe('Transport source and HTTP delivery failed during close');
    expect(aggregate.errors).toEqual([
      expect.objectContaining({ message: 'source failed' }),
      expect.objectContaining({ message: expect.stringContaining('HTTP 503') }),
    ]);
    expect(batchIndexes(receiver.requests)).toEqual([[0]]);
  });

  it('fails close when a receiver does not respond before the request timeout', async () => {
    const receiver = await startReceiver(() => undefined);
    const transport = httpTransport({
      url: receiver.url,
      batchInterval: 60_000,
      maxRetries: 0,
      silent: true,
      timeout: 10,
    });
    pino(transport).info({ index: 0 }, 'record');

    await expect(closeTransport(transport)).rejects.toThrow('timed out');
    expect(receiver.requests).toHaveLength(1);
  });

  it('drops the oldest queued record when the configured buffer limit is exceeded', async () => {
    let releaseFirstResponse: (() => void) | undefined;
    const receiver = await startReceiver(({ response }) => {
      if (!releaseFirstResponse) {
        releaseFirstResponse = () => {
          response.writeHead(204);
          response.end();
        };
        return;
      }

      response.writeHead(204);
      response.end();
    });
    const transport = httpTransport({
      url: receiver.url,
      batchInterval: 60_000,
      batchSize: 1,
      maxBufferSize: 1,
      silent: true,
    });
    const logger = pino(transport);
    logger.info({ index: 0 }, 'first');
    logger.info({ index: 1 }, 'second');
    logger.info({ index: 2 }, 'third');

    await vi.waitFor(() => expect(receiver.requests).toHaveLength(1));
    releaseFirstResponse?.();
    await closeTransport(transport);

    expect(batchIndexes(receiver.requests)).toEqual([[0], [2]]);
  });

  it('adds a JSON content type without replacing a caller-provided one', async () => {
    const defaultReceiver = await startReceiver();
    const customReceiver = await startReceiver();

    const defaultTransport = httpTransport({ url: defaultReceiver.url, batchInterval: 60_000 });
    pino(defaultTransport).info('record');
    await closeTransport(defaultTransport);

    const customTransport = httpTransport({
      url: customReceiver.url,
      batchInterval: 60_000,
      headers: { 'Content-Type': 'application/vnd.logs+json' },
    });
    pino(customTransport).info('record');
    await closeTransport(customTransport);

    expect(defaultReceiver.requests[0]?.headers['content-type']).toBe('application/json');
    expect(customReceiver.requests[0]?.headers['content-type']).toBe('application/vnd.logs+json');
  });

  it('validates malformed runtime options before creating a transport', () => {
    const invalidOptions: Array<[string, HttpTransportOptions]> = [
      ['missing URL', {} as HttpTransportOptions],
      ['malformed URL', { url: 'http://%' }],
      ['URL protocol', { url: 'file:///tmp/logs' }],
      ['batch size', { url: 'https://logs.example.test', batchSize: 0 }],
      ['timer', { url: 'https://logs.example.test', timeout: 0 }],
      ['oversized timer', { url: 'https://logs.example.test', batchInterval: 2_147_483_648 }],
      ['retry count', { url: 'https://logs.example.test', maxRetries: -1 }],
      ['buffer size', { url: 'https://logs.example.test', maxBufferSize: 0 }],
      ['buffer bytes', { url: 'https://logs.example.test', maxBufferBytes: 0 }],
      ['batch bytes', { url: 'https://logs.example.test', maxBatchBytes: 3 }],
      ['non-object headers', { url: 'https://logs.example.test', headers: null as never }],
      ['headers', { url: 'https://logs.example.test', headers: { authorization: 123 as never } }],
      ['empty header', { url: 'https://logs.example.test', headers: { ' ': 'value' } }],
      ['silent flag', { url: 'https://logs.example.test', silent: 'true' as never }],
    ];

    for (const [, options] of invalidOptions) {
      expect(() => httpTransport(options)).toThrow();
    }
  });
});
