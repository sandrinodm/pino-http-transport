# pino-http-transport

[![npm version](https://badge.fury.io/js/pino-http-transport.svg)](https://www.npmjs.com/package/pino-http-transport)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A [Pino](https://github.com/pinojs/pino) transport that sends JSON log batches to an HTTP or HTTPS endpoint. It preserves batch order, retries transient delivery failures, bounds memory use, and drains queued records during shutdown.

Requires Node.js 24 or later.

## Install

```bash
pnpm add pino-http-transport
```

## Usage

Use it through Pino's worker transport in production:

```js
import pino from 'pino';

const transport = pino.transport({
  target: 'pino-http-transport',
  options: {
    url: 'https://logs.example.com/ingest',
    headers: { authorization: `Bearer ${process.env.LOG_API_TOKEN}` },
  },
});
const logger = pino(transport);
```

The endpoint receives an HTTP `POST` with a JSON array of Pino log objects. A response is successful only when it has a 2xx status.

### Received JSON

Each request body is an array containing up to `batchSize` records. Standard Pino fields and any structured fields supplied to the logger are preserved:

```json
[
  {
    "level": 30,
    "time": 1785182400000,
    "pid": 4127,
    "hostname": "api-01",
    "requestId": "req_01K1ABCDEF",
    "userId": "user_123",
    "msg": "Checkout completed"
  },
  {
    "level": 40,
    "time": 1785182400125,
    "pid": 4127,
    "hostname": "api-01",
    "requestId": "req_01K1ABCDEG",
    "durationMs": 842,
    "msg": "Slow request"
  }
]
```

Errors logged with `logger.error({ err }, message)` include Pino's serialized error object:

```json
[
  {
    "level": 50,
    "time": 1785182400250,
    "pid": 4127,
    "hostname": "api-01",
    "err": {
      "type": "Error",
      "message": "Database unavailable",
      "stack": "Error: Database unavailable\n    at updateOrder (file:///app/orders.js:42:11)"
    },
    "orderId": "order_456",
    "msg": "Could not update order"
  }
]
```

The exact fields depend on your Pino configuration and the structured values passed to each log call.

For direct embedding, import the ESM entry point:

```js
import pino from 'pino';
import httpTransport from 'pino-http-transport';

const logger = pino(
  httpTransport({
    url: 'https://logs.example.com/ingest',
  })
);
```

## Options

```ts
interface HttpTransportOptions {
  url: string; // Required HTTP(S) endpoint
  headers?: Record<string, string>; // Content-Type defaults to application/json
  timeout?: number; // Per request, ms; default 2500
  batchSize?: number; // Records per request; default 100
  batchInterval?: number; // Partial-batch delay, ms; default 5000
  maxRetries?: number; // Retries after the initial request; default 2
  retryDelay?: number; // Initial exponential-backoff delay, ms; default 1000
  maxBufferSize?: number; // Waiting records; default 100000
  maxBufferBytes?: number; // Waiting JSON bytes (UTF-8); default 64 MiB
  maxBatchBytes?: number; // Complete request body bytes (UTF-8); default 1 MiB
  silent?: boolean; // Suppress diagnostics only; default false
}
```

`url` must use `http:` or `https:`. Numeric options are validated when the transport is created.

`maxBatchBytes` includes the array brackets and commas and must be at least 4.
Records too large to fit in either byte limit are discarded without evicting
valid waiting records. `maxBufferBytes` measures serialized UTF-8 bytes; queue
metadata, string storage, input stream buffers, and the active request also use
memory.

## Receiver example

[`examples/hono-server`](./examples/hono-server) contains a runnable [Hono](https://hono.dev/) server that validates incoming batches and prints each record.

Point the transport at `http://localhost:3000/logs`. See the example's README for configuration and a sample request.

## Delivery and shutdown behavior

- Only one batch is delivered at a time, preserving record order.
- A batch sends when a record or byte limit fills up. A partial batch becomes
  eligible after `batchInterval`, measured from its oldest record's arrival, and
  sends as soon as the active request finishes.
- Network errors, timeouts, and non-2xx responses retry with exponential backoff. The delay is capped at `timeout`.
- When retries are exhausted, the failed batch is reported and subsequent queued batches continue. The transport then fails close so Pino can surface the delivery failure.
- If the waiting queue exceeds `maxBufferSize` or `maxBufferBytes`, the oldest
  waiting records are discarded. The active request is never discarded. A
  smaller `maxBufferSize` also reduces the effective batch size.
- `flush()` delivers records accepted before the call, including partial
  batches, and reports terminal delivery errors. Logging can continue afterward.
- Closing the direct transport drains its active request and queued records.
  Pino's worker `end()` has a 10-second shutdown limit; await a delivery flush
  before ending a worker with a slow receiver or a backlog.

For the single-target worker shown above, stop accepting work and producing
logs, then flush and end the transport. Worker delivery flush requires
thread-stream 4.2 or later and is tested with Pino 10.3.1:

```js
import { once } from 'node:events';

transport.ref(); // Keep the process alive while the worker delivers the backlog.
try {
  await new Promise((resolve, reject) => {
    transport.flush((error) => (error ? reject(error) : resolve()));
  });
  const completion = once(transport, 'finish');
  transport.end();
  await completion;
} finally {
  transport.unref();
}
```

For the directly imported transport, use `await transport.flush()` and then end
the stream through your normal logging lifecycle. Do not call `process.exit()`
immediately after logging.

Delivery preserves FIFO order by sending one request at a time. For high-volume
services, increasing `batchSize` can reduce request overhead and amortize network
latency. Keep `maxBatchBytes` within the receiver's request-body limit and size
the queue for the backlog you want to retain during slow delivery.

## Development

```bash
pnpm install --frozen-lockfile
pnpm check
```

`pnpm check` runs Oxfmt, Oxlint, and TypeScript through Vite+, plus JSDoc validation.
Configure formatting, linting, and tests in `vite.config.ts`; use `pnpm check:fix`
to apply safe fixes. Development tools require a current Node 24 release
(24.15 or later covers release tooling). The transport supports Node 24.0.0 or later.
CI additionally runs coverage, the production build, a package dry run, and a
transport smoke test on Node 24.0.0.

## License

MIT
