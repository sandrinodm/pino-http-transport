const assert = require('node:assert/strict');
const { fork } = require('node:child_process');
const events = require('node:events');
const path = require('node:path');
const { test } = require('node:test');
const pino = require('pino');

void test(
  'published transport loads, flushes, and shuts down on the minimum Node runtime',
  { timeout: 30_000 },
  async () => {
    const receiver = fork(path.join(__dirname, 'fixtures/receiver.cjs'), {
      stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    });
    let transport;
    try {
      const [{ port }] = await events.once(receiver, 'message');
      transport = pino.transport({
        target: 'pino-http-transport',
        options: { url: `http://127.0.0.1:${port}/logs`, batchSize: 2, batchInterval: 60_000, maxRetries: 0 },
      });
      await events.once(transport, 'ready');
      transport.ref();
      const logger = pino(transport);
      for (let index = 0; index < 5; index++) logger.info({ index }, 'minimum runtime record');
      await new Promise((resolve, reject) => transport.flush((error) => (error ? reject(error) : resolve())));
      const completion = events.once(transport, 'finish');
      transport.end();
      await completion;
      const report = events.once(receiver, 'message');
      receiver.send({ type: 'batches' });
      const [{ batches }] = await report;
      assert.deepEqual(
        batches.map((batch) => batch.map((record) => record.index)),
        [[0, 1], [2, 3], [4]]
      );
    } finally {
      if (transport && !transport.destroyed && !transport.writableEnded) await transport.worker.terminate();
      const exited = events.once(receiver, 'exit');
      receiver.send({ type: 'close' });
      await exited;
    }
  }
);
