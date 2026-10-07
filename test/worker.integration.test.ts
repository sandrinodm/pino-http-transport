import { fork } from 'node:child_process';
import { once } from 'node:events';
import { resolve } from 'node:path';

import pino from 'pino';
import { describe, expect, it } from 'vite-plus/test';

type ReceiverMessage =
  | { type: 'ready'; port: number }
  | { type: 'batches'; batches: Array<Array<Record<string, unknown>>> };

describe('worker transport lifecycle', () => {
  it('loads the package-name target and drains every batch before the worker finishes', async () => {
    const receiver = fork(resolve(process.cwd(), 'test/fixtures/receiver.cjs'), {
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });

    try {
      const [ready] = (await once(receiver, 'message')) as [ReceiverMessage];
      if (ready.type !== 'ready') {
        throw new Error('Receiver did not provide a port');
      }

      const transport = pino.transport({
        target: 'pino-http-transport',
        options: {
          batchInterval: 60_000,
          batchSize: 2,
          maxRetries: 0,
          url: `http://127.0.0.1:${ready.port}/logs`,
        },
      });
      const logger = pino(transport);

      for (let index = 0; index < 5; index += 1) {
        logger.info({ index }, 'worker record');
      }

      const finished = once(transport, 'finish');
      transport.end();
      await finished;

      receiver.send({ type: 'batches' });
      const [result] = (await once(receiver, 'message')) as [ReceiverMessage];
      if (result.type !== 'batches') {
        throw new Error('Receiver did not provide request batches');
      }

      expect(result.batches.map((batch) => batch.map((record) => record.index))).toEqual([[0, 1], [2, 3], [4]]);
    } finally {
      receiver.send({ type: 'close' });
      await once(receiver, 'exit');
    }
  }, 10_000);

  it('awaits delivery flush before ending a worker whose backlog takes over ten seconds', async () => {
    // Run the logging lifecycle in a normal Node process, outside Vitest's module hooks.
    const driver = fork(resolve(process.cwd(), 'test/fixtures/worker-flush.cjs'), {
      execArgv: [],
      stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    });
    const result = await new Promise<{ elapsed: number; indexes: number[] }>((resolve, reject) => {
      let report: { elapsed: number; indexes: number[] } | undefined;
      driver.once('message', (message) => {
        report = message as typeof report;
      });
      driver.once('error', reject);
      driver.once('exit', (code) => {
        if (code !== 0 || !report) reject(new Error(`Worker flush driver exited without a successful report: ${code}`));
        else resolve(report);
      });
    });
    expect(result.elapsed).toBeGreaterThan(10_000);
    expect(result.indexes).toEqual(Array.from({ length: 2000 }, (_, index) => index));
  }, 20_000);
});
