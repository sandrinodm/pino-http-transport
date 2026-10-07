const { fork } = require('node:child_process');
const events = require('node:events');
const path = require('node:path');
const pino = require('pino');

async function main() {
  const receiver = fork(path.join(__dirname, 'receiver.cjs'), {
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    env: { ...process.env, RESPONSE_DELAY: '550' },
  });
  let transport;
  try {
    const [{ port }] = await events.once(receiver, 'message');
    transport = pino.transport({
      target: 'pino-http-transport',
      options: { url: `http://127.0.0.1:${port}/logs`, batchInterval: 60_000 },
    });
    await events.once(transport, 'ready');
    transport.ref();
    const logger = pino(transport);
    for (let index = 0; index < 2000; index++) logger.info({ index }, 'slow worker record');
    const start = performance.now();
    await new Promise((resolve, reject) => transport.flush((error) => (error ? reject(error) : resolve())));
    const elapsed = performance.now() - start;
    const completion = events.once(transport, 'finish');
    transport.end();
    await completion;
    const report = events.once(receiver, 'message');
    receiver.send({ type: 'batches' });
    const [{ batches }] = await report;
    process.send({ elapsed, indexes: batches.flatMap((batch) => batch.map((record) => record.index)) });
  } finally {
    if (transport && !transport.destroyed && !transport.writableEnded) await transport.worker.terminate();
    const exited = events.once(receiver, 'exit');
    receiver.send({ type: 'close' });
    await exited;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
