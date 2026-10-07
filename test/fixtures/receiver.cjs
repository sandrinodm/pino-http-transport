const { createServer } = require('node:http');

const batches = [];
const responseDelay = Number(process.env.RESPONSE_DELAY ?? 0);
const server = createServer((request, response) => {
  const chunks = [];
  request.on('data', (chunk) => chunks.push(chunk));
  request.on('end', () => {
    batches.push(JSON.parse(Buffer.concat(chunks).toString('utf8')));
    setTimeout(() => {
      response.writeHead(204);
      response.end();
    }, responseDelay);
  });
});

server.listen(0, '127.0.0.1', () => {
  const address = server.address();
  process.send?.({ type: 'ready', port: address.port });
});

process.on('message', (message) => {
  if (message?.type === 'batches') {
    process.send?.({ type: 'batches', batches });
    return;
  }

  if (message?.type === 'close') {
    server.closeAllConnections();
    server.close(() => process.exit(0));
  }
});

process.on('disconnect', () => {
  server.closeAllConnections();
  server.close(() => process.exit(0));
});
