import build from 'pino-abstract-transport';

import { postJson } from './http.js';
import { type HttpTransportOptions, validateOptions } from './options.js';
import { RecordQueue } from './queue.js';

export type { HttpTransportOptions } from './options.js';

/**
 * Pino-compatible stream with an asynchronous delivery flush for safe worker shutdown.
 */
export type HttpTransport = ReturnType<typeof build> & {
  /**
   * Delivers records accepted before this call without ending the stream.
   */
  flush(callback?: (error?: Error) => void): Promise<void>;
};

/**
 * Tracks the oldest active record so a flush need not wait for future batches.
 */
interface ActiveDelivery {
  /**
   * Sequence of the first record in this batch.
   */
  firstSequence: number;

  /**
   * Completion after all delivery attempts and failure reporting.
   */
  completion: Promise<void>;
}

/**
 * Sends validated Pino JSON in ordered HTTP batches and exposes an awaitable flush.
 */
export default function httpTransport(options: HttpTransportOptions): HttpTransport {
  const config = validateOptions(options);
  const buffer = new RecordQueue();
  const batchSize = Math.min(config.batchSize, config.maxBufferSize);
  let activeDelivery: ActiveDelivery | undefined;
  let deliveryError: Error | undefined;
  let droppedRecords = 0;
  let nextSequence = 0;
  let flushThrough = -1;
  let flushTimer: ReturnType<typeof setTimeout> | undefined;
  let closing = false;

  /**
   * Reports discarded records at a bounded frequency during sustained pressure.
   */
  function reportDrop(reason: string): void {
    droppedRecords += 1;
    if (!config.silent && (droppedRecords === 1 || droppedRecords % 1000 === 0)) {
      console.warn(`[pino-http-transport] ${reason}; dropped ${droppedRecords} record(s).`);
    }
  }

  /**
   * Clears the timer whenever the queue's oldest record changes.
   */
  function clearFlushTimer(): void {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = undefined;
    }
  }

  /**
   * Preserves the oldest waiting record's deadline even behind an active request.
   */
  function scheduleFlush(): void {
    if (flushTimer || buffer.length === 0 || closing) {
      return;
    }
    const remaining = config.batchInterval - (performance.now() - buffer.oldestReceivedAt);
    // An overdue batch is started by the active delivery's completion callback.
    if (remaining <= 0) {
      return;
    }
    flushTimer = setTimeout(() => {
      flushTimer = undefined;
      maybeStartDelivery();
    }, Math.ceil(remaining));
  }

  /**
   * Starts a FIFO batch when a limit, deadline, explicit flush, or close requires it.
   */
  function maybeStartDelivery(force = false): void {
    if (activeDelivery || buffer.length === 0) {
      return;
    }
    const due = performance.now() - buffer.oldestReceivedAt >= config.batchInterval;
    const full =
      buffer.length >= batchSize ||
      buffer.bytes + buffer.length + 1 >= config.maxBatchBytes ||
      buffer.bytes >= config.maxBufferBytes;
    if (!force && !closing && buffer.oldestSequence > flushThrough && !due && !full) {
      scheduleFlush();
      return;
    }

    clearFlushTimer();
    const firstSequence = buffer.oldestSequence;
    const records = buffer.take(batchSize, config.maxBatchBytes);
    activeDelivery = {
      firstSequence,
      completion: sendBatch(records)
        .catch((error: unknown) => {
          const failure = toError(error);
          deliveryError ??= failure;
          if (!config.silent) {
            console.error('[pino-http-transport] Log delivery failed:', failure);
          }
        })
        .finally(() => {
          activeDelivery = undefined;
          maybeStartDelivery();
        }),
    };
    scheduleFlush();
  }

  /**
   * Encodes a batch once and reuses its bytes across bounded retry attempts.
   */
  async function sendBatch(records: string[]): Promise<void> {
    const body = Buffer.from(`[${records.join(',')}]`);
    // Release queue entries while retaining only the encoded request for retries.
    records.length = 0;
    for (let attempt = 0; attempt <= config.maxRetries; attempt += 1) {
      try {
        await postJson(config.endpoint, config.headers, body, config.timeout);
        return;
      } catch (error) {
        if (attempt === config.maxRetries) {
          throw toError(error);
        }
        const delay = Math.min(config.retryDelay * 2 ** attempt, config.timeout);
        await new Promise<void>((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  /**
   * Flushes a snapshot, including partial batches, without preventing future writes.
   */
  async function flushRecords(): Promise<void> {
    const target = nextSequence - 1;
    flushThrough = Math.max(flushThrough, target);
    maybeStartDelivery();
    while (activeDelivery && activeDelivery.firstSequence <= target) {
      await activeDelivery.completion;
    }
    if (deliveryError) {
      throw deliveryError;
    }
  }

  /**
   * Supports both awaitable delivery flushes and Pino's callback-based logger.flush().
   */
  function flush(callback?: (error?: Error) => void): Promise<void> {
    const completion = flushRecords();
    if (!callback) {
      return completion;
    }
    return completion.then(
      () => callback(),
      (error: unknown) => callback(toError(error))
    );
  }

  /**
   * Retains validated JSON within count and byte limits, protecting the active batch.
   */
  function enqueueRecord(json: string): void {
    const bytes = Buffer.byteLength(json);
    if (bytes > config.maxBufferBytes || bytes + 2 > config.maxBatchBytes) {
      reportDrop('Record exceeds the configured byte limit');
      return;
    }

    if (buffer.bytes + bytes > config.maxBufferBytes) {
      maybeStartDelivery(true);
    }
    while (buffer.length >= config.maxBufferSize || buffer.bytes + bytes > config.maxBufferBytes) {
      buffer.dropOldest();
      clearFlushTimer();
      reportDrop('Buffer limit exceeded; discarded the oldest waiting record');
    }
    buffer.enqueue(json, bytes, performance.now(), nextSequence++);
    maybeStartDelivery();
    scheduleFlush();
  }

  const stream = build(
    (source) => {
      // A data listener accepts every line before an immediate worker end.
      source.on('data', (line: string) => {
        const first = line.trimStart()[0];
        // Preserve the dependency's primitive normalization and its exact metadata timestamp.
        enqueueRecord(first === '{' || first === '[' ? line : JSON.stringify(Reflect.get(source, 'lastObj')));
      });
    },
    {
      // The dependency still validates JSON and populates Pino metadata.
      parse: 'lines',
      /**
       * Drains accepted records and preserves both source and delivery failures.
       */
      close: async (sourceError?: Error) => {
        closing = true;
        clearFlushTimer();
        try {
          await flushRecords();
        } catch (error) {
          const deliveryFailure = toError(error);
          if (sourceError) {
            throw new AggregateError(
              [sourceError, deliveryFailure],
              'Transport source and HTTP delivery failed during close'
            );
          }
          throw deliveryFailure;
        }
        if (sourceError) {
          throw sourceError;
        }
      },
    }
  );
  return Object.assign(stream, { flush });
}

/**
 * Normalizes thrown values for consistent reporting and propagation.
 */
function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
