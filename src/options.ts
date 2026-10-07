/**
 * Largest delay accepted by Node's timer implementation.
 */
const MAX_TIMER_DELAY = 2_147_483_647;

/**
 * Configuration for delivering Pino records as JSON batches.
 */
export interface HttpTransportOptions {
  /**
   * HTTP or HTTPS endpoint that receives JSON batches.
   */
  url: string;

  /**
   * Additional request headers. Content-Type defaults to application/json.
   */
  headers?: Record<string, string>;

  /**
   * Per-request timeout in milliseconds. @default 2500
   */
  timeout?: number;

  /**
   * Records per request. @default 100
   */
  batchSize?: number;

  /**
   * Maximum age of a partial batch in milliseconds. @default 5000
   */
  batchInterval?: number;

  /**
   * Retries after the first failed request. @default 2
   */
  maxRetries?: number;

  /**
   * Initial retry delay in milliseconds. @default 1000
   */
  retryDelay?: number;

  /**
   * Suppress transport diagnostics without changing failure semantics. @default false
   */
  silent?: boolean;

  /**
   * Maximum queued records waiting behind the active request. @default 100000
   */
  maxBufferSize?: number;

  /**
   * Maximum UTF-8 bytes in queued records, excluding the active batch. @default 67108864
   */
  maxBufferBytes?: number;

  /**
   * Maximum UTF-8 request body bytes, including JSON array delimiters. @default 1048576
   */
  maxBatchBytes?: number;
}

/**
 * Validated public options with the parsed endpoint used by the HTTP client.
 */
export type ValidatedOptions = Required<Omit<HttpTransportOptions, 'url'>> & {
  /**
   * Parsed HTTP(S) endpoint shared by all request attempts.
   */
  endpoint: URL;
};

/**
 * Validates public options and applies the transport's delivery defaults.
 */
export function validateOptions(input: unknown): ValidatedOptions {
  if (!isPlainRecord(input)) {
    throw new Error('HTTP transport options must be a plain object');
  }

  const url = input.url;
  if (typeof url !== 'string') {
    throw new Error('url must be a valid HTTP or HTTPS URL');
  }

  let endpoint: URL;
  try {
    endpoint = new URL(url);
  } catch {
    throw new Error('url must be a valid HTTP or HTTPS URL');
  }

  if (endpoint.protocol !== 'http:' && endpoint.protocol !== 'https:') {
    throw new Error('url must be a valid HTTP or HTTPS URL');
  }

  const rawHeaders = input.headers === undefined ? {} : input.headers;
  if (!isPlainRecord(rawHeaders)) {
    throw new Error('headers must be a plain record');
  }

  for (const [name, value] of Object.entries(rawHeaders)) {
    if (name.trim().length === 0 || typeof value !== 'string') {
      throw new Error('headers must contain non-empty names and string values');
    }
  }

  const normalizedHeaders = new Headers(rawHeaders as Record<string, string>);
  if (!normalizedHeaders.has('content-type')) {
    normalizedHeaders.set('content-type', 'application/json');
  }

  const timeout = input.timeout === undefined ? 2500 : input.timeout;
  const batchSize = input.batchSize === undefined ? 100 : input.batchSize;
  const batchInterval = input.batchInterval === undefined ? 5000 : input.batchInterval;
  const maxRetries = input.maxRetries === undefined ? 2 : input.maxRetries;
  const retryDelay = input.retryDelay === undefined ? 1000 : input.retryDelay;
  const silent = input.silent === undefined ? false : input.silent;
  const maxBufferSize = input.maxBufferSize === undefined ? 100_000 : input.maxBufferSize;

  const maxBufferBytes = input.maxBufferBytes === undefined ? 64 * 1024 * 1024 : input.maxBufferBytes;
  const maxBatchBytes = input.maxBatchBytes === undefined ? 1024 * 1024 : input.maxBatchBytes;

  requireTimer(timeout, 'timeout', 1);
  requireSafeInteger(batchSize, 'batchSize', 1);
  requireTimer(batchInterval, 'batchInterval', 1);
  requireSafeInteger(maxRetries, 'maxRetries', 0);
  requireTimer(retryDelay, 'retryDelay', 0);
  requireSafeInteger(maxBufferSize, 'maxBufferSize', 1);
  requireSafeInteger(maxBufferBytes, 'maxBufferBytes', 1);
  requireSafeInteger(maxBatchBytes, 'maxBatchBytes', 4);

  if (typeof silent !== 'boolean') {
    throw new Error('silent must be a boolean');
  }

  return {
    endpoint,
    headers: Object.fromEntries(normalizedHeaders.entries()),
    timeout,
    batchSize,
    batchInterval,
    maxRetries,
    retryDelay,
    silent,
    maxBufferSize,
    maxBufferBytes,
    maxBatchBytes,
  };
}

/**
 * Ensures a number can be used safely as a Node timer delay.
 */
function requireTimer(value: unknown, name: string, minimum: number): asserts value is number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum || value > MAX_TIMER_DELAY) {
    throw new Error(`${name} must be a safe integer between ${minimum} and ${MAX_TIMER_DELAY}`);
  }
}

/**
 * Ensures an option is a safe integer at or above its allowed minimum.
 */
function requireSafeInteger(value: unknown, name: string, minimum: number): asserts value is number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`${name} must be a safe integer of at least ${minimum}`);
  }
}

/**
 * Narrows unknown values to ordinary object records used for runtime options.
 */
function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
