import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';

/**
 * Sends a JSON body through Node's HTTP client and resolves only for 2xx responses.
 */
export function postJson(endpoint: URL, headers: Record<string, string>, body: Buffer, timeout: number): Promise<void> {
  const request = endpoint.protocol === 'https:' ? httpsRequest : httpRequest;

  return new Promise((resolve, reject) => {
    let settled = false;
    let timeoutId: ReturnType<typeof setTimeout> | undefined;

    /**
     * Settles the request promise at most once and clears its timeout.
     */
    function settle(error?: Error): void {
      if (settled) {
        return;
      }

      settled = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    }

    const outgoing = request(
      endpoint,
      {
        method: 'POST',
        headers: {
          ...headers,
          'content-length': String(body.length),
        },
      },
      (response) => {
        response.resume();
        response.once('error', settle);
        response.once('end', () => {
          const status = response.statusCode ?? 0;
          if (status >= 200 && status < 300) {
            settle();
            return;
          }

          const description = response.statusMessage ? ` ${response.statusMessage}` : '';
          settle(new Error(`HTTP ${status}${description}`));
        });
      }
    );

    outgoing.once('error', settle);
    timeoutId = setTimeout(() => {
      outgoing.destroy(new Error(`HTTP request timed out after ${timeout}ms`));
    }, timeout);
    outgoing.end(body);
  });
}
