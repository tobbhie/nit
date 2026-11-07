import { setTimeout as delay } from 'node:timers/promises';
import { FactsRpc } from './execution-facts';

/** Only account reads and slot-time reads; no signing or transaction submission. */
export function executionFactsRpc(
  endpoint: string,
  timeoutMs: number,
  spacingMs: number
): FactsRpc {
  const url = new URL(endpoint);
  if (!['https:', 'http:'].includes(url.protocol) || url.username !== '' || url.password !== '')
    throw new Error('Invalid execution facts RPC endpoint');
  let nextRequest = 0;
  let requestId = 0;
  return async (method, params): Promise<unknown> => {
    if (!['getMultipleAccounts', 'getBlockTime'].includes(method))
      throw new Error('RPC method denied');
    await delay(Math.max(0, nextRequest - Date.now()));
    nextRequest = Date.now() + spacingMs;
    const id = ++requestId;
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), timeoutMs);
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id, method, params }),
        signal: abort.signal,
        redirect: 'error',
      });
      if (!response.ok || response.body === null) throw new Error('RPC response unavailable');
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        const value: unknown = chunk.value;
        if (!(value instanceof Uint8Array)) throw new Error('Invalid RPC body chunk');
        size += value.length;
        if (size > 1048576) {
          await reader.cancel();
          throw new Error('RPC response exceeds 1 MiB');
        }
        chunks.push(value);
      }
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>;
      if (
        body['jsonrpc'] !== '2.0' ||
        body['id'] !== id ||
        body['error'] !== undefined ||
        !('result' in body)
      )
        throw new Error('Invalid RPC response');
      return body['result'];
    } catch {
      throw new Error('Execution facts RPC request failed');
    } finally {
      clearTimeout(timer);
    }
  };
}
