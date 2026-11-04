import { Result, failure, success } from '@nit/domain';
import { SolanaWalletTransaction } from './contracts';

const DEFAULT_TIMEOUT_MS = 15_000;
const DEFAULT_MIN_INTERVAL_MS = 200;

interface RpcHttpResponse {
  readonly ok: boolean;
  readonly status: number;
  json(): Promise<unknown>;
}

export type WalletRpcFetch = (
  input: string,
  init: {
    method: 'POST';
    headers: Readonly<Record<string, string>>;
    body: string;
    signal: AbortSignal;
  }
) => Promise<RpcHttpResponse>;

export interface WalletSignatureInfo {
  readonly signature: string;
  readonly slot: number;
  readonly failed: boolean;
}

export interface WalletSignaturePage {
  readonly entries: readonly WalletSignatureInfo[];
  readonly nextBefore?: string;
}

export interface SolanaWalletRpcOptions {
  endpoint: string;
  fetch?: WalletRpcFetch;
  requestTimeoutMs?: number;
  minRequestIntervalMs?: number;
  clock?: () => number;
  sleep?: (milliseconds: number) => Promise<void>;
}

/** Rate-limited Solana JSON-RPC transport shared by realtime repair and reconciliation. */
export class SolanaWalletRpc {
  private readonly endpoint: string;
  private readonly fetch: WalletRpcFetch;
  private readonly requestTimeoutMs: number;
  private readonly minRequestIntervalMs: number;
  private readonly clock: () => number;
  private readonly sleep: (milliseconds: number) => Promise<void>;
  private requestId = 0;
  private nextRequestAt = 0;
  private rateLimitTail: Promise<void> = Promise.resolve();

  constructor(options: SolanaWalletRpcOptions) {
    this.endpoint = httpEndpoint(options.endpoint);
    this.fetch = options.fetch ?? ((input, init): Promise<RpcHttpResponse> => fetch(input, init));
    this.requestTimeoutMs = positive(
      options.requestTimeoutMs,
      DEFAULT_TIMEOUT_MS,
      'request timeout'
    );
    this.minRequestIntervalMs = nonnegative(
      options.minRequestIntervalMs,
      DEFAULT_MIN_INTERVAL_MS,
      'minimum request interval'
    );
    this.clock = options.clock ?? Date.now;
    this.sleep =
      options.sleep ??
      ((milliseconds): Promise<void> =>
        new Promise((resolve) => setTimeout(resolve, milliseconds)));
  }

  async getFinalizedSlot(): Promise<Result<number, Error>> {
    const response = await this.rpc('getSlot', [{ commitment: 'finalized' }]);
    return response.kind === 'failure'
      ? response
      : isSlot(response.value)
        ? success(response.value)
        : failure(new Error('Solana RPC returned an invalid finalized slot'));
  }

  async getSignaturesForAddress(
    walletAddress: string,
    before: string | undefined,
    limit: number
  ): Promise<Result<WalletSignaturePage, Error>> {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000) {
      return failure(new Error('Wallet signature page limit must be between 1 and 1000'));
    }
    const config: Record<string, unknown> = { commitment: 'finalized', limit };
    if (before !== undefined) config['before'] = before;
    const response = await this.rpc('getSignaturesForAddress', [walletAddress, config]);
    if (response.kind === 'failure') return response;
    if (!Array.isArray(response.value))
      return failure(new Error('Wallet signature page is malformed'));
    const entries: WalletSignatureInfo[] = [];
    for (const raw of response.value) {
      const item = record(raw);
      if (item === null || typeof item['signature'] !== 'string' || !isSlot(item['slot'])) {
        return failure(new Error('Wallet signature entry is malformed'));
      }
      entries.push({
        signature: item['signature'],
        slot: item['slot'],
        failed: item['err'] !== null,
      });
    }
    const next = entries.length === limit ? entries.at(-1)?.signature : undefined;
    return success({ entries, ...(next === undefined ? {} : { nextBefore: next }) });
  }

  async fetchTransaction(
    signature: string,
    expectedSlot: number,
    commitment: 'confirmed' | 'finalized'
  ): Promise<Result<SolanaWalletTransaction, Error>> {
    const response = await this.rpc('getTransaction', [
      signature,
      { encoding: 'jsonParsed', commitment, maxSupportedTransactionVersion: 0 },
    ]);
    if (response.kind === 'failure') return response;
    const value = record(response.value);
    if (value === null)
      return failure(new Error(`Solana RPC did not return transaction '${signature}'`));
    if (!isSlot(value['slot']) || value['slot'] !== expectedSlot) {
      return failure(new Error(`Solana RPC returned a slot mismatch for '${signature}'`));
    }
    return success({
      signature,
      slot: expectedSlot,
      observedAt: new Date(this.clock()),
      transaction: value,
    });
  }

  private async rpc(method: string, params: readonly unknown[]): Promise<Result<unknown, Error>> {
    await this.rateLimit();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    try {
      const response = await this.fetch(this.endpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: ++this.requestId, method, params }),
        signal: controller.signal,
      });
      if (!response.ok)
        return failure(new Error(`Solana RPC HTTP request failed with status ${response.status}`));
      let decoded: unknown;
      try {
        decoded = await response.json();
      } catch {
        return failure(new Error('Solana RPC returned invalid JSON'));
      }
      const envelope = record(decoded);
      if (envelope === null) return failure(new Error('Solana RPC returned a malformed response'));
      const rpcError = record(envelope['error']);
      if (rpcError !== null) {
        return failure(
          new Error(`Solana RPC error: ${String(rpcError['message'] ?? 'unknown error')}`)
        );
      }
      if (!Object.prototype.hasOwnProperty.call(envelope, 'result')) {
        return failure(new Error('Solana RPC response did not contain a result'));
      }
      return success(envelope['result']);
    } catch (error) {
      return failure(
        controller.signal.aborted
          ? new Error(`Solana RPC request timed out after ${this.requestTimeoutMs}ms`)
          : new Error('Solana RPC request failed', { cause: error })
      );
    } finally {
      clearTimeout(timer);
    }
  }

  private async rateLimit(): Promise<void> {
    const previous = this.rateLimitTail;
    let release = (): void => undefined;
    this.rateLimitTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    const wait = Math.max(0, this.nextRequestAt - this.clock());
    if (wait > 0) await this.sleep(wait);
    this.nextRequestAt = this.clock() + this.minRequestIntervalMs;
    release();
  }
}

function httpEndpoint(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error('Solana wallet RPC endpoint must be an absolute URL');
  }
  if (
    (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
    parsed.username !== '' ||
    parsed.password !== ''
  )
    throw new Error('Solana wallet RPC endpoint must be HTTP(S) without embedded credentials');
  return value;
}

function positive(value: number | undefined, fallback: number, label: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0)
    throw new Error(`${label} must be positive`);
  return resolved;
}

function nonnegative(value: number | undefined, fallback: number, label: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved < 0)
    throw new Error(`${label} must be nonnegative`);
  return resolved;
}

function isSlot(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}
