import { describe, expect, it } from 'vitest';
import { success } from '@nit/domain';
import { SolanaWalletRpc, WalletRpcFetch } from './solana-rpc';

describe('wallet RPC compatibility and recovery', () => {
  it('requests version-1 parsed transactions and preserves the attribution evidence', async () => {
    let request: Record<string, unknown> | undefined;
    const transaction = {
      slot: 42,
      version: 1,
      blockTime: 1000,
      transaction: { message: { accountKeys: [] } },
      meta: { err: null, preTokenBalances: [], postTokenBalances: [] },
    };
    const rpc = new SolanaWalletRpc({
      endpoint: 'https://rpc.invalid',
      minRequestIntervalMs: 0,
      fetch: (_url, init): ReturnType<WalletRpcFetch> => {
        request = JSON.parse(init.body) as Record<string, unknown>;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: (): Promise<unknown> => Promise.resolve({ result: transaction }),
        });
      },
    });
    const result = await rpc.fetchTransaction('sig', 42, 'confirmed');
    expect(request?.['params']).toEqual([
      'sig',
      { encoding: 'jsonParsed', commitment: 'confirmed', maxSupportedTransactionVersion: 1 },
    ]);
    expect(result.kind).toBe('success');
    if (result.kind === 'success') expect(result.value.transaction).toEqual(transaction);
  });
  it('retries transient transport and response-body failures with bounded backoff', async () => {
    let attempts = 0;
    const sleeps: number[] = [];
    const rpc = new SolanaWalletRpc({
      endpoint: 'https://rpc.invalid',
      minRequestIntervalMs: 0,
      sleep: (ms): Promise<void> => {
        sleeps.push(ms);
        return Promise.resolve();
      },
      fetch: (): ReturnType<WalletRpcFetch> => {
        attempts++;
        if (attempts === 1) return Promise.reject(new Error('untrusted private endpoint details'));
        return Promise.resolve({
          ok: true,
          status: 200,
          json: (): Promise<unknown> =>
            attempts === 2
              ? Promise.reject(new Error('truncated response'))
              : Promise.resolve({ result: 42 }),
        });
      },
    });
    expect(await rpc.getFinalizedSlot()).toEqual(success(42));
    expect(attempts).toBe(3);
    expect(sleeps).toEqual([250, 500]);
  });
  it('does not retry permanent authorization failures', async () => {
    let attempts = 0;
    const rpc = new SolanaWalletRpc({
      endpoint: 'https://rpc.invalid',
      minRequestIntervalMs: 0,
      fetch: (): ReturnType<WalletRpcFetch> => {
        attempts++;
        return Promise.resolve({
          ok: false,
          status: 401,
          json: (): Promise<unknown> => Promise.resolve({}),
        });
      },
    });
    expect((await rpc.getFinalizedSlot()).kind).toBe('failure');
    expect(attempts).toBe(1);
  });
  it('retries null indexing results but rejects mismatched slots', async () => {
    let attempts = 0;
    const rpc = new SolanaWalletRpc({
      endpoint: 'https://rpc.invalid',
      minRequestIntervalMs: 0,
      sleep: (): Promise<void> => Promise.resolve(),
      fetch: (): ReturnType<WalletRpcFetch> =>
        Promise.resolve({
          ok: true,
          status: 200,
          json: (): Promise<unknown> =>
            Promise.resolve({ result: ++attempts < 3 ? null : { slot: 43 } }),
        }),
    });
    expect((await rpc.fetchTransaction('sig', 42, 'confirmed')).kind).toBe('failure');
    expect(attempts).toBe(3);
  });
});
