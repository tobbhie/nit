import { describe, expect, it } from 'vitest';
import { LaunchSource, Result, success } from '@nit/domain';
import { DecodedMarketOccurrence, ReconciledLaunch } from './reconciliation';
import {
  SolanaLaunchTransaction,
  SolanaLaunchTransactionDecoder,
  SolanaRpcFetch,
  SolanaRpcReconciliationSource,
  CreationHistoryProgress,
} from './solana-rpc';

interface RpcCall {
  method: string;
  params: unknown[];
}

function rpcHarness(...results: unknown[]): { fetch: SolanaRpcFetch; calls: RpcCall[] } {
  const calls: RpcCall[] = [];
  const fetch: SolanaRpcFetch = (_input, init) => {
    const request = JSON.parse(init.body) as RpcCall;
    calls.push({ method: request.method, params: request.params });
    const result = results.shift();
    return Promise.resolve({
      ok: true,
      status: 200,
      json: (): Promise<unknown> => Promise.resolve({ jsonrpc: '2.0', id: calls.length, result }),
    });
  };
  return { fetch, calls };
}

function decodedLaunch(transaction: SolanaLaunchTransaction): ReconciledLaunch {
  return {
    address: 'Mint111111111111111111111111111111111111111',
    ticker: 'MINT',
    name: 'Mint Token',
    launchTime: new Date('2026-11-01T00:00:00.000Z'),
    raw: transaction.transaction,
    market: {
      marketAddress: 'Market111111111111111111111111111111111111',
      origin: {
        kind: 'chain',
        slot: transaction.slot,
        position: {
          transactionSignature: transaction.signature,
          instructionIndex: 2,
        },
      },
    },
  };
}

class RecordingDecoder implements SolanaLaunchTransactionDecoder {
  readonly calls: SolanaLaunchTransaction[] = [];

  decode(transaction: SolanaLaunchTransaction): Result<readonly DecodedMarketOccurrence[], Error> {
    this.calls.push(transaction);
    return success([{ kind: 'launch', launch: decodedLaunch(transaction) }]);
  }
}

function source(
  fetch: SolanaRpcFetch,
  decoder: SolanaLaunchTransactionDecoder = new RecordingDecoder()
): SolanaRpcReconciliationSource {
  return new SolanaRpcReconciliationSource({
    endpoint: 'https://rpc.example.test/?api-key=not-logged',
    programAddress: 'Program11111111111111111111111111111111111',
    launchSource: LaunchSource.PUMP_FUN,
    decoder,
    fetch,
    minRequestIntervalMs: 0,
  });
}

describe('SolanaRpcReconciliationSource', () => {
  it('retries an interrupted JSON response body with the same bounded policy', async () => {
    let calls = 0;
    const reader = new SolanaRpcReconciliationSource({
      endpoint: 'https://rpc.example.test',
      programAddress: 'program',
      launchSource: LaunchSource.PUMP_FUN,
      decoder: new RecordingDecoder(),
      minRequestIntervalMs: 0,
      sleep: (): Promise<void> => Promise.resolve(),
      fetch: (): ReturnType<SolanaRpcFetch> => {
        calls++;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: (): Promise<unknown> =>
            calls === 1
              ? Promise.reject(new Error('untrusted response body'))
              : Promise.resolve({ result: 42 }),
        });
      },
    });
    expect(await reader.getFinalizedSlot()).toEqual(success(42));
    expect(calls).toBe(2);
    expect(reader.getRetryCount()).toBe(1);
  });
  it('resumes a persisted creation search instead of starting again at the newest page', async () => {
    let progress: CreationHistoryProgress | null = null;
    const store = {
      get: (): Promise<Result<CreationHistoryProgress | null, Error>> =>
        Promise.resolve(success(progress)),
      put: (_mint: string, value: CreationHistoryProgress): Promise<Result<void, Error>> => {
        progress = value;
        return Promise.resolve(success(undefined));
      },
    };
    const entries = Array.from({ length: 1000 }, (_, i) => ({
      signature: 'sig-' + i,
      slot: 2000 - i,
      err: null,
    }));
    let calls = 0;
    const first = new SolanaRpcReconciliationSource({
      endpoint: 'https://rpc.example.test',
      programAddress: 'program',
      launchSource: LaunchSource.PUMP_FUN,
      decoder: new RecordingDecoder(),
      minRequestIntervalMs: 0,
      creationHistoryProgress: store,
      fetch: (): ReturnType<SolanaRpcFetch> => {
        calls++;
        return Promise.resolve({
          ok: calls === 1,
          status: calls === 1 ? 200 : 401,
          json: (): Promise<unknown> => Promise.resolve({ result: entries }),
        });
      },
    });
    const mint = 'Mint111111111111111111111111111111111111111';
    expect((await first.findCreation(mint, 3000)).kind).toBe('failure');
    const rpc = rpcHarness([], { slot: 1001 });
    const restarted = new SolanaRpcReconciliationSource({
      endpoint: 'https://rpc.example.test',
      programAddress: 'program',
      launchSource: LaunchSource.PUMP_FUN,
      decoder: new RecordingDecoder(),
      minRequestIntervalMs: 0,
      creationHistoryProgress: store,
      fetch: rpc.fetch,
    });
    expect((await restarted.findCreation(mint, 3000)).kind).toBe('success');
    expect(rpc.calls[0]?.params[1]).toMatchObject({ before: 'sig-999' });
    expect(rpc.calls).toHaveLength(2);
  });
  it('retries transport failures without exposing provider text', async () => {
    let calls = 0;
    const sleeps: number[] = [];
    const reader = new SolanaRpcReconciliationSource({
      endpoint: 'https://rpc.example.test',
      programAddress: 'program',
      launchSource: LaunchSource.PUMP_FUN,
      decoder: new RecordingDecoder(),
      minRequestIntervalMs: 0,
      sleep: (ms): Promise<void> => {
        sleeps.push(ms);
        return Promise.resolve();
      },
      fetch: (): ReturnType<SolanaRpcFetch> => {
        if (++calls < 3) throw new Error('https://secret.invalid/?api-key=private');
        return Promise.resolve({
          ok: true,
          status: 200,
          json: (): Promise<unknown> => Promise.resolve({ result: 42 }),
        });
      },
    });
    expect(await reader.getFinalizedSlot()).toEqual(success(42));
    expect(calls).toBe(3);
    expect(sleeps).toEqual([250, 500]);
    expect(reader.getRetryCount()).toBe(2);
  });
  it.each([401, 403, 400])('does not retry permanent HTTP %s failures', async (status) => {
    let calls = 0;
    const fetch: SolanaRpcFetch = () => {
      calls++;
      return Promise.resolve({
        ok: false,
        status,
        json: (): Promise<unknown> => Promise.resolve({}),
      });
    };
    expect((await source(fetch).getFinalizedSlot()).kind).toBe('failure');
    expect(calls).toBe(1);
  });
  it('bounds retries for service overload and never forwards RPC error text', async () => {
    let calls = 0;
    const reader = new SolanaRpcReconciliationSource({
      endpoint: 'https://rpc.example.test',
      programAddress: 'program',
      launchSource: LaunchSource.PUMP_FUN,
      decoder: new RecordingDecoder(),
      minRequestIntervalMs: 0,
      sleep: (): Promise<void> => Promise.resolve(),
      fetch: (): ReturnType<SolanaRpcFetch> => {
        calls++;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: (): Promise<unknown> =>
            Promise.resolve({ error: { code: -32005, message: 'private-provider-details' } }),
        });
      },
    });
    const result = await reader.getFinalizedSlot();
    expect(result.kind).toBe('failure');
    expect(calls).toBe(3);
    if (result.kind === 'failure') expect(result.error.message).toBe('Solana RPC error -32005');
  });
  it('retries temporarily unindexed transactions but rejects slot mismatches immediately', async () => {
    const rpc = rpcHarness(null, null, { slot: 42 });
    const reader = source(rpc.fetch);
    expect(await reader.readConfirmedTransaction('sig', 42)).toEqual(success({ slot: 42 }));
    expect(reader.getRetryCount()).toBe(2);
    const wrong = rpcHarness({ slot: 43 });
    expect((await source(wrong.fetch).readConfirmedTransaction('sig', 42)).kind).toBe('failure');
    expect(wrong.calls).toHaveLength(1);
  });
  it('recovers the matching original creation from oldest successful mint history', async () => {
    const mint = 'Mint111111111111111111111111111111111111111';
    const rpc = rpcHarness(
      [
        { signature: 'new', slot: 100, err: null },
        { signature: 'creation', slot: 5, err: null },
        { signature: 'prefund', slot: 1, err: null },
      ],
      { slot: 1 },
      { slot: 5 }
    );
    const decoder: SolanaLaunchTransactionDecoder = {
      decode: (tx): Result<readonly DecodedMarketOccurrence[], Error> =>
        success(tx.signature === 'creation' ? [{ kind: 'launch', launch: decodedLaunch(tx) }] : []),
    };
    const result = await source(rpc.fetch, decoder).findCreation(mint, 50);
    expect(result.kind).toBe('success');
    if (result.kind === 'success')
      expect(
        result.value.kind === 'launch' &&
          result.value.launch.market.origin.position.transactionSignature
      ).toBe('creation');
    expect(rpc.calls.map((c) => c.method)).toEqual([
      'getSignaturesForAddress',
      'getTransaction',
      'getTransaction',
    ]);
    expect(rpc.calls[0]?.params[0]).toBe(mint);
  });
  it('refuses to certify incomplete or looping creation history', async () => {
    const entries = Array.from({ length: 1000 }, (_, i) => ({
      signature: 'sig-' + i,
      slot: 2000 - i,
      err: null,
    }));
    const rpc = rpcHarness(entries, entries);
    const result = await source(rpc.fetch).findCreation(
      'Mint111111111111111111111111111111111111111',
      3000
    );
    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') expect(result.error.message).toContain('cursor did not advance');
    expect(rpc.calls).toHaveLength(2);
  });
  it('reads the durable head at finalized commitment', async () => {
    const rpc = rpcHarness(42_001);
    const result = await source(rpc.fetch).getFinalizedSlot();

    expect(result).toEqual(success(42_001));
    expect(rpc.calls).toEqual([{ method: 'getSlot', params: [{ commitment: 'finalized' }] }]);
  });

  it('walks confirmed signatures, filters to the finalized bounds, and decodes successful transactions', async () => {
    const rpc = rpcHarness(
      [
        { signature: 'above-head', slot: 116, err: null },
        { signature: 'in-range', slot: 110, err: null },
        { signature: 'failed', slot: 105, err: { InstructionError: [0, 'Custom'] } },
        { signature: 'below-floor', slot: 99, err: null },
      ],
      { slot: 110, blockTime: null, meta: {}, transaction: {} }
    );
    const decoder = new RecordingDecoder();
    const result = await source(rpc.fetch, decoder).fetchReconciliationPage({
      fromSlotInclusive: 100,
      throughFinalizedSlot: 115,
      beforeSignature: 'newer-page',
      limit: 4,
    });

    expect(result.kind).toBe('success');
    if (result.kind === 'failure') throw result.error;
    expect(result.value.occurrences).toHaveLength(1);
    expect(result.value.occurrences[0]?.observedAt).toBeInstanceOf(Date);
    expect(result.value.reachedFloor).toBe(true);
    expect(result.value.nextBeforeSignature).toBeUndefined();
    expect(decoder.calls.map((call) => call.signature)).toEqual(['in-range']);
    expect(rpc.calls).toEqual([
      {
        method: 'getSignaturesForAddress',
        params: [
          'Program11111111111111111111111111111111111',
          { commitment: 'confirmed', limit: 4, before: 'newer-page' },
        ],
      },
      {
        method: 'getTransaction',
        params: [
          'in-range',
          {
            commitment: 'confirmed',
            encoding: 'jsonParsed',
            maxSupportedTransactionVersion: 1,
          },
        ],
      },
    ]);
  });

  it('returns the oldest signature as the next page cursor while the floor is not reached', async () => {
    const rpc = rpcHarness([
      { signature: 'sig-2', slot: 102, err: { failed: true } },
      { signature: 'sig-1', slot: 101, err: { failed: true } },
    ]);
    const result = await source(rpc.fetch).fetchReconciliationPage({
      fromSlotInclusive: 100,
      throughFinalizedSlot: 110,
      limit: 2,
    });

    expect(result).toEqual(
      success({ occurrences: [], reachedFloor: false, nextBeforeSignature: 'sig-1' })
    );
  });

  it('fails closed when an in-range transaction is unavailable', async () => {
    const rpc = rpcHarness([{ signature: 'missing', slot: 101, err: null }], null, null, null);
    const result = await source(rpc.fetch).fetchReconciliationPage({
      fromSlotInclusive: 100,
      throughFinalizedSlot: 110,
      limit: 10,
    });

    expect(result.kind).toBe('failure');
    if (result.kind === 'success') throw new Error('Expected unavailable transaction to fail');
    expect(result.error.message).toContain("did not return transaction 'missing'");
    expect(result.error.message).not.toContain('api-key');
  });

  it('serializes calls through the default five-per-second limiter', async () => {
    const rpc = rpcHarness(1, 2);
    let time = 1_000;
    const sleeps: number[] = [];
    const limited = new SolanaRpcReconciliationSource({
      endpoint: 'https://rpc.example.test',
      programAddress: 'Program11111111111111111111111111111111111',
      launchSource: LaunchSource.PUMP_FUN,
      decoder: new RecordingDecoder(),
      fetch: rpc.fetch,
      clock: (): number => time,
      sleep: (milliseconds): Promise<void> => {
        sleeps.push(milliseconds);
        time += milliseconds;
        return Promise.resolve();
      },
    });

    await limited.getFinalizedSlot();
    await limited.getFinalizedSlot();

    expect(sleeps).toEqual([200]);
  });
});
