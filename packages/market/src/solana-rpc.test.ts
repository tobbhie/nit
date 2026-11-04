import { describe, expect, it } from 'vitest';
import { LaunchSource, Result, success } from '@nit/domain';
import { DecodedMarketOccurrence, ReconciledLaunch } from './reconciliation';
import {
  SolanaLaunchTransaction,
  SolanaLaunchTransactionDecoder,
  SolanaRpcFetch,
  SolanaRpcReconciliationSource,
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
    const rpc = rpcHarness([{ signature: 'missing', slot: 101, err: null }], null);
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
