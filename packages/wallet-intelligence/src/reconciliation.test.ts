import { Result, failure, success } from '@nit/domain';
import { describe, expect, it } from 'vitest';
import { SolanaWalletTransaction } from './contracts';
import { SolanaWalletReconciler } from './reconciliation';
import { WalletProcessingSummary } from './service';
import { WalletSignaturePage } from './solana-rpc';

const wallet = '11111111111111111111111111111111';

describe('SolanaWalletReconciler', () => {
  it('yields a busy wallet between page budgets so other wallet cursors can advance', async () => {
    const other = '22222222222222222222222222222222';
    const advanced: string[] = [];
    const beforeValues: (string | undefined)[] = [];
    const reconciler = new SolanaWalletReconciler(
      {
        getFinalizedSlot: (): Promise<Result<number, Error>> => Promise.resolve(success(120)),
        getSignaturesForAddress: (address, before): Promise<Result<WalletSignaturePage, Error>> => {
          if (address !== wallet) return Promise.resolve(success({ entries: [] }));
          beforeValues.push(before);
          return Promise.resolve(
            success(
              before === undefined
                ? { entries: [{ signature: 'new', slot: 119, failed: false }], nextBefore: 'older' }
                : { entries: [{ signature: 'old', slot: 90, failed: false }] }
            )
          );
        },
        fetchTransaction: (signature, slot): Promise<Result<SolanaWalletTransaction, Error>> =>
          Promise.resolve(success({ signature, slot, observedAt: new Date(), transaction: {} })),
      },
      {
        getCursor: (): Promise<Result<number | null, Error>> => Promise.resolve(success(100)),
        advanceCursor: (address): Promise<Result<void, Error>> => {
          advanced.push(address);
          return Promise.resolve(success(undefined));
        },
      },
      {
        process: (): Promise<Result<WalletProcessingSummary, Error>> =>
          Promise.resolve(success(emptySummary())),
      },
      [wallet, other],
      { overlapSlots: 5, initialLookbackSlots: 20, pageSize: 1, maxPagesPerWalletPerCycle: 1 }
    );
    expect(await reconciler.reconcile()).toMatchObject({
      kind: 'success',
      value: { deferredWallets: 1 },
    });
    expect(advanced).toEqual([other]);
    expect(await reconciler.reconcile()).toMatchObject({
      kind: 'success',
      value: { deferredWallets: 0 },
    });
    expect(beforeValues).toEqual([undefined, 'older']);
    expect(advanced).toContain(wallet);
  });
  it('retains chronological work across processing budgets without advancing early', async () => {
    const processed: string[] = [];
    const advanced: number[] = [];
    let pages = 0;
    const reconciler = new SolanaWalletReconciler(
      {
        getFinalizedSlot: (): Promise<Result<number, Error>> => Promise.resolve(success(120)),
        getSignaturesForAddress: (): Promise<Result<WalletSignaturePage, Error>> => {
          pages++;
          return Promise.resolve(
            success({
              entries: [
                { signature: 'new', slot: 119, failed: false },
                { signature: 'old', slot: 118, failed: false },
              ],
            })
          );
        },
        fetchTransaction: (signature, slot): Promise<Result<SolanaWalletTransaction, Error>> =>
          Promise.resolve(success({ signature, slot, observedAt: new Date(), transaction: {} })),
      },
      {
        getCursor: (): Promise<Result<number | null, Error>> => Promise.resolve(success(120)),
        advanceCursor: (_address, slot): Promise<Result<void, Error>> => {
          advanced.push(slot);
          return Promise.resolve(success(undefined));
        },
      },
      {
        process: (tx): Promise<Result<WalletProcessingSummary, Error>> => {
          processed.push(tx.signature);
          return Promise.resolve(success(emptySummary()));
        },
      },
      [wallet],
      {
        overlapSlots: 5,
        initialLookbackSlots: 20,
        pageSize: 100,
        maxTransactionsPerWalletPerCycle: 1,
      }
    );
    expect(await reconciler.reconcile()).toMatchObject({
      kind: 'success',
      value: { deferredWallets: 1 },
    });
    expect(processed).toEqual(['old']);
    expect(advanced).toEqual([]);
    expect(await reconciler.reconcile()).toMatchObject({
      kind: 'success',
      value: { deferredWallets: 0 },
    });
    expect(processed).toEqual(['old', 'new']);
    expect(pages).toBe(1);
    expect(advanced).toEqual([120]);
  });
  it('does not advance a cursor when transaction repair fails', async () => {
    const advanced: number[] = [];
    const reconciler = new SolanaWalletReconciler(
      {
        getFinalizedSlot: (): Promise<Result<number, Error>> => Promise.resolve(success(120)),
        getSignaturesForAddress: (): Promise<Result<WalletSignaturePage, Error>> =>
          Promise.resolve(success({ entries: [{ signature: 'sig-1', slot: 119, failed: false }] })),
        fetchTransaction: (): Promise<Result<SolanaWalletTransaction, Error>> =>
          Promise.resolve(failure(new Error('rpc unavailable'))),
      },
      {
        getCursor: (): Promise<Result<number | null, Error>> => Promise.resolve(success(100)),
        advanceCursor: (_address, slot): Promise<Result<void, Error>> => {
          advanced.push(slot);
          return Promise.resolve(success(undefined));
        },
      },
      {
        process: (): Promise<Result<WalletProcessingSummary, Error>> =>
          Promise.resolve(success(emptySummary())),
      },
      [wallet],
      { overlapSlots: 5, initialLookbackSlots: 20, pageSize: 100 }
    );

    const outcome = await reconciler.reconcile();
    expect(outcome).toMatchObject({ kind: 'failure', error: { message: 'rpc unavailable' } });
    expect(advanced).toEqual([]);
  });

  it('deduplicates one signature seen through multiple watched wallets', async () => {
    let fetches = 0;
    let processes = 0;
    const advanced: string[] = [];
    const transaction: SolanaWalletTransaction = {
      signature: 'sig-shared',
      slot: 119,
      observedAt: new Date('2026-01-01T00:00:00Z'),
      transaction: {},
    };
    const reconciler = new SolanaWalletReconciler(
      {
        getFinalizedSlot: (): Promise<Result<number, Error>> => Promise.resolve(success(120)),
        getSignaturesForAddress: (): Promise<Result<WalletSignaturePage, Error>> =>
          Promise.resolve(
            success({ entries: [{ signature: 'sig-shared', slot: 119, failed: false }] })
          ),
        fetchTransaction: (): Promise<Result<SolanaWalletTransaction, Error>> => {
          fetches += 1;
          return Promise.resolve(success(transaction));
        },
      },
      {
        getCursor: (): Promise<Result<number | null, Error>> => Promise.resolve(success(100)),
        advanceCursor: (address): Promise<Result<void, Error>> => {
          advanced.push(address);
          return Promise.resolve(success(undefined));
        },
      },
      {
        process: (): Promise<Result<WalletProcessingSummary, Error>> => {
          processes += 1;
          return Promise.resolve(success(emptySummary()));
        },
      },
      [wallet, '22222222222222222222222222222222'],
      { overlapSlots: 5, initialLookbackSlots: 20, pageSize: 100 }
    );

    expect(await reconciler.reconcile()).toMatchObject({
      kind: 'success',
      value: { wallets: 2, processed: 1 },
    });
    expect({ fetches, processes, advanced }).toEqual({
      fetches: 1,
      processes: 1,
      advanced: [wallet, '22222222222222222222222222222222'],
    });
  });
});

function emptySummary(): WalletProcessingSummary {
  return {
    signature: 'sig-shared',
    swaps: 0,
    fundingEvents: 0,
    relations: 0,
    signals: 0,
    rejectedWallets: 0,
  };
}
