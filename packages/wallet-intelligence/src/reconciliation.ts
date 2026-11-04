import { Result, failure, success } from '@nit/domain';
import { WalletIntelligenceRepository } from './repository';
import { WalletIntelligenceService, WalletProcessingSummary } from './service';
import { SolanaWalletRpc } from './solana-rpc';

export interface WalletReconciliationOptions {
  readonly overlapSlots: number;
  readonly initialLookbackSlots: number;
  readonly pageSize: number;
}

export interface WalletReconciliationSummary {
  readonly finalizedSlot: number;
  readonly wallets: number;
  readonly signatures: number;
  readonly processed: number;
  readonly swaps: number;
}

/** Finalized address-history scan. Cursors advance only after every fetched transaction succeeds. */
export class SolanaWalletReconciler {
  constructor(
    private readonly rpc: Pick<
      SolanaWalletRpc,
      'getFinalizedSlot' | 'getSignaturesForAddress' | 'fetchTransaction'
    >,
    private readonly repository: Pick<WalletIntelligenceRepository, 'getCursor' | 'advanceCursor'>,
    private readonly service: Pick<WalletIntelligenceService, 'process'>,
    private readonly walletAddresses: readonly string[],
    private readonly options: WalletReconciliationOptions
  ) {}

  async reconcile(): Promise<Result<WalletReconciliationSummary, Error>> {
    const finalized = await this.rpc.getFinalizedSlot();
    if (finalized.kind === 'failure') return finalized;
    const seen = new Set<string>();
    let signatures = 0;
    let processed = 0;
    let swaps = 0;

    for (const wallet of this.walletAddresses) {
      const cursor = await this.repository.getCursor(wallet);
      if (cursor.kind === 'failure') return cursor;
      const lowerBound = Math.max(
        0,
        cursor.value === null
          ? finalized.value - this.options.initialLookbackSlots
          : cursor.value - this.options.overlapSlots
      );
      const pending: { signature: string; slot: number }[] = [];
      let before: string | undefined | null;
      while (before !== null) {
        const page = await this.rpc.getSignaturesForAddress(wallet, before, this.options.pageSize);
        if (page.kind === 'failure') return page;
        signatures += page.value.entries.length;
        for (const item of page.value.entries) {
          if (!item.failed && item.slot >= lowerBound && item.slot <= finalized.value) {
            pending.push({ signature: item.signature, slot: item.slot });
          }
        }
        const oldest = page.value.entries.at(-1)?.slot;
        if (page.value.nextBefore === undefined || oldest === undefined || oldest < lowerBound) {
          before = null;
          continue;
        }
        if (page.value.nextBefore === before) {
          return failure(new Error('Wallet reconciliation pagination did not advance'));
        }
        before = page.value.nextBefore;
      }

      pending.sort((a, b) => a.slot - b.slot || a.signature.localeCompare(b.signature));
      for (const item of pending) {
        if (seen.has(item.signature)) continue;
        const transaction = await this.rpc.fetchTransaction(item.signature, item.slot, 'finalized');
        if (transaction.kind === 'failure') return transaction;
        const outcome = await this.service.process(transaction.value);
        if (outcome.kind === 'failure') return outcome;
        accumulate(outcome.value);
        seen.add(item.signature);
      }
      const advanced = await this.repository.advanceCursor(wallet, finalized.value);
      if (advanced.kind === 'failure') return advanced;
    }

    return success({
      finalizedSlot: finalized.value,
      wallets: this.walletAddresses.length,
      signatures,
      processed,
      swaps,
    });

    function accumulate(summary: WalletProcessingSummary): void {
      processed += 1;
      swaps += summary.swaps;
    }
  }
}
