import { Result, failure, success } from '@nit/domain';
import { WalletIntelligenceRepository } from './repository';
import { WalletIntelligenceService, WalletProcessingSummary } from './service';
import { SolanaWalletRpc } from './solana-rpc';

export interface WalletReconciliationOptions {
  readonly overlapSlots: number;
  readonly initialLookbackSlots: number;
  readonly pageSize: number;
  readonly maxPagesPerWalletPerCycle?: number;
  readonly maxTransactionsPerWalletPerCycle?: number;
  readonly maxPendingTransactionsPerWallet?: number;
}

export interface WalletReconciliationSummary {
  readonly finalizedSlot: number;
  readonly wallets: number;
  readonly signatures: number;
  readonly processed: number;
  readonly swaps: number;
  readonly deferredWallets: number;
}

interface WalletScan {
  cursorAtStart: number | null;
  throughSlot: number;
  lowerBound: number;
  before: string | undefined;
  complete: boolean;
  pending: Map<string, number>;
  work: { signature: string; slot: number }[];
  next: number;
}

/** Finalized address-history scan. Cursors advance only after every fetched transaction succeeds. */
export class SolanaWalletReconciler {
  private readonly scans = new Map<string, WalletScan>();
  private readonly pageBudget: number;
  private readonly transactionBudget: number;
  private readonly pendingLimit: number;
  constructor(
    private readonly rpc: Pick<
      SolanaWalletRpc,
      'getFinalizedSlot' | 'getSignaturesForAddress' | 'fetchTransaction'
    >,
    private readonly repository: Pick<WalletIntelligenceRepository, 'getCursor' | 'advanceCursor'>,
    private readonly service: Pick<WalletIntelligenceService, 'process'>,
    private readonly walletAddresses: readonly string[],
    private readonly options: WalletReconciliationOptions
  ) {
    this.pageBudget = options.maxPagesPerWalletPerCycle ?? 2;
    this.transactionBudget = options.maxTransactionsPerWalletPerCycle ?? 25;
    this.pendingLimit = options.maxPendingTransactionsPerWallet ?? 5000;
    for (const n of [this.pageBudget, this.transactionBudget, this.pendingLimit])
      if (!Number.isSafeInteger(n) || n < 1 || n > 10000)
        throw new Error('Invalid wallet reconciliation work budget');
  }

  async reconcile(): Promise<Result<WalletReconciliationSummary, Error>> {
    const finalized = await this.rpc.getFinalizedSlot();
    if (finalized.kind === 'failure') return finalized;
    const seen = new Set<string>();
    let signatures = 0;
    let processed = 0;
    let swaps = 0;
    let deferredWallets = 0;

    for (const wallet of this.walletAddresses) {
      const cursor = await this.repository.getCursor(wallet);
      if (cursor.kind === 'failure') return cursor;
      if (cursor.value !== null && finalized.value < cursor.value)
        return failure(new Error('Finalized wallet head is behind its stored cursor'));
      let scan = this.scans.get(wallet);
      if (scan !== undefined && cursor.value !== scan.cursorAtStart) {
        this.scans.delete(wallet);
        scan = undefined;
      }
      if (scan === undefined) {
        scan = {
          cursorAtStart: cursor.value,
          throughSlot: finalized.value,
          lowerBound: Math.max(
            0,
            cursor.value === null
              ? finalized.value - this.options.initialLookbackSlots
              : cursor.value - this.options.overlapSlots
          ),
          before: undefined,
          complete: false,
          pending: new Map(),
          work: [],
          next: 0,
        };
        this.scans.set(wallet, scan);
      }
      for (let pages = 0; !scan.complete && pages < this.pageBudget; pages++) {
        const page = await this.rpc.getSignaturesForAddress(
          wallet,
          scan.before,
          this.options.pageSize
        );
        if (page.kind === 'failure') return page;
        signatures += page.value.entries.length;
        for (const item of page.value.entries) {
          if (!item.failed && item.slot >= scan.lowerBound && item.slot <= scan.throughSlot) {
            const previous = scan.pending.get(item.signature);
            if (previous !== undefined && previous !== item.slot)
              return failure(new Error('Wallet history signature slot changed'));
            if (previous === undefined && scan.pending.size >= this.pendingLimit)
              return failure(new Error('Wallet reconciliation pending-work limit exceeded'));
            scan.pending.set(item.signature, item.slot);
          }
        }
        const oldest = page.value.entries.at(-1)?.slot;
        if (
          page.value.nextBefore === undefined ||
          oldest === undefined ||
          oldest < scan.lowerBound
        ) {
          scan.complete = true;
          scan.work = [...scan.pending]
            .map(([signature, slot]) => ({ signature, slot }))
            .sort((a, b) => a.slot - b.slot || a.signature.localeCompare(b.signature));
          break;
        }
        if (page.value.nextBefore === scan.before) {
          return failure(new Error('Wallet reconciliation pagination did not advance'));
        }
        scan.before = page.value.nextBefore;
      }
      if (!scan.complete) {
        deferredWallets++;
        continue;
      }
      let attempted = 0;
      while (scan.next < scan.work.length && attempted < this.transactionBudget) {
        const item = scan.work[scan.next]!;
        attempted++;
        if (seen.has(item.signature)) {
          scan.next++;
          continue;
        }
        const transaction = await this.rpc.fetchTransaction(item.signature, item.slot, 'finalized');
        if (transaction.kind === 'failure') return transaction;
        const outcome = await this.service.process(transaction.value);
        if (outcome.kind === 'failure') return outcome;
        accumulate(outcome.value);
        seen.add(item.signature);
        scan.next++;
      }
      if (scan.next < scan.work.length) {
        deferredWallets++;
        continue;
      }
      const advanced = await this.repository.advanceCursor(wallet, scan.throughSlot);
      if (advanced.kind === 'failure') return advanced;
      this.scans.delete(wallet);
    }

    return success({
      finalizedSlot: finalized.value,
      wallets: this.walletAddresses.length,
      signatures,
      processed,
      swaps,
      deferredWallets,
    });

    function accumulate(summary: WalletProcessingSummary): void {
      processed += 1;
      swaps += summary.swaps;
    }
  }
}
