import { PostgresSignalStore } from '@nit/database-public';
import { Chain, Result, generateEvidenceId, generateSignalId, success } from '@nit/domain';
import { SignalEnvelope, SignalKind } from '@nit/signals';
import {
  SolanaWalletTransaction,
  WalletConvergenceSignalPayload,
  WalletRelationEvidence,
  WalletRiskSignalPayload,
  WalletSwap,
  WalletSwapSignalPayload,
} from './contracts';
import { SolanaWalletSwapParser } from './parser';
import { WalletIntelligenceRepository } from './repository';

export interface WalletConvergencePolicy {
  readonly windowMs: number;
  readonly minimumIndependentWallets: number;
}

export interface WalletProcessingSummary {
  readonly signature: string;
  readonly swaps: number;
  readonly fundingEvents: number;
  readonly relations: number;
  readonly signals: number;
  readonly rejectedWallets: number;
}

/** Orchestrates idempotent evidence persistence and canonical signal publication. */
export class WalletIntelligenceService {
  private readonly sourceVersion: string;

  constructor(
    private readonly parser: SolanaWalletSwapParser,
    private readonly repository: WalletIntelligenceRepository,
    private readonly signals: PostgresSignalStore,
    private readonly policy: WalletConvergencePolicy
  ) {
    if (!Number.isSafeInteger(policy.windowMs) || policy.windowMs <= 0) {
      throw new Error('Wallet convergence window must be a positive integer');
    }
    if (
      !Number.isSafeInteger(policy.minimumIndependentWallets) ||
      policy.minimumIndependentWallets < 2
    ) {
      throw new Error('Wallet convergence requires at least two independent wallets');
    }
    this.sourceVersion = parser.version;
  }

  async process(
    transaction: SolanaWalletTransaction
  ): Promise<Result<WalletProcessingSummary, Error>> {
    const parsed = this.parser.parse(transaction);
    if (parsed.kind === 'failure') return parsed;
    let relations = 0;
    let signalCount = 0;

    for (const event of parsed.value.fundingEvents) {
      const recorded = await this.repository.recordFundingEvent(event);
      if (recorded.kind === 'failure') return recorded;
      const funded = await this.repository.listWalletsFundedBy(
        event.funderAddress,
        event.occurredAt
      );
      if (funded.kind === 'failure') return funded;
      for (const other of funded.value) {
        if (other === event.fundedWalletAddress) continue;
        const evidence = relation(
          event.fundedWalletAddress,
          other,
          'shared_funder',
          `funder:${event.funderAddress}`,
          event.occurredAt,
          event.observedAt,
          { funderAddress: event.funderAddress }
        );
        const stored = await this.repository.recordRelation(evidence);
        if (stored.kind === 'failure') return stored;
        if (stored.value.inserted) {
          relations += 1;
          const published = await this.publishRisk(stored.value.value);
          if (published.kind === 'failure') return published;
          if (published.value) signalCount += 1;
        }
      }
    }

    for (const candidate of parsed.value.swaps) {
      const recorded = await this.repository.recordSwap(candidate);
      if (recorded.kind === 'failure') return recorded;
      const swap = recorded.value.value;
      const published = await this.publishSwap(swap);
      if (published.kind === 'failure') return published;
      if (published.value) signalCount += 1;

      const sameSlot = await this.repository.listSameSlotSwaps(
        swap.tokenMint,
        swap.side,
        swap.slot
      );
      if (sameSlot.kind === 'failure') return sameSlot;
      for (const other of sameSlot.value) {
        if (other.walletAddress === swap.walletAddress) continue;
        const kind = swap.side === 'buy' ? 'same_slot_entry' : 'same_slot_exit';
        const evidence = relation(
          swap.walletAddress,
          other.walletAddress,
          kind,
          `slot:${swap.slot}:mint:${swap.tokenMint}`,
          swap.occurredAt,
          swap.observedAt,
          { tokenMint: swap.tokenMint, slot: swap.slot, side: swap.side }
        );
        const stored = await this.repository.recordRelation(evidence);
        if (stored.kind === 'failure') return stored;
        if (stored.value.inserted) {
          relations += 1;
          const risk = await this.publishRisk(stored.value.value);
          if (risk.kind === 'failure') return risk;
          if (risk.value) signalCount += 1;
        }
      }

      const convergence = await this.publishConvergence(swap);
      if (convergence.kind === 'failure') return convergence;
      if (convergence.value) signalCount += 1;
    }

    return success({
      signature: transaction.signature,
      swaps: parsed.value.swaps.length,
      fundingEvents: parsed.value.fundingEvents.length,
      relations,
      signals: signalCount,
      rejectedWallets: parsed.value.rejectedWallets.length,
    });
  }

  private async publishSwap(swap: WalletSwap): Promise<Result<boolean, Error>> {
    const payload: WalletSwapSignalPayload = {
      walletAddress: swap.walletAddress,
      side: swap.side,
      tokenRawAmount: swap.tokenRawAmount,
      tokenDecimals: swap.tokenDecimals,
      quoteAsset: swap.quoteAsset,
      quoteRawAmount: swap.quoteRawAmount,
      quoteDecimals: swap.quoteDecimals,
      slot: swap.slot,
      programAddresses: swap.programAddresses,
      attributionMethod: 'owner_balance_delta',
    };
    const signal: SignalEnvelope<WalletSwapSignalPayload> = {
      eventId: generateSignalId(),
      source: 'wallet_tracker',
      sourceVersion: this.sourceVersion,
      kind: swap.side === 'buy' ? SignalKind.WALLET_BUY : SignalKind.WALLET_SELL,
      chain: Chain.SOLANA,
      tokenMint: swap.tokenMint,
      occurredAt: swap.occurredAt,
      observedAt: swap.observedAt,
      availableAt: swap.availableAt,
      origin: {
        kind: 'chain',
        chain: Chain.SOLANA,
        transactionSignature: swap.transactionSignature,
        transactionLevel: true,
        actorAddress: swap.walletAddress,
      },
      evidenceIds: [swap.walletSwapId],
      payload,
    };
    const written = await this.signals.append(signal);
    return written.kind === 'failure' ? written : success(written.value.inserted);
  }

  private async publishRisk(evidence: WalletRelationEvidence): Promise<Result<boolean, Error>> {
    const payload: WalletRiskSignalPayload = {
      walletAddresses: [evidence.walletA, evidence.walletB],
      relationKind: evidence.kind,
      sourceReference: evidence.sourceReference,
      details: evidence.details,
    };
    const written = await this.signals.append<WalletRiskSignalPayload>({
      eventId: generateSignalId(),
      source: 'wallet_tracker',
      sourceVersion: this.sourceVersion,
      kind: SignalKind.WALLET_RISK,
      chain: Chain.SOLANA,
      occurredAt: evidence.occurredAt,
      observedAt: evidence.observedAt,
      availableAt: evidence.observedAt,
      origin: {
        kind: 'provider',
        provider: 'wallet_tracker',
        referenceId: `risk:${evidence.evidenceId}`,
      },
      evidenceIds: [evidence.evidenceId],
      payload,
    });
    return written.kind === 'failure' ? written : success(written.value.inserted);
  }

  private async publishConvergence(swap: WalletSwap): Promise<Result<boolean, Error>> {
    const from = new Date(swap.occurredAt.getTime() - this.policy.windowMs);
    const recent = await this.repository.listSwaps(
      swap.tokenMint,
      swap.side,
      from,
      swap.occurredAt
    );
    if (recent.kind === 'failure') return recent;
    const wallets = [...new Set(recent.value.map((item) => item.walletAddress))].sort();
    if (wallets.length < this.policy.minimumIndependentWallets) return success(false);
    const relations = await this.repository.listRelations(wallets, from, swap.occurredAt);
    if (relations.kind === 'failure') return relations;
    const groups = connectedGroups(wallets, relations.value);
    if (groups.length < this.policy.minimumIndependentWallets) return success(false);
    const relatedGroups = groups.filter((group) => group.length > 1);
    const relationEvidenceIds = relations.value.map((item) => item.evidenceId);
    const payload: WalletConvergenceSignalPayload = {
      side: swap.side,
      windowStart: from,
      windowEnd: swap.occurredAt,
      walletAddresses: wallets,
      independentSourceCount: groups.length,
      relatedGroups,
      relationEvidenceIds,
    };
    const referenceId = [
      'convergence',
      swap.side,
      swap.tokenMint,
      swap.transactionSignature,
      ...wallets,
    ].join(':');
    const written = await this.signals.append<WalletConvergenceSignalPayload>({
      eventId: generateSignalId(),
      source: 'wallet_tracker',
      sourceVersion: this.sourceVersion,
      kind: SignalKind.WALLET_CONVERGENCE,
      chain: Chain.SOLANA,
      tokenMint: swap.tokenMint,
      occurredAt: swap.occurredAt,
      observedAt: swap.observedAt,
      availableAt: swap.availableAt,
      origin: { kind: 'provider', provider: 'wallet_tracker', referenceId },
      evidenceIds: [...recent.value.map((item) => item.walletSwapId), ...relationEvidenceIds],
      payload,
    });
    return written.kind === 'failure' ? written : success(written.value.inserted);
  }
}

function relation(
  left: string,
  right: string,
  kind: WalletRelationEvidence['kind'],
  sourceReference: string,
  occurredAt: Date,
  observedAt: Date,
  details: Readonly<Record<string, unknown>>
): WalletRelationEvidence {
  const [walletA, walletB] = [left, right].sort() as [string, string];
  return {
    evidenceId: generateEvidenceId(),
    walletA,
    walletB,
    kind,
    sourceReference,
    occurredAt,
    observedAt,
    details,
  };
}

function connectedGroups(
  wallets: readonly string[],
  relations: readonly WalletRelationEvidence[]
): string[][] {
  const parent = new Map(wallets.map((wallet) => [wallet, wallet]));
  const find = (wallet: string): string => {
    const next = parent.get(wallet) ?? wallet;
    if (next === wallet) return wallet;
    const root = find(next);
    parent.set(wallet, root);
    return root;
  };
  for (const item of relations) {
    const left = find(item.walletA);
    const right = find(item.walletB);
    if (left !== right) parent.set(right, left);
  }
  const grouped = new Map<string, string[]>();
  for (const wallet of wallets) {
    const root = find(wallet);
    const group = grouped.get(root) ?? [];
    group.push(wallet);
    grouped.set(root, group);
  }
  return [...grouped.values()]
    .map((group) => group.sort())
    .sort((a, b) => a[0]!.localeCompare(b[0]!));
}
