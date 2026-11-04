import { Chain, EvidenceId } from '@nit/domain';

export type WalletSwapSide = 'buy' | 'sell';

export interface SolanaWalletTransaction {
  readonly signature: string;
  readonly slot: number;
  readonly observedAt: Date;
  readonly transaction: Record<string, unknown>;
}

export interface WalletSwap {
  readonly walletSwapId: EvidenceId;
  readonly chain: Chain.SOLANA;
  readonly transactionSignature: string;
  readonly slot: number;
  readonly walletAddress: string;
  readonly side: WalletSwapSide;
  readonly tokenMint: string;
  readonly tokenRawAmount: string;
  readonly tokenDecimals: number;
  readonly quoteAsset: string;
  readonly quoteRawAmount: string;
  readonly quoteDecimals: number;
  readonly programAddresses: readonly string[];
  readonly attribution: Readonly<Record<string, unknown>>;
  readonly occurredAt: Date;
  readonly observedAt: Date;
  readonly availableAt: Date;
}

export interface WalletFundingEvent {
  readonly walletFundingEventId: EvidenceId;
  readonly chain: Chain.SOLANA;
  readonly transactionSignature: string;
  readonly instructionPath: string;
  readonly slot: number;
  readonly funderAddress: string;
  readonly fundedWalletAddress: string;
  readonly lamports: string;
  readonly occurredAt: Date;
  readonly observedAt: Date;
}

export type WalletRelationKind = 'shared_funder' | 'same_slot_entry' | 'same_slot_exit';

export interface WalletRelationEvidence {
  readonly evidenceId: EvidenceId;
  readonly walletA: string;
  readonly walletB: string;
  readonly kind: WalletRelationKind;
  readonly sourceReference: string;
  readonly occurredAt: Date;
  readonly observedAt: Date;
  readonly details: Readonly<Record<string, unknown>>;
}

export interface ParsedWalletTransaction {
  readonly swaps: readonly WalletSwap[];
  readonly fundingEvents: readonly WalletFundingEvent[];
  readonly rejectedWallets: readonly Readonly<{
    walletAddress: string;
    reason: 'not_signer' | 'no_swap_program' | 'ambiguous_balance_deltas' | 'no_balance_change';
  }>[];
}

export interface WalletSwapSignalPayload {
  readonly walletAddress: string;
  readonly side: WalletSwapSide;
  readonly tokenRawAmount: string;
  readonly tokenDecimals: number;
  readonly quoteAsset: string;
  readonly quoteRawAmount: string;
  readonly quoteDecimals: number;
  readonly slot: number;
  readonly programAddresses: readonly string[];
  readonly attributionMethod: 'owner_balance_delta';
}

export interface WalletConvergenceSignalPayload {
  readonly side: WalletSwapSide;
  readonly windowStart: Date;
  readonly windowEnd: Date;
  readonly walletAddresses: readonly string[];
  readonly independentSourceCount: number;
  readonly relatedGroups: readonly (readonly string[])[];
  readonly relationEvidenceIds: readonly EvidenceId[];
}

export interface WalletRiskSignalPayload {
  readonly walletAddresses: readonly [string, string];
  readonly relationKind: WalletRelationKind;
  readonly sourceReference: string;
  readonly details: Readonly<Record<string, unknown>>;
}
