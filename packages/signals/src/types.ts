import { Chain, EvidenceId, SignalId, Timestamp } from '@nit/domain';

export enum SignalKind {
  NARRATIVE = 'narrative',
  WALLET_BUY = 'wallet_buy',
  WALLET_SELL = 'wallet_sell',
  MARKET = 'market',
  CANDIDATE_UPDATE = 'candidate_update',
}

export interface ProviderSignalOrigin {
  readonly kind: 'provider';
  readonly provider: string;
  readonly referenceId: string;
}

export interface ChainSignalOrigin {
  readonly kind: 'chain';
  readonly chain: Chain;
  readonly transactionSignature: string;
  readonly instructionIndex?: number;
  readonly logIndex?: number;
}

/** The immutable source occurrence used for ingestion deduplication. */
export type SignalOrigin = ProviderSignalOrigin | ChainSignalOrigin;

/**
 * A source-agnostic signal carried by the existing domain event system.
 *
 * `occurredAt` is source time, `observedAt` is receipt time, and `availableAt`
 * is the first instant a decision process could use the normalized evidence.
 */
export interface SignalEnvelope<TPayload = unknown> {
  readonly eventId: SignalId;
  readonly source: string;
  readonly sourceVersion: string;
  readonly kind: SignalKind;
  readonly chain: Chain;
  readonly tokenMint?: string;
  readonly canonicalNarrativeId?: string;
  readonly occurredAt: Timestamp;
  readonly observedAt: Timestamp;
  readonly availableAt: Timestamp;
  readonly origin: SignalOrigin;
  readonly evidenceIds: readonly EvidenceId[];
  readonly payload: TPayload;
}
