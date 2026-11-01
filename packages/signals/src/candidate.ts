import {
  CandidateResolutionId,
  EvidenceId,
  SignalId,
  Timestamp,
} from '@nit/domain';

export enum CandidateActionState {
  TRADE_ELIGIBLE = 'trade_eligible',
  WATCH = 'watch',
  MANUAL_REVIEW = 'manual_review',
  BLOCKED = 'blocked',
  NO_CANDIDATE = 'no_candidate',
}

export interface CandidateRank {
  readonly tokenMint: string;
  readonly rank: number;
  readonly score: string;
  readonly reasonCodes: readonly string[];
}

/**
 * An immutable version of one narrative's complete candidate-resolution state.
 * Late evidence creates the next version; it never rewrites what was known.
 */
export interface CandidateResolutionVersion {
  readonly resolutionId: CandidateResolutionId;
  readonly canonicalNarrativeId: string;
  readonly version: number;
  readonly sourceSignalId: SignalId;
  readonly state: CandidateActionState;
  readonly candidates: readonly CandidateRank[];
  readonly evidenceIds: readonly EvidenceId[];
  readonly reasonCodes: readonly string[];
  readonly occurredAt: Timestamp;
  readonly observedAt: Timestamp;
  readonly availableAt: Timestamp;
}
