import { EvidenceId, SignalId } from '@nit/domain';
import { CandidateActionState, EvidenceStatus } from '@nit/signals';

export interface ResolverPolicy {
  readonly minimumNarrativeStrengthBps: number;
  readonly minimumLinkConfidenceBps: number;
  readonly minimumSelectionMarginBps: number;
  readonly minimumIndependentWallets: number;
  readonly coalescingMs: number;
  readonly walletWindowMs: number;
  readonly maxMarketAgeMs: number;
  readonly watchDurationMs: number;
  readonly pollIntervalMs: number;
  readonly maxScenariosPerCycle: number;
  readonly evidencePolicy?: {
    readonly version: 1;
    readonly narrativeRelationshipHandling: 'review' | 'advisory' | 'block';
    readonly combinedRelationshipHandling: 'review' | 'block' | 'discount';
  };
}

export interface NarrativeCandidates {
  readonly schemaVersion: 1 | 2;
  readonly episodeId?: string;
  readonly interpretationId?: string;
  readonly narrativeStatus?: EvidenceStatus;
  readonly title: string;
  readonly narrativeStrengthBps: number;
  readonly candidates: readonly {
    tokenMint: string;
    linkConfidenceBps: number;
    reasonCodes: readonly string[];
    linkageStatus?: EvidenceStatus;
  }[];
}

export interface ResolvedCandidate {
  readonly episodeId?: string;
  readonly featureSnapshotId?: string;
  readonly coverageIds?: readonly string[];
  readonly evidence?: {
    readonly narrative: EvidenceStatus;
    readonly linkage: EvidenceStatus;
    readonly wallet: EvidenceStatus;
    readonly relationship: 'unknown' | 'clear' | 'suspected';
    readonly attributionAmbiguous: boolean;
  };
  readonly qualification?: { readonly E01: boolean; readonly E03: boolean; readonly E04: boolean };
  readonly tokenMint: string;
  readonly ticker: string;
  readonly name: string;
  readonly launchTime: string | null;
  readonly rank: number;
  /** Source linkage score on 0..10000, not a probability of trading profit. */
  readonly score: string;
  readonly linkConfidenceBps: number;
  readonly independentWallets: number;
  readonly walletConfirmed: boolean;
  readonly narrativeConfirmed: boolean;
  readonly marketStatus: 'safe' | 'blocked' | 'unknown';
  readonly state: CandidateActionState;
  readonly reasonCodes: readonly string[];
  readonly evidenceIds: readonly EvidenceId[];
  readonly candidateSignalId?: SignalId;
  readonly narrativeSignalId?: SignalId;
}

export interface CandidateReport {
  readonly resolutionId: string;
  readonly narrativeId: string;
  readonly sourceSignalId: SignalId;
  readonly version: number;
  readonly title: string;
  readonly narrativeStrengthBps: number;
  readonly state: CandidateActionState;
  readonly reasonCodes: readonly string[];
  readonly candidates: readonly ResolvedCandidate[];
  readonly policyHash: string;
  readonly materialHash: string;
  readonly availableAt: string;
}

export function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Expected structured evidence');
  return value as Record<string, unknown>;
}

export function nonempty(value: unknown, max = 500): string {
  if (typeof value !== 'string' || value.trim() === '' || value.length > max)
    throw new Error('Invalid evidence text');
  return value;
}

export function integer(value: unknown, min: number, max = Number.MAX_SAFE_INTEGER): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max)
    throw new Error('Invalid bounded configuration value');
  return value;
}
