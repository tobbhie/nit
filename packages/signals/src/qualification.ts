import { EvidenceStatus } from './research';

export interface EvidenceQualificationPolicy {
  readonly version: 1;
  readonly minimumNarrativeStrengthBps: number;
  readonly minimumLinkConfidenceBps: number;
  readonly minimumIndependentWallets: number;
  readonly narrativeRelationshipHandling: 'review' | 'advisory' | 'block';
  readonly combinedRelationshipHandling: 'review' | 'block' | 'discount';
}
export interface QualificationFacts {
  readonly narrative: EvidenceStatus;
  readonly linkage: EvidenceStatus;
  readonly wallet: EvidenceStatus;
  readonly narrativeStrengthBps: number;
  readonly linkConfidenceBps: number;
  readonly independentWallets: number;
  readonly relationship: 'unknown' | 'clear' | 'suspected';
  readonly attributionAmbiguous: boolean;
  readonly selectionRank: number;
  readonly correlationWindowOpen: boolean;
  readonly executionSafe: boolean;
}
export function validateQualificationPolicy(policy: EvidenceQualificationPolicy): void {
  if (
    policy.version !== 1 ||
    ![policy.minimumNarrativeStrengthBps, policy.minimumLinkConfidenceBps].every(
      (n) => Number.isSafeInteger(n) && n >= 0 && n <= 10000
    ) ||
    !Number.isSafeInteger(policy.minimumIndependentWallets) ||
    policy.minimumIndependentWallets < 2 ||
    !['review', 'advisory', 'block'].includes(policy.narrativeRelationshipHandling) ||
    !['review', 'block', 'discount'].includes(policy.combinedRelationshipHandling)
  )
    throw new Error('Invalid pinned qualification policy');
}
/** Shared by live resolution and historical evidence masks; no source I/O or model judgment. */
export function qualifyEvidence(
  facts: QualificationFacts,
  policy: EvidenceQualificationPolicy
): { E01: boolean; E03: boolean; E04: boolean } {
  validateQualificationPolicy(policy);
  const narrative =
    facts.narrative === 'confirmed' &&
    facts.linkage === 'confirmed' &&
    facts.narrativeStrengthBps >= policy.minimumNarrativeStrengthBps &&
    facts.linkConfidenceBps >= policy.minimumLinkConfidenceBps &&
    !facts.correlationWindowOpen &&
    facts.executionSafe;
  const selected = narrative && !facts.attributionAmbiguous && facts.selectionRank === 1;
  const related = facts.relationship === 'suspected';
  return {
    E01: selected && (!related || policy.narrativeRelationshipHandling === 'advisory'),
    E03:
      selected &&
      facts.wallet === 'confirmed' &&
      facts.independentWallets >= policy.minimumIndependentWallets &&
      (!related || policy.combinedRelationshipHandling === 'discount'),
    E04: narrative && (!related || policy.narrativeRelationshipHandling === 'advisory'),
  };
}
