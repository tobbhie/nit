import { describe, it, expect } from 'vitest';
import { EvidenceQualificationPolicy, QualificationFacts, qualifyEvidence } from './qualification';
const policy: EvidenceQualificationPolicy = {
  version: 1,
  minimumNarrativeStrengthBps: 5000,
  minimumLinkConfidenceBps: 5000,
  minimumIndependentWallets: 2,
  narrativeRelationshipHandling: 'review',
  combinedRelationshipHandling: 'review',
};
const facts: QualificationFacts = {
  narrative: 'confirmed',
  linkage: 'confirmed',
  wallet: 'unavailable',
  narrativeStrengthBps: 9000,
  linkConfidenceBps: 9000,
  independentWallets: 0,
  relationship: 'unknown',
  attributionAmbiguous: false,
  selectionRank: 1,
  correlationWindowOpen: false,
  executionSafe: true,
};
describe('shared versioned evidence qualification', () => {
  it('keeps narrative-only independent of wallet coverage', () =>
    expect(qualifyEvidence(facts, policy)).toEqual({ E01: true, E03: false, E04: true }));
  it('keeps ambiguous attribution out of selected entries while allowing all-eligible evaluation', () => {
    expect(qualifyEvidence({ ...facts, attributionAmbiguous: true }, policy)).toEqual({
      E01: false,
      E03: false,
      E04: true,
    });
  });
  it('uses collapsed wallet groups under an explicit discount policy independently of narrative relationship handling', () => {
    const related = {
      ...facts,
      wallet: 'confirmed' as const,
      independentWallets: 2,
      relationship: 'suspected' as const,
    };
    expect(qualifyEvidence(related, policy).E03).toBe(false);
    expect(
      qualifyEvidence(related, { ...policy, combinedRelationshipHandling: 'discount' })
    ).toEqual({ E01: false, E03: true, E04: false });
    expect(
      qualifyEvidence(
        { ...related, independentWallets: 1 },
        { ...policy, combinedRelationshipHandling: 'discount' }
      ).E03
    ).toBe(false);
  });
  it('never substitutes a positive extraction status for pinned score thresholds or technical safety', () => {
    for (const altered of [
      { ...facts, narrativeStrengthBps: 1000 },
      { ...facts, executionSafe: false },
      { ...facts, linkage: 'contradictory' as const },
      { ...facts, correlationWindowOpen: true },
    ])
      expect(qualifyEvidence(altered, policy)).toEqual({ E01: false, E03: false, E04: false });
  });
});
