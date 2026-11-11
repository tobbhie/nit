import { describe, expect, it } from 'vitest';
import { TokenRiskGate, tokenRiskEntryRejection } from './token-risk';
const now = new Date('2026-01-01T00:00:10Z');
const hash = 'a'.repeat(64);
const gate: TokenRiskGate = {
  assessmentId: '00000000-0000-4000-8000-000000000001',
  policyHash: hash,
  tokenMint: 'mint',
  marketAddress: 'market',
  status: 'clear',
  reasonCodes: [],
  observedAt: '2026-01-01T00:00:00Z',
  availableAt: '2026-01-01T00:00:01Z',
  expiresAt: '2026-01-01T00:00:30Z',
};
describe('pinned token risk entry gate', () => {
  it('preserves existing strategies until they opt into a pinned risk policy', () => {
    expect(tokenRiskEntryRejection(undefined, undefined, 'mint', undefined, now)).toBeNull();
  });
  it('requires fresh evidence for the exact mint, venue address and policy', () => {
    expect(tokenRiskEntryRejection(gate, hash, 'mint', 'market', now)).toBeNull();
    expect(tokenRiskEntryRejection(undefined, hash, 'mint', 'market', now)).toBe(
      'TOKEN_RISK_UNAVAILABLE'
    );
    expect(tokenRiskEntryRejection(gate, hash, 'other', 'market', now)).toBe(
      'TOKEN_RISK_MARKET_MISMATCH'
    );
    expect(tokenRiskEntryRejection(gate, 'b'.repeat(64), 'mint', 'market', now)).toBe(
      'TOKEN_RISK_POLICY_MISMATCH'
    );
  });
  it('never converts partial, unavailable or blocked evidence into clearance', () => {
    for (const status of ['blocked', 'incomplete', 'unavailable', 'invalid'])
      expect(
        tokenRiskEntryRejection({ ...gate, status }, hash, 'mint', 'market', now)
      ).not.toBeNull();
  });
  it('rejects expiry, future availability, invalid clocks and malformed evidence', () => {
    for (const change of [
      { expiresAt: now.toISOString() },
      { availableAt: '2027-01-01T00:00:00Z' },
      { observedAt: 'invalid' },
      { assessmentId: 'invalid' },
      { reasonCodes: [null] },
    ])
      expect(
        tokenRiskEntryRejection({ ...gate, ...change }, hash, 'mint', 'market', now)
      ).not.toBeNull();
  });
});
