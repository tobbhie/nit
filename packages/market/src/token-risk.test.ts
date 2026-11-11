import { describe, expect, it } from 'vitest';
import { concentrationBounds } from './token-risk';
describe('conservative owner concentration bounds', () => {
  it('groups multiple accounts by owner and excludes verified market vault quantities', () => {
    const result = concentrationBounds(1000n, [
      { owner: 'vault', amount: 700n, vault: true },
      { owner: 'a', amount: 100n, vault: false },
      { owner: 'a', amount: 50n, vault: false },
      { owner: 'b', amount: 150n, vault: false },
    ]);
    expect(result).toMatchObject({
      ownerLowerBps: 5000,
      ownerUpperBps: 5000,
      capturedOwners: 2,
      uncapturedAmount: '0',
    });
  });
  it('keeps uncaptured supply in the upper bound instead of assuming unknown owners are independent', () => {
    expect(concentrationBounds(1000n, [{ owner: 'a', amount: 100n, vault: false }])).toMatchObject({
      ownerLowerBps: 1000,
      ownerUpperBps: 10000,
      topTenUpperBps: 10000,
      uncapturedAmount: '900',
    });
  });
  it('uses integer arithmetic beyond floating-point supply precision and rejects inconsistent evidence', () => {
    const supply = 1000000000000000000n;
    expect(
      concentrationBounds(supply, [{ owner: 'a', amount: supply / 2n, vault: false }]).ownerLowerBps
    ).toBe(5000);
    expect(() => concentrationBounds(1n, [{ owner: 'a', amount: 2n, vault: false }])).toThrow();
    expect(() => concentrationBounds(1n, [{ owner: 'pool', amount: 1n, vault: true }])).toThrow();
  });
});
