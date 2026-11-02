import { describe, expect, it } from 'vitest';
import {
  MarketVenue,
  TokenLifecycleState,
  generateTokenId,
  generateTokenMarketId,
} from '@nit/domain';
import { deriveTokenLifecycle, TokenMarketInterval } from './lifecycle';

function interval(venue: MarketVenue): TokenMarketInterval {
  return {
    tokenMarketId: generateTokenMarketId(),
    tokenId: generateTokenId(),
    venue,
    marketAddress: 'Market111',
    activeFrom: new Date('2026-08-20T09:00:00.000Z'),
    origin: { kind: 'legacy', key: 'fixture' },
    provenance: {},
    recordedAt: new Date('2026-08-20T09:00:01.000Z'),
  };
}

describe('deriveTokenLifecycle', () => {
  it('keeps lifecycle derived from market history rather than token mutation', () => {
    expect(deriveTokenLifecycle(null)).toBe(TokenLifecycleState.DISCOVERED);
    expect(deriveTokenLifecycle(interval(MarketVenue.PUMP_FUN_BONDING_CURVE))).toBe(
      TokenLifecycleState.BONDING_CURVE_ACTIVE
    );
    expect(deriveTokenLifecycle(interval(MarketVenue.PUMPSWAP))).toBe(
      TokenLifecycleState.GRADUATED
    );
  });
});
