import { describe, expect, it } from 'vitest';
import { amount, units } from './fixed';
import { paperQuote, validatePaperMarket } from './paper-market';
import { paperPolicy } from './paper-policy';
import { OrderSide } from './runtime';
import { PAPER_START, fixtureMarket, fixtureStrategy } from './paper.fixtures';

describe('paper reserve model and exact amounts', () => {
  it('round-trips the ledger precision and rejects unsupported decimal precision', () => {
    expect(amount(units('123.000000000000000001'))).toBe('123.000000000000000001');
    expect(() => units('0.0000000000000000001')).toThrow();
    expect(() => units('100000000000000000000')).toThrow();
  });
  it('prices both venues from reserves and charges costs separately', () => {
    const market = fixtureMarket(PAPER_START);
    const policy = paperPolicy(fixtureStrategy());
    for (const venue of ['pump_fun_bonding_curve', 'pumpswap'] as const) {
      const quote = paperQuote({ ...market, venue }, OrderSide.BUY, '10', policy);
      expect(quote).toMatchObject({ input: '10', output: '990.099009', cashFee: '0.011' });
      expect(quote.impactBps).toBeGreaterThan(0);
      const sell = paperQuote({ ...market, venue }, OrderSide.SELL, quote.output, policy);
      expect(units(sell.output)).toBeLessThan(units('10'));
    }
  });
  it('rejects virtual liquidity without sufficient real reserves and future evidence', () => {
    const market = fixtureMarket(PAPER_START);
    expect(() =>
      paperQuote(
        { ...market, realTokenReserves: '1' },
        OrderSide.BUY,
        '10',
        paperPolicy(fixtureStrategy())
      )
    ).toThrow('real market reserves');
    expect(() =>
      validatePaperMarket(
        { ...market, availableAt: new Date(PAPER_START.getTime() + 1) },
        PAPER_START
      )
    ).toThrow('unavailable');
  });
});
