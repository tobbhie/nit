import { ValidationError } from '@nit/domain';
import { BPS, UNIT, amount, minimum, quantize, units } from './fixed';
import { PaperPolicy } from './paper-policy';
import { OrderSide } from './runtime';

/** Reserve-model evidence, not an executable route or a guarantee of real fills. */
export interface PaperMarketSnapshot {
  readonly snapshotId: string;
  readonly tokenMint: string;
  readonly cashAsset: string;
  readonly venue: 'pump_fun_bonding_curve' | 'pumpswap';
  readonly tokenReserves: string;
  readonly cashReserves: string;
  readonly realTokenReserves: string;
  readonly realCashReserves: string;
  readonly tokenDecimals: number;
  readonly cashDecimals: number;
  readonly verified: boolean;
  readonly tradable: boolean;
  readonly sellable: boolean;
  readonly criticalRisk: boolean;
  readonly occurredAt: Date;
  readonly observedAt: Date;
  readonly availableAt: Date;
}

export interface PaperQuote {
  readonly input: string;
  readonly output: string;
  readonly cashFee: string;
  readonly impactBps: number;
}

export function validatePaperMarket(market: PaperMarketSnapshot, now: Date): void {
  if (
    !/^[0-9a-f-]{36}$/i.test(market.snapshotId) ||
    !/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(market.tokenMint) ||
    market.cashAsset.trim() === '' ||
    !['pump_fun_bonding_curve', 'pumpswap'].includes(market.venue)
  ) {
    throw new ValidationError('Invalid paper market identity');
  }
  if (
    !Number.isFinite(now.getTime()) ||
    !Number.isFinite(market.occurredAt.getTime()) ||
    !Number.isFinite(market.observedAt.getTime()) ||
    !Number.isFinite(market.availableAt.getTime()) ||
    market.occurredAt > market.availableAt ||
    market.occurredAt > market.observedAt ||
    market.observedAt > market.availableAt ||
    market.availableAt > now
  ) {
    throw new ValidationError('Paper market evidence is invalid or unavailable as of the clock');
  }
  for (const key of ['verified', 'tradable', 'sellable', 'criticalRisk'] as const) {
    if (typeof market[key] !== 'boolean') throw new ValidationError('Missing critical market flag');
  }
  for (const value of [
    market.tokenReserves,
    market.cashReserves,
    market.realTokenReserves,
    market.realCashReserves,
  ])
    units(value);
  quantize(0n, market.tokenDecimals);
  quantize(0n, market.cashDecimals);
}

/** Constant-product estimate with separate real reserves for bonding-curve capacity. */
export function paperQuote(
  market: PaperMarketSnapshot,
  side: OrderSide,
  inputAmount: string,
  policy: PaperPolicy,
  fractionBps = 10_000
): PaperQuote {
  if (!Number.isSafeInteger(fractionBps) || fractionBps < 1 || fractionBps > 10_000) {
    throw new ValidationError('Paper fill fraction must be between 1 and 10000 basis points');
  }
  const token = units(market.tokenReserves);
  const cash = units(market.cashReserves);
  if (token === 0n || cash === 0n) throw new ValidationError('Missing market reserves');
  const buying = side === OrderSide.BUY;
  const input = quantize(
    (units(inputAmount) * BigInt(fractionBps)) / BPS,
    buying ? market.cashDecimals : market.tokenDecimals
  );
  if (input === 0n) throw new ValidationError('Paper order rounds to zero');
  const reserveIn = buying ? cash : token;
  const reserveOut = buying ? token : cash;
  const output = quantize(
    (input * reserveOut) / (reserveIn + input),
    buying ? market.tokenDecimals : market.cashDecimals
  );
  const realOut = units(buying ? market.realTokenReserves : market.realCashReserves);
  if (output === 0n || output > realOut)
    throw new ValidationError('Insufficient real market reserves');
  const spot = (input * reserveOut) / reserveIn;
  const impactBps = Number(minimum(BPS, ((spot - output) * BPS) / (spot || 1n)));
  const cashTurnover = buying ? input : output;
  const feeQuantum = 10n ** BigInt(18 - market.cashDecimals);
  const exactFeeNumerator = cashTurnover * BigInt(policy.feeBps) + units(policy.fixedFeeCash) * BPS;
  const cashFee = ((exactFeeNumerator + BPS * feeQuantum - 1n) / (BPS * feeQuantum)) * feeQuantum;
  if (!buying && cashFee >= output) throw new ValidationError('Sell proceeds do not cover costs');
  if (buying && ((input + cashFee) * UNIT) / output === 0n) {
    throw new ValidationError('Paper unit price is below ledger precision');
  }
  return { input: amount(input), output: amount(output), cashFee: amount(cashFee), impactBps };
}
