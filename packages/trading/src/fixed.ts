import { ValidationError } from '@nit/domain';

/** Matches NUMERIC(38,18); all arithmetic stays in integer minor units. */
export const UNIT = 10n ** 18n;
export const BPS = 10_000n;
const LIMIT = 10n ** 38n;

export function units(value: string): bigint {
  if (!/^(0|[1-9]\d*)(\.\d{1,18})?$/.test(value)) {
    throw new ValidationError('Paper amounts require nonnegative decimals with at most 18 places');
  }
  const [whole = '0', fraction = ''] = value.split('.');
  const result = BigInt(whole) * UNIT + BigInt(fraction.padEnd(18, '0'));
  if (result >= LIMIT) throw new ValidationError('Paper amount exceeds ledger precision');
  return result;
}

export function amount(value: bigint): string {
  if (value < 0n || value >= LIMIT) throw new ValidationError('Paper amount exceeds ledger bounds');
  const fraction = (value % UNIT).toString().padStart(18, '0').replace(/0+$/, '');
  return `${value / UNIT}${fraction === '' ? '' : `.${fraction}`}`;
}

export function quantize(value: bigint, decimals: number): bigint {
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 18) {
    throw new ValidationError('Paper asset decimals must be between 0 and 18');
  }
  const step = 10n ** BigInt(18 - decimals);
  return (value / step) * step;
}

export function minimum(left: bigint, right: bigint): bigint {
  return left < right ? left : right;
}
