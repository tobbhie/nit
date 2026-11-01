import { ValidationError } from '@nit/domain';

export type DecimalString = string & { readonly __brand: 'DecimalString' };

const DECIMAL_PATTERN = /^(0|[1-9]\d*)(\.\d+)?$/;

/** Exact non-negative decimal used at money and quantity boundaries. */
export function decimal(value: string): DecimalString {
  if (!DECIMAL_PATTERN.test(value)) {
    throw new ValidationError(`Invalid non-negative decimal: ${value}`);
  }
  return value as DecimalString;
}
