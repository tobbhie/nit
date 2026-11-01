import { ValidationError } from '@nit/domain';
import { SignalOrigin } from './types';

function component(value: string): string {
  return `${value.length}:${value}`;
}

/**
 * Stable, collision-resistant-within-the-contract key for source occurrences.
 * A mint is deliberately absent: one mint can produce many valid lifecycle
 * events, while transaction signature plus instruction/log index identifies
 * the exact on-chain occurrence.
 */
export function signalOriginKey(origin: SignalOrigin): string {
  if (origin.kind === 'provider') {
    return `provider|${component(origin.provider)}|${component(origin.referenceId)}`;
  }

  const index = origin.instructionIndex ?? origin.logIndex;
  if (index === undefined) {
    throw new ValidationError('Chain signal origin requires instructionIndex or logIndex');
  }

  const indexKind = origin.instructionIndex !== undefined ? 'instruction' : 'log';
  return `chain|${component(origin.chain)}|${component(origin.transactionSignature)}|${indexKind}|${index}`;
}
