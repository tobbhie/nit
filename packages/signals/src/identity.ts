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

  if (origin.transactionLevel === true) {
    if (origin.actorAddress === undefined || origin.actorAddress.trim() === '') {
      throw new ValidationError('Transaction-level chain signal origin requires actorAddress');
    }
    return `chain|${component(origin.chain)}|${component(origin.transactionSignature)}|transaction|${component(origin.actorAddress)}`;
  }

  const index = origin.instructionIndex ?? origin.logIndex;
  if (index === undefined) {
    throw new ValidationError(
      'Chain signal origin requires instructionIndex, logIndex, or transactionLevel'
    );
  }

  const indexKind = origin.instructionIndex !== undefined ? 'instruction' : 'log';
  return `chain|${component(origin.chain)}|${component(origin.transactionSignature)}|${indexKind}|${index}`;
}
