import { ConfigurationError } from '@nit/domain';
import { ProviderIdentity } from './interfaces';

/**
 * Slots for every provider capability. A production composition must bind a
 * real implementation to each one it enables.
 */
export type ProviderSlot =
  | 'narrativeScorer'
  | 'narrativeTokenMatcher'
  | 'sourcePriorityProvider'
  | 'catalystClassifier'
  | 'alertPolicy'
  | 'watchlistProvider'
  | 'backtestDatasetProvider'
  | 'promptProvider'
  | 'executionPolicy'
  | 'privateDataEnricher'
  // The adapter that reads monitored accounts (TIS ch.4 §2). A slot rather than a
  // direct construction because it is the one binding that decides whether the
  // system observes the real world or a fabricated corpus, which is exactly the
  // distinction the production guard exists to police.
  | 'observationSource';

export const ALL_PROVIDER_SLOTS: readonly ProviderSlot[] = [
  'narrativeScorer',
  'narrativeTokenMatcher',
  'sourcePriorityProvider',
  'catalystClassifier',
  'alertPolicy',
  'watchlistProvider',
  'backtestDatasetProvider',
  'promptProvider',
  'executionPolicy',
  'privateDataEnricher',
  'observationSource',
];

/**
 * Registry for generic adapter registration through public interfaces.
 */
export class ProviderRegistry {
  private readonly bindings = new Map<ProviderSlot, ProviderIdentity>();

  register(slot: ProviderSlot, provider: ProviderIdentity): void {
    this.bindings.set(slot, provider);
  }

  get<T extends ProviderIdentity>(slot: ProviderSlot): T {
    const provider = this.bindings.get(slot);
    if (!provider) {
      throw new ConfigurationError(`No provider bound for slot '${slot}'`, { slot });
    }
    return provider as T;
  }

  has(slot: ProviderSlot): boolean {
    return this.bindings.has(slot);
  }

  entries(): ReadonlyArray<[ProviderSlot, ProviderIdentity]> {
    return [...this.bindings.entries()];
  }

  /**
   * Slots that are bound to a toy implementation.
   */
  toyBindings(): readonly ProviderSlot[] {
    return this.entries()
      .filter(([, provider]) => provider.isToyImplementation)
      .map(([slot]) => slot);
  }

  /**
   * Slots with no binding at all.
   */
  missingBindings(required: readonly ProviderSlot[] = ALL_PROVIDER_SLOTS): readonly ProviderSlot[] {
    return required.filter((slot) => !this.bindings.has(slot));
  }
}

export interface ProductionGuardOptions {
  environment: string;
  requiredSlots?: readonly ProviderSlot[];
}

/**
 * Fail closed. In production, a toy binding or an absent required binding is a
 * startup error — never a silent fallback to sanitized behaviour.
 */
export function assertProductionProviderBindings(
  registry: ProviderRegistry,
  options: ProductionGuardOptions
): void {
  if (options.environment !== 'production') {
    return;
  }

  const required = options.requiredSlots ?? ALL_PROVIDER_SLOTS;
  const toy = registry.toyBindings();
  const missing = registry.missingBindings(required);

  if (toy.length === 0 && missing.length === 0) {
    return;
  }

  throw new ConfigurationError(
    'Production startup rejected: toy or missing provider bindings',
    {
      environment: options.environment,
      toyBoundSlots: toy,
      missingSlots: missing,
    }
  );
}
