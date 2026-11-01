import { Config } from '@nit/config';
import { ConfigurationError } from '@nit/domain';
import { SyntheticObservationSource } from '@nit/observation';
import {
  ProviderRegistry,
  ProviderSlot,
  ToyAlertPolicy,
  ToyBacktestDatasetProvider,
  ToyCatalystClassifier,
  ToyExecutionPolicy,
  ToyNarrativeScorer,
  ToyNarrativeTokenMatcher,
  ToyPrivateDataEnricher,
  ToyPromptProvider,
  ToySourcePriorityProvider,
  ToyWatchlistProvider,
  assertProductionProviderBindings,
} from '@nit/provider-interfaces';

/**
 * Bind every provider slot to its deterministic toy implementation.
 * Non-production only — the guard below rejects these under production.
 *
 * The observation source comes from `@nit/observation` rather than from the toy
 * module beside the others: it implements a contract declared there, and moving
 * that contract into `@nit/provider-interfaces` to keep the toys together would
 * make the provider package depend on the observation model. The composition root
 * is the layer allowed to know about both.
 */
export function bindToyProviders(registry: ProviderRegistry): ProviderRegistry {
  registry.register('narrativeScorer', new ToyNarrativeScorer());
  registry.register('narrativeTokenMatcher', new ToyNarrativeTokenMatcher());
  registry.register('sourcePriorityProvider', new ToySourcePriorityProvider());
  registry.register('catalystClassifier', new ToyCatalystClassifier());
  registry.register('alertPolicy', new ToyAlertPolicy());
  registry.register('watchlistProvider', new ToyWatchlistProvider());
  registry.register('backtestDatasetProvider', new ToyBacktestDatasetProvider());
  registry.register('promptProvider', new ToyPromptProvider());
  registry.register('executionPolicy', new ToyExecutionPolicy());
  registry.register('privateDataEnricher', new ToyPrivateDataEnricher());
  registry.register('observationSource', new SyntheticObservationSource());
  return registry;
}

export interface StartupGuardOptions {
  config: Config;
  registry: ProviderRegistry;
  requiredSlots?: readonly ProviderSlot[];
}

/**
 * The composition-root guard required by the foundation and configuration
 * specs. Production startup fails when a toy provider, mock authentication,
 * synthetic dataset, or required private binding is selected or absent.
 * It never falls back to sanitized behaviour.
 */
export function assertSafeStartup(options: StartupGuardOptions): void {
  const { config, registry } = options;

  if (config.environment !== 'production') {
    return;
  }

  const problems: string[] = [];

  // Toy or missing provider bindings.
  try {
    const guardOptions: Parameters<typeof assertProductionProviderBindings>[1] = {
      environment: config.environment,
    };
    if (options.requiredSlots !== undefined) {
      guardOptions.requiredSlots = options.requiredSlots;
    }
    assertProductionProviderBindings(registry, guardOptions);
  } catch (error) {
    problems.push(error instanceof Error ? error.message : String(error));
  }

  // Mock authentication / absent credentials.
  if (!config.database.password) {
    problems.push('Database credentials are absent in production');
  }

  // Synthetic dataset selection.
  const backtestBound = registry.has('backtestDatasetProvider');
  if (backtestBound) {
    const provider = registry.get('backtestDatasetProvider');
    if (provider.isToyImplementation) {
      problems.push('Synthetic backtest dataset is selected in production');
    }
  }

  // Synthetic observation source. The generic check above already names the slot,
  // but this one gets its own sentence because the consequence is not obvious
  // from a slot name: a production build bound to it observes a fabricated corpus
  // and every alert downstream is about something that never happened.
  if (registry.has('observationSource') && registry.get('observationSource').isToyImplementation) {
    problems.push('Synthetic observation source is selected in production');
  }

  if (problems.length > 0) {
    throw new ConfigurationError('Production startup rejected by composition guard', {
      environment: config.environment,
      problems,
      toyBoundSlots: registry.toyBindings(),
    });
  }
}
