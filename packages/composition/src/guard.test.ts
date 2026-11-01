import { describe, it, expect } from 'vitest';
import { Config } from '@nit/config';
import { ConfigurationError } from '@nit/domain';
import {
  ALL_PROVIDER_SLOTS,
  ProviderRegistry,
  ToyAlertPolicy,
  ToyBacktestDatasetProvider,
} from '@nit/provider-interfaces';
import { SyntheticObservationSource } from '@nit/observation';
import { assertSafeStartup, bindToyProviders } from './guard';

function config(overrides: Partial<Config> = {}): Config {
  return {
    environment: 'development',
    version: { schemaVersion: '1.0.0', buildId: 'test' },
    database: {
      host: 'db.internal',
      port: 5432,
      database: 'nit',
      user: 'nit',
      password: 'fake-password',
      ssl: false,
      poolMin: 2,
      poolMax: 10,
    },
    queue: {
      leaseTimeoutMs: 30000,
      maxRetries: 3,
      retryBackoffMs: 1000,
      maxBackoffMs: 60000,
      pollIntervalMs: 5000,
    },
    providers: {},
    featureFlags: {
      privateIntelligence: false,
      productionAlerts: false,
      execution: false,
      privateSources: false,
    },
    logLevel: 'info',
    ...overrides,
  } as Config;
}

class PrivateProvider {
  readonly providerName = 'private';
  readonly isToyImplementation = false;
  readonly interfaceVersion = '1.0.0';
}

function fullyPrivateRegistry(): ProviderRegistry {
  const registry = new ProviderRegistry();
  for (const slot of ALL_PROVIDER_SLOTS) {
    registry.register(slot, new PrivateProvider());
  }
  return registry;
}

describe('bindToyProviders', () => {
  it('binds every provider slot', () => {
    const registry = bindToyProviders(new ProviderRegistry());
    expect(registry.missingBindings()).toEqual([]);
  });

  it('binds only toy implementations', () => {
    const registry = bindToyProviders(new ProviderRegistry());
    expect(registry.toyBindings()).toHaveLength(ALL_PROVIDER_SLOTS.length);
  });
});

describe('assertSafeStartup', () => {
  it('permits toy bindings in development', () => {
    const registry = bindToyProviders(new ProviderRegistry());
    expect(() => assertSafeStartup({ config: config(), registry })).not.toThrow();
  });

  it('permits toy bindings in staging', () => {
    const registry = bindToyProviders(new ProviderRegistry());
    expect(() =>
      assertSafeStartup({ config: config({ environment: 'staging' }), registry })
    ).not.toThrow();
  });

  it('rejects production startup with toy bindings', () => {
    const registry = bindToyProviders(new ProviderRegistry());
    expect(() =>
      assertSafeStartup({ config: config({ environment: 'production' }), registry })
    ).toThrow(ConfigurationError);
  });

  it('rejects production startup with a synthetic dataset', () => {
    const registry = fullyPrivateRegistry();
    registry.register('backtestDatasetProvider', new ToyBacktestDatasetProvider());

    try {
      assertSafeStartup({ config: config({ environment: 'production' }), registry });
      expect.unreachable('guard should have thrown');
    } catch (error) {
      const details = (error as ConfigurationError).details as Record<string, unknown>;
      const problems = details['problems'] as string[];
      expect(problems.some((p) => p.includes('Synthetic backtest dataset'))).toBe(true);
    }
  });

  it('rejects production startup with a synthetic observation source', () => {
    const registry = fullyPrivateRegistry();
    registry.register('observationSource', new SyntheticObservationSource());

    try {
      assertSafeStartup({ config: config({ environment: 'production' }), registry });
      expect.unreachable('guard should have thrown');
    } catch (error) {
      const details = (error as ConfigurationError).details as Record<string, unknown>;
      const problems = details['problems'] as string[];
      expect(problems.some((p) => p.includes('Synthetic observation source'))).toBe(true);
    }
  });

  it('rejects production startup with absent credentials', () => {
    const registry = fullyPrivateRegistry();
    const withoutPassword = config({ environment: 'production' });
    withoutPassword.database.password = '';

    try {
      assertSafeStartup({ config: withoutPassword, registry });
      expect.unreachable('guard should have thrown');
    } catch (error) {
      const details = (error as ConfigurationError).details as Record<string, unknown>;
      const problems = details['problems'] as string[];
      expect(problems.some((p) => p.includes('credentials are absent'))).toBe(true);
    }
  });

  it('accepts production startup when every binding is private', () => {
    const registry = fullyPrivateRegistry();
    expect(() =>
      assertSafeStartup({ config: config({ environment: 'production' }), registry })
    ).not.toThrow();
  });

  it('reports every problem at once rather than the first', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new ToyAlertPolicy());
    const broken = config({ environment: 'production' });
    broken.database.password = '';

    try {
      assertSafeStartup({ config: broken, registry });
      expect.unreachable('guard should have thrown');
    } catch (error) {
      const details = (error as ConfigurationError).details as Record<string, unknown>;
      expect((details['problems'] as string[]).length).toBeGreaterThan(1);
    }
  });

  it('does not fall back to sanitized behaviour on rejection', () => {
    const registry = bindToyProviders(new ProviderRegistry());
    let threw = false;
    try {
      assertSafeStartup({ config: config({ environment: 'production' }), registry });
    } catch {
      threw = true;
    }
    // The guard throws; it never returns a degraded-but-running composition.
    expect(threw).toBe(true);
  });
});
