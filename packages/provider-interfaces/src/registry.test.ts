import { describe, it, expect } from 'vitest';
import { ConfigurationError, createVersion } from '@nit/domain';
import {
  ALL_PROVIDER_SLOTS,
  ProviderRegistry,
  assertProductionProviderBindings,
} from './registry';
import type {
  AlertDecision,
  AlertPolicy,
  NarrativeInput,
  NarrativeScorer,
  ScoredCandidate,
} from './interfaces';
import { ToyAlertPolicy } from './toy';

/**
 * Stand-ins for private production providers. These implement the interfaces
 * rather than extending the toy classes: the toy classes pin providerName and
 * isToyImplementation to literal types, so a subclass cannot widen them and an
 * `override` would not compile.
 */
class RealAlertPolicy implements AlertPolicy {
  readonly providerName = 'private-alert-policy';
  readonly isToyImplementation = false;
  readonly interfaceVersion = '1.0.0';

  evaluate(_candidate: ScoredCandidate<NarrativeInput>): Promise<AlertDecision> {
    return Promise.resolve({
      shouldAlert: true,
      reason: 'test double',
      version: createVersion('1.0.0', 'test-double'),
    });
  }
}

class RealScorer implements NarrativeScorer {
  readonly providerName = 'private-scorer';
  readonly isToyImplementation = false;
  readonly interfaceVersion = '1.0.0';

  score(_inputs: readonly NarrativeInput[]): Promise<readonly ScoredCandidate<NarrativeInput>[]> {
    return Promise.resolve([]);
  }
}

describe('ProviderRegistry', () => {
  it('returns a bound provider', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new ToyAlertPolicy());
    expect(registry.get('alertPolicy').providerName).toBe('toy-alert-policy');
  });

  it('throws for an unbound slot', () => {
    const registry = new ProviderRegistry();
    expect(() => registry.get('alertPolicy')).toThrow(ConfigurationError);
  });

  it('reports toy bindings', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new ToyAlertPolicy());
    registry.register('narrativeScorer', new RealScorer());
    expect(registry.toyBindings()).toEqual(['alertPolicy']);
  });

  it('reports missing bindings', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new RealAlertPolicy());
    expect(registry.missingBindings()).toEqual(
      ALL_PROVIDER_SLOTS.filter((slot) => slot !== 'alertPolicy')
    );
  });
});

describe('assertProductionProviderBindings', () => {
  it('permits toy bindings outside production', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new ToyAlertPolicy());

    expect(() =>
      assertProductionProviderBindings(registry, { environment: 'development' })
    ).not.toThrow();
    expect(() =>
      assertProductionProviderBindings(registry, { environment: 'staging' })
    ).not.toThrow();
  });

  it('rejects a toy binding in production', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new ToyAlertPolicy());

    expect(() =>
      assertProductionProviderBindings(registry, {
        environment: 'production',
        requiredSlots: ['alertPolicy'],
      })
    ).toThrow(ConfigurationError);
  });

  it('rejects a missing required binding in production', () => {
    const registry = new ProviderRegistry();

    expect(() =>
      assertProductionProviderBindings(registry, {
        environment: 'production',
        requiredSlots: ['alertPolicy'],
      })
    ).toThrow(ConfigurationError);
  });

  it('names the offending slots so failures are diagnosable', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new ToyAlertPolicy());

    try {
      assertProductionProviderBindings(registry, {
        environment: 'production',
        requiredSlots: ['alertPolicy', 'narrativeScorer'],
      });
      expect.unreachable('guard should have thrown');
    } catch (error) {
      const details = (error as ConfigurationError).details as Record<string, unknown>;
      expect(details['toyBoundSlots']).toEqual(['alertPolicy']);
      expect(details['missingSlots']).toEqual(['narrativeScorer']);
    }
  });

  it('passes when every required slot has a private binding', () => {
    const registry = new ProviderRegistry();
    registry.register('alertPolicy', new RealAlertPolicy());
    registry.register('narrativeScorer', new RealScorer());

    expect(() =>
      assertProductionProviderBindings(registry, {
        environment: 'production',
        requiredSlots: ['alertPolicy', 'narrativeScorer'],
      })
    ).not.toThrow();
  });

  it('never silently falls back to sanitized behaviour', () => {
    const registry = new ProviderRegistry();
    registry.register('executionPolicy', new ToyAlertPolicy());

    let threw = false;
    try {
      assertProductionProviderBindings(registry, {
        environment: 'production',
        requiredSlots: ['executionPolicy'],
      });
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });
});
