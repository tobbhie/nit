import { ValidationError } from '@nit/domain';
import { describe, expect, it } from 'vitest';
import { createStrategySnapshot, decimal, type StrategyCombinationDefinition } from '.';

function definition(): StrategyCombinationDefinition {
  return {
    entry: { kind: 'entry', policyId: 'E03', version: 2, config: { minWallets: 2 } },
    exit: {
      kind: 'exit',
      policyId: 'X05',
      version: 4,
      config: { windowSeconds: 60, minSellPct: 25 },
      compatibleEntryPolicyIds: ['E02', 'E03'],
    },
    protection: {
      kind: 'protection',
      policyId: 'P01',
      version: 1,
      config: { moonbagOriginalQuantityPct: 20 },
    },
    risk: { kind: 'risk', policyId: 'R01', version: 1, config: { maxPositions: 3 } },
  };
}

describe('strategy snapshots', () => {
  it('creates a structured display id and stable hash', () => {
    const first = createStrategySnapshot(definition());
    const base = definition();
    const reordered = {
      ...base,
      exit: { ...base.exit, config: { minSellPct: 25, windowSeconds: 60 } },
    };
    const second = createStrategySnapshot(reordered);

    expect(first.displayId).toBe('E03.v2/X05.v4/P01.v1/R01.v1');
    expect(first.configHash).toMatch(/^[a-f0-9]{64}$/);
    expect(second.configHash).toBe(first.configHash);
  });

  it('changes the hash when a decision-affecting setting changes', () => {
    const first = createStrategySnapshot(definition());
    const base = definition();
    const changed = {
      ...base,
      protection: { ...base.protection, config: { moonbagOriginalQuantityPct: 10 } },
    };
    expect(createStrategySnapshot(changed).configHash).not.toBe(first.configHash);
  });

  it('rejects incompatible entry and exit policies', () => {
    const base = definition();
    const incompatible = {
      ...base,
      entry: { kind: 'entry' as const, policyId: 'E01', version: 1, config: {} },
    };
    expect(() => createStrategySnapshot(incompatible)).toThrow(ValidationError);
  });

  it('owns and deeply freezes configuration after hashing', () => {
    const mutableConfig = { nested: { threshold: 2 } };
    const base = definition();
    const snapshot = createStrategySnapshot({
      ...base,
      entry: { ...base.entry, config: mutableConfig },
    });

    mutableConfig.nested.threshold = 99;
    expect(snapshot.entry.config).toEqual({ nested: { threshold: 2 } });
    expect(Object.isFrozen(snapshot.entry.config)).toBe(true);
  });
});

describe('decimal', () => {
  it('accepts exact non-negative values', () => {
    expect(decimal('0.000000001')).toBe('0.000000001');
  });

  it.each(['-1', '1e-9', 'NaN', '01.2', ''])('rejects unsafe decimal %s', (value) => {
    expect(() => decimal(value)).toThrow(ValidationError);
  });
});
