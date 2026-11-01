import { describe, it, expect } from 'vitest';
import { AggregateId, ProviderId } from '@nit/domain';
import { NarrativeInput, TokenReference } from './interfaces';
import {
  ToyAlertPolicy,
  ToyBacktestDatasetProvider,
  ToyCatalystClassifier,
  ToyExecutionPolicy,
  ToyNarrativeScorer,
  ToyNarrativeTokenMatcher,
  ToySourcePriorityProvider,
  ToyWatchlistProvider,
} from './toy';

// All fixtures below are invented. No real identity, address, or market path.
const OBSERVED_AT = new Date('2020-01-01T00:00:00.000Z');

function fixture(id: string, text: string): NarrativeInput {
  return {
    narrativeId: id as AggregateId,
    text,
    observedAt: OBSERVED_AT,
    sourceProviderId: 'toy-provider' as ProviderId,
  };
}

const UNIVERSE: readonly TokenReference[] = [
  { tokenId: 'toy-1', symbol: 'AAAA' },
  { tokenId: 'toy-2', symbol: 'BBBB' },
];

describe('toy implementations are self-identifying', () => {
  it('every toy marks itself as a toy', () => {
    const toys = [
      new ToyNarrativeScorer(),
      new ToyNarrativeTokenMatcher(),
      new ToySourcePriorityProvider(),
      new ToyCatalystClassifier(),
      new ToyAlertPolicy(),
      new ToyWatchlistProvider(),
      new ToyBacktestDatasetProvider(),
      new ToyExecutionPolicy(),
    ];

    for (const toy of toys) {
      expect(toy.isToyImplementation).toBe(true);
      expect(toy.providerName.startsWith('toy-')).toBe(true);
    }
  });
});

describe('ToyNarrativeScorer', () => {
  it('preserves the candidate collection', async () => {
    const inputs = [fixture('n-1', 'text one'), fixture('n-2', 'text two')];
    const scored = await new ToyNarrativeScorer().score(inputs);

    expect(scored).toHaveLength(2);
    expect(scored.map((c) => c.candidateId)).toEqual(['n-1', 'n-2']);
  });

  it('attaches an explanation and version to every candidate', async () => {
    const scored = await new ToyNarrativeScorer().score([fixture('n-1', 'text')]);
    const candidate = scored[0];

    expect(candidate?.explanation.summary).toContain('toy implementation');
    expect(candidate?.version.schemaVersion).toBe('toy-1');
  });

  it('is deterministic for identical input (replay)', async () => {
    const scorer = new ToyNarrativeScorer();
    const inputs = [fixture('n-1', 'text one'), fixture('n-2', 'text two')];

    const first = await scorer.score(inputs);
    const second = await scorer.score(inputs);

    expect(second.map((c) => c.score)).toEqual(first.map((c) => c.score));
  });

  it('produces scores in the unit interval', async () => {
    const inputs = Array.from({ length: 25 }, (_, i) => fixture(`n-${i}`, `text ${i}`));
    const scored = await new ToyNarrativeScorer().score(inputs);

    for (const candidate of scored) {
      expect(candidate.score).toBeGreaterThanOrEqual(0);
      expect(candidate.score).toBeLessThan(1);
    }
  });

  it('returns an empty collection for empty input', async () => {
    expect(await new ToyNarrativeScorer().score([])).toEqual([]);
  });
});

describe('ToyNarrativeTokenMatcher', () => {
  it('matches literal symbols case-insensitively', async () => {
    const result = await new ToyNarrativeTokenMatcher().match(
      fixture('n-1', 'mentions aaaa only'),
      UNIVERSE
    );

    expect(result.matches.map((t) => t.symbol)).toEqual(['AAAA']);
  });

  it('returns no matches when nothing is referenced', async () => {
    const result = await new ToyNarrativeTokenMatcher().match(
      fixture('n-1', 'mentions nothing relevant'),
      UNIVERSE
    );

    expect(result.matches).toEqual([]);
  });

  it('preserves the narrative id and version metadata', async () => {
    const result = await new ToyNarrativeTokenMatcher().match(fixture('n-9', 'AAAA'), UNIVERSE);

    expect(result.narrativeId).toBe('n-9');
    expect(result.version.schemaVersion).toBe('toy-1');
  });

  it('is deterministic across runs', async () => {
    const matcher = new ToyNarrativeTokenMatcher();
    const input = fixture('n-1', 'AAAA and BBBB');

    const first = await matcher.match(input, UNIVERSE);
    const second = await matcher.match(input, UNIVERSE);

    expect(second.matches).toEqual(first.matches);
  });
});

describe('ToySourcePriorityProvider', () => {
  it('orders sources deterministically without encoding real preference', async () => {
    const ids = ['zeta', 'alpha', 'mid'] as ProviderId[];
    const result = await new ToySourcePriorityProvider().prioritize(ids);

    expect(result.map((p) => p.providerId)).toEqual(['alpha', 'mid', 'zeta']);
    expect(result.map((p) => p.priority)).toEqual([0, 1, 2]);
  });

  it('does not mutate the caller collection', async () => {
    const ids = ['zeta', 'alpha'] as ProviderId[];
    await new ToySourcePriorityProvider().prioritize(ids);
    expect(ids).toEqual(['zeta', 'alpha']);
  });
});

describe('ToyCatalystClassifier', () => {
  it('is deterministic for the same narrative', async () => {
    const classifier = new ToyCatalystClassifier();
    const input = fixture('n-1', 'text');

    const first = await classifier.classify(input);
    const second = await classifier.classify(input);

    expect(second.category).toBe(first.category);
  });

  it('only emits placeholder categories', async () => {
    const classifier = new ToyCatalystClassifier();
    const allowed = ['category-a', 'category-b', 'category-c'];

    for (let i = 0; i < 30; i += 1) {
      const result = await classifier.classify(fixture(`n-${i}`, 'text'));
      expect(allowed).toContain(result.category);
    }
  });
});

describe('capabilities that must never act in a sanitized build', () => {
  it('the toy alert policy never alerts', async () => {
    const scored = await new ToyNarrativeScorer().score([fixture('n-1', 'urgent text')]);
    const candidate = scored[0];
    expect(candidate).toBeDefined();

    const decision = await new ToyAlertPolicy().evaluate(candidate!);
    expect(decision.shouldAlert).toBe(false);
  });

  it('the toy execution policy never authorizes', async () => {
    const decision = await new ToyExecutionPolicy().authorize({
      intentId: 'intent-1' as AggregateId,
      token: { tokenId: 'toy-1', symbol: 'AAAA' },
      notional: 1_000_000,
    });

    expect(decision.approved).toBe(false);
  });
});

describe('ToyBacktestDatasetProvider', () => {
  it('generates the requested number of synthetic samples', async () => {
    const samples = await new ToyBacktestDatasetProvider().getSamples(5);
    expect(samples).toHaveLength(5);
  });

  it('uses fabricated ids and artificial timestamps', async () => {
    const samples = await new ToyBacktestDatasetProvider().getSamples(2);

    expect(samples[0]?.sampleId).toBe('toy-sample-0');
    expect(samples[0]?.observedAt.toISOString()).toBe('2020-01-01T00:00:00.000Z');
    expect(samples[1]?.observedAt.toISOString()).toBe('2020-01-01T00:01:00.000Z');
  });

  it('is deterministic across instances (replay)', async () => {
    const first = await new ToyBacktestDatasetProvider().getSamples(4);
    const second = await new ToyBacktestDatasetProvider().getSamples(4);

    expect(second.map((s) => s.observedAt.getTime())).toEqual(
      first.map((s) => s.observedAt.getTime())
    );
  });
});

describe('ToyWatchlistProvider', () => {
  it('returns only invented symbols', async () => {
    const watchlist = await new ToyWatchlistProvider().getWatchlist();
    expect(watchlist.map((t) => t.symbol)).toEqual(['AAAA', 'BBBB', 'CCCC']);
  });
});
