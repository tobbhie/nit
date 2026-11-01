import { AggregateId, ProviderId, Version, now } from '@nit/domain';
import {
  AlertDecision,
  AlertPolicy,
  BacktestDatasetProvider,
  BacktestSample,
  CatalystClassification,
  CatalystClassifier,
  ExecutionDecision,
  ExecutionIntent,
  ExecutionPolicy,
  Explanation,
  MatchResult,
  NarrativeInput,
  NarrativeScorer,
  NarrativeTokenMatcher,
  PrivateDataEnricher,
  PromptProvider,
  PromptTemplate,
  ScoredCandidate,
  SourcePriority,
  SourcePriorityProvider,
  TokenReference,
  WatchlistProvider,
} from './interfaces';

const TOY_VERSION: Version = {
  schemaVersion: 'toy-1',
  buildId: 'toy',
};

const TOY_INTERFACE_VERSION = '1.0.0';

/**
 * Deterministic, content-independent hash.
 *
 * This exists so toy output is stable across runs (replay tests depend on it).
 * It encodes no ranking judgement whatsoever — it is a string checksum.
 */
function stableHash(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash);
}

function toyExplanation(kind: string): Explanation {
  return {
    summary: `Synthetic ${kind} produced by a toy implementation. Not a real assessment.`,
    factors: [{ name: 'toy-placeholder', contribution: 0 }],
  };
}

export class ToyNarrativeScorer implements NarrativeScorer {
  readonly providerName = 'toy-narrative-scorer';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  score(inputs: readonly NarrativeInput[]): Promise<readonly ScoredCandidate<NarrativeInput>[]> {
    return Promise.resolve(
      inputs.map((input) => ({
        candidateId: input.narrativeId,
        subject: input,
        score: (stableHash(input.narrativeId) % 1000) / 1000,
        explanation: toyExplanation('score'),
        scoredAt: input.observedAt,
        version: TOY_VERSION,
      }))
    );
  }
}

export class ToyNarrativeTokenMatcher implements NarrativeTokenMatcher {
  readonly providerName = 'toy-narrative-token-matcher';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  match(input: NarrativeInput, universe: readonly TokenReference[]): Promise<MatchResult> {
    // Case-insensitive literal symbol presence. Deliberately naive.
    const haystack = input.text.toLowerCase();
    const matches = universe.filter((token) => haystack.includes(token.symbol.toLowerCase()));

    return Promise.resolve({
      narrativeId: input.narrativeId,
      matches,
      explanation: toyExplanation('match'),
      version: TOY_VERSION,
    });
  }
}

export class ToySourcePriorityProvider implements SourcePriorityProvider {
  readonly providerName = 'toy-source-priority-provider';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  prioritize(providerIds: readonly ProviderId[]): Promise<readonly SourcePriority[]> {
    // Stable lexicographic order. Carries no real source preference.
    return Promise.resolve(
      [...providerIds]
        .sort((a, b) => a.localeCompare(b))
        .map((providerId, index) => ({ providerId, priority: index }))
    );
  }
}

export class ToyCatalystClassifier implements CatalystClassifier {
  readonly providerName = 'toy-catalyst-classifier';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  private static readonly CATEGORIES = ['category-a', 'category-b', 'category-c'] as const;

  classify(input: NarrativeInput): Promise<CatalystClassification> {
    const index = stableHash(input.narrativeId) % ToyCatalystClassifier.CATEGORIES.length;
    return Promise.resolve({
      category: ToyCatalystClassifier.CATEGORIES[index] ?? 'category-a',
      confidence: 0.5,
      explanation: toyExplanation('classification'),
      version: TOY_VERSION,
    });
  }
}

export class ToyAlertPolicy implements AlertPolicy {
  readonly providerName = 'toy-alert-policy';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  evaluate(_candidate: ScoredCandidate<NarrativeInput>): Promise<AlertDecision> {
    // Always declines. A toy build must never emit an alert.
    return Promise.resolve({
      shouldAlert: false,
      reason: 'Toy alert policy never alerts.',
      version: TOY_VERSION,
    });
  }
}

export class ToyWatchlistProvider implements WatchlistProvider {
  readonly providerName = 'toy-watchlist-provider';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  // Invented symbols. Not a real watchlist.
  private static readonly FIXTURE: readonly TokenReference[] = [
    { tokenId: 'toy-token-1', symbol: 'AAAA' },
    { tokenId: 'toy-token-2', symbol: 'BBBB' },
    { tokenId: 'toy-token-3', symbol: 'CCCC' },
  ];

  getWatchlist(): Promise<readonly TokenReference[]> {
    return Promise.resolve(ToyWatchlistProvider.FIXTURE);
  }
}

export class ToyBacktestDatasetProvider implements BacktestDatasetProvider {
  readonly providerName = 'toy-backtest-dataset-provider';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  constructor(private readonly epochMs = Date.UTC(2020, 0, 1)) {}

  getSamples(limit: number): Promise<readonly BacktestSample[]> {
    const samples: BacktestSample[] = [];
    for (let i = 0; i < limit; i += 1) {
      const observedAt = new Date(this.epochMs + i * 60_000);
      samples.push({
        sampleId: `toy-sample-${i}`,
        observedAt,
        input: {
          narrativeId: `toy-narrative-${i}` as AggregateId,
          text: `Fabricated sample text ${i} referencing AAAA.`,
          observedAt,
          sourceProviderId: 'toy-provider' as ProviderId,
        },
      });
    }
    return Promise.resolve(samples);
  }
}

export class ToyPromptProvider implements PromptProvider {
  readonly providerName = 'toy-prompt-provider';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  getPrompt(promptId: string): Promise<PromptTemplate> {
    return Promise.resolve({
      promptId,
      template: `Toy prompt template for ${promptId}. Contains no private prompt content.`,
      version: TOY_VERSION,
    });
  }
}

export class ToyExecutionPolicy implements ExecutionPolicy {
  readonly providerName = 'toy-execution-policy';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  authorize(_intent: ExecutionIntent): Promise<ExecutionDecision> {
    // Always refuses. This repository has no execution authority.
    return Promise.resolve({
      approved: false,
      reason: 'Toy execution policy never authorizes execution.',
      version: TOY_VERSION,
    });
  }
}

export class ToyPrivateDataEnricher implements PrivateDataEnricher {
  readonly providerName = 'toy-private-data-enricher';
  readonly isToyImplementation = true;
  readonly interfaceVersion = TOY_INTERFACE_VERSION;

  enrich(input: NarrativeInput): Promise<NarrativeInput & { enrichment: Record<string, unknown> }> {
    return Promise.resolve({
      ...input,
      enrichment: { toy: true, enrichedAt: now().toISOString() },
    });
  }
}
