import { AggregateId, ProviderId, Timestamp, Version } from '@nit/domain';

/**
 * Provider capability interfaces. These declare shape only.
 *
 * Every implementation in this repository is a deterministic toy used for
 * development and tests; the production guard rejects them. The policy logic
 * behind these slots — scoring, matching, source priority, classification,
 * alert policy — is built here. What arrives from outside the repository is
 * narrower: calibrated weights and thresholds, real datasets such as the
 * watchlist and backtest corpus, and the credentials a live provider needs.
 * See docs/handoffs/provider-implementations.md for the per-slot detail.
 */

/** Marks an implementation as non-production. Guards reject these. */
export interface ProviderIdentity {
  readonly providerName: string;
  readonly isToyImplementation: boolean;
  readonly interfaceVersion: string;
}

export interface Explanation {
  /** Human-readable rationale. Toy implementations emit synthetic text. */
  summary: string;
  factors: ReadonlyArray<{ name: string; contribution: number }>;
}

export interface Candidate<TSubject = unknown> {
  candidateId: AggregateId;
  subject: TSubject;
}

export interface ScoredCandidate<TSubject = unknown> extends Candidate<TSubject> {
  score: number;
  explanation: Explanation;
  scoredAt: Timestamp;
  version: Version;
}

export interface NarrativeInput {
  narrativeId: AggregateId;
  text: string;
  observedAt: Timestamp;
  sourceProviderId: ProviderId;
}

export interface NarrativeScorer extends ProviderIdentity {
  score(inputs: readonly NarrativeInput[]): Promise<readonly ScoredCandidate<NarrativeInput>[]>;
}

export interface TokenReference {
  tokenId: string;
  symbol: string;
}

export interface MatchResult {
  narrativeId: AggregateId;
  matches: readonly TokenReference[];
  explanation: Explanation;
  version: Version;
}

export interface NarrativeTokenMatcher extends ProviderIdentity {
  match(input: NarrativeInput, universe: readonly TokenReference[]): Promise<MatchResult>;
}

export interface SourcePriority {
  providerId: ProviderId;
  priority: number;
}

export interface SourcePriorityProvider extends ProviderIdentity {
  prioritize(providerIds: readonly ProviderId[]): Promise<readonly SourcePriority[]>;
}

export interface CatalystClassification {
  category: string;
  confidence: number;
  explanation: Explanation;
  version: Version;
}

export interface CatalystClassifier extends ProviderIdentity {
  classify(input: NarrativeInput): Promise<CatalystClassification>;
}

export interface AlertDecision {
  shouldAlert: boolean;
  reason: string;
  version: Version;
}

export interface AlertPolicy extends ProviderIdentity {
  evaluate(candidate: ScoredCandidate<NarrativeInput>): Promise<AlertDecision>;
}

export interface WatchlistProvider extends ProviderIdentity {
  getWatchlist(): Promise<readonly TokenReference[]>;
}

export interface BacktestSample {
  sampleId: string;
  observedAt: Timestamp;
  input: NarrativeInput;
}

export interface BacktestDatasetProvider extends ProviderIdentity {
  getSamples(limit: number): Promise<readonly BacktestSample[]>;
}

export interface PromptTemplate {
  promptId: string;
  template: string;
  version: Version;
}

export interface PromptProvider extends ProviderIdentity {
  getPrompt(promptId: string): Promise<PromptTemplate>;
}

export interface ExecutionIntent {
  intentId: AggregateId;
  token: TokenReference;
  notional: number;
}

export interface ExecutionDecision {
  approved: boolean;
  reason: string;
  version: Version;
}

export interface ExecutionPolicy extends ProviderIdentity {
  authorize(intent: ExecutionIntent): Promise<ExecutionDecision>;
}

export interface PrivateDataEnricher extends ProviderIdentity {
  enrich(input: NarrativeInput): Promise<NarrativeInput & { enrichment: Record<string, unknown> }>;
}
