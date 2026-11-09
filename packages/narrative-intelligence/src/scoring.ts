import { evidenceHash } from '@nit/signals';
import {
  NarrativePolicy,
  SourceText,
  TextExtraction,
  normalized,
  phrase,
  subjectKey,
} from './contracts';

export interface InterpretedSource {
  source: SourceText;
  extraction: TextExtraction;
  artifactId: string;
  novel: boolean;
}
export interface NarrativeGroup {
  key: string;
  subject: ReturnType<typeof subjectKey>;
  category: string;
  catalystKey: string;
  startsAt: Date;
  expiresAt: Date;
  sources: InterpretedSource[];
}
export interface TokenFacts {
  mint: string;
  name: string;
  ticker: string;
  launchTime: Date;
  discoveredAt: Date;
}
export interface ScoredNarrative {
  narrativeStrengthBps: number;
  status: 'confirmed' | 'pending' | 'contradictory' | 'observed_nonconfirming' | 'unavailable';
  independentSources: number;
  features: { independence: number; persistence: number; priority: number };
  candidates: {
    tokenMint: string;
    linkConfidenceBps: number;
    status: 'confirmed' | 'pending' | 'contradictory';
    reasonCodes: string[];
    features: {
      lexical: number;
      identity: number;
      directReference: number;
      temporal: number;
      contradiction: number;
    };
    launchRelation: 'PRE_CATALYST_LAUNCH' | 'POST_CATALYST_LAUNCH';
  }[];
  overflow: boolean;
}

export function groupSources(
  inputs: readonly InterpretedSource[],
  policy: NarrativePolicy,
  asOf: Date
): NarrativeGroup[] {
  const groups = new Map<string, NarrativeGroup>();
  for (const input of inputs) {
    if (input.source.ingestedAt > asOf || input.source.publishedAt > asOf)
      throw new Error('Future source evidence');
    for (const topic of input.extraction.topics) {
      const subject = subjectKey(topic.subjectQuote, policy);
      const start =
        Math.floor(input.source.publishedAt.getTime() / policy.episodeWindowMs) *
        policy.episodeWindowMs;
      const catalystKey = normalized(topic.catalystQuote);
      const key = evidenceHash({
        subject: subject.key,
        catalystKey,
        start,
        window: policy.episodeWindowMs,
      });
      const expiresAt = new Date(start + policy.episodeWindowMs + policy.watchDurationMs);
      if (expiresAt <= asOf) continue;
      let group = groups.get(key);
      if (group === undefined) {
        group = {
          key,
          subject,
          category: topic.category,
          catalystKey,
          startsAt: new Date(start),
          expiresAt,
          sources: [],
        };
        groups.set(key, group);
      }
      if (group.category !== topic.category) group.category = 'mixed';
      if (!group.sources.some((s) => s.artifactId === input.artifactId)) group.sources.push(input);
    }
  }
  return [...groups.values()].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
}
/** Reposts/quotes are propagation context; absent their origins they never create corroboration. */
function independent(input: readonly InterpretedSource[]): InterpretedSource[][] {
  const groups: { keys: Set<string>; sources: InterpretedSource[] }[] = [];
  for (const item of input) {
    if (item.source.derivativeIds.length > 0 || !item.novel) continue;
    const keys = new Set([
      `author:${item.source.provider}:${item.source.authorId}`,
      `content:${evidenceHash(normalized(item.source.text))}`,
    ]);
    const sources = [item];
    for (let i = groups.length - 1; i >= 0; i--) {
      const other = groups[i]!;
      if ([...other.keys].some((key) => keys.has(key))) {
        for (const key of other.keys) keys.add(key);
        sources.push(...other.sources);
        groups.splice(i, 1);
      }
    }
    groups.push({ keys, sources });
  }
  return groups.map((g) => g.sources);
}
const scaled = (n: number, d: number): number => Math.min(10000, Math.floor((n * 10000) / d));
function weighted(features: Record<string, number>, weights: Record<string, number>): number {
  return Math.floor(
    Object.entries(weights).reduce((sum, [key, weight]) => sum + (features[key] ?? 0) * weight, 0) /
      10000
  );
}
function containsMint(text: string, mint: string): boolean {
  const parts: readonly string[] = text.match(/[1-9A-HJ-NP-Za-km-z]+/g) ?? [];
  return parts.includes(mint);
}
export function scoreGroup(
  group: NarrativeGroup,
  tokens: readonly TokenFacts[],
  policy: NarrativePolicy,
  asOf: Date,
  complete: boolean
): ScoredNarrative {
  const available = group.sources.filter(
    (s) => s.source.publishedAt.getTime() + policy.attentionWindowMs > asOf.getTime()
  );
  const stance = (item: InterpretedSource, value: string): boolean =>
    item.extraction.topics.some(
      (t) =>
        subjectKey(t.subjectQuote, policy).key === group.subject.key &&
        normalized(t.catalystQuote) === group.catalystKey &&
        t.stance === value
    );
  const support = available.filter((s) => stance(s, 'support'));
  const contradicts = available.some((s) => stance(s, 'contradiction'));
  const roots = independent(support);
  const times = roots.flat().map((s) => s.source.publishedAt.getTime());
  const features = {
    independence: scaled(roots.length, policy.sourceSaturation),
    persistence:
      times.length < 2
        ? 0
        : scaled(Math.max(...times) - Math.min(...times), policy.persistenceSaturationMs),
    priority:
      roots.length === 0
        ? 0
        : Math.floor(
            roots.reduce(
              (sum, g) =>
                sum +
                Math.max(
                  ...g.map(
                    (s) => policy.sourcePriority[`${s.source.provider}:${s.source.authorId}`] ?? 0
                  )
                ),
              0
            ) / roots.length
          ),
  };
  const narrativeStrengthBps = weighted(features, policy.narrativeWeights);
  const status = !complete
    ? 'unavailable'
    : contradicts
      ? 'contradictory'
      : group.subject.ambiguous
        ? 'pending'
        : roots.length >= policy.minimumIndependentSources &&
            narrativeStrengthBps >= policy.narrativeThresholdBps
          ? 'confirmed'
          : 'observed_nonconfirming';
  const candidates: ScoredNarrative['candidates'] = [];
  const contexts = available.flatMap((s) =>
    s.extraction.topics
      .filter(
        (t) =>
          subjectKey(t.subjectQuote, policy).key === group.subject.key &&
          normalized(t.catalystQuote) === group.catalystKey
      )
      .map((t) => ({
        text: t.contextQuote,
        support: t.stance === 'support',
        ambiguous: s.extraction.topics.some(
          (other) =>
            normalized(other.catalystQuote) !== group.catalystKey &&
            t.contextQuote.includes(other.catalystQuote)
        ),
      }))
  );
  const catalyst = times.length === 0 ? group.startsAt.getTime() : Math.min(...times);
  for (const token of tokens) {
    if (token.discoveredAt > asOf) throw new Error('Future token evidence');
    const direct = contexts.some((c) => containsMint(c.text, token.mint));
    const supportedDirect = contexts.some(
      (c) => c.support && !c.ambiguous && containsMint(c.text, token.mint)
    );
    const exact = contexts.some((c) => phrase(c.text, token.name));
    const ticker = contexts.some((c) =>
      (c.text.match(/\$[\p{L}\p{N}_]+/gu) ?? []).some(
        (tag) => normalized(tag.slice(1)) === normalized(token.ticker)
      )
    );
    const identity = group.subject.aliases.some((alias) => phrase(token.name, alias));
    if (!direct && !exact && !ticker && !identity) continue;
    const vector = {
      lexical: exact ? 10000 : ticker ? 5000 : 0,
      identity: identity ? 10000 : 0,
      directReference: direct ? 10000 : 0,
      temporal: Math.max(
        0,
        10000 - scaled(Math.abs(token.launchTime.getTime() - catalyst), policy.temporalWindowMs)
      ),
      contradiction: contradicts ? 10000 : 0,
    };
    const { contradiction, ...positive } = policy.linkWeights;
    let score = Math.max(
      0,
      weighted(vector, positive) - Math.floor((vector.contradiction * contradiction) / 10000)
    );
    if (!direct) score = Math.min(score, policy.lexicalOnlyCapBps);
    if (score < policy.inclusionBps) continue;
    const reasons = [
      direct ? 'EXACT_MINT_REFERENCE' : 'LEXICAL_ONLY_NOT_VERIFIED',
      token.launchTime.getTime() <= catalyst ? 'PRE_CATALYST_LAUNCH' : 'POST_CATALYST_LAUNCH',
      'INTERPRETATION_NOT_VERIFIED_FACT',
      'SEMANTIC_VISUAL_MEDIA_UNAVAILABLE',
    ];
    if (group.subject.ambiguous) reasons.push('AMBIGUOUS_SUBJECT_ALIAS');
    if (contexts.some((c) => c.ambiguous && containsMint(c.text, token.mint)))
      reasons.push('AMBIGUOUS_SOURCE_CONTEXT');
    if (tokens.filter((t) => normalized(t.ticker) === normalized(token.ticker)).length > 1)
      reasons.push('TICKER_COLLISION');
    if (contradicts) reasons.push('CONTRADICTORY_SOURCE_EVIDENCE');
    candidates.push({
      tokenMint: token.mint,
      linkConfidenceBps: score,
      status: contradicts
        ? 'contradictory'
        : status === 'confirmed' && supportedDirect && score >= policy.alertBps
          ? 'confirmed'
          : 'pending',
      reasonCodes: reasons,
      features: vector,
      launchRelation:
        token.launchTime.getTime() <= catalyst ? 'PRE_CATALYST_LAUNCH' : 'POST_CATALYST_LAUNCH',
    });
  }
  candidates.sort(
    (a, b) =>
      b.linkConfidenceBps - a.linkConfidenceBps ||
      (a.tokenMint < b.tokenMint ? -1 : a.tokenMint > b.tokenMint ? 1 : 0)
  );
  return {
    narrativeStrengthBps,
    status,
    independentSources: roots.length,
    features,
    candidates: candidates.length > 32 ? [] : candidates,
    overflow: candidates.length > 32,
  };
}
