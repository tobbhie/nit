import { evidenceHash } from '@nit/signals';
import {
  NarrativePolicy,
  SourceText,
  TextExtraction,
  normalized,
  phrase,
  subjectKey,
  MediaRepresentationSchema,
} from './contracts';
import { z } from 'zod';

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
  memberIds?: string[];
  previousGroupKey?: string | null;
}
export interface TokenFacts {
  mint: string;
  name: string;
  ticker: string;
  launchTime: Date;
  discoveredAt: Date;
  representation?: TextExtraction['representation'];
  mediaRepresentations?: z.infer<typeof MediaRepresentationSchema>[];
  enrichmentEvidence?: {
    tokenId: string;
    extractorHash: string;
    inputHash: string;
    availableAt: string;
    payloadHash: string;
  };
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
      semantic: number;
      visual: number;
      ocr: number;
      transcript: number;
    };
    launchRelation: 'PRE_CATALYST_LAUNCH' | 'POST_CATALYST_LAUNCH';
    enrichmentEvidence?: TokenFacts['enrichmentEvidence'];
  }[];
  overflow: boolean;
}
export function topicInGroup(
  input: InterpretedSource,
  topic: TextExtraction['topics'][number],
  group: NarrativeGroup,
  policy: NarrativePolicy
): boolean {
  return group.memberIds === undefined
    ? subjectKey(topic.subjectQuote, policy).key === group.subject.key &&
        normalized(topic.catalystQuote) === group.catalystKey
    : group.memberIds.includes(evidenceHash({ artifact: input.artifactId, topic }));
}
function similarity(a: readonly number[], b: readonly number[]): number {
  if (a.length !== b.length) return 0;
  const norm = Math.sqrt(a.reduce((n, v) => n + v * v, 0) * b.reduce((n, v) => n + v * v, 0));
  return norm === 0
    ? 0
    : Math.max(
        0,
        Math.min(10000, Math.round((a.reduce((n, v, i) => n + v * b[i]!, 0) / norm) * 10000))
      );
}
function dhashSimilarity(a: string, b: string): number {
  let bits = BigInt(`0x${a}`) ^ BigInt(`0x${b}`);
  let count = 0;
  while (bits !== 0n) {
    count++;
    bits &= bits - 1n;
  }
  return Math.floor(((64 - count) * 10000) / 64);
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
      `content:${evidenceHash(
        normalized(
          item.source.text.trim() === ''
            ? (item.extraction.mediaRepresentations ?? [])
                .map((m) => m.sha256)
                .sort()
                .join(' ')
            : item.source.text
        )
      )}`,
      ...(item.extraction.mediaRepresentations ?? []).flatMap((m) =>
        [m.sha256, ...m.frameHashes].map((h) => `media:${h}`)
      ),
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
function weighted(
  features: Record<string, number>,
  weights: Record<string, number | undefined>
): number {
  return Math.floor(
    Object.entries(weights).reduce(
      (sum, [key, weight]) => sum + (features[key] ?? 0) * (weight ?? 0),
      0
    ) / 10000
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
    item.extraction.topics.some((t) => topicInGroup(item, t, group, policy) && t.stance === value);
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
      .filter((t) => topicInGroup(s, t, group, policy))
      .map((t) => ({
        text: t.contextQuote,
        modality:
          t.evidenceId === undefined
            ? 'text'
            : s.extraction.mediaEvidence?.find((e) => e.id === t.evidenceId)?.modality,
        meaning: t.meaning,
        media: (s.extraction.mediaRepresentations ?? []).filter((m) =>
          t.evidenceId === undefined
            ? s.extraction.topics.length === 1
            : s.extraction.mediaEvidence?.some(
                (e) => e.id === t.evidenceId && e.mediaId === m.mediaId
              )
        ),
        support: t.stance === 'support',
        ambiguous: s.extraction.topics.some(
          (other) =>
            !topicInGroup(s, other, group, policy) && t.contextQuote.includes(other.catalystQuote)
        ),
      }))
  );
  const catalyst = times.length === 0 ? group.startsAt.getTime() : Math.min(...times);
  for (const token of tokens) {
    if (token.discoveredAt > asOf) throw new Error('Future token evidence');
    const direct = contexts.some((c) => c.modality === 'text' && containsMint(c.text, token.mint));
    const supportedDirect = contexts.some(
      (c) => c.modality === 'text' && c.support && !c.ambiguous && containsMint(c.text, token.mint)
    );
    const exact = contexts.some((c) => phrase(c.text, token.name));
    const ticker = contexts.some((c) =>
      (c.text.match(/\$[\p{L}\p{N}_]+/gu) ?? []).some(
        (tag) => normalized(tag.slice(1)) === normalized(token.ticker)
      )
    );
    const identity = group.subject.aliases.some((alias) => phrase(token.name, alias));
    const sourceMedia = contexts.flatMap((c) => c.media);
    const tokenMedia = token.mediaRepresentations ?? [];
    const samePixels = (
      a: z.infer<typeof MediaRepresentationSchema>,
      b: z.infer<typeof MediaRepresentationSchema>
    ): boolean => a.sha256 === b.sha256 || a.frameHashes.some((h) => b.frameHashes.includes(h));
    const exactVisual = sourceMedia.some((a) => tokenMedia.some((b) => samePixels(a, b)));
    const supportedVisual = contexts.some(
      (c) =>
        c.support && !c.ambiguous && c.media.some((a) => tokenMedia.some((b) => samePixels(a, b)))
    );
    const visual = exactVisual
      ? 10000
      : Math.max(
          0,
          ...sourceMedia.flatMap((a) =>
            a.dhashes.flatMap((h) =>
              tokenMedia.flatMap((b) =>
                b.decoderVersion === a.decoderVersion
                  ? b.dhashes.map((j) => dhashSimilarity(h, j))
                  : []
              )
            )
          )
        );
    const sem = policy.semantics;
    const semantic =
      token.representation?.modelVersion === sem?.modelVersion
        ? Math.max(
            0,
            ...contexts.map((c) =>
              c.meaning?.modelVersion === sem?.modelVersion && c.meaning !== undefined
                ? similarity(c.meaning.vector, token.representation!.vector)
                : 0
            )
          )
        : 0;
    const retrievedSemantic = sem !== undefined && semantic >= sem.retrievalBps;
    const retrievedVisual = sem !== undefined && visual >= sem.visualMatchBps;
    if (
      !direct &&
      !exact &&
      !ticker &&
      !identity &&
      !retrievedSemantic &&
      !exactVisual &&
      !retrievedVisual
    )
      continue;
    const vector = {
      lexical: exact ? 10000 : ticker ? 5000 : 0,
      identity: identity ? 10000 : 0,
      directReference: direct ? 10000 : 0,
      temporal: Math.max(
        0,
        10000 - scaled(Math.abs(token.launchTime.getTime() - catalyst), policy.temporalWindowMs)
      ),
      contradiction: contradicts ? 10000 : 0,
      semantic,
      visual,
      ocr: contexts.some(
        (c) =>
          c.modality === 'ocr' && (phrase(c.text, token.name) || containsMint(c.text, token.mint))
      )
        ? 10000
        : 0,
      transcript: contexts.some((c) => c.modality === 'transcript' && phrase(c.text, token.name))
        ? 10000
        : 0,
    };
    const { contradiction, ...positive } = policy.linkWeights;
    let score = Math.max(
      0,
      weighted(vector, positive) - Math.floor((vector.contradiction * contradiction) / 10000)
    );
    if (!direct && !exactVisual) score = Math.min(score, policy.lexicalOnlyCapBps);
    if (score < policy.inclusionBps) continue;
    const reasons = [
      direct ? 'EXACT_MINT_REFERENCE' : 'LEXICAL_ONLY_NOT_VERIFIED',
      token.launchTime.getTime() <= catalyst ? 'PRE_CATALYST_LAUNCH' : 'POST_CATALYST_LAUNCH',
      'INTERPRETATION_NOT_VERIFIED_FACT',
      ...(sem === undefined
        ? ['SEMANTIC_VISUAL_MEDIA_UNAVAILABLE']
        : ['VERSIONED_SEMANTIC_EVIDENCE', 'MEDIA_INTERPRETATION_NOT_VERIFIED_FACT']),
    ];
    if (exactVisual) reasons.push('EXACT_SOURCE_ARTWORK_MATCH_NOT_ENDORSEMENT');
    else if (retrievedVisual) reasons.push('PERCEPTUAL_SIMILARITY_NOT_IDENTITY_PROOF');
    if (retrievedSemantic) reasons.push('SEMANTIC_SIMILARITY_NOT_IDENTITY_PROOF');
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
        : status === 'confirmed' && (supportedDirect || supportedVisual) && score >= policy.alertBps
          ? 'confirmed'
          : 'pending',
      reasonCodes: reasons,
      features: vector,
      ...(token.enrichmentEvidence === undefined
        ? {}
        : { enrichmentEvidence: token.enrichmentEvidence }),
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
