import { NarrativeRuntimeConfig } from './config';
import { InterpretedSource, TokenFacts } from './scoring';
import { TextExtraction } from './contracts';

export const NOW = new Date('2026-11-09T10:00:00.000Z');
export const MINT_A = '22222222222222222222222222222222';
export const MINT_B = '33333333333333333333333333333333';
export function configuration(): NarrativeRuntimeConfig {
  return {
    policy: {
      version: 'toy-calibration-for-tests-only',
      subjects: [{ id: 'nova', label: 'Nova', aliases: ['Nova'] }],
      sourcePriority: { 'x:a': 10000, 'x:b': 10000 },
      episodeWindowMs: 3600000,
      attentionWindowMs: 3600000,
      watchDurationMs: 3600000,
      temporalWindowMs: 3600000,
      minimumIndependentSources: 2,
      sourceSaturation: 2,
      persistenceSaturationMs: 1000,
      narrativeThresholdBps: 5000,
      inclusionBps: 2000,
      alertBps: 7000,
      lexicalOnlyCapBps: 4000,
      narrativeWeights: { independence: 10000, persistence: 0, priority: 0 },
      linkWeights: {
        lexical: 2000,
        identity: 1000,
        directReference: 6000,
        temporal: 1000,
        contradiction: 10000,
      },
    },
    interpreter: { kind: 'catalog' },
    intervalMs: 1000,
    timeoutMs: 1000,
    observationsPerCycle: 8,
    sourceLimit: 32,
    universeLimit: 100,
    episodeLimit: 16,
    maxRequestsPerDay: 100,
    maxCostMicrosPerDay: 0,
  };
}
export function extraction(
  text: string,
  stance: 'support' | 'contradiction' | 'uncertain' = 'support'
): TextExtraction {
  return {
    schemaVersion: 1,
    topics: [
      {
        subjectQuote: 'Nova',
        catalystQuote: 'Nova festival begins',
        contextQuote: text,
        category: 'meme',
        stance,
      },
    ],
  };
}
export function input(author = 'a', suffix = author): InterpretedSource {
  const text = `Nova festival begins ${MINT_A} ${MINT_B}. Context ${suffix}`;
  return {
    source: {
      observationId: `00000000-0000-4000-8000-${author.padEnd(12, '0')}`,
      provider: 'x',
      postId: author,
      authorId: author,
      text,
      publishedAt: new Date(NOW.getTime() - 1000),
      ingestedAt: NOW,
      derivativeIds: [],
    },
    artifactId: `00000000-0000-4000-8000-${author.padEnd(12, '0')}`,
    extraction: extraction(text),
    novel: true,
  };
}
export function universe(): TokenFacts[] {
  return [MINT_A, MINT_B].map((mint, i) => ({
    mint,
    name: i === 0 ? 'Nova' : 'Nova Echo',
    ticker: 'NOVA',
    launchTime: new Date(NOW.getTime() - 2000),
    discoveredAt: NOW,
  }));
}
