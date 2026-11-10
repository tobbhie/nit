import { describe, it, expect, vi } from 'vitest';
import { validateExtraction, RICH_PROMPT_HASH, PolicySchema } from './contracts';
import { GatewayTextInterpreter } from './interpreter';
import { groupSources, scoreGroup } from './scoring';
import { cosineBps } from './semantic';
import { NOW, MINT_A, universe } from './narrative.fixtures';
import { richConfiguration, richInput, withMedia, mediaId, tokenExtraction } from './rich.fixtures';

describe('grounded multimodal interpretation and deterministic matching', () => {
  it('accepts cited OCR and transcript segments and preserves contradictory modalities', () => {
    const value = withMedia();
    value.extraction.mediaEvidence = [
      {
        id: 'ocr-1',
        mediaId,
        sha256: 'a'.repeat(64),
        modality: 'ocr',
        text: 'Nova festival begins',
        startMs: null,
        endMs: null,
      },
      {
        id: 'audio-1',
        mediaId,
        sha256: 'a'.repeat(64),
        modality: 'transcript',
        text: 'Nova festival begins',
        startMs: 0,
        endMs: 1000,
      },
    ];
    value.extraction.topics = [
      ...value.extraction.topics.map((t) => ({
        ...t,
        evidenceId: 'ocr-1',
        contextQuote: 'Nova festival begins',
      })),
      {
        ...value.extraction.topics[0]!,
        evidenceId: 'audio-1',
        contextQuote: 'Nova festival begins',
        stance: 'contradiction',
      },
    ];
    const parsed = validateExtraction(value.extraction, value.source);
    expect(parsed.topics.map((t) => t.stance)).toEqual(['support', 'contradiction']);
  });
  it('rejects invented excerpts, unknown media, forged hashes and impossible transcript times', () => {
    for (const change of [
      { sha256: 'f'.repeat(64) },
      { mediaId: '20000000-0000-4000-8000-000000000001' },
      { startMs: 1000, endMs: 1001 },
      { endMs: 2000 },
    ]) {
      const value = withMedia();
      value.extraction.mediaEvidence = [
        {
          id: 'audio',
          mediaId,
          sha256: 'a'.repeat(64),
          modality: 'transcript',
          text: 'Nova festival begins',
          startMs: 0,
          endMs: 1000,
          ...change,
        },
      ];
      expect(() => validateExtraction(value.extraction, value.source)).toThrow();
    }
    const value = withMedia();
    value.extraction.topics[0]!.evidenceId = 'missing';
    expect(() => validateExtraction(value.extraction, value.source)).toThrow('citation');
  });
  it('does not permit rich fields to masquerade as legacy text output', () => {
    const value = richInput();
    value.extraction.schemaVersion = 1;
    expect(() => validateExtraction(value.extraction, value.source)).toThrow('version 2');
  });
  it('gives semantic-only candidates watch status and retains competing mints', () => {
    const policy = richConfiguration().policy;
    policy.inclusionBps = 500;
    const a = richInput('a', 'Nova festival begins for all');
    const b = richInput('b', 'Nova festival begins with music');
    const group = groupSources([a, b], policy, NOW)[0]!;
    const tokens = universe().map((t) => ({
      ...t,
      name: 'Unrelated name',
      ticker: 'OTHER',
      representation: { modelVersion: 'fixture-embedding-v1', vector: [1, 0] },
    }));
    const score = scoreGroup(group, tokens, policy, NOW, true);
    expect(score.candidates).toHaveLength(2);
    expect(
      score.candidates.every((c) => c.status === 'pending' && c.linkConfidenceBps < policy.alertBps)
    ).toBe(true);
    expect(
      score.candidates.every((c) =>
        c.reasonCodes.includes('SEMANTIC_SIMILARITY_NOT_IDENTITY_PROOF')
      )
    ).toBe(true);
  });
  it('uses source-frame/artwork identity independently of token name and preserves copies as competitors', () => {
    const policy = richConfiguration().policy;
    policy.linkWeights = {
      lexical: 0,
      identity: 0,
      directReference: 0,
      temporal: 0,
      visual: 10000,
      contradiction: 10000,
    };
    const a = withMedia(richInput('a', 'Nova festival begins for all'));
    const b = richInput('b', 'Nova festival begins with music');
    const tokens = universe().map((t) => ({
      ...t,
      name: 'Unknown token',
      ticker: 'OTHER',
      mediaRepresentations: a.extraction.mediaRepresentations!,
    }));
    const score = scoreGroup(groupSources([a, b], policy, NOW)[0]!, tokens, policy, NOW, true);
    expect(score.candidates).toHaveLength(2);
    expect(
      score.candidates.every((c) => c.status === 'confirmed' && c.features.visual === 10000)
    ).toBe(true);
    expect(score.candidates[0]!.reasonCodes).toContain(
      'EXACT_SOURCE_ARTWORK_MATCH_NOT_ENDORSEMENT'
    );
  });
  it('perceptual similarity and OCR containing a mint cannot claim exact identity or raw mint support', () => {
    const policy = richConfiguration().policy;
    policy.inclusionBps = 500;
    policy.linkWeights = {
      lexical: 0,
      identity: 0,
      directReference: 0,
      temporal: 0,
      visual: 10000,
      contradiction: 10000,
    };
    const a = withMedia(richInput('a', `Nova festival begins for all`));
    a.extraction.mediaEvidence = [
      {
        id: 'ocr',
        mediaId,
        sha256: 'a'.repeat(64),
        modality: 'ocr',
        text: `Nova festival begins ${MINT_A}`,
        startMs: null,
        endMs: null,
      },
    ];
    a.extraction.topics[0]!.evidenceId = 'ocr';
    a.extraction.topics[0]!.contextQuote = a.extraction.mediaEvidence[0]!.text;
    const b = richInput('b', 'Nova festival begins with music');
    const token = {
      ...universe()[0]!,
      mediaRepresentations: [
        {
          ...a.extraction.mediaRepresentations![0]!,
          sha256: 'c'.repeat(64),
          frameHashes: ['d'.repeat(64)],
        },
      ],
    };
    const score = scoreGroup(groupSources([a, b], policy, NOW)[0]!, [token], policy, NOW, true);
    expect(score.candidates[0]!.features.directReference).toBe(0);
    expect(score.candidates[0]!.status).toBe('pending');
    expect(score.candidates[0]!.linkConfidenceBps).toBeLessThan(policy.alertBps);
  });
  it('shared image bytes do not multiply independent source support', () => {
    const policy = richConfiguration().policy;
    const sources = [withMedia(richInput('a')), withMedia(richInput('b'))];
    expect(
      scoreGroup(groupSources(sources, policy, NOW)[0]!, universe(), policy, NOW, true)
        .independentSources
    ).toBe(1);
  });
  it('semantic and visual evidence from a different topic cannot lend another topic its match', () => {
    const policy = richConfiguration().policy;
    const source = withMedia(richInput());
    source.extraction.mediaEvidence = [
      {
        id: 'other',
        mediaId,
        sha256: 'a'.repeat(64),
        modality: 'visual',
        text: 'Other story starts',
        startMs: null,
        endMs: null,
      },
    ];
    source.extraction.topics.push({
      subjectQuote: 'Other',
      catalystQuote: 'Other story starts',
      contextQuote: 'Other story starts',
      evidenceId: 'other',
      category: 'news',
      stance: 'support',
      meaning: {
        subject: 'Other',
        event: 'Other story',
        modelVersion: 'fixture-embedding-v1',
        vector: [0, 1],
      },
    });
    const group = groupSources([source], policy, NOW).find((g) => g.subject.key === 'nova')!;
    const token = {
      ...universe()[0]!,
      mediaRepresentations: source.extraction.mediaRepresentations!,
    };
    expect(scoreGroup(group, [token], policy, NOW, true).candidates[0]!.features.visual).toBe(0);
  });
  it('checks cosine dimensions and keeps opposite meanings separate', () => {
    expect(cosineBps([1, 0], [1, 0])).toBe(10000);
    expect(cosineBps([1, 0], [-1, 0])).toBe(0);
    expect(cosineBps([1, 0], [1])).toBe(0);
  });
  it('rejects invalid expanded scoring weight totals', () => {
    const policy = richConfiguration().policy;
    policy.linkWeights.visual = 10000;
    expect(() => PolicySchema.parse(policy)).toThrow();
  });
  it('requires real embedding identities and keeps server paths out of the request', async () => {
    const options = {
      endpoint: 'https://gateway.invalid/text',
      allowedOrigin: 'https://gateway.invalid',
      modelVersion: 'fixture-rich',
      bearer: 'test-only',
      maxInputBytes: 32000,
      maxOutputTokens: 1000,
      maxChargeMicros: 1,
      rich: { modelVersion: 'fixture-embedding-v1', dimensions: 2 },
    };
    const fetcher = vi.fn((_url: unknown, init?: RequestInit) => {
      const body = JSON.parse(init!.body as string) as Record<string, unknown>;
      expect(body['embedding']).toEqual({ modelVersion: 'fixture-embedding-v1', dimensions: 2 });
      expect(body['purpose']).toBe('token_representation');
      expect(JSON.stringify(body)).not.toContain('scratchRoot');
      return Promise.resolve(
        new Response(
          JSON.stringify({
            modelVersion: 'fixture-rich',
            promptHash: RICH_PROMPT_HASH,
            extraction: tokenExtraction(),
          })
        )
      );
    });
    const source = richInput().source;
    source.purpose = 'token_representation';
    expect(
      validateExtraction(
        await new GatewayTextInterpreter({ ...options, fetcher }).interpret(
          source,
          new AbortController().signal
        ),
        source
      ).representation!.modelVersion
    ).toBe('fixture-embedding-v1');
    await expect(
      new GatewayTextInterpreter({
        ...options,
        rich: { ...options.rich, dimensions: 3 },
        fetcher,
      }).interpret(source, new AbortController().signal)
    ).rejects.toThrow('unavailable or invalid');
  });
});
