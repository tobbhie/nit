import { describe, expect, it } from 'vitest';
import { CatalogTextInterpreter } from './interpreter';
import { PolicySchema, validateExtraction } from './contracts';
import { groupSources, scoreGroup } from './scoring';
import { MINT_A, NOW, configuration, input, universe } from './narrative.fixtures';

describe('deterministic text narrative qualification', () => {
  it('keeps competing mints, separate narrative/link scores and pre-catalyst candidates', () => {
    const p = configuration().policy;
    const group = groupSources([input(), input('b')], p, NOW)[0]!;
    const result = scoreGroup(group, universe(), p, NOW, true);
    expect(result.status).toBe('confirmed');
    expect(result.independentSources).toBe(2);
    expect(result.candidates).toHaveLength(2);
    expect(
      result.candidates.every(
        (c) => c.status === 'confirmed' && c.launchRelation === 'PRE_CATALYST_LAUNCH'
      )
    ).toBe(true);
    expect(result.candidates[0]?.reasonCodes).toContain('TICKER_COLLISION');
  });
  it('does not count repeat authors, copied content, derivatives or old material as new corroboration', () => {
    for (const second of [
      input('a', 'different'),
      input('b', 'a'),
      { ...input('b'), novel: false },
      { ...input('b'), source: { ...input('b').source, derivativeIds: ['origin'] } },
    ]) {
      const p = configuration().policy;
      const group = groupSources([input(), second], p, NOW)[0]!;
      expect(scoreGroup(group, universe(), p, NOW, true).independentSources).toBe(1);
    }
  });
  it('never promotes lexical identity alone, even with maximum lexical weights', () => {
    const p = configuration().policy;
    p.linkWeights = {
      lexical: 10000,
      identity: 0,
      directReference: 0,
      temporal: 0,
      contradiction: 10000,
    };
    const sources = [input(), input('b')].map((s) => {
      const text = s.source.text
        .replaceAll(MINT_A, '')
        .replaceAll('33333333333333333333333333333333', '');
      return {
        ...s,
        source: { ...s.source, text },
        extraction: {
          ...s.extraction,
          topics: s.extraction.topics.map((t) => ({ ...t, contextQuote: text })),
        },
      };
    });
    const result = scoreGroup(groupSources(sources, p, NOW)[0]!, universe(), p, NOW, true);
    expect(result.narrativeStrengthBps).toBe(10000);
    expect(result.candidates[0]?.linkConfidenceBps).toBe(4000);
    expect(result.candidates.every((c) => c.status === 'pending')).toBe(true);
  });
  it('keeps same-subject different events apart and category changes inside one stable episode', () => {
    const p = configuration().policy;
    const a = input();
    const different = input('b');
    different.source.text += ' Nova festival cancelled';
    different.extraction.topics[0]!.catalystQuote = 'Nova festival cancelled';
    different.extraction.topics[0]!.contextQuote = different.source.text;
    expect(groupSources([a, different], p, NOW)).toHaveLength(2);
    const recategorized = input('b');
    recategorized.extraction.topics[0]!.category = 'news';
    expect(groupSources([a, recategorized], p, NOW)).toHaveLength(1);
  });
  it('preserves contradiction and marks incomplete evidence unavailable', () => {
    const p = configuration().policy;
    const other = input('b');
    other.extraction.topics[0]!.stance = 'contradiction';
    const group = groupSources([input(), other], p, NOW)[0]!;
    expect(scoreGroup(group, universe(), p, NOW, true).status).toBe('contradictory');
    expect(scoreGroup(group, universe(), p, NOW, false).status).toBe('unavailable');
  });
  it('does not borrow a mint reference from another topic in the same post', () => {
    const p = configuration().policy;
    const sources = [input(), input('b')].map((s) => ({
      ...s,
      extraction: {
        ...s.extraction,
        topics: s.extraction.topics.map((t) => ({ ...t, contextQuote: 'Nova festival begins' })),
      },
    }));
    const result = scoreGroup(groupSources(sources, p, NOW)[0]!, universe(), p, NOW, true);
    expect(result.candidates.every((c) => c.status === 'pending')).toBe(true);
  });
  it('reports overflow instead of silently selecting the first 32 tokens', () => {
    const p = configuration().policy;
    const tokens = Array.from({ length: 33 }, (_, i) => ({ ...universe()[0]!, mint: `mint-${i}` }));
    const result = scoreGroup(
      groupSources([input(), input('b')], p, NOW)[0]!,
      tokens,
      p,
      NOW,
      true
    );
    expect(result.overflow).toBe(true);
    expect(result.candidates).toEqual([]);
  });
  it('rejects future evidence and invented excerpt/model score fields', () => {
    const p = configuration().policy;
    expect(() => groupSources([input()], p, new Date(NOW.getTime() - 1))).toThrow('Future');
    expect(() =>
      validateExtraction(
        {
          schemaVersion: 1,
          topics: [
            {
              subjectQuote: 'Nova',
              catalystQuote: 'invented',
              contextQuote: input().source.text,
              category: 'meme',
              stance: 'support',
            },
          ],
        },
        input().source
      )
    ).toThrow();
    expect(() =>
      validateExtraction({ ...input().extraction, score: 10000 }, input().source)
    ).toThrow();
    expect(() => PolicySchema.parse({ ...p, lexicalOnlyCapBps: 10000 })).toThrow();
  });
  it('literal catalog mode only surfaces uncertainty and refuses toy-like semantic claims', async () => {
    const p = configuration().policy;
    const interpreter = new CatalogTextInterpreter(p);
    const parsed = validateExtraction(
      await interpreter.interpret(input().source, new AbortController().signal),
      input().source
    );
    expect(parsed.topics[0]?.stance).toBe('uncertain');
    expect(interpreter.identity.isToyImplementation).toBe(false);
  });
});
