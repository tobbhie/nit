import { afterAll, beforeAll, beforeEach, describe, it, expect } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { semanticGroups, topicId } from './semantic';
import { richConfiguration, richInput } from './rich.fixtures';
import { NOW } from './narrative.fixtures';
import { scoreGroup } from './scoring';

describe.skipIf(!hasTestDatabase())('immutable online semantic graph', () => {
  let harness: TestDatabase;
  beforeAll(async () => {
    harness = await createTestDatabase('semantic_graph');
  }, 30000);
  afterAll(async () => {
    await harness.teardown();
  });
  beforeEach(async () => {
    const r = await harness.db.query('TRUNCATE nit_semantic_members,nit_semantic_nodes');
    if (r.kind === 'failure') throw r.error;
  });
  it('converges paraphrases and reuses immutable anchor memberships after late source arrival', async () => {
    const policy = richConfiguration().policy;
    const a = richInput('a');
    const first = await semanticGroups(harness.db, [a], policy, NOW);
    const b = richInput('b', 'Nova party opens with music', [0.999, 0.01]);
    const later = await semanticGroups(harness.db, [b, a], policy, new Date(NOW.getTime() + 1));
    expect(later).toHaveLength(1);
    expect(later[0]!.key).toBe(first[0]!.key);
    expect(later[0]!.sources).toHaveLength(2);
    expect(later[0]!.memberIds).toContain(topicId(b, b.extraction.topics[0]!));
    expect(scoreGroup(later[0]!, [], policy, NOW, true).independentSources).toBe(2);
    const members = await harness.db.query<{ relation: string }>(
      'SELECT relation FROM nit_semantic_members ORDER BY relation'
    );
    if (members.kind === 'failure') throw members.error;
    expect(members.value.map((m) => m.relation)).toEqual(['created', 'merged']);
    expect((await harness.db.query('UPDATE nit_semantic_members SET relation=relation')).kind).toBe(
      'failure'
    );
    expect(
      (await semanticGroups(harness.db, [a, b], policy, new Date(NOW.getTime() + 2)))[0]!.key
    ).toBe(first[0]!.key);
  });
  it('keeps diverging claims separate and records the split without changing its earlier parent', async () => {
    const policy = richConfiguration().policy;
    const a = richInput();
    const first = await semanticGroups(harness.db, [a], policy, NOW);
    const b = richInput('b', 'Nova party opens after relocation', [0, 1]);
    b.extraction.topics[0]!.meaning!.event = 'Nova relocation';
    const groups = await semanticGroups(harness.db, [a, b], policy, new Date(NOW.getTime() + 1));
    expect(groups).toHaveLength(2);
    expect(groups.some((g) => g.key === first[0]!.key)).toBe(true);
    const r = await harness.db.query(
      "SELECT 1 FROM nit_semantic_members WHERE relation='split' AND jsonb_array_length(compared)=1"
    );
    expect(r.kind === 'success' && r.value.length).toBe(1);
  });
  it('preserves negated claims inside the same event instead of treating disagreement as corroboration', async () => {
    const policy = richConfiguration().policy;
    const a = richInput();
    const b = richInput('b', 'Nova party opens is false');
    b.extraction.topics[0]!.stance = 'contradiction';
    const groups = await semanticGroups(harness.db, [a, b], policy, NOW);
    expect(groups).toHaveLength(1);
    expect(scoreGroup(groups[0]!, [], policy, NOW, true).status).toBe('contradictory');
  });
  it('starts a new linked attention episode after an older episode while retaining the old membership', async () => {
    const policy = richConfiguration().policy;
    policy.temporalWindowMs = 86400000;
    const a = richInput();
    const old = (await semanticGroups(harness.db, [a], policy, NOW))[0]!;
    const later = new Date(NOW.getTime() + 3 * 3600000);
    const b = richInput('b', 'Nova party opens again');
    b.source.publishedAt = later;
    b.source.ingestedAt = later;
    const groups = await semanticGroups(harness.db, [b], policy, later);
    expect(groups[0]!.key).not.toBe(old.key);
    expect(groups[0]!.previousGroupKey).toBe(old.key);
    const r = await harness.db.query("SELECT 1 FROM nit_semantic_members WHERE relation='revival'");
    expect(r.kind === 'success' && r.value.length).toBe(1);
  });
  it('marks conflicting catalog identities ambiguous rather than merging through a model label', async () => {
    const policy = richConfiguration().policy;
    policy.subjects.push({ id: 'other', label: 'Other', aliases: ['Other'] });
    const a = richInput();
    a.extraction.topics[0]!.meaning!.subject = 'Other';
    const groups = await semanticGroups(harness.db, [a], policy, NOW);
    expect(groups[0]!.subject.ambiguous).toBe(true);
    expect(scoreGroup(groups[0]!, [], policy, NOW, true).status).toBe('pending');
  });
  it('refuses a model/dimension mismatch and earlier-time rewrites', async () => {
    const policy = richConfiguration().policy;
    const a = richInput();
    a.extraction.topics[0]!.meaning!.modelVersion = 'wrong';
    await expect(semanticGroups(harness.db, [a], policy, NOW)).rejects.toThrow('model/dimension');
    a.extraction.topics[0]!.meaning!.modelVersion = 'fixture-embedding-v1';
    await semanticGroups(harness.db, [a], policy, new Date(NOW.getTime() + 1000));
    await expect(semanticGroups(harness.db, [a], policy, NOW)).rejects.toThrow(
      'future semantic membership'
    );
  });
});
