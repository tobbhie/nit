import { describe, expect, it } from 'vitest';
import { MediaType, ObservationProvider } from '@nit/domain';
import { SourcePost } from './contracts';
import { normalizeSourcePost } from './normalize';
import { SyntheticObservationSource } from './synthetic-source';

const AUTHOR = 'monitored-account-1';
const INGESTED = new Date('2026-08-19T10:00:00.000Z');

async function fetchAll(
  source: SyntheticObservationSource,
  authorId = AUTHOR,
  limit = 100
): Promise<SourcePost[]> {
  const result = await source.fetchAuthorPosts({ authorId, limit });
  if (result.kind === 'failure') {
    throw result.error;
  }
  return result.value;
}

describe('SyntheticObservationSource', () => {
  it('returns the same corpus on every run and in every instance', async () => {
    // Replay and backtest tests are only meaningful if the fabricated input is
    // reproducible, so this is the property the whole adapter exists to have.
    const first = await fetchAll(new SyntheticObservationSource());
    const second = await fetchAll(new SyntheticObservationSource());
    expect(second).toEqual(first);

    const again = await fetchAll(new SyntheticObservationSource());
    expect(again).toEqual(first);
  });

  it('gives different authors different posts', async () => {
    const source = new SyntheticObservationSource();
    const mine = await fetchAll(source, 'author-a');
    const theirs = await fetchAll(source, 'author-b');
    const overlap = mine.filter((post) =>
      theirs.some((other) => other.providerPostId === post.providerPostId)
    );
    expect(overlap).toEqual([]);
  });

  it('answers newest first, the way a provider does', async () => {
    const posts = await fetchAll(new SyntheticObservationSource());
    const times = posts.map((post) => post.publishedAt.getTime());
    expect(times).toEqual([...times].sort((left, right) => right - left));
  });

  it('produces posts the canonical model accepts', async () => {
    // The fake and the validator have to agree, or every test built on this
    // adapter is testing a shape the real pipeline would reject.
    const posts = await fetchAll(
      new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 40 })
    );
    expect(posts).toHaveLength(40);

    for (const post of posts) {
      const result = normalizeSourcePost(ObservationProvider.X, post, INGESTED);
      if (result.kind === 'failure') {
        throw new Error(`${post.providerPostId} was rejected: ${result.error.message}`);
      }
    }
  });

  it('covers every post shape the pipeline branches on', async () => {
    const posts = await fetchAll(new SyntheticObservationSource());

    expect(posts.some((post) => post.text !== undefined && post.media.length === 0)).toBe(true);
    expect(posts.some((post) => post.text === undefined && post.media.length > 0)).toBe(true);
    expect(posts.some((post) => post.media.length > 1)).toBe(true);
    expect(posts.some((post) => post.quotedProviderPostId !== undefined)).toBe(true);
    expect(
      posts.some((post) => post.media.some((media) => media.mediaType === MediaType.VIDEO))
    ).toBe(true);
    // No post is both textless and attachment-less: an observation carrying
    // nothing at all is not a shape any provider produces.
    expect(posts.some((post) => post.text === undefined && post.media.length === 0)).toBe(false);
  });

  it('only quotes posts that are in the corpus', async () => {
    const posts = await fetchAll(new SyntheticObservationSource());
    const ids = new Set(posts.map((post) => post.providerPostId));

    for (const post of posts) {
      if (post.quotedProviderPostId !== undefined) {
        expect(ids.has(post.quotedProviderPostId)).toBe(true);
        expect(post.quotedProviderPostId).not.toBe(post.providerPostId);
      }
    }
  });

  it('marks every post as fabricated', async () => {
    const posts = await fetchAll(new SyntheticObservationSource());
    for (const post of posts) {
      expect(post.raw['synthetic']).toBe(true);
      expect(post.providerPostId.startsWith('synthetic-')).toBe(true);
      for (const media of post.media) {
        expect(media.sourceUrl).toContain('synthetic.invalid');
      }
    }
  });

  it('returns only what follows the cursor', async () => {
    const source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 6 });
    const all = await fetchAll(source);
    // Newest first, so counting back from the end picks the third-oldest post.
    const thirdOldest = all[all.length - 3] as SourcePost;

    const after = await source.fetchAuthorPosts({
      authorId: AUTHOR,
      sinceProviderPostId: thirdOldest.providerPostId,
      limit: 100,
    });
    if (after.kind === 'failure') {
      throw after.error;
    }

    expect(after.value).toHaveLength(3);
    for (const post of after.value) {
      expect(post.publishedAt.getTime()).toBeGreaterThan(thirdOldest.publishedAt.getTime());
    }
  });

  it('takes the oldest unseen posts when a poll is capped', async () => {
    // A cap that returned the newest posts would leave a permanent hole: the
    // cursor would jump past posts that were never ingested.
    const source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 10 });
    const capped = await source.fetchAuthorPosts({ authorId: AUTHOR, limit: 3 });
    if (capped.kind === 'failure') {
      throw capped.error;
    }

    const all = await fetchAll(source);
    const oldestThree = all.slice(-3);
    expect(capped.value).toEqual(oldestThree);
  });

  it('rejects a cursor it did not issue', async () => {
    const source = new SyntheticObservationSource();
    const foreign = await source.fetchAuthorPosts({
      authorId: 'author-a',
      sinceProviderPostId: 'synthetic-x-author-b-3',
      limit: 10,
    });

    expect(foreign.kind).toBe('failure');
    if (foreign.kind === 'failure') {
      expect(foreign.error.message).toContain('Unknown cursor');
    }
  });

  it('filters by publication time when asked', async () => {
    const source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 6 });
    const all = await fetchAll(source);
    const cutoff = (all[all.length - 2] as SourcePost).publishedAt;

    const after = await source.fetchAuthorPosts({ authorId: AUTHOR, since: cutoff, limit: 100 });
    if (after.kind === 'failure') {
      throw after.error;
    }
    expect(after.value).toHaveLength(4);
    for (const post of after.value) {
      expect(post.publishedAt.getTime()).toBeGreaterThan(cutoff.getTime());
    }
  });

  it('fails for exactly as many fetches as the outage covers', async () => {
    const source = new SyntheticObservationSource();
    source.simulateOutage(2);

    const first = await source.fetchAuthorPosts({ authorId: AUTHOR, limit: 10 });
    const second = await source.fetchAuthorPosts({ authorId: AUTHOR, limit: 10 });
    const third = await source.fetchAuthorPosts({ authorId: AUTHOR, limit: 10 });

    expect(first.kind).toBe('failure');
    expect(second.kind).toBe('failure');
    expect(third.kind).toBe('success');
    // Failed attempts count: a caller asserting on retries needs to see them.
    expect(source.fetchCount()).toBe(3);
  });

  it('is a toy implementation, so production cannot start on it', () => {
    const source = new SyntheticObservationSource();
    expect(source.isToyImplementation).toBe(true);
    expect(source.provider).toBe(ObservationProvider.X);
    expect(new SyntheticObservationSource(ObservationProvider.TELEGRAM).provider).toBe(
      ObservationProvider.TELEGRAM
    );
  });
});
