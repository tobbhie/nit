import { describe, expect, it } from 'vitest';
import { MediaType, ObservationProvider, ValidationError } from '@nit/domain';
import { INGESTED, PUBLISHED, PostOverrides, post } from './in-memory.fixtures';
import { normalizeSourcePost } from './normalize';

function normalize(overrides: PostOverrides = {}): ReturnType<typeof normalizeSourcePost> {
  return normalizeSourcePost(ObservationProvider.X, post(overrides), INGESTED);
}

function expectRejected(overrides: PostOverrides): ValidationError {
  const result = normalize(overrides);
  if (result.kind !== 'failure') {
    throw new Error('Expected normalization to be rejected');
  }
  return result.error;
}

describe('normalizeSourcePost', () => {
  it('produces a canonical observation with a fresh identifier', () => {
    const result = normalize({ text: 'gm' });
    if (result.kind === 'failure') {
      throw result.error;
    }

    expect(result.value.provider).toBe(ObservationProvider.X);
    expect(result.value.providerPostId).toBe('post-1');
    expect(result.value.authorId).toBe('author-1');
    expect(result.value.publishedAt).toEqual(PUBLISHED);
    expect(result.value.ingestedAt).toEqual(INGESTED);
    expect(result.value.text).toBe('gm');
    // v7, so the primary key sorts chronologically.
    expect(result.value.observationId[14]).toBe('7');
  });

  it('does not reuse an identifier across calls', () => {
    const first = normalize();
    const second = normalize();
    if (first.kind === 'failure' || second.kind === 'failure') {
      throw new Error('Expected both to normalize');
    }
    expect(first.value.observationId).not.toBe(second.value.observationId);
  });

  it('keeps the provider payload verbatim', () => {
    const result = normalize();
    if (result.kind === 'failure') {
      throw result.error;
    }
    expect(result.value.rawPayload).toEqual({ id: 'post-1', source: 'fixture' });
  });

  it('trims text and treats whitespace-only text as absent', () => {
    const trimmed = normalize({ text: '  gm  ' });
    const blank = normalize({ text: '   ' });
    if (trimmed.kind === 'failure' || blank.kind === 'failure') {
      throw new Error('Expected both to normalize');
    }
    expect(trimmed.value.text).toBe('gm');
    // A post with only an image is normal; text must be absent, not empty.
    expect('text' in blank.value).toBe(false);
  });

  it('rejects a post with no provider id or no author', () => {
    expect(expectRejected({ providerPostId: '   ' })).toBeInstanceOf(ValidationError);
    expect(expectRejected({ authorId: '' })).toBeInstanceOf(ValidationError);
  });

  it('rejects an unparseable publication time', () => {
    // A NaN date would be persisted as an unusable timestamp and silently break
    // every query that orders by publication.
    const error = expectRejected({ publishedAt: new Date('not a date') });
    expect(error.message).toContain('publishedAt');
  });

  it('rejects a post that quotes itself', () => {
    const error = expectRejected({ providerPostId: 'post-1', quotedProviderPostId: 'post-1' });
    expect(error.message).toContain('quotes itself');
  });

  it('carries the quoted post through as a provider id', () => {
    const result = normalize({ quotedProviderPostId: 'post-0' });
    if (result.kind === 'failure') {
      throw result.error;
    }
    expect(result.value.quotedProviderPostId).toBe('post-0');
  });

  it('assigns every media attachment its own identifier', () => {
    const result = normalize({
      media: [
        { providerMediaKey: 'm1', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/1' },
        {
          providerMediaKey: 'm2',
          mediaType: MediaType.VIDEO,
          sourceUrl: 'https://x.invalid/2',
          durationMs: 4200,
        },
      ],
    });
    if (result.kind === 'failure') {
      throw result.error;
    }

    expect(result.value.media).toHaveLength(2);
    expect(result.value.media[0]?.mediaId).not.toBe(result.value.media[1]?.mediaId);
    expect(result.value.media[1]?.durationMs).toBe(4200);
    // Only video carried a duration; the image must not have invented one.
    expect('durationMs' in (result.value.media[0] as object)).toBe(false);
  });

  it('rejects two attachments sharing one provider key', () => {
    // The media unique constraint would absorb the second one silently.
    const error = expectRejected({
      media: [
        { providerMediaKey: 'm1', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/1' },
        { providerMediaKey: 'm1', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/2' },
      ],
    });
    expect(error.message).toContain('Duplicate media key');
  });

  it('rejects a media type the domain does not define', () => {
    // The static type says MediaType, but the value crosses a JSON boundary in a
    // job payload before it gets here.
    const error = expectRejected({
      media: [
        {
          providerMediaKey: 'm1',
          mediaType: 'gif' as MediaType,
          sourceUrl: 'https://x.invalid/1',
        },
      ],
    });
    expect(error.message).toContain('Unknown media type');
  });

  it('rejects media with no location or an impossible duration', () => {
    expect(
      expectRejected({
        media: [{ providerMediaKey: 'm1', mediaType: MediaType.IMAGE, sourceUrl: ' ' }],
      })
    ).toBeInstanceOf(ValidationError);

    expect(
      expectRejected({
        media: [
          {
            providerMediaKey: 'm1',
            mediaType: MediaType.VIDEO,
            sourceUrl: 'https://x.invalid/1',
            durationMs: -1,
          },
        ],
      }).message
    ).toContain('duration');
  });
});
