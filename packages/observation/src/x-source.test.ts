import { describe, expect, it, vi, Mock } from 'vitest';
import { XObservationSource } from './x-source';

function fixture(
  pages: unknown[],
  maxPages = 2
): { source: XObservationSource; fetch: Mock<Parameters<typeof fetch>, ReturnType<typeof fetch>> } {
  let page = 0;
  const mock = vi.fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>((): Promise<Response> =>
    Promise.resolve(
      new Response(JSON.stringify(pages[page++]), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    )
  );
  return {
    fetch: mock,
    source: new XObservationSource({
      bearerToken: 'test-only-token',
      requestTimeoutMs: 1000,
      minRequestIntervalMs: 1000,
      maxPages,
      fetch: mock,
    }),
  };
}
const post = (id: string, author = '123'): Record<string, unknown> => ({
  id,
  author_id: author,
  created_at: '2026-11-01T00:00:00.000Z',
  text: 'Untrusted source text',
});
describe('X author capture', () => {
  it('captures photo and playable video attachments through exact media-key expansions', async () => {
    const { source, fetch } = fixture([
      {
        data: [{ ...post('20'), attachments: { media_keys: ['photo', 'clip'] } }],
        meta: {},
        includes: {
          media: [
            { media_key: 'photo', type: 'photo', url: 'https://pbs.twimg.com/test.png' },
            {
              media_key: 'clip',
              type: 'video',
              duration_ms: 1000,
              variants: [
                { content_type: 'application/x-mpegURL', url: 'https://video.twimg.com/test.m3u8' },
                {
                  content_type: 'video/mp4',
                  bit_rate: 1000,
                  url: 'https://video.twimg.com/large.mp4',
                },
                {
                  content_type: 'video/mp4',
                  bit_rate: 100,
                  url: 'https://video.twimg.com/small.mp4',
                },
              ],
            },
          ],
        },
      },
    ]);
    const result = await source.fetchAuthorPosts({ authorId: '123', limit: 10 });
    expect(result.kind).toBe('success');
    if (result.kind === 'success')
      expect(result.value[0]!.media).toEqual([
        {
          providerMediaKey: 'photo',
          mediaType: 'image',
          sourceUrl: 'https://pbs.twimg.com/test.png',
        },
        {
          providerMediaKey: 'clip',
          mediaType: 'video',
          sourceUrl: 'https://video.twimg.com/small.mp4',
          durationMs: 1000,
        },
      ]);
    const url = (fetch.mock.calls[0] as unknown as [URL])[0];
    expect(url.searchParams.get('expansions')).toBe('attachments.media_keys');
    expect(url.searchParams.get('media.fields')).toContain('variants');
  });
  it('does not advance past missing or unplayable media expansions', async () => {
    for (const media of [
      [],
      [
        {
          media_key: 'clip',
          type: 'video',
          variants: [
            { content_type: 'application/x-mpegURL', url: 'https://video.twimg.com/test.m3u8' },
          ],
        },
      ],
    ]) {
      const { source } = fixture([
        {
          data: [{ ...post('20'), attachments: { media_keys: ['clip'] } }],
          meta: {},
          includes: { media },
        },
      ]);
      expect((await source.fetchAuthorPosts({ authorId: '123', limit: 10 })).kind).toBe('failure');
    }
  });
  it('uses exact IDs, drains pagination, deduplicates and returns oldest first', async () => {
    const { source, fetch } = fixture([
      { data: [post('9007199254740995')], meta: { next_token: 'next' } },
      { data: [post('9007199254740994'), post('9007199254740995')], meta: {} },
    ]);
    const result = await source.fetchAuthorPosts({
      authorId: '123',
      sinceProviderPostId: '9007199254740993',
      limit: 10,
    });
    expect(result.kind).toBe('success');
    if (result.kind === 'success')
      expect(result.value.map((item) => item.providerPostId)).toEqual([
        '9007199254740994',
        '9007199254740995',
      ]);
    const first = fetch.mock.calls[0] as unknown as [URL, RequestInit];
    const second = fetch.mock.calls[1] as unknown as [URL, RequestInit];
    expect(first[0].origin).toBe('https://api.x.com');
    expect(first[0].searchParams.get('since_id')).toBe('9007199254740993');
    expect(second[0].searchParams.get('pagination_token')).toBe('next');
    expect(first[1].redirect).toBe('error');
  });
  it('does not return a partial batch when pagination is exhausted', async () => {
    const { source } = fixture([{ data: [post('20')], meta: { next_token: 'more' } }], 1);
    expect((await source.fetchAuthorPosts({ authorId: '123', limit: 10 })).kind).toBe('failure');
  });
  it('rejects partial errors, wrong authors, malformed posts and cursor regressions', async () => {
    for (const page of [
      { data: [post('20')], errors: [{ detail: 'private response' }], meta: {} },
      { data: [post('20', '999')], meta: {} },
      { data: [{ ...post('20'), created_at: 'invalid' }], meta: {} },
      { data: [post('9')], meta: {} },
    ]) {
      const { source } = fixture([page]);
      const result = await source.fetchAuthorPosts({
        authorId: '123',
        sinceProviderPostId: '10',
        limit: 10,
      });
      expect(result.kind).toBe('failure');
      if (result.kind === 'failure') expect(result.error.message).not.toContain('private response');
    }
  });
  it('rejects an oversized backlog without moving the cursor', async () => {
    const { source } = fixture([{ data: [post('20'), post('21')], meta: {} }]);
    expect((await source.fetchAuthorPosts({ authorId: '123', limit: 1 })).kind).toBe('failure');
  });
  it('does not request the network after cancellation', async () => {
    const abort = new AbortController();
    abort.abort();
    const mock = vi.fn();
    const source = new XObservationSource({
      bearerToken: 'test',
      requestTimeoutMs: 1000,
      maxPages: 1,
      minRequestIntervalMs: 1000,
      signal: abort.signal,
      fetch: mock,
    });
    expect((await source.fetchAuthorPosts({ authorId: '123', limit: 1 })).kind).toBe('failure');
    expect(mock).not.toHaveBeenCalled();
  });
});
