import { z } from 'zod';
import { ObservationProvider, Result, failure, success } from '@nit/domain';
import { AuthorFetchRequest, ObservationSourceAdapter, SourcePost } from './contracts';

const id = z.string().regex(/^\d{1,30}$/);
export const XCaptureConfigSchema = z
  .object({
    authorIds: z
      .array(id)
      .min(1)
      .max(100)
      .refine((ids) => new Set(ids).size === ids.length),
    intervalMs: z.number().int().min(10000).max(3600000),
    requestTimeoutMs: z.number().int().min(1000).max(60000),
    maxPages: z.number().int().min(1).max(32),
    maxPosts: z.number().int().min(1).max(3200),
    minRequestIntervalMs: z.number().int().min(1000).max(60000),
    initialSince: z.string().datetime(),
  })
  .strict();
const Post = z
  .object({
    id,
    author_id: id,
    created_at: z.string().datetime(),
    text: z.string().max(16000),
    referenced_tweets: z.array(z.object({ type: z.string(), id })).optional(),
  })
  .passthrough();
const Page = z.object({
  data: z.array(Post).max(100).optional(),
  errors: z.array(z.unknown()).optional(),
  meta: z.object({ next_token: z.string().max(2000).optional() }),
});
export interface XSourceOptions {
  bearerToken: string;
  requestTimeoutMs: number;
  maxPages: number;
  minRequestIntervalMs: number;
  signal?: AbortSignal;
  /** Injectable for tests; production always uses the official HTTPS endpoint. */
  fetch?: typeof fetch;
}

/** Explicit author watchlist capture. Never infers narrative truth from a post. */
export class XObservationSource implements ObservationSourceAdapter {
  readonly provider = ObservationProvider.X;
  readonly providerName = 'x-api-v2';
  readonly interfaceVersion = '1.0.0';
  readonly isToyImplementation = false;
  private nextRequestAt = 0;
  constructor(private readonly options: XSourceOptions) {
    if (
      options.bearerToken.trim() === '' ||
      /[\r\n]/.test(options.bearerToken) ||
      !Number.isSafeInteger(options.requestTimeoutMs) ||
      options.requestTimeoutMs < 1000 ||
      options.requestTimeoutMs > 60000 ||
      !Number.isSafeInteger(options.maxPages) ||
      options.maxPages < 1 ||
      options.maxPages > 32 ||
      !Number.isSafeInteger(options.minRequestIntervalMs) ||
      options.minRequestIntervalMs < 1000
    )
      throw new Error('Invalid X source limits or credential');
  }

  async fetchAuthorPosts(request: AuthorFetchRequest): Promise<Result<SourcePost[], Error>> {
    try {
      id.parse(request.authorId);
      if (request.sinceProviderPostId !== undefined) id.parse(request.sinceProviderPostId);
      if (!Number.isSafeInteger(request.limit) || request.limit < 1 || request.limit > 3200)
        throw new Error('Invalid X capture limit');
      const posts = new Map<string, SourcePost>();
      let next: string | undefined;
      const tokens = new Set<string>();
      for (let pageIndex = 0; pageIndex < this.options.maxPages; pageIndex++) {
        if (this.options.signal?.aborted === true) throw new Error('X capture stopped');
        const url = new URL(`https://api.x.com/2/users/${request.authorId}/tweets`);
        url.searchParams.set('max_results', '100');
        url.searchParams.set('tweet.fields', 'author_id,created_at,referenced_tweets');
        if (request.sinceProviderPostId !== undefined)
          url.searchParams.set('since_id', request.sinceProviderPostId);
        else if (request.since !== undefined)
          url.searchParams.set('start_time', request.since.toISOString());
        if (next !== undefined) url.searchParams.set('pagination_token', next);
        const page = Page.parse(await this.read(url));
        if ((page.errors?.length ?? 0) !== 0) throw new Error('X returned partial errors');
        for (const post of page.data ?? []) {
          if (
            post.author_id !== request.authorId ||
            (request.sinceProviderPostId !== undefined &&
              BigInt(post.id) <= BigInt(request.sinceProviderPostId))
          )
            throw new Error('X response does not match requested author/cursor');
          posts.set(post.id, {
            providerPostId: post.id,
            authorId: post.author_id,
            publishedAt: new Date(post.created_at),
            text: post.text,
            media: [],
            raw: post,
            ...(post.referenced_tweets?.find((reference) => reference.type === 'quoted') ===
            undefined
              ? {}
              : {
                  quotedProviderPostId: post.referenced_tweets.find(
                    (reference) => reference.type === 'quoted'
                  )?.id as string,
                }),
          });
        }
        if (posts.size > request.limit) throw new Error('X capture backlog exceeds limit');
        next = page.meta.next_token;
        if (next === undefined)
          return success(
            [...posts.values()].sort((a, b) =>
              BigInt(a.providerPostId) < BigInt(b.providerPostId) ? -1 : 1
            )
          );
        if (tokens.has(next)) throw new Error('X repeated pagination token');
        tokens.add(next);
      }
      throw new Error('X capture pagination budget exhausted');
    } catch {
      // No partial success: the persisted cursor must never skip an unconsumed page.
      return failure(
        new Error('X capture incomplete; cursor unchanged. Check access, quotas and backlog.')
      );
    }
  }

  private async read(url: URL): Promise<unknown> {
    await new Promise<void>((resolve) =>
      setTimeout(resolve, Math.max(0, this.nextRequestAt - Date.now()))
    );
    this.nextRequestAt = Date.now() + this.options.minRequestIntervalMs;
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), this.options.requestTimeoutMs);
    try {
      const response = await (this.options.fetch ?? fetch)(url, {
        headers: { authorization: `Bearer ${this.options.bearerToken}` },
        redirect: 'error',
        signal:
          this.options.signal === undefined
            ? abort.signal
            : AbortSignal.any([abort.signal, this.options.signal]),
      });
      if (!response.ok || response.body === null) throw new Error('X unavailable');
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        const value: unknown = chunk.value;
        if (!(value instanceof Uint8Array)) throw new Error('Invalid X body chunk');
        size += value.length;
        if (size > 2097152) {
          await reader.cancel();
          throw new Error('X response too large');
        }
        chunks.push(value);
      }
      return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
    } finally {
      clearTimeout(timer);
    }
  }
}
