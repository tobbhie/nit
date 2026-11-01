import {
  MediaType,
  ObservationProvider,
  Result,
  Timestamp,
  failure,
  success,
} from '@nit/domain';
import { AuthorFetchRequest, ObservationSourceAdapter, SourceMedia, SourcePost } from './contracts';

/**
 * First publication time in the synthetic corpus. A fixed calendar constant
 * rather than a clock read, so two runs of the same test see the same posts.
 */
const SYNTHETIC_EPOCH_MS = Date.UTC(2026, 0, 1);

const DEFAULT_POSTS_PER_AUTHOR = 12;
const DEFAULT_INTERVAL_MS = 60_000;
const DEFAULT_LIMIT = 50;

export interface SyntheticSourceOptions {
  /** Size of the fabricated corpus per author. */
  postsPerAuthor?: number;
  /** Publication time of the author's first post. */
  epochMs?: number;
  /** Gap between consecutive posts. */
  intervalMs?: number;
}

/**
 * A source of fabricated observations.
 *
 * Its purpose is to let the whole ingestion path — fetch, normalize, persist,
 * announce — run and be tested with no X credential present, which is what makes
 * the pipeline developable in an environment that holds no real ones.
 *
 * The corpus is generated from the author id and a post index, so it is
 * deterministic: the same request returns the same posts on every run, in every
 * process, which is what replay and backtest tests need. It deliberately covers
 * the shapes the pipeline has distinct code paths for — text-only posts, posts
 * with one or two attachments, media-only posts, and quotes of earlier posts —
 * because a fixture that only produces the easy shape tests only the easy path.
 *
 * Every string it emits says it is fabricated. `isToyImplementation` is true, so
 * the composition-root guard refuses to start production with this bound.
 */
export class SyntheticObservationSource implements ObservationSourceAdapter {
  readonly providerName = 'synthetic-observation-source';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1.0.0';
  readonly provider: ObservationProvider;

  private readonly postsPerAuthor: number;
  private readonly epochMs: number;
  private readonly intervalMs: number;
  /** Fetches still owed a simulated failure. See `simulateOutage`. */
  private outageRemaining = 0;
  private fetches = 0;

  constructor(
    provider: ObservationProvider = ObservationProvider.X,
    options: SyntheticSourceOptions = {}
  ) {
    this.provider = provider;
    this.postsPerAuthor = options.postsPerAuthor ?? DEFAULT_POSTS_PER_AUTHOR;
    this.epochMs = options.epochMs ?? SYNTHETIC_EPOCH_MS;
    this.intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
  }

  /**
   * Make the next `count` fetches report failure.
   *
   * TIS ch.4 §10 requires proving that a provider outage retries correctly, and
   * an outage is the one condition a caller cannot provoke from the outside. A
   * real adapter has no such control surface and needs none; this one is a stand
   * -in for a provider, so standing in for a broken provider is part of the job.
   */
  simulateOutage(count = 1): void {
    this.outageRemaining = Math.max(0, count);
  }

  /** Fetch attempts made, including the ones an outage failed. */
  fetchCount(): number {
    return this.fetches;
  }

  fetchAuthorPosts(request: AuthorFetchRequest): Promise<Result<SourcePost[], Error>> {
    this.fetches += 1;

    if (this.outageRemaining > 0) {
      this.outageRemaining -= 1;
      return Promise.resolve(
        failure(
          new Error(
            `Synthetic provider outage for author ${request.authorId} (simulated, not a real failure)`
          )
        )
      );
    }

    let from = 0;
    if (request.sinceProviderPostId !== undefined) {
      const cursor = this.indexOfCursor(request.authorId, request.sinceProviderPostId);
      if (cursor === null) {
        // A real provider rejects a cursor it did not issue. Failing here rather
        // than restarting from the beginning matters because dedupe would absorb
        // the re-fetch silently, leaving a caller that passed the wrong author's
        // cursor with no signal that it had.
        return Promise.resolve(
          failure(
            new Error(
              `Unknown cursor '${request.sinceProviderPostId}' for author ${request.authorId}`
            )
          )
        );
      }
      from = cursor + 1;
    }

    const limit = request.limit > 0 ? request.limit : DEFAULT_LIMIT;
    const selected: SourcePost[] = [];
    for (let index = from; index < this.postsPerAuthor && selected.length < limit; index += 1) {
      const post = this.post(request.authorId, index);
      // `since` filters what the cursor did not. A caller may pass either.
      if (request.since !== undefined && post.publishedAt.getTime() <= request.since.getTime()) {
        continue;
      }
      selected.push(post);
    }

    // Selected oldest-first so a capped poll takes the oldest unseen posts and
    // therefore skips nothing; returned newest-first because that is the order
    // providers answer in, which keeps the ingestion service's own ordering
    // honest rather than accidentally satisfied by the adapter.
    return Promise.resolve(success(selected.reverse()));
  }

  private postIdPrefix(authorId: string): string {
    return `synthetic-${this.provider}-${authorId}-`;
  }

  private indexOfCursor(authorId: string, cursor: string): number | null {
    const prefix = this.postIdPrefix(authorId);
    if (!cursor.startsWith(prefix)) {
      return null;
    }
    const index = Number(cursor.slice(prefix.length));
    return Number.isInteger(index) && index >= 0 ? index : null;
  }

  /**
   * One fabricated post.
   *
   * The shape is a function of the index alone, which is what makes the corpus
   * reproducible: every third post carries media, every sixth carries two
   * attachments including a video, the odd-indexed media posts carry no text at
   * all, and every fifth post quotes its predecessor.
   */
  private post(authorId: string, index: number): SourcePost {
    const providerPostId = `${this.postIdPrefix(authorId)}${index}`;
    const publishedAt: Timestamp = new Date(this.epochMs + index * this.intervalMs);
    const hasMedia = index % 3 === 0;
    // Media-only posts are drawn from the posts that have media, so no post ends
    // up with neither text nor an attachment.
    const hasText = !(hasMedia && index % 2 === 1);

    const media: SourceMedia[] = [];
    if (hasMedia) {
      media.push({
        providerMediaKey: `${providerPostId}-image`,
        mediaType: MediaType.IMAGE,
        sourceUrl: `https://synthetic.invalid/${providerPostId}/image.png`,
      });
    }
    if (index > 0 && index % 6 === 0) {
      media.push({
        providerMediaKey: `${providerPostId}-video`,
        mediaType: MediaType.VIDEO,
        sourceUrl: `https://synthetic.invalid/${providerPostId}/video.mp4`,
        durationMs: 1_000 * (index % 30),
      });
    }

    const post: SourcePost = {
      providerPostId,
      authorId,
      publishedAt,
      media,
      raw: {
        id: providerPostId,
        author_id: authorId,
        created_at: publishedAt.toISOString(),
        synthetic: true,
        note: 'Fabricated by SyntheticObservationSource. Not real provider data.',
      },
    };
    if (hasText) {
      post.text = `Fabricated synthetic post ${index} by ${authorId}. Not a real post.`;
    }
    if (index > 0 && index % 5 === 0) {
      post.quotedProviderPostId = `${this.postIdPrefix(authorId)}${index - 1}`;
    }
    return post;
  }
}
