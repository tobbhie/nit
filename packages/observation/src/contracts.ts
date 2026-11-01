import { MediaType, ObservationProvider, Result, Timestamp } from '@nit/domain';
import { ProviderIdentity } from '@nit/provider-interfaces';

/**
 * One media attachment as the provider described it.
 *
 * `providerMediaKey` is the provider's own identifier for the attachment. It is
 * the only stable identity available before the bytes are fetched, which makes
 * it what re-ingestion deduplicates on — a sha256 would need the download that
 * has not happened yet.
 */
export interface SourceMedia {
  providerMediaKey: string;
  mediaType: MediaType;
  sourceUrl: string;
  durationMs?: number;
}

/**
 * A post exactly as an adapter read it, before normalization.
 *
 * Adapters do the provider-specific work — pagination, auth, rate limits, field
 * names — and nothing else. They do not assign identifiers, decide what counts
 * as a duplicate, or touch the database; `normalizeSourcePost` and the
 * repository own those, so a new provider is a new adapter and no other change.
 */
export interface SourcePost {
  providerPostId: string;
  authorId: string;
  publishedAt: Timestamp;
  text?: string;
  media: readonly SourceMedia[];
  quotedProviderPostId?: string;
  /**
   * The provider's response for this post, verbatim. Persisted to
   * `observations.raw_payload` so a later worker can extract a field nobody
   * thought to normalize, and so replay can rebuild from what actually arrived
   * rather than from what today's normalizer happened to keep.
   */
  raw: Record<string, unknown>;
}

/**
 * What to ask a provider for. `sinceProviderPostId` is preferred where the
 * provider supports it: cursors are exact, whereas a timestamp filter drops
 * posts that were published during the previous poll but indexed after it.
 */
export interface AuthorFetchRequest {
  authorId: string;
  sinceProviderPostId?: string;
  since?: Timestamp;
  limit: number;
}

/**
 * A source of observations.
 *
 * Extends `ProviderIdentity` so a synthetic adapter is registered like any other
 * provider capability and the composition-root guard refuses to start production
 * on one. An adapter reports failure as a value rather than throwing, because a
 * provider outage is expected operation (TIS ch.4 §9) and the caller's job is to
 * retry it, not to treat it as a bug.
 */
export interface ObservationSourceAdapter extends ProviderIdentity {
  readonly provider: ObservationProvider;
  fetchAuthorPosts(request: AuthorFetchRequest): Promise<Result<SourcePost[], Error>>;
}
