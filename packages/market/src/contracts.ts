import { Chain, LaunchSource, Result, Timestamp } from '@nit/domain';
import { ProviderIdentity } from '@nit/provider-interfaces';

/**
 * A token launch exactly as an adapter read it, before normalization.
 *
 * Adapters do the launchpad-specific work — pagination, auth, rate limits, field
 * names — and nothing else. They do not assign identifiers, decide what counts as
 * a duplicate, or touch the database; `normalizeSourceLaunch` and the repository
 * own those.
 */
export interface SourceTokenLaunch {
  /** The mint address. Case-sensitive base58 — see the note in `normalize`. */
  address: string;
  ticker: string;
  name: string;
  launchTime: Timestamp;
  creatorWallet?: string;
  description?: string;
  artworkUrl?: string;
  /**
   * The launchpad's response for this token, verbatim. Persisted to
   * `tokens.raw_payload` so a later worker can extract a field nobody thought to
   * normalize, and so replay can rebuild from what actually arrived rather than
   * from what today's normalizer happened to keep.
   */
  raw: Record<string, unknown>;
}

/**
 * What to ask a launchpad for.
 *
 * A timestamp rather than a cursor, unlike `AuthorFetchRequest` in
 * `@nit/observation`. A post feed is an append-only timeline with stable ids, so
 * a cursor is exact; a launch feed is a set of mints with no monotonic ordering
 * to page from, so a caller polls a time window and accepts that windows overlap.
 * Overlap is safe here rather than merely tolerable: `(chain, address)` is unique,
 * so a re-seen launch is absorbed by the database as a duplicate (EDS ch.21 §8).
 */
export interface LaunchFetchRequest {
  since?: Timestamp;
  limit: number;
}

/**
 * A source of token launches.
 *
 * Extends `ProviderIdentity` so a synthetic adapter is registered like any other
 * provider capability and the composition-root guard refuses to start production
 * on one. An adapter reports failure as a value rather than throwing, because a
 * launchpad outage is expected operation and the caller's job is to retry it.
 *
 * `chain` and `launchSource` are properties of the adapter rather than arguments
 * to the fetch: one adapter reads one feed on one chain, and making them
 * per-call would invite a caller to mislabel where a launch came from.
 */
export interface LaunchSourceAdapter extends ProviderIdentity {
  readonly chain: Chain;
  readonly launchSource: LaunchSource;
  fetchRecentLaunches(request: LaunchFetchRequest): Promise<Result<SourceTokenLaunch[], Error>>;
}
