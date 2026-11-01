import { Chain, QueueName, TokenId } from '@nit/domain';
import { EnqueueRequest } from '@nit/queue';
import { NormalizedTokenLaunch } from './normalize';

/**
 * The jobs a discovered token fans out into, per EDS ch.21 §6.
 *
 * Three queues, not one, and the enum's own comment says why: they run in parallel
 * off a persisted token, and sharing a queue would serialise them behind whichever
 * provider is slowest. Nothing here depends on anything else here — a token whose
 * artwork host is down still gets its market snapshot.
 *
 * Each payload carries the values its worker needs rather than only a `tokenId`, so
 * no handler reads `tokens` back. That is safe because `tokens` is append-only and
 * its columns are never rewritten: a copied `address` cannot become stale the way a
 * copied liquidity figure could. What it buys is one fewer round trip in the stage
 * already spending its time waiting on third parties.
 *
 * Payload fields are JSON primitives, because a payload is stored as JSONB and read
 * back by `pg` — the same reason `LaunchPollPayload.since` is a string. There are no
 * timestamps below, so nothing needs parsing on the way out.
 */

export const TOKEN_ARTWORK_PAYLOAD_VERSION = '1';
export const TOKEN_LIQUIDITY_PAYLOAD_VERSION = '1';
export const TOKEN_CREATOR_PAYLOAD_VERSION = '1';

/** One image to fetch, hash, store and read text out of. */
export interface TokenArtworkPayload {
  tokenId: string;
  chain: Chain;
  address: string;
  /** From the launch feed. Only enqueued when the launch published one. */
  artworkUrl: string;
}

/** One token to ask a market-data provider about. */
export interface TokenLiquidityPayload {
  tokenId: string;
  chain: Chain;
  address: string;
}

/**
 * One creator's launch history to recompute.
 *
 * The wallet is carried because it identifies the profile — `creator_profiles` is
 * UNIQUE (chain, creator_wallet) — and the token is carried because the event the
 * worker emits is keyed to it.
 */
export interface TokenCreatorPayload {
  tokenId: string;
  chain: Chain;
  creatorWallet: string;
}

/**
 * Any of the three, as the discovery transaction hands them to the enqueuer.
 *
 * A union rather than a common supertype: the three payloads have no shared shape
 * worth naming beyond `tokenId`, and widening them to one interface with optional
 * fields would make every handler re-check what the planner already knows.
 */
export type EnrichmentJobPayload =
  | TokenArtworkPayload
  | TokenLiquidityPayload
  | TokenCreatorPayload;

/**
 * The union sits at the payload rather than around the whole request, and it has to.
 *
 * `JobEnqueuer.enqueue` is generic in its payload, so a
 * `EnqueueRequest<A> | EnqueueRequest<B> | EnqueueRequest<C>` argument makes inference
 * pick one member and reject the other two — a type that could not be passed to the
 * only function it exists to be passed to. `EnqueueRequest` mentions `TPayload` once,
 * as a plain field, so hoisting the union inward loses nothing: each literal below is
 * still checked against exactly one of the three shapes.
 */
export type EnrichmentJobRequest = EnqueueRequest<EnrichmentJobPayload>;

/**
 * Discovery-time idempotency keys.
 *
 * Per token, which is what makes redelivery of the discovery poll harmless: the
 * second attempt plans the same keys and `enqueue` absorbs them on
 * `(queue_name, idempotency_key)`.
 *
 * The liquidity key carries a `:launch` discriminator and the other two do not, and
 * the asymmetry is load-bearing. Artwork and creator profiles are recomputed by
 * *overwriting* a row, so one job per token is all there will ever be. Market
 * snapshots accumulate: a re-poll of the same token later is a different, wanted job,
 * and if it derived its key the same way it would collide with this one forever and
 * the token would be polled exactly once in its life. Naming this one the launch
 * poll leaves the key space open for whatever schedules the rest.
 */
function artworkKey(tokenId: TokenId): string {
  return `token_artwork:${tokenId}`;
}

function liquidityKey(tokenId: TokenId): string {
  return `token_liquidity:${tokenId}:launch`;
}

/**
 * Keyed by token, deliberately, and not by wallet.
 *
 * Keying it `token_creator:<chain>/<wallet>` reads more naturally and is wrong: the
 * second token from a wallet would be absorbed as a duplicate enqueue and the profile
 * would keep reporting a launch count of one. Per token means every discovery
 * triggers exactly one recompute, which is affordable only because the recompute
 * counts rows in an append-only table and so lands on the same answer regardless of
 * how many of them run.
 */
function creatorKey(tokenId: TokenId): string {
  return `token_creator:${tokenId}`;
}

/**
 * The enrichment jobs a launch warrants.
 *
 * Pure, and the only place that decides which of the three a given launch gets — a
 * decision worth isolating because it is the whole of "progressive enrichment" as far
 * as discovery is concerned, and because getting it wrong is invisible: a job never
 * planned is not a failure anywhere, just a token that stays thin.
 *
 * The market job is unconditional; the other two are planned only when the launch
 * supplied what they need. `normalizeSourceLaunch` has already collapsed empty
 * strings to absent, so an artwork URL of `''` cannot reach here as a download of
 * nothing.
 */
export function enrichmentJobsFor(launch: NormalizedTokenLaunch): EnrichmentJobRequest[] {
  const requests: EnrichmentJobRequest[] = [
    {
      queueName: QueueName.TOKEN_LIQUIDITY,
      payload: {
        tokenId: launch.tokenId,
        chain: launch.chain,
        address: launch.address,
      },
      payloadVersion: TOKEN_LIQUIDITY_PAYLOAD_VERSION,
      idempotencyKey: liquidityKey(launch.tokenId),
    },
  ];

  if (launch.artworkUrl !== undefined) {
    requests.push({
      queueName: QueueName.TOKEN_ARTWORK,
      payload: {
        tokenId: launch.tokenId,
        chain: launch.chain,
        address: launch.address,
        artworkUrl: launch.artworkUrl,
      },
      payloadVersion: TOKEN_ARTWORK_PAYLOAD_VERSION,
      idempotencyKey: artworkKey(launch.tokenId),
    });
  }

  if (launch.creatorWallet !== undefined) {
    requests.push({
      queueName: QueueName.TOKEN_CREATOR,
      payload: {
        tokenId: launch.tokenId,
        chain: launch.chain,
        creatorWallet: launch.creatorWallet,
      },
      payloadVersion: TOKEN_CREATOR_PAYLOAD_VERSION,
      idempotencyKey: creatorKey(launch.tokenId),
    });
  }

  return requests;
}
