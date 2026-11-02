import { Chain, MarketVenue, Result, Timestamp } from '@nit/domain';
import { PoolClient } from '@nit/database-public';
import { EnqueueRequest, Job } from '@nit/queue';
import { ProviderIdentity } from '@nit/provider-interfaces';

/**
 * Ports for progressive enrichment, per EDS ch.21 §6 and TIS ch.9.
 *
 * One rule shapes every money-valued field below: amounts cross this package as
 * decimal *strings*, never as `number`. `market_snapshots` stores them as
 * NUMERIC(38, 18) and NUMERIC(38, 6), which no IEEE-754 double can hold, and `pg`
 * hands NUMERIC back as a string for exactly that reason. Parsing one into a float
 * on the way in and formatting it back out on the way to the database would lose
 * digits in the middle of a value a human will read as a price.
 *
 * What that costs is comparison: `===` on two of these is textual, and a provider
 * reporting `1000.5` against a stored `1000.500000` is the same amount written two
 * ways. `decimalsEqual` below is the comparison to use.
 *
 * `holderCount` is a count, so it is an `INTEGER` and an ordinary `number`.
 */

/** A decimal amount, exactly as its source expressed it. See the note above. */
export type Decimal = string;

/**
 * Matches an unsigned decimal literal Postgres will accept into NUMERIC.
 *
 * Deliberately narrower than Postgres itself: no exponent, no sign, no leading
 * `+`. A provider that reports `1e-7` or `-3` for a price or a liquidity depth is
 * reporting something this stage should refuse rather than silently store, and a
 * value that reached the INSERT malformed would surface as a driver error naming a
 * column instead of naming the provider that sent it.
 */
const DECIMAL_PATTERN = /^\d+(\.\d+)?$/;

export function isDecimal(value: string): boolean {
  return DECIMAL_PATTERN.test(value);
}

/**
 * Whether two decimal amounts are the same amount.
 *
 * Not `===`, and the difference is the whole reason this exists. A provider that
 * reports `1000.5` and a NUMERIC(38, 6) column that returns `1000.500000` hold one
 * value written at two scales, and comparing the text would report a liquidity
 * change on every poll of a token whose liquidity never moved — the sort of bug that
 * shows up as an alert storm rather than as a failure.
 *
 * Compared digit by digit after padding both sides to a common shape, rather than
 * through `Number`: the point of keeping these as strings is that the values do not
 * fit a double, and a comparison that parsed them would be exactly the conversion
 * the type exists to avoid. Both operands are assumed to have passed `isDecimal` —
 * unsigned, no exponent — which is what makes padding sufficient.
 */
export function decimalsEqual(left: Decimal, right: Decimal): boolean {
  const [leftWhole = '', leftFraction = ''] = left.split('.');
  const [rightWhole = '', rightFraction = ''] = right.split('.');

  const wholeWidth = Math.max(leftWhole.length, rightWhole.length);
  if (leftWhole.padStart(wholeWidth, '0') !== rightWhole.padStart(wholeWidth, '0')) {
    return false;
  }

  const fractionWidth = Math.max(leftFraction.length, rightFraction.length);
  return leftFraction.padEnd(fractionWidth, '0') === rightFraction.padEnd(fractionWidth, '0');
}

/**
 * What an artwork analyser produced for one image.
 *
 * Every field is optional because the analysis is a chain of steps that fail
 * independently: a download can succeed and OCR find no text, a hash can be
 * computed for an image no store accepted. A partial analysis is worth persisting —
 * `token_artwork` updates in place, so the next run fills in what this one missed.
 *
 * There is no embedding here. Perceptual hashes are near-duplicate detection, which
 * is a launch-level fact; embeddings belong to multimodal processing (build order
 * step 4) and the column does not exist yet.
 */
export interface ArtworkAnalysis {
  /** Where the bytes were durably stored, if they were. */
  storageLocation?: string;
  /** Content hash of the exact bytes: identical uploads, not similar images. */
  sha256?: string;
  /** Perceptual hash: the same picture recompressed or rescaled (TIS ch.5). */
  phash?: string;
  /** Text found in the image. Memecoin artwork is frequently mostly text. */
  ocrText?: string;
}

export interface ArtworkAnalysisRequest {
  /** The URL the launch feed published. */
  sourceUrl: string;
}

/**
 * A source of artwork analysis.
 *
 * Fetching, hashing, storing and reading text out of an image are one port rather
 * than four because they are one round trip over the bytes: an implementation that
 * downloaded the image once per capability would pay for it four times, and a
 * launchpad CDN is the thing most likely to rate-limit this pipeline.
 */
export interface ArtworkAnalysisAdapter extends ProviderIdentity {
  analyse(request: ArtworkAnalysisRequest): Promise<Result<ArtworkAnalysis, Error>>;
}

/**
 * One observation of a token's market state, as a provider reported it.
 *
 * `observedAt` is the provider's timestamp for the reading, not the time this
 * process asked for it. It is part of `market_snapshots`' unique key, so getting it
 * from the clock would make every re-poll a new row and the append-only table would
 * grow a duplicate per retry.
 *
 * The metrics are optional individually because providers cover them unevenly — a
 * DEX quote knows liquidity and price and nothing about holders. A snapshot with no
 * metric at all is still worth recording: it says the provider was reachable and had
 * nothing, which is different from the poll never happening.
 */
export interface SourceMarketSnapshot {
  observedAt: Timestamp;
  priceUsd?: Decimal;
  liquidityUsd?: Decimal;
  marketCapUsd?: Decimal;
  volume24hUsd?: Decimal;
  holderCount?: number;
  /** The provider's response verbatim, for the reason `tokens.raw_payload` exists. */
  raw: Record<string, unknown>;
}

export interface MarketDataRequest {
  chain: Chain;
  address: string;
  /** The venue selects the correct reserve/quote path for this token at this time. */
  venue: MarketVenue;
}

/**
 * A source of market data.
 *
 * `chain` and `address` rather than a `tokenId`: a market-data provider has never
 * heard of this system's identifiers, and passing one would force every adapter to
 * carry a translation it cannot perform.
 */
export interface MarketDataAdapter extends ProviderIdentity {
  fetchSnapshot(request: MarketDataRequest): Promise<Result<SourceMarketSnapshot, Error>>;
}

/**
 * Enqueues jobs inside a transaction opened by someone else.
 *
 * Narrower than `JobQueue` on purpose — this package needs to enqueue and nothing
 * else, and depending on the whole queue would let a repository claim or complete
 * jobs. `JobQueue.enqueue` satisfies it structurally, so the composition root passes
 * the queue itself and no adapter class exists to drift.
 */
export interface TransactionalJobEnqueuer {
  enqueue<TPayload>(
    request: EnqueueRequest<TPayload>,
    client: PoolClient
  ): Promise<Result<Job, Error>>;
}

/**
 * The same capability with the transaction already bound.
 *
 * What a unit of work hands to the code inside it, so that code cannot enqueue onto
 * a connection other than the one its domain writes are going to.
 */
export interface JobEnqueuer {
  enqueue<TPayload>(request: EnqueueRequest<TPayload>): Promise<Result<Job, Error>>;
}
