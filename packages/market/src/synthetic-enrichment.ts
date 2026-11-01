import { Result, Timestamp, failure, success } from '@nit/domain';
import {
  ArtworkAnalysis,
  ArtworkAnalysisAdapter,
  ArtworkAnalysisRequest,
  Decimal,
  MarketDataAdapter,
  MarketDataRequest,
  SourceMarketSnapshot,
} from './enrichment-contracts';

/**
 * Fabricated enrichment providers.
 *
 * They exist for the reason `SyntheticLaunchSource` does: the whole enrichment path —
 * claim a job, call a provider, upsert a row, append an event — has to be runnable
 * and testable in an environment holding no image host, no RPC endpoint and no
 * market-data key. Both report `isToyImplementation`, so the composition-root guard
 * refuses to start production with either bound.
 *
 * Every value below is a pure function of its input, which is what makes them usable
 * in tests of idempotency: re-running a job re-derives byte-identical output, so a
 * duplicate really is a duplicate rather than a coincidence that happened to hold.
 */

/** Fixed calendar constant, not a clock read, so observations are reproducible. */
const SYNTHETIC_EPOCH_MS = Date.UTC(2026, 0, 1, 12);

const DEFAULT_OBSERVATION_INTERVAL_MS = 60_000;

/** Scale of `market_snapshots.price_usd` — NUMERIC(38, 18). */
const PRICE_SCALE = 18;
/** Scale of the three NUMERIC(38, 6) amount columns. */
const AMOUNT_SCALE = 6;

/**
 * FNV-1a, 32 bits.
 *
 * A named non-cryptographic hash rather than an ad-hoc character sum, because the
 * property every derived value here depends on is that unrelated inputs do not
 * collide by accident — a corpus where two tokens shared a "price" would make a test
 * pass for the wrong reason.
 */
function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    // `Math.imul`, not `*`: the product exceeds 2^53 and a double would round it,
    // which would make the hash platform-dependent in the low bits.
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/** `length` hex characters derived from `input`, by rehashing with a round counter. */
function hexDigest(input: string, length: number): string {
  let out = '';
  for (let round = 0; out.length < length; round += 1) {
    out += fnv1a(`${input}#${round}`).toString(16).padStart(8, '0');
  }
  return out.slice(0, length);
}

/** `count` decimal digits derived from `seed`. */
function decimalDigits(seed: string, count: number): string {
  let out = '';
  for (let round = 0; out.length < count; round += 1) {
    // A 32-bit value is at most 4294967295, so ten characters is always exactly ten.
    out += String(fnv1a(`${seed}:${round}`)).padStart(10, '0');
  }
  return out.slice(0, count);
}

/**
 * An amount with a whole part in `[floor, floor + span)` and a fixed scale.
 *
 * Emitted at the column's declared scale rather than trimmed, so a stored value and
 * a freshly fetched one are textually identical as well as numerically equal. That
 * is a convenience for reading test failures, not something the pipeline relies on —
 * `decimalsEqual` is what compares them.
 */
function amount(seed: string, floor: number, span: number, scale: number): Decimal {
  const whole = floor + (fnv1a(`${seed}:whole`) % span);
  return `${String(whole)}.${decimalDigits(`${seed}:fraction`, scale)}`;
}

export interface SyntheticArtworkAnalyserOptions {
  /**
   * How many distinct perceptual hashes the analyser can produce.
   *
   * Small on purpose. A perceptual hash exists to collide — that is what makes it
   * perceptual — and an analyser whose hashes were as unique as its input URLs would
   * make `findByPhash` a query nothing could ever match, so the near-duplicate path
   * would be untestable. Raise it to make collisions rare, lower it to force them.
   */
  phashSpace?: number;
}

const DEFAULT_PHASH_SPACE = 8;

/** Width of a 64-bit perceptual hash in hex. */
const PHASH_LENGTH = 16;

/** Width of a SHA-256 digest in hex. */
const SHA256_LENGTH = 64;

/**
 * A fabricated artwork analyser.
 *
 * `sha256` is not a real digest — nothing is downloaded — but it preserves the one
 * property anything downstream depends on: it is a function of the image's URL
 * alone, so two launches pointing at the same image agree on it and two pointing at
 * different images do not. That is precisely what exact-duplicate detection asks of
 * a content hash, so the toy value exercises the real path.
 */
export class SyntheticArtworkAnalyser implements ArtworkAnalysisAdapter {
  readonly providerName = 'synthetic-artwork-analyser';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1.0.0';

  private readonly phashSpace: number;
  private outageRemaining = 0;
  private partialRemaining = 0;
  private analyses = 0;

  constructor(options: SyntheticArtworkAnalyserOptions = {}) {
    this.phashSpace = Math.max(1, options.phashSpace ?? DEFAULT_PHASH_SPACE);
  }

  /**
   * Make the next `count` analyses report failure.
   *
   * An image host that is down is the condition the handler's retry path exists for
   * and the one a caller cannot provoke from outside. A real adapter has no such
   * control surface and needs none; this one stands in for a CDN, so standing in for
   * a broken CDN is part of the job.
   */
  simulateOutage(count = 1): void {
    this.outageRemaining = Math.max(0, count);
  }

  /**
   * Make the next `count` analyses succeed with only a subset of their fields.
   *
   * The partial outcome is a first-class one — analysis is a chain of steps that fail
   * independently — and it is the only thing that exercises the repository's
   * fill-the-gaps upsert. Without a way to provoke it, that COALESCE would be
   * untested behaviour protecting against an untested failure.
   */
  simulatePartialAnalysis(count = 1): void {
    this.partialRemaining = Math.max(0, count);
  }

  /** Analyses attempted, including the ones an outage failed. */
  analysisCount(): number {
    return this.analyses;
  }

  analyse(request: ArtworkAnalysisRequest): Promise<Result<ArtworkAnalysis, Error>> {
    this.analyses += 1;

    if (this.outageRemaining > 0) {
      this.outageRemaining -= 1;
      return Promise.resolve(
        failure(
          new Error(
            `Synthetic artwork host unreachable for ${request.sourceUrl} (simulated, not a real failure)`
          )
        )
      );
    }

    const sha256 = hexDigest(`sha:${request.sourceUrl}`, SHA256_LENGTH);

    if (this.partialRemaining > 0) {
      this.partialRemaining -= 1;
      // The bytes were fetched and hashed; nothing else got as far as running. Which
      // fields survive is not arbitrary — this is the order the steps depend on each
      // other in, so a partial result is always a prefix of a complete one.
      return Promise.resolve(success({ sha256 }));
    }

    const bucket = fnv1a(`phash:${request.sourceUrl}`) % this.phashSpace;
    const analysis: ArtworkAnalysis = {
      storageLocation: `synthetic://artwork/${sha256}`,
      sha256,
      phash: hexDigest(`phash-bucket:${String(bucket)}`, PHASH_LENGTH),
    };

    // Most memecoin artwork is mostly text, but not all of it is, and a handler that
    // never saw an image without text would never exercise `hasOcrText: false`.
    if (fnv1a(`ocr:${request.sourceUrl}`) % 4 !== 0) {
      analysis.ocrText = `SYNTHETIC OCR ${hexDigest(`ocr-text:${request.sourceUrl}`, 8)} — fabricated, not read from an image`;
    }

    return Promise.resolve(success(analysis));
  }
}

export interface SyntheticMarketDataSourceOptions {
  /** Timestamp of the first observation. */
  epochMs?: number;
  /** Gap between consecutive observations. See `advanceObservation`. */
  intervalMs?: number;
}

/**
 * A fabricated market-data provider.
 *
 * `observedAt` comes from a step counter, not a clock, and that is the design point
 * rather than a convenience. It is part of `market_snapshots`' unique key, so a
 * clock-derived value would make every re-fetch a new row and the idempotency this
 * stage is built on would be untestable — the duplicate would never occur. Here, two
 * fetches return the same observation until `advanceObservation` is called, which is
 * exactly the behaviour of a real provider polled faster than it updates.
 */
export class SyntheticMarketDataSource implements MarketDataAdapter {
  readonly providerName = 'synthetic-market-data';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1.0.0';

  private readonly epochMs: number;
  private readonly intervalMs: number;
  private step = 0;
  private outageRemaining = 0;
  private emptyRemaining = 0;
  private fetches = 0;

  constructor(options: SyntheticMarketDataSourceOptions = {}) {
    this.epochMs = options.epochMs ?? SYNTHETIC_EPOCH_MS;
    this.intervalMs = options.intervalMs ?? DEFAULT_OBSERVATION_INTERVAL_MS;
  }

  /**
   * Move to the next observation.
   *
   * Advances `observedAt` and, because the metrics are seeded with the step, changes
   * every amount with it. One call is the difference between "the same reading again"
   * and "the market moved", which is the pair of cases the snapshot and liquidity
   * events divide between them.
   */
  advanceObservation(steps = 1): void {
    this.step += Math.max(0, steps);
  }

  /** Make the next `count` fetches report failure. */
  simulateOutage(count = 1): void {
    this.outageRemaining = Math.max(0, count);
  }

  /**
   * Make the next `count` fetches answer with no metrics at all.
   *
   * A reachable provider that knows nothing about a token is the normal case for a
   * mint minutes old, and it is a different fact from the poll not happening — worth
   * recording, per the contract note on `SourceMarketSnapshot`. It is also what
   * produces a liquidity reading that disappears, which is the only way to reach
   * `LiquidityUpdated`'s both-amounts-absent branch.
   */
  simulateEmptyResponse(count = 1): void {
    this.emptyRemaining = Math.max(0, count);
  }

  /** Fetch attempts made, including the ones an outage failed. */
  fetchCount(): number {
    return this.fetches;
  }

  fetchSnapshot(request: MarketDataRequest): Promise<Result<SourceMarketSnapshot, Error>> {
    this.fetches += 1;

    if (this.outageRemaining > 0) {
      this.outageRemaining -= 1;
      return Promise.resolve(
        failure(
          new Error(
            `Synthetic market data unavailable for ${request.chain}/${request.address} (simulated, not a real failure)`
          )
        )
      );
    }

    const observedAt: Timestamp = new Date(this.epochMs + this.step * this.intervalMs);
    const seed = `${request.chain}/${request.address}@${String(this.step)}`;

    if (this.emptyRemaining > 0) {
      this.emptyRemaining -= 1;
      return Promise.resolve(
        success({
          observedAt,
          raw: this.raw(request, observedAt, false),
        })
      );
    }

    return Promise.resolve(
      success({
        observedAt,
        // Sub-cent prices at full NUMERIC(38, 18) scale, because that is the shape a
        // memecoin price actually has and the shape a float would quietly flatten.
        priceUsd: `0.${decimalDigits(`${seed}:price`, PRICE_SCALE)}`,
        liquidityUsd: amount(`${seed}:liquidity`, 1_000, 900_000, AMOUNT_SCALE),
        marketCapUsd: amount(`${seed}:marketcap`, 10_000, 9_000_000, AMOUNT_SCALE),
        volume24hUsd: amount(`${seed}:volume`, 100, 500_000, AMOUNT_SCALE),
        holderCount: 10 + (fnv1a(`${seed}:holders`) % 5_000),
        raw: this.raw(request, observedAt, true),
      })
    );
  }

  private raw(
    request: MarketDataRequest,
    observedAt: Timestamp,
    hasMetrics: boolean
  ): Record<string, unknown> {
    return {
      chainId: request.chain,
      tokenAddress: request.address,
      observation: this.step,
      updatedAt: observedAt.toISOString(),
      indexed: hasMetrics,
      synthetic: true,
      note: 'Fabricated by SyntheticMarketDataSource. Not real market data.',
    };
  }
}
