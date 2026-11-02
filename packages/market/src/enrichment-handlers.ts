import {
  Chain,
  MarketVenue,
  QueueName,
  Result,
  Timestamp,
  TokenId,
  failure,
  generateCreatorProfileId,
  generateMarketSnapshotId,
  generateTokenArtworkId,
  now,
  success,
} from '@nit/domain';
import { Job, JobHandler, PermanentJobFailure } from '@nit/queue';
import {
  ArtworkAnalysis,
  ArtworkAnalysisAdapter,
  Decimal,
  MarketDataAdapter,
  decimalsEqual,
} from './enrichment-contracts';
import {
  creatorProfileUpdatedEvent,
  liquidityUpdatedEvent,
  marketSnapshotCreatedEvent,
  tokenArtworkProcessedEvent,
} from './enrichment-events';
import {
  TOKEN_ARTWORK_PAYLOAD_VERSION,
  TOKEN_CREATOR_PAYLOAD_VERSION,
  TOKEN_LIQUIDITY_PAYLOAD_VERSION,
  TokenArtworkPayload,
  TokenCreatorPayload,
  TokenLiquidityPayload,
} from './enrichment-jobs';
import { CreatorProfile, TokenArtwork } from './enrichment-repository';
import { EnrichmentTransactor } from './unit-of-work';

/**
 * The three enrichment workers, per EDS ch.21 §6.
 *
 * One failure policy holds across all three, and it is the same one `LaunchPollHandler`
 * documents: a malformed payload throws `PermanentJobFailure` because no number of
 * retries changes a string, and a provider that could not be reached throws an
 * ordinary error because the queue's backoff is exactly the right response to an
 * outage.
 *
 * The invariant they exist to honour is that enrichment is idempotent (EDS ch.21 §8),
 * and honouring it takes more than an upsert. The row is only half the state — the
 * event log is the other half, and it is append-only, so a handler that appended
 * unconditionally would leave a different system behind on its second run even though
 * the table looked identical. Downstream, that is a duplicate reaction to a fact that
 * did not change. So every event below is gated on the enrichment having actually
 * changed something, and each handler reads the prior state to find out.
 *
 * `analysedAt` is excluded from every one of those comparisons. It records when the
 * work ran rather than what it found, so including it would make every re-run a
 * change and defeat the gate entirely.
 */

const CHAINS = new Set<string>(Object.values(Chain));
const MARKET_VENUES = new Set<string>(Object.values(MarketVenue));

export interface EnrichmentHandlerOptions {
  /** Recorded on every event as its origin. */
  producer: string;
  /** Injected for deterministic tests; defaults to wall clock. */
  clock?: () => Timestamp;
}

/** What one artwork job did. */
export interface ArtworkEnrichmentSummary {
  tokenId: string;
  artworkId: string;
  /** The analyser that answered, so a synthetic run is distinguishable in a log. */
  provider: string;
  /** `unchanged` is a success: the analysis agreed with what was already stored. */
  outcome: 'created' | 'updated' | 'unchanged';
  hasPhash: boolean;
  hasOcrText: boolean;
}

/** What one market job did. */
export interface MarketEnrichmentSummary {
  tokenId: string;
  venue: MarketVenue;
  source: string;
  observedAt: string;
  /** False when this exact observation was already stored. */
  stored: boolean;
  liquidityChanged: boolean;
}

/** What one creator job did. */
export interface CreatorEnrichmentSummary {
  tokenId: string;
  creatorProfileId: string;
  creatorWallet: string;
  launchCount: number;
  changed: boolean;
}

/**
 * Reject a payload whose version this handler does not implement.
 *
 * Permanent rather than retryable: a payload written by a newer producer will not
 * become readable by waiting, and the dead-letter queue is where an operator will
 * see that a deploy went out in the wrong order.
 */
function requireVersion(job: Job<unknown>, expected: string, queue: string): void {
  if (job.payloadVersion !== expected) {
    throw new PermanentJobFailure(`Unsupported ${queue} payload version '${job.payloadVersion}'`);
  }
}

/**
 * A payload field that has to be present to do any work at all.
 *
 * Typed as `string` and checked anyway, because a payload crosses a JSONB boundary:
 * the type is a claim about what was enqueued, not a guarantee about what came back.
 */
function requireText(value: string, field: string, queue: string): string {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new PermanentJobFailure(`${queue} payload has a missing or empty ${field}`);
  }
  return value;
}

/**
 * Reject an unrecognised chain.
 *
 * Not redundant with the enum: an unknown chain here would be stored against a token
 * identity that does not exist, and because identity is `(chain, address)` that forks
 * the token rather than failing.
 */
function requireChain(chain: Chain, queue: string): Chain {
  if (!CHAINS.has(chain)) {
    throw new PermanentJobFailure(`${queue} payload has unknown chain '${String(chain)}'`);
  }
  return chain;
}

function requireMarketVenue(venue: MarketVenue, queue: string): MarketVenue {
  if (!MARKET_VENUES.has(venue)) {
    throw new PermanentJobFailure(`${queue} payload has unknown venue '${String(venue)}'`);
  }
  return venue;
}

/**
 * Whether an analysis tells us anything the stored row does not.
 *
 * Compared field by field rather than by identity, and only over the fields the
 * repository's upsert can actually change. The upsert COALESCEs, so an analysis that
 * omits a field leaves the stored one in place — meaning an absent field is never a
 * change, and reporting one would announce a loss that did not happen.
 */
function artworkChanged(stored: TokenArtwork, analysis: ArtworkAnalysis): boolean {
  return (
    fills(stored.storageLocation, analysis.storageLocation) ||
    fills(stored.sha256, analysis.sha256) ||
    fills(stored.phash, analysis.phash) ||
    fills(stored.ocrText, analysis.ocrText)
  );
}

/** Whether `incoming` would replace `stored` under COALESCE semantics. */
function fills(stored: string | undefined, incoming: string | undefined): boolean {
  return incoming !== undefined && incoming !== stored;
}

/**
 * Whether liquidity moved between two readings.
 *
 * `decimalsEqual` rather than `===`, because a provider reporting `1000.5` against a
 * stored `1000.500000` has reported no movement at all.
 *
 * Absence on exactly one side is a change, and a deliberate one: a provider that
 * reported liquidity last time and reports none now has told us something, and it is
 * not that liquidity is zero.
 */
function liquidityChanged(previous: Decimal | undefined, current: Decimal | undefined): boolean {
  if (previous === undefined || current === undefined) {
    return previous !== current;
  }
  return !decimalsEqual(previous, current);
}

/**
 * Spread-in for one optional field, present only when there is a value.
 *
 * `exactOptionalPropertyTypes` refuses an explicit `undefined` where a property is
 * merely optional, so the alternative is a mutable object built up field by field.
 * The event builders do exactly that internally; doing it again at every call site
 * reads worse than a named helper.
 */
function optional<K extends string, V>(key: K, value: V | undefined): { [P in K]?: V } {
  return (value === undefined ? {} : { [key]: value }) as { [P in K]?: V };
}

/** The metric fields of one reading, shared by the snapshot row and its event. */
interface ReadingMetrics {
  priceUsd?: Decimal;
  liquidityUsd?: Decimal;
  marketCapUsd?: Decimal;
  volume24hUsd?: Decimal;
  holderCount?: number;
}

/**
 * The metrics of a reading, with absent ones left out rather than set to `undefined`.
 *
 * Spread into both the insert and the event so the two cannot disagree about what the
 * provider said, which is the point of `MarketSnapshotCreated` carrying them at all.
 */
function pickMetrics(reading: ReadingMetrics): ReadingMetrics {
  return {
    ...optional('priceUsd', reading.priceUsd),
    ...optional('liquidityUsd', reading.liquidityUsd),
    ...optional('marketCapUsd', reading.marketCapUsd),
    ...optional('volume24hUsd', reading.volume24hUsd),
    ...optional('holderCount', reading.holderCount),
  };
}

/**
 * Fetches, hashes, stores and reads text out of one token's artwork.
 *
 * The row is written here rather than at discovery, which keeps discovery's
 * transaction to exactly a token, its event and its jobs. `source_url` still comes
 * from the launch feed — it travels in the job payload — so the row records what the
 * launchpad published even though nothing wrote it until this worker ran.
 */
export class ArtworkEnrichmentHandler implements JobHandler<TokenArtworkPayload> {
  readonly queueName: string = QueueName.TOKEN_ARTWORK;

  private readonly clock: () => Timestamp;

  constructor(
    private readonly analyser: ArtworkAnalysisAdapter,
    private readonly unitOfWork: EnrichmentTransactor,
    private readonly options: EnrichmentHandlerOptions
  ) {
    this.clock = options.clock ?? now;
  }

  async handle(job: Job<TokenArtworkPayload>): Promise<void> {
    requireVersion(job, TOKEN_ARTWORK_PAYLOAD_VERSION, QueueName.TOKEN_ARTWORK);
    const result = await this.enrich(job.payload);
    if (result.kind === 'failure') {
      throw result.error;
    }
  }

  /**
   * Analyse and store one token's artwork.
   *
   * Separate from `handle` so the work can be exercised without a job envelope, and
   * so the summary is a return value rather than something only a log sees.
   */
  async enrich(payload: TokenArtworkPayload): Promise<Result<ArtworkEnrichmentSummary, Error>> {
    const queue = QueueName.TOKEN_ARTWORK;
    const tokenId = requireText(payload.tokenId, 'tokenId', queue);
    const sourceUrl = requireText(payload.artworkUrl, 'artworkUrl', queue);
    requireChain(payload.chain, queue);

    // Outside the transaction. The analyser downloads an image over a network this
    // process does not control, and holding a Postgres transaction open across that
    // would idle a connection for as long as a CDN feels like taking.
    const analysed = await this.analyser.analyse({ sourceUrl });
    if (analysed.kind === 'failure') {
      return failure(analysed.error);
    }
    const analysis = analysed.value;
    const analysedAt = this.clock();

    return this.unitOfWork.transact(async ({ artwork, events }) => {
      const typedTokenId = tokenId as TokenId;

      const existing = await artwork.findByToken(typedTokenId);
      if (existing.kind === 'failure') {
        return failure(existing.error);
      }
      const before = existing.value;

      const upserted = await artwork.upsert({
        // Generated per attempt but only used if this is the insert: `upsert` returns
        // the identifier the row actually holds, and the event has to carry that one
        // or it would name a row that never existed.
        artworkId: generateTokenArtworkId(),
        tokenId: typedTokenId,
        sourceUrl,
        ...analysis,
        analysedAt,
      });
      if (upserted.kind === 'failure') {
        return failure(upserted.error);
      }
      const artworkId = upserted.value;

      const outcome =
        before === null ? 'created' : artworkChanged(before, analysis) ? 'updated' : 'unchanged';

      const summary: ArtworkEnrichmentSummary = {
        tokenId,
        artworkId,
        provider: this.analyser.providerName,
        outcome,
        hasPhash: (analysis.phash ?? before?.phash) !== undefined,
        hasOcrText: (analysis.ocrText ?? before?.ocrText) !== undefined,
      };

      if (outcome === 'unchanged') {
        return success(summary);
      }

      // The event carries the merged state rather than only what this run produced,
      // so a consumer reading the log sees what is true of the token now. A partial
      // re-analysis that filled in a hash should not announce a token with no sha256
      // just because this attempt did not compute one.
      const event = tokenArtworkProcessedEvent({
        tokenId: typedTokenId,
        artworkId,
        sourceUrl,
        ...optional('sha256', analysis.sha256 ?? before?.sha256),
        ...optional('phash', analysis.phash ?? before?.phash),
        hasOcrText: summary.hasOcrText,
        analysedAt,
        provider: this.analyser.providerName,
        producer: this.options.producer,
      });
      const appended = await events.append(event);
      if (appended.kind === 'failure') {
        return failure(appended.error);
      }

      return success(summary);
    });
  }
}

/**
 * Polls one token's market state and records it.
 *
 * Two events come out of one reading and they say different things. The snapshot
 * event fires for every reading actually stored; the liquidity event fires only when
 * the reading differs from the previous one from the same source. A consumer that
 * wants "the market moved" subscribes to the second and is not woken by a poll that
 * found the market exactly where it left it.
 *
 * No clock. `observedAt` is the provider's timestamp for the reading and part of the
 * row's identity, so a clock read here would make every re-poll a new row and the
 * append-only table would grow a duplicate per retry.
 */
export class MarketEnrichmentHandler implements JobHandler<TokenLiquidityPayload> {
  readonly queueName: string = QueueName.TOKEN_LIQUIDITY;

  constructor(
    private readonly provider: MarketDataAdapter,
    private readonly unitOfWork: EnrichmentTransactor,
    private readonly options: EnrichmentHandlerOptions
  ) {}

  async handle(job: Job<TokenLiquidityPayload>): Promise<void> {
    requireVersion(job, TOKEN_LIQUIDITY_PAYLOAD_VERSION, QueueName.TOKEN_LIQUIDITY);
    const result = await this.enrich(job.payload);
    if (result.kind === 'failure') {
      throw result.error;
    }
  }

  async enrich(payload: TokenLiquidityPayload): Promise<Result<MarketEnrichmentSummary, Error>> {
    const queue = QueueName.TOKEN_LIQUIDITY;
    const tokenId = requireText(payload.tokenId, 'tokenId', queue);
    const address = requireText(payload.address, 'address', queue);
    const chain = requireChain(payload.chain, queue);
    const venue = requireMarketVenue(payload.venue, queue);

    // Outside the transaction, for the reason the artwork handler gives.
    const fetched = await this.provider.fetchSnapshot({ chain, address, venue });
    if (fetched.kind === 'failure') {
      return failure(fetched.error);
    }
    const reading = fetched.value;
    const source = this.provider.providerName;

    return this.unitOfWork.transact(async ({ snapshots, events }) => {
      const typedTokenId = tokenId as TokenId;

      // Read before writing. After the insert this token's newest snapshot is the one
      // just stored, and comparing a reading against itself would report that
      // liquidity never moves.
      const latest = await snapshots.findLatest(typedTokenId, venue, source);
      if (latest.kind === 'failure') {
        return failure(latest.error);
      }
      const previous = latest.value;

      const inserted = await snapshots.insert({
        snapshotId: generateMarketSnapshotId(),
        tokenId: typedTokenId,
        venue,
        observedAt: reading.observedAt,
        source,
        ...pickMetrics(reading),
        rawPayload: reading.raw,
      });
      if (inserted.kind === 'failure') {
        return failure(inserted.error);
      }

      const summary: MarketEnrichmentSummary = {
        tokenId,
        venue,
        source,
        observedAt: reading.observedAt.toISOString(),
        stored: inserted.value.inserted,
        liquidityChanged: false,
      };

      // A redelivered job that re-fetched the same observation. Nothing was written,
      // so nothing is announced — the events for this reading were appended by the
      // attempt that stored it.
      if (!inserted.value.inserted) {
        return success(summary);
      }

      const snapshotId = inserted.value.snapshotId;
      const createdEvent = await events.append(
        marketSnapshotCreatedEvent({
          tokenId: typedTokenId,
          snapshotId,
          venue,
          source,
          observedAt: reading.observedAt,
          ...pickMetrics(reading),
          producer: this.options.producer,
        })
      );
      if (createdEvent.kind === 'failure') {
        return failure(createdEvent.error);
      }

      // No previous reading, so nothing to have moved. Announcing a first observation
      // as an update would make every token look like it had just changed.
      //
      // The ordering guard is the second half of that: a retry delivered after a
      // newer observation was already stored would otherwise compare backwards and
      // report a move in the wrong direction. Storing the late reading is still
      // right — `market_snapshots` is a history, not a current-value cache.
      if (
        previous === null ||
        previous.observedAt.getTime() >= reading.observedAt.getTime() ||
        !liquidityChanged(previous.liquidityUsd, reading.liquidityUsd)
      ) {
        return success(summary);
      }

      const updatedEvent = await events.append(
        liquidityUpdatedEvent({
          tokenId: typedTokenId,
          snapshotId,
          venue,
          source,
          observedAt: reading.observedAt,
          ...optional('previousLiquidityUsd', previous.liquidityUsd),
          ...optional('liquidityUsd', reading.liquidityUsd),
          previousObservedAt: previous.observedAt,
          producer: this.options.producer,
        })
      );
      if (updatedEvent.kind === 'failure') {
        return failure(updatedEvent.error);
      }

      return success({ ...summary, liquidityChanged: true });
    });
  }
}

/**
 * Recomputes one creator's launch history.
 *
 * The only one of the three that calls no provider, because the answer is already in
 * this database: a creator profile is an aggregate over `tokens`. That is also what
 * makes it the most straightforwardly idempotent of the three — counting rows in an
 * append-only table gives the same answer however many times it runs, so the worker
 * never has to know whether it has run before.
 *
 * It carries no reputation score. EDS ch.21 §9 puts creator reputation in the
 * Production tier, and a score computed here from a launch count would be a number
 * correlation reads as meaningful with nothing behind it.
 */
export class CreatorEnrichmentHandler implements JobHandler<TokenCreatorPayload> {
  readonly queueName: string = QueueName.TOKEN_CREATOR;

  private readonly clock: () => Timestamp;

  constructor(
    private readonly unitOfWork: EnrichmentTransactor,
    private readonly options: EnrichmentHandlerOptions
  ) {
    this.clock = options.clock ?? now;
  }

  async handle(job: Job<TokenCreatorPayload>): Promise<void> {
    requireVersion(job, TOKEN_CREATOR_PAYLOAD_VERSION, QueueName.TOKEN_CREATOR);
    const result = await this.enrich(job.payload);
    if (result.kind === 'failure') {
      throw result.error;
    }
  }

  async enrich(payload: TokenCreatorPayload): Promise<Result<CreatorEnrichmentSummary, Error>> {
    const queue = QueueName.TOKEN_CREATOR;
    const tokenId = requireText(payload.tokenId, 'tokenId', queue);
    const creatorWallet = requireText(payload.creatorWallet, 'creatorWallet', queue);
    const chain = requireChain(payload.chain, queue);
    const analysedAt = this.clock();

    return this.unitOfWork.transact(async ({ creators, events }) => {
      // Both reads inside the transaction, unlike the other two handlers: there is no
      // provider call to keep out of it, and the count and the profile it is compared
      // against have to come from one snapshot or a concurrent discovery could make
      // the comparison describe two different states of `tokens`.
      const stats = await creators.computeStats(chain, creatorWallet);
      if (stats.kind === 'failure') {
        return failure(stats.error);
      }
      if (stats.value === null) {
        // The job names a wallet with no launches on this chain. The token that
        // triggered it was committed in the same transaction that enqueued this job,
        // so its absence is not a race — it is a payload that does not describe
        // anything, and it will not start describing something on a retry.
        throw new PermanentJobFailure(`No launches found for creator ${chain}/${creatorWallet}`);
      }
      const computed = stats.value;

      const existing = await creators.findByWallet(chain, creatorWallet);
      if (existing.kind === 'failure') {
        return failure(existing.error);
      }
      const before = existing.value;

      const upserted = await creators.upsert({
        creatorProfileId: generateCreatorProfileId(),
        chain,
        creatorWallet,
        ...computed,
        analysedAt,
      });
      if (upserted.kind === 'failure') {
        return failure(upserted.error);
      }
      const creatorProfileId = upserted.value;

      const changed = before === null || profileChanged(before, computed);
      const summary: CreatorEnrichmentSummary = {
        tokenId,
        creatorProfileId,
        creatorWallet,
        launchCount: computed.launchCount,
        changed,
      };

      // A redelivered job for a wallet that has launched nothing since. The row was
      // rewritten with identical values, so there is no fact to announce.
      if (!changed) {
        return success(summary);
      }

      const appended = await events.append(
        creatorProfileUpdatedEvent({
          tokenId: tokenId as TokenId,
          creatorProfileId,
          chain,
          creatorWallet,
          ...computed,
          analysedAt,
          producer: this.options.producer,
        })
      );
      if (appended.kind === 'failure') {
        return failure(appended.error);
      }

      return success(summary);
    });
  }
}

/**
 * Whether a recomputed history differs from the stored one.
 *
 * `analysedAt` is excluded, for the reason given at the top of this file. Timestamps
 * are compared by value rather than by identity because they arrive as distinct
 * `Date` objects from the driver on every read.
 */
function profileChanged(
  stored: CreatorProfile,
  computed: { launchCount: number; firstLaunchAt: Timestamp; lastLaunchAt: Timestamp }
): boolean {
  return (
    stored.launchCount !== computed.launchCount ||
    stored.firstLaunchAt.getTime() !== computed.firstLaunchAt.getTime() ||
    stored.lastLaunchAt.getTime() !== computed.lastLaunchAt.getTime()
  );
}
