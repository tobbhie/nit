import { describe, expect, it } from 'vitest';
import {
  Chain,
  EventType,
  JobId,
  JobStatus,
  LaunchSource,
  MarketVenue,
  QueueName,
  Timestamp,
  TokenId,
  failure,
  generateTokenId,
  success,
} from '@nit/domain';
import { Job, PermanentJobFailure } from '@nit/queue';
import { SourceMarketSnapshot } from './enrichment-contracts';
import {
  CreatorProfileUpdatedPayload,
  LiquidityUpdatedPayload,
  MarketSnapshotCreatedPayload,
  TokenArtworkProcessedPayload,
} from './enrichment-events';
import {
  ArtworkEnrichmentHandler,
  CreatorEnrichmentHandler,
  EnrichmentHandlerOptions,
  MarketEnrichmentHandler,
} from './enrichment-handlers';
import {
  TOKEN_ARTWORK_PAYLOAD_VERSION,
  TOKEN_CREATOR_PAYLOAD_VERSION,
  TOKEN_LIQUIDITY_PAYLOAD_VERSION,
  TokenArtworkPayload,
  TokenCreatorPayload,
  TokenLiquidityPayload,
  enrichmentJobsFor,
} from './enrichment-jobs';
import {
  DISCOVERED,
  FakeEnrichmentUnitOfWork,
  FakeUnitOfWork,
  LAUNCHED,
  PUMP_FUN,
  Store,
  StubMarketSource,
  launch,
  reading,
} from './in-memory.fixtures';
import { NormalizedTokenLaunch, normalizeSourceLaunch } from './normalize';
import { MarketDiscoveryService } from './service';
import { SyntheticArtworkAnalyser, SyntheticMarketDataSource } from './synthetic-enrichment';

/**
 * The three enrichment workers against in-memory doubles.
 *
 * What this file is for is the half of idempotency that a unique constraint does not
 * cover. `enrichment.pg.test.ts` proves the constraints behave as claimed; here the
 * question is what the handler does *around* them — whether a second run announces a
 * fact that did not change, whether a partial analysis erases what a complete one
 * stored, whether a late reading is compared backwards. Every one of those is a
 * decision in the handler, and every one of them is invisible in the table afterwards.
 */

/** When analysis runs, and when a re-run of it runs. Fixed, so nothing reads a clock. */
const ANALYSED_AT: Timestamp = new Date('2026-08-20T09:05:00.000Z');
const REANALYSED_AT: Timestamp = new Date('2026-08-20T09:30:00.000Z');

const ARTWORK_URL = 'https://cdn.invalid/token.png';
const ADDRESS = 'So11111111111111111111111111111111111111Aa';
const CREATOR = 'Creator111111111111111111111111111111111Cc';

function optionsAt(clock: Timestamp): EnrichmentHandlerOptions {
  return { producer: 'enrichment-test', clock: () => clock };
}

const OPTIONS = optionsAt(ANALYSED_AT);

/**
 * The payloads of every event of one type, in append order.
 *
 * Filtered rather than indexed because a creator test seeds the store through
 * discovery, so `TokenDiscovered` envelopes share it — and because the whole point of
 * several assertions below is that a *second* event was not appended.
 */
function payloadsOf<TPayload>(store: Store, eventType: EventType): TPayload[] {
  return store.events
    .filter((event) => event.eventType === eventType)
    .map((event) => event.payload as TPayload);
}

/**
 * A job envelope around a payload.
 *
 * Only `payload` and `payloadVersion` carry meaning; the rest exist because `Job`
 * describes a durable row and a handler is typed against all of it.
 */
function jobFor<TPayload>(
  queueName: QueueName,
  payload: TPayload,
  payloadVersion: string
): Job<TPayload> {
  return {
    jobId: 'job-1' as JobId,
    queueName,
    payload,
    payloadVersion,
    status: JobStatus.RUNNING,
    attemptCount: 1,
    maxAttempts: 3,
    leaseDeadline: ANALYSED_AT,
    leasedBy: 'worker-1',
    idempotencyKey: `${queueName}:fixture`,
    availableAt: DISCOVERED,
    lastError: null,
    correlationId: null,
    createdAt: DISCOVERED,
    updatedAt: DISCOVERED,
    completedAt: null,
  };
}

/** Unwrap an enrichment result, failing the test with the underlying error. */
function value<T>(result: { kind: 'success'; value: T } | { kind: 'failure'; error: Error }): T {
  if (result.kind === 'failure') {
    throw result.error;
  }
  return result.value;
}

describe('ArtworkEnrichmentHandler', () => {
  const TOKEN = generateTokenId();

  const payload = (overrides: Partial<TokenArtworkPayload> = {}): TokenArtworkPayload => ({
    tokenId: overrides.tokenId ?? TOKEN,
    chain: overrides.chain ?? Chain.SOLANA,
    address: overrides.address ?? ADDRESS,
    artworkUrl: overrides.artworkUrl ?? ARTWORK_URL,
  });

  it('stores an analysis and announces what is now true of the token', async () => {
    const uow = new FakeEnrichmentUnitOfWork();
    const analyser = new SyntheticArtworkAnalyser();
    const handler = new ArtworkEnrichmentHandler(analyser, uow, OPTIONS);

    const summary = value(await handler.enrich(payload()));

    expect(summary.outcome).toBe('created');
    expect(summary.provider).toBe('synthetic-artwork-analyser');
    expect(uow.store.artwork.size).toBe(1);

    const stored = uow.store.artwork.get(TOKEN);
    expect(stored?.sourceUrl).toBe(ARTWORK_URL);
    expect(stored?.analysedAt).toEqual(ANALYSED_AT);

    const announced = payloadsOf<TokenArtworkProcessedPayload>(
      uow.store,
      EventType.TOKEN_ARTWORK_PROCESSED
    );
    expect(announced).toHaveLength(1);
    // The event has to name the row that actually holds the analysis, not the
    // identifier this attempt generated — on a conflict the stored one wins.
    expect(announced[0]?.artworkId).toBe(stored?.artworkId);
    expect(announced[0]?.sha256).toBe(stored?.sha256);
    expect(announced[0]?.phash).toBe(stored?.phash);
    // Whether text was found, never the text itself.
    expect(announced[0]?.hasOcrText).toBe(stored?.ocrText !== undefined);
    expect(JSON.stringify(announced[0])).not.toContain('SYNTHETIC OCR');
  });

  it('announces nothing on a re-run that found the same thing', async () => {
    // The half of idempotency the unique constraint does not cover. `token_artwork`
    // is UNIQUE (token_id), so the table is identical either way — but the event log
    // is append-only, and a second announcement is a duplicate reaction downstream
    // to a fact that did not change.
    const uow = new FakeEnrichmentUnitOfWork();
    const handler = new ArtworkEnrichmentHandler(new SyntheticArtworkAnalyser(), uow, OPTIONS);

    const first = value(await handler.enrich(payload()));
    const second = value(await handler.enrich(payload()));

    expect(first.outcome).toBe('created');
    expect(second.outcome).toBe('unchanged');
    expect(second.artworkId).toBe(first.artworkId);
    expect(uow.store.artwork.size).toBe(1);
    expect(payloadsOf(uow.store, EventType.TOKEN_ARTWORK_PROCESSED)).toHaveLength(1);
  });

  it('fills the gaps a partial analysis left, and announces the merged state', async () => {
    // A partial result is a prefix of a complete one: the bytes were fetched and
    // hashed and nothing else got as far as running. The next run has to complete the
    // row rather than duplicate it.
    const uow = new FakeEnrichmentUnitOfWork();
    const analyser = new SyntheticArtworkAnalyser();
    const handler = new ArtworkEnrichmentHandler(analyser, uow, OPTIONS);

    analyser.simulatePartialAnalysis();
    const partial = value(await handler.enrich(payload()));
    expect(partial.outcome).toBe('created');
    expect(uow.store.artwork.get(TOKEN)?.phash).toBeUndefined();

    const complete = value(await handler.enrich(payload()));

    expect(complete.outcome).toBe('updated');
    expect(complete.artworkId).toBe(partial.artworkId);
    expect(uow.store.artwork.size).toBe(1);

    const stored = uow.store.artwork.get(TOKEN);
    expect(stored?.phash).toBeDefined();
    expect(stored?.storageLocation).toBeDefined();

    const announced = payloadsOf<TokenArtworkProcessedPayload>(
      uow.store,
      EventType.TOKEN_ARTWORK_PROCESSED
    );
    expect(announced).toHaveLength(2);
    expect(announced[1]?.phash).toBe(stored?.phash);
  });

  it('erases nothing when a later analysis comes back partial', async () => {
    // The direction that actually loses data. The upsert COALESCEs, so an absent
    // incoming field leaves the stored one — and because an absent field is never a
    // change, the run is `unchanged` and announces nothing.
    //
    // Two clocks, because `analysedAt` is the one field that does move: it records
    // when the work ran rather than what it found, which is why it is excluded from
    // the comparison that gates the event.
    const uow = new FakeEnrichmentUnitOfWork();
    const analyser = new SyntheticArtworkAnalyser();

    const complete = value(
      await new ArtworkEnrichmentHandler(analyser, uow, OPTIONS).enrich(payload())
    );
    const before = uow.store.artwork.get(TOKEN);
    expect(before?.storageLocation).toBeDefined();

    analyser.simulatePartialAnalysis();
    const later = value(
      await new ArtworkEnrichmentHandler(analyser, uow, optionsAt(REANALYSED_AT)).enrich(payload())
    );

    expect(later.outcome).toBe('unchanged');
    expect(later.artworkId).toBe(complete.artworkId);
    expect(uow.store.artwork.get(TOKEN)).toEqual({ ...before, analysedAt: REANALYSED_AT });
    expect(payloadsOf(uow.store, EventType.TOKEN_ARTWORK_PROCESSED)).toHaveLength(1);
  });

  it('reports an image host that is down as retryable, without opening a transaction', async () => {
    // A CDN that is down is exactly what the queue's backoff exists for. Holding a
    // transaction open across the download is what the handler avoids by calling the
    // analyser first, and `transactions` is what proves it did.
    const uow = new FakeEnrichmentUnitOfWork();
    const analyser = new SyntheticArtworkAnalyser();
    analyser.simulateOutage();
    const handler = new ArtworkEnrichmentHandler(analyser, uow, OPTIONS);

    const result = await handler.enrich(payload());

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error).not.toBeInstanceOf(PermanentJobFailure);
    }
    expect(uow.transactions).toBe(0);
    expect(uow.store.artwork.size).toBe(0);
  });

  it('stores nothing when the event could not be appended', async () => {
    // A row whose event never landed is a token enriched as far as this database is
    // concerned and un-enriched as far as every consumer is concerned.
    const uow = new FakeEnrichmentUnitOfWork(new Store(), true);
    const handler = new ArtworkEnrichmentHandler(new SyntheticArtworkAnalyser(), uow, OPTIONS);

    const result = await handler.enrich(payload());

    expect(result.kind).toBe('failure');
    expect(uow.store.artwork.size).toBe(0);
  });

  it('refuses a payload it cannot read, before touching the image host', async () => {
    const uow = new FakeEnrichmentUnitOfWork();
    const analyser = new SyntheticArtworkAnalyser();
    const handler = new ArtworkEnrichmentHandler(analyser, uow, OPTIONS);

    await expect(handler.handle(jobFor(QueueName.TOKEN_ARTWORK, payload(), '2'))).rejects.toThrow(
      PermanentJobFailure
    );
    await expect(handler.enrich(payload({ artworkUrl: '   ' }))).rejects.toThrow(
      PermanentJobFailure
    );
    await expect(handler.enrich(payload({ chain: 'ethereum' as Chain }))).rejects.toThrow(
      PermanentJobFailure
    );

    expect(analyser.analysisCount()).toBe(0);
  });

  it('claims the artwork queue', () => {
    const handler = new ArtworkEnrichmentHandler(
      new SyntheticArtworkAnalyser(),
      new FakeEnrichmentUnitOfWork(),
      OPTIONS
    );

    expect(handler.queueName).toBe(QueueName.TOKEN_ARTWORK);
  });
});

describe('MarketEnrichmentHandler', () => {
  const TOKEN = generateTokenId();

  const payload = (overrides: Partial<TokenLiquidityPayload> = {}): TokenLiquidityPayload => ({
    tokenId: overrides.tokenId ?? TOKEN,
    chain: overrides.chain ?? Chain.SOLANA,
    address: overrides.address ?? ADDRESS,
    venue: overrides.venue ?? MarketVenue.PUMP_FUN_BONDING_CURVE,
  });

  /** A handler over a scripted provider, so a test can state both of two readings. */
  function scripted(
    ...answers: Array<
      | { kind: 'success'; value: SourceMarketSnapshot }
      | {
          kind: 'failure';
          error: Error;
        }
    >
  ): { handler: MarketEnrichmentHandler; uow: FakeEnrichmentUnitOfWork } {
    const uow = new FakeEnrichmentUnitOfWork();
    return {
      handler: new MarketEnrichmentHandler(new StubMarketSource(answers), uow, OPTIONS),
      uow,
    };
  }

  const created = (store: Store): MarketSnapshotCreatedPayload[] =>
    payloadsOf<MarketSnapshotCreatedPayload>(store, EventType.MARKET_SNAPSHOT_CREATED);

  const moved = (store: Store): LiquidityUpdatedPayload[] =>
    payloadsOf<LiquidityUpdatedPayload>(store, EventType.LIQUIDITY_UPDATED);

  const T1: Timestamp = new Date('2026-08-20T09:10:00.000Z');
  const T2: Timestamp = new Date('2026-08-20T09:20:00.000Z');

  it('records a reading and announces it', async () => {
    const uow = new FakeEnrichmentUnitOfWork();
    const source = new SyntheticMarketDataSource();
    const handler = new MarketEnrichmentHandler(source, uow, OPTIONS);

    const summary = value(await handler.enrich(payload()));

    expect(summary.stored).toBe(true);
    expect(summary.source).toBe('synthetic-market-data');
    expect(uow.store.snapshots).toHaveLength(1);

    const stored = uow.store.snapshots[0];
    expect(created(uow.store)).toHaveLength(1);
    expect(created(uow.store)[0]?.snapshotId).toBe(stored?.snapshotId);
    expect(created(uow.store)[0]?.liquidityUsd).toBe(stored?.liquidityUsd);
    expect(created(uow.store)[0]?.observedAt).toBe(stored?.observedAt.toISOString());

    // Nothing to have moved from. Announcing a first observation as an update would
    // make every token in the system look like it had just changed.
    expect(summary.liquidityChanged).toBe(false);
    expect(moved(uow.store)).toHaveLength(0);
  });

  it('absorbs a re-poll that found the same observation', async () => {
    // `market_snapshots` is append-only and UNIQUE
    // (token_id, venue, source, observed_at), so
    // a redelivered job that re-fetched an unchanged provider must not grow the
    // history — and must not announce the reading a second time either.
    const uow = new FakeEnrichmentUnitOfWork();
    const source = new SyntheticMarketDataSource();
    const handler = new MarketEnrichmentHandler(source, uow, OPTIONS);

    const first = value(await handler.enrich(payload()));
    const second = value(await handler.enrich(payload()));

    expect(first.stored).toBe(true);
    expect(second.stored).toBe(false);
    expect(second.observedAt).toBe(first.observedAt);
    expect(uow.store.snapshots).toHaveLength(1);
    expect(created(uow.store)).toHaveLength(1);
    expect(moved(uow.store)).toHaveLength(0);
    expect(source.fetchCount()).toBe(2);
  });

  it('announces a liquidity move against the previous reading from the same source', async () => {
    const uow = new FakeEnrichmentUnitOfWork();
    const source = new SyntheticMarketDataSource();
    const handler = new MarketEnrichmentHandler(source, uow, OPTIONS);

    const first = value(await handler.enrich(payload()));
    source.advanceObservation();
    const second = value(await handler.enrich(payload()));

    expect(second.stored).toBe(true);
    expect(second.liquidityChanged).toBe(true);
    expect(uow.store.snapshots).toHaveLength(2);
    expect(created(uow.store)).toHaveLength(2);

    const update = moved(uow.store)[0];
    expect(moved(uow.store)).toHaveLength(1);
    expect(update?.observedAt).toBe(second.observedAt);
    expect(update?.previousObservedAt).toBe(first.observedAt);
    // The reading it was compared against, so a consumer can size the move without
    // querying the history back.
    expect(update?.previousLiquidityUsd).toBe(created(uow.store)[0]?.liquidityUsd);
    expect(update?.liquidityUsd).toBe(created(uow.store)[1]?.liquidityUsd);
  });

  it('reports no move when the same amount arrives at a different scale', async () => {
    // NUMERIC(38, 6) returns `1000.500000` for a provider that said `1000.5`. Compared
    // as text this is a liquidity change on every single poll of a token whose
    // liquidity never moved — an alert storm rather than a failure, which is why
    // `decimalsEqual` exists and why this test would pass under `===` only by luck.
    const { handler, uow } = scripted(
      success(reading(T1, { liquidityUsd: '1000.500000' })),
      success(reading(T2, { liquidityUsd: '1000.5' }))
    );

    await handler.enrich(payload());
    const second = value(await handler.enrich(payload()));

    expect(second.stored).toBe(true);
    expect(second.liquidityChanged).toBe(false);
    // Stored and announced as a reading — it happened — but not as a move.
    expect(uow.store.snapshots).toHaveLength(2);
    expect(created(uow.store)).toHaveLength(2);
    expect(moved(uow.store)).toHaveLength(0);
  });

  it('treats liquidity that has disappeared as a move', async () => {
    // A provider that reported liquidity last time and reports none now has told us
    // something, and it is not that liquidity is zero.
    const { handler, uow } = scripted(
      success(reading(T1, { liquidityUsd: '4200.000000' })),
      success(reading(T2))
    );

    await handler.enrich(payload());
    const second = value(await handler.enrich(payload()));

    expect(second.liquidityChanged).toBe(true);

    const update = moved(uow.store)[0];
    expect(update?.previousLiquidityUsd).toBe('4200.000000');
    expect(update).toBeDefined();
    expect('liquidityUsd' in (update as LiquidityUpdatedPayload)).toBe(false);
  });

  it('stores a late reading but does not compare backwards against it', async () => {
    // A retry delivered after a newer observation was already stored. Storing it is
    // right — the table is a history, not a current-value cache — but comparing the
    // newest stored reading against an older one would report a move in the wrong
    // direction, and the amounts here differ enough that only the guard suppresses it.
    const { handler, uow } = scripted(
      success(reading(T2, { liquidityUsd: '9000.000000' })),
      success(reading(T1, { liquidityUsd: '4200.000000' }))
    );

    await handler.enrich(payload());
    const late = value(await handler.enrich(payload()));

    expect(late.stored).toBe(true);
    expect(late.observedAt).toBe(T1.toISOString());
    expect(late.liquidityChanged).toBe(false);
    expect(uow.store.snapshots).toHaveLength(2);
    expect(created(uow.store)).toHaveLength(2);
    expect(moved(uow.store)).toHaveLength(0);
  });

  it('records a reachable provider that knew nothing', async () => {
    // Different from the poll not happening, and the normal case for a mint minutes
    // old. A row with no metric at all still says the provider answered.
    const uow = new FakeEnrichmentUnitOfWork();
    const source = new SyntheticMarketDataSource();
    source.simulateEmptyResponse();
    const handler = new MarketEnrichmentHandler(source, uow, OPTIONS);

    const summary = value(await handler.enrich(payload()));

    expect(summary.stored).toBe(true);
    expect(uow.store.snapshots[0]?.liquidityUsd).toBeUndefined();
    expect(uow.store.snapshots[0]?.holderCount).toBeUndefined();
    expect(created(uow.store)).toHaveLength(1);
    expect('liquidityUsd' in (created(uow.store)[0] as MarketSnapshotCreatedPayload)).toBe(false);
    expect(moved(uow.store)).toHaveLength(0);
  });

  it('reports a provider outage as retryable, without opening a transaction', async () => {
    const uow = new FakeEnrichmentUnitOfWork();
    const source = new SyntheticMarketDataSource();
    source.simulateOutage();
    const handler = new MarketEnrichmentHandler(source, uow, OPTIONS);

    const result = await handler.enrich(payload());

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error).not.toBeInstanceOf(PermanentJobFailure);
    }
    expect(uow.transactions).toBe(0);
  });

  it('propagates a provider failure unchanged', async () => {
    const { handler } = scripted(failure(new Error('market data returned 503')));

    const result = await handler.enrich(payload());

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error.message).toBe('market data returned 503');
    }
  });

  it('stores nothing when the event could not be appended', async () => {
    const uow = new FakeEnrichmentUnitOfWork(new Store(), true);
    const handler = new MarketEnrichmentHandler(new SyntheticMarketDataSource(), uow, OPTIONS);

    const result = await handler.enrich(payload());

    expect(result.kind).toBe('failure');
    expect(uow.store.snapshots).toHaveLength(0);
  });

  it('refuses a payload it cannot read, before touching the provider', async () => {
    const uow = new FakeEnrichmentUnitOfWork();
    const source = new SyntheticMarketDataSource();
    const handler = new MarketEnrichmentHandler(source, uow, OPTIONS);

    await expect(handler.handle(jobFor(QueueName.TOKEN_LIQUIDITY, payload(), '1'))).rejects.toThrow(
      PermanentJobFailure
    );
    await expect(handler.enrich(payload({ address: '' }))).rejects.toThrow(PermanentJobFailure);
    await expect(handler.enrich(payload({ chain: 'ethereum' as Chain }))).rejects.toThrow(
      PermanentJobFailure
    );
    await expect(handler.enrich(payload({ venue: 'unknown' as MarketVenue }))).rejects.toThrow(
      PermanentJobFailure
    );

    expect(source.fetchCount()).toBe(0);
  });

  it('claims the liquidity queue', () => {
    const handler = new MarketEnrichmentHandler(
      new SyntheticMarketDataSource(),
      new FakeEnrichmentUnitOfWork(),
      OPTIONS
    );

    expect(handler.queueName).toBe(QueueName.TOKEN_LIQUIDITY);
  });
});

/**
 * Guards the premise the two synthetic-source assertions above rest on.
 *
 * Every metric is seeded with the observation step, so consecutive readings differ —
 * but they differ because of a hash, and a hash can collide. If it ever did for this
 * address, "announces a liquidity move" would pass for the wrong reason and this test
 * is what would say so.
 */
describe('the synthetic readings this file compares', () => {
  it('report different liquidity at consecutive observations', async () => {
    const source = new SyntheticMarketDataSource();
    const request = {
      chain: Chain.SOLANA,
      address: ADDRESS,
      venue: MarketVenue.PUMP_FUN_BONDING_CURVE,
    };

    const first = value(await source.fetchSnapshot(request));
    source.advanceObservation();
    const second = value(await source.fetchSnapshot(request));

    expect(second.observedAt.getTime()).toBeGreaterThan(first.observedAt.getTime());
    expect(second.liquidityUsd).not.toBe(first.liquidityUsd);
  });
});

describe('CreatorEnrichmentHandler', () => {
  /**
   * A store seeded by discovery, which is what makes these tests real: the profile is
   * an aggregate over `tokens`, so a handler pointed at an empty store computes
   * nothing and every assertion below would be about the wrong thing.
   */
  async function discovered(
    ...wallets: Array<{ address: string; creatorWallet?: string; launchTime?: Timestamp }>
  ): Promise<{ store: Store; tokenIds: TokenId[] }> {
    const uow = new FakeUnitOfWork();
    const service = new MarketDiscoveryService(uow, {
      producer: 'creator-test-discovery',
      clock: () => DISCOVERED,
    });

    const tokenIds: TokenId[] = [];
    for (const spec of wallets) {
      const result = await service.discover(PUMP_FUN, launch(spec));
      tokenIds.push(value(result).tokenId);
    }
    return { store: uow.store, tokenIds };
  }

  function handlerOn(
    store: Store,
    appendFails = false
  ): { handler: CreatorEnrichmentHandler; uow: FakeEnrichmentUnitOfWork } {
    const uow = new FakeEnrichmentUnitOfWork(store, appendFails);
    return { handler: new CreatorEnrichmentHandler(uow, OPTIONS), uow };
  }

  const payloadFor = (tokenId: TokenId): TokenCreatorPayload => ({
    tokenId,
    chain: Chain.SOLANA,
    creatorWallet: CREATOR,
  });

  const announced = (store: Store): CreatorProfileUpdatedPayload[] =>
    payloadsOf<CreatorProfileUpdatedPayload>(store, EventType.CREATOR_PROFILE_UPDATED);

  it('computes a profile from the launches already in the database', async () => {
    const { store, tokenIds } = await discovered({
      address: 'Creator1Mint1',
      creatorWallet: CREATOR,
    });
    const { handler, uow } = handlerOn(store);

    const summary = value(await handler.enrich(payloadFor(tokenIds[0] as TokenId)));

    expect(summary.launchCount).toBe(1);
    expect(summary.changed).toBe(true);
    expect(uow.store.profiles.size).toBe(1);

    const profile = uow.store.profiles.get(`${Chain.SOLANA}/${CREATOR}`);
    expect(profile?.launchCount).toBe(1);
    expect(profile?.firstLaunchAt).toEqual(LAUNCHED);
    expect(profile?.lastLaunchAt).toEqual(LAUNCHED);
    expect(profile?.analysedAt).toEqual(ANALYSED_AT);

    expect(announced(uow.store)).toHaveLength(1);
    expect(announced(uow.store)[0]?.creatorProfileId).toBe(profile?.creatorProfileId);
    expect(announced(uow.store)[0]?.launchCount).toBe(1);
    // Keyed to the token whose discovery triggered the recompute.
    expect(announced(uow.store)[0]?.tokenId).toBe(tokenIds[0]);
    // No reputation score: EDS ch.21 §9 puts that in the Production tier, and a
    // number derived from a launch count would be one correlation reads as
    // meaningful with nothing behind it.
    expect(JSON.stringify(announced(uow.store)[0])).not.toContain('reputation');
  });

  it('announces nothing when the wallet has launched nothing since', async () => {
    // Idempotent without needing to know it is a re-run: counting rows in an
    // append-only table lands on the same answer however many times it happens. The
    // row is rewritten with identical values, so there is no fact to announce.
    const { store, tokenIds } = await discovered({
      address: 'Creator1Mint1',
      creatorWallet: CREATOR,
    });
    const { handler, uow } = handlerOn(store);
    const token = tokenIds[0] as TokenId;

    const first = value(await handler.enrich(payloadFor(token)));
    const second = value(await handler.enrich(payloadFor(token)));

    expect(first.changed).toBe(true);
    expect(second.changed).toBe(false);
    expect(second.creatorProfileId).toBe(first.creatorProfileId);
    expect(second.launchCount).toBe(1);
    expect(uow.store.profiles.size).toBe(1);
    expect(announced(uow.store)).toHaveLength(1);
  });

  it('raises the count when the same wallet launches again', async () => {
    // Why the job's idempotency key is per token rather than per wallet. Keyed
    // `token_creator:<chain>/<wallet>` this second job would be absorbed as a
    // duplicate enqueue and the profile would report one launch forever.
    const later = new Date(LAUNCHED.getTime() + 3_600_000);
    const { store, tokenIds } = await discovered(
      { address: 'Creator1Mint1', creatorWallet: CREATOR },
      { address: 'Creator1Mint2', creatorWallet: CREATOR, launchTime: later }
    );
    const { handler, uow } = handlerOn(store);

    const first = value(await handler.enrich(payloadFor(tokenIds[0] as TokenId)));
    const second = value(await handler.enrich(payloadFor(tokenIds[1] as TokenId)));

    expect(first.launchCount).toBe(2);
    expect(second.launchCount).toBe(2);
    // Both tokens were discovered before either job ran, so the first recompute
    // already saw both — and the second is then a genuine no-op.
    expect(first.changed).toBe(true);
    expect(second.changed).toBe(false);
    expect(uow.store.profiles.size).toBe(1);

    const profile = uow.store.profiles.get(`${Chain.SOLANA}/${CREATOR}`);
    expect(profile?.firstLaunchAt).toEqual(LAUNCHED);
    expect(profile?.lastLaunchAt).toEqual(later);
    expect(announced(uow.store)).toHaveLength(1);
  });

  it('announces the raised count when a launch arrives between recomputes', async () => {
    // The ordering that actually happens in production: one token is discovered, its
    // job runs, a second token from the same wallet is discovered, its job runs.
    const { store, tokenIds } = await discovered({
      address: 'Creator1Mint1',
      creatorWallet: CREATOR,
    });
    const { handler, uow } = handlerOn(store);

    const first = value(await handler.enrich(payloadFor(tokenIds[0] as TokenId)));
    expect(first.launchCount).toBe(1);

    const later = new Date(LAUNCHED.getTime() + 3_600_000);
    const second = await discovered({
      address: 'Creator1Mint2',
      creatorWallet: CREATOR,
      launchTime: later,
    });
    // Discovery wrote to its own store; move the row across so one store holds both.
    for (const [key, token] of second.store.tokens) {
      store.tokens.set(key, token);
    }

    const recomputed = value(await handler.enrich(payloadFor(second.tokenIds[0] as TokenId)));

    expect(recomputed.launchCount).toBe(2);
    expect(recomputed.changed).toBe(true);
    expect(announced(uow.store)).toHaveLength(2);
    expect(announced(uow.store)[1]?.launchCount).toBe(2);
    expect(announced(uow.store)[1]?.lastLaunchAt).toBe(later.toISOString());
  });

  it('refuses a wallet with no launches, and rolls the transaction back', async () => {
    // The token that triggered the job was committed in the transaction that enqueued
    // it, so an absent wallet is not a race — it is a payload that does not describe
    // anything, and waiting will not make it describe something.
    //
    // The throw happens *inside* the transaction, which `transactResult` turns into a
    // failure Result. `handle` is what puts it back on the queue's path as a throw.
    const { store, tokenIds } = await discovered({ address: 'NoCreator1' });
    const { handler, uow } = handlerOn(store);
    const payload = payloadFor(tokenIds[0] as TokenId);

    const result = await handler.enrich(payload);

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error).toBeInstanceOf(PermanentJobFailure);
    }
    expect(uow.store.profiles.size).toBe(0);
    expect(announced(uow.store)).toHaveLength(0);

    await expect(
      handler.handle(jobFor(QueueName.TOKEN_CREATOR, payload, TOKEN_CREATOR_PAYLOAD_VERSION))
    ).rejects.toThrow(PermanentJobFailure);
  });

  it('stores nothing when the event could not be appended', async () => {
    const { store, tokenIds } = await discovered({
      address: 'Creator1Mint1',
      creatorWallet: CREATOR,
    });
    const { handler, uow } = handlerOn(store, true);

    const result = await handler.enrich(payloadFor(tokenIds[0] as TokenId));

    expect(result.kind).toBe('failure');
    expect(uow.store.profiles.size).toBe(0);
  });

  it('refuses a payload it cannot read', async () => {
    const { store, tokenIds } = await discovered({
      address: 'Creator1Mint1',
      creatorWallet: CREATOR,
    });
    const { handler } = handlerOn(store);
    const token = tokenIds[0] as TokenId;

    await expect(
      handler.handle(jobFor(QueueName.TOKEN_CREATOR, payloadFor(token), '2'))
    ).rejects.toThrow(PermanentJobFailure);
    await expect(handler.enrich({ ...payloadFor(token), creatorWallet: '  ' })).rejects.toThrow(
      PermanentJobFailure
    );
    await expect(
      handler.enrich({ ...payloadFor(token), chain: 'ethereum' as Chain })
    ).rejects.toThrow(PermanentJobFailure);
  });

  it('calls no provider at all', async () => {
    // The only one of the three whose answer is already in this database. That is
    // what makes it the most straightforwardly idempotent, and it is why the
    // constructor takes no adapter — there is nothing to inject.
    const { store, tokenIds } = await discovered({
      address: 'Creator1Mint1',
      creatorWallet: CREATOR,
    });
    const { handler, uow } = handlerOn(store);

    await handler.enrich(payloadFor(tokenIds[0] as TokenId));

    // Both reads and the write in one transaction, so a concurrent discovery cannot
    // make the count and the profile it is compared against describe two different
    // states of `tokens`.
    expect(uow.transactions).toBe(1);
    expect(handler.queueName).toBe(QueueName.TOKEN_CREATOR);
  });
});

describe('enrichmentJobsFor', () => {
  function normalized(overrides: Parameters<typeof launch>[0] = {}): NormalizedTokenLaunch {
    return value(
      normalizeSourceLaunch(Chain.SOLANA, LaunchSource.PUMP_FUN, launch(overrides), DISCOVERED)
    );
  }

  it('always plans the market job, and plans it first', () => {
    // Unconditional because every token has a market; first because it is the one
    // enrichment that applies to all of them, and a planner that appended it last
    // would put the universal job behind two conditional ones.
    const requests = enrichmentJobsFor(normalized());

    expect(requests.map((request) => request.queueName)).toEqual([QueueName.TOKEN_LIQUIDITY]);
    expect(requests[0]?.payloadVersion).toBe(TOKEN_LIQUIDITY_PAYLOAD_VERSION);
  });

  it('plans artwork and creator jobs only when the launch supplied what they need', () => {
    const full = normalized({
      address: 'Full1',
      artworkUrl: ARTWORK_URL,
      creatorWallet: CREATOR,
    });

    expect(enrichmentJobsFor(full).map((request) => request.queueName)).toEqual([
      QueueName.TOKEN_LIQUIDITY,
      QueueName.TOKEN_ARTWORK,
      QueueName.TOKEN_CREATOR,
    ]);
    expect(
      enrichmentJobsFor(normalized({ address: 'Art1', artworkUrl: ARTWORK_URL })).map(
        (request) => request.queueName
      )
    ).toEqual([QueueName.TOKEN_LIQUIDITY, QueueName.TOKEN_ARTWORK]);
    expect(
      enrichmentJobsFor(normalized({ address: 'Wallet1', creatorWallet: CREATOR })).map(
        (request) => request.queueName
      )
    ).toEqual([QueueName.TOKEN_LIQUIDITY, QueueName.TOKEN_CREATOR]);
  });

  it('keys every job by token, and the market job by token and occasion', () => {
    // The `:launch` discriminator is load-bearing. Artwork and creator profiles are
    // recomputed by overwriting a row, so one job per token is all there will ever
    // be — but snapshots accumulate, and a later re-poll deriving its key the same
    // way would collide with this one forever and the token would be polled exactly
    // once in its life.
    const token = normalized({
      address: 'Keys1',
      artworkUrl: ARTWORK_URL,
      creatorWallet: CREATOR,
    });
    const keys = enrichmentJobsFor(token).map((request) => request.idempotencyKey);

    expect(keys).toEqual([
      `token_liquidity:${token.tokenId}:launch`,
      `token_artwork:${token.tokenId}`,
      `token_creator:${token.tokenId}`,
    ]);
  });

  it('carries what each worker needs and nothing it does not', () => {
    // Copied rather than read back, which is safe only because `tokens` is
    // append-only and its columns are never rewritten: a copied address cannot go
    // stale the way a copied liquidity figure could.
    const token = normalized({
      address: 'Payloads1',
      artworkUrl: ARTWORK_URL,
      creatorWallet: CREATOR,
    });
    const [market, artwork, creator] = enrichmentJobsFor(token);

    expect(market?.payload).toEqual({
      tokenId: token.tokenId,
      chain: Chain.SOLANA,
      address: 'Payloads1',
      venue: MarketVenue.PUMP_FUN_BONDING_CURVE,
    });
    expect(artwork?.payload).toEqual({
      tokenId: token.tokenId,
      chain: Chain.SOLANA,
      address: 'Payloads1',
      artworkUrl: ARTWORK_URL,
    });
    // No address: the wallet is what identifies the profile, and the recompute reads
    // `tokens` by creator rather than by mint.
    expect(creator?.payload).toEqual({
      tokenId: token.tokenId,
      chain: Chain.SOLANA,
      creatorWallet: CREATOR,
    });
    expect(artwork?.payloadVersion).toBe(TOKEN_ARTWORK_PAYLOAD_VERSION);
    expect(creator?.payloadVersion).toBe(TOKEN_CREATOR_PAYLOAD_VERSION);
  });

  it('plans nothing for an artwork URL the canonical model collapsed to absent', () => {
    // `normalizeSourceLaunch` turns an empty string into an absent field, so a
    // download of nothing cannot reach the planner.
    const requests = enrichmentJobsFor(normalized({ address: 'Blank1', artworkUrl: '   ' }));

    expect(requests.map((request) => request.queueName)).toEqual([QueueName.TOKEN_LIQUIDITY]);
  });
});
