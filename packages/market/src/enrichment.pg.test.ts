import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgresEventStore } from '@nit/database-public';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import {
  Chain,
  EventType,
  MarketVenue,
  QueueName,
  Result,
  Timestamp,
  TokenId,
  success,
} from '@nit/domain';
import { EventEnvelope, PayloadSchemaRegistry } from '@nit/events';
import { BackoffPolicy, Job, JobQueue } from '@nit/queue';
import { SourceMarketSnapshot } from './enrichment-contracts';
import {
  ArtworkEnrichmentHandler,
  CreatorEnrichmentHandler,
  EnrichmentHandlerOptions,
  MarketEnrichmentHandler,
} from './enrichment-handlers';
import { TokenArtworkPayload, TokenCreatorPayload, TokenLiquidityPayload } from './enrichment-jobs';
import {
  PostgresCreatorProfileRepository,
  PostgresMarketSnapshotRepository,
  PostgresTokenArtworkRepository,
} from './enrichment-repository';
import { registerMarketSchemas } from './events';
import { LaunchOverrides, PUMP_FUN, StubMarketSource, launch, reading } from './in-memory.fixtures';
import { MarketDiscoveryService } from './service';
import { SyntheticArtworkAnalyser } from './synthetic-enrichment';
import { PostgresEnrichmentUnitOfWork, PostgresUnitOfWork } from './unit-of-work';

/**
 * Market intelligence against TIS ch.9's acceptance list, on a real PostgreSQL.
 *
 * Its four items are canonical token records, concurrent enrichment, deterministic
 * replay and correlation-ready events, and every one of them is a claim about the
 * database rather than about the handlers. `enrichment.test.ts` covers the decisions
 * the handlers make; nothing there can fail the way this suite can, because an
 * in-memory `Map` cannot round-trip a NUMERIC through its own scale, cannot refuse an
 * UPDATE, and cannot have two transactions reach one unique index at the same moment.
 *
 * Where a test would only pass or fail on timing it says so and asserts the converged
 * state instead. Nothing here sleeps.
 */

const PRODUCER = 'enrichment-pg-test';

/** Discovery, then the two observations, then analysis — distinct so order is legible. */
const DISCOVERED_AT: Timestamp = new Date('2026-08-20T09:00:06.000Z');
const OBSERVED_AT: Timestamp = new Date('2026-08-20T09:05:00.000Z');
const OBSERVED_LATER: Timestamp = new Date('2026-08-20T09:15:00.000Z');
const ANALYSED_AT: Timestamp = new Date('2026-08-20T09:10:00.000Z');

const ARTWORK_URL = 'https://cdn.invalid/pg-token.png';
const CREATOR = 'Creator111111111111111111111111111111111Cc';
const BACKOFF: BackoffPolicy = { baseMs: 1_000, maxMs: 60_000, factor: 2 };

/**
 * One reading with a value in every money column, chosen to be awkward.
 *
 * `priceUsd` uses all 18 of NUMERIC(38, 18)'s decimal places and `volume24hUsd` sits
 * at the last place NUMERIC(38, 6) can represent, because a column that silently
 * rounded would still look right against a figure like `100.5`.
 */
const EXACT_METRICS: Omit<SourceMarketSnapshot, 'observedAt' | 'raw'> = {
  priceUsd: '0.000000000000000123',
  liquidityUsd: '1000.5',
  marketCapUsd: '987654321.123456',
  volume24hUsd: '0.000001',
  holderCount: 42,
};

/** Unwrap a Result, failing the test with the underlying error rather than a cast. */
function unwrap<T>(result: Result<T, Error>): T {
  if (result.kind === 'failure') {
    throw result.error;
  }
  return result.value;
}

describe.skipIf(!hasTestDatabase())('Market intelligence against Postgres', () => {
  let harness: TestDatabase;
  let discovery: MarketDiscoveryService;
  let uow: PostgresEnrichmentUnitOfWork;
  let events: PostgresEventStore;
  let queue: JobQueue;

  /** Analysis runs at a fixed instant, so an event's `occurred_at` is exact. */
  const OPTIONS: EnrichmentHandlerOptions = { producer: PRODUCER, clock: () => ANALYSED_AT };

  /** Shared by the queue and the discovery service, so both agree on "now". */
  const discoveryClock = (): Timestamp => DISCOVERED_AT;

  beforeAll(async () => {
    harness = await createTestDatabase('enrichment');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  beforeEach(async () => {
    // The same list and the same reason as `discovery.pg.test.ts`: enrichment tables
    // first because their FKs point at `tokens`, so TRUNCATE needs no CASCADE, and
    // TRUNCATE fires no row-level DELETE trigger, so the append-only triggers on
    // `tokens` and `market_snapshots` do not block the reset.
    const truncated = await harness.db.query(
      `TRUNCATE nit_token_interpretations, token_artwork, market_snapshots, token_markets, creator_profiles, tokens,
                domain_events, job_history, durable_jobs RESTART IDENTITY`
    );
    expect(truncated.kind).toBe('success');

    queue = new JobQueue(harness.db, {
      leaseTimeoutMs: 30_000,
      defaultMaxAttempts: 3,
      backoff: BACKOFF,
      clock: discoveryClock,
    });
    discovery = new MarketDiscoveryService(new PostgresUnitOfWork(harness.db, queue), {
      producer: PRODUCER,
      clock: discoveryClock,
    });
    uow = new PostgresEnrichmentUnitOfWork(harness.db);
    events = new PostgresEventStore(harness.db);
  });

  /**
   * Discover a token for real rather than inserting one.
   *
   * The enrichment tables' foreign keys point at `tokens`, and the creator worker
   * recomputes from rows in it, so a fixture that wrote tokens directly would be
   * testing enrichment against a token shape discovery does not actually produce.
   */
  const discover = async (overrides: LaunchOverrides = {}): Promise<TokenId> => {
    const result = await discovery.discover(PUMP_FUN, launch(overrides));
    return unwrap(result).tokenId;
  };

  /** A token with an image and a creator, so all three workers have something to do. */
  const discoverFull = (address = 'Full1'): Promise<TokenId> =>
    discover({ address, artworkUrl: ARTWORK_URL, creatorWallet: CREATOR });

  const artworkOn = (unit: PostgresEnrichmentUnitOfWork = uow): ArtworkEnrichmentHandler =>
    new ArtworkEnrichmentHandler(new SyntheticArtworkAnalyser(), unit, OPTIONS);

  /**
   * A market handler over a scripted provider.
   *
   * Scripted rather than synthetic because `SyntheticMarketDataSource` derives its
   * observation instants from its own epoch, months before the discovery times here —
   * and a market event is stamped with the observation, so that would invert the log.
   */
  const marketOn = (
    readings: Array<Result<SourceMarketSnapshot, Error>>,
    unit: PostgresEnrichmentUnitOfWork = uow
  ): MarketEnrichmentHandler =>
    new MarketEnrichmentHandler(new StubMarketSource(readings), unit, OPTIONS);

  const creatorOn = (unit: PostgresEnrichmentUnitOfWork = uow): CreatorEnrichmentHandler =>
    new CreatorEnrichmentHandler(unit, OPTIONS);

  const artworkJob = (tokenId: TokenId, address = 'Full1'): TokenArtworkPayload => ({
    tokenId,
    chain: Chain.SOLANA,
    address,
    artworkUrl: ARTWORK_URL,
  });

  const marketJob = (tokenId: TokenId, address = 'Full1'): TokenLiquidityPayload => ({
    tokenId,
    chain: Chain.SOLANA,
    address,
    venue: MarketVenue.PUMP_FUN_BONDING_CURVE,
  });

  const creatorJob = (tokenId: TokenId): TokenCreatorPayload => ({
    tokenId,
    chain: Chain.SOLANA,
    creatorWallet: CREATOR,
  });

  const countOf = async (table: string): Promise<number> => {
    const result = await harness.db.query<{ count: string }>(`SELECT COUNT(*) FROM ${table}`);
    return Number(unwrap(result)[0]?.count ?? '0');
  };

  /** One token's history, in the order `PostgresEventStore` defines for replay. */
  const logFor = async (tokenId: TokenId): Promise<EventEnvelope[]> =>
    unwrap(await events.getByAggregateId(tokenId));

  const typesIn = async (tokenId: TokenId): Promise<EventType[]> =>
    (await logFor(tokenId)).map((event) => event.eventType);

  describe('canonical token records', () => {
    it('assembles the artwork, timeline and creator profile of one token', async () => {
      const tokenId = await discoverFull();

      const artwork = unwrap(await artworkOn().enrich(artworkJob(tokenId)));
      const market = unwrap(
        await marketOn([success(reading(OBSERVED_AT, EXACT_METRICS))]).enrich(marketJob(tokenId))
      );
      const creator = unwrap(await creatorOn().enrich(creatorJob(tokenId)));

      expect(artwork.outcome).toBe('created');
      expect(market.stored).toBe(true);
      expect(creator.launchCount).toBe(1);

      // Read back through the repositories rather than by raw SELECT: the row mapping
      // is part of what this suite is for, since a column renamed on one side only
      // would still store and still read and simply return undefined forever.
      const storedArtwork = unwrap(
        await new PostgresTokenArtworkRepository(harness.db).findByToken(tokenId)
      );
      expect(storedArtwork?.tokenId).toBe(tokenId);
      expect(storedArtwork?.sourceUrl).toBe(ARTWORK_URL);
      expect(storedArtwork?.analysedAt).toEqual(ANALYSED_AT);
      expect(storedArtwork?.sha256).toBeDefined();

      const timeline = unwrap(
        await new PostgresMarketSnapshotRepository(harness.db).listForToken(tokenId, 10)
      );
      expect(timeline).toHaveLength(1);
      expect(timeline[0]?.tokenId).toBe(tokenId);
      expect(timeline[0]?.observedAt).toEqual(OBSERVED_AT);
      // Defaulted by the database, not by the handler: it records when this system
      // wrote the row, which is the one timestamp a provider must not be able to set.
      expect(timeline[0]?.recordedAt).toBeInstanceOf(Date);

      const profile = unwrap(
        await new PostgresCreatorProfileRepository(harness.db).findByWallet(Chain.SOLANA, CREATOR)
      );
      expect(profile?.creatorWallet).toBe(CREATOR);
      expect(profile?.launchCount).toBe(1);
      expect(profile?.analysedAt).toEqual(ANALYSED_AT);
    });

    it('keeps every money column exact through the database and back', async () => {
      const tokenId = await discoverFull();

      unwrap(
        await marketOn([success(reading(OBSERVED_AT, EXACT_METRICS))]).enrich(marketJob(tokenId))
      );

      const stored = unwrap(
        await new PostgresMarketSnapshotRepository(harness.db).findLatest(
          tokenId,
          MarketVenue.PUMP_FUN_BONDING_CURVE,
          'stub-market-source'
        )
      );

      // Text in, text out, no `Number` anywhere between: NUMERIC(38, 18) does not fit
      // a double, and a price with 18 decimal places is the normal case for a token
      // minutes old rather than an edge one.
      expect(stored?.priceUsd).toBe('0.000000000000000123');
      expect(stored?.marketCapUsd).toBe('987654321.123456');
      expect(stored?.volume24hUsd).toBe('0.000001');
      // Padded to the column's scale, which is the whole reason `decimalsEqual` exists.
      expect(stored?.liquidityUsd).toBe('1000.500000');
      // The one number that is parsed, because a holder count fits a double exactly.
      expect(stored?.holderCount).toBe(42);
    });

    it('reports no move when the database returns the same amount at its own scale', async () => {
      // The test only a real database can fail, and the reason `liquidityChanged`
      // compares with `decimalsEqual` instead of `===`. The stored reading comes back
      // from NUMERIC(38, 6) as `1000.500000`; the provider says `1000.5`. Compared as
      // text that is a liquidity move on every poll of a token whose liquidity has
      // never moved — an alert storm, and one no in-memory double would ever produce.
      const tokenId = await discoverFull();
      const handler = marketOn([
        success(reading(OBSERVED_AT, { liquidityUsd: '1000.5' })),
        // The same figure, written the way a provider writes it, which is the only way
        // this test discriminates: scripting the second reading as `1000.500000` would
        // make it string-identical to the stored value and `===` would pass too.
        success(reading(OBSERVED_LATER, { liquidityUsd: '1000.5' })),
      ]);

      unwrap(await handler.enrich(marketJob(tokenId)));
      const second = unwrap(await handler.enrich(marketJob(tokenId)));

      // Stored and announced as a reading, because it happened — but not as a move.
      expect(second.stored).toBe(true);
      expect(second.liquidityChanged).toBe(false);
      expect(await countOf('market_snapshots')).toBe(2);
      expect(await typesIn(tokenId)).not.toContain(EventType.LIQUIDITY_UPDATED);
    });
  });

  describe('concurrent enrichment', () => {
    it('runs the three workers against one token at the same time', async () => {
      // TIS ch.9's pipeline puts the artwork, liquidity and creator workers in
      // parallel off a persisted token. Three separate units of work, so three pooled
      // connections and three real transactions — and they all write `domain_events`,
      // which is where a shared-key deadlock would show up if one existed.
      const tokenId = await discoverFull();

      const outcomes = await Promise.all([
        artworkOn(new PostgresEnrichmentUnitOfWork(harness.db)).enrich(artworkJob(tokenId)),
        marketOn(
          [success(reading(OBSERVED_AT, EXACT_METRICS))],
          new PostgresEnrichmentUnitOfWork(harness.db)
        ).enrich(marketJob(tokenId)),
        creatorOn(new PostgresEnrichmentUnitOfWork(harness.db)).enrich(creatorJob(tokenId)),
      ]);

      for (const outcome of outcomes) {
        expect(outcome.kind).toBe('success');
      }
      expect(await countOf('token_artwork')).toBe(1);
      expect(await countOf('market_snapshots')).toBe(1);
      expect(await countOf('creator_profiles')).toBe(1);

      const types = await typesIn(tokenId);
      expect(types).toContain(EventType.TOKEN_ARTWORK_PROCESSED);
      expect(types).toContain(EventType.MARKET_SNAPSHOT_CREATED);
      expect(types).toContain(EventType.CREATOR_PROFILE_UPDATED);
    });

    it('converges on one artwork row when two workers analyse the same token at once', async () => {
      const tokenId = await discoverFull();

      const [first, second] = await Promise.all([
        artworkOn(new PostgresEnrichmentUnitOfWork(harness.db)).enrich(artworkJob(tokenId)),
        artworkOn(new PostgresEnrichmentUnitOfWork(harness.db)).enrich(artworkJob(tokenId)),
      ]);

      // UNIQUE (token_id) is the guarantee, and it holds whichever transaction commits
      // first: the loser's ON CONFLICT DO UPDATE blocks on the row lock and then writes
      // over the winner, so both callers are handed the identifier that survived.
      expect(unwrap(first).artworkId).toBe(unwrap(second).artworkId);
      expect(await countOf('token_artwork')).toBe(1);

      // Deliberately a range. Both transactions read `findByToken` before either wrote,
      // so under READ COMMITTED both can see no row and both can announce a creation;
      // if the second happens to read after the first commits it reports `unchanged`
      // and announces nothing. Pinning either number would be pinning a race. The
      // guarantee that matters is at-least-once, which the log satisfies either way.
      const announced = (await typesIn(tokenId)).filter(
        (type) => type === EventType.TOKEN_ARTWORK_PROCESSED
      );
      expect(announced.length).toBeGreaterThanOrEqual(1);
      expect(announced.length).toBeLessThanOrEqual(2);
    });

    it('stores one snapshot when two workers report the same observation at once', async () => {
      // This one *is* deterministic, and the reason is the constraint rather than the
      // ordering: `market_snapshots` is append-only, so the repository inserts with ON
      // CONFLICT DO NOTHING and reads the existing identifier back. Exactly one of the
      // two attempts can be the one that inserted.
      const tokenId = await discoverFull();
      const observation = (): Array<Result<SourceMarketSnapshot, Error>> => [
        success(reading(OBSERVED_AT, { liquidityUsd: '4200.000000' })),
      ];

      const outcomes = await Promise.all([
        marketOn(observation(), new PostgresEnrichmentUnitOfWork(harness.db)).enrich(
          marketJob(tokenId)
        ),
        marketOn(observation(), new PostgresEnrichmentUnitOfWork(harness.db)).enrich(
          marketJob(tokenId)
        ),
      ]);

      const stored = outcomes.map((outcome) => unwrap(outcome).stored);
      expect(stored.filter(Boolean)).toHaveLength(1);
      expect(await countOf('market_snapshots')).toBe(1);
      // The snapshot is announced by whichever attempt stored it, and only by that one.
      expect(
        (await typesIn(tokenId)).filter((type) => type === EventType.MARKET_SNAPSHOT_CREATED)
      ).toHaveLength(1);
    });

    it('keeps one creator profile when two tokens of a wallet are enriched at once', async () => {
      // Both jobs exist because the idempotency key is per token — that is the point of
      // keying it that way — so two recomputes for one wallet running together is the
      // normal case for a wallet launching twice, not an unlucky one.
      const first = await discoverFull('Wallet1');
      const second = await discover({
        address: 'Wallet2',
        creatorWallet: CREATOR,
        launchTime: new Date('2026-08-20T09:02:00.000Z'),
      });

      const outcomes = await Promise.all([
        creatorOn(new PostgresEnrichmentUnitOfWork(harness.db)).enrich(creatorJob(first)),
        creatorOn(new PostgresEnrichmentUnitOfWork(harness.db)).enrich(creatorJob(second)),
      ]);

      // UNIQUE (chain, creator_wallet) with DO UPDATE, so the second waits and then
      // overwrites with a count computed from the same append-only `tokens` rows.
      for (const outcome of outcomes) {
        expect(unwrap(outcome).launchCount).toBe(2);
      }
      expect(await countOf('creator_profiles')).toBe(1);

      const profile = unwrap(
        await new PostgresCreatorProfileRepository(harness.db).findByWallet(Chain.SOLANA, CREATOR)
      );
      expect(profile?.launchCount).toBe(2);
      expect(profile?.lastLaunchAt).toEqual(new Date('2026-08-20T09:02:00.000Z'));
    });
  });

  describe('append-only snapshots', () => {
    it('refuses to rewrite or delete a stored snapshot', async () => {
      // TIS ch.9's invariant, and ch.13 depends on it: a metric rewritten after the
      // fact silently changes the result of a backtest that has already been run.
      // `migrations.test.ts` asserts the trigger is declared; this asserts it fires.
      const tokenId = await discoverFull();
      unwrap(
        await marketOn([success(reading(OBSERVED_AT, EXACT_METRICS))]).enrich(marketJob(tokenId))
      );

      const rewritten = await harness.db.query(
        `UPDATE market_snapshots SET liquidity_usd = '1' WHERE token_id = $1`,
        [tokenId]
      );
      expect(rewritten.kind).toBe('failure');
      if (rewritten.kind === 'failure') {
        expect(rewritten.error.message).toContain('append-only');
      }

      const deleted = await harness.db.query(`DELETE FROM market_snapshots WHERE token_id = $1`, [
        tokenId,
      ]);
      expect(deleted.kind).toBe('failure');

      const survivor = unwrap(
        await new PostgresMarketSnapshotRepository(harness.db).findLatest(
          tokenId,
          MarketVenue.PUMP_FUN_BONDING_CURVE,
          'stub-market-source'
        )
      );
      expect(survivor?.liquidityUsd).toBe('1000.500000');
    });
  });

  describe('deterministic replay', () => {
    it('orders the history of one token by when each thing was true', async () => {
      const tokenId = await discoverFull();

      // Artwork runs *before* the market poll, and the assertion below puts its event
      // after. That inversion is the test: run the three in the order the log expects
      // and it would pass just as well against a store that ordered by insertion, which
      // is the one thing worth ruling out here.
      unwrap(await artworkOn().enrich(artworkJob(tokenId)));
      unwrap(
        await marketOn([success(reading(OBSERVED_AT, EXACT_METRICS))]).enrich(marketJob(tokenId))
      );
      unwrap(await creatorOn().enrich(creatorJob(tokenId)));

      // A market event is stamped with the instant the market was observed and the other
      // two with the instant analysis ran, so the log reconstructs the token's timeline
      // rather than this system's polling schedule — which is what ch.13 replays.
      const log = await logFor(tokenId);
      expect(log.map((event) => event.eventType)).toEqual([
        EventType.TOKEN_DISCOVERED,
        EventType.MARKET_SNAPSHOT_CREATED,
        EventType.TOKEN_ARTWORK_PROCESSED,
        EventType.CREATOR_PROFILE_UPDATED,
      ]);
      expect(log.map((event) => event.occurredAt)).toEqual([
        DISCOVERED_AT,
        OBSERVED_AT,
        ANALYSED_AT,
        ANALYSED_AT,
      ]);
      // The last two share an instant exactly, so their order is the `sequence`
      // tie-break rather than the timestamp — artwork was appended first, so it reads
      // back first. Without that tie-break the order would be the query plan's to
      // choose, and replay may not depend on the query plan.
      for (const event of log) {
        expect(event.aggregateId).toBe(tokenId);
        expect(event.producer).toBe(PRODUCER);
      }
    });

    it('appends nothing when every worker runs a second time', async () => {
      // What makes replay deterministic is that the log is a function of what happened
      // and not of how many times a job was delivered. Redelivery is normal — a lease
      // expires whenever a worker is killed mid-job — so this is the ordinary path.
      const tokenId = await discoverFull();
      const readings = (): Array<Result<SourceMarketSnapshot, Error>> => [
        success(reading(OBSERVED_AT, EXACT_METRICS)),
      ];

      unwrap(await marketOn(readings()).enrich(marketJob(tokenId)));
      unwrap(await artworkOn().enrich(artworkJob(tokenId)));
      unwrap(await creatorOn().enrich(creatorJob(tokenId)));
      const before = await logFor(tokenId);

      unwrap(await marketOn(readings()).enrich(marketJob(tokenId)));
      unwrap(await artworkOn().enrich(artworkJob(tokenId)));
      unwrap(await creatorOn().enrich(creatorJob(tokenId)));
      const after = await logFor(tokenId);

      // The same events, not merely the same number of them: identical `eventId`s prove
      // no second announcement was appended rather than one having replaced another.
      expect(after.map((event) => event.eventId)).toEqual(before.map((event) => event.eventId));
      expect(await countOf('token_artwork')).toBe(1);
      expect(await countOf('market_snapshots')).toBe(1);
      expect(await countOf('creator_profiles')).toBe(1);
    });
  });

  describe('correlation-ready events', () => {
    it('validates every stored payload against its registered schema', async () => {
      // Correlation reads `domain_events`, so what has to conform is the payload as
      // *stored* — after JSON.stringify dropped absent fields and JSONB handed the rest
      // back. Validating the object the builder returned would prove nothing about
      // that, and `PostgresEventStore.append` does no validation of its own.
      const registry = new PayloadSchemaRegistry();
      registerMarketSchemas(registry);

      const tokenId = await discoverFull();
      const handler = marketOn([
        success(reading(OBSERVED_AT, EXACT_METRICS)),
        // A different amount at a later instant, so `LiquidityUpdated` is emitted too
        // and all five of this stage's event types end up in the log.
        success(reading(OBSERVED_LATER, { ...EXACT_METRICS, liquidityUsd: '2500.25' })),
      ]);
      unwrap(await handler.enrich(marketJob(tokenId)));
      unwrap(await handler.enrich(marketJob(tokenId)));
      unwrap(await artworkOn().enrich(artworkJob(tokenId)));
      unwrap(await creatorOn().enrich(creatorJob(tokenId)));

      const log = await logFor(tokenId);
      for (const event of log) {
        // Throws on an unregistered event type as well as on a bad payload, so this
        // covers "published but never registered" — an event reaching the log unchecked.
        expect(() => registry.validate(event)).not.toThrow();
      }

      // Asserted so the loop above cannot pass by validating nothing, and so a sixth
      // event type added later fails here rather than silently going unvalidated.
      expect([...new Set(log.map((event) => event.eventType))].sort()).toEqual(
        [
          EventType.TOKEN_DISCOVERED,
          EventType.MARKET_SNAPSHOT_CREATED,
          EventType.LIQUIDITY_UPDATED,
          EventType.TOKEN_ARTWORK_PROCESSED,
          EventType.CREATOR_PROFILE_UPDATED,
        ].sort()
      );

      // Every payload names the token, which is the join correlation performs. The
      // envelope's `aggregateId` carries it too, but a consumer reading only payloads
      // out of a stream would have nothing to key on.
      for (const event of log) {
        expect((event.payload as { tokenId?: string }).tokenId).toBe(tokenId);
      }
    });
  });

  describe('the jobs discovery enqueued', () => {
    it('hands the artwork worker a payload it can run', async () => {
      // The seam between the outbox and these handlers. Discovery writes the payload,
      // JSONB stores it, `claim` reads it back, and the handler is typed against a
      // shape nothing has checked end to end until here — a planner field renamed on
      // one side only would pass every other test in this package.
      const tokenId = await discoverFull();

      const claimed = unwrap(await queue.claim(QueueName.TOKEN_ARTWORK, 'worker-1'));
      if (claimed === null) {
        throw new Error('discovery enqueued no artwork job');
      }
      expect(claimed.idempotencyKey).toBe(`token_artwork:${tokenId}`);

      // `handle`, not `enrich`: the version guard is part of what the queue calls, and
      // the payload it guards is the one the database just handed back.
      await artworkOn().handle(claimed as Job<TokenArtworkPayload>);

      const stored = unwrap(
        await new PostgresTokenArtworkRepository(harness.db).findByToken(tokenId)
      );
      expect(stored?.sourceUrl).toBe(ARTWORK_URL);
      expect(await typesIn(tokenId)).toContain(EventType.TOKEN_ARTWORK_PROCESSED);
    });
  });
});
