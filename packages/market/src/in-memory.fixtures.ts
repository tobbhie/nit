import {
  Chain,
  CreatorProfileId,
  JobStatus,
  LaunchSource,
  Result,
  Timestamp,
  TokenArtworkId,
  TokenId,
  failure,
  generateJobId,
  success,
} from '@nit/domain';
import { EventEnvelope, EventStore } from '@nit/events';
import { EnqueueRequest, Job } from '@nit/queue';
import { LaunchFetchRequest, LaunchSourceAdapter, SourceTokenLaunch } from './contracts';
import {
  JobEnqueuer,
  MarketDataAdapter,
  MarketDataRequest,
  SourceMarketSnapshot,
} from './enrichment-contracts';
import {
  CreatorLaunchStats,
  CreatorProfile,
  CreatorProfileRepository,
  CreatorProfileUpsert,
  MarketSnapshot,
  MarketSnapshotInsert,
  MarketSnapshotInsertOutcome,
  MarketSnapshotRepository,
  TokenArtwork,
  TokenArtworkRepository,
  TokenArtworkUpsert,
} from './enrichment-repository';
import { NormalizedTokenLaunch } from './normalize';
import { TokenInsertOutcome, TokenRepository, TokenSearch } from './repository';
import { LaunchOrigin } from './service';
import { Token } from './types';
import {
  DiscoveryUnitOfWork,
  EnrichmentTransactor,
  EnrichmentUnitOfWork,
  UnitOfWork,
} from './unit-of-work';

/**
 * In-memory doubles shared by this package's tests.
 *
 * Excluded from the build (see this package's tsconfig) so nothing ships them,
 * but compiled and linted like any other source file, which is what keeps them
 * honest about the interfaces they claim to implement.
 */

export const LAUNCHED: Timestamp = new Date('2026-08-20T09:00:00.000Z');
export const DISCOVERED: Timestamp = new Date('2026-08-20T09:00:06.000Z');

/** The origin every test uses unless it is specifically about provenance. */
export const PUMP_FUN: LaunchOrigin = {
  chain: Chain.SOLANA,
  launchSource: LaunchSource.PUMP_FUN,
};

export interface LaunchOverrides {
  address?: string;
  ticker?: string;
  name?: string;
  launchTime?: Timestamp;
  creatorWallet?: string;
  description?: string;
  artworkUrl?: string;
}

/**
 * Build a source launch.
 *
 * Takes an explicit override interface rather than `Partial<SourceTokenLaunch>`
 * because spreading a Partial widens every optional field to `| undefined`, which
 * `exactOptionalPropertyTypes` then refuses to assign back.
 *
 * The default address is mixed-case on purpose: a base58 mint address is
 * case-sensitive, so a fixture that happened to be all-lowercase would let a
 * case-folding bug through unnoticed.
 */
export function launch(overrides: LaunchOverrides = {}): SourceTokenLaunch {
  const address = overrides.address ?? 'So11111111111111111111111111111111111111Aa';
  const built: SourceTokenLaunch = {
    address,
    ticker: overrides.ticker ?? 'TICK',
    name: overrides.name ?? 'Tick Token',
    launchTime: overrides.launchTime ?? LAUNCHED,
    raw: { mint: address, source: 'fixture' },
  };
  if (overrides.creatorWallet !== undefined) {
    built.creatorWallet = overrides.creatorWallet;
  }
  if (overrides.description !== undefined) {
    built.description = overrides.description;
  }
  if (overrides.artworkUrl !== undefined) {
    built.artworkUrl = overrides.artworkUrl;
  }
  return built;
}

/**
 * Shared mutable state behind the doubles, so `FakeUnitOfWork` can snapshot and
 * restore it. Without that, a "rolled back" assertion would prove nothing.
 */
export class Store {
  tokens = new Map<string, NormalizedTokenLaunch>();
  events: EventEnvelope[] = [];
  /** Keyed by token, matching `token_artwork`'s UNIQUE (token_id). */
  artwork = new Map<string, TokenArtwork>();
  /** A list, not a map: `market_snapshots` is append-only history. */
  snapshots: MarketSnapshot[] = [];
  /** Keyed `chain/wallet`, matching `creator_profiles`' UNIQUE (chain, creator_wallet). */
  profiles = new Map<string, CreatorProfile>();
  /** Keyed `queue/idempotencyKey`, matching the queue's own unique constraint. */
  jobs = new Map<string, Job>();

  /** Token identity, exactly as the database's unique constraint defines it. */
  key(chain: Chain, address: string): string {
    return `${chain}/${address}`;
  }
}

export class InMemoryTokenRepository implements TokenRepository {
  constructor(private readonly store: Store) {}

  insert(token: NormalizedTokenLaunch): Promise<Result<TokenInsertOutcome, Error>> {
    const key = this.store.key(token.chain, token.address);
    const existing = this.store.tokens.get(key);
    if (existing !== undefined) {
      return Promise.resolve(
        success<TokenInsertOutcome>({ inserted: false, tokenId: existing.tokenId })
      );
    }

    this.store.tokens.set(key, token);
    return Promise.resolve(
      success<TokenInsertOutcome>({ inserted: true, tokenId: token.tokenId })
    );
  }

  findById(tokenId: TokenId): Promise<Result<Token | null, Error>> {
    for (const stored of this.store.tokens.values()) {
      if (stored.tokenId === tokenId) {
        return Promise.resolve(success(toToken(stored)));
      }
    }
    return Promise.resolve(success(null));
  }

  findByAddress(chain: Chain, address: string): Promise<Result<Token | null, Error>> {
    const stored = this.store.tokens.get(this.store.key(chain, address));
    return Promise.resolve(success(stored === undefined ? null : toToken(stored)));
  }

  search(criteria: TokenSearch): Promise<Result<Token[], Error>> {
    // Newest launch first with the address as tie-break, matching the Postgres
    // repository's ORDER BY. Case-insensitive on ticker and name, matching its
    // LOWER(...) predicates.
    const matches = [...this.store.tokens.values()]
      .filter((stored) => {
        if (stored.chain !== criteria.chain) {
          return false;
        }
        if (
          criteria.ticker !== undefined &&
          stored.ticker.toLowerCase() !== criteria.ticker.trim().toLowerCase()
        ) {
          return false;
        }
        if (
          criteria.name !== undefined &&
          stored.name.toLowerCase() !== criteria.name.trim().toLowerCase()
        ) {
          return false;
        }
        if (
          criteria.launchedSince !== undefined &&
          stored.launchTime.getTime() < criteria.launchedSince.getTime()
        ) {
          return false;
        }
        return true;
      })
      .sort((left, right) => {
        const byTime = right.launchTime.getTime() - left.launchTime.getTime();
        return byTime !== 0 ? byTime : right.address.localeCompare(left.address);
      })
      .slice(0, criteria.limit)
      .map(toToken);
    return Promise.resolve(success(matches));
  }
}

/**
 * Project a stored launch down to the canonical token, dropping `rawPayload` the
 * way the real repository's SELECT does by not listing it.
 */
function toToken(stored: NormalizedTokenLaunch): Token {
  const token: Token = {
    tokenId: stored.tokenId,
    chain: stored.chain,
    address: stored.address,
    ticker: stored.ticker,
    name: stored.name,
    launchTime: stored.launchTime,
    launchSource: stored.launchSource,
    discoveredAt: stored.discoveredAt,
  };
  if (stored.creatorWallet !== undefined) {
    token.creatorWallet = stored.creatorWallet;
  }
  if (stored.description !== undefined) {
    token.description = stored.description;
  }
  if (stored.artworkUrl !== undefined) {
    token.artworkUrl = stored.artworkUrl;
  }
  return token;
}

export class InMemoryEventStore implements EventStore {
  constructor(
    private readonly store: Store,
    private readonly appendFails: boolean
  ) {}

  append<TPayload>(event: EventEnvelope<TPayload>): Promise<Result<void, Error>> {
    if (this.appendFails) {
      return Promise.resolve(failure(new Error('event store unavailable')));
    }
    this.store.events.push(event as EventEnvelope);
    return Promise.resolve(success(undefined));
  }

  // Neither discovery nor enrichment reads events back. Throwing rather than
  // returning an empty result means a future caller finds out immediately instead
  // of quietly reading nothing.
  getByAggregateId(): Promise<Result<EventEnvelope[], Error>> {
    throw new Error('not used by the market pipelines');
  }

  getByEventType(): Promise<Result<EventEnvelope[], Error>> {
    throw new Error('not used by the market pipelines');
  }

  getById(): Promise<Result<EventEnvelope | null, Error>> {
    throw new Error('not used by the market pipelines');
  }

  getAfter(): Promise<Result<EventEnvelope[], Error>> {
    throw new Error('not used by the market pipelines');
  }
}

/**
 * Records what a transaction enqueued, deduplicating the way the real queue does.
 *
 * The dedup is the point rather than a detail: discovery's whole idempotency story
 * is that a redelivered poll re-plans the same keys and the second enqueue is
 * absorbed, so a double that appended blindly would let a broken planner pass.
 */
export class InMemoryJobEnqueuer implements JobEnqueuer {
  constructor(
    private readonly store: Store,
    private readonly enqueueFails = false
  ) {}

  enqueue<TPayload>(request: EnqueueRequest<TPayload>): Promise<Result<Job, Error>> {
    if (this.enqueueFails) {
      return Promise.resolve(failure(new Error('job queue unavailable')));
    }

    const key = `${request.queueName}/${request.idempotencyKey}`;
    const existing = this.store.jobs.get(key);
    if (existing !== undefined) {
      return Promise.resolve(success(existing));
    }

    const job: Job = {
      jobId: generateJobId(),
      queueName: request.queueName,
      payload: request.payload,
      payloadVersion: request.payloadVersion,
      status: JobStatus.PENDING,
      attemptCount: 0,
      maxAttempts: request.maxAttempts ?? 3,
      leaseDeadline: null,
      leasedBy: null,
      idempotencyKey: request.idempotencyKey,
      availableAt: request.availableAt ?? DISCOVERED,
      lastError: null,
      correlationId: request.correlationId ?? null,
      createdAt: DISCOVERED,
      updatedAt: DISCOVERED,
      completedAt: null,
    };
    this.store.jobs.set(key, job);
    return Promise.resolve(success(job));
  }
}

/** Mirrors `token_artwork`: one mutable row per token, filled in by COALESCE. */
export class InMemoryTokenArtworkRepository implements TokenArtworkRepository {
  constructor(private readonly store: Store) {}

  upsert(artwork: TokenArtworkUpsert): Promise<Result<TokenArtworkId, Error>> {
    const existing = this.store.artwork.get(artwork.tokenId);
    const merged: TokenArtwork = {
      // The stored identifier wins, exactly as `ON CONFLICT DO UPDATE` leaves it.
      artworkId: existing?.artworkId ?? artwork.artworkId,
      tokenId: artwork.tokenId,
      sourceUrl: artwork.sourceUrl,
      analysedAt: artwork.analysedAt,
      createdAt: existing?.createdAt ?? artwork.analysedAt,
    };

    // COALESCE(EXCLUDED.x, stored.x): an absent incoming field leaves what is
    // there, so a partial re-analysis fills gaps and erases nothing.
    const storageLocation = artwork.storageLocation ?? existing?.storageLocation;
    if (storageLocation !== undefined) {
      merged.storageLocation = storageLocation;
    }
    const sha256 = artwork.sha256 ?? existing?.sha256;
    if (sha256 !== undefined) {
      merged.sha256 = sha256;
    }
    const phash = artwork.phash ?? existing?.phash;
    if (phash !== undefined) {
      merged.phash = phash;
    }
    const ocrText = artwork.ocrText ?? existing?.ocrText;
    if (ocrText !== undefined) {
      merged.ocrText = ocrText;
    }

    this.store.artwork.set(artwork.tokenId, merged);
    return Promise.resolve(success(merged.artworkId));
  }

  findByToken(tokenId: TokenId): Promise<Result<TokenArtwork | null, Error>> {
    return Promise.resolve(success(this.store.artwork.get(tokenId) ?? null));
  }

  findByPhash(phash: string, limit: number): Promise<Result<TokenArtwork[], Error>> {
    const matches = [...this.store.artwork.values()]
      .filter((row) => row.phash === phash)
      .sort((left, right) => {
        const byTime = left.createdAt.getTime() - right.createdAt.getTime();
        return byTime !== 0 ? byTime : left.artworkId.localeCompare(right.artworkId);
      })
      .slice(0, limit);
    return Promise.resolve(success(matches));
  }
}

/** Mirrors `market_snapshots`: append-only, unique on token + source + observation. */
export class InMemoryMarketSnapshotRepository implements MarketSnapshotRepository {
  constructor(private readonly store: Store) {}

  insert(snapshot: MarketSnapshotInsert): Promise<Result<MarketSnapshotInsertOutcome, Error>> {
    const existing = this.store.snapshots.find(
      (row) =>
        row.tokenId === snapshot.tokenId &&
        row.source === snapshot.source &&
        row.observedAt.getTime() === snapshot.observedAt.getTime()
    );
    // The read-back the real repository performs after `DO NOTHING` returns no row.
    if (existing !== undefined) {
      return Promise.resolve(
        success<MarketSnapshotInsertOutcome>({ inserted: false, snapshotId: existing.snapshotId })
      );
    }

    const stored: MarketSnapshot = {
      snapshotId: snapshot.snapshotId,
      tokenId: snapshot.tokenId,
      observedAt: snapshot.observedAt,
      source: snapshot.source,
      recordedAt: DISCOVERED,
    };
    if (snapshot.priceUsd !== undefined) {
      stored.priceUsd = snapshot.priceUsd;
    }
    if (snapshot.liquidityUsd !== undefined) {
      stored.liquidityUsd = snapshot.liquidityUsd;
    }
    if (snapshot.marketCapUsd !== undefined) {
      stored.marketCapUsd = snapshot.marketCapUsd;
    }
    if (snapshot.volume24hUsd !== undefined) {
      stored.volume24hUsd = snapshot.volume24hUsd;
    }
    if (snapshot.holderCount !== undefined) {
      stored.holderCount = snapshot.holderCount;
    }

    this.store.snapshots.push(stored);
    return Promise.resolve(
      success<MarketSnapshotInsertOutcome>({ inserted: true, snapshotId: snapshot.snapshotId })
    );
  }

  findLatest(tokenId: TokenId, source: string): Promise<Result<MarketSnapshot | null, Error>> {
    const matches = this.forToken(tokenId).filter((row) => row.source === source);
    return Promise.resolve(success(matches[0] ?? null));
  }

  listForToken(tokenId: TokenId, limit: number): Promise<Result<MarketSnapshot[], Error>> {
    return Promise.resolve(success(this.forToken(tokenId).slice(0, limit)));
  }

  /** Newest observation first, matching the Postgres repository's ORDER BY. */
  private forToken(tokenId: TokenId): MarketSnapshot[] {
    return this.store.snapshots
      .filter((row) => row.tokenId === tokenId)
      .sort((left, right) => right.observedAt.getTime() - left.observedAt.getTime());
  }
}

/** Mirrors `creator_profiles`, including the aggregate over `tokens` that feeds it. */
export class InMemoryCreatorProfileRepository implements CreatorProfileRepository {
  constructor(private readonly store: Store) {}

  computeStats(
    chain: Chain,
    creatorWallet: string
  ): Promise<Result<CreatorLaunchStats | null, Error>> {
    const launches = [...this.store.tokens.values()].filter(
      (token) => token.chain === chain && token.creatorWallet === creatorWallet
    );
    if (launches.length === 0) {
      return Promise.resolve(success(null));
    }

    const times = launches.map((token) => token.launchTime.getTime());
    return Promise.resolve(
      success<CreatorLaunchStats>({
        launchCount: launches.length,
        firstLaunchAt: new Date(Math.min(...times)),
        lastLaunchAt: new Date(Math.max(...times)),
      })
    );
  }

  upsert(profile: CreatorProfileUpsert): Promise<Result<CreatorProfileId, Error>> {
    const key = this.store.key(profile.chain, profile.creatorWallet);
    const existing = this.store.profiles.get(key);
    // A straight overwrite, unlike artwork: every field was recomputed from
    // `tokens`, so the incoming values are never less complete than the stored ones.
    const stored: CreatorProfile = {
      creatorProfileId: existing?.creatorProfileId ?? profile.creatorProfileId,
      chain: profile.chain,
      creatorWallet: profile.creatorWallet,
      launchCount: profile.launchCount,
      firstLaunchAt: profile.firstLaunchAt,
      lastLaunchAt: profile.lastLaunchAt,
      analysedAt: profile.analysedAt,
    };
    this.store.profiles.set(key, stored);
    return Promise.resolve(success(stored.creatorProfileId));
  }

  findByWallet(
    chain: Chain,
    creatorWallet: string
  ): Promise<Result<CreatorProfile | null, Error>> {
    return Promise.resolve(
      success(this.store.profiles.get(this.store.key(chain, creatorWallet)) ?? null)
    );
  }
}

/**
 * Models the transaction, not just its interface: a failure Result restores the
 * pre-call snapshot. That is what lets a test assert a token was *not* persisted
 * when its event could not be appended — or that its enrichment jobs were not
 * enqueued, which is the half of the outbox a fake without `jobs` could not check.
 */
export class FakeUnitOfWork implements UnitOfWork {
  readonly store = new Store();
  transactions = 0;

  constructor(
    private readonly appendFails = false,
    private readonly enqueueFails = false
  ) {}

  async transact<T>(
    work: (uow: DiscoveryUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>> {
    this.transactions += 1;
    const snapshot = snapshotOf(this.store);

    const result = await work({
      tokens: new InMemoryTokenRepository(this.store),
      events: new InMemoryEventStore(this.store, this.appendFails),
      jobs: new InMemoryJobEnqueuer(this.store, this.enqueueFails),
    });

    if (result.kind === 'failure') {
      restore(this.store, snapshot);
    }
    return result;
  }
}

/**
 * The enrichment side of the same thing.
 *
 * Takes a store rather than owning one, because the creator worker's whole job is
 * an aggregate over `tokens`: a test has to be able to hand it the store discovery
 * just wrote to, or `computeStats` counts nothing.
 */
export class FakeEnrichmentUnitOfWork implements EnrichmentTransactor {
  transactions = 0;

  constructor(
    readonly store: Store = new Store(),
    private readonly appendFails = false
  ) {}

  async transact<T>(
    work: (uow: EnrichmentUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>> {
    this.transactions += 1;
    const snapshot = snapshotOf(this.store);

    // A handler may throw `PermanentJobFailure` from inside the transaction — the
    // creator worker does exactly that for a wallet with no launches. `transactResult`
    // runs on `DatabasePool.transaction`, which catches any throw, rolls back, and
    // hands the error back as a *failure Result*. So must this, or the fake would
    // reject where production returns, and a test asserting on the real behaviour of
    // `enrich` would have to be written backwards to pass here.
    let result: Result<T, Error>;
    try {
      result = await work({
        artwork: new InMemoryTokenArtworkRepository(this.store),
        snapshots: new InMemoryMarketSnapshotRepository(this.store),
        creators: new InMemoryCreatorProfileRepository(this.store),
        events: new InMemoryEventStore(this.store, this.appendFails),
      });
    } catch (error) {
      restore(this.store, snapshot);
      return failure(error as Error);
    }

    if (result.kind === 'failure') {
      restore(this.store, snapshot);
    }
    return result;
  }
}

interface StoreSnapshot {
  tokens: Map<string, NormalizedTokenLaunch>;
  events: EventEnvelope[];
  artwork: Map<string, TokenArtwork>;
  snapshots: MarketSnapshot[];
  profiles: Map<string, CreatorProfile>;
  jobs: Map<string, Job>;
}

/**
 * A shallow copy of every collection in the store.
 *
 * Shallow is enough because nothing in here is mutated in place — the repositories
 * above replace map entries and push rows rather than editing stored objects.
 */
function snapshotOf(store: Store): StoreSnapshot {
  return {
    tokens: new Map(store.tokens),
    events: [...store.events],
    artwork: new Map(store.artwork),
    snapshots: [...store.snapshots],
    profiles: new Map(store.profiles),
    jobs: new Map(store.jobs),
  };
}

function restore(store: Store, snapshot: StoreSnapshot): void {
  store.tokens = snapshot.tokens;
  store.events = snapshot.events;
  store.artwork = snapshot.artwork;
  store.snapshots = snapshot.snapshots;
  store.profiles = snapshot.profiles;
  store.jobs = snapshot.jobs;
}

/**
 * A launch source that answers from a script.
 *
 * Covers what a real launchpad does that a well-behaved fake never will — fail,
 * then recover, or hand back a launch the canonical model refuses. `requests`
 * records what it was asked for, which is how a test checks the window a poll
 * handler derived.
 */
export class StubLaunchSource implements LaunchSourceAdapter {
  readonly providerName = 'stub-launch-source';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1.0.0';
  readonly requests: LaunchFetchRequest[] = [];

  constructor(
    readonly chain: Chain,
    readonly launchSource: LaunchSource,
    private readonly answers: Array<Result<SourceTokenLaunch[], Error>>
  ) {}

  fetchRecentLaunches(request: LaunchFetchRequest): Promise<Result<SourceTokenLaunch[], Error>> {
    this.requests.push(request);
    // Past the end of the script the launchpad simply has nothing new, which is
    // what a quiet window looks like.
    return Promise.resolve(this.answers.shift() ?? success<SourceTokenLaunch[]>([]));
  }
}

/**
 * A market-data provider that answers from a script.
 *
 * `SyntheticMarketDataSource` covers the readings a working provider produces, and it
 * derives its metrics from the address, which is exactly what makes it unable to
 * express the cases that matter most here: a reading that arrives out of order, the
 * same amount reported at a different scale, or liquidity that was there last time and
 * is gone now. Each of those is a *relationship* between two readings, so a test has to
 * state both.
 *
 * The provider name is fixed rather than per-instance, because `market_snapshots` keys
 * on `(token_id, source, observed_at)` and the handler compares a reading against the
 * previous one *from the same source*. Two stubs in one test are two consecutive
 * readings from one provider, not two providers.
 */
export class StubMarketSource implements MarketDataAdapter {
  readonly providerName = 'stub-market-source';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1.0.0';
  readonly requests: MarketDataRequest[] = [];

  constructor(private readonly answers: Array<Result<SourceMarketSnapshot, Error>>) {}

  fetchSnapshot(request: MarketDataRequest): Promise<Result<SourceMarketSnapshot, Error>> {
    this.requests.push(request);
    const answer = this.answers.shift();
    // Unlike the launch source there is no benign empty answer: a market provider
    // with nothing to say still reports a reading, so running off the end of the
    // script is a test that asked for more fetches than it scripted.
    return Promise.resolve(
      answer ?? failure<Error>(new Error('StubMarketSource: no scripted reading left'))
    );
  }
}

/** A scripted reading, with the metrics a case does not care about left out. */
export function reading(
  observedAt: Timestamp,
  metrics: Omit<SourceMarketSnapshot, 'observedAt' | 'raw'> = {}
): SourceMarketSnapshot {
  return { observedAt, ...metrics, raw: { observedAt: observedAt.toISOString() } };
}
