import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgresEventStore } from '@nit/database-public';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import {
  Chain,
  EventType,
  LaunchSource,
  MarketVenue,
  QueueName,
  Timestamp,
  TokenId,
  failure,
  generateTokenId,
} from '@nit/domain';
import { BackoffPolicy, JobQueue } from '@nit/queue';
import { SourceTokenLaunch } from './contracts';
import {
  TOKEN_LIQUIDITY_PAYLOAD_VERSION,
  TokenArtworkPayload,
  TokenCreatorPayload,
  TokenLiquidityPayload,
} from './enrichment-jobs';
import { TokenDiscoveredPayload, tokenDiscoveredEvent } from './events';
import { LaunchOverrides, PUMP_FUN, launch } from './in-memory.fixtures';
import { normalizeSourceLaunch } from './normalize';
import { PostgresTokenRepository, TokenSearch } from './repository';
import { MarketDiscoveryService } from './service';
import { Token } from './types';
import { PostgresUnitOfWork } from './unit-of-work';

/**
 * Market discovery against a real PostgreSQL.
 *
 * Every claim this package makes about correctness is a claim about the database:
 * `(chain, address)` holding under concurrency so a graduated token stays one
 * token, the append-only trigger refusing a rewrite of raw launch data, a token
 * being returned by search with no enrichment rows in existence, an event landing
 * in the same transaction as the row it announces. An in-memory double can be
 * written to satisfy all of them and prove none, which is why this suite skips
 * when no test database is configured rather than substituting one.
 */

const PRODUCER = 'market-pg-test';
const DISCOVERED_AT: Timestamp = new Date('2026-08-20T09:00:06.000Z');
const PUMPSWAP = { chain: Chain.SOLANA, launchSource: LaunchSource.PUMPSWAP };
const BACKOFF: BackoffPolicy = { baseMs: 1_000, maxMs: 60_000, factor: 2 };

/** The search fields a test may vary; `chain` is fixed and `limit` defaulted. */
interface SearchOverrides {
  ticker?: string;
  name?: string;
  launchedSince?: Timestamp;
  limit?: number;
}

/** One row of `durable_jobs`, as the outbox assertions read it. */
interface EnqueuedJob {
  queue_name: string;
  idempotency_key: string;
  payload_version: string;
  /**
   * `unknown`, like `EventEnvelope.payload`, and not `Record<string, unknown>`: an
   * interface gets no implicit index signature, so neither direction of that
   * assignment holds and `as TokenArtworkPayload` is rejected outright. The column is
   * JSONB and the driver hands back whatever was stored, which is what `unknown` says.
   */
  payload: unknown;
}

describe.skipIf(!hasTestDatabase())('Market discovery against Postgres', () => {
  let harness: TestDatabase;
  let service: MarketDiscoveryService;
  let repository: PostgresTokenRepository;
  let events: PostgresEventStore;
  let queue: JobQueue;

  /** Frozen so discovery times are exact rather than racy. */
  const clock = (): Timestamp => DISCOVERED_AT;

  beforeAll(async () => {
    harness = await createTestDatabase('market');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  beforeEach(async () => {
    // The enrichment tables before tokens: their FKs point that way, and listing
    // all of them means TRUNCATE does not need CASCADE. TRUNCATE does not fire
    // row-level DELETE triggers, so the append-only trigger on tokens does not
    // block the reset. The queue tables are here because discovery now writes to
    // them: the enrichment jobs commit with the token.
    const truncated = await harness.db.query(
      `TRUNCATE token_artwork, market_snapshots, token_markets, creator_profiles, tokens,
                domain_events, job_history, durable_jobs RESTART IDENTITY`
    );
    expect(truncated.kind).toBe('success');

    queue = new JobQueue(harness.db, {
      leaseTimeoutMs: 30_000,
      defaultMaxAttempts: 3,
      backoff: BACKOFF,
      clock,
    });
    service = new MarketDiscoveryService(new PostgresUnitOfWork(harness.db, queue), {
      producer: PRODUCER,
      clock,
    });
    repository = new PostgresTokenRepository(harness.db);
    events = new PostgresEventStore(harness.db);
  });

  const discover = async (source: SourceTokenLaunch): Promise<TokenId> => {
    const result = await service.discover(PUMP_FUN, source);
    if (result.kind === 'failure') {
      throw result.error;
    }
    return result.value.tokenId;
  };

  const read = async (address: string): Promise<Token> => {
    const found = await repository.findByAddress(Chain.SOLANA, address);
    if (found.kind === 'failure') {
      throw found.error;
    }
    if (found.value === null) {
      throw new Error(`${address} was not stored`);
    }
    return found.value;
  };

  const countOf = async (table: string): Promise<number> => {
    const result = await harness.db.query<{ count: string }>(`SELECT COUNT(*) FROM ${table}`);
    if (result.kind === 'failure') {
      throw result.error;
    }
    return Number(result.value[0]?.count ?? '0');
  };

  /** Every enqueued job, ordered so assertions do not depend on insertion order. */
  const enqueuedJobs = async (): Promise<EnqueuedJob[]> => {
    const result = await harness.db.query<EnqueuedJob>(
      `SELECT queue_name, idempotency_key, payload_version, payload
         FROM durable_jobs ORDER BY queue_name`
    );
    if (result.kind === 'failure') {
      throw result.error;
    }
    return result.value;
  };

  /**
   * Search this chain, defaulting the limit.
   *
   * Explicit optional fields rather than `Partial<TokenSearch>` for the reason the
   * fixtures give: spreading a Partial widens every field to `| undefined`, and
   * `limit` is required, so the spread would not typecheck.
   */
  const search = async (criteria: SearchOverrides = {}): Promise<Token[]> => {
    const built: TokenSearch = { chain: Chain.SOLANA, limit: criteria.limit ?? 50 };
    if (criteria.ticker !== undefined) {
      built.ticker = criteria.ticker;
    }
    if (criteria.name !== undefined) {
      built.name = criteria.name;
    }
    if (criteria.launchedSince !== undefined) {
      built.launchedSince = criteria.launchedSince;
    }
    const found = await repository.search(built);
    if (found.kind === 'failure') {
      throw found.error;
    }
    return found.value;
  };

  describe('duplicate discovery is absorbed', () => {
    it('stores one token and one event when the same launch is re-seen', async () => {
      const first = await service.discover(PUMP_FUN, launch());
      const second = await service.discover(PUMP_FUN, launch());
      if (first.kind === 'failure' || second.kind === 'failure') {
        throw new Error('Expected both discoveries to succeed');
      }

      expect(first.value.status).toBe('discovered');
      expect(second.value.status).toBe('duplicate');
      expect(second.value.tokenId).toBe(first.value.tokenId);
      expect(await countOf('tokens')).toBe(1);
      expect(await countOf('domain_events')).toBe(1);
    });

    it('stores one token when two pollers race on the same address', async () => {
      // The point of putting identity in a UNIQUE constraint rather than a
      // read-then-write check: two workers can both pass the check. Overlapping
      // poll windows make this the normal case, not an edge case.
      const other = new MarketDiscoveryService(new PostgresUnitOfWork(harness.db, queue), {
        producer: PRODUCER,
        clock,
      });

      const [left, right] = await Promise.all([
        service.discover(PUMP_FUN, launch({ address: 'Contended1' })),
        other.discover(PUMP_FUN, launch({ address: 'Contended1' })),
      ]);
      if (left.kind === 'failure' || right.kind === 'failure') {
        throw new Error('Neither racing discovery should fail');
      }

      const statuses = [left.value.status, right.value.status].sort();
      expect(statuses).toEqual(['discovered', 'duplicate']);
      expect(left.value.tokenId).toBe(right.value.tokenId);
      expect(await countOf('tokens')).toBe(1);
      // One row, therefore one announcement.
      expect(await countOf('domain_events')).toBe(1);
    });

    it('treats a graduated token as the one it already knows', async () => {
      // A pump.fun mint reappears on PumpSwap when it graduates. Identity is
      // (chain, address) and not (launch_source, address) precisely so this stays
      // one token with one history instead of two with the evidence split.
      const mint = launch({ address: 'Graduate1' });
      const onPumpFun = await discover(mint);
      const graduatedAt = new Date(mint.launchTime.getTime() + 60_000);
      const onPumpSwap = await service.discover(
        PUMPSWAP,
        launch({
          address: mint.address,
          launchTime: graduatedAt,
          market: {
            marketAddress: 'PoolGraduate1',
            origin: {
              kind: 'chain',
              position: { transactionSignature: 'graduation-signature', logIndex: 7 },
              slot: 350_001_000,
            },
          },
        })
      );
      if (onPumpSwap.kind === 'failure') {
        throw onPumpSwap.error;
      }

      expect(onPumpSwap.value.status).toBe('graduated');
      expect(onPumpSwap.value.tokenId).toBe(onPumpFun);
      expect(await countOf('tokens')).toBe(1);
      // The venue it was first seen on is what stays recorded.
      expect((await read('Graduate1')).launchSource).toBe(LaunchSource.PUMP_FUN);
      expect(await countOf('token_markets')).toBe(2);
      expect(await countOf('domain_events')).toBe(2);
    });

    it('does not announce a duplicate a second time', async () => {
      await discover(launch({ address: 'Twice1' }));
      await service.discover(PUMP_FUN, launch({ address: 'Twice1' }));

      const appended = await events.getByEventType(EventType.TOKEN_DISCOVERED);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      expect(appended.value).toHaveLength(1);
    });
  });

  describe('the token and its announcement commit together', () => {
    it('writes the row and the event in one transaction', async () => {
      const tokenId = await discover(launch({ address: 'Together1' }));

      const appended = await events.getByAggregateId(tokenId);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      expect(appended.value).toHaveLength(1);
      const envelope = appended.value[0];
      expect(envelope?.eventType).toBe(EventType.TOKEN_DISCOVERED);
      expect(envelope?.producer).toBe(PRODUCER);
      // The payload survives the JSONB round trip as the values it was built with.
      const payload = envelope?.payload as TokenDiscoveredPayload;
      expect(payload.tokenId).toBe(tokenId);
      expect(payload.address).toBe('Together1');
      expect(payload.chain).toBe(Chain.SOLANA);
      expect(payload.launchSource).toBe(LaunchSource.PUMP_FUN);
      expect(payload.launchTime).toBe('2026-08-20T09:00:00.000Z');
      expect(payload.hasArtwork).toBe(false);
      expect(payload.hasCreatorWallet).toBe(false);
    });

    it('persists nothing when the transaction cannot complete', async () => {
      // The transactional outbox, written as a rollback because the failure mode
      // is silent: a committed token whose event was lost is never enriched, and
      // because the row exists the next poll reports a duplicate rather than
      // re-announcing it. Nothing ever reports it as missing.
      const normalized = normalizeSourceLaunch(
        Chain.SOLANA,
        LaunchSource.PUMP_FUN,
        launch({ address: 'RolledBack1' }),
        DISCOVERED_AT
      );
      if (normalized.kind === 'failure') {
        throw normalized.error;
      }
      const token = normalized.value;

      const rolled = await new PostgresUnitOfWork(harness.db, queue).transact(async (uow) => {
        const inserted = await uow.tokens.insert(token);
        if (inserted.kind === 'failure') {
          return failure(inserted.error);
        }
        const appended = await uow.events.append(
          tokenDiscoveredEvent({
            tokenId: inserted.value.tokenId,
            chain: token.chain,
            address: token.address,
            launchSource: token.launchSource,
            launchTime: token.launchTime,
            discoveredAt: token.discoveredAt,
            hasArtwork: false,
            hasCreatorWallet: false,
            producer: PRODUCER,
          })
        );
        if (appended.kind === 'failure') {
          return failure(appended.error);
        }
        // The job too, because the outbox is the reason the transaction spans three
        // tables: a committed job for a token that was rolled back would be a worker
        // enriching a token that does not exist.
        const enqueued = await uow.jobs.enqueue<TokenLiquidityPayload>({
          queueName: QueueName.TOKEN_LIQUIDITY,
          payload: {
            tokenId: inserted.value.tokenId,
            chain: token.chain,
            address: token.address,
            venue: MarketVenue.PUMP_FUN_BONDING_CURVE,
          },
          payloadVersion: TOKEN_LIQUIDITY_PAYLOAD_VERSION,
          idempotencyKey: `token_liquidity:${inserted.value.tokenId}:launch`,
        });
        if (enqueued.kind === 'failure') {
          return failure(enqueued.error);
        }
        // Everything above succeeded; the transaction still must not commit.
        return failure(new Error('simulated failure after the writes'));
      });

      expect(rolled.kind).toBe('failure');
      expect(await countOf('tokens')).toBe(0);
      expect(await countOf('domain_events')).toBe(0);
      expect(await countOf('durable_jobs')).toBe(0);
    });

    it("returns the caller's error rather than the rollback signal", async () => {
      const sentinel = new Error('the reason the caller cares about');
      const rolled = await new PostgresUnitOfWork(harness.db, queue).transact(() =>
        Promise.resolve(failure(sentinel))
      );

      expect(rolled.kind).toBe('failure');
      if (rolled.kind === 'success') {
        throw new Error('Expected a failure');
      }
      expect(rolled.error).toBe(sentinel);
    });
  });

  describe('enrichment jobs commit with the token', () => {
    const FULL: LaunchOverrides = {
      address: 'Outbox1',
      creatorWallet: 'Creator111111111111111111111111111111111Cc',
      artworkUrl: 'https://cdn.invalid/outbox.png',
    };

    it('enqueues all three jobs for a launch that supplied everything', async () => {
      const tokenId = await discover(launch(FULL));

      const jobs = await enqueuedJobs();
      expect(jobs.map((job) => job.queue_name)).toEqual([
        QueueName.TOKEN_ARTWORK,
        QueueName.TOKEN_CREATOR,
        QueueName.TOKEN_LIQUIDITY,
      ]);
      expect(jobs.map((job) => job.idempotency_key)).toEqual([
        `token_artwork:${tokenId}`,
        `token_creator:${tokenId}`,
        `token_liquidity:${tokenId}:launch`,
      ]);
      expect(jobs.map((job) => job.payload_version)).toEqual([
        '1',
        '1',
        TOKEN_LIQUIDITY_PAYLOAD_VERSION,
      ]);

      // The driver parses JSONB back into JS values, so a payload is the object the
      // planner built — which is what makes carrying the address instead of only a
      // token id worth doing.
      const artwork = jobs[0]?.payload as TokenArtworkPayload;
      expect(artwork.tokenId).toBe(tokenId);
      expect(artwork.chain).toBe(Chain.SOLANA);
      expect(artwork.address).toBe('Outbox1');
      expect(artwork.artworkUrl).toBe('https://cdn.invalid/outbox.png');

      const creator = jobs[1]?.payload as TokenCreatorPayload;
      expect(creator.creatorWallet).toBe(FULL.creatorWallet);
      expect('address' in creator).toBe(false);

      const liquidity = jobs[2]?.payload as TokenLiquidityPayload;
      expect(liquidity.address).toBe('Outbox1');
    });

    it('plans only the market job for a launch that supplied neither', async () => {
      // Progressive enrichment is per-token, not a fixed fan-out: a token with no
      // image and no creator has one thing to ask a provider about, and waking the
      // other two workers to discover there is nothing to do costs a claimed job
      // and a lease apiece.
      await discover(launch({ address: 'Sparse2' }));

      const jobs = await enqueuedJobs();
      expect(jobs.map((job) => job.queue_name)).toEqual([QueueName.TOKEN_LIQUIDITY]);
    });

    it('does not re-enqueue when the same launch is re-seen', async () => {
      await discover(launch(FULL));
      const second = await service.discover(PUMP_FUN, launch(FULL));
      if (second.kind === 'failure') {
        throw second.error;
      }

      // Two paths guard this and only one of them is exercised here: discovery
      // short-circuits on a duplicate row before planning anything, and the keys
      // are per-token so `ON CONFLICT (queue_name, idempotency_key)` would absorb
      // them even if it did not.
      expect(second.value.status).toBe('duplicate');
      expect(await countOf('durable_jobs')).toBe(3);
    });

    it('absorbs a re-plan of the same token as a duplicate enqueue', async () => {
      // The other guard, on its own: the same keys enqueued outside discovery's
      // short-circuit. This is what a redelivered poll job would do if the token
      // row were somehow not yet visible to it.
      const tokenId = await discover(launch(FULL));

      const replanned = await new PostgresUnitOfWork(harness.db, queue).transact((uow) =>
        uow.jobs.enqueue<TokenLiquidityPayload>({
          queueName: QueueName.TOKEN_LIQUIDITY,
          payload: {
            tokenId,
            chain: Chain.SOLANA,
            address: 'Outbox1',
            venue: MarketVenue.PUMP_FUN_BONDING_CURVE,
          },
          payloadVersion: TOKEN_LIQUIDITY_PAYLOAD_VERSION,
          idempotencyKey: `token_liquidity:${tokenId}:launch`,
        })
      );
      if (replanned.kind === 'failure') {
        throw replanned.error;
      }

      expect(await countOf('durable_jobs')).toBe(3);
    });

    it('leaves the jobs claimable by a worker', async () => {
      // The point of the outbox: the row and the work are one commit, so a claim
      // right after discovery finds the job rather than racing a dispatcher.
      await discover(launch(FULL));

      const claimed = await queue.claim(QueueName.TOKEN_LIQUIDITY, 'outbox-test-worker');
      if (claimed.kind === 'failure') {
        throw claimed.error;
      }
      expect(claimed.value?.payloadVersion).toBe(TOKEN_LIQUIDITY_PAYLOAD_VERSION);
      expect((claimed.value?.payload as TokenLiquidityPayload | undefined)?.address).toBe(
        'Outbox1'
      );
    });
  });

  describe('candidate tokens are searchable before enrichment', () => {
    it('returns a token that has no enrichment rows at all', async () => {
      // EDS ch.21 §5: tokens remain searchable before enrichment completes. The
      // assertion that makes it meaningful is the second one — there is nothing to
      // join to, so the query cannot be relying on an enrichment row existing.
      await discover(launch({ address: 'Bare1', ticker: 'BARE' }));

      expect(await countOf('token_artwork')).toBe(0);
      expect(await countOf('market_snapshots')).toBe(0);
      expect(await search({ ticker: 'BARE' })).toHaveLength(1);
    });

    it('matches a ticker whatever case the caller asked in', async () => {
      // A narrative says "pepe" and a launch says "PEPE".
      await discover(launch({ address: 'Pepe1', ticker: 'PEPE' }));

      expect(await search({ ticker: 'pepe' })).toHaveLength(1);
      expect(await search({ ticker: 'PePe' })).toHaveLength(1);
      expect(await search({ ticker: '  pepe  ' })).toHaveLength(1);
      expect(await search({ ticker: 'pepes' })).toHaveLength(0);
    });

    it('matches a name whatever case the caller asked in', async () => {
      await discover(launch({ address: 'Wif1', ticker: 'WIF', name: 'dogWifHat' }));

      expect(await search({ name: 'DOGWIFHAT' })).toHaveLength(1);
      expect(await search({ name: 'dogwifhat' })).toHaveLength(1);
    });

    it('stores the ticker in the case the launchpad published it', async () => {
      // Matching case-insensitively is a property of the query, not of the stored
      // value: the ticker is shown to a human.
      await discover(launch({ address: 'Case1', ticker: 'wIf' }));

      expect((await read('Case1')).ticker).toBe('wIf');
    });

    it('preserves a mixed-case mint address through a round trip', async () => {
      // Base58 is case-sensitive, so a case-folded address is one that does not
      // exist on chain while looking entirely plausible. The search path lowercases
      // ticker and name, which is exactly why the address needs its own check.
      const mixed = 'So11111111111111111111111111111111111111Aa';
      await discover(launch({ address: mixed }));

      expect((await read(mixed)).address).toBe(mixed);
      const lowered = await repository.findByAddress(Chain.SOLANA, mixed.toLowerCase());
      if (lowered.kind === 'failure') {
        throw lowered.error;
      }
      expect(lowered.value).toBeNull();
    });

    it('excludes tokens launched before the window', async () => {
      await discover(launch({ address: 'Old1', launchTime: new Date('2026-08-19T00:00:00.000Z') }));
      await discover(launch({ address: 'New1', launchTime: new Date('2026-08-20T12:00:00.000Z') }));

      const recent = await search({ launchedSince: new Date('2026-08-20T00:00:00.000Z') });
      expect(recent.map((token) => token.address)).toEqual(['New1']);
    });

    it('returns newest launch first, with a deterministic tie-break', async () => {
      const sameInstant = new Date('2026-08-20T09:00:00.000Z');
      await discover(launch({ address: 'Bbb', launchTime: sameInstant }));
      await discover(launch({ address: 'Aaa', launchTime: sameInstant }));
      await discover(launch({ address: 'Zzz', launchTime: new Date('2026-08-20T10:00:00.000Z') }));

      // launch_time DESC, then address DESC — a total order, so two callers paging
      // the same window see the same sequence.
      expect((await search()).map((token) => token.address)).toEqual(['Zzz', 'Bbb', 'Aaa']);
    });

    it('returns no more than the limit asked for', async () => {
      await discover(launch({ address: 'L1', launchTime: new Date('2026-08-20T09:00:01.000Z') }));
      await discover(launch({ address: 'L2', launchTime: new Date('2026-08-20T09:00:02.000Z') }));
      await discover(launch({ address: 'L3', launchTime: new Date('2026-08-20T09:00:03.000Z') }));

      const page = await search({ limit: 2 });
      expect(page.map((token) => token.address)).toEqual(['L3', 'L2']);
    });

    it('reads a token back by its identifier', async () => {
      const tokenId = await discover(launch({ address: 'ById1', ticker: 'BYID' }));

      const found = await repository.findById(tokenId);
      if (found.kind === 'failure') {
        throw found.error;
      }
      expect(found.value?.address).toBe('ById1');
      expect(found.value?.ticker).toBe('BYID');
      expect(found.value?.discoveredAt).toEqual(DISCOVERED_AT);
    });

    it('reports an unknown token as absent rather than as an error', async () => {
      const byId = await repository.findById(generateTokenId());
      const byAddress = await repository.findByAddress(Chain.SOLANA, 'NeverSeen1');
      if (byId.kind === 'failure' || byAddress.kind === 'failure') {
        throw new Error('A miss is not a failure');
      }

      expect(byId.value).toBeNull();
      expect(byAddress.value).toBeNull();
    });
  });

  describe('stored launches are immutable', () => {
    it('rejects an update to a stored token', async () => {
      // EDS ch.21 §8: raw launch data MUST remain immutable. Enforced by a trigger
      // rather than by convention, because progressive enrichment is a stream of
      // writes and the temptation to update this row in place is constant.
      await discover(launch({ address: 'Frozen1', ticker: 'TICK' }));

      const updated = await harness.db.query(
        `UPDATE tokens SET ticker = 'HACKED' WHERE address = 'Frozen1'`
      );
      expect(updated.kind).toBe('failure');
      if (updated.kind === 'success') {
        throw new Error('Expected the update to be rejected');
      }
      expect(updated.error.message).toContain('append-only');
      expect((await read('Frozen1')).ticker).toBe('TICK');
    });

    it('rejects a delete', async () => {
      await discover(launch({ address: 'Frozen2' }));

      const deleted = await harness.db.query(`DELETE FROM tokens WHERE address = 'Frozen2'`);
      expect(deleted.kind).toBe('failure');
      expect(await countOf('tokens')).toBe(1);
    });

    it('keeps the launchpad payload verbatim', async () => {
      // Replay rebuilds from what actually arrived rather than from what today's
      // normalizer happened to keep, so the payload is stored unedited — including
      // the fields nobody has thought to normalize yet.
      const source = launch({ address: 'Raw1' });
      source.raw = {
        mint: 'Raw1',
        bonding_curve: { progress: 0.42, complete: false },
        unmapped_future_field: ['a', 'b'],
      };
      await discover(source);

      const stored = await harness.db.query<{ raw_payload: Record<string, unknown> }>(
        `SELECT raw_payload FROM tokens WHERE address = 'Raw1'`
      );
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      // The driver parses JSONB into JS values, so this is the object back, not text.
      expect(stored.value[0]?.raw_payload).toEqual(source.raw);
    });
  });

  describe('optional launch fields', () => {
    it('round-trips the fields a launchpad supplied', async () => {
      await discover(
        launch({
          address: 'Full1',
          creatorWallet: 'Creator111111111111111111111111111111111Cc',
          description: 'a frog',
          artworkUrl: 'https://cdn.invalid/pepe.png',
        })
      );

      const token = await read('Full1');
      expect(token.creatorWallet).toBe('Creator111111111111111111111111111111111Cc');
      expect(token.description).toBe('a frog');
      expect(token.artworkUrl).toBe('https://cdn.invalid/pepe.png');
    });

    it('omits the fields a launchpad left out rather than reporting them as null', async () => {
      // `exactOptionalPropertyTypes` is on, so a consumer branching on absence has
      // to see absence — a null would satisfy neither the type nor the branch.
      await discover(launch({ address: 'Sparse1' }));

      const token = await read('Sparse1');
      expect('creatorWallet' in token).toBe(false);
      expect('description' in token).toBe(false);
      expect('artworkUrl' in token).toBe(false);
    });

    it('tells the enrichment workers which of them have work to do', async () => {
      const overrides: LaunchOverrides = {
        address: 'Flags1',
        artworkUrl: 'https://cdn.invalid/art.png',
      };
      const tokenId = await discover(launch(overrides));

      const appended = await events.getByAggregateId(tokenId);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      const payload = appended.value[0]?.payload as TokenDiscoveredPayload;
      expect(payload.hasArtwork).toBe(true);
      expect(payload.hasCreatorWallet).toBe(false);
    });
  });

  describe('a poll window is discovered as a batch', () => {
    it('announces the window oldest first and separates duplicates', async () => {
      const seen = launch({ address: 'Seen1', launchTime: new Date('2026-08-20T09:00:00.000Z') });
      await discover(seen);

      const batch = await service.discoverBatch(PUMP_FUN, [
        launch({ address: 'Third1', launchTime: new Date('2026-08-20T09:00:03.000Z') }),
        seen,
        launch({ address: 'Second1', launchTime: new Date('2026-08-20T09:00:02.000Z') }),
      ]);

      expect(batch.discovered).toHaveLength(2);
      expect(batch.duplicates).toHaveLength(1);
      expect(batch.failures).toHaveLength(0);
      expect(await countOf('tokens')).toBe(3);

      const appended = await events.getByEventType(EventType.TOKEN_DISCOVERED);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      // Three events for three tokens, in launch order, with the re-seen one not
      // announced again. The log's own sequence is what replay walks.
      expect(
        appended.value.map((envelope) => (envelope.payload as TokenDiscoveredPayload).address)
      ).toEqual(['Seen1', 'Second1', 'Third1']);
    });

    it('commits the good launches in a window that also contains a bad one', async () => {
      // One token is one transaction, so a malformed entry costs itself and
      // nothing else. The next poll's window will have moved past it.
      const batch = await service.discoverBatch(PUMP_FUN, [
        launch({ address: 'Good1', launchTime: new Date('2026-08-20T09:00:00.000Z') }),
        launch({ address: 'Bad1', launchTime: new Date('2026-08-20T09:00:01.000Z'), ticker: ' ' }),
        launch({ address: 'Good2', launchTime: new Date('2026-08-20T09:00:02.000Z') }),
      ]);

      expect(batch.discovered).toHaveLength(2);
      expect(batch.failures.map((entry) => entry.address)).toEqual(['Bad1']);
      expect(await countOf('tokens')).toBe(2);
      expect(await countOf('domain_events')).toBe(2);
    });
  });
});
