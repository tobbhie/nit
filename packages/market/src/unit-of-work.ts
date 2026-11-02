import { Result } from '@nit/domain';
import { DatabasePool, PostgresEventStore, transactResult } from '@nit/database-public';
import { EventStore } from '@nit/events';
import { EnqueueRequest, Job } from '@nit/queue';
import { JobEnqueuer, TransactionalJobEnqueuer } from './enrichment-contracts';
import {
  CreatorProfileRepository,
  MarketSnapshotRepository,
  PostgresCreatorProfileRepository,
  PostgresMarketSnapshotRepository,
  PostgresTokenArtworkRepository,
  TokenArtworkRepository,
} from './enrichment-repository';
import { PostgresTokenRepository, TokenRepository } from './repository';
import { PostgresTokenMarketRepository, TokenMarketRepository } from './lifecycle';

/**
 * The writes that make up one discovery, bound to a single transaction.
 *
 * `jobs` is the third member and the reason this interface exists rather than three
 * arguments: the enrichment jobs for a token have to commit with the token row and
 * its event, or the guarantee that a discovered token is always enriched is only a
 * hope about process lifetimes.
 */
export interface DiscoveryUnitOfWork {
  readonly tokens: TokenRepository;
  readonly markets: TokenMarketRepository;
  readonly events: EventStore;
  readonly jobs: JobEnqueuer;
}

export interface UnitOfWork {
  /**
   * Run `work` inside one transaction. A failure Result rolls it back and comes
   * back to the caller unchanged.
   */
  transact<T>(
    work: (uow: DiscoveryUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>>;
}

/** The writes that make up one enrichment, bound to a single transaction. */
export interface EnrichmentUnitOfWork {
  readonly artwork: TokenArtworkRepository;
  readonly snapshots: MarketSnapshotRepository;
  readonly creators: CreatorProfileRepository;
  readonly events: EventStore;
}

export interface EnrichmentTransactor {
  transact<T>(
    work: (uow: EnrichmentUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>>;
}

/**
 * A unit of work over one PostgreSQL transaction.
 *
 * The transactional outbox for discovery: the token row, its `TokenDiscovered` event
 * and its enrichment jobs are all written through handles bound to the same client,
 * so they commit together. Without that, a crash between them leaves a token that no
 * enrichment worker will ever wake for — and because the row exists, the next poll
 * sees a duplicate and does not re-announce it, so the token stays permanently
 * unenriched. That failure is silent, which is what makes it worth a transaction.
 *
 * The enqueuer is a constructor parameter rather than optional. A discovery that
 * quietly skipped its jobs would look identical to one that ran, and the whole point
 * of the paragraph above is that nobody would find out.
 */
export class PostgresUnitOfWork implements UnitOfWork {
  constructor(
    private readonly db: DatabasePool,
    private readonly enqueuer: TransactionalJobEnqueuer
  ) {}

  async transact<T>(
    work: (uow: DiscoveryUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>> {
    return transactResult(this.db, (executor, client) =>
      work({
        tokens: new PostgresTokenRepository(executor),
        markets: new PostgresTokenMarketRepository(executor),
        events: new PostgresEventStore(executor),
        // The client, not the executor: `JobQueue` enlists on a raw `PoolClient`,
        // and binding it here is what stops the code inside the transaction from
        // enqueueing onto a different connection by accident. Written as a generic
        // arrow rather than passed as a bound method so the payload type survives.
        jobs: {
          enqueue: <TPayload>(request: EnqueueRequest<TPayload>): Promise<Result<Job, Error>> =>
            this.enqueuer.enqueue(request, client),
        },
      })
    );
  }
}

/**
 * A unit of work over one PostgreSQL transaction, for the enrichment workers.
 *
 * All four handles rather than one per worker. Each is an object wrapping an
 * executor, so building the set costs nothing, and the alternative — three
 * interfaces and three implementations differing only in which repository they name —
 * would triple the places a new enrichment table has to be threaded through.
 *
 * No `jobs` member: no enrichment worker enqueues anything, and an unused enqueuer
 * on a transaction is an invitation to start a fan-out from inside one.
 */
export class PostgresEnrichmentUnitOfWork implements EnrichmentTransactor {
  constructor(private readonly db: DatabasePool) {}

  async transact<T>(
    work: (uow: EnrichmentUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>> {
    return transactResult(this.db, (executor) =>
      work({
        artwork: new PostgresTokenArtworkRepository(executor),
        snapshots: new PostgresMarketSnapshotRepository(executor),
        creators: new PostgresCreatorProfileRepository(executor),
        events: new PostgresEventStore(executor),
      })
    );
  }
}
