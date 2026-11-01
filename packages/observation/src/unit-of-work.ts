import { Result } from '@nit/domain';
import { DatabasePool, PostgresEventStore, transactResult } from '@nit/database-public';
import { EventStore } from '@nit/events';
import { ObservationRepository, PostgresObservationRepository } from './repository';

/**
 * The writes that make up one ingestion, bound to a single transaction.
 */
export interface IngestionUnitOfWork {
  readonly observations: ObservationRepository;
  readonly events: EventStore;
}

export interface UnitOfWork {
  /**
   * Run `work` inside one transaction. A failure Result rolls it back and comes
   * back to the caller unchanged.
   */
  transact<T>(
    work: (uow: IngestionUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>>;
}

/**
 * A unit of work over one PostgreSQL transaction.
 *
 * This is the transactional outbox for ingestion: the observation, its media
 * references and the `ObservationCreated` event are written through executors
 * bound to the same client, so they commit together. Without that, a crash
 * between the row and the event leaves an observation no worker will ever
 * process, and TIS ch.4 §11's "events trigger multimodal processing" quietly
 * stops being true for the posts that mattered most.
 *
 * `transactResult` owns the rollback-on-failure bridge, so all this class decides
 * is which repositories the transaction exposes.
 */
export class PostgresUnitOfWork implements UnitOfWork {
  constructor(private readonly db: DatabasePool) {}

  async transact<T>(
    work: (uow: IngestionUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>> {
    return transactResult(this.db, (executor) =>
      work({
        observations: new PostgresObservationRepository(executor),
        events: new PostgresEventStore(executor),
      })
    );
  }
}
