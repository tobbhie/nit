import { Pool, PoolClient, PoolConfig } from 'pg';
import { Result, success, failure } from '@nit/domain';

export type { PoolClient, PoolConfig };

/**
 * Anything that can run a parameterised statement and report failure as a
 * value rather than an exception.
 *
 * `DatabasePool` implements it by taking a fresh connection per call;
 * `clientExecutor` binds it to one open transaction. Code that writes should
 * accept an `Executor` instead of a `DatabasePool` so a caller can pull those
 * writes into a transaction it owns — that is what makes the transactional
 * outbox possible: a domain row and the job that follows from it commit
 * together or not at all.
 */
export interface Executor {
  query<T = unknown>(text: string, params?: unknown[]): Promise<Result<T[], Error>>;
}

/**
 * Present a transaction's client as an `Executor`.
 *
 * Note the hazard this creates. A failed statement poisons its transaction:
 * Postgres rejects everything after it until rollback. Because failures come
 * back as values here, nothing forces the caller to notice — so code running
 * on a borrowed executor MUST return a failure upward rather than continue or
 * swallow it, and only the transaction's owner may decide to roll back.
 * `DatabasePool.transaction` rolls back when its callback throws.
 */
export function clientExecutor(client: PoolClient): Executor {
  return {
    async query<T = unknown>(text: string, params?: unknown[]): Promise<Result<T[], Error>> {
      try {
        const result = await client.query(text, params);
        return success(result.rows as T[]);
      } catch (error) {
        return failure(error as Error);
      }
    },
  };
}

/**
 * Database connection pool
 */
export class DatabasePool implements Executor {
  private pool: Pool;

  constructor(config: PoolConfig) {
    this.pool = new Pool(config);
  }

  async getClient(): Promise<Result<PoolClient, Error>> {
    try {
      const client = await this.pool.connect();
      return success(client);
    } catch (error) {
      return failure(error as Error);
    }
  }

  async query<T = unknown>(
    text: string,
    params?: unknown[]
  ): Promise<Result<T[], Error>> {
    try {
      const result = await this.pool.query(text, params);
      return success(result.rows as T[]);
    } catch (error) {
      return failure(error as Error);
    }
  }

  /**
   * Run `callback` inside BEGIN/COMMIT. Throw from the callback to roll back —
   * that is the only signal this method reads, so a callback that turns a
   * failure into a return value will be committed.
   */
  async transaction<T>(
    callback: (client: PoolClient) => Promise<T>
  ): Promise<Result<T, Error>> {
    const clientResult = await this.getClient();
    if (clientResult.kind === 'failure') {
      return failure(clientResult.error);
    }

    const client = clientResult.value;
    let failed: Error | undefined;

    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return success(result);
    } catch (error) {
      failed = error as Error;
      // A rollback that fails means the connection is already unusable, almost
      // always a dead socket. Swallow it: the caller needs the error that
      // caused the abort, not the symptom, and letting this one escape would
      // break the promise that failure arrives as a Result.
      await client.query('ROLLBACK').catch(() => undefined);
      return failure(failed);
    } finally {
      // Passing the error tells pg to destroy this connection rather than
      // return it to the pool, so a half-open transaction is never handed to
      // the next borrower.
      client.release(failed);
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async health(): Promise<Result<boolean, Error>> {
    try {
      await this.pool.query('SELECT 1');
      return success(true);
    } catch (error) {
      return failure(error as Error);
    }
  }
}

/**
 * Converts a failure Result into the thrown error `transaction` needs.
 *
 * Not exported: a caller gets its own error back, never this signal.
 */
class Rollback extends Error {
  constructor(readonly reason: Error) {
    super('Transaction rolled back');
    this.name = 'Rollback';
    Object.setPrototypeOf(this, Rollback.prototype);
  }
}

/**
 * Run `work` in one transaction, rolling back if it returns a failure.
 *
 * The missing half of `clientExecutor`'s hazard note above. `transaction` rolls
 * back only when its callback *throws*, so a callback that dutifully returns the
 * failure it was told to return would commit the very transaction it is
 * reporting as broken — and because a failed statement has already poisoned that
 * transaction, the commit fails in a place far from the statement that caused it.
 *
 * Every unit of work in this repository needs this bridge, and a bridge copied
 * once per package is a bridge that drifts, so it lives next to the hazard it
 * closes. The original error is carried through untouched.
 *
 * `work` also receives the raw `PoolClient`, because not every collaborator that
 * belongs in one commit takes an `Executor`: `JobQueue`'s mutating methods enlist
 * on a client, so a unit of work that writes a domain row and enqueues the job
 * that follows from it needs both handles onto the same transaction. Callbacks
 * that only query declare one parameter and ignore the second.
 */
export async function transactResult<T>(
  db: DatabasePool,
  work: (executor: Executor, client: PoolClient) => Promise<Result<T, Error>>
): Promise<Result<T, Error>> {
  const outcome = await db.transaction(async (client) => {
    const result = await work(clientExecutor(client), client);
    if (result.kind === 'failure') {
      throw new Rollback(result.error);
    }
    return result.value;
  });

  if (outcome.kind === 'failure') {
    return failure(outcome.error instanceof Rollback ? outcome.error.reason : outcome.error);
  }
  return success(outcome.value);
}
