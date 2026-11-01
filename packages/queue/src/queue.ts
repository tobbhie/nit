import { DatabasePool, clientExecutor, type Executor, type PoolClient } from '@nit/database-public';
import {
  JobId,
  JobStatus,
  CorrelationId,
  Result,
  success,
  failure,
  generateJobId,
} from '@nit/domain';
import { Job, EnqueueRequest, DeadLetter, allowedPredecessors, canTransition } from './job';
import { BackoffPolicy, nextAvailableAt } from './backoff';

interface JobRow {
  job_id: string;
  queue_name: string;
  payload: unknown;
  payload_version: string;
  status: string;
  attempt_count: number;
  max_attempts: number;
  lease_deadline: Date | null;
  leased_by: string | null;
  idempotency_key: string;
  available_at: Date;
  last_error: string | null;
  correlation_id: string | null;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
}

function toJob(row: JobRow): Job {
  return {
    jobId: row.job_id as JobId,
    queueName: row.queue_name,
    payload: row.payload,
    payloadVersion: row.payload_version,
    status: row.status as JobStatus,
    attemptCount: row.attempt_count,
    maxAttempts: row.max_attempts,
    leaseDeadline: row.lease_deadline,
    leasedBy: row.leased_by,
    idempotencyKey: row.idempotency_key,
    availableAt: row.available_at,
    lastError: row.last_error,
    correlationId: row.correlation_id as CorrelationId | null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    completedAt: row.completed_at,
  };
}

export interface JobQueueOptions {
  leaseTimeoutMs: number;
  defaultMaxAttempts: number;
  backoff: BackoffPolicy;
  /**
   * Most expired leases recovered per sweep. Bounds how long the recovery
   * transaction holds its rows; a backlog is drained across several sweeps.
   */
  recoveryBatchSize?: number;
  /** Injected for deterministic tests; defaults to wall clock. */
  clock?: () => Date;
}

const DEFAULT_RECOVERY_BATCH_SIZE = 100;

/**
 * What a recovery sweep did. Requeued and dead-lettered are reported
 * separately because they mean opposite things operationally: the first is
 * routine crash recovery, the second is work that will never complete and
 * needs a human.
 */
export interface LeaseRecovery {
  /** Leases returned to the pending pool for another attempt. */
  requeued: JobId[];
  /** Leases whose job had no attempts left, so it was dead-lettered instead. */
  deadLettered: JobId[];
}

export type FailOutcome = 'retry' | 'dead_letter' | 'ignored';

/**
 * Optional constraints applied to a status change.
 *
 * `workerId` fences the write on lease ownership: the update only lands if the
 * job is still leased by that worker. Without it a worker whose lease expired
 * mid-handler can still complete or reschedule a job another worker has since
 * claimed, which produces two live attempts on one job.
 */
export interface StatusChangeOptions {
  workerId?: string;
}

/**
 * PostgreSQL-backed durable job queue.
 *
 * Two invariants shape the implementation:
 *
 * 1. Claiming uses SELECT ... FOR UPDATE SKIP LOCKED inside a short
 *    transaction, so concurrent workers never claim the same job (TIS ch.3 §4).
 * 2. Every status change and the `job_history` row describing it commit
 *    together. TIS ch.3 §8 treats the audit trail as a guarantee and ch.19 §7
 *    requires replay to be deterministic, neither of which survives a
 *    best-effort history write.
 *
 * Handler execution happens outside any transaction — long-running work must
 * not hold a database lock.
 *
 * Mutating methods accept an optional `PoolClient` so a caller can enlist them
 * in a transaction it owns. That is what makes the transactional outbox
 * possible: write the domain row and enqueue the job that follows from it in
 * one commit, and a crash can no longer leave one without the other.
 */
export class JobQueue {
  private readonly clock: () => Date;
  private readonly recoveryBatchSize: number;

  constructor(
    private readonly db: DatabasePool,
    private readonly options: JobQueueOptions
  ) {
    this.clock = options.clock ?? ((): Date => new Date());
    this.recoveryBatchSize = options.recoveryBatchSize ?? DEFAULT_RECOVERY_BATCH_SIZE;
  }

  /**
   * Enqueue a job. Idempotent on idempotencyKey — re-enqueueing returns the
   * existing job rather than creating a duplicate.
   *
   * Pass `client` to enqueue inside a transaction you already opened. Doing so
   * makes you responsible for aborting it: a failure comes back as a value, so
   * you must throw or otherwise abandon the transaction rather than commit.
   */
  async enqueue<TPayload>(
    request: EnqueueRequest<TPayload>,
    client?: PoolClient
  ): Promise<Result<Job, Error>> {
    const now = this.clock();
    const jobId = generateJobId();
    const maxAttempts = request.maxAttempts ?? this.options.defaultMaxAttempts;
    const availableAt = request.availableAt ?? now;

    return this.atomically(client, async (exec): Promise<Result<Job, Error>> => {
      const inserted = await exec.query<JobRow>(
        `INSERT INTO durable_jobs (
           job_id, queue_name, payload, payload_version, status,
           attempt_count, max_attempts, idempotency_key, available_at,
           correlation_id, created_at, updated_at
         ) VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9, $10, $10)
         ON CONFLICT (queue_name, idempotency_key) DO NOTHING
         RETURNING *`,
        [
          jobId,
          request.queueName,
          JSON.stringify(request.payload),
          request.payloadVersion,
          JobStatus.PENDING,
          maxAttempts,
          request.idempotencyKey,
          availableAt,
          request.correlationId ?? null,
          now,
        ]
      );

      if (inserted.kind === 'failure') {
        return failure(inserted.error);
      }

      const row = inserted.value[0];
      if (row) {
        const history = await this.recordHistory(
          exec,
          row.job_id as JobId,
          null,
          JobStatus.PENDING,
          0,
          'enqueued'
        );
        if (history.kind === 'failure') {
          return failure(history.error);
        }
        return success(toJob(row));
      }

      // Conflict: return the pre-existing job. Visible even from inside this
      // transaction because READ COMMITTED takes a fresh snapshot per
      // statement, and ON CONFLICT already waited out the competing insert.
      const existing = await exec.query<JobRow>(
        'SELECT * FROM durable_jobs WHERE queue_name = $1 AND idempotency_key = $2',
        [request.queueName, request.idempotencyKey]
      );
      if (existing.kind === 'failure') {
        return failure(existing.error);
      }
      const existingRow = existing.value[0];
      if (!existingRow) {
        return failure(new Error('Enqueue conflicted but no existing job was found'));
      }
      return success(toJob(existingRow));
    });
  }

  /**
   * Claim one job with a transactional lease. Returns null when the queue has
   * no available work.
   *
   * Deliberately not enlistable in a caller's transaction: the point of the
   * short transaction is that the row lock is released immediately, and a
   * caller's transaction would hold it for as long as the caller runs.
   */
  async claim(queueName: string, workerId: string): Promise<Result<Job | null, Error>> {
    const now = this.clock();
    const leaseDeadline = new Date(now.getTime() + this.options.leaseTimeoutMs);

    return this.atomically(undefined, async (exec): Promise<Result<Job | null, Error>> => {
      const selected = await exec.query<{ job_id: string }>(
        `SELECT job_id FROM durable_jobs
         WHERE queue_name = $1
           AND status = $2
           AND available_at <= $3
         ORDER BY available_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT 1`,
        [queueName, JobStatus.PENDING, now]
      );
      if (selected.kind === 'failure') {
        return failure(selected.error);
      }

      const candidate = selected.value[0];
      if (!candidate) {
        return success(null);
      }

      const updated = await exec.query<JobRow>(
        `UPDATE durable_jobs
         SET status = $1,
             lease_deadline = $2,
             leased_by = $3,
             attempt_count = attempt_count + 1,
             updated_at = $4
         WHERE job_id = $5 AND status = ANY($6)
         RETURNING *`,
        [
          JobStatus.LEASED,
          leaseDeadline,
          workerId,
          now,
          candidate.job_id,
          allowedPredecessors(JobStatus.LEASED) as string[],
        ]
      );
      if (updated.kind === 'failure') {
        return failure(updated.error);
      }
      const row = updated.value[0];
      if (!row) {
        return success(null);
      }

      const job = toJob(row);
      const history = await this.recordHistory(
        exec,
        job.jobId,
        JobStatus.PENDING,
        JobStatus.LEASED,
        job.attemptCount,
        `leased by ${workerId}`
      );
      if (history.kind === 'failure') {
        return failure(history.error);
      }
      return success(job);
    });
  }

  /**
   * Move a leased job to RUNNING. Returns false when the job is no longer in a
   * status from which RUNNING is reachable, or when `workerId` no longer holds
   * the lease — normally because the lease expired and another worker
   * recovered the job.
   */
  async markRunning(
    jobId: JobId,
    options: StatusChangeOptions = {}
  ): Promise<Result<boolean, Error>> {
    const now = this.clock();

    return this.atomically(undefined, async (exec): Promise<Result<boolean, Error>> => {
      const result = await exec.query<{ previous_status: string; attempt_count: number }>(
        `WITH prev AS (SELECT status FROM durable_jobs WHERE job_id = $3)
         UPDATE durable_jobs SET status = $1, updated_at = $2
         WHERE job_id = $3
           AND status = ANY($4)
           AND ($5::text IS NULL OR leased_by = $5)
         RETURNING (SELECT status FROM prev) AS previous_status, attempt_count`,
        [
          JobStatus.RUNNING,
          now,
          jobId,
          allowedPredecessors(JobStatus.RUNNING) as string[],
          options.workerId ?? null,
        ]
      );
      if (result.kind === 'failure') {
        return failure(result.error);
      }
      const row = result.value[0];
      if (!row) {
        return success(false);
      }

      const history = await this.recordHistory(
        exec,
        jobId,
        row.previous_status as JobStatus,
        JobStatus.RUNNING,
        row.attempt_count,
        'started'
      );
      if (history.kind === 'failure') {
        return failure(history.error);
      }
      return success(true);
    });
  }

  /**
   * Mark a job completed. Returns false when this call did not own the job — a
   * completed or dead-lettered job is terminal and is never resurrected, and a
   * fenced call whose lease was reclaimed does not win.
   *
   * Accepts a `client` so a handler can commit its output and the completion
   * together, which is what stops a crash in between from re-running the work.
   */
  async complete(
    jobId: JobId,
    options: StatusChangeOptions = {},
    client?: PoolClient
  ): Promise<Result<boolean, Error>> {
    const now = this.clock();

    return this.atomically(client, async (exec): Promise<Result<boolean, Error>> => {
      const result = await exec.query<{ previous_status: string; attempt_count: number }>(
        `WITH prev AS (SELECT status FROM durable_jobs WHERE job_id = $3)
         UPDATE durable_jobs
         SET status = $1, completed_at = $2, updated_at = $2,
             lease_deadline = NULL, leased_by = NULL
         WHERE job_id = $3
           AND status = ANY($4)
           AND ($5::text IS NULL OR leased_by = $5)
         RETURNING (SELECT status FROM prev) AS previous_status, attempt_count`,
        [
          JobStatus.COMPLETED,
          now,
          jobId,
          allowedPredecessors(JobStatus.COMPLETED) as string[],
          options.workerId ?? null,
        ]
      );
      if (result.kind === 'failure') {
        return failure(result.error);
      }
      const row = result.value[0];
      if (!row) {
        return success(false);
      }

      const history = await this.recordHistory(
        exec,
        jobId,
        row.previous_status as JobStatus,
        JobStatus.COMPLETED,
        row.attempt_count,
        'completed'
      );
      if (history.kind === 'failure') {
        return failure(history.error);
      }
      return success(true);
    });
  }

  /**
   * Schedule a retry with bounded exponential backoff, or dead-letter the job
   * when attempts are exhausted. Returns 'ignored' when the job already
   * reached a terminal status or when `workerId` no longer holds the lease.
   */
  async fail(
    jobId: JobId,
    error: string,
    options: { permanent?: boolean; workerId?: string } = {}
  ): Promise<Result<FailOutcome, Error>> {
    const now = this.clock();

    return this.atomically(undefined, async (exec): Promise<Result<FailOutcome, Error>> => {
      // FOR UPDATE holds the row until this transaction ends, so the status
      // the decision below rests on cannot change underneath it. The status
      // predicates on the writes are then belt-and-braces rather than the only
      // guard — still worth having, because they make each write correct when
      // read on its own.
      const current = await exec.query<JobRow>(
        'SELECT * FROM durable_jobs WHERE job_id = $1 FOR UPDATE',
        [jobId]
      );
      if (current.kind === 'failure') {
        return failure(current.error);
      }
      const row = current.value[0];
      if (!row) {
        return failure(new Error(`Job ${jobId} not found`));
      }

      // Comparing in JS is safe here only because of the FOR UPDATE above.
      if (options.workerId !== undefined && row.leased_by !== options.workerId) {
        return success<FailOutcome>('ignored');
      }

      const from = row.status as JobStatus;
      const exhausted = row.attempt_count >= row.max_attempts;

      if (options.permanent === true || exhausted) {
        if (!canTransition(from, JobStatus.DEAD_LETTER)) {
          return success<FailOutcome>('ignored');
        }
        const reason = options.permanent === true ? 'permanent_failure' : 'attempts_exhausted';
        const moved = await this.deadLetter(exec, row, reason, error, now);
        if (moved.kind === 'failure') {
          return failure(moved.error);
        }
        return success<FailOutcome>(moved.value ? 'dead_letter' : 'ignored');
      }

      if (!canTransition(from, JobStatus.PENDING)) {
        return success<FailOutcome>('ignored');
      }

      const retryAt = nextAvailableAt(now, row.attempt_count, this.options.backoff);
      const updated = await exec.query<{ attempt_count: number }>(
        `UPDATE durable_jobs
         SET status = $1, available_at = $2, last_error = $3, updated_at = $4,
             lease_deadline = NULL, leased_by = NULL
         WHERE job_id = $5 AND status = ANY($6)
         RETURNING attempt_count`,
        [
          JobStatus.PENDING,
          retryAt,
          error,
          now,
          jobId,
          allowedPredecessors(JobStatus.PENDING) as string[],
        ]
      );
      if (updated.kind === 'failure') {
        return failure(updated.error);
      }
      const updatedRow = updated.value[0];
      if (!updatedRow) {
        return success<FailOutcome>('ignored');
      }

      const history = await this.recordHistory(
        exec,
        jobId,
        from,
        JobStatus.PENDING,
        updatedRow.attempt_count,
        `retry scheduled for ${retryAt.toISOString()}`
      );
      if (history.kind === 'failure') {
        return failure(history.error);
      }
      return success<FailOutcome>('retry');
    });
  }

  /**
   * Reclaim jobs whose lease expired. This is how the system recovers from
   * worker crashes. SKIP LOCKED keeps concurrent recovery sweeps from blocking
   * each other.
   *
   * A job that has already used every attempt is dead-lettered rather than
   * requeued. Requeueing it unconditionally — which is what this used to do —
   * lets a job that crashes its worker every time exceed maxAttempts without
   * limit, because each crash is recovered rather than counted as a failure.
   */
  async recoverExpiredLeases(): Promise<Result<LeaseRecovery, Error>> {
    const now = this.clock();

    return this.atomically(undefined, async (exec): Promise<Result<LeaseRecovery, Error>> => {
      const expired = await exec.query<JobRow>(
        `SELECT * FROM durable_jobs
         WHERE status = ANY($1)
           AND lease_deadline IS NOT NULL
           AND lease_deadline < $2
         ORDER BY lease_deadline ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $3`,
        [[JobStatus.LEASED, JobStatus.RUNNING] as string[], now, this.recoveryBatchSize]
      );
      if (expired.kind === 'failure') {
        return failure(expired.error);
      }

      const requeued: JobId[] = [];
      const deadLettered: JobId[] = [];

      for (const row of expired.value) {
        const jobId = row.job_id as JobId;

        if (row.attempt_count >= row.max_attempts) {
          const moved = await this.deadLetter(
            exec,
            row,
            'attempts_exhausted',
            row.last_error ?? 'lease expired with no attempts remaining',
            now
          );
          if (moved.kind === 'failure') {
            return failure(moved.error);
          }
          if (moved.value) {
            deadLettered.push(jobId);
          }
          continue;
        }

        const returned = await exec.query<{ attempt_count: number }>(
          `UPDATE durable_jobs
           SET status = $1, lease_deadline = NULL, leased_by = NULL, updated_at = $2
           WHERE job_id = $3 AND status = ANY($4)
           RETURNING attempt_count`,
          [JobStatus.PENDING, now, jobId, allowedPredecessors(JobStatus.PENDING) as string[]]
        );
        if (returned.kind === 'failure') {
          return failure(returned.error);
        }
        const updatedRow = returned.value[0];
        if (!updatedRow) {
          continue;
        }

        const history = await this.recordHistory(
          exec,
          jobId,
          row.status as JobStatus,
          JobStatus.PENDING,
          updatedRow.attempt_count,
          'lease expired; recovered'
        );
        if (history.kind === 'failure') {
          return failure(history.error);
        }
        requeued.push(jobId);
      }

      return success({ requeued, deadLettered });
    });
  }

  /**
   * Jobs waiting to be claimed on a queue. Counts PENDING only — in-flight and
   * terminal jobs are excluded, so this is backlog rather than total rows.
   */
  async pendingDepth(queueName: string): Promise<Result<number, Error>> {
    const result = await this.db.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM durable_jobs WHERE queue_name = $1 AND status = $2`,
      [queueName, JobStatus.PENDING]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    return success(Number(result.value[0]?.count ?? '0'));
  }

  async getDeadLetters(queueName: string): Promise<Result<DeadLetter[], Error>> {
    const result = await this.db.query<{
      job_id: string;
      queue_name: string;
      payload: unknown;
      payload_version: string;
      attempt_count: number;
      idempotency_key: string;
      failure_reason: string;
      last_error: string | null;
      correlation_id: string | null;
      dead_lettered_at: Date;
    }>(
      `SELECT * FROM dead_letter_jobs WHERE queue_name = $1 ORDER BY dead_lettered_at ASC`,
      [queueName]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }

    return success(
      result.value.map((row) => ({
        jobId: row.job_id as JobId,
        queueName: row.queue_name,
        payload: row.payload,
        payloadVersion: row.payload_version,
        attemptCount: row.attempt_count,
        idempotencyKey: row.idempotency_key,
        failureReason: row.failure_reason,
        lastError: row.last_error,
        correlationId: row.correlation_id as CorrelationId | null,
        deadLetteredAt: row.dead_lettered_at,
      }))
    );
  }

  async getJob(jobId: JobId): Promise<Result<Job | null, Error>> {
    const result = await this.db.query<JobRow>('SELECT * FROM durable_jobs WHERE job_id = $1', [
      jobId,
    ]);
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    const row = result.value[0];
    return success(row ? toJob(row) : null);
  }

  async getHistory(jobId: JobId): Promise<Result<
    Array<{ fromStatus: string | null; toStatus: string; detail: string | null; occurredAt: Date }>,
    Error
  >> {
    const result = await this.db.query<{
      from_status: string | null;
      to_status: string;
      detail: string | null;
      occurred_at: Date;
    }>(
      `SELECT from_status, to_status, detail, occurred_at
       FROM job_history WHERE job_id = $1 ORDER BY id ASC`,
      [jobId]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    return success(
      result.value.map((row) => ({
        fromStatus: row.from_status,
        toStatus: row.to_status,
        detail: row.detail,
        occurredAt: row.occurred_at,
      }))
    );
  }

  /**
   * Run `work` in a transaction, or inside the caller's if one was handed in.
   *
   * Every mutating method funnels through here so that a status change and the
   * history row describing it cannot be separated by a crash.
   */
  private async atomically<T>(
    client: PoolClient | undefined,
    work: (exec: Executor) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>> {
    if (client !== undefined) {
      // The caller owns the transaction, so a failure is reported and left for
      // them to act on. They must not commit after seeing one.
      return work(clientExecutor(client));
    }

    return this.db.transaction(async (own) => {
      const result = await work(clientExecutor(own));
      if (result.kind === 'failure') {
        // Throwing is how DatabasePool.transaction is told to roll back.
        throw result.error;
      }
      return result.value;
    });
  }

  /**
   * Move a job to the dead-letter store. Returns false when the job was no
   * longer in a status from which DEAD_LETTER is reachable.
   *
   * Runs on the caller's executor so it joins the transaction that decided to
   * dead-letter — the archive row, the status change and the history entry are
   * one commit.
   */
  private async deadLetter(
    exec: Executor,
    row: JobRow,
    reason: string,
    error: string,
    now: Date
  ): Promise<Result<boolean, Error>> {
    // Flip the job first, guarded on status. If the guard misses, someone else
    // already moved the job and there is nothing to archive; inserting first
    // would leave an orphan dead-letter row describing a job that is still
    // live.
    const updated = await exec.query<{ attempt_count: number }>(
      `UPDATE durable_jobs
       SET status = $1, last_error = $2, updated_at = $3,
           lease_deadline = NULL, leased_by = NULL
       WHERE job_id = $4 AND status = ANY($5)
       RETURNING attempt_count`,
      [
        JobStatus.DEAD_LETTER,
        error,
        now,
        row.job_id,
        allowedPredecessors(JobStatus.DEAD_LETTER) as string[],
      ]
    );
    if (updated.kind === 'failure') {
      return failure(updated.error);
    }
    const updatedRow = updated.value[0];
    if (!updatedRow) {
      return success(false);
    }

    const archived = await exec.query(
      `INSERT INTO dead_letter_jobs (
         job_id, queue_name, payload, payload_version, attempt_count,
         idempotency_key, failure_reason, last_error, correlation_id, dead_lettered_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (job_id) DO NOTHING`,
      [
        row.job_id,
        row.queue_name,
        JSON.stringify(row.payload),
        row.payload_version,
        updatedRow.attempt_count,
        row.idempotency_key,
        reason,
        error,
        row.correlation_id,
        now,
      ]
    );
    if (archived.kind === 'failure') {
      return failure(archived.error);
    }

    const history = await this.recordHistory(
      exec,
      row.job_id as JobId,
      row.status as JobStatus,
      JobStatus.DEAD_LETTER,
      updatedRow.attempt_count,
      reason
    );
    if (history.kind === 'failure') {
      return failure(history.error);
    }
    return success(true);
  }

  /**
   * Append to the job audit trail.
   *
   * `attemptCount` is always supplied by the caller from the RETURNING clause
   * of the write it is describing, so the history row records the attempt the
   * transition actually belonged to and costs no extra query.
   */
  private async recordHistory(
    exec: Executor,
    jobId: JobId,
    fromStatus: JobStatus | null,
    toStatus: JobStatus,
    attemptCount: number,
    detail: string
  ): Promise<Result<void, Error>> {
    const inserted = await exec.query(
      `INSERT INTO job_history (job_id, from_status, to_status, attempt_count, detail, occurred_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [jobId, fromStatus, toStatus, attemptCount, detail, this.clock()]
    );
    if (inserted.kind === 'failure') {
      return failure(inserted.error);
    }
    return success(undefined);
  }
}
