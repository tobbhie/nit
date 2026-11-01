import { JobId, Result, success, failure } from '@nit/domain';
import { Job, PermanentJobFailure } from './job';
import { FailOutcome, LeaseRecovery, StatusChangeOptions } from './queue';

export interface JobHandler<TPayload = unknown> {
  queueName: string;
  /**
   * Must be idempotent: the same job may be delivered more than once after a
   * crash or lease expiry.
   */
  handle(job: Job<TPayload>): Promise<void>;
}

/**
 * The queue surface a worker needs.
 *
 * Depending on this rather than on `JobQueue` directly is what makes the
 * lease-loss branches below testable. Those branches only run when a lease is
 * reclaimed between two of the worker's own calls, which is the hardest thing
 * to provoke on demand against a real database and the easiest to get wrong.
 */
export interface WorkerQueue {
  claim(queueName: string, workerId: string): Promise<Result<Job | null, Error>>;
  markRunning(jobId: JobId, options?: StatusChangeOptions): Promise<Result<boolean, Error>>;
  complete(jobId: JobId, options?: StatusChangeOptions): Promise<Result<boolean, Error>>;
  fail(
    jobId: JobId,
    error: string,
    options?: { permanent?: boolean; workerId?: string }
  ): Promise<Result<FailOutcome, Error>>;
  recoverExpiredLeases(): Promise<Result<LeaseRecovery, Error>>;
}

export interface WorkerOptions {
  workerId: string;
  pollIntervalMs: number;
  /** Stop after this many idle polls. Used by tests; undefined runs forever. */
  maxIdlePolls?: number;
  /**
   * Sweep expired leases on every Nth poll. Sweeping on every poll means the
   * work scales with the fleet — N workers contending on the same rows every
   * interval — for a job that only needs doing occasionally. The first poll of
   * a run always sweeps, so a restarted worker still recovers a crashed
   * predecessor's jobs immediately. Defaults to 10.
   */
  leaseSweepEveryPolls?: number;
}

export interface WorkerStats {
  processed: number;
  failed: number;
  /** Jobs this worker's own failures sent to the dead-letter queue. */
  deadLettered: number;
  /** Jobs lost to a reclaimed lease between claim and completion. */
  lostLeases: number;
  /** Expired leases this worker returned to the pending pool. */
  recoveredLeases: number;
  /** Expired leases this worker found out of attempts and dead-lettered. */
  recoveredToDeadLetter: number;
  lastSuccessAt: Date | null;
  errorCount: number;
}

const DEFAULT_LEASE_SWEEP_EVERY_POLLS = 10;

/**
 * Stateless, restart-safe worker. All progress lives in the database, so a
 * crashed worker loses nothing beyond the in-flight attempt, which another
 * worker recovers once the lease expires.
 */
export class Worker<TPayload = unknown> {
  private running = false;
  /** Resolves the current poll sleep early. Null when not sleeping. */
  private wake: (() => void) | null = null;
  private readonly stats: WorkerStats = {
    processed: 0,
    failed: 0,
    deadLettered: 0,
    lostLeases: 0,
    recoveredLeases: 0,
    recoveredToDeadLetter: 0,
    lastSuccessAt: null,
    errorCount: 0,
  };

  constructor(
    private readonly queue: WorkerQueue,
    private readonly handler: JobHandler<TPayload>,
    private readonly options: WorkerOptions
  ) {}

  getStats(): Readonly<WorkerStats> {
    return { ...this.stats };
  }

  stop(): void {
    this.running = false;
    // Cut the current poll sleep short. Otherwise shutdown takes up to a full
    // pollIntervalMs, which for a long interval eats most of the SIGTERM grace
    // period the platform allows (TIS ch.14) — time that belongs to draining
    // the in-flight job, not to waiting on a timer.
    this.wake?.();
  }

  /**
   * Claim and process a single job. Returns false when nothing was available.
   */
  async processOne(): Promise<Result<boolean, Error>> {
    const claimed = await this.queue.claim(this.handler.queueName, this.options.workerId);
    if (claimed.kind === 'failure') {
      this.stats.errorCount += 1;
      return failure(claimed.error);
    }
    if (!claimed.value) {
      return success(false);
    }

    const job = claimed.value as Job<TPayload>;
    const fence: StatusChangeOptions = { workerId: this.options.workerId };

    const marked = await this.queue.markRunning(job.jobId, fence);
    if (marked.kind === 'failure') {
      this.stats.errorCount += 1;
      return failure(marked.error);
    }
    if (!marked.value) {
      // Lost the job between claim and mark — another worker recovered the
      // expired lease. Do not run the handler; treat the poll as idle work.
      this.stats.lostLeases += 1;
      return success(false);
    }

    // Handler runs outside any database transaction.
    try {
      await this.handler.handle(job);
    } catch (error) {
      return this.onHandlerError(job.jobId, error);
    }

    const completed = await this.queue.complete(job.jobId, fence);
    if (completed.kind === 'failure') {
      this.stats.errorCount += 1;
      return failure(completed.error);
    }
    if (!completed.value) {
      // Handler succeeded but the lease was already reclaimed. Handlers are
      // required to be idempotent, so the duplicate run is safe.
      this.stats.lostLeases += 1;
      return success(true);
    }

    this.stats.processed += 1;
    this.stats.lastSuccessAt = new Date();
    return success(true);
  }

  /**
   * Run the poll loop until stop() is called or the idle budget is spent.
   */
  async run(): Promise<void> {
    this.running = true;
    let idlePolls = 0;
    let polls = 0;
    const sweepEvery = Math.max(1, this.options.leaseSweepEveryPolls ?? DEFAULT_LEASE_SWEEP_EVERY_POLLS);

    while (this.running) {
      if (polls % sweepEvery === 0) {
        await this.sweepExpiredLeases();
      }
      polls += 1;

      const result = await this.processOne();
      const didWork = result.kind === 'success' && result.value;

      if (didWork) {
        idlePolls = 0;
        continue;
      }

      idlePolls += 1;
      if (this.options.maxIdlePolls !== undefined && idlePolls >= this.options.maxIdlePolls) {
        this.running = false;
        break;
      }
      await this.sleep(this.options.pollIntervalMs);
    }

    this.wake = null;
  }

  private async sweepExpiredLeases(): Promise<void> {
    const recovered = await this.queue.recoverExpiredLeases();
    if (recovered.kind === 'failure') {
      this.stats.errorCount += 1;
      return;
    }
    this.stats.recoveredLeases += recovered.value.requeued.length;
    this.stats.recoveredToDeadLetter += recovered.value.deadLettered.length;
  }

  private async onHandlerError(jobId: JobId, error: unknown): Promise<Result<boolean, Error>> {
    const message = error instanceof Error ? error.message : String(error);
    const permanent = error instanceof PermanentJobFailure;

    const outcome = await this.queue.fail(jobId, message, {
      permanent,
      workerId: this.options.workerId,
    });
    if (outcome.kind === 'failure') {
      this.stats.errorCount += 1;
      return failure(outcome.error);
    }

    if (outcome.value === 'ignored') {
      // The lease was reclaimed while the handler ran, so this attempt has
      // already been written off by whoever recovered it. Recording it as this
      // worker's failure would double-count it.
      this.stats.lostLeases += 1;
      return success(true);
    }

    this.stats.failed += 1;
    this.stats.errorCount += 1;
    if (outcome.value === 'dead_letter') {
      this.stats.deadLettered += 1;
    }
    return success(true);
  }

  /**
   * Sleep that stop() can cut short. Clearing the timer also lets the process
   * exit promptly, rather than being held open by a pending poll interval.
   */
  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.wake = null;
        resolve();
      }, ms);
      this.wake = (): void => {
        clearTimeout(timer);
        this.wake = null;
        resolve();
      };
    });
  }
}
