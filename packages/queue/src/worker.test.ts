import { describe, expect, it } from 'vitest';
import { JobId, JobStatus, Result, success, failure } from '@nit/domain';
import { Job, PermanentJobFailure } from './job';
import { FailOutcome, LeaseRecovery, StatusChangeOptions } from './queue';
import { JobHandler, Worker, WorkerOptions, WorkerQueue } from './worker';

/**
 * Worker behaviour under lease loss, sweep throttling, and shutdown.
 *
 * Every lease-loss branch here fires only when a lease is reclaimed between two
 * of the worker's own calls. Provoking that against a real Postgres means
 * winning a race on purpose, so these drive a scripted queue instead — the
 * database-backed suite in worker.pg.test.ts covers what a fake cannot prove.
 */

const JOB_ID = '33333333-3333-4333-8333-333333333333' as JobId;
const QUEUE = 'ingestion';

function makeJob(): Job<{ handle: string }> {
  const at = new Date('2026-01-01T00:00:00.000Z');
  return {
    jobId: JOB_ID,
    queueName: QUEUE,
    payload: { handle: 'scripted' },
    payloadVersion: '1',
    status: JobStatus.LEASED,
    attemptCount: 1,
    maxAttempts: 3,
    leaseDeadline: at,
    leasedBy: 'worker-a',
    idempotencyKey: 'scripted',
    availableAt: at,
    lastError: null,
    correlationId: null,
    createdAt: at,
    updatedAt: at,
    completedAt: null,
  };
}

interface Script {
  /** Jobs to hand out before reporting an empty queue. Defaults to one. */
  claims?: number;
  markRunning?: Result<boolean, Error>;
  complete?: Result<boolean, Error>;
  fail?: Result<FailOutcome, Error>;
  recover?: Result<LeaseRecovery, Error>;
}

/** A queue whose every answer is decided up front. */
class ScriptedQueue implements WorkerQueue {
  sweeps = 0;
  /** The workerId passed to each status change, in call order. */
  readonly fences: Array<string | undefined> = [];
  private remainingClaims: number;

  constructor(private readonly script: Script = {}) {
    this.remainingClaims = script.claims ?? 1;
  }

  claim(_queueName: string, _workerId: string): Promise<Result<Job | null, Error>> {
    if (this.remainingClaims <= 0) {
      return Promise.resolve(success(null));
    }
    this.remainingClaims -= 1;
    return Promise.resolve(success(makeJob()));
  }

  markRunning(_jobId: JobId, options?: StatusChangeOptions): Promise<Result<boolean, Error>> {
    this.fences.push(options?.workerId);
    return Promise.resolve(this.script.markRunning ?? success(true));
  }

  complete(_jobId: JobId, options?: StatusChangeOptions): Promise<Result<boolean, Error>> {
    this.fences.push(options?.workerId);
    return Promise.resolve(this.script.complete ?? success(true));
  }

  fail(
    _jobId: JobId,
    _error: string,
    options?: { permanent?: boolean; workerId?: string }
  ): Promise<Result<FailOutcome, Error>> {
    this.fences.push(options?.workerId);
    return Promise.resolve(this.script.fail ?? success('retry'));
  }

  recoverExpiredLeases(): Promise<Result<LeaseRecovery, Error>> {
    this.sweeps += 1;
    return Promise.resolve(this.script.recover ?? success({ requeued: [], deadLettered: [] }));
  }
}

class CountingHandler implements JobHandler<{ handle: string }> {
  readonly queueName: string = QUEUE;
  calls = 0;

  constructor(private readonly throws: Error | null = null) {}

  handle(): Promise<void> {
    this.calls += 1;
    return this.throws === null ? Promise.resolve() : Promise.reject(this.throws);
  }
}

/** Stops its own worker mid-job, standing in for a SIGTERM during a handler. */
class StoppingHandler implements JobHandler<{ handle: string }> {
  readonly queueName: string = QUEUE;
  calls = 0;
  worker: Worker<{ handle: string }> | null = null;

  handle(): Promise<void> {
    this.calls += 1;
    this.worker?.stop();
    return Promise.resolve();
  }
}

const workerOptions = (overrides: Partial<Omit<WorkerOptions, 'workerId'>> = {}): WorkerOptions => ({
  workerId: 'worker-a',
  pollIntervalMs: 0,
  ...overrides,
});

describe('Worker lease loss', () => {
  it('does not run the handler when the lease was reclaimed before RUNNING', async () => {
    const queue = new ScriptedQueue({ markRunning: success(false) });
    const handler = new CountingHandler();
    const worker = new Worker(queue, handler, workerOptions());

    // Reported as no work done: the poll was idle, not successful.
    expect(await worker.processOne()).toEqual({ kind: 'success', value: false });
    expect(handler.calls).toBe(0);
    expect(worker.getStats()).toMatchObject({ processed: 0, failed: 0, lostLeases: 1 });
  });

  it('counts a lease lost after the handler already succeeded', async () => {
    const queue = new ScriptedQueue({ complete: success(false) });
    const handler = new CountingHandler();
    const worker = new Worker(queue, handler, workerOptions());

    // The handler did run, so the poll did work — but the completion belongs to
    // whoever holds the lease now. Handlers are required to be idempotent
    // precisely so this duplicate run is harmless.
    expect(await worker.processOne()).toEqual({ kind: 'success', value: true });
    expect(handler.calls).toBe(1);
    expect(worker.getStats()).toMatchObject({ processed: 0, lostLeases: 1 });
  });

  it('does not record a failure the queue ignored', async () => {
    const queue = new ScriptedQueue({ fail: success('ignored') });
    const handler = new CountingHandler(new Error('boom'));
    const worker = new Worker(queue, handler, workerOptions());

    expect(await worker.processOne()).toEqual({ kind: 'success', value: true });
    // The attempt was already written off by whoever reclaimed the lease.
    // Counting it here would book one failure twice across two workers.
    expect(worker.getStats()).toMatchObject({ failed: 0, deadLettered: 0, lostLeases: 1 });
  });

  it('records a dead-lettered permanent failure', async () => {
    const queue = new ScriptedQueue({ fail: success('dead_letter') });
    const handler = new CountingHandler(new PermanentJobFailure('unsupported version'));
    const worker = new Worker(queue, handler, workerOptions());

    expect(await worker.processOne()).toEqual({ kind: 'success', value: true });
    expect(worker.getStats()).toMatchObject({ failed: 1, deadLettered: 1, lostLeases: 0 });
  });

  it('counts a retry as failed without dead-lettering it', async () => {
    const queue = new ScriptedQueue({ fail: success('retry') });
    const worker = new Worker(queue, new CountingHandler(new Error('transient')), workerOptions());

    await worker.processOne();
    expect(worker.getStats()).toMatchObject({ failed: 1, deadLettered: 0, lostLeases: 0 });
  });

  it('fences every status change with its own worker id', async () => {
    const queue = new ScriptedQueue();
    const worker = new Worker(queue, new CountingHandler(), workerOptions());

    await worker.processOne();

    // markRunning and complete must both carry the id, or a worker whose lease
    // expired mid-handler can still act on a job another worker now owns.
    expect(queue.fences).toEqual(['worker-a', 'worker-a']);
  });

  it('fences the failure path too', async () => {
    const queue = new ScriptedQueue();
    const worker = new Worker(queue, new CountingHandler(new Error('boom')), workerOptions());

    await worker.processOne();

    // markRunning, then fail — an unfenced fail() would consume an attempt on
    // whichever attempt is live now, not the one that actually failed.
    expect(queue.fences).toEqual(['worker-a', 'worker-a']);
  });

  it('surfaces a queue error as a failure and counts it', async () => {
    const queue = new ScriptedQueue({ markRunning: failure(new Error('connection reset')) });
    const worker = new Worker(queue, new CountingHandler(), workerOptions());

    const result = await worker.processOne();
    expect(result.kind).toBe('failure');
    expect(worker.getStats().errorCount).toBe(1);
  });
});

describe('Worker recovery sweeps', () => {
  it('sweeps on the first poll and then only every Nth', async () => {
    const queue = new ScriptedQueue({ claims: 0 });
    const worker = new Worker(
      queue,
      new CountingHandler(),
      workerOptions({ maxIdlePolls: 5, leaseSweepEveryPolls: 3 })
    );

    await worker.run();

    // Five polls, sweeping on 0 and 3. Sweeping every poll would make the work
    // scale with fleet size — N workers contending on the same rows every
    // interval — for a job that only needs doing occasionally.
    expect(queue.sweeps).toBe(2);
  });

  it('still sweeps immediately on startup with the default interval', async () => {
    const queue = new ScriptedQueue({ claims: 0 });
    const worker = new Worker(queue, new CountingHandler(), workerOptions({ maxIdlePolls: 5 }));

    await worker.run();

    // A restarted worker must recover a crashed predecessor's jobs on its first
    // poll rather than after the throttle interval has elapsed.
    expect(queue.sweeps).toBe(1);
  });

  it('attributes recovered leases to the right counter', async () => {
    const queue = new ScriptedQueue({
      claims: 0,
      recover: success({
        requeued: ['a' as JobId, 'b' as JobId],
        deadLettered: ['c' as JobId],
      }),
    });
    const worker = new Worker(queue, new CountingHandler(), workerOptions({ maxIdlePolls: 1 }));

    await worker.run();

    // A job that ran out of attempts during a crash is not the same event as a
    // routine requeue, and conflating them hides work that needs a human.
    // deadLettered stays zero: it counts only this worker's own handler
    // failures, so it remains diagnostic of this worker rather than the fleet.
    expect(worker.getStats()).toMatchObject({
      recoveredLeases: 2,
      recoveredToDeadLetter: 1,
      deadLettered: 0,
    });
  });

  it('counts a failed sweep without stopping the loop', async () => {
    const queue = new ScriptedQueue({ claims: 0, recover: failure(new Error('sweep failed')) });
    const worker = new Worker(queue, new CountingHandler(), workerOptions({ maxIdlePolls: 2 }));

    await worker.run();

    // A database blip during recovery must not take the worker down; the
    // remaining polls still ran and drained the idle budget.
    expect(worker.getStats().errorCount).toBe(1);
  });
});

describe('Worker shutdown', () => {
  it('cuts a long poll sleep short on stop', async () => {
    const queue = new ScriptedQueue({ claims: 0 });
    // No maxIdlePolls: the loop ends only when stop() is called.
    const worker = new Worker(queue, new CountingHandler(), {
      workerId: 'worker-a',
      pollIntervalMs: 60_000,
    });

    const startedAt = Date.now();
    const running = worker.run();
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    worker.stop();
    await running;

    // An uninterruptible sleep would hold the process for the full minute,
    // which against a platform SIGTERM grace period means being killed
    // mid-job instead of draining cleanly.
    expect(Date.now() - startedAt).toBeLessThan(2_000);
  });

  it('finishes the job in hand before shutting down', async () => {
    const queue = new ScriptedQueue({ claims: 5 });
    const handler = new StoppingHandler();
    const worker = new Worker(queue, handler, workerOptions());
    handler.worker = worker;

    await worker.run();

    // Five jobs were waiting but stop() landed during the first. The claimed job
    // still completes and is counted — shutdown drains the in-flight attempt
    // rather than abandoning it to a lease timeout.
    expect(handler.calls).toBe(1);
    expect(worker.getStats().processed).toBe(1);
  });
});
