import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { JobId, JobStatus, QueueName } from '@nit/domain';
import { Job, PermanentJobFailure } from './job';
import { JobQueue } from './queue';
import { JobHandler, Worker } from './worker';

/**
 * Worker behaviour end to end against a real Postgres. The worker's whole
 * contract is that it holds no state of its own — a crash must lose nothing
 * beyond the in-flight attempt — which is only observable through the database.
 */

const BACKOFF = { baseMs: 1_000, maxMs: 60_000, factor: 2 };
const LEASE_MS = 30_000;
const QUEUE = QueueName.INGESTION;

/** Records what it saw and optionally fails a configurable number of times. */
class RecordingHandler implements JobHandler<{ handle: string }> {
  readonly queueName: string = QUEUE;
  readonly seen: string[] = [];
  private remainingFailures: number;

  constructor(
    private readonly failure: Error | null = null,
    failTimes = Number.POSITIVE_INFINITY
  ) {
    this.remainingFailures = failure === null ? 0 : failTimes;
  }

  handle(job: Job<{ handle: string }>): Promise<void> {
    this.seen.push(job.payload.handle);
    if (this.failure !== null && this.remainingFailures > 0) {
      this.remainingFailures -= 1;
      return Promise.reject(this.failure);
    }
    return Promise.resolve();
  }
}

describe.skipIf(!hasTestDatabase())('Worker against Postgres', () => {
  let harness: TestDatabase;
  let now: Date;
  const clock = (): Date => now;

  const makeQueue = (): JobQueue =>
    new JobQueue(harness.db, {
      leaseTimeoutMs: LEASE_MS,
      defaultMaxAttempts: 3,
      backoff: BACKOFF,
      clock,
    });

  const makeWorker = (
    queue: JobQueue,
    handler: JobHandler<{ handle: string }>,
    workerId = 'worker-a'
  ): Worker<{ handle: string }> =>
    new Worker(queue, handler, { workerId, pollIntervalMs: 1, maxIdlePolls: 1 });

  const enqueue = async (
    queue: JobQueue,
    handle: string,
    maxAttempts?: number
  ): Promise<JobId> => {
    const result = await queue.enqueue({
      queueName: QUEUE,
      payload: { handle },
      payloadVersion: '1',
      idempotencyKey: handle,
      ...(maxAttempts !== undefined ? { maxAttempts } : {}),
    });
    if (result.kind === 'failure') {
      throw result.error;
    }
    return result.value.jobId;
  };

  beforeAll(async () => {
    harness = await createTestDatabase('worker');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  beforeEach(async () => {
    now = new Date('2026-01-01T00:00:00.000Z');
    const truncated = await harness.db.query(
      'TRUNCATE durable_jobs, dead_letter_jobs, job_history RESTART IDENTITY'
    );
    expect(truncated.kind).toBe('success');
  });

  describe('processOne', () => {
    it('claims, runs, and completes a single job', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'ok-1');
      const handler = new RecordingHandler();
      const worker = makeWorker(queue, handler);

      expect(await worker.processOne()).toEqual({ kind: 'success', value: true });
      expect(handler.seen).toEqual(['ok-1']);

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.COMPLETED);
      expect(stored.value?.leasedBy).toBeNull();
      expect(worker.getStats()).toMatchObject({
        processed: 1,
        failed: 0,
        deadLettered: 0,
        lostLeases: 0,
        errorCount: 0,
      });
    });

    it('reports false without touching the handler on an empty queue', async () => {
      const queue = makeQueue();
      const handler = new RecordingHandler();
      const worker = makeWorker(queue, handler);

      expect(await worker.processOne()).toEqual({ kind: 'success', value: false });
      expect(handler.seen).toEqual([]);
      expect(worker.getStats().processed).toBe(0);
    });

    it('schedules a retry when the handler throws a transient error', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'retry-1', 3);
      const handler = new RecordingHandler(new Error('transient'), 1);
      const worker = makeWorker(queue, handler);

      expect(await worker.processOne()).toEqual({ kind: 'success', value: true });
      expect(worker.getStats()).toMatchObject({ processed: 0, failed: 1, deadLettered: 0 });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.PENDING);
      expect(stored.value?.lastError).toBe('transient');
      expect(stored.value?.availableAt.getTime()).toBe(now.getTime() + BACKOFF.baseMs);

      // Second pass, past the backoff, succeeds and completes the job.
      now = new Date(now.getTime() + BACKOFF.baseMs);
      expect(await worker.processOne()).toEqual({ kind: 'success', value: true });
      expect(handler.seen).toEqual(['retry-1', 'retry-1']);

      const settled = await queue.getJob(jobId);
      if (settled.kind === 'failure') {
        throw settled.error;
      }
      expect(settled.value?.status).toBe(JobStatus.COMPLETED);
      expect(worker.getStats()).toMatchObject({ processed: 1, failed: 1 });
    });

    it('dead-letters immediately on PermanentJobFailure', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'permanent-1', 10);
      const handler = new RecordingHandler(new PermanentJobFailure('unsupported version'));
      const worker = makeWorker(queue, handler);

      expect(await worker.processOne()).toEqual({ kind: 'success', value: true });
      expect(worker.getStats()).toMatchObject({ processed: 0, failed: 1, deadLettered: 1 });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.DEAD_LETTER);
      // Nine attempts remained; a permanent failure must not consume them.
      expect(stored.value?.attemptCount).toBe(1);

      const letters = await queue.getDeadLetters(QUEUE);
      if (letters.kind === 'failure') {
        throw letters.error;
      }
      expect(letters.value[0]?.failureReason).toBe('permanent_failure');
      expect(handler.seen).toEqual(['permanent-1']);
    });

    it('dead-letters once transient attempts are exhausted', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'exhaust-1', 2);
      const handler = new RecordingHandler(new Error('always'));
      const worker = makeWorker(queue, handler);

      await worker.processOne();
      now = new Date(now.getTime() + BACKOFF.baseMs);
      await worker.processOne();

      expect(handler.seen).toEqual(['exhaust-1', 'exhaust-1']);
      expect(worker.getStats()).toMatchObject({ processed: 0, failed: 2, deadLettered: 1 });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.DEAD_LETTER);
      expect(stored.value?.attemptCount).toBe(2);
    });

    it('leaves a job alone once it reached a terminal status', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'terminal-1', 1);
      const handler = new RecordingHandler();
      const worker = makeWorker(queue, handler);

      await queue.claim(QUEUE, 'worker-a');
      await queue.fail(jobId, 'terminal');

      // Dead-lettered work is invisible to the claim query, so the worker sees
      // an empty queue rather than re-running a job that already gave up.
      expect(await worker.processOne()).toEqual({ kind: 'success', value: false });
      expect(handler.seen).toEqual([]);
    });
  });

  describe('run loop', () => {
    it('drains the queue then stops on the idle budget', async () => {
      const queue = makeQueue();
      for (let index = 0; index < 5; index += 1) {
        await enqueue(queue, `drain-${index}`);
      }

      const handler = new RecordingHandler();
      const worker = makeWorker(queue, handler);
      await worker.run();

      expect(handler.seen).toHaveLength(5);
      expect(new Set(handler.seen).size).toBe(5);
      expect(worker.getStats().processed).toBe(5);
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 0 });
    });

    it('recovers a lease abandoned by a crashed worker', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'crashed-1');

      // Simulate a worker that claimed, started, then died without reporting.
      await queue.claim(QUEUE, 'worker-dead');
      await queue.markRunning(jobId);

      const handler = new RecordingHandler();
      const survivor = makeWorker(queue, handler, 'worker-live');

      // Before the lease expires the job belongs to the dead worker.
      await survivor.run();
      expect(handler.seen).toEqual([]);

      // After expiry the run loop's recovery sweep releases and processes it.
      now = new Date(now.getTime() + LEASE_MS + 1);
      await survivor.run();

      expect(handler.seen).toEqual(['crashed-1']);
      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.COMPLETED);
      // Two attempts: the crashed one and the recovered one.
      expect(stored.value?.attemptCount).toBe(2);
    });

    it('leaves another queue untouched', async () => {
      const queue = makeQueue();
      const other = await queue.enqueue({
        queueName: QueueName.SCORING,
        payload: { handle: 'other-queue' },
        payloadVersion: '1',
        idempotencyKey: 'other-queue',
      });
      if (other.kind === 'failure') {
        throw other.error;
      }

      const handler = new RecordingHandler();
      await makeWorker(queue, handler).run();

      expect(handler.seen).toEqual([]);
      expect(await queue.pendingDepth(QueueName.SCORING)).toEqual({ kind: 'success', value: 1 });
    });

    it('stops when stop() is called', async () => {
      const queue = makeQueue();
      const handler = new RecordingHandler();
      const worker = new Worker(queue, handler, {
        workerId: 'worker-a',
        pollIntervalMs: 5,
      });

      const running = worker.run();
      worker.stop();
      await running;

      expect(handler.seen).toEqual([]);
    });
  });

  describe('concurrent workers', () => {
    it('splits a batch with no job processed twice', async () => {
      const queue = makeQueue();
      for (let index = 0; index < 10; index += 1) {
        await enqueue(queue, `shared-${index}`);
      }

      const handlers = [new RecordingHandler(), new RecordingHandler(), new RecordingHandler()];
      const workers = handlers.map((handler, index) =>
        makeWorker(queue, handler, `worker-${index}`)
      );

      // Driven with a fixed number of explicit polls rather than run(): under
      // SKIP LOCKED a worker can legitimately read an empty queue while others
      // hold rows, so an idle-poll budget would make this test racy.
      const drain = async (worker: Worker<{ handle: string }>): Promise<void> => {
        for (let poll = 0; poll < 20; poll += 1) {
          const result = await worker.processOne();
          if (result.kind === 'failure') {
            throw result.error;
          }
        }
      };
      await Promise.all(workers.map(drain));

      const allSeen = handlers.flatMap((handler) => handler.seen);
      expect(allSeen).toHaveLength(10);
      expect(new Set(allSeen).size).toBe(10);
      expect(workers.reduce((total, worker) => total + worker.getStats().processed, 0)).toBe(10);
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 0 });
    });
  });
});
