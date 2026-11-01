import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { JobId, JobStatus, QueueName, generateCorrelationId } from '@nit/domain';
import { computeBackoffMs, BackoffPolicy } from './backoff';
import { JobQueue } from './queue';

/**
 * Durable-queue behaviour that only a real Postgres can prove: SKIP LOCKED
 * claim exclusivity, transactional leases, and the audit trail. A fake or
 * in-memory driver would report green here while the production path stayed
 * unverified, so these suites skip rather than substitute.
 */

const BACKOFF: BackoffPolicy = { baseMs: 1_000, maxMs: 60_000, factor: 2 };
const LEASE_MS = 30_000;
const QUEUE = QueueName.INGESTION;

describe.skipIf(!hasTestDatabase())('JobQueue against Postgres', () => {
  let harness: TestDatabase;

  /** Frozen clock, advanced explicitly so retry timing is exact, not racy. */
  let now: Date;
  const clock = (): Date => now;

  const makeQueue = (options: { maxAttempts?: number } = {}): JobQueue =>
    new JobQueue(harness.db, {
      leaseTimeoutMs: LEASE_MS,
      defaultMaxAttempts: options.maxAttempts ?? 3,
      backoff: BACKOFF,
      clock,
    });

  const enqueue = async (
    queue: JobQueue,
    idempotencyKey: string,
    overrides: { queueName?: string; maxAttempts?: number; availableAt?: Date } = {}
  ): Promise<JobId> => {
    const result = await queue.enqueue({
      queueName: overrides.queueName ?? QUEUE,
      payload: { handle: idempotencyKey },
      payloadVersion: '1',
      idempotencyKey,
      ...(overrides.maxAttempts !== undefined ? { maxAttempts: overrides.maxAttempts } : {}),
      ...(overrides.availableAt !== undefined ? { availableAt: overrides.availableAt } : {}),
    });
    if (result.kind === 'failure') {
      throw result.error;
    }
    return result.value.jobId;
  };

  beforeAll(async () => {
    harness = await createTestDatabase('queue');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  beforeEach(async () => {
    now = new Date('2026-01-01T00:00:00.000Z');
    const truncated = await harness.db.query(
      'TRUNCATE durable_jobs, dead_letter_jobs, job_history, domain_events RESTART IDENTITY'
    );
    expect(truncated.kind).toBe('success');
  });

  describe('publication and consumption', () => {
    it('round-trips a job from enqueue to claim', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'publish-1');

      const depth = await queue.pendingDepth(QUEUE);
      expect(depth).toEqual({ kind: 'success', value: 1 });

      const claimed = await queue.claim(QUEUE, 'worker-a');
      if (claimed.kind === 'failure') {
        throw claimed.error;
      }
      expect(claimed.value?.jobId).toBe(jobId);
      expect(claimed.value?.status).toBe(JobStatus.LEASED);
      expect(claimed.value?.leasedBy).toBe('worker-a');
      expect(claimed.value?.attemptCount).toBe(1);
      expect(claimed.value?.leaseDeadline?.getTime()).toBe(now.getTime() + LEASE_MS);
      expect(claimed.value?.payload).toEqual({ handle: 'publish-1' });

      // Leased work is no longer counted as queue depth.
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 0 });
    });

    it('returns null rather than failing on an empty queue', async () => {
      const queue = makeQueue();
      expect(await queue.claim(QUEUE, 'worker-a')).toEqual({ kind: 'success', value: null });
    });

    it('does not claim a job scheduled for the future', async () => {
      const queue = makeQueue();
      await enqueue(queue, 'delayed-1', { availableAt: new Date(now.getTime() + 5_000) });

      expect(await queue.claim(QUEUE, 'worker-a')).toEqual({ kind: 'success', value: null });

      now = new Date(now.getTime() + 5_000);
      const claimed = await queue.claim(QUEUE, 'worker-a');
      if (claimed.kind === 'failure') {
        throw claimed.error;
      }
      expect(claimed.value).not.toBeNull();
    });

    it('isolates queues from each other', async () => {
      const queue = makeQueue();
      await enqueue(queue, 'scoped-1', { queueName: QueueName.SCORING });

      expect(await queue.claim(QUEUE, 'worker-a')).toEqual({ kind: 'success', value: null });
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 0 });
      expect(await queue.pendingDepth(QueueName.SCORING)).toEqual({ kind: 'success', value: 1 });
    });
  });

  describe('concurrent claiming', () => {
    it('hands a single job to exactly one of eight racing workers', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'contended-1');

      const results = await Promise.all(
        Array.from({ length: 8 }, (_unused, index) => queue.claim(QUEUE, `worker-${index}`))
      );

      const winners = results.filter(
        (result) => result.kind === 'success' && result.value !== null
      );
      expect(winners).toHaveLength(1);
      expect(results.every((result) => result.kind === 'success')).toBe(true);

      // Without SKIP LOCKED the losers would block and then double-claim; the
      // attempt counter is the ground truth that only one lease was taken.
      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.attemptCount).toBe(1);
    });

    it('distributes twelve jobs across twelve workers without duplication', async () => {
      const queue = makeQueue();
      const enqueued: JobId[] = [];
      for (let index = 0; index < 12; index += 1) {
        enqueued.push(await enqueue(queue, `fanout-${index}`));
      }

      const results = await Promise.all(
        Array.from({ length: 12 }, (_unused, index) => queue.claim(QUEUE, `worker-${index}`))
      );

      const claimedIds = results.flatMap((result) =>
        result.kind === 'success' && result.value !== null ? [result.value.jobId] : []
      );
      expect(claimedIds).toHaveLength(12);
      expect(new Set(claimedIds).size).toBe(12);
      expect([...claimedIds].sort()).toEqual([...enqueued].sort());
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 0 });
    });
  });

  describe('lease expiry recovery', () => {
    it('leaves an unexpired lease alone', async () => {
      const queue = makeQueue();
      await enqueue(queue, 'lease-hold-1');
      await queue.claim(QUEUE, 'worker-a');

      now = new Date(now.getTime() + LEASE_MS - 1);
      expect(await queue.recoverExpiredLeases()).toEqual({
        kind: 'success',
        value: { requeued: [], deadLettered: [] },
      });
    });

    it('returns an expired lease to the pending pool for another worker', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'lease-expire-1');
      await queue.claim(QUEUE, 'worker-a');

      now = new Date(now.getTime() + LEASE_MS + 1);
      const recovered = await queue.recoverExpiredLeases();
      expect(recovered).toEqual({
        kind: 'success',
        value: { requeued: [jobId], deadLettered: [] },
      });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.PENDING);
      expect(stored.value?.leasedBy).toBeNull();
      expect(stored.value?.leaseDeadline).toBeNull();

      const reclaimed = await queue.claim(QUEUE, 'worker-b');
      if (reclaimed.kind === 'failure') {
        throw reclaimed.error;
      }
      expect(reclaimed.value?.jobId).toBe(jobId);
      expect(reclaimed.value?.leasedBy).toBe('worker-b');
      // The recovered attempt is counted, so retries stay bounded.
      expect(reclaimed.value?.attemptCount).toBe(2);
    });

    it('recovers a job that had already reached RUNNING', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'lease-running-1');
      await queue.claim(QUEUE, 'worker-a');
      expect(await queue.markRunning(jobId)).toEqual({ kind: 'success', value: true });

      now = new Date(now.getTime() + LEASE_MS + 1);
      expect(await queue.recoverExpiredLeases()).toEqual({
        kind: 'success',
        value: { requeued: [jobId], deadLettered: [] },
      });

      const history = await queue.getHistory(jobId);
      if (history.kind === 'failure') {
        throw history.error;
      }
      const recovery = history.value.at(-1);
      // The audit trail must name the status the job actually held.
      expect(recovery?.fromStatus).toBe(JobStatus.RUNNING);
      expect(recovery?.toStatus).toBe(JobStatus.PENDING);
    });

    it('dead-letters an expired lease that has no attempts left', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'lease-exhausted-1', { maxAttempts: 1 });
      await queue.claim(QUEUE, 'worker-a');

      // The worker died holding its only attempt. Requeueing here would let a
      // job that crashes its worker every time run forever, because a crash
      // would be recovered rather than counted.
      now = new Date(now.getTime() + LEASE_MS + 1);
      expect(await queue.recoverExpiredLeases()).toEqual({
        kind: 'success',
        value: { requeued: [], deadLettered: [jobId] },
      });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.DEAD_LETTER);
      expect(await queue.claim(QUEUE, 'worker-b')).toEqual({ kind: 'success', value: null });

      const letters = await queue.getDeadLetters(QUEUE);
      if (letters.kind === 'failure') {
        throw letters.error;
      }
      expect(letters.value).toHaveLength(1);
      expect(letters.value[0]?.failureReason).toBe('attempts_exhausted');
      expect(letters.value[0]?.attemptCount).toBe(1);
    });

    it('splits a mixed sweep into requeued and dead-lettered', async () => {
      const queue = makeQueue();
      const retryable = await enqueue(queue, 'lease-mixed-retry', { maxAttempts: 3 });
      const exhausted = await enqueue(queue, 'lease-mixed-exhausted', { maxAttempts: 1 });
      await queue.claim(QUEUE, 'worker-a');
      await queue.claim(QUEUE, 'worker-b');

      now = new Date(now.getTime() + LEASE_MS + 1);
      const recovered = await queue.recoverExpiredLeases();
      if (recovered.kind === 'failure') {
        throw recovered.error;
      }
      expect(recovered.value.requeued).toEqual([retryable]);
      expect(recovered.value.deadLettered).toEqual([exhausted]);
    });

    it('bounds a sweep to the configured batch size', async () => {
      const queue = new JobQueue(harness.db, {
        leaseTimeoutMs: LEASE_MS,
        defaultMaxAttempts: 3,
        backoff: BACKOFF,
        recoveryBatchSize: 2,
        clock,
      });
      for (let index = 0; index < 5; index += 1) {
        await enqueue(queue, `lease-batch-${index}`);
        await queue.claim(QUEUE, `worker-${index}`);
      }

      now = new Date(now.getTime() + LEASE_MS + 1);
      // A large backlog drains across sweeps rather than in one long
      // transaction holding every expired row.
      const first = await queue.recoverExpiredLeases();
      if (first.kind === 'failure') {
        throw first.error;
      }
      expect(first.value.requeued).toHaveLength(2);

      const second = await queue.recoverExpiredLeases();
      if (second.kind === 'failure') {
        throw second.error;
      }
      expect(second.value.requeued).toHaveLength(2);
    });

    it('fences markRunning on lease ownership when a worker id is given', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'lease-fence-1');
      await queue.claim(QUEUE, 'worker-a');

      now = new Date(now.getTime() + LEASE_MS + 1);
      await queue.recoverExpiredLeases();
      await queue.claim(QUEUE, 'worker-b');

      // worker-a's lease is gone. Fenced, its late markRunning must not touch
      // the job worker-b now owns — otherwise two handlers run concurrently
      // and each believes it holds the job.
      expect(await queue.markRunning(jobId, { workerId: 'worker-a' })).toEqual({
        kind: 'success',
        value: false,
      });
      expect(await queue.markRunning(jobId, { workerId: 'worker-b' })).toEqual({
        kind: 'success',
        value: true,
      });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.leasedBy).toBe('worker-b');
      expect(stored.value?.status).toBe(JobStatus.RUNNING);
    });

    it('fences complete and fail on lease ownership too', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'lease-fence-2', { maxAttempts: 5 });
      await queue.claim(QUEUE, 'worker-a');

      now = new Date(now.getTime() + LEASE_MS + 1);
      await queue.recoverExpiredLeases();
      await queue.claim(QUEUE, 'worker-b');

      expect(await queue.complete(jobId, { workerId: 'worker-a' })).toEqual({
        kind: 'success',
        value: false,
      });
      expect(await queue.fail(jobId, 'stale', { workerId: 'worker-a' })).toEqual({
        kind: 'success',
        value: 'ignored',
      });

      // worker-b's lease survived both stale calls intact.
      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.LEASED);
      expect(stored.value?.leasedBy).toBe('worker-b');
      expect(stored.value?.lastError).toBeNull();
    });

    it('guards on status alone when no worker id is given', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'lease-unfenced-1');
      await queue.claim(QUEUE, 'worker-a');

      now = new Date(now.getTime() + LEASE_MS + 1);
      await queue.recoverExpiredLeases();
      await queue.claim(QUEUE, 'worker-b');

      // Fencing is opt-in, so an unfenced caller — a maintenance script, say —
      // can still advance whatever lease currently exists.
      expect(await queue.markRunning(jobId)).toEqual({ kind: 'success', value: true });
    });

    it('returns false from markRunning once the job left the leased pool', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'lease-terminal-1', { maxAttempts: 1 });
      await queue.claim(QUEUE, 'worker-a');
      await queue.fail(jobId, 'terminal');

      // Dead-lettered work is terminal: RUNNING is unreachable from it.
      expect(await queue.markRunning(jobId)).toEqual({ kind: 'success', value: false });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.DEAD_LETTER);
    });
  });

  describe('retry determinism', () => {
    it('schedules each retry at exactly the computed backoff', async () => {
      const queue = makeQueue({ maxAttempts: 5 });
      const jobId = await enqueue(queue, 'retry-1', { maxAttempts: 5 });

      for (let attempt = 1; attempt <= 3; attempt += 1) {
        const claimed = await queue.claim(QUEUE, 'worker-a');
        if (claimed.kind === 'failure') {
          throw claimed.error;
        }
        expect(claimed.value?.attemptCount).toBe(attempt);

        const failedAt = now;
        const outcome = await queue.fail(jobId, `boom ${attempt}`);
        expect(outcome).toEqual({ kind: 'success', value: 'retry' });

        const stored = await queue.getJob(jobId);
        if (stored.kind === 'failure') {
          throw stored.error;
        }
        expect(stored.value?.status).toBe(JobStatus.PENDING);
        expect(stored.value?.lastError).toBe(`boom ${attempt}`);
        expect(stored.value?.availableAt.getTime()).toBe(
          failedAt.getTime() + computeBackoffMs(attempt, BACKOFF)
        );

        // Advance past the backoff so the next claim is eligible.
        now = new Date(stored.value?.availableAt.getTime() ?? 0);
      }
    });

    it('produces identical retry timing for two independent jobs', async () => {
      const queue = makeQueue({ maxAttempts: 5 });
      const first = await enqueue(queue, 'determinism-a', { maxAttempts: 5 });
      const second = await enqueue(queue, 'determinism-b', { maxAttempts: 5 });

      for (const jobId of [first, second]) {
        await queue.claim(QUEUE, 'worker-a');
        await queue.fail(jobId, 'same failure');
      }

      const [a, b] = await Promise.all([queue.getJob(first), queue.getJob(second)]);
      if (a.kind === 'failure' || b.kind === 'failure') {
        throw a.kind === 'failure' ? a.error : new Error('unreachable');
      }
      expect(a.value?.availableAt.getTime()).toBe(b.value?.availableAt.getTime());
    });

    it('caps backoff at the policy ceiling', () => {
      expect(computeBackoffMs(1, BACKOFF)).toBe(1_000);
      expect(computeBackoffMs(2, BACKOFF)).toBe(2_000);
      expect(computeBackoffMs(7, BACKOFF)).toBe(60_000);
      expect(computeBackoffMs(50, BACKOFF)).toBe(60_000);
    });
  });

  describe('idempotency', () => {
    it('returns the existing job instead of creating a duplicate', async () => {
      const queue = makeQueue();
      const first = await enqueue(queue, 'idem-1');
      const second = await enqueue(queue, 'idem-1');

      expect(second).toBe(first);
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 1 });
    });

    it('deduplicates a burst of concurrent enqueues to one job', async () => {
      const queue = makeQueue();
      const results = await Promise.all(
        Array.from({ length: 6 }, () =>
          queue.enqueue({
            queueName: QUEUE,
            payload: { handle: 'idem-burst' },
            payloadVersion: '1',
            idempotencyKey: 'idem-burst',
          })
        )
      );

      const ids = results.flatMap((result) =>
        result.kind === 'success' ? [result.value.jobId] : []
      );
      expect(ids).toHaveLength(6);
      expect(new Set(ids).size).toBe(1);
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 1 });
    });

    it('scopes the idempotency key per queue', async () => {
      const queue = makeQueue();
      const ingestion = await enqueue(queue, 'shared-key');
      const scoring = await enqueue(queue, 'shared-key', { queueName: QueueName.SCORING });

      // The same logical key in two queues is two distinct jobs.
      expect(scoring).not.toBe(ingestion);
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 1 });
      expect(await queue.pendingDepth(QueueName.SCORING)).toEqual({ kind: 'success', value: 1 });
    });

    it('preserves the original payload when a duplicate arrives', async () => {
      const queue = makeQueue();
      const first = await queue.enqueue({
        queueName: QUEUE,
        payload: { version: 'original' },
        payloadVersion: '1',
        idempotencyKey: 'idem-payload',
      });
      const second = await queue.enqueue({
        queueName: QUEUE,
        payload: { version: 'replacement' },
        payloadVersion: '1',
        idempotencyKey: 'idem-payload',
      });

      if (first.kind === 'failure' || second.kind === 'failure') {
        throw first.kind === 'failure' ? first.error : new Error('unreachable');
      }
      expect(second.value.jobId).toBe(first.value.jobId);
      expect(second.value.payload).toEqual({ version: 'original' });
    });
  });

  describe('dead-lettering', () => {
    it('dead-letters after exactly maxAttempts attempts', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'dlq-exhaust', { maxAttempts: 2 });

      const claimA = await queue.claim(QUEUE, 'worker-a');
      if (claimA.kind === 'failure') {
        throw claimA.error;
      }
      expect(claimA.value?.attemptCount).toBe(1);
      expect(await queue.fail(jobId, 'first')).toEqual({ kind: 'success', value: 'retry' });

      now = new Date(now.getTime() + computeBackoffMs(1, BACKOFF));
      const claimB = await queue.claim(QUEUE, 'worker-a');
      if (claimB.kind === 'failure') {
        throw claimB.error;
      }
      expect(claimB.value?.attemptCount).toBe(2);
      expect(await queue.fail(jobId, 'second')).toEqual({
        kind: 'success',
        value: 'dead_letter',
      });

      const letters = await queue.getDeadLetters(QUEUE);
      if (letters.kind === 'failure') {
        throw letters.error;
      }
      expect(letters.value).toHaveLength(1);
      expect(letters.value[0]?.jobId).toBe(jobId);
      expect(letters.value[0]?.failureReason).toBe('attempts_exhausted');
      expect(letters.value[0]?.lastError).toBe('second');
      expect(letters.value[0]?.attemptCount).toBe(2);

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.DEAD_LETTER);
      expect(await queue.claim(QUEUE, 'worker-b')).toEqual({ kind: 'success', value: null });
    });

    it('dead-letters a permanent failure on the first attempt', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'dlq-permanent', { maxAttempts: 10 });
      await queue.claim(QUEUE, 'worker-a');

      expect(await queue.fail(jobId, 'unsupported payload', { permanent: true })).toEqual({
        kind: 'success',
        value: 'dead_letter',
      });

      const letters = await queue.getDeadLetters(QUEUE);
      if (letters.kind === 'failure') {
        throw letters.error;
      }
      expect(letters.value[0]?.failureReason).toBe('permanent_failure');
      expect(letters.value[0]?.attemptCount).toBe(1);
    });

    it('carries the correlation id into the dead-letter record', async () => {
      const queue = makeQueue();
      const correlationId = generateCorrelationId();
      const enqueued = await queue.enqueue({
        queueName: QUEUE,
        payload: { handle: 'dlq-correlated' },
        payloadVersion: '1',
        idempotencyKey: 'dlq-correlated',
        maxAttempts: 1,
        correlationId,
      });
      if (enqueued.kind === 'failure') {
        throw enqueued.error;
      }

      await queue.claim(QUEUE, 'worker-a');
      await queue.fail(enqueued.value.jobId, 'nope');

      const letters = await queue.getDeadLetters(QUEUE);
      if (letters.kind === 'failure') {
        throw letters.error;
      }
      expect(letters.value[0]?.correlationId).toBe(correlationId);
    });

    it('ignores a second failure on an already dead-lettered job', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'dlq-twice', { maxAttempts: 1 });
      await queue.claim(QUEUE, 'worker-a');
      await queue.fail(jobId, 'first');

      expect(await queue.fail(jobId, 'second')).toEqual({ kind: 'success', value: 'ignored' });

      const letters = await queue.getDeadLetters(QUEUE);
      if (letters.kind === 'failure') {
        throw letters.error;
      }
      expect(letters.value).toHaveLength(1);
      expect(letters.value[0]?.lastError).toBe('first');
    });

    it('refuses to complete a dead-lettered job', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'dlq-complete', { maxAttempts: 1 });
      await queue.claim(QUEUE, 'worker-a');
      await queue.fail(jobId, 'gone');

      expect(await queue.complete(jobId)).toEqual({ kind: 'success', value: false });

      const stored = await queue.getJob(jobId);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.DEAD_LETTER);
      expect(stored.value?.completedAt).toBeNull();
    });

    it('reports a missing job as a failure rather than a dead letter', async () => {
      const queue = makeQueue();
      const outcome = await queue.fail('00000000-0000-4000-8000-000000000000' as JobId, 'x');
      expect(outcome.kind).toBe('failure');
    });
  });

  describe('replayable audit history', () => {
    it('records the full success path in order', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'audit-success');
      await queue.claim(QUEUE, 'worker-a');
      await queue.markRunning(jobId);
      expect(await queue.complete(jobId)).toEqual({ kind: 'success', value: true });

      const history = await queue.getHistory(jobId);
      if (history.kind === 'failure') {
        throw history.error;
      }
      expect(history.value.map((entry) => [entry.fromStatus, entry.toStatus])).toEqual([
        [null, JobStatus.PENDING],
        [JobStatus.PENDING, JobStatus.LEASED],
        [JobStatus.LEASED, JobStatus.RUNNING],
        [JobStatus.RUNNING, JobStatus.COMPLETED],
      ]);
    });

    it('records a retry from the status the job actually held', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'audit-retry', { maxAttempts: 3 });
      await queue.claim(QUEUE, 'worker-a');
      // No markRunning: a worker may fail before it ever reports RUNNING.
      await queue.fail(jobId, 'early failure');

      const history = await queue.getHistory(jobId);
      if (history.kind === 'failure') {
        throw history.error;
      }
      expect(history.value.map((entry) => [entry.fromStatus, entry.toStatus])).toEqual([
        [null, JobStatus.PENDING],
        [JobStatus.PENDING, JobStatus.LEASED],
        [JobStatus.LEASED, JobStatus.PENDING],
      ]);
      expect(history.value.at(-1)?.detail).toContain('retry scheduled for');
    });

    it('records every attempt of a job that ends dead-lettered', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'audit-dlq', { maxAttempts: 2 });

      await queue.claim(QUEUE, 'worker-a');
      await queue.fail(jobId, 'first');
      now = new Date(now.getTime() + computeBackoffMs(1, BACKOFF));
      await queue.claim(QUEUE, 'worker-b');
      await queue.markRunning(jobId);
      await queue.fail(jobId, 'second');

      const history = await queue.getHistory(jobId);
      if (history.kind === 'failure') {
        throw history.error;
      }
      expect(history.value.map((entry) => entry.toStatus)).toEqual([
        JobStatus.PENDING,
        JobStatus.LEASED,
        JobStatus.PENDING,
        JobStatus.LEASED,
        JobStatus.RUNNING,
        JobStatus.DEAD_LETTER,
      ]);
      expect(history.value.at(-1)?.detail).toBe('attempts_exhausted');
      expect(history.value.at(-1)?.fromStatus).toBe(JobStatus.RUNNING);
    });

    it('attributes each lease to the worker that took it', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'audit-attribution');
      await queue.claim(QUEUE, 'worker-alpha');

      const history = await queue.getHistory(jobId);
      if (history.kind === 'failure') {
        throw history.error;
      }
      expect(history.value.at(-1)?.detail).toBe('leased by worker-alpha');
    });

    it('stamps history with the injected clock so replays are reproducible', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'audit-clock');

      const history = await queue.getHistory(jobId);
      if (history.kind === 'failure') {
        throw history.error;
      }
      expect(history.value[0]?.occurredAt.getTime()).toBe(now.getTime());
    });

    it('does not apply a state change whose history write fails', async () => {
      const queue = makeQueue();
      const jobId = await enqueue(queue, 'audit-atomic');
      await queue.claim(QUEUE, 'worker-a');
      await queue.markRunning(jobId);

      // Force the history insert to fail. TIS ch.3 §8 treats the audit trail as
      // a guarantee, so a job must not be able to reach a status the history
      // does not record — otherwise replay (ch.19 §7) reconstructs a different
      // timeline from the one that ran.
      const constrained = await harness.db.query(
        `ALTER TABLE job_history ADD CONSTRAINT reject_completed
         CHECK (to_status <> 'completed')`
      );
      expect(constrained.kind).toBe('success');

      try {
        expect((await queue.complete(jobId)).kind).toBe('failure');

        const stored = await queue.getJob(jobId);
        if (stored.kind === 'failure') {
          throw stored.error;
        }
        expect(stored.value?.status).toBe(JobStatus.RUNNING);
        expect(stored.value?.completedAt).toBeNull();
      } finally {
        await harness.db.query('ALTER TABLE job_history DROP CONSTRAINT reject_completed');
      }
    });
  });

  describe('transactional enqueue', () => {
    it('commits a caller-owned write and the job it schedules together', async () => {
      const queue = makeQueue();

      const committed = await harness.db.transaction(async (client) => {
        await client.query(
          `INSERT INTO domain_events (
             event_id, aggregate_id, aggregate_type, event_type,
             version, occurred_at, producer, payload
           ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
          [
            '11111111-1111-4111-8111-111111111111',
            '22222222-2222-4222-8222-222222222222',
            'narrative',
            'narrative.ingested',
            '1',
            now,
            'queue.pg.test',
            JSON.stringify({ handle: 'outbox-commit' }),
          ]
        );
        const job = await queue.enqueue(
          {
            queueName: QUEUE,
            payload: { handle: 'outbox-commit' },
            payloadVersion: '1',
            idempotencyKey: 'outbox-commit',
          },
          client
        );
        if (job.kind === 'failure') {
          throw job.error;
        }
        return job.value.jobId;
      });

      if (committed.kind === 'failure') {
        throw committed.error;
      }
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 1 });

      const stored = await queue.getJob(committed.value);
      if (stored.kind === 'failure') {
        throw stored.error;
      }
      expect(stored.value?.status).toBe(JobStatus.PENDING);
    });

    it('discards the job when the caller aborts the transaction', async () => {
      const queue = makeQueue();

      const aborted = await harness.db.transaction(async (client) => {
        const job = await queue.enqueue(
          {
            queueName: QUEUE,
            payload: { handle: 'outbox-abort' },
            payloadVersion: '1',
            idempotencyKey: 'outbox-abort',
          },
          client
        );
        if (job.kind === 'failure') {
          throw job.error;
        }
        // Stands in for the domain write failing after the job was staged. The
        // job must not survive: a consumer would otherwise process work for a
        // row that does not exist.
        throw new Error('domain write failed');
      });

      expect(aborted.kind).toBe('failure');
      expect(await queue.pendingDepth(QUEUE)).toEqual({ kind: 'success', value: 0 });

      const history = await harness.db.query<{ count: string }>(
        'SELECT COUNT(*)::text AS count FROM job_history'
      );
      if (history.kind === 'failure') {
        throw history.error;
      }
      // The history row was rolled back with the job, leaving no orphan audit
      // entry for a job that never existed.
      expect(history.value[0]?.count).toBe('0');
    });
  });
});
