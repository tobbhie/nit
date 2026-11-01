import { describe, expect, it } from 'vitest';
import {
  JobStatus,
  ObservationProvider,
  QueueName,
  failure,
  generateJobId,
  success,
} from '@nit/domain';
import { Job, PermanentJobFailure } from '@nit/queue';
import { ObservationSourceAdapter, SourcePost } from './contracts';
import {
  FakeUnitOfWork,
  INGESTED,
  InMemoryObservationRepository,
  StubObservationSource,
  post,
} from './in-memory.fixtures';
import {
  OBSERVATION_POLL_PAYLOAD_VERSION,
  ObservationPollHandler,
  ObservationPollPayload,
  PollSummary,
} from './poll-handler';
import { ObservationIngestionService } from './service';
import { SyntheticObservationSource } from './synthetic-source';

const AUTHOR = 'monitored-account-1';

interface Harness {
  handler: ObservationPollHandler;
  uow: FakeUnitOfWork;
  summaries: PollSummary[];
}

function harness(
  source: ObservationSourceAdapter,
  options: { appendFails?: boolean } = {}
): Harness {
  const uow = new FakeUnitOfWork(options.appendFails ?? false);
  const summaries: PollSummary[] = [];
  const handler = new ObservationPollHandler(
    new Map([[source.provider, source]]),
    // Outside the transaction, the way a pool-backed repository reaches the
    // handler: the cursor read is its own query, not part of the ingest.
    new InMemoryObservationRepository(uow.store),
    new ObservationIngestionService(uow, { producer: 'poll-test', clock: () => INGESTED }),
    { onPolled: (summary) => summaries.push(summary) }
  );
  return { handler, uow, summaries };
}

interface JobOverrides {
  provider?: ObservationProvider;
  authorId?: string;
  sinceProviderPostId?: string;
  limit?: number;
  payloadVersion?: string;
}

/**
 * Build a job envelope. Spelled out rather than spread from a Partial for the
 * same reason `post` is: a Partial widens the optional fields to `| undefined`,
 * which `exactOptionalPropertyTypes` will not assign back to the payload.
 */
function job(overrides: JobOverrides = {}): Job<ObservationPollPayload> {
  const payload: ObservationPollPayload = {
    provider: overrides.provider ?? ObservationProvider.X,
    authorId: overrides.authorId ?? AUTHOR,
  };
  if (overrides.sinceProviderPostId !== undefined) {
    payload.sinceProviderPostId = overrides.sinceProviderPostId;
  }
  if (overrides.limit !== undefined) {
    payload.limit = overrides.limit;
  }

  return {
    jobId: generateJobId(),
    queueName: QueueName.OBSERVATION_POLL,
    payload,
    payloadVersion: overrides.payloadVersion ?? OBSERVATION_POLL_PAYLOAD_VERSION,
    status: JobStatus.RUNNING,
    attemptCount: 1,
    maxAttempts: 3,
    leaseDeadline: null,
    leasedBy: 'worker-1',
    idempotencyKey: `${QueueName.OBSERVATION_POLL}:${payload.provider}:${payload.authorId}`,
    availableAt: INGESTED,
    lastError: null,
    correlationId: null,
    createdAt: INGESTED,
    updatedAt: INGESTED,
    completedAt: null,
  };
}

/** A post from the account under poll. The default author is not this one. */
function authored(providerPostId: string, secondsIn: number): SourcePost {
  return post({
    providerPostId,
    authorId: AUTHOR,
    publishedAt: new Date(`2026-08-19T10:00:0${secondsIn}.000Z`),
    text: `post ${providerPostId}`,
  });
}

function unwrap(summary: PollSummary | undefined): PollSummary {
  if (summary === undefined) {
    throw new Error('Expected a poll summary');
  }
  return summary;
}

/** Assert a handle() call rejected, and hand back the error so its type can be checked. */
async function rejection(work: Promise<void>): Promise<Error> {
  try {
    await work;
  } catch (error) {
    return error as Error;
  }
  throw new Error('Expected the poll to fail');
}

async function corpusOf(source: SyntheticObservationSource): Promise<SourcePost[]> {
  const result = await source.fetchAuthorPosts({ authorId: AUTHOR, limit: 100 });
  if (result.kind === 'failure') {
    throw result.error;
  }
  return result.value;
}

describe('ObservationPollHandler', () => {
  it('ingests an account it has never seen', async () => {
    const source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 5 });
    const { handler, uow, summaries } = harness(source);

    await handler.handle(job());

    const summary = unwrap(summaries[0]);
    expect(summary.sinceProviderPostId).toBeNull();
    expect(summary.fetched).toBe(5);
    expect(summary.ingested).toBe(5);
    expect(summary.duplicates).toBe(0);
    expect(summary.rejected).toBe(0);
    expect(uow.store.events).toHaveLength(5);
  });

  it('resumes from the newest stored post rather than re-reading everything', async () => {
    // The cursor is derived from the observations table, so nothing needs to
    // persist it separately and a restart cannot lose it.
    const source = new StubObservationSource(ObservationProvider.X, [
      success([authored('p-1', 0), authored('p-2', 1)]),
      success([authored('p-3', 2)]),
    ]);
    const { handler, uow, summaries } = harness(source);

    await handler.handle(job());
    await handler.handle(job());

    expect(source.requests[0]?.sinceProviderPostId).toBeUndefined();
    expect(source.requests[1]?.sinceProviderPostId).toBe('p-2');
    expect(unwrap(summaries[1]).sinceProviderPostId).toBe('p-2');
    expect(uow.store.observations.size).toBe(3);
  });

  it('prefers a cursor the payload pins, so a backfill can choose its window', async () => {
    const source = new StubObservationSource(ObservationProvider.X, [
      success([authored('p-1', 0)]),
      success([]),
    ]);
    const { handler } = harness(source);

    await handler.handle(job());
    await handler.handle(job({ sinceProviderPostId: 'p-0' }));

    // p-1 is stored and would have been derived; the pinned cursor wins.
    expect(source.requests[1]?.sinceProviderPostId).toBe('p-0');
  });

  it('re-polling a window it already ingested changes nothing', async () => {
    // The queue may redeliver a job after a crash or lease expiry, so this is
    // the idempotency the JobHandler contract requires.
    const source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 4 });
    const { handler, uow, summaries } = harness(source);
    // Newest first, so the last entry is the oldest post in the corpus.
    const corpus = await corpusOf(source);
    const oldest = corpus[corpus.length - 1] as SourcePost;

    await handler.handle(job());
    await handler.handle(job({ sinceProviderPostId: oldest.providerPostId }));

    expect(uow.store.observations.size).toBe(4);
    expect(uow.store.events).toHaveLength(4);
    const second = unwrap(summaries[1]);
    expect(second.fetched).toBe(3);
    expect(second.duplicates).toBe(3);
    expect(second.ingested).toBe(0);
  });

  it('throws a retryable error on a provider outage', async () => {
    const source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 3 });
    source.simulateOutage(2);
    const { handler, uow, summaries } = harness(source);

    const first = await rejection(handler.handle(job()));
    const second = await rejection(handler.handle(job()));

    expect(first.message).toContain('outage');
    // Not permanent: dead-lettering a poll would stop watching the account, and
    // the provider being down is precisely the case backoff exists for.
    expect(first).not.toBeInstanceOf(PermanentJobFailure);
    expect(second).not.toBeInstanceOf(PermanentJobFailure);
    expect(uow.store.observations.size).toBe(0);
    expect(summaries).toHaveLength(0);
  });

  it('succeeds once the provider recovers', async () => {
    const source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 3 });
    source.simulateOutage(1);
    const { handler, uow } = harness(source);

    await rejection(handler.handle(job()));
    await handler.handle(job());

    expect(uow.store.observations.size).toBe(3);
    expect(source.fetchCount()).toBe(2);
  });

  it('dead-letters a payload version it cannot read', async () => {
    const { handler } = harness(new SyntheticObservationSource());
    const error = await rejection(handler.handle(job({ payloadVersion: '99' })));
    expect(error).toBeInstanceOf(PermanentJobFailure);
  });

  it('dead-letters a payload naming a provider the domain does not define', async () => {
    // The value crosses a JSON boundary in the job payload, so the static type
    // is no guarantee by the time it arrives here.
    const { handler } = harness(new SyntheticObservationSource());
    const error = await rejection(
      handler.handle(job({ provider: 'mastodon' as ObservationProvider }))
    );
    expect(error).toBeInstanceOf(PermanentJobFailure);
  });

  it('dead-letters a payload with no author', async () => {
    const { handler } = harness(new SyntheticObservationSource());
    const error = await rejection(handler.handle(job({ authorId: '   ' })));
    expect(error).toBeInstanceOf(PermanentJobFailure);
  });

  it('dead-letters a provider this process has no adapter for', async () => {
    // A composition error. Waiting will not produce an adapter, so retrying is
    // just a slower way of never running.
    const { handler } = harness(new SyntheticObservationSource(ObservationProvider.X));
    const error = await rejection(handler.handle(job({ provider: ObservationProvider.TELEGRAM })));
    expect(error).toBeInstanceOf(PermanentJobFailure);
  });

  it('counts a post the canonical model refused without failing the poll', async () => {
    // Retrying would re-ingest the good posts as duplicates and fail on the same
    // bad one until the job dead-letters, costing the account its polling.
    const source = new StubObservationSource(ObservationProvider.X, [
      success([
        authored('good', 0),
        post({
          providerPostId: 'broken',
          authorId: '  ',
          publishedAt: new Date('2026-08-19T10:00:01.000Z'),
        }),
      ]),
    ]);
    const { handler, uow, summaries } = harness(source);

    await handler.handle(job());

    const summary = unwrap(summaries[0]);
    expect(summary.fetched).toBe(2);
    expect(summary.ingested).toBe(1);
    expect(summary.rejected).toBe(1);
    expect(uow.store.observations.size).toBe(1);
  });

  it('throws when the failure is the database rather than the post', async () => {
    // Counting an event-store outage as a "rejected post" would report a clean
    // poll that happened to ingest nothing, and the account would silently stop
    // being observed.
    const source = new StubObservationSource(ObservationProvider.X, [
      success([authored('p-1', 0)]),
    ]);
    const { handler, uow, summaries } = harness(source, { appendFails: true });

    const error = await rejection(handler.handle(job()));

    expect(error.message).toContain('Ingestion failed');
    expect(error).not.toBeInstanceOf(PermanentJobFailure);
    expect(uow.store.observations.size).toBe(0);
    expect(summaries).toHaveLength(0);
  });

  it('reports a fetch failure as a Result rather than throwing out of poll()', async () => {
    // poll() is the value-returning surface; handle() is what converts a failure
    // into the throw a worker reads as "retry".
    const source = new StubObservationSource(ObservationProvider.X, [
      failure(new Error('rate limited')),
    ]);
    const { handler } = harness(source);

    const result = await handler.poll({ provider: ObservationProvider.X, authorId: AUTHOR });

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error.message).toBe('rate limited');
    }
  });

  it('trims the author id it polls and reports', async () => {
    const source = new StubObservationSource(ObservationProvider.X, [success<SourcePost[]>([])]);
    const { handler, summaries } = harness(source);

    await handler.handle(job({ authorId: `  ${AUTHOR}  ` }));

    expect(source.requests[0]?.authorId).toBe(AUTHOR);
    expect(unwrap(summaries[0]).authorId).toBe(AUTHOR);
  });

  it('passes the payload limit through to the provider', async () => {
    const source = new StubObservationSource(ObservationProvider.X, [success<SourcePost[]>([])]);
    const { handler } = harness(source);

    await handler.handle(job({ limit: 7 }));

    expect(source.requests[0]?.limit).toBe(7);
  });

  it('handles the observation_poll queue', () => {
    const { handler } = harness(new SyntheticObservationSource());
    expect(handler.queueName).toBe(QueueName.OBSERVATION_POLL);
  });
});
