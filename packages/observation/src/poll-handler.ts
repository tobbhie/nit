import { ObservationProvider, QueueName, Result, ValidationError, failure, success } from '@nit/domain';
import { Job, JobHandler, PermanentJobFailure } from '@nit/queue';
import { AuthorFetchRequest, ObservationSourceAdapter } from './contracts';
import { ObservationRepository } from './repository';
import { ObservationIngestionService } from './service';

export const OBSERVATION_POLL_PAYLOAD_VERSION = '1';

const DEFAULT_POLL_LIMIT = 50;

const PROVIDERS = new Set<string>(Object.values(ObservationProvider));

/**
 * One account to poll.
 *
 * `sinceProviderPostId` is optional because the handler can work the cursor out
 * for itself; supplying one is how a backfill asks for a specific window.
 */
export interface ObservationPollPayload {
  provider: ObservationProvider;
  authorId: string;
  sinceProviderPostId?: string;
  limit?: number;
}

/** What one poll did. Enough to log or alert on without re-querying. */
export interface PollSummary {
  provider: ObservationProvider;
  authorId: string;
  /** The cursor actually used, whether supplied or derived. */
  sinceProviderPostId: string | null;
  fetched: number;
  ingested: number;
  duplicates: number;
  /** Posts the canonical model refused. See `handle` on why these do not retry. */
  rejected: number;
}

export interface ObservationPollOptions {
  limit?: number;
  /** Called after a successful poll so the caller can log the summary. */
  onPolled?: (summary: PollSummary) => void;
}

/**
 * Polls one monitored account per job.
 *
 * Per-account rather than per-provider because the durable queue's retry and
 * backoff are per job: one account whose fetch keeps failing must not delay every
 * other account behind it, and TIS ch.4 §9 requires ingestion to continue despite
 * provider failures.
 *
 * The handler is idempotent, as the queue requires: a redelivered poll re-fetches
 * the same posts and ingestion absorbs them as duplicates on
 * `(provider, provider_post_id)`.
 *
 * It does not schedule itself. A handler that enqueued its own successor would
 * lose the chain the first time a process died between committing the ingest and
 * committing the next job, and polling would then stop for that account
 * permanently with nothing to report it. Scheduling is the scheduler's job
 * (ch.4 §5 draws it as a separate stage upstream).
 */
export class ObservationPollHandler implements JobHandler<ObservationPollPayload> {
  readonly queueName: string = QueueName.OBSERVATION_POLL;

  constructor(
    private readonly sources: ReadonlyMap<ObservationProvider, ObservationSourceAdapter>,
    private readonly observations: ObservationRepository,
    private readonly ingestion: ObservationIngestionService,
    private readonly options: ObservationPollOptions = {}
  ) {}

  /**
   * Run a poll for the queue.
   *
   * Throwing is how a worker learns to retry, so what does and does not throw
   * here is the whole of this handler's failure policy:
   *
   * - A malformed payload or an unbound provider throws `PermanentJobFailure`.
   *   No number of retries fixes either, and the dead-letter queue is where an
   *   operator will actually see it.
   * - A failed fetch throws an ordinary error, so the queue retries it with
   *   backoff. This is the provider-outage path.
   * - Posts the canonical model rejected do not throw. The rest of the batch is
   *   already committed, so retrying would re-ingest good posts as duplicates and
   *   fail on the same bad post until the job dead-letters, costing the account
   *   its poll over one malformed post. They are counted in the summary instead.
   */
  async handle(job: Job<ObservationPollPayload>): Promise<void> {
    if (job.payloadVersion !== OBSERVATION_POLL_PAYLOAD_VERSION) {
      throw new PermanentJobFailure(
        `Unsupported observation poll payload version '${job.payloadVersion}'`
      );
    }

    const result = await this.poll(job.payload);
    if (result.kind === 'failure') {
      throw result.error;
    }
    this.options.onPolled?.(result.value);
  }

  /**
   * Fetch and ingest one account's new posts.
   *
   * Separate from `handle` so the poll can be exercised without a job envelope,
   * and so the summary is a return value rather than something only a log sees.
   */
  async poll(payload: ObservationPollPayload): Promise<Result<PollSummary, Error>> {
    const provider = payload.provider;
    if (!PROVIDERS.has(provider)) {
      throw new PermanentJobFailure(`Unknown observation provider '${provider}'`);
    }
    if (typeof payload.authorId !== 'string' || payload.authorId.trim().length === 0) {
      throw new PermanentJobFailure('Observation poll payload has no authorId');
    }

    const source = this.sources.get(provider);
    if (source === undefined) {
      // A composition error, not a runtime one: this process has no adapter for
      // the provider and will not grow one by waiting.
      throw new PermanentJobFailure(`No observation source bound for provider '${provider}'`);
    }

    const authorId = payload.authorId.trim();
    const cursor = await this.resolveCursor(provider, authorId, payload.sinceProviderPostId);
    if (cursor.kind === 'failure') {
      return failure(cursor.error);
    }

    const request: AuthorFetchRequest = {
      authorId,
      limit: payload.limit ?? this.options.limit ?? DEFAULT_POLL_LIMIT,
    };
    if (cursor.value !== null) {
      request.sinceProviderPostId = cursor.value;
    }

    const fetched = await source.fetchAuthorPosts(request);
    if (fetched.kind === 'failure') {
      return failure(fetched.error);
    }

    const batch = await this.ingestion.ingestBatch(provider, fetched.value);

    // A failure that is not a ValidationError did not come from the post: it came
    // from the database or the event store. Reporting those as "rejected posts"
    // would turn an outage into a clean poll that quietly ingested nothing, so
    // they are surfaced as a retryable failure instead.
    const infrastructure = batch.failures.filter(
      (entry) => !(entry.error instanceof ValidationError)
    );
    if (infrastructure.length > 0) {
      const first = infrastructure[0];
      return failure(
        new Error(
          `Ingestion failed for ${infrastructure.length} of ${fetched.value.length} posts from ${provider}/${authorId}: ${first?.error.message ?? 'unknown error'}`
        )
      );
    }

    return success({
      provider,
      authorId,
      sinceProviderPostId: cursor.value,
      fetched: fetched.value.length,
      ingested: batch.ingested.length,
      duplicates: batch.duplicates.length,
      rejected: batch.failures.length,
    });
  }

  /**
   * Where to resume from.
   *
   * Derived from the newest stored observation rather than from a cursor column
   * the poller maintains. The observations table already records exactly what has
   * been ingested, so a separate cursor could only ever disagree with it — and it
   * would disagree in the dangerous direction, skipping posts, after any restart
   * that lost an uncommitted cursor write.
   */
  private async resolveCursor(
    provider: ObservationProvider,
    authorId: string,
    supplied: string | undefined
  ): Promise<Result<string | null, Error>> {
    if (supplied !== undefined && supplied.trim().length > 0) {
      return success(supplied.trim());
    }

    const newest = await this.observations.findByAuthor(provider, authorId, 1);
    if (newest.kind === 'failure') {
      return failure(newest.error);
    }
    return success(newest.value[0]?.providerPostId ?? null);
  }
}
