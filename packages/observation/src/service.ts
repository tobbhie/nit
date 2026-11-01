import {
  ObservationId,
  ObservationProvider,
  Result,
  Timestamp,
  failure,
  now,
  success,
} from '@nit/domain';
import { SourcePost } from './contracts';
import { observationCreatedEvent } from './events';
import { normalizeSourcePost } from './normalize';
import { UnitOfWork } from './unit-of-work';

export interface IngestionOutcome {
  /** `duplicate` means the provider post was already stored (TIS ch.4 §7). */
  status: 'ingested' | 'duplicate';
  observationId: ObservationId;
}

/**
 * What one poll's worth of posts did. Failures carry the provider id rather than
 * an observation id, because a post that could not be normalized never got one.
 */
export interface BatchOutcome {
  ingested: ObservationId[];
  duplicates: ObservationId[];
  failures: Array<{ providerPostId: string; error: Error }>;
}

export interface ObservationIngestionOptions {
  /** Recorded on every event as its origin. */
  producer: string;
  /** Injected for deterministic tests; defaults to wall clock. */
  clock?: () => Timestamp;
}

/**
 * Normalizes provider posts, persists them, and announces them.
 *
 * The whole of TIS ch.4 §6 minus the fetching, which belongs to an adapter, and
 * minus the media download, which belongs to the multimodal worker that the
 * event wakes. It performs no narrative analysis — §6 forbids it, and the reason
 * is not tidiness: analysis is the part that gets rewritten, and an observation
 * re-derived by today's analyser is no longer the thing that was observed.
 */
export class ObservationIngestionService {
  private readonly clock: () => Timestamp;

  constructor(
    private readonly unitOfWork: UnitOfWork,
    private readonly options: ObservationIngestionOptions
  ) {
    this.clock = options.clock ?? now;
  }

  /**
   * Ingest one post in a single transaction.
   *
   * One post is one aggregate, which is why a batch is not one transaction
   * (TIS ch.2 §7). It also means a malformed post in a batch cannot roll back
   * the good ones.
   */
  async ingest(
    provider: ObservationProvider,
    post: SourcePost
  ): Promise<Result<IngestionOutcome, Error>> {
    const normalized = normalizeSourcePost(provider, post, this.clock());
    if (normalized.kind === 'failure') {
      return failure(normalized.error);
    }
    const observation = normalized.value;

    return this.unitOfWork.transact(async ({ observations, events }) => {
      const inserted = await observations.insert(observation);
      if (inserted.kind === 'failure') {
        return failure(inserted.error);
      }

      // Already stored. Return without republishing: the event was appended in
      // the transaction that first inserted the row, so a second one would be a
      // duplicate announcement of a single fact. Committing an empty
      // transaction is correct — nothing was written and nothing is being undone.
      if (!inserted.value.inserted) {
        return success<IngestionOutcome>({
          status: 'duplicate',
          observationId: inserted.value.observationId,
        });
      }

      const appended = await events.append(
        observationCreatedEvent({
          observationId: inserted.value.observationId,
          provider: observation.provider,
          providerPostId: observation.providerPostId,
          authorId: observation.authorId,
          publishedAt: observation.publishedAt,
          ingestedAt: observation.ingestedAt,
          mediaIds: inserted.value.mediaIds,
          hasText: observation.text !== undefined,
          ...(observation.quotedProviderPostId !== undefined
            ? { quotedProviderPostId: observation.quotedProviderPostId }
            : {}),
          producer: this.options.producer,
        })
      );
      if (appended.kind === 'failure') {
        return failure(appended.error);
      }

      return success<IngestionOutcome>({
        status: 'ingested',
        observationId: inserted.value.observationId,
      });
    });
  }

  /**
   * Ingest a poll's worth of posts, oldest first.
   *
   * Sorting matters for more than tidiness: the event log is what replay reads,
   * and emitting a reply before the post it replies to would make the log
   * describe a sequence that never happened. Publication time is the provider's
   * claim about ordering and the only one available, with the provider id as a
   * tie-break so two posts in the same millisecond still order deterministically.
   *
   * Every post is attempted even after one fails, per TIS ch.4 §9's requirement
   * to continue despite provider failures — a single malformed post must not
   * cost the rest of the batch.
   */
  async ingestBatch(
    provider: ObservationProvider,
    posts: readonly SourcePost[]
  ): Promise<BatchOutcome> {
    const ordered = [...posts].sort((left, right) => {
      const byTime = left.publishedAt.getTime() - right.publishedAt.getTime();
      return byTime !== 0 ? byTime : left.providerPostId.localeCompare(right.providerPostId);
    });

    const batch: BatchOutcome = { ingested: [], duplicates: [], failures: [] };

    for (const post of ordered) {
      const result = await this.ingest(provider, post);
      if (result.kind === 'failure') {
        batch.failures.push({ providerPostId: post.providerPostId, error: result.error });
      } else if (result.value.status === 'ingested') {
        batch.ingested.push(result.value.observationId);
      } else {
        batch.duplicates.push(result.value.observationId);
      }
    }

    return batch;
  }
}
