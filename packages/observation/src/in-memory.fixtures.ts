import {
  MediaId,
  ObservationId,
  ObservationProvider,
  Result,
  Timestamp,
  failure,
  success,
} from '@nit/domain';
import { EventEnvelope, EventStore } from '@nit/events';
import {
  AuthorFetchRequest,
  ObservationSourceAdapter,
  SourceMedia,
  SourcePost,
} from './contracts';
import { NormalizedObservation } from './normalize';
import { InsertOutcome, ObservationRepository } from './repository';
import { Observation } from './types';
import { IngestionUnitOfWork, UnitOfWork } from './unit-of-work';

/**
 * In-memory doubles shared by this package's tests.
 *
 * Excluded from the build (see this package's tsconfig) so nothing ships them,
 * but compiled and linted like any other source file, which is what keeps them
 * honest about the interfaces they claim to implement.
 */

export const PUBLISHED: Timestamp = new Date('2026-08-19T10:00:00.000Z');
export const INGESTED: Timestamp = new Date('2026-08-19T10:00:04.000Z');

export interface PostOverrides {
  providerPostId?: string;
  authorId?: string;
  publishedAt?: Timestamp;
  text?: string;
  media?: readonly SourceMedia[];
  quotedProviderPostId?: string;
}

/**
 * Build a source post.
 *
 * Takes an explicit override interface rather than `Partial<SourcePost>` because
 * spreading a Partial widens every optional field to `| undefined`, which
 * `exactOptionalPropertyTypes` then refuses to assign back.
 */
export function post(overrides: PostOverrides = {}): SourcePost {
  const providerPostId = overrides.providerPostId ?? 'post-1';
  const built: SourcePost = {
    providerPostId,
    authorId: overrides.authorId ?? 'author-1',
    publishedAt: overrides.publishedAt ?? PUBLISHED,
    media: overrides.media ?? [],
    raw: { id: providerPostId, source: 'fixture' },
  };
  if (overrides.text !== undefined) {
    built.text = overrides.text;
  }
  if (overrides.quotedProviderPostId !== undefined) {
    built.quotedProviderPostId = overrides.quotedProviderPostId;
  }
  return built;
}

/**
 * Shared mutable state behind the doubles, so `FakeUnitOfWork` can snapshot and
 * restore it. Without that, a "rolled back" assertion would prove nothing.
 */
export class Store {
  observations = new Map<string, NormalizedObservation>();
  mediaIds = new Map<string, MediaId[]>();
  events: EventEnvelope[] = [];

  key(provider: ObservationProvider, providerPostId: string): string {
    return `${provider}/${providerPostId}`;
  }
}

export class InMemoryObservationRepository implements ObservationRepository {
  constructor(private readonly store: Store) {}

  insert(observation: NormalizedObservation): Promise<Result<InsertOutcome, Error>> {
    const key = this.store.key(observation.provider, observation.providerPostId);
    const existing = this.store.observations.get(key);
    if (existing !== undefined) {
      return Promise.resolve(
        success<InsertOutcome>({ inserted: false, observationId: existing.observationId })
      );
    }

    const mediaIds = observation.media.map((item) => item.mediaId);
    this.store.observations.set(key, observation);
    this.store.mediaIds.set(observation.observationId, mediaIds);
    return Promise.resolve(
      success<InsertOutcome>({
        inserted: true,
        observationId: observation.observationId,
        mediaIds,
      })
    );
  }

  findById(observationId: ObservationId): Promise<Result<Observation | null, Error>> {
    for (const stored of this.store.observations.values()) {
      if (stored.observationId === observationId) {
        return Promise.resolve(success(this.toObservation(stored)));
      }
    }
    return Promise.resolve(success(null));
  }

  findByProviderPostId(
    provider: ObservationProvider,
    providerPostId: string
  ): Promise<Result<Observation | null, Error>> {
    const stored = this.store.observations.get(this.store.key(provider, providerPostId));
    return Promise.resolve(success(stored === undefined ? null : this.toObservation(stored)));
  }

  findByAuthor(
    provider: ObservationProvider,
    authorId: string,
    limit: number
  ): Promise<Result<Observation[], Error>> {
    // Newest first, matching the Postgres repository's contract — the poll
    // handler derives its cursor from the first row, so the order is load-bearing.
    const matches = [...this.store.observations.values()]
      .filter((stored) => stored.provider === provider && stored.authorId === authorId)
      .sort((left, right) => {
        const byTime = right.publishedAt.getTime() - left.publishedAt.getTime();
        return byTime !== 0 ? byTime : right.providerPostId.localeCompare(left.providerPostId);
      })
      .slice(0, limit)
      .map((stored) => this.toObservation(stored));
    return Promise.resolve(success(matches));
  }

  private toObservation(stored: NormalizedObservation): Observation {
    const observation: Observation = {
      observationId: stored.observationId,
      provider: stored.provider,
      providerPostId: stored.providerPostId,
      authorId: stored.authorId,
      publishedAt: stored.publishedAt,
      ingestedAt: stored.ingestedAt,
      mediaIds: this.store.mediaIds.get(stored.observationId) ?? [],
    };
    if (stored.text !== undefined) {
      observation.text = stored.text;
    }
    // Resolved the way the real repository resolves it: by looking the quoted
    // provider id back up, which works in either arrival order.
    if (stored.quotedProviderPostId !== undefined) {
      const quoted = this.store.observations.get(
        this.store.key(stored.provider, stored.quotedProviderPostId)
      );
      if (quoted !== undefined) {
        observation.quotedObservationId = quoted.observationId;
      }
    }
    return observation;
  }
}

export class InMemoryEventStore implements EventStore {
  constructor(
    private readonly store: Store,
    private readonly appendFails: boolean
  ) {}

  append<TPayload>(event: EventEnvelope<TPayload>): Promise<Result<void, Error>> {
    if (this.appendFails) {
      return Promise.resolve(failure(new Error('event store unavailable')));
    }
    this.store.events.push(event as EventEnvelope);
    return Promise.resolve(success(undefined));
  }

  // The ingestion path never reads events back. Throwing rather than returning an
  // empty result means a future caller finds out immediately instead of quietly
  // reading nothing.
  getByAggregateId(): Promise<Result<EventEnvelope[], Error>> {
    throw new Error('not used by the ingestion service');
  }

  getByEventType(): Promise<Result<EventEnvelope[], Error>> {
    throw new Error('not used by the ingestion service');
  }

  getById(): Promise<Result<EventEnvelope | null, Error>> {
    throw new Error('not used by the ingestion service');
  }

  getAfter(): Promise<Result<EventEnvelope[], Error>> {
    throw new Error('not used by the ingestion service');
  }
}

/**
 * Models the transaction, not just its interface: a failure Result restores the
 * pre-call snapshot. That is what lets a test assert an observation was *not*
 * persisted when its event could not be appended.
 */
export class FakeUnitOfWork implements UnitOfWork {
  readonly store = new Store();
  transactions = 0;

  constructor(private readonly appendFails = false) {}

  async transact<T>(
    work: (uow: IngestionUnitOfWork) => Promise<Result<T, Error>>
  ): Promise<Result<T, Error>> {
    this.transactions += 1;
    const observations = new Map(this.store.observations);
    const mediaIds = new Map(this.store.mediaIds);
    const events = [...this.store.events];

    const result = await work({
      observations: new InMemoryObservationRepository(this.store),
      events: new InMemoryEventStore(this.store, this.appendFails),
    });

    if (result.kind === 'failure') {
      this.store.observations = observations;
      this.store.mediaIds = mediaIds;
      this.store.events = events;
    }
    return result;
  }
}

/**
 * An adapter that answers from a script.
 *
 * The synthetic source covers the happy path; this covers what a real provider
 * does that a well-behaved fake never will — fail, then recover, or hand back a
 * post the canonical model refuses. `requests` records what it was asked for,
 * which is how a test checks the cursor the poll handler derived.
 */
export class StubObservationSource implements ObservationSourceAdapter {
  readonly providerName = 'stub-observation-source';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1.0.0';
  readonly requests: AuthorFetchRequest[] = [];

  constructor(
    readonly provider: ObservationProvider,
    private readonly answers: Array<Result<SourcePost[], Error>>
  ) {}

  fetchAuthorPosts(request: AuthorFetchRequest): Promise<Result<SourcePost[], Error>> {
    this.requests.push(request);
    // Past the end of the script an account simply has nothing new, which is what
    // a quiet account looks like.
    return Promise.resolve(this.answers.shift() ?? success<SourcePost[]>([]));
  }
}
