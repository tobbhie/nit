import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PostgresEventStore } from '@nit/database-public';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import {
  EventType,
  JobStatus,
  MediaType,
  ObservationProvider,
  QueueName,
  Timestamp,
  failure,
} from '@nit/domain';
import { BackoffPolicy, JobQueue, Worker } from '@nit/queue';
import { SourcePost } from './contracts';
import { ObservationCreatedPayload, observationCreatedEvent } from './events';
import { post } from './in-memory.fixtures';
import { normalizeSourcePost } from './normalize';
import {
  OBSERVATION_POLL_PAYLOAD_VERSION,
  ObservationPollHandler,
  ObservationPollPayload,
} from './poll-handler';
import { PostgresObservationRepository } from './repository';
import { ObservationIngestionService } from './service';
import { SyntheticObservationSource } from './synthetic-source';
import { Observation } from './types';
import { PostgresUnitOfWork } from './unit-of-work';

/**
 * TIS ch.4 §10's acceptance checklist, run against a real PostgreSQL.
 *
 * Every item on that list is a claim about the database: a unique constraint
 * holding under concurrency, a join resolving a quote whichever post arrived
 * first, media rows committing with their observation, an event landing in the
 * same transaction as the row it announces. An in-memory double can be written
 * to satisfy all of them and prove none, which is why these suites skip when no
 * test database is configured rather than substituting one.
 */

const PRODUCER = 'observation-pg-test';
const AUTHOR = 'monitored-account-1';
const INGESTED: Timestamp = new Date('2026-08-19T10:00:04.000Z');
const BACKOFF: BackoffPolicy = { baseMs: 1_000, maxMs: 60_000, factor: 2 };

interface MediaRow {
  media_id: string;
  provider_media_key: string;
  media_type: string;
  source_url: string;
  duration_ms: number | null;
  storage_location: string | null;
  sha256: string | null;
  phash: string | null;
}

describe.skipIf(!hasTestDatabase())('Observation ingestion against Postgres', () => {
  let harness: TestDatabase;
  let service: ObservationIngestionService;
  let repository: PostgresObservationRepository;
  let events: PostgresEventStore;

  /** Frozen so ingestion times are exact rather than racy. */
  const clock = (): Timestamp => INGESTED;

  beforeAll(async () => {
    harness = await createTestDatabase('observation');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  beforeEach(async () => {
    // Include dependent interpretation ledgers and media so
    // TRUNCATE does not need CASCADE.
    const truncated = await harness.db.query(
      `TRUNCATE nit_text_results, nit_text_attempts, media, observations, domain_events,
                durable_jobs, dead_letter_jobs, job_history RESTART IDENTITY`
    );
    expect(truncated.kind).toBe('success');

    service = new ObservationIngestionService(new PostgresUnitOfWork(harness.db), {
      producer: PRODUCER,
      clock,
    });
    repository = new PostgresObservationRepository(harness.db);
    events = new PostgresEventStore(harness.db);
  });

  const ingest = async (source: SourcePost): Promise<string> => {
    const result = await service.ingest(ObservationProvider.X, source);
    if (result.kind === 'failure') {
      throw result.error;
    }
    return result.value.observationId;
  };

  const read = async (providerPostId: string): Promise<Observation> => {
    const found = await repository.findByProviderPostId(ObservationProvider.X, providerPostId);
    if (found.kind === 'failure') {
      throw found.error;
    }
    if (found.value === null) {
      throw new Error(`${providerPostId} was not stored`);
    }
    return found.value;
  };

  const countOf = async (table: string): Promise<number> => {
    const result = await harness.db.query<{ count: string }>(`SELECT COUNT(*) FROM ${table}`);
    if (result.kind === 'failure') {
      throw result.error;
    }
    return Number(result.value[0]?.count ?? '0');
  };

  describe('duplicate ingestion is prevented', () => {
    it('stores one observation and one event when the same post is re-seen', async () => {
      const first = await service.ingest(ObservationProvider.X, post({ text: 'gm' }));
      const second = await service.ingest(ObservationProvider.X, post({ text: 'gm' }));
      if (first.kind === 'failure' || second.kind === 'failure') {
        throw new Error('Expected both ingests to succeed');
      }

      expect(first.value.status).toBe('ingested');
      expect(second.value.status).toBe('duplicate');
      expect(second.value.observationId).toBe(first.value.observationId);
      expect(await countOf('observations')).toBe(1);
      expect(await countOf('domain_events')).toBe(1);
    });

    it('stores one observation when two pollers race on the same post', async () => {
      // The point of putting the idempotency key in a UNIQUE constraint rather
      // than in a read-then-write check: two workers can both pass the check.
      const other = new ObservationIngestionService(new PostgresUnitOfWork(harness.db), {
        producer: PRODUCER,
        clock,
      });

      const [left, right] = await Promise.all([
        service.ingest(ObservationProvider.X, post({ providerPostId: 'contended' })),
        other.ingest(ObservationProvider.X, post({ providerPostId: 'contended' })),
      ]);
      if (left.kind === 'failure' || right.kind === 'failure') {
        throw new Error('Neither racing ingest should fail');
      }

      const statuses = [left.value.status, right.value.status].sort();
      expect(statuses).toEqual(['duplicate', 'ingested']);
      expect(left.value.observationId).toBe(right.value.observationId);
      expect(await countOf('observations')).toBe(1);
      // One row, therefore one announcement.
      expect(await countOf('domain_events')).toBe(1);
    });

    it('does not duplicate media when a post with attachments is re-seen', async () => {
      const withMedia = post({
        providerPostId: 'has-media',
        media: [
          { providerMediaKey: 'm1', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/1' },
        ],
      });

      await ingest(withMedia);
      await ingest(withMedia);

      expect(await countOf('media')).toBe(1);
    });
  });

  describe('quote posts are linked correctly', () => {
    it('links a quote to a post already stored', async () => {
      const quotedId = await ingest(post({ providerPostId: 'original', text: 'the claim' }));
      await ingest(post({ providerPostId: 'quote', quotedProviderPostId: 'original' }));

      expect((await read('quote')).quotedObservationId).toBe(quotedId);
    });

    it('links a quote to a post that arrives afterwards', async () => {
      // The link is a join on the stored provider id, not a foreign key written
      // at insert time, so a quote may be ingested before the post it quotes —
      // which is what happens whenever a poll window opens mid-conversation.
      await ingest(post({ providerPostId: 'quote', quotedProviderPostId: 'original' }));
      expect((await read('quote')).quotedObservationId).toBeUndefined();

      const quotedId = await ingest(post({ providerPostId: 'original', text: 'the claim' }));
      expect((await read('quote')).quotedObservationId).toBe(quotedId);
    });

    it('carries the quoted provider id on the event, since the link may not resolve yet', async () => {
      await ingest(post({ providerPostId: 'quote', quotedProviderPostId: 'original' }));

      const appended = await events.getByEventType(EventType.OBSERVATION_CREATED);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      const payload = appended.value[0]?.payload as ObservationCreatedPayload;
      expect(payload.quotedProviderPostId).toBe('original');
    });

    it('leaves a post that quotes nothing unlinked', async () => {
      await ingest(post({ providerPostId: 'plain', text: 'gm' }));
      expect((await read('plain')).quotedObservationId).toBeUndefined();
    });
  });

  describe('media references are persisted', () => {
    it('writes a row per attachment and reports them on the observation', async () => {
      const observationId = await ingest(
        post({
          providerPostId: 'gallery',
          text: 'two files',
          media: [
            {
              providerMediaKey: 'k-image',
              mediaType: MediaType.IMAGE,
              sourceUrl: 'https://x.invalid/image.png',
            },
            {
              providerMediaKey: 'k-video',
              mediaType: MediaType.VIDEO,
              sourceUrl: 'https://x.invalid/video.mp4',
              durationMs: 4_200,
            },
          ],
        })
      );

      const rows = await harness.db.query<MediaRow>(
        'SELECT * FROM media WHERE observation_id = $1 ORDER BY provider_media_key',
        [observationId]
      );
      if (rows.kind === 'failure') {
        throw rows.error;
      }

      expect(rows.value).toHaveLength(2);
      const [image, video] = rows.value as [MediaRow, MediaRow];
      expect(image.provider_media_key).toBe('k-image');
      expect(image.media_type).toBe(MediaType.IMAGE);
      expect(image.source_url).toBe('https://x.invalid/image.png');
      expect(image.duration_ms).toBeNull();
      expect(video.media_type).toBe(MediaType.VIDEO);
      expect(video.duration_ms).toBe(4_200);

      // The canonical observation reports the same identifiers the rows carry.
      const stored = await read('gallery');
      expect([...stored.mediaIds].sort()).toEqual([image.media_id, video.media_id].sort());
    });

    it('leaves the download columns null, because ingestion has not fetched bytes', async () => {
      // TIS ch.5 §7's durability columns are the multimodal worker's to fill.
      // Ingestion inventing a value here would make an unprocessed attachment
      // look processed.
      await ingest(
        post({
          providerPostId: 'unfetched',
          media: [
            { providerMediaKey: 'k', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/1' },
          ],
        })
      );

      const rows = await harness.db.query<MediaRow>('SELECT * FROM media');
      if (rows.kind === 'failure') {
        throw rows.error;
      }
      const media = rows.value[0] as MediaRow;
      expect(media.storage_location).toBeNull();
      expect(media.sha256).toBeNull();
      expect(media.phash).toBeNull();
    });

    it('announces the media identifiers so a consumer needs no query', async () => {
      // TIS ch.4 §11: the event is what wakes multimodal processing.
      const observationId = await ingest(
        post({
          providerPostId: 'announced',
          media: [
            { providerMediaKey: 'k', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/1' },
          ],
        })
      );

      const appended = await events.getByAggregateId(observationId);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      const payload = appended.value[0]?.payload as ObservationCreatedPayload;
      expect(payload.mediaIds).toEqual((await read('announced')).mediaIds);
    });
  });

  describe('ObservationCreated is emitted', () => {
    it('appends exactly one event describing the stored observation', async () => {
      const observationId = await ingest(
        post({
          providerPostId: 'announced',
          text: 'gm',
          publishedAt: new Date('2026-08-19T09:59:00.000Z'),
        })
      );

      const appended = await events.getByAggregateId(observationId);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      expect(appended.value).toHaveLength(1);

      const envelope = appended.value[0];
      if (envelope === undefined) {
        throw new Error('Expected an event');
      }
      expect(envelope.eventType).toBe(EventType.OBSERVATION_CREATED);
      expect(envelope.aggregateId).toBe(observationId);
      expect(envelope.producer).toBe(PRODUCER);
      // Occurrence is when this system observed the post, not when it was written.
      expect(envelope.occurredAt).toEqual(INGESTED);

      const payload = envelope.payload as ObservationCreatedPayload;
      expect(payload.observationId).toBe(observationId);
      expect(payload.providerPostId).toBe('announced');
      expect(payload.authorId).toBe('author-1');
      expect(payload.publishedAt).toBe('2026-08-19T09:59:00.000Z');
      expect(payload.hasText).toBe(true);
      expect(payload.mediaIds).toEqual([]);
      // The body stays in the immutable row a consumer reads by id.
      expect(JSON.stringify(payload)).not.toContain('gm');
    });

    it('commits the row, its media and the event together', async () => {
      // The transactional outbox. Written as a rollback because the failure mode
      // is silent: a committed observation whose event was lost is never
      // processed and nothing reports it missing.
      const normalized = normalizeSourcePost(
        ObservationProvider.X,
        post({
          providerPostId: 'rolled-back',
          text: 'gm',
          media: [
            { providerMediaKey: 'k', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/1' },
          ],
        }),
        INGESTED
      );
      if (normalized.kind === 'failure') {
        throw normalized.error;
      }
      const observation = normalized.value;

      const rolled = await new PostgresUnitOfWork(harness.db).transact(async (uow) => {
        const inserted = await uow.observations.insert(observation);
        if (inserted.kind === 'failure') {
          return failure(inserted.error);
        }
        const appended = await uow.events.append(
          observationCreatedEvent({
            observationId: observation.observationId,
            provider: observation.provider,
            providerPostId: observation.providerPostId,
            authorId: observation.authorId,
            publishedAt: observation.publishedAt,
            ingestedAt: observation.ingestedAt,
            mediaIds: inserted.value.inserted ? inserted.value.mediaIds : [],
            hasText: true,
            producer: PRODUCER,
          })
        );
        if (appended.kind === 'failure') {
          return failure(appended.error);
        }
        // Everything above succeeded; the transaction still must not commit.
        return failure(new Error('simulated failure after the writes'));
      });

      expect(rolled.kind).toBe('failure');
      expect(await countOf('observations')).toBe(0);
      expect(await countOf('media')).toBe(0);
      expect(await countOf('domain_events')).toBe(0);
    });

    it('does not announce a duplicate a second time', async () => {
      await ingest(post({ providerPostId: 'twice', text: 'gm' }));
      await service.ingest(ObservationProvider.X, post({ providerPostId: 'twice', text: 'gm' }));

      const appended = await events.getByEventType(EventType.OBSERVATION_CREATED);
      if (appended.kind === 'failure') {
        throw appended.error;
      }
      expect(appended.value).toHaveLength(1);
    });
  });

  describe('provider outages retry correctly', () => {
    let now: Timestamp;
    let queue: JobQueue;
    let source: SyntheticObservationSource;
    let worker: Worker<ObservationPollPayload>;

    beforeEach(() => {
      now = new Date('2026-08-19T10:00:00.000Z');
      queue = new JobQueue(harness.db, {
        leaseTimeoutMs: 30_000,
        defaultMaxAttempts: 3,
        backoff: BACKOFF,
        clock: (): Timestamp => now,
      });
      source = new SyntheticObservationSource(ObservationProvider.X, { postsPerAuthor: 5 });
      const handler = new ObservationPollHandler(
        new Map([[ObservationProvider.X, source]]),
        repository,
        service
      );
      worker = new Worker<ObservationPollPayload>(queue, handler, {
        workerId: 'poll-worker',
        pollIntervalMs: 10,
      });
    });

    const enqueuePoll = async (
      payloadVersion = OBSERVATION_POLL_PAYLOAD_VERSION
    ): Promise<void> => {
      const enqueued = await queue.enqueue<ObservationPollPayload>({
        queueName: QueueName.OBSERVATION_POLL,
        payload: { provider: ObservationProvider.X, authorId: AUTHOR },
        payloadVersion,
        idempotencyKey: `poll:${AUTHOR}`,
      });
      if (enqueued.kind === 'failure') {
        throw enqueued.error;
      }
    };

    const claimed = async (): Promise<boolean> => {
      const result = await worker.processOne();
      if (result.kind === 'failure') {
        throw result.error;
      }
      return result.value;
    };

    it('backs the job off and ingests everything on the retry', async () => {
      source.simulateOutage(1);
      await enqueuePoll();

      expect(await claimed()).toBe(true);
      expect(worker.getStats().failed).toBe(1);
      expect(worker.getStats().deadLettered).toBe(0);
      expect(await countOf('observations')).toBe(0);

      const pending = await queue.pendingDepth(QueueName.OBSERVATION_POLL);
      if (pending.kind === 'failure') {
        throw pending.error;
      }
      // Back in the pending pool rather than in a failed state, per ch.3 §5.
      expect(pending.value).toBe(1);

      // Still inside the backoff window, so there is nothing to claim yet.
      expect(await claimed()).toBe(false);

      now = new Date(now.getTime() + 60_000);
      expect(await claimed()).toBe(true);

      expect(worker.getStats().processed).toBe(1);
      expect(await countOf('observations')).toBe(5);
      expect(await countOf('domain_events')).toBe(5);
      expect(source.fetchCount()).toBe(2);
    });

    it('marks the job completed with the failed attempt recorded', async () => {
      source.simulateOutage(1);
      await enqueuePoll();
      await claimed();
      now = new Date(now.getTime() + 60_000);
      await claimed();

      const jobs = await harness.db.query<{ status: string; attempt_count: number }>(
        'SELECT status, attempt_count FROM durable_jobs'
      );
      if (jobs.kind === 'failure') {
        throw jobs.error;
      }
      const job = jobs.value[0];
      expect(job?.status).toBe(JobStatus.COMPLETED);
      // The outage cost an attempt, which is what makes the retry budget finite.
      expect(job?.attempt_count).toBe(2);
    });

    it('dead-letters a payload no retry can fix instead of burning the budget', async () => {
      await enqueuePoll('99');

      expect(await claimed()).toBe(true);

      expect(worker.getStats().deadLettered).toBe(1);
      const dead = await queue.getDeadLetters(QueueName.OBSERVATION_POLL);
      if (dead.kind === 'failure') {
        throw dead.error;
      }
      expect(dead.value).toHaveLength(1);
      expect(dead.value[0]?.lastError).toContain('payload version');
      // On the first attempt, not after exhausting three.
      expect(dead.value[0]?.attemptCount).toBe(1);
    });

    it('resumes from the newest stored post on the next poll', async () => {
      await enqueuePoll();
      expect(await claimed()).toBe(true);
      expect(await countOf('observations')).toBe(5);

      // A second poll of an account with nothing new: the cursor comes from
      // findByAuthor against real SQL, so this is what proves that ORDER BY
      // returns the row the handler assumes it does.
      const second = await queue.enqueue<ObservationPollPayload>({
        queueName: QueueName.OBSERVATION_POLL,
        payload: { provider: ObservationProvider.X, authorId: AUTHOR },
        payloadVersion: OBSERVATION_POLL_PAYLOAD_VERSION,
        idempotencyKey: `poll:${AUTHOR}:2`,
      });
      if (second.kind === 'failure') {
        throw second.error;
      }

      expect(await claimed()).toBe(true);
      expect(await countOf('observations')).toBe(5);
      expect(await countOf('domain_events')).toBe(5);
    });
  });

  describe('stored observations are immutable', () => {
    it('rejects an update to a stored observation', async () => {
      await ingest(post({ providerPostId: 'immutable', text: 'as observed' }));

      const updated = await harness.db.query(
        "UPDATE observations SET text = 'rewritten' WHERE provider_post_id = $1",
        ['immutable']
      );

      expect(updated.kind).toBe('failure');
      if (updated.kind === 'failure') {
        expect(updated.error.message).toContain('append-only');
      }
      expect((await read('immutable')).text).toBe('as observed');
    });

    it('rejects a delete', async () => {
      await ingest(post({ providerPostId: 'permanent', text: 'gm' }));

      const deleted = await harness.db.query(
        'DELETE FROM observations WHERE provider_post_id = $1',
        ['permanent']
      );

      expect(deleted.kind).toBe('failure');
      expect(await countOf('observations')).toBe(1);
    });

    it('keeps the provider payload verbatim', async () => {
      // ch.13 reconstruction reads this, so it has to be what the provider sent
      // and not a normalized projection of it.
      await ingest(post({ providerPostId: 'raw-kept', text: 'gm' }));

      const rows = await harness.db.query<{ raw_payload: Record<string, unknown> }>(
        'SELECT raw_payload FROM observations WHERE provider_post_id = $1',
        ['raw-kept']
      );
      if (rows.kind === 'failure') {
        throw rows.error;
      }
      expect(rows.value[0]?.raw_payload).toEqual({ id: 'raw-kept', source: 'fixture' });
    });
  });

  describe('author queries', () => {
    it('returns an author newest first, with a deterministic tie-break', async () => {
      // The poll handler takes its cursor from the first row, so this ordering is
      // load-bearing rather than cosmetic — and the in-memory double mirrors it.
      const at = (seconds: number): Timestamp => new Date(`2026-08-19T10:00:0${seconds}.000Z`);
      for (const [providerPostId, seconds] of [
        ['a-oldest', 0],
        ['b-tied', 1],
        ['a-tied', 1],
        ['c-newest', 2],
      ] as Array<[string, number]>) {
        await ingest(post({ providerPostId, authorId: AUTHOR, publishedAt: at(seconds) }));
      }

      const found = await repository.findByAuthor(ObservationProvider.X, AUTHOR, 10);
      if (found.kind === 'failure') {
        throw found.error;
      }
      expect(found.value.map((observation) => observation.providerPostId)).toEqual([
        'c-newest',
        'b-tied',
        'a-tied',
        'a-oldest',
      ]);
    });

    it('does not mix authors or providers together', async () => {
      await ingest(post({ providerPostId: 'mine', authorId: AUTHOR }));
      await ingest(post({ providerPostId: 'theirs', authorId: 'someone-else' }));

      const elsewhere = await service.ingest(
        ObservationProvider.TELEGRAM,
        post({ providerPostId: 'mine', authorId: AUTHOR })
      );
      expect(elsewhere.kind).toBe('success');

      const found = await repository.findByAuthor(ObservationProvider.X, AUTHOR, 10);
      if (found.kind === 'failure') {
        throw found.error;
      }
      expect(found.value.map((observation) => observation.providerPostId)).toEqual(['mine']);
      // Same provider post id under a different provider is a different post.
      expect(await countOf('observations')).toBe(3);
    });
  });

  it('persists a post well inside the ch.4 §9 budget', async () => {
    // §9 allows 500ms per observation excluding media. Asserted on the median of
    // a run rather than a single call, so an unlucky first connection or a
    // checkpoint does not fail the suite on a number it is not measuring.
    const durations: number[] = [];
    for (let index = 0; index < 20; index += 1) {
      const started = process.hrtime.bigint();
      await ingest(post({ providerPostId: `timed-${index}`, text: 'gm' }));
      durations.push(Number(process.hrtime.bigint() - started) / 1_000_000);
    }

    durations.sort((left, right) => left - right);
    const median = durations[Math.floor(durations.length / 2)] as number;
    expect(median).toBeLessThan(500);
  });
});
