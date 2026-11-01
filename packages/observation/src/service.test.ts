import { describe, expect, it } from 'vitest';
import { MediaType, ObservationProvider } from '@nit/domain';
import { EventEnvelope } from '@nit/events';
import { ObservationCreatedPayload } from './events';
import { FakeUnitOfWork, INGESTED, post } from './in-memory.fixtures';
import { ObservationIngestionService } from './service';

function serviceOn(uow: FakeUnitOfWork): ObservationIngestionService {
  return new ObservationIngestionService(uow, {
    producer: 'ingestion-test',
    clock: () => INGESTED,
  });
}

function payloads(uow: FakeUnitOfWork): ObservationCreatedPayload[] {
  return uow.store.events.map((event) => event.payload as ObservationCreatedPayload);
}

describe('ObservationIngestionService.ingest', () => {
  it('persists the observation and announces it exactly once', async () => {
    const uow = new FakeUnitOfWork();
    const result = await serviceOn(uow).ingest(ObservationProvider.X, post({ text: 'gm' }));

    expect(result.kind).toBe('success');
    if (result.kind === 'failure') {
      throw result.error;
    }
    expect(result.value.status).toBe('ingested');
    expect(uow.store.observations.size).toBe(1);
    expect(uow.store.events).toHaveLength(1);

    const event = uow.store.events[0] as EventEnvelope;
    expect(event.eventType).toBe('observation.created');
    expect(event.aggregateType).toBe('observation');
    expect(event.aggregateId).toBe(result.value.observationId);
    expect(event.producer).toBe('ingestion-test');
    // Occurrence is ingestion time, not publication time.
    expect(event.occurredAt).toEqual(INGESTED);
  });

  it('tells a consumer what to process without carrying the post body', async () => {
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).ingest(
      ObservationProvider.X,
      post({
        text: 'look at this',
        quotedProviderPostId: 'post-0',
        media: [
          { providerMediaKey: 'm1', mediaType: MediaType.IMAGE, sourceUrl: 'https://x.invalid/1' },
        ],
      })
    );

    const payload = payloads(uow)[0] as ObservationCreatedPayload;
    expect(payload.mediaIds).toHaveLength(1);
    expect(payload.hasText).toBe(true);
    expect(payload.quotedProviderPostId).toBe('post-0');
    expect(payload.publishedAt).toBe('2026-08-19T10:00:00.000Z');
    // The text itself stays in the immutable row a consumer can read by id.
    expect(JSON.stringify(payload)).not.toContain('look at this');
  });

  it('reports a re-seen post as a duplicate and does not announce it again', async () => {
    const uow = new FakeUnitOfWork();
    const service = serviceOn(uow);

    const first = await service.ingest(ObservationProvider.X, post());
    const second = await service.ingest(ObservationProvider.X, post());
    if (first.kind === 'failure' || second.kind === 'failure') {
      throw new Error('Expected both ingests to succeed');
    }

    expect(first.value.status).toBe('ingested');
    expect(second.value.status).toBe('duplicate');
    // The same observation, not a second one.
    expect(second.value.observationId).toBe(first.value.observationId);
    expect(uow.store.observations.size).toBe(1);
    expect(uow.store.events).toHaveLength(1);
  });

  it('persists nothing when the event cannot be appended', async () => {
    // The outbox guarantee. An observation no worker will ever hear about is
    // worse than no observation, because nothing reports it as missing.
    const uow = new FakeUnitOfWork(true);
    const result = await serviceOn(uow).ingest(ObservationProvider.X, post());

    expect(result.kind).toBe('failure');
    expect(uow.store.observations.size).toBe(0);
    expect(uow.store.events).toHaveLength(0);
  });

  it('rejects a malformed post without opening a transaction', async () => {
    const uow = new FakeUnitOfWork();
    const result = await serviceOn(uow).ingest(ObservationProvider.X, post({ authorId: '' }));

    expect(result.kind).toBe('failure');
    expect(uow.transactions).toBe(0);
  });
});

describe('ObservationIngestionService.ingestBatch', () => {
  it('announces posts oldest first regardless of the order they arrived in', async () => {
    // The event log is what replay reads, so a reply must not be announced
    // before the post it replies to.
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).ingestBatch(ObservationProvider.X, [
      post({ providerPostId: 'third', publishedAt: new Date('2026-08-19T10:00:02.000Z') }),
      post({ providerPostId: 'first', publishedAt: new Date('2026-08-19T10:00:00.000Z') }),
      post({ providerPostId: 'second', publishedAt: new Date('2026-08-19T10:00:01.000Z') }),
    ]);

    expect(payloads(uow).map((payload) => payload.providerPostId)).toEqual([
      'first',
      'second',
      'third',
    ]);
  });

  it('orders posts sharing a publication time deterministically', async () => {
    const sameInstant = new Date('2026-08-19T10:00:00.000Z');
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).ingestBatch(ObservationProvider.X, [
      post({ providerPostId: 'b', publishedAt: sameInstant }),
      post({ providerPostId: 'a', publishedAt: sameInstant }),
    ]);

    expect(payloads(uow).map((payload) => payload.providerPostId)).toEqual(['a', 'b']);
  });

  it('keeps going after a bad post and reports which one failed', async () => {
    // TIS ch.4 §9: continue processing despite provider failures.
    const uow = new FakeUnitOfWork();
    const batch = await serviceOn(uow).ingestBatch(ObservationProvider.X, [
      post({ providerPostId: 'good-1', publishedAt: new Date('2026-08-19T10:00:00.000Z') }),
      post({
        providerPostId: 'broken',
        publishedAt: new Date('2026-08-19T10:00:01.000Z'),
        authorId: '  ',
      }),
      post({ providerPostId: 'good-2', publishedAt: new Date('2026-08-19T10:00:02.000Z') }),
    ]);

    expect(batch.ingested).toHaveLength(2);
    expect(batch.failures).toHaveLength(1);
    expect(batch.failures[0]?.providerPostId).toBe('broken');
    expect(payloads(uow).map((payload) => payload.providerPostId)).toEqual(['good-1', 'good-2']);
  });

  it('separates duplicates from new posts when a poll overlaps the last one', async () => {
    const uow = new FakeUnitOfWork();
    const service = serviceOn(uow);
    const earlier = post({
      providerPostId: 'seen',
      publishedAt: new Date('2026-08-19T10:00:00.000Z'),
    });
    const later = post({
      providerPostId: 'fresh',
      publishedAt: new Date('2026-08-19T10:00:01.000Z'),
    });

    await service.ingest(ObservationProvider.X, earlier);
    const batch = await service.ingestBatch(ObservationProvider.X, [earlier, later]);

    expect(batch.duplicates).toHaveLength(1);
    expect(batch.ingested).toHaveLength(1);
    expect(uow.store.events).toHaveLength(2);
  });
});
