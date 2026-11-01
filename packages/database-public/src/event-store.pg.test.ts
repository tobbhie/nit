import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  AggregateId,
  AggregateType,
  EventId,
  EventType,
  Timestamp,
  generateAggregateId,
  generateEventId,
} from '@nit/domain';
import { EventEnvelope } from '@nit/events';
import { PostgresEventStore } from './event-store';
import { clientExecutor } from './pool';
import { createTestDatabase, hasTestDatabase, TestDatabase } from './testing';

/**
 * The event store against a real PostgreSQL.
 *
 * The read path only has one job — hand back what was appended — and it is
 * exactly the job that cannot be verified without a database, because what makes
 * it hard is the driver's JSONB handling rather than any logic here. Every read
 * method used to throw for that reason, undetected, so these tests exist to keep
 * the round trip pinned rather than to describe behaviour that was ever in doubt.
 */

const PRODUCER = 'event-store-test';

interface Payload {
  readonly [key: string]: unknown;
}

describe.skipIf(!hasTestDatabase())('PostgresEventStore against Postgres', () => {
  let harness: TestDatabase;
  let store: PostgresEventStore;

  beforeAll(async () => {
    harness = await createTestDatabase('event-store');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  beforeEach(async () => {
    const truncated = await harness.db.query('TRUNCATE domain_events');
    expect(truncated.kind).toBe('success');
    store = new PostgresEventStore(harness.db);
  });

  /** An envelope with fresh identifiers, so tests never collide on the primary key. */
  const envelope = (
    overrides: {
      eventType?: EventType;
      aggregateId?: AggregateId;
      occurredAt?: Timestamp;
      payload?: Payload;
    } = {}
  ): EventEnvelope<Payload> => ({
    eventId: generateEventId(),
    eventType: overrides.eventType ?? EventType.OBSERVATION_CREATED,
    aggregateType: AggregateType.OBSERVATION,
    aggregateId: overrides.aggregateId ?? generateAggregateId(),
    version: '1',
    occurredAt: overrides.occurredAt ?? new Date('2026-08-19T10:00:00.000Z'),
    producer: PRODUCER,
    payload: overrides.payload ?? { note: 'nothing in particular' },
  });

  const append = async (event: EventEnvelope<Payload>): Promise<void> => {
    const result = await store.append(event);
    if (result.kind === 'failure') {
      throw result.error;
    }
  };

  const byAggregate = async (aggregateId: AggregateId): Promise<EventEnvelope[]> => {
    const result = await store.getByAggregateId(aggregateId);
    if (result.kind === 'failure') {
      throw result.error;
    }
    return result.value;
  };

  it('returns an appended event field for field', async () => {
    const event = envelope();
    await append(event);

    const [stored] = await byAggregate(event.aggregateId);
    expect(stored).toEqual(event);
  });

  it('preserves a payload of every JSON shape', async () => {
    // The read path is the whole risk here: a JSONB column comes back as a parsed
    // value, so anything that treats it as a string breaks on the first read.
    const payload: Payload = {
      text: 'gm',
      count: 42,
      ratio: 0.5,
      flag: false,
      absent: null,
      list: [1, 'two', { three: true }],
      nested: { deep: { deeper: ['x'] } },
      empty: {},
    };
    const event = envelope({ payload });
    await append(event);

    const [stored] = await byAggregate(event.aggregateId);
    expect(stored?.payload).toEqual(payload);
  });

  it('preserves a payload that is not an object', async () => {
    // JSONB accepts a bare scalar, and a consumer that expected an object would
    // rather see the scalar than a parse error.
    const event = envelope();
    const scalar: EventEnvelope<unknown> = { ...event, payload: 'just a string' };
    const appended = await store.append(scalar);
    expect(appended.kind).toBe('success');

    const [stored] = await byAggregate(event.aggregateId);
    expect(stored?.payload).toBe('just a string');
  });

  it('returns one aggregate oldest first', async () => {
    // Replay walks the log forward, so this order is the order effects happened in.
    const aggregateId = generateAggregateId();
    const at = (minute: number): Timestamp => new Date(`2026-08-19T10:0${minute}:00.000Z`);
    await append(envelope({ aggregateId, occurredAt: at(2), payload: { step: 'third' } }));
    await append(envelope({ aggregateId, occurredAt: at(0), payload: { step: 'first' } }));
    await append(envelope({ aggregateId, occurredAt: at(1), payload: { step: 'second' } }));

    const stored = await byAggregate(aggregateId);
    expect(stored.map((event) => (event.payload as { step: string }).step)).toEqual([
      'first',
      'second',
      'third',
    ]);
  });

  it('does not return another aggregate\'s events', async () => {
    const mine = generateAggregateId();
    await append(envelope({ aggregateId: mine }));
    await append(envelope({ aggregateId: generateAggregateId() }));

    expect(await byAggregate(mine)).toHaveLength(1);
  });

  it('filters by event type', async () => {
    await append(envelope({ eventType: EventType.OBSERVATION_CREATED }));
    await append(envelope({ eventType: EventType.NARRATIVE_SCORED }));
    await append(envelope({ eventType: EventType.NARRATIVE_SCORED }));

    const scored = await store.getByEventType(EventType.NARRATIVE_SCORED);
    if (scored.kind === 'failure') {
      throw scored.error;
    }
    expect(scored.value).toHaveLength(2);
    for (const event of scored.value) {
      expect(event.eventType).toBe(EventType.NARRATIVE_SCORED);
    }
  });

  it('reads an event back by its own id', async () => {
    const event = envelope();
    await append(event);

    const found = await store.getById(event.eventId);
    if (found.kind === 'failure') {
      throw found.error;
    }
    expect(found.value?.eventId).toBe(event.eventId);
  });

  it('reports an unknown event id as absent rather than as an error', async () => {
    const found = await store.getById(generateEventId());
    expect(found.kind).toBe('success');
    if (found.kind === 'success') {
      expect(found.value).toBeNull();
    }
  });

  it('rejects an id that is not a uuid as a failure, not a throw', async () => {
    // Every id here is branded, but the value crosses an HTTP or job boundary
    // before it arrives, so the brand is a claim rather than a guarantee.
    const found = await store.getById('not-a-uuid' as EventId);
    expect(found.kind).toBe('failure');
  });

  it('absorbs a re-appended event instead of duplicating it', async () => {
    // At-least-once delivery means a producer retrying after an ambiguous failure
    // will re-append; the event id is what makes that safe.
    const event = envelope();
    await append(event);
    await append(event);

    expect(await byAggregate(event.aggregateId)).toHaveLength(1);
  });

  it('keeps the first append when a re-append carries a different payload', async () => {
    // DO NOTHING, not DO UPDATE: the log is append-only, so the same event id
    // arriving with new contents is a producer bug and must not rewrite history.
    const event = envelope({ payload: { as: 'recorded' } });
    await append(event);
    await append({ ...event, payload: { as: 'rewritten' } });

    const [stored] = await byAggregate(event.aggregateId);
    expect(stored?.payload).toEqual({ as: 'recorded' });
  });

  it('pages forward from a known event', async () => {
    const aggregateId = generateAggregateId();
    const at = (minute: number): Timestamp => new Date(`2026-08-19T10:0${minute}:00.000Z`);
    const events = [0, 1, 2, 3].map((minute) =>
      envelope({ aggregateId, occurredAt: at(minute), payload: { minute } })
    );
    for (const event of events) {
      await append(event);
    }

    const after = await store.getAfter((events[1] as EventEnvelope).eventId, 2);
    if (after.kind === 'failure') {
      throw after.error;
    }
    expect(after.value.map((event) => (event.payload as { minute: number }).minute)).toEqual([2, 3]);
  });

  it('pages past a shared timestamp without skipping its siblings', async () => {
    // The case a distinct timestamp per event cannot reach, and the one the log
    // actually produces: ingestion writes a batch from one clock read, so a whole
    // batch shares an instant. A boundary of `occurred_at > last` steps over every
    // sibling at that instant, and paging never comes back for them.
    const aggregateId = generateAggregateId();
    const together = new Date('2026-08-19T10:00:00.000Z');
    const events = ['first', 'second', 'third'].map((step) =>
      envelope({ aggregateId, occurredAt: together, payload: { step } })
    );
    for (const event of events) {
      await append(event);
    }

    const after = await store.getAfter((events[0] as EventEnvelope).eventId, 10);
    if (after.kind === 'failure') {
      throw after.error;
    }
    expect(after.value.map((event) => (event.payload as { step: string }).step)).toEqual([
      'second',
      'third',
    ]);
  });

  it('walks a whole log of one instant in pages of one, losing nothing', async () => {
    // Paging is only correct if the pages reassemble into the log. Size one is
    // where an off-by-one at the boundary has nowhere to hide.
    const aggregateId = generateAggregateId();
    const together = new Date('2026-08-19T10:00:00.000Z');
    const steps = [0, 1, 2, 3, 4];
    for (const step of steps) {
      await append(envelope({ aggregateId, occurredAt: together, payload: { step } }));
    }

    const stored = await byAggregate(aggregateId);
    const walked: number[] = [];
    let cursor = (stored[0] as EventEnvelope).eventId;
    for (;;) {
      const page = await store.getAfter(cursor, 1);
      if (page.kind === 'failure') {
        throw page.error;
      }
      const next = page.value[0];
      if (next === undefined) {
        break;
      }
      walked.push((next.payload as { step: number }).step);
      cursor = next.eventId;
    }

    expect([(stored[0] as EventEnvelope).payload, ...walked.map((step) => ({ step }))]).toEqual(
      steps.map((step) => ({ step }))
    );
  });

  it('orders events at one instant by the order they were appended in', async () => {
    // The tie-break is insertion order, not event_id: ids here are v4, so an
    // id tie-break would shuffle a batch the producer had deliberately sorted —
    // and the ingestion service sorts precisely so the log cannot describe a
    // reply arriving before the post it replies to.
    const aggregateId = generateAggregateId();
    const together = new Date('2026-08-19T10:00:00.000Z');
    const steps = ['one', 'two', 'three', 'four', 'five'];
    for (const step of steps) {
      await append(envelope({ aggregateId, occurredAt: together, payload: { step } }));
    }

    const stored = await byAggregate(aggregateId);
    expect(stored.map((event) => (event.payload as { step: string }).step)).toEqual(steps);
  });

  it('reads one aggregate in the same order on every read', async () => {
    // Determinism is the property, so re-reading is the test. A plan free to
    // choose among ties may choose differently once the data changes shape.
    const aggregateId = generateAggregateId();
    const together = new Date('2026-08-19T10:00:00.000Z');
    for (const step of [0, 1, 2, 3, 4, 5, 6, 7]) {
      await append(envelope({ aggregateId, occurredAt: together, payload: { step } }));
    }

    const first = await byAggregate(aggregateId);
    const second = await byAggregate(aggregateId);
    expect(second.map((event) => event.eventId)).toEqual(first.map((event) => event.eventId));
  });

  it('orders a filtered read the same way it orders an aggregate', async () => {
    // getByEventType feeds the same replay as getByAggregateId, so a tie it left
    // unordered would be just as undeterministic.
    const together = new Date('2026-08-19T10:00:00.000Z');
    const steps = ['a', 'b', 'c'];
    for (const step of steps) {
      await append(
        envelope({ eventType: EventType.NARRATIVE_SCORED, occurredAt: together, payload: { step } })
      );
    }

    const scored = await store.getByEventType(EventType.NARRATIVE_SCORED);
    if (scored.kind === 'failure') {
      throw scored.error;
    }
    expect(scored.value.map((event) => (event.payload as { step: string }).step)).toEqual(steps);
  });

  it('appends nothing when the transaction it joined rolls back', async () => {
    // The outbox guarantee, from the store's side: an event committed alongside a
    // row that was rolled back would announce something that never happened.
    const event = envelope();
    const rolled = await harness.db.transaction(async (client) => {
      const appended = await new PostgresEventStore(clientExecutor(client)).append(event);
      expect(appended.kind).toBe('success');
      throw new Error('caller aborted the transaction');
    });

    expect(rolled.kind).toBe('failure');
    expect(await byAggregate(event.aggregateId)).toHaveLength(0);
  });
});
