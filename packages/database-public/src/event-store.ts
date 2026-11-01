import { EventEnvelope, EventStore } from '@nit/events';
import {
  EventId,
  EventType,
  AggregateType,
  AggregateId,
  Result,
  success,
  failure,
} from '@nit/domain';
import { Executor } from './pool';

/**
 * PostgreSQL-backed event store.
 *
 * Takes an `Executor` rather than a `DatabasePool` so an append can join a
 * transaction the caller already opened. That is what makes the transactional
 * outbox work: a domain row and the event announcing it commit together, so no
 * consumer can ever see an event for a row that was rolled back, and no row can
 * be persisted with its event silently lost. `DatabasePool` satisfies
 * `Executor`, so passing one still gives a connection per call.
 */
/**
 * The one order every read here uses.
 *
 * `occurred_at` first because that is when the thing happened, and `sequence`
 * second because timestamps tie: a batch written from one clock read shares one
 * `occurred_at` exactly. Without the tie-break the order among siblings is
 * whatever the plan produces, which is not something replay may depend on.
 */
const EVENT_ORDER = ' ORDER BY occurred_at ASC, sequence ASC';

export class PostgresEventStore implements EventStore {
  constructor(private db: Executor) {}

  async append<TPayload>(event: EventEnvelope<TPayload>): Promise<Result<void, Error>> {
    const query = `
      INSERT INTO domain_events (
        event_id, event_type, aggregate_type, aggregate_id,
        version, occurred_at, producer, payload
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (event_id) DO NOTHING
    `;

    const params = [
      event.eventId,
      event.eventType,
      event.aggregateType,
      event.aggregateId,
      event.version,
      event.occurredAt,
      event.producer,
      JSON.stringify(event.payload),
    ];

    const result = await this.db.query(query, params);
    if (result.kind === 'failure') {
      return failure(result.error);
    }

    return success(undefined);
  }

  async getByAggregateId(aggregateId: string): Promise<Result<EventEnvelope[], Error>> {
    const query = `
      SELECT * FROM domain_events
      WHERE aggregate_id = $1
      ${EVENT_ORDER}
    `;

    return this.queryEvents(query, [aggregateId]);
  }

  async getByEventType(eventType: string): Promise<Result<EventEnvelope[], Error>> {
    const query = `
      SELECT * FROM domain_events
      WHERE event_type = $1
      ${EVENT_ORDER}
    `;

    return this.queryEvents(query, [eventType]);
  }

  async getById(eventId: EventId): Promise<Result<EventEnvelope | null, Error>> {
    const query = `
      SELECT * FROM domain_events
      WHERE event_id = $1
    `;

    const result = await this.queryEvents(query, [eventId]);
    if (result.kind === 'failure') {
      return failure(result.error);
    }

    const events = result.value;
    return success(events[0] ?? null);
  }

  /**
   * The page of events following one known event.
   *
   * The boundary is the whole sort key compared as a tuple, not just
   * `occurred_at`. Comparing only the timestamp skipped every event sharing the
   * boundary's instant, so a batch that ingested in one millisecond lost all but
   * its last member — silently, and permanently, since paging never goes back.
   */
  async getAfter(afterEventId: EventId, limit: number): Promise<Result<EventEnvelope[], Error>> {
    // The boundary's columns are renamed so the comparison and the shared
    // EVENT_ORDER can both name `occurred_at` and `sequence` unqualified without
    // being ambiguous. An unknown id makes the CTE empty, so the join yields no
    // rows — the same empty page the previous scalar subquery returned.
    const query = `
      WITH boundary AS (
        SELECT occurred_at AS at, sequence AS seq FROM domain_events WHERE event_id = $1
      )
      SELECT domain_events.* FROM domain_events, boundary
      WHERE (occurred_at, sequence) > (boundary.at, boundary.seq)
      ${EVENT_ORDER}
      LIMIT $2
    `;

    return this.queryEvents(query, [afterEventId, limit]);
  }

  private async queryEvents(
    query: string,
    params: unknown[]
  ): Promise<Result<EventEnvelope[], Error>> {
    const result = await this.db.query<{
      event_id: string;
      event_type: string;
      aggregate_type: string;
      aggregate_id: string;
      version: string;
      occurred_at: Date;
      producer: string;
      // Already a JS value: the column is JSONB and the driver parses it. Typing
      // it as a string and calling JSON.parse here threw on every read, because
      // an object coerced to a string is "[object Object]".
      payload: unknown;
    }>(query, params);

    if (result.kind === 'failure') {
      return failure(result.error);
    }

    const events = result.value.map((row) => ({
      eventId: row.event_id as EventId,
      eventType: row.event_type as EventType,
      aggregateType: row.aggregate_type as AggregateType,
      aggregateId: row.aggregate_id as AggregateId,
      version: row.version,
      occurredAt: row.occurred_at,
      producer: row.producer,
      payload: row.payload,
    }));

    return success(events);
  }
}
