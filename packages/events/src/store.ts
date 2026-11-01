import { EventEnvelope } from './envelope';
import { EventId, Result } from '@nit/domain';

/**
 * Event store interface - append-only with duplicate tolerance
 */
export interface EventStore {
  /**
   * Append a new event to the store
   * Idempotent - duplicate eventId is silently ignored
   */
  append<TPayload>(event: EventEnvelope<TPayload>): Promise<Result<void, Error>>;

  /**
   * Get events by aggregate ID
   */
  getByAggregateId(aggregateId: string): Promise<Result<EventEnvelope[], Error>>;

  /**
   * Get events by event type
   */
  getByEventType(eventType: string): Promise<Result<EventEnvelope[], Error>>;

  /**
   * Get event by ID
   */
  getById(eventId: EventId): Promise<Result<EventEnvelope | null, Error>>;

  /**
   * Get events after a specific event ID (for streaming/replay)
   */
  getAfter(afterEventId: EventId, limit: number): Promise<Result<EventEnvelope[], Error>>;
}
