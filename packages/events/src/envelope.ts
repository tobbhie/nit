import {
  EventId,
  AggregateId,
  AggregateType,
  EventType,
  Timestamp,
} from '@nit/domain';

/**
 * Versioned event envelope - immutable and append-only
 */
export interface EventEnvelope<TPayload = unknown> {
  eventId: EventId;
  eventType: EventType;
  aggregateType: AggregateType;
  aggregateId: AggregateId;
  version: string;
  occurredAt: Timestamp;
  producer: string;
  payload: TPayload;
}

/**
 * Event metadata for storage
 */
export interface EventMetadata {
  eventId: EventId;
  eventType: EventType;
  aggregateType: AggregateType;
  aggregateId: AggregateId;
  version: string;
  occurredAt: Timestamp;
  producer: string;
  payloadSchema: string;
}
