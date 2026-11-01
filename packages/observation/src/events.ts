import { z } from 'zod';
import {
  AggregateId,
  AggregateType,
  EventType,
  MediaId,
  ObservationId,
  ObservationProvider,
  Timestamp,
  generateEventId,
} from '@nit/domain';
import { EventEnvelope, PayloadSchemaRegistry } from '@nit/events';

/** Payload schema version carried in the envelope's `version` field. */
export const OBSERVATION_CREATED_VERSION = '1';

/**
 * What `ObservationCreated` announces.
 *
 * Deliberately thin: identifiers and routing facts, not the post's text. A
 * consumer that needs the body reads the observation by id, and cannot be told
 * anything stale by doing so, because the row is immutable — the same guarantee
 * that makes a thin event unsafe in a mutable-row system makes it the better
 * choice here. What *is* carried is what a consumer needs in order to decide
 * whether to do any work at all: `mediaIds` so multimodal processing can start
 * without a query (TIS ch.4 §11), and `hasText` so an interpretation worker can
 * skip a post that has none.
 *
 * Timestamps are ISO strings because the payload round-trips through JSONB.
 */
export interface ObservationCreatedPayload {
  observationId: string;
  provider: ObservationProvider;
  providerPostId: string;
  authorId: string;
  publishedAt: string;
  mediaIds: string[];
  hasText: boolean;
  quotedProviderPostId?: string;
}

export const observationCreatedPayloadSchema = z.object({
  observationId: z.string().uuid(),
  provider: z.nativeEnum(ObservationProvider),
  providerPostId: z.string().min(1),
  authorId: z.string().min(1),
  publishedAt: z.string().datetime(),
  mediaIds: z.array(z.string().uuid()),
  hasText: z.boolean(),
  quotedProviderPostId: z.string().min(1).optional(),
});

export interface ObservationCreatedInput {
  observationId: ObservationId;
  provider: ObservationProvider;
  providerPostId: string;
  authorId: string;
  publishedAt: Timestamp;
  ingestedAt: Timestamp;
  mediaIds: readonly MediaId[];
  hasText: boolean;
  quotedProviderPostId?: string;
  producer: string;
}

/**
 * Build the envelope for a newly ingested observation.
 *
 * `occurredAt` is the ingestion time, not the publication time. The event
 * records something that happened in this system, and replay walks the log in
 * that order; when the provider says the post was written is a fact *about* the
 * observation and travels in the payload.
 */
export function observationCreatedEvent(
  input: ObservationCreatedInput
): EventEnvelope<ObservationCreatedPayload> {
  const payload: ObservationCreatedPayload = {
    observationId: input.observationId,
    provider: input.provider,
    providerPostId: input.providerPostId,
    authorId: input.authorId,
    publishedAt: input.publishedAt.toISOString(),
    mediaIds: [...input.mediaIds],
    hasText: input.hasText,
  };
  if (input.quotedProviderPostId !== undefined) {
    payload.quotedProviderPostId = input.quotedProviderPostId;
  }

  return {
    eventId: generateEventId(),
    eventType: EventType.OBSERVATION_CREATED,
    aggregateType: AggregateType.OBSERVATION,
    aggregateId: input.observationId as string as AggregateId,
    version: OBSERVATION_CREATED_VERSION,
    occurredAt: input.ingestedAt,
    producer: input.producer,
    payload,
  };
}

/**
 * Register this package's payload schemas.
 *
 * Called by the composition root rather than run on import: a registry mutated
 * as a side effect of loading a module is a registry whose contents depend on
 * import order.
 */
export function registerObservationSchemas(registry: PayloadSchemaRegistry): void {
  registry.register(EventType.OBSERVATION_CREATED, observationCreatedPayloadSchema);
}
