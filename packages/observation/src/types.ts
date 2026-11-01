import { MediaId, MediaType, ObservationId, ObservationProvider, Timestamp } from '@nit/domain';

/**
 * The canonical observation, per TIS ch.4 §4.
 *
 * Every downstream worker consumes this rather than a provider payload. That is
 * the point of the shape: interpretation, multimodal processing and the
 * narrative graph must not need to know whether a post came from X or Telegram,
 * and adding a provider must not require touching them.
 *
 * `quotedObservationId` is resolved on read rather than stored (see the schema
 * note in `@nit/database-public`'s migrations): the row holds the quoted post's
 * *provider* id, and the repository joins it back to an observation. So this
 * field is populated once the quoted post has itself been ingested, in whichever
 * order the two arrive.
 */
export interface Observation {
  observationId: ObservationId;
  provider: ObservationProvider;
  providerPostId: string;
  authorId: string;
  publishedAt: Timestamp;
  ingestedAt: Timestamp;
  text?: string;
  mediaIds: MediaId[];
  quotedObservationId?: ObservationId;
}

/**
 * A media attachment as it exists immediately after ingestion: identified and
 * located, but not yet downloaded.
 *
 * The bytes arrive later, outside the ingest transaction (TIS ch.2 §7, ch.4 §9),
 * so `sha256`, `phash`, the storage location and any measured duration are the
 * multimodal worker's business and are absent here.
 */
export interface MediaReference {
  mediaId: MediaId;
  observationId: ObservationId;
  providerMediaKey: string;
  mediaType: MediaType;
  sourceUrl: string;
  durationMs?: number;
}
