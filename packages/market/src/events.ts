import { z } from 'zod';
import {
  AggregateId,
  AggregateType,
  Chain,
  EventType,
  LaunchSource,
  Timestamp,
  TokenId,
  generateEventId,
} from '@nit/domain';
import { EventEnvelope, PayloadSchemaRegistry } from '@nit/events';
import {
  creatorProfileUpdatedPayloadSchema,
  liquidityUpdatedPayloadSchema,
  marketSnapshotCreatedPayloadSchema,
  tokenArtworkProcessedPayloadSchema,
} from './enrichment-events';

/** Payload schema version carried in the envelope's `version` field. */
export const TOKEN_DISCOVERED_VERSION = '1';

/**
 * What `TokenDiscovered` announces.
 *
 * Deliberately thin: identifiers and routing facts, not the token's description.
 * A consumer that needs the content reads the token by id, and cannot be told
 * anything stale by doing so, because the row is immutable.
 *
 * What *is* carried is what a consumer needs in order to decide whether to do any
 * work at all. `address` because the liquidity worker queries a chain by address
 * and would otherwise read the row back just to learn it. `hasArtwork` and
 * `hasCreatorWallet` because those two enrichments do not apply to every token:
 * without them the artwork worker wakes for tokens with no image and the creator
 * worker wakes for tokens with no creator, and both discover there is nothing to
 * do only after claiming a job.
 *
 * `ticker` and `name` are absent for the same reason `@nit/observation` omits a
 * post's text — they are content, not routing. That they are also what a narrative
 * would be matched against is precisely why they are not here: EDS ch.21 §3 says
 * this engine performs no narrative matching, and an event carrying the match keys
 * is an invitation to do it in the wrong stage.
 *
 * Timestamps are ISO strings because the payload round-trips through JSONB.
 */
export interface TokenDiscoveredPayload {
  tokenId: string;
  chain: Chain;
  address: string;
  launchSource: LaunchSource;
  launchTime: string;
  hasArtwork: boolean;
  hasCreatorWallet: boolean;
}

export const tokenDiscoveredPayloadSchema = z.object({
  tokenId: z.string().uuid(),
  chain: z.nativeEnum(Chain),
  address: z.string().min(1),
  launchSource: z.nativeEnum(LaunchSource),
  launchTime: z.string().datetime(),
  hasArtwork: z.boolean(),
  hasCreatorWallet: z.boolean(),
});

export interface TokenDiscoveredInput {
  tokenId: TokenId;
  chain: Chain;
  address: string;
  launchSource: LaunchSource;
  launchTime: Timestamp;
  discoveredAt: Timestamp;
  hasArtwork: boolean;
  hasCreatorWallet: boolean;
  producer: string;
}

/**
 * Build the envelope for a newly discovered token.
 *
 * `occurredAt` is the discovery time, not the launch time. The event records
 * something that happened in this system, and replay walks the log in that order;
 * when the token was actually launched is a fact *about* it and travels in the
 * payload. The distinction is larger here than for an observation: a launch feed
 * queried with an overlapping window routinely returns tokens minutes old, so
 * ordering the log by launch time would interleave today's discoveries with
 * yesterday's and make replay non-deterministic.
 */
export function tokenDiscoveredEvent(
  input: TokenDiscoveredInput
): EventEnvelope<TokenDiscoveredPayload> {
  const payload: TokenDiscoveredPayload = {
    tokenId: input.tokenId,
    chain: input.chain,
    address: input.address,
    launchSource: input.launchSource,
    launchTime: input.launchTime.toISOString(),
    hasArtwork: input.hasArtwork,
    hasCreatorWallet: input.hasCreatorWallet,
  };

  return {
    eventId: generateEventId(),
    eventType: EventType.TOKEN_DISCOVERED,
    aggregateType: AggregateType.TOKEN,
    aggregateId: input.tokenId as string as AggregateId,
    version: TOKEN_DISCOVERED_VERSION,
    occurredAt: input.discoveredAt,
    producer: input.producer,
    payload,
  };
}

/**
 * Register this package's payload schemas.
 *
 * Called by the composition root rather than run on import: a registry mutated as
 * a side effect of loading a module is a registry whose contents depend on import
 * order.
 *
 * All five of this stage's events are here — discovery's one and enrichment's four.
 * Registration is what makes a payload validated on the way into the log, so an
 * event type this package can publish and has not registered is one that reaches
 * `domain_events` unchecked.
 */
export function registerMarketSchemas(registry: PayloadSchemaRegistry): void {
  registry.register(EventType.TOKEN_DISCOVERED, tokenDiscoveredPayloadSchema);
  registry.register(EventType.TOKEN_ARTWORK_PROCESSED, tokenArtworkProcessedPayloadSchema);
  registry.register(EventType.MARKET_SNAPSHOT_CREATED, marketSnapshotCreatedPayloadSchema);
  registry.register(EventType.LIQUIDITY_UPDATED, liquidityUpdatedPayloadSchema);
  registry.register(EventType.CREATOR_PROFILE_UPDATED, creatorProfileUpdatedPayloadSchema);
}
