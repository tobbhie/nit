import { z } from 'zod';
import {
  AggregateId,
  AggregateType,
  Chain,
  CreatorProfileId,
  EventType,
  MarketSnapshotId,
  MarketVenue,
  Timestamp,
  TokenArtworkId,
  TokenId,
  generateEventId,
} from '@nit/domain';
import { EventEnvelope } from '@nit/events';
import { Decimal, isDecimal } from './enrichment-contracts';

/**
 * The four events progressive enrichment publishes (TIS ch.9 §"Domain Events").
 *
 * All four are keyed to `AggregateType.TOKEN` with the token's own identifier,
 * including `CreatorProfileUpdated` — whose subject, a creator's launch history,
 * is not a token. That is deliberate. These events exist because a token was
 * enriched, they are emitted by a worker holding that token's job, and keying them
 * to the token makes `getByAggregateId(tokenId)` return one token's complete
 * enrichment history in order, which is the query replay and any audit of "what do
 * we know about this token, and when did we learn it" actually needs. A first-class
 * creator aggregate would answer a different question, and EDS ch.21 §9 puts creator
 * reputation in the Production tier, so inventing one now would be inventing a
 * contract before there is anything to honour it. The profile's own identifier and
 * wallet travel in the payload.
 *
 * Two of the three enrichment tables are *mutable* — `token_artwork` and
 * `creator_profiles` are overwritten in place, which is what makes re-running
 * enrichment idempotent rather than version-accumulating. That changes what a
 * payload has to carry compared to `TokenDiscovered`: there, the payload could stay
 * thin because a consumer reading `tokens` by id gets an immutable row and cannot be
 * told anything stale. Here the row can have been rewritten since, so the event log
 * is the only durable record of what a given analysis actually produced, and the
 * bounded facts a consumer would route on belong in the payload.
 *
 * Timestamps are ISO strings and amounts are decimal strings, because the payload
 * round-trips through JSONB. See `enrichment-contracts` on why amounts are never
 * `number`.
 */

export const TOKEN_ARTWORK_PROCESSED_VERSION = '1';
export const MARKET_SNAPSHOT_CREATED_VERSION = '1';
export const LIQUIDITY_UPDATED_VERSION = '1';
export const CREATOR_PROFILE_UPDATED_VERSION = '1';

/** A decimal amount that survived the round trip as one. */
const decimalSchema = z
  .string()
  .refine(isDecimal, { message: 'must be an unsigned decimal amount' });

/**
 * What `TokenArtworkProcessed` announces.
 *
 * The hashes are carried; the OCR text is not. A hash is a bounded identifier a
 * consumer routes on — `phash` is how a near-duplicate launch is recognised without
 * fetching an image — whereas OCR text is unbounded content that belongs in the row
 * and would put an arbitrary amount of a memecoin's artwork into the event log.
 *
 * Every analysis field is optional because a partial analysis is a real and useful
 * outcome: the download can succeed while the store rejects the bytes, or the image
 * can hash cleanly and contain no text.
 */
export interface TokenArtworkProcessedPayload {
  tokenId: string;
  artworkId: string;
  sourceUrl: string;
  sha256?: string;
  phash?: string;
  /** Whether text was found, without reproducing it. */
  hasOcrText: boolean;
  analysedAt: string;
  /** The analyser that produced this, so a toy run is distinguishable in the log. */
  provider: string;
}

export const tokenArtworkProcessedPayloadSchema = z.object({
  tokenId: z.string().uuid(),
  artworkId: z.string().uuid(),
  sourceUrl: z.string().min(1),
  sha256: z.string().min(1).optional(),
  phash: z.string().min(1).optional(),
  hasOcrText: z.boolean(),
  analysedAt: z.string().datetime(),
  provider: z.string().min(1),
});

export interface TokenArtworkProcessedInput {
  tokenId: TokenId;
  artworkId: TokenArtworkId;
  sourceUrl: string;
  sha256?: string;
  phash?: string;
  hasOcrText: boolean;
  analysedAt: Timestamp;
  provider: string;
  producer: string;
}

export function tokenArtworkProcessedEvent(
  input: TokenArtworkProcessedInput
): EventEnvelope<TokenArtworkProcessedPayload> {
  const payload: TokenArtworkProcessedPayload = {
    tokenId: input.tokenId,
    artworkId: input.artworkId,
    sourceUrl: input.sourceUrl,
    hasOcrText: input.hasOcrText,
    analysedAt: input.analysedAt.toISOString(),
    provider: input.provider,
  };
  // Assigned conditionally rather than spread: `exactOptionalPropertyTypes` refuses
  // an explicit `undefined` where the property is merely optional, and a JSONB round
  // trip drops the key anyway, so a consumer must branch on absence either way.
  if (input.sha256 !== undefined) {
    payload.sha256 = input.sha256;
  }
  if (input.phash !== undefined) {
    payload.phash = input.phash;
  }

  return {
    eventId: generateEventId(),
    eventType: EventType.TOKEN_ARTWORK_PROCESSED,
    aggregateType: AggregateType.TOKEN,
    aggregateId: input.tokenId as string as AggregateId,
    version: TOKEN_ARTWORK_PROCESSED_VERSION,
    occurredAt: input.analysedAt,
    producer: input.producer,
    payload,
  };
}

/**
 * What `MarketSnapshotCreated` announces: one reading of a token's market state.
 *
 * The metrics are carried in full rather than referenced, because they are bounded,
 * because a consumer that had to read `market_snapshots` back would be querying an
 * append-only table by its newest row and racing the next poll, and because
 * `LiquidityUpdated` below is derived from exactly these values — a consumer
 * receiving both should be able to see that they agree.
 *
 * `observedAt` is the provider's timestamp for the reading and is part of the row's
 * identity; `occurredAt` on the envelope is the same instant, because the fact this
 * event reports is the observation rather than the act of storing it.
 */
export interface MarketSnapshotCreatedPayload {
  tokenId: string;
  snapshotId: string;
  venue: MarketVenue;
  /** The provider that answered. Part of the snapshot's identity. */
  source: string;
  observedAt: string;
  priceUsd?: Decimal;
  liquidityUsd?: Decimal;
  marketCapUsd?: Decimal;
  volume24hUsd?: Decimal;
  holderCount?: number;
}

export const marketSnapshotCreatedPayloadSchema = z.object({
  tokenId: z.string().uuid(),
  snapshotId: z.string().uuid(),
  venue: z.nativeEnum(MarketVenue),
  source: z.string().min(1),
  observedAt: z.string().datetime(),
  priceUsd: decimalSchema.optional(),
  liquidityUsd: decimalSchema.optional(),
  marketCapUsd: decimalSchema.optional(),
  volume24hUsd: decimalSchema.optional(),
  holderCount: z.number().int().nonnegative().optional(),
});

export interface MarketSnapshotCreatedInput {
  tokenId: TokenId;
  snapshotId: MarketSnapshotId;
  venue: MarketVenue;
  source: string;
  observedAt: Timestamp;
  priceUsd?: Decimal;
  liquidityUsd?: Decimal;
  marketCapUsd?: Decimal;
  volume24hUsd?: Decimal;
  holderCount?: number;
  producer: string;
}

export function marketSnapshotCreatedEvent(
  input: MarketSnapshotCreatedInput
): EventEnvelope<MarketSnapshotCreatedPayload> {
  const payload: MarketSnapshotCreatedPayload = {
    tokenId: input.tokenId,
    snapshotId: input.snapshotId,
    venue: input.venue,
    source: input.source,
    observedAt: input.observedAt.toISOString(),
  };
  if (input.priceUsd !== undefined) {
    payload.priceUsd = input.priceUsd;
  }
  if (input.liquidityUsd !== undefined) {
    payload.liquidityUsd = input.liquidityUsd;
  }
  if (input.marketCapUsd !== undefined) {
    payload.marketCapUsd = input.marketCapUsd;
  }
  if (input.volume24hUsd !== undefined) {
    payload.volume24hUsd = input.volume24hUsd;
  }
  if (input.holderCount !== undefined) {
    payload.holderCount = input.holderCount;
  }

  return {
    eventId: generateEventId(),
    eventType: EventType.MARKET_SNAPSHOT_CREATED,
    aggregateType: AggregateType.TOKEN,
    aggregateId: input.tokenId as string as AggregateId,
    version: MARKET_SNAPSHOT_CREATED_VERSION,
    occurredAt: input.observedAt,
    producer: input.producer,
    payload,
  };
}

/**
 * What `LiquidityUpdated` announces: that a token's liquidity is not what the last
 * snapshot from the same source said.
 *
 * Distinct from `MarketSnapshotCreated`, which fires for every recorded reading.
 * This one fires only on a change, and it exists because liquidity moving is the
 * market signal a consumer wants to wake on, where "another snapshot was stored" is
 * bookkeeping. The first snapshot of a token emits only the created event: there is
 * no previous reading for it to differ from, and calling a first observation an
 * update would make every token look like it had just moved.
 *
 * There is no materiality threshold. Any difference is reported, both values are
 * carried, and the consumer decides what counts as significant — a percentage
 * constant chosen here would be a number nothing in the specification justifies,
 * living in the stage least able to know what a given alert cares about.
 *
 * Both amounts are optional, and their absence is itself a change worth announcing:
 * a provider that reported liquidity last time and reports none now has told us
 * something, and it is not that liquidity is zero.
 */
export interface LiquidityUpdatedPayload {
  tokenId: string;
  /** The snapshot that carried the new value, so the two events can be joined. */
  snapshotId: string;
  venue: MarketVenue;
  source: string;
  observedAt: string;
  previousLiquidityUsd?: Decimal;
  liquidityUsd?: Decimal;
  /** When the reading it is compared against was taken. */
  previousObservedAt: string;
}

export const liquidityUpdatedPayloadSchema = z.object({
  tokenId: z.string().uuid(),
  snapshotId: z.string().uuid(),
  venue: z.nativeEnum(MarketVenue),
  source: z.string().min(1),
  observedAt: z.string().datetime(),
  previousLiquidityUsd: decimalSchema.optional(),
  liquidityUsd: decimalSchema.optional(),
  previousObservedAt: z.string().datetime(),
});

export interface LiquidityUpdatedInput {
  tokenId: TokenId;
  snapshotId: MarketSnapshotId;
  venue: MarketVenue;
  source: string;
  observedAt: Timestamp;
  previousLiquidityUsd?: Decimal;
  liquidityUsd?: Decimal;
  previousObservedAt: Timestamp;
  producer: string;
}

export function liquidityUpdatedEvent(
  input: LiquidityUpdatedInput
): EventEnvelope<LiquidityUpdatedPayload> {
  const payload: LiquidityUpdatedPayload = {
    tokenId: input.tokenId,
    snapshotId: input.snapshotId,
    venue: input.venue,
    source: input.source,
    observedAt: input.observedAt.toISOString(),
    previousObservedAt: input.previousObservedAt.toISOString(),
  };
  if (input.previousLiquidityUsd !== undefined) {
    payload.previousLiquidityUsd = input.previousLiquidityUsd;
  }
  if (input.liquidityUsd !== undefined) {
    payload.liquidityUsd = input.liquidityUsd;
  }

  return {
    eventId: generateEventId(),
    eventType: EventType.LIQUIDITY_UPDATED,
    aggregateType: AggregateType.TOKEN,
    aggregateId: input.tokenId as string as AggregateId,
    version: LIQUIDITY_UPDATED_VERSION,
    occurredAt: input.observedAt,
    producer: input.producer,
    payload,
  };
}

/**
 * What `CreatorProfileUpdated` announces.
 *
 * No reputation score, and not because one would be hard to compute. EDS ch.21 §9
 * puts creator reputation in the Production tier, and a score field filled with a
 * placeholder is worse than an absent one: correlation would read it as though it
 * meant something, and nothing downstream would record that it did not.
 *
 * What is here is countable from `tokens` alone, which is what makes the creator
 * worker idempotent — recomputing from an append-only source gives the same answer
 * however many times it runs, so the worker never has to know whether it has run
 * before.
 */
export interface CreatorProfileUpdatedPayload {
  /** The token whose discovery triggered the recompute. */
  tokenId: string;
  creatorProfileId: string;
  chain: Chain;
  creatorWallet: string;
  launchCount: number;
  firstLaunchAt: string;
  lastLaunchAt: string;
  analysedAt: string;
}

export const creatorProfileUpdatedPayloadSchema = z.object({
  tokenId: z.string().uuid(),
  creatorProfileId: z.string().uuid(),
  chain: z.nativeEnum(Chain),
  creatorWallet: z.string().min(1),
  launchCount: z.number().int().positive(),
  firstLaunchAt: z.string().datetime(),
  lastLaunchAt: z.string().datetime(),
  analysedAt: z.string().datetime(),
});

export interface CreatorProfileUpdatedInput {
  tokenId: TokenId;
  creatorProfileId: CreatorProfileId;
  chain: Chain;
  creatorWallet: string;
  launchCount: number;
  firstLaunchAt: Timestamp;
  lastLaunchAt: Timestamp;
  analysedAt: Timestamp;
  producer: string;
}

export function creatorProfileUpdatedEvent(
  input: CreatorProfileUpdatedInput
): EventEnvelope<CreatorProfileUpdatedPayload> {
  const payload: CreatorProfileUpdatedPayload = {
    tokenId: input.tokenId,
    creatorProfileId: input.creatorProfileId,
    chain: input.chain,
    creatorWallet: input.creatorWallet,
    launchCount: input.launchCount,
    firstLaunchAt: input.firstLaunchAt.toISOString(),
    lastLaunchAt: input.lastLaunchAt.toISOString(),
    analysedAt: input.analysedAt.toISOString(),
  };

  return {
    eventId: generateEventId(),
    eventType: EventType.CREATOR_PROFILE_UPDATED,
    aggregateType: AggregateType.TOKEN,
    aggregateId: input.tokenId as string as AggregateId,
    version: CREATOR_PROFILE_UPDATED_VERSION,
    occurredAt: input.analysedAt,
    producer: input.producer,
    payload,
  };
}
