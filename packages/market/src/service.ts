import {
  Chain,
  LaunchSource,
  MarketVenue,
  Result,
  Timestamp,
  TokenId,
  failure,
  generateTokenMarketId,
  now,
  success,
} from '@nit/domain';
import { SourceTokenLaunch } from './contracts';
import { enrichmentJobsFor } from './enrichment-jobs';
import { tokenDiscoveredEvent, tokenGraduatedEvent } from './events';
import { normalizeSourceLaunch } from './normalize';
import { UnitOfWork } from './unit-of-work';

/** Which feed, on which chain, a batch of launches came from. */
export interface LaunchOrigin {
  chain: Chain;
  launchSource: LaunchSource;
}

export interface DiscoveryOutcome {
  /** `graduated` keeps the token identity and advances only its market lifecycle. */
  status: 'discovered' | 'duplicate' | 'graduated';
  tokenId: TokenId;
}

/**
 * What one poll's worth of launches did. Failures carry the address rather than a
 * token id, because a launch that could not be normalized never got one.
 */
export interface DiscoveryBatchOutcome {
  discovered: TokenId[];
  graduated: TokenId[];
  duplicates: TokenId[];
  failures: Array<{ address: string; error: Error }>;
}

export interface MarketDiscoveryOptions {
  /** Recorded on every event as its origin. */
  producer: string;
  /** Injected for deterministic tests; defaults to wall clock. */
  clock?: () => Timestamp;
}

/**
 * Normalizes launches, persists them, and announces them.
 *
 * The discovery half of TIS ch.9's pipeline: read a feed, persist a canonical
 * token, emit the event the enrichment workers wake on. It performs no narrative
 * matching (EDS ch.21 §3) and does not wait on enrichment — a token is committed
 * and searchable the moment it is discovered, with artwork, liquidity and creator
 * data arriving afterwards and independently (§5, §6).
 *
 * It also holds no reference to social intelligence. EDS ch.21 §2 requires the two
 * to operate independently, and the reason is ordering: a token frequently exists
 * before any narrative mentions it, and a narrative frequently exists before any
 * token is launched for it. Either engine waiting on the other would deadlock on
 * the common case.
 */
export class MarketDiscoveryService {
  private readonly clock: () => Timestamp;

  constructor(
    private readonly unitOfWork: UnitOfWork,
    private readonly options: MarketDiscoveryOptions
  ) {
    this.clock = options.clock ?? now;
  }

  /**
   * Discover one token in a single transaction.
   *
   * One token is one aggregate, which is why a batch is not one transaction
   * (TIS ch.2 §7). It also means a malformed launch in a batch cannot roll back
   * the good ones.
   */
  async discover(
    origin: LaunchOrigin,
    launch: SourceTokenLaunch
  ): Promise<Result<DiscoveryOutcome, Error>> {
    const normalized = normalizeSourceLaunch(
      origin.chain,
      origin.launchSource,
      launch,
      this.clock()
    );
    if (normalized.kind === 'failure') {
      return failure(normalized.error);
    }
    const token = normalized.value;

    return this.unitOfWork.transact(async ({ tokens, markets, events, jobs }) => {
      const inserted = await tokens.insert(token);
      if (inserted.kind === 'failure') {
        return failure(inserted.error);
      }

      // Already stored. A PumpSwap sighting can be the lifecycle transition of a
      // pump.fun token; an ordinary overlap remains a no-op.
      // the transaction that first inserted the row, so a second one would be a
      // duplicate announcement of a single fact — and would re-enqueue every
      // enrichment for a token that has already been through them. Committing an
      // empty transaction is correct: nothing was written and nothing is undone.
      if (!inserted.value.inserted) {
        if (origin.launchSource === LaunchSource.PUMPSWAP) {
          const current = await markets.findCurrent(inserted.value.tokenId);
          if (current.kind === 'failure') {
            return failure(current.error);
          }
          if (current.value?.venue === MarketVenue.PUMP_FUN_BONDING_CURVE) {
            const transitioned = await markets.transition({
              tokenMarketId: generateTokenMarketId(),
              tokenId: inserted.value.tokenId,
              venue: MarketVenue.PUMPSWAP,
              ...(token.market === undefined ? {} : { marketAddress: token.market.marketAddress }),
              activeFrom: token.launchTime,
              origin: token.market?.origin ?? {
                kind: 'legacy',
                key: `graduation:${inserted.value.tokenId}:${token.launchTime.toISOString()}`,
              },
              provenance: {
                adapterContract: 'source-token-launch-v1',
                launchSource: token.launchSource,
              },
            });
            if (transitioned.kind === 'failure') {
              return failure(transitioned.error);
            }
            if (transitioned.value.inserted) {
              const graduated = await events.append(
                tokenGraduatedEvent({
                  tokenId: inserted.value.tokenId,
                  ...(token.market === undefined
                    ? {}
                    : { marketAddress: token.market.marketAddress }),
                  graduatedAt: token.launchTime,
                  producer: this.options.producer,
                })
              );
              if (graduated.kind === 'failure') {
                return failure(graduated.error);
              }
            }
            return success({ status: 'graduated', tokenId: inserted.value.tokenId });
          }
        }
        return success<DiscoveryOutcome>({
          status: 'duplicate',
          tokenId: inserted.value.tokenId,
        });
      }

      const venue =
        token.launchSource === LaunchSource.PUMP_FUN
          ? MarketVenue.PUMP_FUN_BONDING_CURVE
          : MarketVenue.PUMPSWAP;
      const market = await markets.open({
        tokenMarketId: generateTokenMarketId(),
        tokenId: inserted.value.tokenId,
        venue,
        ...(token.market === undefined ? {} : { marketAddress: token.market.marketAddress }),
        activeFrom: token.launchTime,
        origin: token.market?.origin ?? {
          kind: 'legacy',
          key: `discovery:${inserted.value.tokenId}`,
        },
        provenance: {
          adapterContract: 'source-token-launch-v1',
          launchSource: token.launchSource,
        },
      });
      if (market.kind === 'failure') {
        return failure(market.error);
      }

      const appended = await events.append(
        tokenDiscoveredEvent({
          tokenId: inserted.value.tokenId,
          chain: token.chain,
          address: token.address,
          launchSource: token.launchSource,
          launchTime: token.launchTime,
          discoveredAt: token.discoveredAt,
          hasArtwork: token.artworkUrl !== undefined,
          hasCreatorWallet: token.creatorWallet !== undefined,
          producer: this.options.producer,
        })
      );
      if (appended.kind === 'failure') {
        return failure(appended.error);
      }

      // The outbox. Enqueued here rather than by something reading the event log,
      // because a dispatcher would need a durable cursor over that log and a process
      // to advance it, and until both existed a token could commit with no jobs. In
      // the transaction, "no token without its enrichment jobs" is not a guarantee
      // anything has to maintain.
      //
      // Sequential, not concurrent: these run on the one `PoolClient` this
      // transaction owns, and a client cannot have two statements in flight. The cost
      // is three round trips on the discovery path, which is the price of the
      // guarantee above.
      for (const request of enrichmentJobsFor(token)) {
        const enqueued = await jobs.enqueue(request);
        if (enqueued.kind === 'failure') {
          return failure(enqueued.error);
        }
      }

      return success<DiscoveryOutcome>({
        status: 'discovered',
        tokenId: inserted.value.tokenId,
      });
    });
  }

  /**
   * Discover a poll's worth of launches, oldest first.
   *
   * Sorting matters for more than tidiness: the event log is what replay reads,
   * and announcing a token that launched at noon before one that launched at dawn
   * would make the log describe a sequence that never happened. Launch time is the
   * launchpad's claim about ordering and the only one available, with the address
   * as a tie-break so two mints in the same millisecond still order
   * deterministically.
   *
   * Every launch is attempted even after one fails: a single malformed entry must
   * not cost the rest of the window, because the next poll's window will have moved
   * past it and nothing would go back for the ones that were skipped.
   */
  async discoverBatch(
    origin: LaunchOrigin,
    launches: readonly SourceTokenLaunch[]
  ): Promise<DiscoveryBatchOutcome> {
    const ordered = [...launches].sort((left, right) => {
      const byTime = left.launchTime.getTime() - right.launchTime.getTime();
      return byTime !== 0 ? byTime : left.address.localeCompare(right.address);
    });

    const batch: DiscoveryBatchOutcome = {
      discovered: [],
      graduated: [],
      duplicates: [],
      failures: [],
    };

    for (const launch of ordered) {
      const result = await this.discover(origin, launch);
      if (result.kind === 'failure') {
        batch.failures.push({ address: launch.address, error: result.error });
      } else if (result.value.status === 'discovered') {
        batch.discovered.push(result.value.tokenId);
      } else if (result.value.status === 'graduated') {
        batch.graduated.push(result.value.tokenId);
      } else {
        batch.duplicates.push(result.value.tokenId);
      }
    }

    return batch;
  }
}
