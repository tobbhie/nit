import {
  Chain,
  LaunchSource,
  QueueName,
  Result,
  Timestamp,
  ValidationError,
  failure,
  now,
  success,
} from '@nit/domain';
import { Job, JobHandler, PermanentJobFailure } from '@nit/queue';
import { LaunchFetchRequest, LaunchSourceAdapter } from './contracts';
import { MarketDiscoveryService } from './service';

export const LAUNCH_POLL_PAYLOAD_VERSION = '1';

const DEFAULT_POLL_LIMIT = 50;

/**
 * How far back a poll looks when the payload does not say.
 *
 * Generous relative to any sane poll interval, because the cost of overlap is a
 * re-fetch the unique constraint absorbs and the cost of a gap is a token nobody
 * ever discovers. It also has to cover launchpad indexing lag: a mint is queryable
 * some seconds after it exists, so a window ending at the previous poll's start
 * would miss whatever was still being indexed then.
 */
const DEFAULT_LOOKBACK_MS = 15 * 60_000;

const CHAINS = new Set<string>(Object.values(Chain));
const LAUNCH_SOURCES = new Set<string>(Object.values(LaunchSource));

/**
 * One launchpad feed to poll.
 *
 * `since` is an ISO 8601 string rather than a `Timestamp` because a payload is
 * persisted as JSONB and read back by `pg`, which returns a string — a `Date` here
 * would typecheck at the enqueue site and arrive as something else. It is optional
 * because the handler derives its own window; supplying one is how an operator
 * asks for a specific window after an outage longer than the lookback.
 */
export interface LaunchPollPayload {
  chain: Chain;
  launchSource: LaunchSource;
  since?: string;
  limit?: number;
}

/** What one poll did. Enough to log or alert on without re-querying. */
export interface LaunchPollSummary {
  chain: Chain;
  launchSource: LaunchSource;
  /** The adapter that answered, so a synthetic poll is distinguishable in a log. */
  provider: string;
  /**
   * The window actually used, as ISO 8601. Never null, unlike the cursor in
   * `@nit/observation`'s `PollSummary`: a clock-derived window always exists,
   * where a cursor does not until something has been ingested.
   */
  since: string;
  fetched: number;
  discovered: number;
  duplicates: number;
  /** Launches the canonical model refused. See `handle` on why these do not retry. */
  rejected: number;
}

export interface LaunchPollOptions {
  limit?: number;
  /** Overrides `DEFAULT_LOOKBACK_MS`. Should exceed the scheduled poll interval. */
  lookbackMs?: number;
  /** Injected for deterministic tests; defaults to wall clock. */
  clock?: () => Timestamp;
  /** Called after a successful poll so the caller can log the summary. */
  onPolled?: (summary: LaunchPollSummary) => void;
}

/**
 * Polls one launchpad feed per job.
 *
 * The window is a lookback from the clock, not a resume point read from the
 * database, and that is the substantive difference from
 * `ObservationPollHandler.resolveCursor`. Deriving it from the newest stored token
 * looks symmetrical and is wrong here: `tokens.launch_source` records which feed
 * surfaced a mint *first*, so a graduation venue like PumpSwap — where nearly every
 * token was already stored under `pump_fun` — would find no rows of its own, fall
 * back to the beginning of the feed on every poll, and never advance. A clock
 * window cannot fail to make progress.
 *
 * What it gives up is the gap after an outage longer than the lookback. That is a
 * real limitation, deliberately taken in exchange for one that cannot be detected
 * from inside the handler, and the payload's `since` is the remedy.
 *
 * Per-feed rather than per-chain because the durable queue's retry and backoff are
 * per job: one launchpad whose fetch keeps failing must not delay the others behind
 * it. The handler is idempotent, as the queue requires — a redelivered poll
 * re-fetches the same launches and discovery absorbs them as duplicates on
 * `(chain, address)` (EDS ch.21 §8).
 *
 * It does not schedule itself, for the same reason the observation handler does
 * not: a handler that enqueued its own successor would lose the chain the first
 * time a process died between committing the discovery and committing the next job,
 * and polling would then stop for that feed permanently with nothing to report it.
 */
export class LaunchPollHandler implements JobHandler<LaunchPollPayload> {
  readonly queueName: string = QueueName.TOKEN_DISCOVERY_POLL;

  private readonly clock: () => Timestamp;

  constructor(
    private readonly sources: ReadonlyMap<LaunchSource, LaunchSourceAdapter>,
    private readonly discovery: MarketDiscoveryService,
    private readonly options: LaunchPollOptions = {}
  ) {
    this.clock = options.clock ?? now;
  }

  /**
   * Run a poll for the queue.
   *
   * Throwing is how a worker learns to retry, so what does and does not throw here
   * is the whole of this handler's failure policy:
   *
   * - A malformed payload, an unknown feed, or an unbound adapter throws
   *   `PermanentJobFailure`. No number of retries fixes any of them, and the
   *   dead-letter queue is where an operator will actually see it.
   * - A failed fetch throws an ordinary error, so the queue retries it with
   *   backoff. This is the launchpad-outage path.
   * - Launches the canonical model rejected do not throw. The rest of the window is
   *   already committed, so retrying would re-discover good launches as duplicates
   *   and fail on the same bad mint until the job dead-letters, costing the feed its
   *   poll over one malformed entry. They are counted in the summary instead.
   */
  async handle(job: Job<LaunchPollPayload>): Promise<void> {
    if (job.payloadVersion !== LAUNCH_POLL_PAYLOAD_VERSION) {
      throw new PermanentJobFailure(
        `Unsupported launch poll payload version '${job.payloadVersion}'`
      );
    }

    const result = await this.poll(job.payload);
    if (result.kind === 'failure') {
      throw result.error;
    }
    this.options.onPolled?.(result.value);
  }

  /**
   * Fetch and discover one feed's recent launches.
   *
   * Separate from `handle` so the poll can be exercised without a job envelope, and
   * so the summary is a return value rather than something only a log sees.
   */
  async poll(payload: LaunchPollPayload): Promise<Result<LaunchPollSummary, Error>> {
    const source = this.resolveSource(payload);
    const since = this.resolveSince(payload.since);

    const request: LaunchFetchRequest = {
      since,
      limit: payload.limit ?? this.options.limit ?? DEFAULT_POLL_LIMIT,
    };

    const fetched = await source.fetchRecentLaunches(request);
    if (fetched.kind === 'failure') {
      return failure(fetched.error);
    }

    // Provenance comes from the adapter rather than the payload. They agree —
    // `resolveSource` refuses a mismatch — but the adapter is the one that read the
    // feed, and mislabelling where a launch came from is a token-identity fault
    // rather than a reporting one.
    const batch = await this.discovery.discoverBatch(
      { chain: source.chain, launchSource: source.launchSource },
      fetched.value
    );

    // A failure that is not a ValidationError did not come from the launch: it came
    // from the database or the event store. Reporting those as "rejected launches"
    // would turn an outage into a clean poll that quietly discovered nothing, so
    // they are surfaced as a retryable failure instead.
    const infrastructure = batch.failures.filter(
      (entry) => !(entry.error instanceof ValidationError)
    );
    if (infrastructure.length > 0) {
      const first = infrastructure[0];
      return failure(
        new Error(
          `Discovery failed for ${infrastructure.length} of ${fetched.value.length} launches from ${source.chain}/${source.launchSource}: ${first?.error.message ?? 'unknown error'}`
        )
      );
    }

    return success({
      chain: source.chain,
      launchSource: source.launchSource,
      provider: source.providerName,
      since: since.toISOString(),
      fetched: fetched.value.length,
      discovered: batch.discovered.length,
      duplicates: batch.duplicates.length,
      rejected: batch.failures.length,
    });
  }

  /**
   * The adapter for the feed named in the payload.
   *
   * Keyed by launch source, then checked against the payload's chain. The check is
   * not redundant with the key: one launchpad brand can run on more than one chain,
   * and a composition root that bound the wrong one would otherwise persist real
   * mints under a chain they do not exist on — and since identity is
   * `(chain, address)`, that forks the token rather than failing.
   */
  private resolveSource(payload: LaunchPollPayload): LaunchSourceAdapter {
    if (!CHAINS.has(payload.chain)) {
      throw new PermanentJobFailure(`Unknown chain '${payload.chain}'`);
    }
    if (!LAUNCH_SOURCES.has(payload.launchSource)) {
      throw new PermanentJobFailure(`Unknown launch source '${payload.launchSource}'`);
    }

    const source = this.sources.get(payload.launchSource);
    if (source === undefined) {
      // A composition error, not a runtime one: this process has no adapter for the
      // feed and will not grow one by waiting.
      throw new PermanentJobFailure(
        `No launch source bound for '${payload.launchSource}'`
      );
    }
    // Compared as a string, not as the enum. `Chain` has one member today, so
    // TypeScript narrows the mismatch branch to `never` and the message below
    // becomes uncheckable — while the branch stays reachable at runtime, because an
    // adapter's `chain` is whatever it was constructed with. Widening keeps the
    // guard honest now and correct when the enum grows.
    const boundChain: string = source.chain;
    const wantedChain: string = payload.chain;
    if (boundChain !== wantedChain) {
      throw new PermanentJobFailure(
        `Launch source '${payload.launchSource}' is bound to chain '${boundChain}', not '${wantedChain}'`
      );
    }
    return source;
  }

  /**
   * The start of the window to poll.
   *
   * A supplied `since` is parsed rather than trusted: it crosses a JSONB boundary
   * as a string, and `new Date('nonsense')` is a `Date`, so only a value check
   * catches it. An unparseable one is permanent — the same string will not parse on
   * a retry.
   */
  private resolveSince(supplied: string | undefined): Timestamp {
    if (supplied === undefined) {
      return new Date(this.clock().getTime() - (this.options.lookbackMs ?? DEFAULT_LOOKBACK_MS));
    }

    const parsed = new Date(supplied);
    if (Number.isNaN(parsed.getTime())) {
      throw new PermanentJobFailure(`Launch poll payload has an unparseable since '${supplied}'`);
    }
    return parsed;
  }
}
