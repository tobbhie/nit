import { Chain, LaunchSource, Result, Timestamp, failure, success } from '@nit/domain';
import { LaunchFetchRequest, LaunchSourceAdapter, SourceTokenLaunch } from './contracts';

/**
 * Launch time of the first token in the synthetic corpus. A fixed calendar
 * constant rather than a clock read, so two runs of the same test see the same
 * launches.
 */
const SYNTHETIC_EPOCH_MS = Date.UTC(2026, 0, 1);

const DEFAULT_LAUNCH_COUNT = 40;
const DEFAULT_INTERVAL_MS = 30_000;
const DEFAULT_LIMIT = 50;

/** The length of a base58-encoded Solana mint address. */
const ADDRESS_LENGTH = 44;

/**
 * Tickers cycle rather than being unique per token, because tickers are not
 * unique on a launchpad — a dozen mints called PEPE is the normal case, and it is
 * exactly the case the candidate-index search has to handle. Mixed case is
 * deliberate for the same reason as the addresses below.
 */
const TICKERS = ['PEPE', 'WIF', 'BONK', 'MOG', 'Turbo', 'giga'] as const;

export interface SyntheticLaunchSourceOptions {
  /** Size of the fabricated corpus. */
  launchCount?: number;
  /** Launch time of the first token. */
  epochMs?: number;
  /** Gap between consecutive launches. */
  intervalMs?: number;
}

/**
 * A source of fabricated token launches.
 *
 * Its purpose is to let the whole discovery path — fetch, normalize, persist,
 * announce — run and be tested with no launchpad credential and no RPC endpoint
 * present, which is what makes market intelligence developable in an environment
 * that holds no real ones.
 *
 * The corpus is a pure function of the launch index, so it is deterministic: the
 * same window returns the same launches on every run, in every process, which is
 * what replay and backtest tests need. It deliberately covers the shapes the
 * pipeline branches on — launches with and without artwork, with and without a
 * creator wallet, with and without a description — and it includes pairs of mints
 * sharing a launch time, because that is ordinary on a launchpad and it is the
 * case that forces the discovery service's tie-break to be deterministic rather
 * than incidentally correct.
 *
 * Every string it emits says it is fabricated. `isToyImplementation` is true, so
 * the composition-root guard refuses to start production with this bound.
 */
export class SyntheticLaunchSource implements LaunchSourceAdapter {
  readonly providerName = 'synthetic-launch-source';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1.0.0';
  readonly chain: Chain;
  readonly launchSource: LaunchSource;

  private readonly launchCount: number;
  private readonly epochMs: number;
  private readonly intervalMs: number;
  /** Fetches still owed a simulated failure. See `simulateOutage`. */
  private outageRemaining = 0;
  private fetches = 0;

  constructor(
    chain: Chain = Chain.SOLANA,
    launchSource: LaunchSource = LaunchSource.PUMP_FUN,
    options: SyntheticLaunchSourceOptions = {}
  ) {
    this.chain = chain;
    this.launchSource = launchSource;
    this.launchCount = options.launchCount ?? DEFAULT_LAUNCH_COUNT;
    this.epochMs = options.epochMs ?? SYNTHETIC_EPOCH_MS;
    this.intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
  }

  /**
   * Make the next `count` fetches report failure.
   *
   * A launchpad outage is the one condition a caller cannot provoke from the
   * outside, and the poll handler's retry path exists entirely for it. A real
   * adapter has no such control surface and needs none; this one stands in for a
   * launchpad, so standing in for a broken launchpad is part of the job.
   */
  simulateOutage(count = 1): void {
    this.outageRemaining = Math.max(0, count);
  }

  /** Fetch attempts made, including the ones an outage failed. */
  fetchCount(): number {
    return this.fetches;
  }

  /**
   * The launches in `request`'s window.
   *
   * There is no cursor to honour, unlike `SyntheticObservationSource`: the window
   * is a timestamp, so the filter is `launchTime >= since` and re-polling an
   * overlapping window legitimately returns launches the caller already has. That
   * is what the unique constraint on `(chain, address)` absorbs.
   */
  fetchRecentLaunches(request: LaunchFetchRequest): Promise<Result<SourceTokenLaunch[], Error>> {
    this.fetches += 1;

    if (this.outageRemaining > 0) {
      this.outageRemaining -= 1;
      return Promise.resolve(
        failure(
          new Error(
            `Synthetic launchpad outage on ${this.chain}/${this.launchSource} (simulated, not a real failure)`
          )
        )
      );
    }

    const limit = request.limit > 0 ? request.limit : DEFAULT_LIMIT;
    const selected: SourceTokenLaunch[] = [];
    for (let index = 0; index < this.launchCount && selected.length < limit; index += 1) {
      const launch = this.launch(index);
      // The window is inclusive of its lower bound. A caller polling with an
      // overlap wants the launch on the boundary rather than one fewer than it
      // asked for, and an exclusive bound would drop a mint that shares its
      // launch time with the last one seen.
      if (request.since !== undefined && launch.launchTime.getTime() < request.since.getTime()) {
        continue;
      }
      selected.push(launch);
    }

    // Selected oldest-first so a capped poll takes the oldest launches in the
    // window and therefore skips nothing; returned newest-first because that is
    // the order a launchpad answers in, which keeps the discovery service's own
    // ordering honest rather than accidentally satisfied by the adapter.
    return Promise.resolve(success(selected.reverse()));
  }

  /**
   * One fabricated launch.
   *
   * The shape is a function of the index alone, which is what makes the corpus
   * reproducible: every third mint carries artwork, every fourth names a creator
   * wallet, every fifth carries a description, and every eighth shares its
   * predecessor's launch time. Index 0 satisfies all of them, so the first launch
   * in the corpus is the fully-populated one.
   */
  private launch(index: number): SourceTokenLaunch {
    const address = this.address(index);
    const ticker = TICKERS[index % TICKERS.length] as string;
    // Same-instant pairs: an index one past a multiple of eight reuses its
    // predecessor's slot, so the corpus contains pairs of mints the launchpad
    // cannot order between. Slots stay non-decreasing in the index, which is what
    // lets the fetch loop treat index order as launch order.
    const slot = index - (index % 8 === 1 ? 1 : 0);
    const launchTime: Timestamp = new Date(this.epochMs + slot * this.intervalMs);

    const launch: SourceTokenLaunch = {
      address,
      ticker,
      name: `${ticker} Coin ${index + 1} (synthetic)`,
      launchTime,
      raw: {
        mint: address,
        symbol: ticker,
        created_timestamp: launchTime.getTime(),
        launchpad: this.launchSource,
        synthetic: true,
        note: 'Fabricated by SyntheticLaunchSource. Not real launchpad data.',
      },
    };
    if (index % 3 === 0) {
      launch.artworkUrl = `https://synthetic.invalid/${address}/artwork.png`;
    }
    if (index % 4 === 0) {
      launch.creatorWallet = this.creatorWallet(index);
    }
    if (index % 5 === 0) {
      launch.description = `Fabricated launch ${index} on ${this.launchSource}. Not a real token.`;
    }
    return launch;
  }

  /**
   * A mint address for the launch at `index`.
   *
   * Mixed case on purpose: base58 is case-sensitive, so an all-lowercase corpus
   * would let a case-folding bug through unnoticed. Padded to the length of a real
   * base58 address, and unique per index and per launchpad so two adapters bound
   * in one process do not fabricate the same mint and have the database absorb the
   * second as a duplicate of the first.
   */
  private address(index: number): string {
    const stem = `Syn${this.launchSource === LaunchSource.PUMP_FUN ? 'F' : 'S'}${String(
      index + 1
    ).padStart(4, '1')}Aa`;
    return stem.padEnd(ADDRESS_LENGTH, '1');
  }

  /**
   * A creator wallet, reused across every fourth launch.
   *
   * Deliberately not unique per token: creator reputation is only meaningful for a
   * wallet that has launched before, so a corpus of one-launch creators would make
   * the creator enrichment path untestable.
   */
  private creatorWallet(index: number): string {
    const stem = `SynDev${String((index % 12) + 1).padStart(2, '1')}Bb`;
    return stem.padEnd(ADDRESS_LENGTH, '1');
  }
}
