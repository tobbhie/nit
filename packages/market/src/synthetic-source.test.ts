import { describe, expect, it } from 'vitest';
import { Chain, LaunchSource, Timestamp } from '@nit/domain';
import { SourceTokenLaunch } from './contracts';
import { normalizeSourceLaunch } from './normalize';
import { SyntheticLaunchSource } from './synthetic-source';

const DISCOVERED_AT: Timestamp = new Date('2026-08-20T09:00:06.000Z');

/** Fetch a window and fail the test if the launchpad reported an error. */
async function fetched(
  source: SyntheticLaunchSource,
  request: { since?: Timestamp; limit: number }
): Promise<SourceTokenLaunch[]> {
  const result = await source.fetchRecentLaunches(request);
  if (result.kind === 'failure') {
    throw result.error;
  }
  return result.value;
}

/** The first ten characters of each address: enough to identify the launch. */
function stems(launches: readonly SourceTokenLaunch[]): string[] {
  return launches.map((launch) => launch.address.slice(0, 10));
}

describe('SyntheticLaunchSource identity', () => {
  it('declares itself a toy so the production guard rejects it', () => {
    const source = new SyntheticLaunchSource();

    expect(source.isToyImplementation).toBe(true);
    expect(source.providerName).toBe('synthetic-launch-source');
    expect(source.interfaceVersion).toBe('1.0.0');
  });

  it('reports the feed it stands in for', () => {
    // Provenance lives on the adapter, not on the fetch, so a caller cannot
    // mislabel where a launch came from.
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMPSWAP);

    expect(source.chain).toBe(Chain.SOLANA);
    expect(source.launchSource).toBe(LaunchSource.PUMPSWAP);
  });

  it('defaults to the pump.fun feed on Solana', () => {
    const source = new SyntheticLaunchSource();

    expect(source.chain).toBe(Chain.SOLANA);
    expect(source.launchSource).toBe(LaunchSource.PUMP_FUN);
  });
});

describe('SyntheticLaunchSource corpus', () => {
  it('fabricates the same launches on every run', async () => {
    // The whole point of a synthetic source: a replay or backtest test that saw a
    // different corpus on its second run would be untrustworthy in both.
    const first = await fetched(new SyntheticLaunchSource(), { limit: 10 });
    const second = await fetched(new SyntheticLaunchSource(), { limit: 10 });

    expect(second).toEqual(first);
  });

  it('produces addresses shaped like real mint addresses', async () => {
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 10 });

    for (const launch of launches) {
      expect(launch.address).toHaveLength(44);
      // Mixed case, because base58 is case-sensitive and an all-lowercase corpus
      // would let a case-folding bug through unnoticed.
      expect(launch.address).toMatch(/[a-z]/);
      expect(launch.address).toMatch(/[A-Z]/);
    }
    expect(new Set(launches.map((launch) => launch.address)).size).toBe(launches.length);
  });

  it('fabricates different mints for different feeds', async () => {
    // Two adapters bound in one process must not fabricate the same mint: identity
    // is (chain, address), so the second feed's launches would be absorbed as
    // duplicates of the first's and the second adapter would look broken.
    const pumpFun = await fetched(
      new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN),
      { limit: 5 }
    );
    const pumpSwap = await fetched(
      new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMPSWAP),
      { limit: 5 }
    );

    const overlap = new Set(pumpFun.map((launch) => launch.address));
    expect(pumpSwap.some((launch) => overlap.has(launch.address))).toBe(false);
  });

  it('includes a pair of mints sharing a launch time', async () => {
    // Ordinary on a launchpad, and the case that forces the discovery service's
    // address tie-break to be exercised rather than incidentally satisfied.
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 10 });
    const times = launches.map((launch) => launch.launchTime.getTime());

    expect(new Set(times).size).toBeLessThan(times.length);
  });

  it('covers the optional fields the enrichment workers branch on', async () => {
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 20 });

    // Both sides of every branch: a corpus where every launch had artwork would
    // never exercise the has-nothing-to-enrich path, and vice versa.
    expect(launches.some((launch) => launch.artworkUrl !== undefined)).toBe(true);
    expect(launches.some((launch) => launch.artworkUrl === undefined)).toBe(true);
    expect(launches.some((launch) => launch.creatorWallet !== undefined)).toBe(true);
    expect(launches.some((launch) => launch.creatorWallet === undefined)).toBe(true);
    expect(launches.some((launch) => launch.description !== undefined)).toBe(true);
    expect(launches.some((launch) => launch.description === undefined)).toBe(true);
  });

  it('reuses tickers across launches', async () => {
    // Tickers are not unique on a launchpad. A corpus of unique tickers would make
    // the candidate-index search look exact when it is not.
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 20 });
    const tickers = launches.map((launch) => launch.ticker);

    expect(new Set(tickers).size).toBeLessThan(tickers.length);
  });

  it('reuses creator wallets across launches', async () => {
    // Creator reputation is only meaningful for a wallet that has launched before,
    // so a corpus of one-launch creators would make that path untestable.
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 40 });
    const wallets = launches
      .map((launch) => launch.creatorWallet)
      .filter((wallet): wallet is string => wallet !== undefined);

    expect(wallets.length).toBeGreaterThan(1);
    expect(new Set(wallets).size).toBeLessThan(wallets.length);
  });

  it('marks the raw payload as fabricated', async () => {
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 1 });
    const launch = launches[0] as SourceTokenLaunch;

    expect(launch.raw).toMatchObject({ mint: launch.address, synthetic: true });
    const note = launch.raw['note'];
    expect(typeof note).toBe('string');
    expect(note as string).toContain('Not real launchpad data');
  });

  it('emits launches the canonical model accepts', async () => {
    // The corpus is only useful if the pipeline can actually swallow it. Without
    // this, a fixture that fails validation would show up as a mysterious
    // "rejected" count in every downstream test instead of as a broken fixture.
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 40 });

    for (const launch of launches) {
      const normalized = normalizeSourceLaunch(
        Chain.SOLANA,
        LaunchSource.PUMP_FUN,
        launch,
        DISCOVERED_AT
      );
      if (normalized.kind === 'failure') {
        throw new Error(`Synthetic launch ${launch.address} was rejected: ${normalized.error.message}`);
      }
    }
  });
});

describe('SyntheticLaunchSource.fetchRecentLaunches', () => {
  it('answers newest first', async () => {
    // The order a launchpad answers in. The discovery service re-sorts to
    // oldest-first for the event log, and it has to be the thing that does it.
    const launches = await fetched(new SyntheticLaunchSource(), { limit: 3 });

    expect(stems(launches)).toEqual(['SynF1113Aa', 'SynF1112Aa', 'SynF1111Aa']);
  });

  it('takes the oldest launches in the window when the limit caps it', async () => {
    // Oldest rather than newest: a capped poll that returned the newest would skip
    // everything between the window's start and what it returned, and the next
    // poll's window starts later still, so nothing would ever go back for them.
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 12,
    });
    const launches = await fetched(source, { limit: 3 });

    expect(stems(launches)).toEqual(['SynF1113Aa', 'SynF1112Aa', 'SynF1111Aa']);
  });

  it('returns only launches at or after the window start', async () => {
    const epochMs = Date.UTC(2026, 5, 1);
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 6,
      epochMs,
      intervalMs: 60_000,
    });
    // Slots run 0, 0, 2, 3, 4, 5 minutes, so a window opening at minute three
    // leaves the three launches at minutes three, four and five.
    const launches = await fetched(source, { since: new Date(epochMs + 3 * 60_000), limit: 10 });

    expect(launches).toHaveLength(3);
    for (const launch of launches) {
      expect(launch.launchTime.getTime()).toBeGreaterThanOrEqual(epochMs + 3 * 60_000);
    }
  });

  it('includes a launch sitting exactly on the window start', async () => {
    // The bound is inclusive: a caller polling with an overlap wants the launch on
    // the boundary, and an exclusive bound would drop a mint that shares its launch
    // time with the last one already seen.
    const epochMs = Date.UTC(2026, 5, 1);
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 4,
      epochMs,
      intervalMs: 60_000,
    });
    const launches = await fetched(source, { since: new Date(epochMs), limit: 10 });

    expect(launches).toHaveLength(4);
  });

  it('reports a quiet window as empty rather than as a failure', async () => {
    const epochMs = Date.UTC(2026, 5, 1);
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 4,
      epochMs,
      intervalMs: 60_000,
    });
    const launches = await fetched(source, { since: new Date(epochMs + 60 * 60_000), limit: 10 });

    expect(launches).toEqual([]);
  });

  it('falls back to a default limit rather than fetching nothing', async () => {
    // A limit of zero is a caller bug, and honouring it literally would look
    // exactly like a launchpad with nothing to report.
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 3,
    });
    const launches = await fetched(source, { limit: 0 });

    expect(launches).toHaveLength(3);
  });
});

describe('SyntheticLaunchSource.simulateOutage', () => {
  it('fails the next fetch and then recovers', async () => {
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 2,
    });
    source.simulateOutage();

    const down = await source.fetchRecentLaunches({ limit: 10 });
    expect(down.kind).toBe('failure');
    if (down.kind === 'failure') {
      // The message has to say it is simulated, or an operator reading a test log
      // spends an afternoon on a launchpad that was never down.
      expect(down.error.message).toContain('simulated');
    }

    const up = await source.fetchRecentLaunches({ limit: 10 });
    expect(up.kind).toBe('success');
  });

  it('fails as many fetches as it was asked to', async () => {
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 2,
    });
    source.simulateOutage(2);

    expect((await source.fetchRecentLaunches({ limit: 10 })).kind).toBe('failure');
    expect((await source.fetchRecentLaunches({ limit: 10 })).kind).toBe('failure');
    expect((await source.fetchRecentLaunches({ limit: 10 })).kind).toBe('success');
  });

  it('counts the attempts an outage failed', async () => {
    // A retry test asserts the poll was attempted again, and an attempt that
    // failed is still an attempt.
    const source = new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      launchCount: 2,
    });
    source.simulateOutage(1);

    await source.fetchRecentLaunches({ limit: 10 });
    await source.fetchRecentLaunches({ limit: 10 });

    expect(source.fetchCount()).toBe(2);
  });
});
