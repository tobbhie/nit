import { describe, expect, it } from 'vitest';
import {
  Chain,
  JobId,
  JobStatus,
  LaunchSource,
  Result,
  Timestamp,
  failure,
  success,
} from '@nit/domain';
import { Job, PermanentJobFailure } from '@nit/queue';
import { LaunchSourceAdapter, SourceTokenLaunch } from './contracts';
import { FakeUnitOfWork, StubLaunchSource, launch } from './in-memory.fixtures';
import {
  LAUNCH_POLL_PAYLOAD_VERSION,
  LaunchPollHandler,
  LaunchPollOptions,
  LaunchPollPayload,
  LaunchPollSummary,
} from './poll-handler';
import { MarketDiscoveryService } from './service';
import { SyntheticLaunchSource } from './synthetic-source';

/** When every poll in this file happens. */
const POLL_AT: Timestamp = new Date('2026-08-20T10:00:00.000Z');

/** Where the synthetic corpus sits: inside the default lookback from `POLL_AT`. */
const CORPUS_EPOCH_MS = POLL_AT.getTime() - 5 * 60_000;

/**
 * A synthetic feed whose launches fall inside a window derived from `POLL_AT`.
 *
 * The adapter's own default epoch is January, which is correct for a fixture and
 * useless here: a poll with a fifteen-minute lookback would find an empty feed and
 * every assertion below would pass for the wrong reason.
 */
function synthetic(launchCount = 4): SyntheticLaunchSource {
  return new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, {
    launchCount,
    epochMs: CORPUS_EPOCH_MS,
    intervalMs: 30_000,
  });
}

function sourceMap(...adapters: LaunchSourceAdapter[]): Map<LaunchSource, LaunchSourceAdapter> {
  return new Map(adapters.map((adapter) => [adapter.launchSource, adapter]));
}

interface Harness {
  handler: LaunchPollHandler;
  uow: FakeUnitOfWork;
  polled: LaunchPollSummary[];
}

function harnessFor(
  adapters: LaunchSourceAdapter[],
  options: { lookbackMs?: number; limit?: number; appendFails?: boolean } = {}
): Harness {
  const uow = new FakeUnitOfWork(options.appendFails ?? false);
  const polled: LaunchPollSummary[] = [];
  const discovery = new MarketDiscoveryService(uow, {
    producer: 'launch-poll-test',
    clock: () => POLL_AT,
  });
  // Built field by field rather than spread: under `exactOptionalPropertyTypes` a
  // conditional spread widens the optional fields to `| undefined`, which is not
  // assignable back to them.
  const pollOptions: LaunchPollOptions = {
    clock: () => POLL_AT,
    onPolled: (summary) => polled.push(summary),
  };
  if (options.lookbackMs !== undefined) {
    pollOptions.lookbackMs = options.lookbackMs;
  }
  if (options.limit !== undefined) {
    pollOptions.limit = options.limit;
  }
  const handler = new LaunchPollHandler(sourceMap(...adapters), discovery, pollOptions);
  return { handler, uow, polled };
}

/** Poll and fail the test if it reported an error. */
async function poll(
  handler: LaunchPollHandler,
  payload: LaunchPollPayload
): Promise<LaunchPollSummary> {
  const result = await handler.poll(payload);
  if (result.kind === 'failure') {
    throw result.error;
  }
  return result.value;
}

const PUMP_FUN_POLL: LaunchPollPayload = {
  chain: Chain.SOLANA,
  launchSource: LaunchSource.PUMP_FUN,
};

/**
 * A job envelope around a payload.
 *
 * Only the two fields the handler reads carry meaning; the rest exist because the
 * queue's `Job` describes a durable row and the handler is typed against all of it.
 */
function jobFor(
  payload: LaunchPollPayload,
  payloadVersion: string = LAUNCH_POLL_PAYLOAD_VERSION
): Job<LaunchPollPayload> {
  return {
    jobId: 'job-1' as JobId,
    queueName: 'token_discovery_poll',
    payload,
    payloadVersion,
    status: JobStatus.RUNNING,
    attemptCount: 1,
    maxAttempts: 5,
    leaseDeadline: POLL_AT,
    leasedBy: 'worker-1',
    idempotencyKey: 'token_discovery_poll:solana:pump_fun',
    availableAt: POLL_AT,
    lastError: null,
    correlationId: null,
    createdAt: POLL_AT,
    updatedAt: POLL_AT,
    completedAt: null,
  };
}

describe('LaunchPollHandler.poll', () => {
  it('discovers the launches in the window and reports what it did', async () => {
    const { handler, uow } = harnessFor([synthetic()]);
    const summary = await poll(handler, PUMP_FUN_POLL);

    expect(summary.fetched).toBe(4);
    expect(summary.discovered).toBe(4);
    expect(summary.duplicates).toBe(0);
    expect(summary.rejected).toBe(0);
    expect(uow.store.tokens.size).toBe(4);
    expect(uow.store.events).toHaveLength(4);
  });

  it('reports a re-polled window as duplicates rather than as new launches', async () => {
    // Overlapping windows are how this handler avoids skipping launches, so
    // re-seeing them is the normal case and not a fault (EDS ch.21 §8).
    const { handler, uow } = harnessFor([synthetic()]);

    await poll(handler, PUMP_FUN_POLL);
    const second = await poll(handler, PUMP_FUN_POLL);

    expect(second.fetched).toBe(4);
    expect(second.discovered).toBe(0);
    expect(second.duplicates).toBe(4);
    expect(uow.store.tokens.size).toBe(4);
    // No second announcement, so nothing re-enqueues enrichment for tokens that
    // have already been through it.
    expect(uow.store.events).toHaveLength(4);
  });

  it('labels launches with the adapter\'s provenance', async () => {
    // The payload and the adapter agree, but the adapter is the one that read the
    // feed. Reading provenance from the payload would let a mislabelled job fork a
    // token's identity, since identity is (chain, address).
    const { handler, uow } = harnessFor([synthetic(1)]);
    await poll(handler, PUMP_FUN_POLL);

    const stored = [...uow.store.tokens.values()][0];
    expect(stored?.chain).toBe(Chain.SOLANA);
    expect(stored?.launchSource).toBe(LaunchSource.PUMP_FUN);
  });

  it('names the adapter that answered', async () => {
    // So a log makes it obvious a poll was served by a toy rather than a launchpad.
    const { handler } = harnessFor([synthetic(1)]);
    const summary = await poll(handler, PUMP_FUN_POLL);

    expect(summary.provider).toBe('synthetic-launch-source');
  });

  it('reports a quiet window as a clean poll', async () => {
    const { handler, uow } = harnessFor([
      new SyntheticLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, { launchCount: 0 }),
    ]);
    const summary = await poll(handler, PUMP_FUN_POLL);

    expect(summary.fetched).toBe(0);
    expect(summary.discovered).toBe(0);
    expect(uow.transactions).toBe(0);
  });
});

describe('LaunchPollHandler window', () => {
  it('derives the window from the clock and the lookback', async () => {
    const stub = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [success([])]);
    const { handler } = harnessFor([stub], { lookbackMs: 60_000 });

    const summary = await poll(handler, PUMP_FUN_POLL);

    expect(stub.requests[0]?.since).toEqual(new Date(POLL_AT.getTime() - 60_000));
    expect(summary.since).toBe(new Date(POLL_AT.getTime() - 60_000).toISOString());
  });

  it('looks back far enough to cover an overlapping poll by default', async () => {
    // The default has to exceed any sane poll interval plus launchpad indexing lag.
    // A window that started where the last one ended would miss whatever was still
    // being indexed at that moment, and nothing would go back for it.
    const stub = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [success([])]);
    const { handler } = harnessFor([stub]);

    await poll(handler, PUMP_FUN_POLL);

    const since = stub.requests[0]?.since;
    expect(since).toBeDefined();
    expect(POLL_AT.getTime() - (since as Timestamp).getTime()).toBeGreaterThanOrEqual(5 * 60_000);
  });

  it('uses a window the payload supplied instead of deriving one', async () => {
    // How an operator asks for a specific window after an outage longer than the
    // lookback, which is the gap a clock-derived window has.
    const stub = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [success([])]);
    const { handler } = harnessFor([stub]);

    const summary = await poll(handler, {
      ...PUMP_FUN_POLL,
      since: '2026-08-19T00:00:00.000Z',
    });

    expect(stub.requests[0]?.since).toEqual(new Date('2026-08-19T00:00:00.000Z'));
    expect(summary.since).toBe('2026-08-19T00:00:00.000Z');
  });

  it('accepts the window as a string because a payload is JSONB', async () => {
    // A Date in a payload typechecks at the enqueue site and arrives as a string,
    // so a handler that expected a Date would fail only in production.
    const source = synthetic(2);
    const { handler } = harnessFor([source]);

    const summary = await poll(handler, {
      ...PUMP_FUN_POLL,
      since: new Date(CORPUS_EPOCH_MS).toISOString(),
    });

    expect(summary.fetched).toBe(2);
  });

  it('passes the payload limit ahead of the configured one', async () => {
    const stub = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [success([])]);
    const { handler } = harnessFor([stub], { limit: 25 });

    await poll(handler, { ...PUMP_FUN_POLL, limit: 7 });

    expect(stub.requests[0]?.limit).toBe(7);
  });

  it('falls back to the configured limit, then to a default', async () => {
    const configured = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [success([])]);
    await poll(harnessFor([configured], { limit: 25 }).handler, PUMP_FUN_POLL);
    expect(configured.requests[0]?.limit).toBe(25);

    const bare = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [success([])]);
    await poll(harnessFor([bare]).handler, PUMP_FUN_POLL);
    expect(bare.requests[0]?.limit).toBeGreaterThan(0);
  });
});

describe('LaunchPollHandler failure policy', () => {
  it('retries a launchpad outage', async () => {
    // The provider-outage path: a failure the queue should back off and repeat, not
    // one it should dead-letter.
    const source = synthetic(2);
    source.simulateOutage();
    const { handler } = harnessFor([source]);

    const result = await handler.poll(PUMP_FUN_POLL);

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error).not.toBeInstanceOf(PermanentJobFailure);
    }
  });

  it('counts a launch the canonical model rejected without failing the poll', async () => {
    // The good launches in the window are already committed. Retrying would
    // re-discover them as duplicates and fail on the same bad mint until the job
    // dead-letters, costing the feed its poll over one malformed entry.
    const stub = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [
      success([
        launch({ address: 'Good1', launchTime: new Date(CORPUS_EPOCH_MS) }),
        launch({ address: 'Bad1', ticker: '  ', launchTime: new Date(CORPUS_EPOCH_MS + 1_000) }),
      ]),
    ]);
    const { handler, uow } = harnessFor([stub]);

    const summary = await poll(handler, PUMP_FUN_POLL);

    expect(summary.fetched).toBe(2);
    expect(summary.discovered).toBe(1);
    expect(summary.rejected).toBe(1);
    expect(uow.store.tokens.size).toBe(1);
  });

  it('reports a database failure as retryable rather than as a rejected launch', async () => {
    // Otherwise an event-store outage reads as a clean poll that happened to reject
    // every launch, and the feed goes quiet with nothing to alert on.
    const stub = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [
      success([launch({ address: 'Good1', launchTime: new Date(CORPUS_EPOCH_MS) })]),
    ]);
    const { handler } = harnessFor([stub], { appendFails: true });

    const result = await handler.poll(PUMP_FUN_POLL);

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error).not.toBeInstanceOf(PermanentJobFailure);
      expect(result.error.message).toContain('event store unavailable');
    }
  });

  it('propagates a fetch failure unchanged', async () => {
    const stub = new StubLaunchSource(Chain.SOLANA, LaunchSource.PUMP_FUN, [
      failure(new Error('launchpad returned 503')),
    ]);
    const { handler } = harnessFor([stub]);

    const result = await handler.poll(PUMP_FUN_POLL);

    expect(result.kind).toBe('failure');
    if (result.kind === 'failure') {
      expect(result.error.message).toBe('launchpad returned 503');
    }
  });

  it('refuses a chain it does not recognise', async () => {
    // The payload crosses a JSONB boundary, so its enum types are a claim rather
    // than a guarantee. An unrecognised chain forks token identity.
    const { handler } = harnessFor([synthetic(1)]);

    await expect(
      handler.poll({ ...PUMP_FUN_POLL, chain: 'ethereum' as Chain })
    ).rejects.toThrow(PermanentJobFailure);
  });

  it('refuses a launch source it does not recognise', async () => {
    const { handler } = harnessFor([synthetic(1)]);

    await expect(
      handler.poll({ ...PUMP_FUN_POLL, launchSource: 'raydium' as LaunchSource })
    ).rejects.toThrow(PermanentJobFailure);
  });

  it('refuses a feed no adapter is bound for', async () => {
    // A composition error. This process has no adapter for the feed and will not
    // grow one by waiting, so retrying only delays the dead-letter an operator
    // needs to see.
    const { handler } = harnessFor([synthetic(1)]);

    await expect(
      handler.poll({ chain: Chain.SOLANA, launchSource: LaunchSource.PUMPSWAP })
    ).rejects.toThrow(PermanentJobFailure);
  });

  it('refuses an adapter bound to a different chain than the payload names', async () => {
    // Not redundant with the map key: one launchpad brand can run on more than one
    // chain, and a wrong binding would persist real mints under a chain they do not
    // exist on. Since identity is (chain, address), that forks the token silently
    // instead of failing.
    const elsewhere = new StubLaunchSource(
      'ethereum' as Chain,
      LaunchSource.PUMP_FUN,
      [success([])]
    );
    const { handler } = harnessFor([elsewhere]);

    await expect(handler.poll(PUMP_FUN_POLL)).rejects.toThrow(/bound to chain/);
  });

  it('refuses a window it cannot parse', async () => {
    // `new Date('nonsense')` is a Date, so only a value check catches it — and the
    // same string will not parse on a retry.
    const { handler } = harnessFor([synthetic(1)]);

    await expect(
      handler.poll({ ...PUMP_FUN_POLL, since: 'last tuesday' })
    ).rejects.toThrow(PermanentJobFailure);
  });
});

describe('LaunchPollHandler.handle', () => {
  it('claims the token discovery poll queue', () => {
    const { handler } = harnessFor([synthetic(1)]);

    expect(handler.queueName).toBe('token_discovery_poll');
  });

  it('runs the poll and hands the summary to the caller', async () => {
    const { handler, polled } = harnessFor([synthetic(2)]);

    await handler.handle(jobFor(PUMP_FUN_POLL));

    expect(polled).toHaveLength(1);
    expect(polled[0]?.discovered).toBe(2);
  });

  it('refuses a payload version it does not understand', async () => {
    // A version bump means the payload's shape changed. Guessing at it would
    // persist tokens from a payload this handler cannot read.
    const { handler } = harnessFor([synthetic(1)]);

    await expect(handler.handle(jobFor(PUMP_FUN_POLL, '2'))).rejects.toThrow(PermanentJobFailure);
  });

  it('checks the payload version before touching the launchpad', async () => {
    const source = synthetic(1);
    const { handler } = harnessFor([source]);

    await expect(handler.handle(jobFor(PUMP_FUN_POLL, '0'))).rejects.toThrow(PermanentJobFailure);
    expect(source.fetchCount()).toBe(0);
  });

  it('throws so the queue retries when the poll failed', async () => {
    const source = synthetic(2);
    source.simulateOutage();
    const { handler, polled } = harnessFor([source]);

    await expect(handler.handle(jobFor(PUMP_FUN_POLL))).rejects.toThrow(/simulated/);
    // A failed poll has no summary to report, and reporting one would show up in a
    // log as a successful poll that discovered nothing.
    expect(polled).toHaveLength(0);
  });

  it('is idempotent when the queue redelivers a job', async () => {
    // The queue requires it: a lease expiry or a crash after the work committed
    // both put the same job back in front of a worker.
    const { handler, uow } = harnessFor([synthetic(3)]);
    const job = jobFor(PUMP_FUN_POLL);

    await handler.handle(job);
    await handler.handle(job);

    expect(uow.store.tokens.size).toBe(3);
    expect(uow.store.events).toHaveLength(3);
  });
});

describe('LaunchPollHandler ordering', () => {
  it('announces a polled window oldest first', async () => {
    // The adapter answers newest-first and the event log is what replay reads, so
    // the poll has to come out of the pipeline in launch order.
    const { handler, uow } = harnessFor([synthetic(4)]);
    await poll(handler, PUMP_FUN_POLL);

    const announced = uow.store.events.map((event) => {
      const payload = event.payload as { address: string; launchTime: string };
      return payload.launchTime;
    });

    expect([...announced].sort()).toEqual(announced);
  });

  it('discovers every launch in the window exactly once', async () => {
    const { handler, uow } = harnessFor([synthetic(6)]);
    await poll(handler, PUMP_FUN_POLL);

    const ids = uow.store.events.map((event) => event.aggregateId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toHaveLength(6);
  });
});

/** Guards the assumption every summary assertion above rests on. */
describe('the synthetic corpus this file polls', () => {
  it('sits inside the default lookback window', async () => {
    const result: Result<SourceTokenLaunch[], Error> = await synthetic(4).fetchRecentLaunches({
      limit: 50,
    });
    if (result.kind === 'failure') {
      throw result.error;
    }

    for (const item of result.value) {
      expect(item.launchTime.getTime()).toBeLessThanOrEqual(POLL_AT.getTime());
      expect(POLL_AT.getTime() - item.launchTime.getTime()).toBeLessThan(15 * 60_000);
    }
  });
});
