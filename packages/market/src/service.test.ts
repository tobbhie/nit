import { describe, expect, it } from 'vitest';
import { Chain, EventType, LaunchSource } from '@nit/domain';
import { EventEnvelope } from '@nit/events';
import { TokenDiscoveredPayload } from './events';
import { DISCOVERED, FakeUnitOfWork, PUMP_FUN, launch } from './in-memory.fixtures';
import { MarketDiscoveryService } from './service';

function serviceOn(uow: FakeUnitOfWork): MarketDiscoveryService {
  return new MarketDiscoveryService(uow, {
    producer: 'market-test',
    clock: () => DISCOVERED,
  });
}

function payloads(uow: FakeUnitOfWork): TokenDiscoveredPayload[] {
  return uow.store.events.map((event) => event.payload as TokenDiscoveredPayload);
}

describe('MarketDiscoveryService.discover', () => {
  it('persists the token and announces it exactly once', async () => {
    const uow = new FakeUnitOfWork();
    const result = await serviceOn(uow).discover(PUMP_FUN, launch());

    expect(result.kind).toBe('success');
    if (result.kind === 'failure') {
      throw result.error;
    }
    expect(result.value.status).toBe('discovered');
    expect(uow.store.tokens.size).toBe(1);
    expect(uow.store.events).toHaveLength(1);

    const event = uow.store.events[0] as EventEnvelope;
    expect(event.eventType).toBe('token.discovered');
    expect(event.aggregateType).toBe('token');
    expect(event.aggregateId).toBe(result.value.tokenId);
    expect(event.producer).toBe('market-test');
    // Occurrence is discovery time, not launch time: a launch feed polled with an
    // overlapping window routinely returns tokens minutes old, so ordering the log
    // by launch time would interleave today's discoveries with yesterday's.
    expect(event.occurredAt).toEqual(DISCOVERED);
  });

  it('tells a consumer what to enrich without carrying the token name', async () => {
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).discover(
      PUMP_FUN,
      launch({
        address: 'Mint1',
        ticker: 'PEPE',
        name: 'Pepe Coin',
        artworkUrl: 'https://cdn.invalid/pepe.png',
        creatorWallet: 'Creator1',
      })
    );

    const payload = payloads(uow)[0] as TokenDiscoveredPayload;
    expect(payload.address).toBe('Mint1');
    expect(payload.chain).toBe(Chain.SOLANA);
    expect(payload.launchSource).toBe(LaunchSource.PUMP_FUN);
    expect(payload.launchTime).toBe('2026-08-20T09:00:00.000Z');
    expect(payload.hasArtwork).toBe(true);
    expect(payload.hasCreatorWallet).toBe(true);
    // The ticker and name stay in the immutable row a consumer can read by id.
    // They are also the keys a narrative would be matched on, and EDS ch.21 §3
    // says this engine performs no narrative matching — an event carrying the
    // match keys is an invitation to do it in the wrong stage.
    expect(JSON.stringify(payload)).not.toContain('PEPE');
    expect(JSON.stringify(payload)).not.toContain('Pepe Coin');
  });

  it('tells the enrichment workers when there is nothing for them to do', async () => {
    // Without these flags the artwork worker wakes for a token with no image and
    // the creator worker wakes for a token with no creator, and both find out
    // only after claiming a job.
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).discover(PUMP_FUN, launch());

    const payload = payloads(uow)[0] as TokenDiscoveredPayload;
    expect(payload.hasArtwork).toBe(false);
    expect(payload.hasCreatorWallet).toBe(false);
  });

  it('reports a re-seen launch as a duplicate and does not announce it again', async () => {
    // Polling uses overlapping windows, so re-seeing a launch is normal operation
    // (EDS ch.21 §8). A second announcement would re-enqueue every enrichment for
    // a token that has already been through them.
    const uow = new FakeUnitOfWork();
    const service = serviceOn(uow);

    const first = await service.discover(PUMP_FUN, launch());
    const second = await service.discover(PUMP_FUN, launch());
    if (first.kind === 'failure' || second.kind === 'failure') {
      throw new Error('Expected both discoveries to succeed');
    }

    expect(first.value.status).toBe('discovered');
    expect(second.value.status).toBe('duplicate');
    // The same token, not a second one.
    expect(second.value.tokenId).toBe(first.value.tokenId);
    expect(uow.store.tokens.size).toBe(1);
    expect(uow.store.events).toHaveLength(1);
  });

  it('treats a graduated token as the one it already knows', async () => {
    // A pump.fun mint reappears on PumpSwap when it graduates. Identity is
    // (chain, address), so it stays one token with one history rather than
    // becoming two with the evidence split between them.
    const uow = new FakeUnitOfWork();
    const service = serviceOn(uow);
    const mint = launch({ address: 'Graduate1' });

    const onPumpFun = await service.discover(PUMP_FUN, mint);
    const onPumpSwap = await service.discover(
      { chain: Chain.SOLANA, launchSource: LaunchSource.PUMPSWAP },
      launch({
        address: mint.address,
        launchTime: new Date(mint.launchTime.getTime() + 60_000),
        market: {
          marketAddress: 'PoolGraduate1',
          origin: {
            kind: 'chain',
            position: { transactionSignature: 'graduation-signature', logIndex: 7 },
            slot: 350_001_000,
          },
        },
      })
    );
    if (onPumpFun.kind === 'failure' || onPumpSwap.kind === 'failure') {
      throw new Error('Expected both discoveries to succeed');
    }

    expect(onPumpSwap.value.status).toBe('graduated');
    expect(onPumpSwap.value.tokenId).toBe(onPumpFun.value.tokenId);
    expect(uow.store.tokens.size).toBe(1);
    expect(uow.store.markets).toHaveLength(2);
    expect(uow.store.markets[1]?.marketAddress).toBe('PoolGraduate1');
    expect(uow.store.markets[0]?.activeUntil).toEqual(new Date(mint.launchTime.getTime() + 60_000));
    expect(uow.store.events.map((event) => event.eventType)).toEqual([
      EventType.TOKEN_DISCOVERED,
      EventType.TOKEN_GRADUATED,
    ]);
  });

  it('persists nothing when the event cannot be appended', async () => {
    // The outbox guarantee, and the failure is silent without it: a token no
    // worker hears about stays permanently unenriched, because the row exists and
    // so the next poll sees a duplicate and does not re-announce it.
    const uow = new FakeUnitOfWork(true);
    const result = await serviceOn(uow).discover(PUMP_FUN, launch());

    expect(result.kind).toBe('failure');
    expect(uow.store.tokens.size).toBe(0);
    expect(uow.store.events).toHaveLength(0);
  });

  it('rejects a malformed launch without opening a transaction', async () => {
    const uow = new FakeUnitOfWork();
    const result = await serviceOn(uow).discover(PUMP_FUN, launch({ ticker: '' }));

    expect(result.kind).toBe('failure');
    expect(uow.transactions).toBe(0);
  });
});

/**
 * The third member of the discovery transaction.
 *
 * `discovery.pg.test.ts` asserts the same guarantees against real constraints; these
 * cover the decisions the service itself makes — which jobs, keyed how, and whether
 * they survive a rollback — and cover them where no database is needed to find out.
 */
describe('MarketDiscoveryService outbox', () => {
  const keysIn = (uow: FakeUnitOfWork): string[] => [...uow.store.jobs.keys()].sort();

  it('commits the enrichment jobs with the token', async () => {
    const uow = new FakeUnitOfWork();
    const result = await serviceOn(uow).discover(
      PUMP_FUN,
      launch({
        address: 'Full1',
        artworkUrl: 'https://cdn.invalid/full.png',
        creatorWallet: 'Creator1',
      })
    );
    if (result.kind === 'failure') {
      throw result.error;
    }
    const tokenId = result.value.tokenId;

    expect(keysIn(uow)).toEqual([
      `token_artwork/token_artwork:${tokenId}`,
      `token_creator/token_creator:${tokenId}`,
      `token_liquidity/token_liquidity:${tokenId}:launch`,
    ]);
  });

  it('plans only the market job for a launch that supplied neither', async () => {
    // Progressive enrichment is not "run everything and let the worker find out":
    // a job for an image that does not exist would claim a lease, discover there is
    // nothing to fetch, and count as work.
    const uow = new FakeUnitOfWork();
    const result = await serviceOn(uow).discover(PUMP_FUN, launch({ address: 'Thin1' }));
    if (result.kind === 'failure') {
      throw result.error;
    }

    expect(keysIn(uow)).toEqual([`token_liquidity/token_liquidity:${result.value.tokenId}:launch`]);
  });

  it('enqueues nothing when the token could not be committed', async () => {
    // The half of the outbox guarantee that a fake without `jobs` could not check:
    // a committed job for a rolled-back token is a worker enriching a token that
    // does not exist.
    const uow = new FakeUnitOfWork(true);
    const result = await serviceOn(uow).discover(PUMP_FUN, launch());

    expect(result.kind).toBe('failure');
    expect(uow.store.jobs.size).toBe(0);
  });

  it('rolls the token back when a job could not be enqueued', async () => {
    // And the other direction. A token committed with no jobs stays permanently
    // unenriched, silently: the row exists, so the next poll reads it as a
    // duplicate and never re-announces it.
    const uow = new FakeUnitOfWork(false, true);
    const result = await serviceOn(uow).discover(PUMP_FUN, launch());

    expect(result.kind).toBe('failure');
    expect(uow.store.tokens.size).toBe(0);
    expect(uow.store.events).toHaveLength(0);
  });

  it('does not re-enqueue for a launch it has already seen', async () => {
    const uow = new FakeUnitOfWork();
    const service = serviceOn(uow);
    const mint = launch({ address: 'Reseen1', creatorWallet: 'Creator1' });

    await service.discover(PUMP_FUN, mint);
    await service.discover(PUMP_FUN, mint);

    // Two, not four: the duplicate short-circuit returns before the enqueue loop.
    // Were it not there the unique constraint on (queue_name, idempotency_key)
    // would absorb them anyway — two independent guards, which is why both are
    // asserted rather than only the count.
    expect(uow.store.jobs.size).toBe(2);
    expect(uow.transactions).toBe(2);
  });
});

describe('MarketDiscoveryService.discoverBatch', () => {
  it('announces launches oldest first regardless of the order they arrived in', async () => {
    // A launch feed has no monotonic ordering to page from, so a window comes back
    // in whatever order the launchpad felt like. The event log is what replay
    // reads, so announcing a noon launch before a dawn one would make the log
    // describe a sequence that never happened.
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).discoverBatch(PUMP_FUN, [
      launch({ address: 'third', launchTime: new Date('2026-08-20T09:00:02.000Z') }),
      launch({ address: 'first', launchTime: new Date('2026-08-20T09:00:00.000Z') }),
      launch({ address: 'second', launchTime: new Date('2026-08-20T09:00:01.000Z') }),
    ]);

    expect(payloads(uow).map((payload) => payload.address)).toEqual(['first', 'second', 'third']);
  });

  it('orders launches sharing a launch time deterministically', async () => {
    // Two mints in the same millisecond is ordinary on a launchpad, and replay has
    // to produce the same log every time it runs.
    const sameInstant = new Date('2026-08-20T09:00:00.000Z');
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).discoverBatch(PUMP_FUN, [
      launch({ address: 'Bbb', launchTime: sameInstant }),
      launch({ address: 'Aaa', launchTime: sameInstant }),
    ]);

    expect(payloads(uow).map((payload) => payload.address)).toEqual(['Aaa', 'Bbb']);
  });

  it('keeps going after a bad launch and reports which one failed', async () => {
    // The next poll's window will have moved past a skipped launch and nothing
    // goes back for it, so one malformed entry must not cost the rest.
    const uow = new FakeUnitOfWork();
    const batch = await serviceOn(uow).discoverBatch(PUMP_FUN, [
      launch({ address: 'good-1', launchTime: new Date('2026-08-20T09:00:00.000Z') }),
      launch({
        address: 'broken',
        launchTime: new Date('2026-08-20T09:00:01.000Z'),
        ticker: '  ',
      }),
      launch({ address: 'good-2', launchTime: new Date('2026-08-20T09:00:02.000Z') }),
    ]);

    expect(batch.discovered).toHaveLength(2);
    expect(batch.failures).toHaveLength(1);
    expect(batch.failures[0]?.address).toBe('broken');
    expect(payloads(uow).map((payload) => payload.address)).toEqual(['good-1', 'good-2']);
  });

  it('separates duplicates from new launches when a poll overlaps the last one', async () => {
    const uow = new FakeUnitOfWork();
    const service = serviceOn(uow);
    const earlier = launch({
      address: 'seen',
      launchTime: new Date('2026-08-20T09:00:00.000Z'),
    });
    const later = launch({
      address: 'fresh',
      launchTime: new Date('2026-08-20T09:00:01.000Z'),
    });

    await service.discover(PUMP_FUN, earlier);
    const batch = await service.discoverBatch(PUMP_FUN, [earlier, later]);

    expect(batch.duplicates).toHaveLength(1);
    expect(batch.discovered).toHaveLength(1);
    expect(uow.store.events).toHaveLength(2);
  });

  it('opens one transaction per launch rather than one for the batch', async () => {
    // One token is one aggregate (TIS ch.2 §7). Batching them into a single
    // transaction would let one malformed launch roll back the good ones.
    const uow = new FakeUnitOfWork();
    await serviceOn(uow).discoverBatch(PUMP_FUN, [
      launch({ address: 'a', launchTime: new Date('2026-08-20T09:00:00.000Z') }),
      launch({ address: 'b', launchTime: new Date('2026-08-20T09:00:01.000Z') }),
      launch({ address: 'c', launchTime: new Date('2026-08-20T09:00:02.000Z') }),
    ]);

    expect(uow.transactions).toBe(3);
  });

  it('reports an empty window without opening a transaction', async () => {
    const uow = new FakeUnitOfWork();
    const batch = await serviceOn(uow).discoverBatch(PUMP_FUN, []);

    expect(batch).toEqual({ discovered: [], graduated: [], duplicates: [], failures: [] });
    expect(uow.transactions).toBe(0);
  });

  it("does not reorder the caller's array", async () => {
    // The sort is on a copy. A caller that polled a window and kept the array for
    // its own bookkeeping would otherwise find it silently rearranged.
    const launches = [
      launch({ address: 'later', launchTime: new Date('2026-08-20T09:00:05.000Z') }),
      launch({ address: 'earlier', launchTime: new Date('2026-08-20T09:00:00.000Z') }),
    ];
    await serviceOn(new FakeUnitOfWork()).discoverBatch(PUMP_FUN, launches);

    expect(launches.map((item) => item.address)).toEqual(['later', 'earlier']);
  });
});
