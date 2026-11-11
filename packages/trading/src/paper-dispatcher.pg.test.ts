import { randomUUID } from 'node:crypto';
import { PostgresSignalStore } from '@nit/database-public';
import { TestDatabase, createTestDatabase, hasTestDatabase } from '@nit/database-public/testing';
import { PortfolioId, failure } from '@nit/domain';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { ContinuousPaperDispatcher, PaperDispatcherOptions } from './paper-dispatcher';
import { PostgresPaperTrader } from './paper-engine';
import { PostgresPaperMarketFeed } from './paper-feed';
import { PostgresPaperRegistry, paperRows } from './paper-store';
import { ExecutionMode } from './runtime';
import {
  PAPER_START,
  PAPER_TOKEN,
  fixtureMarket,
  fixtureSignal,
  fixtureStrategy,
} from './paper.fixtures';

const secondToken = '22222222222222222222222222222222';
describe.skipIf(!hasTestDatabase())('continuous paper dispatch and approved market feed', () => {
  let harness: TestDatabase;
  let now = new Date(PAPER_START);
  const clock = { now: (): Date => now };
  beforeAll(async () => {
    harness = await createTestDatabase('paper_dispatcher');
  }, 30_000);
  beforeEach(async () => {
    now = new Date(PAPER_START);
    await paperRows(harness.db, 'TRUNCATE token_risk_assessments,token_risk_policies');
    await paperRows(
      harness.db,
      `TRUNCATE tokens,signals,strategy_entry_versions,
      strategy_exit_versions,strategy_protection_versions,strategy_risk_versions CASCADE`
    );
  });
  afterAll(async () => harness.teardown());

  async function portfolio(): Promise<PortfolioId> {
    const created = await new PostgresPaperRegistry(harness.db).createRun({
      strategy: fixtureStrategy(),
      mode: ExecutionMode.FORWARD_PAPER,
      codeHash: 'c'.repeat(64),
      codeCommitSha: 'fixture-dispatch',
      periodStart: PAPER_START,
      periodEnd: new Date(PAPER_START.getTime() + 86_400_000),
      cashAsset: 'SOL',
      initialCash: '100',
    });
    if (created.kind === 'failure') throw created.error;
    return created.value.portfolioId;
  }
  function options(
    ids: PortfolioId[],
    overrides: Partial<PaperDispatcherOptions> = {}
  ): PaperDispatcherOptions {
    return {
      portfolioIds: ids,
      intervalMs: 1000,
      retryMs: 1000,
      maxProtectionTargets: 10,
      maxSignalsPerCycle: 10,
      maxSignalAgeMs: 60_000,
      ...overrides,
    };
  }
  function dispatcher(
    ids: PortfolioId[],
    feed = new PostgresPaperMarketFeed(harness.db, ['approved-rpc']),
    overrides: Partial<PaperDispatcherOptions> = {}
  ): ContinuousPaperDispatcher {
    return new ContinuousPaperDispatcher(
      harness.db,
      new PostgresPaperTrader(harness.db, clock),
      feed,
      clock,
      options(ids, overrides)
    );
  }
  async function publish(
    mint = PAPER_TOKEN,
    rawOverride?: Record<string, unknown>,
    recordedAt = now
  ): Promise<string> {
    await paperRows(
      harness.db,
      `INSERT INTO tokens
      (token_id,chain,address,ticker,name,launch_time,launch_source,raw_payload)
      VALUES ($1,'solana',$2,'FIX','Fixture',$3,'pump_fun','{}')
      ON CONFLICT (chain,address) DO NOTHING`,
      [randomUUID(), mint, PAPER_START]
    );
    const rows = await paperRows<{ token_id: string }>(
      harness.db,
      'SELECT token_id FROM tokens WHERE address=$1',
      [mint]
    );
    const tokenId = rows[0]!.token_id;
    await paperRows(
      harness.db,
      `INSERT INTO token_markets
      (token_market_id,token_id,venue,active_from,source_event_kind,source_event_key,
       market_address,transaction_signature,instruction_index,slot)
      SELECT $1,$2,'pump_fun_bonding_curve',$3,'chain',$4,
        '33333333333333333333333333333333',$4,0,1
      WHERE NOT EXISTS (SELECT 1 FROM token_markets WHERE token_id=$2)`,
      [randomUUID(), tokenId, PAPER_START, randomUUID()]
    );
    const facts = {
      ...fixtureMarket(now, '1000', mint),
      schemaVersion: 1,
      marketAddress: '33333333333333333333333333333333',
    };
    const id = randomUUID();
    await paperRows(
      harness.db,
      `INSERT INTO market_snapshots
      (snapshot_id,token_id,venue,observed_at,source,raw_payload,recorded_at)
      VALUES ($1,$2,'pump_fun_bonding_curve',$3,'approved-rpc',$4,$5)`,
      [id, tokenId, now, JSON.stringify(rawOverride ?? { paperExecution: facts }), recordedAt]
    );
    return id;
  }
  async function signal(mint = PAPER_TOKEN, available = now): Promise<void> {
    const created = await new PostgresSignalStore(harness.db).append({
      ...fixtureSignal(undefined, mint),
      availableAt: available,
    });
    if (created.kind === 'failure') throw created.error;
  }

  it('attaches point-in-time risk for the current market without hiding newer unavailable evidence', async () => {
    await publish();
    const hash = 'a'.repeat(64);
    const marketAddress = '33333333333333333333333333333333';
    await paperRows(
      harness.db,
      "INSERT INTO token_risk_policies(policy_hash,policy) VALUES ($1,'{}')",
      [hash]
    );
    async function assessment(
      status: string,
      available: Date,
      market = marketAddress
    ): Promise<void> {
      const id = randomUUID();
      const gate = {
        assessmentId: id,
        policyHash: hash,
        tokenMint: PAPER_TOKEN,
        marketAddress: market,
        status,
        reasonCodes: [],
        observedAt: available.toISOString(),
        availableAt: available.toISOString(),
        expiresAt: new Date(available.getTime() + 60000).toISOString(),
      };
      await paperRows(
        harness.db,
        `INSERT INTO token_risk_assessments
        (assessment_id,token_mint,market_address,policy_hash,observed_at,available_at,gate,evidence)
        VALUES ($1,$2,$3,$4,$5,$5,$6::jsonb,'{}')`,
        [id, PAPER_TOKEN, market, hash, available, JSON.stringify(gate)]
      );
    }
    await assessment('clear', now);
    const later = new Date(now.getTime() + 1000);
    await assessment('unavailable', later);
    const feed = new PostgresPaperMarketFeed(harness.db, ['approved-rpc']);
    const first = await feed.latest(PAPER_TOKEN, 'SOL', now);
    expect(first.kind === 'success' ? first.value.tokenRisk?.status : null).toBe('clear');
    now = later;
    await assessment('clear', now, '44444444444444444444444444444444');
    const second = await feed.latest(PAPER_TOKEN, 'SOL', now);
    expect(second.kind).toBe('success');
    expect(second.kind === 'success' ? second.value.tokenRisk?.status : null).toBe('unavailable');
    if (first.kind === 'success' && second.kind === 'success') {
      expect(first.value.snapshotId).not.toBe(second.value.snapshotId);
      expect(second.value.availableAt).toEqual(later);
      expect(first.value.tokenRisk?.status).toBe('clear');
    }
  });
  it('shares approved feed observations across portfolios and resumes with durable receipts', async () => {
    const ids = [await portfolio(), await portfolio()];
    await publish();
    await signal();
    const feed = new PostgresPaperMarketFeed(harness.db, ['approved-rpc']);
    const spy = vi.spyOn(feed, 'latest');
    expect(await dispatcher(ids, feed).cycle()).toMatchObject({
      kind: 'success',
      value: { dispatched: 2 },
    });
    expect(spy).toHaveBeenCalledTimes(1);
    now = new Date(PAPER_START.getTime() + 1000);
    await publish();
    expect(await dispatcher(ids).cycle()).toMatchObject({
      kind: 'success',
      value: { protected: 2, dispatched: 0 },
    });
    const fills = await paperRows<{ count: string }>(
      harness.db,
      'SELECT COUNT(*)::text AS count FROM order_fills'
    );
    expect(fills[0]?.count).toBe('2');
  });

  it('retries unavailable evidence without consuming its signal', async () => {
    const id = await portfolio();
    await signal();
    expect(await dispatcher([id]).cycle()).toMatchObject({
      kind: 'success',
      value: { deferred: 1 },
    });
    const waiting = await paperRows<{ completed_at: Date | null }>(
      harness.db,
      'SELECT completed_at FROM paper_signal_dispatch'
    );
    expect(waiting[0]?.completed_at).toBeNull();
    now = new Date(PAPER_START.getTime() + 1000);
    await publish();
    expect(await dispatcher([id]).cycle()).toMatchObject({
      kind: 'success',
      value: { dispatched: 1 },
    });
    expect(
      (
        await paperRows(
          harness.db,
          'SELECT 1 FROM paper_signal_dispatch WHERE completed_at IS NOT NULL'
        )
      ).length
    ).toBe(1);
  });

  it('blocks fresh entries when a held position loses its feed, then recovers', async () => {
    const id = await portfolio();
    await publish();
    await signal();
    await dispatcher([id]).cycle();
    now = new Date(PAPER_START.getTime() + 1000);
    await publish();
    await dispatcher([id]).cycle();
    now = new Date(PAPER_START.getTime() + 2000);
    await publish(secondToken);
    await signal(secondToken);
    const feed = new PostgresPaperMarketFeed(harness.db, ['approved-rpc']);
    const original = feed.latest.bind(feed);
    vi.spyOn(feed, 'latest').mockImplementation(async (mint, cash, at) =>
      mint === PAPER_TOKEN
        ? failure(new Error('fixture feed outage'))
        : await original(mint, cash, at)
    );
    expect(await dispatcher([id], feed).cycle()).toMatchObject({
      kind: 'success',
      value: { dispatched: 0, deferred: 1 },
    });
    expect((await paperRows(harness.db, 'SELECT 1 FROM orders')).length).toBe(1);
    now = new Date(PAPER_START.getTime() + 3000);
    await publish();
    await publish(secondToken);
    expect(await dispatcher([id]).cycle()).toMatchObject({
      kind: 'success',
      value: { dispatched: 1 },
    });
  });

  it('does not infer execution facts from USD metrics or fall back past newer unknown risk', async () => {
    await publish();
    const feed = new PostgresPaperMarketFeed(harness.db, ['approved-rpc']);
    expect((await feed.latest(PAPER_TOKEN, 'SOL', now)).kind).toBe('success');
    now = new Date(PAPER_START.getTime() + 1000);
    await publish(PAPER_TOKEN, { priceUsd: '1', liquidityUsd: '100000' });
    expect((await feed.latest(PAPER_TOKEN, 'SOL', now)).kind).toBe('failure');
    now = new Date(PAPER_START.getTime() + 2000);
    await publish(PAPER_TOKEN, {
      paperExecution: {
        ...fixtureMarket(now),
        schemaVersion: 1,
        marketAddress: '33333333333333333333333333333333',
        sellable: undefined,
      },
    });
    expect(await feed.latest(PAPER_TOKEN, 'SOL', now)).toMatchObject({
      kind: 'failure',
      error: {
        message: 'Paper execution risk fact is unknown',
      },
    });
    expect(() => new PostgresPaperMarketFeed(harness.db, ['synthetic-market'])).toThrow();
  });

  it('preserves chain-state age and rejects source time after capture or before venue activation', async () => {
    const feed = new PostgresPaperMarketFeed(harness.db, ['approved-rpc']);
    now = new Date(PAPER_START.getTime() + 60_000);
    const facts = {
      ...fixtureMarket(now),
      schemaVersion: 1,
      marketAddress: '33333333333333333333333333333333',
    };
    await publish(PAPER_TOKEN, {
      paperExecution: { ...facts, occurredAt: PAPER_START.toISOString() },
    });
    const stale = await feed.latest(PAPER_TOKEN, 'SOL', now);
    expect(stale.kind).toBe('success');
    if (stale.kind === 'success') expect(stale.value.occurredAt).toEqual(PAPER_START);
    now = new Date(now.getTime() + 1000);
    await publish(PAPER_TOKEN, {
      paperExecution: { ...facts, occurredAt: new Date(now.getTime() + 1).toISOString() },
    });
    expect((await feed.latest(PAPER_TOKEN, 'SOL', now)).kind).toBe('failure');
    now = new Date(now.getTime() + 1000);
    await publish(PAPER_TOKEN, {
      paperExecution: { ...facts, occurredAt: new Date(PAPER_START.getTime() - 1).toISOString() },
    });
    expect((await feed.latest(PAPER_TOKEN, 'SOL', now)).kind).toBe('failure');
  });

  it('uses availability time and the current venue instead of historical launch liquidity', async () => {
    await publish();
    const feed = new PostgresPaperMarketFeed(harness.db, ['approved-rpc']);
    now = new Date(PAPER_START.getTime() + 1000);
    await publish(PAPER_TOKEN, { priceUsd: '1' }, new Date(now.getTime() + 1000));
    expect((await feed.latest(PAPER_TOKEN, 'SOL', now)).kind).toBe('success');
    const tokens = await paperRows<{ token_id: string }>(
      harness.db,
      'SELECT token_id FROM tokens WHERE address=$1',
      [PAPER_TOKEN]
    );
    await paperRows(harness.db, 'UPDATE token_markets SET active_until=$2 WHERE token_id=$1', [
      tokens[0]!.token_id,
      now,
    ]);
    await paperRows(
      harness.db,
      `INSERT INTO token_markets
      (token_market_id,token_id,venue,active_from,source_event_kind,source_event_key)
      VALUES ($1,$2,'pumpswap',$3,'legacy',$4)`,
      [randomUUID(), tokens[0]!.token_id, now, randomUUID()]
    );
    expect((await feed.latest(PAPER_TOKEN, 'SOL', now)).kind).toBe('failure');
  });

  it('finds a late-committed signal even after a newer signal was completed', async () => {
    const id = await portfolio();
    now = new Date(PAPER_START.getTime() + 2000);
    await publish();
    await signal();
    await dispatcher([id]).cycle();
    await publish(secondToken);
    await signal(secondToken, PAPER_START);
    expect(await dispatcher([id]).cycle()).toMatchObject({
      kind: 'success',
      value: { dispatched: 1 },
    });
    expect(
      (
        await paperRows(
          harness.db,
          'SELECT 1 FROM paper_signal_dispatch WHERE completed_at IS NOT NULL'
        )
      ).length
    ).toBe(2);
  });

  it('bounds protection batches and rotates through outstanding positions before entries', async () => {
    const id = await portfolio();
    await publish();
    await publish(secondToken);
    await signal();
    await signal(secondToken);
    await dispatcher([id]).cycle();
    now = new Date(PAPER_START.getTime() + 1000);
    await publish();
    await publish(secondToken);
    const service = dispatcher([id], undefined, { maxProtectionTargets: 1 });
    expect(await service.cycle()).toMatchObject({
      kind: 'success',
      value: { protected: 1, protectionBacklog: true },
    });
    now = new Date(PAPER_START.getTime() + 1001);
    expect(await service.cycle()).toMatchObject({
      kind: 'success',
      value: { protected: 1, protectionBacklog: true },
    });
    expect(
      (await paperRows(harness.db, 'SELECT 1 FROM positions WHERE open_quantity>0')).length
    ).toBe(2);
  });

  it('expires stale opportunities and stops its loop without an extra timer cycle', async () => {
    const id = await portfolio();
    await signal();
    now = new Date(PAPER_START.getTime() + 2000);
    const service = dispatcher([id], undefined, { maxSignalAgeMs: 1000 });
    let cycles = 0;
    await service.run((result): void => {
      expect(result.kind).toBe('success');
      cycles += 1;
      service.stop();
    });
    expect(cycles).toBe(1);
    const receipt = await paperRows<{ reason: string; completed_at: Date | null }>(
      harness.db,
      'SELECT reason,completed_at FROM paper_signal_dispatch'
    );
    expect(receipt[0]?.reason).toBe('SIGNAL_EXPIRED');
    expect(receipt[0]?.completed_at).not.toBeNull();
  });
});
