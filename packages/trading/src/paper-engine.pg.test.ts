import { DatabasePool, PostgresSignalStore } from '@nit/database-public';
import { TestDatabase, createTestDatabase, hasTestDatabase } from '@nit/database-public/testing';
import { PortfolioId, SignalId, TokenRiskGate } from '@nit/domain';
import { SignalKind } from '@nit/signals';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { amount, units } from './fixed';
import { PostgresPaperTrader } from './paper-engine';
import { PostgresPaperRegistry, paperRows } from './paper-store';
import { ExecutionMode } from './runtime';
import {
  PAPER_START,
  PAPER_TOKEN,
  fixtureMarket,
  fixtureSignal,
  fixtureStrategy,
} from './paper.fixtures';

describe.skipIf(!hasTestDatabase())('persisted standalone paper trader', () => {
  let harness: TestDatabase;
  let db: DatabasePool;
  let now = new Date(PAPER_START);
  const clock = { now: (): Date => now };
  beforeAll(async () => {
    harness = await createTestDatabase('paper_engine');
    db = harness.db;
  }, 30_000);
  afterAll(async () => harness.teardown());

  async function run(
    options: Parameters<typeof fixtureStrategy>[0] = {},
    mode = ExecutionMode.FORWARD_PAPER
  ): Promise<PortfolioId> {
    const result = await new PostgresPaperRegistry(db).createRun({
      strategy: fixtureStrategy(options),
      mode,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'fixture-paper-v1',
      periodStart: PAPER_START,
      periodEnd: new Date(PAPER_START.getTime() + 86_400_000),
      cashAsset: 'SOL',
      initialCash: '100',
    });
    if (result.kind === 'failure') throw result.error;
    return result.value.portfolioId;
  }
  async function signal(
    kind = SignalKind.WALLET_CONVERGENCE,
    tokenMint = PAPER_TOKEN
  ): Promise<SignalId> {
    const value = fixtureSignal(kind, tokenMint);
    const result = await new PostgresSignalStore(db).append(value);
    if (result.kind === 'failure') throw result.error;
    return result.value.signalId;
  }
  async function position(id: PortfolioId): Promise<Record<string, string>> {
    const rows = await paperRows<Record<string, string>>(
      db,
      'SELECT * FROM positions WHERE portfolio_id=$1',
      [id]
    );
    if (rows[0] === undefined) throw new Error('Position missing');
    return rows[0];
  }

  function riskGate(): TokenRiskGate {
    return {
      assessmentId: '00000000-0000-4000-8000-000000000001',
      tokenMint: PAPER_TOKEN,
      marketAddress: PAPER_TOKEN,
      policyHash: 'a'.repeat(64),
      status: 'clear',
      reasonCodes: [],
      observedAt: now.toISOString(),
      availableAt: now.toISOString(),
      expiresAt: new Date(now.getTime() + 60000).toISOString(),
    };
  }
  it('requires pinned risk for entries but permits protective sells when risk evidence is lost', async () => {
    now = new Date(PAPER_START);
    const missing = await run({ version: 1000, tokenRiskPolicyHash: 'a'.repeat(64) });
    const good = await run({ version: 1000, tokenRiskPolicyHash: 'a'.repeat(64) });
    const trader = new PostgresPaperTrader(db, clock);
    await trader.tick(missing, fixtureMarket(now), await signal());
    expect(
      await paperRows(db, 'SELECT 1 FROM orders WHERE portfolio_id=$1', [missing])
    ).toHaveLength(0);
    const gate = riskGate();
    await trader.tick(
      good,
      { ...fixtureMarket(now), marketAddress: PAPER_TOKEN, tokenRisk: gate },
      await signal()
    );
    expect(await paperRows(db, 'SELECT 1 FROM orders WHERE portfolio_id=$1', [good])).toHaveLength(
      1
    );
    now = new Date(now.getTime() + 1000);
    await trader.tick(good, { ...fixtureMarket(now), marketAddress: PAPER_TOKEN, tokenRisk: gate });
    expect((await position(good))['open_quantity']).not.toBe('0');
    now = new Date(now.getTime() + 1000);
    await trader.tick(good, fixtureMarket(now, '3000'));
    expect(
      await paperRows(db, "SELECT 1 FROM orders WHERE portfolio_id=$1 AND side='sell'", [good])
    ).toHaveLength(1);
  });
  it('rechecks risk expiry before delayed buy settlement', async () => {
    now = new Date(PAPER_START);
    const id = await run({ version: 1000, tokenRiskPolicyHash: 'a'.repeat(64) });
    const trader = new PostgresPaperTrader(db, clock);
    const gate = { ...riskGate(), expiresAt: new Date(now.getTime() + 500).toISOString() };
    await trader.tick(
      id,
      { ...fixtureMarket(now), marketAddress: PAPER_TOKEN, tokenRisk: gate },
      await signal()
    );
    now = new Date(now.getTime() + 1000);
    await trader.tick(id, { ...fixtureMarket(now), marketAddress: PAPER_TOKEN, tokenRisk: gate });
    expect(
      await paperRows(db, 'SELECT 1 FROM positions WHERE portfolio_id=$1 AND open_quantity>0', [id])
    ).toHaveLength(0);
    expect(
      await paperRows(
        db,
        `SELECT e.detail->>'reason' AS reason FROM order_events e JOIN orders o USING(order_id)
      WHERE o.portfolio_id=$1 AND e.status='failed'`,
        [id]
      )
    ).toEqual([{ reason: 'TOKEN_RISK_STALE_OR_FUTURE' }]);
  });
  it('isolates exit-child portfolios and protects the actual original-quantity moonbag', async () => {
    now = new Date(PAPER_START);
    const first = await run();
    const second = await run({ exit: 'X02' });
    const trader = new PostgresPaperTrader(db, clock);
    const source = await signal();
    const planned = await trader.tickMatrix([first, second], fixtureMarket(now), source);
    expect(planned.every((item) => item.kind === 'success' && item.value.orders === 1)).toBe(true);
    now = new Date(PAPER_START.getTime() + 500);
    expect(await trader.tick(first, fixtureMarket(now))).toMatchObject({
      kind: 'success',
      value: { settled: 0 },
    });
    now = new Date(PAPER_START.getTime() + 1000);
    await trader.tickMatrix([first, second], fixtureMarket(now));
    const cash = await paperRows<{ current_cash: string }>(
      db,
      'SELECT current_cash FROM paper_portfolios WHERE portfolio_id=ANY($1::uuid[])',
      [[first, second]]
    );
    expect(cash.map((item) => amount(units(item.current_cash)))).toEqual(['89.989', '89.989']);
    now = new Date(PAPER_START.getTime() + 2000);
    const exits = await trader.tickMatrix([first, second], fixtureMarket(now, '3000'));
    expect(exits).toMatchObject([
      { kind: 'success', value: { orders: 1 } },
      { kind: 'success', value: { orders: 0 } },
    ]);
    now = new Date(PAPER_START.getTime() + 3000);
    await trader.tickMatrix([first, second], fixtureMarket(now, '3000'));
    const moonbag = await position(first);
    expect(moonbag['state']).toBe('moonbag');
    const original = units(moonbag['original_quantity']!);
    const reserve = units(moonbag['open_quantity']!);
    expect(reserve * 5n >= original && reserve * 5n - original < 5n * 10n ** 12n).toBe(true);
    expect((await position(second))['state']).toBe('open');
    now = new Date(PAPER_START.getTime() + 4000);
    expect(await trader.tick(first, fixtureMarket(now, '500'))).toMatchObject({
      kind: 'success',
      value: { orders: 1 },
    });
    now = new Date(PAPER_START.getTime() + 5000);
    expect(await trader.tick(first, fixtureMarket(now, '500'))).toMatchObject({
      kind: 'success',
      value: { settled: 1 },
    });
    expect((await position(first))['state']).toBe('closed');
    const rebuilt = await paperRows<{ matches: boolean }>(
      db,
      `SELECT p.current_cash = p.initial_cash+
      COALESCE((SELECT SUM(x.cash_delta) FROM paper_execution_results x JOIN orders o ON o.order_id=x.order_id
      WHERE o.portfolio_id=p.portfolio_id),0) AS matches FROM paper_portfolios p WHERE portfolio_id=$1`,
      [first]
    );
    expect(rebuilt[0]?.matches).toBe(true);
  });

  it('deduplicates concurrent signals and settles exactly once after a process restart', async () => {
    now = new Date(PAPER_START);
    const id = await run();
    const source = await signal();
    const market = fixtureMarket(now);
    const outcomes = await Promise.all([
      new PostgresPaperTrader(db, clock).tick(id, market, source),
      new PostgresPaperTrader(db, clock).tick(id, market, source),
    ]);
    expect(outcomes.every((result) => result.kind === 'success')).toBe(true);
    now = new Date(PAPER_START.getTime() + 1000);
    const restarted = new PostgresPaperTrader(db, clock);
    await restarted.tick(id, fixtureMarket(now));
    await restarted.tick(id, fixtureMarket(now));
    const fills = await paperRows<{ count: string }>(
      db,
      `SELECT COUNT(*)::text AS count FROM order_fills f
      JOIN orders o ON o.order_id=f.order_id WHERE o.portfolio_id=$1`,
      [id]
    );
    expect(fills[0]?.count).toBe('1');
  });

  it('rejects changed execution prices and keeps failed orders distinct from fills', async () => {
    now = new Date(PAPER_START);
    const id = await run();
    const trader = new PostgresPaperTrader(db, clock);
    await trader.tick(id, fixtureMarket(now), await signal());
    now = new Date(PAPER_START.getTime() + 1000);
    expect(await trader.tick(id, fixtureMarket(now, '3000'))).toMatchObject({
      kind: 'success',
      value: { settled: 1 },
    });
    const ledger = await paperRows<{ current_cash: string; fills: string }>(
      db,
      `SELECT p.current_cash,
      (SELECT COUNT(*)::text FROM order_fills f JOIN orders o ON o.order_id=f.order_id
      WHERE o.portfolio_id=p.portfolio_id) AS fills FROM paper_portfolios p WHERE portfolio_id=$1`,
      [id]
    );
    expect(ledger[0]).toMatchObject({ current_cash: '100.000000000000000000', fills: '0' });
  });

  it('records partial fills and failed exits, arming protection only after a profitable fill', async () => {
    now = new Date(PAPER_START);
    const id = await run({ version: 20, fillBps: 5000, failEveryNthOrder: 2 });
    const trader = new PostgresPaperTrader(db, clock);
    await trader.tick(id, fixtureMarket(now), await signal());
    now = new Date(PAPER_START.getTime() + 1000);
    await trader.tick(id, fixtureMarket(now));
    const opened = await position(id);
    now = new Date(PAPER_START.getTime() + 2000);
    await trader.tick(id, fixtureMarket(now, '3000'));
    now = new Date(PAPER_START.getTime() + 3000);
    await trader.tick(id, fixtureMarket(now, '3000'));
    expect((await position(id))['open_quantity']).toBe(opened['open_quantity']);
    expect((await position(id))['stop_price']).toBe(opened['stop_price']);
    now = new Date(PAPER_START.getTime() + 4000);
    await trader.tick(id, fixtureMarket(now, '3000'));
    const reduced = await position(id);
    expect(units(reduced['open_quantity']!)).toBeLessThan(units(opened['open_quantity']!));
    expect(units(reduced['stop_price']!)).toBeGreaterThan(units(opened['stop_price']!));
    const failed = await paperRows<{ count: string }>(
      db,
      `SELECT COUNT(*)::text AS count FROM position_events
      WHERE position_id=$1 AND event_type='exit_failed'`,
      [opened['position_id']]
    );
    expect(failed[0]?.count).toBe('1');
  });

  it('accounts for pending entries in capital and position limits', async () => {
    now = new Date(PAPER_START);
    const id = await run({ version: 30, maxPositions: 1 });
    const trader = new PostgresPaperTrader(db, clock);
    await trader.tick(id, fixtureMarket(now), await signal());
    const token = '22222222222222222222222222222222';
    const second = await trader.tick(
      id,
      fixtureMarket(now, '1000', token),
      await signal(SignalKind.WALLET_CONVERGENCE, token)
    );
    expect(second).toMatchObject({ kind: 'success', value: { decisions: 1, orders: 0 } });
    const reason = await paperRows<{ reason_codes: string[] }>(
      db,
      `SELECT reason_codes FROM strategy_decisions
      WHERE portfolio_id=$1 AND decision_type='abstain'`,
      [id]
    );
    expect(reason[0]?.reason_codes).toEqual(['MAX_POSITIONS']);
  });

  it('uses the same engine for narrative-only historical paper and rejects live registry creation', async () => {
    now = new Date(PAPER_START);
    const id = await run({ entry: 'E01' }, ExecutionMode.HISTORICAL);
    expect(
      await new PostgresPaperTrader(db, clock).tick(
        id,
        fixtureMarket(now),
        await signal(SignalKind.NARRATIVE)
      )
    ).toMatchObject({ kind: 'success', value: { orders: 1 } });
    await expect(run({}, ExecutionMode.LIVE)).rejects.toThrow('live');
    now = new Date(PAPER_START.getTime() - 1);
    expect((await new PostgresPaperTrader(db, clock).tick(id, fixtureMarket(now))).kind).toBe(
      'failure'
    );
  });

  it('rebuilds portfolio cash and positions from immutable events', async () => {
    now = new Date(PAPER_START);
    const id = await run();
    const trader = new PostgresPaperTrader(db, clock);
    await trader.tick(id, fixtureMarket(now), await signal());
    now = new Date(PAPER_START.getTime() + 1000);
    await trader.tick(id, fixtureMarket(now));
    const original = await position(id);
    await paperRows(db, 'UPDATE paper_portfolios SET current_cash=0 WHERE portfolio_id=$1', [id]);
    await paperRows(
      db,
      "UPDATE positions SET open_quantity=0,state='closed' WHERE portfolio_id=$1",
      [id]
    );
    expect(await trader.rebuild(id)).toEqual({ kind: 'success', value: undefined });
    expect((await position(id))['open_quantity']).toBe(original['open_quantity']);
    expect((await position(id))['state']).toBe('open');
    const cash = await paperRows<{ current_cash: string }>(
      db,
      'SELECT current_cash FROM paper_portfolios WHERE portfolio_id=$1',
      [id]
    );
    expect(amount(units(cash[0]!.current_cash))).toBe('89.989');
  });

  it('dispatches combined and buy-all entries, including the open-ended child', async () => {
    now = new Date(PAPER_START);
    const combined = await run({ entry: 'E03', exit: 'X04' });
    const all = await run({ entry: 'E04', exit: 'X04' });
    const trader = new PostgresPaperTrader(db, clock);
    const source = await signal(SignalKind.CANDIDATE_UPDATE);
    const result = await trader.tickMatrix([combined, all], fixtureMarket(now), source);
    expect(result).toMatchObject([
      { kind: 'success', value: { orders: 1 } },
      { kind: 'success', value: { orders: 1 } },
    ]);
    now = new Date(PAPER_START.getTime() + 1000);
    await trader.tickMatrix([combined, all], fixtureMarket(now));
    now = new Date(PAPER_START.getTime() + 2000);
    const held = await trader.tickMatrix([combined, all], fixtureMarket(now, '3000'));
    expect(held.every((value) => value.kind === 'success' && value.value.orders === 0)).toBe(true);
  });

  it('runs trailing-profit and time exits from the same persisted position engine', async () => {
    now = new Date(PAPER_START);
    const trailing = await run({ exit: 'X03' });
    const timed = await run({ exit: 'X07' });
    const trader = new PostgresPaperTrader(db, clock);
    await trader.tickMatrix([trailing, timed], fixtureMarket(now), await signal());
    now = new Date(PAPER_START.getTime() + 1000);
    await trader.tickMatrix([trailing, timed], fixtureMarket(now));
    now = new Date(PAPER_START.getTime() + 2000);
    expect(await trader.tick(trailing, fixtureMarket(now, '3500'))).toMatchObject({
      kind: 'success',
      value: { orders: 0 },
    });
    now = new Date(PAPER_START.getTime() + 3000);
    expect(await trader.tick(trailing, fixtureMarket(now, '3000'))).toMatchObject({
      kind: 'success',
      value: { orders: 1 },
    });
    now = new Date(PAPER_START.getTime() + 61_000);
    expect(await trader.tick(timed, fixtureMarket(now))).toMatchObject({
      kind: 'success',
      value: { orders: 1 },
    });
  });

  it('rolls back a failed settlement and retries its persisted order', async () => {
    now = new Date(PAPER_START);
    const id = await run();
    const trader = new PostgresPaperTrader(db, clock);
    await trader.tick(id, fixtureMarket(now), await signal());
    await paperRows(
      db,
      `CREATE FUNCTION fail_paper_fixture_fill() RETURNS trigger AS $$
      BEGIN RAISE EXCEPTION 'fixture settlement failure'; END;
      $$ LANGUAGE plpgsql;
      CREATE TRIGGER fail_paper_fixture_fill BEFORE INSERT ON order_fills
      FOR EACH ROW EXECUTE FUNCTION fail_paper_fixture_fill()`
    );
    now = new Date(PAPER_START.getTime() + 1000);
    try {
      expect((await trader.tick(id, fixtureMarket(now))).kind).toBe('failure');
      const results = await paperRows<{ count: string }>(
        db,
        `SELECT COUNT(*)::text AS count
        FROM paper_execution_results x JOIN orders o ON o.order_id=x.order_id WHERE o.portfolio_id=$1`,
        [id]
      );
      expect(results[0]?.count).toBe('0');
    } finally {
      await paperRows(db, 'DROP TRIGGER fail_paper_fixture_fill ON order_fills');
    }
    expect(await trader.tick(id, fixtureMarket(now))).toMatchObject({
      kind: 'success',
      value: { settled: 1 },
    });
    expect((await position(id))['state']).toBe('open');
  });

  it('fails closed for stale market evidence and rejects conflicting registered versions', async () => {
    now = new Date(PAPER_START.getTime() + 20_000);
    const id = await run();
    const trader = new PostgresPaperTrader(db, clock);
    expect(await trader.tick(id, fixtureMarket(PAPER_START), await signal())).toMatchObject({
      kind: 'success',
      value: { orders: 0, decisions: 1 },
    });
    const snapshot = fixtureStrategy();
    const altered = {
      ...snapshot,
      configHash: '0'.repeat(64),
    };
    const result = await new PostgresPaperRegistry(db).createRun({
      strategy: altered,
      mode: ExecutionMode.FORWARD_PAPER,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'fixture',
      periodStart: PAPER_START,
      periodEnd: now,
      cashAsset: 'SOL',
      initialCash: '100',
    });
    expect(result.kind).toBe('failure');
    await expect(run({ fillBps: 5000 })).rejects.toThrow('different content');
  });
});
