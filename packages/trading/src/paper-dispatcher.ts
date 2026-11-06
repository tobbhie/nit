import { DatabasePool } from '@nit/database-public';
import { PortfolioId, Result, SignalId, failure, success } from '@nit/domain';
import { PostgresPaperTrader } from './paper-engine';
import { PaperMarketFeed } from './paper-feed';
import { PaperMarketSnapshot } from './paper-market';
import { paperRows } from './paper-store';
import { Clock } from './runtime';

export interface PaperDispatcherOptions {
  readonly portfolioIds: readonly PortfolioId[];
  readonly intervalMs: number;
  readonly retryMs: number;
  readonly maxProtectionTargets: number;
  readonly maxSignalsPerCycle: number;
  readonly maxSignalAgeMs: number;
}

export interface PaperDispatchSummary {
  readonly protected: number;
  readonly dispatched: number;
  readonly deferred: number;
  readonly protectionBacklog: boolean;
}

interface Portfolio {
  portfolio_id: PortfolioId;
  cash_asset: string;
  period_start: Date;
  period_end: Date;
  status: string;
  max_market_age_ms: string;
}
interface SignalTarget {
  signal_id: SignalId;
  token_mint: string;
  available_at: Date;
}

/** Bounded polling with durable per-signal receipts instead of a commit-order-unsafe event cursor. */
export class ContinuousPaperDispatcher {
  private stopped = true;
  private wake: (() => void) | undefined;
  private activeCycle: Promise<Result<PaperDispatchSummary, Error>> | undefined;

  constructor(
    private readonly db: DatabasePool,
    private readonly trader: Pick<PostgresPaperTrader, 'tick'>,
    private readonly feed: PaperMarketFeed,
    private readonly clock: Clock,
    private readonly options: PaperDispatcherOptions
  ) {
    if (options.portfolioIds.length === 0 || options.portfolioIds.length > 128)
      throw new Error('Configure 1 to 128 paper portfolios');
    for (const value of [
      options.intervalMs,
      options.retryMs,
      options.maxProtectionTargets,
      options.maxSignalsPerCycle,
      options.maxSignalAgeMs,
    ]) {
      if (!Number.isSafeInteger(value) || value <= 0)
        throw new Error('Paper dispatcher bounds must be positive integers');
    }
    if (options.maxProtectionTargets > 1024 || options.maxSignalsPerCycle > 100)
      throw new Error('Paper dispatcher batch exceeds its resource bound');
  }

  cycle(): Promise<Result<PaperDispatchSummary, Error>> {
    if (this.activeCycle !== undefined) return this.activeCycle;
    const work = this.runCycle();
    this.activeCycle = work;
    void work.finally(() => {
      if (this.activeCycle === work) this.activeCycle = undefined;
    });
    return work;
  }

  async run(onCycle: (outcome: Result<PaperDispatchSummary, Error>) => void): Promise<void> {
    if (!this.stopped) throw new Error('Paper dispatcher is already running');
    this.stopped = false;
    while (!this.stopped) {
      const outcome = await this.cycle();
      onCycle(outcome);
      if (this.stopped) break;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          this.wake = undefined;
          resolve();
        }, this.options.intervalMs);
        this.wake = (): void => {
          clearTimeout(timer);
          this.wake = undefined;
          resolve();
        };
      });
    }
  }

  stop(): void {
    this.stopped = true;
    this.wake?.();
  }

  private async runCycle(): Promise<Result<PaperDispatchSummary, Error>> {
    try {
      const now = this.clock.now();
      const summary = { protected: 0, dispatched: 0, deferred: 0, protectionBacklog: false };
      const portfolios = await paperRows<Portfolio>(
        this.db,
        `SELECT p.portfolio_id,p.cash_asset,
        r.period_start,r.period_end,r.status,
        c.strategy_snapshot->'risk'->'config'->>'maxMarketAgeMs' AS max_market_age_ms FROM paper_portfolios p
        JOIN strategy_runs r ON r.run_id=p.run_id JOIN paper_run_configs c ON c.run_id=r.run_id
        WHERE p.portfolio_id=ANY($1::uuid[])
        AND r.mode='forward_paper' ORDER BY p.portfolio_id`,
        [this.options.portfolioIds]
      );
      if (portfolios.length !== new Set(this.options.portfolioIds).size)
        throw new Error('A configured forward-paper portfolio was not found');
      const targets = await paperRows<{ portfolio_id: PortfolioId; token_mint: string }>(
        this.db,
        `
        WITH targets AS (
          SELECT portfolio_id,token_mint FROM positions WHERE open_quantity>0 AND portfolio_id=ANY($1::uuid[])
          UNION SELECT o.portfolio_id,o.token_mint FROM orders o JOIN paper_execution_plans p ON p.order_id=o.order_id
            WHERE o.portfolio_id=ANY($1::uuid[]) AND NOT EXISTS
              (SELECT 1 FROM paper_execution_results x WHERE x.order_id=o.order_id)
        ) SELECT t.* FROM targets t LEFT JOIN paper_feed_health h
          ON h.portfolio_id=t.portfolio_id AND h.token_mint=t.token_mint
        ORDER BY h.checked_at ASC NULLS FIRST,t.portfolio_id,t.token_mint LIMIT $2`,
        [this.options.portfolioIds, this.options.maxProtectionTargets + 1]
      );
      summary.protectionBacklog = targets.length > this.options.maxProtectionTargets;
      const blocked = new Set<PortfolioId>();
      const cache = new Map<string, Promise<Result<PaperMarketSnapshot, Error>>>();
      const market = (mint: string, cash: string): Promise<Result<PaperMarketSnapshot, Error>> => {
        const key = `${mint}:${cash}`;
        let value = cache.get(key);
        if (value === undefined) {
          value = this.feed.latest(mint, cash, now);
          cache.set(key, value);
        }
        return value;
      };
      const byId = new Map(portfolios.map((p) => [p.portfolio_id, p]));
      for (const target of targets.slice(0, this.options.maxProtectionTargets)) {
        const portfolio = byId.get(target.portfolio_id);
        if (portfolio === undefined)
          throw new Error('Protection target has no configured portfolio');
        const snapshot = await market(target.token_mint, portfolio.cash_asset);
        let reason = snapshot.kind === 'failure' ? snapshot.error.message : null;
        if (snapshot.kind === 'success') {
          const outcome = await this.trader.tick(portfolio.portfolio_id, snapshot.value);
          if (outcome.kind === 'failure') reason = outcome.error.message;
          else reason = unusable(snapshot.value, portfolio, now);
          if (reason === null) {
            const exits = await paperRows(
              this.db,
              `SELECT 1 FROM orders o
              WHERE o.portfolio_id=$1 AND o.side='sell' AND NOT EXISTS
              (SELECT 1 FROM paper_execution_results x WHERE x.order_id=o.order_id) LIMIT 1`,
              [portfolio.portfolio_id]
            );
            if (exits.length > 0) reason = 'PROTECTIVE_ORDER_PENDING';
          }
        }
        await this.health(portfolio.portfolio_id, target.token_mint, reason, now);
        if (reason !== null) {
          blocked.add(portfolio.portfolio_id);
          summary.deferred += 1;
        } else summary.protected += 1;
      }
      if (summary.protectionBacklog) return success(summary);
      let budget = this.options.maxSignalsPerCycle;
      // Rotate fairly by each portfolio's last dispatch time, including idle cycles.
      const ordered = await paperRows<{ portfolio_id: PortfolioId }>(
        this.db,
        `SELECT p.portfolio_id
        FROM paper_portfolios p
        LEFT JOIN paper_dispatch_health h ON h.portfolio_id=p.portfolio_id
        WHERE p.portfolio_id=ANY($1::uuid[])
        ORDER BY h.checked_at ASC NULLS FIRST,p.portfolio_id`,
        [this.options.portfolioIds]
      );
      for (const entry of ordered) {
        const portfolio = byId.get(entry.portfolio_id);
        if (portfolio === undefined) throw new Error('Dispatch portfolio was not loaded');
        if (budget === 0) break;
        if (
          blocked.has(portfolio.portfolio_id) ||
          portfolio.status !== 'running' ||
          now < portfolio.period_start ||
          now > portfolio.period_end
        )
          continue;
        const signals = await paperRows<SignalTarget>(
          this.db,
          `SELECT s.signal_id,s.token_mint,s.available_at FROM signals s
          LEFT JOIN paper_signal_dispatch d ON d.portfolio_id=$1 AND d.signal_id=s.signal_id
          WHERE s.token_mint IS NOT NULL AND s.kind IN ('narrative','wallet_convergence','candidate_update')
            AND s.available_at >= $2 AND s.available_at <= $3
            AND (d.signal_id IS NULL OR (d.completed_at IS NULL AND d.retry_at <= $3))
          ORDER BY COALESCE(d.attempted_at,s.available_at),s.signal_id LIMIT $4`,
          [portfolio.portfolio_id, portfolio.period_start, now, budget]
        );
        for (const signal of signals) {
          budget -= 1;
          if (now.getTime() - signal.available_at.getTime() > this.options.maxSignalAgeMs) {
            await this.receipt(
              portfolio.portfolio_id,
              signal.signal_id,
              'SIGNAL_EXPIRED',
              now,
              true
            );
            summary.deferred += 1;
            continue;
          }
          const snapshot = await market(signal.token_mint, portfolio.cash_asset);
          let reason = snapshot.kind === 'failure' ? snapshot.error.message : null;
          if (snapshot.kind === 'success') {
            reason = unusable(snapshot.value, portfolio, now);
            if (reason === null) {
              const outcome = await this.trader.tick(
                portfolio.portfolio_id,
                snapshot.value,
                signal.signal_id
              );
              if (outcome.kind === 'failure') reason = outcome.error.message;
              else {
                const decisions = await paperRows(
                  this.db,
                  'SELECT 1 FROM strategy_decisions WHERE portfolio_id=$1 AND source_signal_id=$2 LIMIT 1',
                  [portfolio.portfolio_id, signal.signal_id]
                );
                if (decisions.length === 0) reason = 'POSITION_OR_ORDER_PENDING';
              }
            }
          }
          await this.health(portfolio.portfolio_id, signal.token_mint, reason, now);
          await this.receipt(
            portfolio.portfolio_id,
            signal.signal_id,
            reason,
            now,
            reason === null
          );
          if (reason === null) summary.dispatched += 1;
          else summary.deferred += 1;
        }
        await paperRows(
          this.db,
          `INSERT INTO paper_dispatch_health (portfolio_id,checked_at) VALUES ($1,$2)
          ON CONFLICT (portfolio_id) DO UPDATE SET checked_at=EXCLUDED.checked_at`,
          [portfolio.portfolio_id, now]
        );
      }
      return success(summary);
    } catch (error) {
      return failure(error instanceof Error ? error : new Error('Paper dispatch failed'));
    }
  }

  private async health(
    portfolio: PortfolioId,
    mint: string,
    reason: string | null,
    now: Date
  ): Promise<void> {
    await paperRows(
      this.db,
      `INSERT INTO paper_feed_health (portfolio_id,token_mint,checked_at,reason)
      VALUES ($1,$2,$3,$4) ON CONFLICT (portfolio_id,token_mint) DO UPDATE
      SET checked_at=EXCLUDED.checked_at,reason=EXCLUDED.reason
      WHERE paper_feed_health.checked_at<=EXCLUDED.checked_at`,
      [portfolio, mint, now, reason]
    );
  }

  private async receipt(
    portfolio: PortfolioId,
    signal: SignalId,
    reason: string | null,
    now: Date,
    complete: boolean
  ): Promise<void> {
    await paperRows(
      this.db,
      `INSERT INTO paper_signal_dispatch
      (portfolio_id,signal_id,attempted_at,retry_at,completed_at,reason) VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (portfolio_id,signal_id) DO UPDATE SET attempted_at=EXCLUDED.attempted_at,
        retry_at=EXCLUDED.retry_at,completed_at=EXCLUDED.completed_at,reason=EXCLUDED.reason
      WHERE paper_signal_dispatch.completed_at IS NULL AND paper_signal_dispatch.attempted_at<=EXCLUDED.attempted_at`,
      [
        portfolio,
        signal,
        now,
        new Date(now.getTime() + this.options.retryMs),
        complete ? now : null,
        reason,
      ]
    );
  }
}

function unusable(snapshot: PaperMarketSnapshot, portfolio: Portfolio, now: Date): string | null {
  const age = Number(portfolio.max_market_age_ms);
  if (!Number.isSafeInteger(age) || age <= 0)
    throw new Error('Invalid pinned market freshness policy');
  if (now.getTime() - snapshot.occurredAt.getTime() > age) return 'STALE_MARKET';
  if (!snapshot.verified || !snapshot.sellable || !snapshot.tradable)
    return 'MARKET_NOT_EXECUTABLE';
  return null;
}
