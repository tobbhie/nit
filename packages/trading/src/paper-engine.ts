import { DatabasePool, Executor, clientExecutor } from '@nit/database-public';
import {
  PortfolioId,
  Result,
  SignalId,
  ValidationError,
  generateDecisionId,
  generateFillId,
  generateOrderEventId,
  generateOrderId,
  generatePositionEventId,
  generatePositionId,
} from '@nit/domain';
import { SignalEnvelope, SignalKind, parseSignalEnvelope } from '@nit/signals';
import { BPS, UNIT, amount, minimum, units } from './fixed';
import { PaperMarketSnapshot, PaperQuote, paperQuote, validatePaperMarket } from './paper-market';
import { PaperPolicy, paperPolicy } from './paper-policy';
import { paperRows } from './paper-store';
import { Clock, OrderSide } from './runtime';
import { StrategySnapshot } from './strategy';

interface PortfolioRow {
  portfolio_id: string;
  run_id: string;
  combination_id: string;
  cash_asset: string;
  current_cash: string;
  strategy_snapshot: StrategySnapshot;
  config_hash: string;
  status: string;
  mode: string;
  period_start: Date;
  period_end: Date;
  paper_last_tick_at: Date | null;
}

interface PositionRow {
  position_id: string;
  token_mint: string;
  original_quantity: string;
  open_quantity: string;
  cost_basis: string;
  high_water_price: string;
  stop_price: string | null;
  state: string;
  created_at: Date;
}

interface ExecutionPlan {
  readonly side: OrderSide;
  readonly quote: PaperQuote;
  readonly reason: string;
  readonly profitTaking: boolean;
  readonly ordinal: number;
  readonly snapshotOccurredAt: string;
}

interface PendingRow {
  order_id: string;
  plan: ExecutionPlan;
  due_at: Date;
  expires_at: Date;
}

export interface PaperTickResult {
  readonly decisions: number;
  readonly orders: number;
  readonly settled: number;
}

/**
 * Both historical and forward-paper modes use this persisted state machine.
 * One portfolio lock serializes settlement, protection and entry capital checks.
 */
export class PostgresPaperTrader {
  constructor(
    private readonly db: DatabasePool,
    private readonly clock: Clock
  ) {}

  /** Repairs mutable projections from immutable cash settlements and position events. */
  async rebuild(portfolioId: PortfolioId): Promise<Result<void, Error>> {
    return this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const portfolio = await paperRows(
        db,
        'SELECT 1 FROM paper_portfolios WHERE portfolio_id=$1 FOR UPDATE',
        [portfolioId]
      );
      if (portfolio.length !== 1) throw new ValidationError('Paper portfolio not found');
      await paperRows(
        db,
        `UPDATE paper_portfolios p SET current_cash=p.initial_cash+
        COALESCE((SELECT SUM(x.cash_delta) FROM paper_execution_results x
          JOIN orders o ON o.order_id=x.order_id WHERE o.portfolio_id=p.portfolio_id),0)
        WHERE p.portfolio_id=$1`,
        [portfolioId]
      );
      await paperRows(
        db,
        `UPDATE positions p SET
        original_quantity=(e.projection->>'original_quantity')::numeric,
        open_quantity=(e.projection->>'open_quantity')::numeric,
        cost_basis=(e.projection->>'cost_basis')::numeric,
        high_water_price=(e.projection->>'high_water_price')::numeric,
        stop_price=(e.projection->>'stop_price')::numeric,
        state=e.projection->>'state',
        updated_at=(e.projection->>'updated_at')::timestamptz
        FROM (SELECT DISTINCT ON (v.position_id) v.position_id,v.detail->'projection' AS projection
          FROM position_events v JOIN positions target ON target.position_id=v.position_id
          WHERE target.portfolio_id=$1 ORDER BY v.position_id,v.occurred_at DESC,v.position_event_id DESC) e
        WHERE p.position_id=e.position_id`,
        [portfolioId]
      );
    });
  }

  async tick(
    portfolioId: PortfolioId,
    market: PaperMarketSnapshot,
    sourceSignalId: SignalId | null = null
  ): Promise<Result<PaperTickResult, Error>> {
    const now = this.clock.now();
    return this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      validatePaperMarket(market, now);
      const portfolios = await paperRows<PortfolioRow>(
        db,
        `SELECT p.*,c.strategy_snapshot,
        r.config_hash,r.status,r.mode,r.period_start,r.period_end
        FROM paper_portfolios p JOIN strategy_runs r ON r.run_id=p.run_id
        JOIN paper_run_configs c ON c.run_id=r.run_id
        WHERE p.portfolio_id=$1 FOR UPDATE OF p`,
        [portfolioId]
      );
      const portfolio = portfolios[0];
      if (portfolio === undefined) throw new ValidationError('Paper portfolio not found');
      if (!['historical', 'forward_paper'].includes(portfolio.mode)) {
        throw new ValidationError('Paper trader rejects live mode');
      }
      if (portfolio.cash_asset !== market.cashAsset)
        throw new ValidationError('Market cash asset mismatch');
      if (portfolio.paper_last_tick_at !== null && now < portfolio.paper_last_tick_at) {
        throw new ValidationError('Paper portfolio clock cannot regress');
      }
      await paperRows(
        db,
        'UPDATE paper_portfolios SET paper_last_tick_at=$2 WHERE portfolio_id=$1',
        [portfolioId, now]
      );
      if (portfolio.config_hash !== portfolio.strategy_snapshot.configHash) {
        throw new ValidationError('Run strategy hash mismatch');
      }
      const policy = paperPolicy(portfolio.strategy_snapshot);
      await captureMarket(db, market);
      const outcome = { decisions: 0, orders: 0, settled: 0 };
      const pending = await paperRows<PendingRow>(
        db,
        `SELECT p.* FROM paper_execution_plans p
        JOIN orders o ON o.order_id=p.order_id
        WHERE o.portfolio_id=$1 AND o.token_mint=$2 AND p.due_at <= $3
          AND NOT EXISTS (SELECT 1 FROM paper_execution_results x WHERE x.order_id=p.order_id)
        ORDER BY o.created_at,o.order_id`,
        [portfolioId, market.tokenMint, now]
      );
      for (const order of pending) {
        await settle(db, portfolio, market, policy, order, now);
        outcome.settled += 1;
      }
      const busy = await paperRows(
        db,
        `SELECT 1 FROM orders o
        JOIN paper_execution_plans p ON p.order_id=o.order_id
        WHERE o.portfolio_id=$1 AND o.token_mint=$2
        AND NOT EXISTS (SELECT 1 FROM paper_execution_results x WHERE x.order_id=o.order_id)`,
        [portfolioId, market.tokenMint]
      );
      if (busy.length > 0) return outcome;
      const positions = await paperRows<PositionRow>(
        db,
        'SELECT * FROM positions WHERE portfolio_id=$1 AND token_mint=$2',
        [portfolioId, market.tokenMint]
      );
      const position = positions[0];
      // Exits remain active even when a run is paused/completed or its entry window ended.
      if (position !== undefined && units(position.open_quantity) > 0n) {
        const exit = await exitIntent(db, position, market, policy, portfolio, now);
        if (exit !== null) {
          const source = await positionSource(db, position.position_id);
          const created = await planOrder(
            db,
            portfolio,
            market,
            policy,
            source,
            OrderSide.SELL,
            exit.quantity,
            exit.reason,
            exit.profitTaking,
            now,
            `exit:${position.position_id}:${market.snapshotId}:${exit.reason}`
          );
          outcome.decisions += created.decisions;
          outcome.orders += created.orders;
        }
        return outcome;
      }
      if (sourceSignalId === null) return outcome;
      const signal = await loadSignal(db, sourceSignalId);
      if (signal.availableAt > now)
        throw new ValidationError('Signal is not available as of the clock');
      const reason = entryRejection(portfolio, signal, market, policy, position, now);
      const riskReason = reason ?? (await capitalRejection(db, portfolio, market, policy, now));
      const key = `entry:${signal.eventId}:${market.tokenMint}`;
      if (riskReason !== null) {
        const inserted = await recordDecision(
          db,
          portfolio,
          signal,
          'abstain',
          riskReason,
          key,
          now
        );
        outcome.decisions += Number(inserted !== null);
      } else {
        const created = await planOrder(
          db,
          portfolio,
          market,
          policy,
          signal,
          OrderSide.BUY,
          policy.orderCash,
          'ENTRY_ELIGIBLE',
          false,
          now,
          key
        );
        outcome.decisions += created.decisions;
        outcome.orders += created.orders;
      }
      return outcome;
    });
  }

  /** Shared evidence can be dispatched to every independently accounted child portfolio. */
  async tickMatrix(
    portfolioIds: readonly PortfolioId[],
    market: PaperMarketSnapshot,
    signalId: SignalId | null = null
  ): Promise<readonly Result<PaperTickResult, Error>[]> {
    const outcomes: Result<PaperTickResult, Error>[] = [];
    for (const portfolioId of [...new Set(portfolioIds)]) {
      outcomes.push(await this.tick(portfolioId, market, signalId));
    }
    return outcomes;
  }
}

async function captureMarket(db: Executor, market: PaperMarketSnapshot): Promise<void> {
  const snapshot = JSON.stringify(market);
  await paperRows(
    db,
    `INSERT INTO paper_market_observations
    (snapshot_id,token_mint,occurred_at,available_at,snapshot) VALUES ($1,$2,$3,$4,$5)
    ON CONFLICT (snapshot_id) DO NOTHING`,
    [market.snapshotId, market.tokenMint, market.occurredAt, market.availableAt, snapshot]
  );
  const match = await paperRows(
    db,
    'SELECT 1 FROM paper_market_observations WHERE snapshot_id=$1 AND snapshot=$2::jsonb',
    [market.snapshotId, snapshot]
  );
  if (match.length !== 1) throw new ValidationError('Market snapshot identity collision');
}

async function loadSignal(db: Executor, id: SignalId): Promise<SignalEnvelope> {
  const rows = await paperRows<{
    signal_id: string;
    source: string;
    source_version: string;
    kind: string;
    chain: string;
    token_mint: string | null;
    canonical_narrative_id: string | null;
    occurred_at: Date;
    observed_at: Date;
    available_at: Date;
    provider: string | null;
    reference_id: string | null;
    origin_kind: string;
    transaction_signature: string | null;
    instruction_index: number | null;
    log_index: number | null;
    transaction_level: boolean;
    actor_address: string | null;
    evidence_ids: string[];
    payload: unknown;
  }>(db, 'SELECT * FROM signals WHERE signal_id=$1', [id]);
  const row = rows[0];
  if (row === undefined) throw new ValidationError('Source signal not found');
  return parseSignalEnvelope({
    eventId: row.signal_id,
    source: row.source,
    sourceVersion: row.source_version,
    kind: row.kind,
    chain: row.chain,
    ...(row.token_mint === null ? {} : { tokenMint: row.token_mint }),
    ...(row.canonical_narrative_id === null
      ? {}
      : { canonicalNarrativeId: row.canonical_narrative_id }),
    occurredAt: row.occurred_at,
    observedAt: row.observed_at,
    availableAt: row.available_at,
    origin:
      row.origin_kind === 'provider'
        ? { kind: 'provider', provider: row.provider, referenceId: row.reference_id }
        : {
            kind: 'chain',
            chain: row.chain,
            transactionSignature: row.transaction_signature,
            ...(row.transaction_level
              ? { transactionLevel: true, actorAddress: row.actor_address }
              : row.instruction_index !== null
                ? { instructionIndex: row.instruction_index }
                : { logIndex: row.log_index }),
          },
    evidenceIds: row.evidence_ids,
    payload: row.payload,
  });
}

function entryRejection(
  portfolio: PortfolioRow,
  signal: SignalEnvelope,
  market: PaperMarketSnapshot,
  policy: PaperPolicy,
  position: PositionRow | undefined,
  now: Date
): string | null {
  if (portfolio.status !== 'running' || now < portfolio.period_start || now > portfolio.period_end)
    return 'RUN_NOT_ACTIVE';
  if (position !== undefined) return 'TOKEN_ALREADY_TRADED';
  if (signal.tokenMint !== market.tokenMint) return 'SIGNAL_TOKEN_MISMATCH';
  const payload = asObject(signal.payload);
  const narrative =
    signal.source === 'nit' && signal.kind === SignalKind.NARRATIVE && payload['eligible'] === true;
  const wallet =
    signal.source === 'wallet_tracker' &&
    signal.kind === SignalKind.WALLET_CONVERGENCE &&
    payload['side'] === 'buy' &&
    typeof payload['independentSourceCount'] === 'number' &&
    Number.isSafeInteger(payload['independentSourceCount']) &&
    payload['independentSourceCount'] >= policy.minimumIndependentWallets;
  const combined =
    signal.kind === SignalKind.CANDIDATE_UPDATE &&
    signal.source === 'nit' &&
    payload['actionState'] === 'trade_eligible' &&
    payload['narrativeConfirmed'] === true &&
    payload['walletConfirmed'] === true;
  const entry = portfolio.strategy_snapshot.entry.policyId;
  if (
    !(entry === 'E01'
      ? narrative
      : entry === 'E02'
        ? wallet
        : entry === 'E03'
          ? combined
          : narrative || wallet || combined)
  )
    return 'ENTRY_EVIDENCE_NOT_QUALIFIED';
  return marketRejection(market, policy, now, true);
}

function marketRejection(
  market: PaperMarketSnapshot,
  policy: PaperPolicy,
  now: Date,
  entry: boolean
): string | null {
  if (now.getTime() - market.occurredAt.getTime() > policy.maxMarketAgeMs) return 'STALE_MARKET';
  if (!market.verified || !market.tradable || !market.sellable) return 'MARKET_NOT_EXECUTABLE';
  if (entry && market.criticalRisk) return 'CRITICAL_TOKEN_RISK';
  if (entry && units(market.realCashReserves) < units(policy.minLiquidityCash))
    return 'INSUFFICIENT_LIQUIDITY';
  return null;
}

async function capitalRejection(
  db: Executor,
  portfolio: PortfolioRow,
  market: PaperMarketSnapshot,
  policy: PaperPolicy,
  now: Date
): Promise<string | null> {
  if (units(policy.orderCash) > units(policy.maxTradeCash)) return 'MAX_TRADE_CAPITAL';
  const rows = await paperRows<{ exposure: string; count: string }>(
    db,
    `SELECT
    ROUND(COALESCE(SUM(cost_basis*open_quantity/original_quantity),0),18)::text AS exposure,
    COUNT(*)::text AS count FROM positions WHERE portfolio_id=$1 AND open_quantity>0`,
    [portfolio.portfolio_id]
  );
  const pending = await paperRows<{ cash: string; count: string }>(
    db,
    `SELECT
    COALESCE(SUM(o.quantity+(p.plan->'quote'->>'cashFee')::numeric),0)::text AS cash,
    COUNT(*)::text AS count FROM orders o JOIN paper_execution_plans p ON p.order_id=o.order_id
    WHERE o.portfolio_id=$1 AND o.side='buy'
    AND NOT EXISTS (SELECT 1 FROM paper_execution_results x WHERE x.order_id=o.order_id)`,
    [portfolio.portfolio_id]
  );
  const exposure = units(rows[0]?.exposure ?? '0') + units(pending[0]?.cash ?? '0');
  if (Number(rows[0]?.count ?? 0) + Number(pending[0]?.count ?? 0) >= policy.maxPositions)
    return 'MAX_POSITIONS';
  let proposedCost: bigint;
  try {
    const quote = paperQuote(market, OrderSide.BUY, policy.orderCash, policy);
    proposedCost = units(quote.input) + units(quote.cashFee);
  } catch {
    return 'QUOTE_UNAVAILABLE';
  }
  if (exposure + proposedCost > units(policy.maxExposureCash)) return 'MAX_EXPOSURE';
  const last = await paperRows<{ created_at: Date }>(
    db,
    `SELECT created_at FROM orders
    WHERE portfolio_id=$1 AND token_mint=$2 AND side='buy' ORDER BY created_at DESC LIMIT 1`,
    [portfolio.portfolio_id, market.tokenMint]
  );
  if (last[0] !== undefined && now.getTime() - last[0].created_at.getTime() < policy.cooldownMs)
    return 'COOLDOWN';
  return null;
}

async function recordDecision(
  db: Executor,
  portfolio: PortfolioRow,
  signal: SignalEnvelope,
  type: string,
  reason: string,
  key: string,
  now: Date
): Promise<string | null> {
  const rows = await paperRows<{ decision_id: string }>(
    db,
    `INSERT INTO strategy_decisions
    (decision_id,run_id,portfolio_id,combination_id,source_signal_id,decision_type,evidence_ids,
     reason_codes,risk_gate_results,idempotency_key,decided_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (portfolio_id,idempotency_key) DO NOTHING RETURNING decision_id`,
    [
      generateDecisionId(),
      portfolio.run_id,
      portfolio.portfolio_id,
      portfolio.combination_id,
      signal.eventId,
      type,
      signal.evidenceIds,
      [reason],
      JSON.stringify({ reason, configHash: portfolio.config_hash }),
      key,
      now,
    ]
  );
  return rows[0]?.decision_id ?? null;
}

async function planOrder(
  db: Executor,
  portfolio: PortfolioRow,
  market: PaperMarketSnapshot,
  policy: PaperPolicy,
  signal: SignalEnvelope,
  side: OrderSide,
  input: string,
  reason: string,
  profitTaking: boolean,
  now: Date,
  key: string
): Promise<PaperTickResult> {
  let quote: PaperQuote | null = null;
  let rejection = marketRejection(market, policy, now, side === OrderSide.BUY);
  if (rejection === null) {
    try {
      quote = paperQuote(market, side, input, policy);
      if (quote.impactBps > policy.maxImpactBps) rejection = 'MAX_PRICE_IMPACT';
    } catch {
      rejection = 'QUOTE_UNAVAILABLE';
    }
  }
  if (side === OrderSide.BUY && quote !== null) {
    if (units(quote.input) + units(quote.cashFee) > units(policy.maxTradeCash))
      rejection = 'MAX_TRADE_CAPITAL';
    const reserved = await paperRows<{ reserved: string }>(
      db,
      `SELECT COALESCE(SUM(
      (p.plan->'quote'->>'input')::numeric + (p.plan->'quote'->>'cashFee')::numeric),0)::text AS reserved
      FROM paper_execution_plans p JOIN orders o ON o.order_id=p.order_id
      WHERE o.portfolio_id=$1 AND o.side='buy'
      AND NOT EXISTS (SELECT 1 FROM paper_execution_results x WHERE x.order_id=o.order_id)`,
      [portfolio.portfolio_id]
    );
    if (
      units(quote.input) + units(quote.cashFee) + units(reserved[0]?.reserved ?? '0') >
      units(portfolio.current_cash)
    ) {
      rejection = 'INSUFFICIENT_CASH';
    }
  }
  const decision = await recordDecision(
    db,
    portfolio,
    signal,
    rejection === null ? (side === OrderSide.BUY ? 'enter' : 'exit') : 'abstain',
    rejection ?? reason,
    key,
    now
  );
  if (decision === null) return { decisions: 0, orders: 0, settled: 0 };
  if (rejection !== null || quote === null) {
    if (side === OrderSide.SELL) {
      const positions = await paperRows<PositionRow>(
        db,
        'SELECT * FROM positions WHERE portfolio_id=$1 AND token_mint=$2',
        [portfolio.portfolio_id, market.tokenMint]
      );
      if (positions[0] !== undefined)
        await positionEvent(
          db,
          positions[0],
          null,
          null,
          'exit_failed',
          '0',
          { reason: rejection ?? 'QUOTE_UNAVAILABLE' },
          now
        );
    }
    return { decisions: 1, orders: 0, settled: 0 };
  }
  const ordinal = await paperRows<{ count: string }>(
    db,
    'SELECT COUNT(*)::text AS count FROM orders WHERE portfolio_id=$1',
    [portfolio.portfolio_id]
  );
  const orderId = generateOrderId();
  const plan: ExecutionPlan = {
    side,
    quote,
    reason,
    profitTaking,
    ordinal: Number(ordinal[0]?.count ?? 0) + 1,
    snapshotOccurredAt: market.occurredAt.toISOString(),
  };
  await paperRows(
    db,
    `INSERT INTO orders (order_id,decision_id,run_id,portfolio_id,combination_id,
    token_mint,side,quantity_asset,quantity,quote_id,idempotency_key,created_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
    [
      orderId,
      decision,
      portfolio.run_id,
      portfolio.portfolio_id,
      portfolio.combination_id,
      market.tokenMint,
      side,
      side === OrderSide.BUY ? market.cashAsset : market.tokenMint,
      quote.input,
      market.snapshotId,
      key,
      now,
    ]
  );
  await paperRows(
    db,
    `INSERT INTO paper_execution_plans (order_id,snapshot_id,due_at,expires_at,plan)
    VALUES ($1,$2,$3,$4,$5)`,
    [
      orderId,
      market.snapshotId,
      new Date(now.getTime() + policy.latencyMs),
      new Date(now.getTime() + policy.quoteTtlMs),
      JSON.stringify(plan),
    ]
  );
  await orderEvent(db, orderId, 'created', { reason, snapshotId: market.snapshotId }, now);
  await orderEvent(
    db,
    orderId,
    'accepted',
    { simulation: 'constant_product_v1', latencyMs: policy.latencyMs },
    now
  );
  return { decisions: 1, orders: 1, settled: 0 };
}

async function settle(
  db: Executor,
  portfolio: PortfolioRow,
  market: PaperMarketSnapshot,
  policy: PaperPolicy,
  pending: PendingRow,
  now: Date
): Promise<void> {
  const plan = pending.plan;
  let reason =
    now > pending.expires_at
      ? 'QUOTE_EXPIRED'
      : marketRejection(market, policy, now, plan.side === OrderSide.BUY);
  if (market.occurredAt < new Date(plan.snapshotOccurredAt)) reason = 'MARKET_PRECEDES_QUOTE';
  let quote: PaperQuote | null = null;
  if (reason === null) {
    try {
      const full = paperQuote(market, plan.side, plan.quote.input, policy);
      if (full.impactBps > policy.maxImpactBps) reason = 'MAX_PRICE_IMPACT';
      if (
        units(full.output) * BPS <
        units(plan.quote.output) * BigInt(10_000 - policy.maxSlippageBps)
      )
        reason = 'SLIPPAGE_EXCEEDED';
      quote = paperQuote(market, plan.side, plan.quote.input, policy, policy.fillBps);
    } catch {
      reason = 'EXECUTION_ROUTE_UNAVAILABLE';
    }
  }
  const simulatedFailure =
    reason === null &&
    policy.failEveryNthOrder > 0 &&
    plan.ordinal % policy.failEveryNthOrder === 0;
  if (simulatedFailure) reason = 'SIMULATED_TRANSACTION_FAILURE';
  if (
    plan.side === OrderSide.BUY &&
    quote !== null &&
    units(quote.input) + units(quote.cashFee) > units(portfolio.current_cash)
  ) {
    reason = 'INSUFFICIENT_CASH_AT_FILL';
  }
  const positions = await paperRows<PositionRow>(
    db,
    'SELECT * FROM positions WHERE portfolio_id=$1 AND token_mint=$2',
    [portfolio.portfolio_id, market.tokenMint]
  );
  const position = positions[0];
  if (
    plan.side === OrderSide.SELL &&
    (position === undefined || quote === null || units(quote.input) > units(position.open_quantity))
  ) {
    reason = reason ?? 'POSITION_QUANTITY_UNAVAILABLE';
  }
  let cashDelta = 0n;
  if (reason !== null || quote === null) {
    if (simulatedFailure)
      cashDelta = -minimum(units(policy.fixedFeeCash), units(portfolio.current_cash));
    await orderEvent(
      db,
      pending.order_id,
      'failed',
      { reason: reason ?? 'NO_FILL', cashFee: amount(-cashDelta) },
      now
    );
    if (plan.side === OrderSide.SELL && position !== undefined) {
      await positionEvent(
        db,
        position,
        pending.order_id,
        null,
        'exit_failed',
        '0',
        { reason: reason ?? 'NO_FILL' },
        now
      );
    }
  } else {
    const status = units(quote.input) < units(plan.quote.input) ? 'partially_filled' : 'filled';
    const fillId = generateFillId();
    const buying = plan.side === OrderSide.BUY;
    cashDelta = buying
      ? -(units(quote.input) + units(quote.cashFee))
      : units(quote.output) - units(quote.cashFee);
    await paperRows(
      db,
      `INSERT INTO order_fills (fill_id,order_id,status,input_asset,input_quantity,
      output_asset,output_quantity,fees,venue,recorded_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        fillId,
        pending.order_id,
        status,
        buying ? market.cashAsset : market.tokenMint,
        quote.input,
        buying ? market.tokenMint : market.cashAsset,
        quote.output,
        JSON.stringify([{ asset: market.cashAsset, quantity: quote.cashFee }]),
        market.venue,
        now,
      ]
    );
    await orderEvent(
      db,
      pending.order_id,
      status,
      {
        snapshotId: market.snapshotId,
        unfilledRemainder: 'cancelled',
        simulation: 'constant_product_v1',
      },
      now
    );
    if (buying) {
      const quantity = units(quote.output);
      const cost = -cashDelta;
      const unitCost = (cost * UNIT) / quantity;
      const stop = (unitCost * BigInt(10_000 - policy.initialStopLossBps)) / BPS;
      const opened = await paperRows<PositionRow>(
        db,
        `INSERT INTO positions
        (position_id,portfolio_id,token_mint,original_quantity,open_quantity,cost_basis,
         high_water_price,state,protection_config,stop_price,updated_at,created_at)
        VALUES ($1,$2,$3,$4,$4,$5,$6,'open',$7,$8,$9,$9) RETURNING *`,
        [
          generatePositionId(),
          portfolio.portfolio_id,
          market.tokenMint,
          quote.output,
          amount(cost),
          amount(unitCost),
          JSON.stringify(portfolio.strategy_snapshot.protection.config),
          amount(stop),
          now,
        ]
      );
      const current = opened[0];
      if (current === undefined) throw new Error('Position insert did not return');
      await positionEvent(
        db,
        current,
        pending.order_id,
        fillId,
        'opened',
        quote.output,
        { reason: plan.reason },
        now
      );
    } else if (position !== undefined) {
      const remaining = units(position.open_quantity) - units(quote.input);
      const unitCost = (units(position.cost_basis) * UNIT) / units(position.original_quantity);
      const profitable =
        units(quote.output) - units(quote.cashFee) > (units(quote.input) * unitCost) / UNIT;
      const reserve = reserveQuantity(position, policy, market.tokenDecimals);
      const arm = plan.profitTaking && profitable && remaining > 0n;
      const proposedStop = arm ? (unitCost * units(policy.postProfitStopMultiple)) / UNIT : 0n;
      const stop =
        units(position.stop_price ?? '0') > proposedStop
          ? units(position.stop_price ?? '0')
          : proposedStop;
      const state =
        remaining === 0n ? 'closed' : remaining <= reserve && arm ? 'moonbag' : 'partially_closed';
      const updated = await paperRows<PositionRow>(
        db,
        `UPDATE positions SET open_quantity=$2,state=$3,
        stop_price=$4,updated_at=$5 WHERE position_id=$1 RETURNING *`,
        [position.position_id, amount(remaining), state, amount(stop), now]
      );
      const current = updated[0];
      if (current === undefined) throw new Error('Position update did not return');
      await positionEvent(
        db,
        current,
        pending.order_id,
        fillId,
        remaining === 0n ? 'closed' : 'partial_sell',
        `-${quote.input}`,
        { reason: plan.reason },
        now
      );
      if (arm)
        await positionEvent(
          db,
          current,
          pending.order_id,
          fillId,
          'moonbag_armed',
          '0',
          { reserveQuantity: amount(reserve), stopPrice: amount(stop) },
          now
        );
    }
  }
  const balance = units(portfolio.current_cash) + cashDelta;
  portfolio.current_cash = amount(balance);
  await paperRows(
    db,
    'UPDATE paper_portfolios SET current_cash=$2,updated_at=$3 WHERE portfolio_id=$1',
    [portfolio.portfolio_id, portfolio.current_cash, now]
  );
  await paperRows(
    db,
    `INSERT INTO paper_execution_results (order_id,snapshot_id,cash_delta,settled_at)
    VALUES ($1,$2,$3,$4)`,
    [
      pending.order_id,
      market.snapshotId,
      cashDelta < 0n ? `-${amount(-cashDelta)}` : amount(cashDelta),
      now,
    ]
  );
}

async function exitIntent(
  db: Executor,
  position: PositionRow,
  market: PaperMarketSnapshot,
  policy: PaperPolicy,
  portfolio: PortfolioRow,
  now: Date
): Promise<{ quantity: string; reason: string; profitTaking: boolean } | null> {
  const open = units(position.open_quantity);
  if (market.criticalRisk)
    return { quantity: amount(open), reason: 'CRITICAL_RISK_EXIT', profitTaking: false };
  if (now.getTime() - market.occurredAt.getTime() > policy.maxMarketAgeMs) return null;
  let netValue: bigint;
  try {
    const quote = paperQuote(market, OrderSide.SELL, position.open_quantity, policy);
    netValue = units(quote.output) - units(quote.cashFee);
  } catch {
    return null;
  }
  const unitPrice = (netValue * UNIT) / open;
  if (position.stop_price !== null && unitPrice <= units(position.stop_price)) {
    return { quantity: amount(open), reason: 'PROTECTIVE_STOP', profitTaking: false };
  }
  const high =
    units(position.high_water_price) > unitPrice ? units(position.high_water_price) : unitPrice;
  if (high > units(position.high_water_price)) {
    const updated = await paperRows<PositionRow>(
      db,
      'UPDATE positions SET high_water_price=$2,updated_at=$3 WHERE position_id=$1 RETURNING *',
      [position.position_id, amount(high), now]
    );
    if (updated[0] !== undefined)
      await positionEvent(
        db,
        updated[0],
        null,
        null,
        'stop_changed',
        '0',
        { reason: 'HIGH_WATER_UPDATED' },
        now
      );
  }
  const exit = portfolio.strategy_snapshot.exit.policyId;
  const entryUnit = (units(position.cost_basis) * UNIT) / units(position.original_quantity);
  const target = (entryUnit * units(policy.takeProfitMultiple)) / UNIT;
  const timeExit =
    exit === 'X07' && now.getTime() - position.created_at.getTime() >= policy.maxHoldingMs;
  const fixed = (exit === 'X01' || exit === 'X02') && unitPrice >= target;
  const trailing =
    exit === 'X03' &&
    high >= target &&
    unitPrice * BPS <= high * BigInt(10_000 - policy.trailingBps);
  if (!timeExit && !fixed && !trailing) return null;
  const profitable = netValue > (open * entryUnit) / UNIT;
  const quantity = profitable
    ? open > reserveQuantity(position, policy, market.tokenDecimals)
      ? open - reserveQuantity(position, policy, market.tokenDecimals)
      : 0n
    : open;
  return quantity === 0n
    ? null
    : {
        quantity: amount(quantity),
        reason: timeExit ? 'TIME_EXIT' : trailing ? 'TRAILING_PROFIT' : 'PROFIT_TARGET',
        profitTaking: profitable,
      };
}

function reserveQuantity(position: PositionRow, policy: PaperPolicy, decimals: number): bigint {
  const quantum = 10n ** BigInt(18 - decimals);
  const numerator = units(position.original_quantity) * BigInt(policy.moonbagBps);
  return ((numerator + BPS * quantum - 1n) / (BPS * quantum)) * quantum;
}

async function positionSource(db: Executor, positionId: string): Promise<SignalEnvelope> {
  const rows = await paperRows<{ source_signal_id: SignalId }>(
    db,
    `SELECT d.source_signal_id FROM position_events p
    JOIN orders o ON o.order_id=p.order_id JOIN strategy_decisions d ON d.decision_id=o.decision_id
    WHERE p.position_id=$1 AND p.event_type='opened' ORDER BY p.occurred_at LIMIT 1`,
    [positionId]
  );
  if (rows[0] === undefined) throw new ValidationError('Position has no originating decision');
  return loadSignal(db, rows[0].source_signal_id);
}

async function orderEvent(
  db: Executor,
  orderId: string,
  status: string,
  detail: unknown,
  now: Date
): Promise<void> {
  await paperRows(
    db,
    `INSERT INTO order_events (order_event_id,order_id,status,detail,occurred_at)
    VALUES ($1,$2,$3,$4,$5)`,
    [generateOrderEventId(), orderId, status, JSON.stringify(detail), now]
  );
}

async function positionEvent(
  db: Executor,
  position: PositionRow,
  orderId: string | null,
  fillId: string | null,
  type: string,
  quantityDelta: string,
  detail: Readonly<Record<string, unknown>>,
  now: Date
): Promise<void> {
  await paperRows(
    db,
    `INSERT INTO position_events (position_event_id,position_id,order_id,fill_id,
    event_type,quantity_delta,detail,occurred_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [
      generatePositionEventId(),
      position.position_id,
      orderId,
      fillId,
      type,
      quantityDelta,
      JSON.stringify({ ...detail, projection: position }),
      now,
    ]
  );
}

function asObject(value: unknown): Readonly<Record<string, unknown>> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}
