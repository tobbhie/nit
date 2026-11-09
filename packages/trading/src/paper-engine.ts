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
import { SignalEnvelope, SignalKind, parseSignalEnvelope, evidenceHash } from '@nit/signals';
import { BPS, UNIT, amount, minimum, units } from './fixed';
import { PaperMarketSnapshot, PaperQuote, paperQuote, validatePaperMarket } from './paper-market';
import { PaperPolicy, paperPolicy } from './paper-policy';
import { paperRows } from './paper-store';
import { Clock, OrderSide } from './runtime';
import { StrategySnapshot } from './strategy';
import { MarketDiscoveryEvidence, marketDiscoveryEvidence } from './market-discovery-evidence';

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
  episode_id: string | null;
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

  /** Authenticated console requests still use the same sell quote/risk/ledger path. */
  async closePosition(
    portfolioId: PortfolioId,
    positionId: string,
    requestId: string,
    market: PaperMarketSnapshot
  ): Promise<Result<PaperTickResult, Error>> {
    const now = this.clock.now();
    return this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      validatePaperMarket(market, now);
      const portfolios = await paperRows<PortfolioRow>(
        db,
        `SELECT p.*,c.strategy_snapshot,r.config_hash,
        r.status,r.mode,r.period_start,r.period_end FROM paper_portfolios p
        JOIN strategy_runs r ON r.run_id=p.run_id JOIN paper_run_configs c ON c.run_id=r.run_id
        WHERE p.portfolio_id=$1 FOR UPDATE OF p`,
        [portfolioId]
      );
      const portfolio = portfolios[0];
      if (
        portfolio === undefined ||
        !['historical', 'forward_paper'].includes(portfolio.mode) ||
        portfolio.cash_asset !== market.cashAsset
      )
        throw new ValidationError('Invalid paper close scope');
      if (
        requestId.trim() === '' ||
        requestId.length > 100 ||
        portfolio.config_hash !== portfolio.strategy_snapshot.configHash
      )
        throw new ValidationError('Invalid paper close request');
      if (portfolio.paper_last_tick_at !== null && portfolio.paper_last_tick_at > now)
        throw new ValidationError('Paper clock cannot regress');
      await paperRows(
        db,
        'UPDATE paper_portfolios SET paper_last_tick_at=$2 WHERE portfolio_id=$1',
        [portfolioId, now]
      );
      const positions = await paperRows<PositionRow>(
        db,
        'SELECT * FROM positions WHERE position_id=$1 AND portfolio_id=$2',
        [positionId, portfolioId]
      );
      const position = positions[0];
      if (
        position === undefined ||
        position.token_mint !== market.tokenMint ||
        units(position.open_quantity) === 0n
      )
        throw new ValidationError('Position is no longer open');
      const pending = await paperRows(
        db,
        `SELECT 1 FROM orders o WHERE portfolio_id=$1 AND token_mint=$2
        AND NOT EXISTS(SELECT 1 FROM paper_execution_results x WHERE x.order_id=o.order_id)`,
        [portfolioId, market.tokenMint]
      );
      if (pending.length > 0) return { decisions: 0, orders: 0, settled: 0 };
      await captureMarket(db, market);
      const source = await positionSource(db, positionId);
      return planOrder(
        db,
        portfolio,
        market,
        paperPolicy(portfolio.strategy_snapshot),
        source,
        OrderSide.SELL,
        position.open_quantity,
        'AUTHENTICATED_CLOSE',
        false,
        now,
        `operator-close:${requestId}`
      );
    });
  }

  async tick(
    portfolioId: PortfolioId,
    market: PaperMarketSnapshot,
    sourceSignalId: SignalId | null = null,
    confirmedRequestId?: string
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
        'SELECT * FROM positions WHERE portfolio_id=$1 AND token_mint=$2 ORDER BY (open_quantity>0) DESC,created_at DESC,position_id DESC LIMIT 1',
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
      const episodeId = asObject(signal.payload)['episodeId'];
      const alreadyUsed =
        typeof episodeId !== 'string'
          ? []
          : await paperRows(
              db,
              'SELECT 1 FROM positions WHERE portfolio_id=$1 AND token_mint=$2 AND episode_id=$3',
              [portfolioId, market.tokenMint, episodeId]
            );
      const reason =
        (alreadyUsed.length > 0 ? 'TOKEN_ALREADY_TRADED' : null) ??
        (await resolverRejection(db, signal, now)) ??
        entryRejection(portfolio, signal, market, policy, position, now);
      let entryNarrativeId = signal.canonicalNarrativeId;
      let authorized = false;
      if (confirmedRequestId !== undefined) {
        const actions = await paperRows<{ narrative_id: string }>(
          db,
          `SELECT cr.report->>'narrativeId' AS narrative_id FROM telegram_actions a JOIN console_trade_requests r USING(action_token)
          JOIN candidate_resolution_reports cr ON cr.resolution_id=a.resolution_id
          JOIN candidate_scenarios c ON c.narrative_id=cr.report->>'narrativeId'
          WHERE a.action_token=$1 AND a.portfolio_id=$2 AND a.token_mint=$3 AND a.kind='confirm_buy'
            AND a.status='used' AND r.status='pending' AND a.expires_at>$4
            AND a.quote->>'sourceSignalId'=$5 AND a.quote->>'configHash'=$6
            AND c.report->>'materialHash'=cr.report->>'materialHash'
            AND c.report->>'policyHash'=cr.report->>'policyHash'
            AND c.expires_at>$4
            AND NOT EXISTS (SELECT 1 FROM paper_candidate_exclusions e WHERE e.portfolio_id=a.portfolio_id
              AND e.narrative_id=cr.report->>'narrativeId' AND e.token_mint=a.token_mint AND e.created_at>a.created_at)`,
          [
            confirmedRequestId,
            portfolioId,
            market.tokenMint,
            now,
            sourceSignalId,
            portfolio.config_hash,
          ]
        );
        if (actions.length !== 1)
          throw new ValidationError('Confirmed request expired or candidate state changed');
        authorized = true;
        entryNarrativeId = actions[0]!.narrative_id;
      }
      const excluded =
        entryNarrativeId === undefined
          ? []
          : await paperRows(
              db,
              'SELECT 1 FROM paper_candidate_exclusions WHERE portfolio_id=$1 AND narrative_id=$2 AND token_mint=$3',
              [portfolioId, entryNarrativeId, market.tokenMint]
            );
      const riskReason =
        excluded.length > 0 && !authorized
          ? 'OPERATOR_WITHHELD'
          : (reason ?? (await capitalRejection(db, portfolio, market, policy, now)));
      const key =
        confirmedRequestId === undefined
          ? `entry:${signal.eventId}:${market.tokenMint}`
          : `operator-buy:${confirmedRequestId}`;
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
        if (authorized && created.orders > 0)
          await paperRows(
            db,
            'DELETE FROM paper_candidate_exclusions WHERE portfolio_id=$1 AND narrative_id=$2 AND token_mint=$3',
            [portfolioId, entryNarrativeId, market.tokenMint]
          );
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

async function resolverRejection(
  db: Executor,
  signal: SignalEnvelope,
  now: Date
): Promise<string | null> {
  if (!signal.sourceVersion.startsWith('resolver:')) return null;
  const rows = await paperRows(
    db,
    `SELECT 1 FROM candidate_scenarios c JOIN candidate_resolution_reports r
    ON r.resolution_id::text=$2 WHERE c.narrative_id=$1 AND c.expires_at>$3
      AND c.report->>'materialHash'=r.report->>'materialHash'
      AND c.report->>'policyHash'=r.report->>'policyHash'
      AND c.report->>'sourceSignalId'=(SELECT s.signal_id::text FROM signals s
        WHERE s.source='nit' AND s.kind='narrative' AND s.canonical_narrative_id=c.narrative_id
          AND s.source_version NOT LIKE 'resolver:%' AND s.available_at<=$3
        ORDER BY s.available_at DESC,s.signal_id DESC LIMIT 1) FOR SHARE OF c`,
    [signal.canonicalNarrativeId, asObject(signal.payload)['resolutionId'], now]
  );
  if (rows.length !== 1) return 'CANDIDATE_STATE_CHANGED';
  const featureId = asObject(signal.payload)['featureSnapshotId'];
  if (typeof featureId !== 'string') return null;
  const producer = await paperRows<{ version: string }>(
    db,
    `SELECT i.payload->'data'->>'extractorVersion' AS version
    FROM research_records f JOIN research_records i ON i.record_id=(f.payload->'data'->>'interpretationId')::uuid
    WHERE f.record_id=$1 AND f.kind='feature' AND i.kind='interpretation' AND f.available_at<=$2 AND i.available_at<=$2`,
    [featureId, now]
  );
  const version = producer[0]?.version;
  if (version === undefined || !version.startsWith('live-text-v1:')) return null;
  const healthy = await paperRows(
    db,
    `SELECT 1 FROM nit_text_health WHERE runtime_key='live-text-v1'
    AND policy_hash=$1 AND checked_at<=$2 AND expires_at>$2 AND NOT incomplete`,
    [version.slice('live-text-v1:'.length), now]
  );
  return healthy.length === 1 ? null : 'NARRATIVE_PRODUCER_UNAVAILABLE';
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
  const payload = asObject(signal.payload);
  const episode = typeof payload['episodeId'] === 'string' ? payload['episodeId'] : null;
  if (
    position !== undefined &&
    (units(position.open_quantity) > 0n || episode === null || position.episode_id === episode)
  )
    return 'TOKEN_ALREADY_TRADED';
  if (episode !== null) {
    // A valid episode and immutable decision snapshot are checked before persistence.
    if (
      payload['featureSnapshotId'] === undefined &&
      portfolio.strategy_snapshot.entry.policyId !== 'E00' &&
      !(
        portfolio.strategy_snapshot.entry.policyId === 'E02' &&
        typeof payload['walletSourceSignalId'] === 'string'
      )
    )
      return 'EPISODE_FEATURE_REQUIRED';
  }
  if (signal.tokenMint !== market.tokenMint) return 'SIGNAL_TOKEN_MISMATCH';
  const qualification =
    payload['qualification'] === undefined ? null : asObject(payload['qualification']);
  const baseline =
    signal.source === 'market' &&
    signal.kind === SignalKind.MARKET &&
    payload['marketBaselineEligible'] === true;
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
    payload['walletConfirmed'] === true &&
    (payload['independentSourceCount'] === undefined ||
      (typeof payload['independentSourceCount'] === 'number' &&
        Number.isSafeInteger(payload['independentSourceCount']) &&
        payload['independentSourceCount'] >= policy.minimumIndependentWallets)) &&
    (qualification === null || qualification['E03'] === true);
  const entry = portfolio.strategy_snapshot.entry.policyId;
  if (
    !(entry === 'E00'
      ? baseline
      : entry === 'E01'
        ? narrative
        : entry === 'E02'
          ? wallet
          : entry === 'E03'
            ? combined
            : qualification === null
              ? narrative || wallet || combined
              : qualification['E04'] === true)
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
  const decisionId = rows[0]?.decision_id;
  if (decisionId !== undefined) {
    const payload = asObject(signal.payload);
    const featureId =
      typeof payload['featureSnapshotId'] === 'string' ? payload['featureSnapshotId'] : null;
    const episodeId = typeof payload['episodeId'] === 'string' ? payload['episodeId'] : null;
    let feature: unknown = null;
    let marketDiscovery: MarketDiscoveryEvidence | null = null;
    if (episodeId !== null) {
      const episodes = await paperRows<{
        payload: { data: { tokenMints: string[]; universeMode?: 'open' } };
      }>(
        db,
        "SELECT payload FROM research_records WHERE record_id=$1 AND kind='episode' AND available_at<=$2",
        [episodeId, now]
      );
      if (
        episodes[0]?.payload.data.universeMode === 'open' &&
        featureId === null &&
        (type === 'abstain' || ['E00', 'E02'].includes(portfolio.strategy_snapshot.entry.policyId))
      )
        marketDiscovery = await marketDiscoveryEvidence(db, signal.tokenMint ?? '', now);
      if (
        episodes[0] === undefined ||
        !(
          episodes[0].payload.data.tokenMints.includes(signal.tokenMint ?? '') ||
          (episodes[0].payload.data.universeMode === 'open' &&
            (featureId !== null || marketDiscovery !== null))
        )
      )
        throw new ValidationError('Decision episode is unavailable or wrong mint');
    }
    if (featureId !== null) {
      const found = await paperRows<{ payload: Record<string, unknown> }>(
        db,
        `SELECT payload FROM research_records WHERE record_id=$1 AND kind='feature' AND available_at<=$2`,
        [featureId, now]
      );
      if (found[0] === undefined) throw new ValidationError('Decision feature is unavailable');
      const data = asObject(found[0].payload['data']);
      if (data['tokenMint'] !== signal.tokenMint || data['episodeId'] !== episodeId)
        throw new ValidationError('Decision feature scope mismatch');
      const evidenceMask =
        payload['evidenceMask'] === undefined ? null : asObject(payload['evidenceMask']);
      feature =
        evidenceMask === null
          ? found[0].payload
          : {
              ...found[0].payload,
              data: {
                ...data,
                ...(evidenceMask['wallet'] === true
                  ? {}
                  : {
                      wallet: 'unavailable',
                      relationship: 'unknown',
                      independentWallets: 0,
                      coverageIds: [],
                    }),
                ...(evidenceMask['narrative'] === true
                  ? {}
                  : {
                      narrative: 'unavailable',
                      linkage: 'unavailable',
                      narrativeStrengthBps: 0,
                      linkConfidenceBps: 0,
                      artifactIds: [],
                    }),
              },
            };
    }
    const snapshot = {
      signalId: signal.eventId,
      availableAt: signal.availableAt.toISOString(),
      payload: signal.payload,
      feature,
      marketDiscovery,
      policyHash: portfolio.config_hash,
    };
    await paperRows(
      db,
      `INSERT INTO decision_feature_snapshots(decision_id,feature_record_id,episode_id,evidence_mask,snapshot,content_hash)
      VALUES ($1,$2,$3,$4,$5,$6)`,
      [
        decisionId,
        featureId,
        episodeId,
        JSON.stringify(payload['evidenceMask'] ?? { legacy: true }),
        JSON.stringify(snapshot),
        evidenceHash(snapshot),
      ]
    );
  }
  return decisionId ?? null;
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
        'SELECT * FROM positions WHERE portfolio_id=$1 AND token_mint=$2 ORDER BY (open_quantity>0) DESC,created_at DESC,position_id DESC LIMIT 1',
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
  const excluded =
    plan.side !== OrderSide.BUY
      ? []
      : await paperRows(
          db,
          `SELECT 1 FROM orders o
    JOIN strategy_decisions d ON d.decision_id=o.decision_id JOIN signals s ON s.signal_id=d.source_signal_id
    LEFT JOIN telegram_actions a ON o.idempotency_key='operator-buy:'||a.action_token
    LEFT JOIN candidate_resolution_reports r ON r.resolution_id=a.resolution_id
    JOIN paper_candidate_exclusions x ON x.portfolio_id=o.portfolio_id
      AND x.narrative_id=COALESCE(s.canonical_narrative_id,r.report->>'narrativeId') AND x.token_mint=o.token_mint WHERE o.order_id=$1`,
          [pending.order_id]
        );
  let reason =
    now > pending.expires_at
      ? 'QUOTE_EXPIRED'
      : marketRejection(market, policy, now, plan.side === OrderSide.BUY);
  if (excluded.length > 0) reason = 'OPERATOR_WITHHELD';
  if (plan.side === OrderSide.BUY) {
    const changed = await paperRows(
      db,
      `SELECT 1 FROM orders o JOIN telegram_actions a
      ON o.idempotency_key='operator-buy:'||a.action_token JOIN candidate_resolution_reports r ON r.resolution_id=a.resolution_id
      WHERE o.order_id=$1 AND NOT EXISTS (SELECT 1 FROM candidate_scenarios c
        WHERE c.narrative_id=r.report->>'narrativeId' AND c.expires_at>$2
          AND c.report->>'materialHash'=r.report->>'materialHash' AND c.report->>'policyHash'=r.report->>'policyHash')`,
      [pending.order_id, now]
    );
    if (changed.length > 0) reason = 'CANDIDATE_STATE_CHANGED';
    const decisions = await paperRows<{ source_signal_id: SignalId }>(
      db,
      `SELECT d.source_signal_id FROM orders o
      JOIN strategy_decisions d USING(decision_id) WHERE o.order_id=$1`,
      [pending.order_id]
    );
    const source = decisions[0]?.source_signal_id;
    if (source !== undefined)
      reason = (await resolverRejection(db, await loadSignal(db, source), now)) ?? reason;
  }
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
    'SELECT * FROM positions WHERE portfolio_id=$1 AND token_mint=$2 ORDER BY (open_quantity>0) DESC,created_at DESC,position_id DESC LIMIT 1',
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
         high_water_price,state,protection_config,stop_price,updated_at,created_at,episode_id)
        VALUES ($1,$2,$3,$4,$4,$5,$6,'open',$7,$8,$9,$9,$10) RETURNING *`,
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
          asObject(
            (
              await loadSignal(
                db,
                (
                  await paperRows<{ source_signal_id: SignalId }>(
                    db,
                    'SELECT source_signal_id FROM strategy_decisions WHERE decision_id=(SELECT decision_id FROM orders WHERE order_id=$1)',
                    [pending.order_id]
                  )
                )[0]!.source_signal_id
              )
            ).payload
          )['episodeId'] ?? null,
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
