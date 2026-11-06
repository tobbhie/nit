import { randomBytes, randomUUID } from 'node:crypto';
import { DatabasePool, Executor, clientExecutor } from '@nit/database-public';
import { PortfolioId, SignalId, generatePositionEventId } from '@nit/domain';
import { CandidateActionState } from '@nit/signals';
import {
  Clock,
  OrderSide,
  PaperMarketFeed,
  PaperMarketSnapshot,
  PaperQuote,
  PostgresPaperTrader,
  StrategySnapshot,
  paperPolicy,
  paperQuote,
  paperRows,
  units,
  amount,
  BPS,
  validatePaperMarket,
} from '@nit/trading';
import { CandidateReport, ResolvedCandidate, integer, nonempty, object } from './types';
import { TelegramFailure, TelegramMessageBody, TelegramTransport } from './telegram';

export interface ConsoleOperator {
  readonly userId: string;
  readonly chatId: string;
  readonly role: 'viewer' | 'trader';
  readonly portfolioIds: readonly PortfolioId[];
}
export interface ConsoleOptions {
  readonly botName: string;
  readonly operators: readonly ConsoleOperator[];
  readonly actionTtlMs: number;
  readonly maxOutbox: number;
}
interface Action {
  action_token: string;
  bot_name: string;
  user_id: string;
  chat_id: string;
  resolution_id: string | null;
  token_mint: string | null;
  portfolio_id: PortfolioId | null;
  position_id: string | null;
  kind: string;
  expires_at: Date;
  status: string;
  quote: QuoteContext | null;
  message_id: string | null;
  result: { text: string } | null;
}
interface QuoteContext {
  readonly quote: PaperQuote;
  readonly configHash: string;
  readonly sourceSignalId: SignalId | null;
  readonly expiresAt: string;
  readonly snapshotId: string;
}
interface Portfolio {
  portfolio_id: PortfolioId;
  cash_asset: string;
  strategy_snapshot: StrategySnapshot;
  mode: string;
  status: string;
  current_cash: string;
}

export class TelegramDecisionConsole {
  constructor(
    private readonly db: DatabasePool,
    private readonly api: TelegramTransport,
    private readonly feed: PaperMarketFeed,
    private readonly trader: PostgresPaperTrader,
    private readonly clock: Clock,
    private readonly options: ConsoleOptions
  ) {
    nonempty(options.botName, 100);
    integer(options.actionTtlMs, 1);
    integer(options.maxOutbox, 64, 2048);
    if (options.operators.length === 0 || options.operators.length > 16)
      throw new Error('Configure 1 to 16 Telegram operators');
    const identities = new Set<string>();
    for (const operator of options.operators) {
      if (
        !/^\d+$/.test(operator.userId) ||
        !/^-?\d+$/.test(operator.chatId) ||
        !['viewer', 'trader'].includes(operator.role)
      )
        throw new Error('Invalid Telegram operator');
      if (operator.portfolioIds.length > 32) throw new Error('Too many operator portfolios');
      const key = operator.userId;
      if (identities.has(key)) throw new Error('Configure one chat per Telegram operator');
      identities.add(key);
    }
  }

  async handleUpdate(raw: unknown): Promise<void> {
    const update = object(raw);
    const updateId = integer(update['update_id'], 0);
    const callback =
      update['callback_query'] === undefined ? null : object(update['callback_query']);
    if (
      (callback !== null && callback['message'] === undefined) ||
      (callback === null && update['message'] === undefined)
    ) {
      if (callback !== null && typeof callback['id'] === 'string')
        await this.api.acknowledge(callback['id'], 'Unsupported action context.');
      await this.finishUpdate(updateId);
      return;
    }
    const message =
      callback === null
        ? update['message'] === undefined
          ? null
          : object(update['message'])
        : object(callback['message']);
    const from = callback === null ? message?.['from'] : callback['from'];
    if (from === undefined || from === null) {
      await this.finishUpdate(updateId);
      return;
    }
    const userId = telegramId(object(from)['id']);
    const chatId = telegramId(object(message?.['chat'])['id']);
    const operator = this.options.operators.find(
      (entry) => entry.userId === userId && entry.chatId === chatId
    );
    if (operator === undefined) {
      if (callback !== null)
        await this.api.acknowledge(nonempty(callback['id']), 'Not authorized.');
      await this.finishUpdate(updateId);
      return;
    }
    if (callback !== null) {
      const callbackId = nonempty(callback['id']);
      let response: string;
      let completed = false;
      try {
        response = await this.callback(
          operator,
          nonempty(callback['data'], 64),
          callbackId,
          integer(message?.['message_id'], 1)
        );
        completed = true;
      } catch (error) {
        response = error instanceof Error ? error.message : 'Action failed';
      }
      await this.api.acknowledge(callbackId, response);
      // Keyboard cleanup is cosmetic; database state rejects obsolete/replayed actions.
      if (completed)
        try {
          await this.api.removeKeyboard(chatId, integer(message?.['message_id'], 1));
        } catch {
          /* Keep authoritative action state. */
        }
      await this.finishUpdate(updateId);
      return;
    }
    const text = typeof message?.['text'] === 'string' ? message['text'] : '';
    try {
      await this.command(operator, text, updateId);
    } catch (error) {
      await this.db
        .transaction(async (client) => {
          const db = clientExecutor(client);
          await this.enqueue(
            db,
            operator,
            { text: error instanceof Error ? error.message : 'Command failed' },
            `update:${updateId}:error`
          );
          await paperRows(
            db,
            `INSERT INTO telegram_update_receipts(bot_name,update_id,handled_at)
          VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
            [this.options.botName, updateId, this.clock.now()]
          );
        })
        .then((result) => {
          if (result.kind === 'failure') throw result.error;
        });
    }
  }

  async poll(signal?: AbortSignal): Promise<void> {
    const rows = await paperRows<{ next_offset: string }>(
      this.db,
      'SELECT next_offset FROM telegram_poll_offsets WHERE bot_name=$1',
      [this.options.botName]
    );
    const offset = Number(rows[0]?.next_offset ?? 0);
    const updates = await this.api.updates(offset, signal);
    let last = offset;
    for (const raw of updates) {
      const id = integer(object(raw)['update_id'], 0);
      if (id < last) continue;
      await this.handleUpdate(raw);
      last = id + 1;
      await paperRows(
        this.db,
        `INSERT INTO telegram_poll_offsets(bot_name,next_offset) VALUES ($1,$2)
        ON CONFLICT(bot_name) DO UPDATE SET next_offset=GREATEST(telegram_poll_offsets.next_offset,EXCLUDED.next_offset)`,
        [this.options.botName, last]
      );
    }
  }

  async notify(): Promise<void> {
    const now = this.clock.now();
    for (const operator of this.options.operators) {
      const scenarios = await paperRows<{ report: CandidateReport }>(
        this.db,
        `
        SELECT c.report FROM candidate_scenarios c WHERE c.report IS NOT NULL AND c.expires_at>=$1
          AND NOT EXISTS (SELECT 1 FROM console_notification_receipts n WHERE n.bot_name=$2 AND n.user_id=$3
            AND n.reference_id='candidate:'||(c.report->>'narrativeId')||':'||(c.report->>'materialHash'))
        ORDER BY c.next_check_at,c.narrative_id LIMIT 8`,
        [now, this.options.botName, operator.userId]
      );
      for (const { report } of scenarios) {
        const result = await this.db.transaction(async (client) => {
          const db = clientExecutor(client);
          const key = `candidate:${report.narrativeId}:${report.materialHash}`;
          const inserted = await paperRows(
            db,
            `INSERT INTO console_notification_receipts(bot_name,user_id,reference_id,handled_at)
            VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING reference_id`,
            [this.options.botName, operator.userId, key, now]
          );
          if (inserted.length > 0) await this.reportMessages(db, operator, report, key);
        });
        if (result.kind === 'failure') throw result.error;
      }
      const events = await paperRows<{
        position_event_id: string;
        position_id: string;
        event_type: string;
        quantity_delta: string;
        detail: unknown;
        token_mint: string;
        portfolio_id: PortfolioId;
      }>(
        this.db,
        `
        SELECT e.*,p.token_mint,p.portfolio_id FROM position_events e JOIN positions p ON p.position_id=e.position_id
        LEFT JOIN console_notification_receipts n ON n.bot_name=$1 AND n.user_id=$2 AND n.reference_id='position:'||e.position_event_id::text
        WHERE p.portfolio_id=ANY($3::uuid[]) AND n.reference_id IS NULL ORDER BY e.occurred_at,e.position_event_id LIMIT 8`,
        [this.options.botName, operator.userId, operator.portfolioIds]
      );
      for (const event of events) {
        const result = await this.db.transaction(async (client) => {
          const db = clientExecutor(client);
          const key = `position:${event.position_event_id}`;
          const inserted = await paperRows(
            db,
            `INSERT INTO console_notification_receipts(bot_name,user_id,reference_id,handled_at)
            VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING reference_id`,
            [this.options.botName, operator.userId, key, now]
          );
          if (inserted.length === 0) return;
          const detail = object(event.detail);
          const projection = detail['projection'] === undefined ? {} : object(detail['projection']);
          const body =
            `Paper position: ${event.event_type}\n${event.token_mint}\nPortfolio: ${event.portfolio_id}\n` +
            `Quantity change: ${event.quantity_delta}\nRemaining: ${String(projection['open_quantity'] ?? 'unknown')}\n` +
            `Stop: ${String(projection['stop_price'] ?? 'unarmed')}\nReason: ${String(detail['reason'] ?? event.event_type)}`;
          const actions = await this.positionActions(
            db,
            operator,
            event.portfolio_id,
            event.position_id,
            event.token_mint
          );
          await this.enqueue(db, operator, { text: body, buttons: actions }, key);
        });
        if (result.kind === 'failure') throw result.error;
      }
    }
  }

  async flush(limit = 4): Promise<void> {
    integer(limit, 1, 16);
    const now = this.clock.now();
    await paperRows(
      this.db,
      `UPDATE telegram_outbox SET status='uncertain',last_error='Delivery process interrupted'
      WHERE bot_name=$1 AND status='sending' AND lease_until<$2`,
      [this.options.botName, now]
    );
    for (let index = 0; index < limit; index++) {
      const claim = await this.db.transaction(async (client) => {
        const db = clientExecutor(client);
        return paperRows<{
          outbox_id: string;
          user_id: string;
          chat_id: string;
          body: TelegramMessageBody;
          action_tokens: string[];
          attempts: number;
        }>(
          db,
          `
          UPDATE telegram_outbox SET status='sending',attempts=attempts+1,lease_until=$2
          WHERE outbox_id=(SELECT outbox_id FROM telegram_outbox WHERE bot_name=$1 AND status='pending'
            AND available_at<=$3 ORDER BY created_at,outbox_id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *`,
          [this.options.botName, new Date(now.getTime() + 60_000), now]
        );
      });
      if (claim.kind === 'failure') throw claim.error;
      const row = claim.value[0];
      if (row === undefined) return;
      const operator = this.options.operators.find(
        (entry) => entry.userId === row.user_id && entry.chatId === row.chat_id
      );
      if (operator === undefined) {
        await paperRows(
          this.db,
          "UPDATE telegram_outbox SET status='failed',last_error='Operator revoked' WHERE outbox_id=$1",
          [row.outbox_id]
        );
        continue;
      }
      try {
        const messageId = await this.api.send(row.body);
        const result = await this.db.transaction(async (client) => {
          const db = clientExecutor(client);
          await paperRows(
            db,
            "UPDATE telegram_outbox SET status='sent',message_id=$2 WHERE outbox_id=$1",
            [row.outbox_id, messageId]
          );
          await paperRows(
            db,
            'UPDATE telegram_actions SET message_id=$2 WHERE action_token=ANY($1::text[])',
            [row.action_tokens, messageId]
          );
        });
        if (result.kind === 'failure') throw result.error;
      } catch (error) {
        const known = error instanceof TelegramFailure && !error.ambiguous;
        const retry = known && error.retryAfterMs !== null && row.attempts < 3;
        await paperRows(
          this.db,
          `UPDATE telegram_outbox SET status=$2,last_error=$3,available_at=$4 WHERE outbox_id=$1`,
          [
            row.outbox_id,
            retry ? 'pending' : known ? 'failed' : 'uncertain',
            known ? 'Telegram rejected delivery' : 'Delivery outcome unknown',
            new Date(
              this.clock.now().getTime() +
                (error instanceof TelegramFailure ? (error.retryAfterMs ?? 1000) : 1000)
            ),
          ]
        );
      }
    }
  }

  async executeTrades(limit = 4): Promise<void> {
    integer(limit, 1, 16);
    const rows = await paperRows<Action>(
      this.db,
      `SELECT a.* FROM console_trade_requests r JOIN telegram_actions a USING(action_token)
      WHERE a.bot_name=$1 AND r.status='pending' ORDER BY r.created_at LIMIT $2`,
      [this.options.botName, limit]
    );
    for (const action of rows) {
      const handled = await this.db.transaction(async (client) => {
        const db = clientExecutor(client);
        const claimed = await paperRows(
          db,
          `SELECT 1 FROM console_trade_requests WHERE action_token=$1
        AND status='pending' FOR UPDATE SKIP LOCKED`,
          [action.action_token]
        );
        if (claimed.length === 0) return;
        const operator = this.options.operators.find(
          (entry) => entry.userId === action.user_id && entry.chatId === action.chat_id
        );
        let text: string;
        try {
          if (operator === undefined) throw new Error('Operator revoked');
          this.requireTrader(operator, action.portfolio_id);
          const portfolio = await this.portfolio(this.db, operator, action.portfolio_id);
          const orderKey = `operator-${action.kind === 'confirm_close' ? 'close' : 'buy'}:${action.action_token}`;
          let orders = await paperRows<{ order_id: string }>(
            this.db,
            'SELECT order_id FROM orders WHERE portfolio_id=$1 AND idempotency_key=$2',
            [portfolio.portfolio_id, orderKey]
          );
          if (orders.length === 0) {
            const quote = await this.validateQuote(action, portfolio);
            if (action.kind === 'confirm_buy') await this.currentCandidate(action);
            const result =
              action.kind === 'confirm_close'
                ? await this.trader.closePosition(
                    portfolio.portfolio_id,
                    nonempty(action.position_id),
                    action.action_token,
                    quote.market
                  )
                : await this.trader.tick(
                    portfolio.portfolio_id,
                    quote.market,
                    action.quote?.sourceSignalId ?? null,
                    action.action_token
                  );
            if (result.kind === 'failure') throw result.error;
            orders = await paperRows<{ order_id: string }>(
              this.db,
              'SELECT order_id FROM orders WHERE portfolio_id=$1 AND idempotency_key=$2',
              [portfolio.portfolio_id, orderKey]
            );
          }
          if (orders[0] === undefined)
            throw new Error(
              'Paper request did not pass the current strategy/risk gates or another order is pending'
            );
          text = `Paper order accepted: ${orders[0].order_id}. Settlement still requires the configured delay and market checks.`;
        } catch (error) {
          text = `Paper request rejected: ${error instanceof Error ? error.message : 'unknown failure'}`;
        }
        const changed = await paperRows(
          db,
          `UPDATE console_trade_requests SET status=$2,result=$3
          WHERE action_token=$1 AND status='pending' RETURNING action_token`,
          [
            action.action_token,
            text.startsWith('Paper order accepted') ? 'complete' : 'rejected',
            JSON.stringify({ text }),
          ]
        );
        if (changed.length > 0 && operator !== undefined)
          await this.enqueue(db, operator, { text }, `trade:${action.action_token}`);
      });
      if (handled.kind === 'failure') throw handled.error;
    }
  }

  private async callback(
    operator: ConsoleOperator,
    token: string,
    callbackId: string,
    messageId: number
  ): Promise<string> {
    if (!/^[A-Za-z0-9_-]{32}$/.test(token)) throw new Error('Unknown action');
    const rows = await paperRows<Action>(
      this.db,
      'SELECT * FROM telegram_actions WHERE action_token=$1',
      [token]
    );
    const action = rows[0];
    if (action === undefined) throw new Error('Unknown action');
    this.boundAction(operator, action, messageId);
    if (action.status !== 'pending')
      return action.result?.text ?? 'This action is already complete.';
    if (action.expires_at <= this.clock.now())
      throw new Error('Action expired; refresh /candidates or /positions.');
    this.requireTrader(operator, action.portfolio_id);
    let prepared: QuoteContext | null = null;
    if (action.kind === 'buy' || action.kind === 'close') {
      const portfolio = await this.portfolio(this.db, operator, action.portfolio_id);
      const candidate = action.kind === 'buy' ? await this.currentCandidate(action) : null;
      const source = action.kind === 'buy' ? await this.entrySource(candidate!, portfolio) : null;
      const market = await this.feed.latest(
        nonempty(action.token_mint),
        portfolio.cash_asset,
        this.clock.now()
      );
      if (market.kind === 'failure') throw market.error;
      validatePaperMarket(market.value, this.clock.now());
      if (
        market.value.tokenMint !== action.token_mint ||
        market.value.cashAsset !== portfolio.cash_asset
      )
        throw new Error('Market scope mismatch');
      const policy = paperPolicy(portfolio.strategy_snapshot);
      if (this.clock.now().getTime() - market.value.occurredAt.getTime() > policy.maxMarketAgeMs)
        throw new Error('Fresh market data required');
      if (
        !market.value.verified ||
        !market.value.tradable ||
        !market.value.sellable ||
        (market.value.criticalRisk && action.kind === 'buy')
      )
        throw new Error('Market risk gate blocked this quote');
      let input = policy.orderCash;
      if (action.kind === 'close') {
        const position = await this.position(this.db, operator, nonempty(action.position_id));
        input = String(position['open_quantity']);
        if (units(input) === 0n) throw new Error('Position is already closed');
      }
      const quote = paperQuote(
        market.value,
        action.kind === 'buy' ? OrderSide.BUY : OrderSide.SELL,
        input,
        policy
      );
      if (quote.impactBps > policy.maxImpactBps)
        throw new Error('Quote exceeds the price-impact gate');
      prepared = {
        quote,
        configHash: portfolio.strategy_snapshot.configHash,
        sourceSignalId: source,
        expiresAt: new Date(
          this.clock.now().getTime() + Math.min(this.options.actionTtlMs, policy.quoteTtlMs)
        ).toISOString(),
        snapshotId: market.value.snapshotId,
      };
    } else if (action.kind === 'confirm_buy' || action.kind === 'confirm_close') {
      const portfolio = await this.portfolio(this.db, operator, action.portfolio_id);
      await this.validateQuote(action, portfolio);
      if (action.kind === 'confirm_buy') await this.currentCandidate(action);
    }
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const locked = await paperRows<Action>(
        db,
        'SELECT * FROM telegram_actions WHERE action_token=$1 FOR UPDATE',
        [token]
      );
      const current = locked[0]!;
      this.boundAction(operator, current, messageId);
      if (current.status !== 'pending') return current.result?.text ?? 'Already handled.';
      if (current.expires_at <= this.clock.now()) throw new Error('Action expired.');
      if (current.resolution_id !== null) {
        const latest = await paperRows(
          db,
          `SELECT 1 FROM candidate_scenarios c JOIN candidate_resolution_reports r ON r.resolution_id=$1
            WHERE c.narrative_id=r.report->>'narrativeId' AND c.expires_at>$2
              AND c.report->>'materialHash'=r.report->>'materialHash' AND c.report->>'policyHash'=r.report->>'policyHash'
            FOR SHARE OF c`,
          [current.resolution_id, this.clock.now()]
        );
        if (latest.length !== 1) throw new Error('Candidate state changed; refresh /candidates.');
      }
      let text = 'Action saved.';
      if (prepared !== null) {
        const confirm = await this.action(db, operator, {
          ...current,
          kind: current.kind === 'buy' ? 'confirm_buy' : 'confirm_close',
          quote: prepared,
          expires_at: new Date(prepared.expiresAt),
        });
        text = 'Fresh paper quote prepared. Explicit confirmation is required.';
        await this.enqueue(
          db,
          operator,
          {
            text:
              `PAPER ${current.kind === 'buy' ? 'BUY' : 'CLOSE REMAINING'}\n${current.token_mint}\n` +
              `Portfolio: ${current.portfolio_id}\nInput: ${prepared.quote.input}\nEstimated output: ${prepared.quote.output}\n` +
              `Cash fees: ${prepared.quote.cashFee}\nImpact: ${prepared.quote.impactBps} bps\nExpires: ${prepared.expiresAt}\nNo fill has occurred.`,
            buttons: [{ text: 'Confirm paper request', callback_data: confirm }],
          },
          `quote:${token}`
        );
      } else if (current.kind === 'confirm_buy' || current.kind === 'confirm_close') {
        await paperRows(
          db,
          'INSERT INTO console_trade_requests(action_token,created_at) VALUES ($1,$2) ON CONFLICT DO NOTHING',
          [token, this.clock.now()]
        );
        text = 'Paper request confirmed; execution will recheck the quote and risk gates.';
      } else if (current.kind === 'watch' || current.kind === 'discard') {
        const report = await this.reportForAction(current, db);
        await paperRows(db, 'SELECT 1 FROM paper_portfolios WHERE portfolio_id=$1 FOR UPDATE', [
          current.portfolio_id,
        ]);
        await paperRows(
          db,
          `INSERT INTO console_candidate_preferences(bot_name,user_id,narrative_id,token_mint,state,rationale,updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(bot_name,user_id,narrative_id,token_mint) DO UPDATE
          SET state=EXCLUDED.state,rationale=EXCLUDED.rationale,updated_at=EXCLUDED.updated_at`,
          [
            this.options.botName,
            operator.userId,
            report.narrativeId,
            current.token_mint,
            current.kind,
            'Authenticated Telegram inline action',
            this.clock.now(),
          ]
        );
        await paperRows(
          db,
          `INSERT INTO paper_candidate_exclusions(portfolio_id,narrative_id,token_mint,operator_user_id,created_at)
          VALUES ($1,$2,$3,$4,$5) ON CONFLICT(portfolio_id,narrative_id,token_mint) DO UPDATE SET created_at=EXCLUDED.created_at,
            operator_user_id=EXCLUDED.operator_user_id`,
          [
            current.portfolio_id,
            report.narrativeId,
            current.token_mint,
            operator.userId,
            this.clock.now(),
          ]
        );
        text =
          current.kind === 'watch'
            ? 'Watching this candidate; new entries are withheld until an explicit Buy.'
            : 'Discarded for new entries in this portfolio. Evidence and existing positions are retained.';
      } else if (current.kind === 'stop')
        text = `Use /stop ${current.position_id} <new stop price>. Only tighter protection is accepted.`;
      else if (current.kind === 'view') {
        const position = await this.position(db, operator, nonempty(current.position_id));
        text = `Position ${current.position_id}: remaining ${String(position['open_quantity'])}; stop ${String(position['stop_price'] ?? 'unarmed')}.`;
      }
      await paperRows(
        db,
        "UPDATE telegram_actions SET status='used',result=$2 WHERE action_token=$1",
        [token, JSON.stringify({ text })]
      );
      await paperRows(
        db,
        `INSERT INTO telegram_action_receipts(bot_name,callback_id,action_token,result,handled_at)
        VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
        [this.options.botName, callbackId, token, JSON.stringify({ text }), this.clock.now()]
      );
      if (prepared === null && current.kind !== 'confirm_buy' && current.kind !== 'confirm_close')
        await this.enqueue(db, operator, { text }, `action:${token}`);
      return text;
    });
    if (result.kind === 'failure') throw result.error;
    return result.value;
  }

  private async command(operator: ConsoleOperator, text: string, updateId: number): Promise<void> {
    const [command = '', arg = '', value = ''] = text.trim().split(/\s+/);
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const inserted = await paperRows(
        db,
        `INSERT INTO telegram_update_receipts(bot_name,update_id,handled_at)
        VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING update_id`,
        [this.options.botName, updateId, this.clock.now()]
      );
      if (inserted.length === 0) return;
      let response: string;
      if (command === '/candidates') {
        const rows = await paperRows<{ report: CandidateReport }>(
          db,
          'SELECT report FROM candidate_scenarios WHERE narrative_id=$1',
          [arg]
        );
        if (rows[0]?.report === undefined || rows[0].report === null)
          throw new Error('No candidate report for that narrative.');
        await this.reportMessages(db, operator, rows[0].report, `command:${updateId}`);
        return;
      } else if (command === '/use') {
        this.requireTrader(operator, arg as PortfolioId);
        await this.portfolio(db, operator, arg as PortfolioId);
        await paperRows(
          db,
          `INSERT INTO console_operator_preferences(bot_name,user_id,chat_id,portfolio_id)
          VALUES ($1,$2,$3,$4) ON CONFLICT(bot_name,user_id,chat_id) DO UPDATE SET portfolio_id=EXCLUDED.portfolio_id`,
          [this.options.botName, operator.userId, operator.chatId, arg]
        );
        response = `Selected paper portfolio ${arg}. Refresh candidate buttons with /candidates <narrative>.`;
      } else if (command === '/strategies' || command === '/portfolio' || command === '/strategy') {
        const ids = command === '/strategy' ? [arg] : operator.portfolioIds;
        if (ids.some((id) => !operator.portfolioIds.includes(id as PortfolioId)))
          throw new Error('Portfolio not authorized.');
        const rows = await paperRows<{
          portfolio_id: string;
          current_cash: string;
          cash_asset: string;
          status: string;
          strategy_snapshot: StrategySnapshot;
        }>(
          db,
          `SELECT p.*,r.status,c.strategy_snapshot FROM paper_portfolios p JOIN strategy_runs r USING(run_id)
           JOIN paper_run_configs c USING(run_id) WHERE p.portfolio_id=ANY($1::uuid[]) ORDER BY p.portfolio_id`,
          [ids]
        );
        response =
          rows.length === 0
            ? 'No configured paper portfolios.'
            : rows
                .map(
                  (row) =>
                    `${row.portfolio_id}\n${row.strategy_snapshot.displayId}\n` +
                    `PAPER ${row.status}; cash ${row.current_cash} ${row.cash_asset}`
                )
                .join('\n\n');
      } else if (command === '/positions') {
        const positions = await paperRows<{
          position_id: string;
          portfolio_id: PortfolioId;
          token_mint: string;
          open_quantity: string;
          stop_price: string | null;
        }>(
          db,
          'SELECT * FROM positions WHERE portfolio_id=ANY($1::uuid[]) AND open_quantity>0 ORDER BY updated_at DESC LIMIT 16',
          [operator.portfolioIds]
        );
        if (positions.length === 0) response = 'No open paper positions.';
        else {
          for (const position of positions)
            await this.enqueue(
              db,
              operator,
              {
                text:
                  `${position.token_mint}\nPosition: ${position.position_id}\n` +
                  `Remaining: ${position.open_quantity}\nStop: ${position.stop_price ?? 'unarmed'}`,
                buttons: await this.positionActions(
                  db,
                  operator,
                  position.portfolio_id,
                  position.position_id,
                  position.token_mint
                ),
              },
              `positions:${updateId}:${position.position_id}`
            );
          return;
        }
      } else if (command === '/pause' || command === '/resume') {
        this.requireTrader(operator, arg as PortfolioId);
        await this.portfolio(db, operator, arg as PortfolioId);
        const changed = await paperRows(
          db,
          `UPDATE strategy_runs r SET status=$2,updated_at=$3
          FROM paper_portfolios p WHERE p.run_id=r.run_id AND p.portfolio_id=$1
          AND r.mode='forward_paper' AND r.status IN ('planned','running') RETURNING r.run_id`,
          [arg, command === '/pause' ? 'planned' : 'running', this.clock.now()]
        );
        if (changed.length === 0) throw new Error('Run cannot change entry state.');
        response =
          command === '/pause'
            ? 'New entries paused; protective position handling remains active.'
            : 'Paper entries resumed under the pinned strategy.';
      } else if (command === '/stop') {
        this.requireTrader(operator, null);
        const position = await this.position(db, operator, arg);
        const portfolioId = String(position['portfolio_id']) as PortfolioId;
        await this.portfolio(db, operator, portfolioId);
        await paperRows(db, 'SELECT 1 FROM paper_portfolios WHERE portfolio_id=$1 FOR UPDATE', [
          portfolioId,
        ]);
        const current = await this.position(db, operator, arg);
        const next = units(value);
        if (
          units(String(current['open_quantity'])) === 0n ||
          next <= units(String(current['stop_price'] ?? '0'))
        )
          throw new Error('Stop must tighten an open position.');
        const changed = await paperRows<Record<string, unknown>>(
          db,
          'UPDATE positions SET stop_price=$2,updated_at=$3 WHERE position_id=$1 RETURNING *',
          [arg, amount(next), this.clock.now()]
        );
        await paperRows(
          db,
          `INSERT INTO position_events(position_event_id,position_id,event_type,quantity_delta,detail,occurred_at)
          VALUES ($1,$2,'stop_changed',0,$3,$4)`,
          [
            generatePositionEventId(),
            arg,
            JSON.stringify({
              reason: 'AUTHENTICATED_STOP_TIGHTENED',
              operatorUserId: operator.userId,
              projection: changed[0],
            }),
            this.clock.now(),
          ]
        );
        response = 'Paper stop tightened. A trigger still requires an executable sell.';
      } else if (command === '/outbox') {
        const rows = await paperRows<{ status: string; count: string }>(
          db,
          'SELECT status,COUNT(*)::text AS count FROM telegram_outbox WHERE bot_name=$1 AND user_id=$2 GROUP BY status',
          [this.options.botName, operator.userId]
        );
        response = rows.map((row) => `${row.status}: ${row.count}`).join('\n') || 'No deliveries.';
      } else if (
        /^\/(backtest|backtest-event|backtest-compare|scenario|strategy-backtest|matrix|run-report)$/.test(
          command
        )
      ) {
        response =
          'Historical replay/report commands are not implemented yet; no backtest has been started.';
      } else
        response =
          '/candidates <narrative> · /strategies · /strategy <portfolio> · /use <portfolio> · /portfolio · /positions · /pause <portfolio> · /resume <portfolio> · /stop <position> <price> · /outbox\nPaper only; no live execution.';
      await this.enqueue(db, operator, { text: response.slice(0, 4000) }, `command:${updateId}`);
    });
    if (result.kind === 'failure') throw result.error;
  }

  private async reportMessages(
    db: Executor,
    operator: ConsoleOperator,
    report: CandidateReport,
    key: string
  ): Promise<void> {
    await this.enqueue(
      db,
      operator,
      {
        text:
          `NIT: ${clean(report.title)}\nNarrative strength: ${report.narrativeStrengthBps}/10000\n` +
          `${report.candidates.length} competing mint(s); ${report.state}\n${report.reasonCodes.join(', ')}\n` +
          `Resolution ${report.version}: ${report.resolutionId}\nScores describe source evidence, not profit probabilities.`,
      },
      `${key}:summary`
    );
    const selected = await this.selectedPortfolio(db, operator);
    for (const candidate of report.candidates) {
      const preference = await paperRows<{ state: string }>(
        db,
        `SELECT state FROM console_candidate_preferences
        WHERE bot_name=$1 AND user_id=$2 AND narrative_id=$3 AND token_mint=$4`,
        [this.options.botName, operator.userId, report.narrativeId, candidate.tokenMint]
      );
      const buttons: { text: string; callback_data: string }[] = [];
      if (operator.role === 'trader' && selected !== null) {
        for (const [kind, label] of [
          ['buy', 'Buy (paper)'],
          ['watch', 'Watch'],
          ['discard', 'Discard'],
        ] as const) {
          const token = await this.action(db, operator, {
            kind,
            resolution_id: report.resolutionId,
            token_mint: candidate.tokenMint,
            portfolio_id: selected,
            position_id: null,
            quote: null,
            expires_at: new Date(this.clock.now().getTime() + this.options.actionTtlMs),
          });
          buttons.push({ text: label, callback_data: token });
        }
      }
      await this.enqueue(
        db,
        operator,
        {
          text:
            `#${candidate.rank} ${clean(candidate.ticker)} — ${clean(candidate.name)}\n${candidate.tokenMint}\n` +
            `Launch: ${candidate.launchTime ?? 'unknown'}\nLink: ${candidate.linkConfidenceBps}/10000; narrative: ${report.narrativeStrengthBps}/10000\n` +
            `Independent wallet groups: ${candidate.independentWallets}; market safety: ${candidate.marketStatus}\n` +
            `State: ${candidate.state}; preference: ${preference[0]?.state ?? 'none'}\nReasons: ${candidate.reasonCodes.join(', ')}\n` +
            `Portfolio: ${selected ?? 'select using /use'}\nMarket USD liquidity/volume: unavailable in this report.`,
          buttons,
        },
        `${key}:${candidate.tokenMint}`
      );
    }
  }

  private async validateQuote(
    action: Action,
    portfolio: Portfolio
  ): Promise<{ market: PaperMarketSnapshot }> {
    const quote = action.quote;
    if (
      quote === null ||
      new Date(quote.expiresAt) <= this.clock.now() ||
      quote.configHash !== portfolio.strategy_snapshot.configHash
    )
      throw new Error('Quote expired or strategy changed.');
    const market = await this.feed.latest(
      nonempty(action.token_mint),
      portfolio.cash_asset,
      this.clock.now()
    );
    if (market.kind === 'failure') throw market.error;
    validatePaperMarket(market.value, this.clock.now());
    if (
      market.value.tokenMint !== action.token_mint ||
      market.value.cashAsset !== portfolio.cash_asset
    )
      throw new Error('Market scope mismatch');
    const policy = paperPolicy(portfolio.strategy_snapshot);
    if (
      this.clock.now().getTime() - market.value.occurredAt.getTime() > policy.maxMarketAgeMs ||
      !market.value.verified ||
      !market.value.sellable ||
      !market.value.tradable
    )
      throw new Error('Fresh executable market evidence required.');
    const sell = action.kind === 'confirm_close';
    if (!sell && market.value.criticalRisk) throw new Error('Critical risk blocks entry.');
    if (sell) {
      const position = await this.position(
        this.db,
        {
          userId: action.user_id,
          chatId: action.chat_id,
          role: 'trader',
          portfolioIds: [portfolio.portfolio_id],
        },
        nonempty(action.position_id)
      );
      if (units(String(position['open_quantity'])) !== units(quote.quote.input))
        throw new Error('Position quantity changed; request another quote.');
    }
    const current = paperQuote(
      market.value,
      sell ? OrderSide.SELL : OrderSide.BUY,
      quote.quote.input,
      policy
    );
    if (
      current.impactBps > policy.maxImpactBps ||
      units(current.output) * BPS <
        units(quote.quote.output) * BigInt(10000 - policy.maxSlippageBps)
    )
      throw new Error('Quote moved beyond the impact/slippage limits.');
    return { market: market.value };
  }
  private async currentCandidate(action: Action): Promise<ResolvedCandidate> {
    const report = await this.reportForAction(action);
    const current = await paperRows(
      this.db,
      `SELECT 1 FROM candidate_scenarios c JOIN candidate_resolution_reports r ON r.resolution_id=$1
        WHERE c.narrative_id=r.report->>'narrativeId' AND c.expires_at>$2
          AND c.report->>'materialHash'=r.report->>'materialHash' AND c.report->>'policyHash'=r.report->>'policyHash'`,
      [report.resolutionId, this.clock.now()]
    );
    if (current.length !== 1) throw new Error('Candidate state changed; refresh /candidates.');
    const candidate = report.candidates.find((item) => item.tokenMint === action.token_mint);
    if (
      candidate === undefined ||
      candidate.state === CandidateActionState.BLOCKED ||
      candidate.state === CandidateActionState.MANUAL_REVIEW
    )
      throw new Error('Candidate requires watch/manual review before a paper buy.');
    const preferences = await paperRows<{ state: string }>(
      this.db,
      `SELECT state FROM console_candidate_preferences WHERE bot_name=$1 AND user_id=$2
      AND narrative_id=$3 AND token_mint=$4`,
      [this.options.botName, action.user_id, report.narrativeId, action.token_mint]
    );
    if (preferences[0]?.state === 'discard')
      throw new Error('Candidate is discarded; change it to Watch first.');
    return candidate;
  }
  private async entrySource(candidate: ResolvedCandidate, portfolio: Portfolio): Promise<SignalId> {
    const entry = portfolio.strategy_snapshot.entry.policyId;
    if (entry === 'E02') {
      const rows = await paperRows<{ signal_id: SignalId }>(
        this.db,
        `SELECT signal_id FROM signals WHERE source='wallet_tracker' AND kind='wallet_convergence'
        AND token_mint=$1 AND available_at<=$2 ORDER BY available_at DESC LIMIT 1`,
        [candidate.tokenMint, this.clock.now()]
      );
      if (!candidate.walletConfirmed || rows[0] === undefined)
        throw new Error('Wallet-only strategy has no applicable convergence signal');
      return rows[0].signal_id;
    }
    const source =
      entry === 'E01' ||
      (entry === 'E04' && candidate.state !== CandidateActionState.TRADE_ELIGIBLE)
        ? candidate.narrativeSignalId
        : candidate.candidateSignalId;
    if (source === undefined) throw new Error('Candidate has no applicable strategy signal');
    const rows = await paperRows<{ payload: Record<string, unknown> }>(
      this.db,
      'SELECT payload FROM signals WHERE signal_id=$1',
      [source]
    );
    if (
      (entry === 'E01' && rows[0]?.payload['eligible'] !== true) ||
      (entry === 'E03' && candidate.state !== CandidateActionState.TRADE_ELIGIBLE)
    )
      throw new Error('Selected strategy does not qualify this candidate');
    return source;
  }
  private async reportForAction(action: Action, db: Executor = this.db): Promise<CandidateReport> {
    const rows = await paperRows<{ report: CandidateReport }>(
      db,
      'SELECT report FROM candidate_resolution_reports WHERE resolution_id=$1',
      [action.resolution_id]
    );
    if (rows[0] === undefined) throw new Error('Candidate report not found');
    return rows[0].report;
  }
  private requireTrader(operator: ConsoleOperator, portfolio: PortfolioId | null): void {
    if (
      operator.role !== 'trader' ||
      (portfolio !== null && !operator.portfolioIds.includes(portfolio))
    )
      throw new Error('Trading action not authorized');
  }
  private boundAction(operator: ConsoleOperator, action: Action, messageId: number): void {
    if (
      action.bot_name !== this.options.botName ||
      action.user_id !== operator.userId ||
      action.chat_id !== operator.chatId ||
      action.message_id === null ||
      Number(action.message_id) !== messageId
    )
      throw new Error('Action is not bound to this user, chat and message');
  }
  private async selectedPortfolio(
    db: Executor,
    operator: ConsoleOperator
  ): Promise<PortfolioId | null> {
    const rows = await paperRows<{ portfolio_id: PortfolioId | null }>(
      db,
      'SELECT portfolio_id FROM console_operator_preferences WHERE bot_name=$1 AND user_id=$2 AND chat_id=$3',
      [this.options.botName, operator.userId, operator.chatId]
    );
    const selected = rows[0]?.portfolio_id ?? operator.portfolioIds[0] ?? null;
    return selected !== null && operator.portfolioIds.includes(selected) ? selected : null;
  }
  private async portfolio(
    db: Executor,
    operator: ConsoleOperator,
    id: PortfolioId | null
  ): Promise<Portfolio> {
    if (id === null || !operator.portfolioIds.includes(id))
      throw new Error('Portfolio not authorized');
    const rows = await paperRows<Portfolio>(
      db,
      `SELECT p.*,r.mode,r.status,c.strategy_snapshot FROM paper_portfolios p
      JOIN strategy_runs r USING(run_id) JOIN paper_run_configs c USING(run_id) WHERE p.portfolio_id=$1`,
      [id]
    );
    if (rows[0] === undefined || rows[0].mode !== 'forward_paper')
      throw new Error('Only configured forward-paper portfolios are actionable');
    return rows[0];
  }
  private async position(
    db: Executor,
    operator: ConsoleOperator,
    id: string
  ): Promise<Record<string, unknown>> {
    const rows = await paperRows<Record<string, unknown>>(
      db,
      'SELECT * FROM positions WHERE position_id=$1 AND portfolio_id=ANY($2::uuid[])',
      [id, operator.portfolioIds]
    );
    if (rows[0] === undefined) throw new Error('Position not authorized or not found');
    return rows[0];
  }
  private async positionActions(
    db: Executor,
    operator: ConsoleOperator,
    portfolio: PortfolioId,
    positionId: string,
    mint: string
  ): Promise<{ text: string; callback_data: string }[]> {
    const actions: { text: string; callback_data: string }[] = [];
    if (operator.role !== 'trader') return actions;
    for (const [kind, text] of [
      ['close', 'Close Remaining'],
      ['stop', 'Adjust Stop'],
      ['view', 'View Position'],
    ]) {
      const token = await this.action(db, operator, {
        kind: kind!,
        resolution_id: null,
        portfolio_id: portfolio,
        position_id: positionId,
        token_mint: mint,
        quote: null,
        expires_at: new Date(this.clock.now().getTime() + this.options.actionTtlMs),
      });
      actions.push({ text: text!, callback_data: token });
    }
    return actions;
  }
  private async action(
    db: Executor,
    operator: ConsoleOperator,
    input: Pick<
      Action,
      | 'kind'
      | 'resolution_id'
      | 'token_mint'
      | 'portfolio_id'
      | 'position_id'
      | 'quote'
      | 'expires_at'
    >
  ): Promise<string> {
    const token = randomBytes(24).toString('base64url');
    await paperRows(
      db,
      `INSERT INTO telegram_actions(action_token,bot_name,user_id,chat_id,resolution_id,token_mint,portfolio_id,
      position_id,kind,expires_at,quote,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [
        token,
        this.options.botName,
        operator.userId,
        operator.chatId,
        input.resolution_id,
        input.token_mint,
        input.portfolio_id,
        input.position_id,
        input.kind,
        input.expires_at,
        input.quote === null ? null : JSON.stringify(input.quote),
        this.clock.now(),
      ]
    );
    return token;
  }
  private async enqueue(
    db: Executor,
    operator: ConsoleOperator,
    body: { text: string; buttons?: readonly { text: string; callback_data: string }[] },
    key: string
  ): Promise<void> {
    const pending = await paperRows<{ count: string }>(
      db,
      "SELECT COUNT(*)::text AS count FROM telegram_outbox WHERE bot_name=$1 AND status IN ('pending','sending')",
      [this.options.botName]
    );
    if (Number(pending[0]?.count ?? 0) >= this.options.maxOutbox)
      throw new Error('Telegram outbox is full');
    const message: TelegramMessageBody = {
      chat_id: operator.chatId,
      text: body.text.slice(0, 4000),
      ...(body.buttons === undefined || body.buttons.length === 0
        ? {}
        : { reply_markup: { inline_keyboard: [body.buttons] } }),
    };
    await paperRows(
      db,
      `INSERT INTO telegram_outbox(outbox_id,bot_name,user_id,chat_id,body,action_tokens,dedupe_key,available_at,created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8) ON CONFLICT(bot_name,dedupe_key) DO NOTHING`,
      [
        randomUUID(),
        this.options.botName,
        operator.userId,
        operator.chatId,
        JSON.stringify(message),
        body.buttons?.map((button) => button.callback_data) ?? [],
        `${operator.userId}:${operator.chatId}:${key}`,
        this.clock.now(),
      ]
    );
  }
  private async finishUpdate(id: number): Promise<void> {
    await paperRows(
      this.db,
      `INSERT INTO telegram_update_receipts(bot_name,update_id,handled_at)
    VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
      [this.options.botName, id, this.clock.now()]
    );
  }
}

function telegramId(value: unknown): string {
  if (typeof value === 'number' && Number.isSafeInteger(value)) return String(value);
  throw new Error('Invalid Telegram identity');
}
function clean(value: string): string {
  return Array.from(value, (char) =>
    char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127 ? ' ' : char
  )
    .join('')
    .slice(0, 200);
}
