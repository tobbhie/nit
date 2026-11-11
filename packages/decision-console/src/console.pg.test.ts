import { randomUUID } from 'node:crypto';
import { DatabasePool, PostgresSignalStore, PostgresResearchStore } from '@nit/database-public';
import { TestDatabase, createTestDatabase, hasTestDatabase } from '@nit/database-public/testing';
import { Chain, PortfolioId, generateSignalId } from '@nit/domain';
import { SignalKind, contentHash, ResearchRecord } from '@nit/signals';
import { ExecutionMode, PostgresPaperRegistry, PostgresPaperTrader, paperRows } from '@nit/trading';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { CandidateResolver } from './resolver';
import { TelegramDecisionConsole } from './console';
import { CandidateReport } from './types';
import { TelegramFailure } from './telegram';
import {
  FixtureFeed,
  FixtureTelegram,
  START,
  TOKEN,
  TOKEN_B,
  POLICY,
  market,
  strategy,
} from './console.fixtures';

describe.skipIf(!hasTestDatabase())(
  'candidate resolution and authenticated Telegram paper console',
  () => {
    let harness: TestDatabase;
    let db: DatabasePool;
    let now: Date;
    let feed: FixtureFeed;
    let api: FixtureTelegram;
    let resolver: CandidateResolver;
    let consoleService: TelegramDecisionConsole;
    let portfolioId: PortfolioId;
    let botName: string;
    let updateId = 0;
    const clock = { now: (): Date => now };
    beforeAll(async () => {
      harness = await createTestDatabase('decision_console');
      db = harness.db;
      for (const mint of [TOKEN, TOKEN_B])
        await paperRows(
          db,
          `INSERT INTO tokens
      (token_id,chain,address,ticker,name,launch_time,launch_source,discovered_at,raw_payload)
      VALUES ($1,'solana',$2,'TEST','Synthetic test token',$3,'fixture',$3,'{}')`,
          [randomUUID(), mint, START]
        );
    });
    afterAll(async () => harness.teardown());
    beforeEach(async () => {
      now = new Date(START.getTime() + 2000);
      feed = new FixtureFeed();
      feed.value = market(now);
      api = new FixtureTelegram();
      botName = randomUUID();
      resolver = new CandidateResolver(db, feed, clock, POLICY, 'SOL');
      const run = await new PostgresPaperRegistry(db).createRun({
        strategy: strategy(),
        mode: ExecutionMode.FORWARD_PAPER,
        codeHash: 'a'.repeat(64),
        codeCommitSha: 'console-fixture',
        periodStart: START,
        periodEnd: new Date(START.getTime() + 86400000),
        cashAsset: 'SOL',
        initialCash: '100',
      });
      if (run.kind === 'failure') throw run.error;
      portfolioId = run.value.portfolioId;
      consoleService = new TelegramDecisionConsole(
        db,
        api,
        feed,
        new PostgresPaperTrader(db, clock),
        clock,
        {
          botName,
          operators: [
            { userId: '1', chatId: '10', role: 'trader', portfolioIds: [portfolioId] },
            { userId: '2', chatId: '20', role: 'viewer', portfolioIds: [portfolioId] },
          ],
          actionTtlMs: 10000,
          maxOutbox: 256,
        }
      );
    });
    async function narrative(
      candidates = [{ tokenMint: TOKEN, linkConfidenceBps: 9000, reasonCodes: ['EXACT_LINK'] }],
      id = randomUUID(),
      available = START
    ): Promise<string> {
      const result = await new PostgresSignalStore(db).append({
        eventId: generateSignalId(),
        source: 'nit',
        sourceVersion: 'fixture',
        kind: SignalKind.NARRATIVE,
        chain: Chain.SOLANA,
        canonicalNarrativeId: id,
        occurredAt: START,
        observedAt: START,
        availableAt: available,
        origin: { kind: 'provider', provider: 'fixture-console', referenceId: randomUUID() },
        evidenceIds: [],
        payload: {
          schemaVersion: 1,
          title: 'Synthetic narrative',
          narrativeStrengthBps: 9000,
          candidates,
        },
      });
      if (result.kind === 'failure') throw result.error;
      return id;
    }
    async function resolve(id: string): Promise<CandidateReport> {
      const result = await resolver.resolve(id);
      if (result.kind === 'failure') throw result.error;
      return result.value;
    }
    async function command(text: string, user = 1, chat = 10): Promise<void> {
      await consoleService.handleUpdate({
        update_id: ++updateId,
        message: { message_id: updateId, from: { id: user }, chat: { id: chat }, text },
      });
    }
    async function report(id: string): Promise<void> {
      await command(`/candidates ${id}`);
      await consoleService.flush(16);
    }
    function button(label: string): { token: string; messageId: number } {
      for (const message of [...api.sent].reverse())
        for (const row of message.body.reply_markup?.inline_keyboard ?? [])
          for (const button of row)
            if (button.text === label)
              return { token: button.callback_data, messageId: message.messageId };
      throw new Error(`Missing button ${label}`);
    }
    async function click(
      action: { token: string; messageId: number },
      user = 1,
      chat = 10
    ): Promise<void> {
      await consoleService.handleUpdate({
        update_id: ++updateId,
        callback_query: {
          id: `cb${updateId}`,
          from: { id: user },
          data: action.token,
          message: { message_id: action.messageId, chat: { id: chat } },
        },
      });
    }
    async function count(table: string): Promise<number> {
      const rows = await paperRows<{ count: string }>(
        db,
        `SELECT COUNT(*)::text AS count FROM ${table} WHERE portfolio_id=$1`,
        [portfolioId]
      );
      return Number(rows[0]?.count);
    }

    function monitoring(): TelegramDecisionConsole {
      return new TelegramDecisionConsole(db, api, feed, new PostgresPaperTrader(db, clock), clock, {
        botName,
        monitorOnly: true,
        actionTtlMs: 10000,
        maxOutbox: 64,
        operators: [{ userId: '1', chatId: '10', role: 'viewer', portfolioIds: [] }],
      });
    }
    it('provides authenticated monitoring status and delivery checks without mutating paper runs', async () => {
      consoleService = monitoring();
      await command('/status', 2);
      await consoleService.flush();
      expect(api.sent).toHaveLength(0);
      await command('/status');
      await consoleService.flush();
      expect(api.sent.at(-1)?.body.text).toContain('Mode: MONITOR ONLY');
      expect(api.sent.at(-1)?.body.text).toContain('Database: reachable');
      const before = await paperRows(db, 'SELECT status FROM strategy_runs ORDER BY run_id');
      await command(`/pause ${portfolioId}`);
      await consoleService.flush();
      expect(api.sent.at(-1)?.body.text).toContain('not authorized');
      expect(await paperRows(db, 'SELECT status FROM strategy_runs ORDER BY run_id')).toEqual(
        before
      );
      await command('/test-alert');
      await consoleService.flush();
      expect(api.sent.at(-1)?.body.text).toContain('NIT TEST ALERT');
      await consoleService.handleUpdate({
        update_id: updateId,
        message: {
          message_id: updateId,
          from: { id: 1 },
          chat: { id: 10 },
          text: '/test-alert',
        },
      });
      const sent = api.sent.length;
      await consoleService.flush();
      expect(api.sent).toHaveLength(sent);
      expect(await count('orders')).toBe(0);
    });
    it('does not process previously confirmed paper requests in monitoring mode', async () => {
      const id = await narrative();
      await resolve(id);
      await report(id);
      await click(button('Buy (paper)'));
      await consoleService.flush(16);
      const confirm = button('Confirm paper request');
      await click(confirm);
      consoleService = monitoring();
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(0);
      expect(
        await paperRows(db, 'SELECT status FROM console_trade_requests WHERE action_token=$1', [
          confirm.token,
        ])
      ).toEqual([{ status: 'pending' }]);
    });
    it('refuses trader permissions or paper portfolio scope in monitoring mode', () => {
      for (const operator of [
        { userId: '1', chatId: '10', role: 'trader' as const, portfolioIds: [] },
        { userId: '1', chatId: '10', role: 'viewer' as const, portfolioIds: [portfolioId] },
      ])
        expect(
          () =>
            new TelegramDecisionConsole(db, api, feed, new PostgresPaperTrader(db, clock), clock, {
              botName,
              monitorOnly: true,
              actionTtlMs: 10000,
              maxOutbox: 64,
              operators: [operator],
            })
        ).toThrow('Monitoring requires viewers');
    });
    it('preserves competing mints and separates narrative qualification from wallet confirmation', async () => {
      const id = await narrative([
        { tokenMint: TOKEN, linkConfidenceBps: 9000, reasonCodes: ['LINK'] },
        { tokenMint: TOKEN_B, linkConfidenceBps: 9000, reasonCodes: ['LINK'] },
      ]);
      const result = await resolve(id);
      expect(result.state).toBe('manual_review');
      expect(result.candidates).toHaveLength(2);
      expect(
        result.candidates.every(
          (candidate) => candidate.narrativeConfirmed && !candidate.walletConfirmed
        )
      ).toBe(true);
      const signals = await paperRows<{ payload: { eligible: boolean } }>(
        db,
        "SELECT payload FROM signals WHERE canonical_narrative_id=$1 AND source_version='resolver:v2' AND kind='narrative'",
        [id]
      );
      expect(signals.every((signal) => !signal.payload.eligible)).toBe(true);
    });
    it('coalesces, versions material changes and does not churn identical evidence', async () => {
      const id = await narrative(undefined, undefined, now);
      const first = await resolve(id);
      expect(first.reasonCodes).toContain('CORRELATION_WINDOW_OPEN');
      expect((await resolve(id)).version).toBe(1);
      now = new Date(now.getTime() + 1000);
      const second = await resolve(id);
      expect(second.version).toBe(2);
      const source = await paperRows<{ payload: { eligible: boolean } }>(
        db,
        'SELECT payload FROM signals WHERE signal_id=$1',
        [second.candidates[0]!.narrativeSignalId]
      );
      expect(source[0]?.payload.eligible).toBe(true);
      expect(second.candidates[0]?.walletConfirmed).toBe(false);
      expect((await resolve(id)).version).toBe(2);
      const scenario = await paperRows<{ next_check_at: Date }>(
        db,
        'SELECT next_check_at FROM candidate_scenarios WHERE narrative_id=$1',
        [id]
      );
      expect(scenario[0]!.next_check_at.getTime()).toBeGreaterThan(now.getTime());
    });
    it('publishes provenance-backed candidates with immutable feature snapshots and explicit wallet coverage', async () => {
      const store = new PostgresResearchStore(db);
      const artifactId = randomUUID();
      const episodeId = randomUUID();
      const interpretationId = randomUUID();
      const narrativeId = randomUUID();
      const common = {
        version: 1,
        occurredAt: START.toISOString(),
        observedAt: START.toISOString(),
        availableAt: START.toISOString(),
        producerVersion: 'fixture:v1',
        evidenceIds: [],
      };
      const records: ResearchRecord[] = [
        {
          ...common,
          recordId: artifactId,
          scope: artifactId,
          kind: 'artifact',
          data: {
            provider: 'fixture',
            sourceId: artifactId,
            authorId: 'fixture-author',
            contentHash: contentHash('Explicit public text'),
            text: 'Explicit public text',
            sourceUrl: 'https://example.test/source',
            derivativeOf: null,
            media: [],
          },
        },
        {
          ...common,
          recordId: episodeId,
          scope: episodeId,
          kind: 'episode',
          data: {
            narrativeId,
            tokenMints: [TOKEN],
            episodeType: 'launch',
            previousEpisodeId: null,
            originArtifactIds: [artifactId],
          },
        },
        {
          ...common,
          recordId: interpretationId,
          scope: interpretationId,
          kind: 'interpretation',
          data: {
            episodeId,
            title: 'Captured text',
            status: 'confirmed',
            narrativeStrengthBps: 9000,
            artifactIds: [artifactId],
            extractorVersion: 'explicit-links:v1',
            modelVersion: null,
            promptVersion: null,
            historicalMode: 'captured',
            candidates: [
              {
                tokenMint: TOKEN,
                linkConfidenceBps: 9000,
                status: 'confirmed',
                disposition: 'candidate',
                reasonCodes: ['EXPLICIT_MINT'],
              },
            ],
          },
        },
      ];
      for (const record of records) {
        const written = await store.append(record);
        if (written.kind === 'failure') throw written.error;
      }
      const published = await store.publishInterpretation(interpretationId, START);
      if (published.kind === 'failure') throw published.error;
      const first = await resolve(narrativeId);
      expect(first.candidates[0]?.evidence?.wallet).toBe('unavailable');
      expect(first.candidates[0]?.qualification?.E01).toBe(true);
      const feature = await store.at(first.candidates[0]!.featureSnapshotId!, now);
      expect(feature?.kind).toBe('feature');
      expect(feature?.kind === 'feature' && feature.data.artifactIds).toEqual([artifactId]);
      const coverage: ResearchRecord = {
        ...common,
        recordId: randomUUID(),
        scope: randomUUID(),
        kind: 'coverage',
        availableAt: now.toISOString(),
        data: {
          family: 'wallet',
          tokenMint: TOKEN,
          status: 'confirmed',
          windowStart: new Date(now.getTime() - POLICY.walletWindowMs).toISOString(),
          windowEnd: now.toISOString(),
          coveredWallets: ['fixture-watched-wallet'],
        },
      };
      const saved = await store.append(coverage);
      if (saved.kind === 'failure') throw saved.error;
      const updated = await resolve(narrativeId);
      expect(updated.candidates[0]?.evidence?.wallet).toBe('observed_nonconfirming');
      expect(updated.version).toBe(first.version + 1);
      await new PostgresPaperTrader(db, clock).tick(
        portfolioId,
        market(now),
        updated.candidates[0]!.narrativeSignalId
      );
      expect(await count('orders')).toBe(1);
    });
    it('retains unknown safety separately from explicit risk blocks and rejects future evidence', async () => {
      const id = await narrative();
      feed.value = market(START);
      now = new Date(START.getTime() + 20000);
      expect((await resolve(id)).candidates[0]?.marketStatus).toBe('unknown');
      feed.value = { ...market(now), criticalRisk: true };
      expect((await resolve(id)).state).toBe('blocked');
      const future = await narrative(undefined, undefined, new Date(now.getTime() + 1000));
      expect(await resolver.resolve(future)).toMatchObject({ kind: 'failure' });
    });
    it('counts exact-mint wallet evidence and routes linked funders to manual review', async () => {
      const id = await narrative([
        { tokenMint: TOKEN_B, linkConfidenceBps: 9000, reasonCodes: ['LINK'] },
      ]);
      const wallets = [`a${randomUUID()}`, `b${randomUUID()}`].sort();
      for (const wallet of wallets)
        await paperRows(
          db,
          `INSERT INTO wallet_swaps(wallet_swap_id,chain,transaction_signature,slot,wallet_address,side,
      token_mint,token_raw_amount,token_decimals,quote_asset,quote_raw_amount,quote_decimals,program_addresses,attribution,occurred_at,observed_at,available_at)
      VALUES ($1,'solana',$2,1,$3,'buy',$4,100,6,'SOL',100,9,ARRAY['fixture'],'{}',$5,$5,$5)`,
          [randomUUID(), randomUUID(), wallet, TOKEN_B, START]
        );
      expect((await resolve(id)).state).toBe('trade_eligible');
      await paperRows(
        db,
        `INSERT INTO wallet_relation_evidence VALUES ($1,$2,$3,'shared_funder',$4,$5,$5,'{}')`,
        [randomUUID(), wallets[0], wallets[1], randomUUID(), now]
      );
      const linked = await resolve(id);
      expect(linked.state).toBe('manual_review');
      expect(linked.candidates[0]?.independentWallets).toBe(1);
      expect(linked.candidates[0]?.reasonCodes).toContain('RELATED_WALLET_SUSPICION');
      const advisory = new CandidateResolver(
        db,
        feed,
        clock,
        {
          ...POLICY,
          evidencePolicy: {
            version: 1,
            narrativeRelationshipHandling: 'advisory',
            combinedRelationshipHandling: 'review',
          },
        },
        'SOL'
      );
      const reassessed = await advisory.resolve(id);
      if (reassessed.kind === 'failure') throw reassessed.error;
      expect(reassessed.value.candidates[0]?.qualification).toMatchObject({
        E01: true,
        E03: false,
      });
      expect(reassessed.value.policyHash).not.toBe(linked.policyHash);
      const signal = await paperRows<{ payload: { eligible: boolean } }>(
        db,
        'SELECT payload FROM signals WHERE signal_id=$1',
        [reassessed.value.candidates[0]!.narrativeSignalId]
      );
      expect(signal[0]?.payload.eligible).toBe(true);
    });
    it('binds callbacks to user/chat/message and requires explicit fresh quote confirmation', async () => {
      const id = await narrative();
      await resolve(id);
      await report(id);
      const buy = button('Buy (paper)');
      expect(buy.token).toHaveLength(32);
      await click(buy, 2, 20);
      expect(api.acknowledgements.at(-1)).toContain('bound');
      await click({ ...buy, messageId: 999 });
      expect(api.acknowledgements.at(-1)).toContain('bound');
      await click(buy);
      await consoleService.flush(16);
      expect(await count('orders')).toBe(0);
      const confirm = button('Confirm paper request');
      await click(confirm);
      await click(confirm);
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(1);
      expect(await count('positions')).toBe(0);
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(1);
      now = new Date(now.getTime() + 1000);
      await new PostgresPaperTrader(db, clock).tick(portfolioId, market(now));
      expect(await count('positions')).toBe(1);
    });
    it('withholds Watch entries and only clears the exclusion after a confirmed order succeeds', async () => {
      const id = await narrative();
      const resolved = await resolve(id);
      await report(id);
      await click(button('Watch'));
      await new PostgresPaperTrader(db, clock).tick(
        portfolioId,
        market(now),
        resolved.candidates[0]!.narrativeSignalId
      );
      expect(await count('orders')).toBe(0);
      expect(await count('paper_candidate_exclusions')).toBe(1);
      now = new Date(now.getTime() + 1);
      await report(id);
      await click(button('Buy (paper)'));
      await consoleService.flush(16);
      await click(button('Confirm paper request'));
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(1);
      expect(await count('paper_candidate_exclusions')).toBe(0);
    });
    it('rejects stale resolutions and quote movement before creating an order', async () => {
      const id = await narrative();
      await resolve(id);
      await report(id);
      const oldBuy = button('Buy (paper)');
      feed.value = { ...feed.value, criticalRisk: true };
      await resolve(id);
      await click(oldBuy);
      expect(api.acknowledgements.at(-1)).toContain('changed');
      feed.value = market(now);
      await resolve(id);
      await report(id);
      await click(button('Buy (paper)'));
      await consoleService.flush(16);
      feed.value = { ...market(now), cashReserves: '2000', realCashReserves: '2000' };
      await click(button('Confirm paper request'));
      expect(api.acknowledgements.at(-1)).toContain('slippage');
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(0);
    });
    it('makes command replay harmless, persists polling offsets and ignores unsupported updates', async () => {
      const raw = {
        update_id: ++updateId,
        message: { message_id: 1, from: { id: 1 }, chat: { id: 10 }, text: '/portfolio' },
      };
      await consoleService.handleUpdate(raw);
      await consoleService.handleUpdate(raw);
      await consoleService.flush(16);
      expect(api.sent).toHaveLength(1);
      api.nextUpdates = [
        { update_id: updateId + 1, edited_message: {} },
        {
          update_id: updateId + 2,
          message: { message_id: 2, from: { id: 999 }, chat: { id: 10 }, text: '/portfolio' },
        },
      ];
      await consoleService.poll();
      api.nextUpdates = [];
      await consoleService.poll();
      expect(api.offsets).toEqual([0, updateId + 3]);
    });
    it('blocks superseded resolver signals but accepts semantically equivalent evidence refreshes', async () => {
      const id = await narrative();
      const first = await resolve(id);
      await report(id);
      const buy = button('Buy (paper)');
      feed.value = market(now);
      const refreshed = await resolve(id);
      expect(refreshed.version).toBe(first.version + 1);
      expect(refreshed.materialHash).toBe(first.materialHash);
      await click(buy);
      expect(api.acknowledgements.at(-1)).toContain('quote prepared');
      const trader = new PostgresPaperTrader(db, clock);
      await trader.tick(portfolioId, market(now), first.candidates[0]!.narrativeSignalId);
      expect(await count('orders')).toBe(1);
      feed.value = { ...market(now), criticalRisk: true };
      await resolve(id);
      now = new Date(now.getTime() + 1000);
      await trader.tick(portfolioId, market(now));
      expect(await count('positions')).toBe(0);
      const reasons = await paperRows<{ reason: string }>(
        db,
        `SELECT e.detail->>'reason' AS reason FROM order_events e
        JOIN orders o USING(order_id) WHERE o.portfolio_id=$1 AND e.status='failed'`,
        [portfolioId]
      );
      expect(reasons[0]?.reason).toBe('CANDIDATE_STATE_CHANGED');
      await trader.tick(portfolioId, market(now), first.candidates[0]!.narrativeSignalId);
      expect(await count('orders')).toBe(1);
    });
    it('cancels pending entry on Discard and keeps expired confirmations harmless', async () => {
      const id = await narrative();
      const resolved = await resolve(id);
      await report(id);
      const trader = new PostgresPaperTrader(db, clock);
      await trader.tick(portfolioId, market(now), resolved.candidates[0]!.narrativeSignalId);
      expect(await count('orders')).toBe(1);
      await click(button('Discard'));
      now = new Date(now.getTime() + 1000);
      await trader.tick(portfolioId, market(now));
      expect(await count('positions')).toBe(0);
      const outcome = await paperRows<{ reason: string }>(
        db,
        `SELECT x.detail->>'reason' AS reason FROM order_events x JOIN orders o USING(order_id)
        WHERE o.portfolio_id=$1 AND x.status='failed'`,
        [portfolioId]
      );
      expect(outcome[0]?.reason).toBe('OPERATOR_WITHHELD');
      await report(id);
      await click(button('Watch'));
      now = new Date(now.getTime() + 1);
      await report(id);
      await click(button('Buy (paper)'));
      await consoleService.flush(16);
      const confirm = button('Confirm paper request');
      now = new Date(now.getTime() + 10001);
      await click(confirm);
      expect(api.acknowledgements.at(-1)).toContain('expired');
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(1);
    });
    it('serializes confirmed requests and recovers an accepted order after a worker interruption', async () => {
      const id = await narrative();
      await resolve(id);
      await report(id);
      await click(button('Buy (paper)'));
      await consoleService.flush(16);
      const confirm = button('Confirm paper request');
      await click(confirm);
      await Promise.all([consoleService.executeTrades(), consoleService.executeTrades()]);
      expect(await count('orders')).toBe(1);
      // Simulate a crash between the order transaction and the request acknowledgement transaction.
      await paperRows(
        db,
        "UPDATE console_trade_requests SET status='pending',result=NULL WHERE action_token=$1",
        [confirm.token]
      );
      now = new Date(now.getTime() + 20000);
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(1);
      const request = await paperRows<{ status: string }>(
        db,
        'SELECT status FROM console_trade_requests WHERE action_token=$1',
        [confirm.token]
      );
      expect(request[0]?.status).toBe('complete');
    });
    it('closes the full remainder through delayed settlement and only tightens authenticated stops', async () => {
      const id = await narrative();
      const resolved = await resolve(id);
      const trader = new PostgresPaperTrader(db, clock);
      await trader.tick(portfolioId, market(now), resolved.candidates[0]!.narrativeSignalId);
      now = new Date(now.getTime() + 1000);
      feed.value = market(now);
      await trader.tick(portfolioId, feed.value);
      const positions = await paperRows<{ position_id: string; stop_price: string }>(
        db,
        'SELECT position_id,stop_price FROM positions WHERE portfolio_id=$1',
        [portfolioId]
      );
      const position = positions[0]!;
      await command(`/stop ${position.position_id} 0.008`);
      await command(`/stop ${position.position_id} 0.001`);
      await consoleService.flush(16);
      expect(api.sent.at(-1)?.body.text).toContain('tighten');
      await command(`/pause ${portfolioId}`, 2, 20);
      await consoleService.flush(16);
      expect(api.sent.at(-1)?.body.text).toContain('not authorized');
      await command(`/pause ${portfolioId}`);
      await command('/positions');
      await consoleService.flush(16);
      await click(button('Close Remaining'));
      await consoleService.flush(16);
      await click(button('Confirm paper request'));
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(2);
      now = new Date(now.getTime() + 1000);
      await trader.tick(portfolioId, market(now));
      const closed = await paperRows<{ open_quantity: string }>(
        db,
        'SELECT open_quantity FROM positions WHERE position_id=$1',
        [position.position_id]
      );
      expect(Number(closed[0]?.open_quantity)).toBe(0);
      const requests = await paperRows<{ status: string }>(
        db,
        `SELECT r.status FROM console_trade_requests r JOIN telegram_actions a USING(action_token)
        WHERE a.portfolio_id=$1`,
        [portfolioId]
      );
      expect(requests[0]?.status).toBe('complete');
    });
    it('rolls back emitted signals when resolution persistence fails', async () => {
      const id = await narrative();
      await paperRows(
        db,
        `CREATE FUNCTION fail_console_report() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'fixture write failure'; END; $$ LANGUAGE plpgsql;
        CREATE TRIGGER fixture_report_failure BEFORE INSERT ON candidate_resolution_reports FOR EACH ROW EXECUTE FUNCTION fail_console_report()`
      );
      try {
        expect(await resolver.resolve(id)).toMatchObject({ kind: 'failure' });
        const signals = await paperRows(
          db,
          "SELECT 1 FROM signals WHERE canonical_narrative_id=$1 AND source_version='resolver:v2'",
          [id]
        );
        expect(signals).toHaveLength(0);
        const reports = await paperRows(
          db,
          'SELECT 1 FROM candidate_resolution_events WHERE canonical_narrative_id=$1',
          [id]
        );
        expect(reports).toHaveLength(0);
      } finally {
        await paperRows(
          db,
          'DROP TRIGGER fixture_report_failure ON candidate_resolution_reports; DROP FUNCTION fail_console_report()'
        );
      }
      expect((await resolve(id)).version).toBe(1);
    });
    it('does not resend ambiguous deliveries and keeps each operator notification independent', async () => {
      await command('/portfolio');
      api.fail = new TelegramFailure('unknown', true);
      await consoleService.flush();
      api.fail = null;
      await consoleService.flush();
      expect(api.sent).toHaveLength(0);
      const status = await paperRows<{ status: string }>(
        db,
        'SELECT status FROM telegram_outbox WHERE bot_name=$1',
        [botName]
      );
      expect(status[0]?.status).toBe('uncertain');
      const id = await narrative();
      await resolve(id);
      await consoleService.notify();
      for (let i = 0; i < 4; i++) await consoleService.flush(16);
      expect(api.sent.some(({ body }) => body.chat_id === '10')).toBe(true);
      expect(api.sent.some(({ body }) => body.chat_id === '20')).toBe(true);
    });
    it('keeps scenario exclusions attached to a manual wallet-only order without inventing NIT evidence', async () => {
      const run = await new PostgresPaperRegistry(db).createRun({
        strategy: strategy('E02'),
        mode: ExecutionMode.FORWARD_PAPER,
        codeHash: 'a'.repeat(64),
        codeCommitSha: 'console-fixture',
        periodStart: START,
        periodEnd: new Date(START.getTime() + 86400000),
        cashAsset: 'SOL',
        initialCash: '100',
      });
      if (run.kind === 'failure') throw run.error;
      portfolioId = run.value.portfolioId;
      consoleService = new TelegramDecisionConsole(
        db,
        api,
        feed,
        new PostgresPaperTrader(db, clock),
        clock,
        {
          botName,
          operators: [{ userId: '1', chatId: '10', role: 'trader', portfolioIds: [portfolioId] }],
          actionTtlMs: 10000,
          maxOutbox: 256,
        }
      );
      for (const wallet of [`c${randomUUID()}`, `d${randomUUID()}`])
        await paperRows(
          db,
          `INSERT INTO wallet_swaps(wallet_swap_id,chain,
        transaction_signature,slot,wallet_address,side,token_mint,token_raw_amount,token_decimals,quote_asset,quote_raw_amount,
        quote_decimals,program_addresses,attribution,occurred_at,observed_at,available_at)
        VALUES ($1,'solana',$2,1,$3,'buy',$4,100,6,'SOL',100,9,ARRAY['fixture'],'{}',$5,$5,$5)`,
          [randomUUID(), randomUUID(), wallet, TOKEN, START]
        );
      const signal = await new PostgresSignalStore(db).append({
        eventId: generateSignalId(),
        source: 'wallet_tracker',
        sourceVersion: 'fixture',
        kind: SignalKind.WALLET_CONVERGENCE,
        chain: Chain.SOLANA,
        tokenMint: TOKEN,
        occurredAt: START,
        observedAt: START,
        availableAt: START,
        origin: { kind: 'provider', provider: 'fixture-console', referenceId: randomUUID() },
        evidenceIds: [],
        payload: { side: 'buy', independentSourceCount: 2 },
      });
      if (signal.kind === 'failure') throw signal.error;
      const id = await narrative();
      await resolve(id);
      await report(id);
      await click(button('Watch'));
      now = new Date(now.getTime() + 1);
      await report(id);
      await click(button('Buy (paper)'));
      await consoleService.flush(16);
      await click(button('Confirm paper request'));
      await consoleService.executeTrades();
      expect(await count('orders')).toBe(1);
      expect(await count('paper_candidate_exclusions')).toBe(0);
      await report(id);
      await click(button('Discard'));
      now = new Date(now.getTime() + 1000);
      await new PostgresPaperTrader(db, clock).tick(portfolioId, market(now));
      expect(await count('positions')).toBe(0);
    });
    it('limits research report reads to experiments whose entire portfolio scope is authorized', async () => {
      const experimentId = randomUUID();
      const hash = 'f'.repeat(64);
      await paperRows(
        db,
        "INSERT INTO research_datasets(dataset_hash,manifest) VALUES ($1,'{}') ON CONFLICT DO NOTHING",
        [hash]
      );
      await paperRows(
        db,
        "INSERT INTO research_experiments(experiment_id,dataset_hash,config_hash,config) VALUES ($1,$2,$2,'{}')",
        [experimentId, hash]
      );
      await paperRows(db, "INSERT INTO research_replay_variants VALUES ($1,'A',$2)", [
        experimentId,
        portfolioId,
      ]);
      await paperRows(
        db,
        'INSERT INTO research_reports(experiment_id,report,report_hash) VALUES ($1,$2,$3)',
        [
          experimentId,
          JSON.stringify({
            datasetHash: hash,
            timing: 'fixed_decision',
            underlyingScenarioCount: 1,
            variants: [
              {
                variant: 'A',
                dataStatus: 'COMPLETE_UNDER_DECLARED_ASSUMPTIONS',
                netCashAfterResearchCosts: '0',
                researchCostCash: '0',
              },
            ],
          }),
          hash,
        ]
      );
      await command(`/run-report ${experimentId}`);
      await consoleService.flush(16);
      expect(api.sent.at(-1)?.body.text).toContain('HISTORICAL PAPER RESEARCH');
      const extra = await new PostgresPaperRegistry(db).createRun({
        strategy: strategy(),
        mode: ExecutionMode.FORWARD_PAPER,
        codeHash: 'a'.repeat(64),
        codeCommitSha: 'console-fixture',
        periodStart: START,
        periodEnd: new Date(START.getTime() + 86400000),
        cashAsset: 'SOL',
        initialCash: '100',
      });
      if (extra.kind === 'failure') throw extra.error;
      await paperRows(db, "INSERT INTO research_replay_variants VALUES ($1,'B',$2)", [
        experimentId,
        extra.value.portfolioId,
      ]);
      await command(`/run-report ${experimentId}`);
      await consoleService.flush(16);
      expect(api.sent.at(-1)?.body.text).toContain('not authorized');
    });
  }
);
