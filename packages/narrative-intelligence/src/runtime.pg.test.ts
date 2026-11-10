import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { PostgresResearchStore } from '@nit/database-public';
import { CandidateResolver } from '@nit/decision-console';
import {
  ExecutionMode,
  PostgresPaperRegistry,
  PostgresPaperTrader,
  PostgresResearchReplay,
  ResearchDataset,
  paperRows,
} from '@nit/trading';
import { fixtureMarket, fixtureStrategy, POLICY } from './integration.fixtures';
import { success } from '@nit/domain';
import { ResearchRecord } from '@nit/signals';
import { LiveNarrativeRuntime } from './runtime';
import { TextInterpreter, SourceText } from './contracts';
import {
  MINT_A,
  MINT_B,
  NOW,
  configuration as baseConfiguration,
  extraction,
} from './narrative.fixtures';
import { richConfiguration, richInput, tokenExtraction } from './rich.fixtures';

describe.skipIf(!hasTestDatabase())('automated text provenance to deterministic paper path', () => {
  let harness: TestDatabase;
  let now: Date;
  function configuration(): ReturnType<typeof baseConfiguration> {
    const config = baseConfiguration();
    config.interpreter = {
      kind: 'gateway',
      endpoint: 'https://gateway.invalid/text',
      allowedOrigin: 'https://gateway.invalid',
      modelVersion: 'test',
      allowExternalContext: true,
      expiresAt: new Date(now.getTime() + 3600000).toISOString(),
      maxInputBytes: 4000,
      maxOutputTokens: 100,
      maxChargeMicros: 1,
    };
    config.maxCostMicrosPerDay = 10000;
    return config;
  }
  beforeAll(async () => {
    harness = await createTestDatabase('live_narratives');
  }, 30000);
  afterAll(async () => {
    await harness.teardown();
  });
  beforeEach(async () => {
    now = new Date(NOW);
    await paperRows(
      harness.db,
      'TRUNCATE nit_semantic_members,nit_semantic_nodes,nit_media_objects,nit_token_interpretations,nit_narrative_reports,nit_text_results,nit_text_attempts,nit_text_policies,nit_text_budgets,nit_token_search_index,research_records,signals,domain_events,observations,tokens,strategy_entry_versions,strategy_exit_versions,strategy_protection_versions,strategy_risk_versions CASCADE'
    );
  });
  async function observation(
    author = 'a',
    text = `Nova festival begins ${MINT_A} ${MINT_B} context ${author}`
  ): Promise<void> {
    await paperRows(
      harness.db,
      `INSERT INTO observations(observation_id,provider,provider_post_id,author_id,published_at,ingested_at,text,raw_payload)
      VALUES ($1,'x',$2,$3,$4,$5,$6,'{}')`,
      [randomUUID(), randomUUID(), author, new Date(now.getTime() - 1000), now, text]
    );
  }
  async function token(mint = MINT_A, discovered = now): Promise<void> {
    await paperRows(
      harness.db,
      `INSERT INTO tokens(token_id,chain,address,ticker,name,launch_time,launch_source,discovered_at,raw_payload)
      VALUES ($1,'solana',$2,'NOVA',$3,$4,'pump_fun',$5,'{}')`,
      [
        randomUUID(),
        mint,
        mint === MINT_A ? 'Nova' : 'Nova Echo',
        new Date(now.getTime() - 2000),
        discovered,
      ]
    );
  }
  function interpreter(): TextInterpreter & {
    interpret: ReturnType<typeof vi.fn<[SourceText, AbortSignal], Promise<unknown>>>;
  } {
    return {
      kind: 'gateway',
      identity: {
        name: 'test-interpreter',
        version: 'test',
        promptHash: '0'.repeat(64),
        isToyImplementation: false,
      },
      interpret: vi.fn<[SourceText, AbortSignal], Promise<unknown>>((s) =>
        Promise.resolve(extraction(s.text))
      ),
    };
  }
  async function source(): Promise<{
    payload: { episodeId: string; interpretationId: string; candidates: unknown[] };
    canonical_narrative_id: string;
  }> {
    const rows = await paperRows<{
      payload: { episodeId: string; interpretationId: string; candidates: unknown[] };
      canonical_narrative_id: string;
    }>(
      harness.db,
      "SELECT payload,canonical_narrative_id FROM signals WHERE source_version='provenance:v1' ORDER BY available_at DESC,signal_id DESC LIMIT 1"
    );
    return rows[0]!;
  }
  it('opens before token discovery, then reuses extraction and the attention episode as competing mints arrive', async () => {
    await observation();
    await observation('b');
    const model = interpreter();
    const config = configuration();
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    expect(await runtime.cycle()).toMatchObject({ captured: 2, published: 1, incomplete: false });
    const initial = await source();
    expect(initial.payload.candidates).toEqual([]);
    now = new Date(now.getTime() + 1000);
    await token();
    await token(MINT_B);
    expect(await runtime.cycle()).toMatchObject({ claimed: 0, published: 1, incomplete: false });
    const updated = await source();
    expect(updated.payload.episodeId).toBe(initial.payload.episodeId);
    expect(updated.payload.candidates).toHaveLength(2);
    expect(model.interpret).toHaveBeenCalledTimes(2);
    expect(
      await paperRows(
        harness.db,
        `SELECT p.configuration->'config'->'interpreter'->>'maxOutputTokens' AS output_limit
      FROM nit_text_attempts a JOIN nit_text_policies p ON p.policy_hash=a.runtime_policy_hash`
      )
    ).toEqual([{ output_limit: '100' }, { output_limit: '100' }]);
    expect(await runtime.cycle()).toMatchObject({ claimed: 0, published: 0 });
    const revised = configuration();
    revised.policy.version = 'revised-test-weights';
    const next = new LiveNarrativeRuntime(harness.db, model, revised, () => now);
    expect(next.extractorHash).toBe(runtime.extractorHash);
    await next.cycle();
    expect(model.interpret).toHaveBeenCalledTimes(2);
    expect((await source()).payload.episodeId).toBe(initial.payload.episodeId);
    await expect(
      paperRows(harness.db, "UPDATE nit_text_results SET status='unknown'")
    ).rejects.toThrow();
  });
  it('passes a real provenance feature through E01 paper entry and pins late membership for replay', async () => {
    await observation();
    await observation('b');
    const runtime = new LiveNarrativeRuntime(harness.db, interpreter(), configuration(), () => now);
    await runtime.cycle();
    now = new Date(now.getTime() + 1000);
    await token();
    await token(MINT_B);
    await runtime.cycle();
    const published = await source();
    now = new Date(now.getTime() + 1001);
    const feed = {
      latest: (mint: string): ReturnType<typeof success<ReturnType<typeof fixtureMarket>>> =>
        success(fixtureMarket(now, '1000', mint)),
    };
    const resolver = new CandidateResolver(
      harness.db,
      {
        latest: (mint): Promise<ReturnType<typeof feed.latest>> =>
          Promise.resolve(feed.latest(mint)),
      },
      { now: (): Date => now },
      POLICY,
      'SOL'
    );
    const resolved = await resolver.resolve(published.canonical_narrative_id);
    expect(resolved.kind).toBe('success');
    if (resolved.kind === 'failure') throw resolved.error;
    const candidate = resolved.value.candidates.find((c) => c.tokenMint === MINT_A)!;
    expect(candidate.qualification?.E01).toBe(true);
    expect(candidate.evidence?.wallet).toBe('unavailable');
    const registered = await new PostgresPaperRegistry(harness.db).createRun({
      strategy: fixtureStrategy({ entry: 'E01' }),
      mode: ExecutionMode.FORWARD_PAPER,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'narrative-test',
      periodStart: NOW,
      periodEnd: new Date(now.getTime() + 3600000),
      cashAsset: 'SOL',
      initialCash: '100',
    });
    if (registered.kind === 'failure') throw registered.error;
    const trader = new PostgresPaperTrader(harness.db, { now: (): Date => now });
    const entered = await trader.tick(
      registered.value.portfolioId,
      fixtureMarket(now, '1000', MINT_A),
      candidate.narrativeSignalId
    );
    expect(entered.kind).toBe('success');
    expect(await paperRows(harness.db, 'SELECT 1 FROM orders')).toHaveLength(1);
    now = new Date(now.getTime() + 1000);
    await trader.tick(registered.value.portfolioId, fixtureMarket(now, '1000', MINT_A));
    expect(
      await paperRows(harness.db, 'SELECT 1 FROM positions WHERE open_quantity>0')
    ).toHaveLength(1);
    const dataset: ResearchDataset = {
      schemaVersion: 1,
      periodStart: NOW.toISOString(),
      decisionEnd: new Date(now.getTime() + 1000).toISOString(),
      evaluationEnd: new Date(now.getTime() + 10000).toISOString(),
      partition: 'development',
      universeDefinition: 'Synthetic point-in-time candidate membership',
      discovery: 'curated_proxy',
      measurement: 'assumed_proxy',
      interpretationMode: 'captured',
      coverage: 'Synthetic, incomplete exits',
      seed: 'fixed',
      extractorVersions: [`live-text-v1:${runtime.policyHash}`],
      universe: [
        {
          scenarioId: 'one',
          episodeId: published.payload.episodeId,
          tokenMint: MINT_A,
          discoveredAt: new Date(now.getTime() - 1000).toISOString(),
          outcomeClass: 'live',
          exitHorizonComplete: false,
        },
      ],
      events: [
        {
          id: 'one',
          scenarioId: 'one',
          tokenMint: MINT_A,
          availableAt: now.toISOString(),
          kind: 'opportunity',
          market: null,
          featureSnapshotId: candidate.featureSnapshotId!,
          latencyMs: { market: 0, narrative: 0, wallet: 0 },
          costs: { marketCash: '0', narrativeCash: '0', walletCash: '0' },
        },
      ],
    };
    const replay = new PostgresResearchReplay(harness.db);
    await expect(replay.freezeDataset(dataset)).rejects.toThrow('membership');
    const withMembership = {
      ...dataset,
      universe: dataset.universe.map((u) => ({
        ...u,
        membershipFeatureSnapshotId: candidate.featureSnapshotId!,
      })),
    };
    expect(await replay.freezeDataset(withMembership)).toMatch(/^[a-f0-9]{64}$/);
    const census: ResearchDataset = {
      ...dataset,
      discovery: 'market_census',
      universe: dataset.universe.map((u) => ({
        ...u,
        discoveredAt: new Date(NOW.getTime() + 1000).toISOString(),
      })),
    };
    const censusHash = await replay.freezeDataset(census);
    const frozen = await paperRows<{ manifest: { discoveryHashes: Record<string, string> } }>(
      harness.db,
      'SELECT manifest FROM research_datasets WHERE dataset_hash=$1',
      [censusHash]
    );
    expect(frozen[0]?.manifest.discoveryHashes['one']).toMatch(/^[a-f0-9]{64}$/);
    const start = NOW.getTime() + 1000;
    const independent: ResearchDataset = {
      ...census,
      decisionEnd: new Date(start + 3000).toISOString(),
      evaluationEnd: new Date(start + 4000).toISOString(),
      events: [0, 1000, 2000].map((delay, index) => {
        const at = new Date(start + delay);
        const market = fixtureMarket(at, '1000', MINT_A);
        return {
          id: `census-${index}`,
          scenarioId: 'one',
          tokenMint: MINT_A,
          availableAt: at.toISOString(),
          kind: index === 0 ? ('opportunity' as const) : ('market' as const),
          market: {
            ...market,
            occurredAt: at.toISOString(),
            observedAt: at.toISOString(),
            availableAt: at.toISOString(),
          },
          featureSnapshotId: null,
          latencyMs: { market: 0, narrative: 0, wallet: 0 },
          costs: { marketCash: '0', narrativeCash: '0', walletCash: '0' },
        };
      }),
    };
    const independentHash = await replay.freezeDataset(independent);
    const experimentId = randomUUID();
    await replay.create({
      experimentId,
      datasetHash: independentHash,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'narrative-test',
      cashAsset: 'SOL',
      initialCash: '100',
      timing: 'fixed_decision',
      policyVersion: 'test',
      qualificationPolicy: {
        version: 1,
        minimumNarrativeStrengthBps: 5000,
        minimumLinkConfidenceBps: 5000,
        minimumIndependentWallets: 2,
        narrativeRelationshipHandling: 'advisory',
        combinedRelationshipHandling: 'review',
      },
      variants: {
        A: fixtureStrategy({ entry: 'E00' }),
        B: fixtureStrategy({ entry: 'E01' }),
        C: fixtureStrategy({ entry: 'E03' }),
      },
    });
    let complete = false;
    for (let i = 0; i < 10 && !complete; i++)
      complete = (await replay.advance(experimentId, 8)).complete;
    expect(complete).toBe(true);
    expect(
      await paperRows(
        harness.db,
        `SELECT v.variant,COUNT(*)::text AS fills FROM order_fills f
      JOIN orders o ON o.order_id=f.order_id JOIN research_replay_variants v ON v.portfolio_id=o.portfolio_id
      WHERE v.experiment_id=$1 GROUP BY v.variant ORDER BY v.variant`,
        [experimentId]
      )
    ).toEqual([{ variant: 'A', fills: '1' }]);
    expect(
      await paperRows(
        harness.db,
        "SELECT 1 FROM decision_feature_snapshots WHERE snapshot->'marketDiscovery'->>'hash' IS NOT NULL"
      )
    ).not.toHaveLength(0);
    await expect(
      replay.freezeDataset({
        ...census,
        universe: census.universe.map((u) => ({ ...u, discoveredAt: NOW.toISOString() })),
      })
    ).rejects.toThrow('census discovery');
    await expect(
      replay.freezeDataset({
        ...withMembership,
        universe: withMembership.universe.map((u) => ({ ...u, discoveredAt: NOW.toISOString() })),
      })
    ).rejects.toThrow('membership');
  });
  it('cancels an unsettled narrative buy when newer producer evidence has not reached the resolver yet', async () => {
    await observation();
    await observation('b');
    await token();
    const config = configuration();
    config.universeLimit = 1;
    const runtime = new LiveNarrativeRuntime(harness.db, interpreter(), config, () => now);
    await runtime.cycle();
    const published = await source();
    now = new Date(now.getTime() + 1001);
    const resolver = new CandidateResolver(
      harness.db,
      {
        latest: (mint): Promise<ReturnType<typeof success<ReturnType<typeof fixtureMarket>>>> =>
          Promise.resolve(success(fixtureMarket(now, '1000', mint))),
      },
      { now: (): Date => now },
      POLICY,
      'SOL'
    );
    const resolved = await resolver.resolve(published.canonical_narrative_id);
    if (resolved.kind === 'failure') throw resolved.error;
    expect(resolved.value.candidates[0]?.qualification?.E01).toBe(true);
    const run = await new PostgresPaperRegistry(harness.db).createRun({
      strategy: fixtureStrategy({ entry: 'E01' }),
      mode: ExecutionMode.FORWARD_PAPER,
      codeHash: 'b'.repeat(64),
      codeCommitSha: 'test',
      periodStart: NOW,
      periodEnd: new Date(now.getTime() + 3600000),
      cashAsset: 'SOL',
      initialCash: '100',
    });
    if (run.kind === 'failure') throw run.error;
    const trader = new PostgresPaperTrader(harness.db, { now: (): Date => now });
    await trader.tick(
      run.value.portfolioId,
      fixtureMarket(now, '1000', MINT_A),
      resolved.value.candidates[0]!.narrativeSignalId
    );
    expect(await paperRows(harness.db, 'SELECT 1 FROM orders')).toHaveLength(1);
    now = new Date(now.getTime() + 1001);
    await token(MINT_B);
    await runtime.cycle();
    expect((await source()).payload.candidates).toHaveLength(0);
    await trader.tick(run.value.portfolioId, fixtureMarket(now, '1000', MINT_A));
    expect(await paperRows(harness.db, 'SELECT 1 FROM order_fills')).toHaveLength(0);
    expect(await paperRows(harness.db, 'SELECT 1 FROM positions')).toHaveLength(0);
  });

  it('cancels a pending narrative buy on producer-health expiry while market data remains fresh', async () => {
    await observation();
    await observation('b');
    await token();
    const runtime = new LiveNarrativeRuntime(harness.db, interpreter(), configuration(), () => now);
    await runtime.cycle();
    const published = await source();
    now = new Date(now.getTime() + 1001);
    const resolver = new CandidateResolver(
      harness.db,
      {
        latest: (mint): Promise<ReturnType<typeof success<ReturnType<typeof fixtureMarket>>>> =>
          Promise.resolve(success(fixtureMarket(now, '1000', mint))),
      },
      { now: (): Date => now },
      POLICY,
      'SOL'
    );
    const resolved = await resolver.resolve(published.canonical_narrative_id);
    if (resolved.kind === 'failure') throw resolved.error;
    const run = await new PostgresPaperRegistry(harness.db).createRun({
      strategy: fixtureStrategy({ entry: 'E01' }),
      mode: ExecutionMode.FORWARD_PAPER,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'health-test',
      periodStart: NOW,
      periodEnd: new Date(now.getTime() + 3600000),
      cashAsset: 'SOL',
      initialCash: '100',
    });
    if (run.kind === 'failure') throw run.error;
    const trader = new PostgresPaperTrader(harness.db, { now: (): Date => now });
    await trader.tick(
      run.value.portfolioId,
      fixtureMarket(now, '1000', MINT_A),
      resolved.value.candidates[0]!.narrativeSignalId
    );
    expect(await paperRows(harness.db, 'SELECT 1 FROM orders')).toHaveLength(1);
    now = new Date(now.getTime() + 4000);
    await trader.tick(run.value.portfolioId, fixtureMarket(now, '1000', MINT_A));
    expect(await paperRows(harness.db, 'SELECT 1 FROM order_fills')).toHaveLength(0);
    expect(
      await paperRows(
        harness.db,
        "SELECT detail->>'reason' AS reason FROM order_events WHERE detail ? 'reason'"
      )
    ).toEqual(expect.arrayContaining([{ reason: 'NARRATIVE_PRODUCER_UNAVAILABLE' }]));
  });

  it('refuses future or nonexistent token additions to an open episode', async () => {
    await observation();
    await observation('b');
    const runtime = new LiveNarrativeRuntime(harness.db, interpreter(), configuration(), () => now);
    await runtime.cycle();
    const published = await source();
    const store = new PostgresResearchStore(harness.db);
    const previous = await store.at(published.payload.interpretationId, now);
    if (previous?.kind !== 'interpretation') throw new Error('Missing fixture interpretation');
    await token(MINT_A, new Date(now.getTime() + 1000));
    const attempted: ResearchRecord = {
      ...previous,
      recordId: randomUUID(),
      version: 2,
      data: {
        ...previous.data,
        candidates: [
          {
            tokenMint: MINT_A,
            linkConfidenceBps: 10000,
            status: 'confirmed',
            disposition: 'candidate',
            reasonCodes: ['TEST'],
          },
        ],
      },
    };
    const saved = await store.append(attempted);
    expect(saved.kind).toBe('failure');
  });
  it('persists failed/unknown outcomes, never auto-retries them, and retains the daily worst-case reservation', async () => {
    await observation();
    await observation('b');
    const config = configuration();
    config.interpreter = {
      kind: 'gateway',
      endpoint: 'https://gateway.invalid/text',
      allowedOrigin: 'https://gateway.invalid',
      modelVersion: 'test',
      allowExternalContext: true,
      expiresAt: new Date(now.getTime() + 60000).toISOString(),
      maxInputBytes: 4000,
      maxOutputTokens: 100,
      maxChargeMicros: 100,
    };
    config.maxRequestsPerDay = 1;
    config.maxCostMicrosPerDay = 100;
    const call = vi.fn(() => Promise.reject(new Error('private provider response')));
    const model: TextInterpreter = {
      ...interpreter(),
      kind: 'gateway',
      interpret: call,
    };
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    expect(await runtime.cycle()).toMatchObject({ claimed: 1, failed: 1, incomplete: true });
    expect(await runtime.cycle()).toMatchObject({ claimed: 0, incomplete: true });
    expect(call).toHaveBeenCalledTimes(1);
    expect(
      await paperRows(harness.db, 'SELECT requests,reserved_cost_micros FROM nit_text_budgets')
    ).toEqual([{ requests: '1', reserved_cost_micros: '100' }]);
    expect(await paperRows(harness.db, 'SELECT status FROM nit_text_results')).toEqual([
      { status: 'unknown' },
    ]);
  });
  it('does not charge an expired external-context grant', async () => {
    await observation();
    const config = configuration();
    config.interpreter = {
      kind: 'gateway',
      endpoint: 'https://gateway.invalid/text',
      allowedOrigin: 'https://gateway.invalid',
      modelVersion: 'test',
      allowExternalContext: true,
      expiresAt: new Date(now.getTime() - 1).toISOString(),
      maxInputBytes: 4000,
      maxOutputTokens: 100,
      maxChargeMicros: 100,
    };
    const model = { ...interpreter(), kind: 'gateway' as const };
    expect(
      await new LiveNarrativeRuntime(harness.db, model, config, () => now).cycle()
    ).toMatchObject({ claimed: 0, incomplete: true });
    expect(model.interpret).not.toHaveBeenCalled();
    expect(await paperRows(harness.db, 'SELECT 1 FROM nit_text_budgets')).toEqual([]);
  });
  it('times out an uncooperative provider without a second call or publication', async () => {
    await observation();
    const config = configuration();
    config.timeoutMs = 100;
    const model = {
      ...interpreter(),
      interpret: vi.fn(() => new Promise<unknown>(() => undefined)),
    };
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    expect(await runtime.cycle()).toMatchObject({ failed: 1, published: 0 });
    await runtime.cycle();
    expect(model.interpret).toHaveBeenCalledTimes(1);
  });
  it('serializes producers and refuses toy interpreters', async () => {
    await observation();
    let release: ((value: unknown) => void) | undefined;
    let started: (() => void) | undefined;
    const entered = new Promise<void>((resolve) => {
      started = resolve;
    });
    const model = {
      ...interpreter(),
      interpret: vi.fn(
        () =>
          new Promise<unknown>((resolve) => {
            release = resolve;
            started?.();
          })
      ),
    };
    const config = configuration();
    config.timeoutMs = 10000;
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    const first = runtime.cycle();
    await Promise.race([
      entered,
      first.then(() => {
        throw new Error('Producer completed before entering test provider');
      }),
    ]);
    try {
      expect(await runtime.cycle()).toMatchObject({ busy: true });
    } finally {
      release?.({ schemaVersion: 1, topics: [] });
      await first;
    }
    expect(
      () =>
        new LiveNarrativeRuntime(
          harness.db,
          { ...model, identity: { ...model.identity, isToyImplementation: true } },
          configuration()
        )
    ).toThrow('toy');
  });
  it('applies the universe cap to retrieved candidates, not unrelated archived tokens', async () => {
    await observation();
    await observation('b');
    await token();
    const alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
    const archive = Array.from({ length: 200 }, (_, i) => ({
      id: randomUUID(),
      mint: '4'.repeat(30) + alphabet[Math.floor(i / 58)] + alphabet[i % 58],
      name: `Archive ${i}`,
    }));
    await paperRows(
      harness.db,
      `INSERT INTO tokens(token_id,chain,address,ticker,name,launch_time,launch_source,discovered_at,raw_payload)
      SELECT x.id,'solana',x.mint,'ALT',x.name,$2::timestamptz,'pump_fun',$2::timestamptz,'{}'::jsonb
      FROM jsonb_to_recordset($1::jsonb) AS x(id UUID,mint TEXT,name TEXT)`,
      [JSON.stringify(archive), now]
    );
    const config = configuration();
    config.universeLimit = 1;
    const result = await new LiveNarrativeRuntime(
      harness.db,
      interpreter(),
      config,
      () => now
    ).cycle();
    expect(result.incomplete).toBe(false);
    expect((await source()).payload.candidates).toHaveLength(1);
    expect(
      await paperRows(harness.db, 'SELECT COUNT(*)::text AS count FROM nit_token_search_index')
    ).toEqual([{ count: '201' }]);
  });

  it('converges live paraphrases, retrieves semantic competitors and pins metered token representations', async () => {
    const config = richConfiguration();
    await observation();
    await observation('b', `Nova party opens ${MINT_A} ${MINT_B} for b`);
    await token();
    await token(MINT_B);
    const model: TextInterpreter = {
      kind: 'gateway',
      identity: {
        name: 'fixture-rich',
        version: 'fixture-rich',
        promptHash: '1'.repeat(64),
        isToyImplementation: false,
      },
      interpret: (s) =>
        Promise.resolve(
          s.purpose === 'token_representation'
            ? tokenExtraction()
            : richInput(s.authorId, s.text).extraction
        ),
    };
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    expect((await runtime.cycle()).incomplete).toBe(true);
    now = new Date(now.getTime() + 1);
    expect((await runtime.cycle()).incomplete).toBe(false);
    expect((await source()).payload.candidates).toHaveLength(2);
    expect(
      await paperRows(harness.db, 'SELECT COUNT(*)::text AS count FROM nit_semantic_nodes')
    ).toEqual([{ count: '1' }]);
    expect(
      await paperRows(
        harness.db,
        'SELECT requests::text AS requests,reserved_cost_micros::text AS cost FROM nit_text_budgets'
      )
    ).toEqual([{ requests: '4', cost: '4' }]);
    const ledger = await paperRows<{ state: string; input_hash: string; policy_hash: string }>(
      harness.db,
      'SELECT state,input_hash,policy_hash FROM nit_token_interpretations'
    );
    expect(
      ledger.every(
        (r) =>
          r.state === 'captured' &&
          r.input_hash.length === 64 &&
          r.policy_hash === runtime.policyHash
      )
    ).toBe(true);
    expect(
      (
        await harness.db.query(
          "UPDATE nit_token_interpretations SET state='unknown' WHERE state='captured'"
        )
      ).kind
    ).toBe('failure');
  });
  it('keeps a failed token interpretation terminal and blocks semantic-universe completeness without repurchasing it', async () => {
    const config = richConfiguration();
    await token();
    const interpret = vi.fn(() => Promise.reject(new Error('Synthetic gateway outage')));
    const model: TextInterpreter = {
      kind: 'gateway',
      identity: {
        name: 'fixture-rich',
        version: 'fixture-rich',
        promptHash: '1'.repeat(64),
        isToyImplementation: false,
      },
      interpret,
    };
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    expect((await runtime.cycle()).incomplete).toBe(true);
    now = new Date(now.getTime() + 1);
    expect((await runtime.cycle()).incomplete).toBe(true);
    expect(interpret).toHaveBeenCalledTimes(1);
    expect(await paperRows(harness.db, 'SELECT state FROM nit_token_interpretations')).toEqual([
      { state: 'unknown' },
    ]);
    expect(
      await paperRows(harness.db, 'SELECT requests::text AS requests FROM nit_text_budgets')
    ).toEqual([{ requests: '1' }]);
  });
  it('retrieves a token by its exact source frame even when lexical and semantic retrieval do not match', async () => {
    const config = richConfiguration();
    config.policy.linkWeights = {
      lexical: 0,
      identity: 0,
      directReference: 0,
      temporal: 0,
      visual: 10000,
      contradiction: 10000,
    };
    await observation('a', 'Nova festival begins for everyone');
    await observation('b', 'Nova party opens for everyone');
    await token();
    const id = randomUUID();
    await paperRows(
      harness.db,
      `INSERT INTO media(media_id,observation_id,provider_media_key,media_type,source_url,created_at)
      SELECT $1,observation_id,'m','image','https://media.invalid/image',$2 FROM observations WHERE author_id='a'`,
      [id, now]
    );
    const model: TextInterpreter = {
      kind: 'gateway',
      identity: {
        name: 'fixture-rich',
        version: 'fixture-rich',
        promptHash: '1'.repeat(64),
        isToyImplementation: false,
      },
      interpret: (s) => {
        if (s.purpose === 'token_representation')
          return Promise.resolve({
            ...tokenExtraction(),
            representation: { modelVersion: 'fixture-embedding-v1', vector: [0, 1] },
            mediaRepresentations: [
              {
                mediaId: s.observationId,
                sha256: 'c'.repeat(64),
                frameHashes: ['b'.repeat(64)],
                dhashes: ['1234567890abcdef'],
                durationMs: null,
              },
            ],
          });
        const value = richInput(s.authorId, s.text).extraction;
        if (s.authorId === 'a')
          value.mediaRepresentations = [
            {
              mediaId: id,
              sha256: 'a'.repeat(64),
              frameHashes: ['b'.repeat(64)],
              dhashes: ['1234567890abcdef'],
              durationMs: null,
            },
          ];
        return Promise.resolve(value);
      },
    };
    // A canonical artwork reference legitimizes the captured token representation.
    await paperRows(harness.db, 'UPDATE nit_text_health SET incomplete=true');
    const tokens = await paperRows<{ token_id: string }>(harness.db, 'SELECT token_id FROM tokens');
    await paperRows(
      harness.db,
      "INSERT INTO token_artwork(artwork_id,token_id,source_url,created_at) VALUES ($1,$1,'https://media.invalid/artwork',$2)",
      [tokens[0]!.token_id, now]
    );
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    expect((await runtime.cycle()).incomplete).toBe(false);
    const candidates = (await source()).payload.candidates as {
      tokenMint: string;
      linkConfidenceBps: number;
    }[];
    expect(candidates).toHaveLength(1);
    expect(candidates[0]!.tokenMint).toBe(MINT_A);
    expect(candidates[0]!.linkConfidenceBps).toBe(10000);
    now = new Date(now.getTime() + 1001);
    const published = await source();
    const resolver = new CandidateResolver(
      harness.db,
      {
        latest: (
          mint: string
        ): Promise<ReturnType<typeof success<ReturnType<typeof fixtureMarket>>>> =>
          Promise.resolve(success(fixtureMarket(now, '1000', mint))),
      },
      { now: (): Date => now },
      POLICY,
      'SOL'
    );
    const resolved = await resolver.resolve(published.canonical_narrative_id);
    if (resolved.kind === 'failure') throw resolved.error;
    const candidate = resolved.value.candidates[0]!;
    expect(candidate.qualification?.E01).toBe(true);
    expect(candidate.evidence?.wallet).toBe('unavailable');
    const run = await new PostgresPaperRegistry(harness.db).createRun({
      strategy: fixtureStrategy({ entry: 'E01' }),
      mode: ExecutionMode.FORWARD_PAPER,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'rich-media-fixture',
      periodStart: NOW,
      periodEnd: new Date(now.getTime() + 3600000),
      cashAsset: 'SOL',
      initialCash: '100',
    });
    if (run.kind === 'failure') throw run.error;
    const trader = new PostgresPaperTrader(harness.db, { now: (): Date => now });
    expect(
      (
        await trader.tick(
          run.value.portfolioId,
          fixtureMarket(now, '1000', MINT_A),
          candidate.narrativeSignalId
        )
      ).kind
    ).toBe('success');
    now = new Date(now.getTime() + 1000);
    await trader.tick(run.value.portfolioId, fixtureMarket(now, '1000', MINT_A));
    expect(
      await paperRows(harness.db, 'SELECT 1 FROM positions WHERE open_quantity>0')
    ).toHaveLength(1);
  });

  it('withdraws prior candidates on overflow and closes expired attention without reinference', async () => {
    await observation();
    await observation('b');
    await token();
    const model = interpreter();
    const config = configuration();
    config.universeLimit = 1;
    const runtime = new LiveNarrativeRuntime(harness.db, model, config, () => now);
    await runtime.cycle();
    expect((await source()).payload.candidates).toHaveLength(1);
    now = new Date(now.getTime() + 1000);
    await token(MINT_B);
    expect(await runtime.cycle()).toMatchObject({ incomplete: true, published: 1 });
    expect((await source()).payload.candidates).toHaveLength(0);
    now = new Date(now.getTime() + 7200000);
    await runtime.cycle();
    expect(
      await paperRows(
        harness.db,
        'SELECT state FROM nit_narrative_reports ORDER BY available_at DESC LIMIT 1'
      )
    ).toEqual([{ state: 'closed' }]);
    expect(model.interpret).toHaveBeenCalledTimes(2);
  });
});
