import { randomUUID } from 'node:crypto';
import { DatabasePool, PostgresResearchStore, PostgresSignalStore } from '@nit/database-public';
import { TestDatabase, createTestDatabase, hasTestDatabase } from '@nit/database-public/testing';
import { ResearchRecord, contentHash, evidenceHash, SignalKind } from '@nit/signals';
import { Chain, generateSignalId } from '@nit/domain';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  PostgresResearchReplay,
  ResearchDataset,
  ResearchExperiment,
  ResearchReplayEvent,
} from './research-replay';
import { paperRows } from './paper-store';
import { createStrategySnapshot } from './strategy';
import { fixtureStrategy, fixtureMarket, PAPER_START, PAPER_TOKEN } from './paper.fixtures';

describe.skipIf(!hasTestDatabase())('immutable provenance and matched research replay', () => {
  let harness: TestDatabase;
  let db: DatabasePool;
  let store: PostgresResearchStore;
  let replay: PostgresResearchReplay;
  const stamp = (ms = 0): string => new Date(PAPER_START.getTime() + ms).toISOString();
  beforeAll(async () => {
    harness = await createTestDatabase('research_replay');
    db = harness.db;
    store = new PostgresResearchStore(db);
    replay = new PostgresResearchReplay(db);
  });
  afterAll(async () => harness.teardown());
  async function save(record: ResearchRecord): Promise<ResearchRecord> {
    const result = await store.append(record);
    if (result.kind === 'failure') throw result.error;
    return result.value;
  }
  function base(
    kind: ResearchRecord['kind'],
    ms = 0
  ): {
    recordId: string;
    scope: string;
    version: number;
    occurredAt: string;
    observedAt: string;
    availableAt: string;
    producerVersion: string;
    evidenceIds: string[];
    kind: ResearchRecord['kind'];
  } {
    return {
      recordId: randomUUID(),
      scope: randomUUID(),
      version: 1,
      occurredAt: stamp(),
      observedAt: stamp(),
      availableAt: stamp(ms),
      producerVersion: 'fixture:v1',
      evidenceIds: [],
      kind,
    };
  }
  async function evidence(
    ms = 0,
    previousEpisodeId: string | null = null
  ): Promise<{
    episodeId: string;
    featureId: string;
    artifactId: string;
    interpretationId: string;
  }> {
    const text = `Synthetic public catalyst ${randomUUID()}`;
    const artifact = await save({
      ...base('artifact'),
      kind: 'artifact',
      data: {
        provider: 'fixture',
        sourceId: randomUUID(),
        authorId: randomUUID(),
        contentHash: contentHash(text),
        text,
        sourceUrl: 'https://example.test/source',
        derivativeOf: null,
        media: [],
      },
    });
    const episode = await save({
      ...base('episode'),
      kind: 'episode',
      data: {
        narrativeId: randomUUID(),
        tokenMints: [PAPER_TOKEN],
        episodeType: previousEpisodeId === null ? 'launch' : 'revival',
        previousEpisodeId,
        originArtifactIds: [artifact.recordId],
      },
    });
    const interpretation = await save({
      ...base('interpretation'),
      kind: 'interpretation',
      data: {
        episodeId: episode.recordId,
        title: 'Synthetic narrative',
        narrativeStrengthBps: 9000,
        status: 'confirmed',
        artifactIds: [artifact.recordId],
        extractorVersion: 'explicit-text:v1',
        modelVersion: null,
        promptVersion: null,
        historicalMode: 'captured',
        candidates: [
          {
            tokenMint: PAPER_TOKEN,
            linkConfidenceBps: 9000,
            status: 'confirmed',
            disposition: 'candidate',
            reasonCodes: ['EXPLICIT_MINT'],
          },
        ],
      },
    });
    const feature = await save({
      ...base('feature', ms),
      kind: 'feature',
      data: {
        episodeId: episode.recordId,
        interpretationId: interpretation.recordId,
        tokenMint: PAPER_TOKEN,
        narrative: 'confirmed',
        linkage: 'confirmed',
        wallet: 'confirmed',
        relationship: 'clear',
        independentWallets: 2,
        attributionAmbiguous: false,
        selectionRank: 1,
        correlationWindowOpen: false,
        canonicalTokenKnown: true,
        narrativeStrengthBps: 9000,
        linkConfidenceBps: 9000,
        artifactIds: [artifact.recordId],
        coverageIds: [],
        policyVersion: 'fixture:v1',
        confidenceMeaning: 'evidence_score_not_profit_probability',
      },
    });
    return {
      episodeId: episode.recordId,
      featureId: feature.recordId,
      artifactId: artifact.recordId,
      interpretationId: interpretation.recordId,
    };
  }
  async function dataset(featureDelay = 0, unsellable = false): Promise<ResearchDataset> {
    const records = await evidence(featureDelay);
    const scenarioId = randomUUID();
    const event = (ms: number, index: number, cash = '1000'): ResearchReplayEvent => {
      const m = fixtureMarket(new Date(stamp(ms)), cash);
      return {
        id: `${index}`,
        scenarioId,
        tokenMint: PAPER_TOKEN,
        availableAt: stamp(ms),
        kind: index === 0 ? 'opportunity' : 'market',
        market: {
          ...m,
          ...(unsellable && ms >= 2000 ? { sellable: false, realCashReserves: '0' } : {}),
          occurredAt: stamp(ms),
          observedAt: stamp(ms),
          availableAt: stamp(ms),
        },
        featureSnapshotId: index === 0 ? records.featureId : null,
        latencyMs: { market: 0, narrative: featureDelay, wallet: featureDelay },
        costs: { marketCash: '0.001', narrativeCash: '0.002', walletCash: '0.003' },
      };
    };
    return {
      schemaVersion: 1,
      periodStart: stamp(),
      decisionEnd: stamp(5000),
      evaluationEnd: stamp(10000),
      partition: 'development',
      universeDefinition: 'Ex-ante synthetic market census, includes failed tokens',
      discovery: 'market_census',
      measurement: 'assumed_proxy',
      interpretationMode: 'captured',
      coverage: 'Synthetic complete reserve path; no provider performance claim',
      seed: 'fixed',
      extractorVersions: ['explicit-text:v1'],
      universe: [
        {
          scenarioId,
          episodeId: records.episodeId,
          tokenMint: PAPER_TOKEN,
          discoveredAt: stamp(),
          outcomeClass: unsellable ? 'dead' : 'live',
          exitHorizonComplete: !unsellable,
        },
      ],
      events: [
        event(0, 0),
        event(1000, 1),
        event(2000, 2, '400'),
        event(3000, 3, '400'),
        event(4000, 4, '400'),
        event(10000, 5, '400'),
      ],
    };
  }
  async function experiment(
    data: ResearchDataset,
    timing: ResearchExperiment['timing'] = 'fixed_decision'
  ): Promise<ResearchExperiment> {
    const datasetHash = await replay.freezeDataset(data);
    const variants: ResearchExperiment['variants'] = {
      A: fixtureStrategy({ entry: 'E00' }),
      B: fixtureStrategy({ entry: 'E01' }),
      C: fixtureStrategy({ entry: 'E03' }),
    };
    const config: ResearchExperiment = {
      experimentId: randomUUID(),
      datasetHash,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'fixture-research-v1',
      cashAsset: 'SOL',
      initialCash: '100',
      timing,
      policyVersion: 'research:v1',
      qualificationPolicy: {
        version: 1,
        minimumNarrativeStrengthBps: 5000,
        minimumLinkConfidenceBps: 5000,
        minimumIndependentWallets: 2,
        narrativeRelationshipHandling: 'advisory',
        combinedRelationshipHandling: 'review',
      },
      variants,
    };
    await replay.create(config);
    return config;
  }
  async function finish(id: string, chunk = 2): Promise<Record<string, unknown>> {
    for (let i = 0; i < 20; i++) {
      if ((await replay.advance(id, chunk)).complete)
        return (await replay.report(id)) as Record<string, unknown>;
    }
    throw new Error('Replay failed to complete');
  }
  it('deduplicates exact evidence, forbids mutation and discounts repost lineages', async () => {
    const records = await evidence();
    const original = await store.at(records.artifactId, new Date(stamp()));
    expect(original?.kind).toBe('artifact');
    expect(await store.append(original)).toMatchObject({ kind: 'success' });
    if (original?.kind !== 'artifact') throw new Error('Expected artifact');
    expect(
      await store.append({ ...original, data: { ...original.data, text: 'changed' } })
    ).toMatchObject({ kind: 'failure' });
    const repost = await save({
      ...original,
      ...base('artifact'),
      kind: 'artifact',
      data: {
        ...original.data,
        sourceId: randomUUID(),
        authorId: 'reposter',
        derivativeOf: original.recordId,
      },
    });
    expect(
      await store.independentSources([original.recordId, repost.recordId], new Date(stamp()))
    ).toBe(1);
    expect(
      (
        await db.query('UPDATE research_records SET version=2 WHERE record_id=$1', [
          original.recordId,
        ])
      ).kind
    ).toBe('failure');
    expect(
      (await db.query('DELETE FROM research_links WHERE record_id=$1', [repost.recordId])).kind
    ).toBe('failure');
  });
  it('keeps future evidence unavailable and requires coverage for negative observations', async () => {
    const records = await evidence(5000);
    expect(await store.at(records.featureId, new Date(stamp(4999)))).toBeNull();
    const feature = await store.at(records.featureId, new Date(stamp(5000)));
    if (feature?.kind !== 'feature') throw new Error('Missing feature');
    expect(
      await store.append({
        ...feature,
        recordId: randomUUID(),
        scope: randomUUID(),
        data: { ...feature.data, wallet: 'observed_nonconfirming' },
      })
    ).toMatchObject({ kind: 'failure' });
    const published = await store.publishInterpretation(
      records.interpretationId,
      new Date(stamp())
    );
    expect(published.kind).toBe('success');
    if (published.kind === 'failure') throw published.error;
    const signal = await new PostgresSignalStore(db).getById(published.value as never);
    expect(signal.kind).toBe('success');
  });
  it('freezes deterministic identities, rejects future market data and mismatched experiment assumptions', async () => {
    const data = await dataset();
    expect(await replay.freezeDataset(data)).toBe(
      await replay.freezeDataset({ ...data, events: [...data.events].reverse() })
    );
    const first = data.events[0]!;
    await expect(
      replay.freezeDataset({
        ...data,
        events: [{ ...first, market: { ...first.market!, availableAt: stamp(5000) } }],
      })
    ).rejects.toThrow('unavailable');
    const config = await experiment(data);
    await replay.create(config);
    await expect(replay.create({ ...config, policyVersion: 'changed' })).rejects.toThrow(
      'collision'
    );
    const altered = fixtureStrategy({ entry: 'E03', exit: 'X02' });
    await expect(
      replay.create({
        ...config,
        experimentId: randomUUID(),
        variants: { ...config.variants, C: altered },
      })
    ).rejects.toThrow('identical');
  });
  it('replays identical inputs across chunk sizes with matched execution and explicit costs', async () => {
    const data = await dataset();
    const one = await experiment(data);
    const two = await experiment(data);
    const first = await finish(one.experimentId, 1);
    const second = await finish(two.experimentId, 6);
    expect(first['variants']).toEqual(second['variants']);
    expect(first['underlyingScenarioCount']).toBe(1);
    const variants = first['variants'] as {
      counts: { fills: string };
      researchCostCash: string;
      netCashAfterResearchCosts: string;
    }[];
    expect(variants.map((v) => v.counts.fills)).toEqual(['2', '2', '2']);
    expect(variants.map((v) => v.researchCostCash)).toEqual(['0.001', '0.003', '0.006']);
    const snapshots = await paperRows(
      db,
      `SELECT 1 FROM decision_feature_snapshots s JOIN strategy_decisions d USING(decision_id)
      JOIN research_replay_variants v USING(portfolio_id) WHERE v.experiment_id=$1`,
      [one.experimentId]
    );
    expect(snapshots.length).toBeGreaterThan(0);
    expect((await replay.advance(one.experimentId)).steps).toBe(0);
  });
  it('separates fixed-time information from measured pipeline delay without backdating evidence', async () => {
    const data = await dataset(1000);
    const fixed = await experiment(data);
    const delayed = await experiment(data, 'end_to_end');
    const a = await finish(fixed.experimentId);
    const b = await finish(delayed.experimentId);
    const fixedVariants = a['variants'] as { counts: { orders: string } }[];
    expect(fixedVariants[0]?.counts.orders).toBe('2');
    expect(fixedVariants[1]?.counts.orders).toBe('0');
    const delayedVariants = b['variants'] as { counts: { orders: string } }[];
    expect(Number(delayedVariants[1]?.counts.orders)).toBeGreaterThan(0);
  });
  it('survives an interruption after engine commit and yields before real-time protection is starved', async () => {
    const config = await experiment(await dataset());
    expect(await replay.advance(config.experimentId, 1, () => Promise.resolve(true))).toEqual({
      complete: false,
      steps: 0,
    });
    await paperRows(
      db,
      `CREATE FUNCTION fail_replay_checkpoint() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'fixture interruption'; END; $$ LANGUAGE plpgsql;
      CREATE TRIGGER fixture_checkpoint_failure BEFORE INSERT ON research_replay_steps FOR EACH ROW WHEN (NEW.step=2) EXECUTE FUNCTION fail_replay_checkpoint()`
    );
    try {
      await expect(replay.advance(config.experimentId, 4)).rejects.toThrow('fixture interruption');
      const cursor = await paperRows(
        db,
        'SELECT 1 FROM research_replay_steps WHERE experiment_id=$1',
        [config.experimentId]
      );
      expect(cursor).toHaveLength(2);
    } finally {
      await paperRows(
        db,
        'DROP TRIGGER fixture_checkpoint_failure ON research_replay_steps; DROP FUNCTION fail_replay_checkpoint()'
      );
    }
    const report = await finish(config.experimentId);
    const variants = report['variants'] as { counts: { fills: string } }[];
    expect(variants.map((v) => v.counts.fills)).toEqual(['2', '2', '2']);
  });
  it('retains failed/dead tokens and unsellable residuals without fabricating terminal proceeds', async () => {
    const config = await experiment(await dataset(0, true));
    const result = await finish(config.experimentId);
    const variants = result['variants'] as {
      counts: { residuals: string };
      terminalResidualValue: string;
    }[];
    expect(
      variants.every((v) => v.counts.residuals === '1' && v.terminalResidualValue === '0')
    ).toBe(true);
    expect(evidenceHash(result)).toBe(evidenceHash(await replay.report(config.experimentId)));
  });
  it('creates a new revival episode rather than overwriting earlier interpretations', async () => {
    const first = await evidence();
    const revival = await evidence(0, first.episodeId);
    expect(revival.episodeId).not.toBe(first.episodeId);
    const original = await store.at(first.episodeId, new Date(stamp()));
    expect(original?.kind === 'episode' && original.data.episodeType).toBe('launch');
    const input = fixtureStrategy();
    expect(createStrategySnapshot(input).configHash).toBe(input.configHash);
  });
  it('permits a closed mint to trade a later episode while preserving earlier positions', async () => {
    const data = await dataset();
    const first = data.universe[0]!;
    const later = await evidence(0, first.episodeId);
    const scenarioId = randomUUID();
    const late = data.events.slice(0, 4).map((event, index): ResearchReplayEvent => {
      const ms = 5000 + index * 1000;
      const m = fixtureMarket(new Date(stamp(ms)), index < 2 ? '1000' : '400');
      return {
        ...event,
        id: `revival:${index}`,
        scenarioId,
        availableAt: stamp(ms),
        featureSnapshotId: index === 0 ? later.featureId : null,
        market: { ...m, occurredAt: stamp(ms), observedAt: stamp(ms), availableAt: stamp(ms) },
      };
    });
    const config = await experiment({
      ...data,
      universe: [
        ...data.universe,
        { ...first, scenarioId, episodeId: later.episodeId, discoveredAt: stamp(5000) },
      ],
      events: [...data.events.filter((e) => e.availableAt < stamp(5000)), ...late],
    });
    const report = await finish(config.experimentId);
    expect(
      (report['variants'] as { counts: { fills: string } }[]).map((v) => v.counts.fills)
    ).toEqual(['4', '4', '4']);
    const positions = await paperRows<{ count: string }>(
      db,
      `SELECT COUNT(*)::text AS count FROM positions p JOIN research_replay_variants v USING(portfolio_id)
      WHERE v.experiment_id=$1`,
      [config.experimentId]
    );
    expect(Number(positions[0]?.count)).toBe(6);
  });
  it('runs wallet-only replay without a narrative feature being available', async () => {
    const data = await dataset(20000);
    const source = await new PostgresSignalStore(db).append({
      eventId: generateSignalId(),
      source: 'wallet_tracker',
      sourceVersion: 'fixture',
      kind: SignalKind.WALLET_CONVERGENCE,
      chain: Chain.SOLANA,
      tokenMint: PAPER_TOKEN,
      occurredAt: PAPER_START,
      observedAt: PAPER_START,
      availableAt: PAPER_START,
      origin: { kind: 'provider', provider: 'fixture-research', referenceId: randomUUID() },
      evidenceIds: [],
      payload: { side: 'buy', independentSourceCount: 2 },
    });
    if (source.kind === 'failure') throw source.error;
    const stripped = {
      ...data,
      events: data.events.map((event) => ({
        ...event,
        featureSnapshotId: null,
        walletSignalId: source.value.signalId,
      })),
    };
    const hash = await replay.freezeDataset(stripped);
    const config: ResearchExperiment = {
      experimentId: randomUUID(),
      datasetHash: hash,
      codeHash: 'a'.repeat(64),
      codeCommitSha: 'fixture-research-v1',
      cashAsset: 'SOL',
      initialCash: '100',
      timing: 'fixed_decision',
      policyVersion: 'research:v1',
      qualificationPolicy: {
        version: 1,
        minimumNarrativeStrengthBps: 5000,
        minimumLinkConfidenceBps: 5000,
        minimumIndependentWallets: 2,
        narrativeRelationshipHandling: 'review',
        combinedRelationshipHandling: 'review',
      },
      variants: {
        A: fixtureStrategy({ entry: 'E00' }),
        B: fixtureStrategy({ entry: 'E01' }),
        C: fixtureStrategy({ entry: 'E03' }),
        W: fixtureStrategy({ entry: 'E02' }),
      },
    };
    await replay.create(config);
    const report = await finish(config.experimentId);
    const variants = report['variants'] as { variant: string; counts: { fills: string } }[];
    expect(variants.find((v) => v.variant === 'W')?.counts.fills).toBe('2');
    expect(variants.find((v) => v.variant === 'B')?.counts.fills).toBe('0');
  });
});
