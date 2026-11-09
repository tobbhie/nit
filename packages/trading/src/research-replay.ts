import { randomUUID } from 'node:crypto';
import {
  DatabasePool,
  PostgresResearchStore,
  PostgresSignalStore,
  clientExecutor,
} from '@nit/database-public';
import { Chain, PortfolioId, SignalId, generateSignalId } from '@nit/domain';
import {
  ResearchFeature,
  SignalKind,
  evidenceHash,
  EvidenceQualificationPolicy,
  qualifyEvidence,
  validateQualificationPolicy,
} from '@nit/signals';
import { PaperMarketSnapshot, validatePaperMarket, paperQuote } from './paper-market';
import { PostgresPaperTrader } from './paper-engine';
import { PostgresPaperRegistry, paperRows } from './paper-store';
import { ExecutionMode, OrderSide } from './runtime';
import { StrategySnapshot, createStrategySnapshot } from './strategy';
import { paperPolicy } from './paper-policy';
import { units, amount, UNIT } from './fixed';
import { marketDiscoveryEvidence } from './market-discovery-evidence';

type Variant = 'A' | 'B' | 'C' | 'W';
function signedAmount(value: bigint): string {
  return value < 0n ? `-${amount(-value)}` : amount(value);
}
function signedUnits(value: string): bigint {
  return value.startsWith('-') ? -units(value.slice(1)) : units(value);
}
export interface ResearchReplayEvent {
  readonly id: string;
  readonly scenarioId: string;
  readonly tokenMint: string;
  readonly availableAt: string;
  readonly kind: 'opportunity' | 'market';
  readonly market:
    | (Omit<PaperMarketSnapshot, 'occurredAt' | 'observedAt' | 'availableAt'> & {
        occurredAt: string;
        observedAt: string;
        availableAt: string;
      })
    | null;
  readonly featureSnapshotId: string | null;
  readonly walletSignalId?: string;
  readonly latencyMs: {
    readonly market: number;
    readonly narrative: number;
    readonly wallet: number;
  };
  readonly costs: {
    readonly marketCash: string;
    readonly narrativeCash: string;
    readonly walletCash: string;
  };
}
export interface ResearchDataset {
  readonly featureHashes?: Readonly<Record<string, string>>;
  readonly discoveryHashes?: Readonly<Record<string, string>>;
  readonly schemaVersion: 1;
  readonly periodStart: string;
  readonly decisionEnd: string;
  readonly evaluationEnd: string;
  readonly partition: 'development' | 'holdout' | 'forward';
  readonly universeDefinition: string;
  readonly discovery: 'market_census' | 'curated_proxy';
  readonly measurement: 'measured' | 'assumed_proxy';
  readonly interpretationMode: 'captured' | 'regenerated_approximation';
  readonly coverage: string;
  readonly seed: string;
  readonly extractorVersions: readonly string[];
  readonly universe: readonly {
    scenarioId: string;
    episodeId: string;
    /** Required for a live open-universe episode: immutable membership evidence known at discovery. */
    membershipFeatureSnapshotId?: string;
    tokenMint: string;
    discoveredAt: string;
    outcomeClass: 'live' | 'dead' | 'failed' | 'illiquid' | 'ungraduated';
    exitHorizonComplete: boolean;
  }[];
  readonly events: readonly ResearchReplayEvent[];
}
export interface ResearchExperiment {
  readonly experimentId: string;
  readonly datasetHash: string;
  readonly codeHash: string;
  readonly codeCommitSha: string;
  readonly cashAsset: string;
  readonly initialCash: string;
  readonly timing: 'fixed_decision' | 'end_to_end';
  readonly policyVersion: string;
  readonly qualificationPolicy: EvidenceQualificationPolicy;
  readonly variants: Readonly<Partial<Record<Variant, StrategySnapshot>>>;
}
function time(value: string): number {
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed) || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/.test(value))
    throw new Error('Invalid replay UTC timestamp');
  return parsed;
}
function market(event: ResearchReplayEvent): PaperMarketSnapshot | null {
  return event.market === null
    ? null
    : {
        ...event.market,
        occurredAt: new Date(event.market.occurredAt),
        observedAt: new Date(event.market.observedAt),
        availableAt: new Date(event.market.availableAt),
      };
}
function mask(variant: Variant): { narrative: boolean; wallet: boolean } {
  return {
    narrative: variant === 'B' || variant === 'C',
    wallet: variant === 'C' || variant === 'W',
  };
}

/** Closed, explicitly supplied history only. No provider fetching, model calls or inferred prices. */
export class PostgresResearchReplay {
  constructor(private readonly db: DatabasePool) {}
  async freezeDataset(raw: ResearchDataset): Promise<string> {
    if (
      raw.schemaVersion !== 1 ||
      raw.events.length > 5000 ||
      raw.universe.length === 0 ||
      raw.universe.length > 512 ||
      !raw.universeDefinition.trim() ||
      !raw.coverage.trim() ||
      !raw.seed.trim() ||
      raw.extractorVersions.length === 0 ||
      !['development', 'holdout', 'forward'].includes(raw.partition) ||
      !['market_census', 'curated_proxy'].includes(raw.discovery) ||
      !['measured', 'assumed_proxy'].includes(raw.measurement) ||
      !['captured', 'regenerated_approximation'].includes(raw.interpretationMode)
    )
      throw new Error('Invalid bounded dataset manifest');
    if (
      time(raw.periodStart) > time(raw.decisionEnd) ||
      time(raw.decisionEnd) > time(raw.evaluationEnd)
    )
      throw new Error('Invalid evaluation horizon');
    if (
      new Set(raw.universe.map((s) => s.scenarioId)).size !== raw.universe.length ||
      new Set(raw.events.map((e) => e.id)).size !== raw.events.length
    )
      throw new Error('Duplicate replay identity');
    for (const item of raw.universe) {
      if (
        !['live', 'dead', 'failed', 'illiquid', 'ungraduated'].includes(item.outcomeClass) ||
        typeof item.exitHorizonComplete !== 'boolean' ||
        !item.scenarioId.trim()
      )
        throw new Error('Invalid universe coverage classification');
      time(item.discoveredAt);
    }
    const featureHashes: Record<string, string> = {};
    const discoveryHashes: Record<string, string> = {};
    for (const event of raw.events) {
      const scenario = raw.universe.find(
        (s) => s.scenarioId === event.scenarioId && s.tokenMint === event.tokenMint
      );
      if (
        scenario === undefined ||
        time(scenario.discoveredAt) > time(event.availableAt) ||
        time(event.availableAt) < time(raw.periodStart) ||
        time(event.availableAt) > time(raw.evaluationEnd) ||
        (event.kind === 'opportunity' && time(event.availableAt) > time(raw.decisionEnd))
      )
        throw new Error('Event outside ex-ante universe/horizon');
      const episode = await new PostgresResearchStore(this.db).at(
        scenario.episodeId,
        new Date(scenario.discoveredAt)
      );
      if (episode?.kind !== 'episode') throw new Error('Ex-ante episode identity is unavailable');
      if (!episode.data.tokenMints.includes(event.tokenMint)) {
        if (episode.data.universeMode === 'open' && raw.discovery === 'market_census') {
          // The market baseline must not require prior NIT selection. Pin independent
          // canonical discovery instead of conditioning its universe on a narrative feature.
          const discovery = await marketDiscoveryEvidence(
            this.db,
            event.tokenMint,
            new Date(scenario.discoveredAt)
          );
          if (discovery === null) throw new Error('Ex-ante market census discovery is unavailable');
          discoveryHashes[scenario.scenarioId] = discovery.hash;
        } else {
          const membership =
            scenario.membershipFeatureSnapshotId === undefined
              ? null
              : await new PostgresResearchStore(this.db).at(
                  scenario.membershipFeatureSnapshotId,
                  new Date(scenario.discoveredAt)
                );
          if (
            episode.data.universeMode !== 'open' ||
            membership?.kind !== 'feature' ||
            membership.data.episodeId !== episode.recordId ||
            membership.data.tokenMint !== event.tokenMint
          )
            throw new Error('Ex-ante live episode membership is unavailable');
          featureHashes[membership.recordId] = evidenceHash(membership);
        }
      }
      if (!['opportunity', 'market'].includes(event.kind) || !event.id.trim())
        throw new Error('Invalid replay event');
      for (const n of Object.values(event.latencyMs))
        if (!Number.isSafeInteger(n) || n < 0) throw new Error('Invalid measured latency');
      for (const cash of Object.values(event.costs)) units(cash);
      if (event.walletSignalId !== undefined) {
        const wallet = await new PostgresSignalStore(this.db).getById(
          event.walletSignalId as SignalId
        );
        if (wallet.kind === 'failure') throw wallet.error;
        if (
          wallet.value === null ||
          wallet.value.kind !== SignalKind.WALLET_CONVERGENCE ||
          wallet.value.tokenMint !== event.tokenMint ||
          wallet.value.availableAt > new Date(raw.evaluationEnd)
        )
          throw new Error('Invalid frozen wallet source');
      }
      const snapshot = market(event);
      if (snapshot !== null) {
        validatePaperMarket(snapshot, new Date(event.availableAt));
        if (snapshot.tokenMint !== event.tokenMint) throw new Error('Replay market mint mismatch');
      }
      if (event.featureSnapshotId !== null) {
        const record = await new PostgresResearchStore(this.db).at(
          event.featureSnapshotId,
          new Date(raw.evaluationEnd)
        );
        if (
          record?.kind !== 'feature' ||
          record.data.tokenMint !== event.tokenMint ||
          record.data.episodeId !== scenario.episodeId
        )
          throw new Error('Replay feature scope is unavailable or inconsistent');
        const interpretation = await new PostgresResearchStore(this.db).at(
          record.data.interpretationId,
          new Date(record.availableAt)
        );
        if (
          interpretation?.kind !== 'interpretation' ||
          (raw.interpretationMode === 'captured' &&
            interpretation.data.historicalMode !== 'captured')
        )
          throw new Error('Regenerated interpretation must be labeled as approximation');
        if (!raw.extractorVersions.includes(interpretation.data.extractorVersion))
          throw new Error('Dataset must pin the actual extractor version');
        featureHashes[record.recordId] = evidenceHash(record);
      }
    }
    const snapshots = raw.events.flatMap((event) =>
      event.market === null ? [] : [event.market.snapshotId]
    );
    if (new Set(snapshots).size !== snapshots.length)
      throw new Error('Each recorded reserve snapshot must have one manifest identity');
    const manifest = {
      ...raw,
      featureHashes,
      discoveryHashes,
      universe: [...raw.universe].sort((a, b) =>
        a.scenarioId < b.scenarioId ? -1 : a.scenarioId > b.scenarioId ? 1 : 0
      ),
      events: [...raw.events].sort(
        (a, b) =>
          time(a.availableAt) - time(b.availableAt) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)
      ),
    };
    const digest = evidenceHash(manifest);
    await paperRows(
      this.db,
      'INSERT INTO research_datasets(dataset_hash,manifest) VALUES ($1,$2) ON CONFLICT DO NOTHING',
      [digest, JSON.stringify(manifest)]
    );
    return digest;
  }
  async create(config: ResearchExperiment): Promise<void> {
    validateQualificationPolicy(config.qualificationPolicy);
    const rows = await paperRows<{ manifest: ResearchDataset }>(
      this.db,
      'SELECT manifest FROM research_datasets WHERE dataset_hash=$1',
      [config.datasetHash]
    );
    const dataset = rows[0]?.manifest;
    if (dataset === undefined) throw new Error('Dataset is not frozen');
    if (!['fixed_decision', 'end_to_end'].includes(config.timing) || !config.policyVersion.trim())
      throw new Error('Explicit experiment semantics required');
    if (
      config.variants.A === undefined ||
      config.variants.B === undefined ||
      config.variants.C === undefined ||
      Object.keys(config.variants).some((v) => !['A', 'B', 'C', 'W'].includes(v))
    )
      throw new Error('Matched A/B/C variants are required');
    let common: string | undefined;
    for (const [variant, strategy] of Object.entries(config.variants)) {
      const snapshot = createStrategySnapshot(strategy);
      paperPolicy(snapshot);
      if (
        snapshot.configHash !== strategy.configHash ||
        strategy.entry.policyId !== { A: 'E00', B: 'E01', C: 'E03', W: 'E02' }[variant]
      )
        throw new Error('Variant entry hypothesis mismatch');
      const digest = evidenceHash({
        exit: snapshot.exit,
        protection: snapshot.protection,
        risk: snapshot.risk,
        entryConfig: snapshot.entry.config,
      });
      if (common !== undefined && common !== digest)
        throw new Error(
          'Matched experiments require identical execution, exit, risk, and sizing assumptions'
        );
      common = digest;
    }
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      await paperRows(db, 'SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [
        config.experimentId,
      ]);
      const existing = await paperRows<{ config_hash: string }>(
        db,
        'SELECT config_hash FROM research_experiments WHERE experiment_id=$1',
        [config.experimentId]
      );
      if (existing[0] !== undefined) {
        if (existing[0].config_hash !== evidenceHash(config))
          throw new Error('Experiment identity collision');
        return;
      }
      await paperRows(
        db,
        'INSERT INTO research_experiments(experiment_id,dataset_hash,config_hash,config) VALUES ($1,$2,$3,$4)',
        [config.experimentId, config.datasetHash, evidenceHash(config), JSON.stringify(config)]
      );
      for (const [variant, strategy] of Object.entries(config.variants)) {
        const run = await new PostgresPaperRegistry(this.db).createRun(
          {
            strategy,
            mode: ExecutionMode.HISTORICAL,
            codeHash: config.codeHash,
            codeCommitSha: config.codeCommitSha,
            periodStart: new Date(dataset.periodStart),
            periodEnd: new Date(dataset.decisionEnd),
            cashAsset: config.cashAsset,
            initialCash: config.initialCash,
          },
          db
        );
        if (run.kind === 'failure') throw run.error;
        await paperRows(db, 'INSERT INTO research_replay_variants VALUES ($1,$2,$3)', [
          config.experimentId,
          variant,
          run.value.portfolioId,
        ]);
      }
    });
    if (result.kind === 'failure') throw result.error;
  }
  /** One low-priority chunk. Callers can yield between chunks; cursor and frozen identities survive restart. */
  async advance(
    id: string,
    maxSteps = 16,
    shouldYield?: () => Promise<boolean>
  ): Promise<{ complete: boolean; steps: number }> {
    if (!Number.isSafeInteger(maxSteps) || maxSteps < 1 || maxSteps > 64)
      throw new Error('Invalid replay chunk bound');
    const setup = await paperRows<{ config: ResearchExperiment; manifest: ResearchDataset }>(
      this.db,
      `SELECT e.config,d.manifest FROM research_experiments e JOIN research_datasets d USING(dataset_hash) WHERE experiment_id=$1`,
      [id]
    );
    const input = setup[0];
    if (input === undefined) throw new Error('Experiment not found');
    const { config, manifest } = input;
    const variants = await paperRows<{ variant: Variant; portfolio_id: PortfolioId }>(
      this.db,
      'SELECT * FROM research_replay_variants WHERE experiment_id=$1 ORDER BY variant',
      [id]
    );
    let steps = 0;
    let complete = true;
    for (const variant of variants) {
      const result = await this.db.transaction(async (client) => {
        const db = clientExecutor(client);
        const lock = await paperRows<{ locked: boolean }>(
          db,
          'SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS locked',
          [`${id}:${variant.variant}`]
        );
        if (!lock[0]?.locked) {
          complete = false;
          return;
        }
        const checkpoint = await paperRows<{ step: number }>(
          db,
          'SELECT step FROM research_replay_steps WHERE experiment_id=$1 AND variant=$2 ORDER BY step DESC LIMIT 1',
          [id, variant.variant]
        );
        const start = (checkpoint[0]?.step ?? -1) + 1;
        const end = Math.min(manifest.events.length, start + maxSteps);
        for (let step = start; step < end; step++) {
          if (shouldYield !== undefined && (await shouldYield())) {
            complete = false;
            return;
          }
          const event = manifest.events[step]!;
          const now = new Date(event.availableAt);
          const snapshot = market(event);
          const previous = manifest.events.slice(0, step + 1);
          const outcomes: unknown[] = [];
          if (snapshot !== null && snapshot.cashAsset !== config.cashAsset)
            throw new Error('Experiment cash-asset mismatch');
          const trader = new PostgresPaperTrader(this.db, { now: (): Date => now });
          // Re-delivering this exact step after a crash is harmless: signal, decision, market and fill identities are durable.
          const held = await paperRows<{ token_mint: string }>(
            this.db,
            'SELECT DISTINCT token_mint FROM positions WHERE portfolio_id=$1 AND open_quantity>0',
            [variant.portfolio_id]
          );
          let protectedAll = true;
          for (const position of held) {
            const latest = [...previous]
              .reverse()
              .find((e) => e.tokenMint === position.token_mint && e.market !== null);
            const observation = latest === undefined ? null : market(latest);
            if (
              observation === null ||
              now.getTime() - observation.occurredAt.getTime() >
                paperPolicy(config.variants[variant.variant]!).maxMarketAgeMs
            ) {
              protectedAll = false;
              continue;
            }
            const tick = await trader.tick(variant.portfolio_id, observation);
            if (tick.kind === 'failure') throw tick.error;
            outcomes.push(tick.value);
          }
          if (snapshot !== null) {
            const settled = await trader.tick(variant.portfolio_id, snapshot);
            if (settled.kind === 'failure') throw settled.error;
            outcomes.push(settled.value);
          }
          const attempted = await paperRows<{ outcome: { opportunityIds?: string[] } }>(
            db,
            'SELECT outcome FROM research_replay_steps WHERE experiment_id=$1 AND variant=$2',
            [id, variant.variant]
          );
          const done = new Set(attempted.flatMap((row) => row.outcome.opportunityIds ?? []));
          const opportunityIds: string[] = [];
          const evaluations: unknown[] = [];
          const candidates =
            config.timing === 'fixed_decision'
              ? event.kind === 'opportunity'
                ? [event]
                : []
              : previous.filter(
                  (e) =>
                    e.kind === 'opportunity' && !done.has(e.id) && e.tokenMint === event.tokenMint
                );
          for (const opportunity of candidates) {
            if (done.has(opportunity.id)) continue;
            const evidenceMask = mask(variant.variant);
            const extra = Math.max(
              opportunity.latencyMs.market,
              evidenceMask.narrative ? opportunity.latencyMs.narrative : 0,
              evidenceMask.wallet ? opportunity.latencyMs.wallet : 0
            );
            const feature =
              opportunity.featureSnapshotId === null
                ? null
                : await new PostgresResearchStore(this.db).at(
                    opportunity.featureSnapshotId,
                    new Date(manifest.evaluationEnd)
                  );
            if (feature !== null && feature.kind !== 'feature')
              throw new Error('Replay requires a feature snapshot');
            const due = Math.max(
              time(opportunity.availableAt) + extra,
              (evidenceMask.narrative || evidenceMask.wallet) && feature !== null
                ? time(feature.availableAt)
                : 0
            );
            const walletSource =
              variant.variant === 'W' && opportunity.walletSignalId !== undefined
                ? await new PostgresSignalStore(this.db).getById(
                    opportunity.walletSignalId as SignalId
                  )
                : null;
            if (walletSource?.kind === 'failure') throw walletSource.error;
            const walletAvailable =
              walletSource?.value !== null && walletSource?.value !== undefined
                ? walletSource.value.availableAt.getTime()
                : 0;
            if (config.timing === 'end_to_end' && now.getTime() < Math.max(due, walletAvailable))
              continue;
            opportunityIds.push(opportunity.id);
            done.add(opportunity.id);
            let reason: string | null =
              snapshot === null
                ? 'INSUFFICIENT_DATA'
                : !protectedAll
                  ? 'PROTECTION_DATA_UNAVAILABLE'
                  : null;
            if (now.getTime() > time(manifest.decisionEnd))
              reason = 'EVIDENCE_AFTER_DECISION_WINDOW';
            const available =
              feature !== null && time(feature.availableAt) <= now.getTime() ? feature : null;
            if (
              (evidenceMask.narrative || evidenceMask.wallet) &&
              available === null &&
              !(variant.variant === 'W' && walletAvailable > 0 && walletAvailable <= now.getTime())
            )
              reason = 'INSUFFICIENT_DATA';
            if (snapshot !== null && reason === null) {
              const signalId = await this.signal(
                config,
                variant.variant,
                opportunity,
                available,
                snapshot,
                now
              );
              const tick = await trader.tick(variant.portfolio_id, snapshot, signalId);
              if (tick.kind === 'failure') throw tick.error;
              outcomes.push(tick.value);
              const decisions = await paperRows<{ decision_type: string; reason_codes: string[] }>(
                this.db,
                'SELECT decision_type,reason_codes FROM strategy_decisions WHERE portfolio_id=$1 AND source_signal_id=$2 ORDER BY decided_at DESC LIMIT 1',
                [variant.portfolio_id, signalId]
              );
              if (decisions[0]?.decision_type === 'abstain')
                reason = decisions[0].reason_codes.join(',');
            }
            evaluations.push({
              opportunityId: opportunity.id,
              scenarioId: opportunity.scenarioId,
              reason,
              evidenceMask,
              delayMs: now.getTime() - time(opportunity.availableAt),
              costCash: amount(
                units(opportunity.costs.marketCash) +
                  (evidenceMask.narrative ? units(opportunity.costs.narrativeCash) : 0n) +
                  (evidenceMask.wallet ? units(opportunity.costs.walletCash) : 0n)
              ),
            });
          }
          const mark = await this.mark(
            variant.portfolio_id,
            previous,
            now,
            config.variants[variant.variant]!
          );
          await paperRows(
            this.db,
            'INSERT INTO research_replay_steps VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
            [
              id,
              variant.variant,
              step,
              now,
              JSON.stringify({ opportunityIds, evaluations, outcomes, ...mark }),
            ]
          );
          steps++;
        }
        if (end < manifest.events.length) complete = false;
      });
      if (result.kind === 'failure') throw result.error;
    }
    return { complete, steps };
  }
  private async signal(
    config: ResearchExperiment,
    variant: Variant,
    event: ResearchReplayEvent,
    feature: ResearchFeature | null,
    snapshot: PaperMarketSnapshot,
    now: Date
  ): Promise<import('@nit/domain').SignalId> {
    const data = feature?.data;
    const dataset = await paperRows<{ manifest: ResearchDataset }>(
      this.db,
      'SELECT manifest FROM research_datasets WHERE dataset_hash=$1',
      [config.datasetHash]
    );
    const episodeId = dataset[0]!.manifest.universe.find(
      (s) => s.scenarioId === event.scenarioId
    )!.episodeId;
    if (feature !== null && feature.data.episodeId !== episodeId)
      throw new Error('Feature episode does not match ex-ante opportunity');
    const evidenceMask = mask(variant);
    const qualification = qualifyEvidence(
      {
        narrative: evidenceMask.narrative ? (data?.narrative ?? 'unavailable') : 'unavailable',
        linkage: evidenceMask.narrative ? (data?.linkage ?? 'unavailable') : 'unavailable',
        wallet: evidenceMask.wallet ? (data?.wallet ?? 'unavailable') : 'unavailable',
        narrativeStrengthBps: evidenceMask.narrative ? (data?.narrativeStrengthBps ?? 0) : 0,
        linkConfidenceBps: evidenceMask.narrative ? (data?.linkConfidenceBps ?? 0) : 0,
        independentWallets: evidenceMask.wallet ? (data?.independentWallets ?? 0) : 0,
        relationship: evidenceMask.wallet ? (data?.relationship ?? 'unknown') : 'unknown',
        attributionAmbiguous: data?.attributionAmbiguous ?? true,
        selectionRank: data?.selectionRank ?? 1,
        correlationWindowOpen: data?.correlationWindowOpen ?? false,
        executionSafe:
          data?.canonicalTokenKnown === true &&
          snapshot.verified &&
          snapshot.tradable &&
          snapshot.sellable &&
          !snapshot.criticalRisk,
      },
      {
        ...config.qualificationPolicy,
        minimumIndependentWallets: Math.max(
          config.qualificationPolicy.minimumIndependentWallets,
          paperPolicy(config.variants[variant]!).minimumIndependentWallets
        ),
      }
    );
    let wallet =
      data?.wallet === 'confirmed' &&
      data.independentWallets >= paperPolicy(config.variants[variant]!).minimumIndependentWallets;
    let walletCount = data?.independentWallets ?? 0;
    if (variant === 'W' && event.walletSignalId !== undefined) {
      const source = await new PostgresSignalStore(this.db).getById<Record<string, unknown>>(
        event.walletSignalId as SignalId
      );
      if (source.kind === 'failure') throw source.error;
      if (source.value === null || source.value.availableAt > now)
        throw new Error('Wallet source not available at decision time');
      const count = source.value.payload['independentSourceCount'];
      walletCount = typeof count === 'number' && Number.isSafeInteger(count) ? count : 0;
      wallet =
        source.value.payload['side'] === 'buy' &&
        walletCount >= paperPolicy(config.variants[variant]!).minimumIndependentWallets;
    }
    const relation = evidenceMask.wallet && data?.relationship === 'suspected';
    const safeNarrative = qualification.E01;
    const kind =
      variant === 'A'
        ? SignalKind.MARKET
        : variant === 'W'
          ? SignalKind.WALLET_CONVERGENCE
          : variant === 'B'
            ? SignalKind.NARRATIVE
            : SignalKind.CANDIDATE_UPDATE;
    const result = await new PostgresSignalStore(this.db).append({
      eventId: generateSignalId(),
      source: variant === 'A' ? 'market' : variant === 'W' ? 'wallet_tracker' : 'nit',
      sourceVersion: 'research-replay:v1',
      kind,
      chain: Chain.SOLANA,
      tokenMint: event.tokenMint,
      ...(variant === 'B' || variant === 'C'
        ? { canonicalNarrativeId: `replay:${config.experimentId}:${event.scenarioId}` }
        : {}),
      occurredAt: new Date(event.availableAt),
      observedAt: now,
      availableAt: now,
      origin: {
        kind: 'provider',
        provider: 'nit-research-replay',
        referenceId: `${config.experimentId}:${variant}:${event.id}`,
      },
      evidenceIds: [],
      payload: {
        episodeId,
        marketBaselineEligible: variant === 'A',
        eligible: evidenceMask.narrative && safeNarrative,
        narrativeConfirmed:
          evidenceMask.narrative && (qualification.E01 || qualification.E03 || qualification.E04),
        walletConfirmed:
          evidenceMask.wallet &&
          wallet &&
          (!relation || config.qualificationPolicy.combinedRelationshipHandling === 'discount'),
        actionState: qualification.E03 ? 'trade_eligible' : 'watch',
        qualification,
        side: 'buy',
        independentSourceCount: evidenceMask.wallet ? walletCount : 0,
        ...(variant === 'W' && event.walletSignalId !== undefined
          ? { walletSourceSignalId: event.walletSignalId }
          : {}),
        evidenceMask,
        research: {
          datasetHash: config.datasetHash,
          scenarioId: event.scenarioId,
          featureHash: feature === null ? null : evidenceHash(feature),
          snapshotId: snapshot.snapshotId,
        },
        ...(feature === null || variant === 'A' ? {} : { featureSnapshotId: feature.recordId }),
      },
    });
    if (result.kind === 'failure') throw result.error;
    return result.value.signalId;
  }
  private async mark(
    portfolioId: PortfolioId,
    events: readonly ResearchReplayEvent[],
    now: Date,
    strategy: StrategySnapshot
  ): Promise<{ equityEstimateCash: string; exposureCostCash: string }> {
    const balances = await paperRows<{ current_cash: string }>(
      this.db,
      'SELECT current_cash FROM paper_portfolios WHERE portfolio_id=$1',
      [portfolioId]
    );
    let equity = units(balances[0]!.current_cash);
    let exposure = 0n;
    const positions = await paperRows<{
      token_mint: string;
      open_quantity: string;
      cost_basis: string;
      original_quantity: string;
    }>(this.db, 'SELECT * FROM positions WHERE portfolio_id=$1 AND open_quantity>0', [portfolioId]);
    const policy = paperPolicy(strategy);
    for (const position of positions) {
      exposure +=
        (units(position.cost_basis) * units(position.open_quantity)) /
        units(position.original_quantity);
      const latest = [...events]
        .reverse()
        .find((e) => e.tokenMint === position.token_mint && e.market !== null);
      const observed = latest === undefined ? null : market(latest);
      if (
        observed !== null &&
        observed.verified &&
        observed.tradable &&
        observed.sellable &&
        now.getTime() - observed.occurredAt.getTime() <= policy.maxMarketAgeMs
      ) {
        try {
          const quote = paperQuote(observed, OrderSide.SELL, position.open_quantity, policy);
          equity += units(quote.output) - units(quote.cashFee);
        } catch {
          /* Unexecutable residual has zero conservative valuation. */
        }
      }
    }
    return { equityEstimateCash: amount(equity), exposureCostCash: amount(exposure) };
  }
  async report(id: string): Promise<unknown> {
    const frozen = await paperRows<{ report: unknown }>(
      this.db,
      'SELECT report FROM research_reports WHERE experiment_id=$1',
      [id]
    );
    if (frozen[0] !== undefined) return frozen[0].report;
    const inputs = await paperRows<{ config: ResearchExperiment; manifest: ResearchDataset }>(
      this.db,
      `SELECT e.config,d.manifest FROM research_experiments e JOIN research_datasets d USING(dataset_hash) WHERE experiment_id=$1`,
      [id]
    );
    const input = inputs[0];
    if (input === undefined) throw new Error('Unknown experiment');
    const variants = await paperRows<{
      variant: Variant;
      portfolio_id: PortfolioId;
      current_cash: string;
    }>(
      this.db,
      'SELECT v.*,p.current_cash FROM research_replay_variants v JOIN paper_portfolios p USING(portfolio_id) WHERE experiment_id=$1 ORDER BY variant',
      [id]
    );
    const reports: (Record<string, unknown> & {
      variant: Variant;
      scenarioOutcomes: { scenarioId: string; cashPnl: string }[];
    })[] = [];
    let allComplete = true;
    for (const variant of variants) {
      const rows = await paperRows<{
        outcome: {
          equityEstimateCash: string;
          exposureCostCash: string;
          evaluations: {
            scenarioId: string;
            opportunityId: string;
            costCash: string;
            reason: string | null;
            delayMs: number;
          }[];
        };
      }>(
        this.db,
        'SELECT outcome FROM research_replay_steps WHERE experiment_id=$1 AND variant=$2 ORDER BY step',
        [id, variant.variant]
      );
      const complete = rows.length === input.manifest.events.length;
      allComplete &&= complete;
      const evaluated = rows.flatMap((r) => r.outcome.evaluations);
      const evaluatedIds = new Set(evaluated.map((e) => e.opportunityId));
      const variantMask = mask(variant.variant);
      const costFor = (e: ResearchReplayEvent): bigint =>
        units(e.costs.marketCash) +
        (variantMask.narrative ? units(e.costs.narrativeCash) : 0n) +
        (variantMask.wallet ? units(e.costs.walletCash) : 0n);
      const missing = input.manifest.events
        .filter((e) => e.kind === 'opportunity' && !evaluatedIds.has(e.id))
        .map((e) => ({
          opportunityId: e.id,
          scenarioId: e.scenarioId,
          reason: 'INSUFFICIENT_DATA',
          costCash: amount(costFor(e)),
          latencyCensored: true,
        }));
      const fees = await paperRows<{ cash_delta: string }>(
        this.db,
        'SELECT SUM(x.cash_delta)::text AS cash_delta FROM paper_execution_results x JOIN orders o USING(order_id) WHERE o.portfolio_id=$1 GROUP BY x.settled_at ORDER BY x.settled_at',
        [variant.portfolio_id]
      );
      let cash = units(input.config.initialCash);
      let peak = cash;
      let maxDrawdown = 0n;
      for (const row of fees) {
        cash +=
          BigInt(row.cash_delta.startsWith('-') ? '-1' : '1') *
          units(row.cash_delta.replace('-', ''));
        if (cash > peak) peak = cash;
        if (peak > 0n && ((peak - cash) * UNIT) / peak > maxDrawdown)
          maxDrawdown = ((peak - cash) * UNIT) / peak;
      }
      const counts = await paperRows<{
        orders: string;
        fills: string;
        failed: string;
        residuals: string;
        pending: string;
        partial: string;
      }>(
        this.db,
        `SELECT
        (SELECT COUNT(*) FROM orders WHERE portfolio_id=$1)::text AS orders,
        (SELECT COUNT(*) FROM order_fills f JOIN orders o USING(order_id) WHERE o.portfolio_id=$1)::text AS fills,
        (SELECT COUNT(*) FROM order_events e JOIN orders o USING(order_id) WHERE o.portfolio_id=$1 AND e.status='failed')::text AS failed,
        (SELECT COUNT(*) FROM positions WHERE portfolio_id=$1 AND open_quantity>0)::text AS residuals,
        (SELECT COUNT(*) FROM orders o WHERE portfolio_id=$1 AND NOT EXISTS(SELECT 1 FROM paper_execution_results x WHERE x.order_id=o.order_id))::text AS pending,
        (SELECT COUNT(*) FROM order_fills f JOIN orders o USING(order_id) WHERE o.portfolio_id=$1 AND f.status='partially_filled')::text AS partial`,
        [variant.portfolio_id]
      );
      const cost = input.manifest.events
        .filter((e) => e.kind === 'opportunity')
        .reduce((sum, e) => sum + costFor(e), 0n);
      const net = units(variant.current_cash) - cost - units(input.config.initialCash);
      const scenarioRows = await paperRows<{ scenario_id: string; cash_delta: string }>(
        this.db,
        `SELECT s.payload->'research'->>'scenarioId' AS scenario_id,SUM(x.cash_delta)::text AS cash_delta
        FROM paper_execution_results x JOIN orders o USING(order_id) JOIN strategy_decisions d USING(decision_id)
        JOIN signals s ON s.signal_id=d.source_signal_id WHERE o.portfolio_id=$1 GROUP BY s.payload->'research'->>'scenarioId'`,
        [variant.portfolio_id]
      );
      const signed = (value: string): bigint =>
        value.startsWith('-') ? -units(value.slice(1)) : units(value);
      const signedText = (value: bigint): string =>
        value < 0n ? `-${amount(-value)}` : amount(value);
      const scenarioOutcomes = input.manifest.universe.map((s) => ({
        scenarioId: s.scenarioId,
        outcomeClass: s.outcomeClass,
        cashPnl: signedText(
          scenarioRows
            .filter((r) => r.scenario_id === s.scenarioId)
            .reduce((sum, r) => sum + signed(r.cash_delta), 0n) -
            input.manifest.events
              .filter((e) => e.kind === 'opportunity' && e.scenarioId === s.scenarioId)
              .reduce((sum, e) => sum + costFor(e), 0n)
        ),
      }));
      const profits = scenarioOutcomes
        .map((s) => signed(s.cashPnl))
        .sort((a, b) => (a > b ? -1 : a < b ? 1 : 0));
      const positive = profits.filter((p) => p > 0n).reduce((sum, p) => sum + p, 0n);
      const winner = profits[0] !== undefined && profits[0] > 0n ? profits[0] : 0n;
      let equityPeak = units(input.config.initialCash);
      let equityDrawdown = 0n;
      let maxExposure = 0n;
      for (const row of rows) {
        const equity = units(row.outcome.equityEstimateCash);
        if (equity > equityPeak) equityPeak = equity;
        if (equityPeak > 0n && ((equityPeak - equity) * UNIT) / equityPeak > equityDrawdown)
          equityDrawdown = ((equityPeak - equity) * UNIT) / equityPeak;
        const exposure = units(row.outcome.exposureCostCash);
        if (exposure > maxExposure) maxExposure = exposure;
      }
      const fills = await paperRows<{
        fees: { quantity: string }[];
        input_quantity: string;
        output_quantity: string;
        plan: { quote: { input: string; output: string } };
      }>(
        this.db,
        `SELECT f.fees,f.input_quantity,f.output_quantity,p.plan FROM order_fills f JOIN orders o USING(order_id)
          JOIN paper_execution_plans p USING(order_id) WHERE o.portfolio_id=$1`,
        [variant.portfolio_id]
      );
      const delays = evaluated.map((e) => e.delayMs).sort((a, b) => a - b);
      reports.push({
        variant: variant.variant,
        complete,
        dataStatus:
          input.manifest.events.some((e) => e.market === null) ||
          input.manifest.universe.some((s) => !s.exitHorizonComplete) ||
          Number(counts[0]?.residuals ?? 0) > 0 ||
          Number(counts[0]?.pending ?? 0) > 0
            ? 'INSUFFICIENT_DATA'
            : 'COMPLETE_UNDER_DECLARED_ASSUMPTIONS',
        counts: counts[0],
        researchCostCash: amount(cost),
        cashBalance: variant.current_cash,
        netCashAfterResearchCosts: net < 0n ? `-${amount(-net)}` : amount(net),
        cashDrawdown: amount(maxDrawdown),
        equityEstimateDrawdown: amount(equityDrawdown),
        equityValuation:
          'Reserve liquidation estimate at recorded timestamps; unknown/unexecutable residuals valued at zero; not a guaranteed fill.',
        maxExposureCostCash: amount(maxExposure),
        executionFeesCash: amount(
          fills.reduce((sum, f) => sum + f.fees.reduce((n, fee) => n + units(fee.quantity), 0n), 0n)
        ),
        slippageBps: fills.map((f) =>
          String(
            ((units(f.plan.quote.output) * units(f.input_quantity) -
              units(f.output_quantity) * units(f.plan.quote.input)) *
              10000n) /
              (units(f.plan.quote.output) * units(f.input_quantity))
          )
        ),
        latencyMs: {
          median: delays[Math.floor(delays.length / 2)] ?? null,
          p95: delays[Math.floor(delays.length * 0.95)] ?? null,
        },
        scenarioOutcomes,
        worstScenarioCash: profits.length === 0 ? null : signedText(profits.at(-1)!),
        positivePnlShareTopWinner: positive === 0n ? null : amount((winner * UNIT) / positive),
        netCashWithoutTopWinner: signedText(net - winner),
        terminalResidualValue: '0',
        terminalTreatment:
          'Open/unsellable positions valued at zero; outstanding orders are unfilled, never invented exits.',
        pairedOpportunities: [...evaluated, ...missing],
        sampleUnit: 'underlying scenarios, not variant-trade combinations',
        attributionPrecision: null,
        attributionPrecisionReason:
          'Requires independently labeled ground truth; not inferred from source scores.',
      });
    }
    const report = {
      experimentId: id,
      datasetHash: input.config.datasetHash,
      configHash: evidenceHash(input.config),
      codeCommitSha: input.config.codeCommitSha,
      underlyingScenarioCount: input.manifest.universe.length,
      independentSampleCount: null,
      correlationWarning:
        'Variants and repeated-mint episodes share market paths; scenario counts do not establish statistical independence.',
      executionMode: 'historical',
      pairedScenarioDifferences: input.manifest.universe.map((s) => {
        const pnl = (variant: Variant): bigint =>
          signedUnits(
            reports
              .find((v) => v.variant === variant)!
              .scenarioOutcomes.find((o) => o.scenarioId === s.scenarioId)!.cashPnl
          );
        return {
          scenarioId: s.scenarioId,
          narrativeIncrementCash: signedAmount(pnl('B') - pnl('A')),
          walletIncrementCash: signedAmount(pnl('C') - pnl('B')),
        };
      }),
      partition: input.manifest.partition,
      timing: input.config.timing,
      limitations: {
        discovery: input.manifest.discovery,
        measurement: input.manifest.measurement,
        interpretationMode: input.manifest.interpretationMode,
        coverage: input.manifest.coverage,
        missingMarketEventIds: input.manifest.events
          .filter((e) => e.market === null)
          .map((e) => e.id),
        incompleteExitHorizons: input.manifest.universe
          .filter((s) => !s.exitHorizonComplete)
          .map((s) => s.scenarioId),
      },
      variants: reports,
    };
    if (allComplete) {
      await paperRows(
        this.db,
        'INSERT INTO research_reports(experiment_id,report,report_hash) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING',
        [id, JSON.stringify(report), evidenceHash(report)]
      );
    }
    return report;
  }
}
export function newResearchExperimentId(): string {
  return randomUUID();
}
