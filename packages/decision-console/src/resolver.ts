import { createHash } from 'node:crypto';
import { DatabasePool, PostgresSignalStore, clientExecutor } from '@nit/database-public';
import {
  Chain,
  EvidenceId,
  Result,
  SignalId,
  generateCandidateResolutionId,
  generateSignalId,
  failure,
} from '@nit/domain';
import { CandidateActionState as State, SignalEnvelope, SignalKind } from '@nit/signals';
import { Clock, PaperMarketFeed, paperRows, validatePaperMarket } from '@nit/trading';
import {
  CandidateReport,
  NarrativeCandidates,
  ResolvedCandidate,
  ResolverPolicy,
  integer,
  nonempty,
  object,
} from './types';

const SOURCE_VERSION = 'resolver:v1';
interface NarrativeRow {
  signal_id: SignalId;
  canonical_narrative_id: string;
  payload: unknown;
  available_at: Date;
  observed_at: Date;
  occurred_at: Date;
  evidence_ids: EvidenceId[];
}

export class CandidateResolver {
  readonly policyHash: string;
  constructor(
    private readonly db: DatabasePool,
    private readonly feed: PaperMarketFeed,
    private readonly clock: Clock,
    private readonly policy: ResolverPolicy,
    private readonly cashAsset: string
  ) {
    for (const value of [
      policy.minimumNarrativeStrengthBps,
      policy.minimumLinkConfidenceBps,
      policy.minimumSelectionMarginBps,
    ])
      integer(value, 0, 10000);
    integer(policy.minimumIndependentWallets, 2);
    for (const value of [
      policy.coalescingMs,
      policy.walletWindowMs,
      policy.maxMarketAgeMs,
      policy.watchDurationMs,
      policy.pollIntervalMs,
    ])
      integer(value, 1);
    integer(policy.maxScenariosPerCycle, 1, 32);
    if (policy.watchDurationMs <= policy.coalescingMs)
      throw new Error('Watch duration must exceed correlation window');
    nonempty(cashAsset);
    this.policyHash = hash(
      Object.fromEntries(Object.entries(policy).sort(([a], [b]) => a.localeCompare(b)))
    );
  }

  async resolve(narrativeId: string): Promise<Result<CandidateReport, Error>> {
    try {
      const now = this.clock.now();
      const source = await this.latestSource(narrativeId, now);
      if (source === null) throw new Error('No available narrative candidate evidence');
      if (source.available_at.getTime() + this.policy.watchDurationMs <= now.getTime())
        throw new Error('Narrative watch window expired');
      const input = parseNarrative(source.payload);
      const coalescing = now.getTime() < source.available_at.getTime() + this.policy.coalescingMs;
      const candidates: ResolvedCandidate[] = [];
      for (const link of input.candidates) {
        const token = await paperRows<{ name: string; ticker: string; launch_time: Date }>(
          this.db,
          'SELECT name,ticker,launch_time FROM tokens WHERE chain=$1 AND address=$2 AND discovered_at<=$3',
          [Chain.SOLANA, link.tokenMint, now]
        );
        const buys = await paperRows<{
          wallet_address: string;
          wallet_swap_id: EvidenceId;
          side: string;
        }>(
          this.db,
          `
          SELECT DISTINCT ON (wallet_address) wallet_address,wallet_swap_id,side FROM wallet_swaps
          WHERE token_mint=$1 AND occurred_at >= $2 AND occurred_at <= $3 AND available_at <= $3
          ORDER BY wallet_address,occurred_at DESC,available_at DESC,wallet_swap_id DESC`,
          [link.tokenMint, new Date(now.getTime() - this.policy.walletWindowMs), now]
        );
        const active = buys.filter((row) => row.side === 'buy');
        const addresses = active.map((row) => row.wallet_address);
        const relations = await paperRows<{
          wallet_a: string;
          wallet_b: string;
          wallet_relation_evidence_id: EvidenceId;
        }>(
          this.db,
          `
          SELECT wallet_a,wallet_b,wallet_relation_evidence_id FROM wallet_relation_evidence
          WHERE wallet_a=ANY($1::text[]) AND wallet_b=ANY($1::text[]) AND occurred_at<=$2 AND observed_at<=$2
            AND (relation_kind='shared_funder' OR (occurred_at>=$3 AND details->>'tokenMint'=$4))`,
          [addresses, now, new Date(now.getTime() - this.policy.walletWindowMs), link.tokenMint]
        );
        const count = independentCount(addresses, relations);
        const market = await this.feed.latest(link.tokenMint, this.cashAsset, now);
        let validMarket = false;
        if (market.kind === 'success')
          try {
            validatePaperMarket(market.value, now);
            validMarket =
              market.value.tokenMint === link.tokenMint &&
              market.value.cashAsset === this.cashAsset;
          } catch {
            /* Invalid or unavailable evidence remains unknown. */
          }
        let marketStatus: ResolvedCandidate['marketStatus'] = 'unknown';
        const evidenceIds: EvidenceId[] = [
          ...source.evidence_ids,
          ...active.map((row) => row.wallet_swap_id),
          ...relations.map((row) => row.wallet_relation_evidence_id),
        ];
        if (
          market.kind === 'success' &&
          validMarket &&
          now.getTime() - market.value.occurredAt.getTime() <= this.policy.maxMarketAgeMs
        ) {
          marketStatus =
            market.value.verified &&
            market.value.tradable &&
            market.value.sellable &&
            !market.value.criticalRisk
              ? 'safe'
              : 'blocked';
          evidenceIds.push(market.value.snapshotId as EvidenceId);
        }
        const narrativeConfirmed =
          input.narrativeStrengthBps >= this.policy.minimumNarrativeStrengthBps &&
          link.linkConfidenceBps >= this.policy.minimumLinkConfidenceBps &&
          token.length === 1;
        const walletConfirmed =
          count >= this.policy.minimumIndependentWallets && relations.length === 0;
        const reasons = [...link.reasonCodes];
        if (token.length === 0) reasons.push('TOKEN_NOT_CANONICALLY_DISCOVERED');
        if (!narrativeConfirmed) reasons.push('NARRATIVE_LINK_NOT_QUALIFIED');
        if (!walletConfirmed) reasons.push('WALLET_CONFIRMATION_INSUFFICIENT');
        if (relations.length > 0) reasons.push('RELATED_WALLET_SUSPICION');
        if (marketStatus === 'unknown') reasons.push('MARKET_SAFETY_UNKNOWN');
        if (marketStatus === 'blocked') reasons.push('MARKET_SAFETY_BLOCKED');
        if (coalescing) reasons.push('CORRELATION_WINDOW_OPEN');
        const state =
          marketStatus === 'blocked'
            ? State.BLOCKED
            : relations.length > 0
              ? State.MANUAL_REVIEW
              : State.WATCH;
        candidates.push({
          tokenMint: link.tokenMint,
          ticker: token[0]?.ticker ?? 'unknown',
          name: token[0]?.name ?? 'Unknown token',
          launchTime: token[0]?.launch_time.toISOString() ?? null,
          rank: 0,
          score: String(link.linkConfidenceBps),
          linkConfidenceBps: link.linkConfidenceBps,
          independentWallets: count,
          walletConfirmed,
          narrativeConfirmed,
          marketStatus,
          state,
          reasonCodes: [...new Set(reasons)].sort(),
          evidenceIds: [...new Set(evidenceIds)].sort(),
        });
      }
      candidates.sort(
        (a, b) =>
          b.linkConfidenceBps - a.linkConfidenceBps ||
          b.independentWallets - a.independentWallets ||
          a.tokenMint.localeCompare(b.tokenMint)
      );
      const first = candidates[0];
      const selectionClear =
        first !== undefined &&
        (candidates.length === 1 ||
          (first.linkConfidenceBps - (candidates[1]?.linkConfidenceBps ?? 0) >=
            this.policy.minimumSelectionMarginBps &&
            first.linkConfidenceBps > (candidates[1]?.linkConfidenceBps ?? 0)));
      const ranked = candidates.map((candidate, index): ResolvedCandidate => {
        const reasons = [...candidate.reasonCodes];
        let state = candidate.state;
        if (!selectionClear && candidates.length > 1) {
          reasons.push('COMPETING_MINTS_AMBIGUOUS');
          if (state !== State.BLOCKED) state = State.MANUAL_REVIEW;
        }
        if (
          index === 0 &&
          selectionClear &&
          !coalescing &&
          candidate.narrativeConfirmed &&
          candidate.walletConfirmed &&
          candidate.marketStatus === 'safe'
        )
          state = State.TRADE_ELIGIBLE;
        return { ...candidate, rank: index + 1, state, reasonCodes: reasons };
      });
      const state =
        ranked.length === 0
          ? State.NO_CANDIDATE
          : ranked.some((item) => item.state === State.TRADE_ELIGIBLE)
            ? State.TRADE_ELIGIBLE
            : ranked.every((item) => item.state === State.BLOCKED)
              ? State.BLOCKED
              : ranked.some((item) => item.state === State.MANUAL_REVIEW)
                ? State.MANUAL_REVIEW
                : State.WATCH;
      const materialHash = hash({
        title: input.title,
        narrativeStrengthBps: input.narrativeStrengthBps,
        state,
        candidates: ranked.map(({ evidenceIds, ...candidate }) => {
          void evidenceIds;
          return candidate;
        }),
      });
      const fingerprint = hash({
        source: source.signal_id,
        materialHash,
        policy: this.policyHash,
        evidence: ranked.map((candidate) => candidate.evidenceIds),
      });
      return await this.db.transaction(async (client) => {
        const db = clientExecutor(client);
        await paperRows(
          db,
          `INSERT INTO candidate_scenarios (narrative_id,source_signal_id,next_check_at,expires_at)
          VALUES ($1,$2,$3,$4) ON CONFLICT (narrative_id) DO NOTHING`,
          [
            narrativeId,
            source.signal_id,
            now,
            new Date(source.available_at.getTime() + this.policy.watchDurationMs),
          ]
        );
        const rows = await paperRows<{
          fingerprint: string | null;
          report: CandidateReport | null;
        }>(
          db,
          'SELECT fingerprint,report FROM candidate_scenarios WHERE narrative_id=$1 FOR UPDATE',
          [narrativeId]
        );
        const existing = rows[0];
        if (
          existing?.report !== null &&
          existing?.report !== undefined &&
          new Date(existing.report.availableAt) > now
        )
          throw new Error('Resolver clock cannot regress');
        if (existing?.fingerprint === fingerprint && existing.report !== null) {
          await paperRows(
            db,
            'UPDATE candidate_scenarios SET next_check_at=$2,last_error=NULL WHERE narrative_id=$1',
            [narrativeId, new Date(now.getTime() + this.policy.pollIntervalMs)]
          );
          return existing.report;
        }
        const latest = await paperRows<{ signal_id: string }>(
          db,
          `SELECT signal_id FROM signals WHERE source='nit'
          AND kind='narrative' AND canonical_narrative_id=$1 AND available_at<=$2 AND source_version NOT LIKE 'resolver:%'
          ORDER BY available_at DESC,signal_id DESC LIMIT 1`,
          [narrativeId, now]
        );
        if (latest[0]?.signal_id !== source.signal_id)
          throw new Error('Narrative changed during resolution; retry');
        const resolutionId = generateCandidateResolutionId();
        const version = (existing?.report?.version ?? 0) + 1;
        const published: ResolvedCandidate[] = [];
        for (const candidate of ranked) {
          const candidateSignalId = generateSignalId();
          const narrativeSignalId = generateSignalId();
          const payload = {
            resolutionId,
            version,
            actionState: candidate.state,
            narrativeConfirmed: candidate.narrativeConfirmed,
            walletConfirmed: candidate.walletConfirmed,
            reasonCodes: candidate.reasonCodes,
          };
          for (const [eventId, kind, body] of [
            [candidateSignalId, SignalKind.CANDIDATE_UPDATE, payload],
            [
              narrativeSignalId,
              SignalKind.NARRATIVE,
              {
                ...payload,
                eligible:
                  selectionClear &&
                  candidate.rank === 1 &&
                  !coalescing &&
                  candidate.narrativeConfirmed &&
                  candidate.marketStatus === 'safe' &&
                  candidate.state !== State.MANUAL_REVIEW,
              },
            ],
          ] as const) {
            const envelope: SignalEnvelope = {
              eventId,
              source: 'nit',
              sourceVersion: SOURCE_VERSION,
              kind,
              chain: Chain.SOLANA,
              tokenMint: candidate.tokenMint,
              canonicalNarrativeId: narrativeId,
              occurredAt: source.occurred_at,
              observedAt: source.observed_at,
              availableAt: now,
              origin: {
                kind: 'provider',
                provider: 'nit-resolver',
                referenceId: `${resolutionId}:${candidate.tokenMint}:${kind}`,
              },
              evidenceIds: candidate.evidenceIds,
              payload: body,
            };
            const written = await new PostgresSignalStore(this.db).appendInTransaction(
              db,
              envelope
            );
            if (written.kind === 'failure') throw written.error;
          }
          published.push({ ...candidate, candidateSignalId, narrativeSignalId });
        }
        const report: CandidateReport = {
          resolutionId,
          narrativeId,
          sourceSignalId: source.signal_id,
          version,
          title: input.title,
          narrativeStrengthBps: input.narrativeStrengthBps,
          state,
          reasonCodes: coalescing ? ['CORRELATION_WINDOW_OPEN'] : [],
          candidates: published,
          policyHash: this.policyHash,
          materialHash,
          availableAt: now.toISOString(),
        };
        await paperRows(
          db,
          `INSERT INTO candidate_resolution_events
          (resolution_id,canonical_narrative_id,version,source_signal_id,action_state,candidates,evidence_ids,
           reason_codes,occurred_at,observed_at,available_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
          [
            resolutionId,
            narrativeId,
            version,
            source.signal_id,
            state,
            JSON.stringify(published),
            [...new Set(published.flatMap((c) => c.evidenceIds))],
            report.reasonCodes,
            source.occurred_at,
            source.observed_at,
            now,
          ]
        );
        await paperRows(
          db,
          `INSERT INTO candidate_resolution_reports (resolution_id,report,policy) VALUES ($1,$2,$3)`,
          [resolutionId, JSON.stringify(report), JSON.stringify(this.policy)]
        );
        const next = coalescing
          ? new Date(source.available_at.getTime() + this.policy.coalescingMs)
          : new Date(now.getTime() + this.policy.pollIntervalMs);
        await paperRows(
          db,
          `UPDATE candidate_scenarios SET source_signal_id=$2,fingerprint=$3,report=$4,
          next_check_at=$5,expires_at=$6,last_error=NULL WHERE narrative_id=$1`,
          [
            narrativeId,
            source.signal_id,
            fingerprint,
            JSON.stringify(report),
            next,
            new Date(source.available_at.getTime() + this.policy.watchDurationMs),
          ]
        );
        return report;
      });
    } catch (error) {
      return failure(error instanceof Error ? error : new Error('Candidate resolution failed'));
    }
  }

  async cycle(): Promise<void> {
    const now = this.clock.now();
    const sources = await paperRows<{
      canonical_narrative_id: string;
      signal_id: string;
      available_at: Date;
    }>(
      this.db,
      `WITH latest AS (
      SELECT DISTINCT ON (canonical_narrative_id) canonical_narrative_id,signal_id,available_at FROM signals
      WHERE source='nit' AND kind='narrative' AND source_version NOT LIKE 'resolver:%'
        AND canonical_narrative_id IS NOT NULL AND available_at<=$1 AND available_at >= $2
        AND payload->>'schemaVersion'='1' AND payload ? 'candidates'
      ORDER BY canonical_narrative_id,available_at DESC,signal_id DESC)
      SELECT s.* FROM latest s LEFT JOIN candidate_scenarios c ON c.narrative_id=s.canonical_narrative_id
      WHERE c.narrative_id IS NULL OR c.next_check_at <= $1 OR c.source_signal_id<>s.signal_id
      ORDER BY COALESCE(c.next_check_at,s.available_at),s.canonical_narrative_id LIMIT $3`,
      [now, new Date(now.getTime() - this.policy.watchDurationMs), this.policy.maxScenariosPerCycle]
    );
    for (const source of sources) {
      const outcome = await this.resolve(source.canonical_narrative_id);
      if (outcome.kind === 'failure')
        await paperRows(
          this.db,
          `INSERT INTO candidate_scenarios
        (narrative_id,source_signal_id,next_check_at,expires_at,last_error) VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT(narrative_id) DO UPDATE SET source_signal_id=EXCLUDED.source_signal_id,
          next_check_at=EXCLUDED.next_check_at,last_error=EXCLUDED.last_error`,
          [
            source.canonical_narrative_id,
            source.signal_id,
            new Date(now.getTime() + this.policy.pollIntervalMs),
            new Date(source.available_at.getTime() + this.policy.watchDurationMs),
            'Candidate evidence could not be resolved',
          ]
        );
    }
  }

  private async latestSource(id: string, now: Date): Promise<NarrativeRow | null> {
    const rows = await paperRows<NarrativeRow>(
      this.db,
      `SELECT * FROM signals WHERE source='nit' AND kind='narrative'
      AND canonical_narrative_id=$1 AND available_at<=$2 AND source_version NOT LIKE 'resolver:%'
      ORDER BY available_at DESC,signal_id DESC LIMIT 1`,
      [nonempty(id, 200), now]
    );
    return rows[0] ?? null;
  }
}

function parseNarrative(raw: unknown): NarrativeCandidates {
  const value = object(raw);
  if (value['schemaVersion'] !== 1) throw new Error('Unsupported narrative candidate schema');
  const candidates = value['candidates'];
  if (!Array.isArray(candidates) || candidates.length > 32)
    throw new Error('Expected at most 32 explicit narrative candidates');
  const parsed = candidates.map((raw): NarrativeCandidates['candidates'][number] => {
    const item = object(raw);
    const mint = nonempty(item['tokenMint'], 44);
    if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(mint)) throw new Error('Invalid candidate mint');
    const reasons = item['reasonCodes'];
    if (!Array.isArray(reasons) || reasons.length > 16)
      throw new Error('Candidate linkage requires explicit reasons');
    return {
      tokenMint: mint,
      linkConfidenceBps: integer(item['linkConfidenceBps'], 0, 10000),
      reasonCodes: reasons.map((reason) => nonempty(reason, 100)),
    };
  });
  if (new Set(parsed.map((candidate) => candidate.tokenMint)).size !== parsed.length)
    throw new Error('Duplicate candidate mint');
  return {
    schemaVersion: 1,
    title: nonempty(value['title'], 200),
    narrativeStrengthBps: integer(value['narrativeStrengthBps'], 0, 10000),
    candidates: parsed,
  };
}
function hash(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
function independentCount(
  wallets: readonly string[],
  relations: readonly { wallet_a: string; wallet_b: string }[]
): number {
  const groups = wallets.map((wallet) => new Set([wallet]));
  for (const relation of relations) {
    const a = groups.find((group) => group.has(relation.wallet_a));
    const b = groups.find((group) => group.has(relation.wallet_b));
    if (a !== undefined && b !== undefined && a !== b) {
      for (const wallet of b) a.add(wallet);
      groups.splice(groups.indexOf(b), 1);
    }
  }
  return groups.length;
}
