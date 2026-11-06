import { z } from 'zod';
import { DatabasePool } from '@nit/database-public';
import {
  AgentToolName,
  AgentCapability,
  AgentToolDefinition,
  AgentToolCallRequest,
  validateAgentToolCall,
} from '@nit/agent-contracts';
import { evidenceHash } from '@nit/signals';
import { PaperMarketFeed, paperRows } from '@nit/trading';
import { AgentScope, EvidenceRecord, ReadTool, ReadTools, ToolResult } from './types';

const uuid = z.string().uuid();
const mint = z.string().regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/);
const Arguments = {
  [AgentToolName.NIT_EVENTS_SEARCH]: z
    .object({ recordIds: z.array(uuid).min(1).max(16).optional(), decisionId: uuid.optional() })
    .strict()
    .refine((v) => Number(v.recordIds !== undefined) + Number(v.decisionId !== undefined) === 1),
  [AgentToolName.NIT_CANDIDATES_INSPECT]: z
    .object({ narrativeId: z.string().min(1).max(200) })
    .strict(),
  [AgentToolName.WALLET_ACTIVITY_INSPECT]: z
    .object({ tokenMint: mint, limit: z.number().int().min(1).max(16) })
    .strict(),
  [AgentToolName.MARKET_SNAPSHOT_GET]: z.object({ tokenMint: mint }).strict(),
  [AgentToolName.STRATEGY_CATALOG_READ]: z.object({ portfolioId: uuid }).strict(),
  [AgentToolName.REPLAY_COMPARE]: z
    .object({
      experimentId: uuid,
      scenarioIds: z.array(z.string().min(1).max(200)).max(8).optional(),
    })
    .strict(),
  [AgentToolName.TRADER_POSITION_INSPECT]: z.object({ positionId: uuid }).strict(),
} as const;
const descriptions: Record<ReadTool, unknown> = {
  [AgentToolName.NIT_EVENTS_SEARCH]: {
    oneOf: [{ recordIds: 'UUID[1..16]' }, { decisionId: 'UUID' }],
  },
  [AgentToolName.NIT_CANDIDATES_INSPECT]: { narrativeId: 'authorized narrative identity' },
  [AgentToolName.WALLET_ACTIVITY_INSPECT]: {
    tokenMint: 'authorized Solana mint',
    limit: 'integer 1..16',
  },
  [AgentToolName.MARKET_SNAPSHOT_GET]: { tokenMint: 'authorized Solana mint' },
  [AgentToolName.STRATEGY_CATALOG_READ]: { portfolioId: 'authorized UUID' },
  [AgentToolName.REPLAY_COMPARE]: {
    experimentId: 'authorized UUID',
    scenarioIds: 'optional scenario IDs[0..8]',
  },
  [AgentToolName.TRADER_POSITION_INSPECT]: { positionId: 'UUID within authorized portfolio' },
};
export const OBSERVE_TOOL_CATALOG: readonly { name: ReadTool; version: '1'; arguments: unknown }[] =
  (Object.keys(ReadTools) as ReadTool[]).map(
    (name): { name: ReadTool; version: '1'; arguments: unknown } => ({
      name,
      version: '1' as const,
      arguments: descriptions[name],
    })
  );
export class ReadOnlyAgentTools {
  constructor(
    private readonly db: DatabasePool,
    private readonly feed: PaperMarketFeed,
    private readonly cashAsset: string
  ) {}
  async execute(
    request: AgentToolCallRequest,
    scope: AgentScope,
    capabilities: readonly AgentCapability[],
    maxBytes: number
  ): Promise<ToolResult> {
    if (!Object.prototype.hasOwnProperty.call(ReadTools, request.tool))
      throw new Error('TOOL_NOT_ALLOWED');
    const tool = request.tool as ReadTool;
    const required = ReadTools[tool];
    const definition: AgentToolDefinition = {
      name: tool,
      version: '1',
      requiredCapability: required,
      mutating: false,
    };
    validateAgentToolCall(definition, request);
    if (!capabilities.includes(required)) throw new Error('CAPABILITY_DENIED');
    const checked = Arguments[tool].safeParse(request.arguments);
    if (!checked.success) throw new Error('INVALID_TOOL_ARGUMENTS');
    const from = new Date(scope.from);
    const asOf = new Date(scope.asOf);
    const records: EvidenceRecord[] = [];
    let partial = false;
    const add = (
      id: string,
      type: string,
      source: string,
      availableAt: Date,
      data: unknown,
      sourceHash?: string
    ): void => {
      if (availableAt > asOf || availableAt < from) throw new Error('EVIDENCE_OUTSIDE_TIME_SCOPE');
      const normalized = JSON.parse(JSON.stringify(data)) as unknown;
      records.push({
        evidenceId: id,
        type,
        source,
        availableAt: availableAt.toISOString(),
        hash: sourceHash ?? evidenceHash(normalized),
        viewHash: evidenceHash(normalized),
        data: normalized,
      });
    };
    if (tool === AgentToolName.NIT_EVENTS_SEARCH) {
      const args = Arguments[tool].parse(request.arguments);
      if (args.recordIds !== undefined) {
        if (args.recordIds.some((id) => !scope.recordIds.includes(id)))
          throw new Error('RECORD_SCOPE_DENIED');
        const found = await paperRows<{ record_id: string; available_at: Date; payload: unknown }>(
          this.db,
          'SELECT record_id,available_at,payload FROM research_records WHERE record_id=ANY($1::uuid[]) AND available_at BETWEEN $2 AND $3 ORDER BY available_at,record_id',
          [args.recordIds, from, asOf]
        );
        for (const row of found)
          add(row.record_id, 'research_record', 'nit-provenance', row.available_at, row.payload);
        partial = found.length !== args.recordIds.length;
      } else {
        if (!scope.decisionIds.includes(args.decisionId!)) throw new Error('DECISION_SCOPE_DENIED');
        const found = await paperRows<{
          decision_id: string;
          decided_at: Date;
          recorded_at: Date;
          portfolio_id: string;
          token_mint: string;
          source_signal_id: string;
          payload: unknown;
        }>(
          this.db,
          `
          SELECT d.*,s.token_mint,s.payload FROM strategy_decisions d JOIN signals s ON s.signal_id=d.source_signal_id
          WHERE d.decision_id=$1 AND d.recorded_at BETWEEN $2 AND $3 AND d.decided_at<=$3 AND s.available_at<=$3`,
          [args.decisionId, from, asOf]
        );
        const row = found[0];
        if (row !== undefined) {
          if (
            !scope.portfolioIds.includes(row.portfolio_id) ||
            !scope.tokenMints.includes(row.token_mint)
          )
            throw new Error('DECISION_SCOPE_DENIED');
          const features = await paperRows(
            this.db,
            'SELECT evidence_mask,snapshot,content_hash FROM decision_feature_snapshots WHERE decision_id=$1',
            [row.decision_id]
          );
          add(row.decision_id, 'strategy_decision', 'deterministic-paper-engine', row.recorded_at, {
            decision: row,
            features,
          });
        }
      }
    } else if (tool === AgentToolName.NIT_CANDIDATES_INSPECT) {
      const args = Arguments[tool].parse(request.arguments);
      if (!scope.narrativeIds.includes(args.narrativeId)) throw new Error('NARRATIVE_SCOPE_DENIED');
      const found = await paperRows<{ resolution_id: string; available_at: Date; report: unknown }>(
        this.db,
        `
        SELECT e.resolution_id,e.available_at,r.report FROM candidate_resolution_events e JOIN candidate_resolution_reports r USING(resolution_id)
        WHERE e.canonical_narrative_id=$1 AND e.available_at BETWEEN $2 AND $3 ORDER BY e.available_at DESC,e.version DESC LIMIT 1`,
        [args.narrativeId, from, asOf]
      );
      if (found[0] !== undefined)
        add(
          found[0].resolution_id,
          'candidate_resolution',
          'nit-resolver',
          found[0].available_at,
          found[0].report
        );
    } else if (tool === AgentToolName.WALLET_ACTIVITY_INSPECT) {
      const args = Arguments[tool].parse(request.arguments);
      if (!scope.tokenMints.includes(args.tokenMint)) throw new Error('MINT_SCOPE_DENIED');
      const found = await paperRows<{ wallet_swap_id: string; available_at: Date }>(
        this.db,
        `SELECT wallet_swap_id,wallet_address,side,token_mint,
        token_raw_amount,token_decimals,quote_asset,quote_raw_amount,quote_decimals,occurred_at,observed_at,available_at
        FROM wallet_swaps WHERE token_mint=$1 AND wallet_address=ANY($2::text[]) AND occurred_at BETWEEN $3 AND $4 AND available_at BETWEEN $3 AND $4
        ORDER BY available_at DESC,wallet_swap_id LIMIT $5`,
        [args.tokenMint, scope.walletAddresses, from, asOf, args.limit]
      );
      for (const row of found)
        add(row.wallet_swap_id, 'wallet_swap', 'wallet-tracker', row.available_at, row);
      const relations = await paperRows<{ wallet_relation_evidence_id: string; observed_at: Date }>(
        this.db,
        `SELECT wallet_relation_evidence_id,wallet_a,wallet_b,
        relation_kind,source_reference,occurred_at,observed_at,details->>'tokenMint' AS token_mint FROM wallet_relation_evidence
        WHERE wallet_a=ANY($1::text[]) AND wallet_b=ANY($1::text[]) AND occurred_at BETWEEN $2 AND $3 AND observed_at BETWEEN $2 AND $3
          AND (relation_kind='shared_funder' OR details->>'tokenMint'=$4) ORDER BY observed_at DESC,wallet_relation_evidence_id LIMIT $5`,
        [scope.walletAddresses, from, asOf, args.tokenMint, args.limit]
      );
      for (const row of relations)
        add(
          row.wallet_relation_evidence_id,
          'wallet_relationship',
          'wallet-tracker',
          row.observed_at,
          row
        );
      // Stored buys do not establish complete history or source independence.
      partial = true;
    } else if (tool === AgentToolName.MARKET_SNAPSHOT_GET) {
      const args = Arguments[tool].parse(request.arguments);
      if (!scope.tokenMints.includes(args.tokenMint)) throw new Error('MINT_SCOPE_DENIED');
      const found = await this.feed.latest(args.tokenMint, this.cashAsset, asOf);
      if (found.kind === 'success')
        add(
          found.value.snapshotId,
          'reserve_model_snapshot',
          'approved-market-feed',
          found.value.availableAt,
          found.value
        );
    } else if (tool === AgentToolName.STRATEGY_CATALOG_READ) {
      const args = Arguments[tool].parse(request.arguments);
      if (!scope.portfolioIds.includes(args.portfolioId)) throw new Error('PORTFOLIO_SCOPE_DENIED');
      const found = await paperRows<{
        run_id: string;
        created_at: Date;
        strategy_snapshot: unknown;
      }>(
        this.db,
        `SELECT c.* FROM paper_run_configs c JOIN paper_portfolios p USING(run_id)
        WHERE p.portfolio_id=$1 AND c.created_at BETWEEN $2 AND $3`,
        [args.portfolioId, from, asOf]
      );
      if (found[0] !== undefined)
        add(
          found[0].run_id,
          'strategy_snapshot',
          'paper-registry',
          found[0].created_at,
          found[0].strategy_snapshot
        );
    } else if (tool === AgentToolName.REPLAY_COMPARE) {
      const args = Arguments[tool].parse(request.arguments);
      if (!scope.experimentIds.includes(args.experimentId))
        throw new Error('EXPERIMENT_SCOPE_DENIED');
      const portfolios = await paperRows<{ portfolio_id: string }>(
        this.db,
        'SELECT portfolio_id FROM research_replay_variants WHERE experiment_id=$1',
        [args.experimentId]
      );
      if (
        portfolios.length === 0 ||
        portfolios.some((p) => !scope.portfolioIds.includes(p.portfolio_id))
      )
        throw new Error('EXPERIMENT_PORTFOLIO_SCOPE_DENIED');
      const found = await paperRows<{
        created_at: Date;
        report: Record<string, unknown>;
        report_hash: string;
      }>(
        this.db,
        'SELECT * FROM research_reports WHERE experiment_id=$1 AND created_at BETWEEN $2 AND $3',
        [args.experimentId, from, asOf]
      );
      const row = found[0];
      if (row !== undefined) {
        if (evidenceHash(row.report) !== row.report_hash) throw new Error('REPORT_HASH_MISMATCH');
        const variants = row.report['variants'];
        if (!Array.isArray(variants)) throw new Error('INVALID_FROZEN_REPORT');
        const summary = {
          ...row.report,
          pairedScenarioDifferences: undefined,
          variants: variants.map((raw: unknown) => {
            const v = object(raw);
            return {
              ...v,
              pairedOpportunities: undefined,
              slippageBps: undefined,
              scenarioOutcomes: Array.isArray(v['scenarioOutcomes'])
                ? v['scenarioOutcomes'].filter(
                    (s: unknown) =>
                      args.scenarioIds?.includes(String(object(s)['scenarioId'])) ?? false
                  )
                : [],
            };
          }),
        };
        // Avoid undefined properties in canonical evidence; omitted details are explicitly partial.
        add(
          args.experimentId,
          'frozen_research_report',
          'research-replay',
          row.created_at,
          JSON.parse(JSON.stringify({ sourceReportHash: row.report_hash, summary })) as unknown,
          row.report_hash
        );
        partial = true;
      }
    } else {
      const args = Arguments[AgentToolName.TRADER_POSITION_INSPECT].parse(request.arguments);
      const found = await paperRows<{
        position_event_id: string;
        recorded_at: Date;
        portfolio_id: string;
        token_mint: string;
        detail: unknown;
      }>(
        this.db,
        `
        SELECT e.position_event_id,e.recorded_at,p.portfolio_id,p.token_mint,e.detail FROM position_events e JOIN positions p USING(position_id)
        WHERE e.position_id=$1 AND e.recorded_at BETWEEN $2 AND $3 AND e.occurred_at<=$3
        ORDER BY e.occurred_at DESC,e.position_event_id DESC LIMIT 1`,
        [args.positionId, from, asOf]
      );
      const row = found[0];
      if (row !== undefined) {
        if (
          !scope.portfolioIds.includes(row.portfolio_id) ||
          !scope.tokenMints.includes(row.token_mint)
        )
          throw new Error('POSITION_SCOPE_DENIED');
        add(
          row.position_event_id,
          'position_event',
          'deterministic-paper-engine',
          row.recorded_at,
          row.detail
        );
      }
    }
    const result: ToolResult = {
      records,
      dataQuality: records.length === 0 ? 'insufficient' : partial ? 'partial' : 'complete',
      availableAt: scope.asOf,
    };
    if (Buffer.byteLength(JSON.stringify(result)) > maxBytes)
      throw new Error('TOOL_RESULT_TOO_LARGE');
    return result;
  }
}
function object(raw: unknown): Record<string, unknown> {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw))
    throw new Error('INVALID_RECORD');
  return raw as Record<string, unknown>;
}
