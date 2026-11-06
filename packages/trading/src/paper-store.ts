import { DatabasePool, Executor, clientExecutor } from '@nit/database-public';
import {
  PortfolioId,
  Result,
  StrategyRunId,
  ValidationError,
  generatePortfolioId,
  generateStrategyCombinationId,
  generateStrategyPolicyVersionId,
  generateStrategyRunId,
} from '@nit/domain';
import { units } from './fixed';
import { paperPolicy } from './paper-policy';
import { ExecutionMode } from './runtime';
import { PolicyVersionDefinition, StrategySnapshot, createStrategySnapshot } from './strategy';

export async function paperRows<T>(
  db: Executor,
  sql: string,
  values: unknown[] = []
): Promise<T[]> {
  const result = await db.query<T>(sql, values);
  if (result.kind === 'failure') throw result.error;
  return result.value;
}

export interface CreatePaperRun {
  readonly strategy: StrategySnapshot;
  readonly mode: ExecutionMode;
  readonly codeHash: string;
  readonly codeCommitSha: string;
  readonly periodStart: Date;
  readonly periodEnd: Date;
  readonly cashAsset: string;
  readonly initialCash: string;
}

export interface PaperRunIdentity {
  readonly runId: StrategyRunId;
  readonly portfolioId: PortfolioId;
}

/** Registers immutable versions and creates one separately funded portfolio per run. */
export class PostgresPaperRegistry {
  constructor(private readonly db: DatabasePool) {}

  async createRun(input: CreatePaperRun): Promise<Result<PaperRunIdentity, Error>> {
    return this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const snapshot = createStrategySnapshot(input.strategy);
      paperPolicy(input.strategy);
      if (input.mode !== ExecutionMode.HISTORICAL && input.mode !== ExecutionMode.FORWARD_PAPER) {
        throw new ValidationError('Paper registry cannot create live runs');
      }
      if (
        !/^[a-f0-9]{64}$/.test(input.codeHash) ||
        input.codeCommitSha.trim() === '' ||
        input.cashAsset.trim() === '' ||
        units(input.initialCash) === 0n ||
        !Number.isFinite(input.periodStart.getTime()) ||
        !Number.isFinite(input.periodEnd.getTime()) ||
        input.periodEnd < input.periodStart
      )
        throw new ValidationError('Invalid paper run scope');
      const ids: string[] = [];
      for (const policy of [snapshot.entry, snapshot.exit, snapshot.protection, snapshot.risk]) {
        ids.push(await registerPolicy(db, policy, input.codeHash));
      }
      const proposed = generateStrategyCombinationId();
      await paperRows(
        db,
        `INSERT INTO strategy_combinations (
        combination_id, entry_version_id, exit_version_id, protection_version_id,
        risk_version_id, display_id, config_hash, compatibility_status
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,'compatible')
      ON CONFLICT (entry_version_id,exit_version_id,protection_version_id,risk_version_id) DO NOTHING`,
        [proposed, ...ids, snapshot.displayId, snapshot.configHash]
      );
      const combinations = await paperRows<{ combination_id: string; config_hash: string }>(
        db,
        `SELECT combination_id, config_hash FROM strategy_combinations
         WHERE entry_version_id=$1 AND exit_version_id=$2 AND protection_version_id=$3 AND risk_version_id=$4`,
        ids
      );
      const combination = combinations[0];
      if (combination?.config_hash !== snapshot.configHash) {
        throw new ValidationError('Registered combination does not match strategy snapshot');
      }
      const runId = generateStrategyRunId();
      const portfolioId = generatePortfolioId();
      await paperRows(
        db,
        `INSERT INTO strategy_runs (
        run_id,combination_id,mode,config_hash,code_commit_sha,period_start,period_end,status,started_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,'running',$6)`,
        [
          runId,
          combination.combination_id,
          input.mode,
          snapshot.configHash,
          input.codeCommitSha,
          input.periodStart,
          input.periodEnd,
        ]
      );
      await paperRows(
        db,
        `INSERT INTO paper_run_configs (run_id,strategy_snapshot) VALUES ($1,$2)`,
        [runId, JSON.stringify(snapshot)]
      );
      await paperRows(
        db,
        `INSERT INTO paper_portfolios (
        portfolio_id,run_id,combination_id,cash_asset,initial_cash,current_cash,capital_constraints
      ) VALUES ($1,$2,$3,$4,$5,$5,$6)`,
        [
          portfolioId,
          runId,
          combination.combination_id,
          input.cashAsset,
          input.initialCash,
          JSON.stringify(snapshot.risk.config),
        ]
      );
      return { runId, portfolioId };
    });
  }
}

async function registerPolicy(
  db: Executor,
  policy: PolicyVersionDefinition,
  codeHash: string
): Promise<string> {
  const names = {
    entry: ['strategy_entry_versions', 'entry_version_id', 'entry_id'],
    exit: ['strategy_exit_versions', 'exit_version_id', 'exit_id'],
    protection: ['strategy_protection_versions', 'protection_version_id', 'policy_id'],
    risk: ['strategy_risk_versions', 'risk_version_id', 'policy_id'],
  } as const;
  const [table, idColumn, policyColumn] = names[policy.kind];
  const exitColumns = policy.kind === 'exit' ? ',compatible_entry_ids' : '';
  const exitValues = policy.kind === 'exit' ? ',$6' : '';
  const params: unknown[] = [
    generateStrategyPolicyVersionId(),
    policy.policyId,
    policy.version,
    JSON.stringify(policy.config),
    codeHash,
  ];
  if (policy.kind === 'exit') params.push(policy.compatibleEntryPolicyIds ?? null);
  await paperRows(
    db,
    `INSERT INTO ${table} (${idColumn},${policyColumn},version,config,code_hash${exitColumns})
    VALUES ($1,$2,$3,$4,$5${exitValues}) ON CONFLICT (${policyColumn},version) DO NOTHING`,
    params
  );
  const rows = await paperRows<{ id: string }>(
    db,
    `SELECT ${idColumn} AS id FROM ${table}
     WHERE ${policyColumn}=$1 AND version=$2 AND config=$3::jsonb AND code_hash=$4
     ${policy.kind === 'exit' ? 'AND compatible_entry_ids IS NOT DISTINCT FROM $5::text[]' : ''}`,
    [
      policy.policyId,
      policy.version,
      JSON.stringify(policy.config),
      codeHash,
      ...(policy.kind === 'exit' ? [policy.compatibleEntryPolicyIds ?? null] : []),
    ]
  );
  if (rows[0] === undefined)
    throw new ValidationError('Policy version already exists with different content; fork it');
  return rows[0].id;
}
