import { readFile, stat } from 'node:fs/promises';
import { extname } from 'node:path';
import { bootstrap } from '@nit/composition';
import { PortfolioId, SignalId, ValidationError } from '@nit/domain';
import {
  ExecutionMode,
  PaperMarketSnapshot,
  PostgresPaperRegistry,
  PostgresPaperTrader,
  StrategyCombinationDefinition,
  createStrategySnapshot,
} from '@nit/trading';

/** One bounded operator command; a feed can invoke ticks without an AI runtime. */
async function main(): Promise<void> {
  const path = process.env['NIT_PAPER_COMMAND_PATH'];
  if (path === undefined || extname(path).toLowerCase() !== '.json') {
    throw new ValidationError('NIT_PAPER_COMMAND_PATH must identify a JSON command file');
  }
  if ((await stat(path)).size > 1_048_576) throw new ValidationError('Paper command exceeds 1 MiB');
  const input = object(JSON.parse(await readFile(path, 'utf8')) as unknown);
  const { db } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  try {
    const action = text(input['action']);
    if (action === 'create') {
      const strategy = createStrategySnapshot(
        object(input['strategy']) as unknown as StrategyCombinationDefinition
      );
      const result = await new PostgresPaperRegistry(db).createRun({
        strategy,
        mode: text(input['mode']) as ExecutionMode,
        codeHash: text(input['codeHash']),
        codeCommitSha: text(input['codeCommitSha']),
        periodStart: date(input['periodStart']),
        periodEnd: date(input['periodEnd']),
        cashAsset: text(input['cashAsset']),
        initialCash: text(input['initialCash']),
      });
      if (result.kind === 'failure') throw result.error;
      process.stdout.write(`${JSON.stringify(result.value)}\n`);
    } else if (action === 'tick') {
      const marketInput = object(input['market']);
      const market = {
        ...marketInput,
        occurredAt: date(marketInput['occurredAt']),
        observedAt: date(marketInput['observedAt']),
        availableAt: date(marketInput['availableAt']),
      } as unknown as PaperMarketSnapshot;
      const now = date(input['now']);
      const result = await new PostgresPaperTrader(db, { now: (): Date => now }).tick(
        text(input['portfolioId']) as PortfolioId,
        market,
        input['signalId'] === undefined ? null : (text(input['signalId']) as SignalId)
      );
      if (result.kind === 'failure') throw result.error;
      process.stdout.write(`${JSON.stringify(result.value)}\n`);
    } else if (action === 'rebuild') {
      const result = await new PostgresPaperTrader(db, { now: (): Date => new Date() }).rebuild(
        text(input['portfolioId']) as PortfolioId
      );
      if (result.kind === 'failure') throw result.error;
      process.stdout.write(`${JSON.stringify({ rebuilt: true })}\n`);
    } else if (action === 'status') {
      const id = text(input['portfolioId']);
      const result = await db.query(
        `SELECT p.portfolio_id,p.run_id,p.cash_asset,p.initial_cash,p.current_cash,
        r.mode,r.config_hash,r.status FROM paper_portfolios p JOIN strategy_runs r ON r.run_id=p.run_id
        WHERE p.portfolio_id=$1`,
        [id]
      );
      if (result.kind === 'failure') throw result.error;
      process.stdout.write(`${JSON.stringify(result.value)}\n`);
    } else
      throw new ValidationError('Paper command action must be create, tick, status or rebuild');
  } finally {
    await db.close();
  }
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new ValidationError('Expected command object');
  return value as Record<string, unknown>;
}
function text(value: unknown): string {
  if (typeof value !== 'string' || value.trim() === '')
    throw new ValidationError('Missing command string');
  return value;
}
function date(value: unknown): Date {
  const parsed = new Date(text(value));
  if (!Number.isFinite(parsed.getTime())) throw new ValidationError('Invalid command timestamp');
  return parsed;
}

main().catch((error: unknown): void => {
  process.stderr.write(
    `${JSON.stringify({
      service: 'paper-worker',
      error: error instanceof Error ? error.message : 'Paper command failed',
    })}\n`
  );
  process.exitCode = 1;
});
