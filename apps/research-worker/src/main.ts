import { readFile, stat } from 'node:fs/promises';
import { extname } from 'node:path';
import { bootstrap } from '@nit/composition';
import { PostgresResearchStore } from '@nit/database-public';
import {
  PostgresResearchReplay,
  ResearchDataset,
  ResearchExperiment,
  paperRows,
} from '@nit/trading';

async function main(): Promise<void> {
  const path = process.env['NIT_RESEARCH_COMMAND_PATH'];
  if (
    path === undefined ||
    extname(path).toLowerCase() !== '.json' ||
    (await stat(path)).size > 1048576
  )
    throw new Error('Configure a bounded private JSON research command');
  const raw = JSON.parse(await readFile(path, 'utf8')) as Record<string, unknown>;
  const { db } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  try {
    const store = new PostgresResearchStore(db);
    const replay = new PostgresResearchReplay(db);
    let output: unknown;
    if (raw['action'] === 'append') {
      const result = await store.append(raw['record']);
      if (result.kind === 'failure') throw result.error;
      output = result.value;
    } else if (raw['action'] === 'publish') {
      const result = await store.publishInterpretation(text(raw['interpretationId']), new Date());
      if (result.kind === 'failure') throw result.error;
      output = { signalId: result.value };
    } else if (raw['action'] === 'freeze')
      output = { datasetHash: await replay.freezeDataset(raw['dataset'] as ResearchDataset) };
    else if (raw['action'] === 'create') {
      await replay.create(raw['experiment'] as ResearchExperiment);
      output = { created: true };
    } else if (raw['action'] === 'advance') {
      const maxSteps = positive(raw['maxSteps']);
      const maxLag = positive(raw['maxProtectionLagMs']);
      output = await replay.advance(text(raw['experimentId']), maxSteps, async () => {
        const stale = await paperRows(
          db,
          `SELECT 1 FROM paper_portfolios p JOIN strategy_runs r USING(run_id)
          LEFT JOIN paper_dispatch_health h USING(portfolio_id) WHERE r.mode='forward_paper'
          AND (EXISTS(SELECT 1 FROM positions x WHERE x.portfolio_id=p.portfolio_id AND x.open_quantity>0)
            OR EXISTS(SELECT 1 FROM orders o WHERE o.portfolio_id=p.portfolio_id AND NOT EXISTS(SELECT 1 FROM paper_execution_results e WHERE e.order_id=o.order_id)))
          AND (h.checked_at IS NULL OR h.checked_at<$1) LIMIT 1`,
          [new Date(Date.now() - maxLag)]
        );
        return stale.length > 0;
      });
    } else if (raw['action'] === 'report') output = await replay.report(text(raw['experimentId']));
    else throw new Error('Unknown research action');
    process.stdout.write(`${JSON.stringify(output)}\n`);
  } finally {
    await db.close();
  }
}
function text(value: unknown): string {
  if (typeof value !== 'string' || !value.trim()) throw new Error('Missing research identity');
  return value;
}
function positive(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0)
    throw new Error('Missing positive research bound');
  return value;
}
main().catch((): void => {
  process.stderr.write(
    'Research command failed; check private input, pinned identities and data coverage.\n'
  );
  process.exitCode = 1;
});
