import { readFile, stat } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { bootstrap } from '@nit/composition';
import {
  CatalogTextInterpreter,
  GatewayTextInterpreter,
  LiveNarrativeRuntime,
  NarrativeRuntimeSchema,
} from '@nit/narrative-intelligence';

async function main(): Promise<void> {
  const path = process.env['NIT_NARRATIVE_CONFIG_PATH'];
  if (path === undefined || (await stat(path)).size > 262144)
    throw new Error('Configure a bounded private narrative JSON file');
  const input = NarrativeRuntimeSchema.parse(JSON.parse(await readFile(path, 'utf8')) as unknown);
  const interpreter =
    input.interpreter.kind === 'catalog'
      ? new CatalogTextInterpreter(input.policy)
      : new GatewayTextInterpreter({
          ...input.interpreter,
          bearer: process.env['NIT_NARRATIVE_GATEWAY_BEARER_TOKEN'] ?? '',
        });
  if (input.interpreter.kind === 'gateway' && Date.parse(input.interpreter.expiresAt) <= Date.now())
    throw new Error('Narrative external-context grant expired');
  const { db, config, logger } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  if (config.database.poolMax < 2) {
    await db.close();
    throw new Error('Narrative worker requires at least two database pool slots');
  }
  const abort = new AbortController();
  const stop = (): void => abort.abort();
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  try {
    const runtime = new LiveNarrativeRuntime(db, interpreter, input);
    while (!abort.signal.aborted) {
      try {
        logger.info('Narrative cycle', { ...(await runtime.cycle(abort.signal)) });
      } catch {
        logger.error('Narrative cycle failed; provider and source bodies omitted');
      }
      try {
        await delay(input.intervalMs, undefined, { signal: abort.signal });
      } catch {
        break;
      }
    }
  } finally {
    process.off('SIGTERM', stop);
    process.off('SIGINT', stop);
    await db.close();
  }
}
main().catch(() => {
  process.stderr.write(
    'Narrative worker startup failed; validate private configuration and schema readiness.\n'
  );
  process.exitCode = 1;
});
