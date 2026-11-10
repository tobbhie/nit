import { readFile, stat } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { bootstrap } from '@nit/composition';
import {
  CatalogTextInterpreter,
  GatewayTextInterpreter,
  LiveNarrativeRuntime,
  NarrativeRuntimeSchema,
  MediaProcessor,
  R2MediaArchive,
} from '@nit/narrative-intelligence';

async function main(): Promise<void> {
  const path = process.env['NIT_NARRATIVE_CONFIG_PATH'];
  if (path === undefined || (await stat(path)).size > 262144)
    throw new Error('Configure a bounded private narrative JSON file');
  const input = NarrativeRuntimeSchema.parse(JSON.parse(await readFile(path, 'utf8')) as unknown);
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
    const mediaConfig =
      input.interpreter.kind === 'gateway' ? input.interpreter.rich?.media : undefined;
    const media =
      mediaConfig === undefined
        ? undefined
        : new MediaProcessor(
            db,
            mediaConfig,
            new R2MediaArchive(mediaConfig.archive, {
              accessKeyId: process.env['NIT_R2_ACCESS_KEY_ID'] ?? '',
              secretAccessKey: process.env['NIT_R2_SECRET_ACCESS_KEY'] ?? '',
            })
          );
    const interpreter =
      input.interpreter.kind === 'catalog'
        ? new CatalogTextInterpreter(input.policy)
        : new GatewayTextInterpreter({
            endpoint: input.interpreter.endpoint,
            allowedOrigin: input.interpreter.allowedOrigin,
            modelVersion: input.interpreter.modelVersion,
            maxInputBytes: input.interpreter.maxInputBytes,
            maxOutputTokens: input.interpreter.maxOutputTokens,
            maxChargeMicros: input.interpreter.maxChargeMicros,
            bearer: process.env['NIT_NARRATIVE_GATEWAY_BEARER_TOKEN'] ?? '',
            ...(input.interpreter.rich === undefined
              ? {}
              : {
                  rich: {
                    modelVersion: input.interpreter.rich.modelVersion,
                    dimensions: input.interpreter.rich.dimensions,
                  },
                }),
            ...(media === undefined
              ? {}
              : {
                  mediaProcessor: media,
                  allowExternalMedia: input.interpreter.rich?.allowExternalMedia === true,
                }),
          });
    const runtime = new LiveNarrativeRuntime(
      db,
      interpreter,
      input,
      () => new Date(),
      media === undefined
        ? undefined
        : (signal: AbortSignal): Promise<void> => media.recover(signal)
    );
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
    await media?.drain();
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
