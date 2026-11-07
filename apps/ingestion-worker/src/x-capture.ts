import { readFile, stat } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { bootstrap } from '@nit/composition';
import { ObservationProvider } from '@nit/domain';
import {
  ObservationIngestionService,
  PostgresUnitOfWork,
  XCaptureConfigSchema,
  XObservationSource,
} from '@nit/observation';

/** Capture has no dependency on narrative interpretation, model access or trading. */
export async function runXCapture(path: string): Promise<void> {
  if ((await stat(path)).size > 65536) throw new Error('X config exceeds 64 KiB');
  const options = XCaptureConfigSchema.parse(JSON.parse(await readFile(path, 'utf8')) as unknown);
  if (Date.parse(options.initialSince) >= Date.now())
    throw new Error('X initialSince must be in the past');
  const token = process.env['NIT_X_BEARER_TOKEN'];
  if (token === undefined) throw new Error('NIT_X_BEARER_TOKEN is required');
  const abort = new AbortController();
  const source = new XObservationSource({ ...options, bearerToken: token, signal: abort.signal });
  const { db, logger } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  const ingestion = new ObservationIngestionService(new PostgresUnitOfWork(db), {
    producer: 'x-capture',
  });
  const stop = (): void => abort.abort();
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  try {
    while (!abort.signal.aborted) {
      let ingested = 0;
      let failedAuthors = 0;
      for (const authorId of options.authorIds) {
        if (abort.signal.aborted) break;
        try {
          const cursor = await db.query<{ latest: string | null }>(
            `SELECT MAX(provider_post_id::numeric)::text AS latest
            FROM observations WHERE provider='x' AND author_id=$1 AND provider_post_id ~ '^[0-9]+$'`,
            [authorId]
          );
          if (cursor.kind === 'failure') throw cursor.error;
          const latest = cursor.value[0]?.latest;
          const posts = await source.fetchAuthorPosts({
            authorId,
            limit: options.maxPosts,
            ...(latest === undefined || latest === null
              ? { since: new Date(options.initialSince) }
              : { sinceProviderPostId: latest }),
          });
          if (posts.kind === 'failure') throw posts.error;
          // Oldest first, stopping at the first failed write: a newer persisted ID must
          // never jump the cursor past an earlier post that failed to commit.
          for (const post of posts.value) {
            if (abort.signal.aborted) break;
            const result = await ingestion.ingest(ObservationProvider.X, post);
            if (result.kind === 'failure') throw result.error;
            if (result.value.status === 'ingested') ingested++;
          }
        } catch {
          failedAuthors++;
        }
      }
      logger.info('X capture cycle', {
        ingested,
        failedAuthors,
        configuredAuthors: options.authorIds.length,
      });
      try {
        await delay(options.intervalMs, undefined, { signal: abort.signal });
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
