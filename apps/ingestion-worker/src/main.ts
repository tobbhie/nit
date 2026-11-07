import { bootstrap } from '@nit/composition';
import { QueueName, createVersion } from '@nit/domain';
import { HealthReporter, Logger } from '@nit/observability';
import { JobQueue, Worker, JobHandler, Job } from '@nit/queue';
import { runXCapture } from './x-capture';
import {
  NarrativeScorer,
  NarrativeTokenMatcher,
  WatchlistProvider,
} from '@nit/provider-interfaces';

interface IngestionPayload {
  narrativeId: string;
  text: string;
  observedAt: string;
  sourceProviderId: string;
}

/**
 * Long-running ingestion workload. Coordinates with the control app only
 * through durable state — it never synchronously waits on it.
 */
async function main(): Promise<void> {
  const xPath = process.env['NIT_X_CAPTURE_CONFIG_PATH'];
  if (xPath !== undefined) return runXCapture(xPath);
  const { config, db, registry } = await bootstrap(process.env['NIT_CONFIG_PATH']);

  const logger = new Logger({
    service: 'ingestion-worker',
    level: config.logLevel,
  });

  const queue = new JobQueue(db, {
    leaseTimeoutMs: config.queue.leaseTimeoutMs,
    defaultMaxAttempts: config.queue.maxRetries,
    backoff: {
      baseMs: config.queue.retryBackoffMs,
      maxMs: config.queue.maxBackoffMs,
    },
  });

  const scorer = registry.get<NarrativeScorer>('narrativeScorer');
  const matcher = registry.get<NarrativeTokenMatcher>('narrativeTokenMatcher');
  const watchlist = registry.get<WatchlistProvider>('watchlistProvider');

  const handler: JobHandler<IngestionPayload> = {
    queueName: QueueName.INGESTION,
    async handle(job: Job<IngestionPayload>): Promise<void> {
      const payload = job.payload;
      const input = {
        narrativeId: payload.narrativeId as never,
        text: payload.text,
        observedAt: new Date(payload.observedAt),
        sourceProviderId: payload.sourceProviderId as never,
      };

      // Provider capabilities are invoked only through public interfaces.
      const scored = await scorer.score([input]);
      const universe = await watchlist.getWatchlist();
      const matchResult = await matcher.match(input, universe);

      logger.child({ aggregateIds: [input.narrativeId] }).info('Narrative processed', {
        candidateCount: scored.length,
        matchCount: matchResult.matches.length,
        scorerVersion: scored[0]?.version.schemaVersion,
      });
    },
  };

  const worker = new Worker(queue, handler, {
    workerId: `ingestion-${process.pid}`,
    pollIntervalMs: config.queue.pollIntervalMs,
  });

  const health = new HealthReporter(
    'ingestion-worker',
    createVersion(config.version.schemaVersion, config.version.buildId),
    {
      queueDepth: async (): Promise<number> => {
        const depth = await queue.pendingDepth(QueueName.INGESTION);
        return depth.kind === 'success' ? depth.value : -1;
      },
      lastSuccessfulCycleAt: () => worker.getStats().lastSuccessAt,
      errorCount: () => worker.getStats().errorCount,
    }
  );

  const healthTimer = setInterval(() => {
    void health.report().then((report) => logger.info('health', { ...report }));
  }, 30_000);

  const shutdown = (): void => {
    logger.info('Shutdown requested');
    clearInterval(healthTimer);
    worker.stop();
    void db.close();
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  logger.info('Ingestion worker started', { queue: QueueName.INGESTION });
  await worker.run();
}

main().catch((error: unknown) => {
  process.stderr.write(
    `${JSON.stringify({
      level: 'error',
      service: 'ingestion-worker',
      message: 'Fatal startup error',
      error: error instanceof Error ? error.message : String(error),
    })}\n`
  );
  process.exitCode = 1;
});
