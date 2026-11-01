import { createServer, IncomingMessage, ServerResponse } from 'http';
import { bootstrap } from '@nit/composition';
import { QueueName, createVersion, generateCorrelationId } from '@nit/domain';
import { HealthReporter, Logger } from '@nit/observability';
import { JobQueue } from '@nit/queue';

/**
 * User-facing control workload. Runs independently of ingestion and shares
 * state only through the durable queue.
 */
async function main(): Promise<void> {
  const { config, db } = await bootstrap(process.env['NIT_CONFIG_PATH']);

  const logger = new Logger({
    service: 'control-api',
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

  let errorCount = 0;
  let lastSuccessAt: Date | null = null;

  const health = new HealthReporter(
    'control-api',
    createVersion(config.version.schemaVersion, config.version.buildId),
    {
      queueDepth: async (): Promise<number> => {
        const depth = await queue.pendingDepth(QueueName.INGESTION);
        return depth.kind === 'success' ? depth.value : -1;
      },
      lastSuccessfulCycleAt: () => lastSuccessAt,
      errorCount: () => errorCount,
    }
  );

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const correlationId = generateCorrelationId();
    const requestLogger = logger.child({ correlationId });

    void (async (): Promise<void> => {
      try {
        if (req.url === '/health' && req.method === 'GET') {
          const report = await health.report();
          lastSuccessAt = new Date();
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify(report));
          return;
        }

        if (req.url === '/ready' && req.method === 'GET') {
          const dbHealth = await db.health();
          const ready = dbHealth.kind === 'success';
          res.writeHead(ready ? 200 : 503, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ ready }));
          return;
        }

        res.writeHead(404, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'not_found' }));
      } catch (error) {
        errorCount += 1;
        requestLogger.error('Request failed', {
          error: error instanceof Error ? error.message : String(error),
        });
        res.writeHead(500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'internal_error' }));
      }
    })();
  });

  const port = Number(process.env['PORT'] ?? 3000);
  server.listen(port, () => {
    logger.info('Control API started', { port });
  });

  const shutdown = (): void => {
    logger.info('Shutdown requested');
    server.close();
    void db.close();
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((error: unknown) => {
  process.stderr.write(
    `${JSON.stringify({
      level: 'error',
      service: 'control-api',
      message: 'Fatal startup error',
      error: error instanceof Error ? error.message : String(error),
    })}\n`
  );
  process.exitCode = 1;
});
