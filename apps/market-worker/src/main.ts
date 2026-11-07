import { readFile, stat } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { bootstrap } from '@nit/composition';
import {
  ConfigurationError,
  LaunchSource,
  QueueName,
  Result,
  createVersion,
  failure,
  success,
} from '@nit/domain';
import {
  ConfirmedIngestionSummary,
  ExecutionFactsConfigSchema,
  ExecutionFactsRefresher,
  SolanaExecutionFactsSource,
  executionFactsRpc,
  MarketDiscoveryService,
  PostgresSolanaMarketOccurrenceRepository,
  PostgresSolanaReconciliationCursorRepository,
  PostgresUnitOfWork,
  PumpEventDecoder,
  SolanaConfirmedLogSubscriber,
  SolanaConfirmedOccurrenceIngestor,
  SolanaLaunchReconciler,
  SolanaReconciliationHandler,
  SolanaReconciliationScheduler,
  SolanaRpcReconciliationSource,
} from '@nit/market';
import { HealthReporter, Logger } from '@nit/observability';
import { JobQueue, Worker } from '@nit/queue';

interface Ingestor {
  ingest(signature: string, slot: number): Promise<Result<ConfirmedIngestionSummary, Error>>;
}

async function main(): Promise<void> {
  const { config, db } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  if (!config.solanaMarket.enabled) {
    throw new ConfigurationError('Solana market intake is disabled');
  }
  const marketConfig = config.solanaMarket;
  const factsPath = process.env['NIT_EXECUTION_FACTS_CONFIG_PATH'];
  let refresher: ExecutionFactsRefresher | undefined;
  let factsIntervalMs = 1000;
  if (factsPath !== undefined) {
    if ((await stat(factsPath)).size > 65536)
      throw new ConfigurationError('Execution facts config exceeds 64 KiB');
    const factsConfig = ExecutionFactsConfigSchema.parse(
      JSON.parse(await readFile(factsPath, 'utf8')) as unknown
    );
    if (
      factsConfig.pumpProgram !== marketConfig.pumpFunProgramAddress ||
      factsConfig.swapProgram !== marketConfig.pumpSwapProgramAddress
    )
      throw new ConfigurationError('Execution facts programs must match discovery');
    refresher = new ExecutionFactsRefresher(
      db,
      new SolanaExecutionFactsSource(
        executionFactsRpc(
          marketConfig.httpEndpoint,
          marketConfig.requestTimeoutMs,
          marketConfig.minRequestIntervalMs
        ),
        factsConfig
      ),
      factsConfig
    );
    factsIntervalMs = factsConfig.intervalMs;
  }
  const logger = new Logger({ service: 'market-worker', level: config.logLevel });
  const queue = new JobQueue(db, {
    leaseTimeoutMs: config.queue.leaseTimeoutMs,
    defaultMaxAttempts: config.queue.maxRetries,
    backoff: { baseMs: config.queue.retryBackoffMs, maxMs: config.queue.maxBackoffMs },
  });
  const discovery = new MarketDiscoveryService(new PostgresUnitOfWork(db, queue), {
    producer: 'market-worker',
  });
  const occurrences = new PostgresSolanaMarketOccurrenceRepository(db);
  const cursors = new PostgresSolanaReconciliationCursorRepository(db);
  const decoder = new PumpEventDecoder();
  const programBySource = new Map<LaunchSource, string>([
    [LaunchSource.PUMP_FUN, marketConfig.pumpFunProgramAddress],
    [LaunchSource.PUMPSWAP, marketConfig.pumpSwapProgramAddress],
  ]);
  const reconcilers = new Map<LaunchSource, SolanaLaunchReconciler>();
  const ingestorsByProgram = new Map<string, Ingestor[]>();

  for (const [launchSource, programAddress] of programBySource) {
    const source = new SolanaRpcReconciliationSource({
      endpoint: marketConfig.httpEndpoint,
      programAddress,
      launchSource,
      decoder,
      requestTimeoutMs: marketConfig.requestTimeoutMs,
      minRequestIntervalMs: marketConfig.minRequestIntervalMs,
    });
    reconcilers.set(
      launchSource,
      new SolanaLaunchReconciler(source, cursors, occurrences, discovery, {
        overlapSlots: marketConfig.reconciliation.overlapSlots,
        initialLookbackSlots: marketConfig.reconciliation.initialLookbackSlots,
        pageSize: marketConfig.reconciliation.pageSize,
      })
    );
    const ingestors = ingestorsByProgram.get(programAddress) ?? [];
    ingestors.push(new SolanaConfirmedOccurrenceIngestor(source, occurrences, discovery));
    ingestorsByProgram.set(programAddress, ingestors);
  }

  const handler = new SolanaReconciliationHandler(reconcilers, {
    onReconciled: (summary): void => logger.info('Solana reconciliation completed', { ...summary }),
  });
  const worker = new Worker(queue, handler, {
    workerId: `market-reconciliation-${process.pid}`,
    pollIntervalMs: config.queue.pollIntervalMs,
  });
  const scheduler = new SolanaReconciliationScheduler(queue, {
    intervalMs: marketConfig.reconciliation.intervalMs,
    launchSources: [...programBySource.keys()],
    onError: (error): void =>
      logger.error('Failed to schedule reconciliation', { error: error.message }),
  });
  const subscribers = [...ingestorsByProgram.entries()].map(
    ([programAddress, ingestors]) =>
      new SolanaConfirmedLogSubscriber({
        endpoint: marketConfig.websocketEndpoint,
        programAddress,
        ingestor: compositeIngestor(ingestors),
        onIngested: (summary): void =>
          logger.info('Confirmed Solana transaction ingested', { ...summary }),
        onError: (error): void =>
          logger.error('Confirmed Solana ingestion interrupted', {
            error: error.message,
          }),
      })
  );

  const health = new HealthReporter(
    'market-worker',
    createVersion(config.version.schemaVersion, config.version.buildId),
    {
      queueDepth: async (): Promise<number> => {
        const depth = await queue.pendingDepth(QueueName.TOKEN_RECONCILIATION);
        return depth.kind === 'success' ? depth.value : -1;
      },
      lastSuccessfulCycleAt: () => worker.getStats().lastSuccessAt,
      errorCount: () => worker.getStats().errorCount,
    }
  );
  const healthTimer = setInterval(() => {
    void health.report().then((report) => logger.info('health', { ...report }));
  }, 30_000);

  let shuttingDown = false;
  const abort = new AbortController();
  const shutdown = (): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    abort.abort();
    logger.info('Shutdown requested');
    clearInterval(healthTimer);
    scheduler.stop();
    for (const subscriber of subscribers) subscriber.stop();
    worker.stop();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  scheduler.start();
  for (const subscriber of subscribers) subscriber.start();
  logger.info('Market worker started', {
    reconciliationSources: [...programBySource.keys()],
    websocketConnections: subscribers.length,
  });
  const refresh = async (): Promise<void> => {
    if (refresher === undefined) return;
    while (!abort.signal.aborted) {
      try {
        logger.info('Execution facts refresh', { ...(await refresher.cycle()) });
      } catch {
        logger.error('Execution facts cycle failed');
      }
      try {
        await delay(factsIntervalMs, undefined, { signal: abort.signal });
      } catch {
        break;
      }
    }
  };
  const workloads = [worker.run(), refresh()];
  try {
    await Promise.all(workloads);
  } finally {
    shutdown();
    await Promise.allSettled(workloads);
    await db.close();
  }
}

function compositeIngestor(ingestors: readonly Ingestor[]): Ingestor {
  return {
    async ingest(signature, slot): Promise<Result<ConfirmedIngestionSummary, Error>> {
      const combined: ConfirmedIngestionSummary = {
        signature,
        slot,
        fetched: 0,
        discovered: 0,
        graduated: 0,
        duplicates: 0,
      };
      for (const ingestor of ingestors) {
        const result = await ingestor.ingest(signature, slot);
        if (result.kind === 'failure') return failure(result.error);
        combined.fetched += result.value.fetched;
        combined.discovered += result.value.discovered;
        combined.graduated += result.value.graduated;
        combined.duplicates += result.value.duplicates;
      }
      return success(combined);
    },
  };
}

main().catch((error: unknown) => {
  process.stderr.write(
    `${JSON.stringify({
      level: 'error',
      service: 'market-worker',
      message: 'Fatal startup error',
      error: error instanceof Error ? error.message : String(error),
    })}\n`
  );
  process.exitCode = 1;
});
