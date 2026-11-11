import { readFile, stat } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { bootstrap } from '@nit/composition';
import { ConfigurationError, LaunchSource, QueueName, createVersion } from '@nit/domain';
import {
  ExecutionFactsConfigSchema,
  ExecutionFactsRefresher,
  SolanaExecutionFactsSource,
  executionFactsRpc,
  MarketDiscoveryService,
  PostgresSolanaMarketOccurrenceRepository,
  PostgresSolanaReconciliationCursorRepository,
  PostgresUnitOfWork,
  PumpEventDecoder,
  isPumpLifecycleNotification,
  SolanaConfirmedLogSubscriber,
  SolanaConfirmedOccurrenceIngestor,
  SolanaConfirmedProgramIngestor,
  SolanaLaunchReconciler,
  SolanaReconciliationHandler,
  SolanaReconciliationScheduler,
  SolanaRpcReconciliationSource,
  RecoverableConfirmedIngestor,
  MissingTokenCreationRecovery,
  SolanaMarketRecoveryHandler,
  seedStoredMigrationRecovery,
  PostgresCreationHistoryProgress,
} from '@nit/market';
import { HealthReporter, Logger } from '@nit/observability';
import { JobQueue, Worker } from '@nit/queue';

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
  const recoveryQueue = new JobQueue(db, {
    leaseTimeoutMs: Math.max(
      config.queue.leaseTimeoutMs,
      120000,
      46000 + 3 * (marketConfig.requestTimeoutMs + 6 * marketConfig.minRequestIntervalMs)
    ),
    defaultMaxAttempts: 10,
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
    // CompletePumpAmmMigrationEvent is emitted by Pump, not the destination AMM.
    // Keep destination-specific cursors/decoders, sharing the emitting program's socket.
    [LaunchSource.PUMPSWAP, marketConfig.pumpFunProgramAddress],
  ]);
  const reconcilers = new Map<LaunchSource, SolanaLaunchReconciler>();
  const ingestorsByProgram = new Map<
    string,
    { source: SolanaRpcReconciliationSource; ingestor: SolanaConfirmedOccurrenceIngestor }[]
  >();

  for (const [launchSource, programAddress] of programBySource) {
    const source = new SolanaRpcReconciliationSource({
      endpoint: marketConfig.httpEndpoint,
      programAddress,
      launchSource,
      decoder,
      requestTimeoutMs: marketConfig.requestTimeoutMs,
      minRequestIntervalMs: marketConfig.minRequestIntervalMs,
      creationHistoryProgress: new PostgresCreationHistoryProgress(db, programAddress),
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
    ingestors.push({
      source,
      ingestor: new SolanaConfirmedOccurrenceIngestor(source, occurrences, discovery),
    });
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
  let lastLiveSuccessAt: Date | null = null;
  const creationTarget = ingestorsByProgram.get(marketConfig.pumpFunProgramAddress)![0]!;
  const missingCreations = new MissingTokenCreationRecovery(
    db,
    creationTarget.source,
    creationTarget.ingestor
  );
  const recoveryPrograms = new Map(
    [...ingestorsByProgram.entries()].map(([programAddress, targets]) => [
      programAddress,
      new SolanaConfirmedProgramIngestor(targets, (items) => missingCreations.ensure(items)),
    ])
  );
  const recoveryWorker = new Worker(
    recoveryQueue,
    new SolanaMarketRecoveryHandler(recoveryPrograms),
    {
      workerId: `market-recovery-${process.pid}`,
      pollIntervalMs: config.queue.pollIntervalMs,
    }
  );
  const subscribers = [...ingestorsByProgram.entries()].map(
    ([programAddress, ingestors]) =>
      new SolanaConfirmedLogSubscriber({
        endpoint: marketConfig.websocketEndpoint,
        programAddress,
        notificationFilter: (message): boolean =>
          isPumpLifecycleNotification(message, programAddress),
        ingestor: new RecoverableConfirmedIngestor(
          new SolanaConfirmedProgramIngestor(ingestors),
          recoveryQueue,
          programAddress
        ),
        onDeferred: (): void => logger.warn('Confirmed notification queued for durable recovery'),
        onIngested: (summary): void => {
          lastLiveSuccessAt = new Date();
          logger.info('Confirmed Solana transaction ingested', { ...summary });
        },
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
        const depths = await Promise.all([
          queue.pendingDepth(QueueName.TOKEN_RECONCILIATION),
          recoveryQueue.pendingDepth(QueueName.TOKEN_MARKET_RECOVERY),
        ]);
        return depths.every((d) => d.kind === 'success')
          ? depths.reduce((n, d) => n + (d.kind === 'success' ? d.value : 0), 0)
          : -1;
      },
      lastSuccessfulCycleAt: (): Date | null => {
        const dates = [
          lastLiveSuccessAt,
          worker.getStats().lastSuccessAt,
          recoveryWorker.getStats().lastSuccessAt,
        ].filter((date): date is Date => date !== null);
        return dates.length === 0
          ? null
          : new Date(Math.max(...dates.map((date) => date.getTime())));
      },
      errorCount: () =>
        worker.getStats().errorCount +
        recoveryWorker.getStats().errorCount +
        subscribers.reduce((n, s) => n + s.getStats().errors, 0),
    }
  );
  const healthTimer = setInterval(() => {
    void health.report().then((report) =>
      logger.info('health', {
        ...report,
        status: marketConfig.reconciliation.enabled ? report.status : 'degraded',
        reconciliationEnabled: marketConfig.reconciliation.enabled,
        coverage: marketConfig.reconciliation.enabled
          ? 'live_and_repair'
          : 'live_only_unrepaired_gaps',
        subscriptions: subscribers.map((s) => s.getStats()),
        recovery: recoveryWorker.getStats(),
        rpcRetries: [...ingestorsByProgram.values()]
          .flat()
          .reduce((n, t) => n + t.source.getRetryCount(), 0),
      })
    );
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
    recoveryWorker.stop();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  if (marketConfig.reconciliation.enabled) scheduler.start();
  else logger.warn('Historical reconciliation paused; live-only gaps are not repaired');
  for (const subscriber of subscribers) subscriber.start();
  logger.info('Market worker started', {
    reconciliationSources: [...programBySource.keys()],
    websocketConnections: subscribers.length,
    reconciliationEnabled: marketConfig.reconciliation.enabled,
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
  const live = new Promise<void>((resolve) =>
    abort.signal.addEventListener('abort', () => resolve(), { once: true })
  );
  const recoverySeeds = async (): Promise<void> => {
    while (!abort.signal.aborted) {
      try {
        const count = await seedStoredMigrationRecovery(db, recoveryQueue);
        if (count > 0) logger.info('Stored migrations scheduled for recovery', { count });
      } catch {
        logger.error('Stored migration recovery scheduling failed');
      }
      try {
        await delay(30000, undefined, { signal: abort.signal });
      } catch {
        break;
      }
    }
  };
  const workloads = [
    live,
    refresh(),
    recoveryWorker.run(),
    recoverySeeds(),
    ...(marketConfig.reconciliation.enabled ? [worker.run()] : []),
  ];
  try {
    await Promise.all(workloads);
  } finally {
    shutdown();
    await Promise.allSettled(workloads);
    await db.close();
  }
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
