import { bootstrap } from '@nit/composition';
import { PostgresSignalStore } from '@nit/database-public';
import { ConfigurationError } from '@nit/domain';
import { Logger } from '@nit/observability';
import {
  PostgresWalletIntelligenceRepository,
  SolanaWalletReconciler,
  SolanaWalletRpc,
  SolanaWalletSubscriber,
  SolanaWalletSwapParser,
  WalletIntelligenceService,
} from '@nit/wallet-intelligence';

async function main(): Promise<void> {
  const { config, db } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  if (!config.solanaWallet.enabled) {
    throw new ConfigurationError('Solana wallet intelligence is disabled');
  }
  const walletConfig = config.solanaWallet;
  const logger = new Logger({ service: 'wallet-worker', level: config.logLevel });
  const repository = new PostgresWalletIntelligenceRepository(db);
  const rpc = new SolanaWalletRpc({
    endpoint: walletConfig.httpEndpoint,
    requestTimeoutMs: walletConfig.requestTimeoutMs,
    minRequestIntervalMs: walletConfig.minRequestIntervalMs,
  });
  const parser = new SolanaWalletSwapParser({
    watchedWallets: walletConfig.watchedWallets,
    quoteAssets: walletConfig.quoteAssets,
    swapProgramAddresses: walletConfig.swapProgramAddresses,
  });
  const service = new WalletIntelligenceService(
    parser,
    repository,
    new PostgresSignalStore(db),
    walletConfig.convergence
  );
  const reconciler = new SolanaWalletReconciler(
    rpc,
    repository,
    service,
    walletConfig.watchedWallets,
    walletConfig.reconciliation
  );
  const subscriber = new SolanaWalletSubscriber({
    endpoint: walletConfig.websocketEndpoint,
    walletAddresses: walletConfig.watchedWallets,
    rpc,
    service,
    onProcessed: (summary): void => logger.info('Wallet transaction processed', { ...summary }),
    onError: (error): void =>
      logger.error('Wallet realtime intake interrupted', { error: error.message }),
  });

  let reconciling = false;
  const reconcile = async (): Promise<void> => {
    if (reconciling) return;
    reconciling = true;
    try {
      const outcome = await reconciler.reconcile();
      if (outcome.kind === 'failure') {
        logger.error('Wallet reconciliation failed', { error: outcome.error.message });
      } else {
        logger.info('Wallet reconciliation completed', { ...outcome.value });
      }
    } finally {
      reconciling = false;
    }
  };

  subscriber.start();
  await reconcile();
  const timer = setInterval(() => void reconcile(), walletConfig.reconciliation.intervalMs);
  logger.info('Wallet worker started', {
    watchedWalletCount: walletConfig.watchedWallets.length,
    websocketConnections: 1,
  });

  let stopped = false;
  const shutdown = (): void => {
    if (stopped) return;
    stopped = true;
    clearInterval(timer);
    subscriber.stop();
    void db.close();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((error: unknown) => {
  process.stderr.write(
    `${JSON.stringify({
      level: 'error',
      service: 'wallet-worker',
      message: 'Fatal startup error',
      error: error instanceof Error ? error.message : String(error),
    })}\n`
  );
  process.exitCode = 1;
});
