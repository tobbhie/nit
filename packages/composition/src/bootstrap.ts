import { Config, loadConfig } from '@nit/config';
import { DatabasePool, Migrator } from '@nit/database-public';
import { Logger } from '@nit/observability';
import { ProviderRegistry, ProviderSlot } from '@nit/provider-interfaces';
import { assertSafeStartup, bindToyProviders } from './guard';

export interface BootstrapResult {
  config: Config;
  db: DatabasePool;
  logger: Logger;
  registry: ProviderRegistry;
}

export interface BootstrapOptions {
  /** Generic apps retain the deterministic development bindings by default. */
  bindDevelopmentToyProviders?: boolean;
  requiredProviderSlots?: readonly ProviderSlot[];
}

/**
 * Generic application bootstrap with lifecycle hooks.
 */
export async function bootstrap(
  configPath?: string,
  options: BootstrapOptions = {}
): Promise<BootstrapResult> {
  const config = loadConfig(configPath);

  const logger = new Logger({
    service: 'bootstrap',
    level: config.logLevel,
  });

  logger.info('Starting bootstrap', {
    environment: config.environment,
    version: config.version,
  });

  const db = new DatabasePool({
    host: config.database.host,
    port: config.database.port,
    database: config.database.database,
    user: config.database.user,
    password: config.database.password,
    ssl: config.database.ssl,
    min: config.database.poolMin,
    max: config.database.poolMax,
  });

  const healthResult = await db.health();
  if (healthResult.kind === 'failure') {
    logger.error('Database health check failed', { error: healthResult.error.message });
    throw healthResult.error;
  }
  logger.info('Database connected');

  const migrator = new Migrator(db);
  const verifyResult = await migrator.verifySchema();
  if (verifyResult.kind === 'failure') {
    logger.error('Schema verification failed', { error: verifyResult.error.message });
    throw verifyResult.error;
  }
  const missing = verifyResult.value;
  logger.info('Checking pending migrations', { missingTables: missing });
  const migrateResult = await migrator.migrate();
  if (migrateResult.kind === 'failure') {
    logger.error('Migration failed', { error: migrateResult.error.message });
    throw migrateResult.error;
  }
  logger.info('Migrations applied', { applied: migrateResult.value });

  const registry = new ProviderRegistry();
  if (options.bindDevelopmentToyProviders !== false) bindToyProviders(registry);

  assertSafeStartup({
    config,
    registry,
    ...(options.requiredProviderSlots === undefined
      ? {}
      : { requiredSlots: options.requiredProviderSlots }),
  });

  logger.info('Bootstrap complete');

  return { config, db, logger, registry };
}
