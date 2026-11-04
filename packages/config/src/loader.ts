import { readFileSync } from 'fs';
import { parse } from 'yaml';
import { ConfigSchema, Config } from './schema';
import { ConfigurationError } from '@nit/domain';
import { validateProductionSafety } from './guards';

function parseEnvironment(
  value: string | undefined
): 'development' | 'staging' | 'production' | undefined {
  if (value === 'development' || value === 'staging' || value === 'production') {
    return value;
  }
  return undefined;
}

function parseLogLevel(value: string | undefined): 'debug' | 'info' | 'warn' | 'error' | undefined {
  if (value === 'debug' || value === 'info' || value === 'warn' || value === 'error') {
    return value;
  }
  return undefined;
}

/** Parse an integer environment variable, ignoring absent or malformed values. */
function parseIntEnv(value: string | undefined): number | undefined {
  if (value === undefined || value.trim() === '') {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : undefined;
}

function parseBooleanEnv(value: string | undefined): boolean | undefined {
  if (value === 'true') return true;
  if (value === 'false') return false;
  return undefined;
}

function solanaMarketInput(fileConfig: Partial<Config>): unknown {
  const fromFile = fileConfig.solanaMarket;
  const enabled =
    parseBooleanEnv(process.env['NIT_SOLANA_MARKET_ENABLED']) ?? fromFile?.enabled ?? false;
  if (!enabled) return { enabled: false };
  const enabledFile = fromFile?.enabled === true ? fromFile : undefined;
  return {
    enabled: true,
    httpEndpoint: process.env['NIT_SOLANA_RPC_HTTP_ENDPOINT'] ?? enabledFile?.httpEndpoint,
    websocketEndpoint:
      process.env['NIT_SOLANA_RPC_WEBSOCKET_ENDPOINT'] ?? enabledFile?.websocketEndpoint,
    pumpFunProgramAddress:
      process.env['NIT_PUMP_FUN_PROGRAM_ADDRESS'] ?? enabledFile?.pumpFunProgramAddress,
    pumpSwapProgramAddress:
      process.env['NIT_PUMPSWAP_PROGRAM_ADDRESS'] ?? enabledFile?.pumpSwapProgramAddress,
    requestTimeoutMs:
      parseIntEnv(process.env['NIT_SOLANA_REQUEST_TIMEOUT_MS']) ?? enabledFile?.requestTimeoutMs,
    minRequestIntervalMs:
      parseIntEnv(process.env['NIT_SOLANA_MIN_REQUEST_INTERVAL_MS']) ??
      enabledFile?.minRequestIntervalMs,
    reconciliation: {
      ...enabledFile?.reconciliation,
      intervalMs:
        parseIntEnv(process.env['NIT_SOLANA_RECONCILIATION_INTERVAL_MS']) ??
        enabledFile?.reconciliation.intervalMs,
      overlapSlots:
        parseIntEnv(process.env['NIT_SOLANA_RECONCILIATION_OVERLAP_SLOTS']) ??
        enabledFile?.reconciliation.overlapSlots,
      initialLookbackSlots:
        parseIntEnv(process.env['NIT_SOLANA_INITIAL_LOOKBACK_SLOTS']) ??
        enabledFile?.reconciliation.initialLookbackSlots,
      pageSize:
        parseIntEnv(process.env['NIT_SOLANA_RECONCILIATION_PAGE_SIZE']) ??
        enabledFile?.reconciliation.pageSize,
    },
  };
}

/**
 * Load configuration from environment variables and YAML files
 */
export function loadConfig(configPath?: string): Config {
  const env = process.env['NODE_ENV'] || 'development';

  // Load from file if provided
  let fileConfig: Partial<Config> = {};
  if (configPath) {
    try {
      const content = readFileSync(configPath, 'utf-8');
      fileConfig = parse(content) as Partial<Config>;
    } catch (error) {
      throw new ConfigurationError(`Failed to load configuration from ${configPath}`, {
        error: String(error),
      });
    }
  }

  // Override with environment variables. This is pre-validation input, so it
  // is deliberately untyped — ConfigSchema.safeParse below is the type gate.
  const config: Record<string, unknown> = {
    ...fileConfig,
    environment:
      parseEnvironment(process.env['NIT_ENVIRONMENT']) ??
      fileConfig.environment ??
      parseEnvironment(env),
    version: {
      schemaVersion:
        process.env['NIT_SCHEMA_VERSION'] || fileConfig.version?.schemaVersion || '1.0.0',
      buildId: process.env['NIT_BUILD_ID'] || fileConfig.version?.buildId || 'dev',
      commitHash: process.env['GIT_COMMIT'] || fileConfig.version?.commitHash,
    },
    database: {
      host: process.env['DB_HOST'] || fileConfig.database?.host || 'localhost',
      port: parseIntEnv(process.env['DB_PORT']) ?? fileConfig.database?.port ?? 5432,
      database: process.env['DB_NAME'] || fileConfig.database?.database || 'nit_dev',
      user: process.env['DB_USER'] || fileConfig.database?.user || 'postgres',
      password: process.env['DB_PASSWORD'] || fileConfig.database?.password || '',
      ssl: process.env['DB_SSL'] === 'true' || fileConfig.database?.ssl || false,
      poolMin: parseIntEnv(process.env['DB_POOL_MIN']) ?? fileConfig.database?.poolMin ?? 2,
      poolMax: parseIntEnv(process.env['DB_POOL_MAX']) ?? fileConfig.database?.poolMax ?? 10,
    },
    queue: fileConfig.queue,
    providers: fileConfig.providers,
    solanaMarket: solanaMarketInput(fileConfig),
    featureFlags: {
      privateIntelligence:
        process.env['FEATURE_PRIVATE_INTELLIGENCE'] === 'true' ||
        fileConfig.featureFlags?.privateIntelligence ||
        false,
      productionAlerts:
        process.env['FEATURE_PRODUCTION_ALERTS'] === 'true' ||
        fileConfig.featureFlags?.productionAlerts ||
        false,
      execution:
        process.env['FEATURE_EXECUTION'] === 'true' || fileConfig.featureFlags?.execution || false,
      privateSources:
        process.env['FEATURE_PRIVATE_SOURCES'] === 'true' ||
        fileConfig.featureFlags?.privateSources ||
        false,
    },
    logLevel: parseLogLevel(process.env['LOG_LEVEL']) ?? fileConfig.logLevel ?? 'info',
  };

  // Validate against schema
  const result = ConfigSchema.safeParse(config);
  if (!result.success) {
    throw new ConfigurationError('Configuration validation failed', {
      errors: result.error.errors,
    });
  }

  const validatedConfig = result.data;

  // Production safety checks
  if (validatedConfig.environment === 'production') {
    validateProductionSafety(validatedConfig);
  }

  return validatedConfig;
}
