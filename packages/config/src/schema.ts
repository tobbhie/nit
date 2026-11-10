import { z } from 'zod';

/**
 * Configuration schema with validation
 */

export const EnvironmentSchema = z.enum(['development', 'staging', 'production']);

export const DatabaseConfigSchema = z.object({
  host: z.string().min(1),
  port: z.number().int().positive(),
  database: z.string().min(1),
  user: z.string().min(1),
  password: z.string().min(1),
  ssl: z.boolean().default(false),
  poolMin: z.number().int().nonnegative().default(2),
  poolMax: z.number().int().positive().default(10),
});

export const QueueConfigSchema = z.object({
  leaseTimeoutMs: z.number().int().positive().default(30000),
  maxRetries: z.number().int().nonnegative().default(3),
  retryBackoffMs: z.number().int().positive().default(1000),
  maxBackoffMs: z.number().int().positive().default(60000),
  pollIntervalMs: z.number().int().positive().default(5000),
});

export const ProviderConfigSchema = z.object({
  type: z.string().min(1),
  enabled: z.boolean().default(false),
  config: z.record(z.unknown()).optional(),
});

export const FeatureFlagsSchema = z.object({
  privateIntelligence: z.boolean().default(false),
  productionAlerts: z.boolean().default(false),
  execution: z.boolean().default(false),
  privateSources: z.boolean().default(false),
});

const HttpEndpointSchema = z
  .string()
  .url()
  .refine(
    (value) => {
      const parsed = new URL(value);
      return (
        (parsed.protocol === 'http:' || parsed.protocol === 'https:') &&
        parsed.username === '' &&
        parsed.password === ''
      );
    },
    { message: 'must be an HTTP(S) URL without embedded credentials' }
  );

const WebSocketEndpointSchema = z
  .string()
  .url()
  .refine(
    (value) => {
      const parsed = new URL(value);
      return (
        (parsed.protocol === 'ws:' || parsed.protocol === 'wss:') &&
        parsed.username === '' &&
        parsed.password === ''
      );
    },
    { message: 'must be a WS(S) URL without embedded credentials' }
  );

export const SolanaMarketConfigSchema = z
  .discriminatedUnion('enabled', [
    z.object({ enabled: z.literal(false) }),
    z.object({
      enabled: z.literal(true),
      httpEndpoint: HttpEndpointSchema,
      websocketEndpoint: WebSocketEndpointSchema,
      pumpFunProgramAddress: z.string().trim().min(1),
      pumpSwapProgramAddress: z.string().trim().min(1),
      requestTimeoutMs: z.number().int().positive().default(15_000),
      minRequestIntervalMs: z.number().int().nonnegative().default(200),
      reconciliation: z
        .object({
          enabled: z.boolean().default(true),
          intervalMs: z.number().int().positive().default(60_000),
          overlapSlots: z.number().int().nonnegative().default(250),
          initialLookbackSlots: z.number().int().nonnegative().default(2_000),
          pageSize: z.number().int().positive().max(1_000).default(1_000),
        })
        .default({}),
    }),
  ])
  .default({ enabled: false });

const SolanaAddressSchema = z
  .string()
  .trim()
  .regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/);
const WalletQuoteAssetSchema = z.union([z.literal('SOL'), SolanaAddressSchema]);

export const SolanaWalletConfigSchema = z
  .discriminatedUnion('enabled', [
    z.object({ enabled: z.literal(false) }),
    z.object({
      enabled: z.literal(true),
      httpEndpoint: HttpEndpointSchema,
      websocketEndpoint: WebSocketEndpointSchema,
      watchedWallets: z.array(SolanaAddressSchema).min(1),
      quoteAssets: z.array(WalletQuoteAssetSchema).min(1),
      swapProgramAddresses: z.array(SolanaAddressSchema).min(1),
      requestTimeoutMs: z.number().int().positive().default(15_000),
      minRequestIntervalMs: z.number().int().nonnegative().default(200),
      convergence: z.object({
        windowMs: z.number().int().positive(),
        minimumIndependentWallets: z.number().int().min(2),
      }),
      reconciliation: z
        .object({
          intervalMs: z.number().int().positive().default(60_000),
          overlapSlots: z.number().int().nonnegative().default(250),
          initialLookbackSlots: z.number().int().nonnegative().default(2_000),
          pageSize: z.number().int().positive().max(1_000).default(1_000),
        })
        .default({}),
    }),
  ])
  .default({ enabled: false });

export const ConfigSchema = z.object({
  environment: EnvironmentSchema,
  version: z.object({
    schemaVersion: z.string().min(1),
    buildId: z.string().min(1),
    commitHash: z.string().optional(),
  }),
  database: DatabaseConfigSchema,
  queue: QueueConfigSchema,
  providers: z.record(ProviderConfigSchema).default({}),
  solanaMarket: SolanaMarketConfigSchema,
  solanaWallet: SolanaWalletConfigSchema,
  featureFlags: FeatureFlagsSchema,
  logLevel: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
});

export type Config = z.infer<typeof ConfigSchema>;
export type DatabaseConfig = z.infer<typeof DatabaseConfigSchema>;
export type QueueConfig = z.infer<typeof QueueConfigSchema>;
export type ProviderConfig = z.infer<typeof ProviderConfigSchema>;
export type FeatureFlags = z.infer<typeof FeatureFlagsSchema>;
export type SolanaMarketConfig = z.infer<typeof SolanaMarketConfigSchema>;
export type SolanaWalletConfig = z.infer<typeof SolanaWalletConfigSchema>;
