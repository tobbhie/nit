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
  featureFlags: FeatureFlagsSchema,
  logLevel: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
});

export type Config = z.infer<typeof ConfigSchema>;
export type DatabaseConfig = z.infer<typeof DatabaseConfigSchema>;
export type QueueConfig = z.infer<typeof QueueConfigSchema>;
export type ProviderConfig = z.infer<typeof ProviderConfigSchema>;
export type FeatureFlags = z.infer<typeof FeatureFlagsSchema>;
