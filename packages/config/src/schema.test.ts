import { describe, it, expect } from 'vitest';
import { ConfigSchema } from './schema';

describe('ConfigSchema validation', () => {
  it('accepts a complete valid configuration', () => {
    const config = {
      environment: 'development',
      version: { schemaVersion: '1.0.0', buildId: 'test' },
      database: {
        host: 'localhost',
        port: 5432,
        database: 'nit',
        user: 'nit',
        password: 'pass',
        ssl: false,
        poolMin: 2,
        poolMax: 10,
      },
      queue: {
        leaseTimeoutMs: 30000,
        maxRetries: 3,
        retryBackoffMs: 1000,
        maxBackoffMs: 60000,
        pollIntervalMs: 5000,
      },
      providers: {},
      featureFlags: {
        privateIntelligence: false,
        productionAlerts: false,
        execution: false,
        privateSources: false,
      },
      logLevel: 'info',
    };

    expect(ConfigSchema.safeParse(config).success).toBe(true);
  });

  it('applies safe defaults when optional fields are absent', () => {
    const minimal = {
      environment: 'development',
      version: { schemaVersion: '1.0.0', buildId: 'test' },
      database: {
        host: 'localhost',
        port: 5432,
        database: 'nit',
        user: 'nit',
        password: 'pass',
      },
      queue: {},
      featureFlags: {},
    };

    const result = ConfigSchema.safeParse(minimal);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.featureFlags.privateIntelligence).toBe(false);
      expect(result.data.featureFlags.execution).toBe(false);
      expect(result.data.queue.leaseTimeoutMs).toBe(30000);
      expect(result.data.logLevel).toBe('info');
    }
  });

  it('rejects missing required fields', () => {
    const incomplete = {
      environment: 'development',
      version: { schemaVersion: '1.0.0' },
    };

    expect(ConfigSchema.safeParse(incomplete).success).toBe(false);
  });

  it('rejects invalid enum values', () => {
    const badEnum = {
      environment: 'localhost',
      version: { schemaVersion: '1.0.0', buildId: 'x' },
      database: {
        host: 'localhost',
        port: 5432,
        database: 'nit',
        user: 'nit',
        password: 'x',
      },
      queue: {},
      featureFlags: {},
      logLevel: 'trace',
    };

    expect(ConfigSchema.safeParse(badEnum).success).toBe(false);
  });

  it('enforces numeric constraints', () => {
    const badNumbers = {
      environment: 'development',
      version: { schemaVersion: '1.0.0', buildId: 'x' },
      database: {
        host: 'localhost',
        port: -1,
        database: 'nit',
        user: 'nit',
        password: 'x',
      },
      queue: {},
      featureFlags: {},
    };

    expect(ConfigSchema.safeParse(badNumbers).success).toBe(false);
  });

  it('rejects empty required strings', () => {
    const emptyStrings = {
      environment: 'development',
      version: { schemaVersion: '', buildId: 'x' },
      database: {
        host: 'localhost',
        port: 5432,
        database: 'nit',
        user: 'nit',
        password: 'x',
      },
      queue: {},
      featureFlags: {},
    };

    expect(ConfigSchema.safeParse(emptyStrings).success).toBe(false);
  });
});
