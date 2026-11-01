import { Config } from './schema';
import { ConfigurationError } from '@nit/domain';

/**
 * Production safety guards - fail closed with toy/mock providers
 */

const TOY_PROVIDER_TYPES = ['mock', 'fake', 'toy', 'synthetic', 'test'];

export function validateProductionSafety(config: Config): void {
  const errors: string[] = [];

  // Check for toy providers
  for (const [name, providerConfig] of Object.entries(config.providers)) {
    if (TOY_PROVIDER_TYPES.includes(providerConfig.type.toLowerCase())) {
      errors.push(`Provider ${name} uses toy type '${providerConfig.type}' in production`);
    }
  }

  // Check for missing required credentials
  if (!config.database.password) {
    errors.push('Database password is required in production');
  }

  // Check for unsafe defaults
  if (config.database.host === 'localhost') {
    errors.push('Database host cannot be localhost in production');
  }

  if (errors.length > 0) {
    throw new ConfigurationError('Production safety validation failed', {
      errors,
      environment: config.environment,
    });
  }
}

export function validateIncompatibleFlags(config: Config): void {
  // Example: execution requires private intelligence
  if (config.featureFlags.execution && !config.featureFlags.privateIntelligence) {
    throw new ConfigurationError(
      'Execution feature requires private intelligence to be enabled',
      { featureFlags: config.featureFlags }
    );
  }

  // Production alerts require production environment
  if (config.featureFlags.productionAlerts && config.environment !== 'production') {
    throw new ConfigurationError(
      'Production alerts can only be enabled in production environment',
      { environment: config.environment }
    );
  }
}
