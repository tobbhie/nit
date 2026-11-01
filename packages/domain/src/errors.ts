/**
 * Domain error types
 */
export class DomainError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'DomainError';
    Object.setPrototypeOf(this, DomainError.prototype);
  }
}

export class ValidationError extends DomainError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'VALIDATION_ERROR', details);
    this.name = 'ValidationError';
    Object.setPrototypeOf(this, ValidationError.prototype);
  }
}

export class NotFoundError extends DomainError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'NOT_FOUND', details);
    this.name = 'NotFoundError';
    Object.setPrototypeOf(this, NotFoundError.prototype);
  }
}

export class ConflictError extends DomainError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'CONFLICT', details);
    this.name = 'ConflictError';
    Object.setPrototypeOf(this, ConflictError.prototype);
  }
}

export class ConfigurationError extends DomainError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'CONFIGURATION_ERROR', details);
    this.name = 'ConfigurationError';
    Object.setPrototypeOf(this, ConfigurationError.prototype);
  }
}

export class ProviderError extends DomainError {
  constructor(
    message: string,
    public readonly provider: string,
    details?: Record<string, unknown>
  ) {
    super(message, 'PROVIDER_ERROR', { ...details, provider });
    this.name = 'ProviderError';
    Object.setPrototypeOf(this, ProviderError.prototype);
  }
}
