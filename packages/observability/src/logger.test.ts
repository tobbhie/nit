import { describe, it, expect } from 'vitest';
import { CorrelationId } from '@nit/domain';
import { Logger, MemorySink } from './logger';

const FIXED_CLOCK = (): Date => new Date('2020-01-01T00:00:00.000Z');

describe('Logger', () => {
  it('emits a structured record with the required fields', () => {
    const sink = new MemorySink();
    const logger = new Logger({ service: 'test-service', sink, clock: FIXED_CLOCK });

    logger.info('something happened');

    const record = sink.records[0];
    expect(record?.timestamp).toBe('2020-01-01T00:00:00.000Z');
    expect(record?.service).toBe('test-service');
    expect(record?.level).toBe('info');
    expect(record?.message).toBe('something happened');
  });

  it('redacts context centrally', () => {
    const sink = new MemorySink();
    const logger = new Logger({ service: 'svc', sink, clock: FIXED_CLOCK });

    logger.info('connecting', { host: 'db.example', password: 'hunter2' });

    expect(sink.records[0]?.context).toEqual({ host: 'db.example', password: '[REDACTED]' });
  });

  it('filters records below the configured level', () => {
    const sink = new MemorySink();
    const logger = new Logger({ service: 'svc', level: 'warn', sink, clock: FIXED_CLOCK });

    logger.debug('d');
    logger.info('i');
    logger.warn('w');
    logger.error('e');

    expect(sink.records.map((r) => r.level)).toEqual(['warn', 'error']);
  });

  it('carries correlation and aggregate context into child loggers', () => {
    const sink = new MemorySink();
    const correlationId = 'corr-1' as CorrelationId;
    const logger = new Logger({ service: 'svc', sink, clock: FIXED_CLOCK }).child({
      correlationId,
    });

    logger.info('with correlation');

    expect(sink.records[0]?.correlationId).toBe('corr-1');
  });

  it('omits optional fields when they are not provided', () => {
    const sink = new MemorySink();
    new Logger({ service: 'svc', sink, clock: FIXED_CLOCK }).info('bare');

    const record = sink.records[0];
    expect(record).not.toHaveProperty('correlationId');
    expect(record).not.toHaveProperty('context');
  });
});
