import { AggregateId, CorrelationId } from '@nit/domain';
import { redact } from './redaction';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export interface LogRecord {
  timestamp: string;
  level: LogLevel;
  service: string;
  message: string;
  correlationId?: CorrelationId;
  aggregateIds?: readonly AggregateId[];
  context?: Record<string, unknown>;
}

export interface LogSink {
  write(record: LogRecord): void;
}

export class ConsoleSink implements LogSink {
  write(record: LogRecord): void {
    process.stdout.write(`${JSON.stringify(record)}\n`);
  }
}

/** Captures records in memory. Used by tests to assert redaction. */
export class MemorySink implements LogSink {
  readonly records: LogRecord[] = [];

  write(record: LogRecord): void {
    this.records.push(record);
  }

  clear(): void {
    this.records.length = 0;
  }
}

export interface LoggerOptions {
  service: string;
  level?: LogLevel;
  sink?: LogSink;
  correlationId?: CorrelationId;
  aggregateIds?: readonly AggregateId[];
  clock?: () => Date;
}

/**
 * Structured logger. All context is redacted centrally before it reaches a
 * sink, so no call site can leak a credential by accident.
 */
export class Logger {
  private readonly service: string;
  private readonly level: LogLevel;
  private readonly sink: LogSink;
  private readonly correlationId: CorrelationId | undefined;
  private readonly aggregateIds: readonly AggregateId[] | undefined;
  private readonly clock: () => Date;

  constructor(options: LoggerOptions) {
    this.service = options.service;
    this.level = options.level ?? 'info';
    this.sink = options.sink ?? new ConsoleSink();
    this.correlationId = options.correlationId;
    this.aggregateIds = options.aggregateIds;
    this.clock = options.clock ?? ((): Date => new Date());
  }

  /**
   * Derive a logger that carries additional correlation context.
   */
  child(context: {
    correlationId?: CorrelationId;
    aggregateIds?: readonly AggregateId[];
  }): Logger {
    const options: LoggerOptions = {
      service: this.service,
      level: this.level,
      sink: this.sink,
      clock: this.clock,
    };
    const correlationId = context.correlationId ?? this.correlationId;
    if (correlationId !== undefined) {
      options.correlationId = correlationId;
    }
    const aggregateIds = context.aggregateIds ?? this.aggregateIds;
    if (aggregateIds !== undefined) {
      options.aggregateIds = aggregateIds;
    }
    return new Logger(options);
  }

  debug(message: string, context?: Record<string, unknown>): void {
    this.emit('debug', message, context);
  }

  info(message: string, context?: Record<string, unknown>): void {
    this.emit('info', message, context);
  }

  warn(message: string, context?: Record<string, unknown>): void {
    this.emit('warn', message, context);
  }

  error(message: string, context?: Record<string, unknown>): void {
    this.emit('error', message, context);
  }

  private emit(level: LogLevel, message: string, context?: Record<string, unknown>): void {
    if (LEVEL_ORDER[level] < LEVEL_ORDER[this.level]) {
      return;
    }

    const record: LogRecord = {
      timestamp: this.clock().toISOString(),
      level,
      service: this.service,
      message,
    };

    if (this.correlationId !== undefined) {
      record.correlationId = this.correlationId;
    }
    if (this.aggregateIds !== undefined) {
      record.aggregateIds = this.aggregateIds;
    }
    if (context !== undefined) {
      record.context = redact(context) as Record<string, unknown>;
    }

    this.sink.write(record);
  }
}
