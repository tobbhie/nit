import { EventEnvelope } from './envelope';

/**
 * Event consumer interface - must tolerate duplicate delivery
 */
export interface EventConsumer<TPayload = unknown> {
  /**
   * Handle an event
   * Must be idempotent - duplicate events may be delivered
   */
  handle(event: EventEnvelope<TPayload>): Promise<void>;

  /**
   * Event types this consumer handles
   */
  handledEventTypes(): string[];
}

/**
 * Event publisher interface
 */
export interface EventPublisher {
  /**
   * Publish an event to all registered consumers
   */
  publish<TPayload>(event: EventEnvelope<TPayload>): Promise<void>;

  /**
   * Register a consumer for specific event types
   */
  subscribe(consumer: EventConsumer): void;
}
