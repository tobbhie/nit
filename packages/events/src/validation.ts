import { ZodSchema } from 'zod';
import { EventEnvelope } from './envelope';
import { ValidationError } from '@nit/domain';

/**
 * Payload schema registry for event validation
 */
export class PayloadSchemaRegistry {
  private schemas = new Map<string, ZodSchema>();

  register(eventType: string, schema: ZodSchema): void {
    this.schemas.set(eventType, schema);
  }

  validate<TPayload>(event: EventEnvelope<TPayload>): void {
    const schema = this.schemas.get(event.eventType);
    if (!schema) {
      throw new ValidationError(`No schema registered for event type: ${event.eventType}`);
    }

    const result = schema.safeParse(event.payload);
    if (!result.success) {
      throw new ValidationError(`Event payload validation failed for ${event.eventType}`, {
        errors: result.error.errors,
      });
    }
  }

  has(eventType: string): boolean {
    return this.schemas.has(eventType);
  }
}

/**
 * Global schema registry instance
 */
export const globalSchemaRegistry = new PayloadSchemaRegistry();
