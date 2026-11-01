import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { AggregateType, EventType, ValidationError, type EventId, type AggregateId } from '@nit/domain';
import { PayloadSchemaRegistry } from './validation';
import type { EventEnvelope } from './envelope';

const NarrativeIngestedSchema = z.object({
  narrativeId: z.string(),
  text: z.string(),
});

function envelope<TPayload>(payload: TPayload): EventEnvelope<TPayload> {
  return {
    eventId: '00000000-0000-4000-8000-000000000001' as EventId,
    eventType: EventType.NARRATIVE_INGESTED,
    aggregateType: AggregateType.NARRATIVE,
    aggregateId: '00000000-0000-4000-8000-000000000002' as AggregateId,
    version: '1.0.0',
    occurredAt: new Date('2020-01-01T00:00:00.000Z'),
    producer: 'toy-producer',
    payload,
  };
}

describe('PayloadSchemaRegistry', () => {
  it('reports whether a schema is registered', () => {
    const registry = new PayloadSchemaRegistry();
    expect(registry.has(EventType.NARRATIVE_INGESTED)).toBe(false);

    registry.register(EventType.NARRATIVE_INGESTED, NarrativeIngestedSchema);
    expect(registry.has(EventType.NARRATIVE_INGESTED)).toBe(true);
  });

  it('accepts a payload that satisfies the registered schema', () => {
    const registry = new PayloadSchemaRegistry();
    registry.register(EventType.NARRATIVE_INGESTED, NarrativeIngestedSchema);

    expect(() =>
      registry.validate(envelope({ narrativeId: 'toy-narrative-1', text: 'Fabricated text.' }))
    ).not.toThrow();
  });

  it('rejects a payload that violates the registered schema', () => {
    const registry = new PayloadSchemaRegistry();
    registry.register(EventType.NARRATIVE_INGESTED, NarrativeIngestedSchema);

    expect(() => registry.validate(envelope({ narrativeId: 'toy-narrative-1' }))).toThrow(
      ValidationError
    );
  });

  it('fails closed when no schema is registered for the event type', () => {
    const registry = new PayloadSchemaRegistry();

    expect(() => registry.validate(envelope({ narrativeId: 'toy-narrative-1' }))).toThrow(
      ValidationError
    );
  });

  it('names the offending event type in the unregistered-schema error', () => {
    const registry = new PayloadSchemaRegistry();

    expect(() => registry.validate(envelope({}))).toThrow(EventType.NARRATIVE_INGESTED);
  });

  it('keeps schemas isolated per event type', () => {
    const registry = new PayloadSchemaRegistry();
    registry.register(EventType.NARRATIVE_SCORED, NarrativeIngestedSchema);

    expect(registry.has(EventType.NARRATIVE_SCORED)).toBe(true);
    expect(registry.has(EventType.NARRATIVE_INGESTED)).toBe(false);
  });
});
