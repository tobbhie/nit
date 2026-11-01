import { Chain, generateSignalId } from '@nit/domain';
import { SignalEnvelope, SignalKind } from '@nit/signals';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgresSignalStore } from './signal-store';
import { createTestDatabase, hasTestDatabase, TestDatabase } from './testing';

const mint = '11111111111111111111111111111111';

describe.skipIf(!hasTestDatabase())('PostgresSignalStore', () => {
  let harness: TestDatabase;
  let store: PostgresSignalStore;

  beforeAll(async () => {
    harness = await createTestDatabase('signal_store');
    store = new PostgresSignalStore(harness.db);
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  const providerSignal = (
    eventId = generateSignalId(),
    referenceId = 'provider-event-1'
  ): SignalEnvelope<{ readonly confidence: string }> => ({
    eventId,
    source: 'nit',
    sourceVersion: 'normalizer-v1',
    kind: SignalKind.NARRATIVE,
    chain: Chain.SOLANA,
    canonicalNarrativeId: 'narrative-1',
    occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    observedAt: new Date('2026-01-01T00:00:01.000Z'),
    availableAt: new Date('2026-01-01T00:00:02.000Z'),
    origin: { kind: 'provider', provider: 'x', referenceId },
    evidenceIds: [],
    payload: { confidence: '0.75' },
  });

  it('round-trips all three timestamps and publishes one replay event atomically', async () => {
    const signal = providerSignal();
    const written = await store.append(signal);
    expect(written).toEqual({
      kind: 'success',
      value: { signalId: signal.eventId, inserted: true },
    });

    const loaded = await store.getById<{ confidence: string }>(signal.eventId);
    expect(loaded).toEqual({ kind: 'success', value: signal });

    const events = await harness.db.query<{
      aggregate_id: string;
      occurred_at: Date;
      payload: { occurredAt: string; observedAt: string; availableAt: string };
    }>("SELECT aggregate_id, occurred_at, payload FROM domain_events WHERE event_type = 'signal.published'");
    expect(events.kind).toBe('success');
    if (events.kind === 'failure') {
      throw events.error;
    }
    expect(events.value).toHaveLength(1);
    expect(events.value[0]?.aggregate_id).toBe(signal.eventId);
    expect(events.value[0]?.occurred_at).toEqual(signal.availableAt);
    expect(events.value[0]?.payload).toMatchObject({
      occurredAt: signal.occurredAt.toISOString(),
      observedAt: signal.observedAt.toISOString(),
      availableAt: signal.availableAt.toISOString(),
    });
  });

  it('absorbs a redelivery by source identity without appending another event', async () => {
    const duplicate = providerSignal(generateSignalId());
    const written = await store.append(duplicate);
    expect(written).toMatchObject({
      kind: 'success',
      value: { inserted: false },
    });
    if (written.kind === 'failure') {
      throw written.error;
    }
    expect(written.value.signalId).not.toBe(duplicate.eventId);

    const count = await harness.db.query<{ count: string }>(
      "SELECT COUNT(*) AS count FROM domain_events WHERE event_type = 'signal.published'"
    );
    expect(count).toEqual({ kind: 'success', value: [{ count: '1' }] });
  });

  it('keeps distinct on-chain occurrences for the same mint and orders by availability', async () => {
    const chainSignal: SignalEnvelope = {
      eventId: generateSignalId(),
      source: 'wallet_tracker',
      sourceVersion: 'parser-v1',
      kind: SignalKind.WALLET_BUY,
      chain: Chain.SOLANA,
      tokenMint: mint,
      occurredAt: new Date('2026-01-01T00:00:00.500Z'),
      observedAt: new Date('2026-01-01T00:00:02.500Z'),
      availableAt: new Date('2026-01-01T00:00:03.000Z'),
      origin: {
        kind: 'chain',
        chain: Chain.SOLANA,
        transactionSignature: 'transaction-1',
        instructionIndex: 4,
      },
      evidenceIds: [],
      payload: {},
    };
    expect(await store.append(chainSignal)).toMatchObject({
      kind: 'success',
      value: { inserted: true },
    });

    const nextOccurrence: SignalEnvelope = {
      ...chainSignal,
      eventId: generateSignalId(),
      origin: {
        kind: 'chain',
        chain: Chain.SOLANA,
        transactionSignature: 'transaction-1',
        instructionIndex: 5,
      },
      availableAt: new Date('2026-01-01T00:00:04.000Z'),
    };
    expect(await store.append(nextOccurrence)).toMatchObject({
      kind: 'success',
      value: { inserted: true },
    });

    const available = await store.listAvailable(
      new Date('2026-01-01T00:00:00.000Z'),
      new Date('2026-01-01T00:00:05.000Z')
    );
    expect(available.kind).toBe('success');
    if (available.kind === 'failure') {
      throw available.error;
    }
    expect(available.value.map((item) => item.eventId)).toEqual([
      expect.any(String),
      chainSignal.eventId,
      nextOccurrence.eventId,
    ]);
  });

  it('rejects invalid signals before opening a persistence transaction', async () => {
    const invalid = providerSignal(generateSignalId(), 'invalid-time');
    const result = await store.append({
      ...invalid,
      observedAt: new Date('2025-12-31T23:59:59.000Z'),
    });
    expect(result.kind).toBe('failure');

    const stored = await store.getById(invalid.eventId);
    expect(stored).toEqual({ kind: 'success', value: null });
  });
});
