import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { Chain, LaunchSource } from '@nit/domain';
import {
  PostgresSolanaMarketOccurrenceRepository,
  PostgresSolanaReconciliationCursorRepository,
  ReconciledMarketOccurrence,
} from './reconciliation';

describe.skipIf(!hasTestDatabase())('Solana reconciliation cursor against Postgres', () => {
  let harness: TestDatabase;

  beforeAll(async () => {
    harness = await createTestDatabase('solana_reconciliation_cursor');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  it('creates once, advances monotonically, and absorbs a stale writer', async () => {
    const repository = new PostgresSolanaReconciliationCursorRepository(harness.db);
    const created = await repository.advance(Chain.SOLANA, LaunchSource.PUMP_FUN, 1_000);
    const advanced = await repository.advance(Chain.SOLANA, LaunchSource.PUMP_FUN, 1_100);
    const stale = await repository.advance(Chain.SOLANA, LaunchSource.PUMP_FUN, 1_050);

    expect(created.kind).toBe('success');
    expect(advanced.kind).toBe('success');
    expect(stale.kind).toBe('success');
    if (stale.kind === 'success') expect(stale.value.finalizedSlot).toBe(1_100);
  });

  it('rejects direct cursor regression at the database boundary', async () => {
    const repository = new PostgresSolanaReconciliationCursorRepository(harness.db);
    await repository.advance(Chain.SOLANA, LaunchSource.PUMPSWAP, 2_000);
    const regressed = await harness.db.query(
      `UPDATE solana_reconciliation_cursors
          SET finalized_slot = 1999
        WHERE chain = 'solana' AND launch_source = 'pumpswap'`
    );
    expect(regressed.kind).toBe('failure');
  });
});

describe.skipIf(!hasTestDatabase())('Solana market occurrences against Postgres', () => {
  let harness: TestDatabase;

  beforeAll(async () => {
    harness = await createTestDatabase('solana_market_occurrences');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  it('stores exact nested identities, absorbs replay, and rejects identity collisions', async () => {
    const repository = new PostgresSolanaMarketOccurrenceRepository(harness.db);
    const occurrence = launchOccurrence(1, { source: 'rpc-a' });
    const first = await repository.record(Chain.SOLANA, LaunchSource.PUMP_FUN, occurrence);
    const replay = await repository.record(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      ...occurrence,
      observedAt: new Date('2026-11-01T00:05:00.000Z'),
    });
    const sibling = await repository.record(
      Chain.SOLANA,
      LaunchSource.PUMP_FUN,
      launchOccurrence(2, { source: 'rpc-a' })
    );
    const collision = await repository.record(Chain.SOLANA, LaunchSource.PUMP_FUN, {
      ...occurrence,
      launch: { ...occurrence.launch, raw: { source: 'changed' } },
    });

    expect(first).toEqual({ kind: 'success', value: true });
    expect(replay).toEqual({ kind: 'success', value: false });
    expect(sibling).toEqual({ kind: 'success', value: true });
    expect(collision.kind).toBe('failure');
    const rows = await harness.db.query<{ count: string }>(
      'SELECT COUNT(*)::text AS count FROM solana_market_occurrences'
    );
    expect(rows.kind).toBe('success');
    if (rows.kind === 'success') expect(rows.value[0]?.count).toBe('2');
  });
});

function launchOccurrence(
  innerInstructionIndex: number,
  raw: Record<string, unknown>
): ReconciledMarketOccurrence & { kind: 'launch' } {
  return {
    kind: 'launch',
    programAddress: 'ConfiguredPumpProgram111111111111111111111111',
    observedAt: new Date('2026-11-01T00:01:00.000Z'),
    launch: {
      address: `Mint${innerInstructionIndex}`,
      ticker: 'MINT',
      name: 'Mint Token',
      launchTime: new Date('2026-11-01T00:00:00.000Z'),
      raw,
      market: {
        marketAddress: `Curve${innerInstructionIndex}`,
        origin: {
          kind: 'chain',
          slot: 400_000_000,
          position: {
            transactionSignature: 'same-transaction',
            instructionIndex: 3,
            innerInstructionIndex,
          },
        },
      },
    },
  };
}
