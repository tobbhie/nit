import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { Chain, LaunchSource } from '@nit/domain';
import { PostgresSolanaReconciliationCursorRepository } from './reconciliation';

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
