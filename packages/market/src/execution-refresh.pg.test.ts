import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { generateTokenId, generateTokenMarketId } from '@nit/domain';
import { ExecutionFactsRefresher } from './execution-refresh';
import {
  ExecutionFactsConfig,
  SolanaExecutionFactsSource,
  EXECUTION_FACTS_SOURCE,
} from './execution-facts';
import { encodeBase58 } from './pump-events';

describe.skipIf(!hasTestDatabase())('execution refresh persistence', () => {
  let harness: TestDatabase;
  const address = (n: number): string => encodeBase58(Buffer.alloc(32, n));
  const config: ExecutionFactsConfig = {
    pumpProgram: address(1),
    swapProgram: address(2),
    swapGlobalConfig: address(3),
    intervalMs: 1000,
    maxTargets: 1,
    maxSlotAgeMs: 5000,
    candidateMaxAgeMs: 60000,
  };
  beforeAll(async () => {
    harness = await createTestDatabase('execution_refresh');
  }, 30000);
  afterAll(async () => {
    await harness.teardown();
  });
  it('persists a new unavailable reading instead of retaining a reassuring stale snapshot', async () => {
    const token = generateTokenId();
    const now = new Date(Date.now() - 10000);
    const inserted = await harness.db.query(
      `INSERT INTO tokens
      (token_id,chain,address,ticker,name,launch_time,launch_source,discovered_at,raw_payload)
      VALUES ($1,'solana',$2,'TEST','Test',$3,'pump_fun',$3,'{}')`,
      [token, address(4), now]
    );
    expect(inserted.kind).toBe('success');
    const market = await harness.db.query(
      `INSERT INTO token_markets
      (token_market_id,token_id,venue,market_address,active_from,source_event_kind,source_event_key,provenance)
      VALUES ($1,$2,'pump_fun_bonding_curve',$3,$4,'legacy','execution-test','{}')`,
      [generateTokenMarketId(), token, address(5), now]
    );
    expect(market.kind).toBe('success');
    const source = new SolanaExecutionFactsSource(
      (): Promise<unknown> => Promise.reject(new Error('private provider body')),
      config
    );
    expect(await new ExecutionFactsRefresher(harness.db, source, config).cycle()).toEqual({
      captured: 0,
      unavailable: 1,
    });
    const rows = await harness.db.query<{ raw_payload: Record<string, unknown> }>(
      'SELECT raw_payload FROM market_snapshots WHERE token_id=$1 AND source=$2',
      [token, EXECUTION_FACTS_SOURCE]
    );
    expect(rows.kind).toBe('success');
    if (rows.kind === 'success') {
      expect(rows.value).toHaveLength(1);
      expect(rows.value[0]?.raw_payload).toEqual({
        schemaVersion: 1,
        status: 'unavailable',
        marketAddress: address(5),
      });
    }
  });
});
