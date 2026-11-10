import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import {
  Chain,
  LaunchSource,
  MarketVenue,
  Result,
  TokenId,
  generateMarketSnapshotId,
  generateTokenId,
  generateTokenMarketId,
} from '@nit/domain';
import { PostgresMarketSnapshotRepository } from './enrichment-repository';
import { PostgresTokenMarketRepository } from './lifecycle';

function unwrap<T>(result: Result<T, Error>): T {
  if (result.kind === 'failure') throw result.error;
  return result.value;
}

describe.skipIf(!hasTestDatabase())('token market lifecycle against Postgres', () => {
  let harness: TestDatabase;
  let tokenId: TokenId;
  let markets: PostgresTokenMarketRepository;

  const launchedAt = new Date('2026-08-20T09:00:00.000Z');
  const graduatedAt = new Date('2026-08-20T10:00:00.000Z');

  beforeAll(async () => {
    harness = await createTestDatabase('token_market_lifecycle');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  beforeEach(async () => {
    unwrap(
      await harness.db.query(
        'TRUNCATE nit_token_interpretations, token_artwork, market_snapshots, token_markets, tokens RESTART IDENTITY'
      )
    );
    tokenId = generateTokenId();
    unwrap(
      await harness.db.query(
        `INSERT INTO tokens (
         token_id, chain, address, ticker, name, launch_time, launch_source,
         discovered_at, raw_payload
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          tokenId,
          Chain.SOLANA,
          'Mint111',
          'MINT',
          'Mint Token',
          launchedAt,
          LaunchSource.PUMP_FUN,
          launchedAt,
          JSON.stringify({ fixture: true }),
        ]
      )
    );
    markets = new PostgresTokenMarketRepository(harness.db);
    unwrap(
      await markets.open({
        tokenMarketId: generateTokenMarketId(),
        tokenId,
        venue: MarketVenue.PUMP_FUN_BONDING_CURVE,
        marketAddress: 'Curve111',
        activeFrom: launchedAt,
        origin: {
          kind: 'chain',
          position: { transactionSignature: 'launch-signature', instructionIndex: 3 },
          slot: 350_000_000,
        },
        provenance: { program: 'pump.fun' },
      })
    );
  });

  it('transitions atomically and answers point-in-time venue queries', async () => {
    const outcome = unwrap(
      await markets.transition({
        tokenMarketId: generateTokenMarketId(),
        tokenId,
        venue: MarketVenue.PUMPSWAP,
        marketAddress: 'Pool111',
        activeFrom: graduatedAt,
        origin: {
          kind: 'chain',
          position: { transactionSignature: 'graduation-signature', logIndex: 7 },
          slot: 350_001_000,
        },
        provenance: { program: 'pumpswap' },
      })
    );

    expect(outcome.inserted).toBe(true);
    expect(unwrap(await markets.findAt(tokenId, new Date(graduatedAt.getTime() - 1)))?.venue).toBe(
      MarketVenue.PUMP_FUN_BONDING_CURVE
    );
    expect(unwrap(await markets.findAt(tokenId, graduatedAt))?.venue).toBe(MarketVenue.PUMPSWAP);
    expect(unwrap(await markets.findCurrent(tokenId))?.venue).toBe(MarketVenue.PUMPSWAP);
    expect(unwrap(await markets.list(tokenId))).toHaveLength(2);
  });

  it('deduplicates the exact on-chain occurrence', async () => {
    const input = {
      tokenMarketId: generateTokenMarketId(),
      tokenId,
      venue: MarketVenue.PUMPSWAP,
      marketAddress: 'Pool111',
      activeFrom: graduatedAt,
      origin: {
        kind: 'chain' as const,
        position: { transactionSignature: 'graduation-signature', logIndex: 7 },
        slot: 350_001_000,
      },
      provenance: { program: 'pumpswap' },
    };
    const first = unwrap(await markets.transition(input));
    const repeated = unwrap(
      await markets.transition({ ...input, tokenMarketId: generateTokenMarketId() })
    );

    expect(first.inserted).toBe(true);
    expect(repeated).toEqual({ inserted: false, tokenMarketId: first.tokenMarketId });
    expect(unwrap(await markets.list(tokenId))).toHaveLength(2);
  });

  it('allows closing once but rejects rewriting or deleting history', async () => {
    unwrap(
      await markets.transition({
        tokenMarketId: generateTokenMarketId(),
        tokenId,
        venue: MarketVenue.PUMPSWAP,
        marketAddress: 'Pool111',
        activeFrom: graduatedAt,
        origin: { kind: 'legacy', key: 'graduation-fixture' },
        provenance: {},
      })
    );

    const rewrite = await harness.db.query(
      `UPDATE token_markets SET market_address = 'Rewritten' WHERE token_id = $1`,
      [tokenId]
    );
    const remove = await harness.db.query('DELETE FROM token_markets WHERE token_id = $1', [
      tokenId,
    ]);
    expect(rewrite.kind).toBe('failure');
    expect(remove.kind).toBe('failure');
  });

  it('keeps same-time provider readings distinct across venues', async () => {
    const snapshots = new PostgresMarketSnapshotRepository(harness.db);
    const base = {
      tokenId,
      observedAt: launchedAt,
      source: 'venue-aware-provider',
      liquidityUsd: '1000.5',
      rawPayload: {},
    };
    unwrap(
      await snapshots.insert({
        ...base,
        snapshotId: generateMarketSnapshotId(),
        venue: MarketVenue.PUMP_FUN_BONDING_CURVE,
      })
    );
    unwrap(
      await snapshots.insert({
        ...base,
        snapshotId: generateMarketSnapshotId(),
        venue: MarketVenue.PUMPSWAP,
      })
    );

    expect(unwrap(await snapshots.listForToken(tokenId, 10))).toHaveLength(2);
  });
});
