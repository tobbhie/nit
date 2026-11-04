import { PostgresSignalStore } from '@nit/database-public';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SolanaWalletTransaction } from './contracts';
import { SolanaWalletSwapParser } from './parser';
import { PostgresWalletIntelligenceRepository } from './repository';
import { WalletIntelligenceService } from './service';

const walletA = '11111111111111111111111111111111';
const walletB = '22222222222222222222222222222222';
const program = '33333333333333333333333333333333';
const mint = '44444444444444444444444444444444';
const secondMint = '55555555555555555555555555555555';
const funder = '66666666666666666666666666666666';

describe.skipIf(!hasTestDatabase())('wallet intelligence against Postgres', () => {
  let harness: TestDatabase;
  let service: WalletIntelligenceService;

  beforeAll(async () => {
    harness = await createTestDatabase('wallet_intelligence');
    const parser = new SolanaWalletSwapParser({
      watchedWallets: [walletA, walletB],
      quoteAssets: ['SOL'],
      swapProgramAddresses: [program],
      clock: (): Date => new Date('2026-01-01T00:00:10.000Z'),
    });
    service = new WalletIntelligenceService(
      parser,
      new PostgresWalletIntelligenceRepository(harness.db),
      new PostgresSignalStore(harness.db),
      { windowMs: 60_000, minimumIndependentWallets: 2 }
    );
  }, 30_000);

  afterAll(async () => harness.teardown());

  it('publishes deduplicated wallet buys and one independently usable convergence signal', async () => {
    expect((await service.process(buyTransaction(walletA, 'sig-a', 100))).kind).toBe('success');
    expect((await service.process(buyTransaction(walletB, 'sig-b', 101))).kind).toBe('success');
    expect((await service.process(buyTransaction(walletB, 'sig-b', 101))).kind).toBe('success');

    const counts = await harness.db.query<{ kind: string; count: string }>(
      `SELECT kind, COUNT(*)::text AS count FROM signals
        WHERE source = 'wallet_tracker' GROUP BY kind ORDER BY kind`
    );
    expect(counts).toEqual({
      kind: 'success',
      value: [
        { kind: 'wallet_buy', count: '2' },
        { kind: 'wallet_convergence', count: '1' },
      ],
    });
    const swaps = await harness.db.query<{ count: string }>(
      'SELECT COUNT(*)::text AS count FROM wallet_swaps'
    );
    expect(swaps).toEqual({ kind: 'success', value: [{ count: '2' }] });
  });

  it('publishes shared-funder risk and does not count linked wallets as independent', async () => {
    expect((await service.process(fundingTransaction(walletA, 'fund-a', 200))).kind).toBe(
      'success'
    );
    expect((await service.process(fundingTransaction(walletB, 'fund-b', 201))).kind).toBe(
      'success'
    );
    expect((await service.process(buyTransaction(walletA, 'sig-c', 202, secondMint))).kind).toBe(
      'success'
    );
    expect((await service.process(buyTransaction(walletB, 'sig-d', 203, secondMint))).kind).toBe(
      'success'
    );

    const risk = await harness.db.query<{ count: string }>(
      "SELECT COUNT(*)::text AS count FROM signals WHERE kind = 'wallet_risk'"
    );
    expect(risk).toEqual({ kind: 'success', value: [{ count: '1' }] });
    const convergence = await harness.db.query<{ count: string }>(
      "SELECT COUNT(*)::text AS count FROM signals WHERE kind = 'wallet_convergence' AND token_mint = $1",
      [secondMint]
    );
    expect(convergence).toEqual({ kind: 'success', value: [{ count: '0' }] });
  });
});

function buyTransaction(
  wallet: string,
  signature: string,
  slot: number,
  tokenMint = mint
): SolanaWalletTransaction {
  return {
    signature,
    slot,
    observedAt: new Date('2026-01-01T00:02:00.000Z'),
    transaction: {
      slot,
      blockTime: 1767225600 + (slot - 100),
      transaction: {
        signatures: [signature],
        message: {
          accountKeys: [
            { pubkey: wallet, signer: true, writable: true },
            { pubkey: program, signer: false, writable: false },
          ],
          instructions: [{ programId: program, accounts: [0] }],
        },
      },
      meta: {
        err: null,
        fee: 5_000,
        preBalances: [1_000_000_000, 0],
        postBalances: [899_995_000, 0],
        preTokenBalances: [tokenBalance(wallet, tokenMint, '0')],
        postTokenBalances: [tokenBalance(wallet, tokenMint, '1000000')],
        innerInstructions: [],
      },
    },
  };
}

function fundingTransaction(
  destination: string,
  signature: string,
  slot: number
): SolanaWalletTransaction {
  return {
    signature,
    slot,
    observedAt: new Date('2026-01-01T00:00:02.000Z'),
    transaction: {
      slot,
      blockTime: 1767225500,
      transaction: {
        signatures: [signature],
        message: {
          accountKeys: [
            { pubkey: funder, signer: true, writable: true },
            { pubkey: destination, signer: false, writable: true },
          ],
          instructions: [
            {
              program: 'system',
              parsed: {
                type: 'transfer',
                info: { source: funder, destination, lamports: 1_000_000 },
              },
            },
          ],
        },
      },
      meta: {
        err: null,
        fee: 5_000,
        preBalances: [2_000_000, 0],
        postBalances: [995_000, 1_000_000],
        preTokenBalances: [],
        postTokenBalances: [],
        innerInstructions: [],
      },
    },
  };
}

function tokenBalance(owner: string, tokenMint: string, amount: string): Record<string, unknown> {
  return {
    accountIndex: 0,
    mint: tokenMint,
    owner,
    uiTokenAmount: { amount, decimals: 6, uiAmount: null, uiAmountString: amount },
  };
}
