import { describe, expect, it } from 'vitest';
import { SolanaWalletTransaction } from './contracts';
import { SolanaWalletSwapParser } from './parser';

const wallet = '11111111111111111111111111111111';
const program = '22222222222222222222222222222222';
const mint = '33333333333333333333333333333333';
const extraMint = '44444444444444444444444444444444';

function transaction(
  postTokenAmount: string,
  postLamports: number,
  extraToken = false
): SolanaWalletTransaction {
  const preTokenBalances = [tokenBalance(0, mint, '0')];
  const postTokenBalances = [tokenBalance(0, mint, postTokenAmount)];
  if (extraToken) {
    preTokenBalances.push(tokenBalance(2, extraMint, '0'));
    postTokenBalances.push(tokenBalance(2, extraMint, '5'));
  }
  return {
    signature: 'signature-1',
    slot: 42,
    observedAt: new Date('2026-01-01T00:00:02.000Z'),
    transaction: {
      slot: 42,
      blockTime: 1767225600,
      transaction: {
        signatures: ['signature-1'],
        message: {
          accountKeys: [
            { pubkey: wallet, signer: true, writable: true },
            { pubkey: program, signer: false, writable: false },
            { pubkey: extraMint, signer: false, writable: true },
          ],
          instructions: [{ programId: program, accounts: [0] }],
        },
      },
      meta: {
        err: null,
        fee: 5_000,
        preBalances: [1_000_000_000, 0, 0],
        postBalances: [postLamports, 0, 0],
        preTokenBalances,
        postTokenBalances,
        innerInstructions: [],
      },
    },
  };
}

function tokenBalance(
  accountIndex: number,
  tokenMint: string,
  amount: string
): Record<string, unknown> {
  return {
    accountIndex,
    mint: tokenMint,
    owner: wallet,
    uiTokenAmount: { amount, decimals: 6, uiAmount: null, uiAmountString: amount },
  };
}

describe('SolanaWalletSwapParser', () => {
  const parser = new SolanaWalletSwapParser({
    watchedWallets: [wallet],
    quoteAssets: ['SOL'],
    swapProgramAddresses: [program],
    clock: (): Date => new Date('2026-01-01T00:00:03.000Z'),
  });

  it('classifies an exact signer-owned token/SOL delta as a buy and adjusts the fee', () => {
    const parsed = parser.parse(transaction('1000000', 899_995_000));
    expect(parsed.kind).toBe('success');
    if (parsed.kind === 'failure') throw parsed.error;
    expect(parsed.value.swaps).toHaveLength(1);
    expect(parsed.value.swaps[0]).toMatchObject({
      walletAddress: wallet,
      side: 'buy',
      tokenMint: mint,
      tokenRawAmount: '1000000',
      quoteAsset: 'SOL',
      quoteRawAmount: '100000000',
      quoteDecimals: 9,
      programAddresses: [program],
      occurredAt: new Date('2026-01-01T00:00:00.000Z'),
      observedAt: new Date('2026-01-01T00:00:02.000Z'),
      availableAt: new Date('2026-01-01T00:00:03.000Z'),
    });
  });

  it('classifies the inverse balance deltas as a sell', () => {
    const input = transaction('0', 1_099_995_000);
    const meta = input.transaction['meta'] as Record<string, unknown>;
    meta['preTokenBalances'] = [tokenBalance(0, mint, '1000000')];
    const parsed = parser.parse(input);
    expect(parsed.kind).toBe('success');
    if (parsed.kind === 'failure') throw parsed.error;
    expect(parsed.value.swaps[0]).toMatchObject({
      side: 'sell',
      tokenRawAmount: '1000000',
      quoteRawAmount: '100000000',
    });
  });

  it('fails closed instead of guessing when more than two assets move', () => {
    const parsed = parser.parse(transaction('1000000', 899_995_000, true));
    expect(parsed.kind).toBe('success');
    if (parsed.kind === 'failure') throw parsed.error;
    expect(parsed.value.swaps).toEqual([]);
    expect(parsed.value.rejectedWallets).toEqual([
      { walletAddress: wallet, reason: 'ambiguous_balance_deltas' },
    ]);
  });
});
