import { Chain, ValidationError, type EvidenceId, type SignalId } from '@nit/domain';
import { describe, expect, it } from 'vitest';
import { parseSignalEnvelope, signalOriginKey, SignalKind } from '.';

const mint = '11111111111111111111111111111111';

function walletSignal(): Record<string, unknown> {
  return {
    eventId: 'signal-1' as SignalId,
    source: 'wallet_tracker',
    sourceVersion: '1.0.0',
    kind: SignalKind.WALLET_BUY,
    chain: Chain.SOLANA,
    tokenMint: mint,
    occurredAt: new Date('2026-01-01T00:00:00.000Z'),
    observedAt: new Date('2026-01-01T00:00:01.000Z'),
    availableAt: new Date('2026-01-01T00:00:02.000Z'),
    origin: {
      kind: 'chain',
      chain: Chain.SOLANA,
      transactionSignature: 'tx-1',
      instructionIndex: 3,
    },
    evidenceIds: ['evidence-1' as EvidenceId],
    payload: { side: 'buy' },
  };
}

describe('signal contracts', () => {
  it('accepts ordered availability timestamps and exact source identity', () => {
    const signal = parseSignalEnvelope(walletSignal());
    expect(signal.kind).toBe(SignalKind.WALLET_BUY);
    expect(signalOriginKey(signal.origin)).toContain('instruction|3');
  });

  it('rejects evidence that becomes available before it was observed', () => {
    const signal = walletSignal();
    signal['availableAt'] = new Date('2025-12-31T23:59:59.000Z');
    expect(() => parseSignalEnvelope(signal)).toThrow(ValidationError);
  });

  it('requires a mint for wallet and market signals', () => {
    const signal = walletSignal();
    delete signal['tokenMint'];
    expect(() => parseSignalEnvelope(signal)).toThrow('tokenMint');
  });

  it('requires a narrative id for narrative signals', () => {
    const signal = walletSignal();
    signal['kind'] = SignalKind.NARRATIVE;
    expect(() => parseSignalEnvelope(signal)).toThrow('canonicalNarrativeId');
  });

  it('does not deduplicate distinct instructions for the same mint', () => {
    const first = signalOriginKey({
      kind: 'chain',
      chain: Chain.SOLANA,
      transactionSignature: 'tx-1',
      instructionIndex: 1,
    });
    const second = signalOriginKey({
      kind: 'chain',
      chain: Chain.SOLANA,
      transactionSignature: 'tx-1',
      instructionIndex: 2,
    });
    expect(first).not.toBe(second);
  });

  it('rejects ambiguous chain occurrence indexes', () => {
    const signal = walletSignal();
    signal['origin'] = {
      kind: 'chain',
      chain: Chain.SOLANA,
      transactionSignature: 'tx-1',
      instructionIndex: 1,
      logIndex: 2,
    };
    expect(() => parseSignalEnvelope(signal)).toThrow(ValidationError);
  });
});
