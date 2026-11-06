import { randomUUID } from 'node:crypto';
import { Chain, generateSignalId } from '@nit/domain';
import { SignalEnvelope, SignalKind } from '@nit/signals';
import { PaperMarketSnapshot } from './paper-market';
import { StrategySnapshot, createStrategySnapshot } from './strategy';

export const PAPER_TOKEN = '11111111111111111111111111111111';
export const PAPER_START = new Date('2026-01-01T00:00:00Z');

export function fixtureStrategy(
  options: {
    version?: number;
    entry?: string;
    exit?: string;
    fillBps?: number;
    failEveryNthOrder?: number;
    maxPositions?: number;
  } = {}
): StrategySnapshot {
  const version = options.version ?? 1;
  return createStrategySnapshot({
    entry: {
      kind: 'entry',
      policyId: options.entry ?? 'E02',
      version,
      config: { orderCash: '10', minimumIndependentWallets: 2 },
    },
    exit: {
      kind: 'exit',
      policyId: options.exit ?? 'X01',
      version,
      config: {
        takeProfitMultiple: options.exit === 'X02' ? '3' : '2',
        trailingBps: 1000,
        maxHoldingMs: 60_000,
      },
    },
    protection: {
      kind: 'protection',
      policyId: 'P01',
      version,
      config: {
        moonbagBps: 2000,
        initialStopLossBps: 3000,
        postProfitStopMultiple: '1.2',
      },
    },
    risk: {
      kind: 'risk',
      policyId: 'R01',
      version,
      config: {
        maxTradeCash: '20',
        maxExposureCash: '50',
        minLiquidityCash: '100',
        maxPositions: options.maxPositions ?? 5,
        maxImpactBps: 1000,
        maxSlippageBps: 100,
        maxMarketAgeMs: 10_000,
        cooldownMs: 1000,
        simulation: {
          feeBps: 10,
          fixedFeeCash: '0.001',
          latencyMs: 1000,
          quoteTtlMs: 10_000,
          fillBps: options.fillBps ?? 10_000,
          failEveryNthOrder: options.failEveryNthOrder ?? 0,
        },
      },
    },
  });
}

export function fixtureMarket(
  now: Date,
  cashReserves = '1000',
  tokenMint = PAPER_TOKEN
): PaperMarketSnapshot {
  return {
    snapshotId: randomUUID(),
    tokenMint,
    cashAsset: 'SOL',
    venue: 'pump_fun_bonding_curve',
    tokenReserves: '100000',
    cashReserves,
    realTokenReserves: '80000',
    realCashReserves: cashReserves,
    tokenDecimals: 6,
    cashDecimals: 9,
    verified: true,
    tradable: true,
    sellable: true,
    criticalRisk: false,
    occurredAt: now,
    observedAt: now,
    availableAt: now,
  };
}

export function fixtureSignal(
  kind = SignalKind.WALLET_CONVERGENCE,
  tokenMint = PAPER_TOKEN
): SignalEnvelope {
  return {
    eventId: generateSignalId(),
    source:
      kind === SignalKind.NARRATIVE || kind === SignalKind.CANDIDATE_UPDATE
        ? 'nit'
        : 'wallet_tracker',
    sourceVersion: 'fixture',
    kind,
    chain: Chain.SOLANA,
    tokenMint,
    ...(kind === SignalKind.NARRATIVE || kind === SignalKind.CANDIDATE_UPDATE
      ? { canonicalNarrativeId: 'fixture-narrative' }
      : {}),
    occurredAt: PAPER_START,
    observedAt: PAPER_START,
    availableAt: PAPER_START,
    origin: { kind: 'provider', provider: 'fixture-paper', referenceId: randomUUID() },
    evidenceIds: [],
    payload: {
      eligible: true,
      side: 'buy',
      independentSourceCount: 2,
      actionState: 'trade_eligible',
      narrativeConfirmed: true,
      walletConfirmed: true,
    },
  };
}
