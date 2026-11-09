import { randomUUID } from 'node:crypto';
import { PaperMarketSnapshot, StrategySnapshot, createStrategySnapshot } from '@nit/trading';
import { ResolverPolicy } from '@nit/decision-console';

/** Sanitized integration policies, never production calibration. */
export const POLICY: ResolverPolicy = {
  minimumNarrativeStrengthBps: 5000,
  minimumLinkConfidenceBps: 5000,
  minimumSelectionMarginBps: 1000,
  minimumIndependentWallets: 2,
  coalescingMs: 1000,
  walletWindowMs: 60000,
  maxMarketAgeMs: 10000,
  watchDurationMs: 120000,
  pollIntervalMs: 1000,
  maxScenariosPerCycle: 8,
};
export function fixtureMarket(now: Date, cash = '1000', mint: string): PaperMarketSnapshot {
  return {
    snapshotId: randomUUID(),
    tokenMint: mint,
    cashAsset: 'SOL',
    venue: 'pump_fun_bonding_curve',
    tokenReserves: '100000',
    cashReserves: cash,
    realTokenReserves: '80000',
    realCashReserves: cash,
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
export function fixtureStrategy(options: { entry: string }): StrategySnapshot {
  return createStrategySnapshot({
    entry: {
      kind: 'entry',
      policyId: options.entry,
      version: 1,
      config: { orderCash: '10', minimumIndependentWallets: 2 },
    },
    exit: {
      kind: 'exit',
      policyId: 'X01',
      version: 1,
      config: { takeProfitMultiple: '2', trailingBps: 1000, maxHoldingMs: 60000 },
    },
    protection: {
      kind: 'protection',
      policyId: 'P01',
      version: 1,
      config: { moonbagBps: 2000, initialStopLossBps: 3000, postProfitStopMultiple: '1.2' },
    },
    risk: {
      kind: 'risk',
      policyId: 'R01',
      version: 1,
      config: {
        maxTradeCash: '20',
        maxExposureCash: '50',
        minLiquidityCash: '100',
        maxPositions: 5,
        maxImpactBps: 1000,
        maxSlippageBps: 100,
        maxMarketAgeMs: 10000,
        cooldownMs: 1000,
        simulation: {
          feeBps: 10,
          fixedFeeCash: '0.001',
          latencyMs: 1000,
          quoteTtlMs: 10000,
          fillBps: 10000,
          failEveryNthOrder: 0,
        },
      },
    },
  });
}
