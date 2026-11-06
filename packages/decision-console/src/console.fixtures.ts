import { randomUUID } from 'node:crypto';
import { success, Result } from '@nit/domain';
import {
  PaperMarketFeed,
  PaperMarketSnapshot,
  createStrategySnapshot,
  StrategySnapshot,
} from '@nit/trading';
import { ResolverPolicy } from './types';
import { TelegramMessageBody, TelegramTransport } from './telegram';

export const TOKEN = '11111111111111111111111111111111';
export const TOKEN_B = '22222222222222222222222222222222';
export const START = new Date('2026-01-01T00:00:00Z');
export const POLICY: ResolverPolicy = {
  minimumNarrativeStrengthBps: 5000,
  minimumLinkConfidenceBps: 5000,
  minimumSelectionMarginBps: 1000,
  minimumIndependentWallets: 2,
  coalescingMs: 1000,
  walletWindowMs: 60_000,
  maxMarketAgeMs: 10_000,
  watchDurationMs: 120_000,
  pollIntervalMs: 1000,
  maxScenariosPerCycle: 8,
};

export function market(now: Date, mint = TOKEN): PaperMarketSnapshot {
  return {
    snapshotId: randomUUID(),
    tokenMint: mint,
    cashAsset: 'SOL',
    venue: 'pump_fun_bonding_curve',
    tokenReserves: '100000',
    cashReserves: '1000',
    realTokenReserves: '80000',
    realCashReserves: '1000',
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
export function strategy(entry = 'E01'): StrategySnapshot {
  return createStrategySnapshot({
    entry: {
      kind: 'entry',
      policyId: entry,
      version: 1,
      config: { orderCash: '10', minimumIndependentWallets: 2 },
    },
    exit: {
      kind: 'exit',
      policyId: 'X01',
      version: 1,
      config: { takeProfitMultiple: '2', trailingBps: 1000, maxHoldingMs: 60_000 },
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
        maxMarketAgeMs: 10_000,
        cooldownMs: 1000,
        simulation: {
          feeBps: 10,
          fixedFeeCash: '0.001',
          latencyMs: 1000,
          quoteTtlMs: 10_000,
          fillBps: 10000,
          failEveryNthOrder: 0,
        },
      },
    },
  });
}
export class FixtureFeed implements PaperMarketFeed {
  value = market(START);
  latest(mint: string): Promise<Result<PaperMarketSnapshot, Error>> {
    return Promise.resolve(success({ ...this.value, tokenMint: mint }));
  }
}
export class FixtureTelegram implements TelegramTransport {
  sent: { body: TelegramMessageBody; messageId: number }[] = [];
  acknowledgements: string[] = [];
  fail: Error | null = null;
  nextUpdates: unknown[] = [];
  offsets: number[] = [];
  send(body: TelegramMessageBody): Promise<number> {
    if (this.fail !== null) return Promise.reject(this.fail);
    const messageId = this.sent.length + 1;
    this.sent.push({ body, messageId });
    return Promise.resolve(messageId);
  }
  acknowledge(_id: string, text: string): Promise<void> {
    this.acknowledgements.push(text);
    return Promise.resolve();
  }
  removeKeyboard(): Promise<void> {
    return Promise.resolve();
  }
  updates(offset: number): Promise<readonly unknown[]> {
    this.offsets.push(offset);
    return Promise.resolve(this.nextUpdates);
  }
}
