import { ValidationError } from '@nit/domain';
import { StrategySnapshot, JsonValue, createStrategySnapshot } from './strategy';
import { units } from './fixed';

export interface PaperPolicy {
  readonly orderCash: string;
  readonly minimumIndependentWallets: number;
  readonly takeProfitMultiple: string;
  readonly trailingBps: number;
  readonly maxHoldingMs: number;
  readonly moonbagBps: number;
  readonly initialStopLossBps: number;
  readonly postProfitStopMultiple: string;
  readonly maxTradeCash: string;
  readonly maxExposureCash: string;
  readonly minLiquidityCash: string;
  readonly maxPositions: number;
  readonly maxImpactBps: number;
  readonly maxSlippageBps: number;
  readonly maxMarketAgeMs: number;
  readonly cooldownMs: number;
  readonly feeBps: number;
  readonly fixedFeeCash: string;
  readonly latencyMs: number;
  readonly quoteTtlMs: number;
  readonly fillBps: number;
  readonly failEveryNthOrder: number;
}

export function paperPolicy(input: StrategySnapshot): PaperPolicy {
  const snapshot = createStrategySnapshot(input);
  if (snapshot.configHash !== input.configHash) throw new ValidationError('Strategy hash mismatch');
  if (!['E00', 'E01', 'E02', 'E03', 'E04'].includes(snapshot.entry.policyId)) {
    throw new ValidationError('Unsupported paper entry policy');
  }
  if (!['X01', 'X02', 'X03', 'X04', 'X07'].includes(snapshot.exit.policyId)) {
    throw new ValidationError('Unsupported paper exit policy');
  }
  if (snapshot.protection.policyId !== 'P01' || snapshot.risk.policyId !== 'R01') {
    throw new ValidationError('Unsupported paper protection or risk policy');
  }
  const entry = object(snapshot.entry.config);
  const exit = object(snapshot.exit.config);
  const protection = object(snapshot.protection.config);
  const risk = object(snapshot.risk.config);
  const simulation = object(risk['simulation']);
  const result: PaperPolicy = {
    orderCash: money(entry['orderCash'], true),
    minimumIndependentWallets: integer(entry['minimumIndependentWallets'], 2),
    takeProfitMultiple: money(exit['takeProfitMultiple'], true),
    trailingBps: integer(exit['trailingBps'], 1, 9_999),
    maxHoldingMs: integer(exit['maxHoldingMs'], 1),
    moonbagBps: integer(protection['moonbagBps'], 1, 9_999),
    initialStopLossBps: integer(protection['initialStopLossBps'], 1, 9_999),
    postProfitStopMultiple: money(protection['postProfitStopMultiple'], true),
    maxTradeCash: money(risk['maxTradeCash'], true),
    maxExposureCash: money(risk['maxExposureCash'], true),
    minLiquidityCash: money(risk['minLiquidityCash'], true),
    maxPositions: integer(risk['maxPositions'], 1),
    maxImpactBps: integer(risk['maxImpactBps'], 0, 9_999),
    maxSlippageBps: integer(risk['maxSlippageBps'], 0, 9_999),
    maxMarketAgeMs: integer(risk['maxMarketAgeMs'], 1),
    cooldownMs: integer(risk['cooldownMs'], 0),
    feeBps: integer(simulation['feeBps'], 0, 9_999),
    fixedFeeCash: money(simulation['fixedFeeCash'], false),
    latencyMs: integer(simulation['latencyMs'], 0),
    quoteTtlMs: integer(simulation['quoteTtlMs'], 1),
    fillBps: integer(simulation['fillBps'], 1, 10_000),
    failEveryNthOrder: integer(simulation['failEveryNthOrder'], 0),
  };
  if (units(result.takeProfitMultiple) <= units('1')) {
    throw new ValidationError('Profit multiple must exceed one');
  }
  if (result.quoteTtlMs < result.latencyMs) {
    throw new ValidationError('Quote TTL must cover the configured paper latency');
  }
  return Object.freeze(result);
}

function object(value: JsonValue | undefined): { readonly [key: string]: JsonValue } {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new ValidationError('Paper policy requires an object');
  }
  return value as { readonly [key: string]: JsonValue };
}

function money(value: JsonValue | undefined, positive: boolean): string {
  if (typeof value !== 'string')
    throw new ValidationError('Paper monetary config requires strings');
  if (positive && units(value) === 0n) throw new ValidationError('Paper amount must be positive');
  units(value);
  return value;
}

function integer(value: JsonValue | undefined, min: number, max = Number.MAX_SAFE_INTEGER): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new ValidationError('Paper policy integer is missing or outside its bounds');
  }
  return value;
}
