import { AgentCapability, AgentTaskType, AgentToolName } from '@nit/agent-contracts';
import { createStrategySnapshot, StrategySnapshot } from '@nit/trading';
import {
  ModelIdentity,
  AgentConfiguration,
  AgentScope,
  InvestigationTask,
  ModelInput,
  ReasoningModel,
  ModelResponse,
  AgentConclusion,
} from './types';
export const MINT = '11111111111111111111111111111111';
export const OTHER_MINT = '22222222222222222222222222222222';
export const IDENTITY: ModelIdentity = {
  provider: 'fixture',
  model: 'scripted-reasoner',
  adapterVersion: '1',
  configurationHash: 'a'.repeat(64),
  isToyImplementation: true,
};
export function scope(asOf: Date, recordIds: string[] = []): AgentScope {
  return {
    chain: 'solana',
    from: new Date(asOf.getTime() - 3600000).toISOString(),
    asOf: asOf.toISOString(),
    tokenMints: [MINT],
    narrativeIds: ['fixture-story'],
    recordIds,
    decisionIds: [],
    portfolioIds: [],
    experimentIds: [],
    walletAddresses: [],
  };
}
export function configuration(
  asOf: Date,
  recordIds: string[] = [],
  principal = 'fixture-operator'
): AgentConfiguration {
  return {
    version: 1,
    enabled: true,
    environment: 'development',
    codeVersion: 'agent-fixture-v1',
    model: IDENTITY,
    limits: {
      maxSteps: 4,
      maxToolCalls: 3,
      maxWallMs: 30000,
      modelTimeoutMs: 5000,
      maxInputTokens: 32000,
      maxOutputTokens: 1000,
      maxRunTokens: 132000,
      maxRunCostMicros: 1000000,
      dailyTokens: 1000000,
      dailyCostMicros: 10000000,
      maxResultBytes: 16000,
      maxDateSpanMs: 86400000,
      maxQueuedTasks: 8,
      maxProtectionLagMs: 10000,
      inputMicrosPerMillion: 1000000,
      outputMicrosPerMillion: 2000000,
      requestOverheadTokens: 100,
      maxTraceBytes: 2000000,
      dailyTraceBytes: 16000000,
    },
    principals: [
      {
        identity: principal,
        expiresAt: new Date(asOf.getTime() + 86400000).toISOString(),
        scope: scope(asOf, recordIds),
        capabilities: [
          AgentCapability.READ_INTELLIGENCE,
          AgentCapability.READ_WALLETS,
          AgentCapability.READ_MARKET,
          AgentCapability.READ_BACKTEST,
          AgentCapability.READ_POSITIONS,
          AgentCapability.READ_STRATEGIES,
        ],
        allowExternalContext: true,
      },
    ],
  };
}
export function task(config: AgentConfiguration, requestId = 'fixture-task'): InvestigationTask {
  return {
    principal: config.principals[0]!.identity,
    requestId,
    kind: AgentTaskType.INVESTIGATE_NARRATIVE,
    question: 'Explain the recorded catalyst and uncertainty.',
    scope: config.principals[0]!.scope,
  };
}
export function conclusion(ids: string[] = []): AgentConclusion {
  return {
    recommendation: ids.length === 0 ? 'INSUFFICIENT_EVIDENCE' : 'EXPLANATION_ONLY',
    confidenceLabel: ids.length === 0 ? 'INSUFFICIENT' : 'LOW',
    claims:
      ids.length === 0
        ? []
        : [
            {
              interpretation:
                'The captured source text is relevant; its authenticity and predictive value remain unverified.',
              evidenceIds: ids,
            },
          ],
    missingEvidence: ['Independent provenance verification'],
    alternatives: ['Reposts may explain apparent attention.'],
  };
}
export type PlanStep = (
  input: ModelInput,
  signal: AbortSignal
) => ModelResponse | Promise<ModelResponse>;
export class FixtureModel implements ReasoningModel {
  readonly identity = IDENTITY;
  calls: ModelInput[] = [];
  constructor(private readonly steps: PlanStep[]) {}
  async plan(input: ModelInput, signal: AbortSignal): Promise<unknown> {
    this.calls.push(JSON.parse(JSON.stringify(input)) as ModelInput);
    const next = this.steps[this.calls.length - 1];
    if (next === undefined) throw new Error('Unexpected fixture model call');
    return await next(input, signal);
  }
}
export function answer(input: ModelInput, action: ModelResponse['action']): ModelResponse {
  return {
    protocolVersion: 1,
    requestId: input.requestId,
    action,
    usage: { inputTokens: 100, outputTokens: 30 },
  };
}
export function inspect(input: ModelInput, ids: string[]): ModelResponse {
  return answer(input, {
    kind: 'tool',
    tool: AgentToolName.NIT_EVENTS_SEARCH,
    version: '1',
    arguments: { recordIds: ids },
  });
}
export function investigationFixtureStrategy(): StrategySnapshot {
  return createStrategySnapshot({
    entry: {
      kind: 'entry',
      policyId: 'E02',
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
