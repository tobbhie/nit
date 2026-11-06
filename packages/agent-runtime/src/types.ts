import { z } from 'zod';
import { AgentCapability, AgentTaskType, AgentToolName } from '@nit/agent-contracts';
import { evidenceHash } from '@nit/signals';

export const ReadTools = {
  [AgentToolName.NIT_EVENTS_SEARCH]: AgentCapability.READ_INTELLIGENCE,
  [AgentToolName.NIT_CANDIDATES_INSPECT]: AgentCapability.READ_INTELLIGENCE,
  [AgentToolName.WALLET_ACTIVITY_INSPECT]: AgentCapability.READ_WALLETS,
  [AgentToolName.MARKET_SNAPSHOT_GET]: AgentCapability.READ_MARKET,
  [AgentToolName.STRATEGY_CATALOG_READ]: AgentCapability.READ_STRATEGIES,
  [AgentToolName.REPLAY_COMPARE]: AgentCapability.READ_BACKTEST,
  [AgentToolName.TRADER_POSITION_INSPECT]: AgentCapability.READ_POSITIONS,
} as const;
export type ReadTool = keyof typeof ReadTools;
const uuid = z.string().uuid();
const name = z.string().min(1).max(200);
const mint = z.string().regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/);
const ids = z.array(uuid).max(64);
const names = z.array(name).max(32);
export const ScopeSchema = z
  .object({
    chain: z.literal('solana'),
    from: z.string().datetime(),
    asOf: z.string().datetime(),
    tokenMints: z.array(mint).max(32),
    narrativeIds: names,
    recordIds: ids,
    decisionIds: ids,
    portfolioIds: ids,
    experimentIds: ids,
    walletAddresses: z.array(mint).max(64),
  })
  .strict();
export type AgentScope = z.infer<typeof ScopeSchema>;
const observeTasks = [
  AgentTaskType.INVESTIGATE_NARRATIVE,
  AgentTaskType.INVESTIGATE_WALLETS,
  AgentTaskType.EXPLAIN_DECISION,
  AgentTaskType.REVIEW_EPISODE,
  AgentTaskType.REVIEW_EXPERIMENT,
] as const;
export const TaskSchema = z
  .object({
    principal: name,
    requestId: name,
    kind: z.enum(observeTasks),
    question: z.string().min(1).max(2000),
    scope: ScopeSchema,
  })
  .strict();
export type InvestigationTask = z.infer<typeof TaskSchema>;
export const LimitsSchema = z
  .object({
    maxSteps: z.number().int().min(1).max(16),
    maxToolCalls: z.number().int().min(1).max(16),
    maxWallMs: z.number().int().min(10).max(120000),
    modelTimeoutMs: z.number().int().min(5).max(30000),
    maxInputTokens: z.number().int().min(512).max(64000),
    maxOutputTokens: z.number().int().min(64).max(16000),
    maxRunTokens: z.number().int().min(576).max(256000),
    maxRunCostMicros: z.number().int().min(1).max(100000000),
    dailyTokens: z.number().int().positive().max(10000000),
    dailyCostMicros: z.number().int().positive().max(1000000000),
    maxResultBytes: z.number().int().min(1024).max(48000),
    maxDateSpanMs: z.number().int().positive().max(31536000000),
    maxQueuedTasks: z.number().int().min(1).max(64),
    maxProtectionLagMs: z.number().int().positive(),
    maxTraceBytes: z.number().int().min(16000).max(2097152),
    dailyTraceBytes: z.number().int().min(16000).max(33554432),
    inputMicrosPerMillion: z.number().int().nonnegative().max(1000000000),
    outputMicrosPerMillion: z.number().int().nonnegative().max(1000000000),
    requestOverheadTokens: z.number().int().nonnegative().max(8192),
  })
  .strict();
export type AgentLimits = z.infer<typeof LimitsSchema>;
export interface AgentPrincipal {
  readonly identity: string;
  readonly expiresAt: string;
  readonly capabilities: readonly AgentCapability[];
  readonly scope: AgentScope;
  readonly allowExternalContext: boolean;
}
export interface ModelIdentity {
  readonly provider: string;
  readonly model: string;
  readonly adapterVersion: string;
  readonly configurationHash: string;
  readonly isToyImplementation: boolean;
}
export interface AgentConfiguration {
  readonly version: 1;
  readonly enabled: boolean;
  readonly environment: 'development' | 'production';
  readonly codeVersion: string;
  readonly model: ModelIdentity;
  readonly limits: AgentLimits;
  readonly principals: readonly AgentPrincipal[];
}
export interface GuideBundle {
  readonly version: 1;
  readonly hash: string;
  readonly files: Readonly<Record<string, { readonly hash: string; readonly text: string }>>;
}
export interface EvidenceRecord {
  readonly viewHash: string;
  readonly evidenceId: string;
  readonly type: string;
  readonly source: string;
  readonly availableAt: string;
  readonly hash: string;
  readonly data: unknown;
}
export interface ToolResult {
  readonly records: readonly EvidenceRecord[];
  readonly dataQuality: 'complete' | 'partial' | 'insufficient';
  readonly availableAt: string;
}
export interface ModelInput {
  readonly budget: {
    readonly remainingSteps: number;
    readonly remainingToolCalls: number;
    readonly remainingReservedTokens: number;
    readonly remainingReservedCostMicros: number;
  };
  readonly protocolVersion: 1;
  readonly requestId: string;
  readonly identity: ModelIdentity;
  readonly guides: GuideBundle;
  readonly task: InvestigationTask;
  readonly tools: readonly { name: ReadTool; version: '1'; arguments: unknown }[];
  readonly history: readonly unknown[];
  readonly maxInputTokens: number;
  readonly maxOutputTokens: number;
}
export const ConclusionSchema = z
  .object({
    confidenceLabel: z.enum(['LOW', 'MODERATE', 'INSUFFICIENT']),
    recommendation: z.enum(['WATCH', 'INSUFFICIENT_EVIDENCE', 'EXPLANATION_ONLY']),
    claims: z
      .array(
        z
          .object({
            interpretation: z.string().min(1).max(1200),
            evidenceIds: z.array(uuid).min(1).max(16),
          })
          .strict()
      )
      .max(12),
    missingEvidence: z.array(z.string().min(1).max(300)).max(16),
    alternatives: z.array(z.string().min(1).max(500)).max(8),
  })
  .strict();
export type AgentConclusion = z.infer<typeof ConclusionSchema>;
export const ActionSchema = z.discriminatedUnion('kind', [
  z
    .object({
      kind: z.literal('tool'),
      tool: z.string().min(1).max(100),
      version: z.literal('1'),
      arguments: z.unknown(),
    })
    .strict(),
  z.object({ kind: z.literal('conclusion'), conclusion: ConclusionSchema }).strict(),
]);
export const ModelResponseSchema = z
  .object({
    protocolVersion: z.literal(1),
    requestId: name,
    action: ActionSchema,
    usage: z
      .object({
        inputTokens: z.number().int().nonnegative(),
        outputTokens: z.number().int().nonnegative(),
      })
      .strict(),
  })
  .strict();
export type ModelResponse = z.infer<typeof ModelResponseSchema>;
export interface ReasoningModel {
  readonly identity: ModelIdentity;
  plan(input: ModelInput, signal: AbortSignal): Promise<unknown>;
}

export function assertScope(
  scope: AgentScope,
  allowed: AgentScope,
  limits: AgentLimits,
  now: Date
): void {
  ScopeSchema.parse(scope);
  ScopeSchema.parse(allowed);
  const start = Date.parse(scope.from);
  const end = Date.parse(scope.asOf);
  if (
    start > end ||
    end > now.getTime() ||
    end - start > limits.maxDateSpanMs ||
    start < Date.parse(allowed.from) ||
    end > Date.parse(allowed.asOf)
  )
    throw new Error('SCOPE_DENIED');
  for (const key of [
    'tokenMints',
    'narrativeIds',
    'recordIds',
    'decisionIds',
    'portfolioIds',
    'experimentIds',
    'walletAddresses',
  ] as const)
    if (scope[key].some((id) => !allowed[key].includes(id))) throw new Error('SCOPE_DENIED');
}
export function modelCost(inputTokens: number, outputTokens: number, limits: AgentLimits): number {
  return Number(
    (BigInt(inputTokens) * BigInt(limits.inputMicrosPerMillion) +
      BigInt(outputTokens) * BigInt(limits.outputMicrosPerMillion) +
      999999n) /
      1000000n
  );
}
export function sameIdentity(a: ModelIdentity, b: ModelIdentity): boolean {
  return evidenceHash(a) === evidenceHash(b);
}
