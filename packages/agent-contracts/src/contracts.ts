import {
  AgentRunId,
  AgentTaskId,
  AgentToolCallId,
  EvidenceId,
  Timestamp,
  ValidationError,
} from '@nit/domain';

export enum AgentTaskType {
  INVESTIGATE_NARRATIVE = 'investigate_narrative',
  INVESTIGATE_WALLETS = 'investigate_wallets',
  EXPLAIN_DECISION = 'explain_decision',
  REVIEW_EPISODE = 'review_episode',
  PROPOSE_STRATEGY = 'propose_strategy',
  RUN_EXPERIMENT = 'run_experiment',
  REVIEW_EXPERIMENT = 'review_experiment',
  REQUEST_APPROVAL = 'request_approval',
}

export enum AgentCapability {
  READ_INTELLIGENCE = 'read_intelligence',
  READ_WALLETS = 'read_wallets',
  READ_MARKET = 'read_market',
  READ_STRATEGIES = 'read_strategies',
  RUN_BACKTEST = 'run_backtest',
  READ_BACKTEST = 'read_backtest',
  PROPOSE_STRATEGY = 'propose_strategy',
  FORK_PAPER = 'fork_paper',
  REQUEST_PAPER_TRADE = 'request_paper_trade',
  READ_POSITIONS = 'read_positions',
  REQUEST_APPROVAL = 'request_approval',
  WRITE_LEARNING_JOURNAL = 'write_learning_journal',
}

export enum AgentToolName {
  NIT_EVENTS_SEARCH = 'nit.events.search',
  NIT_CANDIDATES_INSPECT = 'nit.candidates.inspect',
  WALLET_ACTIVITY_INSPECT = 'wallet.activity.inspect',
  MARKET_SNAPSHOT_GET = 'market.snapshot.get',
  STRATEGY_CATALOG_READ = 'strategy.catalog.read',
  REPLAY_RUN = 'replay.run',
  REPLAY_COMPARE = 'replay.compare',
  STRATEGY_PROPOSAL_CREATE = 'strategy.proposal.create',
  STRATEGY_PAPER_FORK_CREATE = 'strategy.paper_fork.create',
  TRADER_PAPER_REQUEST_ENTRY = 'trader.paper.request_entry',
  TRADER_POSITION_INSPECT = 'trader.position.inspect',
  OPERATOR_APPROVAL_REQUEST = 'operator.approval.request',
  JOURNAL_LESSON_APPEND = 'journal.lesson.append',
}

export interface AgentToolDefinition {
  readonly name: AgentToolName;
  readonly version: string;
  readonly requiredCapability: AgentCapability;
  readonly mutating: boolean;
}

export interface AgentToolCallRequest<TArguments = unknown> {
  readonly toolCallId: AgentToolCallId;
  readonly agentRunId: AgentRunId;
  readonly taskId: AgentTaskId;
  readonly requestId: string;
  readonly callerIdentity: string;
  readonly tool: AgentToolName;
  readonly toolVersion: string;
  readonly capability: AgentCapability;
  readonly arguments: TArguments;
  readonly idempotencyKey?: string;
  readonly requestedAt: Timestamp;
}

export interface AgentToolCallResult<TResult = unknown> {
  readonly toolCallId: AgentToolCallId;
  readonly completedAt: Timestamp;
  readonly result: TResult;
  readonly evidenceIds: readonly EvidenceId[];
  readonly availableAt: Timestamp;
  readonly dataQuality: 'complete' | 'partial' | 'insufficient';
  readonly costUnits?: number;
}

const MUTATING_TOOLS = new Set<AgentToolName>([
  AgentToolName.REPLAY_RUN,
  AgentToolName.STRATEGY_PROPOSAL_CREATE,
  AgentToolName.STRATEGY_PAPER_FORK_CREATE,
  AgentToolName.TRADER_PAPER_REQUEST_ENTRY,
  AgentToolName.OPERATOR_APPROVAL_REQUEST,
  AgentToolName.JOURNAL_LESSON_APPEND,
]);

export function isMutatingAgentTool(tool: AgentToolName): boolean {
  return MUTATING_TOOLS.has(tool);
}

export function validateAgentToolCall(
  definition: AgentToolDefinition,
  request: AgentToolCallRequest
): void {
  if (request.tool !== definition.name || request.toolVersion !== definition.version) {
    throw new ValidationError('Agent tool name/version does not match the registered definition');
  }
  if (request.capability !== definition.requiredCapability) {
    throw new ValidationError(`Capability ${request.capability} cannot invoke ${request.tool}`);
  }
  if (definition.mutating !== isMutatingAgentTool(definition.name)) {
    throw new ValidationError(`Tool mutation classification is invalid for ${definition.name}`);
  }
  if (definition.mutating && request.idempotencyKey === undefined) {
    throw new ValidationError(`Mutating tool ${request.tool} requires an idempotency key`);
  }
}
