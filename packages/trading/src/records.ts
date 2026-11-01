import {
  DecisionId,
  EvidenceId,
  FillId,
  OrderEventId,
  OrderId,
  PortfolioId,
  PositionEventId,
  PositionId,
  SignalId,
  StrategyCombinationId,
  StrategyRunId,
  Timestamp,
} from '@nit/domain';
import { DecimalString } from './decimal';
import { ExecutionMode, OrderSide } from './runtime';
import { JsonValue } from './strategy';

export enum StrategyRunStatus {
  PLANNED = 'planned',
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

export enum DecisionType {
  ENTER = 'enter',
  ABSTAIN = 'abstain',
  WATCH = 'watch',
  REVIEW = 'review',
  BLOCK = 'block',
  REDUCE = 'reduce',
  EXIT = 'exit',
}

export enum OrderStatus {
  CREATED = 'created',
  ACCEPTED = 'accepted',
  PARTIALLY_FILLED = 'partially_filled',
  FILLED = 'filled',
  REJECTED = 'rejected',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

export enum PositionState {
  OPEN = 'open',
  PARTIALLY_CLOSED = 'partially_closed',
  MOONBAG = 'moonbag',
  CLOSED = 'closed',
}

export enum PositionEventType {
  OPENED = 'opened',
  INCREASED = 'increased',
  PARTIAL_SELL = 'partial_sell',
  MOONBAG_ARMED = 'moonbag_armed',
  STOP_CHANGED = 'stop_changed',
  EXIT_FAILED = 'exit_failed',
  CLOSED = 'closed',
}

export interface StrategyRunRecord {
  readonly runId: StrategyRunId;
  readonly combinationId: StrategyCombinationId;
  readonly mode: Exclude<ExecutionMode, ExecutionMode.LIVE>;
  readonly configHash: string;
  readonly codeCommitSha: string;
  readonly periodStart: Timestamp;
  readonly periodEnd: Timestamp;
  readonly status: StrategyRunStatus;
  readonly startedAt?: Timestamp;
  readonly endedAt?: Timestamp;
}

export interface PortfolioRecord {
  readonly portfolioId: PortfolioId;
  readonly runId: StrategyRunId;
  readonly combinationId: StrategyCombinationId;
  readonly cashAsset: string;
  readonly initialCash: DecimalString;
  readonly currentCash: DecimalString;
  readonly capitalConstraints: JsonValue;
}

export interface StrategyDecisionRecord {
  readonly decisionId: DecisionId;
  readonly runId: StrategyRunId;
  readonly portfolioId: PortfolioId;
  readonly combinationId: StrategyCombinationId;
  readonly sourceSignalId: SignalId;
  readonly type: DecisionType;
  readonly evidenceIds: readonly EvidenceId[];
  readonly reasonCodes: readonly string[];
  readonly riskGateResults: JsonValue;
  readonly idempotencyKey: string;
  readonly decidedAt: Timestamp;
}

export interface OrderRecord {
  readonly orderId: OrderId;
  readonly decisionId: DecisionId;
  readonly runId: StrategyRunId;
  readonly portfolioId: PortfolioId;
  readonly tokenMint: string;
  readonly side: OrderSide;
  readonly quantityAsset: string;
  readonly quantity: DecimalString;
  readonly quoteId: string;
  readonly idempotencyKey: string;
  readonly createdAt: Timestamp;
}

export interface OrderEventRecord {
  readonly orderEventId: OrderEventId;
  readonly orderId: OrderId;
  readonly status: OrderStatus;
  readonly detail?: JsonValue;
  readonly occurredAt: Timestamp;
}

export interface FillRecord {
  readonly fillId: FillId;
  readonly orderId: OrderId;
  readonly status: OrderStatus.PARTIALLY_FILLED | OrderStatus.FILLED;
  readonly inputAsset: string;
  readonly inputQuantity: DecimalString;
  readonly outputAsset: string;
  readonly outputQuantity: DecimalString;
  readonly fees: JsonValue;
  readonly venue: string;
  readonly transactionReference?: string;
  readonly recordedAt: Timestamp;
}

export interface PositionRecord {
  readonly positionId: PositionId;
  readonly portfolioId: PortfolioId;
  readonly tokenMint: string;
  readonly originalQuantity: DecimalString;
  readonly openQuantity: DecimalString;
  readonly costBasis: DecimalString;
  readonly highWaterPrice: DecimalString;
  readonly state: PositionState;
  readonly protectionConfig: JsonValue;
  readonly stopPrice?: DecimalString;
  readonly updatedAt: Timestamp;
}

export interface PositionEventRecord {
  readonly positionEventId: PositionEventId;
  readonly positionId: PositionId;
  readonly orderId?: OrderId;
  readonly fillId?: FillId;
  readonly type: PositionEventType;
  readonly quantityDelta: DecimalString;
  readonly detail: JsonValue;
  readonly occurredAt: Timestamp;
}
