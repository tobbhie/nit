import {
  OrderId,
  PortfolioId,
  Result,
  StrategyRunId,
  Timestamp,
  ValidationError,
} from '@nit/domain';
import { SignalEnvelope } from '@nit/signals';
import { DecimalString } from './decimal';
import { StrategySnapshot } from './strategy';

export enum ExecutionMode {
  HISTORICAL = 'historical',
  FORWARD_PAPER = 'forward_paper',
  LIVE = 'live',
}

export enum OrderSide {
  BUY = 'buy',
  SELL = 'sell',
}

export interface Clock {
  now(): Timestamp;
}

export interface EventFeed<TSignal extends SignalEnvelope = SignalEnvelope> {
  iterate(from: Timestamp, to: Timestamp): AsyncIterable<TSignal>;
}

export interface AssetAmount {
  asset: string;
  quantity: DecimalString;
}

export interface QuoteRequest {
  tokenMint: string;
  side: OrderSide;
  amount: AssetAmount;
  requestedAt: Timestamp;
}

export interface ExecutableQuote {
  quoteId: string;
  tokenMint: string;
  side: OrderSide;
  input: AssetAmount;
  expectedOutput: AssetAmount;
  fees: readonly AssetAmount[];
  venue: string;
  priceImpactBps: number;
  quotedAt: Timestamp;
  expiresAt: Timestamp;
}

export interface OrderIntent {
  orderId: OrderId;
  runId: StrategyRunId;
  portfolioId: PortfolioId;
  strategyConfigHash: string;
  quoteId: string;
  tokenMint: string;
  side: OrderSide;
  amount: AssetAmount;
  reasonCodes: readonly string[];
  idempotencyKey: string;
  createdAt: Timestamp;
}

export interface BrokerReceipt {
  orderId: OrderId;
  status: 'accepted' | 'partially_filled' | 'filled' | 'rejected' | 'failed';
  executedInput?: AssetAmount;
  executedOutput?: AssetAmount;
  fees: readonly AssetAmount[];
  venue: string;
  transactionReference?: string;
  failureReason?: string;
  recordedAt: Timestamp;
}

export interface Broker {
  readonly brokerName: string;
  readonly supportedModes: readonly ExecutionMode[];
  quote(request: QuoteRequest): Promise<Result<ExecutableQuote, Error>>;
  submit(intent: OrderIntent, quote: ExecutableQuote): Promise<Result<BrokerReceipt, Error>>;
}

export interface TradingRuntime<TSignal extends SignalEnvelope = SignalEnvelope> {
  mode: ExecutionMode;
  clock: Clock;
  feed: EventFeed<TSignal>;
  broker: Broker;
  strategy: StrategySnapshot;
}

/**
 * Phase 1 deliberately makes live composition impossible. Phase 9 must replace
 * this with an authorization service that binds a signer, operator approval,
 * exact strategy hash, capital scope, and expiry.
 */
export function createTradingRuntime<TSignal extends SignalEnvelope>(
  runtime: TradingRuntime<TSignal>
): TradingRuntime<TSignal> {
  if (runtime.mode === ExecutionMode.LIVE) {
    throw new ValidationError('Live execution is unavailable before Phase 9 authorization');
  }
  if (!runtime.broker.supportedModes.includes(runtime.mode)) {
    throw new ValidationError(
      `Broker ${runtime.broker.brokerName} does not support ${runtime.mode}`
    );
  }
  return runtime;
}
