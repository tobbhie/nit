import { failure, ValidationError } from '@nit/domain';
import { SignalEnvelope } from '@nit/signals';
import { describe, expect, it } from 'vitest';
import {
  Broker,
  createStrategySnapshot,
  createTradingRuntime,
  EventFeed,
  ExecutionMode,
} from '.';

const clock = { now: (): Date => new Date('2026-01-01T00:00:00.000Z') };
const feed: EventFeed = {
  iterate(): AsyncIterable<SignalEnvelope> {
    return {
      [Symbol.asyncIterator](): AsyncIterator<SignalEnvelope> {
        return {
          next: () => Promise.resolve({ done: true, value: undefined }),
        };
      },
    };
  },
};
const broker: Broker = {
  brokerName: 'deterministic-paper',
  supportedModes: [ExecutionMode.HISTORICAL, ExecutionMode.FORWARD_PAPER],
  quote(): ReturnType<Broker['quote']> {
    return Promise.resolve(failure(new Error('not implemented in contract test')));
  },
  submit(): ReturnType<Broker['submit']> {
    return Promise.resolve(failure(new Error('not implemented in contract test')));
  },
};
const strategy = createStrategySnapshot({
  entry: { kind: 'entry', policyId: 'E01', version: 1, config: {} },
  exit: { kind: 'exit', policyId: 'X01', version: 1, config: {} },
  protection: { kind: 'protection', policyId: 'P01', version: 1, config: {} },
  risk: { kind: 'risk', policyId: 'R01', version: 1, config: {} },
});

describe('trading runtime composition', () => {
  it.each([ExecutionMode.HISTORICAL, ExecutionMode.FORWARD_PAPER])(
    'uses the same contracts in %s mode',
    (mode) => {
      const runtime = createTradingRuntime({ mode, clock, feed, broker, strategy });
      expect(runtime.strategy.configHash).toBe(strategy.configHash);
    }
  );

  it('fails closed for live mode', () => {
    expect(() =>
      createTradingRuntime({
        mode: ExecutionMode.LIVE,
        clock,
        feed,
        broker: { ...broker, supportedModes: [...broker.supportedModes, ExecutionMode.LIVE] },
        strategy,
      })
    ).toThrow(ValidationError);
  });
});
