import { describe, expect, it } from 'vitest';
import { LaunchSource, Result, success } from '@nit/domain';
import { EnqueueRequest } from '@nit/queue';
import {
  ReconciliationJobEnqueuer,
  SolanaReconciliationScheduler,
} from './reconciliation-scheduler';

class RecordingQueue implements ReconciliationJobEnqueuer {
  readonly requests: EnqueueRequest[] = [];

  enqueue<TPayload>(request: EnqueueRequest<TPayload>): Promise<Result<unknown, Error>> {
    this.requests.push(request as EnqueueRequest);
    return Promise.resolve(success({}));
  }
}

describe('SolanaReconciliationScheduler', () => {
  it('uses one deterministic durable key per source and interval', async () => {
    const queue = new RecordingQueue();
    const scheduler = new SolanaReconciliationScheduler(queue, {
      intervalMs: 60_000,
      launchSources: [LaunchSource.PUMP_FUN, LaunchSource.PUMPSWAP],
      clock: (): Date => new Date('2026-11-01T00:00:30.000Z'),
    });

    const result = await scheduler.tick();

    expect(result).toEqual(success(2));
    expect(queue.requests.map((request) => request.idempotencyKey)).toEqual([
      'solana-reconciliation:pump_fun:29891520',
      'solana-reconciliation:pumpswap:29891520',
    ]);
    expect(queue.requests.map((request) => request.payload)).toEqual([
      { chain: 'solana', launchSource: 'pump_fun' },
      { chain: 'solana', launchSource: 'pumpswap' },
    ]);
  });

  it('rejects empty or duplicate source configuration', () => {
    const queue = new RecordingQueue();
    expect(
      () => new SolanaReconciliationScheduler(queue, { intervalMs: 1_000, launchSources: [] })
    ).toThrow('At least one');
    expect(
      () =>
        new SolanaReconciliationScheduler(queue, {
          intervalMs: 1_000,
          launchSources: [LaunchSource.PUMP_FUN, LaunchSource.PUMP_FUN],
        })
    ).toThrow('unique');
  });
});
