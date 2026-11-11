import { describe, expect, it } from 'vitest';
import { QueueName, Result, failure, success } from '@nit/domain';
import { EnqueueRequest, Job, PermanentJobFailure } from '@nit/queue';
import {
  ConfirmedIngestionSummary,
  DeferredConfirmedIngestionError,
  PreparedConfirmedIngestion,
} from './confirmed-ingestion';
import {
  RecoverableConfirmedIngestor,
  MarketRecoveryQueue,
  SolanaMarketRecoveryHandler,
} from './market-recovery';
import { encodeBase58 } from './pump-events';

const program = encodeBase58(Buffer.alloc(32, 1));
class RecordingQueue implements MarketRecoveryQueue {
  readonly saved: EnqueueRequest<unknown>[] = [];
  fail = false;
  enqueue<T>(request: EnqueueRequest<T>): Promise<Result<unknown, Error>> {
    this.saved.push(request);
    return Promise.resolve(this.fail ? failure(new Error('DB unavailable')) : success({}));
  }
}
function summary(signature = 'sig'): Result<ConfirmedIngestionSummary, Error> {
  return success({ signature, slot: 42, fetched: 0, discovered: 0, graduated: 0, duplicates: 0 });
}

describe('durable market recovery', () => {
  it('saves terminal read failures with stable identity before reporting deferral', async () => {
    const queue = new RecordingQueue();
    const base = {
      prepare: (): Promise<Result<PreparedConfirmedIngestion, Error>> =>
        Promise.resolve(failure(new Error('transport'))),
      ingest: (): Promise<Result<ConfirmedIngestionSummary, Error>> => Promise.resolve(summary()),
    };
    const wrapper = new RecoverableConfirmedIngestor(base, queue, program);
    const result = await wrapper.prepare('sig', 42);
    expect(result.kind).toBe('failure');
    if (result.kind === 'failure')
      expect(result.error).toBeInstanceOf(DeferredConfirmedIngestionError);
    expect(queue.saved[0]).toMatchObject({
      queueName: QueueName.TOKEN_MARKET_RECOVERY,
      payload: { programAddress: program, signature: 'sig', slot: 42 },
      payloadVersion: '1',
      idempotencyKey: `solana-market-recovery:${program}:sig`,
    });
    await wrapper.prepare('sig', 42);
    expect(queue.saved[1]?.idempotencyKey).toBe(queue.saved[0]?.idempotencyKey);
  });
  it('never reports successful deferral when the recovery job could not be persisted', async () => {
    const queue = new RecordingQueue();
    queue.fail = true;
    const wrapper = new RecoverableConfirmedIngestor(
      {
        prepare: (): Promise<Result<PreparedConfirmedIngestion, Error>> =>
          Promise.reject(new Error('transport')),
        ingest: (): Promise<Result<ConfirmedIngestionSummary, Error>> => Promise.resolve(summary()),
      },
      queue,
      program
    );
    const result = await wrapper.prepare('sig', 42);
    expect(result.kind).toBe('failure');
    if (result.kind === 'failure')
      expect(result.error).not.toBeInstanceOf(DeferredConfirmedIngestionError);
  });
  it('retains per-mint ordering keys and persists failed commits for retry', async () => {
    const queue = new RecordingQueue();
    const apply: PreparedConfirmedIngestion = () =>
      Promise.resolve(failure(new Error('missing creation')));
    apply.keys = ['mint'];
    const wrapper = new RecoverableConfirmedIngestor(
      {
        prepare: (): Promise<Result<PreparedConfirmedIngestion, Error>> =>
          Promise.resolve(success(apply)),
        ingest: (): Promise<Result<ConfirmedIngestionSummary, Error>> => Promise.resolve(summary()),
      },
      queue,
      program
    );
    const prepared = await wrapper.prepare('sig', 42);
    if (prepared.kind === 'failure') throw prepared.error;
    expect(prepared.value.keys).toEqual(['mint']);
    expect(queue.saved).toHaveLength(0);
    const result = await prepared.value();
    expect(result.kind).toBe('failure');
    expect(queue.saved).toHaveLength(1);
  });
  it('replays the unwrapped ingestor and rejects invalid/unconfigured payloads', async () => {
    const calls: string[] = [];
    const handler = new SolanaMarketRecoveryHandler(
      new Map([
        [
          program,
          {
            ingest: (sig): Promise<Result<ConfirmedIngestionSummary, Error>> => {
              calls.push(sig);
              return Promise.resolve(summary(sig));
            },
          },
        ],
      ])
    );
    const job = {
      payloadVersion: '1',
      payload: { programAddress: program, signature: 'sig', slot: 42 },
    } as Job;
    await handler.handle(job);
    expect(calls).toEqual(['sig']);
    await expect(handler.handle({ ...job, payloadVersion: '2' })).rejects.toBeInstanceOf(
      PermanentJobFailure
    );
    await expect(
      handler.handle({ ...job, payload: { ...(job.payload as object), slot: -1 } })
    ).rejects.toBeInstanceOf(PermanentJobFailure);
    await expect(
      handler.handle({
        ...job,
        payload: { programAddress: encodeBase58(Buffer.alloc(32, 2)), signature: 'sig', slot: 42 },
      })
    ).rejects.toBeInstanceOf(PermanentJobFailure);
  });
});
