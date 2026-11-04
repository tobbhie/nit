import { Chain, LaunchSource, QueueName, Result, failure, success } from '@nit/domain';
import { EnqueueRequest } from '@nit/queue';
import {
  SOLANA_RECONCILIATION_PAYLOAD_VERSION,
  SolanaReconciliationPayload,
} from './reconciliation-handler';

export interface ReconciliationJobEnqueuer {
  enqueue<TPayload>(request: EnqueueRequest<TPayload>): Promise<Result<unknown, Error>>;
}

export interface SolanaReconciliationSchedulerOptions {
  intervalMs: number;
  launchSources: readonly LaunchSource[];
  clock?: () => Date;
  onError?: (error: Error) => void;
  setTimer?: (callback: () => void, milliseconds: number) => ReturnType<typeof setInterval>;
  clearTimer?: (timer: ReturnType<typeof setInterval>) => void;
}

/** Enqueues one durable, idempotent repair sweep per source and time bucket. */
export class SolanaReconciliationScheduler {
  private readonly intervalMs: number;
  private readonly launchSources: readonly LaunchSource[];
  private readonly clock: () => Date;
  private readonly onError: ((error: Error) => void) | undefined;
  private readonly setTimer: NonNullable<SolanaReconciliationSchedulerOptions['setTimer']>;
  private readonly clearTimer: NonNullable<SolanaReconciliationSchedulerOptions['clearTimer']>;
  private timer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly queue: ReconciliationJobEnqueuer,
    options: SolanaReconciliationSchedulerOptions
  ) {
    if (!Number.isSafeInteger(options.intervalMs) || options.intervalMs <= 0) {
      throw new Error('Solana reconciliation interval must be a positive integer');
    }
    if (options.launchSources.length === 0) {
      throw new Error('At least one Solana reconciliation launch source is required');
    }
    if (new Set(options.launchSources).size !== options.launchSources.length) {
      throw new Error('Solana reconciliation launch sources must be unique');
    }
    this.intervalMs = options.intervalMs;
    this.launchSources = [...options.launchSources];
    this.clock = options.clock ?? ((): Date => new Date());
    this.onError = options.onError;
    this.setTimer = options.setTimer ?? setInterval;
    this.clearTimer = options.clearTimer ?? clearInterval;
  }

  async tick(): Promise<Result<number, Error>> {
    const now = this.clock();
    if (!Number.isFinite(now.getTime())) {
      return failure(new Error('Solana reconciliation scheduler clock is invalid'));
    }
    const bucket = Math.floor(now.getTime() / this.intervalMs);
    let enqueued = 0;
    for (const launchSource of this.launchSources) {
      const payload: SolanaReconciliationPayload = {
        chain: Chain.SOLANA,
        launchSource,
      };
      const result = await this.queue.enqueue({
        queueName: QueueName.TOKEN_RECONCILIATION,
        payload,
        payloadVersion: SOLANA_RECONCILIATION_PAYLOAD_VERSION,
        idempotencyKey: `solana-reconciliation:${launchSource}:${bucket}`,
      });
      if (result.kind === 'failure') return result;
      enqueued += 1;
    }
    return success(enqueued);
  }

  start(): void {
    if (this.timer !== undefined) return;
    void this.tick().then((result) => {
      if (result.kind === 'failure') this.onError?.(result.error);
    });
    this.timer = this.setTimer(() => {
      void this.tick().then((result) => {
        if (result.kind === 'failure') this.onError?.(result.error);
      });
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer === undefined) return;
    this.clearTimer(this.timer);
    this.timer = undefined;
  }
}
