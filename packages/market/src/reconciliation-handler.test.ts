import { describe, expect, it } from 'vitest';
import {
  Chain,
  JobId,
  JobStatus,
  LaunchSource,
  QueueName,
  Result,
  failure,
  success,
} from '@nit/domain';
import { Job, PermanentJobFailure } from '@nit/queue';
import {
  SOLANA_RECONCILIATION_PAYLOAD_VERSION,
  SolanaReconciliationHandler,
  SolanaReconciliationPayload,
  SolanaReconciliationRunner,
} from './reconciliation-handler';
import { SolanaReconciliationSummary } from './reconciliation';

const NOW = new Date('2026-11-02T00:00:00.000Z');

function summary(): SolanaReconciliationSummary {
  return {
    chain: Chain.SOLANA,
    launchSource: LaunchSource.PUMP_FUN,
    provider: 'solana-json-rpc',
    fromSlotInclusive: 900,
    throughFinalizedSlot: 1_000,
    pages: 2,
    fetched: 3,
    discovered: 2,
    graduated: 0,
    duplicates: 1,
  };
}

class StubRunner implements SolanaReconciliationRunner {
  calls = 0;

  constructor(private readonly result: Result<SolanaReconciliationSummary, Error>) {}

  reconcile(): Promise<Result<SolanaReconciliationSummary, Error>> {
    this.calls += 1;
    return Promise.resolve(this.result);
  }
}

function job(
  payload: SolanaReconciliationPayload,
  payloadVersion = SOLANA_RECONCILIATION_PAYLOAD_VERSION
): Job<SolanaReconciliationPayload> {
  return {
    jobId: 'job-reconcile-1' as JobId,
    queueName: QueueName.TOKEN_RECONCILIATION,
    payload,
    payloadVersion,
    status: JobStatus.RUNNING,
    attemptCount: 1,
    maxAttempts: 5,
    leaseDeadline: NOW,
    leasedBy: 'worker-1',
    idempotencyKey: 'token_reconciliation:solana:pump_fun:2026-11-02T00:00',
    availableAt: NOW,
    lastError: null,
    correlationId: null,
    createdAt: NOW,
    updatedAt: NOW,
    completedAt: null,
  };
}

const PAYLOAD: SolanaReconciliationPayload = {
  chain: Chain.SOLANA,
  launchSource: LaunchSource.PUMP_FUN,
};

describe('SolanaReconciliationHandler', () => {
  it('runs the source-specific reconciler and reports its summary', async () => {
    const runner = new StubRunner(success(summary()));
    const reconciled: SolanaReconciliationSummary[] = [];
    const handler = new SolanaReconciliationHandler(new Map([[LaunchSource.PUMP_FUN, runner]]), {
      onReconciled: (value): void => {
        reconciled.push(value);
      },
    });

    await handler.handle(job(PAYLOAD));

    expect(handler.queueName).toBe(QueueName.TOKEN_RECONCILIATION);
    expect(runner.calls).toBe(1);
    expect(reconciled).toEqual([summary()]);
  });

  it('leaves operational reconciliation failures retryable', async () => {
    const handler = new SolanaReconciliationHandler(
      new Map([[LaunchSource.PUMP_FUN, new StubRunner(failure(new Error('RPC unavailable')))]])
    );

    await expect(handler.handle(job(PAYLOAD))).rejects.toThrow('RPC unavailable');
    await expect(handler.handle(job(PAYLOAD))).rejects.not.toBeInstanceOf(PermanentJobFailure);
  });

  it('dead-letters an unsupported payload version', async () => {
    const handler = new SolanaReconciliationHandler(new Map());

    await expect(handler.handle(job(PAYLOAD, '2'))).rejects.toBeInstanceOf(PermanentJobFailure);
  });

  it('dead-letters an unknown or unbound source', async () => {
    const handler = new SolanaReconciliationHandler(new Map());
    const unknown = {
      chain: Chain.SOLANA,
      launchSource: 'unknown-source' as LaunchSource,
    };

    await expect(handler.handle(job(unknown))).rejects.toBeInstanceOf(PermanentJobFailure);
    await expect(handler.handle(job(PAYLOAD))).rejects.toThrow(
      "No Solana reconciler bound for 'pump_fun'"
    );
  });
});
