import { Chain, LaunchSource, QueueName, Result, failure } from '@nit/domain';
import { Job, JobHandler, PermanentJobFailure } from '@nit/queue';
import { SolanaReconciliationSummary } from './reconciliation';

export const SOLANA_RECONCILIATION_PAYLOAD_VERSION = '1';

const LAUNCH_SOURCES = new Set<string>(Object.values(LaunchSource));

export interface SolanaReconciliationPayload {
  chain: Chain.SOLANA;
  launchSource: LaunchSource;
}

export interface SolanaReconciliationHandlerOptions {
  onReconciled?: (summary: SolanaReconciliationSummary) => void;
}

export interface SolanaReconciliationRunner {
  reconcile(): Promise<Result<SolanaReconciliationSummary, Error>>;
}

/**
 * Durable queue entrypoint for one launch source's reconciliation sweep.
 *
 * Scheduling is intentionally outside the handler: the scheduler can enqueue a
 * stable idempotency key per source and interval, while the worker remains a
 * normal retryable job consumer.
 */
export class SolanaReconciliationHandler implements JobHandler<SolanaReconciliationPayload> {
  readonly queueName: string = QueueName.TOKEN_RECONCILIATION;

  constructor(
    private readonly reconcilers: ReadonlyMap<LaunchSource, SolanaReconciliationRunner>,
    private readonly options: SolanaReconciliationHandlerOptions = {}
  ) {}

  async handle(job: Job<SolanaReconciliationPayload>): Promise<void> {
    if (job.payloadVersion !== SOLANA_RECONCILIATION_PAYLOAD_VERSION) {
      throw new PermanentJobFailure(
        `Unsupported Solana reconciliation payload version '${job.payloadVersion}'`
      );
    }

    const result = await this.reconcile(job.payload);
    if (result.kind === 'failure') throw result.error;
    this.options.onReconciled?.(result.value);
  }

  async reconcile(
    payload: SolanaReconciliationPayload
  ): Promise<Result<SolanaReconciliationSummary, Error>> {
    const chain: string = payload.chain;
    if (chain !== String(Chain.SOLANA)) {
      throw new PermanentJobFailure(`Unknown Solana reconciliation chain '${chain}'`);
    }
    const launchSource: string = payload.launchSource;
    if (!LAUNCH_SOURCES.has(launchSource)) {
      throw new PermanentJobFailure(
        `Unknown Solana reconciliation launch source '${launchSource}'`
      );
    }

    const reconciler = this.reconcilers.get(payload.launchSource);
    if (reconciler === undefined) {
      throw new PermanentJobFailure(`No Solana reconciler bound for '${payload.launchSource}'`);
    }

    try {
      return await reconciler.reconcile();
    } catch (error) {
      return failure(error instanceof Error ? error : new Error('Solana reconciliation failed'));
    }
  }
}
