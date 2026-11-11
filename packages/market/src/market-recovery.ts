import { Chain, LaunchSource, QueueName, Result, failure, success } from '@nit/domain';
import { Executor } from '@nit/database-public';
import { EnqueueRequest, Job, JobHandler, PermanentJobFailure } from '@nit/queue';
import { z } from 'zod';
import {
  ConfirmedNotificationIngestor,
  ConfirmedIngestionSummary,
  PreparedConfirmedIngestion,
  DeferredConfirmedIngestionError,
  SolanaConfirmedOccurrenceIngestor,
} from './confirmed-ingestion';
import { ReconciledMarketOccurrence } from './reconciliation';
import {
  SolanaRpcReconciliationSource,
  CreationHistoryProgressStore,
  CreationHistoryProgress,
} from './solana-rpc';

const SearchProgress = z
  .object({
    beforeSignature: z.string().min(1).max(128).optional(),
    oldest: z
      .array(
        z.object({
          signature: z.string().min(1).max(128),
          slot: z.number().int().nonnegative(),
          err: z.null(),
        })
      )
      .max(16),
    complete: z.boolean(),
    nextCandidate: z.number().int().min(0).max(16),
  })
  .strict();

export class PostgresCreationHistoryProgress implements CreationHistoryProgressStore {
  constructor(
    private readonly db: Executor,
    private readonly programAddress: string
  ) {}
  async get(mint: string): Promise<Result<CreationHistoryProgress | null, Error>> {
    const rows = await this.db.query<{ progress: unknown }>(
      'SELECT progress FROM solana_creation_searches WHERE program_address=$1 AND mint=$2',
      [this.programAddress, mint]
    );
    if (rows.kind === 'failure') return rows;
    if (rows.value.length === 0) return success(null);
    const parsed = SearchProgress.safeParse(rows.value[0]!.progress);
    if (!parsed.success) return failure(new Error('Invalid persisted creation search progress'));
    const { beforeSignature, ...progress } = parsed.data;
    return success({ ...progress, ...(beforeSignature === undefined ? {} : { beforeSignature }) });
  }
  async put(mint: string, progress: CreationHistoryProgress): Promise<Result<void, Error>> {
    const parsed = SearchProgress.safeParse(progress);
    if (!parsed.success) return failure(new Error('Invalid creation search checkpoint'));
    const saved = await this.db.query(
      `INSERT INTO solana_creation_searches(program_address,mint,progress) VALUES($1,$2,$3::jsonb)
      ON CONFLICT(program_address,mint) DO UPDATE SET progress=EXCLUDED.progress,updated_at=NOW()`,
      [this.programAddress, mint, JSON.stringify(parsed.data)]
    );
    return saved.kind === 'failure' ? saved : success(undefined);
  }
}

const RecoveryPayload = z
  .object({
    programAddress: z.string().regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/),
    signature: z.string().min(1).max(128),
    slot: z.number().int().nonnegative(),
  })
  .strict();
export type MarketRecoveryPayload = z.infer<typeof RecoveryPayload>;
export interface MarketRecoveryQueue {
  enqueue<T>(request: EnqueueRequest<T>): Promise<Result<unknown, Error>>;
}

export async function enqueueMarketRecovery(
  queue: MarketRecoveryQueue,
  payload: MarketRecoveryPayload
): Promise<Result<unknown, Error>> {
  return queue.enqueue({
    queueName: QueueName.TOKEN_MARKET_RECOVERY,
    payload,
    payloadVersion: '1',
    idempotencyKey: `solana-market-recovery:${payload.programAddress}:${payload.signature}`,
    maxAttempts: 10,
    availableAt: new Date(Date.now() + 1000),
  });
}

/** Failed work is durable before live intake is told it can continue. */
export class RecoverableConfirmedIngestor implements ConfirmedNotificationIngestor {
  constructor(
    private readonly base: Required<ConfirmedNotificationIngestor>,
    private readonly queue: MarketRecoveryQueue,
    private readonly programAddress: string
  ) {}
  private async defer(signature: string, slot: number): Promise<Result<never, Error>> {
    const saved = await enqueueMarketRecovery(this.queue, {
      programAddress: this.programAddress,
      signature,
      slot,
    });
    return saved.kind === 'failure'
      ? failure(new Error('Failed to persist confirmed recovery job'))
      : failure(new DeferredConfirmedIngestionError());
  }
  async prepare(
    signature: string,
    slot: number
  ): Promise<Result<PreparedConfirmedIngestion, Error>> {
    let prepared: Result<PreparedConfirmedIngestion, Error>;
    try {
      prepared = await this.base.prepare(signature, slot);
    } catch {
      return this.defer(signature, slot);
    }
    if (prepared.kind === 'failure') return this.defer(signature, slot);
    const original = prepared.value;
    const apply: PreparedConfirmedIngestion = async () => {
      try {
        const result = await original();
        return result.kind === 'failure' ? this.defer(signature, slot) : result;
      } catch {
        return this.defer(signature, slot);
      }
    };
    if (original.keys !== undefined) apply.keys = original.keys;
    return success(apply);
  }
  async ingest(signature: string, slot: number): Promise<Result<ConfirmedIngestionSummary, Error>> {
    const prepared = await this.prepare(signature, slot);
    return prepared.kind === 'failure' ? prepared : prepared.value();
  }
}

/** Requires the original creation event; a pool/mint sighting alone is insufficient. */
export class MissingTokenCreationRecovery {
  constructor(
    private readonly db: Executor,
    private readonly source: Pick<SolanaRpcReconciliationSource, 'findCreation'>,
    private readonly creations: Pick<SolanaConfirmedOccurrenceIngestor, 'apply'>
  ) {}
  async ensure(occurrences: readonly ReconciledMarketOccurrence[]): Promise<Result<void, Error>> {
    const launches = new Set(
      occurrences.flatMap((o) => (o.kind === 'launch' ? [o.launch.address] : []))
    );
    for (const occurrence of occurrences) {
      if (occurrence.kind !== 'graduation' || launches.has(occurrence.graduation.address)) continue;
      const graduation = occurrence.graduation;
      const found = await this.db.query(
        'SELECT token_id FROM tokens WHERE chain=$1 AND address=$2',
        [Chain.SOLANA, graduation.address]
      );
      if (found.kind === 'failure') return found;
      if (found.value.length > 0) continue;
      const recovered = await this.source.findCreation(
        graduation.address,
        graduation.market.origin.slot
      );
      if (recovered.kind === 'failure') return recovered;
      if (
        recovered.value.kind !== 'launch' ||
        recovered.value.launch.address !== graduation.address ||
        recovered.value.launch.launchTime > graduation.graduatedAt ||
        recovered.value.launch.market.origin.slot > graduation.market.origin.slot
      )
        return failure(new Error('Recovered creation does not precede the target migration'));
      const position = recovered.value.launch.market.origin.position;
      const applied = await this.creations.apply(
        position.transactionSignature,
        recovered.value.launch.market.origin.slot,
        [recovered.value]
      );
      if (applied.kind === 'failure') return applied;
    }
    return success(undefined);
  }
}

/** Uses the existing leased/backoff/dead-letter queue; retries never recurse into enqueue. */
export class SolanaMarketRecoveryHandler implements JobHandler {
  readonly queueName = QueueName.TOKEN_MARKET_RECOVERY;
  constructor(private readonly programs: ReadonlyMap<string, ConfirmedNotificationIngestor>) {}
  async handle(job: Job): Promise<void> {
    const parsed = RecoveryPayload.safeParse(job.payload);
    if (job.payloadVersion !== '1' || !parsed.success)
      throw new PermanentJobFailure('Invalid market recovery payload');
    const payload = parsed.data;
    const program = this.programs.get(payload.programAddress);
    if (program === undefined)
      throw new PermanentJobFailure('Market recovery program is not configured');
    const result = await program.ingest(payload.signature, payload.slot);
    if (result.kind === 'failure') throw result.error;
  }
}

/** Restart-safe repair for migration evidence stored before recovery jobs existed. */
export async function seedStoredMigrationRecovery(
  db: Executor,
  queue: MarketRecoveryQueue
): Promise<number> {
  const rows = await db.query<{
    program_address: string;
    transaction_signature: string;
    slot: string;
  }>(
    `SELECT DISTINCT o.program_address,o.transaction_signature,o.slot FROM solana_market_occurrences o
     WHERE o.chain=$1 AND o.launch_source=$2 AND o.event_kind='graduation' AND NOT EXISTS (
       SELECT 1 FROM tokens t JOIN token_markets m ON m.token_id=t.token_id
       WHERE t.chain=o.chain AND t.address=o.raw_payload->>'mint'
         AND m.venue='pumpswap' AND m.market_address=o.raw_payload->>'pool')
     ORDER BY o.slot ASC LIMIT 100`,
    [Chain.SOLANA, LaunchSource.PUMPSWAP]
  );
  if (rows.kind === 'failure') throw rows.error;
  for (const row of rows.value) {
    const payload = RecoveryPayload.parse({
      programAddress: row.program_address,
      signature: row.transaction_signature,
      slot: Number(row.slot),
    });
    const saved = await enqueueMarketRecovery(queue, payload);
    if (saved.kind === 'failure') throw saved.error;
  }
  return rows.value.length;
}
