import {
  AggregateId,
  AggregateType,
  Chain,
  EventType,
  EvidenceId,
  Result,
  SignalId,
  failure,
  generateEventId,
  success,
} from '@nit/domain';
import { SignalEnvelope, SignalKind, parseSignalEnvelope, signalOriginKey } from '@nit/signals';
import { PostgresEventStore } from './event-store';
import { DatabasePool, Executor, transactResult } from './pool';

interface SignalRow {
  signal_id: string;
  source: string;
  source_version: string;
  kind: string;
  chain: string;
  token_mint: string | null;
  canonical_narrative_id: string | null;
  occurred_at: Date;
  observed_at: Date;
  available_at: Date;
  origin_kind: 'provider' | 'chain';
  provider: string | null;
  reference_id: string | null;
  transaction_signature: string | null;
  instruction_index: number | null;
  log_index: number | null;
  transaction_level: boolean;
  actor_address: string | null;
  evidence_ids: string[];
  payload: unknown;
}

export interface SignalWriteResult {
  readonly signalId: SignalId;
  readonly inserted: boolean;
}

function rowToSignal<TPayload>(row: SignalRow): SignalEnvelope<TPayload> {
  const origin =
    row.origin_kind === 'provider'
      ? {
          kind: 'provider' as const,
          provider: row.provider as string,
          referenceId: row.reference_id as string,
        }
      : {
          kind: 'chain' as const,
          chain: row.chain as Chain,
          transactionSignature: row.transaction_signature as string,
          ...(row.instruction_index === null
            ? row.log_index === null
              ? { transactionLevel: true as const, actorAddress: row.actor_address as string }
              : { logIndex: row.log_index }
            : { instructionIndex: row.instruction_index }),
        };

  return parseSignalEnvelope<TPayload>({
    eventId: row.signal_id,
    source: row.source,
    sourceVersion: row.source_version,
    kind: row.kind as SignalKind,
    chain: row.chain as Chain,
    ...(row.token_mint === null ? {} : { tokenMint: row.token_mint }),
    ...(row.canonical_narrative_id === null
      ? {}
      : { canonicalNarrativeId: row.canonical_narrative_id }),
    occurredAt: row.occurred_at,
    observedAt: row.observed_at,
    availableAt: row.available_at,
    origin,
    evidenceIds: row.evidence_ids as EvidenceId[],
    payload: row.payload,
  });
}

/**
 * Persists a normalized signal and its domain event in one transaction.
 * Re-delivery of the same source occurrence returns the original signal id and
 * does not append a second event.
 */
export class PostgresSignalStore {
  constructor(private readonly db: DatabasePool) {}

  async append<TPayload>(
    candidate: SignalEnvelope<TPayload>
  ): Promise<Result<SignalWriteResult, Error>> {
    let signal: SignalEnvelope<TPayload>;
    try {
      signal = parseSignalEnvelope<TPayload>(candidate);
    } catch (error) {
      return failure(error as Error);
    }

    return transactResult(this.db, (executor) => this.appendInTransaction(executor, signal));
  }

  /** Enlists in a caller-owned transaction; the caller must roll back on failure. */
  async appendInTransaction<TPayload>(
    executor: Executor,
    candidate: SignalEnvelope<TPayload>
  ): Promise<Result<SignalWriteResult, Error>> {
    let signal: SignalEnvelope<TPayload>;
    try {
      signal = parseSignalEnvelope<TPayload>(candidate);
    } catch (error) {
      return failure(error as Error);
    }
    const originKey = signalOriginKey(signal.origin);
    const inserted = await executor.query<{ signal_id: string }>(
      `INSERT INTO signals (
           signal_id, source, source_version, kind, chain, token_mint,
           canonical_narrative_id, occurred_at, observed_at, available_at,
           origin_kind, provider, reference_id, transaction_signature,
           instruction_index, log_index, transaction_level, actor_address,
           origin_key, evidence_ids, payload
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
         )
         ON CONFLICT (origin_key) DO NOTHING
         RETURNING signal_id`,
      [
        signal.eventId,
        signal.source,
        signal.sourceVersion,
        signal.kind,
        signal.chain,
        signal.tokenMint ?? null,
        signal.canonicalNarrativeId ?? null,
        signal.occurredAt,
        signal.observedAt,
        signal.availableAt,
        signal.origin.kind,
        signal.origin.kind === 'provider' ? signal.origin.provider : null,
        signal.origin.kind === 'provider' ? signal.origin.referenceId : null,
        signal.origin.kind === 'chain' ? signal.origin.transactionSignature : null,
        signal.origin.kind === 'chain' ? (signal.origin.instructionIndex ?? null) : null,
        signal.origin.kind === 'chain' ? (signal.origin.logIndex ?? null) : null,
        signal.origin.kind === 'chain' ? (signal.origin.transactionLevel ?? false) : false,
        signal.origin.kind === 'chain' ? (signal.origin.actorAddress ?? null) : null,
        originKey,
        signal.evidenceIds,
        JSON.stringify(signal.payload),
      ]
    );
    if (inserted.kind === 'failure') {
      return failure(inserted.error);
    }

    const insertedId = inserted.value[0]?.signal_id;
    if (insertedId === undefined) {
      return this.findExistingId(executor, originKey);
    }

    const event = await new PostgresEventStore(executor).append({
      eventId: generateEventId(),
      eventType: EventType.SIGNAL_PUBLISHED,
      aggregateType: AggregateType.SIGNAL,
      aggregateId: insertedId as AggregateId,
      version: signal.sourceVersion,
      // Event-log replay must not make the payload visible before it was usable.
      occurredAt: signal.availableAt,
      producer: signal.source,
      payload: signal,
    });
    if (event.kind === 'failure') {
      return failure(event.error);
    }

    return success({ signalId: insertedId as SignalId, inserted: true });
  }

  async getById<TPayload = unknown>(
    signalId: SignalId
  ): Promise<Result<SignalEnvelope<TPayload> | null, Error>> {
    const result = await this.db.query<SignalRow>('SELECT * FROM signals WHERE signal_id = $1', [
      signalId,
    ]);
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    const row = result.value[0];
    return success(row === undefined ? null : rowToSignal<TPayload>(row));
  }

  async listAvailable<TPayload = unknown>(
    from: Date,
    to: Date
  ): Promise<Result<SignalEnvelope<TPayload>[], Error>> {
    const result = await this.db.query<SignalRow>(
      `SELECT * FROM signals
        WHERE available_at >= $1 AND available_at <= $2
        ORDER BY available_at ASC, signal_id ASC`,
      [from, to]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    return success(result.value.map((row) => rowToSignal<TPayload>(row)));
  }

  private async findExistingId(
    executor: Executor,
    originKey: string
  ): Promise<Result<SignalWriteResult, Error>> {
    const existing = await executor.query<{ signal_id: string }>(
      'SELECT signal_id FROM signals WHERE origin_key = $1',
      [originKey]
    );
    if (existing.kind === 'failure') {
      return failure(existing.error);
    }
    const signalId = existing.value[0]?.signal_id;
    if (signalId === undefined) {
      return failure(new Error('Signal deduplication conflict did not resolve to a stored signal'));
    }
    return success({ signalId: signalId as SignalId, inserted: false });
  }
}
