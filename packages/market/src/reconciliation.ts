import { Chain, LaunchSource, Result, failure, success } from '@nit/domain';
import { Executor } from '@nit/database-public';
import { ProviderIdentity } from '@nit/provider-interfaces';
import { SourceTokenGraduation, SourceTokenLaunch, SourceTokenMarket } from './contracts';
import { DiscoveryBatchOutcome, LaunchOrigin } from './service';
import { ChainEventOrigin, chainEventKey } from './lifecycle';

export interface SolanaReconciliationCursor {
  chain: Chain.SOLANA;
  launchSource: LaunchSource;
  finalizedSlot: number;
  updatedAt: Date;
}

export interface SolanaReconciliationCursorRepository {
  get(
    chain: Chain.SOLANA,
    launchSource: LaunchSource
  ): Promise<Result<SolanaReconciliationCursor | null, Error>>;
  advance(
    chain: Chain.SOLANA,
    launchSource: LaunchSource,
    finalizedSlot: number
  ): Promise<Result<SolanaReconciliationCursor, Error>>;
}

interface CursorRow {
  chain: string;
  launch_source: string;
  finalized_slot: string;
  updated_at: Date;
}

export class PostgresSolanaReconciliationCursorRepository implements SolanaReconciliationCursorRepository {
  constructor(private readonly exec: Executor) {}

  async get(
    chain: Chain.SOLANA,
    launchSource: LaunchSource
  ): Promise<Result<SolanaReconciliationCursor | null, Error>> {
    const found = await this.exec.query<CursorRow>(
      `SELECT chain, launch_source, finalized_slot, updated_at
         FROM solana_reconciliation_cursors
        WHERE chain = $1 AND launch_source = $2`,
      [chain, launchSource]
    );
    if (found.kind === 'failure') return failure(found.error);
    return success(found.value[0] === undefined ? null : toCursor(found.value[0]));
  }

  async advance(
    chain: Chain.SOLANA,
    launchSource: LaunchSource,
    finalizedSlot: number
  ): Promise<Result<SolanaReconciliationCursor, Error>> {
    if (!isSlot(finalizedSlot)) {
      return failure(new Error(`Invalid finalized Solana slot '${String(finalizedSlot)}'`));
    }
    const advanced = await this.exec.query<CursorRow>(
      `INSERT INTO solana_reconciliation_cursors (
         chain, launch_source, finalized_slot
       ) VALUES ($1, $2, $3)
       ON CONFLICT (chain, launch_source) DO UPDATE
         SET finalized_slot = GREATEST(
               solana_reconciliation_cursors.finalized_slot,
               EXCLUDED.finalized_slot
             ),
             updated_at = CASE
               WHEN EXCLUDED.finalized_slot > solana_reconciliation_cursors.finalized_slot
               THEN NOW()
               ELSE solana_reconciliation_cursors.updated_at
             END
       RETURNING chain, launch_source, finalized_slot, updated_at`,
      [chain, launchSource, finalizedSlot]
    );
    if (advanced.kind === 'failure') return failure(advanced.error);
    const row = advanced.value[0];
    return row === undefined
      ? failure(new Error(`Solana reconciliation cursor did not return after advance`))
      : success(toCursor(row));
  }
}

function toCursor(row: CursorRow): SolanaReconciliationCursor {
  return {
    chain: row.chain as Chain.SOLANA,
    launchSource: row.launch_source as LaunchSource,
    finalizedSlot: Number(row.finalized_slot),
    updatedAt: row.updated_at,
  };
}

export type ReconciledLaunch = SourceTokenLaunch & { market: SourceTokenMarket };
export type ReconciledGraduation = SourceTokenGraduation & { market: SourceTokenMarket };

export type DecodedMarketOccurrence =
  | { kind: 'launch'; launch: ReconciledLaunch }
  | { kind: 'graduation'; graduation: ReconciledGraduation };

export type ReconciledMarketOccurrence = DecodedMarketOccurrence & {
  programAddress: string;
  observedAt: Date;
};

export interface SolanaReconciliationPageRequest {
  fromSlotInclusive: number;
  throughFinalizedSlot: number;
  beforeSignature?: string;
  limit: number;
}

export interface SolanaReconciliationPage {
  occurrences: readonly ReconciledMarketOccurrence[];
  /** True once pagination has crossed the requested slot floor. */
  reachedFloor: boolean;
  /** Required when more pages remain; passed as `before` on the next RPC request. */
  nextBeforeSignature?: string;
}

export interface SolanaReconciliationSource extends ProviderIdentity {
  readonly chain: Chain.SOLANA;
  readonly launchSource: LaunchSource;
  getFinalizedSlot(): Promise<Result<number, Error>>;
  fetchReconciliationPage(
    request: SolanaReconciliationPageRequest
  ): Promise<Result<SolanaReconciliationPage, Error>>;
}

export interface ReconciliationDiscovery {
  discoverBatch(
    origin: LaunchOrigin,
    launches: readonly SourceTokenLaunch[]
  ): Promise<DiscoveryBatchOutcome>;
  graduateBatch(
    origin: LaunchOrigin,
    graduations: readonly SourceTokenGraduation[]
  ): Promise<DiscoveryBatchOutcome>;
}

export interface SolanaMarketOccurrenceRepository {
  record(
    chain: Chain.SOLANA,
    launchSource: LaunchSource,
    occurrence: ReconciledMarketOccurrence
  ): Promise<Result<boolean, Error>>;
}

interface OccurrenceRow {
  source_event_key: string;
  chain: string;
  launch_source: string;
  event_kind: string;
  program_address: string;
  transaction_signature: string;
  instruction_index: number | null;
  inner_instruction_index: number | null;
  log_index: number | null;
  slot: string;
  block_time: Date;
  raw_payload: Record<string, unknown>;
  observed_at: Date;
}

export class PostgresSolanaMarketOccurrenceRepository implements SolanaMarketOccurrenceRepository {
  constructor(private readonly exec: Executor) {}

  async record(
    chain: Chain.SOLANA,
    launchSource: LaunchSource,
    occurrence: ReconciledMarketOccurrence
  ): Promise<Result<boolean, Error>> {
    const fact = occurrenceFact(occurrence);
    const position = fact.origin.position;
    const key = chainEventKey(position);
    const inserted = await this.exec.query<{ source_event_key: string }>(
      `INSERT INTO solana_market_occurrences (
         source_event_key, chain, launch_source, event_kind, program_address,
         transaction_signature, instruction_index, inner_instruction_index,
         log_index, slot, block_time, raw_payload, observed_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       ON CONFLICT (source_event_key) DO NOTHING
       RETURNING source_event_key`,
      [
        key,
        chain,
        launchSource,
        occurrence.kind,
        occurrence.programAddress,
        position.transactionSignature,
        position.instructionIndex ?? null,
        'innerInstructionIndex' in position ? (position.innerInstructionIndex ?? null) : null,
        position.logIndex ?? null,
        fact.origin.slot,
        fact.at,
        JSON.stringify(fact.raw),
        occurrence.observedAt,
      ]
    );
    if (inserted.kind === 'failure') return failure(inserted.error);
    if (inserted.value.length > 0) return success(true);

    const existing = await this.exec.query<OccurrenceRow>(
      `SELECT source_event_key, chain, launch_source, event_kind, program_address,
              transaction_signature, instruction_index, inner_instruction_index,
              log_index, slot, block_time, raw_payload, observed_at
         FROM solana_market_occurrences
        WHERE source_event_key = $1`,
      [key]
    );
    if (existing.kind === 'failure') return failure(existing.error);
    const row = existing.value[0];
    if (row === undefined || !sameOccurrence(row, chain, launchSource, occurrence)) {
      return failure(new Error(`Solana occurrence identity collision for '${key}'`));
    }
    return success(false);
  }
}

export interface SolanaReconciliationOptions {
  overlapSlots?: number;
  initialLookbackSlots?: number;
  pageSize?: number;
}

export interface SolanaReconciliationSummary {
  chain: Chain.SOLANA;
  launchSource: LaunchSource;
  provider: string;
  fromSlotInclusive: number;
  throughFinalizedSlot: number;
  pages: number;
  fetched: number;
  discovered: number;
  graduated: number;
  duplicates: number;
}

const DEFAULT_OVERLAP_SLOTS = 250;
const DEFAULT_INITIAL_LOOKBACK_SLOTS = 2_000;
const DEFAULT_PAGE_SIZE = 1_000;

export class SolanaLaunchReconciler {
  constructor(
    private readonly source: SolanaReconciliationSource,
    private readonly cursors: SolanaReconciliationCursorRepository,
    private readonly occurrences: SolanaMarketOccurrenceRepository,
    private readonly discovery: ReconciliationDiscovery,
    private readonly options: SolanaReconciliationOptions = {}
  ) {}

  async reconcile(): Promise<Result<SolanaReconciliationSummary, Error>> {
    const head = await this.source.getFinalizedSlot();
    if (head.kind === 'failure') return failure(head.error);
    if (!isSlot(head.value)) {
      return failure(new Error(`Provider returned invalid finalized slot '${String(head.value)}'`));
    }

    const stored = await this.cursors.get(this.source.chain, this.source.launchSource);
    if (stored.kind === 'failure') return failure(stored.error);
    if (stored.value !== null && head.value < stored.value.finalizedSlot) {
      return failure(
        new Error(
          `Finalized head ${head.value} is behind stored cursor ${stored.value.finalizedSlot}`
        )
      );
    }

    const overlap = nonnegative(this.options.overlapSlots, DEFAULT_OVERLAP_SLOTS);
    const initial = nonnegative(this.options.initialLookbackSlots, DEFAULT_INITIAL_LOOKBACK_SLOTS);
    const pageSize = positive(this.options.pageSize, DEFAULT_PAGE_SIZE);
    const fromSlotInclusive = Math.max(
      0,
      stored.value === null ? head.value - initial : stored.value.finalizedSlot - overlap
    );

    let beforeSignature: string | undefined;
    let pages = 0;
    let fetched = 0;
    let discovered = 0;
    let graduated = 0;
    let duplicates = 0;

    for (;;) {
      const pageRequest: SolanaReconciliationPageRequest = {
        fromSlotInclusive,
        throughFinalizedSlot: head.value,
        limit: pageSize,
        ...(beforeSignature === undefined ? {} : { beforeSignature }),
      };
      const page = await this.source.fetchReconciliationPage(pageRequest);
      if (page.kind === 'failure') return failure(page.error);
      pages += 1;
      fetched += page.value.occurrences.length;

      for (const occurrence of page.value.occurrences) {
        let fact: ReturnType<typeof occurrenceFact>;
        try {
          fact = occurrenceFact(occurrence);
        } catch {
          return failure(new Error(`Solana reconciliation source returned a malformed occurrence`));
        }
        const slot = fact.origin.slot;
        if (
          !isSlot(slot) ||
          slot < fromSlotInclusive ||
          slot > head.value ||
          occurrence.programAddress.trim() === '' ||
          !Number.isFinite(occurrence.observedAt.getTime()) ||
          (this.source.launchSource === LaunchSource.PUMP_FUN && occurrence.kind !== 'launch') ||
          (this.source.launchSource === LaunchSource.PUMPSWAP && occurrence.kind !== 'graduation')
        ) {
          return failure(
            new Error(
              `Solana reconciliation source returned an invalid or mismatched market occurrence`
            )
          );
        }
        const recorded = await this.occurrences.record(
          this.source.chain,
          this.source.launchSource,
          occurrence
        );
        if (recorded.kind === 'failure') return failure(recorded.error);
      }

      const launchBatch = await this.discovery.discoverBatch(
        { chain: this.source.chain, launchSource: this.source.launchSource },
        page.value.occurrences.flatMap((occurrence) =>
          occurrence.kind === 'launch' ? [occurrence.launch] : []
        )
      );
      const graduationBatch = await this.discovery.graduateBatch(
        { chain: this.source.chain, launchSource: this.source.launchSource },
        page.value.occurrences.flatMap((occurrence) =>
          occurrence.kind === 'graduation' ? [occurrence.graduation] : []
        )
      );
      const failures = [...launchBatch.failures, ...graduationBatch.failures];
      if (failures.length > 0) {
        return failure(
          new Error(
            `Reconciliation failed to ingest ${failures.length} market occurrence(s): ${failures[0]?.error.message ?? 'unknown error'}`
          )
        );
      }
      discovered += launchBatch.discovered.length;
      graduated += graduationBatch.graduated.length;
      duplicates += launchBatch.duplicates.length + graduationBatch.duplicates.length;

      if (page.value.reachedFloor) break;
      const next = page.value.nextBeforeSignature?.trim();
      if (next === undefined || next === '' || next === beforeSignature) {
        return failure(new Error('Solana reconciliation pagination did not advance'));
      }
      beforeSignature = next;
    }

    const advanced = await this.cursors.advance(
      this.source.chain,
      this.source.launchSource,
      head.value
    );
    if (advanced.kind === 'failure') return failure(advanced.error);

    return success({
      chain: this.source.chain,
      launchSource: this.source.launchSource,
      provider: this.source.providerName,
      fromSlotInclusive,
      throughFinalizedSlot: head.value,
      pages,
      fetched,
      discovered,
      graduated,
      duplicates,
    });
  }
}

function occurrenceFact(occurrence: ReconciledMarketOccurrence): {
  origin: ChainEventOrigin;
  at: Date;
  raw: Record<string, unknown>;
} {
  return occurrence.kind === 'launch'
    ? {
        origin: occurrence.launch.market.origin,
        at: occurrence.launch.launchTime,
        raw: occurrence.launch.raw,
      }
    : {
        origin: occurrence.graduation.market.origin,
        at: occurrence.graduation.graduatedAt,
        raw: occurrence.graduation.raw,
      };
}

function sameOccurrence(
  row: OccurrenceRow,
  chain: Chain.SOLANA,
  launchSource: LaunchSource,
  occurrence: ReconciledMarketOccurrence
): boolean {
  const fact = occurrenceFact(occurrence);
  const position = fact.origin.position;
  return (
    row.chain === String(chain) &&
    row.launch_source === String(launchSource) &&
    row.event_kind === occurrence.kind &&
    row.program_address === occurrence.programAddress &&
    row.transaction_signature === position.transactionSignature &&
    row.instruction_index === (position.instructionIndex ?? null) &&
    row.inner_instruction_index ===
      ('innerInstructionIndex' in position ? (position.innerInstructionIndex ?? null) : null) &&
    row.log_index === (position.logIndex ?? null) &&
    Number(row.slot) === fact.origin.slot &&
    row.block_time.getTime() === fact.at.getTime() &&
    canonicalJson(row.raw_payload) === canonicalJson(fact.raw)
  );
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) =>
      left.localeCompare(right)
    );
    return `{${entries
      .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function isSlot(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function nonnegative(value: number | undefined, fallback: number): number {
  return value === undefined || !Number.isSafeInteger(value) || value < 0 ? fallback : value;
}

function positive(value: number | undefined, fallback: number): number {
  return value === undefined || !Number.isSafeInteger(value) || value <= 0 ? fallback : value;
}
