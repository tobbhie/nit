import { Chain, LaunchSource, Result, failure, success } from '@nit/domain';
import { Executor } from '@nit/database-public';
import { ProviderIdentity } from '@nit/provider-interfaces';
import { SourceTokenLaunch, SourceTokenMarket } from './contracts';
import { DiscoveryBatchOutcome, LaunchOrigin } from './service';

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

export interface SolanaReconciliationPageRequest {
  fromSlotInclusive: number;
  throughFinalizedSlot: number;
  beforeSignature?: string;
  limit: number;
}

export interface SolanaReconciliationPage {
  launches: readonly ReconciledLaunch[];
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
      fetched += page.value.launches.length;

      for (const launch of page.value.launches) {
        const slot = launch.market?.origin?.slot;
        if (
          launch.market === undefined ||
          launch.market.origin.kind !== 'chain' ||
          !isSlot(slot) ||
          slot < fromSlotInclusive ||
          slot > head.value
        ) {
          return failure(
            new Error(
              `Solana reconciliation source returned a launch without an in-range chain occurrence`
            )
          );
        }
      }

      const batch = await this.discovery.discoverBatch(
        { chain: this.source.chain, launchSource: this.source.launchSource },
        page.value.launches
      );
      if (batch.failures.length > 0) {
        return failure(
          new Error(
            `Reconciliation failed to ingest ${batch.failures.length} launch occurrence(s): ${batch.failures[0]?.error.message ?? 'unknown error'}`
          )
        );
      }
      discovered += batch.discovered.length;
      graduated += batch.graduated.length;
      duplicates += batch.duplicates.length;

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

function isSlot(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function nonnegative(value: number | undefined, fallback: number): number {
  return value === undefined || !Number.isSafeInteger(value) || value < 0 ? fallback : value;
}

function positive(value: number | undefined, fallback: number): number {
  return value === undefined || !Number.isSafeInteger(value) || value <= 0 ? fallback : value;
}
