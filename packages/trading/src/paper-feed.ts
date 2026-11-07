import { DatabasePool } from '@nit/database-public';
import { Result, failure, success } from '@nit/domain';
import { PaperMarketSnapshot, validatePaperMarket } from './paper-market';
import { paperRows } from './paper-store';

export interface PaperExecutionFacts {
  readonly schemaVersion: 1;
  /** Chain-state time, when capture time differs from the underlying account slot. */
  readonly occurredAt?: string;
  readonly marketAddress: string;
  readonly cashAsset: string;
  readonly tokenReserves: string;
  readonly cashReserves: string;
  readonly realTokenReserves: string;
  readonly realCashReserves: string;
  readonly tokenDecimals: number;
  readonly cashDecimals: number;
  readonly verified: boolean;
  readonly tradable: boolean;
  readonly sellable: boolean;
  readonly criticalRisk: boolean;
}

export interface PaperMarketFeed {
  latest(
    tokenMint: string,
    cashAsset: string,
    asOf: Date
  ): Promise<Result<PaperMarketSnapshot, Error>>;
}

/**
 * Reads explicit raw_payload.paperExecution facts from existing market ingestion.
 * USD liquidity, missing booleans and synthetic prices are never treated as reserves.
 */
export class PostgresPaperMarketFeed implements PaperMarketFeed {
  constructor(
    private readonly db: DatabasePool,
    private readonly approvedSources: readonly string[]
  ) {
    if (
      approvedSources.length === 0 ||
      approvedSources.length > 32 ||
      approvedSources.some((source) => source.trim() === '' || /synthetic|toy/i.test(source))
    ) {
      throw new Error('Paper feed requires explicitly approved non-synthetic sources');
    }
  }

  async latest(
    tokenMint: string,
    cashAsset: string,
    asOf: Date
  ): Promise<Result<PaperMarketSnapshot, Error>> {
    try {
      const rows = await paperRows<{
        snapshot_id: string;
        venue: PaperMarketSnapshot['venue'];
        observed_at: Date;
        recorded_at: Date;
        raw_payload: Record<string, unknown>;
        market_address: string | null;
        market_active_from: Date;
      }>(
        this.db,
        `SELECT s.snapshot_id,s.venue,s.observed_at,s.recorded_at,s.raw_payload,v.market_address,v.active_from AS market_active_from
        FROM market_snapshots s JOIN tokens t ON t.token_id=s.token_id
        JOIN token_markets v ON v.token_id=t.token_id AND v.venue=s.venue
        WHERE t.chain='solana' AND t.address=$1 AND s.source=ANY($2::text[])
          AND s.observed_at <= $3 AND s.recorded_at <= $3
          AND v.active_from <= s.observed_at AND v.active_from <= $3
          AND (v.active_until IS NULL OR v.active_until > $3)
        ORDER BY s.observed_at DESC,s.recorded_at DESC,s.snapshot_id DESC LIMIT 1`,
        [tokenMint, this.approvedSources, asOf]
      );
      const row = rows[0];
      if (row === undefined)
        return failure(new Error('No approved market evidence for the current venue'));
      const raw = row.raw_payload['paperExecution'];
      if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
        return failure(new Error('Market evidence lacks explicit paper execution facts'));
      }
      const facts = raw as Record<string, unknown>;
      if (row.market_address === null || facts['marketAddress'] !== row.market_address) {
        return failure(new Error('Execution reserves are not bound to the current market address'));
      }
      if (facts['schemaVersion'] !== 1 || facts['cashAsset'] !== cashAsset) {
        return failure(new Error('Unsupported paper execution schema or cash asset'));
      }
      const occurredAt =
        facts['occurredAt'] === undefined
          ? row.observed_at
          : new Date(requiredText(facts['occurredAt']));
      if (occurredAt > row.observed_at || occurredAt < row.market_active_from)
        return failure(new Error('Execution state time is outside the captured market interval'));
      const snapshot: PaperMarketSnapshot = {
        snapshotId: row.snapshot_id,
        tokenMint,
        venue: row.venue,
        cashAsset,
        tokenReserves: requiredText(facts['tokenReserves']),
        cashReserves: requiredText(facts['cashReserves']),
        realTokenReserves: requiredText(facts['realTokenReserves']),
        realCashReserves: requiredText(facts['realCashReserves']),
        tokenDecimals: requiredNumber(facts['tokenDecimals']),
        cashDecimals: requiredNumber(facts['cashDecimals']),
        verified: requiredBoolean(facts['verified']),
        tradable: requiredBoolean(facts['tradable']),
        sellable: requiredBoolean(facts['sellable']),
        criticalRisk: requiredBoolean(facts['criticalRisk']),
        occurredAt,
        observedAt: row.recorded_at,
        availableAt: row.recorded_at,
      };
      validatePaperMarket(snapshot, asOf);
      return success(snapshot);
    } catch (error) {
      return failure(error instanceof Error ? error : new Error('Paper feed failed'));
    }
  }
}

function requiredText(value: unknown): string {
  if (typeof value !== 'string') throw new Error('Paper execution amount is missing');
  return value;
}
function requiredNumber(value: unknown): number {
  if (typeof value !== 'number') throw new Error('Paper execution decimals are missing');
  return value;
}
function requiredBoolean(value: unknown): boolean {
  if (typeof value !== 'boolean') throw new Error('Paper execution risk fact is unknown');
  return value;
}
