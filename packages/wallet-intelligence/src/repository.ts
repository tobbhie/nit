import { DatabasePool } from '@nit/database-public';
import { Chain, EvidenceId, Result, failure, success } from '@nit/domain';
import {
  WalletFundingEvent,
  WalletRelationEvidence,
  WalletRelationKind,
  WalletSwap,
  WalletSwapSide,
} from './contracts';

export interface InsertOutcome<T> {
  readonly value: T;
  readonly inserted: boolean;
}

export interface WalletIntelligenceRepository {
  recordSwap(swap: WalletSwap): Promise<Result<InsertOutcome<WalletSwap>, Error>>;
  recordFundingEvent(
    event: WalletFundingEvent
  ): Promise<Result<InsertOutcome<WalletFundingEvent>, Error>>;
  recordRelation(
    evidence: WalletRelationEvidence
  ): Promise<Result<InsertOutcome<WalletRelationEvidence>, Error>>;
  listSwaps(
    tokenMint: string,
    side: WalletSwapSide,
    from: Date,
    to: Date
  ): Promise<Result<WalletSwap[], Error>>;
  listSameSlotSwaps(
    tokenMint: string,
    side: WalletSwapSide,
    slot: number
  ): Promise<Result<WalletSwap[], Error>>;
  listRelations(
    walletAddresses: readonly string[],
    from: Date,
    to: Date
  ): Promise<Result<WalletRelationEvidence[], Error>>;
  listWalletsFundedBy(funderAddress: string, before: Date): Promise<Result<string[], Error>>;
  getCursor(walletAddress: string): Promise<Result<number | null, Error>>;
  advanceCursor(walletAddress: string, finalizedSlot: number): Promise<Result<void, Error>>;
}

interface SwapRow {
  wallet_swap_id: string;
  transaction_signature: string;
  slot: string;
  wallet_address: string;
  side: WalletSwapSide;
  token_mint: string;
  token_raw_amount: string;
  token_decimals: number;
  quote_asset: string;
  quote_raw_amount: string;
  quote_decimals: number;
  program_addresses: string[];
  attribution: Record<string, unknown>;
  occurred_at: Date;
  observed_at: Date;
  available_at: Date;
}

interface FundingRow {
  wallet_funding_event_id: string;
  transaction_signature: string;
  instruction_path: string;
  slot: string;
  funder_address: string;
  funded_wallet_address: string;
  lamports: string;
  occurred_at: Date;
  observed_at: Date;
}

interface RelationRow {
  wallet_relation_evidence_id: string;
  wallet_a: string;
  wallet_b: string;
  relation_kind: WalletRelationKind;
  source_reference: string;
  occurred_at: Date;
  observed_at: Date;
  details: Record<string, unknown>;
}

export class PostgresWalletIntelligenceRepository implements WalletIntelligenceRepository {
  constructor(private readonly db: DatabasePool) {}

  async recordSwap(swap: WalletSwap): Promise<Result<InsertOutcome<WalletSwap>, Error>> {
    const inserted = await this.db.query<SwapRow>(
      `INSERT INTO wallet_swaps (
         wallet_swap_id, chain, transaction_signature, slot, wallet_address,
         side, token_mint, token_raw_amount, token_decimals, quote_asset,
         quote_raw_amount, quote_decimals, program_addresses, attribution,
         occurred_at, observed_at, available_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
       ON CONFLICT (chain, transaction_signature, wallet_address) DO NOTHING
       RETURNING *`,
      [
        swap.walletSwapId,
        swap.chain,
        swap.transactionSignature,
        swap.slot,
        swap.walletAddress,
        swap.side,
        swap.tokenMint,
        swap.tokenRawAmount,
        swap.tokenDecimals,
        swap.quoteAsset,
        swap.quoteRawAmount,
        swap.quoteDecimals,
        swap.programAddresses,
        JSON.stringify(swap.attribution),
        swap.occurredAt,
        swap.observedAt,
        swap.availableAt,
      ]
    );
    if (inserted.kind === 'failure') return inserted;
    const row = inserted.value[0];
    if (row !== undefined) return success({ value: rowToSwap(row), inserted: true });
    const existing = await this.db.query<SwapRow>(
      `SELECT * FROM wallet_swaps
        WHERE chain = $1 AND transaction_signature = $2 AND wallet_address = $3`,
      [swap.chain, swap.transactionSignature, swap.walletAddress]
    );
    if (existing.kind === 'failure') return existing;
    const duplicate = existing.value[0];
    return duplicate === undefined
      ? failure(new Error('Wallet swap conflict did not resolve to an existing row'))
      : success({ value: rowToSwap(duplicate), inserted: false });
  }

  async recordFundingEvent(
    event: WalletFundingEvent
  ): Promise<Result<InsertOutcome<WalletFundingEvent>, Error>> {
    const inserted = await this.db.query<FundingRow>(
      `INSERT INTO wallet_funding_events (
         wallet_funding_event_id, chain, transaction_signature, instruction_path,
         slot, funder_address, funded_wallet_address, lamports, occurred_at, observed_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (chain, transaction_signature, instruction_path, funded_wallet_address) DO NOTHING
       RETURNING *`,
      [
        event.walletFundingEventId,
        event.chain,
        event.transactionSignature,
        event.instructionPath,
        event.slot,
        event.funderAddress,
        event.fundedWalletAddress,
        event.lamports,
        event.occurredAt,
        event.observedAt,
      ]
    );
    if (inserted.kind === 'failure') return inserted;
    const row = inserted.value[0];
    if (row !== undefined) return success({ value: rowToFunding(row), inserted: true });
    const existing = await this.db.query<FundingRow>(
      `SELECT * FROM wallet_funding_events
        WHERE chain = $1 AND transaction_signature = $2
          AND instruction_path = $3 AND funded_wallet_address = $4`,
      [event.chain, event.transactionSignature, event.instructionPath, event.fundedWalletAddress]
    );
    if (existing.kind === 'failure') return existing;
    const duplicate = existing.value[0];
    return duplicate === undefined
      ? failure(new Error('Wallet funding conflict did not resolve to an existing row'))
      : success({ value: rowToFunding(duplicate), inserted: false });
  }

  async recordRelation(
    evidence: WalletRelationEvidence
  ): Promise<Result<InsertOutcome<WalletRelationEvidence>, Error>> {
    const inserted = await this.db.query<RelationRow>(
      `INSERT INTO wallet_relation_evidence (
         wallet_relation_evidence_id, wallet_a, wallet_b, relation_kind,
         source_reference, occurred_at, observed_at, details
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (wallet_a, wallet_b, relation_kind, source_reference) DO NOTHING
       RETURNING *`,
      [
        evidence.evidenceId,
        evidence.walletA,
        evidence.walletB,
        evidence.kind,
        evidence.sourceReference,
        evidence.occurredAt,
        evidence.observedAt,
        JSON.stringify(evidence.details),
      ]
    );
    if (inserted.kind === 'failure') return inserted;
    const row = inserted.value[0];
    if (row !== undefined) return success({ value: rowToRelation(row), inserted: true });
    const existing = await this.db.query<RelationRow>(
      `SELECT * FROM wallet_relation_evidence
        WHERE wallet_a = $1 AND wallet_b = $2 AND relation_kind = $3 AND source_reference = $4`,
      [evidence.walletA, evidence.walletB, evidence.kind, evidence.sourceReference]
    );
    if (existing.kind === 'failure') return existing;
    const duplicate = existing.value[0];
    return duplicate === undefined
      ? failure(new Error('Wallet relation conflict did not resolve to an existing row'))
      : success({ value: rowToRelation(duplicate), inserted: false });
  }

  async listSwaps(
    tokenMint: string,
    side: WalletSwapSide,
    from: Date,
    to: Date
  ): Promise<Result<WalletSwap[], Error>> {
    const result = await this.db.query<SwapRow>(
      `SELECT * FROM wallet_swaps
        WHERE token_mint = $1 AND side = $2 AND occurred_at >= $3 AND occurred_at <= $4
        ORDER BY occurred_at ASC, wallet_swap_id ASC`,
      [tokenMint, side, from, to]
    );
    return result.kind === 'failure' ? result : success(result.value.map(rowToSwap));
  }

  async listSameSlotSwaps(
    tokenMint: string,
    side: WalletSwapSide,
    slot: number
  ): Promise<Result<WalletSwap[], Error>> {
    const result = await this.db.query<SwapRow>(
      `SELECT * FROM wallet_swaps
        WHERE token_mint = $1 AND side = $2 AND slot = $3
        ORDER BY wallet_address ASC`,
      [tokenMint, side, slot]
    );
    return result.kind === 'failure' ? result : success(result.value.map(rowToSwap));
  }

  async listRelations(
    walletAddresses: readonly string[],
    from: Date,
    to: Date
  ): Promise<Result<WalletRelationEvidence[], Error>> {
    if (walletAddresses.length < 2) return success([]);
    const result = await this.db.query<RelationRow>(
      `SELECT * FROM wallet_relation_evidence
        WHERE wallet_a = ANY($1::text[]) AND wallet_b = ANY($1::text[])
          AND occurred_at <= $3
          AND (relation_kind = 'shared_funder' OR occurred_at >= $2)
        ORDER BY occurred_at ASC, wallet_relation_evidence_id ASC`,
      [walletAddresses, from, to]
    );
    return result.kind === 'failure' ? result : success(result.value.map(rowToRelation));
  }

  async listWalletsFundedBy(funderAddress: string, before: Date): Promise<Result<string[], Error>> {
    const result = await this.db.query<{ funded_wallet_address: string }>(
      `SELECT DISTINCT funded_wallet_address FROM wallet_funding_events
        WHERE funder_address = $1 AND occurred_at <= $2
        ORDER BY funded_wallet_address ASC`,
      [funderAddress, before]
    );
    return result.kind === 'failure'
      ? result
      : success(result.value.map((row) => row.funded_wallet_address));
  }

  async getCursor(walletAddress: string): Promise<Result<number | null, Error>> {
    const result = await this.db.query<{ finalized_slot: string }>(
      'SELECT finalized_slot FROM wallet_reconciliation_cursors WHERE wallet_address = $1',
      [walletAddress]
    );
    if (result.kind === 'failure') return result;
    const slot = result.value[0]?.finalized_slot;
    return success(slot === undefined ? null : Number(slot));
  }

  async advanceCursor(walletAddress: string, finalizedSlot: number): Promise<Result<void, Error>> {
    const result = await this.db.query(
      `INSERT INTO wallet_reconciliation_cursors (wallet_address, finalized_slot)
       VALUES ($1, $2)
       ON CONFLICT (wallet_address) DO UPDATE
         SET finalized_slot = EXCLUDED.finalized_slot, updated_at = NOW()`,
      [walletAddress, finalizedSlot]
    );
    return result.kind === 'failure' ? result : success(undefined);
  }
}

function rowToSwap(row: SwapRow): WalletSwap {
  return {
    walletSwapId: row.wallet_swap_id as EvidenceId,
    chain: Chain.SOLANA,
    transactionSignature: row.transaction_signature,
    slot: Number(row.slot),
    walletAddress: row.wallet_address,
    side: row.side,
    tokenMint: row.token_mint,
    tokenRawAmount: row.token_raw_amount,
    tokenDecimals: row.token_decimals,
    quoteAsset: row.quote_asset,
    quoteRawAmount: row.quote_raw_amount,
    quoteDecimals: row.quote_decimals,
    programAddresses: row.program_addresses,
    attribution: row.attribution,
    occurredAt: row.occurred_at,
    observedAt: row.observed_at,
    availableAt: row.available_at,
  };
}

function rowToFunding(row: FundingRow): WalletFundingEvent {
  return {
    walletFundingEventId: row.wallet_funding_event_id as EvidenceId,
    chain: Chain.SOLANA,
    transactionSignature: row.transaction_signature,
    instructionPath: row.instruction_path,
    slot: Number(row.slot),
    funderAddress: row.funder_address,
    fundedWalletAddress: row.funded_wallet_address,
    lamports: row.lamports,
    occurredAt: row.occurred_at,
    observedAt: row.observed_at,
  };
}

function rowToRelation(row: RelationRow): WalletRelationEvidence {
  return {
    evidenceId: row.wallet_relation_evidence_id as EvidenceId,
    walletA: row.wallet_a,
    walletB: row.wallet_b,
    kind: row.relation_kind,
    sourceReference: row.source_reference,
    occurredAt: row.occurred_at,
    observedAt: row.observed_at,
    details: row.details,
  };
}
