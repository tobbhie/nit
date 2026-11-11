import {
  MarketVenue,
  Result,
  Timestamp,
  TokenId,
  TokenLifecycleState,
  TokenMarketId,
  failure,
  success,
} from '@nit/domain';
import { Executor } from '@nit/database-public';

export type ChainEventPosition =
  | {
      transactionSignature: string;
      instructionIndex: number;
      innerInstructionIndex?: number;
      logIndex?: never;
    }
  | { transactionSignature: string; logIndex: number; instructionIndex?: never };

export interface ChainEventOrigin {
  kind: 'chain';
  position: ChainEventPosition;
  slot: number;
}

export interface LegacyMarketOrigin {
  kind: 'legacy';
  key: string;
}

export type TokenMarketOrigin = ChainEventOrigin | LegacyMarketOrigin;

export interface TokenMarketInterval {
  tokenMarketId: TokenMarketId;
  tokenId: TokenId;
  venue: MarketVenue;
  marketAddress?: string;
  activeFrom: Timestamp;
  activeUntil?: Timestamp;
  origin: TokenMarketOrigin;
  provenance: Record<string, unknown>;
  recordedAt: Timestamp;
}

export interface OpenTokenMarket {
  tokenMarketId: TokenMarketId;
  tokenId: TokenId;
  venue: MarketVenue;
  marketAddress?: string;
  activeFrom: Timestamp;
  origin: TokenMarketOrigin;
  provenance: Record<string, unknown>;
}

export interface TokenMarketRepository {
  open(
    input: OpenTokenMarket
  ): Promise<Result<{ inserted: boolean; tokenMarketId: TokenMarketId }, Error>>;
  transition(
    input: OpenTokenMarket
  ): Promise<Result<{ inserted: boolean; tokenMarketId: TokenMarketId }, Error>>;
  findCurrent(tokenId: TokenId): Promise<Result<TokenMarketInterval | null, Error>>;
  findAt(tokenId: TokenId, at: Timestamp): Promise<Result<TokenMarketInterval | null, Error>>;
  list(tokenId: TokenId): Promise<Result<TokenMarketInterval[], Error>>;
}

interface TokenMarketRow {
  token_market_id: string;
  token_id: string;
  venue: string;
  market_address: string | null;
  active_from: Date;
  active_until: Date | null;
  source_event_kind: 'chain' | 'legacy';
  source_event_key: string;
  transaction_signature: string | null;
  instruction_index: number | null;
  inner_instruction_index: number | null;
  log_index: number | null;
  slot: string | null;
  provenance: Record<string, unknown>;
  recorded_at: Date;
}

const SELECT_MARKET = `
  SELECT token_market_id, token_id, venue, market_address, active_from, active_until,
         source_event_kind, source_event_key, transaction_signature, instruction_index,
         inner_instruction_index, log_index, slot, provenance, recorded_at
    FROM token_markets
`;

export function chainEventKey(position: ChainEventPosition): string {
  const index =
    position.instructionIndex !== undefined
      ? `instruction:${position.instructionIndex}${
          position.innerInstructionIndex === undefined
            ? ''
            : `:inner:${position.innerInstructionIndex}`
        }`
      : `log:${position.logIndex}`;
  return `solana:${position.transactionSignature}:${index}`;
}

function originKey(origin: TokenMarketOrigin): string {
  if (origin.kind === 'legacy') return `legacy:${origin.key}`;
  return chainEventKey(origin.position);
}

function originParams(
  origin: TokenMarketOrigin
): [string, string | null, number | null, number | null, number | null, number | null] {
  if (origin.kind === 'legacy') return ['legacy', null, null, null, null, null];
  return [
    'chain',
    origin.position.transactionSignature,
    origin.position.instructionIndex ?? null,
    'innerInstructionIndex' in origin.position
      ? (origin.position.innerInstructionIndex ?? null)
      : null,
    origin.position.logIndex ?? null,
    origin.slot,
  ];
}

export class PostgresTokenMarketRepository implements TokenMarketRepository {
  constructor(private readonly exec: Executor) {}

  async open(
    input: OpenTokenMarket
  ): Promise<Result<{ inserted: boolean; tokenMarketId: TokenMarketId }, Error>> {
    return this.insert(input);
  }

  async transition(
    input: OpenTokenMarket
  ): Promise<Result<{ inserted: boolean; tokenMarketId: TokenMarketId }, Error>> {
    const duplicate = await this.findByOrigin(input.origin);
    if (duplicate.kind === 'failure') return failure(duplicate.error);
    if (duplicate.value !== null) {
      return success({ inserted: false, tokenMarketId: duplicate.value.tokenMarketId });
    }

    const [kind, signature, instructionIndex, innerInstructionIndex, logIndex, slot] = originParams(
      input.origin
    );
    const key = originKey(input.origin);
    const inserted = await this.exec.query<{ token_market_id: string }>(
      `WITH closed AS (
         UPDATE token_markets
            SET active_until = $5
          WHERE token_id = $2 AND active_until IS NULL AND (
            active_from < $5 OR (active_from = $5 AND source_event_kind='chain' AND $6='chain'
              AND venue='pump_fun_bonding_curve' AND $3='pumpswap' AND slot <= $12
              AND source_event_key <> $7))
          RETURNING token_market_id
       )
       INSERT INTO token_markets (
         token_market_id, token_id, venue, market_address, active_from,
         source_event_kind, source_event_key, transaction_signature,
         instruction_index, inner_instruction_index, log_index, slot, provenance
       )
       SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
         FROM closed
       RETURNING token_market_id`,
      [
        input.tokenMarketId,
        input.tokenId,
        input.venue,
        input.marketAddress ?? null,
        input.activeFrom,
        kind,
        key,
        signature,
        instructionIndex,
        innerInstructionIndex,
        logIndex,
        slot,
        JSON.stringify(input.provenance),
      ]
    );
    if (inserted.kind === 'failure') return failure(inserted.error);
    if (inserted.value.length === 0) {
      return failure(
        new Error(`Token ${input.tokenId} has no earlier active market to transition`)
      );
    }
    return success({ inserted: true, tokenMarketId: input.tokenMarketId });
  }

  findCurrent(tokenId: TokenId): Promise<Result<TokenMarketInterval | null, Error>> {
    return this.findOne(' WHERE token_id = $1 AND active_until IS NULL LIMIT 1', [tokenId]);
  }

  findAt(tokenId: TokenId, at: Timestamp): Promise<Result<TokenMarketInterval | null, Error>> {
    return this.findOne(
      ' WHERE token_id = $1 AND active_from <= $2 AND (active_until IS NULL OR $2 < active_until) ORDER BY active_from DESC LIMIT 1',
      [tokenId, at]
    );
  }

  async list(tokenId: TokenId): Promise<Result<TokenMarketInterval[], Error>> {
    const rows = await this.exec.query<TokenMarketRow>(
      SELECT_MARKET + ' WHERE token_id = $1 ORDER BY active_from ASC',
      [tokenId]
    );
    return rows.kind === 'failure' ? failure(rows.error) : success(rows.value.map(toInterval));
  }

  private async insert(
    input: OpenTokenMarket
  ): Promise<Result<{ inserted: boolean; tokenMarketId: TokenMarketId }, Error>> {
    const [kind, signature, instructionIndex, innerInstructionIndex, logIndex, slot] = originParams(
      input.origin
    );
    const key = originKey(input.origin);
    const inserted = await this.exec.query<{ token_market_id: string }>(
      `INSERT INTO token_markets (
         token_market_id, token_id, venue, market_address, active_from,
         source_event_kind, source_event_key, transaction_signature,
         instruction_index, inner_instruction_index, log_index, slot, provenance
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       ON CONFLICT (source_event_key) DO NOTHING
       RETURNING token_market_id`,
      [
        input.tokenMarketId,
        input.tokenId,
        input.venue,
        input.marketAddress ?? null,
        input.activeFrom,
        kind,
        key,
        signature,
        instructionIndex,
        innerInstructionIndex,
        logIndex,
        slot,
        JSON.stringify(input.provenance),
      ]
    );
    if (inserted.kind === 'failure') return failure(inserted.error);
    if (inserted.value[0] !== undefined) {
      return success({ inserted: true, tokenMarketId: input.tokenMarketId });
    }
    const existing = await this.findByOrigin(input.origin);
    if (existing.kind === 'failure') return failure(existing.error);
    if (existing.value === null)
      return failure(new Error(`Market origin ${key} conflicted but could not be read`));
    return success({ inserted: false, tokenMarketId: existing.value.tokenMarketId });
  }

  private async findByOrigin(
    origin: TokenMarketOrigin
  ): Promise<Result<TokenMarketInterval | null, Error>> {
    return this.findOne(' WHERE source_event_key = $1 LIMIT 1', [originKey(origin)]);
  }

  private async findOne(
    where: string,
    params: unknown[]
  ): Promise<Result<TokenMarketInterval | null, Error>> {
    const rows = await this.exec.query<TokenMarketRow>(SELECT_MARKET + where, params);
    if (rows.kind === 'failure') return failure(rows.error);
    return success(rows.value[0] === undefined ? null : toInterval(rows.value[0]));
  }
}

function toInterval(row: TokenMarketRow): TokenMarketInterval {
  const origin: TokenMarketOrigin =
    row.source_event_kind === 'legacy'
      ? { kind: 'legacy', key: row.source_event_key.replace(/^legacy:/, '') }
      : {
          kind: 'chain',
          position:
            row.instruction_index !== null
              ? {
                  transactionSignature: row.transaction_signature!,
                  instructionIndex: row.instruction_index,
                  ...(row.inner_instruction_index === null
                    ? {}
                    : { innerInstructionIndex: row.inner_instruction_index }),
                }
              : { transactionSignature: row.transaction_signature!, logIndex: row.log_index! },
          slot: Number(row.slot),
        };
  return {
    tokenMarketId: row.token_market_id as TokenMarketId,
    tokenId: row.token_id as TokenId,
    venue: row.venue as MarketVenue,
    activeFrom: row.active_from,
    ...(row.market_address === null ? {} : { marketAddress: row.market_address }),
    ...(row.active_until === null ? {} : { activeUntil: row.active_until }),
    origin,
    provenance: row.provenance,
    recordedAt: row.recorded_at,
  };
}

export function deriveTokenLifecycle(current: TokenMarketInterval | null): TokenLifecycleState {
  if (current === null) return TokenLifecycleState.DISCOVERED;
  return current.venue === MarketVenue.PUMP_FUN_BONDING_CURVE
    ? TokenLifecycleState.BONDING_CURVE_ACTIVE
    : TokenLifecycleState.GRADUATED;
}
