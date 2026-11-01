import { Chain, LaunchSource, Result, Timestamp, TokenId, failure, success } from '@nit/domain';
import { Executor } from '@nit/database-public';
import { NormalizedTokenLaunch } from './normalize';
import { Token } from './types';

/**
 * What an insert did.
 *
 * Not a discriminated union like `@nit/observation`'s `InsertOutcome`: there,
 * the inserted branch carried media ids the duplicate branch could not. Here
 * both branches carry exactly a token id, so a union would only add a type to
 * narrow with no fact behind it.
 */
export interface TokenInsertOutcome {
  /**
   * False when `(chain, address)` was already stored. That is a normal outcome,
   * not a fault: polling uses overlapping windows, and a pump.fun token is seen
   * again on PumpSwap when it graduates (EDS ch.21 §8's duplicate tolerance).
   */
  inserted: boolean;
  tokenId: TokenId;
}

/**
 * A candidate-token query, per EDS ch.21 §5.
 *
 * Ticker and name match case-insensitively because a narrative says "pepe" and a
 * launch says "PEPE"; the `LOWER(...)` indexes on `tokens` are what make that a
 * lookup rather than a scan.
 */
export interface TokenSearch {
  chain: Chain;
  ticker?: string;
  name?: string;
  launchedSince?: Timestamp;
  limit: number;
}

export interface TokenRepository {
  /**
   * Persist a discovered launch.
   *
   * Idempotent on `(chain, address)`: an address already stored returns
   * `inserted: false` with the existing identifier rather than an error.
   */
  insert(launch: NormalizedTokenLaunch): Promise<Result<TokenInsertOutcome, Error>>;

  findById(tokenId: TokenId): Promise<Result<Token | null, Error>>;

  findByAddress(chain: Chain, address: string): Promise<Result<Token | null, Error>>;

  /**
   * Candidate tokens matching `criteria`, newest launch first.
   *
   * Reads `tokens` alone, which is what makes EDS ch.21 §5's "tokens remain
   * searchable before enrichment completes" true: nothing here joins an
   * enrichment table, so a token discovered a second ago and enriched not at all
   * is returned on equal footing with one analysed an hour ago.
   */
  search(criteria: TokenSearch): Promise<Result<Token[], Error>>;
}

interface TokenRow {
  token_id: string;
  chain: string;
  address: string;
  ticker: string;
  name: string;
  launch_time: Date;
  launch_source: string;
  creator_wallet: string | null;
  description: string | null;
  artwork_url: string | null;
  discovered_at: Date;
}

const SELECT_TOKEN = `
  SELECT token_id,
         chain,
         address,
         ticker,
         name,
         launch_time,
         launch_source,
         creator_wallet,
         description,
         artwork_url,
         discovered_at
    FROM tokens
`;

export class PostgresTokenRepository implements TokenRepository {
  /**
   * Takes an `Executor` so a caller can pull these writes into a transaction it
   * owns — which is how the token and its `TokenDiscovered` event reach the
   * database in a single commit.
   */
  constructor(private readonly exec: Executor) {}

  async insert(launch: NormalizedTokenLaunch): Promise<Result<TokenInsertOutcome, Error>> {
    // DO NOTHING rather than DO UPDATE: the append-only trigger on tokens rejects
    // UPDATE, so an upsert would fail on exactly the duplicate this is meant to
    // absorb. The tradeoff is that a conflict returns no row, so the existing
    // identifier needs the follow-up SELECT below.
    const inserted = await this.exec.query<{ token_id: string }>(
      `INSERT INTO tokens (
         token_id, chain, address, ticker, name, launch_time, launch_source,
         creator_wallet, description, artwork_url, discovered_at, raw_payload
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
       ON CONFLICT (chain, address) DO NOTHING
       RETURNING token_id`,
      [
        launch.tokenId,
        launch.chain,
        launch.address,
        launch.ticker,
        launch.name,
        launch.launchTime,
        launch.launchSource,
        launch.creatorWallet ?? null,
        launch.description ?? null,
        launch.artworkUrl ?? null,
        launch.discoveredAt,
        JSON.stringify(launch.rawPayload),
      ]
    );
    if (inserted.kind === 'failure') {
      return failure(inserted.error);
    }

    if (inserted.value.length === 0) {
      const existing = await this.exec.query<{ token_id: string }>(
        'SELECT token_id FROM tokens WHERE chain = $1 AND address = $2',
        [launch.chain, launch.address]
      );
      if (existing.kind === 'failure') {
        return failure(existing.error);
      }
      const tokenId = existing.value[0]?.token_id;
      if (tokenId === undefined) {
        // The insert conflicted, so a row exists; not finding it means this
        // executor is reading a different snapshot than it wrote to.
        return failure(
          new Error(
            `Token ${launch.chain}/${launch.address} conflicted on insert but could not be read back`
          )
        );
      }
      return success({ inserted: false, tokenId: tokenId as TokenId });
    }

    return success({ inserted: true, tokenId: launch.tokenId });
  }

  async findById(tokenId: TokenId): Promise<Result<Token | null, Error>> {
    const found = await this.select(' WHERE token_id = $1', [tokenId]);
    if (found.kind === 'failure') {
      return failure(found.error);
    }
    return success(found.value[0] ?? null);
  }

  async findByAddress(chain: Chain, address: string): Promise<Result<Token | null, Error>> {
    const found = await this.select(' WHERE chain = $1 AND address = $2', [chain, address]);
    if (found.kind === 'failure') {
      return failure(found.error);
    }
    return success(found.value[0] ?? null);
  }

  async search(criteria: TokenSearch): Promise<Result<Token[], Error>> {
    const conditions = ['chain = $1'];
    const params: unknown[] = [criteria.chain];

    // Placeholders are numbered from the array as it grows rather than from a
    // fixed layout, because which filters are present varies per call. The
    // predicates are shaped to match the indexes the migration declares —
    // `LOWER(ticker)` on the column, not `LOWER($n)` on the argument, which would
    // be unindexable.
    if (criteria.ticker !== undefined) {
      params.push(criteria.ticker.trim().toLowerCase());
      conditions.push(`LOWER(ticker) = $${params.length}`);
    }
    if (criteria.name !== undefined) {
      params.push(criteria.name.trim().toLowerCase());
      conditions.push(`LOWER(name) = $${params.length}`);
    }
    if (criteria.launchedSince !== undefined) {
      params.push(criteria.launchedSince);
      conditions.push(`launch_time >= $${params.length}`);
    }
    params.push(criteria.limit);

    return this.select(
      ` WHERE ${conditions.join(' AND ')}` +
        ` ORDER BY launch_time DESC, address DESC LIMIT $${params.length}`,
      params
    );
  }

  private async select(where: string, params: unknown[]): Promise<Result<Token[], Error>> {
    const result = await this.exec.query<TokenRow>(SELECT_TOKEN + where, params);
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    return success(result.value.map(toToken));
  }
}

function toToken(row: TokenRow): Token {
  const token: Token = {
    tokenId: row.token_id as TokenId,
    chain: row.chain as Chain,
    address: row.address,
    ticker: row.ticker,
    name: row.name,
    launchTime: row.launch_time,
    launchSource: row.launch_source as LaunchSource,
    discoveredAt: row.discovered_at,
  };
  if (row.creator_wallet !== null) {
    token.creatorWallet = row.creator_wallet;
  }
  if (row.description !== null) {
    token.description = row.description;
  }
  if (row.artwork_url !== null) {
    token.artworkUrl = row.artwork_url;
  }
  return token;
}
