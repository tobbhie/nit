import {
  Chain,
  CreatorProfileId,
  MarketSnapshotId,
  Result,
  Timestamp,
  TokenArtworkId,
  TokenId,
  failure,
  success,
} from '@nit/domain';
import { Executor } from '@nit/database-public';
import { Decimal } from './enrichment-contracts';

/**
 * The three enrichment tables, per EDS ch.21 §6.
 *
 * Their idempotency is not a property of this code — it is the unique constraint on
 * each table, and each of the three is idempotent in a different way:
 *
 * - `token_artwork` is UNIQUE (token_id) and mutable, so a re-run updates the one row
 *   in place and never accumulates versions.
 * - `market_snapshots` is UNIQUE (token_id, source, observed_at) and append-only, so a
 *   re-run of the same observation is absorbed rather than stored twice.
 * - `creator_profiles` is UNIQUE (chain, creator_wallet) and mutable, and every column
 *   is recomputed by counting rows in the append-only `tokens`, so a re-run produces
 *   the same answer without needing to know it is a re-run.
 *
 * Amounts are `Decimal` — strings — throughout. `pg` returns NUMERIC as a string
 * precisely so the driver does not narrow a money value to a double, and parsing one
 * here would undo that at the first hop. See `enrichment-contracts`.
 */

/** A stored artwork row. Analysis fields are absent until a worker has filled them. */
export interface TokenArtwork {
  artworkId: TokenArtworkId;
  tokenId: TokenId;
  sourceUrl: string;
  storageLocation?: string;
  sha256?: string;
  phash?: string;
  ocrText?: string;
  /** When analysis last ran. Absent on a row that is only a URL reference. */
  analysedAt?: Timestamp;
  createdAt: Timestamp;
}

export interface TokenArtworkUpsert {
  /** Used only if this is an insert; a conflict keeps the identifier already stored. */
  artworkId: TokenArtworkId;
  tokenId: TokenId;
  sourceUrl: string;
  storageLocation?: string;
  sha256?: string;
  phash?: string;
  ocrText?: string;
  analysedAt: Timestamp;
}

export interface TokenArtworkRepository {
  /**
   * Store or refresh a token's artwork analysis.
   *
   * Returns the identifier of the row that now holds it, which is the existing one
   * when a row was already there.
   */
  upsert(artwork: TokenArtworkUpsert): Promise<Result<TokenArtworkId, Error>>;

  findByToken(tokenId: TokenId): Promise<Result<TokenArtwork | null, Error>>;

  /**
   * Rows sharing a perceptual hash: the same picture, recompressed or rescaled.
   *
   * Reused artwork across launches is a launch-level fact and the migration declares
   * a partial index for exactly this lookup.
   */
  findByPhash(phash: string, limit: number): Promise<Result<TokenArtwork[], Error>>;
}

/** One observation of a token's market state, ready to store. */
export interface MarketSnapshotInsert {
  snapshotId: MarketSnapshotId;
  tokenId: TokenId;
  /** The provider's timestamp for the reading. Part of the row's identity. */
  observedAt: Timestamp;
  source: string;
  priceUsd?: Decimal;
  liquidityUsd?: Decimal;
  marketCapUsd?: Decimal;
  volume24hUsd?: Decimal;
  holderCount?: number;
  rawPayload: Record<string, unknown>;
}

export interface MarketSnapshot {
  snapshotId: MarketSnapshotId;
  tokenId: TokenId;
  observedAt: Timestamp;
  source: string;
  priceUsd?: Decimal;
  liquidityUsd?: Decimal;
  marketCapUsd?: Decimal;
  volume24hUsd?: Decimal;
  holderCount?: number;
  recordedAt: Timestamp;
}

/**
 * What an insert did. Shaped like `TokenInsertOutcome` and for the same reason: both
 * branches carry exactly an identifier, so a discriminated union would add a type to
 * narrow with no fact behind it.
 */
export interface MarketSnapshotInsertOutcome {
  /**
   * False when this exact observation — same token, same source, same instant — was
   * already stored. A normal outcome: a redelivered job re-fetches a provider that
   * reports the same `observedAt`.
   */
  inserted: boolean;
  snapshotId: MarketSnapshotId;
}

export interface MarketSnapshotRepository {
  /** Idempotent on `(tokenId, source, observedAt)`. */
  insert(snapshot: MarketSnapshotInsert): Promise<Result<MarketSnapshotInsertOutcome, Error>>;

  /**
   * The most recent snapshot for a token from one source, or null.
   *
   * Per-source rather than across all of them because it is what a change is measured
   * against: two providers disagreeing about liquidity is normal, and comparing one's
   * reading to the other's would report a move that never happened.
   */
  findLatest(tokenId: TokenId, source: string): Promise<Result<MarketSnapshot | null, Error>>;

  /** A token's snapshots, newest observation first. */
  listForToken(tokenId: TokenId, limit: number): Promise<Result<MarketSnapshot[], Error>>;
}

/** What `tokens` says about one creator's launch history. */
export interface CreatorLaunchStats {
  launchCount: number;
  firstLaunchAt: Timestamp;
  lastLaunchAt: Timestamp;
}

export interface CreatorProfileUpsert {
  /** Used only if this is an insert. */
  creatorProfileId: CreatorProfileId;
  chain: Chain;
  creatorWallet: string;
  launchCount: number;
  firstLaunchAt: Timestamp;
  lastLaunchAt: Timestamp;
  analysedAt: Timestamp;
}

export interface CreatorProfile {
  creatorProfileId: CreatorProfileId;
  chain: Chain;
  creatorWallet: string;
  launchCount: number;
  firstLaunchAt: Timestamp;
  lastLaunchAt: Timestamp;
  analysedAt: Timestamp;
}

export interface CreatorProfileRepository {
  /**
   * Recompute a creator's launch history from `tokens`. Null if the wallet has
   * launched nothing on this chain.
   *
   * The query reads `tokens`, so it could equally sit on `TokenRepository`. It lives
   * here because its shape is dictated by `creator_profiles`' columns rather than by
   * anything `tokens` needs: put on the token repository it would be a method with one
   * caller, whose signature changes whenever this table does.
   */
  computeStats(
    chain: Chain,
    creatorWallet: string
  ): Promise<Result<CreatorLaunchStats | null, Error>>;

  /** Store or refresh a profile, returning the identifier of the row holding it. */
  upsert(profile: CreatorProfileUpsert): Promise<Result<CreatorProfileId, Error>>;

  findByWallet(
    chain: Chain,
    creatorWallet: string
  ): Promise<Result<CreatorProfile | null, Error>>;
}

interface TokenArtworkRow {
  artwork_id: string;
  token_id: string;
  source_url: string;
  storage_location: string | null;
  sha256: string | null;
  phash: string | null;
  ocr_text: string | null;
  analysed_at: Date | null;
  created_at: Date;
}

const SELECT_ARTWORK = `
  SELECT artwork_id,
         token_id,
         source_url,
         storage_location,
         sha256,
         phash,
         ocr_text,
         analysed_at,
         created_at
    FROM token_artwork
`;

export class PostgresTokenArtworkRepository implements TokenArtworkRepository {
  constructor(private readonly exec: Executor) {}

  async upsert(artwork: TokenArtworkUpsert): Promise<Result<TokenArtworkId, Error>> {
    // COALESCE(EXCLUDED.x, token_artwork.x) rather than a straight overwrite: analysis
    // is a chain of steps that fail independently, so a run that computed a hash but
    // could not reach the OCR service must not erase text an earlier run found. A
    // later partial analysis fills gaps and never opens one.
    //
    // `analysed_at` is the exception and is overwritten unconditionally, because it
    // records when analysis last ran rather than what it produced. `source_url` is
    // likewise overwritten, which is a no-op in practice: it comes from `tokens`, and
    // that row is immutable.
    const result = await this.exec.query<{ artwork_id: string }>(
      `INSERT INTO token_artwork (
         artwork_id, token_id, source_url, storage_location, sha256, phash,
         ocr_text, analysed_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (token_id) DO UPDATE
         SET source_url = EXCLUDED.source_url,
             storage_location = COALESCE(EXCLUDED.storage_location, token_artwork.storage_location),
             sha256 = COALESCE(EXCLUDED.sha256, token_artwork.sha256),
             phash = COALESCE(EXCLUDED.phash, token_artwork.phash),
             ocr_text = COALESCE(EXCLUDED.ocr_text, token_artwork.ocr_text),
             analysed_at = EXCLUDED.analysed_at
       RETURNING artwork_id`,
      [
        artwork.artworkId,
        artwork.tokenId,
        artwork.sourceUrl,
        artwork.storageLocation ?? null,
        artwork.sha256 ?? null,
        artwork.phash ?? null,
        artwork.ocrText ?? null,
        artwork.analysedAt,
      ]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }

    const artworkId = result.value[0]?.artwork_id;
    if (artworkId === undefined) {
      // DO UPDATE always returns a row, so an empty result is not a conflict — it is
      // this executor reading a different snapshot than it wrote to.
      return failure(
        new Error(`Artwork upsert for token ${artwork.tokenId} returned no identifier`)
      );
    }
    return success(artworkId as TokenArtworkId);
  }

  async findByToken(tokenId: TokenId): Promise<Result<TokenArtwork | null, Error>> {
    const found = await this.select(' WHERE token_id = $1', [tokenId]);
    if (found.kind === 'failure') {
      return failure(found.error);
    }
    return success(found.value[0] ?? null);
  }

  async findByPhash(phash: string, limit: number): Promise<Result<TokenArtwork[], Error>> {
    // `created_at, artwork_id` rather than `created_at` alone: two launches reusing one
    // image within a millisecond is exactly the case this query is for, and a partial
    // order would let two callers paging the same hash see different sequences.
    return this.select(' WHERE phash = $1 ORDER BY created_at, artwork_id LIMIT $2', [
      phash,
      limit,
    ]);
  }

  private async select(where: string, params: unknown[]): Promise<Result<TokenArtwork[], Error>> {
    const result = await this.exec.query<TokenArtworkRow>(SELECT_ARTWORK + where, params);
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    return success(result.value.map(toArtwork));
  }
}

function toArtwork(row: TokenArtworkRow): TokenArtwork {
  const artwork: TokenArtwork = {
    artworkId: row.artwork_id as TokenArtworkId,
    tokenId: row.token_id as TokenId,
    sourceUrl: row.source_url,
    createdAt: row.created_at,
  };
  if (row.storage_location !== null) {
    artwork.storageLocation = row.storage_location;
  }
  if (row.sha256 !== null) {
    artwork.sha256 = row.sha256;
  }
  if (row.phash !== null) {
    artwork.phash = row.phash;
  }
  if (row.ocr_text !== null) {
    artwork.ocrText = row.ocr_text;
  }
  if (row.analysed_at !== null) {
    artwork.analysedAt = row.analysed_at;
  }
  return artwork;
}

interface MarketSnapshotRow {
  snapshot_id: string;
  token_id: string;
  observed_at: Date;
  source: string;
  /** NUMERIC. The driver returns it as text so no digits are lost to a double. */
  price_usd: string | null;
  liquidity_usd: string | null;
  market_cap_usd: string | null;
  volume_24h_usd: string | null;
  holder_count: number | null;
  recorded_at: Date;
}

const SELECT_SNAPSHOT = `
  SELECT snapshot_id,
         token_id,
         observed_at,
         source,
         price_usd,
         liquidity_usd,
         market_cap_usd,
         volume_24h_usd,
         holder_count,
         recorded_at
    FROM market_snapshots
`;

export class PostgresMarketSnapshotRepository implements MarketSnapshotRepository {
  constructor(private readonly exec: Executor) {}

  async insert(
    snapshot: MarketSnapshotInsert
  ): Promise<Result<MarketSnapshotInsertOutcome, Error>> {
    // DO NOTHING rather than DO UPDATE, for the reason the token repository gives: the
    // append-only trigger rejects UPDATE, so an upsert would fail on exactly the
    // duplicate this is meant to absorb. The cost is that a conflict returns no row and
    // the existing identifier needs the follow-up SELECT.
    const inserted = await this.exec.query<{ snapshot_id: string }>(
      `INSERT INTO market_snapshots (
         snapshot_id, token_id, observed_at, source, price_usd, liquidity_usd,
         market_cap_usd, volume_24h_usd, holder_count, raw_payload
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (token_id, source, observed_at) DO NOTHING
       RETURNING snapshot_id`,
      [
        snapshot.snapshotId,
        snapshot.tokenId,
        snapshot.observedAt,
        snapshot.source,
        snapshot.priceUsd ?? null,
        snapshot.liquidityUsd ?? null,
        snapshot.marketCapUsd ?? null,
        snapshot.volume24hUsd ?? null,
        snapshot.holderCount ?? null,
        JSON.stringify(snapshot.rawPayload),
      ]
    );
    if (inserted.kind === 'failure') {
      return failure(inserted.error);
    }

    if (inserted.value.length === 0) {
      const existing = await this.exec.query<{ snapshot_id: string }>(
        `SELECT snapshot_id FROM market_snapshots
          WHERE token_id = $1 AND source = $2 AND observed_at = $3`,
        [snapshot.tokenId, snapshot.source, snapshot.observedAt]
      );
      if (existing.kind === 'failure') {
        return failure(existing.error);
      }
      const snapshotId = existing.value[0]?.snapshot_id;
      if (snapshotId === undefined) {
        return failure(
          new Error(
            `Snapshot for token ${snapshot.tokenId} from ${snapshot.source} conflicted on insert but could not be read back`
          )
        );
      }
      return success({ inserted: false, snapshotId: snapshotId as MarketSnapshotId });
    }

    return success({ inserted: true, snapshotId: snapshot.snapshotId });
  }

  async findLatest(
    tokenId: TokenId,
    source: string
  ): Promise<Result<MarketSnapshot | null, Error>> {
    const found = await this.select(
      ' WHERE token_id = $1 AND source = $2 ORDER BY observed_at DESC LIMIT 1',
      [tokenId, source]
    );
    if (found.kind === 'failure') {
      return failure(found.error);
    }
    return success(found.value[0] ?? null);
  }

  async listForToken(tokenId: TokenId, limit: number): Promise<Result<MarketSnapshot[], Error>> {
    return this.select(' WHERE token_id = $1 ORDER BY observed_at DESC LIMIT $2', [
      tokenId,
      limit,
    ]);
  }

  private async select(
    where: string,
    params: unknown[]
  ): Promise<Result<MarketSnapshot[], Error>> {
    const result = await this.exec.query<MarketSnapshotRow>(SELECT_SNAPSHOT + where, params);
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    return success(result.value.map(toSnapshot));
  }
}

function toSnapshot(row: MarketSnapshotRow): MarketSnapshot {
  const snapshot: MarketSnapshot = {
    snapshotId: row.snapshot_id as MarketSnapshotId,
    tokenId: row.token_id as TokenId,
    observedAt: row.observed_at,
    source: row.source,
    recordedAt: row.recorded_at,
  };
  // Passed through as the text Postgres returned, not parsed. NUMERIC(38, 18) does not
  // fit a double, and this is the only place the exact value is still available.
  if (row.price_usd !== null) {
    snapshot.priceUsd = row.price_usd;
  }
  if (row.liquidity_usd !== null) {
    snapshot.liquidityUsd = row.liquidity_usd;
  }
  if (row.market_cap_usd !== null) {
    snapshot.marketCapUsd = row.market_cap_usd;
  }
  if (row.volume_24h_usd !== null) {
    snapshot.volume24hUsd = row.volume_24h_usd;
  }
  if (row.holder_count !== null) {
    snapshot.holderCount = row.holder_count;
  }
  return snapshot;
}

interface CreatorProfileRow {
  creator_profile_id: string;
  chain: string;
  creator_wallet: string;
  launch_count: number;
  first_launch_at: Date;
  last_launch_at: Date;
  analysed_at: Date;
}

const SELECT_PROFILE = `
  SELECT creator_profile_id,
         chain,
         creator_wallet,
         launch_count,
         first_launch_at,
         last_launch_at,
         analysed_at
    FROM creator_profiles
`;

export class PostgresCreatorProfileRepository implements CreatorProfileRepository {
  constructor(private readonly exec: Executor) {}

  async computeStats(
    chain: Chain,
    creatorWallet: string
  ): Promise<Result<CreatorLaunchStats | null, Error>> {
    // An aggregate with no GROUP BY always returns exactly one row, so a wallet with no
    // launches comes back as a row of nulls rather than as no row. That is why the null
    // check below is on the value and not on the array length.
    //
    // COUNT returns BIGINT, which `pg` hands back as a string for the same
    // no-lossy-conversion reason as NUMERIC. A launch count fits a double comfortably,
    // so this one is parsed — unlike the money columns, where parsing would lose digits.
    const result = await this.exec.query<{
      launch_count: string;
      first_launch_at: Date | null;
      last_launch_at: Date | null;
    }>(
      `SELECT COUNT(*) AS launch_count,
              MIN(launch_time) AS first_launch_at,
              MAX(launch_time) AS last_launch_at
         FROM tokens
        WHERE chain = $1 AND creator_wallet = $2`,
      [chain, creatorWallet]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }

    const row = result.value[0];
    if (row === undefined || row.first_launch_at === null || row.last_launch_at === null) {
      return success(null);
    }
    return success({
      launchCount: Number(row.launch_count),
      firstLaunchAt: row.first_launch_at,
      lastLaunchAt: row.last_launch_at,
    });
  }

  async upsert(profile: CreatorProfileUpsert): Promise<Result<CreatorProfileId, Error>> {
    // A straight overwrite, unlike `token_artwork` above. Every column here is
    // recomputed by counting rows in an append-only table, so the incoming values are
    // never less complete than the stored ones — COALESCE would only preserve a stale
    // count that a later launch has already invalidated.
    const result = await this.exec.query<{ creator_profile_id: string }>(
      `INSERT INTO creator_profiles (
         creator_profile_id, chain, creator_wallet, launch_count,
         first_launch_at, last_launch_at, analysed_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (chain, creator_wallet) DO UPDATE
         SET launch_count = EXCLUDED.launch_count,
             first_launch_at = EXCLUDED.first_launch_at,
             last_launch_at = EXCLUDED.last_launch_at,
             analysed_at = EXCLUDED.analysed_at,
             updated_at = NOW()
       RETURNING creator_profile_id`,
      [
        profile.creatorProfileId,
        profile.chain,
        profile.creatorWallet,
        profile.launchCount,
        profile.firstLaunchAt,
        profile.lastLaunchAt,
        profile.analysedAt,
      ]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }

    const profileId = result.value[0]?.creator_profile_id;
    if (profileId === undefined) {
      return failure(
        new Error(
          `Creator profile upsert for ${profile.chain}/${profile.creatorWallet} returned no identifier`
        )
      );
    }
    return success(profileId as CreatorProfileId);
  }

  async findByWallet(
    chain: Chain,
    creatorWallet: string
  ): Promise<Result<CreatorProfile | null, Error>> {
    const result = await this.exec.query<CreatorProfileRow>(
      `${SELECT_PROFILE} WHERE chain = $1 AND creator_wallet = $2`,
      [chain, creatorWallet]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    const row = result.value[0];
    return success(row === undefined ? null : toProfile(row));
  }
}

function toProfile(row: CreatorProfileRow): CreatorProfile {
  return {
    creatorProfileId: row.creator_profile_id as CreatorProfileId,
    chain: row.chain as Chain,
    creatorWallet: row.creator_wallet,
    launchCount: row.launch_count,
    firstLaunchAt: row.first_launch_at,
    lastLaunchAt: row.last_launch_at,
    analysedAt: row.analysed_at,
  };
}
