import { DatabasePool } from './pool';
import { Result, success, failure } from '@nit/domain';

export interface Migration {
  id: string;
  description: string;
  up: string;
}

/**
 * Creates the tracking table every other migration depends on. Applied once
 * before the migration loop, and also listed in MIGRATIONS so it is recorded.
 */
const BOOTSTRAP_MIGRATION: Migration = {
  id: '0001_migrations_table',
  description: 'Track applied migrations',
  up: `
    CREATE TABLE IF NOT EXISTS schema_migrations (
      id TEXT PRIMARY KEY,
      description TEXT NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `,
};

/**
 * The schema, in dependency order: first the Runtime tables (the domain-event
 * log, the durable job queue, its dead-letter store, the job audit history),
 * then the Core tables that carry ingested observations, then the market tables
 * that carry discovered token launches and their enrichment. Table names follow
 * TIS ch.2 §3/§4/§5 and ch.9.
 *
 * Note the distinction TIS draws between two similarly named tables: the
 * Runtime `domain_events` table below is the event bus, while `events` is a
 * Core-domain table for real-world narrative events (TIS ch.8). They are not
 * the same table, so the bus must not be called `events`.
 *
 * Leases live inline on `durable_jobs` (lease_deadline, leased_by) rather than
 * in the separate `job_leases` table TIS ch.2 §3 lists. A claim is then a
 * single-row UPDATE inside the SKIP LOCKED transaction, which keeps claiming
 * atomic without a second write; a dedicated table would buy history we
 * already get from job_history.
 *
 * `observations` stores the quoted post as `quoted_provider_post_id` — the
 * provider's id for it — instead of a foreign key to another observation row.
 * TIS ch.4 §4's canonical model does expose `quotedObservationId`, and the
 * repository resolves it by joining back on the unique `(provider,
 * provider_post_id)` key. Doing it that way rather than with an FK buys two
 * things. Ingestion order stops mattering: a quote fetched before its target
 * links itself the moment the target arrives, in either order, whereas an FK
 * would reject it. And the row stays immutable, because the link never has to be
 * patched in later — which an FK column would eventually require, and the
 * append-only trigger below would then refuse.
 *
 * That trigger covers UPDATE and DELETE, not TRUNCATE. What it defends against
 * is application code mutating an observation; TRUNCATE is a table-owner
 * operation used for schema teardown and per-test reset, and the existing pg
 * suites depend on it.
 *
 * `media` is deliberately not append-only. A row is written at ingestion time as
 * a reference — provider key, type, source URL — and `storage_location`,
 * `sha256`, `phash` and `duration_ms` are filled in afterwards, because TIS
 * ch.2 §7 keeps long media work outside transactions and ch.4 §9 excludes media
 * downloads from the 500 ms persistence target. The dual-tier durability columns
 * TIS ch.5 §7 specifies (local_working_state, durability_state, object_key and
 * the upload counters) are not here yet: they belong with the multimodal worker
 * that advances them, and a state column nothing can write is worse than no
 * column at all.
 *
 * No production topology or private tables belong here.
 */
export const MIGRATIONS: readonly Migration[] = [
  BOOTSTRAP_MIGRATION,
  {
    id: '0002_domain_events',
    description: 'Append-only domain event log',
    up: `
      CREATE TABLE IF NOT EXISTS domain_events (
        event_id UUID PRIMARY KEY,
        event_type TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        aggregate_id UUID NOT NULL,
        version TEXT NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        producer TEXT NOT NULL,
        payload JSONB NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS domain_events_aggregate_idx
        ON domain_events (aggregate_id, occurred_at);
      CREATE INDEX IF NOT EXISTS domain_events_type_idx
        ON domain_events (event_type, occurred_at);
    `,
  },
  {
    id: '0003_durable_jobs',
    description: 'Durable job queue with leases',
    up: `
      CREATE TABLE IF NOT EXISTS durable_jobs (
        job_id UUID PRIMARY KEY,
        queue_name TEXT NOT NULL,
        payload JSONB NOT NULL,
        payload_version TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL,
        lease_deadline TIMESTAMPTZ,
        leased_by TEXT,
        idempotency_key TEXT NOT NULL,
        available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_error TEXT,
        correlation_id UUID,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        -- Scoped per queue: two queues may legitimately reuse a key, and
        -- deduplication is only meaningful within one queue's namespace.
        CONSTRAINT durable_jobs_queue_idempotency_key UNIQUE (queue_name, idempotency_key)
      );
      CREATE INDEX IF NOT EXISTS durable_jobs_claim_idx
        ON durable_jobs (queue_name, status, available_at);
      CREATE INDEX IF NOT EXISTS durable_jobs_lease_idx
        ON durable_jobs (status, lease_deadline);
    `,
  },
  {
    id: '0004_dead_letter_jobs',
    description: 'Auditable dead-letter store',
    up: `
      CREATE TABLE IF NOT EXISTS dead_letter_jobs (
        job_id UUID PRIMARY KEY,
        queue_name TEXT NOT NULL,
        payload JSONB NOT NULL,
        payload_version TEXT NOT NULL,
        attempt_count INTEGER NOT NULL,
        idempotency_key TEXT NOT NULL,
        failure_reason TEXT NOT NULL,
        last_error TEXT,
        correlation_id UUID,
        dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS dead_letter_jobs_queue_idx
        ON dead_letter_jobs (queue_name, dead_lettered_at);
    `,
  },
  {
    id: '0005_job_history',
    description: 'Replayable job audit history',
    up: `
      CREATE TABLE IF NOT EXISTS job_history (
        id BIGSERIAL PRIMARY KEY,
        job_id UUID NOT NULL,
        from_status TEXT,
        to_status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL,
        detail TEXT,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS job_history_job_idx
        ON job_history (job_id, id);
    `,
  },
  {
    id: '0006_observations',
    description: 'Immutable social observations',
    up: `
      CREATE TABLE IF NOT EXISTS observations (
        observation_id UUID PRIMARY KEY,
        provider TEXT NOT NULL,
        provider_post_id TEXT NOT NULL,
        author_id TEXT NOT NULL,
        published_at TIMESTAMPTZ NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        text TEXT,
        quoted_provider_post_id TEXT,
        raw_payload JSONB NOT NULL,
        -- TIS ch.4 §7: the idempotency key for ingestion. A duplicate provider
        -- post must not create a second observation, and the constraint is what
        -- makes that true under concurrent pollers rather than a read-then-write
        -- check that two workers can both pass.
        CONSTRAINT observations_provider_post_key UNIQUE (provider, provider_post_id)
      );
      CREATE INDEX IF NOT EXISTS observations_published_idx
        ON observations (published_at);
      CREATE INDEX IF NOT EXISTS observations_ingested_idx
        ON observations (ingested_at);
      -- "What has this account posted since X" is the query behind polling and
      -- behind ch.13's as_of reconstruction, so author leads and time follows.
      CREATE INDEX IF NOT EXISTS observations_author_idx
        ON observations (author_id, published_at);
      CREATE INDEX IF NOT EXISTS observations_quoted_idx
        ON observations (provider, quoted_provider_post_id)
        WHERE quoted_provider_post_id IS NOT NULL;

      CREATE OR REPLACE FUNCTION observations_reject_mutation()
        RETURNS TRIGGER AS $$
      BEGIN
        RAISE EXCEPTION 'observations is append-only: % is not permitted', TG_OP
          USING ERRCODE = 'restrict_violation';
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS observations_append_only ON observations;
      CREATE TRIGGER observations_append_only
        BEFORE UPDATE OR DELETE ON observations
        FOR EACH ROW EXECUTE FUNCTION observations_reject_mutation();
    `,
  },
  {
    id: '0007_media',
    description: 'Media referenced by observations',
    up: `
      CREATE TABLE IF NOT EXISTS media (
        media_id UUID PRIMARY KEY,
        observation_id UUID NOT NULL REFERENCES observations (observation_id),
        provider_media_key TEXT NOT NULL,
        media_type TEXT NOT NULL,
        source_url TEXT NOT NULL,
        storage_location TEXT,
        sha256 TEXT,
        phash TEXT,
        duration_ms INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        -- Re-ingesting a post must not duplicate its attachments. The provider's
        -- own key for the attachment is the only stable identity available at
        -- ingestion time: sha256 needs the bytes, which arrive later.
        CONSTRAINT media_observation_key UNIQUE (observation_id, provider_media_key)
      );
    `,
  },
  {
    id: '0008_domain_events_sequence',
    description: 'Total order over the domain event log',
    up: `
      -- Replay reads the log in order, so "in order" has to mean one thing.
      -- occurred_at alone does not: a batch of events written from one injected
      -- clock read shares a timestamp exactly, and ordering by a column with ties
      -- leaves the order among them up to the plan. Paging was worse than merely
      -- non-deterministic — it skipped rows, because a boundary of
      -- "occurred_at > last" steps over every sibling at the same instant.
      --
      -- The tie-break is insertion order rather than event_id, which is a v4
      -- value and would order siblings at random. Insertion order is the order the
      -- producer wrote them in, which is the order the ingestion service sorts a
      -- batch into, so causally related events stay in causal order inside a
      -- millisecond.
      --
      -- Sequence values are assigned at INSERT, not at COMMIT, so a live tailing
      -- consumer could in principle read past a lower sequence that has not
      -- committed yet. That hazard belongs to tailing, not to replay: ch.13 reads
      -- a closed history, in which every gap is already filled.
      ALTER TABLE domain_events ADD COLUMN IF NOT EXISTS sequence BIGSERIAL;
      -- Unique so the order really is total, not merely usually total.
      CREATE UNIQUE INDEX IF NOT EXISTS domain_events_sequence_idx
        ON domain_events (sequence);
      CREATE INDEX IF NOT EXISTS domain_events_order_idx
        ON domain_events (occurred_at, sequence);
    `,
  },
  {
    id: '0009_tokens',
    description: 'Immutable token launches',
    up: `
      -- The canonical token of TIS ch.9, holding launch facts only. Identity is
      -- (chain, address) rather than (launch_source, address): PumpSwap is where a
      -- pump.fun token goes when it graduates, so the same token is legitimately
      -- discovered twice, and EDS ch.21 §8 requires duplicate discoveries to be
      -- tolerated. Keying on the venue would create a second token for one asset
      -- and split its evidence in half.
      --
      -- Append-only, per EDS ch.21 §8's "raw launch data MUST remain immutable".
      -- Progressive enrichment (§6) therefore writes to the tables below rather
      -- than back into this row, which is the same division observations and media
      -- already use. A repository assembles §5's candidate view by joining them,
      -- so a token stays searchable from the moment it is discovered — before any
      -- enrichment has run.
      --
      -- Note for a later table: the append-only trigger here reuses the generic
      -- reject_mutation() below. observations predates it and keeps its own
      -- equivalent function; new append-only tables should use the generic one.
      CREATE TABLE IF NOT EXISTS tokens (
        token_id UUID PRIMARY KEY,
        chain TEXT NOT NULL,
        address TEXT NOT NULL,
        ticker TEXT NOT NULL,
        name TEXT NOT NULL,
        launch_time TIMESTAMPTZ NOT NULL,
        launch_source TEXT NOT NULL,
        creator_wallet TEXT,
        description TEXT,
        artwork_url TEXT,
        discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        raw_payload JSONB NOT NULL,
        CONSTRAINT tokens_chain_address_key UNIQUE (chain, address)
      );
      CREATE INDEX IF NOT EXISTS tokens_launch_idx ON tokens (launch_time DESC);
      CREATE INDEX IF NOT EXISTS tokens_discovered_idx ON tokens (discovered_at);
      -- Creator history is a query over this table rather than a stored list, which
      -- is what keeps it correct without a backfill: the profile in
      -- creator_profiles is a materialisation of exactly this index.
      CREATE INDEX IF NOT EXISTS tokens_creator_idx
        ON tokens (chain, creator_wallet, launch_time)
        WHERE creator_wallet IS NOT NULL;
      -- Ticker and name are how a correlation candidate is looked up by the
      -- narrative side, so both are indexed case-insensitively.
      CREATE INDEX IF NOT EXISTS tokens_ticker_idx ON tokens (chain, LOWER(ticker));
      CREATE INDEX IF NOT EXISTS tokens_name_idx ON tokens (chain, LOWER(name));

      CREATE OR REPLACE FUNCTION reject_mutation()
        RETURNS TRIGGER AS $$
      BEGIN
        RAISE EXCEPTION '% is append-only: % is not permitted', TG_TABLE_NAME, TG_OP
          USING ERRCODE = 'restrict_violation';
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS tokens_append_only ON tokens;
      CREATE TRIGGER tokens_append_only
        BEFORE UPDATE OR DELETE ON tokens
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `,
  },
  {
    id: '0010_token_artwork',
    description: 'Token artwork and its visual analysis',
    up: `
      -- Deliberately mutable, exactly like media: the row is written at discovery
      -- as a reference (the URL the launch feed gave), and the analysis columns are
      -- filled by the artwork worker afterwards. One row per token, so the unique
      -- constraint is the token itself — re-running enrichment updates in place
      -- rather than accumulating versions, which is what makes it idempotent in
      -- EDS ch.21 §8's sense.
      --
      -- No embedding column yet. TIS ch.9 lists embeddings under enrichment, but
      -- they belong to multimodal processing (build order step 4) and need a vector
      -- type this schema does not assume is installed. sha256, phash and ocr_text
      -- are here because this stage's worker actually writes them.
      CREATE TABLE IF NOT EXISTS token_artwork (
        artwork_id UUID PRIMARY KEY,
        token_id UUID NOT NULL REFERENCES tokens (token_id),
        source_url TEXT NOT NULL,
        storage_location TEXT,
        sha256 TEXT,
        phash TEXT,
        ocr_text TEXT,
        analysed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT token_artwork_token_key UNIQUE (token_id)
      );
      -- Visual matching asks "which tokens carry this image", so the hash leads.
      CREATE INDEX IF NOT EXISTS token_artwork_phash_idx
        ON token_artwork (phash) WHERE phash IS NOT NULL;
      CREATE INDEX IF NOT EXISTS token_artwork_sha256_idx
        ON token_artwork (sha256) WHERE sha256 IS NOT NULL;
    `,
  },
  {
    id: '0011_market_snapshots',
    description: 'Append-only market metric timeline',
    up: `
      -- TIS ch.9's liquidity, holder, volume and market-cap timelines in one table:
      -- they arrive from the same poll, at the same instant, from the same source,
      -- and splitting them would turn one snapshot into four rows that have to be
      -- rejoined by timestamp to mean anything.
      --
      -- Append-only per ch.9's "append-only snapshots" invariant, which is what
      -- makes ch.13 replay possible at all — a rewritten metric would silently
      -- change the outcome of a backtest that had already been run.
      --
      -- NUMERIC rather than DOUBLE PRECISION: these are money. Note that the
      -- driver hands NUMERIC back as a string to avoid a lossy float conversion,
      -- so the repository converts explicitly.
      CREATE TABLE IF NOT EXISTS market_snapshots (
        snapshot_id UUID PRIMARY KEY,
        token_id UUID NOT NULL REFERENCES tokens (token_id),
        observed_at TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL,
        price_usd NUMERIC(38, 18),
        liquidity_usd NUMERIC(38, 6),
        market_cap_usd NUMERIC(38, 6),
        volume_24h_usd NUMERIC(38, 6),
        holder_count INTEGER,
        raw_payload JSONB NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        -- The idempotency key for enrichment: re-polling a window already stored
        -- must not append a second copy of one observation of one source.
        CONSTRAINT market_snapshots_observation_key UNIQUE (token_id, source, observed_at)
      );
      -- The timeline query: one token, newest first.
      CREATE INDEX IF NOT EXISTS market_snapshots_token_idx
        ON market_snapshots (token_id, observed_at DESC);

      DROP TRIGGER IF EXISTS market_snapshots_append_only ON market_snapshots;
      CREATE TRIGGER market_snapshots_append_only
        BEFORE UPDATE OR DELETE ON market_snapshots
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `,
  },
  {
    id: '0012_creator_profiles',
    description: 'Creator launch history, materialised',
    up: `
      -- Current state, not history, which is why this table is mutable while
      -- tokens is not. Every column is derivable by counting rows in tokens, and
      -- that is the point: recomputing a profile from an append-only source gives
      -- the same answer every time, so the creator worker is idempotent without
      -- needing to know whether it has run before.
      --
      -- Reputation scoring is not here. EDS ch.21 §9 puts it in the Production
      -- tier, and a score column filled with a placeholder would be read by
      -- correlation as though it meant something.
      CREATE TABLE IF NOT EXISTS creator_profiles (
        creator_profile_id UUID PRIMARY KEY,
        chain TEXT NOT NULL,
        creator_wallet TEXT NOT NULL,
        launch_count INTEGER NOT NULL,
        first_launch_at TIMESTAMPTZ NOT NULL,
        last_launch_at TIMESTAMPTZ NOT NULL,
        analysed_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT creator_profiles_wallet_key UNIQUE (chain, creator_wallet)
      );
      -- "Who has launched the most" and "who launched recently" are the two
      -- questions a creator-aware alert asks.
      CREATE INDEX IF NOT EXISTS creator_profiles_activity_idx
        ON creator_profiles (chain, launch_count DESC, last_launch_at DESC);
    `,
  },
];

export const REQUIRED_TABLES: readonly string[] = [
  'schema_migrations',
  'domain_events',
  'durable_jobs',
  'dead_letter_jobs',
  'job_history',
  'observations',
  'media',
  'tokens',
  'token_artwork',
  'market_snapshots',
  'creator_profiles',
];

export class Migrator {
  constructor(private readonly db: DatabasePool) {}

  async migrate(): Promise<Result<string[], Error>> {
    const applied: string[] = [];

    // Bootstrap the tracking table once, before the loop. Every later
    // migration reads and writes schema_migrations, so it must exist first;
    // re-running this DDL per iteration would be wasted work.
    const bootstrap = await this.db.query(BOOTSTRAP_MIGRATION.up);
    if (bootstrap.kind === 'failure') {
      return failure(bootstrap.error);
    }

    for (const migration of MIGRATIONS) {
      const outcome = await this.db.transaction(async (client) => {
        const existing = await client.query(
          'SELECT id FROM schema_migrations WHERE id = $1',
          [migration.id]
        );
        if (existing.rowCount !== null && existing.rowCount > 0) {
          return false;
        }
        await client.query(migration.up);
        await client.query(
          'INSERT INTO schema_migrations (id, description) VALUES ($1, $2)',
          [migration.id, migration.description]
        );
        return true;
      });

      if (outcome.kind === 'failure') {
        return failure(outcome.error);
      }
      if (outcome.value) {
        applied.push(migration.id);
      }
    }

    return success(applied);
  }

  /**
   * Verify expected tables exist without asserting production topology.
   */
  async verifySchema(): Promise<Result<string[], Error>> {
    const result = await this.db.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()`
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }

    const present = new Set(result.value.map((row) => row.table_name));
    const missing = REQUIRED_TABLES.filter((table) => !present.has(table));
    return success(missing);
  }
}
