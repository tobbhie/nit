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
  {
    id: '0013_signals_and_candidate_resolutions',
    description: 'Immutable normalized signals and candidate-resolution versions',
    up: `
      CREATE TABLE IF NOT EXISTS signals (
        signal_id UUID PRIMARY KEY,
        source TEXT NOT NULL,
        source_version TEXT NOT NULL,
        kind TEXT NOT NULL,
        chain TEXT NOT NULL,
        token_mint TEXT,
        canonical_narrative_id TEXT,
        occurred_at TIMESTAMPTZ NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        available_at TIMESTAMPTZ NOT NULL,
        origin_kind TEXT NOT NULL,
        provider TEXT,
        reference_id TEXT,
        transaction_signature TEXT,
        instruction_index INTEGER,
        log_index INTEGER,
        origin_key TEXT NOT NULL,
        evidence_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
        payload JSONB NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT signals_origin_key UNIQUE (origin_key),
        CONSTRAINT signals_source_allowed CHECK (
          source IN ('nit', 'wallet_tracker', 'market')
          OR source ~ '^plugin:[a-z0-9][a-z0-9_-]*$'
        ),
        CONSTRAINT signals_kind_allowed CHECK (
          kind IN ('narrative', 'wallet_buy', 'wallet_sell', 'market', 'candidate_update')
        ),
        CONSTRAINT signals_chain_allowed CHECK (chain = 'solana'),
        CONSTRAINT signals_instruction_index_nonnegative CHECK (
          instruction_index IS NULL OR instruction_index >= 0
        ),
        CONSTRAINT signals_log_index_nonnegative CHECK (log_index IS NULL OR log_index >= 0),
        CONSTRAINT signals_time_order CHECK (
          occurred_at <= observed_at AND observed_at <= available_at
        ),
        CONSTRAINT signals_required_subject CHECK (
          (kind IN ('wallet_buy', 'wallet_sell', 'market') AND token_mint IS NOT NULL)
          OR
          (kind IN ('narrative', 'candidate_update') AND canonical_narrative_id IS NOT NULL)
        ),
        CONSTRAINT signals_origin_shape CHECK (
          (
            origin_kind = 'provider'
            AND provider IS NOT NULL
            AND reference_id IS NOT NULL
            AND transaction_signature IS NULL
            AND instruction_index IS NULL
            AND log_index IS NULL
          )
          OR
          (
            origin_kind = 'chain'
            AND provider IS NULL
            AND reference_id IS NULL
            AND transaction_signature IS NOT NULL
            AND num_nonnulls(instruction_index, log_index) = 1
          )
        )
      );
      CREATE INDEX IF NOT EXISTS signals_available_idx ON signals (available_at, signal_id);
      CREATE UNIQUE INDEX IF NOT EXISTS signals_provider_origin_idx
        ON signals (provider, reference_id) WHERE origin_kind = 'provider';
      CREATE UNIQUE INDEX IF NOT EXISTS signals_chain_instruction_origin_idx
        ON signals (chain, transaction_signature, instruction_index)
        WHERE origin_kind = 'chain' AND instruction_index IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS signals_chain_log_origin_idx
        ON signals (chain, transaction_signature, log_index)
        WHERE origin_kind = 'chain' AND log_index IS NOT NULL;
      CREATE INDEX IF NOT EXISTS signals_token_idx
        ON signals (chain, token_mint, available_at) WHERE token_mint IS NOT NULL;
      CREATE INDEX IF NOT EXISTS signals_narrative_idx
        ON signals (canonical_narrative_id, available_at)
        WHERE canonical_narrative_id IS NOT NULL;

      DROP TRIGGER IF EXISTS signals_append_only ON signals;
      CREATE TRIGGER signals_append_only
        BEFORE UPDATE OR DELETE ON signals
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();

      CREATE TABLE IF NOT EXISTS candidate_resolution_events (
        resolution_id UUID PRIMARY KEY,
        canonical_narrative_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        source_signal_id UUID NOT NULL REFERENCES signals (signal_id),
        action_state TEXT NOT NULL CHECK (
          action_state IN ('trade_eligible', 'watch', 'manual_review', 'blocked', 'no_candidate')
        ),
        candidates JSONB NOT NULL CHECK (jsonb_typeof(candidates) = 'array'),
        evidence_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
        reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
        occurred_at TIMESTAMPTZ NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        available_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT candidate_resolution_version_key UNIQUE (canonical_narrative_id, version),
        CONSTRAINT candidate_resolution_time_order CHECK (
          occurred_at <= observed_at AND observed_at <= available_at
        ),
        CONSTRAINT candidate_resolution_empty_state CHECK (
          action_state <> 'no_candidate' OR jsonb_array_length(candidates) = 0
        )
      );
      CREATE INDEX IF NOT EXISTS candidate_resolution_available_idx
        ON candidate_resolution_events (available_at, resolution_id);

      DROP TRIGGER IF EXISTS candidate_resolution_events_append_only
        ON candidate_resolution_events;
      CREATE TRIGGER candidate_resolution_events_append_only
        BEFORE UPDATE OR DELETE ON candidate_resolution_events
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `,
  },
  {
    id: '0014_strategy_registry_and_runs',
    description: 'Immutable strategy registry with isolated paper runs and portfolios',
    up: `
      CREATE TABLE IF NOT EXISTS strategy_entry_versions (
        entry_version_id UUID PRIMARY KEY,
        entry_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        config JSONB NOT NULL,
        code_hash CHAR(64) NOT NULL CHECK (code_hash ~ '^[0-9a-f]{64}$'),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT strategy_entry_version_key UNIQUE (entry_id, version)
      );
      CREATE TABLE IF NOT EXISTS strategy_exit_versions (
        exit_version_id UUID PRIMARY KEY,
        exit_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        config JSONB NOT NULL,
        code_hash CHAR(64) NOT NULL CHECK (code_hash ~ '^[0-9a-f]{64}$'),
        compatible_entry_ids TEXT[],
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT strategy_exit_version_key UNIQUE (exit_id, version)
      );
      CREATE TABLE IF NOT EXISTS strategy_protection_versions (
        protection_version_id UUID PRIMARY KEY,
        policy_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        config JSONB NOT NULL,
        code_hash CHAR(64) NOT NULL CHECK (code_hash ~ '^[0-9a-f]{64}$'),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT strategy_protection_version_key UNIQUE (policy_id, version)
      );
      CREATE TABLE IF NOT EXISTS strategy_risk_versions (
        risk_version_id UUID PRIMARY KEY,
        policy_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        config JSONB NOT NULL,
        code_hash CHAR(64) NOT NULL CHECK (code_hash ~ '^[0-9a-f]{64}$'),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT strategy_risk_version_key UNIQUE (policy_id, version)
      );

      DROP TRIGGER IF EXISTS strategy_entry_versions_append_only ON strategy_entry_versions;
      CREATE TRIGGER strategy_entry_versions_append_only
        BEFORE UPDATE OR DELETE ON strategy_entry_versions
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      DROP TRIGGER IF EXISTS strategy_exit_versions_append_only ON strategy_exit_versions;
      CREATE TRIGGER strategy_exit_versions_append_only
        BEFORE UPDATE OR DELETE ON strategy_exit_versions
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      DROP TRIGGER IF EXISTS strategy_protection_versions_append_only
        ON strategy_protection_versions;
      CREATE TRIGGER strategy_protection_versions_append_only
        BEFORE UPDATE OR DELETE ON strategy_protection_versions
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      DROP TRIGGER IF EXISTS strategy_risk_versions_append_only ON strategy_risk_versions;
      CREATE TRIGGER strategy_risk_versions_append_only
        BEFORE UPDATE OR DELETE ON strategy_risk_versions
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();

      CREATE TABLE IF NOT EXISTS strategy_combinations (
        combination_id UUID PRIMARY KEY,
        entry_version_id UUID NOT NULL REFERENCES strategy_entry_versions (entry_version_id),
        exit_version_id UUID NOT NULL REFERENCES strategy_exit_versions (exit_version_id),
        protection_version_id UUID NOT NULL
          REFERENCES strategy_protection_versions (protection_version_id),
        risk_version_id UUID NOT NULL REFERENCES strategy_risk_versions (risk_version_id),
        display_id TEXT NOT NULL,
        config_hash CHAR(64) NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$'),
        compatibility_status TEXT NOT NULL CHECK (
          compatibility_status IN ('compatible', 'incompatible')
        ),
        incompatibility_reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT strategy_combination_versions_key UNIQUE (
          entry_version_id, exit_version_id, protection_version_id, risk_version_id
        ),
        CONSTRAINT strategy_combination_hash_key UNIQUE (combination_id, config_hash),
        CONSTRAINT strategy_combination_compatibility_reason CHECK (
          (compatibility_status = 'compatible' AND cardinality(incompatibility_reason_codes) = 0)
          OR
          (compatibility_status = 'incompatible' AND cardinality(incompatibility_reason_codes) > 0)
        )
      );
      CREATE INDEX IF NOT EXISTS strategy_combinations_display_idx
        ON strategy_combinations (display_id);

      CREATE OR REPLACE FUNCTION strategy_combination_validate_compatibility()
        RETURNS TRIGGER AS $$
      DECLARE
        selected_entry_id TEXT;
        allowed_entry_ids TEXT[];
        is_compatible BOOLEAN;
      BEGIN
        SELECT entry_id INTO selected_entry_id
          FROM strategy_entry_versions WHERE entry_version_id = NEW.entry_version_id;
        SELECT compatible_entry_ids INTO allowed_entry_ids
          FROM strategy_exit_versions WHERE exit_version_id = NEW.exit_version_id;
        is_compatible := allowed_entry_ids IS NULL OR selected_entry_id = ANY(allowed_entry_ids);

        IF is_compatible <> (NEW.compatibility_status = 'compatible') THEN
          RAISE EXCEPTION 'combination compatibility status disagrees with the pinned exit policy'
            USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS strategy_combinations_validate_compatibility
        ON strategy_combinations;
      CREATE TRIGGER strategy_combinations_validate_compatibility
        BEFORE INSERT ON strategy_combinations
        FOR EACH ROW EXECUTE FUNCTION strategy_combination_validate_compatibility();

      DROP TRIGGER IF EXISTS strategy_combinations_append_only ON strategy_combinations;
      CREATE TRIGGER strategy_combinations_append_only
        BEFORE UPDATE OR DELETE ON strategy_combinations
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();

      CREATE OR REPLACE FUNCTION strategy_run_reject_incompatible()
        RETURNS TRIGGER AS $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM strategy_combinations
          WHERE combination_id = NEW.combination_id
            AND config_hash = NEW.config_hash
            AND compatibility_status = 'compatible'
        ) THEN
          RAISE EXCEPTION 'strategy run requires a compatible combination and exact config hash'
            USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TABLE IF NOT EXISTS strategy_runs (
        run_id UUID PRIMARY KEY,
        combination_id UUID NOT NULL,
        mode TEXT NOT NULL CHECK (mode IN ('historical', 'forward_paper')),
        config_hash CHAR(64) NOT NULL,
        code_commit_sha TEXT NOT NULL CHECK (length(code_commit_sha) > 0),
        period_start TIMESTAMPTZ NOT NULL,
        period_end TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL CHECK (
          status IN ('planned', 'running', 'completed', 'failed', 'cancelled')
        ),
        started_at TIMESTAMPTZ,
        ended_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT strategy_run_period CHECK (period_start <= period_end),
        CONSTRAINT strategy_run_combination_hash_fk
          FOREIGN KEY (combination_id, config_hash)
          REFERENCES strategy_combinations (combination_id, config_hash),
        CONSTRAINT strategy_run_combination_key UNIQUE (run_id, combination_id)
      );
      CREATE INDEX IF NOT EXISTS strategy_runs_status_idx ON strategy_runs (status, created_at);

      CREATE OR REPLACE FUNCTION strategy_run_reject_scope_mutation()
        RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.combination_id IS DISTINCT FROM OLD.combination_id
          OR NEW.mode IS DISTINCT FROM OLD.mode
          OR NEW.config_hash IS DISTINCT FROM OLD.config_hash
          OR NEW.code_commit_sha IS DISTINCT FROM OLD.code_commit_sha
          OR NEW.period_start IS DISTINCT FROM OLD.period_start
          OR NEW.period_end IS DISTINCT FROM OLD.period_end THEN
          RAISE EXCEPTION 'strategy run scope is immutable after creation'
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS strategy_runs_scope_immutable ON strategy_runs;
      CREATE TRIGGER strategy_runs_scope_immutable
        BEFORE UPDATE ON strategy_runs
        FOR EACH ROW EXECUTE FUNCTION strategy_run_reject_scope_mutation();

      DROP TRIGGER IF EXISTS strategy_runs_compatible_only ON strategy_runs;
      CREATE TRIGGER strategy_runs_compatible_only
        BEFORE INSERT OR UPDATE OF combination_id, config_hash ON strategy_runs
        FOR EACH ROW EXECUTE FUNCTION strategy_run_reject_incompatible();

      CREATE TABLE IF NOT EXISTS paper_portfolios (
        portfolio_id UUID PRIMARY KEY,
        run_id UUID NOT NULL,
        combination_id UUID NOT NULL,
        cash_asset TEXT NOT NULL,
        initial_cash NUMERIC(38, 18) NOT NULL CHECK (initial_cash >= 0),
        current_cash NUMERIC(38, 18) NOT NULL CHECK (current_cash >= 0),
        capital_constraints JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT paper_portfolio_run_combination_fk
          FOREIGN KEY (run_id, combination_id)
          REFERENCES strategy_runs (run_id, combination_id),
        CONSTRAINT paper_portfolio_authority_key UNIQUE (run_id, combination_id),
        CONSTRAINT paper_portfolio_scope_key UNIQUE (
          portfolio_id, run_id, combination_id
        )
      );

      CREATE OR REPLACE FUNCTION paper_portfolio_reject_scope_mutation()
        RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.run_id IS DISTINCT FROM OLD.run_id
          OR NEW.combination_id IS DISTINCT FROM OLD.combination_id
          OR NEW.cash_asset IS DISTINCT FROM OLD.cash_asset
          OR NEW.initial_cash IS DISTINCT FROM OLD.initial_cash
          OR NEW.capital_constraints IS DISTINCT FROM OLD.capital_constraints THEN
          RAISE EXCEPTION 'paper portfolio authority and starting scope are immutable'
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS paper_portfolios_scope_immutable ON paper_portfolios;
      CREATE TRIGGER paper_portfolios_scope_immutable
        BEFORE UPDATE ON paper_portfolios
        FOR EACH ROW EXECUTE FUNCTION paper_portfolio_reject_scope_mutation();
    `,
  },
  {
    id: '0015_trading_ledger',
    description: 'Auditable decisions, orders, fills, positions, and position events',
    up: `
      CREATE TABLE IF NOT EXISTS strategy_decisions (
        decision_id UUID PRIMARY KEY,
        run_id UUID NOT NULL,
        portfolio_id UUID NOT NULL,
        combination_id UUID NOT NULL,
        source_signal_id UUID NOT NULL REFERENCES signals (signal_id),
        decision_type TEXT NOT NULL CHECK (
          decision_type IN ('enter', 'abstain', 'watch', 'review', 'block', 'reduce', 'exit')
        ),
        evidence_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
        reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
        risk_gate_results JSONB NOT NULL,
        idempotency_key TEXT NOT NULL,
        decided_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT strategy_decision_portfolio_scope_fk
          FOREIGN KEY (portfolio_id, run_id, combination_id)
          REFERENCES paper_portfolios (portfolio_id, run_id, combination_id),
        CONSTRAINT strategy_decision_idempotency_key UNIQUE (portfolio_id, idempotency_key),
        CONSTRAINT strategy_decision_scope_key UNIQUE (
          decision_id, run_id, portfolio_id, combination_id
        )
      );
      CREATE INDEX IF NOT EXISTS strategy_decisions_signal_idx
        ON strategy_decisions (source_signal_id, decided_at);

      DROP TRIGGER IF EXISTS strategy_decisions_append_only ON strategy_decisions;
      CREATE TRIGGER strategy_decisions_append_only
        BEFORE UPDATE OR DELETE ON strategy_decisions
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();

      CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY,
        decision_id UUID NOT NULL,
        run_id UUID NOT NULL,
        portfolio_id UUID NOT NULL,
        combination_id UUID NOT NULL,
        token_mint TEXT NOT NULL,
        side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
        quantity_asset TEXT NOT NULL,
        quantity NUMERIC(38, 18) NOT NULL CHECK (quantity > 0),
        quote_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT order_decision_scope_fk
          FOREIGN KEY (decision_id, run_id, portfolio_id, combination_id)
          REFERENCES strategy_decisions (decision_id, run_id, portfolio_id, combination_id),
        CONSTRAINT order_idempotency_key UNIQUE (portfolio_id, idempotency_key)
      );
      CREATE INDEX IF NOT EXISTS orders_portfolio_idx
        ON orders (portfolio_id, created_at);

      DROP TRIGGER IF EXISTS orders_append_only ON orders;
      CREATE TRIGGER orders_append_only
        BEFORE UPDATE OR DELETE ON orders
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();

      CREATE TABLE IF NOT EXISTS order_events (
        order_event_id UUID PRIMARY KEY,
        order_id UUID NOT NULL REFERENCES orders (order_id),
        status TEXT NOT NULL CHECK (
          status IN (
            'created', 'accepted', 'partially_filled', 'filled',
            'rejected', 'failed', 'cancelled'
          )
        ),
        detail JSONB,
        occurred_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS order_events_order_idx
        ON order_events (order_id, occurred_at, order_event_id);

      DROP TRIGGER IF EXISTS order_events_append_only ON order_events;
      CREATE TRIGGER order_events_append_only
        BEFORE UPDATE OR DELETE ON order_events
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();

      CREATE TABLE IF NOT EXISTS order_fills (
        fill_id UUID PRIMARY KEY,
        order_id UUID NOT NULL REFERENCES orders (order_id),
        status TEXT NOT NULL CHECK (status IN ('partially_filled', 'filled')),
        input_asset TEXT NOT NULL,
        input_quantity NUMERIC(38, 18) NOT NULL CHECK (input_quantity > 0),
        output_asset TEXT NOT NULL,
        output_quantity NUMERIC(38, 18) NOT NULL CHECK (output_quantity > 0),
        fees JSONB NOT NULL,
        venue TEXT NOT NULL,
        transaction_reference TEXT,
        recorded_at TIMESTAMPTZ NOT NULL
      );
      CREATE INDEX IF NOT EXISTS order_fills_order_idx ON order_fills (order_id, recorded_at);

      DROP TRIGGER IF EXISTS order_fills_append_only ON order_fills;
      CREATE TRIGGER order_fills_append_only
        BEFORE UPDATE OR DELETE ON order_fills
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();

      CREATE TABLE IF NOT EXISTS positions (
        position_id UUID PRIMARY KEY,
        portfolio_id UUID NOT NULL REFERENCES paper_portfolios (portfolio_id),
        token_mint TEXT NOT NULL,
        original_quantity NUMERIC(38, 18) NOT NULL CHECK (original_quantity > 0),
        open_quantity NUMERIC(38, 18) NOT NULL CHECK (
          open_quantity >= 0 AND open_quantity <= original_quantity
        ),
        cost_basis NUMERIC(38, 18) NOT NULL CHECK (cost_basis >= 0),
        high_water_price NUMERIC(38, 18) NOT NULL CHECK (high_water_price >= 0),
        state TEXT NOT NULL CHECK (
          state IN ('open', 'partially_closed', 'moonbag', 'closed')
        ),
        protection_config JSONB NOT NULL,
        stop_price NUMERIC(38, 18) CHECK (stop_price >= 0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL,
        CONSTRAINT position_portfolio_token_key UNIQUE (portfolio_id, token_mint)
      );

      CREATE TABLE IF NOT EXISTS position_events (
        position_event_id UUID PRIMARY KEY,
        position_id UUID NOT NULL REFERENCES positions (position_id),
        order_id UUID REFERENCES orders (order_id),
        fill_id UUID REFERENCES order_fills (fill_id),
        event_type TEXT NOT NULL CHECK (
          event_type IN (
            'opened', 'increased', 'partial_sell', 'moonbag_armed',
            'stop_changed', 'exit_failed', 'closed'
          )
        ),
        quantity_delta NUMERIC(38, 18) NOT NULL,
        detail JSONB NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS position_events_position_idx
        ON position_events (position_id, occurred_at, position_event_id);

      DROP TRIGGER IF EXISTS position_events_append_only ON position_events;
      CREATE TRIGGER position_events_append_only
        BEFORE UPDATE OR DELETE ON position_events
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `,
  },
  {
    id: '0016_token_market_lifecycle',
    description: 'Venue-aware token lifecycle and market snapshots',
    up: `
      CREATE TABLE IF NOT EXISTS token_markets (
        token_market_id UUID PRIMARY KEY,
        token_id UUID NOT NULL REFERENCES tokens (token_id),
        venue TEXT NOT NULL CHECK (venue IN ('pump_fun_bonding_curve', 'pumpswap')),
        market_address TEXT,
        active_from TIMESTAMPTZ NOT NULL,
        active_until TIMESTAMPTZ,
        source_event_kind TEXT NOT NULL CHECK (source_event_kind IN ('chain', 'legacy')),
        source_event_key TEXT NOT NULL,
        transaction_signature TEXT,
        instruction_index INTEGER,
        log_index INTEGER,
        slot BIGINT,
        provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT token_markets_source_event_key UNIQUE (source_event_key),
        CONSTRAINT token_markets_interval_valid CHECK (
          active_until IS NULL OR active_until > active_from
        ),
        CONSTRAINT token_markets_instruction_index_nonnegative CHECK (
          instruction_index IS NULL OR instruction_index >= 0
        ),
        CONSTRAINT token_markets_log_index_nonnegative CHECK (
          log_index IS NULL OR log_index >= 0
        ),
        CONSTRAINT token_markets_slot_nonnegative CHECK (slot IS NULL OR slot >= 0),
        CONSTRAINT token_markets_source_shape CHECK (
          (
            source_event_kind = 'chain'
            AND transaction_signature IS NOT NULL
            AND market_address IS NOT NULL
            AND num_nonnulls(instruction_index, log_index) = 1
            AND slot IS NOT NULL
          )
          OR
          (
            source_event_kind = 'legacy'
            AND transaction_signature IS NULL
            AND instruction_index IS NULL
            AND log_index IS NULL
            AND slot IS NULL
          )
        ),
        CONSTRAINT token_markets_interval_key UNIQUE (
          token_id, venue, market_address, active_from
        )
      );
      CREATE UNIQUE INDEX IF NOT EXISTS token_markets_current_idx
        ON token_markets (token_id) WHERE active_until IS NULL;
      CREATE INDEX IF NOT EXISTS token_markets_timeline_idx
        ON token_markets (token_id, active_from DESC);
      CREATE UNIQUE INDEX IF NOT EXISTS token_markets_chain_instruction_idx
        ON token_markets (transaction_signature, instruction_index)
        WHERE source_event_kind = 'chain' AND instruction_index IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS token_markets_chain_log_idx
        ON token_markets (transaction_signature, log_index)
        WHERE source_event_kind = 'chain' AND log_index IS NOT NULL;

      -- Existing token rows predate source-occurrence capture. Preserve that fact
      -- explicitly as legacy provenance rather than manufacturing a chain identity.
      INSERT INTO token_markets (
        token_market_id, token_id, venue, market_address, active_from,
        source_event_kind, source_event_key, provenance
      )
      SELECT token_id,
             token_id,
             CASE launch_source
               WHEN 'pump_fun' THEN 'pump_fun_bonding_curve'
               ELSE 'pumpswap'
             END,
             NULL::TEXT,
             launch_time,
             'legacy',
             'legacy:token:' || token_id::text,
             jsonb_build_object('migration', '0016_token_market_lifecycle')
        FROM tokens
      ON CONFLICT (source_event_key) DO NOTHING;

      CREATE OR REPLACE FUNCTION token_market_allow_close_only()
        RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'token market intervals cannot be deleted'
            USING ERRCODE = 'restrict_violation';
        END IF;
        IF OLD.active_until IS NOT NULL
          OR NEW.active_until IS NULL
          OR NEW.active_until <= OLD.active_from
          OR NEW.token_market_id IS DISTINCT FROM OLD.token_market_id
          OR NEW.token_id IS DISTINCT FROM OLD.token_id
          OR NEW.venue IS DISTINCT FROM OLD.venue
          OR NEW.market_address IS DISTINCT FROM OLD.market_address
          OR NEW.active_from IS DISTINCT FROM OLD.active_from
          OR NEW.source_event_kind IS DISTINCT FROM OLD.source_event_kind
          OR NEW.source_event_key IS DISTINCT FROM OLD.source_event_key
          OR NEW.transaction_signature IS DISTINCT FROM OLD.transaction_signature
          OR NEW.instruction_index IS DISTINCT FROM OLD.instruction_index
          OR NEW.log_index IS DISTINCT FROM OLD.log_index
          OR NEW.slot IS DISTINCT FROM OLD.slot
          OR NEW.provenance IS DISTINCT FROM OLD.provenance
          OR NEW.recorded_at IS DISTINCT FROM OLD.recorded_at THEN
          RAISE EXCEPTION 'token market intervals may only be closed once'
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS token_markets_close_only ON token_markets;
      CREATE TRIGGER token_markets_close_only
        BEFORE UPDATE OR DELETE ON token_markets
        FOR EACH ROW EXECUTE FUNCTION token_market_allow_close_only();

      DROP TRIGGER IF EXISTS market_snapshots_append_only ON market_snapshots;
      ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS venue TEXT;
      UPDATE market_snapshots AS snapshot
         SET venue = (
          SELECT venue
            FROM token_markets
           WHERE token_id = snapshot.token_id
             AND active_from <= snapshot.observed_at
             AND (active_until IS NULL OR snapshot.observed_at < active_until)
           ORDER BY active_from DESC
           LIMIT 1
         )
       WHERE snapshot.venue IS NULL;
      UPDATE market_snapshots AS snapshot
         SET venue = (
          SELECT venue
            FROM token_markets
           WHERE token_id = snapshot.token_id
           ORDER BY active_from DESC
           LIMIT 1
         )
       WHERE snapshot.venue IS NULL;
      ALTER TABLE market_snapshots ALTER COLUMN venue SET NOT NULL;
      ALTER TABLE market_snapshots DROP CONSTRAINT IF EXISTS market_snapshots_venue_allowed;
      ALTER TABLE market_snapshots ADD CONSTRAINT market_snapshots_venue_allowed
        CHECK (venue IN ('pump_fun_bonding_curve', 'pumpswap'));
      ALTER TABLE market_snapshots DROP CONSTRAINT IF EXISTS market_snapshots_observation_key;
      ALTER TABLE market_snapshots ADD CONSTRAINT market_snapshots_observation_key
        UNIQUE (token_id, venue, source, observed_at);
      CREATE INDEX IF NOT EXISTS market_snapshots_venue_timeline_idx
        ON market_snapshots (token_id, venue, observed_at DESC);
      CREATE TRIGGER market_snapshots_append_only
        BEFORE UPDATE OR DELETE ON market_snapshots
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `,
  },
  {
    id: '0017_solana_reconciliation_cursors',
    description: 'Monotonic finalized-slot cursors for Solana launch reconciliation',
    up: `
      CREATE TABLE IF NOT EXISTS solana_reconciliation_cursors (
        chain TEXT NOT NULL CHECK (chain = 'solana'),
        launch_source TEXT NOT NULL CHECK (launch_source IN ('pump_fun', 'pumpswap')),
        finalized_slot BIGINT NOT NULL CHECK (finalized_slot >= 0),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT solana_reconciliation_cursor_key PRIMARY KEY (chain, launch_source)
      );

      CREATE OR REPLACE FUNCTION reject_solana_cursor_regression()
        RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.chain IS DISTINCT FROM OLD.chain
          OR NEW.launch_source IS DISTINCT FROM OLD.launch_source
          OR NEW.finalized_slot < OLD.finalized_slot THEN
          RAISE EXCEPTION 'Solana reconciliation cursor cannot regress or change identity'
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS solana_reconciliation_cursor_monotonic
        ON solana_reconciliation_cursors;
      CREATE TRIGGER solana_reconciliation_cursor_monotonic
        BEFORE UPDATE ON solana_reconciliation_cursors
        FOR EACH ROW EXECUTE FUNCTION reject_solana_cursor_regression();
    `,
  },
  {
    id: '0018_solana_market_occurrences',
    description: 'Exact nested Solana occurrence identity and immutable raw capture',
    up: `
      ALTER TABLE token_markets
        ADD COLUMN IF NOT EXISTS inner_instruction_index INTEGER;
      ALTER TABLE token_markets
        DROP CONSTRAINT IF EXISTS token_markets_source_shape;
      ALTER TABLE token_markets
        DROP CONSTRAINT IF EXISTS token_markets_inner_instruction_index_nonnegative;
      ALTER TABLE token_markets
        ADD CONSTRAINT token_markets_inner_instruction_index_nonnegative CHECK (
          inner_instruction_index IS NULL OR inner_instruction_index >= 0
        );
      ALTER TABLE token_markets
        ADD CONSTRAINT token_markets_source_shape CHECK (
          (
            source_event_kind = 'chain'
            AND transaction_signature IS NOT NULL
            AND market_address IS NOT NULL
            AND num_nonnulls(instruction_index, log_index) = 1
            AND (inner_instruction_index IS NULL OR instruction_index IS NOT NULL)
            AND slot IS NOT NULL
          )
          OR
          (
            source_event_kind = 'legacy'
            AND transaction_signature IS NULL
            AND instruction_index IS NULL
            AND inner_instruction_index IS NULL
            AND log_index IS NULL
            AND slot IS NULL
          )
        );

      DROP INDEX IF EXISTS token_markets_chain_instruction_idx;
      CREATE UNIQUE INDEX IF NOT EXISTS token_markets_chain_instruction_idx
        ON token_markets (transaction_signature, instruction_index)
        WHERE source_event_kind = 'chain'
          AND instruction_index IS NOT NULL
          AND inner_instruction_index IS NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS token_markets_chain_inner_instruction_idx
        ON token_markets (
          transaction_signature, instruction_index, inner_instruction_index
        )
        WHERE source_event_kind = 'chain' AND inner_instruction_index IS NOT NULL;

      CREATE OR REPLACE FUNCTION token_market_allow_close_only()
        RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'token market intervals cannot be deleted'
            USING ERRCODE = 'restrict_violation';
        END IF;
        IF OLD.active_until IS NOT NULL
          OR NEW.active_until IS NULL
          OR NEW.active_until <= OLD.active_from
          OR NEW.token_market_id IS DISTINCT FROM OLD.token_market_id
          OR NEW.token_id IS DISTINCT FROM OLD.token_id
          OR NEW.venue IS DISTINCT FROM OLD.venue
          OR NEW.market_address IS DISTINCT FROM OLD.market_address
          OR NEW.active_from IS DISTINCT FROM OLD.active_from
          OR NEW.source_event_kind IS DISTINCT FROM OLD.source_event_kind
          OR NEW.source_event_key IS DISTINCT FROM OLD.source_event_key
          OR NEW.transaction_signature IS DISTINCT FROM OLD.transaction_signature
          OR NEW.instruction_index IS DISTINCT FROM OLD.instruction_index
          OR NEW.inner_instruction_index IS DISTINCT FROM OLD.inner_instruction_index
          OR NEW.log_index IS DISTINCT FROM OLD.log_index
          OR NEW.slot IS DISTINCT FROM OLD.slot
          OR NEW.provenance IS DISTINCT FROM OLD.provenance
          OR NEW.recorded_at IS DISTINCT FROM OLD.recorded_at THEN
          RAISE EXCEPTION 'token market intervals may only be closed once'
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TABLE IF NOT EXISTS solana_market_occurrences (
        source_event_key TEXT PRIMARY KEY,
        chain TEXT NOT NULL CHECK (chain = 'solana'),
        launch_source TEXT NOT NULL CHECK (launch_source IN ('pump_fun', 'pumpswap')),
        event_kind TEXT NOT NULL CHECK (event_kind IN ('launch', 'graduation')),
        program_address TEXT NOT NULL,
        transaction_signature TEXT NOT NULL,
        instruction_index INTEGER,
        inner_instruction_index INTEGER,
        log_index INTEGER,
        slot BIGINT NOT NULL CHECK (slot >= 0),
        block_time TIMESTAMPTZ NOT NULL,
        raw_payload JSONB NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT solana_market_occurrences_position_shape CHECK (
          num_nonnulls(instruction_index, log_index) = 1
          AND (inner_instruction_index IS NULL OR instruction_index IS NOT NULL)
        ),
        CONSTRAINT solana_market_occurrences_instruction_nonnegative CHECK (
          instruction_index IS NULL OR instruction_index >= 0
        ),
        CONSTRAINT solana_market_occurrences_inner_nonnegative CHECK (
          inner_instruction_index IS NULL OR inner_instruction_index >= 0
        ),
        CONSTRAINT solana_market_occurrences_log_nonnegative CHECK (
          log_index IS NULL OR log_index >= 0
        )
      );
      CREATE UNIQUE INDEX IF NOT EXISTS solana_market_occurrences_instruction_idx
        ON solana_market_occurrences (transaction_signature, instruction_index)
        WHERE instruction_index IS NOT NULL AND inner_instruction_index IS NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS solana_market_occurrences_inner_instruction_idx
        ON solana_market_occurrences (
          transaction_signature, instruction_index, inner_instruction_index
        )
        WHERE inner_instruction_index IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS solana_market_occurrences_log_idx
        ON solana_market_occurrences (transaction_signature, log_index)
        WHERE log_index IS NOT NULL;
      CREATE INDEX IF NOT EXISTS solana_market_occurrences_source_slot_idx
        ON solana_market_occurrences (launch_source, slot DESC);

      DROP TRIGGER IF EXISTS solana_market_occurrences_append_only
        ON solana_market_occurrences;
      CREATE TRIGGER solana_market_occurrences_append_only
        BEFORE UPDATE OR DELETE ON solana_market_occurrences
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `,
  },
  {
    id: '0019_wallet_intelligence',
    description: 'Immutable wallet swaps, relationship evidence, and reconciliation cursors',
    up: `
      ALTER TABLE signals ADD COLUMN IF NOT EXISTS transaction_level BOOLEAN NOT NULL DEFAULT FALSE;
      ALTER TABLE signals ADD COLUMN IF NOT EXISTS actor_address TEXT;
      ALTER TABLE signals DROP CONSTRAINT IF EXISTS signals_kind_allowed;
      ALTER TABLE signals ADD CONSTRAINT signals_kind_allowed CHECK (
        kind IN (
          'narrative', 'wallet_buy', 'wallet_sell', 'wallet_convergence',
          'wallet_risk', 'market', 'candidate_update'
        )
      );
      ALTER TABLE signals DROP CONSTRAINT IF EXISTS signals_required_subject;
      ALTER TABLE signals ADD CONSTRAINT signals_required_subject CHECK (
        (kind IN ('wallet_buy', 'wallet_sell', 'wallet_convergence', 'market') AND token_mint IS NOT NULL)
        OR (kind = 'wallet_risk')
        OR (kind IN ('narrative', 'candidate_update') AND canonical_narrative_id IS NOT NULL)
      );
      ALTER TABLE signals DROP CONSTRAINT IF EXISTS signals_origin_shape;
      ALTER TABLE signals ADD CONSTRAINT signals_origin_shape CHECK (
        (
          origin_kind = 'provider'
          AND provider IS NOT NULL
          AND reference_id IS NOT NULL
          AND transaction_signature IS NULL
          AND instruction_index IS NULL
          AND log_index IS NULL
          AND NOT transaction_level
          AND actor_address IS NULL
        )
        OR (
          origin_kind = 'chain'
          AND provider IS NULL
          AND reference_id IS NULL
          AND transaction_signature IS NOT NULL
          AND (
            (CASE WHEN instruction_index IS NULL THEN 0 ELSE 1 END)
            + (CASE WHEN log_index IS NULL THEN 0 ELSE 1 END)
            + (CASE WHEN transaction_level THEN 1 ELSE 0 END)
          ) = 1
          AND (NOT transaction_level OR actor_address IS NOT NULL)
          AND (transaction_level OR actor_address IS NULL)
        )
      );
      CREATE UNIQUE INDEX IF NOT EXISTS signals_chain_transaction_actor_origin_idx
        ON signals (chain, transaction_signature, actor_address)
        WHERE origin_kind = 'chain' AND transaction_level;

      CREATE TABLE IF NOT EXISTS wallet_swaps (
        wallet_swap_id UUID PRIMARY KEY,
        chain TEXT NOT NULL CHECK (chain = 'solana'),
        transaction_signature TEXT NOT NULL,
        slot BIGINT NOT NULL CHECK (slot >= 0),
        wallet_address TEXT NOT NULL,
        side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
        token_mint TEXT NOT NULL,
        token_raw_amount NUMERIC(78, 0) NOT NULL CHECK (token_raw_amount > 0),
        token_decimals SMALLINT NOT NULL CHECK (token_decimals BETWEEN 0 AND 30),
        quote_asset TEXT NOT NULL,
        quote_raw_amount NUMERIC(78, 0) NOT NULL CHECK (quote_raw_amount > 0),
        quote_decimals SMALLINT NOT NULL CHECK (quote_decimals BETWEEN 0 AND 30),
        program_addresses TEXT[] NOT NULL CHECK (cardinality(program_addresses) > 0),
        attribution JSONB NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        available_at TIMESTAMPTZ NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT wallet_swaps_time_order CHECK (
          observed_at >= occurred_at AND available_at >= observed_at
        ),
        CONSTRAINT wallet_swaps_transaction_wallet_key UNIQUE (
          chain, transaction_signature, wallet_address
        )
      );
      CREATE INDEX IF NOT EXISTS wallet_swaps_token_timeline_idx
        ON wallet_swaps (token_mint, side, occurred_at DESC);
      CREATE INDEX IF NOT EXISTS wallet_swaps_wallet_timeline_idx
        ON wallet_swaps (wallet_address, occurred_at DESC);

      CREATE TABLE IF NOT EXISTS wallet_funding_events (
        wallet_funding_event_id UUID PRIMARY KEY,
        chain TEXT NOT NULL CHECK (chain = 'solana'),
        transaction_signature TEXT NOT NULL,
        instruction_path TEXT NOT NULL,
        slot BIGINT NOT NULL CHECK (slot >= 0),
        funder_address TEXT NOT NULL,
        funded_wallet_address TEXT NOT NULL,
        lamports NUMERIC(78, 0) NOT NULL CHECK (lamports > 0),
        occurred_at TIMESTAMPTZ NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        CONSTRAINT wallet_funding_occurrence_key UNIQUE (
          chain, transaction_signature, instruction_path, funded_wallet_address
        )
      );
      CREATE INDEX IF NOT EXISTS wallet_funding_common_source_idx
        ON wallet_funding_events (funder_address, funded_wallet_address, occurred_at DESC);

      CREATE TABLE IF NOT EXISTS wallet_relation_evidence (
        wallet_relation_evidence_id UUID PRIMARY KEY,
        wallet_a TEXT NOT NULL,
        wallet_b TEXT NOT NULL,
        relation_kind TEXT NOT NULL CHECK (
          relation_kind IN ('shared_funder', 'same_slot_entry', 'same_slot_exit')
        ),
        source_reference TEXT NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        details JSONB NOT NULL,
        CONSTRAINT wallet_relation_canonical_pair CHECK (wallet_a < wallet_b),
        CONSTRAINT wallet_relation_occurrence_key UNIQUE (
          wallet_a, wallet_b, relation_kind, source_reference
        )
      );
      CREATE INDEX IF NOT EXISTS wallet_relation_pair_timeline_idx
        ON wallet_relation_evidence (wallet_a, wallet_b, occurred_at DESC);

      CREATE TABLE IF NOT EXISTS wallet_reconciliation_cursors (
        wallet_address TEXT PRIMARY KEY,
        finalized_slot BIGINT NOT NULL CHECK (finalized_slot >= 0),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE OR REPLACE FUNCTION reject_wallet_cursor_regression()
        RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.wallet_address IS DISTINCT FROM OLD.wallet_address
          OR NEW.finalized_slot < OLD.finalized_slot THEN
          RAISE EXCEPTION 'Wallet reconciliation cursor cannot regress or change identity'
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
      DROP TRIGGER IF EXISTS wallet_reconciliation_cursor_monotonic
        ON wallet_reconciliation_cursors;
      CREATE TRIGGER wallet_reconciliation_cursor_monotonic
        BEFORE UPDATE ON wallet_reconciliation_cursors
        FOR EACH ROW EXECUTE FUNCTION reject_wallet_cursor_regression();

      DROP TRIGGER IF EXISTS wallet_swaps_append_only ON wallet_swaps;
      CREATE TRIGGER wallet_swaps_append_only
        BEFORE UPDATE OR DELETE ON wallet_swaps
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      DROP TRIGGER IF EXISTS wallet_funding_events_append_only ON wallet_funding_events;
      CREATE TRIGGER wallet_funding_events_append_only
        BEFORE UPDATE OR DELETE ON wallet_funding_events
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      DROP TRIGGER IF EXISTS wallet_relation_evidence_append_only ON wallet_relation_evidence;
      CREATE TRIGGER wallet_relation_evidence_append_only
        BEFORE UPDATE OR DELETE ON wallet_relation_evidence
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `,
  },
  {
    id: '0020_paper_runtime',
    description:
      'Pinned paper runtime configuration, market evidence, delayed orders and settlements',
    up: `
    ALTER TABLE paper_portfolios ADD COLUMN IF NOT EXISTS paper_last_tick_at TIMESTAMPTZ;
    CREATE TABLE IF NOT EXISTS paper_run_configs (
      run_id UUID PRIMARY KEY REFERENCES strategy_runs (run_id),
      strategy_snapshot JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS paper_market_observations (
      snapshot_id UUID PRIMARY KEY,
      token_mint TEXT NOT NULL,
      occurred_at TIMESTAMPTZ NOT NULL,
      available_at TIMESTAMPTZ NOT NULL CHECK (available_at >= occurred_at),
      snapshot JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS paper_market_available_idx
      ON paper_market_observations (token_mint, available_at);
    CREATE TABLE IF NOT EXISTS paper_execution_plans (
      order_id UUID PRIMARY KEY REFERENCES orders (order_id),
      snapshot_id UUID NOT NULL REFERENCES paper_market_observations (snapshot_id),
      due_at TIMESTAMPTZ NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at >= due_at),
      plan JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS paper_execution_due_idx ON paper_execution_plans (due_at);
    CREATE TABLE IF NOT EXISTS paper_execution_results (
      order_id UUID PRIMARY KEY REFERENCES paper_execution_plans (order_id),
      snapshot_id UUID NOT NULL REFERENCES paper_market_observations (snapshot_id),
      cash_delta NUMERIC(38,18) NOT NULL,
      settled_at TIMESTAMPTZ NOT NULL
    );
    DROP TRIGGER IF EXISTS paper_run_configs_append_only ON paper_run_configs;
    CREATE TRIGGER paper_run_configs_append_only
      BEFORE UPDATE OR DELETE ON paper_run_configs
      FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    DROP TRIGGER IF EXISTS paper_market_observations_append_only ON paper_market_observations;
    CREATE TRIGGER paper_market_observations_append_only
      BEFORE UPDATE OR DELETE ON paper_market_observations
      FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    DROP TRIGGER IF EXISTS paper_execution_plans_append_only ON paper_execution_plans;
    CREATE TRIGGER paper_execution_plans_append_only
      BEFORE UPDATE OR DELETE ON paper_execution_plans
      FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    DROP TRIGGER IF EXISTS paper_execution_results_append_only ON paper_execution_results;
    CREATE TRIGGER paper_execution_results_append_only
      BEFORE UPDATE OR DELETE ON paper_execution_results
      FOR EACH ROW EXECUTE FUNCTION reject_mutation();
  `,
  },
  {
    id: '0021_continuous_paper_dispatch',
    description: 'Durable paper signal delivery and feed health projections',
    up: `
      CREATE TABLE IF NOT EXISTS paper_signal_dispatch (
        portfolio_id UUID NOT NULL REFERENCES paper_portfolios (portfolio_id),
        signal_id UUID NOT NULL REFERENCES signals (signal_id),
        attempted_at TIMESTAMPTZ NOT NULL,
        retry_at TIMESTAMPTZ NOT NULL,
        completed_at TIMESTAMPTZ,
        reason TEXT,
        PRIMARY KEY (portfolio_id,signal_id)
      );
      CREATE INDEX IF NOT EXISTS paper_signal_retry_idx
        ON paper_signal_dispatch (portfolio_id,retry_at) WHERE completed_at IS NULL;
      CREATE TABLE IF NOT EXISTS paper_feed_health (
        portfolio_id UUID NOT NULL REFERENCES paper_portfolios (portfolio_id),
        token_mint TEXT NOT NULL,
        checked_at TIMESTAMPTZ NOT NULL,
        reason TEXT,
        PRIMARY KEY (portfolio_id,token_mint)
      );
      CREATE TABLE IF NOT EXISTS paper_dispatch_health (
        portfolio_id UUID PRIMARY KEY REFERENCES paper_portfolios (portfolio_id),
        checked_at TIMESTAMPTZ NOT NULL
      );
    `,
  },
  {
    id: '0022_resolver_and_telegram_console',
    description: 'Versioned candidate scenarios and authorized durable Telegram controls',
    up: `
      CREATE TABLE IF NOT EXISTS candidate_scenarios (
        narrative_id TEXT PRIMARY KEY,
        source_signal_id UUID NOT NULL REFERENCES signals(signal_id),
        fingerprint TEXT,
        report JSONB,
        next_check_at TIMESTAMPTZ NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        last_error TEXT
      );
      CREATE INDEX IF NOT EXISTS candidate_scenario_due_idx ON candidate_scenarios(next_check_at);
      CREATE TABLE IF NOT EXISTS candidate_resolution_reports (
        resolution_id UUID PRIMARY KEY REFERENCES candidate_resolution_events(resolution_id),
        report JSONB NOT NULL,
        policy JSONB NOT NULL
      );
      DROP TRIGGER IF EXISTS candidate_resolution_reports_append_only ON candidate_resolution_reports;
      CREATE TRIGGER candidate_resolution_reports_append_only BEFORE UPDATE OR DELETE ON candidate_resolution_reports
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      CREATE TABLE IF NOT EXISTS console_operator_preferences (
        bot_name TEXT NOT NULL,user_id TEXT NOT NULL,chat_id TEXT NOT NULL,
        portfolio_id UUID REFERENCES paper_portfolios(portfolio_id),
        PRIMARY KEY(bot_name,user_id,chat_id)
      );
      CREATE TABLE IF NOT EXISTS console_candidate_preferences (
        bot_name TEXT NOT NULL,user_id TEXT NOT NULL,narrative_id TEXT NOT NULL,token_mint TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('watch','discard')),rationale TEXT NOT NULL,updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY(bot_name,user_id,narrative_id,token_mint)
      );
      CREATE TABLE IF NOT EXISTS paper_candidate_exclusions (
        portfolio_id UUID NOT NULL REFERENCES paper_portfolios(portfolio_id),narrative_id TEXT NOT NULL,
        token_mint TEXT NOT NULL,operator_user_id TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY(portfolio_id,narrative_id,token_mint)
      );
      CREATE TABLE IF NOT EXISTS telegram_actions (
        action_token TEXT PRIMARY KEY,bot_name TEXT NOT NULL,user_id TEXT NOT NULL,chat_id TEXT NOT NULL,
        resolution_id UUID REFERENCES candidate_resolution_events(resolution_id),token_mint TEXT,
        portfolio_id UUID REFERENCES paper_portfolios(portfolio_id),position_id UUID REFERENCES positions(position_id),
        kind TEXT NOT NULL CHECK(kind IN ('buy','confirm_buy','watch','discard','close','confirm_close','view','stop')),
        expires_at TIMESTAMPTZ NOT NULL,status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','used','rejected')),
        quote JSONB,message_id BIGINT,result JSONB,created_at TIMESTAMPTZ NOT NULL
      );
      CREATE TABLE IF NOT EXISTS telegram_action_receipts (
        bot_name TEXT NOT NULL,callback_id TEXT NOT NULL,action_token TEXT NOT NULL,
        result JSONB NOT NULL,handled_at TIMESTAMPTZ NOT NULL,PRIMARY KEY(bot_name,callback_id)
      );
      DROP TRIGGER IF EXISTS telegram_action_receipts_append_only ON telegram_action_receipts;
      CREATE TRIGGER telegram_action_receipts_append_only BEFORE UPDATE OR DELETE ON telegram_action_receipts
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      CREATE TABLE IF NOT EXISTS console_trade_requests (
        action_token TEXT PRIMARY KEY REFERENCES telegram_actions(action_token),
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','complete','rejected')),
        result JSONB,created_at TIMESTAMPTZ NOT NULL
      );
      CREATE TABLE IF NOT EXISTS telegram_outbox (
        outbox_id UUID PRIMARY KEY,bot_name TEXT NOT NULL,user_id TEXT NOT NULL,chat_id TEXT NOT NULL,
        body JSONB NOT NULL,action_tokens TEXT[] NOT NULL DEFAULT '{}',dedupe_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','sending','sent','uncertain','failed')),
        attempts INTEGER NOT NULL DEFAULT 0,available_at TIMESTAMPTZ NOT NULL,lease_until TIMESTAMPTZ,
        message_id BIGINT,last_error TEXT,created_at TIMESTAMPTZ NOT NULL,
        UNIQUE(bot_name,dedupe_key)
      );
      CREATE INDEX IF NOT EXISTS telegram_outbox_due_idx ON telegram_outbox(bot_name,status,available_at);
      CREATE TABLE IF NOT EXISTS telegram_update_receipts (
        bot_name TEXT NOT NULL,update_id BIGINT NOT NULL,handled_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY(bot_name,update_id)
      );
      DROP TRIGGER IF EXISTS telegram_update_receipts_append_only ON telegram_update_receipts;
      CREATE TRIGGER telegram_update_receipts_append_only BEFORE UPDATE OR DELETE ON telegram_update_receipts
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
      CREATE TABLE IF NOT EXISTS telegram_poll_offsets (
        bot_name TEXT PRIMARY KEY,next_offset BIGINT NOT NULL CHECK(next_offset>=0)
      );
      CREATE TABLE IF NOT EXISTS console_notification_receipts (
        bot_name TEXT NOT NULL,user_id TEXT NOT NULL,reference_id TEXT NOT NULL,
        handled_at TIMESTAMPTZ NOT NULL,PRIMARY KEY(bot_name,user_id,reference_id)
      );
    `,
  },
];

export const REQUIRED_TABLES: readonly string[] = [
  'candidate_scenarios',
  'candidate_resolution_reports',
  'telegram_actions',
  'telegram_action_receipts',
  'telegram_outbox',
  'telegram_update_receipts',
  'telegram_poll_offsets',
  'console_operator_preferences',
  'console_candidate_preferences',
  'console_trade_requests',
  'console_notification_receipts',
  'paper_candidate_exclusions',
  'paper_signal_dispatch',
  'paper_feed_health',
  'paper_dispatch_health',
  'paper_run_configs',
  'paper_market_observations',
  'paper_execution_plans',
  'paper_execution_results',
  'schema_migrations',
  'domain_events',
  'durable_jobs',
  'dead_letter_jobs',
  'job_history',
  'observations',
  'media',
  'tokens',
  'token_markets',
  'solana_market_occurrences',
  'solana_reconciliation_cursors',
  'wallet_swaps',
  'wallet_funding_events',
  'wallet_relation_evidence',
  'wallet_reconciliation_cursors',
  'token_artwork',
  'market_snapshots',
  'creator_profiles',
  'signals',
  'candidate_resolution_events',
  'strategy_entry_versions',
  'strategy_exit_versions',
  'strategy_protection_versions',
  'strategy_risk_versions',
  'strategy_combinations',
  'strategy_runs',
  'paper_portfolios',
  'strategy_decisions',
  'orders',
  'order_events',
  'order_fills',
  'positions',
  'position_events',
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
        const existing = await client.query('SELECT id FROM schema_migrations WHERE id = $1', [
          migration.id,
        ]);
        if (existing.rowCount !== null && existing.rowCount > 0) {
          return false;
        }
        await client.query(migration.up);
        await client.query('INSERT INTO schema_migrations (id, description) VALUES ($1, $2)', [
          migration.id,
          migration.description,
        ]);
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
