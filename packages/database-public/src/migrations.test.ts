import { describe, it, expect } from 'vitest';
import { MIGRATIONS, Migration, REQUIRED_TABLES } from './migrations';

/**
 * A migration's DDL with its comments removed.
 *
 * Every assertion in this file is about the schema, but comments are part of
 * `up` too, and they fail in both directions. A comment explaining why a column
 * is absent names the absent thing, so a negative assertion matches the prose
 * and fails on a schema that is correct. Worse in the other direction: a comment
 * quoting a constraint satisfies a positive assertion on a schema that never
 * declares it. Strip the prose and both classes go away.
 *
 * Line comments only — deliberately not an SQL parser. It is correct for this
 * DDL because no string literal or dollar-quoted body in it contains `--`, and
 * a test helper that tried to be general would need to be tested itself.
 */
function ddl(migration: Migration | undefined): string {
  return (migration?.up ?? '').replace(/--[^\n]*/g, '');
}

describe('MIGRATIONS', () => {
  it('uses unique, ordered identifiers', () => {
    const ids = MIGRATIONS.map((migration) => migration.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect([...ids].sort()).toEqual(ids);
  });

  it('creates the migrations tracking table first', () => {
    expect(ddl(MIGRATIONS[0])).toContain('CREATE TABLE IF NOT EXISTS schema_migrations');
  });

  it('is re-runnable: every CREATE is guarded against an existing object', () => {
    for (const migration of MIGRATIONS) {
      const up = ddl(migration);

      // UNIQUE sits between CREATE and INDEX, so a pattern without it lets every
      // unique index through unchecked — which is most of the ones here.
      const tablesAndIndexes = up.match(/CREATE (UNIQUE )?(TABLE|INDEX)[^(]*/g) ?? [];
      expect(tablesAndIndexes.length).toBeGreaterThan(0);
      for (const statement of tablesAndIndexes) {
        expect(statement).toContain('IF NOT EXISTS');
      }

      // ALTER TABLE has no IF NOT EXISTS of its own; the column does.
      for (const statement of up.match(/ALTER TABLE \w+ ADD COLUMN[^;]*/g) ?? []) {
        expect(statement).toContain('IF NOT EXISTS');
      }

      // Functions and triggers do not accept IF NOT EXISTS, so each has its own
      // idiom: replace the function in place, and drop the trigger first.
      for (const statement of up.match(/CREATE (OR REPLACE )?FUNCTION/g) ?? []) {
        expect(statement).toContain('OR REPLACE');
      }
      for (const [, name] of up.matchAll(/CREATE TRIGGER (\w+)/g)) {
        expect(up).toContain(`DROP TRIGGER IF EXISTS ${name as string}`);
      }
    }
  });

  it('creates every table the schema verification requires', () => {
    const schema = MIGRATIONS.map(ddl).join('\n');
    for (const table of REQUIRED_TABLES) {
      expect(schema).toContain(`CREATE TABLE IF NOT EXISTS ${table}`);
    }
  });

  it('enforces idempotent enqueue with a per-queue unique key', () => {
    const jobs = ddl(MIGRATIONS.find((migration) => migration.id === '0003_durable_jobs'));
    expect(jobs).toContain('UNIQUE (queue_name, idempotency_key)');
    // A globally unique key would stop two queues from reusing one key.
    expect(jobs).not.toContain('idempotency_key TEXT NOT NULL UNIQUE');
  });

  it('indexes the columns the queue claims on', () => {
    const jobs = ddl(MIGRATIONS.find((migration) => migration.id === '0003_durable_jobs'));
    expect(jobs).toContain('ON durable_jobs (queue_name, status, available_at)');
    expect(jobs).toContain('ON durable_jobs (status, lease_deadline)');
  });

  it('names the runtime tables the way TIS ch.2 does', () => {
    const schema = MIGRATIONS.map(ddl).join('\n');
    // The bus is `domain_events`; `events` is reserved for the Core-domain
    // narrative event table (TIS ch.8) and must not be taken by the bus.
    expect(schema).toContain('CREATE TABLE IF NOT EXISTS domain_events');
    expect(schema).not.toMatch(/CREATE TABLE IF NOT EXISTS events\b/);
    expect(schema).not.toMatch(/CREATE TABLE IF NOT EXISTS jobs\b/);
    expect(schema).not.toMatch(/CREATE TABLE IF NOT EXISTS dead_letters\b/);
  });

  it('retains an append-only audit trail for job transitions', () => {
    const history = ddl(MIGRATIONS.find((migration) => migration.id === '0005_job_history'));
    expect(history).toContain('from_status');
    expect(history).toContain('to_status TEXT NOT NULL');
  });

  it('gives the event log a unique tie-break for replay order', () => {
    // Ordering by occurred_at alone leaves ties, and a paging boundary over a
    // column with ties skips rows rather than merely reordering them.
    const order = ddl(
      MIGRATIONS.find((migration) => migration.id === '0008_domain_events_sequence')
    );
    expect(order).toContain('ADD COLUMN IF NOT EXISTS sequence BIGSERIAL');
    expect(order).toContain('CREATE UNIQUE INDEX IF NOT EXISTS domain_events_sequence_idx');
    expect(order).toContain('ON domain_events (occurred_at, sequence)');
  });

  it('gives every migration a human-readable description', () => {
    for (const migration of MIGRATIONS) {
      expect(migration.description.length).toBeGreaterThan(0);
    }
  });
});

describe('observation schema', () => {
  const observations = ddl(MIGRATIONS.find((migration) => migration.id === '0006_observations'));
  const media = ddl(MIGRATIONS.find((migration) => migration.id === '0007_media'));

  it('makes the ingestion idempotency key a database constraint', () => {
    // TIS ch.4 §7. Enforced by the database rather than by a read-then-write
    // check in the service, which two concurrent pollers can both pass.
    expect(observations).toContain('UNIQUE (provider, provider_post_id)');
  });

  it('rejects mutation of a persisted observation', () => {
    expect(observations).toContain('BEFORE UPDATE OR DELETE ON observations');
    expect(observations).toContain('FOR EACH ROW');
  });

  it('records the quoted post by provider id, not by foreign key', () => {
    // An FK would reject a quote ingested before its target; the provider id
    // lets the link resolve later, in either arrival order, without updating an
    // append-only row.
    expect(observations).toContain('quoted_provider_post_id TEXT');
    expect(observations).not.toContain('quoted_observation_id');
  });

  it('keeps the fields downstream workers filter on relational', () => {
    // TIS ch.2 §8: raw payloads go to JSONB, canonical searchable fields stay
    // columns. Text in particular is what interpretation reads.
    expect(observations).toContain('raw_payload JSONB NOT NULL');
    for (const column of ['provider_post_id', 'author_id', 'published_at', 'text']) {
      expect(observations).toContain(column);
    }
  });

  it('indexes the time columns TIS ch.2 §6 names', () => {
    expect(observations).toContain('ON observations (published_at)');
    expect(observations).toContain('ON observations (ingested_at)');
  });

  it('ties media to an observation and dedupes attachments per post', () => {
    expect(media).toContain('REFERENCES observations (observation_id)');
    expect(media).toContain('UNIQUE (observation_id, provider_media_key)');
  });

  it('leaves the media fields that need the bytes nullable', () => {
    // Media download happens outside the ingest transaction (TIS ch.2 §7,
    // ch.4 §9), so a row exists as a reference before any of these are known.
    for (const column of ['storage_location TEXT', 'sha256 TEXT', 'phash TEXT']) {
      expect(media).toContain(column);
      expect(media).not.toContain(`${column} NOT NULL`);
    }
    expect(media).toContain('source_url TEXT NOT NULL');
  });

  it('does not lock media against the updates that fill it in', () => {
    expect(media).not.toContain('BEFORE UPDATE');
  });
});

describe('market schema', () => {
  const tokens = ddl(MIGRATIONS.find((migration) => migration.id === '0009_tokens'));
  const artwork = ddl(MIGRATIONS.find((migration) => migration.id === '0010_token_artwork'));
  const snapshots = ddl(MIGRATIONS.find((migration) => migration.id === '0011_market_snapshots'));
  const creators = ddl(MIGRATIONS.find((migration) => migration.id === '0012_creator_profiles'));

  it('identifies a token by chain and address, not by the venue it was seen on', () => {
    // A pump.fun token that graduates is discovered again on PumpSwap. Keying on
    // the venue would make that one asset into two, and split its evidence.
    expect(tokens).toContain('UNIQUE (chain, address)');
    expect(tokens).not.toContain('UNIQUE (launch_source, address)');
  });

  it('rejects mutation of a persisted launch', () => {
    // EDS ch.21 §8: raw launch data MUST remain immutable.
    expect(tokens).toContain('BEFORE UPDATE OR DELETE ON tokens');
    expect(tokens).toContain('FOR EACH ROW EXECUTE FUNCTION reject_mutation()');
  });

  it('keeps the launch fields correlation searches on relational', () => {
    // EDS ch.21 §5: a token is searchable before enrichment completes, which is
    // only true if the fields a search filters on are columns.
    expect(tokens).toContain('raw_payload JSONB NOT NULL');
    for (const column of ['chain', 'address', 'ticker', 'name', 'launch_time', 'launch_source']) {
      expect(tokens).toContain(column);
    }
  });

  it('indexes creator launches so history is a query rather than a stored list', () => {
    expect(tokens).toContain('ON tokens (chain, creator_wallet, launch_time)');
    expect(tokens).toContain('WHERE creator_wallet IS NOT NULL');
  });

  it('indexes ticker and name case-insensitively', () => {
    // A narrative says "pepe"; the launch says "PEPE".
    expect(tokens).toContain('ON tokens (chain, LOWER(ticker))');
    expect(tokens).toContain('ON tokens (chain, LOWER(name))');
  });

  it('holds one artwork row per token so re-analysis updates in place', () => {
    expect(artwork).toContain('REFERENCES tokens (token_id)');
    expect(artwork).toContain('UNIQUE (token_id)');
  });

  it('leaves the artwork fields that need the bytes nullable', () => {
    for (const column of ['storage_location TEXT', 'sha256 TEXT', 'phash TEXT', 'ocr_text TEXT']) {
      expect(artwork).toContain(column);
      expect(artwork).not.toContain(`${column} NOT NULL`);
    }
    expect(artwork).toContain('source_url TEXT NOT NULL');
  });

  it('does not lock artwork against the updates that fill it in', () => {
    expect(artwork).not.toContain('BEFORE UPDATE');
  });

  it('declares no embedding column before anything can compute one', () => {
    // TIS ch.9 lists embeddings under enrichment, but they belong to multimodal
    // processing (build order step 4) and need a vector type this schema does not
    // assume. An always-null column reads as "not embedded yet" forever.
    expect(artwork).not.toMatch(/embedding/i);
    expect(artwork).not.toMatch(/vector/i);
  });

  it('makes a snapshot idempotent on its own observation', () => {
    // EDS ch.21 §8: enrichment MUST be idempotent. Re-polling a window already
    // stored must not append a second copy of one source's one observation.
    expect(snapshots).toContain('UNIQUE (token_id, source, observed_at)');
  });

  it('rejects mutation of a stored snapshot', () => {
    // TIS ch.9's append-only snapshots invariant, and what makes ch.13 replay
    // reproducible: a rewritten metric changes the result of a finished backtest.
    expect(snapshots).toContain('BEFORE UPDATE OR DELETE ON market_snapshots');
    expect(snapshots).toContain('FOR EACH ROW EXECUTE FUNCTION reject_mutation()');
  });

  it('stores money as NUMERIC rather than as a float', () => {
    // A float cannot hold a launch price exactly, and market cap compounds the
    // error. NUMERIC costs a string conversion in the repository instead.
    for (const column of ['price_usd', 'liquidity_usd', 'market_cap_usd', 'volume_24h_usd']) {
      expect(snapshots).toMatch(new RegExp(`${column} NUMERIC`));
    }
    expect(snapshots).not.toMatch(/DOUBLE PRECISION|REAL|FLOAT/);
  });

  it('indexes the token timeline newest first', () => {
    expect(snapshots).toContain('ON market_snapshots (token_id, observed_at DESC)');
  });

  it('keys a creator profile on the wallet and leaves it mutable', () => {
    // Current state, not history: the profile is a materialisation of the
    // append-only launch rows, so recomputing it must overwrite rather than append.
    expect(creators).toContain('UNIQUE (chain, creator_wallet)');
    expect(creators).not.toContain('BEFORE UPDATE');
  });

  it('stores no reputation score before anything can compute one', () => {
    // EDS ch.21 §9 puts creator reputation in the Production tier. A placeholder
    // score would be read by correlation as though it meant something.
    expect(creators).not.toMatch(/score|reputation|rug/i);
  });
});

describe('V2 signal and trading schema', () => {
  const signals = ddl(
    MIGRATIONS.find((migration) => migration.id === '0013_signals_and_candidate_resolutions')
  );
  const registry = ddl(
    MIGRATIONS.find((migration) => migration.id === '0014_strategy_registry_and_runs')
  );
  const ledger = ddl(MIGRATIONS.find((migration) => migration.id === '0015_trading_ledger'));

  it('preserves source, receipt, and first-usable signal time independently', () => {
    for (const column of ['occurred_at', 'observed_at', 'available_at']) {
      expect(signals).toContain(`${column} TIMESTAMPTZ NOT NULL`);
    }
    expect(signals).toContain('occurred_at <= observed_at AND observed_at <= available_at');
    expect(signals).toContain('ON signals (available_at, signal_id)');
  });

  it('deduplicates exact source occurrences rather than token mints', () => {
    expect(signals).toContain('UNIQUE (origin_key)');
    expect(signals).toContain('ON signals (provider, reference_id)');
    expect(signals).toContain('ON signals (chain, transaction_signature, instruction_index)');
    expect(signals).toContain('ON signals (chain, transaction_signature, log_index)');
    expect(signals).not.toContain('UNIQUE (token_mint)');
    expect(signals).toContain('num_nonnulls(instruction_index, log_index) = 1');
  });

  it('makes signals and candidate-resolution versions append-only', () => {
    expect(signals).toContain('BEFORE UPDATE OR DELETE ON signals');
    expect(signals).toContain('BEFORE UPDATE OR DELETE ON candidate_resolution_events');
    expect(signals).toContain('UNIQUE (canonical_narrative_id, version)');
  });

  it('pins four immutable policy versions into every strategy combination', () => {
    for (const table of [
      'strategy_entry_versions',
      'strategy_exit_versions',
      'strategy_protection_versions',
      'strategy_risk_versions',
      'strategy_combinations',
    ]) {
      expect(registry).toContain(`BEFORE UPDATE OR DELETE ON ${table}`);
    }
    expect(registry).toContain('UNIQUE (combination_id, config_hash)');
    expect(registry).toContain('strategy_combination_validate_compatibility');
    expect(registry).toContain('selected_entry_id = ANY(allowed_entry_ids)');
  });

  it('fails closed on incompatible or live strategy runs', () => {
    expect(registry).toContain("mode IN ('historical', 'forward_paper')");
    expect(registry).not.toContain("mode IN ('historical', 'forward_paper', 'live')");
    expect(registry).toContain("compatibility_status = 'compatible'");
    expect(registry).toContain('strategy_run_reject_incompatible');
  });

  it('allows one authoritative paper portfolio for each run and combination', () => {
    expect(registry).toContain('UNIQUE (run_id, combination_id)');
    expect(registry).toContain('REFERENCES strategy_runs (run_id, combination_id)');
    expect(registry).toContain('strategy_run_reject_scope_mutation');
    expect(registry).toContain('paper_portfolio_reject_scope_mutation');
  });

  it('stores monetary values as exact numerics throughout the trading ledger', () => {
    const monetarySchema = `${registry}\n${ledger}`;
    expect(monetarySchema).toContain('initial_cash NUMERIC(38, 18)');
    expect(monetarySchema).toContain('quantity NUMERIC(38, 18)');
    expect(monetarySchema).toContain('input_quantity NUMERIC(38, 18)');
    expect(monetarySchema).toContain('open_quantity NUMERIC(38, 18)');
    expect(monetarySchema).not.toMatch(/DOUBLE PRECISION|\bREAL\b|\bFLOAT\b/);
  });

  it('keeps intents and histories immutable while positions remain a projection', () => {
    for (const table of [
      'strategy_decisions',
      'orders',
      'order_events',
      'order_fills',
      'position_events',
    ]) {
      expect(ledger).toContain(`BEFORE UPDATE OR DELETE ON ${table}`);
    }
    expect(ledger).not.toContain('BEFORE UPDATE OR DELETE ON positions');
  });

  it('scopes decision and order idempotency to an isolated portfolio', () => {
    expect(ledger).toContain('UNIQUE (portfolio_id, idempotency_key)');
    expect(ledger).toContain('REFERENCES paper_portfolios (portfolio_id, run_id, combination_id)');
  });
});

describe('token market lifecycle schema', () => {
  const lifecycle = ddl(
    MIGRATIONS.find((migration) => migration.id === '0016_token_market_lifecycle')
  );

  it('stores venue intervals with one current market per token', () => {
    expect(lifecycle).toContain('CREATE TABLE IF NOT EXISTS token_markets');
    expect(lifecycle).toContain('active_until TIMESTAMPTZ');
    expect(lifecycle).toContain('WHERE active_until IS NULL');
    expect(lifecycle).toContain("venue IN ('pump_fun_bonding_curve', 'pumpswap')");
  });

  it('deduplicates exact chain occurrences and permits only interval closure', () => {
    expect(lifecycle).toContain('num_nonnulls(instruction_index, log_index) = 1');
    expect(lifecycle).toContain('token_markets_chain_instruction_idx');
    expect(lifecycle).toContain('token_markets_chain_log_idx');
    expect(lifecycle).toContain('token_market_allow_close_only');
  });

  it('makes market snapshot identity venue-aware', () => {
    expect(lifecycle).toContain('UNIQUE (token_id, venue, source, observed_at)');
    expect(lifecycle).toContain('market_snapshots_venue_timeline_idx');
  });
});

describe('REQUIRED_TABLES', () => {
  it('lists each required table exactly once', () => {
    expect(new Set(REQUIRED_TABLES).size).toBe(REQUIRED_TABLES.length);
  });

  it('contains no table that no migration creates', () => {
    const schema = MIGRATIONS.map(ddl).join('\n');
    const unknown = REQUIRED_TABLES.filter(
      (table) => !schema.includes(`CREATE TABLE IF NOT EXISTS ${table}`)
    );
    expect(unknown).toEqual([]);
  });
});
