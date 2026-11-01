import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Migrator, REQUIRED_TABLES } from './migrations';
import { createTestDatabase, hasTestDatabase, TestDatabase } from './testing';

/**
 * The sibling unit test only string-matches the DDL, which cannot catch a
 * statement Postgres rejects or a constraint that does not behave as written.
 * These cases apply the migrations for real.
 */
describe.skipIf(!hasTestDatabase())('Migrator against Postgres', () => {
  let harness: TestDatabase;

  beforeAll(async () => {
    // createTestDatabase already runs migrate() once, so the harness itself is
    // the first assertion: a broken migration fails setup.
    harness = await createTestDatabase('migrations');
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  it('creates every required table in the isolated schema', async () => {
    const missing = await new Migrator(harness.db).verifySchema();
    expect(missing).toEqual({ kind: 'success', value: [] });
  });

  it('records each migration exactly once', async () => {
    const applied = await harness.db.query<{ id: string }>(
      'SELECT id FROM schema_migrations ORDER BY id'
    );
    if (applied.kind === 'failure') {
      throw applied.error;
    }
    const ids = applied.value.map((row) => row.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toContain('0001_migrations_table');
    expect(ids).toContain('0005_job_history');
  });

  it('is idempotent: a second migrate applies nothing new', async () => {
    const second = await new Migrator(harness.db).migrate();
    expect(second).toEqual({ kind: 'success', value: [] });

    const missing = await new Migrator(harness.db).verifySchema();
    expect(missing).toEqual({ kind: 'success', value: [] });
  });

  it('reports the tables it created as missing on an empty schema', async () => {
    // Guards against verifySchema returning [] for the wrong reason.
    expect(REQUIRED_TABLES.length).toBeGreaterThan(0);
    const dropped = await harness.db.query('DROP TABLE IF EXISTS job_history');
    expect(dropped.kind).toBe('success');

    const missing = await new Migrator(harness.db).verifySchema();
    expect(missing).toEqual({ kind: 'success', value: ['job_history'] });

    // Restore for any later suite sharing this schema.
    const repaired = await harness.db.query('DELETE FROM schema_migrations WHERE id = $1', [
      '0005_job_history',
    ]);
    expect(repaired.kind).toBe('success');
    const remigrated = await new Migrator(harness.db).migrate();
    expect(remigrated).toEqual({ kind: 'success', value: ['0005_job_history'] });
  });

  it('rejects a duplicate idempotency key within one queue', async () => {
    const insert = async (queueName: string): Promise<'ok' | 'rejected'> => {
      const result = await harness.db.query(
        `INSERT INTO durable_jobs (
           job_id, queue_name, payload, payload_version, status,
           max_attempts, idempotency_key
         ) VALUES (gen_random_uuid(), $1, '{}'::jsonb, '1', 'pending', 3, 'dup-key')`,
        [queueName]
      );
      return result.kind === 'success' ? 'ok' : 'rejected';
    };

    expect(await insert('ingestion')).toBe('ok');
    expect(await insert('ingestion')).toBe('rejected');
    // The same key in a different queue is a different job.
    expect(await insert('scoring')).toBe('ok');

    const cleaned = await harness.db.query("DELETE FROM durable_jobs WHERE idempotency_key = 'dup-key'");
    expect(cleaned.kind).toBe('success');
  });

  /**
   * The observation tables carry behaviour that only a real database exercises:
   * a unique constraint standing in for ch.4 §7 idempotency, and a trigger
   * standing in for ch.4 §11's "immutable persistence is verified".
   */
  describe('observations and media', () => {
    const insertObservation = async (
      postId: string,
      columns: Record<string, string | null> = {}
    ): Promise<'ok' | 'rejected'> => {
      const extra = Object.keys(columns);
      const result = await harness.db.query(
        `INSERT INTO observations (
           observation_id, provider, provider_post_id, author_id,
           published_at, raw_payload${extra.map((name) => `, ${name}`).join('')}
         ) VALUES (
           gen_random_uuid(), 'x', $1, 'author-1', NOW(), '{}'::jsonb
           ${extra.map((_, index) => `, $${index + 2}`).join('')}
         )`,
        [postId, ...extra.map((name) => columns[name] ?? null)]
      );
      return result.kind === 'success' ? 'ok' : 'rejected';
    };

    const observationIdFor = async (postId: string): Promise<string> => {
      const found = await harness.db.query<{ observation_id: string }>(
        'SELECT observation_id FROM observations WHERE provider = $1 AND provider_post_id = $2',
        ['x', postId]
      );
      if (found.kind === 'failure') {
        throw found.error;
      }
      return found.value[0]?.observation_id as string;
    };

    it('prevents the same provider post becoming two observations', async () => {
      expect(await insertObservation('post-dup')).toBe('ok');
      expect(await insertObservation('post-dup')).toBe('rejected');
    });

    it('refuses to update or delete a persisted observation', async () => {
      expect(await insertObservation('post-immutable')).toBe('ok');

      const updated = await harness.db.query(
        "UPDATE observations SET text = 'edited' WHERE provider_post_id = 'post-immutable'"
      );
      expect(updated.kind).toBe('failure');

      const deleted = await harness.db.query(
        "DELETE FROM observations WHERE provider_post_id = 'post-immutable'"
      );
      expect(deleted.kind).toBe('failure');

      const survivors = await harness.db.query<{ count: string }>(
        "SELECT COUNT(*) AS count FROM observations WHERE provider_post_id = 'post-immutable'"
      );
      expect(survivors).toEqual({ kind: 'success', value: [{ count: '1' }] });
    });

    it('links a quote to its target whichever arrives first', async () => {
      // The reason the column holds a provider id rather than a foreign key. An
      // FK would have rejected the first insert below outright.
      expect(await insertObservation('quote-early', { quoted_provider_post_id: 'target-late' })).toBe(
        'ok'
      );
      expect(await insertObservation('target-late')).toBe('ok');

      const resolved = await harness.db.query<{ quoted: string | null }>(
        `SELECT target.observation_id AS quoted
           FROM observations AS quote
           LEFT JOIN observations AS target
             ON target.provider = quote.provider
            AND target.provider_post_id = quote.quoted_provider_post_id
          WHERE quote.provider_post_id = 'quote-early'`
      );
      if (resolved.kind === 'failure') {
        throw resolved.error;
      }
      expect(resolved.value[0]?.quoted).toBe(await observationIdFor('target-late'));
    });

    it('requires media to belong to an observation that exists', async () => {
      const orphan = await harness.db.query(
        `INSERT INTO media (media_id, observation_id, provider_media_key, media_type, source_url)
         VALUES (gen_random_uuid(), gen_random_uuid(), 'key-1', 'image', 'https://example.invalid/1')`
      );
      expect(orphan.kind).toBe('failure');
    });

    it('persists a media reference once and lets the download fill it in', async () => {
      expect(await insertObservation('post-media')).toBe('ok');
      const observationId = await observationIdFor('post-media');

      const insertMedia = async (): Promise<'ok' | 'rejected'> => {
        const result = await harness.db.query(
          `INSERT INTO media (media_id, observation_id, provider_media_key, media_type, source_url)
           VALUES (gen_random_uuid(), $1, 'key-1', 'video', 'https://example.invalid/1')`,
          [observationId]
        );
        return result.kind === 'success' ? 'ok' : 'rejected';
      };

      expect(await insertMedia()).toBe('ok');
      // Re-ingesting the post must not attach the same media twice.
      expect(await insertMedia()).toBe('rejected');

      // Media is mutable by design: the bytes arrive after the ingest
      // transaction has committed (TIS ch.2 §7, ch.4 §9).
      const downloaded = await harness.db.query(
        `UPDATE media
            SET storage_location = '/tmp/working/key-1.mp4', sha256 = 'abc', duration_ms = 4200
          WHERE observation_id = $1`,
        [observationId]
      );
      expect(downloaded.kind).toBe('success');
    });

    it('can still be truncated for test reset', async () => {
      // The append-only trigger covers UPDATE and DELETE only, which is what
      // leaves this suite a way to clean up after itself.
      const cleaned = await harness.db.query('TRUNCATE observations CASCADE');
      expect(cleaned.kind).toBe('success');

      const remaining = await harness.db.query<{ count: string }>(
        'SELECT COUNT(*) AS count FROM media'
      );
      expect(remaining).toEqual({ kind: 'success', value: [{ count: '0' }] });
    });
  });

  /**
   * The market tables, whose invariants are all database-enforced: identity by
   * (chain, address) so a graduated token stays one token, immutable launches and
   * snapshots per EDS ch.21 §8, and a mutable enrichment row per token so
   * re-analysis overwrites instead of accumulating.
   */
  describe('tokens and enrichment', () => {
    // Every optional column is a parameter rather than an interpolated name, so an
    // override cannot silently name a column the literal list already fills and
    // turn a constraint test into a syntax-error test.
    const insertToken = async (
      address: string,
      overrides: { chain?: string; launchSource?: string; creatorWallet?: string } = {}
    ): Promise<'ok' | 'rejected'> => {
      const result = await harness.db.query(
        `INSERT INTO tokens (
           token_id, chain, address, ticker, name,
           launch_time, launch_source, creator_wallet, raw_payload
         ) VALUES (gen_random_uuid(), $1, $2, 'TICK', 'Token', NOW(), $3, $4, '{}'::jsonb)`,
        [
          overrides.chain ?? 'solana',
          address,
          overrides.launchSource ?? 'pump_fun',
          overrides.creatorWallet ?? null,
        ]
      );
      return result.kind === 'success' ? 'ok' : 'rejected';
    };

    const tokenIdFor = async (address: string, chain = 'solana'): Promise<string> => {
      const found = await harness.db.query<{ token_id: string }>(
        'SELECT token_id FROM tokens WHERE chain = $1 AND address = $2',
        [chain, address]
      );
      if (found.kind === 'failure') {
        throw found.error;
      }
      return found.value[0]?.token_id as string;
    };

    it('prevents one on-chain address becoming two tokens', async () => {
      // Rediscovery is normal: the same launch is seen on pump.fun and again on
      // PumpSwap after it graduates, and the second sighting must not fork the asset.
      expect(await insertToken('addr-dup')).toBe('ok');
      expect(await insertToken('addr-dup')).toBe('rejected');
      expect(await insertToken('addr-dup', { launchSource: 'pumpswap' })).toBe('rejected');
    });

    it('treats the same address on another chain as another token', async () => {
      expect(await insertToken('addr-shared', { chain: 'solana' })).toBe('ok');
      expect(await insertToken('addr-shared', { chain: 'base' })).toBe('ok');
    });

    it('refuses to update or delete a persisted launch', async () => {
      expect(await insertToken('addr-immutable')).toBe('ok');

      const updated = await harness.db.query(
        "UPDATE tokens SET ticker = 'EDITED' WHERE address = 'addr-immutable'"
      );
      expect(updated.kind).toBe('failure');
      if (updated.kind === 'failure') {
        expect(updated.error.message).toContain('append-only');
      }

      const deleted = await harness.db.query("DELETE FROM tokens WHERE address = 'addr-immutable'");
      expect(deleted.kind).toBe('failure');

      const survivors = await harness.db.query<{ ticker: string }>(
        "SELECT ticker FROM tokens WHERE address = 'addr-immutable'"
      );
      expect(survivors).toEqual({ kind: 'success', value: [{ ticker: 'TICK' }] });
    });

    it('refuses enrichment that references a token that does not exist', async () => {
      // Enrichment is enqueued by token id after discovery commits. An orphan row
      // would be enrichment for nothing, and would survive as a phantom token in
      // any query that starts from the enrichment side.
      const orphanArtwork = await harness.db.query(
        `INSERT INTO token_artwork (artwork_id, token_id, source_url)
         VALUES (gen_random_uuid(), gen_random_uuid(), 'https://example.invalid/art.png')`
      );
      expect(orphanArtwork.kind).toBe('failure');

      const orphanSnapshot = await harness.db.query(
        `INSERT INTO market_snapshots (
           snapshot_id, token_id, observed_at, source, raw_payload
         ) VALUES (gen_random_uuid(), gen_random_uuid(), NOW(), 'pumpswap', '{}'::jsonb)`
      );
      expect(orphanSnapshot.kind).toBe('failure');
    });

    it('holds one artwork row per token and lets analysis fill it in', async () => {
      expect(await insertToken('addr-art')).toBe('ok');
      const tokenId = await tokenIdFor('addr-art');

      const insertArtwork = async (): Promise<'ok' | 'rejected'> => {
        const result = await harness.db.query(
          `INSERT INTO token_artwork (artwork_id, token_id, source_url)
           VALUES (gen_random_uuid(), $1, 'https://example.invalid/art.png')`,
          [tokenId]
        );
        return result.kind === 'success' ? 'ok' : 'rejected';
      };

      expect(await insertArtwork()).toBe('ok');
      // Re-discovering the token must not give it a second artwork row.
      expect(await insertArtwork()).toBe('rejected');

      // Mutable by design: the analysis needs bytes that arrive after discovery
      // has committed, exactly as media does.
      const analysed = await harness.db.query(
        `UPDATE token_artwork
            SET sha256 = 'abc', phash = 'ffff0000', ocr_text = 'GM', analysed_at = NOW()
          WHERE token_id = $1`,
        [tokenId]
      );
      expect(analysed.kind).toBe('success');
    });

    it('keeps one snapshot per source per instant', async () => {
      expect(await insertToken('addr-snap')).toBe('ok');
      const tokenId = await tokenIdFor('addr-snap');
      const observedAt = '2026-08-19T10:00:00.000Z';

      const insertSnapshot = async (source: string, at = observedAt): Promise<'ok' | 'rejected'> => {
        const result = await harness.db.query(
          `INSERT INTO market_snapshots (
             snapshot_id, token_id, observed_at, source, liquidity_usd, raw_payload
           ) VALUES (gen_random_uuid(), $1, $2, $3, 1000, '{}'::jsonb)`,
          [tokenId, at, source]
        );
        return result.kind === 'success' ? 'ok' : 'rejected';
      };

      expect(await insertSnapshot('pumpswap')).toBe('ok');
      // A re-poll of a window already stored is the case the key exists for.
      expect(await insertSnapshot('pumpswap')).toBe('rejected');
      // Two sources may legitimately describe the same instant differently.
      expect(await insertSnapshot('dexscreener')).toBe('ok');
      expect(await insertSnapshot('pumpswap', '2026-08-19T10:01:00.000Z')).toBe('ok');
    });

    it('refuses to rewrite a stored snapshot', async () => {
      expect(await insertToken('addr-snap-immutable')).toBe('ok');
      const tokenId = await tokenIdFor('addr-snap-immutable');
      const inserted = await harness.db.query(
        `INSERT INTO market_snapshots (
           snapshot_id, token_id, observed_at, source, liquidity_usd, raw_payload
         ) VALUES (gen_random_uuid(), $1, NOW(), 'pumpswap', 1000, '{}'::jsonb)`,
        [tokenId]
      );
      expect(inserted.kind).toBe('success');

      const rewritten = await harness.db.query(
        'UPDATE market_snapshots SET liquidity_usd = 2000 WHERE token_id = $1',
        [tokenId]
      );
      expect(rewritten.kind).toBe('failure');
      if (rewritten.kind === 'failure') {
        expect(rewritten.error.message).toContain('append-only');
      }
    });

    it('returns money as an exact string rather than a float', async () => {
      // The driver hands NUMERIC back as a string precisely so it does not have to
      // round, which means the repository has to convert deliberately. Pinned here
      // because the JSONB read path already proved that assuming a driver
      // representation is how a read path breaks.
      expect(await insertToken('addr-precise')).toBe('ok');
      const tokenId = await tokenIdFor('addr-precise');
      const inserted = await harness.db.query(
        `INSERT INTO market_snapshots (
           snapshot_id, token_id, observed_at, source, price_usd, market_cap_usd, raw_payload
         ) VALUES (gen_random_uuid(), $1, NOW(), 'pumpswap', 0.000000000123456789, 1234567.891234, '{}'::jsonb)`,
        [tokenId]
      );
      expect(inserted.kind).toBe('success');

      const read = await harness.db.query<{ price_usd: string; market_cap_usd: string }>(
        'SELECT price_usd, market_cap_usd FROM market_snapshots WHERE token_id = $1',
        [tokenId]
      );
      if (read.kind === 'failure') {
        throw read.error;
      }
      const row = read.value[0] as { price_usd: string; market_cap_usd: string };
      expect(typeof row.price_usd).toBe('string');
      expect(row.price_usd).toBe('0.000000000123456789');
      expect(row.market_cap_usd).toBe('1234567.891234');
    });

    it('keys a creator profile on its wallet and lets it be recomputed', async () => {
      const upsert = async (launchCount: number): Promise<'ok' | 'rejected'> => {
        const result = await harness.db.query(
          `INSERT INTO creator_profiles (
             creator_profile_id, chain, creator_wallet, launch_count,
             first_launch_at, last_launch_at, analysed_at
           ) VALUES (gen_random_uuid(), 'solana', 'wallet-1', $1, NOW(), NOW(), NOW())`,
          [launchCount]
        );
        return result.kind === 'success' ? 'ok' : 'rejected';
      };

      expect(await upsert(1)).toBe('ok');
      expect(await upsert(2)).toBe('rejected');

      // Recomputing overwrites: the profile is current state derived from the
      // append-only launches, not a history of its own.
      const recomputed = await harness.db.query(
        `UPDATE creator_profiles SET launch_count = 2, updated_at = NOW()
          WHERE chain = 'solana' AND creator_wallet = 'wallet-1'`
      );
      expect(recomputed.kind).toBe('success');
    });
  });
});
