import { createHash, randomUUID } from 'node:crypto';
import {
  DatabasePool,
  Executor,
  PostgresResearchStore,
  clientExecutor,
} from '@nit/database-public';
import { ResearchRecord, contentHash, evidenceHash } from '@nit/signals';
import { NarrativeRuntimeConfig, NarrativeRuntimeSchema } from './config';
import {
  SourceText,
  TextInterpreter,
  normalized,
  validateExtraction,
  TextExtraction,
  subjectKey,
} from './contracts';
import { InterpretedSource, NarrativeGroup, TokenFacts, groupSources, scoreGroup } from './scoring';

const ENGINE_VERSION = 'live-text-v1';
// Do not materialize arbitrary provider JSON a second time during interpretation.
const SOURCE_FIELDS = `o.observation_id,o.provider,o.provider_post_id,o.author_id,o.published_at,o.ingested_at,o.text,o.quoted_provider_post_id,
  jsonb_build_object('referenced_tweets',
    (SELECT COALESCE(jsonb_agg(jsonb_build_object('id',LEFT(x.value->>'id',100))),'[]'::jsonb)
      FROM jsonb_array_elements(CASE WHEN jsonb_typeof(o.raw_payload->'referenced_tweets')='array'
        THEN o.raw_payload->'referenced_tweets' ELSE '[]'::jsonb END) WITH ORDINALITY x(value,n) WHERE x.n<=32),
    'referenced_posts',
    (SELECT COALESCE(jsonb_agg(jsonb_build_object('id',LEFT(x.value->>'id',100))),'[]'::jsonb)
      FROM jsonb_array_elements(CASE WHEN jsonb_typeof(o.raw_payload->'referenced_posts')='array'
        THEN o.raw_payload->'referenced_posts' ELSE '[]'::jsonb END) WITH ORDINALITY x(value,n) WHERE x.n<=32)) AS raw_payload`;
async function rows<T>(db: Executor, sql: string, values: unknown[] = []): Promise<T[]> {
  const result = await db.query<T>(sql, values);
  if (result.kind === 'failure') throw result.error;
  return result.value;
}
function stableId(value: unknown): string {
  const data = createHash('sha256').update(evidenceHash(value)).digest().subarray(0, 16);
  data[6] = ((data[6] as number) & 15) | 80;
  data[8] = ((data[8] as number) & 63) | 128;
  const s = data.toString('hex');
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}
interface ObservationRow {
  observation_id: string;
  provider: string;
  provider_post_id: string;
  author_id: string;
  published_at: Date;
  ingested_at: Date;
  text: string | null;
  raw_payload: Record<string, unknown>;
  quoted_provider_post_id: string | null;
}
function sourceOf(row: ObservationRow): SourceText {
  const refs: string[] = [];
  if (row.quoted_provider_post_id !== null) refs.push(row.quoted_provider_post_id);
  for (const key of ['referenced_tweets', 'referenced_posts']) {
    const raw = row.raw_payload[key];
    if (!Array.isArray(raw)) continue;
    for (const item of raw.slice(0, 32) as unknown[]) {
      if (typeof item !== 'object' || item === null) continue;
      const id = (item as Record<string, unknown>)['id'];
      if (typeof id === 'string' && id.length <= 100) refs.push(id);
    }
  }
  return {
    observationId: row.observation_id,
    provider: row.provider,
    postId: row.provider_post_id,
    authorId: row.author_id,
    text: row.text ?? '',
    publishedAt: row.published_at,
    ingestedAt: row.ingested_at,
    derivativeIds: [...new Set(refs)],
  };
}
interface Descriptor {
  key: string;
  subject: NarrativeGroup['subject'];
  category: string;
  catalystKey: string;
  startsAt: string;
  expiresAt: string;
}
interface StoredReport {
  group: Descriptor;
  score: ReturnType<typeof scoreGroup>;
  coverage: {
    complete: boolean;
    universeOverflow: boolean;
    sourceOverflow: boolean;
    evidenceOverflow: boolean;
  };
}
export interface NarrativeCycle {
  claimed: number;
  captured: number;
  failed: number;
  published: number;
  incomplete: boolean;
  busy: boolean;
}

/** A bounded producer. No orders, position access, strategy updates or adaptive tool use. */
export class LiveNarrativeRuntime {
  readonly policyHash: string;
  readonly extractorHash: string;
  private readonly config: NarrativeRuntimeConfig;
  constructor(
    private readonly db: DatabasePool,
    private readonly interpreter: TextInterpreter,
    config: NarrativeRuntimeConfig,
    private readonly clock: () => Date = (): Date => new Date()
  ) {
    this.config = NarrativeRuntimeSchema.parse(config);
    if (
      interpreter.identity.isToyImplementation ||
      interpreter.kind !== this.config.interpreter.kind
    )
      throw new Error('Production narrative runtime refuses toy or mismatched interpreters');
    if (
      this.config.interpreter.kind === 'gateway' &&
      interpreter.identity.version !== this.config.interpreter.modelVersion
    )
      throw new Error('Narrative interpreter model version mismatch');
    this.extractorHash = evidenceHash({
      identity: interpreter.identity,
      endpoint:
        this.config.interpreter.kind === 'gateway' ? this.config.interpreter.endpoint : null,
      allowedOrigin:
        this.config.interpreter.kind === 'gateway' ? this.config.interpreter.allowedOrigin : null,
    });
    this.policyHash = evidenceHash({
      engine: ENGINE_VERSION,
      config: this.config,
      extractor: this.extractorHash,
    });
  }

  async cycle(signal: AbortSignal = new AbortController().signal): Promise<NarrativeCycle> {
    const summary: NarrativeCycle = {
      claimed: 0,
      captured: 0,
      failed: 0,
      published: 0,
      incomplete: false,
      busy: false,
    };
    const client = await this.db.getClient();
    if (client.kind === 'failure') throw client.error;
    let locked = false;
    try {
      const acquired = await client.value.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(hashtext(current_schema()),724902) AS locked'
      );
      locked = acquired.rows[0]?.locked === true;
      if (!locked) return { ...summary, busy: true };
      await this.pin(this.policyHash, {
        engine: ENGINE_VERSION,
        config: this.config,
        extractor: this.extractorHash,
      });
      await this.pin(this.extractorHash, {
        identity: this.interpreter.identity,
        endpoint:
          this.config.interpreter.kind === 'gateway' ? this.config.interpreter.endpoint : null,
        allowedOrigin:
          this.config.interpreter.kind === 'gateway' ? this.config.interpreter.allowedOrigin : null,
      });
      const start = this.clock();
      // Unknown outcomes retain their reservation and cannot be automatically called again.
      await rows(
        this.db,
        `INSERT INTO nit_text_results(attempt_id,status,payload,available_at)
        SELECT a.attempt_id,'unknown',NULL,$2 FROM nit_text_attempts a
        WHERE a.policy_hash=$1 AND a.deadline_at<=$2 AND NOT EXISTS
          (SELECT 1 FROM nit_text_results r WHERE r.attempt_id=a.attempt_id) ON CONFLICT DO NOTHING`,
        [this.extractorHash, start]
      );
      const earliest = new Date(
        start.getTime() - this.config.policy.episodeWindowMs - this.config.policy.watchDurationMs
      );
      const pending = await rows<ObservationRow>(
        this.db,
        `SELECT ${SOURCE_FIELDS} FROM observations o
        WHERE o.ingested_at<=$2 AND o.published_at<=$2 AND o.published_at>=$3
        AND NOT EXISTS(SELECT 1 FROM nit_text_attempts a WHERE a.observation_id=o.observation_id AND a.policy_hash=$1)
        ORDER BY o.ingested_at,o.observation_id LIMIT $4`,
        [this.extractorHash, start, earliest, this.config.observationsPerCycle]
      );
      if (pending.length > 0) await this.health(true);
      for (const row of pending) {
        if (signal.aborted) break;
        const source = sourceOf(row);
        await this.artifact(source);
        const attempt = await this.claim(source);
        if (attempt === null) {
          summary.incomplete = true;
          break;
        }
        summary.claimed++;
        const abort = new AbortController();
        const timer = setTimeout(() => abort.abort(), this.config.timeoutMs);
        let status: 'captured' | 'unavailable' | 'invalid' | 'unknown' = 'unknown';
        let extraction: TextExtraction | null = null;
        try {
          if (source.text.trim() === '') status = 'unavailable';
          else {
            let raw: unknown;
            try {
              raw = await this.boundedInterpret(source, AbortSignal.any([signal, abort.signal]));
            } catch {
              status = 'unknown';
              throw new Error('Interpretation unavailable');
            }
            try {
              extraction = validateExtraction(raw, source);
              if (
                this.config.interpreter.kind === 'catalog' &&
                extraction.topics.some((t) => t.stance !== 'uncertain')
              )
                throw new Error('Catalog interpretation cannot claim semantic confirmation');
              status = 'captured';
            } catch {
              status = 'invalid';
            }
          }
        } catch {
          /* Outcome is recorded without provider bodies or credential-bearing errors. */
        } finally {
          clearTimeout(timer);
        }
        const finished = this.clock();
        if (
          finished.getTime() >= attempt.deadline.getTime() ||
          signal.aborted ||
          abort.signal.aborted
        ) {
          status = 'unknown';
          extraction = null;
        }
        await rows(
          this.db,
          `INSERT INTO nit_text_results(attempt_id,status,payload,available_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
          [
            attempt.id,
            status,
            extraction === null
              ? null
              : JSON.stringify({ extraction, contentKey: contentHash(normalized(source.text)) }),
            finished,
          ]
        );
        if (status === 'captured') summary.captured++;
        else summary.failed++;
      }
      if (!signal.aborted) summary.published = await this.refresh(earliest, summary);
      if (signal.aborted) summary.incomplete = true;
      await this.health(summary.incomplete);
      return summary;
    } finally {
      try {
        if (locked)
          await client.value.query('SELECT pg_advisory_unlock(hashtext(current_schema()),724902)');
      } finally {
        client.value.release();
      }
    }
  }

  private async boundedInterpret(source: SourceText, signal: AbortSignal): Promise<unknown> {
    if (signal.aborted) throw new Error('Interpretation interrupted');
    let rejectStop: (() => void) | undefined;
    const stopped = new Promise<never>((_resolve, reject) => {
      rejectStop = (): void => reject(new Error('Interpretation interrupted'));
      if (signal.aborted) rejectStop();
      else signal.addEventListener('abort', rejectStop, { once: true });
    });
    try {
      return await Promise.race([this.interpreter.interpret(source, signal), stopped]);
    } finally {
      if (rejectStop !== undefined) signal.removeEventListener('abort', rejectStop);
    }
  }

  private async pin(hash: string, value: unknown): Promise<void> {
    await rows(
      this.db,
      'INSERT INTO nit_text_policies(policy_hash,configuration,created_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING',
      [hash, JSON.stringify(value), this.clock()]
    );
    const stored = await rows<{ configuration: unknown }>(
      this.db,
      'SELECT configuration FROM nit_text_policies WHERE policy_hash=$1',
      [hash]
    );
    if (evidenceHash(stored[0]?.configuration) !== evidenceHash(value))
      throw new Error('Pinned narrative configuration mismatch');
  }
  private async health(incomplete: boolean): Promise<void> {
    const now = this.clock();
    await rows(
      this.db,
      `INSERT INTO nit_text_health(runtime_key,policy_hash,checked_at,expires_at,incomplete)
      VALUES ('live-text-v1',$1,$2,$3,$4) ON CONFLICT(runtime_key) DO UPDATE SET policy_hash=EXCLUDED.policy_hash,
        checked_at=EXCLUDED.checked_at,expires_at=EXCLUDED.expires_at,incomplete=EXCLUDED.incomplete`,
      [
        this.policyHash,
        now,
        new Date(now.getTime() + 2 * this.config.intervalMs + this.config.timeoutMs),
        incomplete,
      ]
    );
  }
  private async claim(source: SourceText): Promise<{ id: string; deadline: Date } | null> {
    const now = this.clock();
    if (
      this.config.interpreter.kind === 'gateway' &&
      new Date(this.config.interpreter.expiresAt) <= now
    )
      return null;
    const cost =
      this.config.interpreter.kind === 'gateway' ? this.config.interpreter.maxChargeMicros : 0;
    const claimed = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const reserved = await rows(
        db,
        `INSERT INTO nit_text_budgets(day,requests,reserved_cost_micros)
        SELECT $1::date,1,$2::bigint WHERE $3::bigint>=1 AND $2::bigint<=$4::bigint
        ON CONFLICT(day) DO UPDATE SET requests=nit_text_budgets.requests+1,reserved_cost_micros=nit_text_budgets.reserved_cost_micros+$2::bigint
        WHERE nit_text_budgets.requests<$3::bigint AND nit_text_budgets.reserved_cost_micros+$2::bigint<=$4::bigint RETURNING day`,
        [
          now.toISOString().slice(0, 10),
          cost,
          this.config.maxRequestsPerDay,
          this.config.maxCostMicrosPerDay,
        ]
      );
      if (reserved.length === 0) return null;
      const id = randomUUID();
      const deadline = new Date(now.getTime() + this.config.timeoutMs);
      await rows(
        db,
        `INSERT INTO nit_text_attempts(attempt_id,observation_id,policy_hash,interpreter_hash,runtime_policy_hash,input_hash,started_at,deadline_at,reserved_cost_micros)
        VALUES ($1,$2,$3,$3,$4,$5,$6,$7,$8)`,
        [
          id,
          source.observationId,
          this.extractorHash,
          this.policyHash,
          evidenceHash({ text: source.text, observationId: source.observationId }),
          now,
          deadline,
          cost,
        ]
      );
      return { id, deadline };
    });
    if (claimed.kind === 'failure') throw claimed.error;
    return claimed.value;
  }
  private async artifact(source: SourceText): Promise<void> {
    const id = stableId(['artifact', source.observationId]);
    const research = new PostgresResearchStore(this.db);
    if ((await research.at(id, this.clock())) !== null) return;
    const record: ResearchRecord = {
      recordId: id,
      kind: 'artifact',
      scope: `auto-text:${source.observationId}`,
      version: 1,
      occurredAt: source.publishedAt.toISOString(),
      observedAt: source.ingestedAt.toISOString(),
      availableAt: source.ingestedAt.toISOString(),
      producerVersion: ENGINE_VERSION,
      evidenceIds: [],
      data: {
        provider: source.provider,
        sourceId: source.postId,
        authorId: source.authorId,
        contentHash: contentHash(source.text),
        normalizedContentHash: contentHash(normalized(source.text)),
        text: source.text,
        sourceUrl:
          source.provider === 'x'
            ? `https://x.com/i/status/${encodeURIComponent(source.postId)}`
            : `https://t.me/#nit-source=${encodeURIComponent(`${source.authorId}/${source.postId}`)}`,
        derivativeOf: null,
        independenceEligible: source.derivativeIds.length === 0,
        media: [],
      },
    };
    const saved = await research.append(record);
    if (saved.kind === 'failure') throw saved.error;
  }

  private async refresh(earliest: Date, summary: NarrativeCycle): Promise<number> {
    const now = this.clock();
    const indexReady = await this.indexTokens(now);
    const missing = await rows<{ count: string }>(
      this.db,
      `SELECT COUNT(*)::text AS count FROM observations o
      WHERE o.ingested_at<=$2 AND o.published_at<=$2 AND o.published_at>=$3 AND NOT EXISTS
      (SELECT 1 FROM nit_text_attempts a JOIN nit_text_results r ON r.attempt_id=a.attempt_id
        WHERE a.observation_id=o.observation_id AND a.policy_hash=$1 AND r.status='captured' AND r.available_at<=$2)`,
      [this.extractorHash, now, earliest]
    );
    const captured = await rows<
      ObservationRow & { payload: { extraction: TextExtraction; contentKey: string } }
    >(
      this.db,
      `SELECT ${SOURCE_FIELDS},r.payload FROM observations o JOIN nit_text_attempts a ON a.observation_id=o.observation_id
       JOIN nit_text_results r ON r.attempt_id=a.attempt_id WHERE a.policy_hash=$1 AND r.status='captured'
       AND r.available_at<=$2 AND o.published_at>=$3 AND o.ingested_at<=$2 ORDER BY o.published_at DESC,o.observation_id LIMIT $4`,
      [this.extractorHash, now, earliest, this.config.sourceLimit + 1]
    );
    const sourceOverflow = captured.length > this.config.sourceLimit;
    const keys = captured.map((s) => s.payload.contentKey);
    const first = await rows<{ key: string; first_at: Date }>(
      this.db,
      `SELECT r.payload->>'contentKey' AS key,MIN(o.published_at) AS first_at
      FROM nit_text_results r JOIN nit_text_attempts a ON a.attempt_id=r.attempt_id JOIN observations o ON o.observation_id=a.observation_id
      WHERE r.status='captured' AND r.available_at<=$2 AND r.payload->>'contentKey'=ANY($1::text[]) GROUP BY r.payload->>'contentKey'`,
      [keys, now]
    );
    const inputs: InterpretedSource[] = captured.slice(0, this.config.sourceLimit).map((row) => ({
      source: sourceOf(row),
      artifactId: stableId(['artifact', row.observation_id]),
      extraction: validateExtraction(row.payload.extraction, sourceOf(row)),
      novel:
        (first.find((f) => f.key === row.payload.contentKey)?.first_at.getTime() ?? 0) >=
        Math.floor(row.published_at.getTime() / this.config.policy.episodeWindowMs) *
          this.config.policy.episodeWindowMs,
    }));
    const groups = new Map(groupSources(inputs, this.config.policy, now).map((g) => [g.key, g]));
    const prior = await rows<{ report: StoredReport }>(
      this.db,
      `SELECT report FROM
      (SELECT DISTINCT ON(episode_id) report,state FROM nit_narrative_reports ORDER BY episode_id,available_at DESC,report_id DESC) r
      WHERE state<>'closed' LIMIT $1`,
      [this.config.episodeLimit + 1]
    );
    for (const row of prior) {
      const d = row.report.group;
      if (!groups.has(d.key))
        groups.set(d.key, {
          ...d,
          startsAt: new Date(d.startsAt),
          expiresAt: new Date(d.expiresAt),
          sources: [],
        });
    }
    const complete =
      missing[0]?.count === '0' &&
      !sourceOverflow &&
      indexReady &&
      groups.size <= this.config.episodeLimit &&
      !summary.incomplete;
    summary.incomplete = !complete;
    let published = 0;
    for (const group of [...groups.values()].slice(0, this.config.episodeLimit)) {
      const closed = group.expiresAt <= now;
      const evidenceOverflow = group.sources.length > 64;
      const retrieved =
        complete && !closed && !evidenceOverflow
          ? await this.retrieveTokens(group, now)
          : { tokens: [], overflow: false, ready: indexReady };
      const universeOverflow = retrieved.overflow;
      const groupComplete = complete && retrieved.ready && !universeOverflow;
      if (!groupComplete) summary.incomplete = true;
      const score = scoreGroup(group, retrieved.tokens, this.config.policy, now, groupComplete);
      if (!groupComplete || closed || score.overflow || evidenceOverflow) {
        score.candidates = [];
        score.status = closed ? 'observed_nonconfirming' : 'unavailable';
      }
      const report: StoredReport = {
        group: {
          key: group.key,
          subject: group.subject,
          category: group.category,
          catalystKey: group.catalystKey,
          startsAt: group.startsAt.toISOString(),
          expiresAt: group.expiresAt.toISOString(),
        },
        score,
        coverage: { complete: groupComplete, sourceOverflow, universeOverflow, evidenceOverflow },
      };
      const inputHash = evidenceHash({
        report,
        artifacts: group.sources.map((s) => s.artifactId).sort(),
        extractor: this.extractorHash,
      });
      if (
        await this.publish(
          group,
          report,
          inputHash,
          closed ? 'closed' : groupComplete ? 'active' : 'incomplete',
          now
        )
      )
        published++;
    }
    return published;
  }

  /** Rebuildable projection over immutable metadata; no narrative-derived admission. */
  private async indexTokens(asOf: Date): Promise<boolean> {
    const pending = await rows<{ token_id: string; name: string; ticker: string }>(
      this.db,
      `SELECT t.token_id,t.name,t.ticker
      FROM tokens t LEFT JOIN nit_token_search_index i USING(token_id) WHERE t.chain='solana' AND t.discovered_at<=$1
      AND (i.token_id IS NULL OR i.original_name IS DISTINCT FROM t.name OR i.original_ticker IS DISTINCT FROM t.ticker)
      ORDER BY t.token_id LIMIT 512`,
      [asOf]
    );
    if (pending.length > 0)
      await rows(
        this.db,
        `INSERT INTO nit_token_search_index(token_id,original_name,original_ticker,name_normalized,ticker_normalized)
      SELECT x.id,x.name,x.ticker,x.normal_name,x.normal_ticker FROM jsonb_to_recordset($1::jsonb)
        AS x(id UUID,name TEXT,ticker TEXT,normal_name TEXT,normal_ticker TEXT)
      ON CONFLICT(token_id) DO UPDATE SET original_name=EXCLUDED.original_name,original_ticker=EXCLUDED.original_ticker,
        name_normalized=EXCLUDED.name_normalized,ticker_normalized=EXCLUDED.ticker_normalized`,
        [
          JSON.stringify(
            pending.map((t) => ({
              id: t.token_id,
              name: t.name,
              ticker: t.ticker,
              normal_name: normalized(t.name),
              normal_ticker: normalized(t.ticker),
            }))
          ),
        ]
      );
    const missing = await rows(
      this.db,
      `SELECT 1 FROM tokens t LEFT JOIN nit_token_search_index i USING(token_id)
      WHERE t.chain='solana' AND t.discovered_at<=$1 AND
        (i.token_id IS NULL OR i.original_name IS DISTINCT FROM t.name OR i.original_ticker IS DISTINCT FROM t.ticker) LIMIT 1`,
      [asOf]
    );
    return missing.length === 0;
  }
  private async retrieveTokens(
    group: NarrativeGroup,
    asOf: Date
  ): Promise<{ tokens: TokenFacts[]; overflow: boolean; ready: boolean }> {
    const contexts = [
      ...new Set(
        group.sources.flatMap((s) =>
          s.extraction.topics
            .filter(
              (t) =>
                subjectKey(t.subjectQuote, this.config.policy).key === group.subject.key &&
                normalized(t.catalystQuote) === group.catalystKey
            )
            .map((t) => t.contextQuote)
        )
      ),
    ];
    const mints = [
      ...new Set(
        contexts.flatMap((text) => {
          const parts: readonly string[] = text.match(/[1-9A-HJ-NP-Za-km-z]+/g) ?? [];
          return parts.filter((p) => p.length >= 32 && p.length <= 44);
        })
      ),
    ];
    const tags = [
      ...new Set(
        contexts.flatMap((text) => {
          const parts: readonly string[] = text.match(/\$[\p{L}\p{N}_]+/gu) ?? [];
          return parts.map((p) => normalized(p.slice(1)));
        })
      ),
    ];
    const aliases = group.subject.aliases.map(normalized).filter((a) => a !== '');
    const found = await rows<{
      address: string;
      name: string;
      ticker: string;
      launch_time: Date;
      discovered_at: Date;
      cache_missing: boolean;
    }>(
      this.db,
      `SELECT t.address,t.name,t.ticker,t.launch_time,t.discovered_at,
        (i.token_id IS NULL OR i.original_name IS DISTINCT FROM t.name OR i.original_ticker IS DISTINCT FROM t.ticker) AS cache_missing
      FROM tokens t LEFT JOIN nit_token_search_index i USING(token_id) WHERE t.chain='solana' AND t.discovered_at<=$1 AND (
        i.token_id IS NULL OR i.original_name IS DISTINCT FROM t.name OR i.original_ticker IS DISTINCT FROM t.ticker OR
        t.address=ANY($2::text[]) OR i.ticker_normalized=ANY($5::text[]) OR
        EXISTS (SELECT 1 FROM unnest($3::text[]) a(value) WHERE strpos(i.name_normalized,a.value)>0) OR
        (i.name_normalized<>'' AND EXISTS (SELECT 1 FROM unnest($4::text[]) c(value) WHERE strpos(c.value,i.name_normalized)>0)))
      ORDER BY cache_missing DESC,t.address LIMIT $6`,
      [asOf, mints, aliases, contexts.map(normalized), tags, this.config.universeLimit + 1]
    );
    return {
      tokens: found.slice(0, this.config.universeLimit).map((t) => ({
        mint: t.address,
        name: t.name,
        ticker: t.ticker,
        launchTime: t.launch_time,
        discoveredAt: t.discovered_at,
      })),
      overflow: found.length > this.config.universeLimit,
      ready: found.every((t) => !t.cache_missing),
    };
  }

  private async publish(
    group: NarrativeGroup,
    report: StoredReport,
    inputHash: string,
    state: string,
    now: Date
  ): Promise<boolean> {
    const episodeId = stableId(['episode', group.key]);
    const reportId = stableId(['report', episodeId, this.policyHash, inputHash]);
    const research = new PostgresResearchStore(this.db);
    const existing = await rows(this.db, 'SELECT 1 FROM nit_narrative_reports WHERE report_id=$1', [
      reportId,
    ]);
    if (existing.length > 0) return false;
    const artifacts = group.sources.slice(0, 64).map((s) => s.artifactId);
    if ((await research.at(episodeId, now)) === null) {
      if (artifacts.length === 0) return false;
      const happened = new Date(
        Math.min(...group.sources.map((s) => s.source.publishedAt.getTime()))
      );
      const episode: ResearchRecord = {
        recordId: episodeId,
        kind: 'episode',
        scope: `auto-text:${group.key}`,
        version: 1,
        occurredAt: happened.toISOString(),
        observedAt: now.toISOString(),
        availableAt: now.toISOString(),
        producerVersion: ENGINE_VERSION,
        evidenceIds: artifacts.slice(0, 32),
        data: {
          narrativeId: `nit-text:${group.key}`,
          tokenMints: [],
          universeMode: 'open',
          episodeType: 'viral_burst',
          previousEpisodeId: null,
          originArtifactIds: artifacts.slice(0, 32),
        },
      };
      const saved = await research.append(episode);
      if (saved.kind === 'failure') throw saved.error;
    }
    const episode = await research.at(episodeId, now);
    if (episode?.kind !== 'episode') throw new Error('Missing live episode');
    const evidence = artifacts.length > 0 ? artifacts : episode.data.originArtifactIds;
    const interpretationId = stableId(['interpretation', reportId]);
    if ((await research.at(interpretationId, now)) === null) {
      const versions = await rows<{ next: number }>(
        this.db,
        "SELECT COALESCE(MAX(version),0)+1 AS next FROM research_records WHERE kind='interpretation' AND scope=$1",
        [`auto-text:${group.key}`]
      );
      const interpretation: ResearchRecord = {
        recordId: interpretationId,
        kind: 'interpretation',
        scope: `auto-text:${group.key}`,
        version: versions[0]!.next,
        occurredAt: episode.occurredAt,
        observedAt: now.toISOString(),
        availableAt: now.toISOString(),
        producerVersion: ENGINE_VERSION,
        evidenceIds: [episodeId, ...evidence],
        data: {
          episodeId,
          title: `${group.subject.label}: ${group.category}`.slice(0, 200),
          narrativeStrengthBps: report.score.narrativeStrengthBps,
          status: report.score.status,
          artifactIds: evidence,
          extractorVersion: `${ENGINE_VERSION}:${this.policyHash}`,
          modelVersion:
            this.config.interpreter.kind === 'gateway' ? this.interpreter.identity.version : null,
          promptVersion:
            this.config.interpreter.kind === 'gateway'
              ? this.interpreter.identity.promptHash
              : null,
          historicalMode: 'captured',
          candidates: report.score.candidates.map((c) => ({
            tokenMint: c.tokenMint,
            linkConfidenceBps: c.linkConfidenceBps,
            status: c.status,
            disposition: 'candidate',
            reasonCodes: c.reasonCodes,
          })),
        },
      };
      const saved = await research.append(interpretation);
      if (saved.kind === 'failure') throw saved.error;
    }
    const published = await research.publishInterpretation(interpretationId, now);
    if (published.kind === 'failure') throw published.error;
    await rows(
      this.db,
      `INSERT INTO nit_narrative_reports(report_id,episode_id,policy_hash,input_hash,state,report,available_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`,
      [reportId, episodeId, this.policyHash, inputHash, state, JSON.stringify(report), now]
    );
    return true;
  }
}
