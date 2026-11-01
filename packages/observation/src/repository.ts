import {
  MediaId,
  ObservationId,
  ObservationProvider,
  Result,
  failure,
  success,
} from '@nit/domain';
import { Executor } from '@nit/database-public';
import { NormalizedObservation } from './normalize';
import { Observation } from './types';

/**
 * What an insert did.
 *
 * A discriminated union rather than a flag plus an always-present `mediaIds`,
 * because on the duplicate path there is nothing to report: the media was
 * committed with the original observation, and polling re-sees the same posts
 * constantly, so that path is the hot one and must not pay for a query no
 * caller reads.
 */
export type InsertOutcome =
  | { inserted: true; observationId: ObservationId; mediaIds: MediaId[] }
  | { inserted: false; observationId: ObservationId };

export interface ObservationRepository {
  /**
   * Persist an observation and its media references.
   *
   * Idempotent on `(provider, providerPostId)` per TIS ch.4 §7: a provider post
   * already stored returns `inserted: false` with the existing identifier rather
   * than an error, because re-seeing a post is normal polling, not a fault.
   */
  insert(observation: NormalizedObservation): Promise<Result<InsertOutcome, Error>>;

  findById(observationId: ObservationId): Promise<Result<Observation | null, Error>>;

  findByProviderPostId(
    provider: ObservationProvider,
    providerPostId: string
  ): Promise<Result<Observation | null, Error>>;

  /** Most recent observations for one author, newest first. */
  findByAuthor(
    provider: ObservationProvider,
    authorId: string,
    limit: number
  ): Promise<Result<Observation[], Error>>;
}

interface ObservationRow {
  observation_id: string;
  provider: string;
  provider_post_id: string;
  author_id: string;
  published_at: Date;
  ingested_at: Date;
  text: string | null;
  quoted_observation_id: string | null;
  media_ids: string[];
}

/**
 * Resolves the two fields that are not stored as columns.
 *
 * `quoted_observation_id` comes from joining the stored provider id back to an
 * observation, which is what makes quote linking independent of arrival order.
 * `media_ids` is aggregated rather than fetched separately so one round trip
 * returns a complete canonical observation. The FILTER is what keeps a post with
 * no media from aggregating to `[null]`.
 */
const SELECT_OBSERVATION = `
  SELECT o.observation_id,
         o.provider,
         o.provider_post_id,
         o.author_id,
         o.published_at,
         o.ingested_at,
         o.text,
         quoted.observation_id AS quoted_observation_id,
         COALESCE(
           ARRAY_AGG(m.media_id ORDER BY m.provider_media_key)
             FILTER (WHERE m.media_id IS NOT NULL),
           '{}'
         ) AS media_ids
    FROM observations AS o
    LEFT JOIN observations AS quoted
      ON quoted.provider = o.provider
     AND quoted.provider_post_id = o.quoted_provider_post_id
    LEFT JOIN media AS m
      ON m.observation_id = o.observation_id
`;

const GROUP_OBSERVATION = ' GROUP BY o.observation_id, quoted.observation_id';

export class PostgresObservationRepository implements ObservationRepository {
  /**
   * Takes an `Executor` so a caller can pull these writes into a transaction it
   * owns — which is how the observation, its media and the `ObservationCreated`
   * event reach the database in a single commit.
   */
  constructor(private readonly exec: Executor) {}

  async insert(observation: NormalizedObservation): Promise<Result<InsertOutcome, Error>> {
    // DO NOTHING rather than DO UPDATE: the append-only trigger on observations
    // rejects UPDATE, so an upsert would fail on exactly the duplicate this is
    // meant to absorb. The tradeoff is that a conflict returns no row, so the
    // existing identifier needs the follow-up SELECT below.
    const inserted = await this.exec.query<{ observation_id: string }>(
      `INSERT INTO observations (
         observation_id, provider, provider_post_id, author_id,
         published_at, ingested_at, text, quoted_provider_post_id, raw_payload
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (provider, provider_post_id) DO NOTHING
       RETURNING observation_id`,
      [
        observation.observationId,
        observation.provider,
        observation.providerPostId,
        observation.authorId,
        observation.publishedAt,
        observation.ingestedAt,
        observation.text ?? null,
        observation.quotedProviderPostId ?? null,
        JSON.stringify(observation.rawPayload),
      ]
    );
    if (inserted.kind === 'failure') {
      return failure(inserted.error);
    }

    if (inserted.value.length === 0) {
      const existing = await this.exec.query<{ observation_id: string }>(
        'SELECT observation_id FROM observations WHERE provider = $1 AND provider_post_id = $2',
        [observation.provider, observation.providerPostId]
      );
      if (existing.kind === 'failure') {
        return failure(existing.error);
      }
      const observationId = existing.value[0]?.observation_id;
      if (observationId === undefined) {
        // The insert conflicted, so a row exists; not finding it means this
        // executor is reading a different snapshot than it wrote to.
        return failure(
          new Error(
            `Observation ${observation.provider}/${observation.providerPostId} conflicted on insert but could not be read back`
          )
        );
      }
      return success({ inserted: false, observationId: observationId as ObservationId });
    }

    const mediaIds: MediaId[] = [];
    for (const media of observation.media) {
      // No ON CONFLICT here, unlike the observation above. The observation was
      // brand new, so none of its media can already exist; a conflict would mean
      // a bug, and swallowing it would drop an attachment without a trace.
      const result = await this.exec.query(
        `INSERT INTO media (
           media_id, observation_id, provider_media_key, media_type, source_url, duration_ms
         ) VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          media.mediaId,
          observation.observationId,
          media.providerMediaKey,
          media.mediaType,
          media.sourceUrl,
          media.durationMs ?? null,
        ]
      );
      if (result.kind === 'failure') {
        return failure(result.error);
      }
      mediaIds.push(media.mediaId);
    }

    return success({
      inserted: true,
      observationId: observation.observationId,
      mediaIds,
    });
  }

  async findById(observationId: ObservationId): Promise<Result<Observation | null, Error>> {
    const found = await this.select(' WHERE o.observation_id = $1', [observationId]);
    if (found.kind === 'failure') {
      return failure(found.error);
    }
    return success(found.value[0] ?? null);
  }

  async findByProviderPostId(
    provider: ObservationProvider,
    providerPostId: string
  ): Promise<Result<Observation | null, Error>> {
    const found = await this.select(' WHERE o.provider = $1 AND o.provider_post_id = $2', [
      provider,
      providerPostId,
    ]);
    if (found.kind === 'failure') {
      return failure(found.error);
    }
    return success(found.value[0] ?? null);
  }

  async findByAuthor(
    provider: ObservationProvider,
    authorId: string,
    limit: number
  ): Promise<Result<Observation[], Error>> {
    return this.select(
      ' WHERE o.provider = $1 AND o.author_id = $2',
      [provider, authorId],
      ' ORDER BY o.published_at DESC, o.provider_post_id DESC LIMIT $3',
      [limit]
    );
  }

  private async select(
    where: string,
    whereParams: unknown[],
    order = '',
    orderParams: unknown[] = []
  ): Promise<Result<Observation[], Error>> {
    const result = await this.exec.query<ObservationRow>(
      SELECT_OBSERVATION + where + GROUP_OBSERVATION + order,
      [...whereParams, ...orderParams]
    );
    if (result.kind === 'failure') {
      return failure(result.error);
    }
    return success(result.value.map(toObservation));
  }
}

function toObservation(row: ObservationRow): Observation {
  const observation: Observation = {
    observationId: row.observation_id as ObservationId,
    provider: row.provider as ObservationProvider,
    providerPostId: row.provider_post_id,
    authorId: row.author_id,
    publishedAt: row.published_at,
    ingestedAt: row.ingested_at,
    mediaIds: row.media_ids as MediaId[],
  };
  if (row.text !== null) {
    observation.text = row.text;
  }
  if (row.quoted_observation_id !== null) {
    observation.quotedObservationId = row.quoted_observation_id as ObservationId;
  }
  return observation;
}
