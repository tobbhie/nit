import { Chain, EvidenceId, Result, failure, generateSignalId } from '@nit/domain';
import {
  ResearchRecord,
  ResearchFeature,
  parseResearchRecord,
  evidenceHash,
  SignalKind,
} from '@nit/signals';
import { DatabasePool, Executor, clientExecutor } from './pool';
import { PostgresSignalStore } from './signal-store';

async function rows<T>(db: Executor, sql: string, values: unknown[] = []): Promise<T[]> {
  const result = await db.query<T>(sql, values);
  if (result.kind === 'failure') throw result.error;
  return result.value;
}
export class PostgresResearchStore {
  constructor(private readonly db: DatabasePool) {}
  async append(raw: unknown): Promise<Result<ResearchRecord, Error>> {
    try {
      const record = parseResearchRecord(raw);
      return await this.db.transaction(async (client) => {
        const db = clientExecutor(client);
        const refs = references(record);
        const loaded = await rows<{ payload: ResearchRecord }>(
          db,
          'SELECT payload FROM research_records WHERE record_id=ANY($1::uuid[])',
          [refs]
        );
        if (loaded.length !== new Set(refs).size)
          throw new Error('Research evidence reference is missing');
        if (
          loaded.some(({ payload }) => new Date(payload.availableAt) > new Date(record.availableAt))
        )
          throw new Error('Research result precedes its evidence');
        const get = (id: string, kind: ResearchRecord['kind']): ResearchRecord => {
          const match = loaded.find(
            ({ payload }) => payload.recordId === id && payload.kind === kind
          )?.payload;
          if (match === undefined) throw new Error(`Expected ${kind} evidence`);
          return match;
        };
        if (record.kind === 'artifact' && record.data.derivativeOf !== null)
          get(record.data.derivativeOf, 'artifact');
        if (record.kind === 'episode') {
          record.data.originArtifactIds.forEach((id) => get(id, 'artifact'));
          if (record.data.previousEpisodeId !== null) {
            const earlier = get(record.data.previousEpisodeId, 'episode');
            if (
              earlier.kind !== 'episode' ||
              !record.data.tokenMints.some((mint) => earlier.data.tokenMints.includes(mint))
            )
              throw new Error('Episode lineage must share a mint');
          }
        }
        if (record.kind === 'interpretation') {
          const episode = get(record.data.episodeId, 'episode');
          record.data.artifactIds.forEach((id) => get(id, 'artifact'));
          if (
            episode.kind !== 'episode' ||
            record.data.candidates.some((c) => !episode.data.tokenMints.includes(c.tokenMint))
          )
            throw new Error('Interpretation introduces a mint outside its episode');
        }
        if (record.kind === 'feature') {
          const episode = get(record.data.episodeId, 'episode');
          const interpretation = get(record.data.interpretationId, 'interpretation');
          if (
            episode.kind !== 'episode' ||
            interpretation.kind !== 'interpretation' ||
            interpretation.data.episodeId !== episode.recordId ||
            !interpretation.data.candidates.some((c) => c.tokenMint === record.data.tokenMint)
          )
            throw new Error('Feature scope does not match interpretation');
          const candidate = interpretation.data.candidates.find(
            (c) => c.tokenMint === record.data.tokenMint
          )!;
          if (
            record.data.narrative !== interpretation.data.status ||
            record.data.linkage !== candidate.status ||
            record.data.narrativeStrengthBps !== interpretation.data.narrativeStrengthBps ||
            record.data.linkConfidenceBps !== candidate.linkConfidenceBps
          )
            throw new Error('Feature cannot rewrite its interpretation');
          record.data.artifactIds.forEach((id) => get(id, 'artifact'));
          record.data.coverageIds.forEach((id) => {
            const coverage = get(id, 'coverage');
            if (coverage.kind !== 'coverage' || coverage.data.tokenMint !== record.data.tokenMint)
              throw new Error('Coverage mint mismatch');
          });
          if (
            record.data.wallet === 'observed_nonconfirming' &&
            !loaded.some(
              ({ payload }) =>
                payload.kind === 'coverage' &&
                payload.data.family === 'wallet' &&
                payload.data.status === 'confirmed'
            )
          )
            throw new Error('Negative wallet evidence requires observed coverage');
        }
        const digest = evidenceHash(record);
        await rows(
          db,
          `INSERT INTO research_records(record_id,kind,scope,version,content_hash,occurred_at,observed_at,available_at,payload)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`,
          [
            record.recordId,
            record.kind,
            record.scope,
            record.version,
            digest,
            record.occurredAt,
            record.observedAt,
            record.availableAt,
            JSON.stringify(record),
          ]
        );
        const stored = await rows<{ content_hash: string }>(
          db,
          'SELECT content_hash FROM research_records WHERE record_id=$1',
          [record.recordId]
        );
        if (stored[0]?.content_hash !== digest)
          throw new Error('Research identity/version collision');
        for (const id of new Set(refs))
          await rows(
            db,
            'INSERT INTO research_links(record_id,evidence_id,relation) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING',
            [record.recordId, id, 'derived_from']
          );
        return record;
      });
    } catch (error) {
      return failure(error instanceof Error ? error : new Error('Invalid research record'));
    }
  }
  async at(id: string, asOf: Date): Promise<ResearchRecord | null> {
    const found = await rows<{ payload: ResearchRecord }>(
      this.db,
      'SELECT payload FROM research_records WHERE record_id=$1 AND available_at<=$2',
      [id, asOf]
    );
    return found[0]?.payload ?? null;
  }
  async latestFeature(
    episodeId: string,
    mint: string,
    asOf: Date
  ): Promise<ResearchFeature | null> {
    const found = await rows<{ payload: ResearchFeature }>(
      this.db,
      `SELECT payload FROM research_records WHERE kind='feature'
      AND payload->'data'->>'episodeId'=$1 AND payload->'data'->>'tokenMint'=$2 AND available_at<=$3
      ORDER BY available_at DESC,version DESC,record_id DESC LIMIT 1`,
      [episodeId, mint, asOf]
    );
    return found[0]?.payload ?? null;
  }
  /** Source identity, derivative lineage and identical content each prevent double corroboration. */
  async independentSources(ids: readonly string[], asOf: Date): Promise<number> {
    const groups: Set<string>[] = [];
    for (const id of [...new Set(ids)].sort()) {
      let artifact = await this.at(id, asOf);
      if (artifact?.kind !== 'artifact') throw new Error('Expected available source artifact');
      const hash = artifact.data.contentHash;
      for (let depth = 0; artifact.data.derivativeOf !== null; depth++) {
        if (depth >= 64) throw new Error('Artifact lineage exceeds depth bound');
        const ancestor = await this.at(artifact.data.derivativeOf, asOf);
        if (ancestor?.kind !== 'artifact') throw new Error('Missing derivative origin');
        artifact = ancestor;
      }
      const author = `${artifact.data.provider}:${artifact.data.authorId}`;
      const group = new Set([
        `root:${artifact.recordId}`,
        `author:${author}`,
        `content:${hash}`,
        `content:${artifact.data.contentHash}`,
        `source:${artifact.data.provider}:${artifact.data.sourceId}`,
      ]);
      for (let i = groups.length - 1; i >= 0; i--) {
        const other = groups[i]!;
        if ([...other].some((key) => group.has(key))) {
          for (const key of other) group.add(key);
          groups.splice(i, 1);
        }
      }
      groups.push(group);
    }
    return groups.length;
  }
  /** Narrow real producer: publish captured text interpretation with explicit provenance; no invented extraction/model output. */
  async publishInterpretation(id: string, asOf: Date): Promise<Result<string, Error>> {
    try {
      const interpretation = await this.at(id, asOf);
      if (interpretation?.kind !== 'interpretation')
        throw new Error('Interpretation is unavailable');
      const episode = await this.at(interpretation.data.episodeId, asOf);
      if (episode?.kind !== 'episode') throw new Error('Episode is unavailable');
      const independentSourceCount = await this.independentSources(
        interpretation.data.artifactIds,
        asOf
      );
      const written = await new PostgresSignalStore(this.db).append({
        eventId: generateSignalId(),
        source: 'nit',
        sourceVersion: 'provenance:v1',
        kind: SignalKind.NARRATIVE,
        chain: Chain.SOLANA,
        canonicalNarrativeId: episode.data.narrativeId,
        occurredAt: new Date(interpretation.occurredAt),
        observedAt: new Date(interpretation.observedAt),
        availableAt: asOf,
        origin: { kind: 'provider', provider: 'nit-provenance', referenceId: id },
        evidenceIds: [id, ...interpretation.data.artifactIds] as EvidenceId[],
        payload: {
          schemaVersion: 2,
          episodeId: episode.recordId,
          interpretationId: id,
          title: interpretation.data.title,
          narrativeStrengthBps: interpretation.data.narrativeStrengthBps,
          narrativeStatus: interpretation.data.status,
          independentSourceCount,
          candidates: interpretation.data.candidates
            .filter((c) => c.disposition === 'candidate')
            .map((c) => ({
              tokenMint: c.tokenMint,
              linkConfidenceBps: c.linkConfidenceBps,
              linkageStatus: c.status,
              reasonCodes: c.reasonCodes,
            })),
        },
      });
      if (written.kind === 'failure') throw written.error;
      return { kind: 'success', value: written.value.signalId };
    } catch (error) {
      return failure(error instanceof Error ? error : new Error('Narrative publication failed'));
    }
  }
}
function references(record: ResearchRecord): string[] {
  const result = [...record.evidenceIds];
  if (record.kind === 'artifact' && record.data.derivativeOf !== null)
    result.push(record.data.derivativeOf);
  if (record.kind === 'episode')
    result.push(
      ...record.data.originArtifactIds,
      ...(record.data.previousEpisodeId === null ? [] : [record.data.previousEpisodeId])
    );
  if (record.kind === 'interpretation')
    result.push(record.data.episodeId, ...record.data.artifactIds);
  if (record.kind === 'feature')
    result.push(
      record.data.episodeId,
      record.data.interpretationId,
      ...record.data.artifactIds,
      ...record.data.coverageIds
    );
  return [...new Set(result)];
}
