import { createHash } from 'node:crypto';
import { z } from 'zod';

export const EvidenceStatusSchema = z.enum([
  'pending',
  'unavailable',
  'extraction_failure',
  'observed_nonconfirming',
  'contradictory',
  'confirmed',
]);
export type EvidenceStatus = z.infer<typeof EvidenceStatusSchema>;
const uuid = z.string().uuid();
const text = z.string().min(1).max(500);
const mint = z.string().regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/);
const score = z.number().int().min(0).max(10000);
const at = z.string().datetime();
const base = {
  recordId: uuid,
  scope: text,
  version: z.number().int().positive(),
  occurredAt: at,
  observedAt: at,
  availableAt: at,
  producerVersion: text,
  evidenceIds: z.array(uuid).max(128),
};
export const ResearchRecordSchema = z.discriminatedUnion('kind', [
  z.object({
    ...base,
    kind: z.literal('artifact'),
    data: z.object({
      provider: text,
      sourceId: text,
      authorId: text,
      contentHash: z.string().regex(/^[a-f0-9]{64}$/),
      text: z.string().max(16000),
      sourceUrl: z.string().url().max(2048),
      derivativeOf: uuid.nullable(),
      media: z
        .array(z.object({ hash: z.string().regex(/^[a-f0-9]{64}$/), storageReference: text }))
        .max(16),
    }),
  }),
  z.object({
    ...base,
    kind: z.literal('episode'),
    data: z.object({
      narrativeId: text,
      tokenMints: z.array(mint).min(1).max(32),
      episodeType: z.enum(['launch', 'viral_burst', 'promotion', 'revival', 'cto']),
      previousEpisodeId: uuid.nullable(),
      originArtifactIds: z.array(uuid).min(1).max(32),
    }),
  }),
  z.object({
    ...base,
    kind: z.literal('interpretation'),
    data: z.object({
      episodeId: uuid,
      title: z.string().min(1).max(200),
      narrativeStrengthBps: score,
      status: EvidenceStatusSchema,
      artifactIds: z.array(uuid).min(1).max(64),
      extractorVersion: text,
      modelVersion: text.nullable(),
      promptVersion: text.nullable(),
      historicalMode: z.enum(['captured', 'regenerated_approximation']),
      candidates: z
        .array(
          z.object({
            tokenMint: mint,
            linkConfidenceBps: score,
            status: EvidenceStatusSchema,
            disposition: z.enum(['candidate', 'rejected']),
            reasonCodes: z.array(text).min(1).max(16),
          })
        )
        .min(1)
        .max(32),
    }),
  }),
  z.object({
    ...base,
    kind: z.literal('coverage'),
    data: z.object({
      family: z.enum(['wallet', 'social', 'market']),
      tokenMint: mint,
      status: EvidenceStatusSchema,
      windowStart: at,
      windowEnd: at,
      coveredWallets: z.array(text).max(256),
    }),
  }),
  z.object({
    ...base,
    kind: z.literal('feature'),
    data: z.object({
      episodeId: uuid,
      interpretationId: uuid,
      tokenMint: mint,
      narrative: EvidenceStatusSchema,
      linkage: EvidenceStatusSchema,
      wallet: EvidenceStatusSchema,
      relationship: z.enum(['unknown', 'clear', 'suspected']),
      independentWallets: z.number().int().nonnegative(),
      attributionAmbiguous: z.boolean(),
      selectionRank: z.number().int().positive(),
      correlationWindowOpen: z.boolean(),
      canonicalTokenKnown: z.boolean(),
      narrativeStrengthBps: score,
      linkConfidenceBps: score,
      artifactIds: z.array(uuid).max(64),
      coverageIds: z.array(uuid).max(16),
      policyVersion: text,
      confidenceMeaning: z.literal('evidence_score_not_profit_probability'),
    }),
  }),
]);
export type ResearchRecord = z.infer<typeof ResearchRecordSchema>;
export type ResearchFeature = Extract<ResearchRecord, { kind: 'feature' }>;

export function parseResearchRecord(value: unknown): ResearchRecord {
  const parsed = ResearchRecordSchema.parse(value);
  if (
    Date.parse(parsed.occurredAt) > Date.parse(parsed.observedAt) ||
    Date.parse(parsed.observedAt) > Date.parse(parsed.availableAt)
  )
    throw new Error('Invalid research availability ordering');
  if (
    parsed.kind === 'coverage' &&
    (parsed.data.windowStart > parsed.data.windowEnd || parsed.data.windowEnd > parsed.availableAt)
  )
    throw new Error('Invalid coverage window');
  if (
    parsed.kind === 'episode' &&
    parsed.data.episodeType === 'revival' &&
    parsed.data.previousEpisodeId === null
  )
    throw new Error('A revival must reference its earlier episode');
  if (
    parsed.kind === 'interpretation' &&
    new Set(parsed.data.candidates.map((c) => c.tokenMint)).size !== parsed.data.candidates.length
  )
    throw new Error('Duplicate interpretation mint');
  if (parsed.kind === 'artifact' && contentHash(parsed.data.text) !== parsed.data.contentHash)
    throw new Error('Artifact text hash mismatch');
  return parsed;
}
/** Stable JSON hashing includes every transform/configuration field, including intercepts. */
export function canonicalEvidence(value: unknown): string {
  if (value === null || typeof value === 'string' || typeof value === 'boolean')
    return JSON.stringify(value);
  if (typeof value === 'number' && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalEvidence).join(',')}]`;
  if (typeof value === 'object' && value !== null)
    return `{${Object.entries(value)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, item]) => `${JSON.stringify(key)}:${canonicalEvidence(item)}`)
      .join(',')}}`;
  throw new Error('Evidence must contain finite JSON values');
}
export function contentHash(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}
export function evidenceHash(value: unknown): string {
  return contentHash(canonicalEvidence(value));
}
