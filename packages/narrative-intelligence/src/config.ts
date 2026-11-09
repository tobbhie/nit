import { z } from 'zod';
import { PolicySchema } from './contracts';

export const NarrativeRuntimeSchema = z
  .object({
    policy: PolicySchema,
    interpreter: z.discriminatedUnion('kind', [
      z.object({ kind: z.literal('catalog') }).strict(),
      z
        .object({
          kind: z.literal('gateway'),
          endpoint: z.string().url(),
          allowedOrigin: z.string().url(),
          modelVersion: z.string().min(1).max(200),
          allowExternalContext: z.literal(true),
          expiresAt: z.string().datetime(),
          maxInputBytes: z.number().int().min(2000).max(96000),
          maxOutputTokens: z.number().int().min(64).max(8000),
          maxChargeMicros: z.number().int().min(1).max(10000000),
        })
        .strict(),
    ]),
    intervalMs: z.number().int().min(1000).max(60000),
    timeoutMs: z.number().int().min(100).max(30000),
    observationsPerCycle: z.number().int().min(1).max(64),
    sourceLimit: z.number().int().min(1).max(512),
    universeLimit: z.number().int().min(1).max(5000),
    episodeLimit: z.number().int().min(1).max(64),
    maxRequestsPerDay: z.number().int().min(1).max(100000),
    maxCostMicrosPerDay: z.number().int().min(0).max(100000000),
  })
  .strict();
export type NarrativeRuntimeConfig = z.infer<typeof NarrativeRuntimeSchema>;
