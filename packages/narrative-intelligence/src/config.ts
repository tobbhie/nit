import { z } from 'zod';
import { PolicySchema } from './contracts';
import { MediaConfigSchema } from './media';
import { OPENAI_NARRATIVE_ENDPOINT, OPENAI_NARRATIVE_MODEL } from './openai';

export const NarrativeRuntimeSchema = z
  .object({
    policy: PolicySchema,
    interpreter: z.discriminatedUnion('kind', [
      z.object({ kind: z.literal('catalog') }).strict(),
      z
        .object({
          kind: z.literal('gateway'),
          provider: z.literal('openai').optional(),
          endpoint: z.string().url(),
          allowedOrigin: z.string().url(),
          modelVersion: z.string().min(1).max(200),
          allowExternalContext: z.literal(true),
          expiresAt: z.string().datetime(),
          maxInputBytes: z.number().int().min(2000).max(33554432),
          maxOutputTokens: z.number().int().min(64).max(8000),
          maxChargeMicros: z.number().int().min(1).max(10000000),
          rich: z
            .object({
              modelVersion: z.string().min(1).max(200),
              dimensions: z.number().int().min(2).max(512),
              media: MediaConfigSchema.optional(),
              allowExternalMedia: z.literal(true).optional(),
            })
            .strict()
            .optional(),
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
  .strict()
  .superRefine((c, ctx) => {
    if (
      c.interpreter.kind === 'gateway' &&
      c.interpreter.provider === 'openai' &&
      (c.interpreter.endpoint !== OPENAI_NARRATIVE_ENDPOINT ||
        c.interpreter.allowedOrigin !== 'https://api.openai.com' ||
        c.interpreter.modelVersion !== OPENAI_NARRATIVE_MODEL ||
        c.interpreter.maxInputBytes > 64000 ||
        c.interpreter.rich !== undefined)
    )
      ctx.addIssue({
        code: 'custom',
        message: 'OpenAI requires its pinned text-only configuration',
      });
    if (
      c.interpreter.kind === 'gateway' &&
      c.interpreter.rich?.media !== undefined &&
      c.interpreter.rich.allowExternalMedia !== true
    )
      ctx.addIssue({
        code: 'custom',
        message: 'Media disclosure requires an explicit external-media grant',
      });
  });
export type NarrativeRuntimeConfig = z.infer<typeof NarrativeRuntimeSchema>;
