import { Chain, EvidenceId, SignalId, ValidationError } from '@nit/domain';
import { z } from 'zod';
import { SignalEnvelope, SignalKind } from './types';

const SolanaAddressSchema = z.string().regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/);

const SignalOriginSchema = z.union([
  z.object({
    kind: z.literal('provider'),
    provider: z.string().min(1),
    referenceId: z.string().min(1),
  }),
  z
    .object({
      kind: z.literal('chain'),
      chain: z.nativeEnum(Chain),
      transactionSignature: z.string().min(1),
      instructionIndex: z.number().int().nonnegative().optional(),
      logIndex: z.number().int().nonnegative().optional(),
    })
    .superRefine((origin, context) => {
      const count = Number(origin.instructionIndex !== undefined) + Number(origin.logIndex !== undefined);
      if (count !== 1) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Exactly one of instructionIndex or logIndex is required',
        });
      }
    }),
]);

export const SignalEnvelopeSchema = z
  .object({
    eventId: z.string().min(1),
    source: z.string().regex(/^(nit|wallet_tracker|market|plugin:[a-z0-9][a-z0-9_-]*)$/),
    sourceVersion: z.string().min(1),
    kind: z.nativeEnum(SignalKind),
    chain: z.nativeEnum(Chain),
    tokenMint: SolanaAddressSchema.optional(),
    canonicalNarrativeId: z.string().min(1).optional(),
    occurredAt: z.date(),
    observedAt: z.date(),
    availableAt: z.date(),
    origin: SignalOriginSchema,
    evidenceIds: z.array(z.string().min(1)),
    payload: z.unknown(),
  })
  .superRefine((signal, context) => {
    if (signal.observedAt.getTime() < signal.occurredAt.getTime()) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['observedAt'],
        message: 'observedAt cannot precede occurredAt',
      });
    }
    if (signal.availableAt.getTime() < signal.observedAt.getTime()) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['availableAt'],
        message: 'availableAt cannot precede observedAt',
      });
    }

    const tokenRequired =
      signal.kind === SignalKind.WALLET_BUY ||
      signal.kind === SignalKind.WALLET_SELL ||
      signal.kind === SignalKind.MARKET;
    if (tokenRequired && signal.tokenMint === undefined) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['tokenMint'],
        message: `${signal.kind} signals require tokenMint`,
      });
    }

    const narrativeRequired =
      signal.kind === SignalKind.NARRATIVE || signal.kind === SignalKind.CANDIDATE_UPDATE;
    if (narrativeRequired && signal.canonicalNarrativeId === undefined) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['canonicalNarrativeId'],
        message: `${signal.kind} signals require canonicalNarrativeId`,
      });
    }
  });

export function parseSignalEnvelope<TPayload = unknown>(input: unknown): SignalEnvelope<TPayload> {
  const result = SignalEnvelopeSchema.safeParse(input);
  if (!result.success) {
    const detail = result.error.errors
      .map((error) => `${error.path.join('.') || 'signal'}: ${error.message}`)
      .join('; ');
    throw new ValidationError(`Invalid signal envelope: ${detail}`, {
      errors: result.error.errors,
    });
  }

  return result.data as SignalEnvelope<TPayload> & {
    eventId: SignalId;
    evidenceIds: EvidenceId[];
  };
}
