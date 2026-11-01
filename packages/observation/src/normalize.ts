import {
  MediaId,
  MediaType,
  ObservationId,
  ObservationProvider,
  Result,
  Timestamp,
  ValidationError,
  failure,
  generateMediaId,
  generateObservationId,
  success,
} from '@nit/domain';
import { SourceMedia, SourcePost } from './contracts';

/** A media row ready to insert: identified, located, not yet downloaded. */
export interface NormalizedMedia {
  mediaId: MediaId;
  providerMediaKey: string;
  mediaType: MediaType;
  sourceUrl: string;
  durationMs?: number;
}

/**
 * An observation ready to insert. Close to the canonical `Observation`, with two
 * differences that both come from the write side: the quoted post is still
 * identified by the provider's id (the internal id is resolved on read), and the
 * raw payload is carried because only the repository needs it.
 */
export interface NormalizedObservation {
  observationId: ObservationId;
  provider: ObservationProvider;
  providerPostId: string;
  authorId: string;
  publishedAt: Timestamp;
  ingestedAt: Timestamp;
  text?: string;
  quotedProviderPostId?: string;
  rawPayload: Record<string, unknown>;
  media: NormalizedMedia[];
}

const MEDIA_TYPES = new Set<string>(Object.values(MediaType));

function requireText(value: string, field: string): Result<string, ValidationError> {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return failure(new ValidationError(`Observation ${field} is empty`, { field }));
  }
  return success(trimmed);
}

/** Empty and whitespace-only text is absent text, not text. */
function optionalText(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

function normalizeMedia(
  media: readonly SourceMedia[],
  providerPostId: string
): Result<NormalizedMedia[], ValidationError> {
  const normalized: NormalizedMedia[] = [];
  const seen = new Set<string>();

  for (const item of media) {
    const key = requireText(item.providerMediaKey, 'providerMediaKey');
    if (key.kind === 'failure') {
      return key;
    }
    // Two attachments claiming one key would collide on the media unique
    // constraint, and `ON CONFLICT DO NOTHING` would drop the second one
    // silently. Rejecting the post is louder and easier to diagnose than an
    // observation that quietly lost an image.
    if (seen.has(key.value)) {
      return failure(
        new ValidationError('Duplicate media key within one post', {
          providerPostId,
          providerMediaKey: key.value,
        })
      );
    }
    seen.add(key.value);

    const sourceUrl = requireText(item.sourceUrl, 'sourceUrl');
    if (sourceUrl.kind === 'failure') {
      return sourceUrl;
    }

    // Typed as MediaType, but an adapter's output crosses a JSON boundary on the
    // way here through a job payload, so the type is a claim rather than a
    // guarantee.
    if (!MEDIA_TYPES.has(item.mediaType)) {
      return failure(
        new ValidationError('Unknown media type', {
          providerPostId,
          mediaType: item.mediaType,
        })
      );
    }

    if (item.durationMs !== undefined && !(Number.isInteger(item.durationMs) && item.durationMs >= 0)) {
      return failure(
        new ValidationError('Media duration is not a non-negative integer', {
          providerPostId,
          durationMs: item.durationMs,
        })
      );
    }

    const entry: NormalizedMedia = {
      mediaId: generateMediaId(),
      providerMediaKey: key.value,
      mediaType: item.mediaType,
      sourceUrl: sourceUrl.value,
    };
    if (item.durationMs !== undefined) {
      entry.durationMs = item.durationMs;
    }
    normalized.push(entry);
  }

  return success(normalized);
}

/**
 * Turn a provider payload into a persistable observation, or reject it.
 *
 * This is where TIS ch.4 §11's "canonical observation schema is enforced" is
 * actually true. Validating at the boundary rather than trusting the adapter
 * matters because an adapter is the least trustworthy code in the pipeline: it
 * mirrors a third-party response shape that can change without notice, and
 * everything downstream — matching, interpretation, replay — is written against
 * the canonical model on the assumption that it holds.
 *
 * Pure by design. No identifiers are read from the provider, no clock is read
 * from the environment, and nothing is written, so a fixture and an expected
 * output are enough to test it.
 */
export function normalizeSourcePost(
  provider: ObservationProvider,
  post: SourcePost,
  ingestedAt: Timestamp
): Result<NormalizedObservation, ValidationError> {
  const providerPostId = requireText(post.providerPostId, 'providerPostId');
  if (providerPostId.kind === 'failure') {
    return providerPostId;
  }

  const authorId = requireText(post.authorId, 'authorId');
  if (authorId.kind === 'failure') {
    return authorId;
  }

  if (!Number.isFinite(post.publishedAt.getTime())) {
    return failure(
      new ValidationError('Observation publishedAt is not a valid date', {
        providerPostId: providerPostId.value,
      })
    );
  }

  const quotedProviderPostId = optionalText(post.quotedProviderPostId);
  // A self-quote would make the resolving join return the observation itself,
  // so every consumer walking quote chains would loop.
  if (quotedProviderPostId !== undefined && quotedProviderPostId === providerPostId.value) {
    return failure(
      new ValidationError('Observation quotes itself', {
        providerPostId: providerPostId.value,
      })
    );
  }

  const media = normalizeMedia(post.media, providerPostId.value);
  if (media.kind === 'failure') {
    return media;
  }

  const normalized: NormalizedObservation = {
    observationId: generateObservationId(),
    provider,
    providerPostId: providerPostId.value,
    authorId: authorId.value,
    publishedAt: post.publishedAt,
    ingestedAt,
    rawPayload: post.raw,
    media: media.value,
  };

  const text = optionalText(post.text);
  if (text !== undefined) {
    normalized.text = text;
  }
  if (quotedProviderPostId !== undefined) {
    normalized.quotedProviderPostId = quotedProviderPostId;
  }

  return success(normalized);
}
