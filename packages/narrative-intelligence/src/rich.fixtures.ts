import { configuration, input } from './narrative.fixtures';
import { TextExtraction } from './contracts';
import { MediaConfig } from './media';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

export function richConfiguration(): ReturnType<typeof configuration> {
  const config = configuration();
  config.policy.semantics = {
    modelVersion: 'fixture-embedding-v1',
    dimensions: 2,
    mergeBps: 9500,
    ambiguityMarginBps: 100,
    retrievalBps: 7000,
    visualMatchBps: 9000,
  };
  config.policy.linkWeights = {
    lexical: 1000,
    identity: 1000,
    directReference: 4000,
    temporal: 0,
    semantic: 1000,
    visual: 3000,
    ocr: 0,
    transcript: 0,
    contradiction: 10000,
  };
  config.interpreter = {
    kind: 'gateway',
    endpoint: 'https://gateway.invalid/text',
    allowedOrigin: 'https://gateway.invalid',
    modelVersion: 'fixture-rich',
    allowExternalContext: true,
    expiresAt: '2027-01-01T00:00:00.000Z',
    maxInputBytes: 64000,
    maxOutputTokens: 1000,
    maxChargeMicros: 1,
    rich: { modelVersion: 'fixture-embedding-v1', dimensions: 2 },
  };
  config.maxCostMicrosPerDay = 1000;
  return config;
}
export function richInput(author = 'a', text?: string, vector = [1, 0]): ReturnType<typeof input> {
  const value = input(author);
  value.source.text = text ?? value.source.text;
  value.extraction = {
    schemaVersion: 2,
    representation: { modelVersion: 'fixture-embedding-v1', vector },
    topics: [
      {
        subjectQuote: 'Nova',
        catalystQuote: value.source.text.includes('festival')
          ? 'Nova festival begins'
          : 'Nova party opens',
        contextQuote: value.source.text,
        category: 'meme',
        stance: 'support',
        meaning: {
          subject: 'Nova',
          event: 'Nova festival opening',
          modelVersion: 'fixture-embedding-v1',
          vector,
        },
      },
    ],
  };
  return value;
}
export function mediaConfig(root = join(tmpdir(), 'nit-fixture-media')): MediaConfig {
  return {
    allowedOrigins: ['https://media.invalid'],
    maxBytes: 1048576,
    maxDurationMs: 30000,
    maxFrames: 3,
    frameIntervalMs: 3000,
    scratchRoot: root,
    ffmpegPath: process.env['NIT_TEST_FFMPEG_PATH'] ?? join(root, 'ffmpeg-not-installed'),
    ffprobePath: process.env['NIT_TEST_FFPROBE_PATH'] ?? join(root, 'ffprobe-not-installed'),
    subprocessTimeoutMs: 10000,
    scratchSoftBytes: 10485760,
    scratchHardBytes: 20971520,
    scratchMaxAgeMs: 60000,
    archive: {
      endpoint: `https://${'0'.repeat(32)}.r2.cloudflarestorage.com`,
      bucket: 'nit-test',
      maxUploadAttempts: 3,
      timeoutMs: 1000,
    },
  };
}
export const mediaId = '10000000-0000-4000-8000-000000000001';
export function withMedia(
  value = richInput(),
  hash = 'a'.repeat(64)
): ReturnType<typeof richInput> {
  value.source.media = [
    { mediaId, mediaType: 'image', sourceUrl: 'https://media.invalid/image', durationMs: null },
  ];
  value.extraction.mediaRepresentations = [
    {
      mediaId,
      sha256: hash,
      frameHashes: ['b'.repeat(64)],
      dhashes: ['1234567890abcdef'],
      durationMs: 1000,
    },
  ];
  return value;
}
export function tokenExtraction(): TextExtraction {
  return {
    schemaVersion: 2,
    topics: [],
    representation: { modelVersion: 'fixture-embedding-v1', vector: [1, 0] },
  };
}
