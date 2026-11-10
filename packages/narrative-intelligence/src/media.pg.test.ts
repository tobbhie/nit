import { afterAll, beforeAll, beforeEach, describe, it, expect, vi } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { createHash, randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { join, resolve, sep } from 'node:path';
import { tmpdir } from 'node:os';
import { MediaProcessor, R2MediaArchive, ToyMediaArchive, readBounded } from './media';
import { mediaConfig, mediaId } from './rich.fixtures';

const run = promisify(execFile);
const signal = (): AbortSignal => new AbortController().signal;
const ref = {
  mediaId,
  mediaType: 'image' as const,
  sourceUrl: 'https://media.invalid/image',
  durationMs: null,
};
describe.skipIf(!hasTestDatabase())('bounded local media and independent object durability', () => {
  let harness: TestDatabase;
  let root: string;
  beforeAll(async () => {
    harness = await createTestDatabase('media_evidence');
    root = await mkdtemp(join(tmpdir(), 'nit-media-test-'));
  }, 30000);
  afterAll(async () => {
    await harness.teardown();
    if (!resolve(root).startsWith(resolve(tmpdir()) + sep))
      throw new Error('Test cleanup boundary');
    await rm(root, { recursive: true, force: true });
  });
  beforeEach(async () => {
    const r = await harness.db.query('TRUNCATE nit_media_objects');
    if (r.kind === 'failure') throw r.error;
  });
  function config(): ReturnType<typeof mediaConfig> {
    return mediaConfig(join(root, randomUUID()));
  }
  const archive = {
    isToyImplementation: false,
    put: (_bytes: Buffer, hash: string): Promise<string> => Promise.resolve(`nit/media/${hash}`),
  };
  it('refuses development-only archives in the production media processor', () => {
    expect(() => new MediaProcessor(harness.db, config(), new ToyMediaArchive())).toThrow(
      'toy archive'
    );
  });
  it('does not fetch disallowed origins, redirects or oversized declared durations', async () => {
    const fetcher = vi.fn();
    const processor = new MediaProcessor(harness.db, config(), archive, fetcher);
    await expect(
      processor.prepare({ ...ref, sourceUrl: 'https://private.invalid/secret' }, signal())
    ).rejects.toThrow('origin');
    await expect(processor.prepare({ ...ref, durationMs: 1000000 }, signal())).rejects.toThrow(
      'duration'
    );
    expect(fetcher).not.toHaveBeenCalled();
  });
  it('bounds metadata and media response bytes before decoding and rejects unsafe metadata hosts', async () => {
    const c = config();
    const fetcher = vi.fn(() => Promise.resolve(new Response('x'.repeat(c.maxBytes + 1))));
    const processor = new MediaProcessor(harness.db, c, archive, fetcher);
    await expect(processor.prepare(ref, signal())).rejects.toThrow('byte limit');
    await expect(processor.metadata('https://private.invalid/data', signal())).rejects.toThrow(
      'origin'
    );
    await expect(processor.metadata('https://media.invalid/data', signal())).rejects.toThrow(
      'byte limit'
    );
    const init = fetcher.mock.calls[0] as unknown as [URL, RequestInit];
    expect(init[1].redirect).toBe('error');
    expect(init[1].headers).toBeUndefined();
  });
  it('pauses media under scratch pressure before acquiring another resource', async () => {
    const c = config();
    await mkdir(join(c.scratchRoot, 'nit-media-stale'), { recursive: true });
    await writeFile(
      join(c.scratchRoot, 'nit-media-stale', 'original'),
      Buffer.alloc(c.scratchSoftBytes)
    );
    const fetcher = vi.fn();
    await expect(
      new MediaProcessor(harness.db, c, archive, fetcher).prepare(ref, signal())
    ).rejects.toThrow('pressure');
    expect(fetcher).not.toHaveBeenCalled();
  });
  it('rejects non-R2 archive endpoints and verifies private PUT bytes with HEAD metadata', async () => {
    const c = config();
    expect(
      () =>
        new R2MediaArchive(
          { ...c.archive, endpoint: 'https://other.invalid' },
          { accessKeyId: 'fixture', secretAccessKey: 'fixture' }
        )
    ).toThrow('R2');
    const bytes = Buffer.from('fixture-only');
    const hash = createHash('sha256').update(bytes).digest('hex');
    const fetcher = vi.fn((_url: unknown, init?: RequestInit) =>
      Promise.resolve(
        init?.method === 'PUT'
          ? new Response('')
          : new Response(null, {
              headers: { 'x-amz-meta-sha256': hash, 'content-length': String(bytes.length) },
            })
      )
    );
    const result = await new R2MediaArchive(
      c.archive,
      { accessKeyId: 'fixture', secretAccessKey: 'fixture-only' },
      fetcher
    ).put(bytes, hash, signal());
    expect(result).toBe(`nit/media/${hash}`);
    expect(fetcher).toHaveBeenCalledTimes(2);
    const request = fetcher.mock.calls[0]!;
    expect(String(request[0])).not.toContain('fixture');
    const headers = request[1]!.headers as Record<string, string>;
    expect(headers['authorization']).toMatch(
      /AWS4-HMAC-SHA256 Credential=fixture\/\d{8}\/auto\/s3\/aws4_request/
    );
    expect(headers['x-amz-content-sha256']).toBe(hash);
    expect(request[1]!.redirect).toBe('error');
    const bad = vi.fn((_url: unknown, init?: RequestInit) =>
      Promise.resolve(
        init?.method === 'PUT'
          ? new Response('')
          : new Response(null, {
              headers: { 'x-amz-meta-sha256': 'wrong', 'content-length': '12' },
            })
      )
    );
    await expect(
      new R2MediaArchive(
        c.archive,
        { accessKeyId: 'fixture', secretAccessKey: 'fixture-only' },
        bad
      ).put(bytes, hash, signal())
    ).rejects.toThrow('verification');
  });
  it('bounds failed hash-mismatch redownloads by the archive retry budget', async () => {
    const c = config();
    await harness.db.query(
      `INSERT INTO nit_media_objects(media_id,source_url,media_type,sha256,size_bytes,duration_ms,durability_state,upload_attempts,available_at)
      VALUES ($1,'https://media.invalid/image','image',$2,8,NULL,'failed',0,NOW())`,
      [mediaId, 'a'.repeat(64)]
    );
    const fetcher = vi.fn(() => Promise.resolve(new Response('changed')));
    const processor = new MediaProcessor(harness.db, c, archive, fetcher);
    for (let i = 0; i < 4; i++) {
      await harness.db.query(
        "UPDATE nit_media_objects SET last_attempt_at=NOW()-INTERVAL '1 hour'"
      );
      await processor.recover(signal());
    }
    expect(fetcher).toHaveBeenCalledTimes(c.archive.maxUploadAttempts);
    const state = await harness.db.query<{ upload_attempts: number; durability_state: string }>(
      'SELECT upload_attempts,durability_state FROM nit_media_objects'
    );
    expect(state.kind === 'success' && state.value[0]).toEqual({
      upload_attempts: 3,
      durability_state: 'failed',
    });
  });
  it('rejects redirects/status errors and cancelled reads', async () => {
    await expect(readBounded(new Response('', { status: 302 }), 10, signal())).rejects.toThrow(
      'Unavailable'
    );
    const abort = new AbortController();
    abort.abort();
    await expect(readBounded(new Response('data'), 10, abort.signal)).rejects.toThrow('cancelled');
  });
  it.skipIf(
    process.env['NIT_TEST_FFMPEG_PATH'] === undefined ||
      process.env['NIT_TEST_FFPROBE_PATH'] === undefined
  )(
    'decodes a real image while upload is stalled, retains the original, then removes it after verified durability',
    async () => {
      const c = config();
      const png = await run(
        c.ffmpegPath,
        [
          '-v',
          'error',
          '-f',
          'lavfi',
          '-i',
          'testsrc=size=64x64',
          '-frames:v',
          '1',
          '-f',
          'image2pipe',
          '-vcodec',
          'png',
          'pipe:1',
        ],
        { encoding: 'buffer', maxBuffer: 1048576, windowsHide: true }
      );
      let complete: ((key: string) => void) | undefined;
      const pending = {
        isToyImplementation: false,
        put: (_bytes: Buffer, hash: string): Promise<string> =>
          new Promise((resolveUpload) => {
            complete = (): void => resolveUpload(`nit/media/${hash}`);
          }),
      };
      const processor = new MediaProcessor(harness.db, c, pending, () =>
        Promise.resolve(new Response(png.stdout))
      );
      const result = await processor.prepare(ref, signal());
      expect(result.parts).toHaveLength(1);
      expect(result.parts[0]!.mime).toBe('image/webp');
      expect(result.representation.dhashes[0]).toHaveLength(16);
      const state = await harness.db.query<{
        durability_state: string;
        working_dir: string;
        local_state: string;
      }>('SELECT durability_state,working_dir,local_state FROM nit_media_objects');
      if (state.kind === 'failure') throw state.error;
      expect(state.value[0]).toMatchObject({
        durability_state: 'uploading',
        local_state: 'local_ready',
      });
      expect(await readFile(join(c.scratchRoot, state.value[0]!.working_dir, 'original'))).toEqual(
        png.stdout
      );
      complete!('done');
      await processor.drain();
      expect(await readdir(c.scratchRoot)).toHaveLength(0);
      expect((await harness.db.query("UPDATE nit_media_objects SET sha256='forged'")).kind).toBe(
        'failure'
      );
    }
  );
  it.skipIf(
    process.env['NIT_TEST_FFMPEG_PATH'] === undefined ||
      process.env['NIT_TEST_FFPROBE_PATH'] === undefined
  )(
    'extracts bounded WebP frames and mono audio from a real video, and recovers durability without decoding or inference again',
    async () => {
      const c = config();
      const video = join(root, `${randomUUID()}.mp4`);
      await run(
        c.ffmpegPath,
        [
          '-v',
          'error',
          '-f',
          'lavfi',
          '-i',
          'testsrc=size=64x64:rate=10',
          '-f',
          'lavfi',
          '-i',
          'sine=frequency=1000',
          '-t',
          '1',
          '-c:v',
          'mpeg4',
          '-c:a',
          'aac',
          video,
        ],
        { windowsHide: true }
      );
      const bytes = await readFile(video);
      const fetcher = vi.fn(() => Promise.resolve(new Response(bytes)));
      const failed = {
        isToyImplementation: false,
        put: (): Promise<string> => Promise.reject(new Error('Synthetic archive outage')),
      };
      const processor = new MediaProcessor(harness.db, c, failed, fetcher);
      const result = await processor.prepare(
        { ...ref, mediaType: 'video', durationMs: 1000 },
        signal()
      );
      await processor.drain();
      expect(result.parts.some((p) => p.kind === 'image')).toBe(true);
      expect(result.parts.some((p) => p.kind === 'audio')).toBe(true);
      expect(result.representation.frameHashes.length).toBeLessThanOrEqual(c.maxFrames);
      expect(result.representation.durationMs).toBeGreaterThanOrEqual(1000);
      await harness.db.query(
        "UPDATE nit_media_objects SET last_attempt_at=NOW()-INTERVAL '1 minute'"
      );
      const forbiddenFetch = vi.fn(() =>
        Promise.reject(new Error('Recovery should use the retained original'))
      );
      await new MediaProcessor(harness.db, c, archive, forbiddenFetch).recover(signal());
      expect(forbiddenFetch).not.toHaveBeenCalled();
      const state = await harness.db.query<{ durability_state: string; local_state: string }>(
        'SELECT durability_state,local_state FROM nit_media_objects'
      );
      expect(state.kind === 'success' && state.value[0]).toEqual({
        durability_state: 'durable',
        local_state: 'deleted',
      });
    }
  );
});
