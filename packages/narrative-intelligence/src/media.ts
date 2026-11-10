import { createHash, createHmac } from 'node:crypto';
import { spawn } from 'node:child_process';
import { lstat, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { basename, isAbsolute, join, resolve, sep } from 'node:path';
import { z } from 'zod';
import { Executor } from '@nit/database-public';
import { MediaRepresentationSchema, SourceAttachment } from './contracts';

export const MediaConfigSchema = z
  .object({
    allowedOrigins: z.array(z.string().url()).min(1).max(32),
    ipfsGatewayOrigin: z.string().url().optional(),
    maxBytes: z.number().int().min(1024).max(16777216),
    maxDurationMs: z.number().int().min(1000).max(180000),
    maxFrames: z.number().int().min(1).max(20),
    frameIntervalMs: z.number().int().min(1000).max(30000),
    scratchRoot: z.string().min(1).max(1000),
    ffmpegPath: z.string().min(1).max(1000),
    ffprobePath: z.string().min(1).max(1000),
    subprocessTimeoutMs: z.number().int().min(100).max(30000),
    scratchSoftBytes: z.number().int().min(1048576).max(1073741824),
    scratchHardBytes: z.number().int().min(1048576).max(1073741824),
    scratchMaxAgeMs: z.number().int().min(60000).max(86400000),
    archive: z
      .object({
        endpoint: z.string().url(),
        bucket: z.string().regex(/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/),
        maxUploadAttempts: z.number().int().min(1).max(8),
        timeoutMs: z.number().int().min(100).max(30000),
      })
      .strict(),
  })
  .strict()
  .refine(
    (c) =>
      c.scratchSoftBytes < c.scratchHardBytes &&
      isAbsolute(c.scratchRoot) &&
      isAbsolute(c.ffmpegPath) &&
      isAbsolute(c.ffprobePath),
    'Invalid scratch/binary configuration'
  );
export type MediaConfig = z.infer<typeof MediaConfigSchema>;
export type MediaRepresentation = z.infer<typeof MediaRepresentationSchema>;
export interface PreparedMedia {
  representation: MediaRepresentation;
  parts: { kind: 'image' | 'audio'; mime: string; base64: string; atMs: number | null }[];
}
const sha = (bytes: Uint8Array | string): string =>
  createHash('sha256').update(bytes).digest('hex');
async function query<T>(db: Executor, sql: string, values: unknown[] = []): Promise<T[]> {
  const result = await db.query<T>(sql, values);
  if (result.kind === 'failure') throw result.error;
  return result.value;
}
/** No source redirects, implicit credentials, arbitrary protocols or unbounded buffering. */
export async function readBounded(
  response: Response,
  limit: number,
  signal: AbortSignal
): Promise<Buffer> {
  if (!response.ok || response.body === null) {
    await response.body?.cancel();
    throw new Error('Unavailable bounded resource');
  }
  const reader = response.body.getReader();
  const parts: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      if (signal.aborted) throw new Error('Resource read cancelled');
      const item = await reader.read();
      if (item.done) break;
      const bytes: unknown = item.value;
      if (!(bytes instanceof Uint8Array)) throw new Error('Invalid resource chunk');
      size += bytes.length;
      if (size > limit) throw new Error('Resource exceeds byte limit');
      parts.push(bytes);
    }
    return Buffer.concat(parts);
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}
const hmac = (key: string | Buffer, text: string): Buffer =>
  createHmac('sha256', key).update(text).digest();
/** R2 S3 SigV4, auto region; only private bucket PUT/HEAD, never public/signed URLs. */
export class R2MediaArchive {
  readonly isToyImplementation = false;
  constructor(
    private readonly config: MediaConfig['archive'],
    private readonly credentials: { accessKeyId: string; secretAccessKey: string },
    private readonly fetcher: typeof fetch = fetch
  ) {
    const url = new URL(config.endpoint);
    if (
      url.protocol !== 'https:' ||
      !/^[a-f0-9]{32}\.r2\.cloudflarestorage\.com$/.test(url.hostname) ||
      url.pathname !== '/' ||
      url.search ||
      url.hash ||
      url.username ||
      url.password ||
      !/^[a-zA-Z0-9]+$/.test(credentials.accessKeyId) ||
      credentials.secretAccessKey.trim() === '' ||
      /[\r\n]/.test(credentials.secretAccessKey)
    )
      throw new Error('Invalid R2 archive configuration');
  }
  private async request(
    method: 'PUT' | 'HEAD',
    key: string,
    hash: string,
    signal: AbortSignal,
    body?: Buffer
  ): Promise<Response> {
    if (!/^nit\/media\/[a-f0-9]{64}$/.test(key)) throw new Error('Invalid archive object key');
    const url = new URL(`/${this.config.bucket}/${key}`, this.config.endpoint);
    const date = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
    const day = date.slice(0, 8);
    const payloadHash = body === undefined ? sha('') : sha(body);
    const headers: Record<string, string> = {
      host: url.host,
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': date,
      'x-amz-meta-sha256': hash,
    };
    const names = Object.keys(headers).sort();
    const canonical = `${method}\n${url.pathname}\n\n${names.map((name) => `${name}:${headers[name]}\n`).join('')}\n${names.join(';')}\n${payloadHash}`;
    const scope = `${day}/auto/s3/aws4_request`;
    const signing = hmac(
      hmac(hmac(hmac(`AWS4${this.credentials.secretAccessKey}`, day), 'auto'), 's3'),
      'aws4_request'
    );
    headers['authorization'] =
      `AWS4-HMAC-SHA256 Credential=${this.credentials.accessKeyId}/${scope}, SignedHeaders=${names.join(';')}, Signature=${hmac(signing, `AWS4-HMAC-SHA256\n${date}\n${scope}\n${sha(canonical)}`).toString('hex')}`;
    const timeout = AbortSignal.timeout(this.config.timeoutMs);
    return this.fetcher(url, {
      method,
      headers,
      redirect: 'error',
      signal: AbortSignal.any([signal, timeout]),
      ...(body === undefined ? {} : { body: new Uint8Array(body) }),
    });
  }
  async put(bytes: Buffer, hash: string, signal: AbortSignal): Promise<string> {
    const key = `nit/media/${hash}`;
    const response = await this.request('PUT', key, hash, signal, bytes);
    await response.body?.cancel();
    if (!response.ok) throw new Error('Media archive upload failed');
    const head = await this.request('HEAD', key, hash, signal);
    if (
      !head.ok ||
      head.headers.get('x-amz-meta-sha256') !== hash ||
      head.headers.get('content-length') !== String(bytes.length)
    )
      throw new Error('Media archive verification failed');
    return key;
  }
}
export interface MediaArchive {
  readonly isToyImplementation: boolean;
  put(bytes: Buffer, hash: string, signal: AbortSignal): Promise<string>;
}
export class ToyMediaArchive implements MediaArchive {
  readonly isToyImplementation = true;
  put(_bytes: Buffer, hash: string, _signal: AbortSignal): Promise<string> {
    return Promise.resolve(`toy/media/${hash}`);
  }
}
/** Uses one decoder process at a time; no shell and no URL is passed to a decoder. */
export class MediaProcessor {
  private readonly uploads = new Set<Promise<void>>();
  private readonly active = new Set<string>();
  private decoderIdentity: string | undefined;
  constructor(
    private readonly db: Executor,
    readonly config: MediaConfig,
    private readonly archive: MediaArchive,
    private readonly fetcher: typeof fetch = fetch
  ) {
    MediaConfigSchema.parse(config);
    if (archive.isToyImplementation)
      throw new Error('Production media processor refuses toy archive');
    for (const origin of config.allowedOrigins) {
      const url = new URL(origin);
      if (url.protocol !== 'https:' || url.origin !== origin || url.username || url.password)
        throw new Error('Invalid media origin');
    }
    if (
      config.ipfsGatewayOrigin !== undefined &&
      !config.allowedOrigins.includes(config.ipfsGatewayOrigin)
    )
      throw new Error('IPFS gateway must be an approved origin');
  }
  async drain(): Promise<void> {
    await Promise.all([...this.uploads]);
  }
  private workingPath(name: string): string {
    const root = resolve(this.config.scratchRoot);
    if (!/^nit-media-[a-zA-Z0-9]+$/.test(name)) throw new Error('Invalid working directory');
    const path = resolve(root, name);
    if (!path.startsWith(root + sep)) throw new Error('Scratch cleanup boundary violation');
    return path;
  }
  private async removeWorking(mediaId: string, path: string): Promise<void> {
    const checked = this.workingPath(basename(path));
    if (checked !== resolve(path) || this.active.has(checked)) return;
    await query(
      this.db,
      "UPDATE nit_media_objects SET local_state='deleted',working_dir=NULL WHERE media_id=$1 AND working_dir=$2",
      [mediaId, basename(path)]
    );
    await rm(checked, { recursive: true, force: true });
  }
  async metadata(
    sourceUrl: string,
    signal: AbortSignal
  ): Promise<{ text: string; sha256: string; image: string | null }> {
    const url = this.resourceUrl(sourceUrl);
    const bytes = await readBounded(
      await this.fetcher(url, { redirect: 'error', signal }),
      64000,
      signal
    );
    const parsed = z
      .object({ image: z.string().url().max(2048).optional() })
      .passthrough()
      .parse(JSON.parse(bytes.toString('utf8')) as unknown);
    return { text: bytes.toString('utf8'), sha256: sha(bytes), image: parsed.image ?? null };
  }
  private async binary(
    binary: string,
    args: string[],
    signal: AbortSignal,
    limit: number
  ): Promise<Buffer> {
    if (signal.aborted) throw new Error('Decoder cancelled');
    return new Promise<Buffer>((resolveResult, reject) => {
      const child = spawn(binary, args, {
        shell: false,
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe'],
        signal: AbortSignal.any([signal, AbortSignal.timeout(this.config.subprocessTimeoutMs)]),
      });
      const parts: Buffer[] = [];
      let bytes = 0;
      child.stdout.on('data', (chunk: Buffer) => {
        bytes += chunk.length;
        if (bytes > limit) child.kill();
        else parts.push(chunk);
      });
      child.stderr.resume();
      child.on('error', () => reject(new Error('Media decoder unavailable')));
      child.on('close', (code) =>
        code === 0 && bytes <= limit
          ? resolveResult(Buffer.concat(parts))
          : reject(new Error('Media decoder failed or exceeded budget'))
      );
    });
  }
  private resourceUrl(sourceUrl: string): URL {
    let value = sourceUrl;
    if (sourceUrl.startsWith('ipfs://')) {
      const match = /^ipfs:\/\/(?:ipfs\/)?([a-zA-Z0-9]{32,100})(\/[^?#]*)?$/.exec(sourceUrl);
      if (
        match === null ||
        this.config.ipfsGatewayOrigin === undefined ||
        (match[2] ?? '').split('/').some((p) => p === '..' || p === '.' || p.includes('%'))
      )
        throw new Error('Invalid IPFS resource');
      value = `${this.config.ipfsGatewayOrigin}/ipfs/${match[1]}${match[2] ?? ''}`;
    }
    const url = new URL(value);
    if (
      url.protocol !== 'https:' ||
      !this.config.allowedOrigins.includes(url.origin) ||
      url.username ||
      url.password ||
      url.hash
    )
      throw new Error('Unapproved media origin');
    return url;
  }
  private async scratch(checkPressure = true): Promise<void> {
    const root = resolve(this.config.scratchRoot);
    await mkdir(root, { recursive: true, mode: 0o700 });
    let size = 0;
    for (const item of await readdir(root, { withFileTypes: true })) {
      if (!item.name.startsWith('nit-media-') || !item.isDirectory() || item.isSymbolicLink())
        throw new Error('Unexpected scratch entry');
      const path = resolve(root, item.name);
      if (!path.startsWith(root + sep)) throw new Error('Scratch boundary violation');
      const info = await stat(path);
      if (!this.active.has(path) && Date.now() - info.mtimeMs > this.config.scratchMaxAgeMs) {
        await query(
          this.db,
          "UPDATE nit_media_objects SET local_state=CASE WHEN durability_state='durable' THEN 'deleted' ELSE 'lost' END,working_dir=NULL WHERE working_dir=$1",
          [item.name]
        );
        await rm(path, { recursive: true, force: true });
        continue;
      }
      for (const file of await readdir(path, { withFileTypes: true })) {
        if (!file.isFile() || file.isSymbolicLink())
          throw new Error('Unexpected media scratch file');
        size += (await stat(join(path, file.name))).size;
      }
    }
    if (
      checkPressure &&
      (size + this.config.maxBytes * 3 > this.config.scratchHardBytes ||
        size >= this.config.scratchSoftBytes)
    )
      throw new Error('Media scratch pressure');
  }
  async prepare(ref: SourceAttachment, signal: AbortSignal): Promise<PreparedMedia> {
    const url = this.resourceUrl(ref.sourceUrl);
    if (ref.durationMs !== null && ref.durationMs > this.config.maxDurationMs)
      throw new Error('Media duration exceeds budget');
    await this.scratch();
    const bytes = await readBounded(
      await this.fetcher(url, { redirect: 'error', signal }),
      this.config.maxBytes,
      signal
    );
    const hash = sha(bytes);
    const prior = (
      await query<{ sha256: string }>(
        this.db,
        'SELECT sha256 FROM nit_media_objects WHERE media_id=$1',
        [ref.mediaId]
      )
    )[0];
    if (prior !== undefined && prior.sha256 !== hash)
      throw new Error('Source media changed after capture');
    await query(
      this.db,
      `INSERT INTO nit_media_objects(media_id,source_url,media_type,sha256,size_bytes,duration_ms,durability_state,upload_attempts,available_at)
      VALUES ($1,$2,$3,$4,$5,$6,'pending',0,NOW()) ON CONFLICT DO NOTHING`,
      [ref.mediaId, ref.sourceUrl, ref.mediaType, hash, bytes.length, ref.durationMs]
    );
    // Upload begins independently; analysis never awaits archive latency/outages.
    const claimed = await query(
      this.db,
      `UPDATE nit_media_objects SET durability_state='uploading',upload_attempts=upload_attempts+1,last_attempt_at=NOW()
      WHERE media_id=$1 AND durability_state<>'durable' AND upload_attempts<$2
      AND (durability_state<>'uploading' OR last_attempt_at<NOW()-INTERVAL '60 seconds') RETURNING media_id`,
      [ref.mediaId, this.config.archive.maxUploadAttempts]
    );
    let working: string | undefined;
    if (claimed.length > 0) {
      const upload = this.archive
        .put(bytes, hash, signal)
        .then(async (key) => {
          await query(
            this.db,
            "UPDATE nit_media_objects SET durability_state='durable',object_key=$2 WHERE media_id=$1",
            [ref.mediaId, key]
          );
          if (working !== undefined) await this.removeWorking(ref.mediaId, working);
        })
        .catch(async () => {
          await query(
            this.db,
            "UPDATE nit_media_objects SET durability_state='failed' WHERE media_id=$1",
            [ref.mediaId]
          );
        })
        .catch(() => undefined);
      this.uploads.add(upload);
      void upload.finally(() => this.uploads.delete(upload));
    }
    const root = resolve(this.config.scratchRoot);
    const dir = await mkdtemp(join(root, 'nit-media-'));
    this.active.add(dir);
    try {
      const input = join(dir, 'original');
      await writeFile(input, bytes, { mode: 0o600 });
      working = dir;
      await query(
        this.db,
        "UPDATE nit_media_objects SET working_dir=$2,local_state='local_ready' WHERE media_id=$1",
        [ref.mediaId, basename(dir)]
      );
      if (this.decoderIdentity === undefined) {
        const ffmpeg =
          (await this.binary(this.config.ffmpegPath, ['-version'], signal, 16000))
            .toString('utf8')
            .split(/\r?\n/)[0] ?? '';
        const ffprobe =
          (await this.binary(this.config.ffprobePath, ['-version'], signal, 16000))
            .toString('utf8')
            .split(/\r?\n/)[0] ?? '';
        this.decoderIdentity = `${ffmpeg};${ffprobe};nit-dhash9x8-v1`.slice(0, 1000);
      }
      const probe = z
        .object({
          format: z.object({ duration: z.string().optional() }),
          streams: z.array(
            z.object({
              codec_type: z.string(),
              width: z.number().optional(),
              height: z.number().optional(),
            })
          ),
        })
        .parse(
          JSON.parse(
            (
              await this.binary(
                this.config.ffprobePath,
                [
                  '-v',
                  'error',
                  '-max_alloc',
                  '67108864',
                  '-protocol_whitelist',
                  'file,pipe',
                  '-show_entries',
                  'format=duration:stream=codec_type,width,height',
                  '-of',
                  'json',
                  input,
                ],
                signal,
                32000
              )
            ).toString('utf8')
          ) as unknown
        );
      const duration =
        probe.format.duration === undefined
          ? null
          : Math.ceil(Number(probe.format.duration) * 1000);
      if (
        duration !== null &&
        (!Number.isFinite(duration) || duration <= 0 || duration > this.config.maxDurationMs)
      )
        throw new Error('Invalid decoded duration');
      if (ref.mediaType !== 'image' && duration === null)
        throw new Error('Missing decoded duration');
      if (probe.streams.some((s) => (s.width ?? 0) * (s.height ?? 0) > 16777216))
        throw new Error('Oversized media dimensions');
      const parts: PreparedMedia['parts'] = [];
      const frameHashes: string[] = [];
      const dhashes: string[] = [];
      const base = [
        '-v',
        'error',
        '-nostdin',
        '-max_alloc',
        '67108864',
        '-threads',
        '1',
        '-protocol_whitelist',
        'file,pipe',
        '-i',
        input,
        '-threads',
        '1',
      ];
      if (probe.streams.some((s) => s.codec_type === 'video')) {
        const count = ref.mediaType === 'image' ? 1 : this.config.maxFrames;
        const interval =
          duration === null
            ? this.config.frameIntervalMs
            : Math.max(this.config.frameIntervalMs, duration / Math.max(1, count - 1));
        await this.binary(
          this.config.ffmpegPath,
          [
            ...base,
            '-vf',
            `${ref.mediaType === 'image' ? '' : `fps=1/${interval / 1000}:start_time=0:round=up:eof_action=pass,`}scale=768:768:force_original_aspect_ratio=decrease`,
            '-frames:v',
            String(count),
            '-c:v',
            'libwebp',
            '-an',
            join(dir, 'frame-%03d.webp'),
          ],
          signal,
          1000
        );
        const names = (await readdir(dir)).filter((f) => /^frame-\d{3}\.webp$/.test(f)).sort();
        let total = 0;
        for (const [i, file] of names.entries()) {
          const frame = await readFile(join(dir, file));
          total += frame.length;
          if (total > this.config.maxBytes) throw new Error('Decoded frame budget exceeded');
          const pixels = await this.binary(
            this.config.ffmpegPath,
            [
              '-v',
              'error',
              '-nostdin',
              '-threads',
              '1',
              '-protocol_whitelist',
              'file,pipe',
              '-i',
              join(dir, file),
              '-threads',
              '1',
              '-vf',
              'scale=9:8,format=gray',
              '-frames:v',
              '1',
              '-f',
              'rawvideo',
              'pipe:1',
            ],
            signal,
            72
          );
          if (pixels.length !== 72) throw new Error('Invalid visual hash pixels');
          let bits = 0n;
          for (let y = 0; y < 8; y++)
            for (let x = 0; x < 8; x++)
              bits = (bits << 1n) | (pixels[y * 9 + x]! > pixels[y * 9 + x + 1]! ? 1n : 0n);
          dhashes.push(bits.toString(16).padStart(16, '0'));
          frameHashes.push(sha(frame));
          parts.push({
            kind: 'image',
            mime: 'image/webp',
            base64: frame.toString('base64'),
            atMs: ref.mediaType === 'image' ? null : Math.round(i * interval),
          });
        }
        if (parts.length === 0) throw new Error('No decoded media frames');
      }
      if (probe.streams.some((s) => s.codec_type === 'audio') && ref.mediaType !== 'image') {
        const path = join(dir, 'audio.wav');
        await this.binary(
          this.config.ffmpegPath,
          [
            ...base,
            '-vn',
            '-ac',
            '1',
            '-ar',
            '16000',
            '-t',
            String(this.config.maxDurationMs / 1000),
            '-fs',
            String(this.config.maxBytes),
            path,
          ],
          signal,
          1000
        );
        const audio = await readFile(path);
        if (audio.length >= this.config.maxBytes)
          throw new Error('Audio representation exceeds budget');
        parts.push({
          kind: 'audio',
          mime: 'audio/wav',
          base64: audio.toString('base64'),
          atMs: null,
        });
      }
      if (parts.length === 0) throw new Error('Unsupported media streams');
      return {
        representation: {
          mediaId: ref.mediaId,
          sha256: hash,
          frameHashes,
          dhashes,
          durationMs: duration,
          decoderVersion: this.decoderIdentity,
          sampleTimesMs: parts.filter((p) => p.kind === 'image').map((p) => p.atMs ?? 0),
        },
        parts,
      };
    } finally {
      this.active.delete(dir);
      const object = (
        await query<{ durability_state: string }>(
          this.db,
          'SELECT durability_state FROM nit_media_objects WHERE media_id=$1',
          [ref.mediaId]
        )
      )[0];
      if (object?.durability_state === 'durable') await this.removeWorking(ref.mediaId, dir);
    }
  }
  /** Bounded restart recovery: immutable hashes must match a redownload. */
  async recover(signal: AbortSignal): Promise<void> {
    await this.scratch(false);
    const pending = await query<{
      media_id: string;
      source_url: string;
      media_type: SourceAttachment['mediaType'];
      duration_ms: number | null;
      sha256: string;
      working_dir: string | null;
    }>(
      this.db,
      `SELECT media_id,source_url,media_type,duration_ms,sha256,working_dir FROM nit_media_objects WHERE durability_state<>'durable' AND upload_attempts<$1
        AND (durability_state<>'uploading' OR last_attempt_at<NOW()-INTERVAL '60 seconds')
        AND (last_attempt_at IS NULL OR last_attempt_at<NOW()-POWER(2,upload_attempts)*INTERVAL '1 second') ORDER BY available_at LIMIT 1`,
      [this.config.archive.maxUploadAttempts]
    );
    for (const item of pending) {
      const claimed = await query(
        this.db,
        `UPDATE nit_media_objects SET durability_state='uploading',upload_attempts=upload_attempts+1,last_attempt_at=NOW()
        WHERE media_id=$1 AND durability_state<>'durable' AND upload_attempts<$2
        AND (durability_state<>'uploading' OR last_attempt_at<NOW()-INTERVAL '60 seconds') RETURNING media_id`,
        [item.media_id, this.config.archive.maxUploadAttempts]
      );
      if (claimed.length === 0) continue;
      try {
        let bytes: Buffer | undefined;
        if (item.working_dir !== null) {
          const dir = this.workingPath(item.working_dir);
          if (this.active.has(dir)) throw new Error('Recovery directory is active');
          try {
            const info = await lstat(dir);
            const file = await lstat(join(dir, 'original'));
            if (
              info.isSymbolicLink() ||
              file.isSymbolicLink() ||
              !file.isFile() ||
              file.size > this.config.maxBytes
            )
              throw new Error('Unsafe working copy');
            bytes = await readFile(join(dir, 'original'));
          } catch {
            await query(
              this.db,
              "UPDATE nit_media_objects SET local_state='lost',working_dir=NULL WHERE media_id=$1",
              [item.media_id]
            );
          }
        }
        if (bytes === undefined) {
          const url = this.resourceUrl(item.source_url);
          bytes = await readBounded(
            await this.fetcher(url, { redirect: 'error', signal }),
            this.config.maxBytes,
            signal
          );
        }
        if (sha(bytes) !== item.sha256) throw new Error('Recovery source hash mismatch');
        const key = await this.archive.put(bytes, item.sha256, signal);
        await query(
          this.db,
          "UPDATE nit_media_objects SET durability_state='durable',object_key=$2 WHERE media_id=$1",
          [item.media_id, key]
        );
        if (item.working_dir !== null)
          await this.removeWorking(item.media_id, this.workingPath(item.working_dir));
      } catch {
        await query(
          this.db,
          "UPDATE nit_media_objects SET durability_state='failed' WHERE media_id=$1",
          [item.media_id]
        );
      }
    }
  }
}
