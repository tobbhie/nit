import { evidenceHash } from '@nit/signals';
import {
  InterpreterIdentity,
  NarrativePolicy,
  PROMPT_HASH,
  Prompt,
  SourceText,
  TextInterpreter,
  phrase,
  RichPrompt,
  RICH_PROMPT_HASH,
  ExtractionSchema,
  validateExtraction,
} from './contracts';
import { MediaProcessor, PreparedMedia, readBounded } from './media';

/** Marked development scaffolding; the live runtime rejects it. */
export class ToyTextInterpreter implements TextInterpreter {
  readonly kind = 'catalog';
  readonly identity: InterpreterIdentity = {
    name: 'toy-text',
    version: 'toy-1',
    promptHash: evidenceHash('toy-text-v1'),
    isToyImplementation: true,
  };
  interpret(_source: SourceText, _signal: AbortSignal): Promise<unknown> {
    return Promise.resolve({ schemaVersion: 1, topics: [] });
  }
}

/** Literal production coverage without cultural inference or semantic confirmation. */
export class CatalogTextInterpreter implements TextInterpreter {
  readonly kind = 'catalog';
  readonly identity: InterpreterIdentity;
  constructor(private readonly policy: NarrativePolicy) {
    this.identity = {
      name: 'catalog-text',
      version: '1',
      promptHash: evidenceHash(policy.subjects),
      isToyImplementation: false,
    };
  }
  interpret(source: SourceText, signal: AbortSignal): Promise<unknown> {
    if (signal.aborted) return Promise.reject(new Error('Interpretation stopped'));
    const topics: {
      subjectQuote: string;
      catalystQuote: string;
      contextQuote: string;
      category: 'other';
      stance: 'uncertain';
    }[] = [];
    for (const subject of this.policy.subjects) {
      const alias = subject.aliases.find((a) => phrase(source.text, a));
      if (alias === undefined) continue;
      // Literal mode may surface a watch candidate; it never manufactures semantic support.
      const actual = source.text.toLowerCase().indexOf(alias.toLowerCase());
      if (actual >= 0)
        topics.push({
          subjectQuote: source.text.slice(actual, actual + alias.length),
          catalystQuote: source.text.slice(actual, actual + alias.length),
          contextQuote: source.text.slice(Math.max(0, actual - 500), actual + alias.length + 500),
          category: 'other',
          stance: 'uncertain',
        });
    }
    if (topics.length > 8) return Promise.reject(new Error('Catalog topic overflow'));
    return Promise.resolve({ schemaVersion: 1, topics });
  }
}
export interface GatewayOptions {
  endpoint: string;
  allowedOrigin: string;
  modelVersion: string;
  bearer: string;
  maxInputBytes: number;
  maxOutputTokens: number;
  maxChargeMicros: number;
  fetcher?: typeof fetch;
  rich?: { modelVersion: string; dimensions: number };
  mediaProcessor?: MediaProcessor;
  allowExternalMedia?: boolean;
}
/** Provider-neutral JSON protocol. Vendor translation and billing enforcement belong to the configured gateway. */
export class GatewayTextInterpreter implements TextInterpreter {
  readonly kind = 'gateway';
  readonly identity: InterpreterIdentity;
  private readonly endpoint: string;
  constructor(private readonly options: GatewayOptions) {
    if (
      options.mediaProcessor !== undefined &&
      (options.rich === undefined || options.allowExternalMedia !== true)
    )
      throw new Error('Explicit rich-media disclosure grant required');
    const url = new URL(options.endpoint);
    if (
      url.protocol !== 'https:' ||
      url.origin !== options.allowedOrigin ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      options.bearer.trim() === '' ||
      /[\r\n]/.test(options.bearer)
    )
      throw new Error('Invalid text gateway configuration');
    this.endpoint = url.href;
    this.identity = {
      name: 'json-text-gateway',
      version: options.modelVersion,
      promptHash: options.rich === undefined ? PROMPT_HASH : RICH_PROMPT_HASH,
      isToyImplementation: false,
    };
  }
  async interpret(source: SourceText, signal: AbortSignal): Promise<unknown> {
    const prepared: PreparedMedia[] = [];
    let tokenMetadata: import('./contracts').TextExtraction['tokenMetadata'];
    const attachments = [...(source.media ?? [])];
    if (
      source.purpose === 'token_representation' &&
      source.metadataUrl !== undefined &&
      this.options.mediaProcessor !== undefined
    ) {
      const document = await this.options.mediaProcessor.metadata(source.metadataUrl, signal);
      const artwork =
        document.image === null || attachments.length > 0
          ? null
          : { mediaId: source.observationId, sourceUrl: document.image };
      if (artwork !== null) attachments.push({ ...artwork, mediaType: 'image', durationMs: null });
      tokenMetadata = {
        sourceUrl: source.metadataUrl,
        sha256: document.sha256,
        text: document.text,
        artwork,
      };
    }
    if ((source.media?.length ?? 0) > 4) throw new Error('Attachment overflow');
    if (this.options.mediaProcessor !== undefined) {
      for (const ref of attachments)
        prepared.push(await this.options.mediaProcessor.prepare(ref, signal));
    }
    const promptHash = this.identity.promptHash;
    const body = JSON.stringify({
      schemaVersion: this.options.rich === undefined ? 1 : 2,
      modelVersion: this.options.modelVersion,
      prompt: this.options.rich === undefined ? Prompt : RichPrompt,
      promptHash,
      untrustedSource: {
        id: source.observationId,
        text: source.text,
        ...(tokenMetadata === undefined ? {} : { metadataDocument: tokenMetadata.text }),
        ...(this.options.rich === undefined
          ? {}
          : { media: prepared.map((m) => ({ ...m.representation, parts: m.parts })) }),
      },
      ...(this.options.rich === undefined
        ? {}
        : {
            purpose: source.purpose ?? 'observation',
            embedding: {
              modelVersion: this.options.rich.modelVersion,
              dimensions: this.options.rich.dimensions,
            },
          }),
      limits: {
        maxOutputTokens: this.options.maxOutputTokens,
        maxChargeMicros: this.options.maxChargeMicros,
      },
    });
    if (Buffer.byteLength(body) > this.options.maxInputBytes)
      throw new Error('Text gateway request exceeds byte budget');
    try {
      const response = await (this.options.fetcher ?? fetch)(this.endpoint, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: `Bearer ${this.options.bearer}`,
        },
        body,
        signal,
        redirect: 'error',
      });
      if (!response.ok || response.body === null) {
        await response.body?.cancel();
        throw new Error('Unavailable text gateway');
      }
      const bytes = await readBounded(
        response,
        this.options.rich === undefined ? 32000 : 262144,
        signal
      );
      if (signal.aborted) throw new Error('Text gateway interrupted');
      const result = JSON.parse(bytes.toString('utf8')) as Record<string, unknown>;
      if (
        result['modelVersion'] !== this.options.modelVersion ||
        result['promptHash'] !== promptHash
      )
        throw new Error('Text gateway identity mismatch');
      if (this.options.rich === undefined) {
        if (ExtractionSchema.parse(result['extraction']).schemaVersion !== 1)
          throw new Error('Rich schema requires explicit capability');
        return result['extraction'];
      }
      const parsed = ExtractionSchema.parse(result['extraction']);
      if (
        parsed.schemaVersion !== 2 ||
        parsed.mediaRepresentations !== undefined ||
        parsed.tokenMetadata !== undefined
      )
        throw new Error('Invalid rich interpretation');
      for (const embedding of [parsed.representation, ...parsed.topics.map((t) => t.meaning)]) {
        if (
          embedding !== undefined &&
          (embedding.modelVersion !== this.options.rich.modelVersion ||
            embedding.vector.length !== this.options.rich.dimensions)
        )
          throw new Error('Embedding identity mismatch');
      }
      if (parsed.representation === undefined || parsed.topics.some((t) => t.meaning === undefined))
        throw new Error('Missing configured semantic representations');
      return validateExtraction(
        {
          ...parsed,
          mediaRepresentations: prepared.map((m) => m.representation),
          ...(tokenMetadata === undefined ? {} : { tokenMetadata }),
        },
        source
      );
    } catch {
      throw new Error('Text gateway response unavailable or invalid');
    }
  }
}
