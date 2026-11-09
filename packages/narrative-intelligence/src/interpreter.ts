import { evidenceHash } from '@nit/signals';
import {
  InterpreterIdentity,
  NarrativePolicy,
  PROMPT_HASH,
  Prompt,
  SourceText,
  TextInterpreter,
  phrase,
} from './contracts';

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
}
/** Provider-neutral JSON protocol. Vendor translation and billing enforcement belong to the configured gateway. */
export class GatewayTextInterpreter implements TextInterpreter {
  readonly kind = 'gateway';
  readonly identity: InterpreterIdentity;
  private readonly endpoint: string;
  constructor(private readonly options: GatewayOptions) {
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
      promptHash: PROMPT_HASH,
      isToyImplementation: false,
    };
  }
  async interpret(source: SourceText, signal: AbortSignal): Promise<unknown> {
    const body = JSON.stringify({
      schemaVersion: 1,
      modelVersion: this.options.modelVersion,
      prompt: Prompt,
      promptHash: PROMPT_HASH,
      untrustedSource: { id: source.observationId, text: source.text },
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
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) break;
          const value: unknown = chunk.value;
          if (!(value instanceof Uint8Array)) throw new Error('Invalid gateway body');
          size += value.length;
          if (size > 32000) {
            await reader.cancel();
            throw new Error('Text gateway response exceeds budget');
          }
          chunks.push(value);
        }
      } finally {
        reader.releaseLock();
      }
      if (signal.aborted) throw new Error('Text gateway interrupted');
      const result = JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>;
      if (
        result['modelVersion'] !== this.options.modelVersion ||
        result['promptHash'] !== PROMPT_HASH
      )
        throw new Error('Text gateway identity mismatch');
      return result['extraction'];
    } catch {
      throw new Error('Text gateway response unavailable or invalid');
    }
  }
}
