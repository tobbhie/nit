import { z } from 'zod';
import { PROMPT_HASH, Prompt, SourceText, TextInterpreter, validateExtraction } from './contracts';
import { readBounded } from './media';

export const OPENAI_NARRATIVE_ENDPOINT = 'https://api.openai.com/v1/responses';
export const OPENAI_NARRATIVE_MODEL = 'gpt-5.4-mini-2026-03-17';
export interface OpenAITextOptions {
  endpoint: string;
  allowedOrigin: string;
  modelVersion: string;
  bearer: string;
  maxInputBytes: number;
  maxOutputTokens: number;
  maxChargeMicros: number;
  fetcher?: typeof fetch;
}
const OutputSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['schemaVersion', 'topics'],
  properties: {
    schemaVersion: { type: 'integer', enum: [1] },
    topics: {
      type: 'array',
      maxItems: 8,
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['subjectQuote', 'catalystQuote', 'contextQuote', 'category', 'stance'],
        properties: {
          subjectQuote: { type: 'string', minLength: 1, maxLength: 200 },
          catalystQuote: { type: 'string', minLength: 1, maxLength: 1000 },
          contextQuote: { type: 'string', minLength: 1, maxLength: 2000 },
          category: {
            type: 'string',
            enum: ['news', 'meme', 'endorsement', 'promotion', 'cto', 'other'],
          },
          stance: { type: 'string', enum: ['support', 'contradiction', 'uncertain'] },
        },
      },
    },
  },
};
const ResponseSchema = z.object({
  model: z.literal(OPENAI_NARRATIVE_MODEL),
  status: z.literal('completed'),
  error: z.null(),
  incomplete_details: z.null(),
  usage: z.object({
    input_tokens: z.number().int().nonnegative(),
    output_tokens: z.number().int().nonnegative(),
  }),
  output: z
    .array(
      z.object({
        type: z.literal('message'),
        role: z.literal('assistant'),
        status: z.literal('completed'),
        content: z
          .array(z.object({ type: z.literal('output_text'), text: z.string().max(32000) }))
          .length(1),
      })
    )
    .length(1),
});

/** Text-only provider adapter. No tools, retries, model fallback or vendor error bodies. */
export class OpenAITextInterpreter implements TextInterpreter {
  readonly kind = 'gateway';
  readonly identity = {
    name: 'openai-responses-text',
    version: OPENAI_NARRATIVE_MODEL,
    promptHash: PROMPT_HASH,
    isToyImplementation: false,
  };
  private requests = 0;
  private inputTokens = 0;
  private outputTokens = 0;
  private meteredRequests = 0;
  private lastHttpStatus: number | null = null;
  constructor(private readonly options: OpenAITextOptions) {
    if (
      options.endpoint !== OPENAI_NARRATIVE_ENDPOINT ||
      options.allowedOrigin !== 'https://api.openai.com' ||
      options.modelVersion !== OPENAI_NARRATIVE_MODEL ||
      !options.bearer.trim() ||
      /[\r\n]/.test(options.bearer) ||
      !Number.isInteger(options.maxInputBytes) ||
      options.maxInputBytes < 2000 ||
      options.maxInputBytes > 64000 ||
      !Number.isInteger(options.maxOutputTokens) ||
      options.maxOutputTokens < 64 ||
      options.maxOutputTokens > 8000 ||
      !Number.isInteger(options.maxChargeMicros) ||
      options.maxChargeMicros < 1 ||
      options.maxChargeMicros > 10000000
    )
      throw new Error('Invalid OpenAI narrative configuration');
  }
  getStats(): {
    requests: number;
    inputTokens: number;
    outputTokens: number;
    estimatedCostMicros: number;
    lastHttpStatus: number | null;
    unmeteredRequests: number;
  } {
    return {
      requests: this.requests,
      inputTokens: this.inputTokens,
      outputTokens: this.outputTokens,
      estimatedCostMicros: Math.ceil(this.inputTokens * 0.75 + this.outputTokens * 4.5),
      lastHttpStatus: this.lastHttpStatus,
      unmeteredRequests: this.requests - this.meteredRequests,
    };
  }
  async interpret(source: SourceText, signal: AbortSignal): Promise<unknown> {
    try {
      if (
        signal.aborted ||
        (source.media?.length ?? 0) > 0 ||
        source.purpose === 'token_representation'
      )
        throw new Error('Text-only input required');
      const body = JSON.stringify({
        model: this.options.modelVersion,
        instructions: Prompt,
        input: [{ role: 'user', content: [{ type: 'input_text', text: source.text }] }],
        reasoning: { effort: 'none' },
        text: {
          format: {
            type: 'json_schema',
            name: 'nit_text_extraction',
            strict: true,
            schema: OutputSchema,
          },
        },
        max_output_tokens: this.options.maxOutputTokens,
        store: false,
        tools: [],
        service_tier: 'default',
      });
      const bytes = Buffer.byteLength(body);
      // UTF-8 byte count plus framing allowance bounds text tokens conservatively.
      // Standard token prices checked 2026-11-11; uncached input, all output charged.
      const inputTokenBound = bytes + 1024;
      const costBound = Math.ceil(inputTokenBound * 0.75 + this.options.maxOutputTokens * 4.5);
      if (bytes > this.options.maxInputBytes || costBound > this.options.maxChargeMicros)
        throw new Error('OpenAI narrative request exceeds reservation');
      this.requests++;
      const response = await (this.options.fetcher ?? fetch)(OPENAI_NARRATIVE_ENDPOINT, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: `Bearer ${this.options.bearer}`,
        },
        body,
        signal,
        redirect: 'error',
      });
      this.lastHttpStatus = response.status;
      if (!response.ok || response.body === null) {
        await response.body?.cancel();
        throw new Error('OpenAI unavailable');
      }
      const result = ResponseSchema.parse(
        JSON.parse((await readBounded(response, 65536, signal)).toString('utf8'))
      );
      if (
        signal.aborted ||
        result.usage.input_tokens > inputTokenBound ||
        result.usage.output_tokens > this.options.maxOutputTokens
      )
        throw new Error('OpenAI response exceeds reservation');
      this.inputTokens += result.usage.input_tokens;
      this.outputTokens += result.usage.output_tokens;
      this.meteredRequests++;
      return validateExtraction(JSON.parse(result.output[0]!.content[0]!.text), source);
    } catch {
      throw new Error('OpenAI narrative response unavailable or invalid');
    }
  }
}
