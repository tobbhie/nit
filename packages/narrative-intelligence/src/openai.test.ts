import { describe, expect, it, vi } from 'vitest';
import { OpenAITextInterpreter, OPENAI_NARRATIVE_ENDPOINT, OPENAI_NARRATIVE_MODEL } from './openai';
import { extraction, input, configuration } from './narrative.fixtures';
import { NarrativeRuntimeSchema } from './config';

const options = {
  endpoint: OPENAI_NARRATIVE_ENDPOINT,
  allowedOrigin: 'https://api.openai.com',
  modelVersion: OPENAI_NARRATIVE_MODEL,
  bearer: 'fixture-only',
  maxInputBytes: 16000,
  maxOutputTokens: 2048,
  maxChargeMicros: 30000,
};
function response(overrides: Record<string, unknown> = {}): Response {
  return new Response(
    JSON.stringify({
      model: OPENAI_NARRATIVE_MODEL,
      status: 'completed',
      error: null,
      incomplete_details: null,
      usage: { input_tokens: 500, output_tokens: 100 },
      output: [
        {
          type: 'message',
          role: 'assistant',
          status: 'completed',
          content: [{ type: 'output_text', text: JSON.stringify(extraction(input().source.text)) }],
        },
      ],
      ...overrides,
    })
  );
}
describe('direct OpenAI text boundary', () => {
  it('sends text only with strict output, no retention, tools or redirects', async () => {
    const fetcher = vi
      .fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
      .mockResolvedValue(response());
    const interpreter = new OpenAITextInterpreter({ ...options, fetcher });
    expect(await interpreter.interpret(input().source, new AbortController().signal)).toEqual(
      extraction(input().source.text)
    );
    const [url, init] = fetcher.mock.calls[0]!;
    expect(url).toBe(OPENAI_NARRATIVE_ENDPOINT);
    const body = JSON.parse(init!.body as string) as {
      store: boolean;
      tools: unknown[];
      reasoning: unknown;
      text: { format: { strict: boolean } };
    };
    expect(body.store).toBe(false);
    expect(body.tools).toEqual([]);
    expect(body.reasoning).toEqual({ effort: 'none' });
    expect(body.text.format.strict).toBe(true);
    expect(JSON.stringify(body)).not.toContain(input().source.observationId);
    expect(init!.redirect).toBe('error');
    expect(interpreter.getStats()).toEqual({
      requests: 1,
      inputTokens: 500,
      outputTokens: 100,
      estimatedCostMicros: 825,
      lastHttpStatus: 200,
      unmeteredRequests: 0,
    });
  });
  it('rejects redirects in configuration, model aliases, missing keys and rich runtime configuration', () => {
    for (const change of [
      { endpoint: 'https://other.invalid' },
      { bearer: '' },
      { bearer: 'key\nvalue' },
      { modelVersion: 'gpt-5.4-mini' },
    ])
      expect(() => new OpenAITextInterpreter({ ...options, ...change })).toThrow();
    const config = configuration();
    const runtimeOptions = {
      endpoint: options.endpoint,
      allowedOrigin: options.allowedOrigin,
      modelVersion: options.modelVersion,
      maxInputBytes: options.maxInputBytes,
      maxOutputTokens: options.maxOutputTokens,
      maxChargeMicros: options.maxChargeMicros,
    };
    expect(
      NarrativeRuntimeSchema.safeParse({
        ...config,
        interpreter: {
          kind: 'gateway',
          provider: 'openai',
          ...runtimeOptions,
          allowExternalContext: true,
          expiresAt: '2027-01-01T00:00:00.000Z',
        },
      }).success
    ).toBe(true);
    expect(
      NarrativeRuntimeSchema.safeParse({
        ...config,
        interpreter: {
          kind: 'gateway',
          provider: 'openai',
          ...runtimeOptions,
          allowExternalContext: true,
          expiresAt: '2027-01-01T00:00:00.000Z',
          rich: { modelVersion: 'test', dimensions: 2 },
        },
      }).success
    ).toBe(false);
  });
  it('refuses oversize, underfunded, aborted and media input before a purchase', async () => {
    const fetcher = vi.fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>();
    for (const change of [{ maxChargeMicros: 1 }, { maxInputBytes: 2000 }])
      await expect(
        new OpenAITextInterpreter({ ...options, ...change, fetcher }).interpret(
          change.maxInputBytes === undefined
            ? input().source
            : { ...input().source, text: 'x'.repeat(16000) },
          new AbortController().signal
        )
      ).rejects.toThrow('unavailable or invalid');
    const interpreter = new OpenAITextInterpreter({ ...options, fetcher });
    await expect(interpreter.interpret(input().source, AbortSignal.abort())).rejects.toThrow();
    await expect(
      interpreter.interpret(
        { ...input().source, purpose: 'token_representation' },
        new AbortController().signal
      )
    ).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
  });
  it('fails closed on refusals, truncation, wrong identity, invalid citations and tool calls', async () => {
    const badExtraction = extraction(input().source.text);
    badExtraction.topics[0]!.subjectQuote = 'Invented';
    for (const overrides of [
      { status: 'incomplete' },
      { model: 'different' },
      { error: { message: 'private' } },
      { output: [{ type: 'function_call' }] },
      {
        output: [
          {
            type: 'message',
            role: 'assistant',
            status: 'completed',
            content: [{ type: 'refusal', refusal: 'private' }],
          },
        ],
      },
      {
        output: [
          {
            type: 'message',
            role: 'assistant',
            status: 'completed',
            content: [{ type: 'output_text', text: JSON.stringify(badExtraction) }],
          },
        ],
      },
      { usage: { input_tokens: 999999, output_tokens: 1 } },
    ]) {
      const fetcher = vi
        .fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
        .mockResolvedValue(response(overrides));
      await expect(
        new OpenAITextInterpreter({ ...options, fetcher }).interpret(
          input().source,
          new AbortController().signal
        )
      ).rejects.toThrow('OpenAI narrative response unavailable or invalid');
      expect(fetcher).toHaveBeenCalledTimes(1);
    }
  });
  it('never retries provider failures and suppresses error bodies', async () => {
    for (const code of [401, 429, 500]) {
      const fetcher = vi
        .fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
        .mockResolvedValue(new Response('private-provider-body', { status: code }));
      await expect(
        new OpenAITextInterpreter({ ...options, fetcher }).interpret(
          input().source,
          new AbortController().signal
        )
      ).rejects.toThrow('OpenAI narrative response unavailable or invalid');
      expect(fetcher).toHaveBeenCalledTimes(1);
    }
    const fetcher = vi
      .fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
      .mockResolvedValue(new Response('x'.repeat(65537)));
    await expect(
      new OpenAITextInterpreter({ ...options, fetcher }).interpret(
        input().source,
        new AbortController().signal
      )
    ).rejects.toThrow();
  });
});
