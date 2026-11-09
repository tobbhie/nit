import { describe, expect, it, vi } from 'vitest';
import { GatewayTextInterpreter, ToyTextInterpreter } from './interpreter';
import { PROMPT_HASH, validateExtraction } from './contracts';
import { extraction, input } from './narrative.fixtures';

describe('provider-neutral text gateway boundary', () => {
  it('provides a deterministic marked toy for development without network access', async () => {
    const toy = new ToyTextInterpreter();
    const signal = new AbortController().signal;
    expect(toy.identity.isToyImplementation).toBe(true);
    expect(await toy.interpret(input().source, signal)).toEqual(
      await toy.interpret(input('b').source, signal)
    );
  });
  const options = {
    endpoint: 'https://gateway.invalid/text',
    allowedOrigin: 'https://gateway.invalid',
    modelVersion: 'fixture-v1',
    bearer: 'fixture-only',
    maxInputBytes: 16000,
    maxOutputTokens: 100,
    maxChargeMicros: 10,
  };
  it('sends bounded untrusted text without token inventories or tools and pins response identity', async () => {
    const fetcher = vi.fn(() =>
      Promise.resolve(
        new Response(
          JSON.stringify({
            modelVersion: 'fixture-v1',
            promptHash: PROMPT_HASH,
            extraction: extraction(input().source.text),
          })
        )
      )
    );
    const result = await new GatewayTextInterpreter({ ...options, fetcher }).interpret(
      input().source,
      new AbortController().signal
    );
    expect(validateExtraction(result, input().source).topics).toHaveLength(1);
    const args = fetcher.mock.calls[0] as unknown as [string, RequestInit];
    const body = JSON.parse(args[1].body as string) as Record<string, unknown>;
    expect(body['untrustedSource']).toEqual({
      id: input().source.observationId,
      text: input().source.text,
    });
    expect(body['tools']).toBeUndefined();
    expect(body['tokenUniverse']).toBeUndefined();
    expect(args[1].redirect).toBe('error');
  });
  it('refuses insecure endpoints, cross-origin grants, embedded credentials and query-string secrets', () => {
    for (const endpoint of [
      'http://gateway.invalid/text',
      'https://other.invalid/text',
      'https://user:pass@gateway.invalid/text',
      'https://gateway.invalid/text?key=fixture',
    ])
      expect(() => new GatewayTextInterpreter({ ...options, endpoint })).toThrow();
  });
  it('does not call the network for an oversized request', async () => {
    const fetcher = vi.fn();
    await expect(
      new GatewayTextInterpreter({ ...options, maxInputBytes: 1, fetcher }).interpret(
        input().source,
        new AbortController().signal
      )
    ).rejects.toThrow('budget');
    expect(fetcher).not.toHaveBeenCalled();
  });
  it('rejects wrong response identity and oversized output without exposing provider bodies', async () => {
    for (const body of [
      JSON.stringify({
        modelVersion: 'wrong-model',
        promptHash: PROMPT_HASH,
        private: 'untrusted provider body',
      }),
      'x'.repeat(32001),
    ]) {
      const fetcher = vi.fn(() => Promise.resolve(new Response(body)));
      await expect(
        new GatewayTextInterpreter({ ...options, fetcher }).interpret(
          input().source,
          new AbortController().signal
        )
      ).rejects.toThrow('unavailable or invalid');
    }
  });
});
