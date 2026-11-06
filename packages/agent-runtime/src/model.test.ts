import { describe, expect, it } from 'vitest';
import { JsonGatewayModel, GatewayFetch } from './model';
import { loadAgentGuides } from './guides';
import { configuration, task, answer, conclusion, IDENTITY } from './agent.fixtures';
import { ModelInput, modelCost } from './types';

async function input(): Promise<ModelInput> {
  const config = configuration(new Date());
  return {
    budget: {
      remainingSteps: 4,
      remainingToolCalls: 3,
      remainingReservedTokens: 132000,
      remainingReservedCostMicros: 1000000,
    },
    protocolVersion: 1,
    requestId: 'fixture-request',
    identity: { ...IDENTITY, isToyImplementation: false },
    guides: await loadAgentGuides(),
    task: task(config),
    tools: [],
    history: [],
    maxInputTokens: 32000,
    maxOutputTokens: 1000,
  };
}
describe('provider-neutral model gateway', () => {
  it('pins the operator-selected HTTPS endpoint, rejects redirects and preserves the structured protocol', async () => {
    const request = await input();
    let observed: Parameters<GatewayFetch>[1] | undefined;
    const fetcher: GatewayFetch = (_url, init) => {
      observed = init;
      return Promise.resolve({
        ok: true,
        status: 200,
        headers: { get: () => null },
        body: new ReadableStream({
          start(controller): void {
            controller.enqueue(
              Buffer.from(
                JSON.stringify(answer(request, { kind: 'conclusion', conclusion: conclusion() }))
              )
            );
            controller.close();
          },
        }),
      });
    };
    const model = new JsonGatewayModel(
      request.identity,
      'https://model.example.test/reason',
      'https://model.example.test',
      'fixture-bearer',
      fetcher
    );
    expect(
      await model.plan({ ...request, identity: model.identity }, new AbortController().signal)
    ).toMatchObject({
      protocolVersion: 1,
      requestId: request.requestId,
    });
    expect(observed?.redirect).toBe('error');
    expect(observed?.headers['authorization']).toBe('Bearer fixture-bearer');
    expect(model.identity.configurationHash).not.toBe(request.identity.configurationHash);
    expect(
      () =>
        new JsonGatewayModel(
          request.identity,
          'https://other.example.test/reason',
          'https://model.example.test',
          'fixture-bearer',
          fetcher
        )
    ).toThrow('INVALID_MODEL_ENDPOINT');
    expect(
      () =>
        new JsonGatewayModel(
          request.identity,
          'https://model.example.test/reason?key=fixture',
          'https://model.example.test',
          'fixture-bearer',
          fetcher
        )
    ).toThrow('INVALID_MODEL_ENDPOINT');
  });
  it('sanitizes provider failures without retaining response bodies or credential-bearing URLs', async () => {
    const request = await input();
    const fetcher: GatewayFetch = () =>
      Promise.reject(new Error('https://provider.example/fixture-bearer'));
    const model = new JsonGatewayModel(
      request.identity,
      'https://model.example.test/reason',
      'https://model.example.test',
      'fixture-bearer',
      fetcher
    );
    await expect(
      model.plan({ ...request, identity: model.identity }, new AbortController().signal)
    ).rejects.toThrow('MODEL_RESPONSE_UNAVAILABLE_OR_INVALID');
  });
  it('cancels an oversized response stream before buffering it', async () => {
    const request = await input();
    let cancelled = false;
    const fetcher: GatewayFetch = () =>
      Promise.resolve({
        ok: true,
        status: 200,
        headers: { get: () => String(1000000) },
        body: new ReadableStream({
          cancel(): void {
            cancelled = true;
          },
        }),
      });
    const model = new JsonGatewayModel(
      request.identity,
      'https://model.example.test/reason',
      'https://model.example.test',
      'fixture-bearer',
      fetcher
    );
    await expect(
      model.plan({ ...request, identity: model.identity }, new AbortController().signal)
    ).rejects.toThrow('MODEL_RESPONSE_UNAVAILABLE_OR_INVALID');
    expect(cancelled).toBe(true);
  });
  it('uses pinned rates with integer rounding rather than trusting model-supplied prices', () => {
    const config = configuration(new Date());
    expect(
      modelCost(1, 1, { ...config.limits, inputMicrosPerMillion: 1, outputMicrosPerMillion: 1 })
    ).toBe(1);
    expect(modelCost(100, 30, config.limits)).toBe(160);
  });
});
