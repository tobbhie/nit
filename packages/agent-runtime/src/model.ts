import {
  ModelIdentity,
  ModelInput,
  ModelResponseSchema,
  ReasoningModel,
  sameIdentity,
} from './types';
import { evidenceHash } from '@nit/signals';

export type GatewayFetch = (
  url: string,
  init: {
    method: 'POST';
    headers: Record<string, string>;
    body: string;
    signal: AbortSignal;
    redirect: 'error';
  }
) => Promise<{
  ok: boolean;
  status: number;
  headers: { get(name: string): string | null };
  body: ReadableStream<Uint8Array> | null;
}>;
export function gatewayIdentity(
  identity: ModelIdentity,
  endpoint: string,
  allowedOrigin: string
): ModelIdentity {
  const url = new URL(endpoint);
  if (
    url.protocol !== 'https:' ||
    url.origin !== allowedOrigin ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    identity.isToyImplementation
  )
    throw new Error('INVALID_MODEL_ENDPOINT');
  return { ...identity, configurationHash: evidenceHash({ endpoint: url.href, allowedOrigin }) };
}
/** Protocol adapter, not an arbitrary-fetch tool. Host/path are selected by operator configuration only. */
export class JsonGatewayModel implements ReasoningModel {
  readonly identity: ModelIdentity;
  private readonly endpoint: string;
  constructor(
    identity: ModelIdentity,
    endpoint: string,
    allowedOrigin: string,
    private readonly bearer: string,
    private readonly fetcher: GatewayFetch = fetch
  ) {
    const url = new URL(endpoint);
    if (
      url.protocol !== 'https:' ||
      url.origin !== allowedOrigin ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      !bearer.trim()
    )
      throw new Error('INVALID_MODEL_ENDPOINT');
    if (identity.isToyImplementation) throw new Error('GATEWAY_IDENTITY_CANNOT_BE_TOY');
    this.endpoint = url.href;
    this.identity = gatewayIdentity(identity, endpoint, allowedOrigin);
  }
  async plan(input: ModelInput, signal: AbortSignal): Promise<unknown> {
    try {
      if (!sameIdentity(input.identity, this.identity)) throw new Error('MODEL_IDENTITY_MISMATCH');
      const response = await this.fetcher(this.endpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${this.bearer}` },
        body: JSON.stringify(input),
        signal,
        redirect: 'error',
      });
      if (!response.ok || response.body === null) {
        await response.body?.cancel();
        throw new Error('MODEL_UNAVAILABLE');
      }
      const length = Number(response.headers.get('content-length') ?? 0);
      if (length > 96000) {
        await response.body.cancel();
        throw new Error('MODEL_RESPONSE_TOO_LARGE');
      }
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        while (!signal.aborted) {
          const next = await reader.read();
          if (next.done) break;
          size += next.value.byteLength;
          if (size > 96000) {
            await reader.cancel();
            throw new Error('MODEL_RESPONSE_TOO_LARGE');
          }
          chunks.push(next.value);
        }
      } finally {
        reader.releaseLock();
      }
      if (signal.aborted) throw new Error('MODEL_ABORTED');
      return ModelResponseSchema.parse(
        JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown
      );
    } catch {
      throw new Error('MODEL_RESPONSE_UNAVAILABLE_OR_INVALID');
    }
  }
}
