import { Chain, LaunchSource, Result, failure, success } from '@nit/domain';
import { ProviderIdentity } from '@nit/provider-interfaces';
import {
  DecodedMarketOccurrence,
  ReconciledMarketOccurrence,
  ReconciledLaunch,
  SolanaReconciliationPage,
  SolanaReconciliationPageRequest,
  SolanaReconciliationSource,
} from './reconciliation';

const MAX_SIGNATURE_PAGE_SIZE = 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const DEFAULT_MIN_REQUEST_INTERVAL_MS = 200;

export class SolanaRpcError extends Error {
  constructor(
    message: string,
    readonly retryable: boolean
  ) {
    super(message);
    this.name = 'SolanaRpcError';
  }
}

interface RpcHttpResponse {
  readonly ok: boolean;
  readonly status: number;
  json(): Promise<unknown>;
}

export type SolanaRpcFetch = (
  input: string,
  init: {
    method: 'POST';
    headers: Readonly<Record<string, string>>;
    body: string;
    signal: AbortSignal;
  }
) => Promise<RpcHttpResponse>;

export interface SolanaLaunchTransaction {
  signature: string;
  slot: number;
  launchSource: LaunchSource;
  programAddress: string;
  observedAt: Date;
  /** The complete `getTransaction` result, before launchpad-specific decoding. */
  transaction: Record<string, unknown>;
}

/**
 * Converts one Solana transaction into exact launch/venue occurrences.
 *
 * Instruction layouts belong to the configured launchpad integration, not to
 * JSON-RPC transport. Keeping that boundary explicit also prevents a program
 * address or an unversioned binary layout from becoming an infrastructure
 * constant.
 */
export interface SolanaMarketTransactionDecoder {
  decode(transaction: SolanaLaunchTransaction): Result<readonly DecodedMarketOccurrence[], Error>;
}

/** @deprecated Use `SolanaMarketTransactionDecoder`. */
export type SolanaLaunchTransactionDecoder = SolanaMarketTransactionDecoder;

export interface SolanaRpcReconciliationOptions {
  endpoint: string;
  programAddress: string;
  launchSource: LaunchSource;
  decoder: SolanaMarketTransactionDecoder;
  fetch?: SolanaRpcFetch;
  requestTimeoutMs?: number;
  /** Defaults to 200ms, keeping one adapter at or below five RPC calls/second. */
  minRequestIntervalMs?: number;
  clock?: () => number;
  sleep?: (milliseconds: number) => Promise<void>;
  creationHistoryProgress?: CreationHistoryProgressStore;
}

export interface CreationHistoryProgress {
  beforeSignature?: string;
  oldest: SignatureInfo[];
  complete: boolean;
  nextCandidate: number;
}
export interface CreationHistoryProgressStore {
  get(mint: string): Promise<Result<CreationHistoryProgress | null, Error>>;
  put(mint: string, progress: CreationHistoryProgress): Promise<Result<void, Error>>;
}

export interface SolanaConfirmedTransactionSource extends ProviderIdentity {
  readonly chain: Chain.SOLANA;
  readonly launchSource: LaunchSource;
  fetchConfirmedTransaction(
    signature: string,
    expectedSlot: number
  ): Promise<Result<readonly ReconciledMarketOccurrence[], Error>>;
}

interface SignatureInfo {
  signature: string;
  slot: number;
  err: unknown;
}

/**
 * HTTP JSON-RPC implementation of the finalized-slot reconciliation source.
 *
 * It deliberately does not know a launchpad instruction layout. The injected
 * decoder owns that versioned protocol detail; this class owns commitment,
 * paging, rate limiting, transport validation, and the scan bounds.
 */
export class SolanaRpcReconciliationSource implements SolanaReconciliationSource {
  readonly chain = Chain.SOLANA;
  readonly providerName = 'solana-json-rpc';
  readonly isToyImplementation = false;
  readonly interfaceVersion = '1.0.0';
  readonly launchSource: LaunchSource;

  private readonly endpoint: string;
  private readonly programAddress: string;
  private readonly decoder: SolanaMarketTransactionDecoder;
  private readonly fetch: SolanaRpcFetch;
  private readonly requestTimeoutMs: number;
  private readonly minRequestIntervalMs: number;
  private readonly clock: () => number;
  private readonly sleep: (milliseconds: number) => Promise<void>;
  private requestId = 0;
  private nextRequestAt = 0;
  private rateLimitTail: Promise<void> = Promise.resolve();
  private retryCount = 0;
  private readonly creationHistoryProgress: CreationHistoryProgressStore | undefined;

  getRetryCount(): number {
    return this.retryCount;
  }

  constructor(options: SolanaRpcReconciliationOptions) {
    this.creationHistoryProgress = options.creationHistoryProgress;
    this.endpoint = validEndpoint(options.endpoint);
    this.programAddress = required(options.programAddress, 'Solana program address');
    if (!Object.values(LaunchSource).includes(options.launchSource)) {
      throw new Error(`Unknown Solana launch source '${String(options.launchSource)}'`);
    }
    this.launchSource = options.launchSource;
    this.decoder = options.decoder;
    this.fetch =
      options.fetch ??
      ((input, init): Promise<RpcHttpResponse> =>
        fetch(input, {
          method: init.method,
          headers: init.headers,
          body: init.body,
          signal: init.signal,
        }));
    this.requestTimeoutMs = positiveInteger(
      options.requestTimeoutMs,
      DEFAULT_REQUEST_TIMEOUT_MS,
      'request timeout'
    );
    this.minRequestIntervalMs = nonnegativeInteger(
      options.minRequestIntervalMs,
      DEFAULT_MIN_REQUEST_INTERVAL_MS,
      'minimum request interval'
    );
    this.clock = options.clock ?? Date.now;
    this.sleep =
      options.sleep ??
      ((milliseconds): Promise<void> =>
        new Promise((resolve) => setTimeout(resolve, milliseconds)));
  }

  async getFinalizedSlot(): Promise<Result<number, Error>> {
    const response = await this.rpc('getSlot', [{ commitment: 'finalized' }]);
    if (response.kind === 'failure') return response;
    return isSlot(response.value)
      ? success(response.value)
      : failure(new Error('Solana RPC returned an invalid finalized slot'));
  }

  async fetchReconciliationPage(
    request: SolanaReconciliationPageRequest
  ): Promise<Result<SolanaReconciliationPage, Error>> {
    const invalid = validatePageRequest(request);
    if (invalid !== null) return failure(invalid);

    const config: Record<string, unknown> = {
      commitment: 'confirmed',
      limit: request.limit,
    };
    if (request.beforeSignature !== undefined) {
      config.before = request.beforeSignature;
    }

    const signaturesResponse = await this.rpc('getSignaturesForAddress', [
      this.programAddress,
      config,
    ]);
    if (signaturesResponse.kind === 'failure') return signaturesResponse;
    const parsed = parseSignatures(signaturesResponse.value);
    if (parsed.kind === 'failure') return parsed;

    const entries = parsed.value;
    const reachedFloor =
      entries.length < request.limit ||
      entries.some((entry) => entry.slot < request.fromSlotInclusive);
    const occurrences: ReconciledMarketOccurrence[] = [];

    for (const entry of entries) {
      if (
        entry.err !== null ||
        entry.slot < request.fromSlotInclusive ||
        entry.slot > request.throughFinalizedSlot
      ) {
        continue;
      }

      const transaction = await this.fetchConfirmedTransaction(entry.signature, entry.slot);
      if (transaction.kind === 'failure') return transaction;
      occurrences.push(...transaction.value);
    }

    const last = entries.at(-1);
    return success({
      occurrences,
      reachedFloor,
      ...(!reachedFloor && last !== undefined ? { nextBeforeSignature: last.signature } : {}),
    });
  }

  async fetchConfirmedTransaction(
    signature: string,
    expectedSlot: number
  ): Promise<Result<readonly ReconciledMarketOccurrence[], Error>> {
    const transaction = await this.readConfirmedTransaction(signature, expectedSlot);
    return transaction.kind === 'failure'
      ? transaction
      : this.decodeConfirmedTransaction(signature, expectedSlot, transaction.value);
  }

  /** Read once for all lifecycle decoders sharing an emitting program. */
  async readConfirmedTransaction(
    signature: string,
    expectedSlot: number
  ): Promise<Result<Record<string, unknown>, Error>> {
    if (signature.trim() === '' || !isSlot(expectedSlot)) {
      return failure(new Error('Solana confirmed transaction identity is invalid'));
    }
    let transactionResponse: Result<unknown, Error> = success(null);
    for (let attempt = 0; attempt < 3; attempt++) {
      transactionResponse = await this.rpc('getTransaction', [
        signature,
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          maxSupportedTransactionVersion: 1,
        },
      ]);
      if (transactionResponse.kind === 'failure' || transactionResponse.value !== null) break;
      if (attempt < 2) {
        this.retryCount++;
        await this.sleep(250 * 2 ** attempt);
      }
    }
    if (transactionResponse.kind === 'failure') return transactionResponse;
    if (transactionResponse.value === null) {
      return failure(new Error(`Solana RPC did not return transaction '${signature}'`));
    }
    if (!isRecord(transactionResponse.value)) {
      return failure(new Error('Solana RPC returned a malformed transaction'));
    }
    if (transactionResponse.value.slot !== expectedSlot) {
      return failure(
        new Error(`Solana RPC returned a slot mismatch for transaction '${signature}'`)
      );
    }

    return success(transactionResponse.value);
  }

  /** Bounded mint-specific history; never scan every trade of the emitting program. */
  async findCreation(
    mint: string,
    throughSlot: number
  ): Promise<Result<ReconciledMarketOccurrence, Error>> {
    if (
      this.launchSource !== LaunchSource.PUMP_FUN ||
      !/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(mint) ||
      !isSlot(throughSlot)
    )
      return failure(new Error('Invalid creation recovery target'));
    const deadline = this.clock() + 45000;
    const stored =
      this.creationHistoryProgress === undefined
        ? success(null)
        : await this.creationHistoryProgress.get(mint);
    if (stored.kind === 'failure') return stored;
    let before = stored.value?.beforeSignature;
    let oldest: SignatureInfo[] = stored.value?.oldest ?? [];
    const cursors = new Set<string>();
    let complete = stored.value?.complete ?? false;
    let nextCandidate = stored.value?.nextCandidate ?? 0;
    const checkpoint = (): Promise<Result<void, Error>> =>
      this.creationHistoryProgress?.put(mint, {
        ...(before === undefined ? {} : { beforeSignature: before }),
        oldest,
        complete,
        nextCandidate,
      }) ?? Promise.resolve(success(undefined));
    if (before !== undefined) cursors.add(before);
    for (let page = 0; page < 20 && !complete; page++) {
      if (this.clock() > deadline)
        return failure(new Error('Creation history recovery budget exhausted'));
      const response = await this.rpc('getSignaturesForAddress', [
        mint,
        { commitment: 'confirmed', limit: 1000, ...(before === undefined ? {} : { before }) },
      ]);
      if (response.kind === 'failure') return response;
      const parsed = parseSignatures(response.value);
      if (parsed.kind === 'failure') return parsed;
      const entries = parsed.value;
      oldest = [...oldest, ...entries.filter((e) => e.err === null && e.slot <= throughSlot)].slice(
        -16
      );
      if (entries.length < 1000) {
        complete = true;
        const saved = await checkpoint();
        if (saved.kind === 'failure') return saved;
        break;
      }
      const next = entries.at(-1)!.signature;
      if (cursors.has(next)) return failure(new Error('Creation history cursor did not advance'));
      cursors.add(next);
      before = next;
      const saved = await checkpoint();
      if (saved.kind === 'failure') return saved;
    }
    if (!complete) return failure(new Error('Creation history exceeds recovery page budget'));
    const candidates = [...oldest].reverse();
    for (; nextCandidate < candidates.length; nextCandidate++) {
      const entry = candidates[nextCandidate]!;
      if (this.clock() > deadline)
        return failure(new Error('Creation history recovery budget exhausted'));
      const result = await this.fetchConfirmedTransaction(entry.signature, entry.slot);
      if (result.kind === 'failure') return result;
      const found = result.value.find(
        (o): o is ReconciledMarketOccurrence & { kind: 'launch'; launch: ReconciledLaunch } =>
          o.kind === 'launch' && o.launch.address === mint
      );
      if (found !== undefined) return success(found);
      const saved = await this.creationHistoryProgress?.put(mint, {
        ...(before === undefined ? {} : { beforeSignature: before }),
        oldest,
        complete,
        nextCandidate: nextCandidate + 1,
      });
      if (saved?.kind === 'failure') return saved;
    }
    return failure(new Error('Original creation event unavailable in bounded mint history'));
  }

  decodeConfirmedTransaction(
    signature: string,
    expectedSlot: number,
    transaction: Record<string, unknown>
  ): Result<readonly ReconciledMarketOccurrence[], Error> {
    if (signature.trim() === '' || !isSlot(expectedSlot) || transaction.slot !== expectedSlot)
      return failure(new Error('Solana confirmed transaction identity is invalid'));
    const observedAt = new Date(this.clock());
    if (!Number.isFinite(observedAt.getTime())) {
      return failure(new Error('Solana RPC clock returned an invalid observation time'));
    }
    let decoded: Result<readonly DecodedMarketOccurrence[], Error>;
    try {
      decoded = this.decoder.decode({
        signature,
        slot: expectedSlot,
        launchSource: this.launchSource,
        programAddress: this.programAddress,
        observedAt,
        transaction,
      });
    } catch (error) {
      return failure(
        error instanceof Error
          ? new Error(`Solana market decoder failed: ${error.message}`)
          : new Error('Solana market decoder failed')
      );
    }
    if (decoded.kind === 'failure') return decoded;

    const occurrences: ReconciledMarketOccurrence[] = [];
    for (const occurrence of decoded.value) {
      const fact = occurrence.kind === 'launch' ? occurrence.launch : occurrence.graduation;
      const origin = fact.market?.origin;
      if (
        origin === undefined ||
        origin.slot !== expectedSlot ||
        origin.position.transactionSignature !== signature ||
        (this.launchSource === LaunchSource.PUMP_FUN && occurrence.kind !== 'launch') ||
        (this.launchSource === LaunchSource.PUMPSWAP && occurrence.kind !== 'graduation')
      ) {
        return failure(new Error(`Solana market decoder returned a mismatched occurrence`));
      }
      occurrences.push({ ...occurrence, programAddress: this.programAddress, observedAt });
    }
    return success(occurrences);
  }

  private async rpc(method: string, params: readonly unknown[]): Promise<Result<unknown, Error>> {
    for (let attempt = 0; attempt < 3; attempt++) {
      const result = await this.rpcOnce(method, params);
      if (
        result.kind === 'success' ||
        !(result.error instanceof SolanaRpcError) ||
        !result.error.retryable ||
        attempt === 2
      )
        return result;
      this.retryCount++;
      await this.sleep(250 * 2 ** attempt);
    }
    return failure(new Error('RPC retry budget exhausted'));
  }

  private async rpcOnce(
    method: string,
    params: readonly unknown[]
  ): Promise<Result<unknown, Error>> {
    const controller = new AbortController();
    let timeout: ReturnType<typeof setTimeout> | undefined;

    try {
      await this.acquireRateLimit();
      timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
      const id = ++this.requestId;
      const response = await this.fetch(this.endpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id, method, params }),
        signal: controller.signal,
      });
      if (!response.ok) {
        return failure(
          new SolanaRpcError(
            `Solana RPC HTTP request failed with status ${response.status}`,
            response.status === 429 || response.status >= 500
          )
        );
      }

      let body: unknown;
      try {
        body = await response.json();
      } catch {
        return failure(
          new SolanaRpcError(
            controller.signal.aborted
              ? 'Solana RPC response body timed out'
              : 'Solana RPC returned invalid JSON',
            true
          )
        );
      }
      if (!isRecord(body)) {
        return failure(new Error('Solana RPC returned a malformed response'));
      }
      if (isRecord(body.error)) {
        const code = typeof body.error.code === 'number' ? ` ${body.error.code}` : '';
        return failure(
          new SolanaRpcError(
            `Solana RPC error${code}`,
            [-32004, -32005, -32007, -32009, -32603, 429].includes(Number(body.error.code))
          )
        );
      }
      if (!Object.prototype.hasOwnProperty.call(body, 'result')) {
        return failure(new Error('Solana RPC response did not contain a result'));
      }
      return success(body.result);
    } catch (error) {
      if (controller.signal.aborted) {
        return failure(
          new SolanaRpcError(`Solana RPC request timed out after ${this.requestTimeoutMs}ms`, true)
        );
      }
      return failure(new SolanaRpcError('Solana RPC request failed', true));
    } finally {
      if (timeout !== undefined) clearTimeout(timeout);
    }
  }

  private acquireRateLimit(): Promise<void> {
    const acquired = this.rateLimitTail.then(async () => {
      const delay = Math.max(0, this.nextRequestAt - this.clock());
      if (delay > 0) await this.sleep(delay);
      this.nextRequestAt = this.clock() + this.minRequestIntervalMs;
    });
    this.rateLimitTail = acquired.catch(() => undefined);
    return acquired;
  }
}

function validatePageRequest(request: SolanaReconciliationPageRequest): Error | null {
  if (!isSlot(request.fromSlotInclusive) || !isSlot(request.throughFinalizedSlot)) {
    return new Error('Solana reconciliation page has an invalid slot bound');
  }
  if (request.fromSlotInclusive > request.throughFinalizedSlot) {
    return new Error('Solana reconciliation page starts after its finalized bound');
  }
  if (
    !Number.isSafeInteger(request.limit) ||
    request.limit <= 0 ||
    request.limit > MAX_SIGNATURE_PAGE_SIZE
  ) {
    return new Error(
      `Solana reconciliation page limit must be between 1 and ${MAX_SIGNATURE_PAGE_SIZE}`
    );
  }
  if (request.beforeSignature !== undefined && request.beforeSignature.trim() === '') {
    return new Error('Solana reconciliation before-signature cannot be blank');
  }
  return null;
}

function parseSignatures(value: unknown): Result<SignatureInfo[], Error> {
  if (!Array.isArray(value)) {
    return failure(new Error('Solana RPC returned a malformed signature page'));
  }
  const parsed: SignatureInfo[] = [];
  for (const item of value) {
    if (
      !isRecord(item) ||
      typeof item.signature !== 'string' ||
      item.signature.trim() === '' ||
      !isSlot(item.slot) ||
      !Object.prototype.hasOwnProperty.call(item, 'err')
    ) {
      return failure(new Error('Solana RPC returned a malformed signature entry'));
    }
    parsed.push({ signature: item.signature, slot: item.slot, err: item.err });
  }
  return success(parsed);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isSlot(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function validEndpoint(value: string): string {
  const endpoint = required(value, 'Solana RPC endpoint');
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch {
    throw new Error('Solana RPC endpoint must be an absolute URL');
  }
  if (
    (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
    parsed.username !== '' ||
    parsed.password !== ''
  ) {
    throw new Error('Solana RPC endpoint must be an HTTP(S) URL without embedded credentials');
  }
  return endpoint;
}

function required(value: string, label: string): string {
  const trimmed = value.trim();
  if (trimmed === '') throw new Error(`${label} is required`);
  return trimmed;
}

function positiveInteger(value: number | undefined, fallback: number, label: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new Error(`Solana RPC ${label} must be a positive integer`);
  }
  return resolved;
}

function nonnegativeInteger(value: number | undefined, fallback: number, label: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved < 0) {
    throw new Error(`Solana RPC ${label} must be a nonnegative integer`);
  }
  return resolved;
}
