import { RawData, WebSocket as WebSocketClient } from 'ws';
import { Result, failure, success } from '@nit/domain';
import { ReconciliationDiscovery, SolanaMarketOccurrenceRepository } from './reconciliation';
import { SolanaConfirmedTransactionSource } from './solana-rpc';

export interface ConfirmedIngestionSummary {
  signature: string;
  slot: number;
  fetched: number;
  discovered: number;
  graduated: number;
  duplicates: number;
}

/** Persists and applies one confirmed transaction through the reconciliation path. */
export class SolanaConfirmedOccurrenceIngestor {
  constructor(
    private readonly source: SolanaConfirmedTransactionSource,
    private readonly occurrences: SolanaMarketOccurrenceRepository,
    private readonly discovery: ReconciliationDiscovery
  ) {}

  async ingest(signature: string, slot: number): Promise<Result<ConfirmedIngestionSummary, Error>> {
    const fetched = await this.source.fetchConfirmedTransaction(signature, slot);
    if (fetched.kind === 'failure') return fetched;

    for (const occurrence of fetched.value) {
      const recorded = await this.occurrences.record(
        this.source.chain,
        this.source.launchSource,
        occurrence
      );
      if (recorded.kind === 'failure') return recorded;
    }

    const launches = fetched.value.flatMap((occurrence) =>
      occurrence.kind === 'launch' ? [occurrence.launch] : []
    );
    const graduations = fetched.value.flatMap((occurrence) =>
      occurrence.kind === 'graduation' ? [occurrence.graduation] : []
    );
    const origin = { chain: this.source.chain, launchSource: this.source.launchSource };
    const launchOutcome = await this.discovery.discoverBatch(origin, launches);
    const graduationOutcome = await this.discovery.graduateBatch(origin, graduations);
    const failures = [...launchOutcome.failures, ...graduationOutcome.failures];
    if (failures.length > 0) {
      return failure(
        new Error(
          `Confirmed ingestion failed for ${failures.length} occurrence(s): ${failures[0]?.error.message ?? 'unknown error'}`
        )
      );
    }

    return success({
      signature,
      slot,
      fetched: fetched.value.length,
      discovered: launchOutcome.discovered.length,
      graduated: graduationOutcome.graduated.length,
      duplicates: launchOutcome.duplicates.length + graduationOutcome.duplicates.length,
    });
  }
}

export interface ConfirmedLogNotification {
  signature: string;
  slot: number;
}

export function parseConfirmedLogNotification(
  value: unknown
): Result<ConfirmedLogNotification | null, Error> {
  if (!isRecord(value)) return failure(new Error('Solana WebSocket message is not an object'));
  if (value.method !== 'logsNotification') return success(null);
  const params = value.params;
  if (!isRecord(params) || !isRecord(params.result)) {
    return failure(new Error('Solana log notification is malformed'));
  }
  const context = params.result.context;
  const notification = params.result.value;
  if (
    !isRecord(context) ||
    !isSlot(context.slot) ||
    !isRecord(notification) ||
    typeof notification.signature !== 'string' ||
    notification.signature.trim() === '' ||
    !Object.prototype.hasOwnProperty.call(notification, 'err')
  ) {
    return failure(new Error('Solana log notification identity is malformed'));
  }
  if (notification.err !== null) return success(null);
  return success({ signature: notification.signature, slot: context.slot });
}

export interface SolanaWebSocketConnection {
  onOpen(listener: () => void): void;
  onMessage(listener: (data: string) => void): void;
  onError(listener: (error: Error) => void): void;
  onClose(listener: () => void): void;
  send(data: string): void;
  close(): void;
}

export type SolanaWebSocketFactory = (endpoint: string) => SolanaWebSocketConnection;

export interface SolanaConfirmedLogSubscriberOptions {
  endpoint: string;
  programAddress: string;
  ingestor: Pick<SolanaConfirmedOccurrenceIngestor, 'ingest'>;
  connectionFactory?: SolanaWebSocketFactory;
  reconnectBaseMs?: number;
  reconnectMaxMs?: number;
  setTimer?: (callback: () => void, milliseconds: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  onIngested?: (summary: ConfirmedIngestionSummary) => void;
  onError?: (error: Error) => void;
  maxPendingNotifications?: number;
  notificationFilter?: (message: unknown) => boolean;
}

/** A reconnecting confirmed `logsSubscribe` client; reconciliation repairs every gap. */
export class SolanaConfirmedLogSubscriber {
  private readonly endpoint: string;
  private readonly programAddress: string;
  private readonly ingestor: Pick<SolanaConfirmedOccurrenceIngestor, 'ingest'>;
  private readonly connectionFactory: SolanaWebSocketFactory;
  private readonly reconnectBaseMs: number;
  private readonly reconnectMaxMs: number;
  private readonly setTimer: NonNullable<SolanaConfirmedLogSubscriberOptions['setTimer']>;
  private readonly clearTimer: NonNullable<SolanaConfirmedLogSubscriberOptions['clearTimer']>;
  private readonly onIngested: ((summary: ConfirmedIngestionSummary) => void) | undefined;
  private readonly onError: ((error: Error) => void) | undefined;
  private connection: SolanaWebSocketConnection | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private stopped = true;
  private reconnectAttempt = 0;
  private tail: Promise<void> = Promise.resolve();
  private pending = 0;
  private filtered = 0;
  private overflows = 0;
  private errors = 0;
  private readonly maxPending: number;
  private readonly notificationFilter: ((message: unknown) => boolean) | undefined;

  constructor(options: SolanaConfirmedLogSubscriberOptions) {
    this.endpoint = websocketEndpoint(options.endpoint);
    this.programAddress = required(options.programAddress, 'Solana subscription program address');
    this.ingestor = options.ingestor;
    this.connectionFactory = options.connectionFactory ?? defaultConnectionFactory;
    this.reconnectBaseMs = positive(options.reconnectBaseMs, 1_000, 'reconnect base');
    this.reconnectMaxMs = positive(options.reconnectMaxMs, 30_000, 'reconnect maximum');
    if (this.reconnectMaxMs < this.reconnectBaseMs) {
      throw new Error('Solana reconnect maximum must not be below its base');
    }
    this.setTimer = options.setTimer ?? setTimeout;
    this.clearTimer = options.clearTimer ?? clearTimeout;
    this.onIngested = options.onIngested;
    this.onError = options.onError;
    this.maxPending = positive(options.maxPendingNotifications, 128, 'notification backlog');
    if (this.maxPending > 10000) throw new Error('Notification backlog limit exceeds hard cap');
    this.notificationFilter = options.notificationFilter;
  }
  getStats(): { pending: number; filtered: number; overflows: number; errors: number } {
    return {
      pending: this.pending,
      filtered: this.filtered,
      overflows: this.overflows,
      errors: this.errors,
    };
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer !== undefined) {
      this.clearTimer(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.connection?.close();
    this.connection = undefined;
  }

  private connect(): void {
    if (this.stopped) return;
    let connection: SolanaWebSocketConnection;
    try {
      connection = this.connectionFactory(this.endpoint);
    } catch (error) {
      this.report(error instanceof Error ? error : new Error('Solana WebSocket creation failed'));
      this.scheduleReconnect();
      return;
    }
    this.connection = connection;
    connection.onOpen(() => {
      if (this.connection !== connection || this.stopped) return;
      connection.send(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'logsSubscribe',
          params: [{ mentions: [this.programAddress] }, { commitment: 'confirmed' }],
        })
      );
    });
    connection.onMessage((data) => {
      if (this.connection !== connection || this.stopped) return;
      this.handleMessage(data, connection);
    });
    connection.onError((error) => {
      if (this.connection !== connection || this.stopped) return;
      this.report(error);
      this.disconnect(connection);
    });
    connection.onClose(() => {
      if (this.connection !== connection) return;
      this.connection = undefined;
      this.scheduleReconnect();
    });
  }

  private handleMessage(data: string, connection: SolanaWebSocketConnection): void {
    let message: unknown;
    try {
      message = JSON.parse(data) as unknown;
    } catch {
      this.report(new Error('Solana WebSocket returned invalid JSON'));
      this.disconnect(connection);
      return;
    }
    if (isRecord(message) && message.id === 1) {
      if (typeof message.result !== 'number' || message.error !== undefined) {
        this.report(new Error('Solana log subscription was rejected'));
        this.disconnect(connection);
      }
      return;
    }
    const parsed = parseConfirmedLogNotification(message);
    if (parsed.kind === 'failure') {
      this.report(parsed.error);
      this.disconnect(connection);
      return;
    }
    if (parsed.value === null) return;
    if (this.notificationFilter !== undefined) {
      try {
        if (!this.notificationFilter(message)) {
          this.filtered++;
          return;
        }
      } catch {
        this.report(new Error('Notification filtering failed'));
        this.disconnect(connection);
        return;
      }
    }
    if (this.pending >= this.maxPending) {
      this.overflows++;
      this.report(
        new Error('Confirmed notification backlog limit reached; reconciliation required')
      );
      this.disconnect(connection);
      return;
    }
    const notification = parsed.value;
    this.pending++;
    this.tail = this.tail
      .then(async () => {
        const ingested = await this.ingestor.ingest(notification.signature, notification.slot);
        if (ingested.kind === 'failure') {
          this.report(ingested.error);
          if (this.connection === connection) this.disconnect(connection);
          return;
        }
        this.onIngested?.(ingested.value);
        if (this.connection === connection) this.reconnectAttempt = 0;
      })
      .catch(() => {
        this.report(new Error('Confirmed ingestion handler failed; reconciliation required'));
        if (this.connection === connection) this.disconnect(connection);
      })
      .finally(() => {
        this.pending--;
      });
  }

  private disconnect(connection: SolanaWebSocketConnection): void {
    if (this.connection !== connection) return;
    this.connection = undefined;
    connection.close();
    this.scheduleReconnect();
  }
  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer !== undefined) return;
    const delay = Math.min(
      this.reconnectMaxMs,
      this.reconnectBaseMs * 2 ** Math.min(this.reconnectAttempt, 20)
    );
    this.reconnectAttempt += 1;
    this.reconnectTimer = this.setTimer(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delay);
  }

  private report(error: Error): void {
    this.errors++;
    this.onError?.(error);
  }
}

function defaultConnectionFactory(endpoint: string): SolanaWebSocketConnection {
  const socket = new WebSocketClient(endpoint, { maxPayload: 1048576, handshakeTimeout: 15000 });
  return {
    onOpen: (listener): void => {
      socket.on('open', listener);
    },
    onMessage: (listener): void => {
      socket.on('message', (data) => listener(rawDataText(data)));
    },
    onError: (listener): void => {
      socket.on('error', listener);
    },
    onClose: (listener): void => {
      socket.on('close', listener);
    },
    send: (data): void => socket.send(data),
    close: (): void => socket.close(),
  };
}

function rawDataText(data: RawData): string {
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8');
  if (Buffer.isBuffer(data)) return data.toString('utf8');
  return Buffer.from(new Uint8Array(data)).toString('utf8');
}

function websocketEndpoint(value: string): string {
  const endpoint = required(value, 'Solana WebSocket endpoint');
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch {
    throw new Error('Solana WebSocket endpoint must be an absolute URL');
  }
  if (
    (parsed.protocol !== 'ws:' && parsed.protocol !== 'wss:') ||
    parsed.username !== '' ||
    parsed.password !== ''
  ) {
    throw new Error('Solana WebSocket endpoint must be WS(S) without embedded credentials');
  }
  return endpoint;
}

function required(value: string, label: string): string {
  const trimmed = value.trim();
  if (trimmed === '') throw new Error(`${label} is required`);
  return trimmed;
}

function positive(value: number | undefined, fallback: number, label: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new Error(`Solana ${label} must be a positive integer`);
  }
  return resolved;
}

function isSlot(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
