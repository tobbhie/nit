import { RawData, WebSocket as WebSocketClient } from 'ws';
import { Result, failure, success } from '@nit/domain';
import { WalletIntelligenceService, WalletProcessingSummary } from './service';
import { SolanaWalletRpc } from './solana-rpc';

export interface WalletLogNotification {
  readonly signature: string;
  readonly slot: number;
}

export function parseWalletLogNotification(
  value: unknown
): Result<WalletLogNotification | null, Error> {
  const root = record(value);
  if (root === null) return failure(new Error('Solana WebSocket message is not an object'));
  if (root['method'] !== 'logsNotification') return success(null);
  const params = record(root['params']);
  const result = record(params?.['result']);
  const context = record(result?.['context']);
  const notification = record(result?.['value']);
  if (
    !isSlot(context?.['slot']) ||
    typeof notification?.['signature'] !== 'string' ||
    !Object.prototype.hasOwnProperty.call(notification, 'err')
  )
    return failure(new Error('Solana wallet log notification is malformed'));
  return notification['err'] === null
    ? success({ signature: notification['signature'], slot: context['slot'] })
    : success(null);
}

export interface WalletWebSocketConnection {
  onOpen(listener: () => void): void;
  onMessage(listener: (data: string) => void): void;
  onError(listener: (error: Error) => void): void;
  onClose(listener: () => void): void;
  send(data: string): void;
  close(): void;
}

export type WalletWebSocketFactory = (endpoint: string) => WalletWebSocketConnection;

export interface SolanaWalletSubscriberOptions {
  endpoint: string;
  walletAddresses: readonly string[];
  rpc: Pick<SolanaWalletRpc, 'fetchTransaction'>;
  service: Pick<WalletIntelligenceService, 'process'>;
  connectionFactory?: WalletWebSocketFactory;
  reconnectBaseMs?: number;
  reconnectMaxMs?: number;
  onProcessed?: (summary: WalletProcessingSummary) => void;
  onError?: (error: Error) => void;
  maxPendingNotifications?: number;
}

/** One physical socket with one `mentions` subscription per watched wallet. */
export class SolanaWalletSubscriber {
  private readonly endpoint: string;
  private readonly walletAddresses: readonly string[];
  private readonly rpc: Pick<SolanaWalletRpc, 'fetchTransaction'>;
  private readonly service: Pick<WalletIntelligenceService, 'process'>;
  private readonly connectionFactory: WalletWebSocketFactory;
  private readonly reconnectBaseMs: number;
  private readonly reconnectMaxMs: number;
  private readonly onProcessed: ((summary: WalletProcessingSummary) => void) | undefined;
  private readonly onError: ((error: Error) => void) | undefined;
  private connection: WalletWebSocketConnection | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private stopped = true;
  private reconnectAttempt = 0;
  private tail: Promise<void> = Promise.resolve();
  private pending = 0;
  private duplicates = 0;
  private errors = 0;
  private overflows = 0;
  private readonly inFlight = new Set<string>();
  private readonly recent = new Set<string>();
  private readonly acknowledged = new Set<number>();
  private readonly maxPending: number;

  constructor(options: SolanaWalletSubscriberOptions) {
    this.endpoint = websocketEndpoint(options.endpoint);
    this.walletAddresses = [...new Set(options.walletAddresses)];
    if (this.walletAddresses.length === 0) throw new Error('Wallet subscriber requires addresses');
    this.rpc = options.rpc;
    this.service = options.service;
    this.connectionFactory = options.connectionFactory ?? defaultConnectionFactory;
    this.reconnectBaseMs = options.reconnectBaseMs ?? 1_000;
    this.reconnectMaxMs = options.reconnectMaxMs ?? 30_000;
    this.onProcessed = options.onProcessed;
    this.onError = options.onError;
    this.maxPending = options.maxPendingNotifications ?? 128;
    if (!Number.isSafeInteger(this.maxPending) || this.maxPending < 1 || this.maxPending > 10000)
      throw new Error('Invalid wallet notification backlog limit');
  }

  getStats(): {
    pending: number;
    duplicates: number;
    errors: number;
    overflows: number;
    acknowledged: number;
    expected: number;
  } {
    return {
      pending: this.pending,
      duplicates: this.duplicates,
      errors: this.errors,
      overflows: this.overflows,
      acknowledged: this.acknowledged.size,
      expected: this.walletAddresses.length,
    };
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer !== undefined) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    this.connection?.close();
    this.connection = undefined;
  }

  private connect(): void {
    if (this.stopped) return;
    let connection: WalletWebSocketConnection;
    try {
      connection = this.connectionFactory(this.endpoint);
    } catch (error) {
      this.report(error instanceof Error ? error : new Error('Wallet WebSocket creation failed'));
      this.scheduleReconnect();
      return;
    }
    this.connection = connection;
    connection.onOpen(() => {
      if (this.connection !== connection || this.stopped) return;
      this.acknowledged.clear();
      this.walletAddresses.forEach((wallet, index) =>
        connection.send(
          JSON.stringify({
            jsonrpc: '2.0',
            id: index + 1,
            method: 'logsSubscribe',
            params: [{ mentions: [wallet] }, { commitment: 'confirmed' }],
          })
        )
      );
    });
    connection.onMessage((data) => {
      if (this.connection === connection && !this.stopped) this.handleMessage(data, connection);
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

  private handleMessage(data: string, connection: WalletWebSocketConnection): void {
    let decoded: unknown;
    try {
      decoded = JSON.parse(data) as unknown;
    } catch {
      this.report(new Error('Solana wallet WebSocket returned invalid JSON'));
      this.disconnect(connection);
      return;
    }
    const response = record(decoded);
    const requestId = response?.['id'];
    if (
      typeof requestId === 'number' &&
      Number.isInteger(requestId) &&
      requestId >= 1 &&
      requestId <= this.walletAddresses.length
    ) {
      if (typeof response?.['result'] !== 'number' || response['error'] !== undefined) {
        this.report(new Error('Wallet log subscription rejected'));
        this.disconnect(connection);
      } else this.acknowledged.add(requestId);
      return;
    }
    const notification = parseWalletLogNotification(decoded);
    if (notification.kind === 'failure') {
      this.report(notification.error);
      this.disconnect(connection);
      return;
    }
    if (notification.value === null) return;
    const item = notification.value;
    if (this.inFlight.has(item.signature) || this.recent.has(item.signature)) {
      this.duplicates++;
      return;
    }
    if (this.pending >= this.maxPending) {
      this.overflows++;
      this.report(new Error('Wallet notification backlog full; finalized reconciliation required'));
      this.disconnect(connection);
      return;
    }
    this.pending++;
    this.inFlight.add(item.signature);
    this.tail = this.tail
      .then(async () => {
        const transaction = await this.rpc.fetchTransaction(item.signature, item.slot, 'confirmed');
        if (transaction.kind === 'failure') {
          this.report(transaction.error);
          this.disconnect(connection);
          return;
        }
        const processed = await this.service.process(transaction.value);
        if (processed.kind === 'failure') {
          this.report(processed.error);
          this.disconnect(connection);
          return;
        }
        this.onProcessed?.(processed.value);
        this.recent.add(item.signature);
        if (this.recent.size > 1024) this.recent.delete(this.recent.values().next().value!);
        if (this.connection === connection) this.reconnectAttempt = 0;
      })
      .catch(() => {
        this.report(
          new Error('Wallet ingestion handler failed; finalized reconciliation required')
        );
        this.disconnect(connection);
      })
      .finally(() => {
        this.pending--;
        this.inFlight.delete(item.signature);
      });
  }

  private disconnect(connection: WalletWebSocketConnection): void {
    if (this.connection !== connection) return;
    this.connection = undefined;
    this.acknowledged.clear();
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
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delay);
  }

  private report(error: Error): void {
    this.errors++;
    this.onError?.(error);
  }
}

function defaultConnectionFactory(endpoint: string): WalletWebSocketConnection {
  const socket = new WebSocketClient(endpoint, { maxPayload: 1048576, handshakeTimeout: 15000 });
  return {
    onOpen: (listener) => socket.on('open', listener),
    onMessage: (listener) => socket.on('message', (data: RawData) => listener(rawDataText(data))),
    onError: (listener) => socket.on('error', listener),
    onClose: (listener) => socket.on('close', listener),
    send: (data) => socket.send(data),
    close: () => socket.close(),
  };
}

function rawDataText(data: RawData): string {
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8');
  if (Buffer.isBuffer(data)) return data.toString('utf8');
  return Buffer.from(new Uint8Array(data)).toString('utf8');
}

function websocketEndpoint(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error('Solana wallet WebSocket endpoint must be an absolute URL');
  }
  if (
    (parsed.protocol !== 'ws:' && parsed.protocol !== 'wss:') ||
    parsed.username !== '' ||
    parsed.password !== ''
  )
    throw new Error('Solana wallet WebSocket endpoint must be WS(S) without embedded credentials');
  return value;
}

function isSlot(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}
