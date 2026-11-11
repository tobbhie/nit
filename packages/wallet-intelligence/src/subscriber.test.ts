import { Result, success } from '@nit/domain';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SolanaWalletTransaction } from './contracts';
import { WalletProcessingSummary } from './service';
import {
  SolanaWalletSubscriber,
  WalletWebSocketConnection,
  parseWalletLogNotification,
} from './subscriber';

afterEach((): void => {
  vi.useRealTimers();
});

describe('parseWalletLogNotification', () => {
  function harness(options: { maxPending?: number; throwFirst?: boolean } = {}): {
    subscriber: SolanaWalletSubscriber;
    connection: FakeConnection;
    calls: string[];
    release: () => void;
  } {
    const connection = new FakeConnection(),
      calls: string[] = [];
    let release = (): void => undefined;
    const barrier =
      options.maxPending === undefined
        ? Promise.resolve()
        : new Promise<void>((resolve) => {
            release = resolve;
          });
    const subscriber = new SolanaWalletSubscriber({
      endpoint: 'wss://rpc.example.invalid',
      walletAddresses: ['11111111111111111111111111111111', '22222222222222222222222222222222'],
      ...(options.maxPending === undefined ? {} : { maxPendingNotifications: options.maxPending }),
      connectionFactory: (): WalletWebSocketConnection => connection,
      rpc: {
        fetchTransaction: async (
          signature,
          slot
        ): Promise<Result<SolanaWalletTransaction, Error>> => {
          calls.push(signature);
          await barrier;
          return success({ signature, slot, observedAt: new Date(), transaction: {} });
        },
      },
      service: {
        process: (tx): Promise<Result<WalletProcessingSummary, Error>> =>
          options.throwFirst && tx.signature === 'bad'
            ? Promise.reject(new Error('Synthetic handler failure'))
            : Promise.resolve(
                success({
                  signature: tx.signature,
                  swaps: 0,
                  fundingEvents: 0,
                  relations: 0,
                  signals: 0,
                  rejectedWallets: 0,
                })
              ),
      },
    });
    subscriber.start();
    connection.open();
    return { subscriber, connection, calls, release };
  }
  const notice = (signature: string): string =>
    JSON.stringify({
      method: 'logsNotification',
      params: { result: { context: { slot: 42 }, value: { signature, err: null } } },
    });
  const flush = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
  it('deduplicates shared-wallet notifications before fetching and after successful processing', async () => {
    const h = harness();
    h.connection.message(notice('shared'));
    h.connection.message(notice('shared'));
    await flush();
    h.connection.message(notice('shared'));
    await flush();
    expect(h.calls).toEqual(['shared']);
    expect(h.subscriber.getStats()).toMatchObject({ pending: 0, duplicates: 2, errors: 0 });
    h.subscriber.stop();
  });
  it('bounds the pending queue and disconnects once for finalized repair', async () => {
    const h = harness({ maxPending: 2 });
    for (const sig of ['one', 'two', 'overflow', 'ignored']) h.connection.message(notice(sig));
    expect(h.subscriber.getStats()).toMatchObject({ pending: 2, overflows: 1, errors: 1 });
    h.release();
    await flush();
    expect(h.calls).toEqual(['one', 'two']);
    expect(h.subscriber.getStats().pending).toBe(0);
    h.subscriber.stop();
  });
  it('recovers the processing tail after a thrown handler', async () => {
    const h = harness({ throwFirst: true });
    h.connection.message(notice('bad'));
    h.connection.message(notice('good'));
    await flush();
    expect(h.calls).toEqual(['bad', 'good']);
    expect(h.subscriber.getStats()).toMatchObject({ pending: 0, errors: 1 });
    h.subscriber.stop();
  });
  it('tracks subscription acknowledgements and rejects failed subscriptions', () => {
    const h = harness();
    h.connection.message(JSON.stringify({ id: 1, result: 101 }));
    expect(h.subscriber.getStats().acknowledged).toBe(1);
    h.connection.message(
      JSON.stringify({ id: 2, error: { code: 429, message: 'Untrusted provider message' } })
    );
    expect(h.subscriber.getStats()).toMatchObject({ acknowledged: 0, errors: 1 });
    expect(h.calls).toEqual([]);
    h.subscriber.stop();
  });
  it('accepts successful confirmed log notifications', () => {
    expect(
      parseWalletLogNotification({
        jsonrpc: '2.0',
        method: 'logsNotification',
        params: {
          result: {
            context: { slot: 52 },
            value: { signature: 'sig-1', err: null, logs: [] },
          },
        },
      })
    ).toEqual({ kind: 'success', value: { signature: 'sig-1', slot: 52 } });
  });

  it('ignores failed transactions and rejects malformed notification identity', () => {
    expect(
      parseWalletLogNotification({
        method: 'logsNotification',
        params: {
          result: { context: { slot: 52 }, value: { signature: 'sig-1', err: { code: 1 } } },
        },
      })
    ).toEqual({ kind: 'success', value: null });
    expect(
      parseWalletLogNotification({ method: 'logsNotification', params: { result: {} } }).kind
    ).toBe('failure');
  });

  it('shares one socket across wallet subscriptions and reconnects after closure', () => {
    vi.useFakeTimers();
    const connections: FakeConnection[] = [];
    const subscriber = new SolanaWalletSubscriber({
      endpoint: 'wss://rpc.example.invalid',
      walletAddresses: ['11111111111111111111111111111111', '22222222222222222222222222222222'],
      rpc: {
        fetchTransaction: (): Promise<Result<SolanaWalletTransaction, Error>> =>
          Promise.resolve(
            success({
              signature: 'unused',
              slot: 0,
              observedAt: new Date('2026-01-01T00:00:00Z'),
              transaction: {},
            })
          ),
      },
      service: {
        process: (): Promise<Result<WalletProcessingSummary, Error>> =>
          Promise.resolve(
            success({
              signature: 'unused',
              swaps: 0,
              fundingEvents: 0,
              relations: 0,
              signals: 0,
              rejectedWallets: 0,
            })
          ),
      },
      connectionFactory: (): WalletWebSocketConnection => {
        const connection = new FakeConnection();
        connections.push(connection);
        return connection;
      },
    });

    subscriber.start();
    expect(connections).toHaveLength(1);
    connections[0]!.open();
    expect(connections[0]!.sent.map((item): unknown => JSON.parse(item) as unknown)).toMatchObject([
      { id: 1, method: 'logsSubscribe' },
      { id: 2, method: 'logsSubscribe' },
    ]);

    connections[0]!.closed();
    vi.advanceTimersByTime(1_000);
    expect(connections).toHaveLength(2);
    subscriber.stop();
  });
});

class FakeConnection implements WalletWebSocketConnection {
  readonly sent: string[] = [];
  private openListener: () => void = (): void => undefined;
  private closeListener: () => void = (): void => undefined;
  private messageListener = (_data: string): void => undefined;

  onOpen(listener: () => void): void {
    this.openListener = listener;
  }
  onMessage(listener: (data: string) => void): void {
    this.messageListener = listener;
  }
  onError(listener: (error: Error) => void): void {
    void listener;
  }
  onClose(listener: () => void): void {
    this.closeListener = listener;
  }
  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {
    this.closeListener();
  }
  open(): void {
    this.openListener();
  }
  message(data: string): void {
    this.messageListener(data);
  }
  closed(): void {
    this.closeListener();
  }
}
