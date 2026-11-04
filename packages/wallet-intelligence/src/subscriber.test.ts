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

  onOpen(listener: () => void): void {
    this.openListener = listener;
  }
  onMessage(listener: (data: string) => void): void {
    void listener;
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
  closed(): void {
    this.closeListener();
  }
}
