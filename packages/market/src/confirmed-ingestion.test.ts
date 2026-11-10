import { describe, expect, it } from 'vitest';
import { Chain, LaunchSource, Result, success } from '@nit/domain';
import {
  ConfirmedIngestionSummary,
  SolanaConfirmedLogSubscriber,
  SolanaConfirmedOccurrenceIngestor,
  SolanaWebSocketConnection,
  parseConfirmedLogNotification,
} from './confirmed-ingestion';
import {
  ReconciledMarketOccurrence,
  ReconciliationDiscovery,
  SolanaMarketOccurrenceRepository,
} from './reconciliation';
import { SolanaConfirmedTransactionSource } from './solana-rpc';

const OCCURRENCE: ReconciledMarketOccurrence = {
  kind: 'launch',
  programAddress: 'Program11111111111111111111111111111111111',
  observedAt: new Date('2026-11-01T00:00:01.000Z'),
  launch: {
    address: 'Mint111111111111111111111111111111111111111',
    ticker: 'MINT',
    name: 'Mint Token',
    launchTime: new Date('2026-11-01T00:00:00.000Z'),
    raw: { event: 'CreateEvent' },
    market: {
      marketAddress: 'Curve11111111111111111111111111111111111111',
      origin: {
        kind: 'chain',
        slot: 42,
        position: {
          transactionSignature: 'signature-1',
          instructionIndex: 3,
          innerInstructionIndex: 1,
        },
      },
    },
  },
};

class StubSource implements SolanaConfirmedTransactionSource {
  readonly chain = Chain.SOLANA;
  readonly launchSource = LaunchSource.PUMP_FUN;
  readonly providerName = 'stub-rpc';
  readonly isToyImplementation = false;
  readonly interfaceVersion = '1';
  readonly calls: Array<{ signature: string; slot: number }> = [];

  fetchConfirmedTransaction(
    signature: string,
    slot: number
  ): Promise<Result<readonly ReconciledMarketOccurrence[], Error>> {
    this.calls.push({ signature, slot });
    return Promise.resolve(success([OCCURRENCE]));
  }
}

class RecordingOccurrences implements SolanaMarketOccurrenceRepository {
  readonly values: ReconciledMarketOccurrence[] = [];

  record(
    _chain: Chain.SOLANA,
    _launchSource: LaunchSource,
    occurrence: ReconciledMarketOccurrence
  ): Promise<Result<boolean, Error>> {
    this.values.push(occurrence);
    return Promise.resolve(success(true));
  }
}

class RecordingDiscovery implements ReconciliationDiscovery {
  launches = 0;

  discoverBatch(
    _origin: unknown,
    launches: readonly unknown[]
  ): Promise<{
    discovered: never[];
    graduated: never[];
    duplicates: never[];
    failures: never[];
  }> {
    this.launches += launches.length;
    return Promise.resolve({ discovered: [], graduated: [], duplicates: [], failures: [] });
  }

  graduateBatch(): Promise<{
    discovered: never[];
    graduated: never[];
    duplicates: never[];
    failures: never[];
  }> {
    return Promise.resolve({ discovered: [], graduated: [], duplicates: [], failures: [] });
  }
}

describe('SolanaConfirmedOccurrenceIngestor', () => {
  it('routes a confirmed transaction through raw persistence before discovery', async () => {
    const source = new StubSource();
    const occurrences = new RecordingOccurrences();
    const discovery = new RecordingDiscovery();
    const result = await new SolanaConfirmedOccurrenceIngestor(
      source,
      occurrences,
      discovery
    ).ingest('signature-1', 42);

    expect(result).toEqual(
      success({
        signature: 'signature-1',
        slot: 42,
        fetched: 1,
        discovered: 0,
        graduated: 0,
        duplicates: 0,
      })
    );
    expect(source.calls).toEqual([{ signature: 'signature-1', slot: 42 }]);
    expect(occurrences.values).toEqual([OCCURRENCE]);
    expect(discovery.launches).toBe(1);
  });
});

describe('confirmed Solana log subscription', () => {
  it('bounds queued notifications and closes the overloaded connection for reconciliation', async () => {
    const connection = new FakeConnection();
    let release: () => void = () => undefined;
    const barrier = new Promise<void>((resolve) => {
      release = resolve;
    });
    const calls: string[] = [];
    const subscriber = new SolanaConfirmedLogSubscriber({
      endpoint: 'wss://rpc.example.test',
      programAddress: OCCURRENCE.programAddress,
      maxPendingNotifications: 2,
      connectionFactory: (): SolanaWebSocketConnection => connection,
      ingestor: {
        ingest: async (signature, slot): Promise<Result<ConfirmedIngestionSummary, Error>> => {
          calls.push(signature);
          await barrier;
          return success({
            signature,
            slot,
            fetched: 0,
            discovered: 0,
            graduated: 0,
            duplicates: 0,
          });
        },
      },
    });
    subscriber.start();
    connection.open();
    for (let i = 0; i < 3; i++)
      connection.message(JSON.stringify(notification('sig-' + i, 42 + i, null)));
    expect(subscriber.getStats()).toMatchObject({ pending: 2, overflows: 1, errors: 1 });
    release();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(calls).toEqual(['sig-0', 'sig-1']);
    expect(subscriber.getStats().pending).toBe(0);
    subscriber.stop();
  });
  it('recovers the processing tail after a thrown ingestor and reports rejected subscriptions', async () => {
    const connection = new FakeConnection();
    const calls: string[] = [];
    const subscriber = new SolanaConfirmedLogSubscriber({
      endpoint: 'wss://rpc.example.test',
      programAddress: OCCURRENCE.programAddress,
      connectionFactory: (): SolanaWebSocketConnection => connection,
      ingestor: {
        ingest: (signature, slot): Promise<Result<ConfirmedIngestionSummary, Error>> => {
          calls.push(signature);
          if (signature === 'bad') throw new Error('Synthetic handler failure');
          return Promise.resolve(
            success({
              signature,
              slot,
              fetched: 0,
              discovered: 0,
              graduated: 0,
              duplicates: 0,
            })
          );
        },
      },
    });
    subscriber.start();
    connection.open();
    connection.message(JSON.stringify(notification('bad', 42, null)));
    connection.message(JSON.stringify(notification('good', 43, null)));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(calls).toEqual(['bad', 'good']);
    expect(subscriber.getStats().pending).toBe(0);
    expect(subscriber.getStats().errors).toBe(1);
    subscriber.stop();
    const rejected = new FakeConnection();
    const other = new SolanaConfirmedLogSubscriber({
      endpoint: 'wss://rpc.example.test',
      programAddress: OCCURRENCE.programAddress,
      connectionFactory: (): SolanaWebSocketConnection => rejected,
      ingestor: {
        ingest: (): Promise<Result<ConfirmedIngestionSummary, Error>> =>
          Promise.reject(new Error('Should not ingest a subscription error')),
      },
    });
    other.start();
    rejected.open();
    rejected.message(
      JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        error: { code: 429, message: 'Untrusted provider response' },
      })
    );
    expect(other.getStats().errors).toBe(1);
    other.stop();
  });
  it('parses only successful log notifications', () => {
    const valid = notification('signature-1', 42, null);
    const failed = notification('signature-2', 43, { InstructionError: [0, 'Custom'] });

    expect(parseConfirmedLogNotification(valid)).toEqual(
      success({ signature: 'signature-1', slot: 42 })
    );
    expect(parseConfirmedLogNotification(failed)).toEqual(success(null));
    expect(parseConfirmedLogNotification({ method: 'other' })).toEqual(success(null));
  });

  it('subscribes at confirmed commitment and serially ingests notifications', async () => {
    const connection = new FakeConnection();
    let resolveIngested: (value: ConfirmedIngestionSummary) => void = () => undefined;
    const ingested = new Promise<ConfirmedIngestionSummary>((resolve) => {
      resolveIngested = resolve;
    });
    const subscriber = new SolanaConfirmedLogSubscriber({
      endpoint: 'wss://rpc.example.test',
      programAddress: 'Program11111111111111111111111111111111111',
      connectionFactory: (): SolanaWebSocketConnection => connection,
      ingestor: {
        ingest: (signature, slot): Promise<Result<ConfirmedIngestionSummary, Error>> =>
          Promise.resolve(
            success({ signature, slot, fetched: 1, discovered: 1, graduated: 0, duplicates: 0 })
          ),
      },
      onIngested: resolveIngested,
    });

    subscriber.start();
    connection.open();
    connection.message(JSON.stringify(notification('signature-1', 42, null)));

    await expect(ingested).resolves.toMatchObject({ signature: 'signature-1', slot: 42 });
    expect(JSON.parse(connection.sent[0] as string)).toEqual({
      jsonrpc: '2.0',
      id: 1,
      method: 'logsSubscribe',
      params: [
        { mentions: ['Program11111111111111111111111111111111111'] },
        { commitment: 'confirmed' },
      ],
    });
    subscriber.stop();
  });
});

class FakeConnection implements SolanaWebSocketConnection {
  readonly sent: string[] = [];
  private openListener: () => void = () => undefined;
  private messageListener: (data: string) => void = () => undefined;
  private errorListener: (error: Error) => void = () => undefined;
  private closeListener: () => void = () => undefined;

  onOpen(listener: () => void): void {
    this.openListener = listener;
  }
  onMessage(listener: (data: string) => void): void {
    this.messageListener = listener;
  }
  onError(listener: (error: Error) => void): void {
    this.errorListener = listener;
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
  error(error: Error): void {
    this.errorListener(error);
  }
}

function notification(signature: string, slot: number, err: unknown): unknown {
  return {
    jsonrpc: '2.0',
    method: 'logsNotification',
    params: {
      result: {
        context: { slot },
        value: { signature, err, logs: [] },
      },
      subscription: 1,
    },
  };
}
