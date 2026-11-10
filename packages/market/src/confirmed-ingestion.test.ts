import { describe, expect, it } from 'vitest';
import { Chain, LaunchSource, Result, failure, success } from '@nit/domain';
import {
  ConfirmedIngestionSummary,
  PreparedConfirmedIngestion,
  SolanaConfirmedProgramIngestor,
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
  it('does not apply any decoder output when another decoder fails', async () => {
    let writes = 0;
    const source = {
      readConfirmedTransaction: (): Promise<Result<Record<string, unknown>, Error>> =>
        Promise.resolve(success({ slot: 42 })),
      decodeConfirmedTransaction: (): Result<readonly ReconciledMarketOccurrence[], Error> =>
        success([OCCURRENCE]),
    };
    const ingestor = {
      apply: (): Promise<Result<ConfirmedIngestionSummary, Error>> => {
        writes++;
        return Promise.reject(new Error('Must not write'));
      },
    };
    const program = new SolanaConfirmedProgramIngestor([
      { source, ingestor },
      {
        source: {
          ...source,
          decodeConfirmedTransaction: (): Result<readonly ReconciledMarketOccurrence[], Error> =>
            failure(new Error('Malformed migration')),
        },
        ingestor,
      },
    ]);
    expect((await program.ingest('signature-1', 42)).kind).toBe('failure');
    expect(writes).toBe(0);
  });
  it('shares one transaction read and finishes all decoding before any writes', async () => {
    let reads = 0;
    const writes: number[] = [];
    const source = {
      readConfirmedTransaction: (): Promise<Result<Record<string, unknown>, Error>> => {
        reads++;
        return Promise.resolve(success({ slot: 42 }));
      },
      decodeConfirmedTransaction: (): Result<readonly ReconciledMarketOccurrence[], Error> =>
        success([OCCURRENCE]),
    };
    const targets = [0, 1].map((index) => ({
      source,
      ingestor: {
        apply: (
          signature: string,
          slot: number,
          occurrences: readonly ReconciledMarketOccurrence[]
        ): Promise<Result<ConfirmedIngestionSummary, Error>> => {
          writes.push(index);
          return Promise.resolve(
            success({
              signature,
              slot,
              fetched: occurrences.length,
              discovered: 1,
              graduated: 0,
              duplicates: 0,
            })
          );
        },
      },
    }));
    const program = new SolanaConfirmedProgramIngestor(targets);
    const prepared = await program.prepare('signature-1', 42);
    expect(reads).toBe(1);
    expect(writes).toEqual([]);
    if (prepared.kind === 'failure') throw prepared.error;
    expect(await prepared.value()).toEqual(
      success({
        signature: 'signature-1',
        slot: 42,
        fetched: 2,
        discovered: 2,
        graduated: 0,
        duplicates: 0,
      })
    );
    expect(writes).toEqual([0, 1]);
  });
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
  it('bounds independent mint commits, keeps same-mint order and lets empty decodes drain', async () => {
    const connection = new FakeConnection();
    const started: string[] = [];
    const completed: string[] = [];
    const releases = new Map<string, () => void>();
    let active = 0,
      peak = 0;
    const subscriber = new SolanaConfirmedLogSubscriber({
      endpoint: 'wss://rpc.example.test',
      programAddress: OCCURRENCE.programAddress,
      maxConcurrentCommits: 2,
      connectionFactory: (): SolanaWebSocketConnection => connection,
      ingestor: {
        ingest: (): Promise<Result<ConfirmedIngestionSummary, Error>> =>
          Promise.reject(new Error('Must prepare')),
        prepare: (signature, slot): Promise<Result<PreparedConfirmedIngestion, Error>> => {
          const apply: PreparedConfirmedIngestion = async () => {
            started.push(signature);
            if (signature !== 'empty') {
              peak = Math.max(peak, ++active);
              await new Promise<void>((resolve) => releases.set(signature, resolve));
              active--;
            }
            completed.push(signature);
            return success({
              signature,
              slot,
              fetched: 0,
              discovered: 0,
              graduated: 0,
              duplicates: 0,
            });
          };
          apply.keys = signature === 'empty' ? [] : [signature === 'laterA' ? 'A' : signature];
          return Promise.resolve(success(apply));
        },
      },
    });
    const flush = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
    subscriber.start();
    connection.open();
    for (const signature of ['A', 'B', 'C', 'laterA', 'empty'])
      connection.message(JSON.stringify(notification(signature, 42, null)));
    await flush();
    expect(started).toEqual(['A', 'B', 'empty']);
    expect(completed).toEqual(['empty']);
    releases.get('A')!();
    await flush();
    expect(started).toEqual(['A', 'B', 'empty', 'C']);
    releases.get('B')!();
    await flush();
    expect(started.at(-1)).toBe('laterA');
    releases.get('C')!();
    releases.get('laterA')!();
    await flush();
    expect(peak).toBe(2);
    expect(completed.indexOf('laterA')).toBeGreaterThan(completed.indexOf('A'));
    expect(subscriber.getStats().pending).toBe(0);
    subscriber.stop();
  });
  it('overlaps bounded HTTP preparation but preserves notification order for writes', async () => {
    const connection = new FakeConnection();
    const releases = new Map<string, (result: Result<PreparedConfirmedIngestion, Error>) => void>();
    const started: string[] = [];
    const applied: string[] = [];
    const subscriber = new SolanaConfirmedLogSubscriber({
      endpoint: 'wss://rpc.example.test',
      programAddress: OCCURRENCE.programAddress,
      maxConcurrentPreparations: 2,
      connectionFactory: (): SolanaWebSocketConnection => connection,
      ingestor: {
        ingest: (): Promise<Result<ConfirmedIngestionSummary, Error>> => {
          return Promise.reject(new Error('Preparation should be used'));
        },
        prepare: (signature): Promise<Result<PreparedConfirmedIngestion, Error>> =>
          new Promise((resolve) => {
            started.push(signature);
            releases.set(signature, resolve);
          }),
      },
    });
    const flush = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
    const complete = (signature: string): void =>
      releases.get(signature)!(
        success(() => {
          applied.push(signature);
          return Promise.resolve(
            success({
              signature,
              slot: 42,
              fetched: 0,
              discovered: 0,
              graduated: 0,
              duplicates: 0,
            })
          );
        })
      );
    subscriber.start();
    connection.open();
    for (const signature of ['first', 'second', 'third'])
      connection.message(JSON.stringify(notification(signature, 42, null)));
    await flush();
    expect(started).toEqual(['first', 'second']);
    complete('second');
    await flush();
    expect(started).toEqual(['first', 'second', 'third']);
    expect(applied).toEqual([]);
    complete('third');
    complete('first');
    await flush();
    expect(applied).toEqual(['first', 'second', 'third']);
    expect(subscriber.getStats().pending).toBe(0);
    subscriber.stop();
  });
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
