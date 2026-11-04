import { describe, expect, it } from 'vitest';
import { Chain, LaunchSource, Result, failure, success } from '@nit/domain';
import { SourceTokenGraduation, SourceTokenLaunch } from './contracts';
import {
  ReconciledMarketOccurrence,
  ReconciliationDiscovery,
  SolanaLaunchReconciler,
  SolanaReconciliationCursor,
  SolanaReconciliationCursorRepository,
  SolanaReconciliationPage,
  SolanaReconciliationPageRequest,
  SolanaReconciliationSource,
  SolanaMarketOccurrenceRepository,
} from './reconciliation';

class MemoryCursor implements SolanaReconciliationCursorRepository {
  value: SolanaReconciliationCursor | null;
  advances: number[] = [];

  constructor(slot?: number) {
    this.value =
      slot === undefined
        ? null
        : {
            chain: Chain.SOLANA,
            launchSource: LaunchSource.PUMP_FUN,
            finalizedSlot: slot,
            updatedAt: new Date(0),
          };
  }

  get(): Promise<Result<SolanaReconciliationCursor | null, Error>> {
    return Promise.resolve(success(this.value));
  }

  advance(
    chain: Chain.SOLANA,
    launchSource: LaunchSource,
    finalizedSlot: number
  ): Promise<Result<SolanaReconciliationCursor, Error>> {
    this.advances.push(finalizedSlot);
    this.value = { chain, launchSource, finalizedSlot, updatedAt: new Date(1) };
    return Promise.resolve(success(this.value));
  }
}

class ScriptedSource implements SolanaReconciliationSource {
  readonly chain = Chain.SOLANA;
  readonly launchSource = LaunchSource.PUMP_FUN;
  readonly providerName = 'scripted-solana-rpc';
  readonly isToyImplementation = true;
  readonly interfaceVersion = '1';
  readonly requests: SolanaReconciliationPageRequest[] = [];

  constructor(
    private readonly head: Result<number, Error>,
    private readonly pages: Array<Result<SolanaReconciliationPage, Error>>
  ) {}

  getFinalizedSlot(): Promise<Result<number, Error>> {
    return Promise.resolve(this.head);
  }

  fetchReconciliationPage(
    request: SolanaReconciliationPageRequest
  ): Promise<Result<SolanaReconciliationPage, Error>> {
    this.requests.push(request);
    return Promise.resolve(
      this.pages.shift() ?? failure(new Error('No scripted reconciliation page'))
    );
  }
}

class RecordingDiscovery implements ReconciliationDiscovery {
  calls: readonly SourceTokenLaunch[][] = [];
  fail = false;

  discoverBatch(): Promise<{
    discovered: never[];
    graduated: never[];
    duplicates: never[];
    failures: Array<{ address: string; error: Error }>;
  }> {
    return Promise.resolve({
      discovered: [],
      graduated: [],
      duplicates: [],
      failures: this.fail ? [{ address: 'Mint1', error: new Error('database unavailable') }] : [],
    });
  }

  graduateBatch(
    _origin: unknown,
    _graduations: readonly SourceTokenGraduation[]
  ): Promise<{
    discovered: never[];
    graduated: never[];
    duplicates: never[];
    failures: never[];
  }> {
    return Promise.resolve({ discovered: [], graduated: [], duplicates: [], failures: [] });
  }
}

class MemoryOccurrences implements SolanaMarketOccurrenceRepository {
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

describe('SolanaLaunchReconciler', () => {
  it('overlaps a stored watermark, pages by signature, then advances to finalized head', async () => {
    const source = new ScriptedSource(success(1_100), [
      success({ occurrences: [], reachedFloor: false, nextBeforeSignature: 'sig-2' }),
      success({ occurrences: [], reachedFloor: true }),
    ]);
    const cursors = new MemoryCursor(1_000);
    const result = await new SolanaLaunchReconciler(
      source,
      cursors,
      new MemoryOccurrences(),
      new RecordingDiscovery(),
      { overlapSlots: 250, pageSize: 100 }
    ).reconcile();

    expect(result.kind).toBe('success');
    expect(source.requests).toEqual([
      { fromSlotInclusive: 750, throughFinalizedSlot: 1_100, limit: 100 },
      {
        fromSlotInclusive: 750,
        throughFinalizedSlot: 1_100,
        beforeSignature: 'sig-2',
        limit: 100,
      },
    ]);
    expect(cursors.advances).toEqual([1_100]);
  });

  it('uses a bounded initial lookback when no cursor exists', async () => {
    const source = new ScriptedSource(success(5_000), [
      success({ occurrences: [], reachedFloor: true }),
    ]);
    await new SolanaLaunchReconciler(
      source,
      new MemoryCursor(),
      new MemoryOccurrences(),
      new RecordingDiscovery(),
      { initialLookbackSlots: 2_000 }
    ).reconcile();

    expect(source.requests[0]?.fromSlotInclusive).toBe(3_000);
  });

  it('does not advance after an ingest failure', async () => {
    const source = new ScriptedSource(success(1_100), [
      success({ occurrences: [], reachedFloor: true }),
    ]);
    const cursors = new MemoryCursor(1_000);
    const discovery = new RecordingDiscovery();
    discovery.fail = true;

    const result = await new SolanaLaunchReconciler(
      source,
      cursors,
      new MemoryOccurrences(),
      discovery
    ).reconcile();
    expect(result.kind).toBe('failure');
    expect(cursors.advances).toEqual([]);
  });

  it('rejects a launch that lacks an exact chain occurrence', async () => {
    const malformed = {
      kind: 'launch',
      programAddress: 'Pump111111111111111111111111111111111111',
      observedAt: new Date('2026-08-20T09:00:01.000Z'),
      launch: {
        address: 'Mint1',
        ticker: 'MINT',
        name: 'Mint',
        launchTime: new Date('2026-08-20T09:00:00.000Z'),
        raw: {},
      },
    } as never;
    const source = new ScriptedSource(success(1_100), [
      success({ occurrences: [malformed], reachedFloor: true }),
    ]);
    const cursors = new MemoryCursor(1_000);
    const result = await new SolanaLaunchReconciler(
      source,
      cursors,
      new MemoryOccurrences(),
      new RecordingDiscovery()
    ).reconcile();

    expect(result.kind).toBe('failure');
    expect(cursors.advances).toEqual([]);
  });

  it('fails closed when signature pagination stalls', async () => {
    const source = new ScriptedSource(success(1_100), [
      success({ occurrences: [], reachedFloor: false }),
    ]);
    const cursors = new MemoryCursor(1_000);
    const result = await new SolanaLaunchReconciler(
      source,
      cursors,
      new MemoryOccurrences(),
      new RecordingDiscovery()
    ).reconcile();

    expect(result.kind).toBe('failure');
    expect(cursors.advances).toEqual([]);
  });

  it('refuses a finalized head behind the durable watermark', async () => {
    const source = new ScriptedSource(success(999), []);
    const cursors = new MemoryCursor(1_000);
    const result = await new SolanaLaunchReconciler(
      source,
      cursors,
      new MemoryOccurrences(),
      new RecordingDiscovery()
    ).reconcile();

    expect(result.kind).toBe('failure');
    expect(source.requests).toEqual([]);
  });
});
