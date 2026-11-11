import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { Chain, LaunchSource, Result, success, failure } from '@nit/domain';
import { Job, JobQueue, Worker } from '@nit/queue';
import {
  SolanaConfirmedOccurrenceIngestor,
  ConfirmedIngestionSummary,
} from './confirmed-ingestion';
import {
  ReconciledMarketOccurrence,
  PostgresSolanaMarketOccurrenceRepository,
} from './reconciliation';
import { SolanaConfirmedTransactionSource } from './solana-rpc';
import {
  MissingTokenCreationRecovery,
  SolanaMarketRecoveryHandler,
  seedStoredMigrationRecovery,
  MarketRecoveryPayload,
  PostgresCreationHistoryProgress,
} from './market-recovery';
import { MarketDiscoveryService } from './service';
import { PostgresUnitOfWork } from './unit-of-work';
import { encodeBase58 } from './pump-events';

const address = (n: number): string => encodeBase58(Buffer.alloc(32, n));
const program = address(1);
describe.skipIf(!hasTestDatabase())('market recovery against Postgres', () => {
  let harness: TestDatabase;
  beforeAll(async () => {
    harness = await createTestDatabase('market_recovery');
  }, 30000);
  afterAll(async () => {
    await harness?.teardown();
  });

  function setup(
    n: number,
    available = true,
    sameSecond = false
  ): {
    graduation: ReconciledMarketOccurrence;
    creation: ReconciledMarketOccurrence;
    created: SolanaConfirmedOccurrenceIngestor;
    graduated: SolanaConfirmedOccurrenceIngestor;
    queue: JobQueue;
    handler: SolanaMarketRecoveryHandler;
    worker: Worker;
    reads: () => number;
  } {
    const mint = address(n),
      pool = address(n + 1),
      curve = address(n + 2);
    const creation: ReconciledMarketOccurrence = {
      kind: 'launch',
      programAddress: program,
      observedAt: new Date('2026-11-01T00:00:01Z'),
      launch: {
        address: mint,
        ticker: 'ORIGINAL',
        name: 'Original on-chain name',
        launchTime: new Date('2026-11-01T00:00:00Z'),
        raw: { mint, name: 'Original on-chain name', uri: 'ipfs://original' },
        market: {
          marketAddress: curve,
          origin: {
            kind: 'chain',
            slot: 40,
            position: {
              transactionSignature: 'creation-' + n,
              instructionIndex: 0,
              innerInstructionIndex: 0,
            },
          },
        },
      },
    };
    const graduation: ReconciledMarketOccurrence = {
      kind: 'graduation',
      programAddress: program,
      observedAt: new Date('2026-11-01T00:01:01Z'),
      graduation: {
        address: mint,
        graduatedAt: new Date(sameSecond ? '2026-11-01T00:00:00Z' : '2026-11-01T00:01:00Z'),
        raw: { mint, pool },
        market: {
          marketAddress: pool,
          origin: {
            kind: 'chain',
            slot: 42,
            position: {
              transactionSignature: 'migration-' + n,
              instructionIndex: 1,
              innerInstructionIndex: 0,
            },
          },
        },
      },
    };
    const queue = new JobQueue(harness.db, {
      leaseTimeoutMs: 120000,
      defaultMaxAttempts: 10,
      backoff: { baseMs: 1000, maxMs: 60000 },
      clock: (): Date => new Date(Date.now() + 10000),
    });
    const discovery = new MarketDiscoveryService(new PostgresUnitOfWork(harness.db, queue), {
      producer: 'recovery-test',
    });
    const occurrences = new PostgresSolanaMarketOccurrenceRepository(harness.db);
    const source = (launchSource: LaunchSource): SolanaConfirmedTransactionSource => ({
      chain: Chain.SOLANA,
      launchSource,
      providerName: 'synthetic-recovery',
      isToyImplementation: true,
      interfaceVersion: '1',
      fetchConfirmedTransaction: (): Promise<
        Result<readonly ReconciledMarketOccurrence[], Error>
      > => Promise.resolve(success([])),
    });
    const created = new SolanaConfirmedOccurrenceIngestor(
      source(LaunchSource.PUMP_FUN),
      occurrences,
      discovery
    );
    const graduated = new SolanaConfirmedOccurrenceIngestor(
      source(LaunchSource.PUMPSWAP),
      occurrences,
      discovery
    );
    let reads = 0;
    const dependency = new MissingTokenCreationRecovery(
      harness.db,
      {
        findCreation: (): Promise<Result<ReconciledMarketOccurrence, Error>> => {
          reads++;
          return Promise.resolve(
            available ? success(creation) : failure(new Error('Original creation unavailable'))
          );
        },
      },
      created
    );
    const handler = new SolanaMarketRecoveryHandler(
      new Map([
        [
          program,
          {
            ingest: async (
              signature: string,
              slot: number
            ): Promise<Result<ConfirmedIngestionSummary, Error>> => {
              const ready = await dependency.ensure([graduation]);
              return ready.kind === 'failure'
                ? ready
                : graduated.apply(signature, slot, [graduation]);
            },
          },
        ],
      ])
    );
    const worker = new Worker(queue, handler, {
      workerId: 'recovery-test-' + n,
      pollIntervalMs: 1,
    });
    return {
      creation,
      created,
      graduation,
      graduated,
      queue,
      handler,
      worker,
      reads: (): number => reads,
    };
  }
  it('recovers stored orphan migrations after restart, preserving original metadata and exact-once events', async () => {
    const system = setup(10);
    expect((await system.graduated.apply('migration-10', 42, [system.graduation])).kind).toBe(
      'failure'
    );
    expect(await seedStoredMigrationRecovery(harness.db, system.queue)).toBe(1);
    expect(await system.worker.processOne()).toEqual(success(true));
    const rows = await harness.db.query<{
      ticker: string;
      name: string;
      raw_payload: Record<string, unknown>;
    }>('SELECT ticker,name,raw_payload FROM tokens WHERE address=$1', [address(10)]);
    expect(rows.kind).toBe('success');
    if (rows.kind === 'success')
      expect(rows.value[0]).toMatchObject({
        ticker: 'ORIGINAL',
        name: 'Original on-chain name',
        raw_payload: { uri: 'ipfs://original' },
      });
    const payload: MarketRecoveryPayload = {
      programAddress: program,
      signature: 'migration-10',
      slot: 42,
    };
    await system.handler.handle({ payload, payloadVersion: '1' } as Job);
    expect(system.reads()).toBe(1);
    expect(await seedStoredMigrationRecovery(harness.db, system.queue)).toBe(0);
    const counts = await harness.db.query<{ event_type: string; count: string }>(
      `SELECT event_type,count(*)::text FROM domain_events WHERE event_type IN ('token.discovered','token.graduated') GROUP BY event_type ORDER BY event_type`
    );
    expect(counts.kind).toBe('success');
    if (counts.kind === 'success')
      expect(counts.value).toEqual([
        { event_type: 'token.discovered', count: '1' },
        { event_type: 'token.graduated', count: '1' },
      ]);
    const markets = await harness.db.query<{ venue: string }>(
      `SELECT venue FROM token_markets ORDER BY active_from`
    );
    expect(markets.kind).toBe('success');
    if (markets.kind === 'success')
      expect(markets.value.map((m) => m.venue)).toEqual(['pump_fun_bonding_curve', 'pumpswap']);
  });
  it('leaves missing original evidence retryable without manufacturing a token', async () => {
    const system = setup(20, false);
    await system.graduated.apply('migration-20', 42, [system.graduation]);
    expect(await seedStoredMigrationRecovery(harness.db, system.queue)).toBe(1);
    expect(await system.worker.processOne()).toEqual(success(true));
    expect(system.worker.getStats().failed).toBe(1);
    const token = await harness.db.query('SELECT token_id FROM tokens WHERE address=$1', [
      address(20),
    ]);
    expect(token.kind === 'success' && token.value.length).toBe(0);
    const jobs = await harness.db.query<{ status: string; attempt_count: number }>(
      `SELECT status,attempt_count FROM durable_jobs WHERE queue_name='token_market_recovery' AND payload->>'signature'='migration-20'`
    );
    expect(jobs.kind).toBe('success');
    if (jobs.kind === 'success')
      expect(jobs.value[0]).toMatchObject({ status: 'pending', attempt_count: 1 });
  });
  it('preserves exact timestamps for same-second chain creation and migration', async () => {
    const system = setup(30, true, true);
    await system.handler.handle({
      payloadVersion: '1',
      payload: { programAddress: program, signature: 'migration-30', slot: 42 },
    } as Job);
    const rows = await harness.db.query<{
      venue: string;
      active_from: Date;
      active_until: Date | null;
    }>(
      `SELECT m.venue,m.active_from,m.active_until FROM token_markets m JOIN tokens t ON t.token_id=m.token_id WHERE t.address=$1 ORDER BY m.active_until NULLS LAST`,
      [address(30)]
    );
    expect(rows.kind).toBe('success');
    if (rows.kind === 'success') {
      expect(rows.value.map((r) => r.venue)).toEqual(['pump_fun_bonding_curve', 'pumpswap']);
      expect(rows.value[0]?.active_until).toEqual(rows.value[0]?.active_from);
      expect(rows.value[1]?.active_from).toEqual(new Date('2026-11-01T00:00:00Z'));
    }
    const active = await harness.db.query<{ venue: string }>(
      `SELECT m.venue FROM token_markets m JOIN tokens t ON t.token_id=m.token_id WHERE t.address=$1 AND m.active_from<=$2 AND(m.active_until IS NULL OR $2<m.active_until)`,
      [address(30), new Date('2026-11-01T00:00:00Z')]
    );
    expect(active.kind === 'success' && active.value).toEqual([{ venue: 'pumpswap' }]);
  });
  it('rejects a same-second migration from an earlier slot', async () => {
    const system = setup(40, true, true);
    await system.created.apply('creation-40', 40, [system.creation]);
    if (system.graduation.kind !== 'graduation') throw new Error('Expected graduation');
    const older = {
      ...system.graduation,
      graduation: {
        ...system.graduation.graduation,
        market: {
          ...system.graduation.graduation.market,
          origin: { ...system.graduation.graduation.market.origin, slot: 39 },
        },
      },
    };
    expect((await system.graduated.apply('migration-40', 39, [older])).kind).toBe('failure');
  });
  it('restores search progress through a new store instance after restart', async () => {
    const first = new PostgresCreationHistoryProgress(harness.db, program);
    const progress = {
      beforeSignature: 'cursor',
      oldest: [{ signature: 'oldest', slot: 3, err: null }],
      complete: false,
      nextCandidate: 0,
    };
    expect(await first.put(address(70), progress)).toEqual(success(undefined));
    expect(await new PostgresCreationHistoryProgress(harness.db, program).get(address(70))).toEqual(
      success(progress)
    );
  });
});
