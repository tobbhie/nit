import { describe, expect, it } from 'vitest';
import { LaunchSource } from '@nit/domain';
import { PumpEventDecoder, encodeBase58, isPumpLifecycleNotification } from './pump-events';
import { SolanaLaunchTransaction } from './solana-rpc';

const EVENT_CPI_TAG = bytes(228, 69, 165, 46, 81, 203, 154, 29);
const CREATE_EVENT = bytes(27, 114, 169, 77, 222, 235, 99, 118);
const MIGRATION_EVENT = bytes(189, 233, 93, 185, 92, 148, 234, 148);
const PROGRAM = 'ConfiguredPumpProgram111111111111111111111111';

describe('PumpEventDecoder', () => {
  it('filters only known trade-only logs from the emitting Pump program', () => {
    const program = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
    const notice = (logs: unknown): unknown => ({ params: { result: { value: { logs } } } });
    const logs = (name: string): string[] => [
      'Program ' + program + ' invoke [1]',
      'Program log: Instruction: ' + name,
      'Program ' + program + ' success',
    ];
    expect(isPumpLifecycleNotification(notice(logs('BuyV3')), program)).toBe(false);
    expect(isPumpLifecycleNotification(notice(logs('Sell')), program)).toBe(false);
    for (const name of [
      'MultiHopCurveSwap',
      'CloseUserVolumeAccumulator',
      'SweepCreatorFee',
      'CollectCreatorFeeV2',
    ])
      expect(isPumpLifecycleNotification(notice(logs(name)), program)).toBe(false);
    expect(
      isPumpLifecycleNotification(
        notice([
          'Program 11111111111111111111111111111111 invoke [1]',
          'Program log: Instruction: Create',
          'Program 11111111111111111111111111111111 success',
        ]),
        program
      )
    ).toBe(false);
    expect(isPumpLifecycleNotification(notice(['Program log: Instruction: Create']), program)).toBe(
      true
    );
    for (const name of ['Create', 'CreateV2', 'Migrate', 'FutureInstruction'])
      expect(isPumpLifecycleNotification(notice(logs(name)), program)).toBe(true);
    expect(isPumpLifecycleNotification(notice([...logs('Buy'), 'Log truncated']), program)).toBe(
      true
    );
    expect(isPumpLifecycleNotification(notice([]), program)).toBe(true);
    expect(isPumpLifecycleNotification(notice('missing logs'), program)).toBe(true);
    expect(
      isPumpLifecycleNotification(
        notice([
          'Program ' + program + ' invoke [1]',
          'Program log: Instruction: Buy',
          'Program ' + program + ' invoke [2]',
          'Program log: Instruction: Migrate',
          'Program ' + program + ' success',
          'Program ' + program + ' success',
        ]),
        program
      )
    ).toBe(true);
  });
  it('decodes CreateEvent CPI data with exact nested instruction identity', () => {
    const mint = repeated(1);
    const curve = repeated(2);
    const user = repeated(3);
    const creator = repeated(4);
    const data = concat(
      EVENT_CPI_TAG,
      CREATE_EVENT,
      string('Real Token'),
      string('REAL'),
      string('https://metadata.example/token.json'),
      mint,
      curve,
      user,
      creator,
      integer(1_799_808_000n, 8)
    );
    const result = new PumpEventDecoder().decode(
      transaction(LaunchSource.PUMP_FUN, [otherInstruction(), eventInstruction(data)])
    );

    expect(result.kind).toBe('success');
    if (result.kind === 'failure') throw result.error;
    expect(result.value).toHaveLength(1);
    expect(result.value[0]).toMatchObject({
      kind: 'launch',
      launch: {
        address: encodeBase58(mint),
        ticker: 'REAL',
        name: 'Real Token',
        creatorWallet: encodeBase58(creator),
        market: {
          marketAddress: encodeBase58(curve),
          origin: {
            slot: 42,
            position: {
              transactionSignature: 'signature-1',
              instructionIndex: 3,
              innerInstructionIndex: 1,
            },
          },
        },
      },
    });
    const occurrence = result.value[0];
    if (occurrence?.kind !== 'launch') throw new Error('Expected launch');
    expect(occurrence.launch.launchTime.toISOString()).toBe('2027-01-13T02:40:00.000Z');
    expect(occurrence.launch.raw).toMatchObject({
      event: 'CreateEvent',
      uri: 'https://metadata.example/token.json',
      user: encodeBase58(user),
    });
  });

  it('decodes CompletePumpAmmMigrationEvent without manufacturing launch metadata', () => {
    const user = repeated(5);
    const mint = repeated(6);
    const curve = repeated(7);
    const pool = repeated(8);
    const quoteMint = repeated(9);
    const data = concat(
      EVENT_CPI_TAG,
      MIGRATION_EVENT,
      user,
      mint,
      integer(100n, 8),
      integer(200n, 8),
      integer(3n, 8),
      curve,
      integer(1_799_808_000n, 8),
      pool,
      quoteMint
    );
    const result = new PumpEventDecoder().decode(
      transaction(LaunchSource.PUMPSWAP, [eventInstruction(data)])
    );

    expect(result.kind).toBe('success');
    if (result.kind === 'failure') throw result.error;
    expect(result.value).toEqual([
      {
        kind: 'graduation',
        graduation: {
          address: encodeBase58(mint),
          graduatedAt: new Date('2027-01-13T02:40:00.000Z'),
          market: {
            marketAddress: encodeBase58(pool),
            origin: {
              kind: 'chain',
              slot: 42,
              position: {
                transactionSignature: 'signature-1',
                instructionIndex: 3,
                innerInstructionIndex: 0,
              },
            },
          },
          raw: {
            event: 'CompletePumpAmmMigrationEvent',
            user: encodeBase58(user),
            mint: encodeBase58(mint),
            mintAmount: '100',
            solAmount: '200',
            poolMigrationFee: '3',
            bondingCurve: encodeBase58(curve),
            timestamp: '2027-01-13T02:40:00.000Z',
            pool: encodeBase58(pool),
            quoteMint: encodeBase58(quoteMint),
            instructionData: encodeBase58(data),
          },
        },
      },
    ]);
  });

  it('ignores unrelated Anchor events and fails closed on a truncated recognized event', () => {
    const decoder = new PumpEventDecoder();
    const ignored = decoder.decode(
      transaction(LaunchSource.PUMP_FUN, [eventInstruction(concat(EVENT_CPI_TAG, repeated(99, 8)))])
    );
    const malformed = decoder.decode(
      transaction(LaunchSource.PUMP_FUN, [eventInstruction(concat(EVENT_CPI_TAG, CREATE_EVENT))])
    );

    expect(ignored).toEqual({ kind: 'success', value: [] });
    expect(malformed.kind).toBe('failure');
    if (malformed.kind === 'success') throw new Error('Expected malformed event to fail');
    expect(malformed.error.message).toContain('name length is truncated');
  });

  it('rejects invalid base58 from an instruction attributed to the configured program', () => {
    const result = new PumpEventDecoder().decode(
      transaction(LaunchSource.PUMP_FUN, [{ programId: PROGRAM, data: '0OIl' }])
    );
    expect(result.kind).toBe('failure');
  });
});

function transaction(
  launchSource: LaunchSource,
  instructions: readonly unknown[]
): SolanaLaunchTransaction {
  return {
    signature: 'signature-1',
    slot: 42,
    launchSource,
    programAddress: PROGRAM,
    observedAt: new Date('2027-01-15T00:00:01.000Z'),
    transaction: {
      meta: { innerInstructions: [{ index: 3, instructions }] },
      transaction: {},
    },
  };
}

function eventInstruction(data: Uint8Array): Record<string, unknown> {
  return { programId: PROGRAM, data: encodeBase58(data) };
}

function otherInstruction(): Record<string, unknown> {
  return { programId: 'OtherProgram111111111111111111111111111111', data: 'not-base58!' };
}

function string(value: string): Uint8Array {
  const encoded = new TextEncoder().encode(value);
  return concat(integer(BigInt(encoded.length), 4), encoded);
}

function integer(value: bigint, length: number): Uint8Array {
  const result = new Uint8Array(length);
  let remaining = value;
  for (let index = 0; index < length; index += 1) {
    result[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return result;
}

function repeated(value: number, length = 32): Uint8Array {
  return new Uint8Array(length).fill(value);
}

function bytes(...values: number[]): Uint8Array {
  return Uint8Array.from(values);
}

function concat(...values: readonly Uint8Array[]): Uint8Array {
  const result = new Uint8Array(values.reduce((length, value) => length + value.length, 0));
  let offset = 0;
  for (const value of values) {
    result.set(value, offset);
    offset += value.length;
  }
  return result;
}
