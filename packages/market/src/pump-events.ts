import { LaunchSource, Result, failure, success } from '@nit/domain';
import { DecodedMarketOccurrence } from './reconciliation';
import { SolanaLaunchTransaction, SolanaMarketTransactionDecoder } from './solana-rpc';

const EVENT_CPI_TAG = Uint8Array.from([228, 69, 165, 46, 81, 203, 154, 29]);
const CREATE_EVENT = Uint8Array.from([27, 114, 169, 77, 222, 235, 99, 118]);
const COMPLETE_PUMP_AMM_MIGRATION_EVENT = Uint8Array.from([189, 233, 93, 185, 92, 148, 234, 148]);
const MAX_BORSH_STRING_BYTES = 16_384;
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const BASE58_VALUES = new Map([...BASE58_ALPHABET].map((character, index) => [character, index]));

/** Decodes the two Pump events that alter NIT's canonical token-market lifecycle. */
export class PumpEventDecoder implements SolanaMarketTransactionDecoder {
  decode(transaction: SolanaLaunchTransaction): Result<readonly DecodedMarketOccurrence[], Error> {
    if (
      transaction.launchSource !== LaunchSource.PUMP_FUN &&
      transaction.launchSource !== LaunchSource.PUMPSWAP
    ) {
      return failure(
        new Error(`Pump event decoder does not support '${String(transaction.launchSource)}'`)
      );
    }

    const innerInstructions = readInnerInstructions(transaction.transaction);
    if (innerInstructions.kind === 'failure') return innerInstructions;

    const decoded: DecodedMarketOccurrence[] = [];
    for (const group of innerInstructions.value) {
      for (const [innerInstructionIndex, instruction] of group.instructions.entries()) {
        if (!isRecord(instruction) || instruction.programId !== transaction.programAddress)
          continue;
        if (typeof instruction.data !== 'string') {
          return failure(new Error('Pump inner instruction is missing base58 data'));
        }

        const bytes = decodeBase58(instruction.data);
        if (bytes.kind === 'failure') return bytes;
        if (bytes.value.length < 16 || !equalAt(bytes.value, EVENT_CPI_TAG, 0)) continue;

        const discriminator = bytes.value.subarray(8, 16);
        const payload = bytes.value.subarray(16);
        const position = {
          transactionSignature: transaction.signature,
          instructionIndex: group.index,
          innerInstructionIndex,
        };

        try {
          if (
            transaction.launchSource === LaunchSource.PUMP_FUN &&
            equalAt(discriminator, CREATE_EVENT, 0)
          ) {
            const event = decodeCreateEvent(payload);
            decoded.push({
              kind: 'launch',
              launch: {
                address: event.mint,
                ticker: event.symbol,
                name: event.name,
                launchTime: event.timestamp,
                creatorWallet: event.creator,
                market: {
                  marketAddress: event.bondingCurve,
                  origin: { kind: 'chain', position, slot: transaction.slot },
                },
                raw: {
                  event: 'CreateEvent',
                  name: event.name,
                  symbol: event.symbol,
                  uri: event.uri,
                  mint: event.mint,
                  bondingCurve: event.bondingCurve,
                  user: event.user,
                  creator: event.creator,
                  timestamp: event.timestamp.toISOString(),
                  instructionData: instruction.data,
                },
              },
            });
          } else if (
            transaction.launchSource === LaunchSource.PUMPSWAP &&
            equalAt(discriminator, COMPLETE_PUMP_AMM_MIGRATION_EVENT, 0)
          ) {
            const event = decodeMigrationEvent(payload);
            decoded.push({
              kind: 'graduation',
              graduation: {
                address: event.mint,
                graduatedAt: event.timestamp,
                market: {
                  marketAddress: event.pool,
                  origin: { kind: 'chain', position, slot: transaction.slot },
                },
                raw: {
                  event: 'CompletePumpAmmMigrationEvent',
                  user: event.user,
                  mint: event.mint,
                  mintAmount: event.mintAmount.toString(),
                  solAmount: event.solAmount.toString(),
                  poolMigrationFee: event.poolMigrationFee.toString(),
                  bondingCurve: event.bondingCurve,
                  timestamp: event.timestamp.toISOString(),
                  pool: event.pool,
                  quoteMint: event.quoteMint,
                  instructionData: instruction.data,
                },
              },
            });
          }
        } catch (error) {
          return failure(
            error instanceof Error
              ? new Error(`Malformed Pump event: ${error.message}`)
              : new Error('Malformed Pump event')
          );
        }
      }
    }
    return success(decoded);
  }
}

interface InnerInstructionGroup {
  index: number;
  instructions: readonly unknown[];
}

function readInnerInstructions(
  transaction: Record<string, unknown>
): Result<readonly InnerInstructionGroup[], Error> {
  const meta = transaction.meta;
  if (!isRecord(meta)) return failure(new Error('Solana transaction is missing metadata'));
  if (meta.innerInstructions === null) return success([]);
  if (!Array.isArray(meta.innerInstructions)) {
    return failure(new Error('Solana transaction has malformed inner instructions'));
  }

  const groups: InnerInstructionGroup[] = [];
  for (const value of meta.innerInstructions) {
    if (
      !isRecord(value) ||
      !Number.isSafeInteger(value.index) ||
      (value.index as number) < 0 ||
      !Array.isArray(value.instructions)
    ) {
      return failure(new Error('Solana transaction has a malformed inner-instruction group'));
    }
    groups.push({ index: value.index as number, instructions: value.instructions });
  }
  return success(groups);
}

interface CreateEventFields {
  name: string;
  symbol: string;
  uri: string;
  mint: string;
  bondingCurve: string;
  user: string;
  creator: string;
  timestamp: Date;
}

function decodeCreateEvent(payload: Uint8Array): CreateEventFields {
  const reader = new BorshReader(payload);
  return {
    name: reader.string('name'),
    symbol: reader.string('symbol'),
    uri: reader.string('uri'),
    mint: reader.publicKey('mint'),
    bondingCurve: reader.publicKey('bonding curve'),
    user: reader.publicKey('user'),
    creator: reader.publicKey('creator'),
    timestamp: timestamp(reader.i64('timestamp')),
  };
}

interface MigrationEventFields {
  user: string;
  mint: string;
  mintAmount: bigint;
  solAmount: bigint;
  poolMigrationFee: bigint;
  bondingCurve: string;
  timestamp: Date;
  pool: string;
  quoteMint: string;
}

function decodeMigrationEvent(payload: Uint8Array): MigrationEventFields {
  const reader = new BorshReader(payload);
  return {
    user: reader.publicKey('user'),
    mint: reader.publicKey('mint'),
    mintAmount: reader.u64('mint amount'),
    solAmount: reader.u64('sol amount'),
    poolMigrationFee: reader.u64('pool migration fee'),
    bondingCurve: reader.publicKey('bonding curve'),
    timestamp: timestamp(reader.i64('timestamp')),
    pool: reader.publicKey('pool'),
    quoteMint: reader.publicKey('quote mint'),
  };
}

class BorshReader {
  private offset = 0;

  constructor(private readonly bytes: Uint8Array) {}

  string(label: string): string {
    const length = Number(this.unsigned(4, `${label} length`));
    if (length > MAX_BORSH_STRING_BYTES) throw new Error(`${label} exceeds the string limit`);
    const bytes = this.take(length, label);
    try {
      return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    } catch {
      throw new Error(`${label} is not valid UTF-8`);
    }
  }

  publicKey(label: string): string {
    return encodeBase58(this.take(32, label));
  }

  u64(label: string): bigint {
    return this.unsigned(8, label);
  }

  i64(label: string): bigint {
    const value = this.unsigned(8, label);
    return value >= 1n << 63n ? value - (1n << 64n) : value;
  }

  private unsigned(length: number, label: string): bigint {
    const bytes = this.take(length, label);
    let value = 0n;
    for (let index = bytes.length - 1; index >= 0; index -= 1) {
      value = (value << 8n) | BigInt(bytes[index] as number);
    }
    return value;
  }

  private take(length: number, label: string): Uint8Array {
    if (length < 0 || this.offset + length > this.bytes.length) {
      throw new Error(`${label} is truncated`);
    }
    const value = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return value;
  }
}

function timestamp(seconds: bigint): Date {
  const milliseconds = seconds * 1_000n;
  if (
    milliseconds < BigInt(-8_640_000_000_000_000) ||
    milliseconds > BigInt(8_640_000_000_000_000)
  ) {
    throw new Error('timestamp is outside the supported range');
  }
  const value = new Date(Number(milliseconds));
  if (!Number.isFinite(value.getTime())) throw new Error('timestamp is invalid');
  return value;
}

function decodeBase58(value: string): Result<Uint8Array, Error> {
  if (value === '') return failure(new Error('Pump instruction data is blank'));
  const bytes = [0];
  for (const character of value) {
    const digit = BASE58_VALUES.get(character);
    if (digit === undefined) {
      return failure(new Error('Pump instruction data is not valid base58'));
    }
    let carry = digit;
    for (let index = 0; index < bytes.length; index += 1) {
      carry += (bytes[index] as number) * 58;
      bytes[index] = carry & 0xff;
      carry >>= 8;
    }
    while (carry > 0) {
      bytes.push(carry & 0xff);
      carry >>= 8;
    }
  }
  for (let index = 0; value[index] === '1' && index < value.length - 1; index += 1) {
    bytes.push(0);
  }
  return success(Uint8Array.from(bytes.reverse()));
}

export function encodeBase58(value: Uint8Array): string {
  if (value.length === 0) return '';
  const digits = [0];
  for (const byte of value) {
    let carry = byte;
    for (let index = 0; index < digits.length; index += 1) {
      carry += (digits[index] as number) << 8;
      digits[index] = carry % 58;
      carry = Math.floor(carry / 58);
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = Math.floor(carry / 58);
    }
  }
  let encoded = '';
  for (let index = 0; value[index] === 0 && index < value.length - 1; index += 1) {
    encoded += '1';
  }
  for (let index = digits.length - 1; index >= 0; index -= 1) {
    encoded += BASE58_ALPHABET[digits[index] as number];
  }
  return encoded;
}

function equalAt(value: Uint8Array, expected: Uint8Array, offset: number): boolean {
  if (value.length - offset < expected.length) return false;
  return expected.every((byte, index) => value[offset + index] === byte);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
