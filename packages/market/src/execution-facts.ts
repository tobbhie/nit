import { createHash } from 'node:crypto';
import { z } from 'zod';
import { encodeBase58 } from './pump-events';

export const EXECUTION_FACTS_SOURCE = 'solana-account-facts:v1';
const TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022_PROGRAM = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const WSOL = 'So11111111111111111111111111111111111111112';
const Address = z.string().regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/);
export const ExecutionFactsConfigSchema = z
  .object({
    pumpProgram: Address,
    swapProgram: Address,
    swapGlobalConfig: Address,
    intervalMs: z.number().int().min(1000).max(60000),
    maxTargets: z.number().int().min(1).max(100),
    maxSlotAgeMs: z.number().int().min(1000).max(60000),
    candidateMaxAgeMs: z.number().int().positive(),
  })
  .strict();
export type ExecutionFactsConfig = z.infer<typeof ExecutionFactsConfigSchema>;
export interface ExecutionTarget {
  tokenId: string;
  mint: string;
  marketAddress: string;
  venue: 'pump_fun_bonding_curve' | 'pumpswap';
}
export interface RpcAccount {
  owner: string;
  executable: boolean;
  data: [string, 'base64'];
}
const Account = z.object({
  owner: Address,
  executable: z.boolean(),
  data: z.tuple([z.string().max(32768), z.literal('base64')]),
});
const AccountBatch = z.object({
  context: z.object({ slot: z.number().int().nonnegative() }),
  value: z.array(Account.nullable()),
});
export type FactsRpc = (method: string, params: unknown[]) => Promise<unknown>;

/** A versioned reserve model, not a transaction simulation or a claim of rug safety. */
export class SolanaExecutionFactsSource {
  private lastSlot = 0;
  constructor(
    private readonly rpc: FactsRpc,
    private readonly config: ExecutionFactsConfig,
    private readonly clock: () => number = Date.now
  ) {}

  async capture(
    target: ExecutionTarget
  ): Promise<{ observedAt: Date; raw: Record<string, unknown> }> {
    Address.parse(target.mint);
    Address.parse(target.marketAddress);
    const addresses = [target.marketAddress, target.mint];
    if (target.venue === 'pumpswap') {
      const preliminary = await this.accounts([target.marketAddress]);
      const pool = owned(preliminary.value[0], this.config.swapProgram, 'Pool');
      addresses.push(key(pool, 139), key(pool, 171), this.config.swapGlobalConfig);
    }
    const batch = await this.accounts(addresses);
    const blockTime = z
      .number()
      .int()
      .nonnegative()
      .parse(await this.rpc('getBlockTime', [batch.context.slot]));
    const age = this.clock() - blockTime * 1000;
    if (age < -1000 || age > this.config.maxSlotAgeMs)
      throw new Error('RPC account context is stale or in the future');
    const occurredAt = new Date(blockTime * 1000);
    const observedAt = new Date(this.clock());
    const mint = tokenMint(batch.value[1], target.mint);
    const risk = mint.readUInt32LE(0) !== 0 || mint.readUInt32LE(46) !== 0;
    let token: bigint;
    let cash: bigint;
    let realToken: bigint;
    let realCash: bigint;
    let tradable = true;
    let sellable = true;
    if (target.venue === 'pump_fun_bonding_curve') {
      const curve = owned(batch.value[0], this.config.pumpProgram, 'BondingCurve');
      validateCurve(curve);
      token = curve.readBigUInt64LE(8);
      cash = curve.readBigUInt64LE(16);
      realToken = curve.readBigUInt64LE(24);
      realCash = curve.readBigUInt64LE(32);
      if (curve[48] !== 0 && curve[48] !== 1) throw new Error('Invalid curve completion flag');
      tradable = curve[48] === 0;
      sellable = tradable;
    } else {
      const pool = owned(batch.value[0], this.config.swapProgram, 'Pool');
      if (
        ![243, 244, 245, 261, 287].includes(pool.length) ||
        pool.subarray(243).some((byte) => byte !== 0)
      )
        throw new Error('Unsupported pool account layout, fees or virtual reserve mode');
      if (
        key(pool, 43) !== target.mint ||
        key(pool, 75) !== WSOL ||
        key(pool, 139) !== addresses[2] ||
        key(pool, 171) !== addresses[3]
      )
        throw new Error('Pool mint or vault identity changed');
      token = vault(batch.value[2], target.mint, target.marketAddress, batch.value[1]!.owner);
      cash = vault(batch.value[3], WSOL, target.marketAddress, TOKEN_PROGRAM);
      realToken = token;
      realCash = cash;
      const global = owned(batch.value[4], this.config.swapProgram, 'GlobalConfig');
      if (global.length < 57) throw new Error('Truncated global config');
      tradable = ((global[56] as number) & 8) === 0;
      sellable = ((global[56] as number) & 16) === 0;
    }
    if (token <= 0n || cash <= 0n || realToken < 0n || realCash < 0n)
      throw new Error('Invalid execution reserves');
    return {
      observedAt,
      raw: {
        schemaVersion: 1,
        contextSlot: batch.context.slot,
        addresses,
        accounts: batch.value,
        checks: 'token-owner-mint-authorities-metadata-only-extensions-vaults-and-supported-layout',
        limitations: [
          'not-a-sell-simulation',
          'not-holder-or-bundle-risk-analysis',
          'policy-fees-are-modelled',
        ],
        paperExecution: {
          schemaVersion: 1,
          marketAddress: target.marketAddress,
          cashAsset: 'SOL',
          occurredAt: occurredAt.toISOString(),
          tokenReserves: decimal(token, mint[44] as number),
          cashReserves: decimal(cash, 9),
          realTokenReserves: decimal(realToken, mint[44] as number),
          realCashReserves: decimal(realCash, 9),
          tokenDecimals: mint[44],
          cashDecimals: 9,
          verified: true,
          tradable: tradable && !risk && realToken > 0n,
          sellable: sellable && realCash > 0n,
          criticalRisk: risk,
        },
      },
    };
  }

  private async accounts(addresses: string[]): Promise<z.infer<typeof AccountBatch>> {
    const batch = AccountBatch.parse(
      await this.rpc('getMultipleAccounts', [
        addresses,
        { encoding: 'base64', commitment: 'confirmed', minContextSlot: this.lastSlot },
      ])
    );
    if (batch.value.length !== addresses.length || batch.context.slot < this.lastSlot)
      throw new Error('RPC account batch is incomplete or regressed');
    this.lastSlot = batch.context.slot;
    return batch;
  }
}

function bytes(account: RpcAccount | null | undefined, owner: string): Buffer {
  if (account === null || account === undefined || account.owner !== owner || account.executable)
    throw new Error('Missing account or unexpected account owner');
  const encoded = account.data[0];
  const data = Buffer.from(encoded, 'base64');
  if (data.toString('base64') !== encoded) throw new Error('Malformed account encoding');
  return data;
}
function owned(account: RpcAccount | null | undefined, owner: string, name: string): Buffer {
  const data = bytes(account, owner);
  const discriminator = createHash('sha256').update(`account:${name}`).digest().subarray(0, 8);
  if (!data.subarray(0, 8).equals(discriminator))
    throw new Error('Unknown program account discriminator');
  return data;
}
function tokenMint(account: RpcAccount | null | undefined, mintAddress: string): Buffer {
  const owner = account?.owner;
  if (owner !== TOKEN_PROGRAM && owner !== TOKEN_2022_PROGRAM)
    throw new Error('Unsupported token program');
  const data = bytes(account, owner);
  if (owner === TOKEN_PROGRAM && data.length !== 82)
    throw new Error('Unsupported token mint layout');
  if (owner === TOKEN_2022_PROGRAM) {
    const extensions = tokenExtensions(data, 82, 1);
    for (const [type, value] of extensions) {
      // Only metadata extensions: no fees, hooks, delegates, pauses or scaled amounts.
      if (type === 18 && value.length === 64) continue;
      if (type === 19) {
        validateTokenMetadata(value, mintAddress);
        continue;
      }
      throw new Error('Unsupported Token-2022 mint extension');
    }
  }
  if (
    data.length < 82 ||
    data[45] !== 1 ||
    (data[44] as number) > 18 ||
    ![0, 1].includes(data.readUInt32LE(0)) ||
    ![0, 1].includes(data.readUInt32LE(46))
  )
    throw new Error('Unsupported or uninitialized token mint');
  return data;
}

function validateCurve(curve: Buffer): void {
  // Current Pump IDL: SOL/default quote, ordinary constant-product reserves only.
  // The 150-byte allocation predates the final post-completion fields (166 bytes).
  if (![49, 81, 82, 83, 150, 166].includes(curve.length))
    throw new Error('Unsupported bonding curve account layout or mode');
  if ((curve[81] ?? 0) !== 0 || (curve[82] ?? 0) !== 0)
    throw new Error('Unsupported bonding curve account layout or mode');
  if (
    curve.length >= 150 &&
    (curve.subarray(83, 124).some((v) => v !== 0) ||
      (curve[124] ?? 0) > 1 ||
      curve[141] !== 0 ||
      curve.subarray(150).some((v) => v !== 0))
  )
    throw new Error('Unsupported bonding curve quote, flags or depth mode');
}
function vault(
  account: RpcAccount | null | undefined,
  mint: string,
  authority: string,
  owner: string
): bigint {
  const data = bytes(account, owner);
  if (owner === TOKEN_2022_PROGRAM) {
    for (const [type, value] of tokenExtensions(data, 165, 2)) {
      if (type !== 7 || value.length !== 0)
        throw new Error('Unsupported Token-2022 vault extension');
    }
  } else if (owner !== TOKEN_PROGRAM || data.length !== 165)
    throw new Error('Unsupported token vault layout');
  if (
    data.length < 165 ||
    key(data, 0) !== mint ||
    key(data, 32) !== authority ||
    data[108] !== 1 ||
    data.readUInt32LE(72) !== 0 ||
    data.readUInt32LE(129) !== 0
  )
    throw new Error('Invalid, frozen, delegated or closable pool vault');
  return data.readBigUInt64LE(64);
}

/** Token-2022 keeps the legacy prefix, zero mint padding, account type and u16 TLVs. */
function tokenExtensions(
  data: Buffer,
  baseLength: number,
  accountType: number
): Map<number, Buffer> {
  const result = new Map<number, Buffer>();
  if (data.length === baseLength) return result;
  if (
    data.length < 166 ||
    data.length === 355 ||
    data[165] !== accountType ||
    data.subarray(baseLength, 165).some((v) => v !== 0)
  )
    throw new Error('Malformed Token-2022 account framing');
  let offset = 166;
  while (offset < data.length) {
    if (data.subarray(offset).every((v) => v === 0)) break;
    if (offset + 4 > data.length) throw new Error('Truncated Token-2022 extension');
    const type = data.readUInt16LE(offset);
    const length = data.readUInt16LE(offset + 2);
    offset += 4;
    if (type === 0 || result.has(type) || offset + length > data.length)
      throw new Error('Malformed Token-2022 extension');
    result.set(type, data.subarray(offset, offset + length));
    offset += length;
  }
  return result;
}

function validateTokenMetadata(value: Buffer, mintAddress: string): void {
  if (value.length < 64 || key(value, 32) !== mintAddress)
    throw new Error('Token metadata mint mismatch');
  let offset = 64;
  const string = (): void => {
    if (offset + 4 > value.length) throw new Error('Truncated token metadata');
    const length = value.readUInt32LE(offset);
    offset += 4;
    if (length > 16384 || offset + length > value.length)
      throw new Error('Invalid token metadata string');
    new TextDecoder('utf-8', { fatal: true }).decode(value.subarray(offset, offset + length));
    offset += length;
  };
  string();
  string();
  string();
  if (offset + 4 > value.length) throw new Error('Truncated token metadata');
  const count = value.readUInt32LE(offset);
  offset += 4;
  if (count > 64) throw new Error('Token metadata field limit exceeded');
  for (let i = 0; i < count; i++) {
    string();
    string();
  }
  if (offset !== value.length) throw new Error('Unexpected token metadata trailing bytes');
}
function key(data: Buffer, offset: number): string {
  if (data.length < offset + 32) throw new Error('Truncated public key');
  return encodeBase58(data.subarray(offset, offset + 32));
}

/** Chain atomic units become the human decimal quantities used by paperQuote. */
function decimal(value: bigint, decimals: number): string {
  if (decimals === 0) return value.toString();
  const padded = value.toString().padStart(decimals + 1, '0');
  const fraction = padded.slice(-decimals).replace(/0+$/, '');
  return padded.slice(0, -decimals) + (fraction === '' ? '' : `.${fraction}`);
}
