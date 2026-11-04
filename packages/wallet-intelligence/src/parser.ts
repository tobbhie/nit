import { Chain, Result, failure, generateEvidenceId, success } from '@nit/domain';
import {
  ParsedWalletTransaction,
  SolanaWalletTransaction,
  WalletFundingEvent,
  WalletSwap,
} from './contracts';

const NATIVE_SOL = 'SOL';
const SOL_DECIMALS = 9;

export interface SolanaWalletSwapParserOptions {
  watchedWallets: readonly string[];
  quoteAssets: readonly string[];
  swapProgramAddresses: readonly string[];
  clock?: () => Date;
}

interface BalanceDelta {
  asset: string;
  rawDelta: bigint;
  decimals: number;
  accountIndexes: number[];
}

/**
 * Attributes swaps from complete owner balance deltas, never from log prose.
 * Ambiguous multi-asset transactions are deliberately rejected rather than
 * guessed into a buy or sell.
 */
export class SolanaWalletSwapParser {
  readonly version = 'owner-balance-delta-v1';
  private readonly watchedWallets: readonly string[];
  private readonly quoteAssets: ReadonlySet<string>;
  private readonly swapPrograms: ReadonlySet<string>;
  private readonly clock: () => Date;

  constructor(options: SolanaWalletSwapParserOptions) {
    this.watchedWallets = uniqueAddresses(options.watchedWallets, 'watched wallet');
    this.quoteAssets = new Set(uniqueAssets(options.quoteAssets));
    this.swapPrograms = new Set(uniqueAddresses(options.swapProgramAddresses, 'swap program'));
    this.clock = options.clock ?? ((): Date => new Date());
  }

  parse(input: SolanaWalletTransaction): Result<ParsedWalletTransaction, Error> {
    const root = input.transaction;
    const transaction = record(root['transaction']);
    const message = record(transaction?.['message']);
    const meta = record(root['meta']);
    const blockTime = integer(root['blockTime']);
    if (
      transaction === null ||
      message === null ||
      meta === null ||
      blockTime === null ||
      meta['err'] !== null
    ) {
      return failure(new Error('Solana wallet transaction is unsuccessful or malformed'));
    }

    const accountKeys = parseAccountKeys(message['accountKeys']);
    if (accountKeys === null) return failure(new Error('Solana account keys are malformed'));
    const invokedPrograms = invokedProgramAddresses(message, meta, accountKeys).filter((address) =>
      this.swapPrograms.has(address)
    );
    const occurredAt = new Date(blockTime * 1_000);
    const availableAt = maxDate(this.clock(), input.observedAt);
    const swaps: WalletSwap[] = [];
    const rejectedWallets: ParsedWalletTransaction['rejectedWallets'][number][] = [];

    for (const walletAddress of this.watchedWallets) {
      const walletIndex = accountKeys.findIndex((account) => account.address === walletAddress);
      if (walletIndex < 0 || accountKeys[walletIndex]?.signer !== true) {
        rejectedWallets.push({ walletAddress, reason: 'not_signer' });
        continue;
      }
      if (invokedPrograms.length === 0) {
        rejectedWallets.push({ walletAddress, reason: 'no_swap_program' });
        continue;
      }

      const deltas = walletBalanceDeltas(meta, accountKeys, walletAddress, walletIndex);
      if (deltas.kind === 'failure') return deltas;
      if (deltas.value.length === 0) {
        rejectedWallets.push({ walletAddress, reason: 'no_balance_change' });
        continue;
      }
      const classified = classifySwap(deltas.value, this.quoteAssets);
      if (classified === null) {
        rejectedWallets.push({ walletAddress, reason: 'ambiguous_balance_deltas' });
        continue;
      }
      const { target, quote, side } = classified;
      swaps.push({
        walletSwapId: generateEvidenceId(),
        chain: Chain.SOLANA,
        transactionSignature: input.signature,
        slot: input.slot,
        walletAddress,
        side,
        tokenMint: target.asset,
        tokenRawAmount: abs(target.rawDelta).toString(),
        tokenDecimals: target.decimals,
        quoteAsset: quote.asset,
        quoteRawAmount: abs(quote.rawDelta).toString(),
        quoteDecimals: quote.decimals,
        programAddresses: [...invokedPrograms].sort(),
        attribution: {
          method: 'owner_balance_delta',
          targetAccountIndexes: target.accountIndexes,
          quoteAccountIndexes: quote.accountIndexes,
          feeAdjustedNativeBalance: quote.asset === NATIVE_SOL,
        },
        occurredAt,
        observedAt: input.observedAt,
        availableAt,
      });
    }

    const fundingEvents = parseFundingEvents(
      message,
      meta,
      new Set(this.watchedWallets),
      input,
      occurredAt
    );
    return success({ swaps, fundingEvents, rejectedWallets });
  }
}

function walletBalanceDeltas(
  meta: Record<string, unknown>,
  accountKeys: readonly { address: string; signer: boolean }[],
  wallet: string,
  walletIndex: number
): Result<BalanceDelta[], Error> {
  const byAsset = new Map<string, BalanceDelta>();
  const before = tokenBalances(meta['preTokenBalances'], wallet);
  const after = tokenBalances(meta['postTokenBalances'], wallet);
  if (before === null || after === null) return failure(new Error('Token balances are malformed'));
  for (const key of new Set([...before.keys(), ...after.keys()])) {
    const pre = before.get(key);
    const post = after.get(key);
    const sample = post ?? pre;
    if (sample === undefined) continue;
    const delta = (post?.amount ?? 0n) - (pre?.amount ?? 0n);
    if (delta === 0n) continue;
    const current = byAsset.get(sample.mint);
    if (current === undefined) {
      byAsset.set(sample.mint, {
        asset: sample.mint,
        rawDelta: delta,
        decimals: sample.decimals,
        accountIndexes: [sample.accountIndex],
      });
    } else {
      if (current.decimals !== sample.decimals) {
        return failure(new Error(`Token decimal mismatch for mint '${sample.mint}'`));
      }
      current.rawDelta += delta;
      current.accountIndexes.push(sample.accountIndex);
    }
  }

  const preBalances = safeIntegerArray(meta['preBalances']);
  const postBalances = safeIntegerArray(meta['postBalances']);
  const fee = safeInteger(meta['fee']);
  if (preBalances === null || postBalances === null || fee === null) {
    return failure(new Error('Native balances are malformed or exceed safe integer precision'));
  }
  const preNative = preBalances[walletIndex];
  const postNative = postBalances[walletIndex];
  if (preNative === undefined || postNative === undefined) {
    return failure(new Error('Wallet native balance is absent'));
  }
  const feePayer = accountKeys[0]?.address === wallet;
  const nativeDelta = BigInt(postNative) - BigInt(preNative) + (feePayer ? BigInt(fee) : 0n);
  if (nativeDelta !== 0n) {
    byAsset.set(NATIVE_SOL, {
      asset: NATIVE_SOL,
      rawDelta: nativeDelta,
      decimals: SOL_DECIMALS,
      accountIndexes: [walletIndex],
    });
  }
  return success([...byAsset.values()].filter((delta) => delta.rawDelta !== 0n));
}

function classifySwap(
  deltas: readonly BalanceDelta[],
  quoteAssets: ReadonlySet<string>
): { target: BalanceDelta; quote: BalanceDelta; side: 'buy' | 'sell' } | null {
  if (deltas.length !== 2) return null;
  const quote = deltas.find((delta) => quoteAssets.has(delta.asset));
  const target = deltas.find((delta) => !quoteAssets.has(delta.asset));
  if (
    quote === undefined ||
    target === undefined ||
    quote.rawDelta === 0n ||
    target.rawDelta === 0n
  ) {
    return null;
  }
  if (target.rawDelta > 0n && quote.rawDelta < 0n) return { target, quote, side: 'buy' };
  if (target.rawDelta < 0n && quote.rawDelta > 0n) return { target, quote, side: 'sell' };
  return null;
}

interface TokenBalance {
  mint: string;
  accountIndex: number;
  decimals: number;
  amount: bigint;
}

function tokenBalances(value: unknown, owner: string): Map<string, TokenBalance> | null {
  if (!Array.isArray(value)) return null;
  const balances = new Map<string, TokenBalance>();
  for (const item of value) {
    const entry = record(item);
    if (entry === null || entry['owner'] !== owner) continue;
    const mint = string(entry['mint']);
    const accountIndex = integer(entry['accountIndex']);
    const amount = record(entry['uiTokenAmount']);
    const decimals = integer(amount?.['decimals']);
    const raw = string(amount?.['amount']);
    if (
      mint === null ||
      accountIndex === null ||
      decimals === null ||
      raw === null ||
      !/^\d+$/.test(raw)
    ) {
      return null;
    }
    balances.set(`${accountIndex}:${mint}`, { mint, accountIndex, decimals, amount: BigInt(raw) });
  }
  return balances;
}

function parseFundingEvents(
  message: Record<string, unknown>,
  meta: Record<string, unknown>,
  watched: ReadonlySet<string>,
  input: SolanaWalletTransaction,
  occurredAt: Date
): WalletFundingEvent[] {
  const events: WalletFundingEvent[] = [];
  const groups: { path: string; instructions: unknown }[] = [
    { path: 'top', instructions: message['instructions'] },
  ];
  if (Array.isArray(meta['innerInstructions'])) {
    for (const group of meta['innerInstructions']) {
      const inner = record(group);
      const parent = integer(inner?.['index']);
      if (parent !== null)
        groups.push({ path: `inner:${parent}`, instructions: inner?.['instructions'] });
    }
  }
  for (const group of groups) {
    if (!Array.isArray(group.instructions)) continue;
    group.instructions.forEach((raw, index) => {
      const instruction = record(raw);
      const parsed = record(instruction?.['parsed']);
      const info = record(parsed?.['info']);
      const source = string(info?.['source']);
      const destination = string(info?.['destination']);
      const lamports = safeInteger(info?.['lamports']);
      if (
        instruction?.['program'] !== 'system' ||
        parsed?.['type'] !== 'transfer' ||
        source === null ||
        destination === null ||
        lamports === null ||
        lamports <= 0 ||
        source === destination ||
        !watched.has(destination)
      )
        return;
      events.push({
        walletFundingEventId: generateEvidenceId(),
        chain: Chain.SOLANA,
        transactionSignature: input.signature,
        instructionPath: `${group.path}:${index}`,
        slot: input.slot,
        funderAddress: source,
        fundedWalletAddress: destination,
        lamports: String(lamports),
        occurredAt,
        observedAt: input.observedAt,
      });
    });
  }
  return events;
}

function invokedProgramAddresses(
  message: Record<string, unknown>,
  meta: Record<string, unknown>,
  keys: readonly { address: string }[]
): string[] {
  const addresses = new Set<string>();
  const collect = (value: unknown): void => {
    if (!Array.isArray(value)) return;
    for (const raw of value) {
      const instruction = record(raw);
      const direct = string(instruction?.['programId']);
      const index = integer(instruction?.['programIdIndex']);
      if (direct !== null) addresses.add(direct);
      else if (index !== null) {
        const key = keys[index];
        if (key !== undefined) addresses.add(key.address);
      }
    }
  };
  collect(message['instructions']);
  if (Array.isArray(meta['innerInstructions'])) {
    for (const group of meta['innerInstructions']) collect(record(group)?.['instructions']);
  }
  return [...addresses];
}

function parseAccountKeys(value: unknown): { address: string; signer: boolean }[] | null {
  if (!Array.isArray(value)) return null;
  const parsed: { address: string; signer: boolean }[] = [];
  for (const raw of value) {
    if (typeof raw === 'string') parsed.push({ address: raw, signer: false });
    else {
      const item = record(raw);
      const address = string(item?.['pubkey']);
      if (address === null || typeof item?.['signer'] !== 'boolean') return null;
      parsed.push({ address, signer: item['signer'] });
    }
  }
  return parsed;
}

function uniqueAddresses(values: readonly string[], label: string): string[] {
  const unique = [...new Set(values.map((value) => value.trim()))];
  if (unique.length === 0 || unique.some((value) => !/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value))) {
    throw new Error(`At least one valid ${label} address is required`);
  }
  return unique;
}

function uniqueAssets(values: readonly string[]): string[] {
  const unique = [...new Set(values.map((value) => value.trim()))];
  if (
    unique.length === 0 ||
    unique.some((value) => value !== NATIVE_SOL && !/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value))
  )
    throw new Error('At least one valid quote asset is required');
  return unique;
}

function maxDate(left: Date, right: Date): Date {
  return left.getTime() >= right.getTime() ? left : right;
}

function abs(value: bigint): bigint {
  return value < 0n ? -value : value;
}

function safeIntegerArray(value: unknown): number[] | null {
  if (!Array.isArray(value)) return null;
  const values = value.map(safeInteger);
  return values.some((item) => item === null) ? null : (values as number[]);
}

function safeInteger(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function integer(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function string(value: unknown): string | null {
  return typeof value === 'string' && value.trim() !== '' ? value : null;
}

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}
