import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import {
  ExecutionFactsConfig,
  ExecutionTarget,
  RpcAccount,
  SolanaExecutionFactsSource,
} from './execution-facts';
import { encodeBase58 } from './pump-events';

const address = (byte: number): string => encodeBase58(Buffer.alloc(32, byte));
const config: ExecutionFactsConfig = {
  pumpProgram: address(1),
  swapProgram: address(2),
  swapGlobalConfig: address(3),
  intervalMs: 1000,
  maxTargets: 10,
  maxSlotAgeMs: 5000,
  candidateMaxAgeMs: 60000,
};
const target: ExecutionTarget = {
  tokenId: 'test',
  mint: address(4),
  marketAddress: address(5),
  venue: 'pump_fun_bonding_curve',
};
function account(data: Buffer, owner: string): RpcAccount {
  return { owner, executable: false, data: [data.toString('base64'), 'base64'] };
}
function programAccount(name: string, length: number): Buffer {
  const data = Buffer.alloc(length);
  createHash('sha256').update(`account:${name}`).digest().copy(data, 0, 0, 8);
  return data;
}
function mint(): RpcAccount {
  const data = Buffer.alloc(82);
  data[44] = 6;
  data[45] = 1;
  return account(data, 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
}
function curve(): RpcAccount {
  const data = programAccount('BondingCurve', 81);
  data.writeBigUInt64LE(9007199254740993n, 8);
  data.writeBigUInt64LE(30000000000n, 16);
  data.writeBigUInt64LE(100000000n, 24);
  data.writeBigUInt64LE(2000000000n, 32);
  return account(data, config.pumpProgram);
}
function source(
  accounts: (RpcAccount | null)[],
  blockTime: unknown = 1000,
  slot = 20
): SolanaExecutionFactsSource {
  return new SolanaExecutionFactsSource(
    (method): Promise<unknown> =>
      Promise.resolve(
        method === 'getBlockTime' ? blockTime : { context: { slot }, value: accounts }
      ),
    config,
    () => 1001000
  );
}
function change(input: RpcAccount, mutation: (data: Buffer) => void): RpcAccount {
  const data = Buffer.from(input.data[0], 'base64');
  mutation(data);
  return account(data, input.owner);
}
describe('production account facts', () => {
  it('preserves exact integer reserves, account evidence and slot time', async () => {
    const result = await source([curve(), mint()]).capture(target);
    expect(result.observedAt.toISOString()).toBe('1970-01-01T00:16:41.000Z');
    expect(result.raw['paperExecution']).toMatchObject({
      occurredAt: '1970-01-01T00:16:40.000Z',
      tokenReserves: '9007199254.740993',
      cashReserves: '30',
      realTokenReserves: '100',
      realCashReserves: '2',
      verified: true,
      tradable: true,
      sellable: true,
      criticalRisk: false,
      marketAddress: target.marketAddress,
    });
    expect(result.raw['accounts']).toHaveLength(2);
  });
  it.each([null, 900, 1100])('rejects unavailable or stale slot time %s', async (time) => {
    await expect(source([curve(), mint()], time).capture(target)).rejects.toThrow();
  });
  it('rejects missing accounts, wrong owners and unknown discriminators', async () => {
    for (const bad of [
      null,
      { ...curve(), owner: config.swapProgram },
      change(curve(), (data) => {
        data[0] = 0;
      }),
    ])
      await expect(source([bad, mint()]).capture(target)).rejects.toThrow();
  });
  it('rejects Token 2022 and unmodelled modes instead of assuming sellability', async () => {
    await expect(
      source([curve(), { ...mint(), owner: address(8) }]).capture(target)
    ).rejects.toThrow();
    await expect(
      source([account(programAccount('BondingCurve', 100), config.pumpProgram), mint()]).capture(
        target
      )
    ).rejects.toThrow();
    const mayhem = Buffer.concat([Buffer.from(curve().data[0], 'base64'), Buffer.from([1])]);
    await expect(
      source([account(mayhem, config.pumpProgram), mint()]).capture(target)
    ).rejects.toThrow();
  });
  it('records active authority risk as blocking facts', async () => {
    const risky = change(mint(), (data) => {
      data.writeUInt32LE(1, 46);
    });
    const result = await source([curve(), risky]).capture(target);
    expect(result.raw['paperExecution']).toMatchObject({
      tradable: false,
      sellable: true,
      criticalRisk: true,
    });
  });
  it('blocks completed curves and empty real cash reserves', async () => {
    for (const bad of [
      change(curve(), (data) => {
        data[48] = 1;
      }),
      change(curve(), (data) => {
        data.writeBigUInt64LE(0n, 32);
      }),
    ]) {
      const result = await source([bad, mint()]).capture(target);
      expect(result.raw['paperExecution']).toMatchObject({ sellable: false });
    }
  });
  it('rejects regressed contexts', async () => {
    let slot = 20;
    const rpc = vi.fn((method: string): Promise<unknown> =>
      Promise.resolve(
        method === 'getBlockTime' ? 1000 : { context: { slot: slot-- }, value: [curve(), mint()] }
      )
    );
    const adapter = new SolanaExecutionFactsSource(rpc, config, () => 1001000);
    await adapter.capture(target);
    await expect(adapter.capture(target)).rejects.toThrow('regressed');
  });
  it('distinguishes capture times when two slots share the same block timestamp', async () => {
    let capturedAt = 1001000;
    let slot = 20;
    const adapter = new SolanaExecutionFactsSource(
      (method): Promise<unknown> =>
        Promise.resolve(
          method === 'getBlockTime' ? 1000 : { context: { slot: slot++ }, value: [curve(), mint()] }
        ),
      config,
      () => capturedAt
    );
    const first = await adapter.capture(target);
    capturedAt++;
    const second = await adapter.capture(target);
    expect(second.observedAt.getTime()).toBeGreaterThan(first.observedAt.getTime());
    expect(second.raw['paperExecution']).toMatchObject({ occurredAt: '1970-01-01T00:16:40.000Z' });
  });
  it('binds pool mints/vaults, checks global disable flags and rejects changed pointers', async () => {
    const pool = programAccount('Pool', 243);
    Buffer.alloc(32, 4).copy(pool, 43);
    // WSOL bytes, independently decoded from its canonical base58 address.
    const wsol = Buffer.from(
      '069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001',
      'hex'
    );
    expect(encodeBase58(wsol)).toBe('So11111111111111111111111111111111111111112');
    wsol.copy(pool, 75);
    Buffer.alloc(32, 6).copy(pool, 139);
    Buffer.alloc(32, 7).copy(pool, 171);
    const vault = (base: Buffer): RpcAccount => {
      const data = Buffer.alloc(165);
      base.copy(data);
      Buffer.alloc(32, 5).copy(data, 32);
      data.writeBigUInt64LE(100000n, 64);
      data[108] = 1;
      return account(data, mint().owner);
    };
    const global = programAccount('GlobalConfig', 321);
    const run = async (changed = false): Promise<Record<string, unknown>> => {
      let count = 0;
      const adapter = new SolanaExecutionFactsSource(
        (method): Promise<unknown> => {
          if (method === 'getBlockTime') return Promise.resolve(1000);
          count++;
          const finalPool = Buffer.from(pool);
          if (changed && count > 1) Buffer.alloc(32, 9).copy(finalPool, 139);
          return Promise.resolve({
            context: { slot: 20 },
            value:
              count === 1
                ? [account(pool, config.swapProgram)]
                : [
                    account(finalPool, config.swapProgram),
                    mint(),
                    vault(Buffer.alloc(32, 4)),
                    vault(wsol),
                    account(global, config.swapProgram),
                  ],
          });
        },
        config,
        () => 1001000
      );
      return (await adapter.capture({ ...target, venue: 'pumpswap' })).raw;
    };
    expect((await run())['paperExecution']).toMatchObject({ sellable: true, tradable: true });
    global[56] = 16;
    expect((await run())['paperExecution']).toMatchObject({ sellable: false });
    await expect(run(true)).rejects.toThrow('identity changed');
  });
});
