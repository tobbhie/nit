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

const token2022 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
function extendedMint(entries: [number, Buffer][]): RpcAccount {
  const prefix = Buffer.alloc(166);
  Buffer.from(mint().data[0], 'base64').copy(prefix);
  prefix[165] = 1;
  return account(
    Buffer.concat([
      prefix,
      ...entries.map(([type, value]) => {
        const header = Buffer.alloc(4);
        header.writeUInt16LE(type);
        header.writeUInt16LE(value.length, 2);
        return Buffer.concat([header, value]);
      }),
    ]),
    token2022
  );
}
function metadata(): Buffer {
  const prefix = Buffer.alloc(64);
  Buffer.alloc(32, 4).copy(prefix, 32);
  const string = (value: string): Buffer => {
    const b = Buffer.from(value);
    const n = Buffer.alloc(4);
    n.writeUInt32LE(b.length);
    return Buffer.concat([n, b]);
  };
  return Buffer.concat([
    prefix,
    string('Token'),
    string('TOK'),
    string('ipfs://test-metadata'),
    Buffer.alloc(4),
  ]);
}
describe('production account facts', () => {
  it('accepts Token-2022 metadata-only mints and the current SOL curve layout', async () => {
    const current = Buffer.alloc(166);
    Buffer.from(curve().data[0], 'base64').copy(current);
    current[124] = 1; // Holder rewards do not alter trading reserves.
    current.writeBigUInt64LE(123n, 125);
    current.writeBigUInt64LE(456n, 133);
    current.writeBigUInt64LE(30000000000n, 142);
    const result = await source([
      account(current, config.pumpProgram),
      extendedMint([
        [18, Buffer.alloc(64)],
        [19, metadata()],
      ]),
    ]).capture(target);
    expect(result.raw['paperExecution']).toMatchObject({
      verified: true,
      tradable: true,
      sellable: true,
    });
  });
  it.each([1, 3, 6, 9, 10, 12, 14, 25, 26, 99])(
    'rejects unmodelled mint extension %s',
    async (type) => {
      await expect(
        source([curve(), extendedMint([[type, Buffer.alloc(64)]])]).capture(target)
      ).rejects.toThrow('Unsupported Token-2022');
    }
  );
  it('rejects duplicate, truncated, wrong-type and spoofed Token-2022 metadata', async () => {
    const spoofed = metadata();
    spoofed[32] = 9;
    const good = extendedMint([
      [18, Buffer.alloc(64)],
      [19, metadata()],
    ]);
    const variants = [
      extendedMint([
        [18, Buffer.alloc(64)],
        [18, Buffer.alloc(64)],
      ]),
      extendedMint([[19, spoofed]]),
      change(good, (d) => {
        d[165] = 2;
      }),
      change(good, (d) => {
        d[82] = 1;
      }),
      change(good, (d) => {
        d.writeUInt16LE(65535, 168);
      }),
    ];
    for (const variant of variants)
      await expect(source([curve(), variant]).capture(target)).rejects.toThrow();
  });
  it.each([81, 82, 83, 115, 123, 124, 141, 150, 158])(
    'rejects unsupported current curve mode at byte %s',
    async (offset) => {
      const current = Buffer.alloc(166);
      Buffer.from(curve().data[0], 'base64').copy(current);
      current[offset] = offset == 124 ? 2 : 1;
      await expect(
        source([account(current, config.pumpProgram), mint()]).capture(target)
      ).rejects.toThrow('Unsupported bonding curve');
    }
  );
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
    const vault = (base: Buffer, extended = false, extension = 7): RpcAccount => {
      const data = Buffer.alloc(165);
      base.copy(data);
      Buffer.alloc(32, 5).copy(data, 32);
      data.writeBigUInt64LE(100000n, 64);
      data[108] = 1;
      if (!extended) return account(data, mint().owner);
      const framed = Buffer.alloc(170);
      data.copy(framed);
      framed[165] = 2;
      framed.writeUInt16LE(extension, 166);
      return account(framed, token2022);
    };
    const global = programAccount('GlobalConfig', 321);
    const run = async (
      changed = false,
      extended = false,
      extension = 7,
      wrongOwner = false
    ): Promise<Record<string, unknown>> => {
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
                    extended
                      ? extendedMint([
                          [18, Buffer.alloc(64)],
                          [19, metadata()],
                        ])
                      : mint(),
                    vault(Buffer.alloc(32, 4), extended && !wrongOwner, extension),
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
    expect((await run(false, true))['paperExecution']).toMatchObject({
      sellable: true,
      tradable: true,
    });
    await expect(run(false, true, 8)).rejects.toThrow('Unsupported Token-2022 vault extension');
    await expect(run(false, true, 7, true)).rejects.toThrow('unexpected account owner');
    global[56] = 16;
    expect((await run())['paperExecution']).toMatchObject({ sellable: false });
    await expect(run(true)).rejects.toThrow('identity changed');
  });
});
