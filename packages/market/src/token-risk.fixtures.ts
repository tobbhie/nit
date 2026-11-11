import { TokenRiskConfig } from './token-risk';
export const MINT = '11111111111111111111111111111111';
export const MARKET = '22222222222222222222222222222222';
export const HOLDER = '44444444444444444444444444444444';
export const TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
export const target = {
  tokenId: '00000000-0000-4000-8000-000000000001',
  mint: MINT,
  marketAddress: MARKET,
  venue: 'pump_fun_bonding_curve' as const,
};
export function configuration(): TokenRiskConfig {
  return {
    policy: {
      version: 'synthetic-native-risk-tests-only',
      maxAgeMs: 60000,
      requiredComponents: ['authorities', 'holders', 'liquidity'],
      maxOwnerBps: 10000,
      maxTopTenBps: 10000,
      minimumLiquiditySol: '1',
      maxRelatedWalletPairs: 0,
    },
    providerKey: 'test-only',
    dailyRequestLimit: 2,
    dailyCreditLimit: 2,
    weights: { getTokenLargestAccounts: 1, getMultipleAccounts: 1 },
    timeoutMs: 1000,
    minRequestIntervalMs: 100,
    maxSlotSkew: 8,
    refreshMs: 1000,
    maxTargetsPerCycle: 1,
  };
}
export function facts(): Record<string, unknown> {
  return {
    contextSlot: 100,
    paperExecution: {
      marketAddress: MARKET,
      cashAsset: 'SOL',
      occurredAt: new Date().toISOString(),
      realCashReserves: '10',
      verified: true,
      criticalRisk: false,
      tradable: true,
      sellable: true,
    },
  };
}
export function rpcResult(method: string): unknown {
  if (method === 'getTokenLargestAccounts')
    return {
      context: { slot: 100 },
      value: [
        { address: '33333333333333333333333333333333' },
        { address: '55555555555555555555555555555555' },
      ],
    };
  const account = (type: string, info: unknown): unknown => ({
    owner: TOKEN_PROGRAM,
    executable: false,
    data: { parsed: { type, info } },
  });
  return {
    context: { slot: 100 },
    value: [
      account('mint', {
        supply: '1000',
        decimals: 6,
        isInitialized: true,
        mintAuthority: null,
        freezeAuthority: null,
      }),
      account('account', {
        mint: MINT,
        owner: MARKET,
        tokenAmount: { amount: '800', decimals: 6 },
      }),
      account('account', {
        mint: MINT,
        owner: HOLDER,
        tokenAmount: { amount: '200', decimals: 6 },
      }),
    ],
  };
}
