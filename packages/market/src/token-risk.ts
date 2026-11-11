import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Executor } from '@nit/database-public';
import { TokenRiskGate } from '@nit/domain';
import { z } from 'zod';
import { ExecutionTarget } from './execution-facts';

const Address = z.string().regex(/^[1-9A-HJ-NP-Za-km-z]{32,44}$/);
const IntegerAmount = z.string().regex(/^(0|[1-9][0-9]{0,39})$/);
const Bps = z.number().int().min(0).max(10000);
const Component = z.enum([
  'authorities',
  'holders',
  'liquidity',
  'developer',
  'coordination',
  'clusters',
]);
export const TokenRiskPolicySchema = z
  .object({
    version: z.string().min(1).max(100),
    maxAgeMs: z.number().int().min(1000).max(3600000),
    requiredComponents: z
      .array(Component)
      .min(1)
      .max(6)
      .refine((v) => new Set(v).size === v.length),
    maxOwnerBps: Bps,
    maxTopTenBps: Bps,
    minimumLiquiditySol: z.string().regex(/^(0|[1-9][0-9]{0,15})(\.[0-9]{1,9})?$/),
    maxRelatedWalletPairs: z.number().int().min(0).max(10000),
  })
  .strict();
export const TokenRiskConfigSchema = z
  .object({
    policy: TokenRiskPolicySchema,
    providerKey: z.string().regex(/^[a-zA-Z0-9_-]{1,64}$/),
    dailyRequestLimit: z.number().int().min(1).max(1000000),
    dailyCreditLimit: z.number().int().min(1).max(1000000000),
    weights: z
      .object({
        getTokenLargestAccounts: z.number().int().min(1).max(10000),
        getMultipleAccounts: z.number().int().min(1).max(10000),
      })
      .strict(),
    timeoutMs: z.number().int().min(100).max(30000),
    minRequestIntervalMs: z.number().int().min(100).max(60000),
    maxSlotSkew: z.number().int().min(0).max(128),
    refreshMs: z.number().int().min(1000).max(3600000),
    maxTargetsPerCycle: z.number().int().min(1).max(10),
  })
  .strict();
export type TokenRiskConfig = z.infer<typeof TokenRiskConfigSchema>;
export interface RiskComponent {
  readonly status: TokenRiskGate['status'];
  readonly coverage: 'complete' | 'partial' | 'unavailable';
  readonly reasons: readonly string[];
  readonly facts: Readonly<Record<string, string | number | boolean>>;
}
export interface ConcentrationBounds {
  readonly ownerLowerBps: number;
  readonly ownerUpperBps: number;
  readonly topTenLowerBps: number;
  readonly topTenUpperBps: number;
  readonly capturedOwners: number;
  readonly uncapturedAmount: string;
}
/** Aggregate by owner, not token account. Uncaptured supply may belong to any known owner. */
export function concentrationBounds(
  supply: bigint,
  holders: readonly { owner: string; amount: bigint; vault: boolean }[]
): ConcentrationBounds {
  if (supply <= 0n || holders.some((h) => h.amount < 0n))
    throw new Error('Invalid holder quantities');
  const owners = new Map<string, bigint>();
  let excluded = 0n;
  let captured = 0n;
  for (const h of holders) {
    captured += h.amount;
    if (h.vault) excluded += h.amount;
    else owners.set(h.owner, (owners.get(h.owner) ?? 0n) + h.amount);
  }
  if (captured > supply || excluded >= supply) throw new Error('Inconsistent holder supply');
  const circulating = supply - excluded;
  const remaining = supply - captured;
  const sorted = [...owners.values()].sort((a, b) => (a > b ? -1 : a < b ? 1 : 0));
  const largest = sorted[0] ?? 0n;
  const topTen = sorted.slice(0, 10).reduce((a, b) => a + b, 0n);
  const bps = (n: bigint): number => Number((n * 10000n + circulating - 1n) / circulating);
  return {
    ownerLowerBps: bps(largest),
    ownerUpperBps: bps(largest + remaining),
    topTenLowerBps: bps(topTen),
    topTenUpperBps: bps(topTen + remaining),
    capturedOwners: owners.size,
    uncapturedAmount: remaining.toString(),
  };
}

/** Single attempts, atomic shared collector quota, bounded rollups; no raw URLs or bodies in errors. */
export function meteredRiskRpc(
  db: Executor,
  endpoint: string,
  config: TokenRiskConfig,
  fetcher: typeof fetch = fetch,
  signal?: AbortSignal
): (method: string, params: unknown[]) => Promise<unknown> {
  config = TokenRiskConfigSchema.parse(config);
  const url = new URL(endpoint);
  if (url.protocol !== 'https:' || url.username || url.password)
    throw new Error('Risk RPC requires HTTPS');
  let nextRequestAt = 0;
  const quotaHash = createHash('sha256')
    .update(
      JSON.stringify({
        requests: config.dailyRequestLimit,
        credits: config.dailyCreditLimit,
        weights: config.weights,
      })
    )
    .digest('hex');
  return async (method, params) => {
    if (signal?.aborted) throw new Error('Risk collection stopped');
    if (method !== 'getTokenLargestAccounts' && method !== 'getMultipleAccounts')
      throw new Error('Unsupported risk RPC operation');
    const weight = config.weights[method];
    const day = new Date().toISOString().slice(0, 10);
    const reserved = await db.query(
      `WITH reserved AS (
      INSERT INTO token_risk_provider_budgets(day,provider_key,requests,credits,quota_hash)
      SELECT $1::date,$2::text,1,$3::bigint,$7::text WHERE $4::bigint>=1 AND $3::bigint<=$5::bigint
      ON CONFLICT(day,provider_key) DO UPDATE SET requests=token_risk_provider_budgets.requests+1,credits=token_risk_provider_budgets.credits+$3::bigint
      WHERE token_risk_provider_budgets.quota_hash=$7::text AND token_risk_provider_budgets.requests<$4::bigint AND token_risk_provider_budgets.credits+$3::bigint<=$5::bigint RETURNING 1
    ) INSERT INTO token_risk_provider_usage(day,provider_key,operation,requests,credits)
      SELECT $1::date,$2::text,$6::text,1,$3::bigint FROM reserved
      ON CONFLICT(day,provider_key,operation) DO UPDATE SET requests=token_risk_provider_usage.requests+1,credits=token_risk_provider_usage.credits+$3::bigint
      RETURNING requests`,
      [
        day,
        config.providerKey,
        weight,
        config.dailyRequestLimit,
        config.dailyCreditLimit,
        method,
        quotaHash,
      ]
    );
    if (reserved.kind === 'failure' || reserved.value.length !== 1)
      throw new Error('Risk collector budget unavailable');
    let succeeded = false;
    let result: unknown;
    try {
      const waitMs = Math.max(0, nextRequestAt - Date.now());
      nextRequestAt = Date.now() + waitMs + config.minRequestIntervalMs;
      if (waitMs) await delay(waitMs, undefined, { signal });
      const response = await fetcher(url.href, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
        redirect: 'error',
        signal:
          signal === undefined
            ? AbortSignal.timeout(config.timeoutMs)
            : AbortSignal.any([signal, AbortSignal.timeout(config.timeoutMs)]),
      });
      if (!response.ok || response.body === null) {
        await response.body?.cancel();
        throw new Error();
      }
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let length = 0;
      try {
        for (;;) {
          const part = await reader.read();
          if (part.done) break;
          const bytes: unknown = part.value;
          if (!(bytes instanceof Uint8Array)) throw new Error();
          length += bytes.byteLength;
          if (length > 262144) throw new Error();
          chunks.push(bytes);
        }
      } finally {
        await reader.cancel();
      }
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>;
      if (
        body['jsonrpc'] !== '2.0' ||
        body['id'] !== 1 ||
        body['error'] !== undefined ||
        body['result'] === undefined
      )
        throw new Error();
      succeeded = true;
      result = body['result'];
    } catch {
      succeeded = false;
    }
    const recorded = await db.query(
      `UPDATE token_risk_provider_usage SET succeeded=succeeded+$4,failed=failed+$5
        WHERE day=$1 AND provider_key=$2 AND operation=$3`,
      [day, config.providerKey, method, succeeded ? 1 : 0, succeeded ? 0 : 1]
    );
    if (recorded.kind === 'failure') throw new Error('Risk usage recording unavailable');
    if (!succeeded) throw new Error('Risk RPC unavailable');
    return result;
  };
}
const Largest = z.object({
  context: z.object({ slot: z.number().int().nonnegative() }),
  value: z.array(z.object({ address: Address })).max(20),
});
const MintInfo = z.object({
  supply: IntegerAmount,
  decimals: z.number().int().min(0).max(18),
  isInitialized: z.literal(true),
  mintAuthority: Address.nullable(),
  freezeAuthority: Address.nullable(),
});
const HolderInfo = z.object({
  mint: Address,
  owner: Address,
  tokenAmount: z.object({ amount: IntegerAmount, decimals: z.number().int().min(0).max(18) }),
});
const ParsedAccount = z.object({
  owner: Address,
  executable: z.literal(false),
  data: z.object({ parsed: z.object({ type: z.string(), info: z.unknown() }) }),
});
const Batch = z.object({
  context: z.object({ slot: z.number().int().nonnegative() }),
  value: z.array(ParsedAccount.nullable()).min(1).max(21),
});
const tokenPrograms = new Set([
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
]);
const Execution = z.object({
  contextSlot: z.number().int().nonnegative(),
  paperExecution: z.object({
    marketAddress: Address,
    cashAsset: z.literal('SOL'),
    occurredAt: z.string().datetime(),
    realCashReserves: z.string(),
    verified: z.literal(true),
    criticalRisk: z.boolean(),
    tradable: z.boolean(),
    sellable: z.boolean(),
  }),
});
function sol(value: string): bigint {
  if (!/^(0|[1-9][0-9]*)(\.[0-9]{1,9})?$/.test(value)) throw new Error('Invalid SOL amount');
  const [whole, fraction = ''] = value.split('.');
  return BigInt(whole!) * 1000000000n + BigInt(fraction.padEnd(9, '0'));
}
export class NativeTokenRiskCollector {
  readonly policyHash: string;
  private readonly config: TokenRiskConfig;
  constructor(
    private readonly db: Executor,
    private readonly rpc: (method: string, params: unknown[]) => Promise<unknown>,
    config: TokenRiskConfig
  ) {
    this.config = TokenRiskConfigSchema.parse(config);
    this.policyHash = createHash('sha256').update(JSON.stringify(this.config.policy)).digest('hex');
  }
  async cycle(signal?: AbortSignal): Promise<{ captured: number; unavailable: number }> {
    const targets = await this.db.query<{
      token_id: string;
      address: string;
      market_address: string;
      venue: ExecutionTarget['venue'];
      raw_payload: Record<string, unknown>;
      snapshot_id: string;
    }>(
      `SELECT t.token_id,t.address,v.market_address,v.venue,s.raw_payload,s.snapshot_id
      FROM tokens t JOIN token_markets v ON v.token_id=t.token_id
      JOIN LATERAL (SELECT raw_payload,snapshot_id FROM market_snapshots s WHERE s.token_id=t.token_id AND s.venue=v.venue
        AND s.source='solana-account-facts:v1' AND s.recorded_at<=NOW()
        AND s.recorded_at>=NOW()-($4::bigint*INTERVAL '1 millisecond')
        ORDER BY s.observed_at DESC,s.recorded_at DESC LIMIT 1) s ON TRUE
      LEFT JOIN LATERAL (SELECT MAX(a.available_at) AS latest FROM token_risk_assessments a
        WHERE a.token_mint=t.address AND a.market_address=v.market_address AND a.policy_hash=$1) a ON TRUE
      WHERE t.chain='solana' AND v.active_until IS NULL AND v.active_from<=NOW() AND v.market_address IS NOT NULL
        AND v.venue IN ('pump_fun_bonding_curve','pumpswap')
        AND (a.latest IS NULL OR a.latest<NOW()-($2::bigint*INTERVAL '1 millisecond'))
      ORDER BY a.latest ASC NULLS FIRST,t.discovered_at DESC LIMIT $3`,
      [
        this.policyHash,
        this.config.refreshMs,
        this.config.maxTargetsPerCycle,
        this.config.policy.maxAgeMs,
      ]
    );
    if (targets.kind === 'failure') throw targets.error;
    let captured = 0;
    let unavailable = 0;
    for (const target of targets.value) {
      if (signal?.aborted) break;
      const gate = await this.capture(
        {
          tokenId: target.token_id,
          mint: target.address,
          marketAddress: target.market_address,
          venue: target.venue,
        },
        target.raw_payload,
        target.snapshot_id
      );
      if (gate) {
        captured++;
        if (gate.status === 'unavailable') unavailable++;
      }
    }
    return { captured, unavailable };
  }
  async capture(
    target: ExecutionTarget,
    raw: Record<string, unknown>,
    executionSnapshotId?: string
  ): Promise<TokenRiskGate | null> {
    const pinned = await this.db.query(
      'INSERT INTO token_risk_policies(policy_hash,policy) VALUES ($1,$2::jsonb) ON CONFLICT DO NOTHING',
      [this.policyHash, JSON.stringify(this.config.policy)]
    );
    if (pinned.kind === 'failure') throw pinned.error;
    const recent = await this.db.query(
      `SELECT 1 FROM token_risk_assessments WHERE token_mint=$1 AND market_address=$2 AND policy_hash=$3 AND available_at>NOW()-($4::bigint*INTERVAL '1 millisecond') LIMIT 1`,
      [target.mint, target.marketAddress, this.policyHash, this.config.refreshMs]
    );
    if (recent.kind === 'failure') throw recent.error;
    // Explicit loss of execution evidence must invalidate previous reassurance immediately.
    if (recent.value.length) return null;
    const unknown = (reason: string): RiskComponent => ({
      status: 'incomplete',
      coverage: 'partial',
      reasons: [reason],
      facts: {},
    });
    const components: Record<z.infer<typeof Component>, RiskComponent> = {
      authorities: unknown('AUTHORITIES_UNAVAILABLE'),
      holders: unknown('HOLDERS_UNAVAILABLE'),
      liquidity: unknown('LIQUIDITY_UNAVAILABLE'),
      developer: unknown('DEVELOPER_HISTORY_PARTIAL'),
      coordination: unknown('COORDINATION_WATCHLIST_ONLY'),
      clusters: {
        status: 'unavailable',
        coverage: 'unavailable',
        reasons: ['CLUSTERS_UNAVAILABLE'],
        facts: {},
      },
    };
    let observedAt = new Date();
    let collectionUnavailable = false;
    let holderSource: Record<string, unknown> | null = null;
    try {
      const execution = Execution.parse(raw);
      if (execution.paperExecution.marketAddress !== target.marketAddress)
        throw new Error('Market mismatch');
      observedAt = new Date(execution.paperExecution.occurredAt);
      const authorityRisk = execution.paperExecution.criticalRisk;
      components.authorities = {
        status: authorityRisk ? 'blocked' : 'clear',
        coverage: 'complete',
        reasons: authorityRisk ? ['MINT_OR_FREEZE_AUTHORITY'] : [],
        facts: { executionSnapshotVerified: true },
      };
      const liquid =
        sol(execution.paperExecution.realCashReserves) >=
        sol(this.config.policy.minimumLiquiditySol);
      const executable = execution.paperExecution.tradable && execution.paperExecution.sellable;
      components.liquidity = {
        status: liquid && executable ? 'clear' : 'blocked',
        coverage: 'complete',
        reasons: liquid && executable ? [] : ['INSUFFICIENT_EXECUTABLE_LIQUIDITY'],
        facts: { realCashReservesSol: execution.paperExecution.realCashReserves },
      };
      const largest = Largest.parse(
        await this.rpc('getTokenLargestAccounts', [target.mint, { commitment: 'confirmed' }])
      );
      const addresses = largest.value.map((v) => v.address);
      if (new Set(addresses).size !== addresses.length || addresses.includes(target.mint))
        throw new Error('Duplicate holder account');
      if (Math.abs(largest.context.slot - execution.contextSlot) > this.config.maxSlotSkew)
        throw new Error('Holder slot mismatch');
      const batch = Batch.parse(
        await this.rpc('getMultipleAccounts', [
          [target.mint, ...addresses],
          {
            encoding: 'jsonParsed',
            commitment: 'confirmed',
            minContextSlot: Math.max(largest.context.slot, execution.contextSlot),
          },
        ])
      );
      if (
        batch.value.length !== addresses.length + 1 ||
        batch.context.slot < Math.max(largest.context.slot, execution.contextSlot) ||
        batch.context.slot - largest.context.slot > this.config.maxSlotSkew
      )
        throw new Error('Holder batch mismatch');
      const mintAccount = batch.value[0];
      if (
        !mintAccount ||
        !tokenPrograms.has(mintAccount.owner) ||
        mintAccount.data.parsed.type !== 'mint'
      )
        throw new Error('Invalid mint account');
      const mint = MintInfo.parse(mintAccount.data.parsed.info);
      if (mint.mintAuthority !== null || mint.freezeAuthority !== null)
        components.authorities = {
          status: 'blocked',
          coverage: 'complete',
          reasons: ['MINT_OR_FREEZE_AUTHORITY'],
          facts: {},
        };
      const holders = batch.value.slice(1).map((account, index) => {
        if (
          !account ||
          account.owner !== mintAccount.owner ||
          account.data.parsed.type !== 'account'
        )
          throw new Error('Invalid token account');
        const info = HolderInfo.parse(account.data.parsed.info);
        if (info.mint !== target.mint || info.tokenAmount.decimals !== mint.decimals)
          throw new Error('Holder mint mismatch');
        // Market PDA ownership is verified against the already-validated current curve/pool.
        return {
          address: addresses[index],
          owner: info.owner,
          amount: BigInt(info.tokenAmount.amount),
          vault: info.owner === target.marketAddress,
        };
      });
      const bounds = concentrationBounds(BigInt(mint.supply), holders);
      holderSource = {
        largestAccountsSlot: largest.context.slot,
        accountsSlot: batch.context.slot,
        supplyAtomic: mint.supply,
        decimals: mint.decimals,
        tokenProgram: mintAccount.owner,
        accounts: holders.map((h) => ({ ...h, amount: h.amount.toString() })),
      };
      const blocked =
        bounds.ownerLowerBps > this.config.policy.maxOwnerBps ||
        bounds.topTenLowerBps > this.config.policy.maxTopTenBps;
      const clear =
        bounds.ownerUpperBps <= this.config.policy.maxOwnerBps &&
        bounds.topTenUpperBps <= this.config.policy.maxTopTenBps;
      components.holders = {
        status: blocked ? 'blocked' : clear ? 'clear' : 'incomplete',
        coverage: bounds.uncapturedAmount === '0' ? 'complete' : 'partial',
        reasons: blocked ? ['HOLDER_CONCENTRATION'] : clear ? [] : ['HOLDER_COVERAGE_INCOMPLETE'],
        facts: { ...bounds, excludedVerifiedMarketVaults: holders.filter((h) => h.vault).length },
      };
      const history = await this.db.query<{ launches: string; creator_known: boolean }>(
        `SELECT (t.creator_wallet IS NOT NULL) AS creator_known,
        (SELECT COUNT(*)::text FROM tokens p WHERE p.chain=t.chain AND p.creator_wallet=t.creator_wallet AND p.discovered_at<=NOW()) AS launches
        FROM tokens t WHERE t.chain='solana' AND t.address=$1`,
        [target.mint]
      );
      if (history.kind === 'failure') throw history.error;
      components.developer = {
        ...unknown('DEVELOPER_HISTORY_PARTIAL'),
        facts: {
          observedLaunches: history.value[0]?.launches ?? '0',
          creatorKnown: history.value[0]?.creator_known ?? false,
          reputationAvailable: false,
        },
      };
      const currentOwners = holders.filter((h) => !h.vault && h.amount > 0n).map((h) => h.owner);
      const related = await this.db.query<{ pairs: string }>(
        `SELECT COUNT(DISTINCT (r.wallet_a,r.wallet_b))::text AS pairs
        FROM wallet_relation_evidence r WHERE r.observed_at<=NOW()
        AND r.wallet_a=ANY($1::text[]) AND r.wallet_b=ANY($1::text[])`,
        [currentOwners]
      );
      if (related.kind === 'failure') throw related.error;
      const pairs = related.value[0]?.pairs ?? '0';
      components.coordination = {
        status:
          BigInt(pairs) > BigInt(this.config.policy.maxRelatedWalletPairs)
            ? 'blocked'
            : 'incomplete',
        coverage: 'partial',
        reasons: ['COORDINATION_WATCHLIST_ONLY'],
        facts: { observedRelatedPairs: pairs, bundleDetectionAvailable: false },
      };
    } catch {
      collectionUnavailable = true;
    }
    const availableAt = new Date();
    if (!Number.isFinite(observedAt.getTime()) || observedAt > availableAt) {
      observedAt = availableAt;
      collectionUnavailable = true;
    }
    const required = this.config.policy.requiredComponents.map((c) => components[c]);
    const blocked = Object.values(components).some((c) => c.status === 'blocked');
    const fresh = availableAt.getTime() - observedAt.getTime() < this.config.policy.maxAgeMs;
    const status = blocked
      ? 'blocked'
      : collectionUnavailable
        ? 'unavailable'
        : !fresh || required.some((c) => c.status !== 'clear')
          ? 'incomplete'
          : 'clear';
    const gate: TokenRiskGate = {
      assessmentId: randomUUID(),
      tokenMint: target.mint,
      marketAddress: target.marketAddress,
      policyHash: this.policyHash,
      status,
      observedAt: observedAt.toISOString(),
      availableAt: availableAt.toISOString(),
      expiresAt: new Date(observedAt.getTime() + this.config.policy.maxAgeMs).toISOString(),
      reasonCodes: [
        ...new Set([
          ...Object.values(components).flatMap((c) => c.reasons),
          ...(collectionUnavailable ? ['RISK_COLLECTION_UNAVAILABLE'] : []),
          ...(!fresh ? ['RISK_EVIDENCE_STALE'] : []),
        ]),
      ],
    };
    const inserted = await this.db.query(
      `INSERT INTO token_risk_assessments(assessment_id,token_mint,market_address,policy_hash,observed_at,available_at,gate,evidence)
      VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb)`,
      [
        gate.assessmentId,
        target.mint,
        target.marketAddress,
        this.policyHash,
        gate.observedAt,
        gate.availableAt,
        JSON.stringify(gate),
        JSON.stringify({
          schemaVersion: 1,
          executionSnapshotId: executionSnapshotId ?? null,
          holderSource,
          components,
        }),
      ]
    );
    if (inserted.kind === 'failure') throw inserted.error;
    return gate;
  }
}
