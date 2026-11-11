import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from '@nit/database-public/testing';
import { NativeTokenRiskCollector, meteredRiskRpc } from './token-risk';
import { configuration, facts, rpcResult, target } from './token-risk.fixtures';
describe.skipIf(!hasTestDatabase())(
  'native token risk persistence and bounded provider usage',
  () => {
    let harness: TestDatabase;
    beforeAll(async () => {
      harness = await createTestDatabase('native_risk');
    }, 30000);
    afterAll(async () => harness.teardown());
    it('pins immutable limited-coverage evidence and reuses a fresh assessment without more calls', async () => {
      const config = configuration();
      config.policy.version = randomUUID();
      const rpc = vi.fn((method: string) => Promise.resolve(rpcResult(method)));
      const collector = new NativeTokenRiskCollector(harness.db, rpc, config);
      const gate = await collector.capture(target, facts());
      expect(gate?.status).toBe('clear');
      expect(gate?.reasonCodes).toContain('CLUSTERS_UNAVAILABLE');
      expect(await collector.capture(target, facts())).toBeNull();
      expect(rpc).toHaveBeenCalledTimes(2);
      const mutation = await harness.db.query(
        "UPDATE token_risk_assessments SET evidence='{}' WHERE assessment_id=$1",
        [gate!.assessmentId]
      );
      expect(mutation.kind).toBe('failure');
    });
    it('blocks known concentration and refuses to clear absent developer or cluster coverage', async () => {
      for (const requirement of ['concentration', 'developer', 'clusters'] as const) {
        const config = configuration();
        config.policy.version = randomUUID();
        if (requirement === 'concentration') config.policy.maxOwnerBps = 1000;
        else config.policy.requiredComponents.push(requirement);
        const gate = await new NativeTokenRiskCollector(
          harness.db,
          (method) => Promise.resolve(rpcResult(method)),
          config
        ).capture(target, facts());
        expect(gate?.status).toBe(requirement === 'concentration' ? 'blocked' : 'incomplete');
      }
    });
    it('collects bounded fresh targets and retains source snapshot and holder inputs', async () => {
      const config = configuration();
      config.policy.version = randomUUID();
      const tokenId = randomUUID();
      const snapshotId = randomUUID();
      const token = await harness.db.query(
        `INSERT INTO tokens(token_id,chain,address,ticker,name,launch_time,launch_source,raw_payload)
      VALUES ($1,'solana',$2,'TEST','Synthetic risk token',NOW(),'pump_fun','{}')`,
        [tokenId, target.mint]
      );
      expect(token.kind).toBe('success');
      const market = await harness.db.query(
        `INSERT INTO token_markets(token_market_id,token_id,venue,active_from,source_event_kind,source_event_key,market_address)
      VALUES ($1::uuid,$2,'pump_fun_bonding_curve',NOW(),'legacy',$1::text,$3)`,
        [randomUUID(), tokenId, target.marketAddress]
      );
      expect(market.kind).toBe('success');
      const snapshot = await harness.db.query(
        `INSERT INTO market_snapshots(snapshot_id,token_id,venue,observed_at,source,raw_payload)
      VALUES ($1,$2,'pump_fun_bonding_curve',NOW(),'solana-account-facts:v1',$3::jsonb)`,
        [snapshotId, tokenId, JSON.stringify(facts())]
      );
      expect(snapshot.kind).toBe('success');
      const collector = new NativeTokenRiskCollector(
        harness.db,
        (method) => Promise.resolve(rpcResult(method)),
        config
      );
      expect(await collector.cycle()).toEqual({ captured: 1, unavailable: 0 });
      expect(await collector.cycle()).toEqual({ captured: 0, unavailable: 0 });
      const source = await harness.db.query<{ source_id: string; accounts: number }>(
        `SELECT evidence->>'executionSnapshotId' AS source_id,
      jsonb_array_length(evidence->'holderSource'->'accounts') AS accounts FROM token_risk_assessments WHERE policy_hash=$1`,
        [collector.policyHash]
      );
      expect(source.kind === 'success' ? source.value : []).toEqual([
        { source_id: snapshotId, accounts: 2 },
      ]);
    });
    it('persists a newer unavailable reading instead of falling back to an earlier clear assessment', async () => {
      const config = configuration();
      config.policy.version = randomUUID();
      const good = await new NativeTokenRiskCollector(
        harness.db,
        (method) => Promise.resolve(rpcResult(method)),
        config
      ).capture(target, facts());
      expect(good?.status).toBe('clear');
      await new Promise((resolve) => setTimeout(resolve, 1050));
      const bad = await new NativeTokenRiskCollector(
        harness.db,
        () => Promise.reject(new Error('private provider body')),
        config
      ).capture(target, facts());
      expect(bad?.status).toBe('unavailable');
      expect(JSON.stringify(bad)).not.toContain('private provider body');
    });
    it('bounds concurrent reservations across adapters sharing a provider bucket', async () => {
      const config = configuration();
      config.providerKey = randomUUID();
      const fetcher = vi
        .fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
        .mockImplementation(() =>
          Promise.resolve(
            new Response(
              JSON.stringify({
                jsonrpc: '2.0',
                id: 1,
                result: rpcResult('getTokenLargestAccounts'),
              })
            )
          )
        );
      const adapters = Array.from({ length: 6 }, () =>
        meteredRiskRpc(harness.db, 'https://rpc.invalid', config, fetcher)
      );
      const results = await Promise.allSettled(
        adapters.map((rpc) => rpc('getTokenLargestAccounts', []))
      );
      expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(2);
      expect(fetcher).toHaveBeenCalledTimes(2);
      const usage = await harness.db.query<{
        requests: string;
        credits: string;
        succeeded: string;
      }>('SELECT requests,credits,succeeded FROM token_risk_provider_usage WHERE provider_key=$1', [
        config.providerKey,
      ]);
      expect(usage.kind === 'success' ? usage.value : []).toEqual([
        { requests: '2', credits: '2', succeeded: '2' },
      ]);
    });
    it('charges a failed attempt once, never retries it and excludes raw provider errors', async () => {
      const config = configuration();
      config.providerKey = randomUUID();
      const fetcher = vi
        .fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
        .mockRejectedValue(new Error('secret URL'));
      await expect(
        meteredRiskRpc(
          harness.db,
          'https://rpc.invalid',
          config,
          fetcher
        )('getMultipleAccounts', [])
      ).rejects.toThrow('Risk RPC unavailable');
      expect(fetcher).toHaveBeenCalledTimes(1);
      const result = await harness.db.query<{ failed: string; credits: string }>(
        'SELECT failed,credits FROM token_risk_provider_usage WHERE provider_key=$1',
        [config.providerKey]
      );
      expect(result.kind === 'success' ? result.value : []).toEqual([
        { failed: '1', credits: '1' },
      ]);
    });
    it('refuses quota configuration drift or aborted requests without purchasing another read', async () => {
      const config = configuration();
      config.providerKey = randomUUID();
      const fetcher = vi
        .fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
        .mockImplementation(() =>
          Promise.resolve(new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: {} })))
        );
      await meteredRiskRpc(
        harness.db,
        'https://rpc.invalid',
        config,
        fetcher
      )('getMultipleAccounts', []);
      await expect(
        meteredRiskRpc(
          harness.db,
          'https://rpc.invalid',
          { ...config, dailyRequestLimit: 10 },
          fetcher
        )('getMultipleAccounts', [])
      ).rejects.toThrow('budget unavailable');
      await expect(
        meteredRiskRpc(
          harness.db,
          'https://rpc.invalid',
          config,
          fetcher,
          AbortSignal.abort()
        )('getMultipleAccounts', [])
      ).rejects.toThrow('stopped');
      expect(fetcher).toHaveBeenCalledTimes(1);
    });
  }
);
