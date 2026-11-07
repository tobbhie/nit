import { Executor } from '@nit/database-public';
import { generateMarketSnapshotId } from '@nit/domain';
import {
  EXECUTION_FACTS_SOURCE,
  ExecutionFactsConfig,
  ExecutionTarget,
  SolanaExecutionFactsSource,
} from './execution-facts';

/** Bounded, oldest-first refresh; held tokens and pending orders precede discovery. */
export class ExecutionFactsRefresher {
  constructor(
    private readonly db: Executor,
    private readonly source: SolanaExecutionFactsSource,
    private readonly config: ExecutionFactsConfig
  ) {}

  async cycle(): Promise<{ captured: number; unavailable: number }> {
    const targets = await this.db.query<{
      token_id: string;
      address: string;
      market_address: string;
      venue: ExecutionTarget['venue'];
    }>(
      `WITH protected AS (
        SELECT p.token_mint FROM positions p JOIN paper_portfolios pf ON pf.portfolio_id=p.portfolio_id
          JOIN strategy_runs r ON r.run_id=pf.run_id WHERE p.open_quantity>0 AND r.mode='forward_paper'
        UNION SELECT o.token_mint FROM orders o JOIN paper_execution_plans ep ON ep.order_id=o.order_id
          JOIN strategy_runs r ON r.run_id=o.run_id WHERE r.mode='forward_paper'
          AND NOT EXISTS (SELECT 1 FROM paper_execution_results er WHERE er.order_id=o.order_id)
      ) SELECT t.token_id,t.address,v.market_address,v.venue
      FROM tokens t JOIN token_markets v ON v.token_id=t.token_id
      LEFT JOIN protected held ON held.token_mint=t.address
      LEFT JOIN LATERAL (SELECT MAX(s.observed_at) AS latest FROM market_snapshots s
        WHERE s.token_id=t.token_id AND s.venue=v.venue AND s.source=$1) last ON TRUE
      WHERE t.chain='solana' AND v.active_until IS NULL AND v.active_from <= NOW()
        AND v.market_address IS NOT NULL AND v.venue IN ('pump_fun_bonding_curve','pumpswap')
        AND (t.discovered_at >= NOW() - ($2::bigint * INTERVAL '1 millisecond')
          OR held.token_mint IS NOT NULL)
      ORDER BY (held.token_mint IS NOT NULL) DESC,
        last.latest ASC NULLS FIRST,t.token_id LIMIT $3`,
      [EXECUTION_FACTS_SOURCE, this.config.candidateMaxAgeMs, this.config.maxTargets]
    );
    if (targets.kind === 'failure') throw targets.error;
    let captured = 0;
    let unavailable = 0;
    for (const target of targets.value) {
      let raw: Record<string, unknown>;
      let observedAt: Date;
      try {
        const result = await this.source.capture({
          tokenId: target.token_id,
          mint: target.address,
          marketAddress: target.market_address,
          venue: target.venue,
        });
        raw = result.raw;
        observedAt = result.observedAt;
        captured += 1;
      } catch {
        // A newer unavailable reading invalidates earlier reassuring evidence. Never persist
        // provider error text: URLs may contain an API key and response bodies are untrusted.
        raw = { schemaVersion: 1, status: 'unavailable', marketAddress: target.market_address };
        observedAt = new Date();
        unavailable += 1;
      }
      const inserted = await this.db.query(
        `INSERT INTO market_snapshots
        (snapshot_id,token_id,venue,observed_at,source,raw_payload)
        VALUES ($1,$2,$3,$4,$5,$6::jsonb)
        ON CONFLICT (token_id,venue,source,observed_at) DO NOTHING`,
        [
          generateMarketSnapshotId(),
          target.token_id,
          target.venue,
          observedAt,
          EXECUTION_FACTS_SOURCE,
          JSON.stringify(raw),
        ]
      );
      if (inserted.kind === 'failure') throw inserted.error;
    }
    return { captured, unavailable };
  }
}
