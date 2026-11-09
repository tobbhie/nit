import { Executor } from '@nit/database-public';
import { evidenceHash } from '@nit/signals';
import { paperRows } from './paper-store';

export interface MarketDiscoveryEvidence {
  readonly snapshot: {
    readonly tokenId: string;
    readonly mint: string;
    readonly name: string;
    readonly ticker: string;
    readonly launchSource: string;
    readonly launchTime: string;
    readonly discoveredAt: string;
    readonly rawHash: string;
  };
  readonly hash: string;
}
/** Independent canonical knowledge; it contains no narrative qualification or future token data. */
export async function marketDiscoveryEvidence(
  db: Executor,
  mint: string,
  asOf: Date
): Promise<MarketDiscoveryEvidence | null> {
  const found = await paperRows<{
    token_id: string;
    address: string;
    name: string;
    ticker: string;
    launch_source: string;
    launch_time: Date;
    discovered_at: Date;
    raw_payload: unknown;
  }>(
    db,
    "SELECT token_id,address,name,ticker,launch_source,launch_time,discovered_at,raw_payload FROM tokens WHERE chain='solana' AND address=$1 AND discovered_at<=$2",
    [mint, asOf]
  );
  const token = found[0];
  if (token === undefined) return null;
  const snapshot = {
    tokenId: token.token_id,
    mint: token.address,
    name: token.name,
    ticker: token.ticker,
    launchSource: token.launch_source,
    launchTime: token.launch_time.toISOString(),
    discoveredAt: token.discovered_at.toISOString(),
    rawHash: evidenceHash(token.raw_payload),
  };
  return { snapshot, hash: evidenceHash(snapshot) };
}
