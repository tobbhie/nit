import { Chain, LaunchSource, Timestamp, TokenId } from '@nit/domain';

/**
 * The canonical token, per TIS ch.9 §"Canonical Market Object".
 *
 * Everything downstream — correlation, alerting, backtesting — consumes this
 * rather than a launchpad payload, so adding a launchpad is an adapter and
 * nothing else.
 *
 * `tokenId` is assigned by this system and never changes (ch.9's immutable token
 * IDs invariant), which is what lets evidence accumulate against a token across
 * venues. The launchpad's own identity for the asset is `(chain, address)`.
 *
 * The optional fields are optional because a launch feed frequently omits them,
 * not because they are unimportant: a token with no `artworkUrl` is one the
 * artwork worker must skip rather than fail on, and a token with no
 * `creatorWallet` cannot have a creator profile at all.
 */
export interface Token {
  tokenId: TokenId;
  chain: Chain;
  address: string;
  ticker: string;
  name: string;
  launchTime: Timestamp;
  launchSource: LaunchSource;
  /** When this system first saw the launch, which is not when it happened. */
  discoveredAt: Timestamp;
  creatorWallet?: string;
  description?: string;
  artworkUrl?: string;
}
