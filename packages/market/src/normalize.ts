import {
  Chain,
  LaunchSource,
  Result,
  Timestamp,
  TokenId,
  ValidationError,
  failure,
  generateTokenId,
  success,
} from '@nit/domain';
import { SourceTokenLaunch } from './contracts';

/**
 * A launch ready to insert. Close to the canonical `Token`, plus the raw payload,
 * which only the repository needs.
 */
export interface NormalizedTokenLaunch {
  tokenId: TokenId;
  chain: Chain;
  address: string;
  ticker: string;
  name: string;
  launchTime: Timestamp;
  launchSource: LaunchSource;
  discoveredAt: Timestamp;
  creatorWallet?: string;
  description?: string;
  artworkUrl?: string;
  rawPayload: Record<string, unknown>;
}

const CHAINS = new Set<string>(Object.values(Chain));
const LAUNCH_SOURCES = new Set<string>(Object.values(LaunchSource));

function requireText(value: string, field: string): Result<string, ValidationError> {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return failure(new ValidationError(`Token ${field} is empty`, { field }));
  }
  return success(trimmed);
}

/** Empty and whitespace-only text is absent text, not text. */
function optionalText(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

/**
 * Turn a launchpad payload into a persistable token, or reject it.
 *
 * Validating at the boundary rather than trusting the adapter matters because an
 * adapter is the least trustworthy code in the pipeline: it mirrors a third-party
 * response shape that can change without notice, and everything downstream is
 * written against the canonical model on the assumption that it holds.
 *
 * Note what is deliberately *not* normalized: the address is trimmed but never
 * case-folded. Solana mint addresses are base58, where case is significant, so
 * lowercasing one would produce an address that does not exist while looking
 * entirely plausible. Ticker and name are matched case-insensitively instead —
 * through the `LOWER(...)` indexes on `tokens` — which puts the case-insensitivity
 * where it belongs, in the query, and leaves the stored value as the launchpad
 * published it.
 *
 * Pure by design. No identifiers come from the launchpad, no clock is read from
 * the environment, and nothing is written, so a fixture and an expected output
 * are enough to test it.
 */
export function normalizeSourceLaunch(
  chain: Chain,
  launchSource: LaunchSource,
  launch: SourceTokenLaunch,
  discoveredAt: Timestamp
): Result<NormalizedTokenLaunch, ValidationError> {
  // Typed as enums, but an adapter's output crosses a JSON boundary on the way
  // here through a job payload, so the types are claims rather than guarantees —
  // and an unrecognised chain would fork token identity rather than fail loudly.
  if (!CHAINS.has(chain)) {
    return failure(new ValidationError('Unknown chain', { chain }));
  }
  if (!LAUNCH_SOURCES.has(launchSource)) {
    return failure(new ValidationError('Unknown launch source', { launchSource }));
  }

  const address = requireText(launch.address, 'address');
  if (address.kind === 'failure') {
    return address;
  }

  const ticker = requireText(launch.ticker, 'ticker');
  if (ticker.kind === 'failure') {
    return ticker;
  }

  const name = requireText(launch.name, 'name');
  if (name.kind === 'failure') {
    return name;
  }

  if (!Number.isFinite(launch.launchTime.getTime())) {
    return failure(new ValidationError('Token launchTime is not a valid date', {
      address: address.value,
    }));
  }

  const normalized: NormalizedTokenLaunch = {
    tokenId: generateTokenId(),
    chain,
    address: address.value,
    ticker: ticker.value,
    name: name.value,
    launchTime: launch.launchTime,
    launchSource,
    discoveredAt,
    rawPayload: launch.raw,
  };

  // Absent rather than empty, because the enrichment workers branch on absence:
  // an artwork URL of '' would enqueue a download of nothing.
  const creatorWallet = optionalText(launch.creatorWallet);
  if (creatorWallet !== undefined) {
    normalized.creatorWallet = creatorWallet;
  }
  const description = optionalText(launch.description);
  if (description !== undefined) {
    normalized.description = description;
  }
  const artworkUrl = optionalText(launch.artworkUrl);
  if (artworkUrl !== undefined) {
    normalized.artworkUrl = artworkUrl;
  }

  return success(normalized);
}
