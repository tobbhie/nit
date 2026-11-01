import { ProviderId, ProviderReferenceId } from './identifiers';
import { Timestamp } from './time';

/**
 * Provenance tracks data origin and lineage
 */
export interface Provenance {
  providerId: ProviderId;
  providerReferenceId: ProviderReferenceId;
  sourceTimestamp?: Timestamp;
  retrievedAt: Timestamp;
  ingestedAt: Timestamp;
}

export function createProvenance(
  providerId: ProviderId,
  providerReferenceId: ProviderReferenceId,
  retrievedAt: Timestamp,
  ingestedAt: Timestamp,
  sourceTimestamp?: Timestamp
): Provenance {
  const provenance: Provenance = {
    providerId,
    providerReferenceId,
    retrievedAt,
    ingestedAt,
  };
  if (sourceTimestamp !== undefined) {
    provenance.sourceTimestamp = sourceTimestamp;
  }
  return provenance;
}
