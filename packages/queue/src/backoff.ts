export interface BackoffPolicy {
  baseMs: number;
  maxMs: number;
  factor?: number;
}

/**
 * Deterministic bounded exponential backoff.
 * Deterministic on purpose: retry timing must be reproducible in replay tests.
 */
export function computeBackoffMs(attempt: number, policy: BackoffPolicy): number {
  const factor = policy.factor ?? 2;
  const safeAttempt = Math.max(1, Math.floor(attempt));
  const raw = policy.baseMs * Math.pow(factor, safeAttempt - 1);
  return Math.min(Math.floor(raw), policy.maxMs);
}

export function nextAvailableAt(
  from: Date,
  attempt: number,
  policy: BackoffPolicy
): Date {
  return new Date(from.getTime() + computeBackoffMs(attempt, policy));
}
