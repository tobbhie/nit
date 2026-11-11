/** Evidence gate, never a probability of profit or a guarantee of rug safety. */
export interface TokenRiskGate {
  readonly assessmentId: string;
  readonly policyHash: string;
  readonly tokenMint: string;
  readonly marketAddress: string;
  readonly status: 'clear' | 'blocked' | 'incomplete' | 'unavailable';
  readonly reasonCodes: readonly string[];
  readonly observedAt: string;
  readonly availableAt: string;
  readonly expiresAt: string;
}

/** Apply only to new entries; unavailable optional research must never prevent a sell. */
export function tokenRiskEntryRejection(
  raw: unknown,
  requiredPolicyHash: string | undefined,
  tokenMint: string,
  marketAddress: string | undefined,
  now: Date
): string | null {
  if (requiredPolicyHash === undefined) return null;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw))
    return 'TOKEN_RISK_UNAVAILABLE';
  const gate = raw as Record<string, unknown>;
  if (gate['policyHash'] !== requiredPolicyHash) return 'TOKEN_RISK_POLICY_MISMATCH';
  if (gate['tokenMint'] !== tokenMint || !marketAddress || gate['marketAddress'] !== marketAddress)
    return 'TOKEN_RISK_MARKET_MISMATCH';
  const dates = ['observedAt', 'availableAt', 'expiresAt'].map((k) =>
    typeof gate[k] === 'string' ? Date.parse(gate[k]) : NaN
  );
  if (
    dates.some((d) => !Number.isFinite(d)) ||
    !Number.isFinite(now.getTime()) ||
    dates[0]! > dates[1]! ||
    dates[1]! > now.getTime() ||
    dates[2]! <= now.getTime() ||
    dates[2]! <= dates[1]!
  )
    return 'TOKEN_RISK_STALE_OR_FUTURE';
  if (
    typeof gate['assessmentId'] !== 'string' ||
    !/^[0-9a-f-]{36}$/i.test(gate['assessmentId']) ||
    !Array.isArray(gate['reasonCodes']) ||
    gate['reasonCodes'].length > 32 ||
    gate['reasonCodes'].some((r: unknown) => typeof r !== 'string' || !/^[A-Z0-9_]{1,100}$/.test(r))
  )
    return 'TOKEN_RISK_INVALID';
  return gate['status'] === 'clear'
    ? null
    : gate['status'] === 'blocked'
      ? 'TOKEN_RISK_BLOCKED'
      : 'TOKEN_RISK_INCOMPLETE';
}
