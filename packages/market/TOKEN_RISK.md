# Native token risk evidence and entry gates

This layer records source evidence and applies a pinned deterministic policy. A
`clear` result means the configured required checks passed with bounded evidence;
it is not a guarantee of safety, developer honesty or profitable execution.
There is no composite profit probability or invented reputation score.

## Current coverage

The collector reuses approved native execution facts for supported account
layouts, mint/freeze authority flags and real SOL liquidity. It then requests
`getTokenLargestAccounts` and one `getMultipleAccounts` read with parsed mint and
token accounts from the same configured Solana RPC endpoint. Only the largest
20 token accounts are returned by the
[RPC method](https://solana.com/docs/rpc/http/gettokenlargestaccounts).
Accounts are verified against the token program, mint and decimals, grouped by
owner, and checked for duplicate accounts and incompatible context slots.
Assessments retain the execution snapshot ID and bounded holder inputs (account
and owner identities, atomic balances, supply and RPC slots) for auditing without
duplicating full account blobs.
Verified current market-PDA-owned vault balances are excluded. Arbitrary burn,
exchange or developer addresses are not excluded by label.

Largest-owner and top-ten-owner percentages are computed with integer arithmetic
against supply excluding verified captured market vaults. All uncaptured supply
remains in a conservative upper bound: it may belong to existing large owners.
A policy violation established by a lower bound blocks; an upper bound below the
limit can clear; an ambiguous bound is incomplete. These bounds do not resolve
common beneficial ownership across different wallet addresses.

Developer history reports only locally discovered launches and creator presence.
It cannot establish complete launch history, previous rugs or reputation, and
never clears a required developer check. Coordination reports existing links
between currently sampled holder owners; absence of links remains incomplete.
Shared funders/same-slot observations do not prove a transaction bundle or intent.
Native collection does not implement bundle attribution or Bubblemaps. Cluster
coverage is explicitly unavailable. Requiring these components keeps entry
qualification incomplete until a suitable evidence source is implemented.

## Configuration and activation

Collection is opt-in through a private JSON file at `NIT_TOKEN_RISK_CONFIG_PATH`
when starting the market worker. `TokenRiskConfigSchema` specifies all policy and
resource limits; no tuned thresholds or live policy are shipped. The collector
runs separately from execution-fact refresh and never modifies source snapshots.
It refreshes at most `maxTargetsPerCycle` assessments per cycle, using
`refreshMs` for reuse. Current execution safety is still checked independently
on entry and settlement even while a risk assessment is cached.

Policy fields: version, maximum evidence age, required components, maximum
single-owner/top-ten-owner basis points, minimum real SOL liquidity and maximum
observed related holder pairs. The canonical parsed policy is SHA-256 pinned.
Native limitations are retained even when they are not policy-required. An
operator must deliberately choose which incomplete checks block entries.

To enforce the gate, pin the collector's `policyHash` as `tokenRiskPolicyHash`
in **both** resolver policy and each new strategy's R01 configuration. Recreate
and pin the strategy snapshot normally; never mutate a running strategy version.
Existing historical strategies without the field retain their original semantics
and do not acquire comprehensive risk coverage automatically.

Approved market feeds attach the latest point-in-time assessment for the exact
mint and market address. Missing, stale, future, wrong-policy, wrong-market,
blocked and incomplete assessments prevent opted-in new entries. Buy planning
and delayed settlement recheck the gate. A newer unavailable result supersedes
older clearance. Sells and protection do not require optional risk collection.
The captured market snapshot includes the assessment identity for replay.
Combined market/risk observations receive a deterministic new snapshot identity
and availability no earlier than either source, preventing retroactive risk
overlays from mutating a previously pinned quote or leaking future evidence.

Migration 0028 adds immutable policies/assessments and indexed point-in-time
lookups. Observed/available timestamps are explicit; future evidence is excluded.
Do not delete these assessments while referenced by paper/research evidence.

## Usage and cost controls

`meteredRiskRpc` supports only the two native holder methods. It atomically reserves
requests and operator-declared weighted credits before each network attempt, with
shared daily buckets for risk collectors using the same `providerKey`.
Limits and weights are pinned within that provider/day bucket; inconsistent
configurations are rejected rather than silently increasing the shared quota.
Per-method rollups record requests, declared credits, successes and failures. Pending or
unknown attempts retain their reservation; no credit is refunded automatically.
Limits, request intervals, input count, response size and timeouts are bounded.
There are no automatic retries or new dependencies.

This quota covers **risk collection only**; it is not yet the audit's shared
provider-wide governor for market, wallet and protection traffic. The weights
are estimates, not invoices. Budget conservatively alongside existing workers;
compare actual provider credits separately. No per-mint metric label warehouse is
created. Daily rollups grow by provider/operation/day, while detailed immutable
assessments remain subject to the application's evidence retention plan.

RPC data exposure is the candidate mint and up to 20 public token-account
addresses. No provider credentials, RPC URLs or provider error bodies are stored
in assessments or logs. No LLM, X, Bubblemaps or paid third-party risk calls are
made by this collector. External provider access and policy activation remain
operator configuration; adding the feature does not start a new live worker.
