# NIT Agent Workspace

The development workspace for NIT (Narrative Intelligence Terminal). It compiles
and tests without any live credential.

The boundary here is **credential blast radius**, not code secrecy: NIT's source,
architecture, schema and scoring logic are built in this repository, while real
secrets, other projects, personal wallets, production deployment and the main
GitHub key are inaccessible — enforced by the separate operating-system user this
workspace runs as. Tuned threshold and weight values, the real watchlist and real
backtest datasets stay in `private/`.

## Before use

```powershell
pnpm check:boundary
pnpm scan:export
```

`check:boundary` fails on a forbidden path (`private`, `EDS`, `TIS`,
`config/production`, `docs/private`), on an `.env` outside the repository root,
and on any `.pem`, `.key` or `.p12`. A root `.env` holding NIT-specific burner
development credentials is expected and allowed. `scan:export` looks for generic
secret signatures in repository content.

If either fails, resolve it before sharing this workspace.

See [scope rules](docs/agent-router-scope.md), [agent instructions](AGENTS.md),
[architecture](docs/architecture-public.md), and the
[requirements pack](docs/specs/README.md).

## Solana market intake

`@nit/app-market-worker` is the production entrypoint for Pump token creation
and PumpSwap graduation intake. It shares one confirmed WebSocket subscription
per configured program, fetches the complete confirmed transaction over HTTP,
decodes only the versioned Pump events owned by `@nit/market`, stores the exact
transaction/instruction/inner-instruction occurrence, and applies the canonical
token-market lifecycle. Durable finalized-slot reconciliation runs in parallel
to repair disconnects and provider gaps before advancing its watermark.

The worker is disabled by default. Enable it with
`NIT_SOLANA_MARKET_ENABLED=true` and supply both RPC endpoints and both program
addresses through deployment configuration. Keep provider API keys out of
committed files; query-string credentials are accepted because several RPC
providers require them, while URL user-info credentials are rejected. Endpoint
values are never logged by the worker.

The only new external runtime dependency is `ws`. It sends JSON-RPC
`logsSubscribe` requests and receives public Solana transaction signatures and
logs from the configured RPC provider; it receives no wallet key, signer,
portfolio, order, or private intelligence data.

## Wallet intelligence

`@nit/app-wallet-worker` is an independently runnable Solana wallet signal
source. It attributes buys and sells from complete signer-owned balance deltas,
rejects ambiguous multi-asset transactions, persists exact source timestamps,
detects common-funder and same-slot relationship evidence, discounts linked
wallet groups from convergence, and publishes canonical wallet buy, sell, risk,
and convergence signals. It has no signing or order capability.

The worker is disabled by default. Its enable flag, RPC endpoints, watched
addresses, quote assets, allowed swap programs, convergence policy, and
reconciliation controls are named in `env.example`. Keep the real watchlist and
tuned convergence values in private runtime configuration. One physical
WebSocket carries all configured wallet subscriptions; finalized address-history
scans repair reconnect gaps. A cursor advances only after selected transactions
are fetched, parsed, persisted, and published, so restart recovery safely repeats
the configured overlap window.

Wallet RPC reads request parsed transactions through version 1, matching market
intake. Transient transport, overload, interrupted response bodies and delayed
transaction indexing receive bounded retries; permanent authorization failures
remain failures. Provider error text is not forwarded into application errors.
The shared socket checks subscription acknowledgements and bounds pending work
to 128 notifications. In-flight/recent duplicate signatures share one read;
overload or a failed handler reconnects and relies on finalized reconciliation.
`Wallet health` logs expose subscription coverage, backlog/errors and how many
configured wallets have persisted cursors, including during the initial scan.
Reconciliation gives each wallet a bounded page/transaction budget per cycle,
retains unfinished work between cycles, and processes its selected transactions
oldest first. A busy wallet yields to the remaining watchlist. Its durable cursor
advances only after the entire selected window completes; a restart re-reads
unfinished work from that cursor. The pending-work cap fails closed rather than
skipping history. A completed cycle can still report deferred wallets warming up.

The existing `ws` dependency is reused; no additional network dependency is
introduced. The configured RPC provider receives watched public addresses,
transaction signatures, and parsed public transactions solely for attribution
and reconciliation. It receives no private key, signer, order, portfolio, or
private intelligence payload. To stop new wallet intake without affecting NIT or
market discovery, disable and stop only `@nit/app-wallet-worker`.

## Standalone paper trader

`@nit/app-paper-worker` runs `create`, `tick`, `serve`, `status`, and `rebuild`
commands from the JSON file named by `NIT_PAPER_COMMAND_PATH`:

```powershell
$env:NIT_PAPER_COMMAND_PATH = 'path-to-private-paper-command.json'
pnpm --filter @nit/app-paper-worker start
```

`create` accepts `strategy` (entry/exit/protection/risk definitions), `mode`
(`historical` or `forward_paper`), `codeHash`, `codeCommitSha`, `periodStart`,
`periodEnd`, `cashAsset`, and `initialCash`. The registry computes the immutable
strategy hash and returns `runId` and `portfolioId`. Changing a registered
policy's contents requires a new version. Every entry/exit combination has its
own run and cash balance. Keep policy amounts and thresholds in private files.

`tick` accepts `portfolioId`, `now`, a `PaperMarketSnapshot` as `market`, and an
optional persisted `signalId`. It settles due orders, evaluates existing position
protection, then evaluates an eligible entry. Use the exported `tickMatrix`
method to distribute shared observations to multiple child portfolios. Both
paper modes run this same engine with an injected clock. A future market or
signal is rejected, and a portfolio's clock cannot move backwards.

The implemented parents are E01 narrative-only, E02 wallet convergence, E03
combined candidate evidence, and E04 all qualified signals. E01 requires an
eligible NIT narrative with an exact mint; E03 requires a NIT candidate-update
with `actionState=trade_eligible`, `narrativeConfirmed=true`, and
`walletConfirmed=true`. Their upstream candidate resolver remains separate.
The supported children are X01/X02 configurable profit targets, X03 trailing
profit, X04 open-ended, and X07 time exit, all under P01 protection and R01 risk.
Other policy IDs are rejected until their implementations exist.

Orders are accepted into a durable plan and settled on a subsequent tick after
the configured delay. Prices come from a constant-product reserve estimate for
either the bonding curve or PumpSwap, with real reserve limits, token precision,
cash fees, quote expiry, slippage and impact gates. Fill fractions and periodic
transaction failures are explicit pinned simulation settings. Partial remainders
are cancelled; failed attempts have no fabricated fill. Profitable reductions
reserve the configured share of the original acquired quantity, and arm the
post-profit stop only after an actual profitable fill. Protective or critical
risk exits can sell the entire remainder.

`status` accepts `portfolioId`. `rebuild` repairs mutable cash and position
projections from immutable settlements and position-event snapshots. Restart
recovery requires re-delivering ticks for outstanding plans; portfolio locks and
unique settlements prevent double fills. Failed exits retain the position and
can retry on a later market snapshot. The initial runtime permits one acquisition
cycle per mint per portfolio; a new experiment/run starts a fresh cycle.

This is a reserve-based simulator, not a live route adapter. It does not yet
acquire reserves directly from chain providers, reproduce venue-specific
fees/MEV, or run the forward experiment. An input adapter must supply timestamped
reserves and verified tradability/risk facts; missing critical flags block work.
No new external dependency, telemetry, or network provider is introduced. The
worker uses the existing PostgreSQL connection and has no signing capability.

### Continuous paper dispatch

Set the command action to `serve`, provide an explicit `approvedSources` list,
and supply `dispatch` with `portfolioIds`, `intervalMs`, `retryMs`,
`maxProtectionTargets`, `maxSignalsPerCycle`, and `maxSignalAgeMs` in the private
command file. The loop accepts only configured forward-paper portfolios.
SIGINT/SIGTERM stops scheduling, drains the active cycle, and closes the database.

The feed reads existing `market_snapshots` from approved non-synthetic sources.
Providers must publish `raw_payload.paperExecution` with the exported
`PaperExecutionFacts` contract: schema version 1, the exact `marketAddress`, cash
asset, virtual/model and real reserve amounts, decimals, and explicit verified,
tradable, sellable, and critical-risk booleans. The reserve observation must belong
to the currently active canonical venue and market address. USD liquidity or
price metrics alone are insufficient. A newer incomplete reading blocks use;
the adapter does not fall back to older reassuring risk facts. No provider is
automatically approved and no existing synthetic adapter gains production status.

Each bounded cycle first checks held positions and outstanding orders across
all configured portfolios. Missing/stale/non-executable data or a pending sell
blocks new entries for that portfolio. Protection work rotates by last check;
when its batch is over capacity, new entries wait. Market reads are shared within
the cycle. New signal deliveries have durable retry/terminal records, so restarts
and late database commits cannot silently skip signals. Missing data defers a
signal until recovery or its configured expiry. A record is completed only after
a strategy decision exists, or when the opportunity expires.

`status` returns portfolio data, up to 100 recent feed issues, and a waiting-signal
count. `paper_feed_health` and `paper_signal_dispatch` are durable operational
projections. This integration adds only database reads/writes; it sends no data
to another network provider. The opt-in market worker can now publish the
execution-fact contract from Solana account reads as `solana-account-facts:v1`.
It supports a limited set of legacy layouts and rejects unsupported token/pool
modes. Configure and validate live coverage before unattended use. See
[forward-paper operations](deploy/README.md) for network exposure, limits and startup.

## Candidate resolver and Telegram console

`@nit/app-console-worker` resolves persisted NIT narrative candidates, publishes
versioned candidate and narrative signals, and exposes an authorized **paper-only**
Telegram console. The deterministic console works without a model or agent.
It consumes existing market facts and wallet evidence; it does not introduce a
market-data producer, live broker, historical replay runner, or learning runtime.

An upstream NIT narrative must have a canonical narrative ID and payload
`{schemaVersion: 1, title, narrativeStrengthBps, candidates}`. Each candidate
contains an exact `tokenMint`, `linkConfidenceBps`, and explicit `reasonCodes`.
Scores are integers from 0 to 10000 describing source evidence, not calibrated
profit probabilities. All competing mints remain in the report. Equal or close
links require manual review; wallet activity cannot silently choose a tied mint.
NIT-only E01 eligibility and independent-wallet confirmation remain separate.
Shared-funder/related-wallet evidence causes manual review, and unknown safety
data stays unknown. E03 requires both qualified sources and explicit market safety.
Coalescing and watch expiry use signal availability time; future observations
cannot qualify a candidate. Reports, policy snapshots, signals and their events
are committed atomically. Updates push only when the report materially changes.

Keep configuration in `private/`. Set `NIT_CONSOLE_CONFIG_PATH` to a JSON file and
provide `NIT_TELEGRAM_BOT_TOKEN` through the runtime environment, then run:

```powershell
pnpm --filter @nit/app-console-worker start
```

The JSON object requires `cashAsset`, `approvedMarketSources`, `cycleIntervalMs`,
`resolver`, and `console`. The resolver policy supplies
`minimumNarrativeStrengthBps`, `minimumLinkConfidenceBps`,
`minimumSelectionMarginBps`, `minimumIndependentWallets`, `coalescingMs`,
`walletWindowMs`, `maxMarketAgeMs`, `watchDurationMs`, `pollIntervalMs`, and
`maxScenariosPerCycle`. No tuned policy defaults are embedded in the runtime.
The console configuration supplies `botName`, `actionTtlMs`, `maxOutbox` (64–2048),
and `operators`: `{userId, chatId, role: 'viewer' | 'trader', portfolioIds}`.
IDs are strings; configure one chat per user, with explicit portfolio access.
The worker uses the existing `NIT_CONFIG_PATH` bootstrap configuration and no toy
provider binding. Use one polling process per bot; an existing Telegram webhook
must be disabled before long polling. Poll offsets and message/action state are
persisted. Stop with SIGINT/SIGTERM to drain work and close the database.

Use `/candidates <narrative>` to refresh the narrative and separate token cards,
`/strategies`, `/strategy <portfolio>`, `/portfolio`, and `/positions` to inspect
authorized paper state, and `/use <portfolio>` to bind newly generated buttons to
a portfolio. Trader actions include Buy, Watch, Discard, Close Remaining,
`/pause <portfolio>`, `/resume <portfolio>`, and `/stop <position> <price>`.
Stops can only tighten. Pause withholds new entries while the separate paper
dispatcher continues protection and settlement; keep that worker running.

Buy/Close first prepares a fresh reserve-model quote showing the exact input,
estimated output, fees, impact, portfolio and expiry. Explicit confirmation queues
a durable request, whose execution rechecks the current candidate, strategy,
quote and risk gates. Acceptance creates a delayed paper order, never an instant
fill. Opaque callbacks bind to bot/user/chat/message and expire; duplicates and
worker restarts cannot create a second order. A material candidate or policy
change requires refreshed buttons; equivalent evidence refreshes remain usable.
Watch/Discard withhold entries for that narrative/mint/portfolio and
cancel pending buys at settlement without closing existing positions. Watch may
be followed by an explicit Buy; a discarded candidate must first return to Watch.
Evidence history is retained. Replay scheduling uses the research worker below;
`/run-report <experiment>` reads results only when every variant portfolio is
authorized for that operator.

`/outbox` reports delivery counts. Definite rate-limit failures retry within a
bound; an ambiguous send or interrupted in-flight delivery is marked `uncertain`
and is not automatically resent. Refresh `/candidates` or `/positions` to obtain
new buttons after delivery loss or expiry. The worker does not silently fabricate
provider delivery guarantees.

Network exposure: when explicitly started, this worker sends narrative titles,
token identities, evidence scores/reasons, authorized portfolio identifiers,
paper balances, quotes and position status to configured Telegram chats through
`https://api.telegram.org`. Telegram returns user/chat identities, commands and
callback IDs. The bot token is sent only to that fixed API host; provider URLs,
tokens and raw transport errors are not logged. No third-party SDK, dependency,
telemetry, signing key or live execution capability is added. Automated checks
use mocked Telegram transport and an isolated development database.

## Research provenance and matched replay

`@nit/app-research-worker` provides bounded `append`, `publish`, `freeze`,
`create`, `advance`, and `report` commands. Set `NIT_RESEARCH_COMMAND_PATH` to a
private JSON file (at most 1 MiB), build the workspace, then run:

```powershell
pnpm --filter @nit/app-research-worker start
```

`append` takes a `record` conforming to `ResearchRecord` in
`packages/signals/src/research.ts`: artifact, episode, interpretation, coverage,
or feature. Records have stable identities, immutable versions/content hashes,
producer versions, evidence references and distinct occurrence, observation and
availability timestamps. Source artifacts hold bounded captured text and media
hash/storage references; media bytes remain outside PostgreSQL. Derivative roots,
common authors/source identities and identical content discount independent
corroboration. Similarity or repetition does not establish original authorship or
official endorsement. Captured interpretations preserve competing and rejected
mints, extractor/model/prompt versions and approximation status. Revival episodes
reference their predecessor rather than overwriting a launch interpretation.

`publish` takes `interpretationId` and publishes an available captured
interpretation as a schema-2 NIT narrative with explicit episode/provenance links.
This is an explicit-text evidence producer; it does not fetch social history,
invent model interpretations, or perform OCR/video extraction. Existing ingestion
and future extractors can supply the same immutable contract. The resolver checks
the signal against its recorded interpretation and emits per-token feature
snapshots. Decisions retain the snapshot, evidence mask and pinned strategy hash.
Legacy schema-1 narratives remain supported without invented provenance.

Evidence states distinguish `pending`, `unavailable`, `extraction_failure`,
`observed_nonconfirming`, `contradictory`, and `confirmed`. Empty stored wallet
history means unavailable coverage unless an explicit completed observation
covers the resolver's full window. Relationship suspicion is recorded separately
from technical execution safety. An optional resolver `evidencePolicy` version 1
sets `narrativeRelationshipHandling` (`review`, `advisory`, `block`) and
`combinedRelationshipHandling` (`review`, `block`, `discount`). Legacy defaults
remain review. Discount mode counts connected wallets as one independent group;
it does not permit them to count individually. E01 and E03 qualifications are
separate. E04 can evaluate competing plausible mints without requiring a single
winner; it still observes relationship policy and hard execution/capital gates.
Episode-aware positions allow a later episode after the earlier position closes,
while retaining one open position per mint/portfolio and one acquisition per
episode. Legacy signals retain their original one-cycle behavior.

`freeze` takes `dataset` conforming to `ResearchDataset` in
`packages/trading/src/research-replay.ts`. Supply the ex-ante universe with episode
IDs, discovery timestamps and dead/failed/illiquid/ungraduated tokens; UTC start,
decision-end and full evaluation horizon; chronological partition; discovery and
coverage descriptions; measured/proxy and captured/regenerated labels; extractor
versions; seed; and explicitly timestamped opportunity/reserve events. Each
opportunity supplies family-specific costs and latencies, an optional immutable
feature reference, and optional wallet-only signal reference. Freeze pins a
canonical manifest hash and actual feature hashes. Missing markets/features are
not filled from future observations. A curated universe is explicitly a proxy;
declaring a market census does not independently verify provider completeness.

`create` takes `experiment` conforming to `ResearchExperiment`, including an
explicit experiment ID, dataset hash, code hash/commit, cash asset, initial cash,
policy version, full `qualificationPolicy`, timing mode and strategy snapshots:

- A uses E00 market-only entry over the common ex-ante opportunity universe.
- B uses E01 with narrative provenance and masks wallet information.
- C uses E03 with narrative plus independent wallet evidence.
- Optional W uses E02 and can consume wallet signals without narrative features.

Every variant must pin identical sizing, exit, protection, risk and execution
assumptions. Each gets a separate historical portfolio.
The qualification policy pins narrative/link score thresholds, independent-wallet
minimum and separate narrative/combined relationship handling; the resolver and
replay invoke the same pure qualification function. `fixed_decision` evaluates
then-available information at common timestamps; `end_to_end` separately includes
supplied pipeline delays, later feature availability and costs. Decisions consume
masked evidence, and every fill/exit uses `PostgresPaperTrader`. E00 is a separate
baseline; E04 is not repurposed as a market-only policy.

`advance` takes `experimentId`, `maxSteps` (1–64 per variant) and
`maxProtectionLagMs`. Each recorded timestamp checkpoints independently; replay
can resume after a crash between engine and checkpoint commits. The worker yields
between steps when forward-paper protection is overdue. Datasets are bounded to
512 scenarios and 5000 events; larger research should use separately frozen
chunks rather than loading unrestricted histories into the 2 GB process.

`report` takes `experimentId` and returns immutable completed reports with actual
orders/fills/failures/partials, missing-data and skipped opportunities, per-scenario
cash outcomes and paired increments, fees, research costs, latency, exposure,
cash and reserve-estimate drawdowns, worst outcomes and top-winner sensitivity.
Unknown/unexecutable residual valuations are zero, not invented terminal fills;
incomplete exit horizons remain visible. Reserve estimates are not guaranteed
liquidation prices. Multiple variants and repeated-mint episodes are correlated;
sample counts do not establish independence. Attribution precision remains null
without independently labeled ground truth. Partition labels and matched inputs
support holdout evaluation; running or repeatedly tuning a holdout does not keep
it statistically untouched automatically. No automatic tuning/promotion is added.

These commands add workspace links only and use the existing development database.
They make no new network calls, collect no credentials, and do not start a bot,
provider, model, signer or live broker. Real datasets, thresholds, source artifacts
and research configuration remain private. Source collection, full multimedia
extraction, calibration training and agent learning remain later integrations.

## Observe-only agent

`@nit/agent-runtime` adds an optional evidence-linked investigator above the
deterministic terminal. Its seven tools read captured research records/decisions,
candidate versions, scoped wallet swaps/relationships, approved reserve snapshots,
strategy snapshots, frozen experiment reports and position-event projections.
The model can choose a next read or finish a structured explanation. Trading,
approvals, experiment execution, policy changes, learning-journal writes, SQL,
shell and arbitrary URL fetching are not registered agent tools in this phase.

The runtime ships pinned `SOUL.md`, `INVESTIGATION.md` and `OUTPUT.md` guides in
`packages/agent-runtime/guides/`. Their full text/hashes, catalog hash, code version,
model identity, principal grants and limits are pinned with each task. External
posts and tool data are quoted evidence, never capability grants. Guide/config
changes reject queued stale tasks; submit a new request ID after review. Server
checks remain authoritative independently of guide prose or model instructions.

Use `@nit/app-agent-worker` with `NIT_AGENT_CONFIG_PATH` and
`NIT_AGENT_COMMAND_PATH` pointing to private bounded JSON files, plus the existing
`NIT_CONFIG_PATH` bootstrap configuration:

```powershell
pnpm --filter @nit/app-agent-worker start
```

The configuration wrapper contains `agent: AgentConfiguration`, `cashAsset`,
`approvedMarketSources` (may be empty, making market evidence unavailable),
`pollIntervalMs`, and optional `gateway: {endpoint, allowedOrigin}`. The exported
types and validators in `packages/agent-runtime/src/types.ts` define the complete
contract. Principals have explicit identities, UTC expiry times, allowed read
capabilities, time/resource scopes and `allowExternalContext`. Scopes pin Solana,
record/decision/narrative IDs, mints, public wallet addresses, portfolio IDs and
experiment IDs. Experiment reads additionally require every variant portfolio.
Stored conclusions/traces recheck current owner scope before disclosure. A task
cannot expand its grant by citing an identifier in source text or model output.

Limits explicitly bound global queued tasks, steps, tool calls, wall time,
per-call timeout, input/output tokens, run/day token and cost reservations,
run/day trace-storage reservations, result size, date span and protection lag.
No tuned budget defaults are embedded in production configuration. One global
lease permits one planning task at a time across worker processes; overdue paper
protection makes the agent yield before claiming work. Agent/model availability
is not required by the paper dispatcher or protective exit logic.

Commands are `enqueue` with `task: InvestigationTask`; `run_once`; `serve`;
`status`, `trace`, and `cancel` with `principal` and `taskId`; and `recover`.
Tasks support narrative/wallet investigation, decision explanation, episode
review and frozen-experiment review. Intake is idempotent on principal/request
ID; changing content/settings under that ID is rejected. Status and trace commands
work without a model connection. SIGINT/SIGTERM aborts the active request and
drains the worker. Persisted cancellation is checked at call boundaries; an
already-running external request can consume its reserved budget until timeout.

`ReasoningModel` is provider-neutral. The optional `JsonGatewayModel` speaks NIT's
version-1 JSON protocol to one operator-approved HTTPS origin/path. It is not a
vendor-specific SDK or a claim that arbitrary vendor endpoints accept this
protocol. Implement the interface directly or translate at a configured gateway.
The request contains the pinned guides, scoped task, read catalog, bounded
history, remaining budgets and token limits. A response echoes `requestId` and
`protocolVersion: 1`, returns `action` (`tool` with name/version/arguments or
`conclusion`), and actual `usage: {inputTokens, outputTokens}`. Billing usage must
come from trusted provider metadata, not model-authored prose. The gateway must
honor supplied token limits; pricing rates are explicitly pinned by the operator.

Actual remote calls require `agent.enabled`, an authorized unexpired principal
with `allowExternalContext`, gateway configuration, and
`NIT_AGENT_GATEWAY_BEARER_TOKEN` in the runtime environment. No ambient model is
selected. Production rejects toy identities. Endpoint identity is hashed, query
credentials/userinfo are rejected, redirects are disabled, responses are bounded,
and transport errors are sanitized. No model service has been enabled or called
by the automated tests; those use a scripted development model and mocked HTTP.

Before a request, the controller durably reserves worst-case input/output tokens,
cost at pinned micro-USD-per-million-token rates, and trace bytes. Daily limits
aggregate across that principal's runs. Reservations are conservative budget
consumption, not billing reconciliation: smaller reported usage does not refund
them, and outages/unknown outcomes remain reserved. Actual validated usage and
computed cost are recorded separately. Interrupted requests are not retried
automatically. `recover` marks expired runs interrupted, preserves unknown-call
audits/budgets and uses the existing queue's fenced, single-attempt recovery.
Use a new explicit request ID for an operator-authorized retry. Never delete
audit/queue records to reset a budget or simulate successful completion.

Conclusions are WATCH, INSUFFICIENT_EVIDENCE or EXPLANATION_ONLY, with a
qualitative confidence label, alternatives and missing evidence. Every claim must
cite record IDs actually returned in this run; evidence hashes and availability
are preserved. Bounded report views retain the full immutable source hash and a
separate hash of the returned projection. This validates references, not semantic
entailment of model prose.
Outputs are labeled `MODEL_INTERPRETATION_NOT_FACT_VERIFIED`. Missing/future data
cannot become a verified observation or an order approval. Tool attempts,
validated model responses, cancellations and conclusions have immutable audit
records; status/budget/lease rows are operational projections.

Data exposure, when explicitly enabled: selected source text and narrative
reports, approved public wallet activity, scoped strategy/position data and frozen
research summaries are sent to the configured gateway/model service. This can
include private research or policy parameters if those scopes are granted.
Credentials, filesystem contents outside pinned guides and unscoped portfolios
are not model tools. The bearer credential is used only on the fixed transport
and is excluded from task context and traces. Dependencies add workspace links
and the already-used Zod validator; no vendor SDK, telemetry, signer or live
execution dependency is introduced. Actual provider billing and the complete
2 GB deployment load still need operational validation. Learning, promotion and
paper-trade capabilities remain separate future phases.

## Local configuration and first start

The local template is [env.example](env.example); the private local file is `.env`
in the repository root. It lists runtime database credentials and optional X,
RPC, wallet, Telegram, narrative, R2, observer and research settings. Examples use
placeholders, with external services disabled. Replace the required `DB_HOST`,
`DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` and `DB_SSL` values with your dedicated
NIT development database settings. Never use the PostgreSQL superuser.
`NIT_TEST_DATABASE_URL` is separate and does not configure the running apps.

`NIT_CONFIG_PATH` points to the schema-compatible
[development YAML](config/development.sanitized.yaml). `DB_*` environment variables
override its dummy values. Optional settings are commented out deliberately:
uncomment a setting only after filling it. Empty optional path variables can be
treated as configured paths by workers. Do not set them to empty strings.

Use Node 22+ for the local launcher. After filling the database settings, build with `pnpm build`. When you explicitly
choose to start locally, `pnpm local:control` loads `.env` and starts only the
loopback control API at `http://127.0.0.1:3000` (or the configured port). Startup
checks/applies database migrations. `/health` and `/ready` are API endpoints;
there is no browser dashboard and this command does not start trading workers.
Do not run a `local:*` command until startup is authorized. Existing shell
environment variables take precedence over `.env` values. The launcher loads the
file before spawning the selected Node process so `NODE_EXTRA_CA_CERTS` is present
at process startup. For Supabase set `DB_SSL=true` and use its public database CA
at `config/certs/supabase-prod-ca-2021.crt`; certificate/hostname verification stays
enabled. `node scripts/local.mjs --check control` checks only local file paths;
it does not start any app, connect to a database or apply migrations.

Start additional services individually, after their private configuration is ready:

| Command                | Additional settings                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------------------- |
| `pnpm local:market`    | Market enable flag, RPC endpoints/programs; execution-facts JSON for the fact producer            |
| `pnpm local:wallet`    | Wallet enable flag, RPC endpoints, private watchlist and convergence policy                       |
| `pnpm local:ingestion` | X capture JSON and bearer; without that explicit path, development uses the toy ingestion handler |
| `pnpm local:narrative` | Narrative JSON, gateway bearer when selected; R2 credentials when media is enabled                |
| `pnpm local:paper`     | Paper command JSON; a registered forward-paper portfolio for `serve`                              |
| `pnpm local:console`   | Console JSON and Telegram token with explicit operator/chat/portfolio allowlists                  |
| `pnpm local:observer`  | Agent configuration/command JSON and its optional gateway bearer                                  |
| `pnpm local:research`  | Explicit one-off research/replay command JSON                                                     |

The JSON paths in `env.example` are illustrative, not bundled policy files.
Gateway/model IDs, grants, FFmpeg paths, storage endpoint/bucket, strategy values,
source accounts and operator IDs belong in the respective configuration schemas,
not invented environment variables. Keep those filled files outside the checkout.
See [narrative settings](packages/narrative-intelligence/RICH_MODE.md),
[paper operations](deploy/README.md) and the worker's command schema before
enabling it. `.env` and filled configuration are never part of a public commit.

## Automated narrative evidence

The optional narrative worker turns captured text and explicitly enabled media into validated interpretations,
separate narrative and token-link assessments, immutable provenance and resolver inputs.
It supports a provider-neutral gateway and a literal watch-only catalog adapter.
Rich mode adds versioned semantic claim grouping, revival lineage, token metadata
embeddings, locally decoded video frames/audio, OCR/transcript interpretation and
source-artwork matching. Interpretation remains separate from deterministic scoring
and trading authority. See [producer coverage and operation](packages/narrative-intelligence/README.md).
A configured gateway, decoder binaries, private media storage and live acceptance
checks remain prerequisites for the agreed full-system test.
