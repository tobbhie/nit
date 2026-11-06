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
to another network provider. A real market-data producer must still supply the
execution-fact contract before unattended forward paper trading is usable.

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
Evidence history is retained. Backtest commands report their unavailable status.

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
