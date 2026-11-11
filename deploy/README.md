# Forward-paper operations

Target: one Linux VPS with 2 GB RAM and external Supabase PostgreSQL. No wallet,
signer, live broker or model is required for trading/protection. The observer is
optional and has its own service and quotas. This is an operations profile;
it does not certify a data provider or a strategy as ready.

Build a pinned commit with Node 20+ and pnpm 10 (`pnpm install --frozen-lockfile`,
`pnpm build`). Build off the small VPS when possible. Deploy the workspace with
its built packages and production dependencies to a versioned directory under
`/opt/nit/releases`; point `/opt/nit/current` to that release. Include only
tracked source/config templates and build outputs. Never copy the developer
workspace wholesale, `.env`, `private/`, development docs or datasets.

Create a dedicated unprivileged `nit` account. Keep the release read-only to it.
Install private runtime configuration in `/etc/nit` (`root:nit`, directory 0750,
files 0640). Use `runtime.example.env` as the list of runtime file locations;
it is not a runnable configuration. Put the restricted NIT database role and
provider keys in the private files, never in the release, shell history or chat.
Use a dedicated database/schema, TLS, and the direct or session-mode Supabase
connection. Transaction pooling is unsuitable for session-affine workloads.
Set database poolMin to 0 and poolMax to 2 per process initially; account for all
enabled processes against the project's connection limit. No superuser is needed.

Copy the service template to `/etc/systemd/system/nit@.service`, then reload
systemd. Start `nit@control-api` alone first so schema migrations complete before
starting other services. Startup checks pending migrations even when all tables
already exist; advisory locks serialize concurrent migration checks.

Enable these instances individually after their configuration is verified:

- `nit@market-worker`: discovery/reconciliation and execution fact refresh.
- `nit@wallet-worker`: independent watchlist source, when configured.
- `nit@paper-worker`: **serve** command, explicitly approved sources and registered
  forward-paper portfolio IDs. Never deploy a one-off create/tick command as a
  restarting service. The reserve source is `solana-account-facts:v1`.
- `nit@console-worker`: NIT-only bot, allowlisted users/chats/portfolios, webhook
  disabled. Keep a single poller per bot.
- `nit@agent-worker`: optional, explicit serve command and observation-only grant.
- `nit@narrative-worker`: automated text interpretation and deterministic scoring.
  Supply a private policy, two database pool slots and (for semantic interpretation)
  a real interpreter gateway grant. See [producer coverage and protocol](../packages/narrative-intelligence/README.md).

`nit@ingestion-worker` uses real X capture when `NIT_X_CAPTURE_CONFIG_PATH` and
`NIT_X_BEARER_TOKEN` are set. Without that explicit path it retains its development
toy job handler, which production guards reject. Captured research interpretations
can also be published through the research worker. Automated production currently
covers a bounded text path with opt-in rich semantic/media evidence; the rich
gateway, decoder and storage settings must be configured separately.
A healthy control API is only
database/process health, not proof the trading pipeline has complete coverage.

The template bounds each process to 256 MB, caps heap size and restarts failures.
These are initial containment settings, not measured capacity claims. Start the
deterministic services first; enable the observer only after measuring headroom.
Use bounded journald storage on the host (for example a 200 MB SystemMaxUse)
and database retention/archive planning before an extended run. Never delete
immutable trading evidence to recover space without an explicit archive policy.

## Execution fact contract

For an explicitly limited live-only local collection stage, set
`NIT_SOLANA_RECONCILIATION_ENABLED=false`. This stops the historical scheduler and
repair consumer, but keeps confirmed lifecycle intake and fact refresh running.
Market health is labeled degraded with `live_only_unrepaired_gaps`; it is not a
complete feed or unattended paper-test acceptance. The default is `true`.
Validate historical RPC throughput before enabling repair on high-volume programs.
Both Pump creation and Pump-to-AMM migration events are emitted by the configured
Pump program, sharing one WebSocket. The AMM program remains the owner checked
for pool execution facts. Known non-lifecycle Pump logs are filtered before HTTP
transaction lookup; unknown/truncated logs still fetch. The live queue is bounded
and closes an overloaded subscription, reporting the gap for reconciliation.
Live intake reads each transaction once for both lifecycle decoders. Up to four
rate-limited RPC reads overlap. Up to two independent mint writes overlap; writes
for the same mint retain notification order, so a later migration cannot overtake
an earlier creation. Empty decodes do not hold a database slot. The 128-notification
backlog cap remains in force. Measure sustained backlog and RPC latency before
leaving the worker unattended; increasing the cap does not increase throughput.

Transport/timeouts, HTTP 429/5xx and transient node responses get at most three
rate-limited attempts with bounded backoff. Temporarily unindexed confirmed
transactions also get a bounded retry. Provider error text is not retained.
When live preparation or persistence still fails, the worker must save a
`token_market_recovery` job before reporting the notification as deferred. A
successfully deferred notification does not disconnect the healthy subscription;
health exposes deferred counts, pending recovery depth, retry counts and recovery
worker failures/dead letters. Recovery uses the existing leased, idempotent queue
with ten attempts and a minimum two-minute lease. It runs even when broad
historical reconciliation is disabled.

For migrations whose mint is not yet canonical, recovery queries that mint's
`getSignaturesForAddress` history, then decodes the original Pump creation event
before applying the migration. Only the mint, transaction identities and timing
go to the existing configured RPC provider. No other external endpoint is added.
Each attempt is bounded to 20 pages, 16 oldest successful transactions and a
45-second search budget checked between calls. `solana_creation_searches` stores
page/candidate progress so a later attempt or restart resumes rather than
repeating the scan. Missing/pruned/incomplete history
remains a failed retry/dead-letter item; the worker never fabricates metadata or
advances the broad historical watermark. A periodic bounded scan also schedules
stored, unapplied migrations from earlier local runs. This repairs known failed
notifications and stored occurrences, not transactions never observed during a
disconnect. Full historical coverage still needs validated reconciliation.

Migration `0027_targeted_market_recovery` also permits an empty initial Pump
market interval when creation and migration have the exact same timestamp.
The repository requires chain provenance, a Pump-to-PumpSwap transition and a
non-regressing slot; earlier slots and legacy timestamp ties still fail closed.
The original timestamps are retained, and half-open interval lookup selects the
new pool at that instant. Other immutable market fields remain protected.

Set `NIT_EXECUTION_FACTS_CONFIG_PATH` to JSON with explicit `pumpProgram`,
`swapProgram`, `swapGlobalConfig`, `intervalMs`, `maxTargets`, `maxSlotAgeMs` and
`candidateMaxAgeMs`. Program addresses must match discovery. Keep polling limits
and real tuned values private. Size the number of targets and RPC quota so all
held/pending tokens refresh inside the trading policy's maximum market age;
freshness gates defer entries if that budget is missed. Polling rotates oldest
targets, prioritizing held/pending tokens. It does not promise real-time updates
for an unbounded universe.

This adapter reads `getMultipleAccounts` and `getBlockTime` from the configured
Solana RPC. It exposes public market/mint/vault addresses and request timing to
that provider. No private keys, signing or transaction submission. Raw account
evidence is retained in PostgreSQL. It checks SPL/Token-2022 mint ownership and
authorities, supported curve/pool layouts, vault binding, reserve availability,
slot freshness and PumpSwap trading-disable flags. Token-2022 support is limited
to metadata-pointer/token-metadata mint extensions and immutable-owner vaults;
unknown extensions, transfer fees, hooks, delegates and pauses fail closed.
Ordinary SOL curves include the documented 150/166-byte layouts; Mayhem,
cashback, non-SOL quote and depth modes remain unsupported. Holder-reward SOL
curves use the same reserve model, as documented by Pump. Atomic chain
amounts are converted to exact human decimal quantities before entering the paper
engine. Capture time identifies each read; chain-state time remains explicit so
repeated captures cannot rejuvenate stale reserves. These
checks model paper execution; they are not a sell simulation, bundle analysis,
holder analysis or guarantee against a rug. Policy fees remain model assumptions.

Protocol references: [Pump IDL](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json),
[PumpSwap IDL](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump_amm.json),
[Solana account batches](https://solana.com/docs/rpc/http/getmultipleaccounts).
Validate live account samples before expanding supported modes.

## Token artwork discovery

Artwork bytes do not come from the token program's balance data. Pump's
`CreateEvent.uri` is retained as `tokens.raw_payload.uri`. Resolve that URI to a
metadata JSON document, then follow its `image` field to the artwork. For other
launch adapters, obtain the URI from Metaplex metadata or Token-2022's metadata
pointer/metadata extension rather than treating the mint address as an image URL.

The rich narrative token-representation path already follows this chain through
its media processor, captures the metadata hash and artwork evidence, then
interprets the image. It runs only with the narrative worker, gateway and media
configuration enabled; market-only collection does not download artwork. JSON
and image resolution are deterministic operations; interpretation is separate.
Use explicit approved HTTPS origins and an IPFS gateway, response/time limits,
content hashes and private archival storage. A missing/broken image must leave
artwork unavailable without blocking canonical token discovery. Do not enable
the model gateway just to obtain an image URL.

## Acceptance before the 48-hour run

Check loopback `/ready` and `/health`, confirmed discovery plus finalized repair,
fresh approved execution snapshots, and the absence of stale-data entries.
Exercise a paper entry, delayed fill, stop/exit, pause/resume, restart recovery,
and authorized Telegram confirmation. Stop an upstream provider and the optional
observer separately: missing facts must defer entries; protection must continue
whenever fresh sell facts are available. Inspect paper feed/dispatch health,
pending protection targets, queue/dead-letter counts, database storage, process
RSS and systemd restart/OOM counts. Record missing/unsupported market coverage.
Restart on a new pinned release and verify only pending migrations apply.

The 48-hour run starts after those live checks pass. Its result establishes
operational behavior under observed load, not strategy profitability. A VPS,
restricted database endpoint, RPC access, configured portfolio policies and a
real narrative provider remain operator prerequisites.

## X capture

The private capture JSON must provide explicit `authorIds` (numeric X user IDs),
`initialSince` (UTC timestamp), `intervalMs`, `requestTimeoutMs`, `maxPages`,
`maxPosts` and `minRequestIntervalMs`. The watchlist and tuned limits stay private.
The adapter calls only the official read-only user timeline endpoint; it sends
the configured author IDs/cursor and a NIT-specific bearer credential to X.
No posting, browser login or scraping. Access/quota availability must be checked
with the operator's X application. See [X Get Posts](https://docs.x.com/x-api/users/get-posts).

Capture drains bounded pagination before persisting posts, refuses partial API
errors/backlogs, writes oldest first and stops an author's batch at the first
failed write. The numeric persisted post ID is the restart cursor. Failures do
not advance it. A backlog exceeding the configured budget needs operator action;
it is reported as failed capture rather than silently dropped. Author failures
are isolated. The adapter captures text, references and photo/video attachment
metadata through X media-key expansions. Missing/unplayable expansions fail the
batch; video capture selects the smallest advertised MP4 variant. The narrative
worker downloads attachments only with an explicit media grant and approved
origins. This is not complete platform coverage. Timeline limits/deletions and
provider indexing can still make historical coverage incomplete. Captured posts
are untrusted observations and do not automatically become narrative trade signals.
The provider interface already admits Telegram; no Telegram channel reader is
enabled by this release.

## Rich narrative prerequisites

The optional rich protocol is documented in
[the narrative package](../packages/narrative-intelligence/RICH_MODE.md). Configure
the embedding model and dimensions on both policy and gateway, explicit media
disclosure, approved source/IPFS origins, absolute FFmpeg/ffprobe paths and a
private dedicated scratch directory. Install and pin decoder binaries separately;
they are runtime tools, not vendored application dependencies. The worker's OS
identity needs write access to that scratch directory under service sandboxing.
Keep media CPU/memory below protection workloads and measure total RSS, including
child decoders, on the 2 GB host. One producer session serializes decoding,
metadata inference and archive recovery; uploads can overlap analysis.

Media durability uses private R2 with `NIT_R2_ACCESS_KEY_ID` and
`NIT_R2_SECRET_ACCESS_KEY` in the host environment. Use a NIT-only bucket-scoped
credential; never place it in narrative JSON or repository files. Analysis does
not wait for R2. Verify upload failure/recovery, scratch pressure, semantic index
warm-up, budget exhaustion, model mismatch and producer withdrawal before the
48-hour run. Strict semantic mode blocks new narrative entries until every known
canonical token in its registry has a captured representation; size the initial
registry and measure enrichment throughput before starting the experiment.
