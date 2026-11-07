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

`nit@ingestion-worker` uses real X capture when `NIT_X_CAPTURE_CONFIG_PATH` and
`NIT_X_BEARER_TOKEN` are set. Without that explicit path it retains its development
toy job handler, which production guards reject. Captured research interpretations
may be published through the research worker while automated interpretation is
unfinished. A healthy control API is only
database/process health, not proof the trading pipeline has complete coverage.

The template bounds each process to 256 MB, caps heap size and restarts failures.
These are initial containment settings, not measured capacity claims. Start the
deterministic services first; enable the observer only after measuring headroom.
Use bounded journald storage on the host (for example a 200 MB SystemMaxUse)
and database retention/archive planning before an extended run. Never delete
immutable trading evidence to recover space without an explicit archive policy.

## Execution fact contract

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
evidence is retained in PostgreSQL. It checks legacy SPL mint ownership and
authorities, supported curve/pool layouts, vault binding, reserve availability,
slot freshness and PumpSwap trading-disable flags. It rejects Token 2022 and
unsupported newer reward, cashback, quote and virtual reserve modes. Atomic chain
amounts are converted to exact human decimal quantities before entering the paper
engine. Capture time identifies each read; chain-state time remains explicit so
repeated captures cannot rejuvenate stale reserves. These
checks model paper execution; they are not a sell simulation, bundle analysis,
holder analysis or guarantee against a rug. Policy fees remain model assumptions.

Protocol references: [Pump IDL](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json),
[PumpSwap IDL](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump_amm.json),
[Solana account batches](https://solana.com/docs/rpc/http/getmultipleaccounts).
Validate live account samples before expanding supported modes.

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
are isolated. This initial adapter captures text and reference metadata, not
downloaded media or complete platform coverage. Timeline limits/deletions and
provider indexing can still make historical coverage incomplete. Captured posts
are untrusted observations and do not automatically become narrative trade signals.
The provider interface already admits Telegram; no Telegram channel reader is
enabled by this release.
