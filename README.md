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
