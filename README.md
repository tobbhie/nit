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
