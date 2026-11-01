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
