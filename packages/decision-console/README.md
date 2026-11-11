# Telegram console

Run `pnpm local:console` with `NIT_TELEGRAM_BOT_TOKEN` and a private JSON file
referenced by `NIT_CONSOLE_CONFIG_PATH`. The token is trimmed at startup and never
logged. The local launcher prefers the project's token over an inherited token.
Only explicitly configured user/chat pairs can issue commands or receive replies.
Use a private chat for the first pilot. Never enroll an arbitrary first sender.

## Monitoring before paper trading

Set `mode: "monitor"` with `console.botName`, `console.operators`,
`console.actionTtlMs`, `console.maxOutbox` and
`cycleIntervalMs`. Monitoring requires viewer operators with empty `portfolioIds`.
No resolver policy or paper market source is needed. Each operator supplies numeric string `userId` and
`chatId`; identities and runtime JSON stay private. Cycle intervals are bounded to
100–60000 ms and outbox capacity to 64–2048 messages.

Monitoring polls commands, flushes the durable delivery queue and delivers
existing candidate reports. It does not run candidate resolution, execute paper
requests, start source ingestion or control other worker processes. The console
also refuses trade execution directly when monitoring is selected, including
requests confirmed before a mode change. Old outbox messages can still contain
obsolete buttons; server-side viewer authorization rejects them.

Commands:

- `/start` or `/help`: available commands and active console mode.
- `/status` or `/health`: database connectivity and persisted data counts. These
  counts do not prove workers are currently running or source coverage is complete.
- `/ping` or `/test-alert`: an explicitly labelled delivery test, without creating
  synthetic market evidence or a trade.
- `/outbox`: delivery states for the authorized operator.

Normal paper controls remain available in `mode: "paper"` (the existing default
when mode is absent), with an explicit resolver policy, trader role and authorized
paper portfolio IDs. Monitor startup rejects trader permissions and portfolio
scope. No mode enables real-money execution through this console.

## Network and delivery

The [Telegram Bot API](https://core.telegram.org/bots/api) receives message text,
configured chat IDs, opaque callback tokens and callback acknowledgements.
Messages can include candidate reports and, in paper mode, scoped portfolio data.
Credentials are transmitted only to the fixed HTTPS Telegram host. No telemetry
or extra dependency is added. Long polling requires an unset webhook and a single
active polling consumer for the bot; setup checks must not delete a webhook or
discard pending updates without explicit operator direction.

Commands have durable receipts and polling offsets. Ambiguous message delivery is
marked uncertain instead of being blindly resent. A successful Bot API send means
Telegram accepted the message; the operator must still verify receipt in the app.
