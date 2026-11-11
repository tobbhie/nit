# Automated narrative evidence producer

`@nit/narrative-intelligence` turns persisted observations into captured source
artifacts, bounded interpretations, attention episodes, deterministic candidate
assessments and provenance-backed NIT signals. The existing resolver, console and
paper strategies consume these signals. The observing agent is optional and has
no role in this producer's execution.

This page describes the legacy **text evidence path**. Opt-in rich mode adds
semantic grouping, immutable membership/branch history, linked attention revivals,
token metadata embeddings and image/video/audio evidence. See
[rich mode configuration, coverage and data exposure](RICH_MODE.md).
A captured claim or model interpretation is not a verified fact or evidence of profitability.

## Interpretation and qualification

The interpreter returns topics with exact `subjectQuote`, `catalystQuote` and
`contextQuote` excerpts, category and stance. Context must literally contain both
anchors. Unknown fields, invented excerpts and model scores are rejected. The
interpreter receives no token universe, tools, positions or trading authority.
Citation validation proves attribution to source text, not semantic truth.

Adapters:

`ToyTextInterpreter` is deterministic development scaffolding; production runtime
refuses it. The deployable choices are:

- `catalog`: a local literal alias matcher. It records uncertainty and can surface
  watch candidates; it cannot supply semantic confirmation or qualify E01 alone.
- `gateway`: an HTTPS JSON protocol for a separately configured model gateway.
  Vendor translation and billing enforcement belong to that gateway. This is not
  a drop-in vendor API adapter.

Grouping uses normalized subject identity, the exact normalized catalyst excerpt
and an explicit time bucket. Category changes do not change episode identity.
Different claims about the same subject stay separate. Paraphrases/slang may
remain separate groups: this favors precision and is not semantic clustering.
Ambiguous aliases remain unconfirmed. Freeze extractor and episode policy during
a test; changing extraction behavior can change literal group boundaries.

Narrative strength and token linkage have separate features and thresholds.
All calibrated weights, priorities, aliases and thresholds are supplied privately.
They are evidence scores, not profit probabilities. Known copied content, repeated
authors, reposts/quotes and previously seen material do not create fresh independent
corroboration. Unknown coordination and origins outside captured data remain unknown.

Retrieval checks names, explicit cashtags, catalog aliases and exact mint references
inside the topic's cited context. It retains competing mints, ticker collisions and
older tokens. Timing supports relevance; launch age is not an exclusion. Lexical
and alias-only scores are capped below alert eligibility. Confirmation also requires
an unambiguous supported mint reference and sufficient narrative evidence.
Legacy mode reports semantic/media features unavailable. Rich mode supplies
separate semantic, visual, OCR and transcript features. Market size does not choose a winner.

A bounded, rebuildable normalized-metadata index retrieves a conservative superset
of plausible text matches inside PostgreSQL. The universe limit applies to that
retrieved set, not every token in the archive. Index warm-up or stale/missing index
rows keep producer health incomplete until caught up; no canonical token is admitted
by the search cache alone. Symbol/emoji-only interpretation remains unsupported.
The search projection is rebuildable; rebuild it when changing normalization or
the Node Unicode runtime. Freeze the runtime version for reproducible evaluation.

## Persistence and replay

Migration `0025_live_text_narratives` adds pinned configurations, immutable attempt,
result and report ledgers, and shared daily reservation counters. Artifacts,
episodes and interpretations reuse the existing research store. Reports preserve
feature breakdowns, policy identity and coverage flags.
Each attempt links both its reusable extraction identity and the exact runtime
configuration used, including the output/cost limits and grant in force for that call.

Live episodes may precede tokens. Later interpretations admit only canonical
tokens known at their availability time. New mints join the original episode;
score refreshes do not create another acquisition cycle. Open-universe paper
entries require an immutable candidate feature. Independent E00/E02 decisions and
abstentions can instead pin point-in-time canonical market discovery; they never
need to borrow a future NIT feature. Replay additionally requires a
`membershipFeatureSnapshotId` for a curated open-universe scenario, already available
at ex-ante discovery. A `market_census` universe instead pins canonical token discovery
and hashes its point-in-time record, so its baseline does not require prior NIT
selection. A future token cannot be admitted by either path. Captured universes still work.

New observations are matched against known tokens; new token discoveries reassess
active text groups. Changed assessments publish new interpretations; expired groups
publish empty candidate sets. Pending narrative buys check that the resolver has
consumed the latest producer signal before settlement. New live-text narrative
entries and unsettled buys additionally require fresh, complete producer health
for their pinned policy. An in-flight unknown observation, sustained backlog,
policy mismatch or expired producer heartbeat therefore cannot reuse an older
positive qualification. Historical replay is evaluated from frozen evidence and
does not depend on the live heartbeat. Protection and wallet-only
evaluation remain independent of inference. Scoring-only policy changes reuse
captured extraction, avoiding repeated model purchases for the same text.

## Running the worker

### Direct OpenAI text pilot

The text-only `OpenAITextInterpreter` implements the existing metered interpreter
contract directly using the [OpenAI Responses API](https://developers.openai.com/api/reference/resources/responses/methods/create).
No additional service or dependency is needed. It sends only the fixed extraction
instructions, the supplied source text and a strict output schema to
`https://api.openai.com/v1/responses`. It sends no wallets, positions, token
inventory, source URLs or observation identifiers. Requests disable response
storage (`store: false`); this is not a claim of zero provider retention.
There are no tools, redirects, automatic retries or fallback models.
Refusals, truncated output, model mismatches and invented citations fail closed.

Runtime JSON uses the existing `kind: "gateway"` shape with `provider: "openai"`,
`endpoint: "https://api.openai.com/v1/responses"`,
`allowedOrigin: "https://api.openai.com"`, and
`modelVersion: "gpt-5.4-mini-2026-03-17"`. Supply `OPENAI_API_KEY` in the environment;
the key is never pinned in database configuration. Local launchers prefer the
project's `.env` OpenAI key over an inherited key. The existing expiry,
external-context grant and daily request/cost reservations still apply.
This adapter rejects rich configuration and does not provide embeddings or media
interpretation. Other providers and rich mode continue using the JSON gateway.

Before sending, the adapter conservatively estimates maximum text cost from UTF-8
request bytes plus framing allowance and the output token limit. It requires this
estimate to fit the reserved charge. Rates are $0.75/$4.50 per million input/output
tokens, respectively, checked against the
[pinned model documentation](https://developers.openai.com/api/docs/models/gpt-5.4-mini)
on 2026-11-11. These are application estimates, not provider-enforced billing caps;
review pricing before a deployment. Failed or unknown calls retain their full
runtime reservation.

After building, run `pnpm local:narrative:pilot --check` for an offline prerequisite
check. `pnpm local:narrative:pilot --check-db` also configures the runtime and seeds
the disposable schema, then removes it without any model request.
`pnpm local:narrative:pilot --run` deliberately makes up to **three paid
requests** using synthetic support and contradiction observations. It requires an
explicit disposable `NIT_TEST_DATABASE_URL` on loopback, `OPENAI_API_KEY`, and
`FEATURE_EXECUTION=false`. It never uses the application database configuration.
The existing test harness migrates a fresh isolated schema and removes it after
the pilot; no synthetic data is added to live Supabase tables. The synthetic
calibration is test data, not a live trading policy. The daily reservation is
90,000 microdollars ($0.09), and each response is limited to 2,048 output tokens.
The runner prints only counts, results and estimated usage. Missing usage is
explicitly counted as unmetered requests, so a zero reported estimate does not
establish zero billing. A second cycle checks
extraction reuse without another purchase. This validates text API connectivity,
provenance persistence and signal publication; it does not validate paid X access,
rich semantic grouping, artwork, live narrative coverage or trading profitability.
Normal unit/integration tests never invoke this paid pilot.

Set `NIT_NARRATIVE_CONFIG_PATH` to private JSON conforming to
[the runtime schema](src/config.ts), then run `@nit/app-narrative-worker`.
Supply every policy value and explicit cycle/source/universe/episode/daily limit.
The database pool needs **at least two connections**: one session holds the
producer advisory lock while other statements use the remaining slots. Workers in
the same schema cannot overlap producer cycles.

Gateway mode additionally requires an unexpired `allowExternalContext: true` grant,
an operator-selected HTTPS endpoint/origin, model version, byte/output-token/cost
limits and `NIT_NARRATIVE_GATEWAY_BEARER_TOKEN`. The bearer is never persisted.
Only observation ID, untrusted text, the fixed prompt and limits leave the process.
No source URL is fetched. There is no telemetry or new external library dependency.

Request protocol:

```text
{schemaVersion:1, modelVersion, prompt, promptHash,
 untrustedSource:{id,text}, limits:{maxOutputTokens,maxChargeMicros}}
```

Response protocol:

```text
{modelVersion, promptHash, extraction:{schemaVersion:1, topics:[
 {subjectQuote,catalystQuote,contextQuote,
  category:"news|meme|endorsement|promotion|cto|other",
  stance:"support|contradiction|uncertain"}
]}}
```

Responses are bounded and identity/schema checked; redirects are refused.
Cancellation/deadlines bound local waiting even if an adapter ignores abort.
No live provider is enabled by installation or development tests.

Attempts reserve worst-case charges before invocation under a UTC-day budget
shared across policies. The gateway must enforce the per-request micro-USD ceiling;
this worker cannot enforce an arbitrary vendor's billing behavior. Reservations
measure conservative exposure, not actual billed usage or tokens. They are not
automatically refunded, including local/unknown failures. Failed, invalid or
interrupted attempts are recorded once and not automatically called again.
Overdue attempts without results recover as unknown. Deliberate retries require a
reviewed interpreter/prompt revision; weight changes and grant renewal do not
reset attempts or daily budgets.

## Coverage and acceptance

Missing interpretations, exhausted budgets or source/universe/episode overflow
make current assessments unavailable. More than 32 plausible tokens or 64 supporting
artifacts yields an unavailable report rather than a selected subset. Reports are
revised in bounded batches; producer health gates live-text narrative buys globally
while the resolver's watch expiry also bounds cached eligibility.
Monitor incompleteness and pause NIT entries during sustained backlog before a run.

Legacy mode supports text and reference metadata; rich mode inspects captured
frames/audio with OCR/transcript/visual descriptions returned by the gateway.
Missing supported content stays unavailable. Unknown copied-news origins, complete platform
coverage and authentic attention are not established. X author capture exists;
Telegram channel intake remains separate from the Telegram decision console.

Before the full-system 48-hour test, configure a real gateway, verify source and
venue coverage, measure latency/storage/load, and exercise restart, withdrawal
and outages. Configure the rich gateway and media/storage tools where enabled;
unsupported market modes still fail closed. This feature does not deploy the system.
