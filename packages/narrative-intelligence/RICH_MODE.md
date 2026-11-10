# Rich narrative evidence

The narrative worker supports a versioned rich gateway alongside its legacy text
protocol. This is a bounded evidence producer; it has no trading-agent tools,
position access, signer or policy mutation capability. Models interpret inputs;
deterministic code groups evidence, scores competing candidates and gates signals.

## Configuration and protocol

Set `policy.semantics` with a pinned embedding `modelVersion`, `dimensions`,
`mergeBps`, `ambiguityMarginBps`, `retrievalBps` and `visualMatchBps`. Supply
`interpreter.rich` with the same model/dimensions. Expanded optional `linkWeights`
are `semantic`, `visual`, `ocr` and `transcript`; all positive weights together
must still total 10000. All calibration stays in private configuration.

The rich gateway receives schema version 2, the rich prompt/hash, explicit purpose
(`observation` or `token_representation`), embedding model/dimensions, untrusted
text and optional `metadataDocument`/media parts. It must use that embedding
service; language-model-invented numeric vectors are not valid embeddings.
Vendor routing, embedding generation, OCR/transcription/vision model selection
and actual charge enforcement belong to the operator's gateway.

Response `extraction` uses schemaVersion 2 and retains the original quoted topic
fields. Each topic also has `meaning: {subject,event,modelVersion,vector}` and may
cite an `evidenceId`. `representation: {modelVersion,vector}` embeds the complete
input meaning (text plus available OCR/transcript/visual context). Token requests
return **empty topics** and the token representation; token metadata is never
published as a social narrative. The gateway receives one token's descriptive
material without a competing universe, portfolio, prices or creator inventory.

Optional `mediaEvidence` entries contain `{id,mediaId,sha256,modality,text,startMs,
endMs}`. Modality is `ocr`, `transcript` or `visual`; only transcripts carry
non-null bounded timestamps. Topics cite exact contiguous quotes inside one
segment. Contradictory modalities stay visible. A model-created visual summary is
inference, never silently relabeled as OCR or raw source text. Local code adds
`mediaRepresentations` and captured `tokenMetadata`; the gateway may not supply
those reserved fields. Payloads, topic counts and response sizes are bounded.

## Semantic history and matching

Catalog aliases take precedence; conflicting or ambiguous identities stay pending.
A claim joins a fixed existing anchor only with sufficient semantic agreement in
the same configured time bucket. Near-equal competing anchors create an ambiguous
branch instead of a transitive bridge. Divergent claims create separate branches.
Existing anchors and memberships never move. Model/version, compared node IDs,
similarities, relations and availability remain in immutable PostgreSQL ledgers.
A later matching episode inside the configured lookback links to its prior
episode and retains narrative identity when that earlier episode was published.
These are attribution hypotheses, not verified cultural origins. This implements
conservative source convergence/branching rather than unrestricted rewriting of
previously published narrative graphs.

Retrieval combines literal metadata, pinned semantic vectors and captured artwork
hashes. It uses relational JSON arrays and bit operations, without a vector
extension. All plausible competing mints above inclusion remain visible. Semantic
or perceptual similarity alone is capped below alert eligibility. High linkage
needs a supported raw-text mint reference or exact source/artwork bytes/frame
identity plus sufficient narrative support. Matching artwork establishes image
reuse, not official endorsement. Shared original/frame hashes do not multiply
independent corroboration. Perceptual features remain distinguishable from exact
identity. Lexical and temporal evidence do not establish cultural origin.

One token representation is reserved and processed per cycle, before observation
work, under the same producer lock and global request/cost budget. Attempts pin
input/runtime identities and have one terminal outcome. Timeouts, invalid replies
and crash-unknown attempts are not automatically purchased again. New tokens
trigger reassessment without reinterpreting social observations. Strict rich mode
remains incomplete until the known canonical registry has captured token
representations. Ingestion exceeding enrichment throughput blocks new narrative
entries, rather than silently searching a truncated archive. Measure warm-up/load
and monitor `nit_token_interpretations` before a live run.

## Media processing and durability

Enable `interpreter.rich.media` and `allowExternalMedia: true`. The existing
external-context expiry applies. Media config requires approved HTTPS origins,
byte/duration/frame limits, frame interval, absolute `scratchRoot`, absolute
`ffmpegPath`/`ffprobePath`, subprocess timeout, scratch soft/hard byte limits and
maximum age. Optional `ipfsGatewayOrigin` must be an approved origin. Only explicit
IPFS CIDs/paths map to that HTTPS gateway. Redirects, embedded credentials,
arbitrary protocols and undeclared origins fail.

FFmpeg/ffprobe are external runtime dependencies. They decode local files through
argument arrays without a shell, with network protocols disabled, bounded
dimensions/output/time, one decoder at a time and one thread per codec. Video is
sampled into at most 20 bounded WebP frames; audio is normalized to 16 kHz mono.
Original SHA-256, frame hashes, sample target times, decoder versions and a
versioned difference hash are captured. Perceptual comparisons require matching
decoder identities. Sparse frames do not establish complete scene/video coverage;
crop recognition and semantic correctness are not guaranteed. Freeze binaries,
models and policy for an experiment; replay consumes captured outputs.

Only configured source origins receive media/metadata GET requests, without the
model bearer. When granted, the model gateway receives bounded normalized
frames/audio, source text, captured metadata and processing limits. No telemetry,
vendor SDK or new npm dependency is added. Local validation used a checksum-verified
Windows binary from a build provider linked by
[FFmpeg](https://ffmpeg.org/download.html), kept in ignored development tooling under `node_modules`. No
binary is committed or installed system-wide.

Private R2 configuration supplies `{endpoint,bucket,maxUploadAttempts,timeoutMs}`.
Credentials come only from `NIT_R2_ACCESS_KEY_ID`/`NIT_R2_SECRET_ACCESS_KEY`; they are
not persisted. The archive uses S3 SigV4 PUT/HEAD and verifies hash metadata and
byte length. No public or signed read URLs are created. Original upload starts
concurrently with analysis. Originals stay local until upload succeeds or age/
disk cleanup explicitly records loss. Independent durability/local states live
in `nit_media_objects`. Restart recovery retries one upload with bounded delays,
preferring the retained original and otherwise requiring an identical-hash
redownload. Archive recovery never redoes inference. R2 latency/outage does not
delay sufficient narrative evidence. New media pauses under scratch pressure;
cleanup preserves currently decoding directories. Host provisioning and live
gateway/R2 acceptance remain operator work.

Set `NIT_TEST_FFMPEG_PATH` and `NIT_TEST_FFPROBE_PATH` to pinned binaries to include
the real decoder integration cases. Without them those cases explicitly skip.
Tests generate synthetic images/video/audio and use mocked source/gateway/archive
HTTP. No real model, X account or R2 bucket is called.

Protocol references: [X media dictionary](https://docs.x.com/x-api/fundamentals/data-dictionary),
[R2 S3 API](https://developers.cloudflare.com/r2/api/s3/),
[PostgreSQL bit operations](https://www.postgresql.org/docs/current/functions-bitstring.html).
