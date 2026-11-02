/**
 * Domain enumerations
 */

export enum AggregateType {
  OBSERVATION = 'observation',
  NARRATIVE = 'narrative',
  CATALYST = 'catalyst',
  TOKEN = 'token',
  MARKET_EVENT = 'market_event',
  ALERT = 'alert',
  SIGNAL = 'signal',
  STRATEGY_RUN = 'strategy_run',
  PORTFOLIO = 'portfolio',
  ORDER = 'order',
  POSITION = 'position',
}

export enum EventType {
  OBSERVATION_CREATED = 'observation.created',
  NARRATIVE_INGESTED = 'narrative.ingested',
  NARRATIVE_SCORED = 'narrative.scored',
  NARRATIVE_MATCHED = 'narrative.matched',
  CATALYST_DETECTED = 'catalyst.detected',
  ALERT_TRIGGERED = 'alert.triggered',
  ALERT_DELIVERED = 'alert.delivered',
  /**
   * Market intelligence, per TIS ch.9 §"Domain Events". Only the events this
   * stage actually emits are declared: `TokenOutcomeUpdated` belongs to
   * backtesting (build order step 11) and an event type nothing can publish is
   * a contract consumers would write dead branches against.
   */
  TOKEN_DISCOVERED = 'token.discovered',
  /**
   * A token left one market venue for another — in practice a pump.fun bonding
   * curve completing and the asset moving to PumpSwap.
   *
   * Its own event rather than a second `TOKEN_DISCOVERED`, because the two say
   * different things: discovery announces an asset this system had never seen,
   * and a consumer that has already acted on a token would act again on being
   * told it was discovered. Graduation announces that a token it already knows
   * now trades somewhere else, which is a change of venue rather than of
   * identity — token identity stays `(chain, address)` across the move.
   */
  TOKEN_GRADUATED = 'token.graduated',
  TOKEN_ARTWORK_PROCESSED = 'token.artwork_processed',
  CREATOR_PROFILE_UPDATED = 'token.creator_profile_updated',
  LIQUIDITY_UPDATED = 'token.liquidity_updated',
  MARKET_SNAPSHOT_CREATED = 'token.market_snapshot_created',
  SIGNAL_PUBLISHED = 'signal.published',
}

/**
 * Observation sources, per TIS ch.4 §2. X is the MVP source; Telegram is
 * declared now because the interface has to be provider-shaped from the start,
 * and a single-value enum invites code that hardcodes the one value.
 */
export enum ObservationProvider {
  X = 'x',
  TELEGRAM = 'telegram',
}

/**
 * Media modalities carried by an observation (TIS ch.2 §4, ch.5).
 */
export enum MediaType {
  IMAGE = 'image',
  VIDEO = 'video',
  AUDIO = 'audio',
}

/**
 * Chains a token can live on.
 *
 * An enum rather than a free string because token identity is `(chain, address)`
 * (see the `tokens` migration): were `chain` opaque text, one adapter writing
 * `Solana` and another writing `solana` would silently fork one asset into two
 * and split its evidence — the exact failure the composite key exists to prevent.
 * One member because every launch source in TIS ch.9 is Solana; the constraint is
 * the point, not the count.
 */
export enum Chain {
  SOLANA = 'solana',
}

/**
 * Where a token launch was observed, per TIS ch.9 §"Pipeline".
 *
 * A venue, not an identity: PumpSwap is where a pump.fun token goes on
 * graduating, so one token is legitimately seen on both. Token identity is
 * `(chain, address)` for that reason, and the launch source records which feed
 * surfaced it first.
 */
export enum LaunchSource {
  PUMP_FUN = 'pump_fun',
  PUMPSWAP = 'pumpswap',
}

/**
 * Where a token actually trades at a given moment.
 *
 * Distinct from `LaunchSource`, which records the feed that first surfaced a
 * launch and is immutable once written. A venue is a fact with a lifetime: a
 * pump.fun token trades on its bonding curve, then on PumpSwap, and asking
 * "where does this token trade" has a different answer depending on when it is
 * asked. `token_markets` stores those answers as intervals, and this enum names
 * them.
 *
 * The values overlap with `LaunchSource` without being the same set, which is
 * the reason for a second enum rather than a reused one. `pump_fun` as a launch
 * source means "the pump.fun feed told us"; the bonding curve is the venue, and
 * naming it as such is what stops a graduated token's history from reading as
 * though it were still on the curve.
 */
export enum MarketVenue {
  PUMP_FUN_BONDING_CURVE = 'pump_fun_bonding_curve',
  PUMPSWAP = 'pumpswap',
}

/**
 * How far a token has progressed through its market lifecycle.
 *
 * Derived from a token's venue intervals rather than stored: `tokens` is
 * append-only, so a column here could never be advanced, and EDS ch.27 §2 puts
 * mutable projections downstream of immutable history in any case. See
 * `deriveTokenLifecycle` in `@nit/market`.
 *
 * There is deliberately no separate `PUMPSWAP_ACTIVE`: a graduated pump.fun
 * token *is* the one trading on PumpSwap, so the two would be a single state
 * under two names, and every consumer would have to match both to be correct.
 */
export enum TokenLifecycleState {
  /**
   * Persisted, but with no venue interval yet. The honest state for a token
   * discovered from a feed that did not say where it trades.
   */
  DISCOVERED = 'discovered',
  BONDING_CURVE_ACTIVE = 'bonding_curve_active',
  GRADUATED = 'graduated',
}

/**
 * Job lifecycle states, per TIS ch.3 §5.
 *
 * There is deliberately no `failed` state: a retryable failure returns the job
 * to PENDING with a backoff deadline, so "retrying" is a pending job whose
 * available_at is in the future. A state that nothing can ever write is worse
 * than no state, because callers write guards against it and those guards are
 * never exercised.
 */
export enum JobStatus {
  PENDING = 'pending',
  LEASED = 'leased',
  RUNNING = 'running',
  COMPLETED = 'completed',
  DEAD_LETTER = 'dead_letter',
}

export enum QueueName {
  OBSERVATION_POLL = 'observation_poll',
  OBSERVATION_INGEST = 'observation_ingest',
  MEDIA_PROCESSING = 'media_processing',
  /**
   * Market intelligence. Discovery is one queue; the three enrichment queues are
   * separate because TIS ch.9 runs them in parallel off a persisted token, and a
   * shared queue would serialise them behind whichever provider is slowest —
   * artwork waits on a download, liquidity on an RPC.
   */
  TOKEN_DISCOVERY_POLL = 'token_discovery_poll',
  TOKEN_RECONCILIATION = 'token_reconciliation',
  TOKEN_ARTWORK = 'token_artwork',
  TOKEN_LIQUIDITY = 'token_liquidity',
  TOKEN_CREATOR = 'token_creator',
  INGESTION = 'ingestion',
  SCORING = 'scoring',
  MATCHING = 'matching',
  ALERTING = 'alerting',
  DELIVERY = 'delivery',
}

export enum Environment {
  DEVELOPMENT = 'development',
  STAGING = 'staging',
  PRODUCTION = 'production',
}
