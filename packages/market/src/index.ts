/**
 * @nit/market - Market intelligence (TIS ch.9, EDS ch.21)
 *
 * Discovers token launches, normalizes them into the canonical token model,
 * persists them immutably, and publishes TokenDiscovered. Enrichment — artwork,
 * liquidity, creator history — runs afterwards and independently, off the three
 * jobs that commit with the token row.
 *
 * It performs no narrative matching (EDS ch.21 §3) and operates independently of
 * social intelligence (§2).
 */

export * from './types';
export * from './contracts';
export * from './normalize';
export * from './events';
export * from './repository';
export * from './unit-of-work';
export * from './service';
export * from './synthetic-source';
export * from './poll-handler';
export * from './enrichment-contracts';
export * from './enrichment-events';
export * from './enrichment-jobs';
export * from './enrichment-repository';
export * from './enrichment-handlers';
export * from './synthetic-enrichment';
export * from './lifecycle';
export * from './reconciliation';
export * from './reconciliation-handler';
export * from './solana-rpc';
export * from './pump-events';
export * from './confirmed-ingestion';
export * from './reconciliation-scheduler';
export * from './execution-facts';
export * from './execution-refresh';
export * from './facts-rpc';
export * from './market-recovery';
