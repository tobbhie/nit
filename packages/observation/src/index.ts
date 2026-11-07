/**
 * @nit/observation - Observation ingestion (TIS ch.4)
 *
 * Acquires posts from monitored sources, normalizes them into the canonical
 * observation model, persists them immutably, and publishes ObservationCreated.
 * It performs no narrative analysis (TIS ch.4 §6).
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
export * from './x-source';
