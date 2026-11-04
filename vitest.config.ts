import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['**/*.test.ts'],
    exclude: ['**/node_modules/**', '**/dist/**'],
    // The default 5s is a unit-test budget, and most of the suite is one. The
    // `.pg.test.ts` files are not: five of them run in parallel against one
    // PostgreSQL, each with its own schema and connection pool, so a statement
    // waits on the server's load as well as its own work. At 5s they pass alone
    // and fail as a suite — which makes the gate report the machine's mood
    // rather than the code's state. These bounds are still far below "hung":
    // a deadlocked statement or a lock nobody releases is caught in seconds,
    // not minutes. The pg suites already pass 30s explicitly to beforeAll for
    // schema setup; hookTimeout makes that the default rather than a per-call
    // remembering.
    testTimeout: 20_000,
    hookTimeout: 30_000,
  },
  resolve: {
    alias: {
      '@nit/domain': resolve(__dirname, 'packages/domain/src'),
      '@nit/config': resolve(__dirname, 'packages/config/src'),
      '@nit/events': resolve(__dirname, 'packages/events/src'),
      '@nit/signals': resolve(__dirname, 'packages/signals/src'),
      '@nit/trading': resolve(__dirname, 'packages/trading/src'),
      '@nit/agent-contracts': resolve(__dirname, 'packages/agent-contracts/src'),
      '@nit/database-public': resolve(__dirname, 'packages/database-public/src'),
      '@nit/queue': resolve(__dirname, 'packages/queue/src'),
      '@nit/observation': resolve(__dirname, 'packages/observation/src'),
      '@nit/market': resolve(__dirname, 'packages/market/src'),
      '@nit/wallet-intelligence': resolve(__dirname, 'packages/wallet-intelligence/src'),
      '@nit/observability': resolve(__dirname, 'packages/observability/src'),
      '@nit/provider-interfaces': resolve(__dirname, 'packages/provider-interfaces/src'),
      '@nit/composition': resolve(__dirname, 'packages/composition/src'),
    },
  },
});
