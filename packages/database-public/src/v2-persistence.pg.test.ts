import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTestDatabase, hasTestDatabase, TestDatabase } from './testing';

describe.skipIf(!hasTestDatabase())('V2 persistence against Postgres', () => {
  let harness: TestDatabase;

  const ids = {
    signal: randomUUID(),
    entry: randomUUID(),
    exit: randomUUID(),
    protection: randomUUID(),
    risk: randomUUID(),
    combination: randomUUID(),
    run: randomUUID(),
    portfolio: randomUUID(),
    decision: randomUUID(),
    order: randomUUID(),
    fill: randomUUID(),
    position: randomUUID(),
  };
  const codeHash = 'b'.repeat(64);
  const configHash = 'a'.repeat(64);

  const requireSuccess = async (query: string, params: unknown[] = []): Promise<void> => {
    const result = await harness.db.query(query, params);
    if (result.kind === 'failure') {
      throw result.error;
    }
  };

  beforeAll(async () => {
    harness = await createTestDatabase('v2_persistence');

    await requireSuccess(
      `INSERT INTO signals (
         signal_id, source, source_version, kind, chain, token_mint,
         occurred_at, observed_at, available_at, origin_kind,
         provider, reference_id, origin_key, payload
       ) VALUES ($1, 'wallet_tracker', '1', 'wallet_buy', 'solana', 'mint-one',
         NOW(), NOW(), NOW(), 'provider', 'fixture', 'source-1', 'fixture:source-1', '{}'::jsonb)`,
      [ids.signal]
    );

    await requireSuccess(
      `INSERT INTO strategy_entry_versions
         (entry_version_id, entry_id, version, config, code_hash)
       VALUES ($1, 'E01', 1, '{}'::jsonb, $2)`,
      [ids.entry, codeHash]
    );
    await requireSuccess(
      `INSERT INTO strategy_exit_versions
         (exit_version_id, exit_id, version, config, code_hash, compatible_entry_ids)
       VALUES ($1, 'X01', 1, '{}'::jsonb, $2, ARRAY['E01'])`,
      [ids.exit, codeHash]
    );
    await requireSuccess(
      `INSERT INTO strategy_protection_versions
         (protection_version_id, policy_id, version, config, code_hash)
       VALUES ($1, 'P01', 1, '{}'::jsonb, $2)`,
      [ids.protection, codeHash]
    );
    await requireSuccess(
      `INSERT INTO strategy_risk_versions
         (risk_version_id, policy_id, version, config, code_hash)
       VALUES ($1, 'R01', 1, '{}'::jsonb, $2)`,
      [ids.risk, codeHash]
    );
    await requireSuccess(
      `INSERT INTO strategy_combinations (
         combination_id, entry_version_id, exit_version_id,
         protection_version_id, risk_version_id, display_id, config_hash,
         compatibility_status
       ) VALUES ($1, $2, $3, $4, $5, 'E01.v1/X01.v1/P01.v1/R01.v1', $6, 'compatible')`,
      [ids.combination, ids.entry, ids.exit, ids.protection, ids.risk, configHash]
    );
    await requireSuccess(
      `INSERT INTO strategy_runs (
         run_id, combination_id, mode, config_hash, code_commit_sha,
         period_start, period_end, status
       ) VALUES ($1, $2, 'historical', $3, 'fixture-sha', NOW(), NOW(), 'planned')`,
      [ids.run, ids.combination, configHash]
    );
    await requireSuccess(
      `INSERT INTO paper_portfolios (
         portfolio_id, run_id, combination_id, cash_asset,
         initial_cash, current_cash, capital_constraints
       ) VALUES ($1, $2, $3, 'SOL', '10.000000000000000001',
         '10.000000000000000001', '{}'::jsonb)`,
      [ids.portfolio, ids.run, ids.combination]
    );
    await requireSuccess(
      `INSERT INTO strategy_decisions (
         decision_id, run_id, portfolio_id, combination_id, source_signal_id,
         decision_type, risk_gate_results, idempotency_key, decided_at
       ) VALUES ($1, $2, $3, $4, $5, 'enter', '{}'::jsonb, 'decision-1', NOW())`,
      [ids.decision, ids.run, ids.portfolio, ids.combination, ids.signal]
    );
    await requireSuccess(
      `INSERT INTO orders (
         order_id, decision_id, run_id, portfolio_id, combination_id,
         token_mint, side, quantity_asset, quantity, quote_id,
         idempotency_key, created_at
       ) VALUES ($1, $2, $3, $4, $5, 'mint-one', 'buy', 'SOL',
         '0.100000000000000001', 'quote-1', 'order-1', NOW())`,
      [ids.order, ids.decision, ids.run, ids.portfolio, ids.combination]
    );
    await requireSuccess(
      `INSERT INTO order_fills (
         fill_id, order_id, status, input_asset, input_quantity,
         output_asset, output_quantity, fees, venue, recorded_at
       ) VALUES ($1, $2, 'filled', 'SOL', '0.100000000000000001',
         'mint-one', '123.456789012345678901', '[]'::jsonb, 'paper', NOW())`,
      [ids.fill, ids.order]
    );
    await requireSuccess(
      `INSERT INTO positions (
         position_id, portfolio_id, token_mint, original_quantity, open_quantity,
         cost_basis, high_water_price, state, protection_config, updated_at
       ) VALUES ($1, $2, 'mint-one', '123.456789012345678901',
         '123.456789012345678901', '0.100000000000000001',
         '0.000810000000000000', 'open', '{}'::jsonb, NOW())`,
      [ids.position, ids.portfolio]
    );
  }, 30_000);

  afterAll(async () => {
    await harness.teardown();
  });

  it('deduplicates an exact source occurrence but preserves distinct events for one mint', async () => {
    const duplicate = await harness.db.query(
      `INSERT INTO signals (
         signal_id, source, source_version, kind, chain, token_mint,
         occurred_at, observed_at, available_at, origin_kind,
         provider, reference_id, origin_key, payload
       ) VALUES ($1, 'wallet_tracker', '1', 'wallet_buy', 'solana', 'mint-one',
         NOW(), NOW(), NOW(), 'provider', 'fixture', 'source-1', 'fabricated-key', '{}'::jsonb)`,
      [randomUUID()]
    );
    expect(duplicate.kind).toBe('failure');

    await requireSuccess(
      `INSERT INTO signals (
         signal_id, source, source_version, kind, chain, token_mint,
         occurred_at, observed_at, available_at, origin_kind,
         provider, reference_id, origin_key, payload
       ) VALUES ($1, 'wallet_tracker', '1', 'wallet_buy', 'solana', 'mint-one',
         NOW(), NOW(), NOW(), 'provider', 'fixture', 'source-2', 'fixture:source-2', '{}'::jsonb)`,
      [randomUUID()]
    );

    const count = await harness.db.query<{ count: string }>(
      "SELECT COUNT(*) AS count FROM signals WHERE token_mint = 'mint-one'"
    );
    expect(count).toEqual({ kind: 'success', value: [{ count: '2' }] });
  });

  it('rejects time travel and mutation in the signal history', async () => {
    const outOfOrder = await harness.db.query(
      `INSERT INTO signals (
         signal_id, source, source_version, kind, chain, token_mint,
         occurred_at, observed_at, available_at, origin_kind,
         provider, reference_id, origin_key, payload
       ) VALUES ($1, 'market', '1', 'market', 'solana', 'mint-time',
         NOW(), NOW() - INTERVAL '1 second', NOW(), 'provider',
         'fixture', 'source-time', 'fixture:source-time', '{}'::jsonb)`,
      [randomUUID()]
    );
    expect(outOfOrder.kind).toBe('failure');

    const rewritten = await harness.db.query(
      "UPDATE signals SET token_mint = 'rewritten' WHERE signal_id = $1",
      [ids.signal]
    );
    expect(rewritten.kind).toBe('failure');
  });

  it('keeps policy definitions immutable and refuses incompatible or live runs', async () => {
    const rewritten = await harness.db.query(
      "UPDATE strategy_entry_versions SET config = '{\"changed\":true}'::jsonb WHERE entry_version_id = $1",
      [ids.entry]
    );
    expect(rewritten.kind).toBe('failure');

    const live = await harness.db.query(
      `INSERT INTO strategy_runs (
         run_id, combination_id, mode, config_hash, code_commit_sha,
         period_start, period_end, status
       ) VALUES ($1, $2, 'live', $3, 'fixture-sha', NOW(), NOW(), 'planned')`,
      [randomUUID(), ids.combination, configHash]
    );
    expect(live.kind).toBe('failure');

    const incompatibleEntry = randomUUID();
    const incompatibleCombination = randomUUID();
    await requireSuccess(
      `INSERT INTO strategy_entry_versions
         (entry_version_id, entry_id, version, config, code_hash)
       VALUES ($1, 'E02', 1, '{}'::jsonb, $2)`,
      [incompatibleEntry, codeHash]
    );
    await requireSuccess(
      `INSERT INTO strategy_combinations (
         combination_id, entry_version_id, exit_version_id,
         protection_version_id, risk_version_id, display_id, config_hash,
         compatibility_status, incompatibility_reason_codes
       ) VALUES ($1, $2, $3, $4, $5, 'E02.v1/X01.v1/P01.v1/R01.v1', $6,
         'incompatible', ARRAY['EXIT_REQUIRES_E01'])`,
      [
        incompatibleCombination,
        incompatibleEntry,
        ids.exit,
        ids.protection,
        ids.risk,
        'c'.repeat(64),
      ]
    );
    const incompatibleRun = await harness.db.query(
      `INSERT INTO strategy_runs (
         run_id, combination_id, mode, config_hash, code_commit_sha,
         period_start, period_end, status
       ) VALUES ($1, $2, 'historical', $3, 'fixture-sha', NOW(), NOW(), 'planned')`,
      [randomUUID(), incompatibleCombination, 'c'.repeat(64)]
    );
    expect(incompatibleRun.kind).toBe('failure');

    const mislabeledCombination = await harness.db.query(
      `INSERT INTO strategy_combinations (
         combination_id, entry_version_id, exit_version_id,
         protection_version_id, risk_version_id, display_id, config_hash,
         compatibility_status
       ) VALUES ($1, $2, $3, $4, $5, 'E02.v1/X01.v1/P01.v1/R01.v1', $6,
         'compatible')`,
      [
        randomUUID(),
        incompatibleEntry,
        ids.exit,
        ids.protection,
        ids.risk,
        'd'.repeat(64),
      ]
    );
    expect(mislabeledCombination.kind).toBe('failure');

    const changedScope = await harness.db.query(
      "UPDATE strategy_runs SET mode = 'forward_paper' WHERE run_id = $1",
      [ids.run]
    );
    expect(changedScope.kind).toBe('failure');
  });

  it('allows only one authoritative portfolio for a run and combination', async () => {
    const duplicate = await harness.db.query(
      `INSERT INTO paper_portfolios (
         portfolio_id, run_id, combination_id, cash_asset,
         initial_cash, current_cash, capital_constraints
       ) VALUES ($1, $2, $3, 'SOL', 5, 5, '{}'::jsonb)`,
      [randomUUID(), ids.run, ids.combination]
    );
    expect(duplicate.kind).toBe('failure');

    const changedAuthority = await harness.db.query(
      "UPDATE paper_portfolios SET initial_cash = 20 WHERE portfolio_id = $1",
      [ids.portfolio]
    );
    expect(changedAuthority.kind).toBe('failure');
  });

  it('preserves exact decimal values and scopes idempotency to the portfolio', async () => {
    const balances = await harness.db.query<{ initial_cash: string; quantity: string }>(
      `SELECT p.initial_cash::text, o.quantity::text
         FROM paper_portfolios p
         JOIN orders o ON o.portfolio_id = p.portfolio_id
        WHERE p.portfolio_id = $1`,
      [ids.portfolio]
    );
    expect(balances).toEqual({
      kind: 'success',
      value: [
        {
          initial_cash: '10.000000000000000001',
          quantity: '0.100000000000000001',
        },
      ],
    });

    const duplicateDecision = await harness.db.query(
      `INSERT INTO strategy_decisions (
         decision_id, run_id, portfolio_id, combination_id, source_signal_id,
         decision_type, risk_gate_results, idempotency_key, decided_at
       ) VALUES ($1, $2, $3, $4, $5, 'abstain', '{}'::jsonb, 'decision-1', NOW())`,
      [randomUUID(), ids.run, ids.portfolio, ids.combination, ids.signal]
    );
    expect(duplicateDecision.kind).toBe('failure');
  });

  it('keeps fills immutable while permitting the reconstructable position projection to advance', async () => {
    const rewrittenFill = await harness.db.query(
      'UPDATE order_fills SET output_quantity = 1 WHERE fill_id = $1',
      [ids.fill]
    );
    expect(rewrittenFill.kind).toBe('failure');

    const projected = await harness.db.query(
      `UPDATE positions
          SET open_quantity = '24.691357802469135780', state = 'moonbag', updated_at = NOW()
        WHERE position_id = $1`,
      [ids.position]
    );
    expect(projected.kind).toBe('success');
  });
});
