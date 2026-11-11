import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const require = createRequire(import.meta.url);
const mode = process.argv[2];
let stage = 'configuration';
async function main() {
  if (process.argv.length !== 3 || !['--check', '--check-db', '--run', '--execute'].includes(mode))
    throw new Error('Use --check or --run');
  // The explicitly configured project key must not be shadowed by an inherited key.
  delete process.env.OPENAI_API_KEY;
  process.loadEnvFile(resolve(root, '.env'));
  const testUrl = new URL(process.env.NIT_TEST_DATABASE_URL ?? '');
  if (!['localhost', '127.0.0.1', '[::1]'].includes(testUrl.hostname))
    throw new Error('Pilot requires a disposable loopback test database');
  if (!process.env.OPENAI_API_KEY?.trim()) throw new Error('OpenAI key missing');
  if (process.env.FEATURE_EXECUTION !== 'false') throw new Error('Disable trading first');
  const {
    OpenAITextInterpreter,
    OPENAI_NARRATIVE_ENDPOINT,
    OPENAI_NARRATIVE_MODEL,
    LiveNarrativeRuntime,
  } = require('../packages/narrative-intelligence/dist');
  if (mode === '--check') {
    console.log(
      JSON.stringify({
        ready: true,
        maxRequests: 3,
        reservedCostMicros: 90000,
        syntheticOnly: true,
        loopbackDatabase: true,
      })
    );
    return;
  }
  if (mode === '--run') {
    if (process.env.NODE_EXTRA_CA_CERTS)
      process.env.NODE_EXTRA_CA_CERTS = resolve(root, process.env.NODE_EXTRA_CA_CERTS);
    // Reload certificate trust at process startup, without exposing environment values.
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url), '--execute'], {
      cwd: root,
      env: process.env,
      stdio: 'inherit',
      shell: false,
      windowsHide: true,
    });
    const interrupt = () => child.kill('SIGINT');
    process.on('SIGINT', interrupt);
    process.on('SIGTERM', interrupt);
    child.once('error', () => {
      process.exitCode = 1;
    });
    child.once('exit', (code) => {
      process.exitCode = code ?? 1;
    });
    return;
  }
  stage = 'load-test-harness';
  const { createTestDatabase } = require('../packages/database-public/dist/testing');
  stage = 'create-isolated-schema';
  const harness = await createTestDatabase('openai_pilot');
  try {
    async function rows(sql, values = []) {
      const result = await harness.db.query(sql, values);
      if (result.kind === 'failure') {
        const failure = new Error('Pilot database operation failed');
        failure.code = result.error.code;
        throw failure;
      }
      return result.value;
    }
    // Explicit synthetic calibration; never use these values as a live policy.
    const MINT_A = '22222222222222222222222222222222';
    const config = {
      policy: {
        version: 'synthetic-openai-pilot-v1',
        subjects: [{ id: 'nova', label: 'Nova', aliases: ['Nova'] }],
        sourcePriority: {},
        episodeWindowMs: 3600000,
        attentionWindowMs: 3600000,
        watchDurationMs: 3600000,
        temporalWindowMs: 3600000,
        minimumIndependentSources: 2,
        sourceSaturation: 2,
        persistenceSaturationMs: 1000,
        narrativeThresholdBps: 5000,
        inclusionBps: 2000,
        alertBps: 7000,
        lexicalOnlyCapBps: 4000,
        narrativeWeights: { independence: 10000, persistence: 0, priority: 0 },
        linkWeights: {
          lexical: 2000,
          identity: 1000,
          directReference: 6000,
          temporal: 1000,
          contradiction: 10000,
        },
      },
      intervalMs: 1000,
      observationsPerCycle: 8,
      sourceLimit: 32,
      universeLimit: 100,
      episodeLimit: 16,
    };
    const now = new Date();
    config.policy.version = 'synthetic-openai-pilot-v1';
    config.policy.sourcePriority = { 'x:pilot-a': 10000, 'x:pilot-b': 10000, 'x:pilot-c': 10000 };
    config.timeoutMs = 30000;
    config.maxRequestsPerDay = 3;
    config.maxCostMicrosPerDay = 90000;
    config.interpreter = {
      kind: 'gateway',
      provider: 'openai',
      endpoint: OPENAI_NARRATIVE_ENDPOINT,
      allowedOrigin: 'https://api.openai.com',
      modelVersion: OPENAI_NARRATIVE_MODEL,
      allowExternalContext: true,
      expiresAt: new Date(Date.now() + 10 * 60000).toISOString(),
      maxInputBytes: 16000,
      maxOutputTokens: 2048,
      maxChargeMicros: 30000,
    };
    stage = 'configure-runtime';
    const interpreter = new OpenAITextInterpreter({
      ...config.interpreter,
      bearer: process.env.OPENAI_API_KEY.trim(),
    });
    const runtime = new LiveNarrativeRuntime(harness.db, interpreter, config);
    stage = 'seed-token';
    await rows(
      `INSERT INTO tokens(token_id,chain,address,ticker,name,launch_time,launch_source,discovered_at,raw_payload)
      VALUES ($1,'solana',$2,'NOVA','Nova',$3,'pump_fun',$4,'{}')`,
      [randomUUID(), MINT_A, new Date(now.getTime() - 2000), now]
    );
    const texts = [
      `Nova festival begins today. Organizers announced an opening parade. The associated token mint is ${MINT_A}.`,
      `Nova festival begins today. A local reporter confirms the venue is open. The associated token mint is ${MINT_A}.`,
      `The claim "Nova festival begins today" is false. Organizers have cancelled it. The associated token mint is ${MINT_A}.`,
    ];
    stage = 'seed-observations';
    for (const [i, text] of texts.entries()) {
      await rows(
        `INSERT INTO observations(observation_id,provider,provider_post_id,author_id,published_at,ingested_at,text,raw_payload)
        VALUES ($1,'x',$2,$3,$4,$4,$5,'{"synthetic":true}')`,
        [randomUUID(), randomUUID(), `pilot-${'abc'[i]}`, now, text]
      );
    }
    if (mode === '--check-db') {
      console.log(
        JSON.stringify({
          isolatedDatabaseReady: true,
          runtimeReady: true,
          syntheticInputs: texts.length,
          apiRequests: 0,
        })
      );
      return;
    }
    stage = 'narrative-cycle';
    const cycle = await runtime.cycle();
    const beforeReplay = interpreter.getStats();
    const replay = await runtime.cycle();
    const results = await rows(
      'SELECT status,count(*)::int AS count FROM nit_text_results GROUP BY status'
    );
    const stances = await rows(`SELECT topic->>'stance' AS stance,count(*)::int AS count
      FROM nit_text_results r CROSS JOIN LATERAL jsonb_array_elements(r.payload->'extraction'->'topics') topic
      WHERE r.status='captured' GROUP BY topic->>'stance'`);
    const signals = await rows('SELECT count(*)::int AS count FROM signals');
    const orders = await rows('SELECT count(*)::int AS count FROM orders');
    const budgets = await rows(
      'SELECT requests::int,reserved_cost_micros::int FROM nit_text_budgets'
    );
    const captured = results.find((r) => r.status === 'captured')?.count ?? 0;
    const passed =
      captured === 3 &&
      stances.some((r) => r.stance === 'contradiction' && r.count > 0) &&
      cycle.failed === 0 &&
      signals[0].count > 0 &&
      orders[0].count === 0 &&
      replay.claimed === 0 &&
      interpreter.getStats().requests === beforeReplay.requests;
    console.log(
      JSON.stringify({
        passed,
        syntheticOnly: true,
        cycle,
        replay,
        results,
        stances,
        signalCount: signals[0].count,
        orderCount: orders[0].count,
        budgets,
        api: interpreter.getStats(),
      })
    );
    if (!passed) process.exitCode = 1;
  } finally {
    await harness.teardown();
    console.log(JSON.stringify({ isolatedPilotSchemaRemoved: true }));
  }
}
main().catch((error) => {
  console.error(
    JSON.stringify({
      failedStage: stage,
      code: /^[A-Z0-9_]{2,30}$/.test(error?.code ?? '') ? error.code : null,
    })
  );
  console.error(
    'Narrative pilot failed; configuration, provider bodies and credentials omitted. No automatic retry.'
  );
  process.exitCode = 1;
});
