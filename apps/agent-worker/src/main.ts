import { readFile, stat } from 'node:fs/promises';
import { extname } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { bootstrap } from '@nit/composition';
import { failure } from '@nit/domain';
import {
  AgentConfiguration,
  AgentRuntime,
  JsonGatewayModel,
  gatewayIdentity,
  loadAgentGuides,
  ReadOnlyAgentTools,
} from '@nit/agent-runtime';
import { PaperMarketFeed, PostgresPaperMarketFeed } from '@nit/trading';

async function json(path: string | undefined): Promise<unknown> {
  if (
    path === undefined ||
    extname(path).toLowerCase() !== '.json' ||
    (await stat(path)).size > 1048576
  )
    throw new Error('Configure bounded private JSON agent files');
  return JSON.parse(await readFile(path, 'utf8')) as unknown;
}
async function main(): Promise<void> {
  const config = (await json(process.env['NIT_AGENT_CONFIG_PATH'])) as {
    agent: AgentConfiguration;
    approvedMarketSources: string[];
    cashAsset: string;
    pollIntervalMs: number;
    gateway?: { endpoint: string; allowedOrigin: string };
  };
  const command = (await json(process.env['NIT_AGENT_COMMAND_PATH'])) as Record<string, unknown>;
  if (
    !Array.isArray(config.approvedMarketSources) ||
    !config.cashAsset.trim() ||
    !Number.isSafeInteger(config.pollIntervalMs) ||
    config.pollIntervalMs < 1000 ||
    config.pollIntervalMs > 60000
  )
    throw new Error('Invalid bounded agent runtime configuration');
  const normalized =
    config.gateway === undefined
      ? config.agent
      : {
          ...config.agent,
          model: gatewayIdentity(
            config.agent.model,
            config.gateway.endpoint,
            config.gateway.allowedOrigin
          ),
        };
  let model: JsonGatewayModel | null = null;
  if (
    ['run_once', 'serve'].includes(String(command['action'])) &&
    normalized.enabled &&
    config.gateway !== undefined
  ) {
    const bearer = process.env['NIT_AGENT_GATEWAY_BEARER_TOKEN'];
    if (bearer !== undefined)
      model = new JsonGatewayModel(
        normalized.model,
        config.gateway.endpoint,
        config.gateway.allowedOrigin,
        bearer
      );
  }
  const { db } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  const stop = new AbortController();
  const shutdown = (): void => stop.abort();
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  try {
    const feed: PaperMarketFeed =
      config.approvedMarketSources.length === 0
        ? { latest: () => Promise.resolve(failure(new Error('MARKET_SOURCE_UNAVAILABLE'))) }
        : new PostgresPaperMarketFeed(db, config.approvedMarketSources);
    const runtime = new AgentRuntime(
      db,
      normalized,
      await loadAgentGuides(),
      new ReadOnlyAgentTools(db, feed, config.cashAsset),
      model,
      { now: (): Date => new Date() }
    );
    let result: unknown;
    if (command['action'] === 'enqueue') result = await runtime.enqueue(command['task']);
    else if (command['action'] === 'status')
      result = await runtime.status(text(command['principal']), text(command['taskId']));
    else if (command['action'] === 'trace')
      result = await runtime.trace(text(command['principal']), text(command['taskId']));
    else if (command['action'] === 'cancel') {
      await runtime.cancel(text(command['principal']), text(command['taskId']));
      result = { cancelled: true };
    } else if (command['action'] === 'recover') {
      await runtime.recover();
      result = { recovered: true };
    } else if (command['action'] === 'run_once') result = await runtime.runOne(stop.signal);
    else if (command['action'] === 'serve') {
      if (!normalized.enabled || model === null)
        throw new Error('Model gateway is disabled or unconfigured');
      while (!stop.signal.aborted) {
        try {
          const outcome = await runtime.runOne(stop.signal);
          process.stdout.write(`${JSON.stringify(outcome)}\n`);
        } catch {
          process.stderr.write('Agent cycle unavailable; no provider detail logged.\n');
        }
        try {
          await delay(config.pollIntervalMs, undefined, { signal: stop.signal });
        } catch {
          break;
        }
      }
      result = { stopped: true };
    } else throw new Error('Unknown agent command');
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } finally {
    shutdown();
    process.off('SIGINT', shutdown);
    process.off('SIGTERM', shutdown);
    await db.close();
  }
}
function text(value: unknown): string {
  if (typeof value !== 'string' || !value.trim()) throw new Error('Missing agent command identity');
  return value;
}
main().catch((): void => {
  process.stderr.write(
    'Agent command failed; inspect private configuration and persisted status.\n'
  );
  process.exitCode = 1;
});
