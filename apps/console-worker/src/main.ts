import { readFile, stat } from 'node:fs/promises';
import { extname } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { bootstrap } from '@nit/composition';
import {
  CandidateResolver,
  ConsoleOptions,
  ResolverPolicy,
  TelegramBotApi,
  TelegramDecisionConsole,
} from '@nit/decision-console';
import { PostgresPaperMarketFeed, PostgresPaperTrader } from '@nit/trading';

async function main(): Promise<void> {
  const path = process.env['NIT_CONSOLE_CONFIG_PATH'];
  const token = process.env['NIT_TELEGRAM_BOT_TOKEN'];
  if (path === undefined || extname(path).toLowerCase() !== '.json' || token === undefined)
    throw new Error('Configure NIT_CONSOLE_CONFIG_PATH and NIT_TELEGRAM_BOT_TOKEN');
  if ((await stat(path)).size > 1_048_576) throw new Error('Console configuration exceeds 1 MiB');
  const config = JSON.parse(await readFile(path, 'utf8')) as {
    console: ConsoleOptions;
    resolver: ResolverPolicy;
    cashAsset: string;
    approvedMarketSources: string[];
    cycleIntervalMs: number;
  };
  if (
    !Number.isSafeInteger(config.cycleIntervalMs) ||
    config.cycleIntervalMs < 100 ||
    config.cycleIntervalMs > 60_000
  )
    throw new Error('Console cycleIntervalMs must be between 100 and 60000');
  const api = new TelegramBotApi(token);
  const { db } = await bootstrap(process.env['NIT_CONFIG_PATH'], {
    bindDevelopmentToyProviders: false,
    requiredProviderSlots: [],
  });
  const shutdown = new AbortController();
  const stop = (): void => shutdown.abort();
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  try {
    const clock = { now: (): Date => new Date() };
    const feed = new PostgresPaperMarketFeed(db, config.approvedMarketSources);
    const resolver = new CandidateResolver(db, feed, clock, config.resolver, config.cashAsset);
    const consoleService = new TelegramDecisionConsole(
      db,
      api,
      feed,
      new PostgresPaperTrader(db, clock),
      clock,
      config.console
    );
    // Polling waits independently; it cannot delay resolver coalescing or protection notifications.
    const loop = async (operation: () => Promise<void>, interval: number): Promise<void> => {
      while (!shutdown.signal.aborted) {
        try {
          await operation();
        } catch {
          if (!shutdown.signal.aborted)
            process.stderr.write('Console cycle failed; retrying without logging provider data.\n');
        }
        try {
          await delay(interval, undefined, { signal: shutdown.signal });
        } catch {
          break;
        }
      }
    };
    await Promise.all([
      loop(() => consoleService.poll(shutdown.signal), 1000),
      loop(async () => {
        await consoleService.flush();
        await resolver.cycle();
        await consoleService.executeTrades();
        try {
          await consoleService.notify();
        } finally {
          await consoleService.flush();
        }
      }, config.cycleIntervalMs),
    ]);
  } finally {
    stop();
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
    await db.close();
  }
}

main().catch((): void => {
  process.stderr.write(
    'Console worker failed to start; check private configuration and database readiness.\n'
  );
  process.exitCode = 1;
});
