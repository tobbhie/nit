import { spawn } from 'node:child_process';
import { accessSync, constants } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const services = Object.freeze({
  control: 'control-api',
  market: 'market-worker',
  wallet: 'wallet-worker',
  ingestion: 'ingestion-worker',
  narrative: 'narrative-worker',
  console: 'console-worker',
  paper: 'paper-worker',
  observer: 'agent-worker',
  research: 'research-worker',
});
const args = process.argv.slice(2);
const checkOnly = args[0] === '--check';
if (checkOnly) args.shift();
const service = args[0];
if (args.length !== 1 || !Object.hasOwn(services, service)) {
  process.stderr.write(
    'Usage: node scripts/local.mjs [--check] control|market|wallet|ingestion|narrative|console|paper|observer|research\n'
  );
  process.exitCode = 1;
} else {
  try {
    // Load into the parent, then inherit at child startup. NODE_EXTRA_CA_CERTS
    // is read before --env-file/preload JavaScript on some supported Node builds.
    const projectKeys = ['OPENAI_API_KEY', 'NIT_TELEGRAM_BOT_TOKEN', 'NIT_CONSOLE_CONFIG_PATH'];
    const inherited = new Map(projectKeys.map((key) => [key, process.env[key]]));
    for (const key of projectKeys) delete process.env[key];
    process.loadEnvFile(join(root, '.env'));
    for (const [key, value] of inherited)
      if (process.env[key] === undefined && value !== undefined) process.env[key] = value;
    const entrypoint = join(root, 'apps', services[service], 'dist', 'main.js');
    accessSync(entrypoint, constants.R_OK);
    if (process.env.NODE_EXTRA_CA_CERTS) {
      const caPath = resolve(root, process.env.NODE_EXTRA_CA_CERTS);
      accessSync(caPath, constants.R_OK);
      process.env.NODE_EXTRA_CA_CERTS = caPath;
    }
    if (checkOnly) {
      process.stdout.write(
        'Local configuration paths checked. No application, connection or migration started.\n'
      );
    } else {
      const child = spawn(process.execPath, [entrypoint], {
        cwd: root,
        env: process.env,
        stdio: 'inherit',
        shell: false,
        windowsHide: true,
      });
      const interrupt = () => child.kill('SIGINT');
      const terminate = () => child.kill('SIGTERM');
      const cleanup = () => {
        process.off('SIGINT', interrupt);
        process.off('SIGTERM', terminate);
      };
      process.on('SIGINT', interrupt);
      process.on('SIGTERM', terminate);
      child.once('error', () => {
        cleanup();
        process.stderr.write('Local application process could not start.\n');
        process.exitCode = 1;
      });
      child.once('exit', (code) => {
        cleanup();
        process.exitCode = code ?? 1;
      });
    }
  } catch {
    // Never include environment values or parser errors containing credentials.
    process.stderr.write(
      'Local setup is incomplete. Use Node 22+, fill .env, build the workspace and check the configured certificate path.\n'
    );
    process.exitCode = 1;
  }
}
