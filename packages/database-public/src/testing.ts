import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, parse as parsePath } from 'node:path';
import { Pool } from 'pg';
import { DatabasePool } from './pool';
import { Migrator } from './migrations';

/**
 * Test-only Postgres harness.
 *
 * Every run gets its own schema and a connection whose `search_path` points at
 * it, so concurrent test files never collide and teardown is a single DROP.
 * No production topology, credential, or private table belongs here.
 *
 * The connection string comes from NIT_TEST_DATABASE_URL, resolved from the
 * process environment first and otherwise from the nearest `.env` file at or
 * above the working directory. It must point at a disposable development
 * database. There is no default or fallback URL: with the variable absent,
 * `hasTestDatabase()` returns false and the suites skip rather than reaching
 * for an ambient database.
 */

export const TEST_DATABASE_URL_VAR = 'NIT_TEST_DATABASE_URL';

/** Filename searched for upward from the working directory. */
const ENV_FILENAME = '.env';

/**
 * Marks the workspace root. The upward search stops here.
 */
const WORKSPACE_ROOT_MARKER = 'pnpm-workspace.yaml';

/**
 * Read one key from a `.env` file without mutating `process.env`.
 *
 * Deliberately narrow: `KEY=VALUE`, optional matching surrounding quotes, `#`
 * comments, an optional `export ` prefix, blank lines skipped. It does not
 * support interpolation, multi-line values, or exporting the whole file. That
 * keeps a stray `.env` from leaking arbitrary variables into a process merely
 * because it imported this module — nothing here runs at import time, and the
 * only value ever read is the one the caller asks for.
 */
function readKeyFromEnvFile(filePath: string, key: string): string | undefined {
  let contents: string;
  try {
    contents = readFileSync(filePath, 'utf8');
  } catch {
    return undefined;
  }

  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim().replace(/^export\s+/, '');
    if (line === '' || line.startsWith('#')) {
      continue;
    }
    const separator = line.indexOf('=');
    if (separator === -1 || line.slice(0, separator).trim() !== key) {
      continue;
    }

    let value = line.slice(separator + 1).trim();
    const quote = value.charAt(0);
    if ((quote === '"' || quote === "'") && value.endsWith(quote) && value.length >= 2) {
      value = value.slice(1, -1);
    } else {
      // Unquoted values end at an inline comment.
      const comment = value.indexOf(' #');
      if (comment !== -1) {
        value = value.slice(0, comment).trim();
      }
    }
    return value === '' ? undefined : value;
  }
  return undefined;
}

/**
 * Walk up from the working directory looking for a `.env`, stopping at the
 * workspace root. Each package runs vitest from its own directory, so a single
 * repository-root `.env` is only reachable by searching upward.
 *
 * The search stops at the directory holding pnpm-workspace.yaml, inclusive.
 * Climbing past it would let this repository read a `.env` belonging to a
 * sibling or parent project, which the agent boundary forbids: only this
 * workspace's own environment file is in scope.
 *
 * @param startDirectory Where to begin. Defaults to the working directory;
 * exposed so the containment rule above can be tested without chdir, which
 * vitest's worker pool does not allow.
 */
export function findKeyInNearestEnvFile(
  key: string,
  startDirectory: string = process.cwd()
): string | undefined {
  let directory = startDirectory;
  const root = parsePath(directory).root;

  for (;;) {
    const found = readKeyFromEnvFile(join(directory, ENV_FILENAME), key);
    if (found !== undefined) {
      return found;
    }
    // Checked this directory's .env already, so stopping here is inclusive.
    if (existsSync(join(directory, WORKSPACE_ROOT_MARKER))) {
      return undefined;
    }
    if (directory === root) {
      return undefined;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      return undefined;
    }
    directory = parent;
  }
}

/**
 * The configured test database URL, or undefined.
 *
 * A variable already present in the environment wins, so a one-off shell
 * export still overrides the committed-workflow `.env`.
 */
export function testDatabaseUrl(): string | undefined {
  const fromEnvironment = process.env[TEST_DATABASE_URL_VAR];
  if (fromEnvironment !== undefined && fromEnvironment.trim() !== '') {
    return fromEnvironment;
  }
  return findKeyInNearestEnvFile(TEST_DATABASE_URL_VAR);
}

export interface TestDatabase {
  /** Pool bound to the disposable schema. */
  readonly db: DatabasePool;
  readonly schema: string;
  /** Drop the schema and close both pools. Safe to call twice. */
  teardown(): Promise<void>;
}

/**
 * Postgres identifiers are case-folded and length-limited, so normalise the
 * caller's label rather than trusting it into DDL.
 */
function toSchemaIdentifier(label: string): string {
  const slug = label.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
  return `nit_test_${slug || 'run'}`.slice(0, 63);
}

/**
 * Create an isolated schema, migrate it, and return a pool scoped to it.
 *
 * @param label Short suite name, used to build the schema identifier.
 */
export async function createTestDatabase(label: string): Promise<TestDatabase> {
  const connectionString = testDatabaseUrl();
  if (connectionString === undefined) {
    throw new Error(
      `${TEST_DATABASE_URL_VAR} is not set in the environment or in a .env file at or above ` +
        `${process.cwd()}. Guard database suites with hasTestDatabase().`
    );
  }

  const schema = toSchemaIdentifier(label);
  const admin = new Pool({ connectionString, max: 1 });

  let db: DatabasePool | undefined;
  try {
    await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await admin.query(`CREATE SCHEMA "${schema}"`);

    // `options` is sent in the startup packet, so every client this pool hands
    // out already has the right search_path — including clients used inside
    // transactions, where a per-connection SET would be easy to miss.
    // toSchemaIdentifier() restricts the name to [a-z0-9_], so the value needs
    // no quoting and cannot be confused by libpq's options parser.
    db = new DatabasePool({
      connectionString,
      options: `-c search_path=${schema}`,
      max: 8,
    });

    const migrated = await new Migrator(db).migrate();
    if (migrated.kind === 'failure') {
      throw migrated.error;
    }
  } catch (error) {
    if (db !== undefined) {
      await db.close();
    }
    await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`).catch(() => undefined);
    await admin.end();
    throw error;
  }

  const scoped = db;
  let torndown = false;

  return {
    db: scoped,
    schema,
    async teardown(): Promise<void> {
      if (torndown) {
        return;
      }
      torndown = true;
      await scoped.close();
      await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      await admin.end();
    },
  };
}

/**
 * True when a test database is configured, from either the environment or a
 * `.env` file. Suites use this with `describe.skipIf` so `pnpm test` stays
 * green on a machine with no Postgres, while a configured machine runs them
 * for real.
 */
export function hasTestDatabase(): boolean {
  return testDatabaseUrl() !== undefined;
}
