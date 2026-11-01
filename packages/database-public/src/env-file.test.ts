import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { TEST_DATABASE_URL_VAR, findKeyInNearestEnvFile, testDatabaseUrl } from './testing';

/**
 * Covers `.env` resolution, which needs no database and so runs everywhere.
 *
 * The containment rule is a boundary property, not a convenience: the search
 * must not climb past this workspace into a sibling project's `.env`. It is
 * tested against a synthetic directory tree rather than the real repository so
 * the assertion holds regardless of what the developer has on disk.
 */
describe('findKeyInNearestEnvFile', () => {
  const KEY = 'NIT_TEST_DATABASE_URL';
  let root: string;
  let workspace: string;
  let packageDir: string;

  beforeEach(() => {
    // outer/ (foreign project) > workspace/ (marker) > packages/queue/ (cwd)
    root = mkdtempSync(join(tmpdir(), 'nit-env-'));
    workspace = join(root, 'workspace');
    packageDir = join(workspace, 'packages', 'queue');
    mkdirSync(packageDir, { recursive: true });
    writeFileSync(join(workspace, 'pnpm-workspace.yaml'), 'packages:\n  - packages/*\n');
  });

  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  it('finds a key in the workspace-root .env from a package directory', () => {
    writeFileSync(join(workspace, '.env'), `${KEY}=postgresql://ours@localhost/db\n`);
    expect(findKeyInNearestEnvFile(KEY, packageDir)).toBe('postgresql://ours@localhost/db');
  });

  it('finds a key in a .env beside the caller', () => {
    writeFileSync(join(packageDir, '.env'), `${KEY}=postgresql://near@localhost/db\n`);
    expect(findKeyInNearestEnvFile(KEY, packageDir)).toBe('postgresql://near@localhost/db');
  });

  it('prefers the nearest .env when both levels define the key', () => {
    writeFileSync(join(workspace, '.env'), `${KEY}=postgresql://far@localhost/db\n`);
    writeFileSync(join(packageDir, '.env'), `${KEY}=postgresql://near@localhost/db\n`);
    expect(findKeyInNearestEnvFile(KEY, packageDir)).toBe('postgresql://near@localhost/db');
  });

  it('refuses to read a .env above the workspace root', () => {
    // Belongs to a different project. Reading it would cross the boundary.
    writeFileSync(join(root, '.env'), `${KEY}=postgresql://foreign@localhost/db\n`);
    expect(findKeyInNearestEnvFile(KEY, packageDir)).toBeUndefined();
  });

  it('stops at the workspace root even when it holds no .env at all', () => {
    expect(findKeyInNearestEnvFile(KEY, packageDir)).toBeUndefined();
  });

  it('returns undefined when the key is absent from an existing .env', () => {
    writeFileSync(join(workspace, '.env'), 'SOME_OTHER_KEY=value\n');
    expect(findKeyInNearestEnvFile(KEY, packageDir)).toBeUndefined();
  });

  it('never mutates process.env', () => {
    writeFileSync(join(workspace, '.env'), `${KEY}=x\nUNRELATED_KEY=leaked\n`);
    findKeyInNearestEnvFile(KEY, packageDir);
    expect(process.env['UNRELATED_KEY']).toBeUndefined();
  });

  describe('line parsing', () => {
    const parse = (contents: string): string | undefined => {
      writeFileSync(join(workspace, '.env'), contents);
      return findKeyInNearestEnvFile(KEY, packageDir);
    };

    it('strips an export prefix', () => {
      expect(parse(`export ${KEY}=plain\n`)).toBe('plain');
    });

    it('strips matching double quotes', () => {
      expect(parse(`${KEY}="quoted"\n`)).toBe('quoted');
    });

    it('strips matching single quotes', () => {
      expect(parse(`${KEY}='quoted'\n`)).toBe('quoted');
    });

    it('keeps a password containing an equals sign', () => {
      // Splits on the first '=' only; base64 passwords routinely end in '='.
      expect(parse(`${KEY}=postgresql://u:p=a==@localhost/db\n`)).toBe(
        'postgresql://u:p=a==@localhost/db'
      );
    });

    it('keeps a # inside a quoted value', () => {
      expect(parse(`${KEY}="postgresql://u:pa#ss@localhost/db"\n`)).toBe(
        'postgresql://u:pa#ss@localhost/db'
      );
    });

    it('drops a trailing comment from an unquoted value', () => {
      expect(parse(`${KEY}=postgresql://localhost/db # local only\n`)).toBe(
        'postgresql://localhost/db'
      );
    });

    it('ignores commented-out and blank lines', () => {
      expect(parse(`\n# ${KEY}=commented\n\n${KEY}=real\n`)).toBe('real');
    });

    it('ignores keys that merely share a prefix', () => {
      expect(parse(`${KEY}_EXTRA=wrong\n${KEY}=right\n`)).toBe('right');
    });

    it('treats an empty value as unset', () => {
      expect(parse(`${KEY}=\n`)).toBeUndefined();
    });

    it('tolerates CRLF line endings', () => {
      expect(parse(`# comment\r\n${KEY}=crlf\r\n`)).toBe('crlf');
    });
  });
});

describe('testDatabaseUrl', () => {
  const original = process.env[TEST_DATABASE_URL_VAR];

  afterEach(() => {
    if (original === undefined) {
      delete process.env[TEST_DATABASE_URL_VAR];
    } else {
      process.env[TEST_DATABASE_URL_VAR] = original;
    }
  });

  it('reads the variable from the environment', () => {
    process.env[TEST_DATABASE_URL_VAR] = 'postgresql://from-env@localhost/db';
    expect(testDatabaseUrl()).toBe('postgresql://from-env@localhost/db');
  });

  it('ignores a blank environment variable so .env can still apply', () => {
    process.env[TEST_DATABASE_URL_VAR] = '   ';
    // Whatever the fallback yields, the blank must not be returned as a URL.
    expect(testDatabaseUrl()).not.toBe('   ');
  });
});
