-- Provision (or repair) the NIT development database and its scoped role.
--
-- RUN THIS YOURSELF, as a Postgres superuser. The agent-facing side of this
-- repository must never hold the superuser credential: this instance also hosts
-- other projects, and a scoped role is the whole point.
--
--   psql -U postgres -f scripts/provision-test-db.sql
--
-- Idempotent: safe to run when the role or database already exists. Set the
-- password below before running, then revert the line — this file is committed,
-- so the real password must never be saved into it. Put the password only in
-- `.env` (gitignored; see env.example).
--
-- Everything here is scoped to the nit_dev role and the nit_dev database. It
-- grants nothing on, and revokes nothing from, any other database on this
-- instance.

\set nit_password 'CHANGE_ME'

-- A login role with no superuser, no createdb, and no createrole: it can do
-- nothing outside the database it owns.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nit_dev') THEN
    RAISE NOTICE 'Role nit_dev already exists; resetting password and attributes.';
  ELSE
    CREATE ROLE nit_dev LOGIN;
  END IF;
END
$$;

ALTER ROLE nit_dev
  LOGIN
  PASSWORD :'nit_password'
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  NOBYPASSRLS
  NOREPLICATION;

-- CREATE DATABASE cannot run inside a DO block or a transaction, so this is
-- guarded with \gexec instead: the SELECT emits the statement only when the
-- database is absent.
SELECT 'CREATE DATABASE nit_dev OWNER nit_dev'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'nit_dev')
\gexec

-- If the database predates this script it may have another owner; the harness
-- needs to CREATE SCHEMA and DROP SCHEMA in it.
ALTER DATABASE nit_dev OWNER TO nit_dev;

-- Keep other roles out of this database. Scoped to nit_dev only.
REVOKE ALL ON DATABASE nit_dev FROM PUBLIC;
GRANT ALL ON DATABASE nit_dev TO nit_dev;

-- Confirm the result.
\echo 'Role attributes (all of super/createrole/createdb should be f):'
SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolcanlogin
FROM pg_roles WHERE rolname = 'nit_dev';

\echo 'Database owner (should be nit_dev):'
SELECT datname, pg_get_userbyid(datdba) AS owner
FROM pg_database WHERE datname = 'nit_dev';

-- Next: put the connection string in .env at the repository root, then run
-- `pnpm test`. Without it the database suites skip and the rest still passes.
--
--   cp env.example .env
--   # edit .env, set the password you used above
--   pnpm test
