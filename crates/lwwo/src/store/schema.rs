//! The schema, and the migrations that bring an older database up to it.
//!
//! The version of the schema is `PRAGMA user_version`. Migration `n` takes
//! a database from version `n` to `n + 1`; a new database runs them all.

use anyhow::{Result, bail};
use rusqlite::Connection;

/// One entry per schema version, oldest first. Never edit an entry that
/// was released: add a new one.
const MIGRATIONS: &[&str] = &[
    // 1: the eight tables of the design.
    "
CREATE TABLE dags (
  name        TEXT PRIMARY KEY,
  current     TEXT REFERENCES dag_versions(hash),   -- active version
  paused      INTEGER NOT NULL DEFAULT 0,
  created_at  INTEGER NOT NULL,
  deleted_at  INTEGER
) STRICT;

CREATE TABLE dag_versions (
  hash        TEXT PRIMARY KEY,                     -- hash of the definition
  dag_name    TEXT NOT NULL REFERENCES dags(name),
  yaml        TEXT NOT NULL,                        -- the source
  structure   TEXT NOT NULL,                        -- hash of tasks and dependencies only
  created_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE timetable (
  dag_name    TEXT PRIMARY KEY REFERENCES dags(name),
  next_fire   INTEGER NOT NULL
) STRICT;
CREATE INDEX timetable_by_time ON timetable(next_fire);

CREATE TABLE dag_runs (
  id            TEXT PRIMARY KEY,                   -- UUIDv7
  dag_name      TEXT NOT NULL REFERENCES dags(name),
  version_hash  TEXT NOT NULL REFERENCES dag_versions(hash),
  logical_time  INTEGER NOT NULL,                   -- the instant of the run
  trigger       TEXT NOT NULL CHECK (trigger IN ('cron', 'catchup', 'manual')),
  params        TEXT NOT NULL DEFAULT '{}',         -- JSON, final values
  state         TEXT NOT NULL CHECK (state IN ('queued', 'running', 'success', 'failed', 'cancelled')),
  created_at    INTEGER NOT NULL,
  started_at    INTEGER,
  finished_at   INTEGER,
  UNIQUE (id, dag_name)                             -- target of the key of task_runs
) STRICT;
CREATE UNIQUE INDEX dag_runs_one_per_tick ON dag_runs(dag_name, logical_time) WHERE trigger <> 'manual';
CREATE INDEX dag_runs_by_dag ON dag_runs(dag_name, logical_time DESC);
CREATE INDEX dag_runs_open ON dag_runs(state) WHERE state IN ('queued', 'running');

CREATE TABLE task_runs (
  run_id      TEXT NOT NULL,
  dag_name    TEXT NOT NULL REFERENCES dags(name),  -- direct reference to the DAG
  task        TEXT NOT NULL,
  state       TEXT NOT NULL CHECK (state IN ('blocked', 'assigned', 'retrying', 'success', 'failed', 'skipped', 'cancelled')),
  attempts    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, task),
  FOREIGN KEY (run_id, dag_name) REFERENCES dag_runs(id, dag_name) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;
CREATE INDEX task_runs_by_task ON task_runs(dag_name, task, run_id DESC);

CREATE TABLE task_attempts (
  run_id        TEXT NOT NULL,
  task          TEXT NOT NULL,
  attempt       INTEGER NOT NULL,                   -- 1, 2, ...
  worker        TEXT NOT NULL,
  version_hash  TEXT NOT NULL,                      -- version used by this attempt
  assigned_at   INTEGER NOT NULL,                   -- first write
  started_at    INTEGER,                            -- second write, with the report
  finished_at   INTEGER,
  outcome       TEXT CHECK (outcome IN ('success', 'failed', 'timeout', 'lost', 'cancelled')),
  exit_code     INTEGER,
  peak_memory   INTEGER,                            -- bytes, informative only
  log_bytes     INTEGER,
  log_truncated INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, task, attempt),
  FOREIGN KEY (run_id, task) REFERENCES task_runs(run_id, task) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;
CREATE INDEX task_attempts_in_flight ON task_attempts(worker) WHERE finished_at IS NULL;

CREATE TABLE workers (
  name         TEXT PRIMARY KEY,
  cert_serial  TEXT NOT NULL,                       -- certificate issued by the internal CA
  joined_at    INTEGER NOT NULL,
  revoked_at   INTEGER
) STRICT;

CREATE TABLE tokens (
  id           TEXT PRIMARY KEY,
  kind         TEXT NOT NULL CHECK (kind IN ('api', 'join')),
  label        TEXT,
  secret_hash  TEXT NOT NULL,                       -- never the token itself
  created_at   INTEGER NOT NULL,
  expires_at   INTEGER,
  revoked_at   INTEGER
) STRICT;
",
    // 2: why the engine stopped a run it could no longer trust.
    "ALTER TABLE dag_runs ADD COLUMN problem TEXT;",
    // 3: a finished run waiting to run again, until there is room.
    "ALTER TABLE dag_runs ADD COLUMN rerun TEXT;",
];

/// Brings the database to the latest schema.
pub(super) fn migrate(conn: &Connection) -> Result<()> {
    let version: usize = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > MIGRATIONS.len() {
        bail!(
            "the database has schema version {version}, newer than this lwwo knows ({}): \
             use a newer lwwo",
            MIGRATIONS.len()
        );
    }
    for (index, migration) in MIGRATIONS.iter().enumerate().skip(version) {
        conn.execute_batch(&format!(
            "BEGIN;\n{migration}\nPRAGMA user_version = {};\nCOMMIT;",
            index + 1
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_database_gets_the_latest_schema_once() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        migrate(&conn).unwrap();
        let version: usize = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, MIGRATIONS.len());
        let tables: usize = conn
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tables, 8);
        let problem: Option<String> = conn
            .query_row("SELECT problem FROM dag_runs LIMIT 1", [], |r| r.get(0))
            .unwrap_or(None);
        assert_eq!(problem, None, "the column of version 2 is there");
    }

    #[test]
    fn a_database_from_the_future_is_refused() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        assert!(migrate(&conn).is_err());
    }
}
