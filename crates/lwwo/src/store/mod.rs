//! The SQLite database: the durable copy of everything the master knows.
//!
//! Only the master's event loop holds a [`Store`], so there is one writer
//! and no locking. Times are UTC milliseconds, ids are `UUIDv7` text. The
//! methods are thin: which rows change together, and when, is decided by
//! the master, which wraps them in [`Store::transaction`].

mod schema;

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;

use crate::config::Durability;

/// A DAG with its current version and its next fire.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DagRow {
    pub(crate) name: String,
    /// Hash of the current version.
    pub(crate) current: String,
    pub(crate) paused: bool,
    pub(crate) created_at: i64,
    pub(crate) deleted_at: Option<i64>,
    /// The row of the timetable, if the DAG has a schedule and is active.
    pub(crate) next_fire: Option<i64>,
    #[serde(skip)]
    pub(crate) yaml: String,
}

/// A version of a DAG.
#[derive(Debug, Clone)]
pub(crate) struct VersionRow {
    pub(crate) hash: String,
    pub(crate) yaml: String,
    pub(crate) structure: String,
}

/// A row of `dag_runs`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunRow {
    pub(crate) id: String,
    pub(crate) dag_name: String,
    pub(crate) version_hash: String,
    pub(crate) logical_time: i64,
    pub(crate) trigger: String,
    /// JSON object with the final values of the parameters.
    pub(crate) params: String,
    pub(crate) state: String,
    pub(crate) created_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) finished_at: Option<i64>,
    /// Why the engine stopped the run, if it did: see `set_problem`.
    pub(crate) problem: Option<String>,
}

/// A row of `task_runs`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct TaskRow {
    pub(crate) task: String,
    pub(crate) state: String,
    pub(crate) attempts: u32,
}

/// A row of `task_attempts`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AttemptRow {
    pub(crate) task: String,
    pub(crate) attempt: u32,
    pub(crate) worker: String,
    pub(crate) version_hash: String,
    pub(crate) assigned_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) finished_at: Option<i64>,
    pub(crate) outcome: Option<String>,
    pub(crate) exit_code: Option<i64>,
    pub(crate) peak_memory: Option<i64>,
    pub(crate) log_bytes: Option<i64>,
    pub(crate) log_truncated: bool,
}

/// How an attempt ended, for the second write of its row.
#[derive(Debug, Clone)]
pub(crate) struct AttemptEnd<'a> {
    pub(crate) outcome: &'a str,
    pub(crate) exit_code: Option<i32>,
    pub(crate) started_at: Option<i64>,
    pub(crate) finished_at: i64,
    pub(crate) peak_memory: Option<u64>,
    pub(crate) log_bytes: Option<u64>,
    pub(crate) log_truncated: bool,
}

/// A row of `workers`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkerRow {
    pub(crate) name: String,
    pub(crate) cert_serial: String,
    pub(crate) joined_at: i64,
    pub(crate) revoked_at: Option<i64>,
}

/// A row of `tokens`.
#[derive(Debug, Clone)]
pub(crate) struct TokenRow {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) label: Option<String>,
    pub(crate) created_at: i64,
    pub(crate) secret_hash: String,
    pub(crate) expires_at: Option<i64>,
    pub(crate) revoked_at: Option<i64>,
}

const TOKEN_COLUMNS: &str = "id, kind, label, created_at, secret_hash, expires_at, revoked_at";

fn token_row(row: &Row<'_>) -> rusqlite::Result<TokenRow> {
    Ok(TokenRow {
        id: row.get(0)?,
        kind: row.get(1)?,
        label: row.get(2)?,
        created_at: row.get(3)?,
        secret_hash: row.get(4)?,
        expires_at: row.get(5)?,
        revoked_at: row.get(6)?,
    })
}

const DAG_COLUMNS: &str = "d.name, d.current, d.paused, d.created_at, d.deleted_at, \
     (SELECT next_fire FROM timetable t WHERE t.dag_name = d.name), v.yaml";
const RUN_COLUMNS: &str = "id, dag_name, version_hash, logical_time, trigger, params, state, \
     created_at, started_at, finished_at, problem";

fn dag_row(row: &Row<'_>) -> rusqlite::Result<DagRow> {
    Ok(DagRow {
        name: row.get(0)?,
        current: row.get(1)?,
        paused: row.get(2)?,
        created_at: row.get(3)?,
        deleted_at: row.get(4)?,
        next_fire: row.get(5)?,
        yaml: row.get(6)?,
    })
}

fn run_row(row: &Row<'_>) -> rusqlite::Result<RunRow> {
    Ok(RunRow {
        id: row.get(0)?,
        dag_name: row.get(1)?,
        version_hash: row.get(2)?,
        logical_time: row.get(3)?,
        trigger: row.get(4)?,
        params: row.get(5)?,
        state: row.get(6)?,
        created_at: row.get(7)?,
        started_at: row.get(8)?,
        finished_at: row.get(9)?,
        problem: row.get(10)?,
    })
}

fn attempt_row(row: &Row<'_>) -> rusqlite::Result<AttemptRow> {
    Ok(AttemptRow {
        task: row.get(0)?,
        attempt: row.get(1)?,
        worker: row.get(2)?,
        version_hash: row.get(3)?,
        assigned_at: row.get(4)?,
        started_at: row.get(5)?,
        finished_at: row.get(6)?,
        outcome: row.get(7)?,
        exit_code: row.get(8)?,
        peak_memory: row.get(9)?,
        log_bytes: row.get(10)?,
        log_truncated: row.get(11)?,
    })
}

/// SQLite integers are signed: sizes beyond `i64::MAX` do not exist.
fn signed(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Writes a consistent copy of the database at `db` to `target`, which
/// must not exist: SQLite's `VACUUM INTO`, which reads one snapshot.
///
/// It opens a connection of its own, read-only, so it can run on another
/// thread while the master goes on writing: in WAL mode readers and the
/// writer do not wait for each other.
pub(crate) fn backup(db: &Path, target: &Path) -> Result<()> {
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("cannot open the database {}", db.display()))?;
    let target = target
        .to_str()
        .context("the backup path is not valid text")?;
    conn.execute("VACUUM INTO ?1", [target])?;
    Ok(())
}

/// A second, read-only connection to the database at `db`, for a reader
/// outside the master's loop. In WAL mode it never makes the writer wait.
pub(crate) fn open_read_only(db: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("cannot open the database {}", db.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(conn)
}

/// The DAG of an attempt, if the attempt exists and was assigned to
/// `worker`: what tells the logs a worker may write from those it may not.
pub(crate) fn attempt_dag(
    conn: &Connection,
    run: &str,
    task: &str,
    attempt: u32,
    worker: &str,
) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT r.dag_name FROM task_attempts a JOIN dag_runs r ON r.id = a.run_id
             WHERE a.run_id = ?1 AND a.task = ?2 AND a.attempt = ?3 AND a.worker = ?4",
            params![run, task, attempt, worker],
            |row| row.get(0),
        )
        .optional()?)
}

/// The database of a master.
#[derive(Debug)]
pub(crate) struct Store {
    conn: Connection,
}

impl Store {
    /// Opens the database, creating or migrating it as needed.
    pub(crate) fn open(path: &Path, durability: Durability) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("cannot open the database {}", path.display()))?;
        Self::prepare(conn, durability)
    }

    /// A database in memory, for tests.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Result<Self> {
        Self::prepare(Connection::open_in_memory()?, Durability::Normal)
    }

    fn prepare(conn: Connection, durability: Durability) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(
            None,
            "synchronous",
            match durability {
                Durability::Normal => "NORMAL",
                Durability::Full => "FULL",
            },
        )?;
        conn.pragma_update(None, "foreign_keys", true)?;
        schema::migrate(&conn)?;
        Ok(Self { conn })
    }

    /// Runs `work` in one transaction: all of its writes are kept, or none.
    pub(crate) fn transaction<T>(&self, work: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        match work(self) {
            Ok(value) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                // The original error is the one worth reporting.
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    // DAGs and versions

    /// Saves a version of a DAG and makes it the current one. Returns
    /// whether the version is new for this DAG's current pointer.
    pub(crate) fn apply_dag(
        &self,
        name: &str,
        hash: &str,
        yaml: &str,
        structure: &str,
        now: i64,
    ) -> Result<bool> {
        let previous: Option<Option<String>> = self
            .conn
            .query_row("SELECT current FROM dags WHERE name = ?1", [name], |row| {
                row.get(0)
            })
            .optional()?;
        if previous.is_none() {
            // `current` stays empty for a moment: the version needs the DAG.
            self.conn.execute(
                "INSERT INTO dags (name, created_at) VALUES (?1, ?2)",
                params![name, now],
            )?;
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO dag_versions (hash, dag_name, yaml, structure, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![hash, name, yaml, structure, now],
        )?;
        self.conn.execute(
            "UPDATE dags SET current = ?2, deleted_at = NULL WHERE name = ?1",
            params![name, hash],
        )?;
        Ok(previous.flatten().as_deref() != Some(hash))
    }

    /// A DAG by name, deleted ones included.
    pub(crate) fn dag(&self, name: &str) -> Result<Option<DagRow>> {
        let sql = format!(
            "SELECT {DAG_COLUMNS} FROM dags d JOIN dag_versions v ON v.hash = d.current
             WHERE d.name = ?1"
        );
        Ok(self.conn.query_row(&sql, [name], dag_row).optional()?)
    }

    /// Every DAG that was not deleted, by name.
    pub(crate) fn dags(&self) -> Result<Vec<DagRow>> {
        let sql = format!(
            "SELECT {DAG_COLUMNS} FROM dags d JOIN dag_versions v ON v.hash = d.current
             WHERE d.deleted_at IS NULL ORDER BY d.name"
        );
        let mut statement = self.conn.prepare(&sql)?;
        let rows = statement.query_map([], dag_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Pauses or resumes a DAG.
    pub(crate) fn set_paused(&self, name: &str, paused: bool) -> Result<()> {
        self.conn.execute(
            "UPDATE dags SET paused = ?2 WHERE name = ?1",
            params![name, paused],
        )?;
        Ok(())
    }

    /// Marks a DAG as deleted. Its runs stay in the history.
    pub(crate) fn delete_dag(&self, name: &str, now: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE dags SET deleted_at = ?2 WHERE name = ?1",
            params![name, now],
        )?;
        self.set_next_fire(name, None)
    }

    /// A version by hash.
    pub(crate) fn version(&self, hash: &str) -> Result<Option<VersionRow>> {
        Ok(self
            .conn
            .query_row(
                "SELECT hash, yaml, structure FROM dag_versions WHERE hash = ?1",
                [hash],
                |row| {
                    Ok(VersionRow {
                        hash: row.get(0)?,
                        yaml: row.get(1)?,
                        structure: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    // Timetable

    /// Sets the next fire of a DAG, or removes its row.
    pub(crate) fn set_next_fire(&self, dag: &str, next_fire: Option<i64>) -> Result<()> {
        match next_fire {
            Some(next_fire) => self.conn.execute(
                "INSERT INTO timetable (dag_name, next_fire) VALUES (?1, ?2)
                 ON CONFLICT (dag_name) DO UPDATE SET next_fire = excluded.next_fire",
                params![dag, next_fire],
            )?,
            None => self
                .conn
                .execute("DELETE FROM timetable WHERE dag_name = ?1", [dag])?,
        };
        Ok(())
    }

    /// The DAGs whose fire is due, earliest first.
    pub(crate) fn due(&self, now: i64) -> Result<Vec<String>> {
        let mut statement = self.conn.prepare(
            "SELECT dag_name FROM timetable WHERE next_fire <= ?1 ORDER BY next_fire, dag_name",
        )?;
        let rows = statement.query_map([now], |row| row.get(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// The earliest fire after `now`. Rows at or before `now` are waiting
    /// for a run to finish, not for the clock.
    pub(crate) fn next_fire_after(&self, now: i64) -> Result<Option<i64>> {
        Ok(self.conn.query_row(
            "SELECT min(next_fire) FROM timetable WHERE next_fire > ?1",
            [now],
            |row| row.get(0),
        )?)
    }

    // Runs

    /// Creates a run.
    pub(crate) fn insert_run(&self, run: &RunRow) -> Result<()> {
        self.conn.execute(
            "INSERT INTO dag_runs (id, dag_name, version_hash, logical_time, trigger, params,
                                   state, created_at, started_at, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                run.id,
                run.dag_name,
                run.version_hash,
                run.logical_time,
                run.trigger,
                run.params,
                run.state,
                run.created_at,
                run.started_at,
                run.finished_at
            ],
        )?;
        Ok(())
    }

    /// Creates the rows of the tasks of a run that starts, all blocked.
    pub(crate) fn insert_tasks<'a>(
        &self,
        run: &str,
        dag: &str,
        tasks: impl Iterator<Item = &'a str>,
    ) -> Result<()> {
        let mut statement = self.conn.prepare(
            "INSERT INTO task_runs (run_id, dag_name, task, state) VALUES (?1, ?2, ?3, 'blocked')",
        )?;
        for task in tasks {
            statement.execute(params![run, dag, task])?;
        }
        Ok(())
    }

    /// A queued run starts: it is running from `now`.
    pub(crate) fn start_run(&self, run: &str, now: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE dag_runs SET state = 'running', started_at = ?2 WHERE id = ?1",
            params![run, now],
        )?;
        Ok(())
    }

    /// A run ends, as `success`, `failed` or `cancelled`.
    pub(crate) fn finish_run(&self, run: &str, state: &str, now: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE dag_runs SET state = ?2, finished_at = ?3 WHERE id = ?1",
            params![run, state, now],
        )?;
        Ok(())
    }

    /// A finished run is running again, with the version `version_hash`:
    /// its own, or the current one of the DAG for a rerun with
    /// `--current`.
    pub(crate) fn reopen_run(&self, run: &str, version_hash: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE dag_runs SET state = 'running', finished_at = NULL, problem = NULL, rerun = NULL,
                    version_hash = ?2
             WHERE id = ?1",
            params![run, version_hash],
        )?;
        Ok(())
    }

    /// Writes down why the engine stopped a run: something it was told did
    /// not fit the state of the run, so the run was failed rather than the
    /// master stopped.
    pub(crate) fn set_problem(&self, run: &str, problem: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE dag_runs SET problem = ?2 WHERE id = ?1",
            params![run, problem],
        )?;
        Ok(())
    }

    /// A finished run waits to run again: it is `queued`, with the request
    /// as JSON, until there is room for it.
    pub(crate) fn queue_rerun(&self, run: &str, request: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE dag_runs SET state = 'queued', rerun = ?2 WHERE id = ?1",
            params![run, request],
        )?;
        Ok(())
    }

    /// The request of a run waiting to run again; `None` for any other run.
    pub(crate) fn rerun_request(&self, run: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT rerun FROM dag_runs WHERE id = ?1", [run], |row| {
                row.get(0)
            })
            .optional()?
            .flatten())
    }

    /// A run that waited to run again will not: it goes back to `state`.
    pub(crate) fn drop_rerun(&self, run: &str, state: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE dag_runs SET state = ?2, rerun = NULL WHERE id = ?1",
            params![run, state],
        )?;
        Ok(())
    }

    /// A run by id, or by the beginning of its id if only one matches.
    pub(crate) fn run(&self, id: &str) -> Result<Option<RunRow>> {
        let sql = format!("SELECT {RUN_COLUMNS} FROM dag_runs WHERE id = ?1");
        if let Some(run) = self.conn.query_row(&sql, [id], run_row).optional()? {
            return Ok(Some(run));
        }
        let sql = format!("SELECT {RUN_COLUMNS} FROM dag_runs WHERE id LIKE ?1 || '%' LIMIT 2");
        let mut statement = self.conn.prepare(&sql)?;
        let mut matches: Vec<RunRow> = statement
            .query_map([id.replace(['%', '_'], "")], run_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(if matches.len() == 1 {
            matches.pop()
        } else {
            None
        })
    }

    /// The latest runs, of one DAG or of all, newest first.
    pub(crate) fn runs(&self, dag: Option<&str>, limit: u32) -> Result<Vec<RunRow>> {
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM dag_runs
             WHERE ?1 IS NULL OR dag_name = ?1 ORDER BY id DESC LIMIT ?2"
        );
        let mut statement = self.conn.prepare(&sql)?;
        let rows = statement.query_map(params![dag, limit], run_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// The runs not yet finished, oldest first: what the master loads when
    /// it starts.
    pub(crate) fn open_runs(&self) -> Result<Vec<RunRow>> {
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM dag_runs WHERE state IN ('queued', 'running') ORDER BY id"
        );
        let mut statement = self.conn.prepare(&sql)?;
        let rows = statement.query_map([], run_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// How many tasks a run has, and how many succeeded, failed and are on
    /// a worker: what a progress bar needs.
    pub(crate) fn task_counts(&self, run: &str) -> Result<(u32, u32, u32, u32)> {
        Ok(self.conn.query_row(
            "SELECT count(*),
                    coalesce(sum(state = 'success'), 0),
                    coalesce(sum(state = 'failed'), 0),
                    coalesce(sum(state = 'assigned'), 0)
             FROM task_runs WHERE run_id = ?1",
            [run],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?)
    }

    // Tasks and attempts

    /// The tasks of a run, by name.
    pub(crate) fn tasks(&self, run: &str) -> Result<Vec<TaskRow>> {
        let mut statement = self.conn.prepare(
            "SELECT task, state, attempts FROM task_runs WHERE run_id = ?1 ORDER BY task",
        )?;
        let rows = statement.query_map([run], |row| {
            Ok(TaskRow {
                task: row.get(0)?,
                state: row.get(1)?,
                attempts: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Writes the current state of a task.
    pub(crate) fn save_task(
        &self,
        run: &str,
        task: &str,
        state: &str,
        attempts: u32,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE task_runs SET state = ?3, attempts = ?4 WHERE run_id = ?1 AND task = ?2",
            params![run, task, state, attempts],
        )?;
        Ok(())
    }

    /// The first write of an attempt: it was assigned to a worker.
    pub(crate) fn insert_attempt(
        &self,
        run: &str,
        task: &str,
        attempt: u32,
        worker: &str,
        version_hash: &str,
        now: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO task_attempts
               (run_id, task, attempt, worker, version_hash, assigned_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![run, task, attempt, worker, version_hash, now],
        )?;
        Ok(())
    }

    /// The process of an attempt started.
    pub(crate) fn start_attempt(&self, run: &str, task: &str, attempt: u32, at: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE task_attempts SET started_at = ?4
             WHERE run_id = ?1 AND task = ?2 AND attempt = ?3 AND started_at IS NULL",
            params![run, task, attempt, at],
        )?;
        Ok(())
    }

    /// The second write of an attempt: how it ended, as its worker says.
    /// An attempt the master already gave up as `lost` keeps that outcome.
    /// Only the row of `worker` changes: a report from another worker,
    /// buggy or not, cannot rewrite the history of an attempt.
    pub(crate) fn finish_attempt(
        &self,
        run: &str,
        task: &str,
        attempt: u32,
        worker: &str,
        end: &AttemptEnd<'_>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE task_attempts
             SET outcome = CASE WHEN outcome = 'lost' THEN outcome ELSE ?4 END,
                 exit_code = ?5, started_at = coalesce(?6, started_at), finished_at = ?7,
                 peak_memory = ?8, log_bytes = ?9, log_truncated = ?10
             WHERE run_id = ?1 AND task = ?2 AND attempt = ?3 AND worker = ?11",
            params![
                run,
                task,
                attempt,
                end.outcome,
                end.exit_code,
                end.started_at,
                end.finished_at,
                end.peak_memory.map(signed),
                end.log_bytes.map(signed),
                end.log_truncated,
                worker
            ],
        )?;
        Ok(())
    }

    /// Closes the attempts of a task that never reported, as `lost` or
    /// `cancelled`.
    pub(crate) fn close_attempts(
        &self,
        run: &str,
        task: &str,
        outcome: &str,
        now: i64,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE task_attempts SET outcome = ?3, finished_at = ?4
             WHERE run_id = ?1 AND task = ?2 AND finished_at IS NULL",
            params![run, task, outcome, now],
        )?;
        Ok(())
    }

    /// The attempts of a task of a run, or of all its tasks, oldest first.
    pub(crate) fn attempts(&self, run: &str, task: Option<&str>) -> Result<Vec<AttemptRow>> {
        let mut statement = self.conn.prepare(
            "SELECT task, attempt, worker, version_hash, assigned_at, started_at, finished_at,
                    outcome, exit_code, peak_memory, log_bytes, log_truncated
             FROM task_attempts
             WHERE run_id = ?1 AND (?2 IS NULL OR task = ?2) ORDER BY task, attempt",
        )?;
        let rows = statement.query_map(params![run, task], attempt_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    // Workers and tokens

    /// Registers a worker that joined, or its new certificate.
    pub(crate) fn save_worker(&self, name: &str, cert_serial: &str, now: i64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO workers (name, cert_serial, joined_at) VALUES (?1, ?2, ?3)
             ON CONFLICT (name) DO UPDATE
               SET cert_serial = excluded.cert_serial, joined_at = excluded.joined_at,
                   revoked_at = NULL",
            params![name, cert_serial, now],
        )?;
        Ok(())
    }

    /// Revokes a worker. Returns false if there is no such worker.
    pub(crate) fn revoke_worker(&self, name: &str, now: i64) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE workers SET revoked_at = ?2 WHERE name = ?1 AND revoked_at IS NULL",
            params![name, now],
        )?;
        Ok(changed > 0)
    }

    /// The registry of workers, by name.
    pub(crate) fn workers(&self) -> Result<Vec<WorkerRow>> {
        let mut statement = self.conn.prepare(
            "SELECT name, cert_serial, joined_at, revoked_at FROM workers ORDER BY name",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(WorkerRow {
                name: row.get(0)?,
                cert_serial: row.get(1)?,
                joined_at: row.get(2)?,
                revoked_at: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Saves a token: its hash, never the secret.
    pub(crate) fn insert_token(
        &self,
        id: &str,
        kind: &str,
        label: Option<&str>,
        secret_hash: &str,
        now: i64,
        expires_at: Option<i64>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO tokens (id, kind, label, secret_hash, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, kind, label, secret_hash, now, expires_at],
        )?;
        Ok(())
    }

    /// Every token, newest first. The secrets are not there: only their
    /// hashes are stored, and those are not returned.
    pub(crate) fn tokens(&self) -> Result<Vec<TokenRow>> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT {TOKEN_COLUMNS} FROM tokens ORDER BY created_at DESC, id"
        ))?;
        let rows = statement.query_map([], token_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Revokes one token. Returns false if there is no such token, or it
    /// was revoked already.
    pub(crate) fn revoke_token(&self, id: &str, now: i64) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE tokens SET revoked_at = ?2 WHERE id = ?1 AND revoked_at IS NULL",
            params![id, now],
        )?;
        Ok(changed > 0)
    }

    /// A token by id.
    pub(crate) fn token(&self, id: &str) -> Result<Option<TokenRow>> {
        let sql = format!("SELECT {TOKEN_COLUMNS} FROM tokens WHERE id = ?1");
        Ok(self.conn.query_row(&sql, [id], token_row).optional()?)
    }

    /// Revokes every token of a kind, when the join token is rotated.
    pub(crate) fn revoke_tokens(&self, kind: &str, now: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE tokens SET revoked_at = ?2 WHERE kind = ?1 AND revoked_at IS NULL",
            params![kind, now],
        )?;
        Ok(())
    }

    /// Whether some token of this kind is still valid.
    pub(crate) fn has_token(&self, kind: &str) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM tokens WHERE kind = ?1 AND revoked_at IS NULL
                                              AND (expires_at IS NULL OR expires_at > ?2))",
            params![kind, crate::util::now_ms()],
            |row| row.get(0),
        )?)
    }

    // Retention

    /// Deletes the runs finished before `cutoff`, with their tasks and
    /// attempts, and the versions nobody uses any more. Returns DAG and id
    /// of the deleted runs, whose logs go with them.
    pub(crate) fn delete_runs_before(&self, cutoff: i64) -> Result<Vec<(String, String)>> {
        let mut statement = self.conn.prepare(
            "DELETE FROM dag_runs
             WHERE finished_at IS NOT NULL AND finished_at < ?1
               AND state NOT IN ('queued', 'running')
             RETURNING dag_name, id",
        )?;
        let deleted: Vec<(String, String)> = statement
            .query_map([cutoff], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        self.conn.execute(
            "DELETE FROM dag_versions
             WHERE hash NOT IN (SELECT current FROM dags WHERE current IS NOT NULL)
               AND hash NOT IN (SELECT version_hash FROM dag_runs)
               AND hash NOT IN (SELECT version_hash FROM task_attempts)",
            [],
        )?;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with_dag() -> Store {
        let store = Store::in_memory().unwrap();
        assert!(store.apply_dag("etl", "h1", "name: etl", "s1", 10).unwrap());
        store
    }

    fn run(id: &str, logical_time: i64, trigger: &str) -> RunRow {
        RunRow {
            id: id.to_owned(),
            dag_name: "etl".to_owned(),
            version_hash: "h1".to_owned(),
            logical_time,
            trigger: trigger.to_owned(),
            params: "{}".to_owned(),
            state: "running".to_owned(),
            created_at: 100,
            started_at: Some(100),
            finished_at: None,
            problem: None,
        }
    }

    #[test]
    fn a_finished_run_can_wait_to_run_again() {
        let store = store_with_dag();
        store.insert_run(&run("r1", 100, "manual")).unwrap();
        store.finish_run("r1", "failed", 200).unwrap();
        assert_eq!(store.rerun_request("r1").unwrap(), None);

        store.queue_rerun("r1", "{}").unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().state, "queued");
        assert_eq!(store.rerun_request("r1").unwrap().as_deref(), Some("{}"));
        assert_eq!(store.open_runs().unwrap().len(), 1, "open until it runs");

        store.drop_rerun("r1", "failed").unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().state, "failed");
        assert_eq!(store.rerun_request("r1").unwrap(), None);

        store.queue_rerun("r1", "{}").unwrap();
        store.reopen_run("r1", "h1").unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().state, "running");
        assert_eq!(store.rerun_request("r1").unwrap(), None);
    }

    #[test]
    fn an_attempt_belongs_to_the_worker_it_was_assigned_to() {
        let store = store_with_dag();
        store.insert_run(&run("r1", 100, "manual")).unwrap();
        store
            .insert_tasks("r1", "etl", ["extract"].into_iter())
            .unwrap();
        store
            .insert_attempt("r1", "extract", 1, "w1", "h1", 110)
            .unwrap();
        let dag = |task: &str, attempt, worker: &str| {
            attempt_dag(&store.conn, "r1", task, attempt, worker).unwrap()
        };
        assert_eq!(dag("extract", 1, "w1").as_deref(), Some("etl"));
        assert_eq!(dag("extract", 1, "w2"), None, "another worker's attempt");
        assert_eq!(dag("extract", 2, "w1"), None, "an attempt never assigned");
        assert_eq!(dag("made-up", 1, "w1"), None);
    }

    #[test]
    fn applying_the_same_version_twice_adds_nothing() {
        let store = store_with_dag();
        assert!(!store.apply_dag("etl", "h1", "name: etl", "s1", 20).unwrap());
        assert!(
            store
                .apply_dag("etl", "h2", "name: etl # v2", "s1", 30)
                .unwrap()
        );
        let dag = store.dag("etl").unwrap().unwrap();
        assert_eq!(dag.current, "h2");
        assert_eq!(dag.created_at, 10);
        assert!(store.version("h1").unwrap().is_some(), "old versions stay");
    }

    #[test]
    fn the_timetable_answers_the_two_questions_of_a_fire() {
        let store = store_with_dag();
        store
            .apply_dag("other", "h9", "name: other", "s9", 10)
            .unwrap();
        store.set_next_fire("etl", Some(1_000)).unwrap();
        store.set_next_fire("other", Some(2_000)).unwrap();
        assert_eq!(store.due(1_500).unwrap(), ["etl"]);
        assert_eq!(store.next_fire_after(1_500).unwrap(), Some(2_000));
        store.set_next_fire("etl", Some(3_000)).unwrap();
        assert_eq!(store.dag("etl").unwrap().unwrap().next_fire, Some(3_000));
        store.set_next_fire("other", None).unwrap();
        assert_eq!(store.next_fire_after(5_000).unwrap(), None);
    }

    #[test]
    fn one_run_per_fire_but_manual_runs_can_repeat_it() {
        let store = store_with_dag();
        store.insert_run(&run("r1", 500, "cron")).unwrap();
        assert!(store.insert_run(&run("r2", 500, "catchup")).is_err());
        store.insert_run(&run("r3", 500, "manual")).unwrap();
        assert!(
            store.insert_run(&run("r4", 600, "nightly")).is_err(),
            "unknown trigger"
        );
    }

    #[test]
    fn a_failed_transaction_leaves_nothing() {
        let store = store_with_dag();
        let result: Result<()> = store.transaction(|store| {
            store.insert_run(&run("r1", 500, "cron"))?;
            anyhow::bail!("crash");
        });
        assert!(result.is_err());
        assert!(store.run("r1").unwrap().is_none());
    }

    #[test]
    fn tasks_and_attempts_follow_a_run() {
        let store = store_with_dag();
        store.insert_run(&run("0192-aaaa", 500, "cron")).unwrap();
        store
            .insert_tasks("0192-aaaa", "etl", ["clean", "extract"].into_iter())
            .unwrap();
        store
            .save_task("0192-aaaa", "extract", "assigned", 1)
            .unwrap();
        store
            .insert_attempt("0192-aaaa", "extract", 1, "w1", "h1", 110)
            .unwrap();
        store.start_attempt("0192-aaaa", "extract", 1, 120).unwrap();
        let end = AttemptEnd {
            outcome: "success",
            exit_code: Some(0),
            started_at: Some(120),
            finished_at: 180,
            peak_memory: Some(1 << 20),
            log_bytes: Some(42),
            log_truncated: false,
        };
        store
            .finish_attempt("0192-aaaa", "extract", 1, "w1", &end)
            .unwrap();

        let tasks = store.tasks("0192-aaaa").unwrap();
        assert_eq!(tasks[1].state, "assigned");
        let attempts = store.attempts("0192-aaaa", Some("extract")).unwrap();
        assert_eq!(attempts[0].outcome.as_deref(), Some("success"));
        assert_eq!(attempts[0].log_bytes, Some(42));
        assert_eq!(
            store.run("0192").unwrap().unwrap().id,
            "0192-aaaa",
            "by prefix"
        );

        // A lost attempt stays lost even if its worker reports later.
        store
            .insert_attempt("0192-aaaa", "clean", 1, "w1", "h1", 200)
            .unwrap();
        store
            .close_attempts("0192-aaaa", "clean", "lost", 230)
            .unwrap();
        store
            .finish_attempt("0192-aaaa", "clean", 1, "w1", &end)
            .unwrap();
        // Another worker cannot rewrite the attempt.
        let forged = AttemptEnd {
            outcome: "failed",
            ..end.clone()
        };
        store
            .finish_attempt("0192-aaaa", "extract", 1, "w9", &forged)
            .unwrap();
        let attempts = store.attempts("0192-aaaa", Some("extract")).unwrap();
        assert_eq!(attempts[0].outcome.as_deref(), Some("success"));
        let attempts = store.attempts("0192-aaaa", Some("clean")).unwrap();
        assert_eq!(attempts[0].outcome.as_deref(), Some("lost"));
    }

    #[test]
    fn retention_deletes_old_runs_with_everything_they_own() {
        let store = store_with_dag();
        store.insert_run(&run("old", 500, "cron")).unwrap();
        store
            .insert_tasks("old", "etl", ["extract"].into_iter())
            .unwrap();
        store
            .insert_attempt("old", "extract", 1, "w1", "h1", 110)
            .unwrap();
        store.finish_run("old", "success", 1_000).unwrap();
        store.insert_run(&run("open", 600, "cron")).unwrap();
        store
            .apply_dag("etl", "h2", "name: etl # v2", "s1", 30)
            .unwrap();

        assert_eq!(
            store.delete_runs_before(5_000).unwrap(),
            [("etl".to_owned(), "old".to_owned())]
        );
        assert!(store.tasks("old").unwrap().is_empty());
        assert!(store.attempts("old", None).unwrap().is_empty());
        assert!(store.run("open").unwrap().is_some());
        assert!(
            store.version("h1").unwrap().is_some(),
            "still used by `open`"
        );
    }

    #[test]
    fn a_backup_is_a_whole_database_taken_while_the_master_writes() {
        let dir = std::env::temp_dir().join(format!("lwwo-backup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("lwwo.db");
        let store = Store::open(&db, Durability::Normal).unwrap();
        store.apply_dag("etl", "h1", "name: etl", "s1", 10).unwrap();
        // The master's own connection stays open, as when it runs.
        let target = dir.join("copy.db");
        backup(&db, &target).unwrap();
        store
            .apply_dag("later", "h2", "name: later", "s2", 20)
            .unwrap();

        let copy = Store::open(&target, Durability::Normal).unwrap();
        assert_eq!(copy.dag("etl").unwrap().unwrap().current, "h1");
        assert!(
            copy.dag("later").unwrap().is_none(),
            "the copy is of one instant"
        );
        assert!(backup(&db, &target).is_err(), "it does not overwrite");
        drop((store, copy));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn tokens_and_workers() {
        let store = Store::in_memory().unwrap();
        store
            .insert_token("t1", "join", None, "hash", 10, None)
            .unwrap();
        assert!(store.has_token("join").unwrap());
        store.revoke_tokens("join", 20).unwrap();
        assert!(!store.has_token("join").unwrap());
        assert_eq!(store.token("t1").unwrap().unwrap().revoked_at, Some(20));
        assert!(
            store
                .insert_token("t2", "admin", None, "hash", 10, None)
                .is_err()
        );
        store
            .insert_token("t3", "api", Some("ci"), "hash", 10, Some(1))
            .unwrap();
        assert!(
            !store.has_token("api").unwrap(),
            "an expired token does not count"
        );
        store
            .insert_token("t4", "api", None, "hash", 20, None)
            .unwrap();
        assert!(store.has_token("api").unwrap());
        assert!(store.revoke_token("t4", 30).unwrap());
        assert!(!store.revoke_token("t4", 40).unwrap(), "only once");
        assert!(!store.has_token("api").unwrap());
        let listed: Vec<String> = store.tokens().unwrap().into_iter().map(|t| t.id).collect();
        assert_eq!(listed, ["t4", "t1", "t3"]);

        store.save_worker("w1", "4f:1a", 10).unwrap();
        assert!(store.revoke_worker("w1", 20).unwrap());
        assert!(!store.revoke_worker("w1", 30).unwrap());
        assert_eq!(store.workers().unwrap()[0].revoked_at, Some(20));
    }
}
